"""Library read models for the operator console.

These endpoints project hdcache disks, tape-copy groups, and Remanence
LibraryService state into the P5 console contract. Hardware identity is shaped
server-side from the caller's capabilities; disk mount paths are deliberately
not read into any payload.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass
from typing import Any

import grpc
from fastapi import APIRouter, Request
from google.protobuf.empty_pb2 import Empty
from sqlalchemy import case, func, select
from sqlalchemy.orm import contains_eager

from sutradhara._proto import layer5_pb2, layer5_pb2_grpc
from sutradhara.api.console import iso_utc, raise_console_error, require_view, sanitize_text
from sutradhara.api.identity import parse_identity
from sutradhara.backend.remanence import _grpc_channel_options, _grpc_target
from sutradhara.catalog.models import Backend, Copy, OffsiteConfirmation
from sutradhara.catalog.session import session_scope
from sutradhara.catalog.types import BackendKind, CopyHealth
from sutradhara.hdcache.models import CacheDisk, CacheEntry
from sutradhara.hdcache.repopulate import DrillStatus, drill_status
from sutradhara.replication import _copy_media_id

router = APIRouter()

DEFAULT_LIMIT = 50
MAX_LIMIT = 200
REMOTE_TIMEOUT_SECONDS = 3.0
VTL_REVISIONS = frozenset({"D.00", "D2D0"})
TAPE_BACKENDS = frozenset({BackendKind.REM_TAPE.value, BackendKind.D2_TAPE.value})
HEALTH_PRIORITY = {
    CopyHealth.CORRUPT.value: 4,
    CopyHealth.MISSING.value: 3,
    CopyHealth.SUSPECT.value: 2,
    CopyHealth.OK.value: 1,
}


@dataclass(frozen=True)
class _DiskCounts:
    entry_count: int
    lost_count: int


@dataclass
class _TapeGroup:
    backend_name: str
    backend_kind: str
    pool_id: str | None
    media_id: str
    display_label: str | None
    library: str | None
    copy_ids: set[int]
    health_by_copy: dict[int, str]
    last_checked_at: dt.datetime | None = None


@dataclass(frozen=True)
class _TapeCatalogEntry:
    media_id: str
    library: str | None


@dataclass(frozen=True)
class _TapeCatalog:
    by_uuid: dict[str, _TapeCatalogEntry]
    by_media_id: dict[str, _TapeCatalogEntry]


@dataclass(frozen=True)
class _RemanenceClients:
    library: Any | None
    catalog: Any | None
    channel: grpc.Channel | None = None

    def close(self) -> None:
        if self.channel is not None:
            self.channel.close()


@router.get("/api/ui/library/disks")
def get_library_disks(request: Request, limit: str | None = None) -> dict[str, object]:
    """Return hdcache disks with whitelist-only fields and aggregate counts."""

    identity = require_view(parse_identity(request.headers))
    page_limit = _parse_limit(limit)
    is_admin = identity.has_capability("can_admin")
    with session_scope(request.app.state.engine) as session:
        aggregates = _disk_counts_subquery()
        query = (
            select(
                CacheDisk,
                func.coalesce(aggregates.c.entry_count, 0).label("entry_count"),
                func.coalesce(aggregates.c.lost_count, 0).label("lost_count"),
                func.count().over().label("total"),
            )
            .outerjoin(aggregates, aggregates.c.disk_id == CacheDisk.disk_id)
            .order_by(CacheDisk.disk_id)
            .limit(page_limit)
        )
        rows = list(session.execute(query))
        disks = [
            _disk_payload(
                row[0],
                _DiskCounts(entry_count=int(row[1] or 0), lost_count=int(row[2] or 0)),
                is_admin=is_admin,
            )
            for row in rows
        ]
        total = int(rows[0][3]) if rows else 0
    return {"total": total, "truncated": total > len(disks), "disks": disks}


@router.get("/api/ui/library/disks/{disk_id}")
def get_library_disk(request: Request, disk_id: str) -> dict[str, object]:
    """Return one disk and latest repopulation drill progress."""

    identity = require_view(parse_identity(request.headers))
    is_admin = identity.has_capability("can_admin")
    with session_scope(request.app.state.engine) as session:
        aggregates = _disk_counts_subquery()
        row = session.execute(
            select(
                CacheDisk,
                func.coalesce(aggregates.c.entry_count, 0).label("entry_count"),
                func.coalesce(aggregates.c.lost_count, 0).label("lost_count"),
            )
            .outerjoin(aggregates, aggregates.c.disk_id == CacheDisk.disk_id)
            .where(CacheDisk.disk_id == disk_id)
        ).one_or_none()
        if row is None:
            raise_console_error(404, "not_found", f"unknown disk {disk_id!r}")
        payload = _disk_payload(
            row[0],
            _DiskCounts(entry_count=int(row[1] or 0), lost_count=int(row[2] or 0)),
            is_admin=is_admin,
        )
        payload["drills"] = [_drill_payload(status) for status in drill_status(session, disk_id)]
        return payload


@router.get("/api/ui/library/tapes")
def get_library_tapes(request: Request, limit: str | None = None) -> dict[str, object]:
    """Return tape-copy groups keyed by stable opaque tokens."""

    identity = require_view(parse_identity(request.headers))
    page_limit = _parse_limit(limit)
    is_admin = identity.has_capability("can_admin")
    catalog = _tape_catalog(request)
    with session_scope(request.app.state.engine) as session:
        confirmed_media_ids = set(
            session.scalars(
                select(OffsiteConfirmation.media_id).where(OffsiteConfirmation.revoked_at.is_(None))
            )
        )
        copy_rows = list(
            session.execute(
                select(Copy, Backend)
                .join(Backend, Copy.backend_id == Backend.id)
                .options(contains_eager(Copy.backend))
                .where(Backend.kind.in_(TAPE_BACKENDS), Copy.deleted_at.is_(None))
                .order_by(Backend.name, Copy.pool_id, Copy.id)
            )
        )
    groups = _group_tape_copies(copy_rows, catalog)
    sorted_groups = sorted(
        groups,
        key=lambda group: (
            group.backend_kind,
            group.backend_name,
            group.pool_id or "",
            group.media_id,
        ),
    )
    page = sorted_groups[:page_limit]
    return {
        "total": len(sorted_groups),
        "truncated": len(sorted_groups) > len(page),
        "tapes": [
            _tape_payload(group, is_admin=is_admin, confirmed_media_ids=confirmed_media_ids)
            for group in page
        ],
    }


@router.get("/api/ui/library/drives")
def get_library_drives(request: Request) -> dict[str, object]:
    """Return complete Remanence VTL drive and slot enumeration."""

    identity = require_view(parse_identity(request.headers))
    is_admin = identity.has_capability("can_admin")
    clients = _remanence_clients(request)
    if clients.library is None:
        raise_console_error(503, "unavailable", "remanence library service unreachable")
    try:
        response = clients.library.ListLibraries(Empty(), timeout=REMOTE_TIMEOUT_SECONDS)
        libraries: list[dict[str, object]] = []
        for listed_library in response.libraries:
            state = clients.library.GetLibrary(
                layer5_pb2.GetLibraryRequest(library_uuid=listed_library.library_uuid),
                timeout=REMOTE_TIMEOUT_SECONDS,
            )
            state_library = _effective_library(state.library, listed_library)
            if state_library.product_revision not in VTL_REVISIONS:
                continue
            libraries.append(_library_payload(state, library=state_library, is_admin=is_admin))
    except grpc.RpcError:
        raise_console_error(503, "unavailable", "remanence library service unreachable")
    finally:
        clients.close()
    libraries.sort(
        key=lambda row: (_revision_rank(str(row["changer_revision"])), str(row["library"]))
    )
    return {"libraries": libraries}


def _disk_counts_subquery() -> Any:
    return (
        select(
            CacheEntry.disk_id.label("disk_id"),
            func.count(CacheEntry.content_sha256).label("entry_count"),
            func.sum(case((CacheEntry.state == "lost", 1), else_=0)).label("lost_count"),
        )
        .group_by(CacheEntry.disk_id)
        .subquery()
    )


def _disk_payload(disk: CacheDisk, counts: _DiskCounts, *, is_admin: bool) -> dict[str, object]:
    payload: dict[str, object] = {
        "disk_id": _text(disk.disk_id),
        "state": _text(disk.state),
        "capacity_bytes": int(disk.capacity_bytes),
        "filled_bytes": int(disk.filled_bytes),
        "capacity_state": _text(disk.capacity_state),
        "last_walk_at": _optional_iso(disk.last_walk_at),
        "entry_count": counts.entry_count,
        "lost_count": counts.lost_count,
    }
    if is_admin:
        payload.update(
            {
                "serial": _text(disk.serial),
                "wwn": _optional_text(disk.wwn),
                "fs_uuid": _text(disk.fs_uuid),
                "enclosure": _optional_text(disk.enclosure),
                "slot": _optional_text(disk.slot),
                "smart_status": _optional_text(disk.smart_status),
            }
        )
    return payload


def _drill_payload(status: DrillStatus) -> dict[str, object]:
    return {
        "drill_id": _text(status.drill_id),
        "lost_at": _optional_iso(status.started_at),
        "entries_lost": status.remaining_entries + status.refilled_entries,
        "entries_refilled": status.refilled_entries,
    }


def _group_tape_copies(
    rows: list[tuple[Copy, Backend]],
    catalog: _TapeCatalog,
) -> list[_TapeGroup]:
    groups: dict[tuple[str, str, str | None, str], _TapeGroup] = {}
    for copy, backend in rows:
        backend_kind = _value(backend.kind)
        media_id = _copy_media_id(copy)
        if media_id is None:
            media_id = _fallback_media_id(copy)
        display_label, library = _media_display(
            copy,
            backend_kind=backend_kind,
            catalog=catalog,
        )
        key = (_text(backend.name), backend_kind, copy.pool_id, media_id)
        group = groups.setdefault(
            key,
            _TapeGroup(
                backend_name=_text(backend.name),
                backend_kind=backend_kind,
                pool_id=None if copy.pool_id is None else _text(copy.pool_id),
                media_id=media_id,
                display_label=display_label,
                library=library,
                copy_ids=set(),
                health_by_copy={},
            ),
        )
        if group.library is None and library is not None:
            group.library = library
        group.copy_ids.add(copy.id)
        group.health_by_copy[copy.id] = _value(copy.health)
        if copy.last_checked_at is not None and (
            group.last_checked_at is None or copy.last_checked_at > group.last_checked_at
        ):
            group.last_checked_at = copy.last_checked_at
    return list(groups.values())


def _media_display(
    copy: Copy,
    *,
    backend_kind: str,
    catalog: _TapeCatalog,
) -> tuple[str | None, str | None]:
    """Return operator-facing media labeling, separate from canonical identity."""

    locator = copy.native_locator or {}
    uuid_keys = ("tape_uuid", "volume_uuid")
    for key in uuid_keys:
        value = locator.get(key)
        if isinstance(value, str) and value:
            catalog_entry = catalog.by_uuid.get(_uuid_lookup_key(value))
            if catalog_entry is not None:
                return catalog_entry.media_id, catalog_entry.library
    if backend_kind == BackendKind.D2_TAPE.value:
        media_id = _first_locator_string(locator, ("barcode", "media_id", "voltag", "volume_uuid"))
    else:
        media_id = _first_locator_string(locator, ("voltag", "media_id", "barcode", "tape_uuid"))
    if media_id is None:
        return None, None
    catalog_entry = catalog.by_media_id.get(media_id)
    return media_id, None if catalog_entry is None else catalog_entry.library


def _fallback_media_id(copy: Copy) -> str:
    locator = copy.native_locator or {}
    encoded = json.dumps(locator, sort_keys=True, default=str, separators=(",", ":"))
    return f"unknown:{copy.backend_id}:{hashlib.sha256(encoded.encode('utf-8')).hexdigest()[:12]}"


def _tape_payload(
    group: _TapeGroup,
    *,
    is_admin: bool,
    confirmed_media_ids: set[str],
) -> dict[str, object]:
    media_id = group.media_id
    return {
        "tape_key": _tape_key(group),
        "media_id": media_id if is_admin else None,
        "display_label": group.display_label if is_admin else None,
        "backend_name": group.backend_name,
        "backend_kind": group.backend_kind,
        "pool_id": group.pool_id,
        "library": group.library,
        "object_count": len(group.copy_ids),
        "health_rollup": _health_rollup(group.health_by_copy),
        "last_checked_at": _optional_iso(group.last_checked_at),
        "offsite_confirmed": _is_offsite_confirmed(media_id, confirmed_media_ids),
    }


def _tape_key(group: _TapeGroup) -> str:
    basis = {
        "backend_name": group.backend_name,
        "backend_kind": group.backend_kind,
        "pool_id": group.pool_id,
        "media_id": group.media_id,
    }
    encoded = json.dumps(basis, sort_keys=True, separators=(",", ":"))
    return f"tape_{hashlib.sha256(encoded.encode('utf-8')).hexdigest()[:16]}"


def _is_offsite_confirmed(media_id: str, confirmed_media_ids: set[str]) -> bool:
    return media_id in confirmed_media_ids


def _health_rollup(health_by_copy: dict[int, str]) -> str:
    if not health_by_copy:
        return CopyHealth.MISSING.value
    return max(health_by_copy.values(), key=lambda health: HEALTH_PRIORITY.get(health, 0))


def _library_payload(
    state: layer5_pb2.LibraryState,
    *,
    library: layer5_pb2.Library,
    is_admin: bool,
) -> dict[str, object]:
    uuid_to_voltag = _uuid_to_voltag(state)
    payload: dict[str, object] = {
        "library": _library_label(library),
        "changer_revision": _text(library.product_revision),
        "drives": [
            _drive_payload(drive, uuid_to_voltag=uuid_to_voltag, is_admin=is_admin)
            for drive in sorted(state.drives, key=_drive_sort_key)
        ],
        "slots": [
            _slot_payload(slot, is_admin=is_admin)
            for slot in sorted(state.slots, key=lambda item: item.element_address)
        ],
    }
    return payload


def _effective_library(
    state_library: layer5_pb2.Library,
    listed_library: layer5_pb2.Library,
) -> layer5_pb2.Library:
    if (
        state_library.library_serial
        or state_library.product
        or state_library.product_revision
        or state_library.library_uuid
    ):
        return state_library
    return listed_library


def _revision_rank(revision: str) -> int:
    if revision == "D.00":
        return 0
    if revision == "D2D0":
        return 1
    return 2


def _drive_sort_key(drive: layer5_pb2.Drive) -> tuple[bool, int]:
    """Order known bays numerically and retain unknown bays at the end."""
    known = drive.HasField("element_address")
    return (not known, int(drive.element_address) if known else 0)


def _drive_payload(
    drive: layer5_pb2.Drive,
    *,
    uuid_to_voltag: dict[str, str],
    is_admin: bool,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "bay": (
            str(int(drive.element_address))
            if drive.HasField("element_address")
            else None
        ),
        "status": _drive_status(drive.status),
    }
    if is_admin:
        loaded_uuid = drive.loaded_tape_uuid.hex() if drive.loaded_tape_uuid else None
        payload["serial"] = _optional_text(drive.drive_serial)
        payload["voltag"] = None if loaded_uuid is None else uuid_to_voltag.get(loaded_uuid)
    return payload


def _slot_payload(slot: layer5_pb2.Slot, *, is_admin: bool) -> dict[str, object]:
    payload: dict[str, object] = {
        "address": int(slot.element_address),
        "full": bool(slot.tape_uuid or slot.voltag),
    }
    if is_admin:
        payload["voltag"] = _optional_text(slot.voltag)
    return payload


def _uuid_to_voltag(state: layer5_pb2.LibraryState) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for slot in state.slots:
        if slot.tape_uuid and slot.voltag:
            mapping[slot.tape_uuid.hex()] = _text(slot.voltag)
    for slot in state.import_export_ports:
        if slot.tape_uuid and slot.voltag:
            mapping[slot.tape_uuid.hex()] = _text(slot.voltag)
    return mapping


def _drive_status(value: int) -> str:
    try:
        name = layer5_pb2.Drive.Status.Name(value)
    except ValueError:
        return "unknown"
    return name.removeprefix("DRIVE_STATUS_").lower()


def _library_label(library: layer5_pb2.Library) -> str:
    if library.library_serial:
        return _text(library.library_serial)
    if library.product:
        return _text(library.product)
    if library.library_uuid:
        return library.library_uuid.hex()
    return "unknown"


def _tape_catalog(request: Request) -> _TapeCatalog:
    configured = getattr(request.app.state, "remanence_tape_catalog", None)
    if configured is not None:
        return _configured_tape_catalog(configured)
    clients = _remanence_clients(request)
    if clients.catalog is None:
        return _TapeCatalog(by_uuid={}, by_media_id={})
    try:
        by_uuid: dict[str, _TapeCatalogEntry] = {}
        by_media_id: dict[str, _TapeCatalogEntry] = {}
        libraries = _catalog_libraries(clients)
        if not libraries:
            _record_tapes_from_catalog(
                clients.catalog,
                by_uuid=by_uuid,
                by_media_id=by_media_id,
                library_uuid=None,
                library_label=None,
            )
        for library_uuid, label in libraries:
            _record_tapes_from_catalog(
                clients.catalog,
                by_uuid=by_uuid,
                by_media_id=by_media_id,
                library_uuid=library_uuid,
                library_label=label,
            )
        return _TapeCatalog(by_uuid=by_uuid, by_media_id=by_media_id)
    except grpc.RpcError:
        return _TapeCatalog(by_uuid={}, by_media_id={})
    finally:
        clients.close()


def _configured_tape_catalog(configured: object) -> _TapeCatalog:
    by_uuid: dict[str, _TapeCatalogEntry] = {}
    by_media_id: dict[str, _TapeCatalogEntry] = {}
    if isinstance(configured, dict):
        entries = configured.values()
    elif isinstance(configured, (list, tuple)):
        entries = configured
    else:
        entries = []
    for raw in entries:
        if not isinstance(raw, dict):
            continue
        media_id = raw.get("media_id") or raw.get("voltag")
        if not isinstance(media_id, str) or not media_id:
            continue
        library = raw.get("library")
        catalog_entry = _TapeCatalogEntry(
            media_id=_text(media_id),
            library=_text(library) if isinstance(library, str) and library else None,
        )
        by_media_id[catalog_entry.media_id] = catalog_entry
        tape_uuid = raw.get("tape_uuid") or raw.get("volume_uuid")
        if isinstance(tape_uuid, str) and tape_uuid:
            by_uuid[_uuid_lookup_key(tape_uuid)] = catalog_entry
    return _TapeCatalog(by_uuid=by_uuid, by_media_id=by_media_id)


def _catalog_libraries(clients: _RemanenceClients) -> list[tuple[bytes, str]]:
    if clients.library is None:
        return []
    response = clients.library.ListLibraries(Empty(), timeout=REMOTE_TIMEOUT_SECONDS)
    return [
        (library.library_uuid, _library_label(library))
        for library in response.libraries
        if library.library_uuid
    ]


def _record_tapes_from_catalog(
    catalog: Any,
    *,
    by_uuid: dict[str, _TapeCatalogEntry],
    by_media_id: dict[str, _TapeCatalogEntry],
    library_uuid: bytes | None,
    library_label: str | None,
) -> None:
    page_token = layer5_pb2.PageToken()
    while True:
        request = layer5_pb2.ListTapesRequest(page_size=1000, page_token=page_token)
        if library_uuid is not None:
            request.library_uuid = library_uuid
        response = catalog.ListTapes(request, timeout=REMOTE_TIMEOUT_SECONDS)
        for tape in response.tapes:
            media_id = _text(tape.voltag) if tape.voltag else tape.tape_uuid.hex()
            entry = _TapeCatalogEntry(media_id=media_id, library=library_label)
            by_media_id[media_id] = entry
            if tape.tape_uuid:
                by_uuid[tape.tape_uuid.hex()] = entry
        if not response.next_page_token.value:
            break
        page_token = layer5_pb2.PageToken(value=response.next_page_token.value)


def _remanence_clients(request: Request) -> _RemanenceClients:
    library = getattr(request.app.state, "remanence_library_client", None)
    catalog = getattr(request.app.state, "remanence_catalog_client", None)
    if library is not None or catalog is not None:
        return _RemanenceClients(library=library, catalog=catalog)
    endpoint = _remanence_endpoint(request)
    if endpoint is None:
        return _RemanenceClients(library=None, catalog=None)
    channel = grpc.insecure_channel(_grpc_target(endpoint), options=_grpc_channel_options(endpoint))
    return _RemanenceClients(
        library=layer5_pb2_grpc.LibraryServiceStub(channel),  # type: ignore[no-untyped-call]
        catalog=layer5_pb2_grpc.CatalogStub(channel),  # type: ignore[no-untyped-call]
        channel=channel,
    )


def _remanence_endpoint(request: Request) -> str | None:
    configured = getattr(request.app.state, "remanence_endpoint", None)
    if isinstance(configured, str) and configured:
        return configured
    with session_scope(request.app.state.engine) as session:
        rows = list(
            session.scalars(
                select(Backend)
                .where(Backend.kind == BackendKind.REM_TAPE)
                .order_by(Backend.name, Backend.id)
            )
        )
    for backend in rows:
        endpoint = (backend.config or {}).get("daemon_endpoint")
        if isinstance(endpoint, str) and endpoint:
            return endpoint
    return None


def _first_locator_string(locator: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = locator.get(key)
        if isinstance(value, str) and value:
            return _text(value)
    return None


def _uuid_lookup_key(value: str) -> str:
    return value.replace("-", "").lower()


def _parse_limit(raw: str | None) -> int:
    value = _optional_nonempty(raw)
    if value is None:
        return DEFAULT_LIMIT
    try:
        parsed = int(value)
    except ValueError:
        raise_console_error(400, "bad_request", "limit must be an integer")
    if parsed < 1:
        raise_console_error(400, "bad_request", "limit must be at least 1")
    return min(parsed, MAX_LIMIT)


def _optional_nonempty(raw: str | None) -> str | None:
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped or None


def _optional_iso(value: dt.datetime | None) -> str | None:
    return None if value is None else iso_utc(value)


def _optional_text(value: str | None) -> str | None:
    return None if value is None or value == "" else _text(value)


def _text(value: str) -> str:
    return sanitize_text(value)


def _value(value: object) -> str:
    if hasattr(value, "value"):
        return str(value.value)
    return str(value)
