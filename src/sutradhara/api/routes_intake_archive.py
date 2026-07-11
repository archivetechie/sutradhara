"""Intake, archive, and catalog read models for the operator console.

These endpoints are read-only projections over the catalog tables described in
the console P4 contract. They deliberately expose virtual paths and display
tokens only: host-local source paths and backend locators never cross the API.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Request
from sqlalchemy import func, or_, select
from sqlalchemy.orm import aliased

from sutradhara.api.console import (
    iso_utc,
    raise_console_error,
    require_view,
    sanitize_json,
    sanitize_text,
)
from sutradhara.api.identity import parse_identity
from sutradhara.archive_predicate import intake_archive_state_expr, legacy_archived_expr
from sutradhara.catalog.models import (
    AssetDerivation,
    AssetLocator,
    Backend,
    Bundle,
    BundleMember,
    Copy,
    IngestItem,
    Intake,
    LogicalAsset,
    Submission,
)
from sutradhara.catalog.session import session_scope
from sutradhara.catalog.types import (
    BackendKind,
    CopyHealth,
    IntakeStatus,
    SubmissionStatus,
    is_content_hash,
)
from sutradhara.receive_novelty import novelty_summaries, novelty_summary

router = APIRouter()

DEFAULT_LIMIT = 50
MAX_LIMIT = 200
INTAKE_STATUSES = frozenset(status.value for status in IntakeStatus)
INTAKE_STAGES = frozenset({"archived", "registered_unarchived"})
BUNDLE_STATUSES = frozenset({"open", "flushing", "sealed", "held", "aborted"})
SUBMISSION_STATUSES = frozenset(status.value for status in SubmissionStatus)
HEALTH_PRIORITY = {
    CopyHealth.CORRUPT.value: 4,
    CopyHealth.MISSING.value: 3,
    CopyHealth.SUSPECT.value: 2,
    CopyHealth.OK.value: 1,
}
TAPE_BACKENDS = {BackendKind.REM_TAPE.value, BackendKind.D2_TAPE.value}
CLOUD_BACKENDS = {BackendKind.S3.value, BackendKind.GCS.value, BackendKind.AZURE_BLOB.value}
DISK_BACKENDS = {
    BackendKind.REM_DISK.value,
    BackendKind.PLAIN_DISK.value,
    BackendKind.SSH_DISK.value,
    BackendKind.MEMORY.value,
}


@dataclass(frozen=True)
class _CatalogPair:
    content_sha256: bytes
    artifactclass: str
    latest_at: dt.datetime


@dataclass
class _CopyRollup:
    copy_ids: set[int]
    health_by_copy: dict[int, str]
    last_verified_at: dt.datetime | None = None


@router.get("/api/ui/intakes")
def get_intakes(
    request: Request,
    status: str | None = None,
    stage: str | None = None,
    days: str | None = None,
    limit: str | None = None,
) -> dict[str, object]:
    """Return intake rows newest-first with aggregate item counts."""

    require_view(parse_identity(request.headers))
    status_filter = _optional_enum(status, INTAKE_STATUSES, field="status")
    stage_filter = _optional_enum(stage, INTAKE_STAGES, field="stage")
    days_filter = _optional_days(days)
    page_limit = _parse_limit(limit)
    all_semantics = bool(request.app.state.archived_all_semantics)
    with session_scope(request.app.state.engine) as session:
        aggregates = _intake_aggregates_subquery()
        archived = legacy_archived_expr(all_semantics=all_semantics)
        query = (
            select(
                Intake,
                func.coalesce(aggregates.c.item_count, 0).label("item_count"),
                func.coalesce(aggregates.c.bytes_total, 0).label("bytes_total"),
                intake_archive_state_expr().label("archive_state"),
                archived.label("archived"),
                func.count().over().label("total"),
            )
            .outerjoin(aggregates, aggregates.c.intake_id == Intake.intake_id)
            .order_by(Intake.created_at.desc(), Intake.intake_id.desc())
            .limit(page_limit)
        )
        if status_filter is not None:
            query = query.where(Intake.status == status_filter)
        if stage_filter is not None:
            query = query.where(Intake.status == IntakeStatus.REGISTERED.value)
            if stage_filter == "archived":
                query = query.where(archived)
            elif stage_filter == "registered_unarchived":
                query = query.where(~archived)
        if days_filter is not None:
            query = query.where(Intake.created_at >= days_filter)
        rows = list(session.execute(query))
        summaries = novelty_summaries(session, [row[0].intake_id for row in rows])
        intakes = [
            _intake_payload(
                row[0],
                item_count=int(row[1] or 0),
                bytes_total=int(row[2] or 0),
                archive_state=str(row[3]),
                archived=bool(row[4]),
                novelty=summaries[row[0].intake_id],
            )
            for row in rows
        ]
        total = int(rows[0][5]) if rows else 0
    return {"total": total, "truncated": total > len(intakes), "intakes": intakes}


@router.get("/api/ui/intakes/{intake_id}")
def get_intake(request: Request, intake_id: str) -> dict[str, object]:
    """Return one intake with virtual-path items and item-id derivation edges."""

    require_view(parse_identity(request.headers))
    all_semantics = bool(request.app.state.archived_all_semantics)
    with session_scope(request.app.state.engine) as session:
        row = _intake_row(session, intake_id, all_semantics=all_semantics)
        if row is None:
            raise_console_error(404, "not_found", f"unknown intake {intake_id!r}")
        intake, item_count, bytes_total, archive_state, archived = row
        payload = _intake_payload(
            intake,
            item_count=item_count,
            bytes_total=bytes_total,
            archive_state=archive_state,
            archived=archived,
            novelty=novelty_summary(session, intake.intake_id),
        )
        items = list(
            session.scalars(
                select(IngestItem)
                .where(IngestItem.intake_id == intake_id)
                .order_by(IngestItem.virtual_path, IngestItem.id)
            )
        )
        payload["items"] = [_ingest_item_payload(item) for item in items]
        payload["derivations"] = _derivation_payloads(session, intake_id)
        return payload


@router.get("/api/ui/archive/bundles")
def get_archive_bundles(
    request: Request,
    artifactclass: str | None = None,
    status: str | None = None,
    limit: str | None = None,
) -> dict[str, object]:
    """Return archive bundle rows with direct bundle-copy counts."""

    require_view(parse_identity(request.headers))
    artifactclass_filter = _optional_nonempty(artifactclass)
    status_filter = _optional_enum(status, BUNDLE_STATUSES, field="status")
    page_limit = _parse_limit(limit)
    with session_scope(request.app.state.engine) as session:
        copy_counts = (
            select(Copy.bundle_id, func.count(Copy.id).label("copy_count"))
            .where(Copy.bundle_id.is_not(None), Copy.deleted_at.is_(None))
            .group_by(Copy.bundle_id)
            .subquery()
        )
        query = (
            select(
                Bundle,
                func.coalesce(copy_counts.c.copy_count, 0).label("copy_count"),
                func.count().over().label("total"),
            )
            .outerjoin(copy_counts, copy_counts.c.bundle_id == Bundle.id)
            .order_by(Bundle.opened_at.desc(), Bundle.id.desc())
            .limit(page_limit)
        )
        if artifactclass_filter is not None:
            query = query.where(Bundle.artifactclass == artifactclass_filter)
        if status_filter is not None:
            query = query.where(Bundle.status == status_filter)
        rows = list(session.execute(query))
        bundles = [_bundle_payload(row[0], copy_count=int(row[1] or 0)) for row in rows]
        total = int(rows[0][2]) if rows else 0
    return {"total": total, "truncated": total > len(bundles), "bundles": bundles}


@router.get("/api/ui/archive/submissions")
def get_archive_submissions(
    request: Request,
    status: str | None = None,
    limit: str | None = None,
) -> dict[str, object]:
    """Return archive submissions newest-first."""

    require_view(parse_identity(request.headers))
    status_filter = _optional_enum(status, SUBMISSION_STATUSES, field="status")
    page_limit = _parse_limit(limit)
    with session_scope(request.app.state.engine) as session:
        query = (
            select(Submission, func.count().over().label("total"))
            .order_by(Submission.submitted_at.desc(), Submission.id.desc())
            .limit(page_limit)
        )
        if status_filter is not None:
            query = query.where(Submission.status == status_filter)
        rows = list(session.execute(query))
        submissions = [_submission_payload(row[0]) for row in rows]
        total = int(rows[0][1]) if rows else 0
    return {
        "total": total,
        "truncated": total > len(submissions),
        "submissions": submissions,
    }


@router.get("/api/ui/archive/assets/{content_sha256}")
def get_archive_asset(request: Request, content_sha256: str) -> dict[str, object]:
    """Return one asset and its copies through AssetLocator rows."""

    identity = require_view(parse_identity(request.headers))
    digest = _parse_content_sha256(content_sha256)
    is_admin = identity.has_capability("can_admin")
    with session_scope(request.app.state.engine) as session:
        asset = session.get(LogicalAsset, digest)
        if asset is None:
            raise_console_error(404, "not_found", f"unknown asset {content_sha256!r}")
        origin = _latest_ingest_item(session, digest)
        artifactclass = (
            _text(origin.artifactclass)
            if origin is not None
            else _latest_bundle_artifactclass(session, digest)
        )
        copy_rows = list(
            session.execute(
                select(AssetLocator, Copy, Backend)
                .join(Copy, AssetLocator.copy_id == Copy.id)
                .join(Backend, Copy.backend_id == Backend.id)
                .where(
                    AssetLocator.logical_asset_hash == digest,
                    AssetLocator.copy_id.is_not(None),
                    Copy.deleted_at.is_(None),
                )
                .order_by(Copy.id, AssetLocator.id)
            )
        )
        return {
            "content_sha256": digest.hex(),
            "artifactclass": artifactclass,
            "size_bytes": asset.size_bytes,
            "originating_intake_id": (None if origin is None else _text(origin.intake_id)),
            "copies": [
                _asset_copy_payload(locator, copy, backend, is_admin=is_admin)
                for locator, copy, backend in copy_rows
            ],
        }


@router.get("/api/ui/catalog/assets")
def get_catalog_assets(
    request: Request,
    q: str | None = None,
    artifactclass: str | None = None,
    limit: str | None = None,
    offset: str | None = None,
) -> dict[str, object]:
    """Return deterministic asset-by-artifactclass catalog search rows."""

    require_view(parse_identity(request.headers))
    query_text = _optional_nonempty(q)
    artifactclass_filter = _optional_nonempty(artifactclass)
    page_limit = _parse_limit(limit)
    page_offset = _parse_offset(offset)
    with session_scope(request.app.state.engine) as session:
        pairs = _catalog_pairs(
            session,
            query_text=query_text,
            artifactclass_filter=artifactclass_filter,
        )
        total = len(pairs)
        page = pairs[page_offset : page_offset + page_limit]
        hashes = [pair.content_sha256 for pair in page]
        assets = _assets_by_hash(session, hashes)
        rollups = _copy_rollups_by_hash(session, hashes)
        rows = [
            _catalog_asset_payload(
                pair, assets[pair.content_sha256], rollups.get(pair.content_sha256)
            )
            for pair in page
            if pair.content_sha256 in assets
        ]
    return {
        "total": total,
        "truncated": page_offset + len(rows) < total,
        "assets": rows,
    }


def _intake_aggregates_subquery() -> Any:
    return (
        select(
            IngestItem.intake_id.label("intake_id"),
            func.count(IngestItem.id).label("item_count"),
            func.coalesce(func.sum(IngestItem.size_bytes), 0).label("bytes_total"),
        )
        .group_by(IngestItem.intake_id)
        .subquery()
    )


def _intake_row(
    session: Any,
    intake_id: str,
    *,
    all_semantics: bool,
) -> tuple[Intake, int, int, str, bool] | None:
    aggregates = _intake_aggregates_subquery()
    row = session.execute(
        select(
            Intake,
            func.coalesce(aggregates.c.item_count, 0).label("item_count"),
            func.coalesce(aggregates.c.bytes_total, 0).label("bytes_total"),
            intake_archive_state_expr().label("archive_state"),
            legacy_archived_expr(all_semantics=all_semantics).label("archived"),
        )
        .outerjoin(aggregates, aggregates.c.intake_id == Intake.intake_id)
        .where(Intake.intake_id == intake_id)
    ).one_or_none()
    if row is None:
        return None
    return row[0], int(row[1] or 0), int(row[2] or 0), str(row[3]), bool(row[4])


def _intake_payload(
    intake: Intake,
    *,
    item_count: int,
    bytes_total: int,
    archive_state: str,
    archived: bool,
    novelty: dict[str, int] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "intake_id": _text(intake.intake_id),
        "operator": _text(intake.operator),
        "source_kind": _value(intake.source_kind),
        "artifactclass": _text(intake.artifactclass),
        "label": None if intake.label is None else _text(intake.label),
        "status": _value(intake.status),
        "retention_state": _value(intake.retention_state),
        "created_at": iso_utc(intake.created_at),
        "updated_at": iso_utc(intake.updated_at),
        "registered_at": _optional_iso(intake.registered_at),
        "quarantined_at": _optional_iso(intake.quarantined_at),
        "item_count": item_count,
        "bytes_total": bytes_total,
        "archived": archived,
        "archive_state": archive_state,
        "archiveSemantics": 2,
        "source_release_safe": (
            intake.source_kind == "card" and intake.status == IntakeStatus.REGISTERED
        ),
        "novelty": novelty or {
            "total": 0,
            "new": 0,
            "known_durable": 0,
            "known_under_durable": 0,
            "reverified": 0,
        },
    }
    return payload


def _ingest_item_payload(item: IngestItem) -> dict[str, object]:
    return {
        "content_sha256": item.logical_asset_hash.hex(),
        "virtual_path": _text(item.virtual_path),
        "size_bytes": item.size_bytes,
        "artifactclass": _text(item.artifactclass),
        "disposition": _value(item.disposition),
    }


def _derivation_payloads(session: Any, intake_id: str) -> list[dict[str, object]]:
    source_item = aliased(IngestItem)
    derived_item = aliased(IngestItem)
    rows = list(
        session.execute(
            select(
                AssetDerivation.kind,
                source_item.logical_asset_hash,
                derived_item.logical_asset_hash,
            )
            .join(source_item, AssetDerivation.source_item_id == source_item.id)
            .join(derived_item, AssetDerivation.derived_item_id == derived_item.id)
            .where(or_(source_item.intake_id == intake_id, derived_item.intake_id == intake_id))
            .order_by(AssetDerivation.created_at, AssetDerivation.id)
        )
    )
    return [
        {
            "kind": _text(kind),
            "source_sha256": source_hash.hex(),
            "derived_sha256": derived_hash.hex(),
        }
        for kind, source_hash, derived_hash in rows
    ]


def _bundle_payload(bundle: Bundle, *, copy_count: int) -> dict[str, object]:
    return {
        "id": _text(bundle.id),
        "artifactclass": _text(bundle.artifactclass),
        "status": _text(bundle.status),
        "member_count": bundle.member_count,
        "total_bytes": bundle.total_bytes,
        "copy_count": copy_count,
        "opened_at": iso_utc(bundle.opened_at),
        "sealed_at": _optional_iso(bundle.sealed_at),
    }


def _submission_payload(submission: Submission) -> dict[str, object]:
    return {
        "id": _text(submission.id),
        "arrangement_id": submission.arrangement_id,
        "artifactclass": _text(submission.artifactclass),
        "member_count": submission.member_count,
        "status": _value(submission.status),
        "submitted_by": _text(submission.submitted_by),
        "submitted_at": iso_utc(submission.submitted_at),
        "archived_at": _optional_iso(submission.archived_at),
    }


def _asset_copy_payload(
    locator: AssetLocator,
    copy: Copy,
    backend: Backend,
    *,
    is_admin: bool,
) -> dict[str, object]:
    backend_kind = _value(backend.kind)
    pool_id = locator.pool_id or copy.pool_id
    payload: dict[str, object] = {
        "backend_name": _text(backend.name),
        "backend_kind": backend_kind,
        "tier": _value(backend.tier),
        "pool_id": None if pool_id is None else _text(pool_id),
        "health": _value(copy.health),
        "source": _value(copy.source),
        "representation": _text(locator.representation),
        "last_verified_at": _optional_iso(copy.last_verified_at),
    }
    if is_admin:
        payload["locator_summary"] = _locator_summary(
            backend_kind=backend_kind,
            pool_id=pool_id,
            asset_locator=locator.native_locator,
            copy_locator=copy.native_locator,
        )
    return payload


def _latest_ingest_item(session: Any, digest: bytes) -> IngestItem | None:
    return session.scalars(
        select(IngestItem)
        .where(IngestItem.logical_asset_hash == digest)
        .order_by(IngestItem.created_at.desc(), IngestItem.id.desc())
        .limit(1)
    ).first()


def _latest_bundle_artifactclass(session: Any, digest: bytes) -> str | None:
    row = session.execute(
        select(Bundle.artifactclass)
        .join(BundleMember, BundleMember.bundle_id == Bundle.id)
        .where(BundleMember.logical_asset_hash == digest)
        .order_by(BundleMember.added_at.desc(), BundleMember.id.desc())
        .limit(1)
    ).first()
    if row is None:
        return None
    return _text(row[0])


def _catalog_pairs(
    session: Any,
    *,
    query_text: str | None,
    artifactclass_filter: str | None,
) -> list[_CatalogPair]:
    pair_latest: dict[tuple[bytes, str], dt.datetime] = {}
    for content_hash, artifactclass, latest_at in session.execute(
        select(
            IngestItem.logical_asset_hash,
            IngestItem.artifactclass,
            func.max(IngestItem.created_at),
        ).group_by(IngestItem.logical_asset_hash, IngestItem.artifactclass)
    ):
        _merge_pair(pair_latest, content_hash, artifactclass, latest_at)
    for content_hash, artifactclass, latest_at in session.execute(
        select(
            BundleMember.logical_asset_hash,
            Bundle.artifactclass,
            func.max(BundleMember.added_at),
        )
        .join(Bundle, BundleMember.bundle_id == Bundle.id)
        .group_by(BundleMember.logical_asset_hash, Bundle.artifactclass)
    ):
        _merge_pair(pair_latest, content_hash, artifactclass, latest_at)

    filtered: list[_CatalogPair] = []
    query_lower = None if query_text is None else query_text.lower()
    for (content_hash, artifactclass), latest_at in pair_latest.items():
        if artifactclass_filter is not None and artifactclass != artifactclass_filter:
            continue
        hex_hash = content_hash.hex()
        if query_lower is not None and (
            query_lower not in hex_hash and query_lower not in artifactclass.lower()
        ):
            continue
        filtered.append(_CatalogPair(content_hash, artifactclass, _aware_utc(latest_at)))
    return sorted(
        filtered,
        key=lambda pair: (
            -pair.latest_at.timestamp(),
            pair.content_sha256.hex(),
            pair.artifactclass,
        ),
    )


def _merge_pair(
    pair_latest: dict[tuple[bytes, str], dt.datetime],
    content_hash: bytes,
    artifactclass: str,
    latest_at: dt.datetime,
) -> None:
    key = (content_hash, _text(artifactclass))
    existing = pair_latest.get(key)
    latest = _aware_utc(latest_at)
    if existing is None or latest > _aware_utc(existing):
        pair_latest[key] = latest


def _assets_by_hash(session: Any, hashes: list[bytes]) -> dict[bytes, LogicalAsset]:
    if not hashes:
        return {}
    rows = session.scalars(select(LogicalAsset).where(LogicalAsset.content_sha256.in_(hashes)))
    return {asset.content_sha256: asset for asset in rows}


def _copy_rollups_by_hash(session: Any, hashes: list[bytes]) -> dict[bytes, _CopyRollup]:
    if not hashes:
        return {}
    rollups: dict[bytes, _CopyRollup] = {}
    for content_hash, copy_id, health, last_verified_at in session.execute(
        select(Copy.logical_asset_hash, Copy.id, Copy.health, Copy.last_verified_at).where(
            Copy.logical_asset_hash.in_(hashes),
            Copy.deleted_at.is_(None),
        )
    ):
        if content_hash is not None:
            _add_rollup_copy(rollups, content_hash, copy_id, _value(health), last_verified_at)
    for content_hash, copy_id, health, last_verified_at in session.execute(
        select(
            AssetLocator.logical_asset_hash,
            Copy.id,
            Copy.health,
            Copy.last_verified_at,
        )
        .join(Copy, AssetLocator.copy_id == Copy.id)
        .where(
            AssetLocator.logical_asset_hash.in_(hashes),
            AssetLocator.copy_id.is_not(None),
            Copy.deleted_at.is_(None),
        )
    ):
        _add_rollup_copy(rollups, content_hash, copy_id, _value(health), last_verified_at)
    return rollups


def _add_rollup_copy(
    rollups: dict[bytes, _CopyRollup],
    content_hash: bytes,
    copy_id: int,
    health: str,
    last_verified_at: dt.datetime | None,
) -> None:
    rollup = rollups.setdefault(
        content_hash,
        _CopyRollup(copy_ids=set(), health_by_copy={}),
    )
    rollup.copy_ids.add(copy_id)
    rollup.health_by_copy[copy_id] = health
    if last_verified_at is not None and (
        rollup.last_verified_at is None or last_verified_at > rollup.last_verified_at
    ):
        rollup.last_verified_at = last_verified_at


def _catalog_asset_payload(
    pair: _CatalogPair,
    asset: LogicalAsset,
    rollup: _CopyRollup | None,
) -> dict[str, object]:
    return {
        "content_sha256": pair.content_sha256.hex(),
        "artifactclass": _text(pair.artifactclass),
        "media_kind": None if asset.media_kind is None else _value(asset.media_kind),
        "size_bytes": asset.size_bytes,
        "copy_count": 0 if rollup is None else len(rollup.copy_ids),
        "health_rollup": _health_rollup(rollup),
        "last_verified_at": None if rollup is None else _optional_iso(rollup.last_verified_at),
    }


def _health_rollup(rollup: _CopyRollup | None) -> str:
    if rollup is None or not rollup.health_by_copy:
        return CopyHealth.MISSING.value
    return max(
        rollup.health_by_copy.values(),
        key=lambda health: HEALTH_PRIORITY.get(health, 0),
    )


def _locator_summary(
    *,
    backend_kind: str,
    pool_id: str | None,
    asset_locator: dict[str, Any],
    copy_locator: dict[str, Any],
) -> str:
    asset_safe = sanitize_json(asset_locator)
    copy_safe = sanitize_json(copy_locator)
    if not isinstance(asset_safe, dict):
        asset_safe = {"value": asset_safe}
    if not isinstance(copy_safe, dict):
        copy_safe = {"value": copy_safe}
    combined = {"asset_locator": asset_safe, "copy_locator": copy_safe}
    if backend_kind in TAPE_BACKENDS:
        media_id = _first_locator_string(
            (copy_safe, asset_safe),
            ("voltag", "media_id", "barcode", "tape_uuid", "tape", "volume", "volume_id"),
        )
        if media_id is not None:
            return sanitize_text(f"media {media_id}")
        return f"media {_digest_token(combined)}"
    if backend_kind in CLOUD_BACKENDS:
        key = _first_locator_string(
            (asset_safe, copy_safe),
            ("blob_key", "key", "object_key", "object", "name"),
        )
        return f"blob {_digest_token(key if key is not None else combined)}"
    if backend_kind in DISK_BACKENDS:
        key = _first_locator_string(
            (asset_safe, copy_safe),
            ("opaque_key", "key", "object_id", "hash_hex", "object", "path"),
        )
        safe_pool = "<none>" if pool_id is None else sanitize_text(pool_id)
        return f"pool {safe_pool} key {_digest_token(key if key is not None else combined)}"
    return f"locator {_digest_token(combined)}"


def _first_locator_string(
    locators: tuple[dict[str, Any], ...], keys: tuple[str, ...]
) -> str | None:
    for locator in locators:
        for key in keys:
            value = locator.get(key)
            if isinstance(value, str) and value:
                return sanitize_text(value)
    return None


def _digest_token(value: Any) -> str:
    safe = sanitize_json(value)
    encoded = json.dumps(safe, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:12]


def _parse_content_sha256(raw: str) -> bytes:
    if raw.lower() != raw:
        raise_console_error(400, "bad_request", "content_sha256 must be lowercase hex")
    try:
        digest = bytes.fromhex(raw)
    except ValueError:
        raise_console_error(400, "bad_request", "content_sha256 must be lowercase hex")
    if not is_content_hash(digest):
        raise_console_error(400, "bad_request", "content_sha256 must be sha256")
    return digest


def _optional_enum(raw: str | None, allowed: frozenset[str], *, field: str) -> str | None:
    value = _optional_nonempty(raw)
    if value is None:
        return None
    if value not in allowed:
        raise_console_error(400, "bad_request", f"unknown {field} {value!r}")
    return value


def _optional_days(raw: str | None) -> dt.datetime | None:
    value = _optional_nonempty(raw)
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError:
        raise_console_error(400, "bad_request", "days must be an integer")
    if parsed < 0:
        raise_console_error(400, "bad_request", "days must be non-negative")
    return dt.datetime.now(dt.UTC) - dt.timedelta(days=parsed)


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


def _parse_offset(raw: str | None) -> int:
    value = _optional_nonempty(raw)
    if value is None:
        return 0
    try:
        parsed = int(value)
    except ValueError:
        raise_console_error(400, "bad_request", "offset must be an integer")
    if parsed < 0:
        raise_console_error(400, "bad_request", "offset must be non-negative")
    return parsed


def _optional_nonempty(raw: str | None) -> str | None:
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped or None


def _optional_iso(value: dt.datetime | None) -> str | None:
    return None if value is None else iso_utc(value)


def _aware_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _value(value: object) -> str:
    if hasattr(value, "value"):
        return str(value.value)
    return str(value)


def _text(value: str) -> str:
    return sanitize_text(value)
