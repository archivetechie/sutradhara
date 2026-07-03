"""Restore admission and serve manager for the hdcache read seam.

M4 keeps operator-egress authorization above the cache/tape branch: privacy,
validity, and destination confinement are evaluated before a cache hit can be
served or a tape fallback can run. The manager also owns the persisted
``restore_request`` / ``restore_request_item`` state transitions consumed by
the future restore console API.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import tempfile
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.api.identity import Identity
from sutradhara.archive_restore import (
    ArchiveExtractor,
    ArchiveRestoreError,
    RestoreRejectedAsset,
    RestoreResult,
    RestoreSuspectAsset,
    check_asset_restore_allowed,
    restore_asset,
)
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicyError,
    get_artifactclass_policy,
    hdcache_privacy_capability_map_from_env,
)
from sutradhara.backend.factory import backend_from_row
from sutradhara.backend.port import StorageBackend
from sutradhara.catalog.models import ArtifactClassPool, Backend, Pool
from sutradhara.catalog.types import is_content_hash
from sutradhara.hdcache.fill import (
    effective_privacy_level,
    entry_policy_conformant,
    mark_entry_lost_and_delete,
)
from sutradhara.hdcache.models import CacheDisk, CacheEntry, RestoreRequest, RestoreRequestItem
from sutradhara.hdcache.store import (
    AEAD_REPRESENTATION,
    RAW_REPRESENTATION,
    SENTINEL_NAME,
    StoreError,
    read_entry_verified,
)
from sutradhara.keys import KEY_DOMAIN_HDCACHE, KeyEpoch, KeyRegistry, assert_key_epoch_domain
from sutradhara.restore import atomic_write_verified_file, sha256_file
from sutradhara.sealing.port import Opener, Representation
from sutradhara.sealing.rao import RaoCliOpener

LOGGER = logging.getLogger(__name__)

DEFAULT_SCRATCH_ROOT = Path("/var/lib/replica/hdcache-restore-scratch")
RESTORE_DESTINATIONS_ENV = "SUTRADHARA_HDCACHE_RESTORE_DESTINATIONS"

REQUEST_PENDING = "pending"
REQUEST_ACTIVE = "active"
REQUEST_COMPLETED = "completed"
REQUEST_COMPLETED_WITH_ERRORS = "completed_with_errors"

ITEM_QUEUED = "queued"
ITEM_STREAMING = "streaming"
ITEM_DONE = "done"
ITEM_FELL_BACK_TO_TAPE = "fell_back_to_tape"
ITEM_DENIED = "denied"
ITEM_FAILED = "failed"

CapabilityMap = Mapping[str, str]
RestoreBackendResolver = Callable[[Session, str], dict[int, StorageBackend]]
EventSink = Callable[["RestoreEvent"], None]
SourceKind = Literal["cache", "tape"]


class RestoreManagerError(Exception):
    """Base class for hdcache restore manager failures."""


class RestoreDenied(RestoreManagerError):
    """A restore item was denied by the privacy gate."""

    def __init__(self, required_capability: str | None, detail: str | None = None) -> None:
        self.required_capability = required_capability
        self.detail = detail or f"requires {_capability_label(required_capability or '')}"
        super().__init__(self.detail)


class UnknownRestoreDestination(RestoreManagerError):
    """The requested opaque restore destination id is not configured."""


class InvalidRestoreDestination(RestoreManagerError):
    """A restore destination escaped its configured root or is unsafe."""


class CacheServeFailed(RestoreManagerError):
    """A cache hit could not be served and should fall back to tape."""

    def __init__(self, reason: str, detail: str, *, mark_lost: bool) -> None:
        self.reason = reason
        self.detail = detail
        self.mark_lost = mark_lost
        super().__init__(detail)


@dataclass(frozen=True)
class PrivacyOverride:
    """Trusted CLI assertion allowing a private restore without API identity grants."""

    reason: str
    operator: str = "cli"


@dataclass(frozen=True)
class RestoreDestination:
    """Configured export root exposed through an opaque destination id."""

    id: str
    root: Path
    label: str
    writable: bool = True


@dataclass(frozen=True)
class RestoreEvent:
    """Structured hdcache restore event emitted for alarms and audit hooks."""

    code: str
    severity: str
    content_sha256: str | None = None
    artifactclass: str | None = None
    detail: str | None = None
    request_id: str | None = None
    item_id: int | None = None
    destination_id: str | None = None


@dataclass(frozen=True)
class ResolvedReadSource:
    """Gate-approved branch choice for one restore item."""

    asset_hash: bytes
    artifactclass: str
    source: SourceKind
    destination: Path
    cache_entry: CacheEntry | None = None


@dataclass(frozen=True)
class RestoreItemSpec:
    """Admission input for one API-style restore request item."""

    content_sha256: bytes
    artifactclass: str


@dataclass(frozen=True)
class RestoreConfig:
    """Runtime knobs for M4 restore admission and single-stream serve."""

    destinations: Mapping[str, RestoreDestination] = field(default_factory=dict)
    privacy_capability_map: CapabilityMap | None = None
    scratch_root: Path = DEFAULT_SCRATCH_ROOT
    event_sink: EventSink | None = None
    key_registry: KeyRegistry | None = None
    opener: Opener | None = None
    extractor: ArchiveExtractor | None = None
    restore_backends: dict[int, StorageBackend] | None = None
    restore_backend_resolver: RestoreBackendResolver | None = None
    overwrite: bool = False

    def capability_map(self) -> CapabilityMap:
        return (
            self.privacy_capability_map
            if self.privacy_capability_map is not None
            else hdcache_privacy_capability_map_from_env()
        )

    def registry(self) -> KeyRegistry:
        return self.key_registry or KeyRegistry()


@dataclass(frozen=True)
class ServeResult:
    """Result of serving one restore item."""

    item_id: int | None
    source: SourceKind
    output_path: Path
    size_bytes: int


def restore_config_from_env() -> RestoreConfig:
    """Build hdcache restore config from environment variables."""

    scratch = Path(os.environ.get("SUTRADHARA_HDCACHE_SCRATCH_ROOT") or DEFAULT_SCRATCH_ROOT)
    return RestoreConfig(
        destinations=_destinations_from_env(),
        scratch_root=scratch,
    )


def configured_destinations(config: RestoreConfig | None = None) -> list[dict[str, object]]:
    """Return contract-shaped destination summaries for future API routes."""

    final_config = config or restore_config_from_env()
    return [
        {"id": dest.id, "label": dest.label, "writable": dest.writable}
        for dest in final_config.destinations.values()
    ]


def resolve_read_source(
    session: Session,
    *,
    asset_hash: bytes,
    artifactclass: str,
    destination: Path,
    identity_or_override: Identity | PrivacyOverride | None,
    force_suspect: bool = False,
    force_rejected: bool = False,
    config: RestoreConfig | None = None,
) -> ResolvedReadSource:
    """Apply all operator restore gates and choose cache or tape for one asset."""

    if not is_content_hash(asset_hash):
        raise ValueError("asset_hash must be a 32-byte SHA-256 hash")
    final_config = config or restore_config_from_env()
    _check_privacy_gate(
        session,
        asset_hash,
        artifactclass=artifactclass,
        identity_or_override=identity_or_override,
        config=final_config,
    )
    check_asset_restore_allowed(
        session,
        asset_hash,
        force_suspect=force_suspect,
        force_rejected=force_rejected,
    )
    canonical_destination = canonicalize_restore_destination(
        destination,
        overwrite=final_config.overwrite,
    )
    entry = _select_cache_entry(session, asset_hash)
    if entry is None:
        return ResolvedReadSource(
            asset_hash=asset_hash,
            artifactclass=artifactclass,
            source="tape",
            destination=canonical_destination,
        )
    return ResolvedReadSource(
        asset_hash=asset_hash,
        artifactclass=artifactclass,
        source="cache",
        destination=canonical_destination,
        cache_entry=entry,
    )


def admit_restore_request(
    session: Session,
    *,
    identity: Identity,
    destination_id: str,
    items: Iterable[RestoreItemSpec],
    force_suspect: bool = False,
    force_rejected: bool = False,
    config: RestoreConfig | None = None,
) -> RestoreRequest:
    """Persist an API-style restore request, denying inadmissible items individually."""

    final_config = config or restore_config_from_env()
    destination = _destination_by_id(final_config, destination_id)
    request = RestoreRequest(
        id=_new_request_id(),
        identity=identity.operator_username,
        destination_id=destination.id,
        state=REQUEST_PENDING,
    )
    session.add(request)
    session.flush([request])
    for spec in items:
        item = RestoreRequestItem(
            request_id=request.id,
            content_sha256=spec.content_sha256,
            artifactclass=spec.artifactclass,
            state=ITEM_QUEUED,
            detail=None,
        )
        try:
            _check_privacy_gate(
                session,
                spec.content_sha256,
                artifactclass=spec.artifactclass,
                identity_or_override=identity,
                config=final_config,
                request_id=request.id,
                destination_id=destination.id,
            )
            check_asset_restore_allowed(
                session,
                spec.content_sha256,
                force_suspect=force_suspect,
                force_rejected=force_rejected,
            )
        except RestoreDenied as exc:
            item.state = ITEM_DENIED
            item.detail = exc.detail
        except (RestoreSuspectAsset, RestoreRejectedAsset) as exc:
            item.state = ITEM_DENIED
            item.detail = _sanitize_detail(str(exc))
        session.add(item)
    session.flush()
    _update_request_state(request)
    return request


def serve_restore_request(
    session: Session,
    request: RestoreRequest,
    *,
    identity_or_override: Identity | PrivacyOverride | None,
    force_suspect: bool = False,
    force_rejected: bool = False,
    config: RestoreConfig | None = None,
) -> list[ServeResult]:
    """Serve queued items sequentially, updating persisted request/item states."""

    final_config = config or restore_config_from_env()
    request.state = REQUEST_ACTIVE
    results: list[ServeResult] = []
    for item in request.items:
        if item.state != ITEM_QUEUED:
            continue
        try:
            results.append(
                serve_restore_item(
                    session,
                    item,
                    identity_or_override=identity_or_override,
                    force_suspect=force_suspect,
                    force_rejected=force_rejected,
                    config=final_config,
                )
            )
        finally:
            _update_request_state(request)
            session.flush([request, item])
    _update_request_state(request)
    return results


def serve_restore_item(
    session: Session,
    item: RestoreRequestItem,
    *,
    identity_or_override: Identity | PrivacyOverride | None = None,
    force_suspect: bool = False,
    force_rejected: bool = False,
    gates_already_admitted: bool = False,
    config: RestoreConfig | None = None,
) -> ServeResult:
    """Serve one gated restore item from cache or tape with fallback."""

    final_config = config or restore_config_from_env()
    if item.request is None:
        raise RestoreManagerError("restore request item is not attached to a request")
    destination = destination_for_request_item(final_config, item.request.destination_id, item)
    if gates_already_admitted:
        entry = _select_cache_entry(session, item.content_sha256)
        plan = ResolvedReadSource(
            asset_hash=item.content_sha256,
            artifactclass=item.artifactclass,
            source="cache" if entry is not None else "tape",
            destination=destination,
            cache_entry=entry,
        )
    else:
        try:
            plan = resolve_read_source(
                session,
                asset_hash=item.content_sha256,
                artifactclass=item.artifactclass,
                destination=destination,
                identity_or_override=identity_or_override,
                force_suspect=force_suspect,
                force_rejected=force_rejected,
                config=final_config,
            )
        except RestoreDenied as exc:
            _set_item_state(item, ITEM_DENIED, exc.detail)
            return ServeResult(item.id, "tape", destination, 0)
        except (RestoreSuspectAsset, RestoreRejectedAsset) as exc:
            _set_item_state(item, ITEM_DENIED, _sanitize_detail(str(exc)))
            return ServeResult(item.id, "tape", destination, 0)
        except Exception as exc:
            _set_item_state(item, ITEM_FAILED, _sanitize_detail(str(exc)))
            return ServeResult(item.id, "tape", destination, 0)

    if plan.source == "cache" and plan.cache_entry is not None:
        try:
            _set_item_state(item, ITEM_STREAMING, None)
            result = _serve_from_cache(session, plan.cache_entry, plan.destination, final_config)
            _set_item_state(item, ITEM_DONE, None)
            return ServeResult(item.id, "cache", plan.destination, result.size_bytes)
        except CacheServeFailed as exc:
            _emit(
                final_config,
                RestoreEvent(
                    code=f"cache-fallback:{exc.reason}",
                    severity="warning",
                    content_sha256=item.content_sha256.hex(),
                    artifactclass=item.artifactclass,
                    detail=_sanitize_detail(exc.detail),
                    request_id=item.request_id,
                    item_id=item.id,
                    destination_id=item.request.destination_id,
                ),
            )
            if exc.mark_lost and plan.cache_entry is not None:
                mark_entry_lost_and_delete(session, plan.cache_entry)
            _set_item_state(item, ITEM_FELL_BACK_TO_TAPE, "tape mount pending")

    try:
        _set_item_state(item, ITEM_STREAMING, None)
        tape_result = _serve_from_tape(
            session,
            item.content_sha256,
            item.artifactclass,
            plan.destination,
            final_config,
            force_suspect=force_suspect or gates_already_admitted,
            force_rejected=force_rejected or gates_already_admitted,
        )
    except ArchiveRestoreError as exc:
        _set_item_state(item, ITEM_FAILED, _sanitize_detail(str(exc)))
        return ServeResult(item.id, "tape", plan.destination, 0)
    except Exception as exc:
        _set_item_state(item, ITEM_FAILED, _sanitize_detail(str(exc)))
        return ServeResult(item.id, "tape", plan.destination, 0)
    _set_item_state(item, ITEM_DONE, None)
    return ServeResult(item.id, "tape", tape_result.output_path, tape_result.size_bytes)


def restore_to_path(
    session: Session,
    *,
    asset_hash: bytes,
    artifactclass: str,
    destination: Path | str,
    identity_or_override: Identity | PrivacyOverride | None,
    backends: dict[int, StorageBackend],
    extractor: ArchiveExtractor | None = None,
    force_suspect: bool = False,
    force_rejected: bool = False,
    config: RestoreConfig | None = None,
) -> ServeResult:
    """CLI-friendly one-shot restore through the M4 gates and cache/tape seam."""

    final_config = config or restore_config_from_env()
    final_config = RestoreConfig(
        destinations=final_config.destinations,
        privacy_capability_map=final_config.privacy_capability_map,
        scratch_root=final_config.scratch_root,
        event_sink=final_config.event_sink,
        key_registry=final_config.key_registry,
        opener=final_config.opener,
        extractor=extractor or final_config.extractor,
        restore_backends=backends,
        restore_backend_resolver=final_config.restore_backend_resolver,
        overwrite=final_config.overwrite,
    )
    plan = resolve_read_source(
        session,
        asset_hash=asset_hash,
        artifactclass=artifactclass,
        destination=Path(destination),
        identity_or_override=identity_or_override,
        force_suspect=force_suspect,
        force_rejected=force_rejected,
        config=final_config,
    )
    if plan.source == "cache" and plan.cache_entry is not None:
        try:
            cache_result = _serve_from_cache(session, plan.cache_entry, plan.destination, final_config)
            return ServeResult(None, "cache", plan.destination, cache_result.size_bytes)
        except CacheServeFailed as exc:
            _emit(
                final_config,
                RestoreEvent(
                    code=f"cache-fallback:{exc.reason}",
                    severity="warning",
                    content_sha256=asset_hash.hex(),
                    artifactclass=artifactclass,
                    detail=_sanitize_detail(exc.detail),
                ),
            )
            if exc.mark_lost and plan.cache_entry is not None:
                mark_entry_lost_and_delete(session, plan.cache_entry)
    tape_result = _serve_from_tape(
        session,
        asset_hash,
        artifactclass,
        plan.destination,
        final_config,
        force_suspect=force_suspect,
        force_rejected=force_rejected,
    )
    return ServeResult(None, "tape", tape_result.output_path, tape_result.size_bytes)


def destination_for_request_item(
    config: RestoreConfig,
    destination_id: str,
    item: RestoreRequestItem,
) -> Path:
    """Return the confined output path for a request item under its export root."""

    destination = _destination_by_id(config, destination_id)
    return canonicalize_restore_destination(
        Path(item.content_sha256.hex()),
        root=destination.root,
        overwrite=config.overwrite,
    )


def canonicalize_restore_destination(
    destination: Path | str,
    *,
    root: Path | str | None = None,
    overwrite: bool = False,
) -> Path:
    """Canonicalize a restore destination and reject unsafe overwrite/escape cases."""

    raw_path = Path(destination).expanduser()
    if root is None:
        final = raw_path.resolve(strict=False)
        _reject_overwrite(final, overwrite=overwrite)
        return final

    if raw_path.is_absolute():
        raise InvalidRestoreDestination("restore destination below an export root must be relative")
    if any(part in {"", ".", ".."} for part in raw_path.parts):
        raise InvalidRestoreDestination("restore destination contains unsafe path traversal")
    root_path = Path(root).expanduser().resolve(strict=True)
    candidate = root_path / raw_path
    parent = _existing_parent(candidate.parent)
    parent_real = parent.resolve(strict=True)
    try:
        parent_real.relative_to(root_path)
    except ValueError as exc:
        raise InvalidRestoreDestination("restore destination escapes configured export root") from exc
    final = parent_real / candidate.relative_to(parent).as_posix()
    _reject_overwrite(final, overwrite=overwrite)
    return final


def restore_backends_for_artifactclass(
    session: Session,
    artifactclass: str,
) -> dict[int, StorageBackend]:
    """Instantiate all configured restore backends for an artifactclass."""

    policy = get_artifactclass_policy(session, artifactclass)
    active_pool_ids = [
        row.pool_id
        for row in session.scalars(
            select(ArtifactClassPool)
            .where(
                ArtifactClassPool.artifactclass == artifactclass,
                ArtifactClassPool.active.is_(True),
            )
            .order_by(ArtifactClassPool.sort_order, ArtifactClassPool.pool_id)
        )
    ]
    wanted = [
        *policy.restore_preference,
        *(pool_id for pool_id in active_pool_ids if pool_id not in policy.restore_preference),
    ]
    if not wanted:
        return {}
    rows = list(session.scalars(select(Backend).join(Backend.pools).where(Pool.id.in_(wanted))))
    return {row.id: backend_from_row(row) for row in rows}


def _serve_from_cache(
    session: Session,
    entry: CacheEntry,
    destination: Path,
    config: RestoreConfig,
) -> ServeResult:
    disk = session.get(CacheDisk, entry.disk_id)
    if disk is None or disk.state != "active":
        raise CacheServeFailed("disk-inactive", "cache disk is not active", mark_lost=False)
    live = _cache_disk_live(disk)
    if not live:
        disk.state = "absent"
        session.flush([disk])
        raise CacheServeFailed("disk-absent", "cache disk is absent", mark_lost=False)
    if not entry_policy_conformant(session, entry, key_registry=config.registry()):
        raise CacheServeFailed(
            "representation-mismatch",
            "cache entry representation does not satisfy current privacy policy",
            mark_lost=True,
        )

    destination.parent.mkdir(parents=True, exist_ok=True)
    config.scratch_root.mkdir(parents=True, exist_ok=True)
    os.chmod(config.scratch_root, 0o700)
    try:
        with tempfile.TemporaryDirectory(prefix="hdcache-serve-", dir=config.scratch_root) as raw:
            temp_dir = Path(raw)
            if entry.representation == RAW_REPRESENTATION:
                plaintext = temp_dir / "plain"
                with plaintext.open("wb") as output:
                    read_result = read_entry_verified(
                        Path(disk.mount),
                        entry.content_sha256,
                        representation=RAW_REPRESENTATION,
                        output=output,
                    )
                atomic_write_verified_file(plaintext, destination)
                size_bytes = read_result.size_bytes
            elif entry.representation == AEAD_REPRESENTATION:
                sealed = temp_dir / "sealed"
                with sealed.open("wb") as output:
                    read_entry_verified(
                        Path(disk.mount),
                        entry.content_sha256,
                        representation=AEAD_REPRESENTATION,
                        key_epoch=entry.key_epoch,
                        expected_stream_sha256=entry.stored_digest,
                        output=output,
                    )
                opener = config.opener or RaoCliOpener(config.registry(), work_dir=config.scratch_root)
                key_epoch = _cache_key_epoch(entry)
                with opener.open(
                    sealed,
                    Representation.RAO_AEAD_V1,
                    key_epoch=key_epoch,
                    work_dir=config.scratch_root,
                ) as plaintext:
                    digest = sha256_file(plaintext)
                    if digest != entry.content_sha256:
                        raise StoreError(
                            "opened cache plaintext digest mismatch: "
                            f"{digest.hex()} != {entry.content_sha256.hex()}"
                        )
                    size_bytes = plaintext.stat().st_size
                    atomic_write_verified_file(plaintext, destination)
            else:
                raise StoreError(f"unsupported cache representation {entry.representation!r}")
    except (OSError, StoreError, RuntimeError, ValueError, KeyError) as exc:
        raise CacheServeFailed("read-failed", str(exc), mark_lost=True) from exc

    entry.last_read_at = _utcnow()
    if not entry.trusted:
        entry.trusted = True
    session.flush([entry])
    return ServeResult(None, "cache", destination, size_bytes)


def _serve_from_tape(
    session: Session,
    asset_hash: bytes,
    artifactclass: str,
    destination: Path,
    config: RestoreConfig,
    *,
    force_suspect: bool,
    force_rejected: bool,
) -> RestoreResult:
    backends = config.restore_backends
    if backends is None:
        resolver = config.restore_backend_resolver or restore_backends_for_artifactclass
        backends = resolver(session, artifactclass)
    return restore_asset(
        session,
        asset_hash=asset_hash,
        artifactclass=artifactclass,
        destination=destination,
        backends=backends,
        extractor=config.extractor,
        force_suspect=force_suspect,
        force_rejected=force_rejected,
    )


def _check_privacy_gate(
    session: Session,
    asset_hash: bytes,
    *,
    artifactclass: str,
    identity_or_override: Identity | PrivacyOverride | None,
    config: RestoreConfig,
    request_id: str | None = None,
    destination_id: str | None = None,
) -> None:
    privacy_level = effective_privacy_level(session, asset_hash)
    if privacy_level == "none":
        return
    mapping = config.capability_map()
    required = mapping.get(privacy_level)
    if required is None:
        detail = f"privacy level {privacy_level} unmapped (config error)"
        _emit(
            config,
            RestoreEvent(
                code="privacy-unmapped",
                severity="alarm",
                content_sha256=asset_hash.hex(),
                artifactclass=artifactclass,
                detail=detail,
                request_id=request_id,
                destination_id=destination_id,
            ),
        )
        raise RestoreDenied(None, detail)
    if isinstance(identity_or_override, PrivacyOverride):
        if not identity_or_override.reason.strip():
            raise RestoreDenied(required)
        _emit(
            config,
            RestoreEvent(
                code="privacy-override",
                severity="audit",
                content_sha256=asset_hash.hex(),
                artifactclass=artifactclass,
                detail=identity_or_override.reason,
                request_id=request_id,
                destination_id=destination_id,
            ),
        )
        return
    if identity_or_override is not None and identity_or_override.has_capability(required):
        return
    raise RestoreDenied(required)


def _select_cache_entry(session: Session, asset_hash: bytes) -> CacheEntry | None:
    entry = session.get(CacheEntry, asset_hash)
    if entry is None or entry.state != "present":
        return None
    disk = session.get(CacheDisk, entry.disk_id)
    if disk is None or disk.state != "active":
        return None
    return entry


def _cache_disk_live(disk: CacheDisk) -> bool:
    mount = Path(disk.mount)
    if not mount.exists() or not mount.is_dir():
        return False
    return (mount / SENTINEL_NAME).is_file()


def _cache_key_epoch(entry: CacheEntry) -> KeyEpoch:
    if entry.key_epoch is None:
        raise StoreError("AEAD cache entry has no key_epoch")
    assert_key_epoch_domain(entry.key_epoch, KEY_DOMAIN_HDCACHE, context="hdcache serve")
    return KeyEpoch(key_id=entry.key_epoch, created_at="", active=True)


def _destinations_from_env() -> dict[str, RestoreDestination]:
    raw = os.environ.get(RESTORE_DESTINATIONS_ENV)
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ArtifactClassPolicyError(f"{RESTORE_DESTINATIONS_ENV} must be a JSON object") from exc
    if not isinstance(parsed, dict):
        raise ArtifactClassPolicyError(f"{RESTORE_DESTINATIONS_ENV} must be a JSON object")
    destinations: dict[str, RestoreDestination] = {}
    for dest_id, value in parsed.items():
        if not isinstance(dest_id, str) or not dest_id:
            raise ArtifactClassPolicyError("restore destination ids must be non-empty strings")
        if isinstance(value, str):
            root = Path(value)
            label = value
            writable = True
        elif isinstance(value, dict):
            root_raw = value.get("root")
            if not isinstance(root_raw, str) or not root_raw:
                raise ArtifactClassPolicyError(f"restore destination {dest_id!r} needs root")
            root = Path(root_raw)
            label_raw = value.get("label", root_raw)
            if not isinstance(label_raw, str) or not label_raw:
                raise ArtifactClassPolicyError(f"restore destination {dest_id!r} label must be a string")
            label = label_raw
            writable = bool(value.get("writable", True))
        else:
            raise ArtifactClassPolicyError(
                f"restore destination {dest_id!r} must be a path or object"
            )
        destinations[dest_id] = RestoreDestination(
            id=dest_id,
            root=root,
            label=label,
            writable=writable,
        )
    return destinations


def _destination_by_id(config: RestoreConfig, destination_id: str) -> RestoreDestination:
    destination = config.destinations.get(destination_id)
    if destination is None:
        raise UnknownRestoreDestination(f"unknown restore destination_id {destination_id!r}")
    if not destination.writable:
        raise InvalidRestoreDestination(f"restore destination_id {destination_id!r} is not writable")
    return destination


def _existing_parent(path: Path) -> Path:
    current = path
    missing: list[Path] = []
    while not current.exists():
        missing.append(current)
        parent = current.parent
        if parent == current:
            raise InvalidRestoreDestination("restore destination has no existing parent")
        current = parent
    if not current.is_dir():
        raise InvalidRestoreDestination("restore destination parent is not a directory")
    return current


def _reject_overwrite(path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise InvalidRestoreDestination(f"restore destination already exists: {path.name}")


def _update_request_state(request: RestoreRequest) -> None:
    states = [item.state for item in request.items]
    if not states:
        request.state = REQUEST_COMPLETED
        return
    if any(state in {ITEM_QUEUED, ITEM_STREAMING, ITEM_FELL_BACK_TO_TAPE} for state in states):
        request.state = REQUEST_ACTIVE
        return
    if all(state == ITEM_DONE for state in states):
        request.state = REQUEST_COMPLETED
        return
    request.state = REQUEST_COMPLETED_WITH_ERRORS


def _set_item_state(item: RestoreRequestItem, state: str, detail: str | None) -> None:
    item.state = state
    item.detail = detail
    item.updated_at = _utcnow()


def _emit(config: RestoreConfig, event: RestoreEvent) -> None:
    if config.event_sink is not None:
        config.event_sink(event)
        return
    LOGGER.warning(
        "hdcache restore event code=%s severity=%s content_sha256=%s artifactclass=%s detail=%s",
        event.code,
        event.severity,
        event.content_sha256,
        event.artifactclass,
        event.detail,
    )


def _sanitize_detail(detail: str) -> str:
    return detail.replace(os.getcwd(), "<cwd>")


def _capability_label(capability: str) -> str:
    if capability == "can_restore_p2":
        return "sutradhara-restore-p2"
    if capability == "can_restore_p3":
        return "sutradhara-restore-p3"
    return capability


def _new_request_id() -> str:
    return uuid.uuid4().hex


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
