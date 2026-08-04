"""Restore admission and serve manager for the hdcache read seam.

M4 keeps operator-egress authorization above the cache/tape branch: privacy,
validity, and destination confinement are evaluated before a cache hit can be
served or a tape fallback can run. The manager also owns the persisted
``restore_request`` / ``restore_request_item`` state transitions consumed by
the future restore console API.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import json
import logging
import os
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path, PureWindowsPath
from typing import Literal

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

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
from sutradhara.catalog.models import ArtifactClassPool, Backend, LogicalAsset, Pool
from sutradhara.catalog.types import AssetValidity, is_content_hash
from sutradhara.grpc.store import (
    GrpcDeviceDestinationGrant,
    GrpcLogicalDevice,
    validate_device_scopes,
)
from sutradhara.hdcache.fill import (
    effective_privacy_level,
    entry_policy_conformant,
    mark_entry_lost_and_delete,
)
from sutradhara.hdcache.models import CacheDisk, CacheEntry, RestoreRequest, RestoreRequestItem
from sutradhara.hdcache.read_ordering import plan_restore_request_read_order
from sutradhara.hdcache.store import (
    AEAD_REPRESENTATION,
    BUFFER_SIZE,
    RAW_REPRESENTATION,
    DiskIdentityProbe,
    DiskIdentityResult,
    ExpectedDiskIdentity,
    StoreContentMismatch,
    StoreError,
    StoreReadTimeout,
    probe_disk_liveness_with_deadline,
    read_entry_verified,
    read_hmac_secret,
    verify_disk_identity_with_deadline,
)
from sutradhara.keys import KEY_DOMAIN_HDCACHE, KeyRegistry, assert_key_epoch_domain
from sutradhara.restore import atomic_write_verified_file, restore_progress_context, sha256_file
from sutradhara.sealing.port import Opener, Representation
from sutradhara.sealing.rao import RaoCliOpener

LOGGER = logging.getLogger(__name__)

DEFAULT_SCRATCH_ROOT = Path("/var/lib/replica/hdcache-restore-scratch")
RESTORE_DESTINATIONS_ENV = "SUTRADHARA_HDCACHE_RESTORE_DESTINATIONS"
DEFAULT_STREAM_POOL_SIZE = 24
DEFAULT_AEAD_STREAM_CAP = 4
DEFAULT_WAKE_WINDOW_MULTIPLIER = 2
DEFAULT_READ_DEADLINE_SECONDS = 70.0
DEFAULT_LIVENESS_PROBE_DEADLINE_SECONDS = 2.0

REQUEST_PENDING = "pending"
REQUEST_ACTIVE = "active"
REQUEST_COMPLETED = "completed"
REQUEST_COMPLETED_WITH_ERRORS = "completed_with_errors"

ITEM_QUEUED = "queued"
ITEM_WAKING_DISK = "waking_disk"
ITEM_STREAMING = "streaming"
ITEM_SENT = "sent"
ITEM_DONE = "done"
ITEM_FELL_BACK_TO_TAPE = "fell_back_to_tape"
ITEM_DENIED = "denied"
ITEM_FAILED = "failed"

CapabilityMap = Mapping[str, str]
RestoreBackendResolver = Callable[[Session, str], dict[int, StorageBackend]]
EventSink = Callable[["RestoreEvent"], None]
SourceKind = Literal["cache", "tape"]
SessionFactory = sessionmaker[Session] | Callable[[], Session]
DenialKind = Literal["capability", "privacy_unmapped", "suspect", "rejected"]


class RestoreManagerError(Exception):
    """Base class for hdcache restore manager failures."""


class RestoreDenied(RestoreManagerError):
    """A restore item was denied by the privacy gate."""

    def __init__(self, required_capability: str | None, detail: str | None = None) -> None:
        self.required_capability = required_capability
        self.denial_kind: DenialKind = (
            "privacy_unmapped" if required_capability is None else "capability"
        )
        self.detail = detail or f"requires {_capability_label(required_capability or '')}"
        super().__init__(self.detail)


class UnknownRestoreDestination(RestoreManagerError):
    """The requested opaque restore destination id is not configured."""


class RestoreAdmissionInvalid(RestoreManagerError):
    """A queued restore item lacks persisted admission inputs."""


class InvalidRestoreDestination(RestoreManagerError):
    """A restore destination escaped its configured root or is unsafe."""


class CacheServeFailed(RestoreManagerError):
    """A cache hit could not be served and should fall back to tape."""

    def __init__(
        self,
        reason: str,
        detail: str,
        *,
        mark_lost: bool,
        count_breaker: bool = False,
    ) -> None:
        self.reason = reason
        self.detail = detail
        self.mark_lost = mark_lost
        self.count_breaker = count_breaker
        super().__init__(detail)


@dataclass(frozen=True)
class VerifiedCachePlaintext:
    """A staged, verified cache plaintext with a bounded verifying producer."""

    path: Path
    content_sha256: bytes
    size_bytes: int

    def iter_chunks(self) -> Iterator[bytes]:
        """Yield bounded plaintext and re-verify its catalog identity at EOF."""

        digest = hashlib.sha256()
        emitted = 0
        with self.path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(BUFFER_SIZE), b""):
                digest.update(chunk)
                emitted += len(chunk)
                yield chunk
        if emitted != self.size_bytes:
            raise StoreContentMismatch(
                f"cache plaintext size mismatch: {emitted} != {self.size_bytes}"
            )
        actual = digest.digest()
        if actual != self.content_sha256:
            raise StoreContentMismatch(
                f"cache plaintext digest mismatch: {actual.hex()} != {self.content_sha256.hex()}"
            )


class DiskCircuitBreaker:
    """Process-local per-disk failure breaker for cache serve storms."""

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        window_seconds: float = 300.0,
    ) -> None:
        if failure_threshold <= 0:
            raise ValueError("failure_threshold must be positive")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.failure_threshold = failure_threshold
        self.window_seconds = window_seconds
        self._failures: dict[str, list[float]] = {}
        self._open: set[str] = set()
        self._lock = threading.Lock()

    def is_open(self, disk_id: str) -> bool:
        with self._lock:
            return disk_id in self._open

    def record_failure(self, disk_id: str, *, now: float | None = None) -> bool:
        """Record one failure and return true if this call tripped the disk."""

        stamp = time.monotonic() if now is None else now
        floor = stamp - self.window_seconds
        with self._lock:
            values = [value for value in self._failures.get(disk_id, []) if value >= floor]
            values.append(stamp)
            self._failures[disk_id] = values
            if len(values) < self.failure_threshold or disk_id in self._open:
                return False
            self._open.add(disk_id)
            return True

    def reset(self, disk_id: str) -> None:
        with self._lock:
            self._failures.pop(disk_id, None)
            self._open.discard(disk_id)


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
    final_rel_path: str | None = None


@dataclass(frozen=True)
class RestoreAdmissionInputs:
    """Persisted admission inputs used to re-run worker-side restore gates."""

    identity: Identity
    force_suspect: bool
    force_rejected: bool


@dataclass(frozen=True)
class RestoreConfig:
    """Runtime knobs for restore admission and M5 parallel serve orchestration."""

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
    stream_pool_size: int = DEFAULT_STREAM_POOL_SIZE
    aead_stream_cap: int = DEFAULT_AEAD_STREAM_CAP
    wake_ahead: bool = True
    wake_window_size: int | None = None
    read_deadline_seconds: float = DEFAULT_READ_DEADLINE_SECONDS
    liveness_probe_deadline_seconds: float = DEFAULT_LIVENESS_PROBE_DEADLINE_SECONDS
    breaker: DiskCircuitBreaker = field(default_factory=DiskCircuitBreaker)
    worker_session_factory: SessionFactory | None = None
    hmac_secret: bytes | None = None
    identity_probe: DiskIdentityProbe | None = None

    def __post_init__(self) -> None:
        if self.stream_pool_size <= 0:
            raise ValueError("stream_pool_size must be positive")
        if self.aead_stream_cap <= 0:
            raise ValueError("aead_stream_cap must be positive")
        if self.wake_window_size is not None and self.wake_window_size <= 0:
            raise ValueError("wake_window_size must be positive")
        if self.liveness_probe_deadline_seconds <= 0:
            raise ValueError("liveness_probe_deadline_seconds must be positive")
        if self.hmac_secret is not None and not self.hmac_secret:
            raise ValueError("hmac_secret must not be empty")

    def capability_map(self) -> CapabilityMap:
        return (
            self.privacy_capability_map
            if self.privacy_capability_map is not None
            else hdcache_privacy_capability_map_from_env()
        )

    def registry(self) -> KeyRegistry:
        return self.key_registry or KeyRegistry()

    def disk_hmac_secret(self) -> bytes:
        return self.hmac_secret if self.hmac_secret is not None else read_hmac_secret()

    def wake_window(self) -> int:
        if not self.wake_ahead:
            return self.stream_pool_size
        return self.wake_window_size or (self.stream_pool_size * DEFAULT_WAKE_WINDOW_MULTIPLIER)


@dataclass(frozen=True)
class ServeResult:
    """Result of serving one restore item."""

    item_id: int | None
    source: SourceKind
    output_path: Path
    size_bytes: int
    copy_id: int | None = None


@dataclass(frozen=True)
class _ServeRuntime:
    stream_slots: threading.BoundedSemaphore | None = None
    aead_slots: threading.BoundedSemaphore | None = None
    commit_state_transitions: bool = False


@dataclass(frozen=True)
class _ServeSlotContext:
    stream_slots: threading.BoundedSemaphore | None = None
    aead_slots: threading.BoundedSemaphore | None = None
    aead_slot_held: bool = False


_SERVE_SLOT_CONTEXT = threading.local()


def restore_config_from_env() -> RestoreConfig:
    """Build hdcache restore config from environment variables."""

    from sutradhara.hdcache.alarms import restore_event_alarm_sink

    scratch = Path(os.environ.get("SUTRADHARA_HDCACHE_SCRATCH_ROOT") or DEFAULT_SCRATCH_ROOT)
    return RestoreConfig(
        destinations=_destinations_from_env(),
        scratch_root=scratch,
        event_sink=restore_event_alarm_sink(),
        stream_pool_size=_env_int("SUTRADHARA_HDCACHE_STREAM_POOL_SIZE", DEFAULT_STREAM_POOL_SIZE),
        aead_stream_cap=_env_int("SUTRADHARA_HDCACHE_AEAD_STREAM_CAP", DEFAULT_AEAD_STREAM_CAP),
        wake_ahead=_env_bool("SUTRADHARA_HDCACHE_WAKE_AHEAD", True),
        wake_window_size=_env_optional_int("SUTRADHARA_HDCACHE_WAKE_WINDOW_SIZE"),
        read_deadline_seconds=_env_float(
            "SUTRADHARA_HDCACHE_READ_DEADLINE_SECONDS",
            DEFAULT_READ_DEADLINE_SECONDS,
        ),
        liveness_probe_deadline_seconds=_env_float(
            "SUTRADHARA_HDCACHE_LIVENESS_PROBE_DEADLINE_SECONDS",
            DEFAULT_LIVENESS_PROBE_DEADLINE_SECONDS,
        ),
    )


def configured_destinations(config: RestoreConfig | None = None) -> list[dict[str, object]]:
    """Return contract-shaped destination summaries for future API routes."""

    final_config = config or restore_config_from_env()
    return [
        {"id": dest.id, "label": dest.label, "writable": dest.writable}
        for dest in final_config.destinations.values()
    ]


def bind_restore_event_sink_to_session(config: RestoreConfig, session: Session) -> RestoreConfig:
    """Use the current transaction for the default DB-backed restore alarm sink."""

    from sutradhara.hdcache.alarms import RestoreEventAlarmSink

    sink = config.event_sink
    if isinstance(sink, RestoreEventAlarmSink) and sink.session is None:
        return replace(config, event_sink=sink.bind(session))
    return config


def validate_restore_item_admission(
    session: Session,
    item: RestoreRequestItem,
    *,
    config: RestoreConfig | None = None,
) -> RestoreAdmissionInputs:
    """Validate persisted admission inputs and re-run privacy/validity gates."""

    admission = _admission_inputs_for_item(item)
    final_config = bind_restore_event_sink_to_session(config or restore_config_from_env(), session)
    _check_privacy_gate(
        session,
        item.content_sha256,
        artifactclass=item.artifactclass,
        identity_or_override=admission.identity,
        config=final_config,
        request_id=item.request_id,
        destination_id=item.request.destination_id if item.request is not None else None,
    )
    check_asset_restore_allowed(
        session,
        item.content_sha256,
        force_suspect=admission.force_suspect,
        force_rejected=admission.force_rejected,
    )
    return admission


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
    final_config = bind_restore_event_sink_to_session(config or restore_config_from_env(), session)
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
    entry = _select_cache_entry(session, asset_hash, config=final_config)
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
    delivery_mode: str = "server_local",
    receiver_device_id: str | None = None,
    force_suspect: bool = False,
    force_rejected: bool = False,
    idempotency_key: str | None = None,
    idempotency_body_hash: str | None = None,
    config: RestoreConfig | None = None,
) -> RestoreRequest:
    """Persist an API-style restore request, denying inadmissible items individually."""

    if delivery_mode not in {"server_local", "agent"}:
        raise RestoreAdmissionInvalid(f"unknown delivery_mode {delivery_mode!r}")
    if delivery_mode == "server_local" and receiver_device_id is not None:
        raise RestoreAdmissionInvalid("receiver_device_id is only valid for agent delivery")
    if delivery_mode == "agent" and not receiver_device_id:
        raise RestoreAdmissionInvalid("receiver_device_id is required for agent delivery")

    final_config = bind_restore_event_sink_to_session(config or restore_config_from_env(), session)
    destination = _destination_by_id(final_config, destination_id)
    if delivery_mode == "agent":
        authorize_agent_restore_destination(
            session,
            receiver_device_id=receiver_device_id,
            destination_id=destination.id,
        )
    admitted_at = _utcnow()
    request = RestoreRequest(
        id=_new_request_id(),
        identity=identity.operator_username,
        destination_id=destination.id,
        delivery_mode=delivery_mode,
        receiver_device_id=receiver_device_id,
        state=REQUEST_PENDING,
        admitted_by=identity.operator_username,
        admitted_at=admitted_at,
        admitted_capabilities=list(identity.capabilities),
        idempotency_key=idempotency_key,
        idempotency_body_hash=idempotency_body_hash,
    )
    session.add(request)
    session.flush([request])
    for spec in items:
        if delivery_mode == "agent":
            if spec.final_rel_path is None:
                raise RestoreAdmissionInvalid("final_rel_path is required for every agent item")
            validate_restore_relative_path(spec.final_rel_path)
        item = RestoreRequestItem(
            request_id=request.id,
            content_sha256=spec.content_sha256,
            artifactclass=spec.artifactclass,
            final_rel_path=spec.final_rel_path if delivery_mode == "agent" else None,
            state=ITEM_QUEUED,
            detail=None,
            denial_kind=None,
            size_bytes=_asset_size_bytes(session, spec.content_sha256),
            bytes_restored=0,
            source=None,
            admitted_force_suspect=None,
            admitted_force_rejected=None,
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
            admitted_force_suspect, admitted_force_rejected = _admitted_force_waivers(
                session,
                spec.content_sha256,
                force_suspect=force_suspect,
                force_rejected=force_rejected,
            )
            item.admitted_force_suspect = admitted_force_suspect
            item.admitted_force_rejected = admitted_force_rejected
        except RestoreDenied as exc:
            _set_item_denied(item, exc.denial_kind, exc.detail)
        except (RestoreSuspectAsset, RestoreRejectedAsset) as exc:
            _set_item_denied(
                item,
                _validity_denial_kind(exc),
                _validity_denial_detail(session, spec.content_sha256, exc),
            )
        session.add(item)
    session.flush()
    _update_request_state(request)
    # Read-ordering planning pass: after the request's items are accepted,
    # before item dispatch (design-restore-read-ordering §4.1). Always on;
    # never raises — planning failure degrades to unordered dispatch.
    plan_restore_request_read_order(session, request, config=final_config)
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
    """Serve queued items with the M5 wake window and bounded stream pool."""

    final_config = bind_restore_event_sink_to_session(config or restore_config_from_env(), session)
    if final_config.worker_session_factory is not None:
        return _serve_restore_request_parallel(session, request, config=final_config)
    return _serve_restore_request_in_session(session, request, config=final_config)


def _serve_restore_request_in_session(
    session: Session,
    request: RestoreRequest,
    *,
    config: RestoreConfig,
) -> list[ServeResult]:
    """Serve a request inside the caller's transaction, preserving M4 semantics."""

    request.state = REQUEST_ACTIVE
    results: list[ServeResult] = []
    pending = [item for item in request.items if item.state == ITEM_QUEUED]
    window = config.wake_window()
    _wake_items(session, request, pending[:window], config=config)
    for index, item in enumerate(pending):
        if item.state not in {ITEM_QUEUED, ITEM_WAKING_DISK}:
            continue
        try:
            results.append(
                serve_restore_item(
                    session,
                    item,
                    config=config,
                )
            )
        finally:
            _update_request_state(request)
            session.flush([request, item])
            _wake_items(session, request, pending[index + 1 : index + 1 + window], config=config)
    _update_request_state(request)
    return results


def _serve_restore_request_parallel(
    session: Session,
    request: RestoreRequest,
    *,
    config: RestoreConfig,
) -> list[ServeResult]:
    """Serve a committed request through worker sessions with bounded concurrency."""

    factory = config.worker_session_factory
    if factory is None:
        raise AssertionError("worker_session_factory is required")
    pending = [item for item in request.items if item.state == ITEM_QUEUED]
    if not pending:
        _update_request_state(request)
        return []

    request.state = REQUEST_ACTIVE
    window = config.wake_window()
    _wake_items(session, request, pending[:window], config=config)
    session.flush()
    session.commit()

    runtime = _ServeRuntime(
        stream_slots=threading.BoundedSemaphore(config.stream_pool_size),
        aead_slots=threading.BoundedSemaphore(config.aead_stream_cap),
        commit_state_transitions=True,
    )
    results: list[ServeResult] = []
    submitted = 0
    completed = 0
    futures: dict[Future[ServeResult], int] = {}

    def submit_next(executor: ThreadPoolExecutor) -> None:
        nonlocal submitted
        while submitted < len(pending) and len(futures) < window:
            item_id = pending[submitted].id
            if item_id is None:
                raise RestoreManagerError(
                    "restore request item must be flushed before parallel serve"
                )
            futures[
                executor.submit(
                    _serve_restore_item_in_worker_session,
                    factory,
                    item_id,
                    config,
                    runtime,
                )
            ] = item_id
            submitted += 1

    with ThreadPoolExecutor(max_workers=window) as executor:
        submit_next(executor)
        while futures:
            done, _not_done = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                futures.pop(future)
                results.append(future.result())
                completed += 1
                _wake_items(
                    session,
                    request,
                    pending[completed : completed + window],
                    config=config,
                )
                session.flush()
                session.commit()
            submit_next(executor)

    session.expire_all()
    refreshed = session.get(RestoreRequest, request.id)
    if refreshed is not None:
        _update_request_state(refreshed)
        session.flush([refreshed])
        request.state = refreshed.state
    return results


def _serve_restore_item_in_worker_session(
    factory: SessionFactory,
    item_id: int,
    config: RestoreConfig,
    runtime: _ServeRuntime,
) -> ServeResult:
    session = factory()
    try:
        item = session.get(RestoreRequestItem, item_id)
        if item is None:
            raise RestoreManagerError(f"restore request item id={item_id} disappeared")
        result = serve_restore_item(session, item, config=config, _runtime=runtime)
        session.commit()
        return result
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def serve_restore_item(
    session: Session,
    item: RestoreRequestItem,
    *,
    identity_or_override: Identity | PrivacyOverride | None = None,
    force_suspect: bool = False,
    force_rejected: bool = False,
    gates_already_admitted: bool = False,
    config: RestoreConfig | None = None,
    _runtime: _ServeRuntime | None = None,
) -> ServeResult:
    """Serve one admitted restore item from cache or tape with fallback."""

    if item.request is not None and item.request.delivery_mode != "server_local":
        raise RestoreAdmissionInvalid("agent delivery items cannot enter the server-local writer")
    final_config = bind_restore_event_sink_to_session(config or restore_config_from_env(), session)
    if item.request is None:
        raise RestoreManagerError("restore request item is not attached to a request")
    destination: Path | None = None
    try:
        admission = validate_restore_item_admission(session, item, config=final_config)
        destination = destination_for_request_item(final_config, item.request.destination_id, item)
        _ensure_item_size_bytes(session, item)
        entry = _select_cache_entry(session, item.content_sha256, config=final_config)
        plan = ResolvedReadSource(
            asset_hash=item.content_sha256,
            artifactclass=item.artifactclass,
            source="cache" if entry is not None else "tape",
            destination=destination,
            cache_entry=entry,
        )
    except RestoreAdmissionInvalid as exc:
        _set_item_state(item, ITEM_FAILED, _sanitize_detail(str(exc)))
        raise
    except RestoreDenied as exc:
        _set_item_denied(item, exc.denial_kind, exc.detail)
        return ServeResult(item.id, "tape", destination or Path(item.content_sha256.hex()), 0)
    except (RestoreSuspectAsset, RestoreRejectedAsset) as exc:
        _set_item_denied(
            item,
            _validity_denial_kind(exc),
            _validity_denial_detail(session, item.content_sha256, exc),
        )
        return ServeResult(item.id, "tape", destination or Path(item.content_sha256.hex()), 0)
    except Exception as exc:
        _set_item_state(item, ITEM_FAILED, _sanitize_detail(str(exc)))
        return ServeResult(item.id, "tape", destination or Path(item.content_sha256.hex()), 0)

    fell_back_to_tape = False
    if plan.source == "cache" and plan.cache_entry is not None:
        try:
            _set_item_streaming(item, "cache")
            _update_request_state(item.request)
            session.flush([item.request, item])
            if _runtime is not None and _runtime.commit_state_transitions:
                session.commit()
            with restore_progress_context(_progress_callback(session, item)):
                result = _serve_from_cache_controlled(
                    session,
                    plan.cache_entry,
                    plan.destination,
                    final_config,
                    _runtime,
                )
            _set_item_state(item, ITEM_DONE, None)
            _finish_item_progress(item, result.size_bytes, source="cache")
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
            breaker_open = _record_cache_failure(
                session,
                plan.cache_entry,
                exc,
                final_config,
                content_sha256=item.content_sha256,
                artifactclass=item.artifactclass,
                request_id=item.request_id,
                item_id=item.id,
                destination_id=item.request.destination_id,
            )
            if exc.mark_lost and plan.cache_entry is not None:
                _try_mark_entry_lost_for_cache_fallback(
                    session,
                    plan.cache_entry,
                    final_config,
                    content_sha256=item.content_sha256,
                    artifactclass=item.artifactclass,
                    request_id=item.request_id,
                    item_id=item.id,
                    destination_id=item.request.destination_id,
                    breaker_already_open=breaker_open,
                )
            _set_item_state(item, ITEM_FELL_BACK_TO_TAPE, "tape mount pending")
            item.source = "tape"
            item.bytes_restored = 0
            _update_request_state(item.request)
            session.flush([item.request, item])
            if _runtime is not None and _runtime.commit_state_transitions:
                session.commit()
            fell_back_to_tape = True

    try:
        if not fell_back_to_tape:
            _set_item_streaming(item, "tape")
            _update_request_state(item.request)
            session.flush([item.request, item])
            if _runtime is not None and _runtime.commit_state_transitions:
                session.commit()
        else:
            item.source = "tape"
            item.bytes_restored = 0
            item.updated_at = _utcnow()
            session.flush([item])
            if _runtime is not None and _runtime.commit_state_transitions:
                session.commit()
        with restore_progress_context(_progress_callback(session, item)):
            tape_result = _serve_from_tape(
                session,
                item.content_sha256,
                item.artifactclass,
                plan.destination,
                final_config,
                force_suspect=admission.force_suspect,
                force_rejected=admission.force_rejected,
            )
    except (RestoreSuspectAsset, RestoreRejectedAsset) as exc:
        _set_item_denied(
            item,
            _validity_denial_kind(exc),
            _validity_denial_detail(session, item.content_sha256, exc),
        )
        return ServeResult(item.id, "tape", plan.destination, 0)
    except ArchiveRestoreError as exc:
        _set_item_state(item, ITEM_FAILED, _sanitize_detail(str(exc)))
        return ServeResult(item.id, "tape", plan.destination, 0)
    except Exception as exc:
        _set_item_state(item, ITEM_FAILED, _sanitize_detail(str(exc)))
        return ServeResult(item.id, "tape", plan.destination, 0)
    _set_item_state(item, ITEM_DONE, None)
    _finish_item_progress(item, tape_result.size_bytes, source="tape")
    return ServeResult(
        item.id,
        "tape",
        tape_result.output_path,
        tape_result.size_bytes,
        copy_id=tape_result.copy_id,
    )


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
        stream_pool_size=final_config.stream_pool_size,
        aead_stream_cap=final_config.aead_stream_cap,
        wake_ahead=final_config.wake_ahead,
        wake_window_size=final_config.wake_window_size,
        read_deadline_seconds=final_config.read_deadline_seconds,
        liveness_probe_deadline_seconds=final_config.liveness_probe_deadline_seconds,
        breaker=final_config.breaker,
        worker_session_factory=final_config.worker_session_factory,
        hmac_secret=final_config.hmac_secret,
        identity_probe=final_config.identity_probe,
    )
    final_config = bind_restore_event_sink_to_session(final_config, session)
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
            cache_result = _serve_from_cache_controlled(
                session,
                plan.cache_entry,
                plan.destination,
                final_config,
                None,
            )
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
            breaker_open = _record_cache_failure(
                session,
                plan.cache_entry,
                exc,
                final_config,
                content_sha256=asset_hash,
                artifactclass=artifactclass,
            )
            if exc.mark_lost and plan.cache_entry is not None:
                _try_mark_entry_lost_for_cache_fallback(
                    session,
                    plan.cache_entry,
                    final_config,
                    content_sha256=asset_hash,
                    artifactclass=artifactclass,
                    breaker_already_open=breaker_open,
                )
    tape_result = _serve_from_tape(
        session,
        asset_hash,
        artifactclass,
        plan.destination,
        final_config,
        force_suspect=force_suspect,
        force_rejected=force_rejected,
    )
    return ServeResult(
        None,
        "tape",
        tape_result.output_path,
        tape_result.size_bytes,
        copy_id=tape_result.copy_id,
    )


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
        raise InvalidRestoreDestination(
            "restore destination escapes configured export root"
        ) from exc
    final = parent_real / candidate.relative_to(parent).as_posix()
    _reject_overwrite(final, overwrite=overwrite)
    return final


def validate_restore_relative_path(value: str) -> str:
    """Validate a client-supplied relative path without touching any server filesystem."""

    if not isinstance(value, str) or not value or "\x00" in value:
        raise InvalidRestoreDestination("restore destination must be a non-empty relative path")
    windows_path = PureWindowsPath(value)
    if Path(value).is_absolute() or windows_path.is_absolute() or windows_path.drive:
        raise InvalidRestoreDestination("restore destination must be relative")
    normalized = value.replace("\\", "/")
    parts = normalized.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise InvalidRestoreDestination("restore destination contains unsafe path traversal")
    if len(value) > 2048:
        raise InvalidRestoreDestination("restore destination is too long")
    return value


def authorize_agent_restore_destination(
    session: Session,
    *,
    receiver_device_id: str | None,
    destination_id: str,
) -> GrpcDeviceDestinationGrant:
    """Authorize an opaque device/destination binding without resolving a server path."""

    if not receiver_device_id:
        raise RestoreAdmissionInvalid("receiver_device_id is required for agent delivery")
    device = session.get(GrpcLogicalDevice, receiver_device_id)
    if device is None:
        raise RestoreAdmissionInvalid("receiver device is not enrolled")
    try:
        scopes = validate_device_scopes(device.scopes)
    except ValueError as exc:
        raise RestoreAdmissionInvalid("receiver device has invalid enrollment scopes") from exc
    if "restore" not in scopes:
        raise RestoreAdmissionInvalid("receiver device is not restore-scoped")
    grant = session.scalars(
        select(GrpcDeviceDestinationGrant).where(
            GrpcDeviceDestinationGrant.device_id == receiver_device_id,
            GrpcDeviceDestinationGrant.destination_id == destination_id,
        )
    ).one_or_none()
    if grant is None:
        raise RestoreAdmissionInvalid("receiver device has no grant for the destination")
    return grant


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
    asset = session.get(LogicalAsset, entry.content_sha256)
    if asset is None:
        raise CacheServeFailed(
            "read-failed", "cache entry logical asset is unavailable", mark_lost=False
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with (
        open_verified_cache_plaintext(
            session,
            entry,
            expected_content_sha256=entry.content_sha256,
            expected_size_bytes=asset.size_bytes,
            artifactclass=entry.artifactclass,
            config=config,
        ) as plaintext,
        _optional_semaphore_slot(_current_stream_slots()),
    ):
        _publish_cache_plaintext(plaintext.path, destination)

    entry.last_read_at = _utcnow()
    if not entry.trusted:
        entry.trusted = True
    session.flush([entry])
    return ServeResult(None, "cache", destination, plaintext.size_bytes)


@contextmanager
def open_cache_plaintext_chunks(
    session: Session,
    entry: CacheEntry,
    *,
    expected_content_sha256: bytes,
    expected_size_bytes: int,
    artifactclass: str,
    config: RestoreConfig | None = None,
) -> Iterator[Iterator[bytes]]:
    """Yield bounded, digest-verified plaintext chunks from one hdcache entry."""

    with open_verified_cache_plaintext(
        session,
        entry,
        expected_content_sha256=expected_content_sha256,
        expected_size_bytes=expected_size_bytes,
        artifactclass=artifactclass,
        config=config,
    ) as plaintext:
        yield plaintext.iter_chunks()


@contextmanager
def open_verified_cache_plaintext(
    session: Session,
    entry: CacheEntry,
    *,
    expected_content_sha256: bytes,
    expected_size_bytes: int,
    artifactclass: str,
    config: RestoreConfig | None = None,
) -> Iterator[VerifiedCachePlaintext]:
    """Open one cache entry through the shared stored/plaintext verification funnel.

    Raw and private entries both stage through the existing verified disk reader;
    private entries additionally use the configured RAO opener and hdcache key
    epoch validation. The yielded producer remains bounded and verifies the
    catalog plaintext digest and size again as it is consumed.
    """

    final_config = config or restore_config_from_env()
    if entry.content_sha256 != expected_content_sha256:
        raise CacheServeFailed(
            "read-failed", "cache entry does not match the requested asset", mark_lost=False
        )
    if expected_size_bytes < 0:
        raise CacheServeFailed(
            "read-failed", "cache plaintext size cannot be negative", mark_lost=False
        )
    disk = session.get(CacheDisk, entry.disk_id)
    if disk is None or disk.state != "active":
        raise CacheServeFailed("disk-inactive", "cache disk is not active", mark_lost=False)
    if final_config.breaker.is_open(disk.disk_id):
        raise CacheServeFailed(
            "disk-circuit-open", "cache disk circuit breaker is open", mark_lost=False
        )
    if final_config.read_deadline_seconds <= 0:
        raise CacheServeFailed(
            "read-deadline",
            "cache read deadline exceeded before stream start",
            mark_lost=False,
            count_breaker=True,
        )
    try:
        identity = _verify_cache_disk_identity(disk, final_config)
    except StoreReadTimeout as exc:
        raise CacheServeFailed(
            "disk-identity-timeout",
            str(exc),
            mark_lost=False,
            count_breaker=True,
        ) from exc
    if not identity.ok:
        _emit(
            final_config,
            RestoreEvent(
                code="disk-identity-unverified",
                severity="alarm",
                content_sha256=entry.content_sha256.hex(),
                artifactclass=entry.artifactclass,
                detail=_identity_failure_detail(disk, identity),
            ),
        )
        raise CacheServeFailed(
            "disk-identity-unverified",
            _identity_failure_detail(disk, identity),
            mark_lost=False,
        )
    if not entry_policy_conformant(session, entry, key_registry=final_config.registry()):
        raise CacheServeFailed(
            "representation-mismatch",
            f"cache entry representation does not satisfy current privacy policy for {artifactclass}",
            mark_lost=True,
        )

    final_config.scratch_root.mkdir(parents=True, exist_ok=True)
    os.chmod(final_config.scratch_root, 0o700)
    deadline = time.monotonic() + final_config.read_deadline_seconds
    try:
        with tempfile.TemporaryDirectory(
            prefix="hdcache-serve-", dir=final_config.scratch_root
        ) as raw:
            temp_dir = Path(raw)
            if entry.representation == RAW_REPRESENTATION:
                plaintext = temp_dir / "plain"
                with plaintext.open("wb") as output:
                    read_result = read_entry_verified(
                        Path(disk.mount),
                        entry.content_sha256,
                        representation=RAW_REPRESENTATION,
                        output=output,
                        deadline_monotonic=deadline,
                        disk_id=disk.disk_id,
                    )
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
                        deadline_monotonic=deadline,
                        disk_id=disk.disk_id,
                    )
                opener = final_config.opener or RaoCliOpener(
                    final_config.registry(), work_dir=final_config.scratch_root
                )
                recipient_epochs = _cache_recipient_epochs(entry)
                with (
                    _optional_unheld_aead_slot(),
                    opener.open(
                        sealed,
                        Representation.RAO_AEAD_V1,
                        recipient_epochs=recipient_epochs,
                        key_domain=KEY_DOMAIN_HDCACHE,
                        work_dir=final_config.scratch_root,
                    ) as plaintext,
                ):
                    digest = sha256_file(plaintext)
                    if digest != expected_content_sha256:
                        raise StoreContentMismatch(
                            "opened cache plaintext digest mismatch: "
                            f"{digest.hex()} != {expected_content_sha256.hex()}"
                        )
                    size_bytes = plaintext.stat().st_size
                    if size_bytes != expected_size_bytes:
                        raise StoreContentMismatch(
                            f"opened cache plaintext size mismatch: {size_bytes} != "
                            f"{expected_size_bytes}"
                        )
                    yield VerifiedCachePlaintext(
                        path=plaintext,
                        content_sha256=expected_content_sha256,
                        size_bytes=expected_size_bytes,
                    )
                    return
            else:
                raise StoreError(f"unsupported cache representation {entry.representation!r}")
            if size_bytes != expected_size_bytes:
                raise StoreContentMismatch(
                    f"cache plaintext size mismatch: {size_bytes} != {expected_size_bytes}"
                )
            yield VerifiedCachePlaintext(
                path=plaintext,
                content_sha256=expected_content_sha256,
                size_bytes=expected_size_bytes,
            )
    except StoreReadTimeout as exc:
        raise CacheServeFailed(
            "read-deadline",
            str(exc),
            mark_lost=False,
            count_breaker=True,
        ) from exc
    except StoreContentMismatch as exc:
        raise CacheServeFailed("read-failed", str(exc), mark_lost=True) from exc
    except StoreError as exc:
        raise CacheServeFailed(
            "read-failed",
            str(exc),
            mark_lost=False,
            count_breaker=True,
        ) from exc
    except (OSError, RuntimeError, ValueError, KeyError) as exc:
        raise CacheServeFailed(
            "read-failed",
            str(exc),
            mark_lost=False,
            count_breaker=True,
        ) from exc


def _serve_from_cache_controlled(
    session: Session,
    entry: CacheEntry,
    destination: Path,
    config: RestoreConfig,
    runtime: _ServeRuntime | None,
) -> ServeResult:
    stream_slots = None if runtime is None else runtime.stream_slots
    aead_slots = None if runtime is None else runtime.aead_slots
    if entry.representation == AEAD_REPRESENTATION and aead_slots is not None:
        with (
            _semaphore_slot(aead_slots),
            _serve_slot_context(stream_slots, aead_slots, aead_slot_held=True),
        ):
            return _serve_from_cache(session, entry, destination, config)
    with _serve_slot_context(stream_slots, aead_slots, aead_slot_held=False):
        return _serve_from_cache(session, entry, destination, config)


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


def _try_mark_entry_lost_for_cache_fallback(
    session: Session,
    entry: CacheEntry,
    config: RestoreConfig,
    *,
    content_sha256: bytes,
    artifactclass: str,
    request_id: str | None = None,
    item_id: int | None = None,
    destination_id: str | None = None,
    breaker_already_open: bool = False,
) -> None:
    disk = session.get(CacheDisk, entry.disk_id)
    if disk is None:
        return
    if breaker_already_open or config.breaker.is_open(disk.disk_id):
        _emit(
            config,
            RestoreEvent(
                code="cache-fallback:lost-mark-skipped-breaker",
                severity="warning",
                content_sha256=content_sha256.hex(),
                artifactclass=artifactclass,
                detail=f"cache disk {disk.disk_id} circuit breaker is open",
                request_id=request_id,
                item_id=item_id,
                destination_id=destination_id,
            ),
        )
        return
    try:
        identity = _verify_cache_disk_identity(disk, config)
    except StoreReadTimeout as exc:
        failure = CacheServeFailed(
            "disk-identity-timeout",
            str(exc),
            mark_lost=False,
            count_breaker=True,
        )
        _emit(
            config,
            RestoreEvent(
                code=f"cache-fallback:{failure.reason}",
                severity="alarm",
                content_sha256=content_sha256.hex(),
                artifactclass=artifactclass,
                detail=_sanitize_detail(failure.detail),
                request_id=request_id,
                item_id=item_id,
                destination_id=destination_id,
            ),
        )
        _record_cache_failure(
            session,
            entry,
            failure,
            config,
            content_sha256=content_sha256,
            artifactclass=artifactclass,
            request_id=request_id,
            item_id=item_id,
            destination_id=destination_id,
        )
        return
    if not identity.ok:
        _emit(
            config,
            RestoreEvent(
                code="disk-identity-unverified",
                severity="alarm",
                content_sha256=content_sha256.hex(),
                artifactclass=artifactclass,
                detail=f"{_identity_failure_detail(disk, identity)} before lost mark",
                request_id=request_id,
                item_id=item_id,
                destination_id=destination_id,
            ),
        )
        return
    try:
        mark_entry_lost_and_delete(
            session,
            entry,
            deadline_monotonic=time.monotonic() + config.read_deadline_seconds,
        )
    except StoreReadTimeout as exc:
        failure = CacheServeFailed(
            "delete-timeout",
            str(exc),
            mark_lost=False,
            count_breaker=True,
        )
        _emit(
            config,
            RestoreEvent(
                code=f"cache-fallback:{failure.reason}",
                severity="alarm",
                content_sha256=content_sha256.hex(),
                artifactclass=artifactclass,
                detail=_sanitize_detail(failure.detail),
                request_id=request_id,
                item_id=item_id,
                destination_id=destination_id,
            ),
        )
        _record_cache_failure(
            session,
            entry,
            failure,
            config,
            content_sha256=content_sha256,
            artifactclass=artifactclass,
            request_id=request_id,
            item_id=item_id,
            destination_id=destination_id,
        )
    except Exception as exc:
        _emit(
            config,
            RestoreEvent(
                code="cache-fallback:lost-mark-failed",
                severity="alarm",
                content_sha256=content_sha256.hex(),
                artifactclass=artifactclass,
                detail=_sanitize_detail(str(exc)),
                request_id=request_id,
                item_id=item_id,
                destination_id=destination_id,
            ),
        )


def _record_cache_failure(
    session: Session,
    entry: CacheEntry | None,
    exc: CacheServeFailed,
    config: RestoreConfig,
    *,
    content_sha256: bytes,
    artifactclass: str,
    request_id: str | None = None,
    item_id: int | None = None,
    destination_id: str | None = None,
) -> bool:
    if entry is None:
        return False
    disk = session.get(CacheDisk, entry.disk_id)
    if disk is None:
        return False
    if not exc.count_breaker:
        return config.breaker.is_open(disk.disk_id)
    tripped = config.breaker.record_failure(disk.disk_id)
    breaker_open = config.breaker.is_open(disk.disk_id)
    if tripped or (breaker_open and disk.state != "absent"):
        disk.state = "absent"
        session.flush([disk])
        _emit(
            config,
            RestoreEvent(
                code="disk-circuit-open",
                severity="alarm",
                content_sha256=content_sha256.hex(),
                artifactclass=artifactclass,
                detail=(
                    f"cache disk {disk.disk_id} exceeded cache failure threshold; state set absent"
                ),
                request_id=request_id,
                item_id=item_id,
                destination_id=destination_id,
            ),
        )
    return breaker_open


def _admission_inputs_for_item(item: RestoreRequestItem) -> RestoreAdmissionInputs:
    if item.request is None:
        raise RestoreAdmissionInvalid(
            f"restore request item id={item.id} is not attached to a request"
        )
    request = item.request
    if not isinstance(request.admitted_by, str) or not request.admitted_by.strip():
        raise RestoreAdmissionInvalid(
            f"restore request item id={item.id} is missing admission inputs"
        )
    if not isinstance(request.admitted_at, dt.datetime):
        raise RestoreAdmissionInvalid(
            f"restore request item id={item.id} is missing admission inputs"
        )
    capabilities = request.admitted_capabilities
    if not isinstance(capabilities, list) or not all(
        isinstance(capability, str) for capability in capabilities
    ):
        raise RestoreAdmissionInvalid(
            f"restore request item id={item.id} is missing admission inputs"
        )
    if not isinstance(item.admitted_force_suspect, bool) or not isinstance(
        item.admitted_force_rejected,
        bool,
    ):
        raise RestoreAdmissionInvalid(
            f"restore request item id={item.id} is missing admission inputs"
        )
    identity = Identity(
        operator_username=request.admitted_by.strip(),
        display_name=request.admitted_by.strip(),
        email=None,
        groups=(),
        role=None,
        capabilities=tuple(capabilities),
    )
    return RestoreAdmissionInputs(
        identity=identity,
        force_suspect=item.admitted_force_suspect,
        force_rejected=item.admitted_force_rejected,
    )


def _admitted_force_waivers(
    session: Session,
    asset_hash: bytes,
    *,
    force_suspect: bool,
    force_rejected: bool,
) -> tuple[bool, bool]:
    """Return the validity conditions actually waived at admission time."""

    asset = session.get(LogicalAsset, asset_hash)
    if asset is None:
        return False, False
    return (
        bool(force_suspect and asset.validity == AssetValidity.SUSPECT),
        bool(force_rejected and asset.rejected_at is not None),
    )


def _validity_denial_detail(
    session: Session,
    asset_hash: bytes,
    exc: RestoreSuspectAsset | RestoreRejectedAsset,
) -> str:
    asset = session.get(LogicalAsset, asset_hash)
    if isinstance(exc, RestoreSuspectAsset):
        detail = f"asset {asset_hash.hex()} is flagged suspect"
        note = None if asset is None else asset.validity_note
    else:
        detail = f"asset {asset_hash.hex()} is rejected"
        note = None if asset is None else asset.rejection_reason
    if note:
        detail = f"{detail}: {note}"
    return _sanitize_detail(detail)


def _validity_denial_kind(exc: RestoreSuspectAsset | RestoreRejectedAsset) -> DenialKind:
    if isinstance(exc, RestoreSuspectAsset):
        return "suspect"
    return "rejected"


def _asset_size_bytes(session: Session, asset_hash: bytes) -> int | None:
    asset = session.get(LogicalAsset, asset_hash)
    return None if asset is None else asset.size_bytes


def _ensure_item_size_bytes(session: Session, item: RestoreRequestItem) -> None:
    if item.size_bytes is None:
        item.size_bytes = _asset_size_bytes(session, item.content_sha256)


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


def _select_cache_entry(
    session: Session,
    asset_hash: bytes,
    *,
    config: RestoreConfig | None = None,
    allow_recovery_probe: bool = True,
) -> CacheEntry | None:
    entry = session.get(CacheEntry, asset_hash)
    if entry is None or entry.state != "present":
        return None
    disk = session.get(CacheDisk, entry.disk_id)
    if disk is None:
        return None
    if (
        disk.state != "active" or (config is not None and config.breaker.is_open(disk.disk_id))
    ) and (
        config is None
        or not allow_recovery_probe
        or not _probe_cache_disk_recovery(session, disk, config)
    ):
        return None
    if disk.state != "active":
        return None
    return entry


def _probe_cache_disk_recovery(session: Session, disk: CacheDisk, config: RestoreConfig) -> bool:
    if disk.state in {"retiring", "dead"}:
        return False
    if disk.state != "absent" and not config.breaker.is_open(disk.disk_id):
        return False
    deadline = time.monotonic() + config.liveness_probe_deadline_seconds
    try:
        result = probe_disk_liveness_with_deadline(
            Path(disk.mount),
            ExpectedDiskIdentity(
                disk_id=disk.disk_id,
                serial=disk.serial,
                fs_uuid=disk.fs_uuid,
                wwn=disk.wwn,
            ),
            hmac_secret=config.disk_hmac_secret(),
            disk_id=disk.disk_id,
            probe=config.identity_probe,
            deadline_monotonic=deadline,
        )
    except StoreReadTimeout as exc:
        _emit(
            config,
            RestoreEvent(
                code="disk-recovery-probe-timeout",
                severity="alarm",
                detail=f"cache disk {disk.disk_id} recovery probe timed out: {exc}",
            ),
        )
        return False
    except Exception as exc:
        _emit(
            config,
            RestoreEvent(
                code="disk-recovery-probe-failed",
                severity="alarm",
                detail=f"cache disk {disk.disk_id} recovery probe failed: {_sanitize_detail(str(exc))}",
            ),
        )
        return False
    if not result.ok:
        _emit(
            config,
            RestoreEvent(
                code="disk-recovery-probe-failed",
                severity="alarm",
                detail=_identity_failure_detail(disk, result),
            ),
        )
        return False
    disk.state = "active"
    config.breaker.reset(disk.disk_id)
    session.flush([disk])
    _emit(
        config,
        RestoreEvent(
            code="disk-circuit-closed",
            severity="info",
            detail=f"cache disk {disk.disk_id} liveness probe succeeded",
        ),
    )
    return True


def _wake_items(
    session: Session,
    request: RestoreRequest,
    items: list[RestoreRequestItem],
    *,
    config: RestoreConfig,
) -> None:
    if not config.wake_ahead:
        return
    for item in items:
        if item.state != ITEM_QUEUED:
            continue
        entry = _select_cache_entry(
            session,
            item.content_sha256,
            config=config,
            allow_recovery_probe=False,
        )
        if entry is None:
            continue
        disk = session.get(CacheDisk, entry.disk_id)
        if disk is None or config.breaker.is_open(disk.disk_id):
            continue
        _set_item_state(item, ITEM_WAKING_DISK, None)
    _update_request_state(request)
    session.flush([request, *items])


def _verify_cache_disk_identity(disk: CacheDisk, config: RestoreConfig) -> DiskIdentityResult:
    try:
        return verify_disk_identity_with_deadline(
            Path(disk.mount),
            ExpectedDiskIdentity(
                disk_id=disk.disk_id,
                serial=disk.serial,
                fs_uuid=disk.fs_uuid,
                wwn=disk.wwn,
            ),
            hmac_secret=config.disk_hmac_secret(),
            disk_id=disk.disk_id,
            probe=config.identity_probe,
            deadline_monotonic=time.monotonic() + config.liveness_probe_deadline_seconds,
        )
    except StoreReadTimeout:
        raise
    except Exception as exc:
        return DiskIdentityResult(
            False,
            "identity_unavailable",
            f"disk identity verifier unavailable: {exc}",
        )


def _identity_failure_detail(disk: CacheDisk, result: DiskIdentityResult) -> str:
    return f"cache disk {disk.disk_id} identity unverified ({result.status}: {result.detail})"


@contextmanager
def _serve_slot_context(
    stream_slots: threading.BoundedSemaphore | None,
    aead_slots: threading.BoundedSemaphore | None,
    *,
    aead_slot_held: bool,
) -> Iterator[None]:
    previous = getattr(_SERVE_SLOT_CONTEXT, "value", None)
    _SERVE_SLOT_CONTEXT.value = _ServeSlotContext(
        stream_slots=stream_slots,
        aead_slots=aead_slots,
        aead_slot_held=aead_slot_held,
    )
    try:
        yield
    finally:
        if previous is None:
            with contextlib.suppress(AttributeError):
                delattr(_SERVE_SLOT_CONTEXT, "value")
        else:
            _SERVE_SLOT_CONTEXT.value = previous


def _current_stream_slots() -> threading.BoundedSemaphore | None:
    context = getattr(_SERVE_SLOT_CONTEXT, "value", None)
    return None if context is None else context.stream_slots


def _current_aead_slots() -> threading.BoundedSemaphore | None:
    context = getattr(_SERVE_SLOT_CONTEXT, "value", None)
    return None if context is None else context.aead_slots


def _current_aead_slot_held() -> bool:
    context = getattr(_SERVE_SLOT_CONTEXT, "value", None)
    return bool(context is not None and context.aead_slot_held)


@contextmanager
def _semaphore_slot(semaphore: threading.BoundedSemaphore) -> Iterator[None]:
    semaphore.acquire()
    try:
        yield
    finally:
        semaphore.release()


@contextmanager
def _optional_semaphore_slot(
    semaphore: threading.BoundedSemaphore | None,
) -> Iterator[None]:
    if semaphore is None:
        yield
        return
    with _semaphore_slot(semaphore):
        yield


@contextmanager
def _optional_unheld_aead_slot() -> Iterator[None]:
    if _current_aead_slot_held():
        yield
        return
    with _optional_semaphore_slot(_current_aead_slots()):
        yield


def _publish_cache_plaintext(source: Path, destination: Path) -> None:
    atomic_write_verified_file(source, destination)


def _cache_recipient_epochs(entry: CacheEntry) -> tuple[str, ...]:
    if entry.key_epoch is None:
        raise StoreError("AEAD cache entry has no key_epoch")
    assert_key_epoch_domain(entry.key_epoch, KEY_DOMAIN_HDCACHE, context="hdcache serve")
    return (entry.key_epoch,)


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
        _reject_path_like_destination_id(dest_id)
        if isinstance(value, str):
            root = Path(value)
            label = dest_id
            writable = True
        elif isinstance(value, dict):
            root_raw = value.get("root")
            if not isinstance(root_raw, str) or not root_raw:
                raise ArtifactClassPolicyError(f"restore destination {dest_id!r} needs root")
            root = Path(root_raw)
            label_raw = value.get("label")
            if label_raw is None:
                label = dest_id
            elif not isinstance(label_raw, str) or not label_raw.strip():
                raise ArtifactClassPolicyError(
                    f"restore destination {dest_id!r} label must be a string"
                )
            elif _looks_like_raw_path_label(label_raw.strip(), root_raw):
                raise ArtifactClassPolicyError(
                    f"restore destination {dest_id!r} label must not be a raw path"
                )
            else:
                label = label_raw.strip()
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


def _reject_path_like_destination_id(dest_id: str) -> None:
    try:
        has_drive = bool(PureWindowsPath(dest_id).drive)
    except ValueError:
        has_drive = True
    if "/" in dest_id or "\\" in dest_id or has_drive:
        raise ArtifactClassPolicyError("restore destination ids must be opaque strings")


def _looks_like_raw_path_label(label: str, root_raw: str) -> bool:
    if label == root_raw or label.startswith("~") or "/" in label or "\\" in label:
        return True
    try:
        if Path(label).expanduser().is_absolute():
            return True
    except (OSError, ValueError):
        return True
    try:
        return bool(PureWindowsPath(label).drive)
    except ValueError:
        return True


def _destination_by_id(config: RestoreConfig, destination_id: str) -> RestoreDestination:
    destination = config.destinations.get(destination_id)
    if destination is None:
        raise UnknownRestoreDestination(f"unknown restore destination_id {destination_id!r}")
    if not destination.writable:
        raise InvalidRestoreDestination(
            f"restore destination_id {destination_id!r} is not writable"
        )
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
    if any(
        state in {ITEM_WAKING_DISK, ITEM_STREAMING, ITEM_SENT, ITEM_FELL_BACK_TO_TAPE}
        for state in states
    ):
        request.state = REQUEST_ACTIVE
        return
    if ITEM_QUEUED in states:
        request.state = REQUEST_PENDING
        return
    if all(state == ITEM_DONE for state in states):
        request.state = REQUEST_COMPLETED
        return
    request.state = REQUEST_COMPLETED_WITH_ERRORS


def _set_item_state(item: RestoreRequestItem, state: str, detail: str | None) -> None:
    item.state = state
    item.detail = detail
    if state != ITEM_DENIED:
        item.denial_kind = None
    item.updated_at = _utcnow()
    if item.request is not None:
        _update_request_state(item.request)


def _set_item_denied(
    item: RestoreRequestItem,
    denial_kind: DenialKind,
    detail: str | None,
) -> None:
    item.state = ITEM_DENIED
    item.detail = detail
    item.denial_kind = denial_kind
    item.source = None
    item.bytes_restored = 0
    item.updated_at = _utcnow()
    if item.request is not None:
        _update_request_state(item.request)


def _set_item_streaming(item: RestoreRequestItem, source: SourceKind) -> None:
    _set_item_state(item, ITEM_STREAMING, None)
    item.source = source
    item.bytes_restored = 0


def _finish_item_progress(
    item: RestoreRequestItem,
    size_bytes: int,
    *,
    source: SourceKind,
) -> None:
    item.source = source
    item.size_bytes = size_bytes if item.size_bytes is None else item.size_bytes
    item.bytes_restored = size_bytes


def _progress_callback(
    session: Session,
    item: RestoreRequestItem,
) -> Callable[[int], None]:
    def progress(delta: int) -> None:
        if delta <= 0:
            return
        item.bytes_restored = max(0, item.bytes_restored or 0) + delta
        item.updated_at = _utcnow()
        session.flush([item])
        session.commit()

    return progress


def _emit(config: RestoreConfig, event: RestoreEvent) -> None:
    if config.event_sink is not None:
        config.event_sink(event)
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


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _env_optional_int(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return None
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.lower() not in {"0", "false", "no", "off"}
