"""Authorized restore-agent streaming over the catalog-backed RestorePlan.

Each OpenRestore owns its backend instances and mutable ``RestorePlan``. SQL
transactions are kept outside the data loop: admission writes the attempt row
(design RSR, D2), one transaction validates and leases the open, and one end
transaction (``_end_attempt``, D6) records what happened, releases the lease
or records ``sent``, and applies the retry policy's give-up. The agent path
never resolves or writes an archive-server destination path.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import logging
import os
import socket
import struct
import threading
import time
from collections.abc import Callable, Generator, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, NoReturn

import grpc
from sqlalchemy import Engine, and_, case, exists, func, insert, literal, null, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from sutradhara._proto import restore_pb2, restore_pb2_grpc
from sutradhara.api.live_capabilities import LiveCapabilityResolver
from sutradhara.archive_restore import (
    ArchiveExtractor,
    ArchiveRestoreError,
    LogicalMemberIntegrityError,
    PlannedMember,
    RemArchiveExtractor,
    RestoreIntegrityError,
    RestorePlan,
    RestoreSourceUnavailable,
    build_restore_plan,
)
from sutradhara.artifactclass_policy import hdcache_privacy_capability_map_from_env
from sutradhara.backend.port import BackendError, StorageBackend
from sutradhara.catalog.session import make_session_factory
from sutradhara.catalog.types import BackendKind, CopyHealth
from sutradhara.grpc import ca as grpc_ca
from sutradhara.grpc import store as grpc_store
from sutradhara.hdcache.fill import effective_privacy_level
from sutradhara.hdcache.manager import (
    ITEM_DONE,
    ITEM_FAILED,
    ITEM_QUEUED,
    ITEM_SENT,
    ITEM_STREAMING,
    CacheServeFailed,
    InvalidRestoreDestination,
    RestoreConfig,
    _select_cache_entry,
    _update_request_state,
    authorize_agent_restore_destination,
    bind_restore_event_sink_to_engine,
    bind_restore_event_sink_to_session,
    open_cache_plaintext_chunks,
    record_cache_serve_failure,
    rederive_request_state,
    restore_backends_for_artifactclass,
    restore_config_from_env,
    validate_restore_relative_path,
)
from sutradhara.hdcache.models import (
    CacheEntry,
    RestoreItemAttempt,
    RestoreItemCheckpoint,
    RestoreItemGiveup,
    RestoreOpenSession,
    RestoreRequestItem,
    RestoreServerInstance,
)
from sutradhara.hdcache.restore_retry import (
    OFFER_AT,
    OFFER_GIVE_UP,
    OFFER_OPEN,
    RetryPolicy,
    attempt_summaries,
    bound_message,
    classify,
    compose_detail,
    plan_next_offer,
    retry_policy_from_env,
)
from sutradhara.jobs.attempts import default_code_version
from sutradhara.restore import RestoreIntegrityError as ChunkRestoreIntegrityError
from sutradhara.staging import StagingError

_MANIFEST_DOMAIN = b"sutradhara.restore.manifest.v1\x00"
# Maximum plaintext bytes per `chunk` frame on the wire. Must stay <= the RM2 client's
# MAX_CHUNK_BYTES (256 KiB) — the agent rejects any larger chunk frame. Source producers
# differ (archive yields 256 KiB, the hdcache producer yields 1 MiB), so `_stream`
# re-chunks every source chunk to this bound regardless of the producer's buffer size.
_WIRE_CHUNK_BYTES = 256 * 1024
_SYNTHETIC_MODE = 0o644
_SYNTHETIC_UID = 0
_SYNTHETIC_GID = 0
_SYNTHETIC_MTIME = 0
_NOT_STARTED = "restore service is not started"
# Design RSR, D5's re-check at admission (B2): refusals before any row is written.
_OPEN_IN_PROGRESS = "another open of this item is in progress"
_WAITING_BEFORE_RETRY = "restore item is waiting before its next try"
_WILL_NOT_RETRY = "restore item will not be retried"
_CLOSED_BY_RECOVERY = "restore attempt was closed by recovery"
_DELIVERY_NOT_RECORDED = "the delivery was not recorded"
END_ENDED = "ended"
END_ALREADY_CLOSED = "already_closed"

LOG = logging.getLogger(__name__)

BackendResolver = Callable[[Session, str], dict[int, StorageBackend]]
ExtractorFactory = Callable[[], ArchiveExtractor]


@dataclass(frozen=True)
class RestoreServiceConfig:
    """Runtime dependencies and bounded-worker policy for RestoreService."""

    engine: Engine
    live_capabilities: LiveCapabilityResolver | None = None
    backend_resolver: BackendResolver = restore_backends_for_artifactclass
    extractor_factory: ExtractorFactory = RemArchiveExtractor
    cache_config: RestoreConfig | None = None
    max_concurrent_streams: int = 4
    # Mid-stream lease renewal lands in RM1.3; this default only raises the ceiling.
    lease_duration: dt.timedelta = dt.timedelta(minutes=30)
    assignment_poll_seconds: float = 0.5
    # Design RSR, D7: the instance heartbeat and the staleness rule recovery applies.
    heartbeat_seconds: float = 10.0
    staleness_threshold: dt.timedelta = dt.timedelta(minutes=2)
    # None: ``retry_policy_from_env()`` when the service is constructed.
    retry_policy: RetryPolicy | None = None

    def __post_init__(self) -> None:
        if not 0 < self.max_concurrent_streams < 16:
            raise ValueError("restore concurrency must be positive and below the shared 16 workers")
        if self.lease_duration <= dt.timedelta(0):
            raise ValueError("restore lease duration must be positive")
        if self.assignment_poll_seconds <= 0:
            raise ValueError("assignment poll interval must be positive")
        if self.heartbeat_seconds <= 0:
            raise ValueError("heartbeat interval must be positive")
        if self.staleness_threshold <= dt.timedelta(0):
            raise ValueError("staleness threshold must be positive")
        # D-RSRP-11: a gap longer than half the threshold is a break, so a
        # heartbeat that slow would never build an unbroken run.
        if self.heartbeat_seconds >= self.staleness_threshold.total_seconds() / 2:
            raise ValueError("heartbeat interval must be below half the staleness threshold")


@dataclass(frozen=True)
class _ManifestFile:
    """Frozen single-file v1 manifest entry."""

    index: int
    final_rel_path: str
    size: int
    content_sha256: bytes
    mode: int = _SYNTHETIC_MODE
    uid: int = _SYNTHETIC_UID
    gid: int = _SYNTHETIC_GID
    mtime: int = _SYNTHETIC_MTIME


@dataclass
class _Outcome:
    """The per-call outcome cell (design RSR, D3).

    Set at the site that raises or catches, before any ``_abort`` or ``yield``;
    the end transaction writes it. Nothing classifies by exception type:
    production ``abort`` raises a bare ``Exception()`` and the test fake raises
    its own class (P16). ``fenced`` marks the one end with no write: recovery
    closed the attempt before the lease was granted.
    """

    end_kind: str | None = None
    cause: str | None = None
    message: str | None = None
    blocked_until: dt.datetime | None = None
    bytes_sent: int = 0
    slot_taken: bool = False
    fenced: bool = False
    # The probe's evidence (D2): the source the open streams from and the
    # sources read. A failed cache probe sets no end: the open goes on.
    probe_source: str | None = None
    probe_candidates: int = 0

    @property
    def decided(self) -> bool:
        return self.end_kind is not None or self.fenced

    def set(
        self,
        end_kind: str,
        cause: str | None,
        message: str | None = None,
        *,
        blocked_until: dt.datetime | None = None,
    ) -> None:
        self.end_kind = end_kind
        self.cause = cause
        self.message = message
        self.blocked_until = blocked_until


@dataclass
class _AttemptEntry:
    """What the process remembers of one admitted attempt (``RestoreService._registry``).

    Removed when its end is written or found already closed; kept, marked
    ``ended`` with the cell's outcome and class, when the end write raised, so
    recovery (design RSR, D7) can retry the end with that outcome.
    """

    attempt_id: int
    item_id: int
    request_id: str
    opened_from_state: str
    outcome: _Outcome
    lease: _Lease | None = None
    source: str | None = None
    cache_entry_hash: bytes | None = None
    ended: bool = False
    klass: str | None = None


@dataclass(frozen=True)
class _Admission:
    """An admitted open: its registry entry and whether it is the revealed replay."""

    entry: _AttemptEntry
    revealed: bool


@dataclass(frozen=True)
class _EndResult:
    """``_end_attempt``'s answer: ``ended`` or ``already_closed``, and whether ``sent`` was recorded."""

    status: str
    sent: bool = False


class _AlreadyClosed(Exception):
    """The end update matched no row: recovery or the fence closed the attempt first."""


@dataclass(frozen=True)
class _PreparedOpen:
    """Detached stream inputs built during the short authorization transaction."""

    item_id: int
    device_id: str
    artifactclass: str
    manifest: _ManifestFile
    manifest_sha256: bytes
    plan: RestorePlan | None
    member: PlannedMember | None
    cache_entry_hash: bytes | None
    source: str
    # The planned index: ``min(claim, stored)`` for an unrevealed checkpoint (D10).
    committed_index: int
    # The receiver's claim: the resume token's ``committed_index``, or 0 (D10).
    claim: int
    revealed: bool
    expected_generation: int | None
    # The call's outcome cell (mutable; this record stays frozen), so ``_stream``
    # keeps its signature for the subclasses that wrap it.
    outcome: _Outcome


@dataclass(frozen=True)
class _Lease:
    """Acquired SQL-CAS lease generation."""

    item_id: int
    device_id: str
    manifest_sha256: bytes
    generation: int
    expires_at: dt.datetime


class RestoreService(restore_pb2_grpc.RestoreServiceServicer):
    """mTLS-bound restore stream and assignment service."""

    def __init__(self, config: RestoreServiceConfig) -> None:
        self.config = config
        self._factory = make_session_factory(config.engine)
        self._live = config.live_capabilities or LiveCapabilityResolver.from_database(config.engine)
        self._cache = bind_restore_event_sink_to_engine(
            config.cache_config or restore_config_from_env(), config.engine
        )
        self._slots = threading.BoundedSemaphore(config.max_concurrent_streams)
        self._retry_policy = config.retry_policy or retry_policy_from_env()
        # The attempts this process admitted (design RSR, D2/D7): attempt id ->
        # entry. The lock covers the admission insert and the registration;
        # an end never takes it while it holds the database write lock.
        self._registry_lock = threading.Lock()
        self._registry: dict[int, _AttemptEntry] = {}
        # Lifecycle (design RSR, D7): ``start()`` writes the instance row and
        # runs the heartbeat; nothing is served before it.
        self._lifecycle_lock = threading.Lock()
        self._started = False
        self._instance_id: int | None = None
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        # Monotonic bookkeeping of heartbeat successes: when the current
        # unbroken run began, and the last success.
        self._heartbeat_run_since: float | None = None
        self._heartbeat_last_success: float | None = None

    # ---- lifecycle ---------------------------------------------------------------

    @property
    def instance_id(self) -> int | None:
        """This process's ``restore_server_instance`` row, once started."""

        return self._instance_id

    def start(self, *, background: bool = True) -> None:
        """Write the instance row; with ``background``, run the heartbeat thread.

        A second ``start()`` raises ``RuntimeError``. Tests start without the
        thread and drive ``heartbeat_once()`` themselves.
        """

        with self._lifecycle_lock:
            if self._started:
                raise RuntimeError("restore service is already started")
            with self._factory.begin() as session:
                row = RestoreServerInstance(
                    host=socket.gethostname(),
                    pid=os.getpid(),
                    code_version=default_code_version(),
                    started_at=func.now(),
                    heartbeat_at=func.now(),
                )
                session.add(row)
                session.flush()
                instance_id = row.id
            self._instance_id = instance_id
            self._started = True
            # The row's own heartbeat_at is the first success of the run.
            self._record_heartbeat_success(time.monotonic())
            if background:
                self._heartbeat_stop.clear()
                self._heartbeat_thread = threading.Thread(
                    target=self._heartbeat_loop, name="sutra-restore-heartbeat", daemon=True
                )
                self._heartbeat_thread.start()

    def stop(self) -> None:
        """End and join the heartbeat thread. Idempotent."""

        self._heartbeat_stop.set()
        thread = self._heartbeat_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        self._heartbeat_thread = None

    def heartbeat_once(self) -> None:
        """Set this instance's ``heartbeat_at`` to the database clock (one step)."""

        if self._instance_id is None:
            raise RuntimeError(_NOT_STARTED)
        with self._factory.begin() as session:
            result = session.connection().execute(
                update(RestoreServerInstance)
                .where(RestoreServerInstance.id == self._instance_id)
                .values(heartbeat_at=func.now())
            )
            if result.rowcount != 1:
                raise RuntimeError(f"restore server instance {self._instance_id} row is missing")
        self._record_heartbeat_success(time.monotonic())

    def heartbeat_unbroken_seconds(self) -> float:
        """How long heartbeats have succeeded without a break, on the monotonic clock.

        A gap between successes longer than half ``staleness_threshold`` is a
        break; while the current gap already exceeds that, the answer is 0.
        """

        last = self._heartbeat_last_success
        since = self._heartbeat_run_since
        if last is None or since is None:
            return 0.0
        now = time.monotonic()
        if now - last > self._break_seconds():
            return 0.0
        return now - since

    def _break_seconds(self) -> float:
        return self.config.staleness_threshold.total_seconds() / 2

    def _record_heartbeat_success(self, at: float) -> None:
        last = self._heartbeat_last_success
        if last is None or at - last > self._break_seconds():
            self._heartbeat_run_since = at
        self._heartbeat_last_success = at

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(self.config.heartbeat_seconds):
            try:
                self.heartbeat_once()
            except Exception:
                LOG.exception("restore service heartbeat failed (instance %s)", self._instance_id)

    def _require_started(self, context: Any) -> None:
        if not self._started:
            _abort(context, grpc.StatusCode.UNAVAILABLE, _NOT_STARTED)

    # ---- RPCs ----------------------------------------------------------------------

    def OpenRestore(self, request: Any, context: Any) -> Iterator[Any]:
        """Authorize, exclusively lease, and frame one archive-backed restore item."""

        self._require_started(context)
        cell = _Outcome()
        entry: _AttemptEntry | None = None
        prepared: _PreparedOpen | None = None
        ended_in_body = False
        try:
            identity = self._identity(context)
            admission = self._admit(request, identity, context, cell)
            entry = admission.entry
            if not admission.revealed:
                # Capacity is checked after admission so the refusal leaves a
                # row (design RSR, D2); the revealed replay takes no slot.
                if not self._slots.acquire(blocking=False):
                    _refuse(
                        context,
                        cell,
                        grpc.StatusCode.RESOURCE_EXHAUSTED,
                        "restore stream capacity is full",
                        cause="capacity",
                    )
                cell.slot_taken = True
            prepared = self._prepare_open(request, identity, context, entry)
            entry.source = prepared.source
            entry.cache_entry_hash = prepared.cache_entry_hash
            if prepared.revealed:
                yield restore_pb2.RestoreFrame(
                    job_end=restore_pb2.JobEnd(
                        files=0,
                        bytes=0,
                        manifest_sha256=prepared.manifest_sha256,
                    )
                )
                # Set only once the generator resumed past its last yield (D3).
                cell.set("job_end", None)
                return
            entry.lease = self._acquire_lease(prepared, context, entry)
            try:
                emitted = yield from self._stream(prepared, entry.lease, context)
            except (BackendError, StagingError, OSError) as exc:
                cell.set("error", "source_unavailable", str(exc))
                raise
            except ChunkRestoreIntegrityError as exc:
                cell.set("error", "source_integrity", str(exc))
                raise
            if emitted is not None:
                # The generator resumed past job_end: the delivery is ended in
                # the body, never inside a GeneratorExit close (B4). Step 1 of
                # the end transaction decides its cause.
                cell.set("job_end", None)
                ended_in_body = True
                sent = False
                try:
                    sent = self._end_attempt(entry, deliver=True).sent
                except Exception:
                    LOG.exception(
                        "restore attempt %s: the end write failed after job_end; "
                        "recovery retries it",
                        entry.attempt_id,
                    )
                if not sent:
                    _abort(context, grpc.StatusCode.ABORTED, _DELIVERY_NOT_RECORDED)
        except BaseException as exc:
            if not cell.decided:
                # D3 / D-RSRP-7: a closed generator always means the call ended;
                # otherwise the client's liveness decides.
                if isinstance(exc, GeneratorExit) or not context.is_active():
                    cell.set("cancelled", None)
                else:
                    cell.set("error", "server_error", str(exc) or type(exc).__name__)
            raise
        finally:
            self._finish_open(entry, prepared, cell, ended_in_body=ended_in_body)

    def _finish_open(
        self,
        entry: _AttemptEntry | None,
        prepared: _PreparedOpen | None,
        cell: _Outcome,
        *,
        ended_in_body: bool,
    ) -> None:
        """B4: three guarded steps; one failure never skips the others."""

        if entry is not None and not ended_in_body:
            if cell.fenced:
                self._forget(entry)
            else:
                if not cell.decided:
                    cell.set("error", "server_error", "the call ended without an outcome")
                try:
                    self._end_attempt(entry, deliver=False)
                except Exception:
                    LOG.exception(
                        "restore attempt %s: the end write failed; recovery retries it",
                        entry.attempt_id,
                    )
        if prepared is not None and prepared.plan is not None:
            try:
                prepared.plan.close()
            except Exception:
                LOG.exception("restore plan close failed for item %s", prepared.item_id)
        if cell.slot_taken:
            try:
                self._slots.release()
            except Exception:
                LOG.exception("restore stream slot release failed")

    def CommitRestore(self, request: Any, context: Any) -> Any:
        """Durably advance staged progress or terminally reveal one restore item."""

        identity = self._identity(context)
        self._require_restore_device(identity, context)
        if request.restore_request_item_id <= 0:
            _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "restore item id must be positive")
        if len(request.manifest_sha256) != hashlib.sha256().digest_size:
            _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "manifest digest is invalid")
        if not request.HasField("lease_token"):
            _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "lease token is required")
        token = request.lease_token
        if (
            token.restore_request_item_id != request.restore_request_item_id
            or token.receiver_device_id != identity.device_id
            or token.manifest_sha256 != request.manifest_sha256
            or token.generation <= 0
        ):
            _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "lease token does not match")
        if request.committed_index > 1:
            _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "committed index exceeds manifest")
        if request.durable_state not in {
            restore_pb2.DURABLE_STATE_STAGED,
            restore_pb2.DURABLE_STATE_REVEALED,
        }:
            _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "durable state is invalid")
        if (
            request.durable_state == restore_pb2.DURABLE_STATE_REVEALED
            and request.committed_index != 1
        ):
            _abort(
                context, grpc.StatusCode.FAILED_PRECONDITION, "revealed commit must cover manifest"
            )

        now = dt.datetime.now(dt.UTC)
        with self._factory.begin() as session:
            preconditions = (
                RestoreItemCheckpoint.restore_request_item_id == request.restore_request_item_id,
                RestoreItemCheckpoint.manifest_sha256 == request.manifest_sha256,
                RestoreItemCheckpoint.item.has(
                    RestoreRequestItem.request.has(receiver_device_id=identity.device_id)
                ),
                RestoreItemCheckpoint.item.has(
                    RestoreRequestItem.open_session.has(
                        and_(
                            RestoreOpenSession.receiver_device_id == identity.device_id,
                            RestoreOpenSession.manifest_sha256 == request.manifest_sha256,
                            RestoreOpenSession.generation == token.generation,
                        )
                    )
                ),
            )
            live_lease = RestoreItemCheckpoint.item.has(
                RestoreRequestItem.open_session.has(RestoreOpenSession.expires_at > now)
            )
            if request.durable_state == restore_pb2.DURABLE_STATE_STAGED:
                result = session.connection().execute(
                    update(RestoreItemCheckpoint)
                    .where(
                        *preconditions,
                        live_lease,
                        RestoreItemCheckpoint.revealed.is_(False),
                        RestoreItemCheckpoint.committed_index < request.committed_index,
                    )
                    .values(committed_index=request.committed_index, updated_at=now)
                )
            else:
                result = session.connection().execute(
                    update(RestoreItemCheckpoint)
                    .where(
                        *preconditions,
                        live_lease,
                        RestoreItemCheckpoint.revealed.is_(False),
                    )
                    .values(
                        committed_index=request.committed_index,
                        revealed=True,
                        updated_at=now,
                    )
                )

            checkpoint = session.scalar(
                select(RestoreItemCheckpoint)
                .options(
                    selectinload(RestoreItemCheckpoint.item).selectinload(
                        RestoreRequestItem.request
                    ),
                    selectinload(RestoreItemCheckpoint.item).selectinload(
                        RestoreRequestItem.open_session
                    ),
                )
                .where(*preconditions)
            )
            if checkpoint is None:
                _abort(
                    context,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "manifest, receiver, or lease generation does not match",
                )
            open_session = checkpoint.item.open_session
            lease_is_live = open_session is not None and _as_utc(open_session.expires_at) > now
            if not lease_is_live and not checkpoint.revealed:
                _abort(
                    context,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "restore lease has expired",
                )
            status = "advanced" if result.rowcount == 1 else "unchanged"
            if request.durable_state == restore_pb2.DURABLE_STATE_REVEALED:
                if result.rowcount == 1:
                    item_result = session.connection().execute(
                        update(RestoreRequestItem)
                        .where(
                            RestoreRequestItem.id == request.restore_request_item_id,
                            RestoreRequestItem.state == ITEM_SENT,
                        )
                        .values(state=ITEM_DONE, detail=None, updated_at=now)
                    )
                    if item_result.rowcount != 1:
                        _abort(
                            context,
                            grpc.StatusCode.FAILED_PRECONDITION,
                            "restore item has not reached sent",
                        )
                    session.expire(checkpoint.item)
                    session.refresh(checkpoint.item)
                    if checkpoint.item.request is not None:
                        _update_request_state(checkpoint.item.request)
                    status = "revealed"
                elif checkpoint.revealed and checkpoint.item.state == ITEM_DONE:
                    status = "already_done"
                else:
                    _abort(
                        context,
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "restore reveal could not be committed",
                    )
            elif checkpoint.revealed:
                status = "already_done"

            return restore_pb2.CommitRestoreReply(
                restore_request_item_id=request.restore_request_item_id,
                status=status,
                committed_index=checkpoint.committed_index,
                revealed=checkpoint.revealed,
            )

    def WatchAssignments(self, request: Any, context: Any) -> Iterator[Any]:
        """Emit device-scoped metadata; OpenRestore separately gates object bytes."""

        self._require_started(context)
        identity = self._identity(context)
        if request.device_id != identity.device_id:
            _abort(
                context, grpc.StatusCode.PERMISSION_DENIED, "watch device_id is not the mTLS peer"
            )
        self._require_restore_device(identity, context)
        seen: set[int] = set()
        cancelled = threading.Event()
        context.add_callback(cancelled.set)
        while context.is_active():
            for assignment in self._assignments(identity.device_id):
                if assignment.restore_request_item_id in seen:
                    continue
                seen.add(assignment.restore_request_item_id)
                yield assignment
            cancelled.wait(self.config.assignment_poll_seconds)

    def _identity(self, context: Any) -> grpc_store.DeviceIdentity:
        try:
            return grpc_ca.resolve_peer_identity(self.config.engine, context)
        except PermissionError as exc:
            # TLS has already authenticated the certificate chain. A missing
            # or revoked enrollment is an authorization denial, not a request
            # to retry without credentials.
            _abort(context, grpc.StatusCode.PERMISSION_DENIED, str(exc))

    def _require_restore_device(
        self, identity: grpc_store.DeviceIdentity, context: Any
    ) -> grpc_store.GrpcLogicalDevice:
        with self._factory() as session:
            try:
                grpc_store.resolve_device(
                    session,
                    device_id=identity.device_id,
                    cert_fingerprint=identity.fingerprint,
                )
            except PermissionError as exc:
                _abort(context, grpc.StatusCode.PERMISSION_DENIED, str(exc))
            device = session.get(grpc_store.GrpcLogicalDevice, identity.device_id)
            if device is None:
                _abort(context, grpc.StatusCode.PERMISSION_DENIED, "logical device is unavailable")
            try:
                scopes = grpc_store.validate_device_scopes(device.scopes)
            except ValueError:
                _abort(context, grpc.StatusCode.PERMISSION_DENIED, "device scopes are invalid")
            if "restore" not in scopes:
                _abort(context, grpc.StatusCode.PERMISSION_DENIED, "device lacks restore scope")
            session.expunge(device)
            return device

    def _admit(
        self,
        request: Any,
        identity: grpc_store.DeviceIdentity,
        context: Any,
        cell: _Outcome,
    ) -> _Admission:
        """B2: the refusals that write no row, D5's re-check, then the attempt row.

        A refusal here belongs to the device or the caller, not the item, so no
        row is written. Then, under ``_registry_lock``, the attempt row is
        inserted and committed and its entry registered; only after that does
        the caller take a capacity slot.
        """

        if request.restore_request_item_id <= 0:
            _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "restore item id must be positive")
        self._require_restore_device(identity, context)
        with self._factory() as session:
            item = session.scalar(
                select(RestoreRequestItem)
                .options(selectinload(RestoreRequestItem.request))
                .where(RestoreRequestItem.id == request.restore_request_item_id)
            )
            if item is None or item.request is None:
                _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "restore item is unavailable")
            restore_request = item.request
            if restore_request.delivery_mode != "agent":
                _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "item is not agent-delivery")
            if restore_request.receiver_device_id != identity.device_id:
                _abort(
                    context, grpc.StatusCode.PERMISSION_DENIED, "device is not the bound receiver"
                )
            # ``streaming`` is reopenable only through its persisted lease: the
            # lease CAS below rejects a live generation and supersedes an
            # expired one. A bare streaming state remains fail-closed.
            revealed_done = (
                item.state == ITEM_DONE and item.checkpoint is not None and item.checkpoint.revealed
            )
            if (
                not revealed_done
                and item.state not in {ITEM_QUEUED, ITEM_SENT}
                and (item.state != ITEM_STREAMING or item.open_session is None)
            ):
                _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "item is not streamable")
            # D5's re-check: one open attempt per item; a queued or sent item
            # still backing off, or given up, is refused without a row. A
            # streaming item and the revealed replay meet only the first rule.
            open_attempt = session.scalar(
                select(RestoreItemAttempt.id)
                .where(
                    RestoreItemAttempt.restore_request_item_id == item.id,
                    RestoreItemAttempt.ended_at.is_(None),
                    RestoreItemAttempt.closed_at.is_(None),
                )
                .limit(1)
            )
            if open_attempt is not None:
                _abort(context, grpc.StatusCode.ALREADY_EXISTS, _OPEN_IN_PROGRESS)
            if item.state in {ITEM_QUEUED, ITEM_SENT}:
                summary = attempt_summaries(session, [item.id])[item.id]
                now = session.scalar(select(func.now()))
                assert now is not None
                offer = plan_next_offer(summary, now, self._retry_policy)
                if offer.kind == OFFER_OPEN:
                    _abort(context, grpc.StatusCode.ALREADY_EXISTS, _OPEN_IN_PROGRESS)
                if offer.kind == OFFER_AT:
                    _abort(context, grpc.StatusCode.FAILED_PRECONDITION, _WAITING_BEFORE_RETRY)
                if offer.kind == OFFER_GIVE_UP:
                    _abort(context, grpc.StatusCode.FAILED_PRECONDITION, _WILL_NOT_RETRY)
            item_id = item.id
            request_id = item.request_id
            opened_from_state = item.state
        entry = self._insert_attempt(
            context,
            cell,
            item_id=item_id,
            request_id=request_id,
            device_id=identity.device_id,
            opened_from_state=opened_from_state,
        )
        return _Admission(entry=entry, revealed=revealed_done)

    def _insert_attempt(
        self,
        context: Any,
        cell: _Outcome,
        *,
        item_id: int,
        request_id: str,
        device_id: str,
        opened_from_state: str,
    ) -> _AttemptEntry:
        """Insert the attempt row (``attempt_no`` = max + 1 in the INSERT … SELECT) and register it."""

        instance_id = self._instance_id
        if instance_id is None:
            _abort(context, grpc.StatusCode.UNAVAILABLE, _NOT_STARTED)
        attempt = RestoreItemAttempt
        next_row = select(
            literal(item_id),
            func.coalesce(func.max(attempt.attempt_no), 0) + 1,
            literal(device_id),
            literal(instance_id),
            literal(opened_from_state),
            func.now(),
            literal(default_code_version()),
        ).where(attempt.restore_request_item_id == item_id)
        statement = insert(attempt).from_select(
            [
                "restore_request_item_id",
                "attempt_no",
                "receiver_device_id",
                "server_instance_id",
                "opened_from_state",
                "opened_at",
                "code_version",
            ],
            next_row,
        )
        with self._registry_lock:
            try:
                with self._factory.begin() as session:
                    session.connection().execute(statement)
                    attempt_id = session.scalar(
                        select(attempt.id)
                        .where(attempt.restore_request_item_id == item_id)
                        .order_by(attempt.attempt_no.desc())
                        .limit(1)
                    )
            except IntegrityError as exc:
                # Only the two unique constraints (one open attempt per item;
                # attempt_no) mean a concurrent admission won; anything else
                # is a fault and propagates.
                if not _is_open_attempt_conflict(exc):
                    raise
                _abort(context, grpc.StatusCode.ALREADY_EXISTS, _OPEN_IN_PROGRESS)
            assert attempt_id is not None
            entry = _AttemptEntry(
                attempt_id=attempt_id,
                item_id=item_id,
                request_id=request_id,
                opened_from_state=opened_from_state,
                outcome=cell,
            )
            self._registry[attempt_id] = entry
        return entry

    def _forget(self, entry: _AttemptEntry) -> None:
        with self._registry_lock:
            self._registry.pop(entry.attempt_id, None)

    def _keep_ended(self, entry: _AttemptEntry) -> None:
        """Mark an entry whose end write failed: ended, with the outcome's class (A2)."""

        outcome = entry.outcome
        with self._registry_lock:
            entry.ended = True
            entry.klass = classify(
                outcome.end_kind, outcome.cause, entry.opened_from_state == ITEM_DONE
            )

    def _prepare_open(
        self,
        request: Any,
        identity: grpc_store.DeviceIdentity,
        context: Any,
        entry: _AttemptEntry,
    ) -> _PreparedOpen:
        """Authorize and freeze one admitted open; every refusal sets the cell first (B3)."""

        cell = entry.outcome
        with self._factory() as session:
            item = session.scalar(
                select(RestoreRequestItem)
                .options(selectinload(RestoreRequestItem.request))
                .where(RestoreRequestItem.id == entry.item_id)
            )
            if item is None or item.request is None:
                # Admission saw it moments ago; the ledger's RESTRICT key keeps it.
                _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "restore item is unavailable")
            restore_request = item.request
            try:
                authorize_agent_restore_destination(
                    session,
                    receiver_device_id=identity.device_id,
                    destination_id=restore_request.destination_id,
                )
            except Exception as exc:
                _refuse(
                    context,
                    cell,
                    grpc.StatusCode.PERMISSION_DENIED,
                    str(exc),
                    cause="not_authorized",
                )
            self._require_live_capabilities(session, item, identity, context, cell)
            if item.final_rel_path is None or item.size_bytes is None:
                _refuse(
                    context,
                    cell,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "item manifest is incomplete",
                    cause="manifest_invalid",
                )
            try:
                final_rel_path = validate_restore_relative_path(item.final_rel_path)
            except InvalidRestoreDestination as exc:
                _refuse(
                    context,
                    cell,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    str(exc),
                    cause="manifest_invalid",
                )
            manifest = _ManifestFile(
                index=0,
                final_rel_path=final_rel_path,
                size=item.size_bytes,
                content_sha256=item.content_sha256,
            )
            digest = _manifest_digest([manifest])
            checkpoint = item.checkpoint
            if checkpoint is None:
                checkpoint = RestoreItemCheckpoint(
                    restore_request_item_id=item.id,
                    manifest_sha256=digest,
                    committed_index=0,
                    revealed=False,
                )
                try:
                    with session.begin_nested():
                        session.add(checkpoint)
                        session.flush()
                except IntegrityError:
                    checkpoint = session.get(RestoreItemCheckpoint, item.id)
                    if checkpoint is None:
                        _refuse(
                            context,
                            cell,
                            grpc.StatusCode.ALREADY_EXISTS,
                            "restore checkpoint is being frozen by another open",
                            cause="open_conflict",
                        )
            if checkpoint.manifest_sha256 != digest:
                _refuse(
                    context,
                    cell,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "server-frozen restore manifest changed",
                    cause="manifest_invalid",
                )

            expected_generation: int | None = None
            if request.HasField("lease_token"):
                supplied = request.lease_token
                if (
                    supplied.restore_request_item_id != item.id
                    or supplied.receiver_device_id != identity.device_id
                    or supplied.manifest_sha256 != digest
                ):
                    _refuse(
                        context,
                        cell,
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "lease token does not match",
                        cause="stale_lease",
                    )
                expected_generation = supplied.generation
                if item.open_session is None or item.open_session.generation != expected_generation:
                    _refuse(
                        context,
                        cell,
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "lease generation is stale or superseded",
                        cause="stale_lease",
                    )
            if request.HasField("resume_token"):
                resume = request.resume_token
                if resume.restore_request_item_id != item.id or resume.manifest_sha256 != digest:
                    _refuse(
                        context,
                        cell,
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "resume token manifest does not match the frozen plan",
                        cause="resume_mismatch",
                    )
                if not request.HasField("lease_token"):
                    _refuse(
                        context,
                        cell,
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "resume requires the prior lease generation",
                        cause="protocol",
                    )

            # The receiver's claim (D10): what it says it holds durably.
            claim = request.resume_token.committed_index if request.HasField("resume_token") else 0

            session.commit()
            if checkpoint.revealed:
                return _PreparedOpen(
                    item_id=item.id,
                    device_id=identity.device_id,
                    artifactclass=item.artifactclass,
                    manifest=manifest,
                    manifest_sha256=digest,
                    plan=None,
                    member=None,
                    cache_entry_hash=None,
                    source=item.source or "cache",
                    committed_index=checkpoint.committed_index,
                    claim=claim,
                    revealed=True,
                    expected_generation=expected_generation,
                    outcome=cell,
                )
            if not request.HasField("lease_token"):
                # D3 "Before the probe": a token-less open against a live lease
                # is refused now, read only, instead of after the probe (`:698`).
                current = self._lease_row(session, item.id)
                if current is not None and current[1] > dt.datetime.now(dt.UTC):
                    _refuse(
                        context,
                        cell,
                        grpc.StatusCode.ALREADY_EXISTS,
                        "restore item has a live open",
                        cause="lease_live",
                        blocked_until=current[1],
                    )
            # The plan starts from the lower of the claim and the stored value;
            # a claim above the stored value is never adopted (D10).
            planned = min(claim, checkpoint.committed_index)
            if planned > manifest.index:
                return _PreparedOpen(
                    item_id=item.id,
                    device_id=identity.device_id,
                    artifactclass=item.artifactclass,
                    manifest=manifest,
                    manifest_sha256=digest,
                    plan=None,
                    member=None,
                    cache_entry_hash=None,
                    source=item.source or "cache",
                    committed_index=planned,
                    claim=claim,
                    revealed=False,
                    expected_generation=expected_generation,
                    outcome=cell,
                )
            plan: RestorePlan | None = None
            member: PlannedMember | None = None
            cache_entry_hash: bytes | None = None
            source = "cache"
            cache = bind_restore_event_sink_to_session(self._cache, session)
            cache_entry = _select_cache_entry(session, item.content_sha256, config=cache)
            # A recovered cache disk is a fact about the disk; commit it (and its
            # alarm) now. Nothing else is pending here (the commit above).
            session.commit()
            if cache_entry is not None and self._probe_cache_entry(
                session, cache_entry, item, cell
            ):
                cache_entry_hash = cache_entry.content_sha256
                cell.probe_source = "cache"
                self._record_probe_evidence(session, entry)
            else:
                backends = self.config.backend_resolver(session, item.artifactclass)
                plan = build_restore_plan(
                    session,
                    asset_hash=item.content_sha256,
                    artifactclass=item.artifactclass,
                    backends=backends,
                    extractor=self.config.extractor_factory(),
                )
                try:
                    try:
                        member = _select_verified_disk_member(plan, manifest, cell)
                        cell.probe_source = "disk"
                        # One commit: the SUSPECT marks and the probe's evidence.
                        self._record_probe_evidence(session, entry)
                    except TapeSourceDeferred as exc:
                        plan.close()
                        # A fresh Core read, not session.get: a present cache entry
                        # means the cache disk, not the item, is the fault (B3).
                        entry_state = session.execute(
                            select(CacheEntry.state).where(
                                CacheEntry.content_sha256 == item.content_sha256
                            )
                        ).scalar()
                        cause = (
                            "cache_disk_unavailable" if entry_state == "present" else "tape_only"
                        )
                        self._record_probe_evidence(session, entry)
                        _refuse(
                            context,
                            cell,
                            grpc.StatusCode.FAILED_PRECONDITION,
                            str(exc),
                            cause=cause,
                        )
                    except RestoreIntegrityError as exc:
                        # One commit: the SUSPECT marks and the probe's evidence.
                        self._record_probe_evidence(session, entry)
                        plan.close()
                        _refuse(
                            context,
                            cell,
                            grpc.StatusCode.FAILED_PRECONDITION,
                            str(exc),
                            cause="source_integrity",
                        )
                    except RestoreSourceUnavailable as exc:
                        plan.close()
                        self._record_probe_evidence(session, entry)
                        _refuse(
                            context,
                            cell,
                            grpc.StatusCode.FAILED_PRECONDITION,
                            str(exc),
                            cause="source_unavailable",
                        )
                except BaseException:
                    # C5: nothing raised after the plan was built may leak it
                    # (``close`` is idempotent, so the refusals above stay right).
                    plan.close()
                    raise
            return _PreparedOpen(
                item_id=item.id,
                device_id=identity.device_id,
                artifactclass=item.artifactclass,
                manifest=manifest,
                manifest_sha256=digest,
                plan=plan,
                member=member,
                cache_entry_hash=cache_entry_hash,
                source=source,
                committed_index=planned,
                claim=claim,
                revealed=False,
                expected_generation=expected_generation,
                outcome=cell,
            )

    def _record_probe_evidence(self, session: Session, entry: _AttemptEntry) -> None:
        """C5: the probe's three columns on the open attempt, committed on their own.

        Written in ``_prepare_open``'s own session and committed there
        (D-RSRP-14): no second session is opened while an open is prepared.
        """

        cell = entry.outcome
        session.connection().execute(
            update(RestoreItemAttempt)
            .where(
                RestoreItemAttempt.id == entry.attempt_id,
                RestoreItemAttempt.ended_at.is_(None),
                RestoreItemAttempt.closed_at.is_(None),
            )
            .values(
                probe_source=cell.probe_source,
                probe_candidates_tried=cell.probe_candidates,
                probe_ended_at=func.now(),
            )
        )
        session.commit()

    def _probe_cache_entry(
        self, session: Session, entry: CacheEntry, item: RestoreRequestItem, cell: _Outcome
    ) -> bool:
        """Verify a cache hit to EOF before any restore frame can be emitted.

        A failure goes through the cache failure funnel with this session's
        bound config and is committed at once (the funnel deletes the file and
        only flushes its rows; the refusals after this close the session
        without a commit). The reason is in the funnel's event; the open goes on.
        """

        if item.size_bytes is None:
            raise RuntimeError("cache probe requires a restore item size")
        cache = bind_restore_event_sink_to_session(self._cache, session)
        cell.probe_candidates += 1
        try:
            with open_cache_plaintext_chunks(
                session,
                entry,
                expected_content_sha256=item.content_sha256,
                expected_size_bytes=item.size_bytes,
                artifactclass=item.artifactclass,
                config=cache,
            ) as chunks:
                _drain_chunks(chunks)
        except CacheServeFailed as exc:
            record_cache_serve_failure(
                session,
                entry,
                exc,
                cache,
                content_sha256=item.content_sha256,
                artifactclass=item.artifactclass,
                request_id=item.request_id,
                item_id=item.id,
                destination_id=None if item.request is None else item.request.destination_id,
            )
            session.commit()
            LOG.warning(
                "restore item %s: cache probe failed (%s); falling back to the archive",
                item.id,
                exc.reason,
            )
            return False
        return True

    def _require_live_capabilities(
        self,
        session: Session,
        item: RestoreRequestItem,
        identity: grpc_store.DeviceIdentity,
        context: Any,
        cell: _Outcome,
    ) -> None:
        try:
            capabilities = self._live.capabilities_for(identity.operator)
        except Exception as exc:
            _refuse(
                context,
                cell,
                grpc.StatusCode.FAILED_PRECONDITION,
                str(exc) or type(exc).__name__,
                cause="configuration",
            )
        if "can_restore" not in capabilities:
            _refuse(
                context,
                cell,
                grpc.StatusCode.PERMISSION_DENIED,
                "operator lacks live can_restore",
                cause="not_authorized",
            )
        privacy_level = effective_privacy_level(session, item.content_sha256)
        if privacy_level == "none":
            return
        try:
            mapping = hdcache_privacy_capability_map_from_env()
        except Exception as exc:
            _refuse(
                context,
                cell,
                grpc.StatusCode.FAILED_PRECONDITION,
                str(exc) or type(exc).__name__,
                cause="configuration",
            )
        required = mapping.get(privacy_level)
        if required is None:
            _refuse(
                context,
                cell,
                grpc.StatusCode.FAILED_PRECONDITION,
                f"privacy level {privacy_level} has no restore capability mapping",
                cause="configuration",
            )
        if required not in capabilities:
            _refuse(
                context,
                cell,
                grpc.StatusCode.PERMISSION_DENIED,
                f"operator lacks live {required}",
                cause="not_authorized",
            )

    def _lease_row(self, session: Session, item_id: int) -> tuple[int, dt.datetime] | None:
        """Re-read the lease row the CAS refused: ``(generation, expires_at)``."""

        row = session.execute(
            select(RestoreOpenSession.generation, RestoreOpenSession.expires_at).where(
                RestoreOpenSession.restore_request_item_id == item_id
            )
        ).one_or_none()
        if row is None:
            return None
        return int(row[0]), _as_utc(row[1])

    def _acquire_lease(self, prepared: _PreparedOpen, context: Any, entry: _AttemptEntry) -> _Lease:
        cell = prepared.outcome
        now = dt.datetime.now(dt.UTC)
        expires = now + self.config.lease_duration
        generation: int | None = None
        with self._factory.begin() as session:
            # Apply the lease CAS with Core so ``rowcount`` is the database's
            # arbitration result rather than an ORM identity-map side effect.
            result = session.connection().execute(
                update(RestoreOpenSession)
                .where(
                    RestoreOpenSession.restore_request_item_id == prepared.item_id,
                    RestoreOpenSession.expires_at <= now,
                    *(
                        (RestoreOpenSession.generation == prepared.expected_generation,)
                        if prepared.expected_generation is not None
                        else ()
                    ),
                )
                .values(
                    receiver_device_id=prepared.device_id,
                    manifest_sha256=prepared.manifest_sha256,
                    generation=RestoreOpenSession.generation + 1,
                    expires_at=expires,
                    updated_at=now,
                )
            )
            if result.rowcount == 1:
                generation = session.scalar(
                    select(RestoreOpenSession.generation).where(
                        RestoreOpenSession.restore_request_item_id == prepared.item_id
                    )
                )
            elif prepared.expected_generation is not None:
                # B3's three `:678` rows: re-read the lease the CAS refused.
                detail = "lease generation is stale, live, or superseded"
                current = self._lease_row(session, prepared.item_id)
                if current is None or current[0] != prepared.expected_generation:
                    cell.set("error", "stale_lease", detail)
                else:
                    # Still live, or expired between the CAS and this read: the
                    # expiry read is blocked_until either way (D-RSRP-4).
                    cell.set("error", "lease_live", detail, blocked_until=current[1])
                _abort(context, grpc.StatusCode.FAILED_PRECONDITION, detail)
            else:
                try:
                    # The insert inside a savepoint: on PostgreSQL the
                    # transaction survives the IntegrityError, so the live
                    # lease's expiry is read in this same transaction (C3).
                    with session.begin_nested():
                        session.add(
                            RestoreOpenSession(
                                restore_request_item_id=prepared.item_id,
                                receiver_device_id=prepared.device_id,
                                manifest_sha256=prepared.manifest_sha256,
                                generation=1,
                                expires_at=expires,
                                created_at=now,
                                updated_at=now,
                            )
                        )
                        session.flush()
                except IntegrityError:
                    # `:698`: a live lease with no supplied generation (P51).
                    current = self._lease_row(session, prepared.item_id)
                    cell.set(
                        "error",
                        "lease_live",
                        "restore item has a live open",
                        blocked_until=None if current is None else current[1],
                    )
                    _abort(context, grpc.StatusCode.ALREADY_EXISTS, "restore item has a live open")
                generation = 1
            if generation is None:
                # `:701`, defensive: no expiry to record; the policy waits 30 s.
                cell.set("error", "lease_live", "restore item has a live open")
                _abort(context, grpc.StatusCode.ALREADY_EXISTS, "restore item has a live open")
            # The fence (D6): the attempt still open takes the generation. No
            # row means recovery closed it while the call was probing: the
            # lease write rolls back and no end is written.
            fenced = session.connection().execute(
                update(RestoreItemAttempt)
                .where(
                    RestoreItemAttempt.id == entry.attempt_id,
                    RestoreItemAttempt.ended_at.is_(None),
                    RestoreItemAttempt.closed_at.is_(None),
                )
                .values(lease_generation=generation)
            )
            if fenced.rowcount != 1:
                cell.fenced = True
                _abort(context, grpc.StatusCode.UNAVAILABLE, _CLOSED_BY_RECOVERY)
            # D10: the stored checkpoint never claims more than the receiver
            # holds. Only an open that won the lease gets here; a revealed
            # checkpoint is never lowered; lowering only ever re-sends bytes.
            session.connection().execute(
                update(RestoreItemCheckpoint)
                .where(
                    RestoreItemCheckpoint.restore_request_item_id == prepared.item_id,
                    RestoreItemCheckpoint.committed_index > prepared.claim,
                    RestoreItemCheckpoint.revealed.is_(False),
                )
                .values(committed_index=prepared.claim, updated_at=now)
            )
            # The item's move to ``streaming``, conditioned on it still being
            # openable; the request state is re-derived, never set (D6).
            moved = session.connection().execute(
                update(RestoreRequestItem)
                .where(
                    RestoreRequestItem.id == prepared.item_id,
                    RestoreRequestItem.state.in_([ITEM_QUEUED, ITEM_SENT, ITEM_STREAMING]),
                )
                .values(state=ITEM_STREAMING, detail=None, bytes_restored=0, updated_at=now)
            )
            if moved.rowcount != 1:
                # The whole transaction rolls back: the lease, the fence's
                # generation and the lowering are as before the call.
                detail = "leased restore item ceased to be streamable"
                cell.set("error", "item_withdrawn", detail)
                raise RuntimeError(detail)
            rederive_request_state(session, entry.request_id)
        return _Lease(
            item_id=prepared.item_id,
            device_id=prepared.device_id,
            manifest_sha256=prepared.manifest_sha256,
            generation=generation,
            expires_at=expires,
        )

    def _stream(
        self, prepared: _PreparedOpen, lease: _Lease, context: Any
    ) -> Generator[Any, None, int | None]:
        manifest = prepared.manifest
        top_component = PurePosixPath(manifest.final_rel_path).parts[0]
        token = _lease_proto(lease)
        yield restore_pb2.RestoreFrame(
            manifest_head=restore_pb2.ManifestHead(
                total_bytes=manifest.size,
                file_count=1,
                single_top_level=True,
                top_component=top_component,
                manifest_sha256=prepared.manifest_sha256,
                lease_token=token,
            )
        )
        yield restore_pb2.RestoreFrame(manifest_entry=_manifest_entry_proto(manifest))
        yield restore_pb2.RestoreFrame(
            manifest_end=restore_pb2.ManifestEnd(
                manifest_sha256=prepared.manifest_sha256,
                file_count=1,
            )
        )
        if prepared.committed_index > manifest.index:
            yield restore_pb2.RestoreFrame(
                job_end=restore_pb2.JobEnd(
                    files=0,
                    bytes=0,
                    manifest_sha256=prepared.manifest_sha256,
                )
            )
            return 0
        _require_active(context)
        yield restore_pb2.RestoreFrame(file_header=_file_header_proto(manifest))
        offset = 0
        try:
            with self._open_prepared_chunks(prepared) as chunks:
                for chunk in chunks:
                    # Re-chunk to a wire-safe size regardless of the source producer's
                    # buffer (archive yields 256 KiB, the hdcache producer yields 1 MiB);
                    # the RM2 client rejects any chunk frame larger than its 256 KiB limit.
                    view = memoryview(chunk)
                    for start in range(0, len(view), _WIRE_CHUNK_BYTES):
                        _require_active(context)
                        piece = bytes(view[start : start + _WIRE_CHUNK_BYTES])
                        prepared.outcome.bytes_sent += len(piece)
                        yield restore_pb2.RestoreFrame(
                            chunk=restore_pb2.Chunk(data=piece, offset=offset)
                        )
                        offset += len(piece)
        except (ArchiveRestoreError, CacheServeFailed) as exc:
            # B3: the cell is set at the catch, before the error frame is yielded.
            integrity = isinstance(exc, RestoreIntegrityError | LogicalMemberIntegrityError)
            prepared.outcome.set(
                "error", "source_integrity" if integrity else "source_unavailable", str(exc)
            )
            yield restore_pb2.RestoreFrame(
                error=restore_pb2.RestoreError(code="ARCHIVE_RESTORE_FAILED", message=str(exc))
            )
            return None
        if offset != manifest.size:
            detail = f"restore plan emitted {offset} bytes, expected {manifest.size}"
            prepared.outcome.set("error", "source_integrity", detail)
            raise RestoreSourceUnavailable(detail)
        yield restore_pb2.RestoreFrame(
            file_end=restore_pb2.FileEnd(
                index=manifest.index,
                final_rel_path=manifest.final_rel_path,
                bytes=offset,
                content_sha256=manifest.content_sha256,
            )
        )
        yield restore_pb2.RestoreFrame(
            job_end=restore_pb2.JobEnd(
                files=1,
                bytes=offset,
                manifest_sha256=prepared.manifest_sha256,
            )
        )
        return offset

    @contextmanager
    def _open_prepared_chunks(self, prepared: _PreparedOpen) -> Iterator[Iterator[bytes]]:
        """Open the one pre-verified source through the common plaintext funnel."""

        if prepared.cache_entry_hash is not None:
            with self._factory() as session:
                cache = bind_restore_event_sink_to_session(self._cache, session)
                # The open streams the source prepare just verified; it stays read-only.
                entry = _select_cache_entry(
                    session,
                    prepared.cache_entry_hash,
                    config=cache,
                    allow_recovery_probe=False,
                )
                if entry is None:
                    raise RestoreSourceUnavailable("verified cache source became unavailable")
                with open_cache_plaintext_chunks(
                    session,
                    entry,
                    expected_content_sha256=prepared.manifest.content_sha256,
                    expected_size_bytes=prepared.manifest.size,
                    artifactclass=prepared.artifactclass,
                    config=cache,
                ) as chunks:
                    yield chunks
            return
        if prepared.plan is None or prepared.member is None:
            raise RestoreSourceUnavailable("prepared restore source is incomplete")
        with prepared.plan.open_member_stream(prepared.member) as chunks:
            yield chunks

    # ---- the end (design RSR, D6; order RSR-1b B5) -----------------------------------

    def _end_attempt(self, entry: _AttemptEntry, *, deliver: bool) -> _EndResult:
        """End one attempt: one transaction, then the registry, outside it.

        ``deliver`` is true only for a stream that went through ``_stream`` and
        returned past ``job_end``. ``ended`` or ``already_closed`` removes the
        registry entry; a raise keeps it, marked ended with the cell's outcome,
        for recovery to retry.
        """

        try:
            result = self._end_transaction(entry, deliver=deliver)
        except BaseException:
            self._keep_ended(entry)
            raise
        self._forget(entry)
        return result

    def _end_transaction(self, entry: _AttemptEntry, *, deliver: bool) -> _EndResult:
        outcome = entry.outcome
        lease = entry.lease
        now = dt.datetime.now(dt.UTC)  # the lease clock (D-RSRP-5)
        try:
            with self._factory.begin() as session:
                conn = session.connection()
                sent = False
                cause: Any = outcome.cause
                if deliver:
                    # Step 1: the sent CAS is the first statement, so the write
                    # lock is held from here on SQLite (P50).
                    sent = self._record_sent(conn, entry, now)
                    if sent:
                        cause = None
                    elif (
                        lease is None
                        or not conn.execute(select(_lease_live_clause(lease, now))).scalar()
                    ):
                        cause = "lease_expired"
                    else:
                        cause = "item_withdrawn"
                elif outcome.end_kind == "cancelled" and lease is not None:
                    # A cancelled end whose lease has expired by now is terminal;
                    # decided inside the write so no read precedes it (P50).
                    cause = case(
                        (_lease_live_clause(lease, now), null()), else_=literal("lease_expired")
                    )
                message = None if outcome.message is None else bound_message(outcome.message)
                # Step 2: the end, conditioned on the attempt still being open.
                end = conn.execute(
                    update(RestoreItemAttempt)
                    .where(
                        RestoreItemAttempt.id == entry.attempt_id,
                        RestoreItemAttempt.ended_at.is_(None),
                        RestoreItemAttempt.closed_at.is_(None),
                    )
                    .values(
                        ended_at=func.now(),
                        end_kind=outcome.end_kind,
                        cause=cause,
                        message=message,
                        blocked_until=outcome.blocked_until,
                        bytes_sent=outcome.bytes_sent,
                    )
                )
                if end.rowcount != 1:
                    raise _AlreadyClosed
                # Step 3: an undelivered lease expires and returns the item:
                # the lease row first, then the item, the one lock order every
                # transaction writing both rows keeps (D-RSRP-13). The item's
                # condition matches on the generation, not the expiry.
                if lease is not None and not sent:
                    return_state = (
                        ITEM_QUEUED
                        if entry.opened_from_state == ITEM_STREAMING
                        else entry.opened_from_state
                    )
                    conn.execute(
                        update(RestoreOpenSession)
                        .where(
                            RestoreOpenSession.restore_request_item_id == lease.item_id,
                            _lease_matches(lease),
                        )
                        .values(expires_at=now, updated_at=now)
                    )
                    conn.execute(
                        update(RestoreRequestItem)
                        .where(
                            RestoreRequestItem.id == entry.item_id,
                            RestoreRequestItem.state == ITEM_STREAMING,
                            RestoreRequestItem.open_session.has(_lease_matches(lease)),
                        )
                        .values(state=return_state, bytes_restored=0, updated_at=now)
                    )
                # Step 4: the request state, under the request row's lock.
                rederive_request_state(session, entry.request_id)
                # Step 5: the policy; give-up is an event, failed its projection.
                self._apply_give_up(session, entry, now)
        except _AlreadyClosed:
            return _EndResult(END_ALREADY_CLOSED)
        return _EndResult(END_ENDED, sent=sent)

    def _record_sent(self, conn: Any, entry: _AttemptEntry, now: dt.datetime) -> bool:
        """The ``sent`` CAS and its side writes; true when the delivery was recorded."""

        lease = entry.lease
        if lease is None:
            return False
        result = conn.execute(
            update(RestoreRequestItem)
            .where(
                RestoreRequestItem.id == entry.item_id,
                RestoreRequestItem.state == ITEM_STREAMING,
                RestoreRequestItem.open_session.has(
                    and_(_lease_matches(lease), RestoreOpenSession.expires_at > now)
                ),
            )
            .values(
                state=ITEM_SENT,
                detail=None,
                source=entry.source,
                bytes_restored=entry.outcome.bytes_sent,
                updated_at=now,
            )
        )
        if result.rowcount != 1:
            return False
        if entry.cache_entry_hash is not None:
            conn.execute(
                update(CacheEntry)
                .where(CacheEntry.content_sha256 == entry.cache_entry_hash)
                .values(last_read_at=now, trusted=True)
            )
        return True

    def _apply_give_up(self, session: Session, entry: _AttemptEntry, now: dt.datetime) -> None:
        summary = attempt_summaries(session, [entry.item_id])[entry.item_id]
        db_now = session.scalar(select(func.now()))
        assert db_now is not None
        offer = plan_next_offer(summary, db_now, self._retry_policy)
        if offer.kind != OFFER_GIVE_UP:
            return
        decisive = summary.latest_non_wait
        if decisive is None or offer.rule is None:
            return
        detail = compose_detail(offer.rule, offer.counted, decisive)
        failed = session.connection().execute(
            update(RestoreRequestItem)
            .where(
                RestoreRequestItem.id == entry.item_id,
                or_(
                    RestoreRequestItem.state.in_([ITEM_QUEUED, ITEM_SENT]),
                    and_(
                        RestoreRequestItem.state == ITEM_STREAMING,
                        ~RestoreRequestItem.open_session.has(RestoreOpenSession.expires_at > now),
                    ),
                ),
            )
            .values(state=ITEM_FAILED, detail=detail, updated_at=now)
        )
        if failed.rowcount != 1:
            return
        session.connection().execute(
            insert(RestoreItemGiveup).values(
                restore_request_item_id=entry.item_id,
                decisive_attempt_id=decisive.id,
                rule=offer.rule,
                counted=offer.counted,
                threshold=offer.threshold,
                decided_at=func.now(),
                code_version=default_code_version(),
            )
        )
        rederive_request_state(session, entry.request_id)

    def _assignments(self, device_id: str) -> list[Any]:
        with self._factory() as session:
            items = list(
                session.scalars(
                    select(RestoreRequestItem)
                    .join(RestoreRequestItem.request)
                    .options(selectinload(RestoreRequestItem.request))
                    .where(
                        RestoreRequestItem.request.has(
                            delivery_mode="agent", receiver_device_id=device_id
                        ),
                        RestoreRequestItem.state.in_([ITEM_QUEUED, ITEM_SENT]),
                    )
                    .order_by(RestoreRequestItem.id)
                )
            )
            return [_assignment_proto(item) for item in items if item.request is not None]


def _manifest_digest(files: Iterable[_ManifestFile]) -> bytes:
    """Hash canonical length-prefixed ordered v1 manifest fields.

    The domain and file count are followed by each ordered file's UTF-8
    relative path, plaintext size, and plaintext SHA-256 in network byte
    order. This digest is frozen before any frame is emitted.
    """

    entries = tuple(files)
    digest = hashlib.sha256(_MANIFEST_DOMAIN)
    digest.update(struct.pack(">I", len(entries)))
    for entry in entries:
        path = entry.final_rel_path.encode("utf-8")
        digest.update(struct.pack(">I", len(path)))
        digest.update(path)
        digest.update(struct.pack(">Q", entry.size))
        digest.update(entry.content_sha256)
    return digest.digest()


class TapeSourceDeferred(RestoreSourceUnavailable):
    """The frozen item has only tape-backed sources, deferred until RM3."""


def _select_verified_disk_member(
    plan: RestorePlan, manifest: _ManifestFile, cell: _Outcome
) -> PlannedMember:
    """Probe ordered archive candidates, persisting restore_asset's SUSPECT rule.

    Every disk candidate opened counts one on the cell (C5); tape and
    non-matching members are skipped unread.
    """

    errors: list[str] = []
    saw_tape = False
    saw_disk = False
    for member in plan.iter_members():
        if member.asset_hash != manifest.content_sha256 or (
            member.expected_logical_size != manifest.size
        ):
            continue
        if member.copy.backend.kind in {BackendKind.REM_TAPE, BackendKind.D2_TAPE}:
            saw_tape = True
            continue
        saw_disk = True
        cell.probe_candidates += 1
        try:
            with plan.open_member_stream(member) as chunks:
                _drain_chunks(chunks)
            return member
        except LogicalMemberIntegrityError as exc:
            member.copy.health = CopyHealth.SUSPECT
            errors.append(f"copy id={member.copy.id} pool={member.pool_id}: {exc}")
        except (
            ArchiveRestoreError,
            BackendError,
            StagingError,
            ChunkRestoreIntegrityError,
        ) as exc:
            errors.append(f"copy id={member.copy.id} pool={member.pool_id}: {exc}")

    if errors:
        raise RestoreIntegrityError(
            f"all disk candidate restores for asset {manifest.content_sha256.hex()} "
            "failed integrity: " + "; ".join(errors)
        )
    if saw_tape and not saw_disk:
        raise TapeSourceDeferred("tape source deferred to RM3")
    raise RestoreSourceUnavailable("no healthy disk locator matches the frozen item manifest")


def _drain_chunks(chunks: Iterator[bytes]) -> None:
    """Pull a bounded producer to verified EOF without retaining its payload."""

    for _chunk in chunks:
        pass


def _manifest_entry_proto(entry: _ManifestFile) -> Any:
    return restore_pb2.ManifestEntry(
        index=entry.index,
        final_rel_path=entry.final_rel_path,
        size=entry.size,
        content_sha256=entry.content_sha256,
        mode=entry.mode,
        uid=entry.uid,
        gid=entry.gid,
        mtime_unix_seconds=entry.mtime,
    )


def _file_header_proto(entry: _ManifestFile) -> Any:
    return restore_pb2.FileHeader(
        index=entry.index,
        final_rel_path=entry.final_rel_path,
        size=entry.size,
        content_sha256=entry.content_sha256,
        mode=entry.mode,
        uid=entry.uid,
        gid=entry.gid,
        mtime_unix_seconds=entry.mtime,
    )


def _lease_proto(lease: _Lease) -> Any:
    return restore_pb2.LeaseToken(
        restore_request_item_id=lease.item_id,
        receiver_device_id=lease.device_id,
        manifest_sha256=lease.manifest_sha256,
        generation=lease.generation,
        expires_unix_ms=int(lease.expires_at.timestamp() * 1000),
    )


def _assignment_proto(item: RestoreRequestItem) -> Any:
    if item.request is None:
        raise RuntimeError("restore assignment item is detached from its request")
    digest = b""
    if item.final_rel_path is not None and item.size_bytes is not None:
        digest = _manifest_digest(
            [
                _ManifestFile(
                    index=0,
                    final_rel_path=item.final_rel_path,
                    size=item.size_bytes,
                    content_sha256=item.content_sha256,
                )
            ]
        )
    return restore_pb2.Assignment(
        restore_request_item_id=item.id,
        restore_request_id=item.request_id,
        manifest_sha256=digest,
        final_rel_path=item.final_rel_path or "",
        size=item.size_bytes or 0,
        artifactclass=item.artifactclass,
        destination_id=item.request.destination_id,
        state=item.state,
    )


# What the two dialects say when the attempt insert hits D2's unique constraints:
# PostgreSQL names the constraint, SQLite the columns (both start with the item).
_ATTEMPT_UNIQUE_MARKERS = (
    "uq_restore_item_attempt_open",
    "uq_restore_item_attempt_item_no",
    "restore_item_attempt.restore_request_item_id",
)


def _is_open_attempt_conflict(exc: IntegrityError) -> bool:
    """True when the attempt insert failed on the open-attempt index or ``(item, attempt_no)``."""

    text = str(exc.orig) if exc.orig is not None else str(exc)
    return any(marker in text for marker in _ATTEMPT_UNIQUE_MARKERS)


def _lease_matches(lease: _Lease) -> Any:
    """The lease row's identity: receiver, manifest and generation."""

    return and_(
        RestoreOpenSession.receiver_device_id == lease.device_id,
        RestoreOpenSession.manifest_sha256 == lease.manifest_sha256,
        RestoreOpenSession.generation == lease.generation,
    )


def _lease_live_clause(lease: _Lease, now: dt.datetime) -> Any:
    """SQL: the attempt's lease generation still exists and has not expired at ``now``."""

    return exists().where(
        RestoreOpenSession.restore_request_item_id == lease.item_id,
        RestoreOpenSession.generation == lease.generation,
        RestoreOpenSession.expires_at > now,
    )


def _require_active(context: Any) -> None:
    if not context.is_active():
        raise grpc.RpcError("restore stream cancelled")


def _as_utc(value: dt.datetime) -> dt.datetime:
    """Normalize SQLite-naive and timezone-aware lease timestamps."""

    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _abort(context: Any, code: grpc.StatusCode, detail: str) -> NoReturn:
    context.abort(code, detail)
    raise RuntimeError(detail)


def _refuse(
    context: Any,
    cell: _Outcome,
    code: grpc.StatusCode,
    detail: str,
    *,
    cause: str,
    blocked_until: dt.datetime | None = None,
) -> NoReturn:
    """Record an ``error`` outcome in the cell, then abort (B3: the cell before the abort)."""

    cell.set("error", cause, detail, blocked_until=blocked_until)
    _abort(context, code, detail)
