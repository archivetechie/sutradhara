"""Authorized restore-agent streaming over the catalog-backed RestorePlan.

Each OpenRestore owns its backend instances and mutable ``RestorePlan``. SQL
transactions are kept outside the data loop: one validates and leases the open,
one records ``sent``, and a final conditional transaction releases the lease.
The agent path never resolves or writes an archive-server destination path.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import struct
import threading
from collections.abc import Callable, Generator, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, NoReturn

import grpc
from sqlalchemy import Engine, and_, select, update
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
    ITEM_QUEUED,
    ITEM_SENT,
    ITEM_STREAMING,
    REQUEST_ACTIVE,
    CacheServeFailed,
    InvalidRestoreDestination,
    RestoreConfig,
    _select_cache_entry,
    _update_request_state,
    authorize_agent_restore_destination,
    open_cache_plaintext_chunks,
    restore_backends_for_artifactclass,
    restore_config_from_env,
    validate_restore_relative_path,
)
from sutradhara.hdcache.models import (
    CacheEntry,
    RestoreItemCheckpoint,
    RestoreOpenSession,
    RestoreRequestItem,
)
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

    def __post_init__(self) -> None:
        if not 0 < self.max_concurrent_streams < 16:
            raise ValueError("restore concurrency must be positive and below the shared 16 workers")
        if self.lease_duration <= dt.timedelta(0):
            raise ValueError("restore lease duration must be positive")
        if self.assignment_poll_seconds <= 0:
            raise ValueError("assignment poll interval must be positive")


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


@dataclass(frozen=True)
class _PreparedOpen:
    """Detached stream inputs built during the short authorization transaction."""

    item_id: int
    device_id: str
    artifactclass: str
    original_state: str
    manifest: _ManifestFile
    manifest_sha256: bytes
    plan: RestorePlan | None
    member: PlannedMember | None
    cache_entry_hash: bytes | None
    source: str
    committed_index: int
    revealed: bool
    expected_generation: int | None


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
        self._cache = config.cache_config or restore_config_from_env()
        self._slots = threading.BoundedSemaphore(config.max_concurrent_streams)

    def OpenRestore(self, request: Any, context: Any) -> Iterator[Any]:
        """Authorize, exclusively lease, and frame one archive-backed restore item."""

        if not self._slots.acquire(blocking=False):
            _abort(context, grpc.StatusCode.RESOURCE_EXHAUSTED, "restore stream capacity is full")
        prepared: _PreparedOpen | None = None
        lease: _Lease | None = None
        completed = False
        try:
            identity = self._identity(context)
            prepared = self._prepare_open(request, identity, context)
            if prepared.revealed:
                yield restore_pb2.RestoreFrame(
                    job_end=restore_pb2.JobEnd(
                        files=0,
                        bytes=0,
                        manifest_sha256=prepared.manifest_sha256,
                    )
                )
                return
            lease = self._acquire_lease(prepared, context)
            self._mark_streaming(prepared)
            emitted = yield from self._stream(prepared, lease, context)
            if emitted is not None:
                completed = self._mark_sent(prepared, lease, emitted)
        finally:
            if prepared is not None and prepared.plan is not None:
                prepared.plan.close()
            if lease is not None and not completed and prepared is not None:
                self._restore_interrupted_state(prepared, lease)
                self._release_lease(lease)
            self._slots.release()

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

    def _prepare_open(
        self,
        request: Any,
        identity: grpc_store.DeviceIdentity,
        context: Any,
    ) -> _PreparedOpen:
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
            try:
                authorize_agent_restore_destination(
                    session,
                    receiver_device_id=identity.device_id,
                    destination_id=restore_request.destination_id,
                )
            except Exception as exc:
                _abort(context, grpc.StatusCode.PERMISSION_DENIED, str(exc))
            self._require_live_capabilities(session, item, identity, context)
            if item.final_rel_path is None or item.size_bytes is None:
                _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "item manifest is incomplete")
            try:
                final_rel_path = validate_restore_relative_path(item.final_rel_path)
            except InvalidRestoreDestination as exc:
                _abort(context, grpc.StatusCode.FAILED_PRECONDITION, str(exc))
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
                        _abort(
                            context,
                            grpc.StatusCode.ALREADY_EXISTS,
                            "restore checkpoint is being frozen by another open",
                        )
            if checkpoint.manifest_sha256 != digest:
                _abort(
                    context,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "server-frozen restore manifest changed",
                )

            expected_generation: int | None = None
            if request.HasField("lease_token"):
                supplied = request.lease_token
                if (
                    supplied.restore_request_item_id != item.id
                    or supplied.receiver_device_id != identity.device_id
                    or supplied.manifest_sha256 != digest
                ):
                    _abort(
                        context, grpc.StatusCode.FAILED_PRECONDITION, "lease token does not match"
                    )
                expected_generation = supplied.generation
                if item.open_session is None or item.open_session.generation != expected_generation:
                    _abort(
                        context,
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "lease generation is stale or superseded",
                    )
            if request.HasField("resume_token"):
                resume = request.resume_token
                if resume.restore_request_item_id != item.id or resume.manifest_sha256 != digest:
                    _abort(
                        context,
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "resume token manifest does not match the frozen plan",
                    )
                if not request.HasField("lease_token"):
                    _abort(
                        context,
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "resume requires the prior lease generation",
                    )

            session.commit()
            if checkpoint.revealed:
                return _PreparedOpen(
                    item_id=item.id,
                    device_id=identity.device_id,
                    artifactclass=item.artifactclass,
                    original_state=item.state,
                    manifest=manifest,
                    manifest_sha256=digest,
                    plan=None,
                    member=None,
                    cache_entry_hash=None,
                    source=item.source or "cache",
                    committed_index=checkpoint.committed_index,
                    revealed=True,
                    expected_generation=expected_generation,
                )
            if checkpoint.committed_index > manifest.index:
                return _PreparedOpen(
                    item_id=item.id,
                    device_id=identity.device_id,
                    artifactclass=item.artifactclass,
                    original_state=ITEM_QUEUED if item.state == ITEM_STREAMING else item.state,
                    manifest=manifest,
                    manifest_sha256=digest,
                    plan=None,
                    member=None,
                    cache_entry_hash=None,
                    source=item.source or "cache",
                    committed_index=checkpoint.committed_index,
                    revealed=False,
                    expected_generation=expected_generation,
                )
            plan: RestorePlan | None = None
            member: PlannedMember | None = None
            cache_entry_hash: bytes | None = None
            source = "cache"
            entry = _select_cache_entry(session, item.content_sha256, config=self._cache)
            if entry is not None and self._probe_cache_entry(session, entry, item):
                cache_entry_hash = entry.content_sha256
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
                    member = _select_verified_disk_member(plan, manifest)
                    session.commit()
                except TapeSourceDeferred as exc:
                    plan.close()
                    _abort(context, grpc.StatusCode.FAILED_PRECONDITION, str(exc))
                except RestoreIntegrityError as exc:
                    session.commit()
                    plan.close()
                    _abort(context, grpc.StatusCode.FAILED_PRECONDITION, str(exc))
                except RestoreSourceUnavailable as exc:
                    plan.close()
                    _abort(context, grpc.StatusCode.FAILED_PRECONDITION, str(exc))
            return _PreparedOpen(
                item_id=item.id,
                device_id=identity.device_id,
                artifactclass=item.artifactclass,
                original_state=ITEM_QUEUED if item.state == ITEM_STREAMING else item.state,
                manifest=manifest,
                manifest_sha256=digest,
                plan=plan,
                member=member,
                cache_entry_hash=cache_entry_hash,
                source=source,
                committed_index=checkpoint.committed_index,
                revealed=False,
                expected_generation=expected_generation,
            )

    def _probe_cache_entry(
        self, session: Session, entry: CacheEntry, item: RestoreRequestItem
    ) -> bool:
        """Verify a cache hit to EOF before any restore frame can be emitted."""

        assert item.size_bytes is not None
        try:
            with open_cache_plaintext_chunks(
                session,
                entry,
                expected_content_sha256=item.content_sha256,
                expected_size_bytes=item.size_bytes,
                artifactclass=item.artifactclass,
                config=self._cache,
            ) as chunks:
                _drain_chunks(chunks)
        except CacheServeFailed:
            return False
        return True

    def _require_live_capabilities(
        self,
        session: Session,
        item: RestoreRequestItem,
        identity: grpc_store.DeviceIdentity,
        context: Any,
    ) -> None:
        capabilities = self._live.capabilities_for(identity.operator)
        if "can_restore" not in capabilities:
            _abort(context, grpc.StatusCode.PERMISSION_DENIED, "operator lacks live can_restore")
        privacy_level = effective_privacy_level(session, item.content_sha256)
        if privacy_level == "none":
            return
        required = hdcache_privacy_capability_map_from_env().get(privacy_level)
        if required is None:
            _abort(
                context,
                grpc.StatusCode.FAILED_PRECONDITION,
                f"privacy level {privacy_level} has no restore capability mapping",
            )
        if required not in capabilities:
            _abort(context, grpc.StatusCode.PERMISSION_DENIED, f"operator lacks live {required}")

    def _acquire_lease(self, prepared: _PreparedOpen, context: Any) -> _Lease:
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
                _abort(
                    context,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "lease generation is stale, live, or superseded",
                )
            else:
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
                try:
                    session.flush()
                except IntegrityError:
                    _abort(context, grpc.StatusCode.ALREADY_EXISTS, "restore item has a live open")
                generation = 1
        if generation is None:
            _abort(context, grpc.StatusCode.ALREADY_EXISTS, "restore item has a live open")
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
                        yield restore_pb2.RestoreFrame(
                            chunk=restore_pb2.Chunk(data=piece, offset=offset)
                        )
                        offset += len(piece)
        except (ArchiveRestoreError, CacheServeFailed) as exc:
            yield restore_pb2.RestoreFrame(
                error=restore_pb2.RestoreError(code="ARCHIVE_RESTORE_FAILED", message=str(exc))
            )
            return None
        if offset != manifest.size:
            raise RestoreSourceUnavailable(
                f"restore plan emitted {offset} bytes, expected {manifest.size}"
            )
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
                entry = _select_cache_entry(session, prepared.cache_entry_hash, config=self._cache)
                if entry is None:
                    raise RestoreSourceUnavailable("verified cache source became unavailable")
                with open_cache_plaintext_chunks(
                    session,
                    entry,
                    expected_content_sha256=prepared.manifest.content_sha256,
                    expected_size_bytes=prepared.manifest.size,
                    artifactclass=prepared.artifactclass,
                    config=self._cache,
                ) as chunks:
                    yield chunks
            return
        if prepared.plan is None or prepared.member is None:
            raise RestoreSourceUnavailable("prepared restore source is incomplete")
        with prepared.plan.open_member_stream(prepared.member) as chunks:
            yield chunks

    def _mark_streaming(self, prepared: _PreparedOpen) -> None:
        with self._factory.begin() as session:
            item = session.get(RestoreRequestItem, prepared.item_id)
            if item is None or item.state not in {ITEM_QUEUED, ITEM_SENT, ITEM_STREAMING}:
                raise RuntimeError("leased restore item ceased to be streamable")
            item.state = ITEM_STREAMING
            item.detail = None
            item.bytes_restored = 0
            item.updated_at = dt.datetime.now(dt.UTC)
            if item.request is not None:
                item.request.state = REQUEST_ACTIVE

    def _mark_sent(self, prepared: _PreparedOpen, lease: _Lease, emitted_bytes: int) -> bool:
        now = dt.datetime.now(dt.UTC)
        with self._factory.begin() as session:
            result = session.connection().execute(
                update(RestoreRequestItem)
                .where(
                    RestoreRequestItem.id == prepared.item_id,
                    RestoreRequestItem.state == ITEM_STREAMING,
                    RestoreRequestItem.open_session.has(
                        and_(
                            RestoreOpenSession.receiver_device_id == lease.device_id,
                            RestoreOpenSession.manifest_sha256 == lease.manifest_sha256,
                            RestoreOpenSession.generation == lease.generation,
                            RestoreOpenSession.expires_at > now,
                        )
                    ),
                )
                .values(
                    state=ITEM_SENT,
                    detail=None,
                    source=prepared.source,
                    bytes_restored=emitted_bytes,
                    updated_at=now,
                )
            )
            if result.rowcount != 1:
                return False
            item = session.get(RestoreRequestItem, prepared.item_id)
            if item is not None and item.request is not None:
                item.request.state = REQUEST_ACTIVE
            if prepared.cache_entry_hash is not None:
                entry = session.get(CacheEntry, prepared.cache_entry_hash)
                if entry is not None:
                    entry.last_read_at = now
                    entry.trusted = True
            return True

    def _restore_interrupted_state(self, prepared: _PreparedOpen, lease: _Lease) -> None:
        with self._factory.begin() as session:
            session.connection().execute(
                update(RestoreRequestItem)
                .where(
                    RestoreRequestItem.id == prepared.item_id,
                    RestoreRequestItem.state == ITEM_STREAMING,
                    RestoreRequestItem.open_session.has(
                        and_(
                            RestoreOpenSession.receiver_device_id == lease.device_id,
                            RestoreOpenSession.manifest_sha256 == lease.manifest_sha256,
                            RestoreOpenSession.generation == lease.generation,
                        )
                    ),
                )
                .values(
                    state=prepared.original_state,
                    bytes_restored=0,
                    updated_at=dt.datetime.now(dt.UTC),
                )
            )

    def _release_lease(self, lease: _Lease) -> None:
        now = dt.datetime.now(dt.UTC)
        with self._factory.begin() as session:
            session.execute(
                update(RestoreOpenSession)
                .where(
                    RestoreOpenSession.restore_request_item_id == lease.item_id,
                    RestoreOpenSession.receiver_device_id == lease.device_id,
                    RestoreOpenSession.manifest_sha256 == lease.manifest_sha256,
                    RestoreOpenSession.generation == lease.generation,
                )
                .values(expires_at=now, updated_at=now)
            )

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


def _select_verified_disk_member(plan: RestorePlan, manifest: _ManifestFile) -> PlannedMember:
    """Probe ordered archive candidates, persisting restore_asset's SUSPECT rule."""

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
    assert item.request is not None
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
