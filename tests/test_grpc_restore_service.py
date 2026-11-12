"""RM1.2 authorization, source selection, lease, and restore-stream verification."""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import shutil
import socket
import struct
import time
import tracemalloc
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from itertools import pairwise
from pathlib import Path
from typing import Any
from unittest.mock import patch

import grpc
import pytest
from sqlalchemy import Engine, event, func, select
from sqlalchemy import update as sql_update
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from sutradhara._proto import restore_pb2, restore_pb2_grpc
from sutradhara.api.live_capabilities import LiveCapabilityResolver
from sutradhara.api.routes_restore import _request_payload
from sutradhara.archive_restore import (
    LogicalMemberIntegrityError,
    RemArchiveExtractor,
    RestorePlan,
    RestoreSourceUnavailable,
)
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    ArtifactClassPolicyError,
    BundlingPolicy,
    DurabilityPolicy,
    HdcachePolicy,
    PlacementPolicy,
    apply_artifactclass_policy,
    get_artifactclass_policy,
)
from sutradhara.backend.port import BackendLocator, ByteRange, CopyRecord, StreamKind, VerifyResult
from sutradhara.catalog.models import (
    ArtifactClassPool,
    AssetLocator,
    Backend,
    Bundle,
    BundleMember,
    Copy,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, locator_key, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource
from sutradhara.grpc import ca, store
from sutradhara.grpc.restore_service import (
    END_ALREADY_CLOSED,
    RestoreService,
    RestoreServiceConfig,
    _manifest_digest,
    _ManifestFile,
)
from sutradhara.grpc.server import GrpcServerConfig, make_server
from sutradhara.hdcache.alarms import (
    ALARM_DOMAIN,
    RestoreEventAlarmSink,
    restore_event_alarm_sink,
)
from sutradhara.hdcache.manager import (
    CacheServeFailed,
    DiskCircuitBreaker,
    RestoreConfig,
    RestoreEvent,
    _emit,
    bind_restore_event_sink_to_engine,
    rederive_request_state,
)
from sutradhara.hdcache.models import (
    CacheDisk,
    CacheEntry,
    RestoreItemAttempt,
    RestoreItemCheckpoint,
    RestoreItemGiveup,
    RestoreOpenSession,
    RestoreRequest,
    RestoreRequestItem,
    RestoreServerInstance,
)
from sutradhara.hdcache.restore_retry import (
    attempt_summaries,
    compose_last_error,
    giveup_agreement_violations,
    retry_payload,
    retry_policy_from_env,
)
from sutradhara.hdcache.store import (
    AEAD_REPRESENTATION,
    RAW_REPRESENTATION,
    ExpectedDiskIdentity,
    ObservedBlockIdentity,
    StoreReadTimeout,
    entry_path,
    write_disk_sentinel,
    write_entry,
)
from sutradhara.jobs.models import Job, ReconciliationCondition
from sutradhara.jobs.reconcilers import restore_open as restore_open_reconciler
from sutradhara.jobs.reconcilers.conditions import CONDITION_OPEN
from sutradhara.jobs.reconcilers.spine import reconcile
from sutradhara.keys import KEY_DOMAIN_HDCACHE
from sutradhara.keys.remanence import RemRecipientKeyCodec
from sutradhara.rem_archive_cli import resolve_rem_bin
from sutradhara.restore import RestoreIntegrityError as ChunkRestoreIntegrityError
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rem_object import REM_OBJECT_CHUNK_SIZE, RemObjectCliSealer
from tests.bundle_group_helpers import bundle_kwargs
from tests.key_helpers import registry_with_recovery


class _DiskArchiveBackend:
    """Range-stream archive fixture that never materializes a whole object."""

    def __init__(self) -> None:
        self.paths: dict[str, Path] = {}
        self.max_chunk_seen = 0
        self.whole_reads = 0
        self.open_calls = 0

    @property
    def name(self) -> str:
        return "rm12a-disk"

    @property
    def stream_kind(self) -> StreamKind:
        return StreamKind.native_stream

    def add(self, object_id: str, path: Path) -> BackendLocator:
        self.paths[object_id] = path
        return {"object_id": object_id}

    def enumerate(self) -> Iterator[CopyRecord]:
        return iter(())

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        if byte_range.is_whole_object:
            self.whole_reads += 1
        path = self.paths[str(locator["object_id"])]
        with path.open("rb") as handle:
            handle.seek(byte_range.start)
            if byte_range.is_whole_object:
                return handle.read()
            return handle.read(byte_range.length)

    @contextmanager
    def open_range_chunks(
        self,
        locator: BackendLocator,
        byte_range: ByteRange,
        *,
        chunk_bytes: int,
    ) -> Iterator[Iterator[bytes]]:
        self.open_calls += 1
        path = self.paths[str(locator["object_id"])]
        handle = path.open("rb")
        handle.seek(byte_range.start)
        remaining = path.stat().st_size - byte_range.start
        if not byte_range.is_whole_object:
            remaining = byte_range.length

        def chunks() -> Iterator[bytes]:
            nonlocal remaining
            while remaining:
                chunk = handle.read(min(chunk_bytes, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                self.max_chunk_seen = max(self.max_chunk_seen, len(chunk))
                yield chunk

        try:
            yield chunks()
        finally:
            handle.close()

    def verify(self, locator: BackendLocator) -> VerifyResult:
        del locator
        return VerifyResult(ok=True, measured=False)


class _CacheIdentityProbe:
    """Report the physical identity assigned to the cache fixture disk."""

    def observe(self, _mount: Path) -> ObservedBlockIdentity:
        return ObservedBlockIdentity(
            mounted=True,
            serial="RM12B-CACHE",
            fs_uuid="rm12b-cache-fs",
        )


class _CacheTestOpener:
    """Test REM-OBJECT opener preserving the private stored/plaintext boundary."""

    prefix = b"sealed-aead-test\0"

    @contextmanager
    def open(
        self,
        source_path: Path | str,
        representation: Representation,
        *,
        recipient_epochs: Sequence[str] | None = None,
        key_domain: str | None = None,
        work_dir: Path | str | None = None,
    ) -> Iterator[Path]:
        assert representation is Representation.REM_ENCRYPT_V1
        assert recipient_epochs
        assert key_domain == KEY_DOMAIN_HDCACHE
        root = Path(work_dir) if work_dir is not None else Path(source_path).parent
        plaintext = root / f"opened-{hashlib.sha256(Path(source_path).read_bytes()).hexdigest()}"
        stored = Path(source_path).read_bytes()
        assert stored.startswith(self.prefix)
        plaintext.write_bytes(stored[len(self.prefix) :])
        try:
            yield plaintext
        finally:
            plaintext.unlink(missing_ok=True)


class _Abort(Exception):
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        self.code = code
        self.details = details
        super().__init__(details)


class _Context:
    """Minimal synchronous context for auth/lease tests below the socket layer."""

    def __init__(self, device_id: str, fingerprint: str) -> None:
        self.device_id = device_id
        self.fingerprint = fingerprint
        self.active = True
        self.callbacks: list[Any] = []

    def abort(self, code: grpc.StatusCode, details: str) -> None:
        raise _Abort(code, details)

    def is_active(self) -> bool:
        return self.active

    def add_callback(self, callback: Any) -> bool:
        self.callbacks.append(callback)
        return True


class _BareContext(_Context):
    """Production's behaviour (P16): ``abort`` raises a bare ``Exception()`` with no code."""

    def abort(self, code: grpc.StatusCode, details: str) -> None:
        del code, details
        raise Exception()


def _started(service: RestoreService) -> RestoreService:
    """Start a test service without its heartbeat thread (RSR A7); callers stop it."""

    service.start(background=False)
    return service


def _retry_for_request(session: Session, row: RestoreRequest) -> dict[int, dict[str, object]]:
    """Build the API caller's mapping from one summary query, as a page does."""

    summaries = attempt_summaries(session, [item.id for item in row.items])
    now = session.scalar(select(func.now()))
    assert now is not None
    policy = retry_policy_from_env()
    return {
        item.id: retry_payload(item.state, summaries[item.id], policy, now) for item in row.items
    }


class _HookService(RestoreService):
    """Hooks at the seams the exit-table tests need (RSR-1b E1, E8).

    ``before_lease`` runs before the lease transaction (between ``_prepare_open``
    and ``_acquire_lease``); ``lease_row`` rewrites the row the lease CAS
    re-reads (the race inside one call); ``chunks`` wraps the source iterator
    the stream reads; ``before_insert`` runs between D5's pre-check and the
    attempt insert.
    """

    def __init__(
        self,
        config: RestoreServiceConfig,
        *,
        before_lease: Any = None,
        lease_row: Any = None,
        chunks: Any = None,
        before_insert: Any = None,
    ) -> None:
        super().__init__(config)
        self.before_lease = before_lease
        self.lease_row = lease_row
        self.chunks = chunks
        self.before_insert = before_insert

    def _insert_attempt(self, context: Any, cell: Any, **fields: Any) -> Any:
        if self.before_insert is not None:
            self.before_insert(fields["item_id"])
        return super()._insert_attempt(context, cell, **fields)

    def _acquire_lease(self, prepared: Any, context: Any, entry: Any) -> Any:
        if self.before_lease is not None:
            self.before_lease(prepared.item_id)
        return super()._acquire_lease(prepared, context, entry)

    def _lease_row(self, session: Any, item_id: int) -> tuple[int, dt.datetime] | None:
        row = super()._lease_row(session, item_id)
        if self.lease_row is not None and row is not None:
            return self.lease_row(row)  # type: ignore[no-any-return]
        return row

    @contextmanager
    def _open_prepared_chunks(self, prepared: Any) -> Iterator[Iterator[bytes]]:
        with super()._open_prepared_chunks(prepared) as real:
            yield real if self.chunks is None else self.chunks(real)


@dataclass
class _Rig:
    root: Path
    engine: Engine
    pki_dir: Path
    backend: _DiskArchiveBackend
    backend_by_id: dict[int, _DiskArchiveBackend]
    capabilities: dict[str, frozenset[str]]
    unavailable_operators: set[str]
    service_config: RestoreServiceConfig
    service: RestoreService
    receiver_material: ca.LocalDeviceMaterial
    receiver_cert: ca.DeviceCertificate
    fingerprints: dict[str, str] = field(default_factory=dict)

    def context(self, device_id: str) -> _Context:
        return _Context(device_id, self.fingerprints[device_id])


@pytest.fixture(scope="module")
def rig(tmp_path_factory: pytest.TempPathFactory) -> Iterator[_Rig]:
    """The module's shared rig; its database accumulates every test's items."""

    test_rig = _build_rig(tmp_path_factory.mktemp("grpc-restore"))
    try:
        yield test_rig
    finally:
        test_rig.service.stop()
        test_rig.engine.dispose()


@pytest.fixture
def fresh_rig(tmp_path: Path) -> Iterator[_Rig]:
    """A rig with its own engine, for tests that assert the give-up agreement over the whole database.

    The module rig leaves agent items ``failed`` without a give-up row on
    purpose (the terminal-state refusals), so the agreement check only holds on
    a database one test owns.
    """

    test_rig = _build_rig(tmp_path / "grpc-restore-fresh")
    try:
        yield test_rig
    finally:
        test_rig.service.stop()
        test_rig.engine.dispose()


def _build_rig(root: Path) -> _Rig:
    root.mkdir(parents=True, exist_ok=True)
    engine = make_engine(f"sqlite:///{root / 'catalog.db'}")
    create_all(engine)
    pki_dir = root / "pki"
    backend = _DiskArchiveBackend()
    backend_by_id: dict[int, _DiskArchiveBackend] = {}
    capabilities = {"ada": frozenset({"can_restore"})}
    unavailable_operators: set[str] = set()

    def capability_source(operator: str) -> frozenset[str]:
        if operator in unavailable_operators:
            raise ConnectionError("authoritative capability source unavailable")
        return capabilities.get(operator, frozenset())

    receiver_material, receiver_cert = _enroll_socket_device(
        engine,
        pki_dir,
        root / "receiver",
        device_id="receiver",
        operator="ada",
        scopes=("restore",),
    )
    fingerprints = {"receiver": receiver_cert.fingerprint}
    for device_id, operator, scopes in (
        ("ingest-only", "ada", ("ingest",)),
        ("wrong-receiver", "ada", ("restore",)),
        ("no-capability", "bob", ("restore",)),
        ("resolver-down", "offline", ("restore",)),
        ("revoked", "ada", ("restore",)),
        ("assignment-other", "ada", ("restore",)),
    ):
        fingerprint = hashlib.sha256(device_id.encode()).hexdigest()
        with session_scope(engine) as session:
            store.record_device_enrollment(
                session,
                device_id=device_id,
                cert_fingerprint=fingerprint,
                operator=operator,
                scopes=scopes,
            )
        fingerprints[device_id] = store.normalize_fingerprint(fingerprint)
    unavailable_operators.add("offline")
    with session_scope(engine) as session:
        store.revoke_device(session, "revoked")
        for device_id in (
            "receiver",
            "wrong-receiver",
            "no-capability",
            "resolver-down",
            "revoked",
            "assignment-other",
        ):
            session.add(
                store.GrpcDeviceDestinationGrant(
                    device_id=device_id,
                    destination_id="restore-dest",
                    dest_root=str(root / "agent-destinations" / device_id),
                )
            )

    rem_bin = resolve_rem_bin()
    keys, _recovery = registry_with_recovery(
        root / "keys",
        recipient_codec=RemRecipientKeyCodec(rem_bin),
    )
    cache_config = RestoreConfig(
        scratch_root=root / "cache-scratch",
        key_registry=keys,
        opener=_CacheTestOpener(),
        hmac_secret=b"rm12b-test-hmac-secret",
        identity_probe=_CacheIdentityProbe(),
    )
    config = RestoreServiceConfig(
        engine=engine,
        live_capabilities=LiveCapabilityResolver(capability_source),
        backend_resolver=lambda _session, _artifactclass: dict(backend_by_id),
        extractor_factory=lambda: RemArchiveExtractor(rem_bin, keys=keys),
        cache_config=cache_config,
        max_concurrent_streams=4,
        lease_duration=dt.timedelta(minutes=5),
        assignment_poll_seconds=0.01,
        acknowledgement_poll_seconds=0.01,
    )
    service = RestoreService(config)
    service.start(background=False)
    return _Rig(
        root=root,
        engine=engine,
        pki_dir=pki_dir,
        backend=backend,
        backend_by_id=backend_by_id,
        capabilities=capabilities,
        unavailable_operators=unavailable_operators,
        service_config=config,
        service=service,
        receiver_material=receiver_material,
        receiver_cert=receiver_cert,
        fingerprints=fingerprints,
    )


@pytest.fixture
def socket_port(rig: _Rig) -> Iterator[int]:
    """Run the production shared mTLS server on a real loopback socket."""

    port = _unused_port()
    parts = make_server(
        GrpcServerConfig(
            engine=rig.engine,
            landing_root=rig.root / "landing",
            pki_dir=rig.pki_dir,
            bind="127.0.0.1",
            port=port,
            validate_artifactclass=False,
            restore=rig.service_config,
        )
    )
    parts.restore_service.start(background=False)
    parts.server.start()
    try:
        yield port
    finally:
        parts.server.stop(grace=0).wait()
        parts.restore_service.stop()


def test_open_restore_real_socket_streams_ordered_plaintext_with_bounded_memory(
    rig: _Rig,
    socket_port: int,
) -> None:
    payload = (b"archive-backed restore frame\n" * 300_000)[: 8 * 1024 * 1024]
    item_id = _seed_item(rig, "plain", payload, receiver="receiver")

    channel = _receiver_channel(rig, socket_port)
    stub = restore_pb2_grpc.RestoreServiceStub(channel)  # type: ignore[no-untyped-call]
    kinds: list[str] = []
    digest = hashlib.sha256()
    max_frame = 0
    tracemalloc.start()
    baseline, _ = tracemalloc.get_traced_memory()
    try:
        for frame in _receive(
            stub.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), timeout=20
            ),
            lambda ack: stub.CommitRestore(ack, timeout=20),
        ):
            kind = frame.WhichOneof("payload")
            assert kind is not None
            kinds.append(kind)
            if kind == "chunk":
                digest.update(frame.chunk.data)
                max_frame = max(max_frame, len(frame.chunk.data))
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        channel.close()

    assert kinds[:4] == ["manifest_head", "manifest_entry", "manifest_end", "file_header"]
    assert kinds[-2:] == ["file_end", "job_end"]
    assert set(kinds[4:-2]) == {"chunk"}
    assert digest.digest() == hashlib.sha256(payload).digest()
    assert max_frame <= REM_OBJECT_CHUNK_SIZE
    assert rig.backend.max_chunk_seen <= REM_OBJECT_CHUNK_SIZE
    assert rig.backend.whole_reads == 0
    assert peak - baseline < len(payload) // 2


def test_open_restore_real_socket_streams_rem_encrypt_plaintext(
    rig: _Rig, socket_port: int
) -> None:
    payload = b"authenticated encrypted restore\n" * 20_000
    source = rig.root / "encrypted-source.bin"
    source.write_bytes(payload)
    registry = rig.service_config.cache_config.key_registry
    assert registry is not None
    epoch = registry.create_epoch()
    stored = rig.root / "encrypted-stored.rem-object"
    with RemObjectCliSealer(registry).seal(
        source,
        Representation.REM_ENCRYPT_V1,
        key_epoch=epoch,
    ) as sealed:
        shutil.copyfile(sealed.sealed_path, stored)
        recipient_epochs = sealed.recipient_epochs
    item_id = _seed_item(
        rig,
        "aead",
        payload,
        receiver="receiver",
        representation=Representation.REM_ENCRYPT_V1,
        stored_path=stored,
        recipient_epochs=recipient_epochs,
        member_path=source.name,
    )

    channel = _receiver_channel(rig, socket_port)
    try:
        stub = restore_pb2_grpc.RestoreServiceStub(channel)  # type: ignore[no-untyped-call]
        frames = _receive(
            stub.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), timeout=20
            ),
            lambda ack: stub.CommitRestore(ack, timeout=20),
        )
        digest = hashlib.sha256()
        kinds: list[str] = []
        for frame in frames:
            kinds.append(frame.WhichOneof("payload"))
            if frame.HasField("chunk"):
                digest.update(frame.chunk.data)
    finally:
        channel.close()

    assert digest.digest() == hashlib.sha256(payload).digest()
    assert kinds[:4] == ["manifest_head", "manifest_entry", "manifest_end", "file_header"]
    assert kinds[-2:] == ["file_end", "job_end"]


def test_cache_hit_streams_plaintext_bounded_without_archive_read(rig: _Rig) -> None:
    payload = (b"bounded hdcache plaintext\n" * 400_000)[: 8 * 1024 * 1024]
    item_id = _seed_item(rig, "cache-plain", payload, receiver="receiver")
    _seed_cache_entry(rig, item_id, payload)
    archive_opens = rig.backend.open_calls
    digest = hashlib.sha256()
    max_frame = 0

    tracemalloc.start()
    baseline, _ = tracemalloc.get_traced_memory()
    try:
        for frame in _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        ):
            if frame.HasField("chunk"):
                digest.update(frame.chunk.data)
                max_frame = max(max_frame, len(frame.chunk.data))
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert digest.digest() == hashlib.sha256(payload).digest()
    # Every chunk frame must fit the RM2 client's 256 KiB wire limit — the hdcache
    # producer reads in 1 MiB buffers, so _stream re-chunks to this bound. (A looser
    # <=1 MiB assertion here was vacuous: it let agent-rejecting frames through.)
    assert max_frame <= 256 * 1024
    assert peak - baseline < len(payload) // 2
    assert rig.backend.open_calls == archive_opens


def test_private_cache_hit_aead_opens_verified_plaintext_without_archive_read(rig: _Rig) -> None:
    payload = b"private cache plaintext\n" * 20_000
    item_id = _seed_item(
        rig,
        "cache-aead",
        payload,
        receiver="receiver",
        privacy_level="p2",
    )
    _seed_cache_entry(rig, item_id, payload, private=True)
    archive_opens = rig.backend.open_calls
    original = rig.capabilities["ada"]
    try:
        rig.capabilities["ada"] = frozenset({"can_restore", "can_restore_p2"})
        frames = list(
            _receive(
                rig.service.OpenRestore(
                    restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                    rig.context("receiver"),
                ),
                lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
            )
        )
    finally:
        rig.capabilities["ada"] = original

    assert _payload_digest(frames) == hashlib.sha256(payload).digest()
    assert rig.backend.open_calls == archive_opens


def test_cache_miss_falls_back_to_verified_archive_disk(rig: _Rig) -> None:
    payload = b"cache miss archive fallback"
    item_id = _seed_item(rig, "cache-miss", payload, receiver="receiver")
    archive_opens = rig.backend.open_calls

    frames = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )

    assert _payload_digest(frames) == hashlib.sha256(payload).digest()
    assert rig.backend.open_calls > archive_opens


def test_corrupt_cache_degrades_to_verified_archive(rig: _Rig) -> None:
    payload = b"archive truth survives a corrupt cache"
    item_id = _seed_item(rig, "cache-corrupt", payload, receiver="receiver")
    cache_path = _seed_cache_entry(rig, item_id, payload)
    cache_path.write_bytes(b"corrupt cache bytes")
    archive_opens = rig.backend.open_calls

    frames = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )

    assert _payload_digest(frames) == hashlib.sha256(payload).digest()
    assert rig.backend.open_calls > archive_opens


def test_archive_integrity_failure_marks_suspect_and_uses_next_disk_candidate(
    rig: _Rig,
) -> None:
    payload = b"secondary disk candidate is healthy"
    item_id = _seed_item(rig, "suspect-fallback", payload, receiver="receiver")
    primary_path = rig.backend.paths["object-suspect-fallback"]
    fallback_copy_id = _add_archive_candidate(rig, item_id, payload, suffix="secondary")
    corrupt = bytearray(payload)
    corrupt[0] ^= 0xFF
    primary_path.write_bytes(b"archive-prefix" + corrupt)

    frames = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )

    assert _payload_digest(frames) == hashlib.sha256(payload).digest()
    with session_scope(rig.engine) as session:
        copies = list(
            session.scalars(
                select(Copy).where(Copy.bundle_id == "bundle-suspect-fallback").order_by(Copy.id)
            )
        )
        assert copies[0].health == CopyHealth.SUSPECT
        assert copies[1].id == fallback_copy_id
        assert copies[1].health == CopyHealth.OK


def test_manifest_digest_is_source_independent_between_archive_and_cache(rig: _Rig) -> None:
    payload = b"manifest identity ignores selected source"
    item_id = _seed_item(rig, "manifest-source", payload, receiver="receiver")
    archive_frames = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )
    _seed_cache_entry(rig, item_id, payload)
    with session_scope(rig.engine) as session:
        lease = session.get(RestoreOpenSession, item_id)
        assert lease is not None
        lease.expires_at = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1)
    cache_frames = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )

    assert _manifest_sha256(archive_frames) == _manifest_sha256(cache_frames)
    assert _payload_digest(archive_frames) == _payload_digest(cache_frames)


def test_manifest_digest_golden_vectors_and_cross_frame_consistency(rig: _Rig) -> None:
    """Lock the RM2 digest bytes and every integrity-bearing restore frame."""

    fixed_files = (
        ("golden-alpha", b"alpha archive member\n", "exports/golden-alpha.bin"),
        ("golden-beta", b"beta\x00archive\x00member\n", "exports/golden-beta.bin"),
    )
    emitted_entries: list[_ManifestFile] = []
    for suffix, payload, final_rel_path in fixed_files:
        item_id = _seed_item(rig, suffix, payload, receiver="receiver")
        frames = list(
            _receive(
                rig.service.OpenRestore(
                    restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                    rig.context("receiver"),
                ),
                lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
            )
        )
        manifest_head = next(
            frame.manifest_head for frame in frames if frame.HasField("manifest_head")
        )
        manifest_end = next(
            frame.manifest_end for frame in frames if frame.HasField("manifest_end")
        )
        job_end = next(frame.job_end for frame in frames if frame.HasField("job_end"))
        file_end = next(frame.file_end for frame in frames if frame.HasField("file_end"))
        content_sha256 = hashlib.sha256(payload).digest()
        golden = _reference_manifest_digest([(final_rel_path, len(payload), content_sha256)])

        assert manifest_head.manifest_sha256 == golden
        assert manifest_end.manifest_sha256 == golden
        assert job_end.manifest_sha256 == golden
        assert manifest_head.lease_token.manifest_sha256 == golden
        assert file_end.content_sha256 == content_sha256
        assert file_end.bytes == len(payload)
        emitted_entries.append(
            _ManifestFile(
                index=len(emitted_entries),
                final_rel_path=final_rel_path,
                size=len(payload),
                content_sha256=content_sha256,
            )
        )

    multi_file_golden = _reference_manifest_digest(
        [(entry.final_rel_path, entry.size, entry.content_sha256) for entry in emitted_entries]
    )
    assert _manifest_digest(emitted_entries) == multi_file_golden


def test_open_restore_enforces_live_p2_capability_before_streaming(rig: _Rig) -> None:
    item_id = _seed_item(
        rig,
        "privacy-p2",
        b"privacy-tier payload",
        receiver="receiver",
        privacy_level="p2",
    )
    emitted: list[Any] = []
    original = rig.capabilities["ada"]
    try:
        rig.capabilities["ada"] = frozenset({"can_restore"})
        with pytest.raises(_Abort) as denied:
            emitted.extend(
                rig.service.OpenRestore(
                    restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                    rig.context("receiver"),
                )
            )
        assert denied.value.code == grpc.StatusCode.PERMISSION_DENIED
        assert emitted == []
        # RSR-1b: the denial is a counted row; the item backs off before its next try.
        (denial,) = _attempt_rows(rig, item_id)
        assert (denial.end_kind, denial.cause) == ("error", "not_authorized")
        _age_history(rig, item_id)

        rig.capabilities["ada"] = frozenset({"can_restore", "can_restore_p2"})
        frames = list(
            _receive(
                rig.service.OpenRestore(
                    restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                    rig.context("receiver"),
                ),
                lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
            )
        )
        assert frames[-1].HasField("job_end")
    finally:
        rig.capabilities["ada"] = original


@pytest.mark.parametrize("terminal_state", ["done", "failed", "denied"])
def test_open_restore_refuses_terminal_states_without_streaming(
    rig: _Rig, terminal_state: str
) -> None:
    item_id = _seed_item(
        rig,
        f"terminal-{terminal_state}",
        terminal_state.encode(),
        receiver="receiver",
    )
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        item.state = terminal_state
    emitted: list[Any] = []

    with pytest.raises(_Abort) as refused:
        emitted.extend(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            )
        )

    assert refused.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert emitted == []
    assert _attempt_rows(rig, item_id) == []


def test_open_restore_reconciles_catalog_size_before_streaming(rig: _Rig) -> None:
    item_id = _seed_item(rig, "size-mismatch", b"catalog size", receiver="receiver")
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.size_bytes is not None
        item.size_bytes += 1
    emitted: list[Any] = []

    with pytest.raises(_Abort) as refused:
        emitted.extend(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            )
        )

    assert refused.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert emitted == []


def test_restore_lease_default_allows_slow_large_media(rig: _Rig) -> None:
    assert RestoreServiceConfig(engine=rig.engine).lease_duration == dt.timedelta(minutes=30)


@pytest.mark.parametrize(
    ("device_id", "delivery_mode", "expected"),
    [
        ("ingest-only", "agent", grpc.StatusCode.PERMISSION_DENIED),
        ("revoked", "agent", grpc.StatusCode.PERMISSION_DENIED),
        ("wrong-receiver", "agent", grpc.StatusCode.PERMISSION_DENIED),
        ("no-capability", "agent", grpc.StatusCode.PERMISSION_DENIED),
        ("resolver-down", "agent", grpc.StatusCode.PERMISSION_DENIED),
        ("receiver", "server_local", grpc.StatusCode.FAILED_PRECONDITION),
    ],
)
def test_open_restore_negative_auth_matrix_streams_nothing(
    rig: _Rig,
    device_id: str,
    delivery_mode: str,
    expected: grpc.StatusCode,
) -> None:
    receiver = "receiver"
    item_id = _seed_item(
        rig,
        f"deny-{device_id}-{delivery_mode}",
        device_id.encode() * 100,
        receiver=receiver,
        delivery_mode=delivery_mode,
    )
    emitted: list[Any] = []
    rows_before = _attempt_count(rig)

    with pytest.raises(_Abort) as denied:
        emitted.extend(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context(device_id),
            )
        )

    assert denied.value.code == expected
    assert emitted == []
    # B2: a refusal before admission writes no attempt row.
    assert _attempt_count(rig) == rows_before


def test_duplicate_open_rejected_then_expired_generation_reopens(rig: _Rig) -> None:
    """Re-pinned by RSR-1b: the duplicate is refused at admission with no row; a
    stream closed after its lease expired ends ``cancelled`` / ``lease_expired``
    and the item fails (the owner's rule); the expired generation of a *delivered*
    item reopens with generation + 1."""

    item_id = _seed_item(rig, "lease", b"lease arbitration", receiver="receiver")
    request = restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id)
    first = rig.service.OpenRestore(request, rig.context("receiver"))
    assert next(first).HasField("manifest_head")

    with pytest.raises(_Abort) as duplicate:
        list(rig.service.OpenRestore(request, rig.context("receiver")))
    assert duplicate.value.code == grpc.StatusCode.ALREADY_EXISTS
    assert duplicate.value.details == "another open of this item is in progress"
    assert len(_attempt_rows(rig, item_id)) == 1

    _expire_lease(rig, item_id)
    first.close()  # type: ignore[attr-defined]
    (ended,) = _attempt_rows(rig, item_id)
    assert (ended.end_kind, ended.cause) == ("cancelled", "lease_expired")
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "failed"
        giveup = session.scalar(
            select(RestoreItemGiveup).where(RestoreItemGiveup.restore_request_item_id == item_id)
        )
        assert giveup is not None
        assert giveup.rule == "terminal_cause"

    delivered_id = _seed_item(
        rig, "lease-delivered", b"delivered then expired", receiver="receiver"
    )
    delivered_request = restore_pb2.OpenRestoreRequest(restore_request_item_id=delivered_id)
    delivered = list(
        _receive(
            rig.service.OpenRestore(delivered_request, rig.context("receiver")),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )
    first_generation = delivered[0].manifest_head.lease_token.generation
    assert delivered[-1].HasField("job_end")
    _expire_lease(rig, delivered_id)
    reopened = list(
        _receive(
            rig.service.OpenRestore(delivered_request, rig.context("receiver")),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )
    assert reopened[0].manifest_head.lease_token.generation == first_generation + 1
    assert reopened[-1].HasField("job_end")


def test_restore_capacity_fails_fast_and_cancellation_releases_state(rig: _Rig) -> None:
    first_id = _seed_item(rig, "capacity-first", b"first", receiver="receiver")
    second_id = _seed_item(rig, "capacity-second", b"second", receiver="receiver")
    service = _started(RestoreService(replace(rig.service_config, max_concurrent_streams=1)))
    first_context = rig.context("receiver")
    try:
        first = service.OpenRestore(
            restore_pb2.OpenRestoreRequest(restore_request_item_id=first_id), first_context
        )
        assert next(first).HasField("manifest_head")

        with pytest.raises(_Abort) as saturated:
            list(
                service.OpenRestore(
                    restore_pb2.OpenRestoreRequest(restore_request_item_id=second_id),
                    rig.context("receiver"),
                )
            )
        assert saturated.value.code == grpc.StatusCode.RESOURCE_EXHAUSTED

        first_context.active = False
        with pytest.raises(grpc.RpcError):
            list(first)
    finally:
        service.stop()
    with session_scope(rig.engine) as session:
        first_item = session.get(RestoreRequestItem, first_id)
        assert first_item is not None
        assert first_item.state == "queued"
        lease = session.get(RestoreOpenSession, first_id)
        assert lease is not None
        assert lease.expires_at <= dt.datetime.now(dt.UTC).replace(tzinfo=None)
    # RSR-1b: the capacity refusal is a row (after admission); the cancellation another.
    (saturated_row,) = _attempt_rows(rig, second_id)
    assert (saturated_row.end_kind, saturated_row.cause) == ("error", "capacity")
    (cancelled_row,) = _attempt_rows(rig, first_id)
    assert (cancelled_row.end_kind, cancelled_row.cause) == ("cancelled", None)
    assert cancelled_row.lease_generation == 1


def test_archive_integrity_exhaustion_aborts_before_any_frame(rig: _Rig) -> None:
    item_id = _seed_item(rig, "stream-error", b"expected bytes", receiver="receiver")
    path = rig.backend.paths["object-stream-error"]
    path.write_bytes(b"archive-prefix" + b"corrupted data")

    emitted: list[Any] = []
    with pytest.raises(_Abort) as failed:
        emitted.extend(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            )
        )

    assert failed.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert emitted == []
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "queued"
    (row,) = _attempt_rows(rig, item_id)
    assert (row.end_kind, row.cause) == ("error", "source_integrity")
    assert row.message is not None
    assert "failed integrity" in row.message


def test_completed_stream_is_sent_without_job_or_local_write_and_console_is_active(
    rig: _Rig,
) -> None:
    payload = b"remote terminal belongs to RM1.3"
    item_id = _seed_item(rig, "sent", payload, receiver="receiver")
    forbidden_local = rig.root / "agent-destinations" / "receiver" / "exports" / "sent.bin"

    frames = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )
    assert frames[-1].HasField("job_end")
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "sent"
        assert item.source == "cache"
        assert item.bytes_restored == len(payload)
        assert item.request.state == "active"
        assert (
            _request_payload(item.request, _retry_for_request(session, item.request))["state"]
            == "active"
        )
        assert session.scalar(select(func.count()).select_from(Job)) == 0
    assert not forbidden_local.exists()
    (row,) = _attempt_rows(rig, item_id)
    assert (row.end_kind, row.cause, row.opened_from_state) == ("job_end", None, "queued")
    assert row.bytes_sent == len(payload)
    assert row.lease_generation == 1


def test_tape_only_source_is_deferred_without_opening_tape(rig: _Rig) -> None:
    item_id = _seed_item(rig, "source-tape", b"served from tape", receiver="receiver")
    with session_scope(rig.engine) as session:
        backend = session.scalar(select(Backend).where(Backend.name == "disk-source-tape"))
        assert backend is not None
        backend.kind = BackendKind.REM_TAPE

    archive_opens = rig.backend.open_calls
    emitted: list[Any] = []
    with pytest.raises(_Abort) as deferred:
        emitted.extend(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            )
        )

    assert deferred.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "RM3" in deferred.value.details
    assert emitted == []
    assert rig.backend.open_calls == archive_opens
    # Re-pinned by RSR-1b: tape-only is terminal; the item fails at once with
    # the give-up row and the owner's sentence (E3), not `queued`.
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "failed"
        assert item.detail == _TAPE_ONLY_SENTENCE
        giveup = session.scalar(
            select(RestoreItemGiveup).where(RestoreItemGiveup.restore_request_item_id == item_id)
        )
        assert giveup is not None
        assert (giveup.rule, giveup.counted, giveup.threshold) == ("terminal_cause", None, None)
    (row,) = _attempt_rows(rig, item_id)
    assert (row.end_kind, row.cause) == ("error", "tape_only")
    assert row.message == "tape source deferred to RM3"


def test_watch_assignments_isolates_authenticated_device(rig: _Rig) -> None:
    own_id = _seed_item(rig, "assignment-own", b"own", receiver="receiver")
    other_id = _seed_item(
        rig,
        "assignment-other",
        b"other",
        receiver="assignment-other",
    )
    context = rig.context("receiver")
    stream = rig.service.WatchAssignments(restore_pb2.WatchRequest(device_id="receiver"), context)
    seen: set[int] = set()
    try:
        while own_id not in seen:
            seen.add(next(stream).restore_request_item_id)
    finally:
        context.active = False
        stream.close()  # type: ignore[attr-defined]

    assert own_id in seen
    assert other_id not in seen
    with pytest.raises(_Abort) as wrong_watch:
        next(
            rig.service.WatchAssignments(
                restore_pb2.WatchRequest(device_id="assignment-other"),
                rig.context("receiver"),
            )
        )
    assert wrong_watch.value.code == grpc.StatusCode.PERMISSION_DENIED


def test_commit_restore_rejects_an_invalid_request(rig: _Rig) -> None:
    with pytest.raises(_Abort) as invalid:
        rig.service.CommitRestore(restore_pb2.CommitRestoreRequest(), rig.context("receiver"))
    assert invalid.value.code == grpc.StatusCode.FAILED_PRECONDITION


def test_commit_staged_is_durable_monotonic_and_checks_all_cas_bindings(rig: _Rig) -> None:
    item_id = _seed_item(rig, "commit-staged", b"durable staged", receiver="receiver")
    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
        rig.context("receiver"),
    )
    frames = _pull_until_file_end(stream)
    token = frames[0].manifest_head.lease_token
    digest = frames[0].manifest_head.manifest_sha256

    advanced = rig.service.CommitRestore(
        _commit_request(item_id, digest, token, committed_index=1),
        rig.context("receiver"),
    )
    frames.extend(stream)
    assert frames[-1].HasField("job_end")
    unchanged = rig.service.CommitRestore(
        _commit_request(item_id, digest, token, committed_index=0),
        rig.context("receiver"),
    )
    assert (advanced.status, advanced.committed_index, advanced.revealed) == (
        "advanced",
        1,
        False,
    )
    assert (unchanged.status, unchanged.committed_index) == ("unchanged", 1)
    with session_scope(rig.engine) as session:
        checkpoint = session.get(RestoreItemCheckpoint, item_id)
        assert checkpoint is not None
        assert checkpoint.committed_index == 1

    wrong_digest = hashlib.sha256(b"changed plan").digest()
    wrong_manifest_token = restore_pb2.LeaseToken()
    wrong_manifest_token.CopyFrom(token)
    wrong_manifest_token.manifest_sha256 = wrong_digest
    with pytest.raises(_Abort) as mismatch:
        rig.service.CommitRestore(
            _commit_request(item_id, wrong_digest, wrong_manifest_token, committed_index=1),
            rig.context("receiver"),
        )
    assert mismatch.value.code == grpc.StatusCode.FAILED_PRECONDITION

    stale = restore_pb2.LeaseToken()
    stale.CopyFrom(token)
    stale.generation += 1
    with pytest.raises(_Abort) as stale_lease:
        rig.service.CommitRestore(
            _commit_request(item_id, digest, stale, committed_index=1),
            rig.context("receiver"),
        )
    assert stale_lease.value.code == grpc.StatusCode.FAILED_PRECONDITION

    with pytest.raises(_Abort) as wrong_receiver:
        rig.service.CommitRestore(
            _commit_request(item_id, digest, token, committed_index=1),
            rig.context("wrong-receiver"),
        )
    assert wrong_receiver.value.code == grpc.StatusCode.FAILED_PRECONDITION


def test_concurrent_staged_commits_keep_the_highest_index(rig: _Rig) -> None:
    item_id = _seed_item(rig, "commit-race", b"race safely", receiver="receiver")
    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
        rig.context("receiver"),
    )
    frames = _pull_until_file_end(stream)
    token = frames[0].manifest_head.lease_token
    digest = frames[0].manifest_head.manifest_sha256

    def commit(index: int) -> tuple[str, int]:
        reply = rig.service.CommitRestore(
            _commit_request(item_id, digest, token, committed_index=index),
            rig.context("receiver"),
        )
        return reply.status, reply.committed_index

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(commit, (1, 0)))
    frames.extend(stream)
    assert frames[-1].HasField("job_end")

    assert {index for _status, index in results} <= {0, 1}
    with session_scope(rig.engine) as session:
        checkpoint = session.get(RestoreItemCheckpoint, item_id)
        assert checkpoint is not None
        assert checkpoint.committed_index == 1


def test_revealed_is_terminal_idempotent_and_completes_only_after_every_item(rig: _Rig) -> None:
    first_id = _seed_item(rig, "reveal-first", b"first reveal", receiver="receiver")
    second_id = _seed_item(rig, "reveal-second", b"second reveal", receiver="receiver")
    with session_scope(rig.engine) as session:
        first = session.get(RestoreRequestItem, first_id)
        second = session.get(RestoreRequestItem, second_id)
        assert first is not None
        assert second is not None
        second_request = second.request
        second.request = first.request
        session.flush()
        session.delete(second_request)

    commits: list[tuple[int, bytes, Any]] = []
    for item_id in (first_id, second_id):
        frames = list(
            _receive(
                rig.service.OpenRestore(
                    restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                    rig.context("receiver"),
                ),
                lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
            )
        )
        commits.append(
            (
                item_id,
                frames[0].manifest_head.manifest_sha256,
                frames[0].manifest_head.lease_token,
            )
        )

    first_request = _commit_request(*commits[0], committed_index=1, revealed=True)
    first_reply = rig.service.CommitRestore(first_request, rig.context("receiver"))
    replay = rig.service.CommitRestore(first_request, rig.context("receiver"))
    assert (first_reply.status, first_reply.revealed) == ("revealed", True)
    assert (replay.status, replay.revealed) == ("already_done", True)
    with session_scope(rig.engine) as session:
        first = session.get(RestoreRequestItem, first_id)
        second = session.get(RestoreRequestItem, second_id)
        assert first is not None
        assert second is not None
        assert first.state == "done"
        assert second.state == "sent"
        assert first.request.state == "active"
        assert (
            _request_payload(first.request, _retry_for_request(session, first.request))["state"]
            == "active"
        )

    second_reply = rig.service.CommitRestore(
        _commit_request(*commits[1], committed_index=1, revealed=True),
        rig.context("receiver"),
    )
    assert second_reply.status == "revealed"
    with session_scope(rig.engine) as session:
        second = session.get(RestoreRequestItem, second_id)
        assert second is not None
        assert second.request.state == "completed"


def test_resume_redrives_in_progress_file_from_start_and_reveals(rig: _Rig) -> None:
    payload = b"resume from file boundary\n" * 100_000
    item_id = _seed_item(rig, "resume-redrive", payload, receiver="receiver")
    context = rig.context("receiver")
    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), context
    )
    head = next(stream).manifest_head
    for frame in stream:
        if frame.HasField("chunk"):
            assert frame.chunk.offset == 0
            break
    staged = rig.service.CommitRestore(
        _commit_request(item_id, head.manifest_sha256, head.lease_token, committed_index=0),
        rig.context("receiver"),
    )
    assert staged.committed_index == 0
    stream.close()  # type: ignore[attr-defined]

    resumed = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(
                    restore_request_item_id=item_id,
                    lease_token=head.lease_token,
                    resume_token=restore_pb2.ResumeToken(
                        restore_request_item_id=item_id,
                        manifest_sha256=head.manifest_sha256,
                        committed_index=1,
                    ),
                ),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )
    assert resumed[3].file_header.index == 0
    assert next(frame.chunk.offset for frame in resumed if frame.HasField("chunk")) == 0
    assert _payload_digest(resumed) == hashlib.sha256(payload).digest()
    resumed_token = resumed[0].manifest_head.lease_token
    revealed = rig.service.CommitRestore(
        _commit_request(item_id, head.manifest_sha256, resumed_token, 1, revealed=True),
        rig.context("receiver"),
    )
    assert revealed.revealed
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "done"


def test_resume_skips_committed_file_and_revealed_resume_is_already_done(rig: _Rig) -> None:
    """Re-pinned by RSR-1c (D10): the resume claims the file it holds (1); a
    claim of 0 against a stored 1 is the lost-acknowledgement case (K1)."""

    item_id = _seed_item(rig, "resume-skip", b"already staged", receiver="receiver")
    initial = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )
    head = initial[0].manifest_head
    rig.service.CommitRestore(
        _commit_request(item_id, head.manifest_sha256, head.lease_token, 1),
        rig.context("receiver"),
    )
    rig.backend.paths["object-resume-skip"].unlink()
    with session_scope(rig.engine) as session:
        lease = session.get(RestoreOpenSession, item_id)
        assert lease is not None
        lease.expires_at = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1)

    resumed = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(
                    restore_request_item_id=item_id,
                    lease_token=head.lease_token,
                    resume_token=restore_pb2.ResumeToken(
                        restore_request_item_id=item_id,
                        manifest_sha256=head.manifest_sha256,
                        committed_index=1,
                    ),
                ),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )
    assert [frame.WhichOneof("payload") for frame in resumed] == [
        "manifest_head",
        "manifest_entry",
        "manifest_end",
        "job_end",
    ]
    resumed_token = resumed[0].manifest_head.lease_token
    rig.service.CommitRestore(
        _commit_request(item_id, head.manifest_sha256, resumed_token, 1, revealed=True),
        rig.context("receiver"),
    )
    already_done = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(
                    restore_request_item_id=item_id,
                    lease_token=resumed_token,
                    resume_token=restore_pb2.ResumeToken(
                        restore_request_item_id=item_id,
                        manifest_sha256=head.manifest_sha256,
                        committed_index=1,
                    ),
                ),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )
    assert len(already_done) == 1
    assert already_done[0].HasField("job_end")
    assert already_done[0].job_end.files == 0


def test_resume_manifest_mismatch_is_refused(rig: _Rig) -> None:
    item_id = _seed_item(rig, "resume-mismatch", b"frozen plan", receiver="receiver")
    initial = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )
    head = initial[0].manifest_head
    with session_scope(rig.engine) as session:
        lease = session.get(RestoreOpenSession, item_id)
        assert lease is not None
        lease.expires_at = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1)
    emitted: list[Any] = []
    with pytest.raises(_Abort) as mismatch:
        emitted.extend(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(
                    restore_request_item_id=item_id,
                    lease_token=head.lease_token,
                    resume_token=restore_pb2.ResumeToken(
                        restore_request_item_id=item_id,
                        manifest_sha256=hashlib.sha256(b"other plan").digest(),
                    ),
                ),
                rig.context("receiver"),
            )
        )
    assert mismatch.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert emitted == []
    delivered, refused = _attempt_rows(rig, item_id)
    assert (delivered.end_kind, delivered.cause) == ("job_end", None)
    assert (refused.end_kind, refused.cause, refused.opened_from_state) == (
        "error",
        "resume_mismatch",
        "sent",
    )


def test_expired_sent_lease_reconciles_reopenable_and_gets_new_generation(rig: _Rig) -> None:
    payload = b"remote bytes only"
    item_id = _seed_item(rig, "lease-reconcile", payload, receiver="receiver")
    forbidden = rig.root / "agent-destinations" / "receiver" / "exports" / "lease-reconcile.bin"
    initial = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )
    first_generation = initial[0].manifest_head.lease_token.generation
    with session_scope(rig.engine) as session:
        lease = session.get(RestoreOpenSession, item_id)
        assert lease is not None
        lease.expires_at = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1)
    with session_scope(rig.engine) as session:
        assert reconcile(session, restore_open_reconciler.DOMAIN, cursor=item_id - 1) == (1, 1)
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "sent"
        assert item.request.state == "active"
        assert item.detail == restore_open_reconciler.REOPENABLE_DETAIL

    reopened = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )
    assert reopened[0].manifest_head.lease_token.generation == first_generation + 1
    assert _payload_digest(reopened) == hashlib.sha256(payload).digest()
    assert not forbidden.exists()


def test_restore_proto_cross_language_field_numbers_are_frozen() -> None:
    assert {field.name: field.number for field in restore_pb2.RestoreFrame.DESCRIPTOR.fields} == {
        "manifest_head": 1,
        "manifest_entry": 2,
        "file_header": 3,
        "chunk": 4,
        "file_end": 5,
        "manifest_end": 6,
        "job_end": 7,
        "error": 8,
    }
    assert {
        field.name: field.number for field in restore_pb2.OpenRestoreRequest.DESCRIPTOR.fields
    } == {
        "restore_request_item_id": 1,
        "lease_token": 2,
        "resume_token": 3,
    }
    assert {
        field.name: field.number for field in restore_pb2.CommitRestoreRequest.DESCRIPTOR.fields
    } == {
        "restore_request_item_id": 1,
        "manifest_sha256": 2,
        "committed_index": 3,
        "durable_state": 4,
        "lease_token": 5,
    }
    service = restore_pb2.DESCRIPTOR.services_by_name["RestoreService"]
    assert [method.name for method in service.methods] == [
        "OpenRestore",
        "CommitRestore",
        "WatchAssignments",
    ]


def _seed_item(
    rig: _Rig,
    suffix: str,
    payload: bytes,
    *,
    receiver: str,
    delivery_mode: str = "agent",
    representation: Representation = Representation.RAW_BYTES,
    stored_path: Path | None = None,
    recipient_epochs: Sequence[str] = (),
    member_path: str = "payload.bin",
    privacy_level: str = "none",
    destination_id: str = "restore-dest",
) -> int:
    """Create one archive locator and admitted restore item without dispatching a Job."""

    digest = hashlib.sha256(payload).digest()
    artifactclass = f"rm12a-{suffix}"
    pool_id = f"pool-{suffix}"
    bundle_id = f"bundle-{suffix}"
    request_id = f"request-{suffix}"
    object_id = f"object-{suffix}"
    if stored_path is None:
        prefix = b"archive-prefix"
        stored_path = rig.root / f"{object_id}.raw"
        stored_path.write_bytes(prefix + payload)
        block_range: list[int] | None = [len(prefix), len(prefix) + len(payload)]
    else:
        block_range = None
    native = rig.backend.add(object_id, stored_path)
    stored_digest = hashlib.sha256(stored_path.read_bytes()).digest()

    with session_scope(rig.engine) as session:
        backend_row = Backend(
            name=f"disk-{suffix}",
            kind=BackendKind.REM_DISK,
            tier=BackendTier.SELF_DESCRIBING,
        )
        session.add(backend_row)
        session.flush()
        rig.backend_by_id[backend_row.id] = rig.backend
        session.add(
            Pool(id=pool_id, backend_id=backend_row.id, representation=representation.value)
        )
        session.add(LogicalAsset(content_sha256=digest, size_bytes=len(payload)))
        session.add(
            Bundle(
                id=bundle_id,
                **bundle_kwargs(seed=artifactclass),
                status="sealed",
                total_bytes=len(payload),
                member_count=1,
            )
        )
        session.flush()
        session.add(
            BundleMember(
                bundle_id=bundle_id,
                logical_asset_hash=digest,
                artifactclass=artifactclass,
                member_path=member_path,
                size_bytes=len(payload),
                file_sha256=digest,
            )
        )
        apply_artifactclass_policy(
            session,
            artifactclass,
            ArtifactClassPolicy(
                ruleset=f"rm12a.{suffix}",
                placements=(PlacementPolicy(pool_id),),
                bundling=BundlingPolicy(target_gb=1, max_age_seconds=60),
                restore_preference=(pool_id,),
                expect="messy",
                hdcache=HdcachePolicy(enabled=True, privacy_level=privacy_level),
                durability=DurabilityPolicy(min_copies=1, min_impl_families=1),
            ),
        )
        copy = Copy(
            bundle_id=bundle_id,
            backend_id=backend_row.id,
            pool_id=pool_id,
            native_locator=native,
            native_locator_key=locator_key(native),
            storage_metadata={
                "representation": representation.value,
                "stored_size_bytes": stored_path.stat().st_size,
                **({"recipient_epochs": list(recipient_epochs)} if recipient_epochs else {}),
            },
            integrity_hash=stored_digest,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
        )
        session.add(copy)
        session.flush()
        locator_native: dict[str, object] = {
            "member_path": member_path,
            "size_bytes": len(payload),
        }
        if block_range is not None:
            locator_native["block_range"] = block_range
        if representation is Representation.REM_ENCRYPT_V1:
            locator_native["first_chunk_lba"] = 1
        session.add(
            AssetLocator(
                logical_asset_hash=digest,
                pool_id=pool_id,
                copy_id=copy.id,
                bundle_id=bundle_id,
                member_path=member_path,
                native_locator=locator_native,
                representation=representation.value,
            )
        )
        request = RestoreRequest(
            id=request_id,
            identity="ada",
            destination_id=destination_id,
            delivery_mode=delivery_mode,
            receiver_device_id=receiver if delivery_mode == "agent" else None,
            state="active",
            admitted_by="ada",
            admitted_at=dt.datetime.now(dt.UTC),
            admitted_capabilities=["can_restore"],
        )
        item = RestoreRequestItem(
            content_sha256=digest,
            artifactclass=artifactclass,
            final_rel_path=f"exports/{suffix}.bin" if delivery_mode == "agent" else None,
            state="queued",
            size_bytes=len(payload),
            admitted_force_suspect=False,
            admitted_force_rejected=False,
        )
        request.items.append(item)
        session.add(request)
        session.flush()
        return item.id


def _commit_request(
    item_id: int,
    manifest_sha256: bytes,
    lease_token: Any,
    committed_index: int,
    *,
    revealed: bool = False,
) -> Any:
    """Build a commit bound to the exact server-issued manifest generation."""

    return restore_pb2.CommitRestoreRequest(
        restore_request_item_id=item_id,
        manifest_sha256=manifest_sha256,
        committed_index=committed_index,
        durable_state=(
            restore_pb2.DURABLE_STATE_REVEALED if revealed else restore_pb2.DURABLE_STATE_STAGED
        ),
        lease_token=lease_token,
    )


def _seed_cache_entry(
    rig: _Rig,
    item_id: int,
    payload: bytes,
    *,
    private: bool = False,
) -> Path:
    """Place the item's bytes in a live hdcache disk using the real layout."""

    mount = rig.root / "hdcache-disk"
    mount.mkdir(parents=True, exist_ok=True)
    identity = ExpectedDiskIdentity("d-cache", "RM12B-CACHE", "rm12b-cache-fs")
    write_disk_sentinel(
        mount,
        identity,
        hmac_secret=b"rm12b-test-hmac-secret",
    )
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        disk = session.get(CacheDisk, identity.disk_id)
        if disk is None:
            disk = CacheDisk(
                disk_id=identity.disk_id,
                serial=identity.serial,
                fs_uuid=identity.fs_uuid,
                mount=str(mount),
                state="active",
                capacity_bytes=10 * 1024 * 1024 * 1024,
                filled_bytes=0,
            )
            session.add(disk)
            session.flush()
        representation = RAW_REPRESENTATION
        key_epoch: str | None = None
        stored_digest: bytes | None = None
        if private:
            cache_config = rig.service_config.cache_config
            assert cache_config is not None
            registry = cache_config.key_registry
            assert registry is not None
            epoch = registry.create_epoch(KEY_DOMAIN_HDCACHE)
            stored = _CacheTestOpener.prefix + payload
            representation = AEAD_REPRESENTATION
            key_epoch = epoch.key_id
            stored_digest = hashlib.sha256(stored).digest()
            result = write_entry(
                mount,
                item.content_sha256,
                stored,
                representation=representation,
                key_epoch=key_epoch,
                expected_stream_sha256=stored_digest,
            )
        else:
            result = write_entry(mount, item.content_sha256, payload)
        session.add(
            CacheEntry(
                content_sha256=item.content_sha256,
                artifactclass=item.artifactclass,
                disk_id=disk.disk_id,
                relpath=result.relpath,
                size_bytes=result.size_bytes,
                state="present",
                representation=representation,
                key_epoch=key_epoch,
                stored_digest=stored_digest,
                trusted=True,
            )
        )
        disk.filled_bytes += result.size_bytes
        return entry_path(
            mount,
            item.content_sha256,
            representation=representation,
            key_epoch=key_epoch,
        )


def _add_archive_candidate(rig: _Rig, item_id: int, payload: bytes, *, suffix: str) -> int:
    """Append a lower-priority disk copy for candidate-fallback verification."""

    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        primary = session.scalar(
            select(Copy).where(Copy.bundle_id == "bundle-suspect-fallback").order_by(Copy.id)
        )
        assert primary is not None
        pool_id = f"pool-suspect-fallback-{suffix}"
        backend_row = Backend(
            name=f"disk-suspect-fallback-{suffix}",
            kind=BackendKind.REM_DISK,
            tier=BackendTier.SELF_DESCRIBING,
        )
        session.add(backend_row)
        session.flush()
        rig.backend_by_id[backend_row.id] = rig.backend
        session.add(
            Pool(
                id=pool_id,
                backend_id=backend_row.id,
                representation=Representation.RAW_BYTES.value,
            )
        )
        session.add(
            ArtifactClassPool(
                artifactclass=item.artifactclass,
                pool_id=pool_id,
                active=True,
                sort_order=1,
            )
        )
        policy = get_artifactclass_policy(session, item.artifactclass)
        assert primary.pool_id is not None
        policy.restore_preference = [primary.pool_id, pool_id]
        prefix = b"archive-prefix"
        stored_path = rig.root / f"object-suspect-fallback-{suffix}.raw"
        stored_path.write_bytes(prefix + payload)
        native = rig.backend.add(f"object-suspect-fallback-{suffix}", stored_path)
        copy = Copy(
            bundle_id=primary.bundle_id,
            backend_id=backend_row.id,
            pool_id=pool_id,
            native_locator=native,
            native_locator_key=locator_key(native),
            storage_metadata={
                "representation": Representation.RAW_BYTES.value,
                "stored_size_bytes": stored_path.stat().st_size,
            },
            integrity_hash=hashlib.sha256(stored_path.read_bytes()).digest(),
            source=CopySource.INGEST,
            health=CopyHealth.OK,
        )
        session.add(copy)
        session.flush()
        session.add(
            AssetLocator(
                logical_asset_hash=item.content_sha256,
                pool_id=pool_id,
                copy_id=copy.id,
                bundle_id=primary.bundle_id,
                member_path="payload.bin",
                native_locator={
                    "member_path": "payload.bin",
                    "size_bytes": len(payload),
                    "block_range": [len(prefix), len(prefix) + len(payload)],
                },
                representation=Representation.RAW_BYTES.value,
            )
        )
        return copy.id


def _payload_digest(frames: list[Any]) -> bytes:
    digest = hashlib.sha256()
    for frame in frames:
        if frame.HasField("chunk"):
            digest.update(frame.chunk.data)
    return digest.digest()


def _manifest_sha256(frames: list[Any]) -> bytes:
    return bytes(
        next(
            frame.manifest_head.manifest_sha256
            for frame in frames
            if frame.HasField("manifest_head")
        )
    )


def _reference_manifest_digest(entries: list[tuple[str, int, bytes]]) -> bytes:
    """Independently encode the documented canonical restore-manifest layout."""

    encoded = bytearray(b"sutradhara.restore.manifest.v1\0")
    encoded.extend(struct.pack(">I", len(entries)))
    for final_rel_path, size, content_sha256 in entries:
        path = final_rel_path.encode("utf-8")
        encoded.extend(struct.pack(">I", len(path)))
        encoded.extend(path)
        encoded.extend(struct.pack(">Q", size))
        encoded.extend(content_sha256)
    return hashlib.sha256(encoded).digest()


def _enroll_socket_device(
    engine: Engine,
    pki_dir: Path,
    device_dir: Path,
    *,
    device_id: str,
    operator: str,
    scopes: tuple[str, ...],
) -> tuple[ca.LocalDeviceMaterial, ca.DeviceCertificate]:
    material = ca.generate_device_csr(device_dir, device_id=device_id)
    with session_scope(engine) as session:
        token = store.issue_enroll_token(
            session,
            operator=operator,
            device_id=device_id,
            scopes=scopes,
        )
    certificate = ca.sign_device_csr(
        engine,
        pki_dir=pki_dir,
        csr_path=material.csr_path,
        token=token,
    )
    return material, certificate


def _receiver_channel(rig: _Rig, port: int) -> grpc.Channel:
    ca_cert, _ca_key = ca.ensure_ca(rig.pki_dir)
    credentials = grpc.ssl_channel_credentials(
        root_certificates=ca_cert.read_bytes(),
        private_key=rig.receiver_material.key_path.read_bytes(),
        certificate_chain=rig.receiver_cert.cert_path.read_bytes(),
    )
    channel = grpc.secure_channel(f"localhost:{port}", credentials)
    grpc.channel_ready_future(channel).result(timeout=10)
    return channel


def _unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def test_unstarted_service_refuses_open_and_watch_and_writes_nothing(rig: _Rig) -> None:
    item_id = _seed_item(rig, "unstarted", b"unstarted", receiver="receiver")
    with session_scope(rig.engine) as session:
        instances_before = session.scalar(select(func.count(RestoreServerInstance.id)))
    service = RestoreService(rig.service_config)
    with pytest.raises(_Abort) as refused_open:
        next(
            service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            )
        )
    assert refused_open.value.code == grpc.StatusCode.UNAVAILABLE
    assert refused_open.value.details == "restore service is not started"
    with pytest.raises(_Abort) as refused_watch:
        next(
            service.WatchAssignments(
                restore_pb2.WatchRequest(device_id="receiver"), rig.context("receiver")
            )
        )
    assert refused_watch.value.code == grpc.StatusCode.UNAVAILABLE
    assert refused_watch.value.details == "restore service is not started"
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "queued"
        assert session.get(RestoreOpenSession, item_id) is None
        assert session.scalar(select(func.count(RestoreServerInstance.id))) == instances_before
    # The started rig service still serves the same item.
    frames = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )
    assert frames[-1].HasField("job_end")


def test_start_writes_one_instance_row_and_a_second_start_raises(rig: _Rig) -> None:
    with session_scope(rig.engine) as session:
        ids_before = set(session.scalars(select(RestoreServerInstance.id)))
    service = RestoreService(rig.service_config)
    service.start(background=False)
    try:
        with pytest.raises(RuntimeError):
            service.start(background=False)
        with session_scope(rig.engine) as session:
            new_rows = [
                row
                for row in session.scalars(select(RestoreServerInstance))
                if row.id not in ids_before
            ]
        assert len(new_rows) == 1
        row = new_rows[0]
        assert row.id == service.instance_id
        assert row.host == socket.gethostname()
        assert row.pid == os.getpid()
        assert row.code_version
        assert row.started_at == row.heartbeat_at
        assert service.heartbeat_unbroken_seconds() >= 0.0
    finally:
        service.stop()
    with pytest.raises(RuntimeError):
        service.start(background=False)


def test_d_c_commit_executor_matches_stream_limit_and_stops_with_service(rig: _Rig) -> None:
    """The service's commit executor follows its stream limit and stops with it."""

    config = replace(rig.service_config, max_concurrent_streams=3)
    service = RestoreService(config)
    try:
        service.start(background=False)
        assert service._commit_executor._max_workers == config.max_concurrent_streams
    finally:
        service.stop()
    with pytest.raises(RuntimeError, match="cannot schedule new futures after shutdown"):
        service._commit_executor.submit(lambda: None)


def test_background_heartbeat_advances_and_stop_joins(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = RestoreService(
        replace(
            rig.service_config, heartbeat_seconds=0.05, staleness_threshold=dt.timedelta(seconds=4)
        )
    )
    recovery_calls: list[None] = []
    monkeypatch.setattr(service, "recover_once", lambda: recovery_calls.append(None))
    service.start(background=True)
    thread = service._heartbeat_thread
    assert thread is not None
    assert thread.is_alive()
    try:
        with session_scope(rig.engine) as session:
            row = session.get(RestoreServerInstance, service.instance_id)
            assert row is not None
            first_heartbeat = row.heartbeat_at
        deadline = time.monotonic() + 3.0
        advanced = False
        while time.monotonic() < deadline:
            with session_scope(rig.engine) as session:
                row = session.get(RestoreServerInstance, service.instance_id)
                assert row is not None
                if row.heartbeat_at > first_heartbeat:
                    advanced = True
                    break
            time.sleep(0.05)
        assert advanced, "heartbeat_at did not advance within 3 s"
        unbroken = service.heartbeat_unbroken_seconds()
        assert unbroken > 0.0
        time.sleep(0.2)
        assert service.heartbeat_unbroken_seconds() > unbroken
        assert recovery_calls
    finally:
        service.stop()
    # stop() joined the thread it captured above.
    assert not thread.is_alive()


def test_heartbeat_once_records_a_break_after_half_the_threshold(rig: _Rig) -> None:
    # A 2 s break window (half of 4 s); every sleep sits well inside or beyond it,
    # so an oversleep of tens of milliseconds on a loaded host cannot fail it.
    service = RestoreService(
        replace(
            rig.service_config, heartbeat_seconds=0.5, staleness_threshold=dt.timedelta(seconds=4)
        )
    )
    service.start(background=False)
    try:
        service.heartbeat_once()
        time.sleep(0.3)
        assert 0.2 <= service.heartbeat_unbroken_seconds() < 1.5
        time.sleep(2.2)  # the gap since the last success now exceeds 2 s: broken
        assert service.heartbeat_unbroken_seconds() == 0.0
        service.heartbeat_once()  # a new run starts at this success
        time.sleep(0.3)
        assert 0.2 <= service.heartbeat_unbroken_seconds() < 1.5
    finally:
        service.stop()


def test_config_refuses_a_heartbeat_at_or_above_half_the_staleness_threshold(rig: _Rig) -> None:
    with pytest.raises(ValueError, match="half the staleness threshold"):
        replace(
            rig.service_config, heartbeat_seconds=60.0, staleness_threshold=dt.timedelta(minutes=2)
        )
    replace(rig.service_config, heartbeat_seconds=59.0, staleness_threshold=dt.timedelta(minutes=2))
    with pytest.raises(ValueError, match="half the staleness threshold"):
        replace(
            rig.service_config, heartbeat_seconds=10.0, staleness_threshold=dt.timedelta(seconds=20)
        )


def test_default_service_alarm_sink_writes_to_the_service_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A service built without a cache_config (as make_server does) must not leak to make_engine()."""

    for name in [key for key in os.environ if key.startswith("SUTRADHARA_HDCACHE_")]:
        monkeypatch.delenv(name)
    engine = make_engine(f"sqlite:///{tmp_path / 'svc.db'}")
    create_all(engine)

    def leaked() -> Engine:
        raise AssertionError("alarm sink opened make_engine() instead of the service engine")

    monkeypatch.setattr("sutradhara.hdcache.alarms.make_engine", leaked)
    service: RestoreService | None = None
    try:
        service = _started(RestoreService(RestoreServiceConfig(engine=engine)))
        _emit(
            service._cache,
            RestoreEvent(
                code="disk-circuit-open",
                severity="alarm",
                detail="cache disk d001 exceeded failure threshold",
            ),
        )
        with engine.connect() as conn:
            rows = {
                target_key: reason
                for target_key, reason in conn.execute(
                    select(
                        ReconciliationCondition.target_key, ReconciliationCondition.reason
                    ).where(
                        ReconciliationCondition.domain == ALARM_DOMAIN,
                        ReconciliationCondition.condition == CONDITION_OPEN,
                    )
                )
            }
    finally:
        if service is not None:
            service.stop()
        engine.dispose()
    assert rows == {"disk-unreachable:restore": "disk-unreachable"}


def test_bind_restore_event_sink_to_engine_only_rebinds_unbound_alarm_sink(
    tmp_path: Path,
) -> None:
    engine = make_engine(f"sqlite:///{tmp_path / 'a.db'}")
    other = make_engine(f"sqlite:///{tmp_path / 'b.db'}")
    try:
        unbound = RestoreConfig(event_sink=RestoreEventAlarmSink())
        bound = bind_restore_event_sink_to_engine(unbound, engine)
        assert bound is not unbound
        assert bound.event_sink == restore_event_alarm_sink(engine=engine)

        none_cfg = RestoreConfig(event_sink=None)
        assert bind_restore_event_sink_to_engine(none_cfg, engine) is none_cfg

        with session_scope(other) as session:
            session_cfg = RestoreConfig(event_sink=restore_event_alarm_sink(session=session))
            assert bind_restore_event_sink_to_engine(session_cfg, engine) is session_cfg

        other_cfg = RestoreConfig(event_sink=restore_event_alarm_sink(engine=other))
        assert bind_restore_event_sink_to_engine(other_cfg, engine) is other_cfg

        callable_cfg = RestoreConfig(event_sink=lambda event: None)
        assert bind_restore_event_sink_to_engine(callable_cfg, engine) is callable_cfg
    finally:
        engine.dispose()
        other.dispose()


def _set_cache_disk_state(rig: _Rig, state: str) -> None:
    with session_scope(rig.engine) as session:
        disk = session.get(CacheDisk, "d-cache")
        assert disk is not None
        disk.state = state


def test_cache_recovery_on_absent_disk_commits_without_alarm_lock_stall(rig: _Rig) -> None:
    payload = b"recovered cache disk serves\n" * 1000
    item_id = _seed_item(rig, "cache-recover", payload, receiver="receiver")
    _seed_cache_entry(rig, item_id, payload)
    archive_opens = rig.backend.open_calls
    # The rig's cache config has no event sink (events only log); production wires
    # the DB-backed alarm sink, so this test does too.
    assert rig.service_config.cache_config is not None
    service = _started(
        RestoreService(
            replace(
                rig.service_config,
                cache_config=replace(
                    rig.service_config.cache_config, event_sink=restore_event_alarm_sink()
                ),
            )
        )
    )
    _set_cache_disk_state(rig, "absent")
    try:
        started = time.monotonic()
        frames = list(
            _receive(
                service.OpenRestore(
                    restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                    rig.context("receiver"),
                ),
                lambda ack: service.CommitRestore(ack, rig.context("receiver")),
            )
        )
        elapsed = time.monotonic() - started
        service.stop()

        assert elapsed < 10
        assert _payload_digest(frames) == hashlib.sha256(payload).digest()
        assert rig.backend.open_calls == archive_opens
        with session_scope(rig.engine) as session:
            disk = session.get(CacheDisk, "d-cache")
            assert disk is not None
            assert disk.state == "active"
            # _set_alarm(active=False) records an observation: an inactive row
            # (observed present, not open) is created when none existed.
            row = session.scalars(
                select(ReconciliationCondition).where(
                    ReconciliationCondition.domain == ALARM_DOMAIN,
                    ReconciliationCondition.target_key == "disk-unreachable:restore",
                )
            ).one()
            assert row.condition != CONDITION_OPEN
    finally:
        _set_cache_disk_state(rig, "active")


class _AbsentAfterPrepareService(RestoreService):
    """Turn the cache disk absent after prepare verified it, before the open."""

    def _acquire_lease(self, prepared: Any, context: Any, entry: Any) -> Any:
        lease = super()._acquire_lease(prepared, context, entry)
        with session_scope(self.config.engine) as session:
            disk = session.get(CacheDisk, "d-cache")
            assert disk is not None
            disk.state = "absent"
        return lease


def test_open_prepared_cache_source_never_probes_recovery(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = b"prepared cache source goes absent\n" * 100
    item_id = _seed_item(rig, "cache-no-open-probe", payload, receiver="receiver")
    _seed_cache_entry(rig, item_id, payload)

    def _boom(*_args: Any, **_kwargs: Any) -> bool:
        raise AssertionError("the open must not run a recovery probe")

    monkeypatch.setattr("sutradhara.hdcache.manager._probe_cache_disk_recovery", _boom)
    service = _started(_AbsentAfterPrepareService(rig.service_config))
    try:
        frames = list(
            service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            )
        )
    finally:
        service.stop()
        _set_cache_disk_state(rig, "active")

    errors = [frame.error for frame in frames if frame.HasField("error")]
    assert [e.code for e in errors] == ["ARCHIVE_RESTORE_FAILED"]
    assert "verified cache source became unavailable" in errors[0].message
    # RSR-1b: the in-stream source failure is a counted row.
    (row,) = _attempt_rows(rig, item_id)
    assert (row.end_kind, row.cause) == ("error", "source_unavailable")


# ---- RSR-1b: the attempt ledger's writer (design RSR D2, D3, D6) ------------------

# The owner's two terminal sentences (D-RSRP-6), composed by the one composer, not copied.
_TAPE_ONLY_SENTENCE = compose_last_error("error", "tape_only", None)
_LEASE_EXPIRED_SENTENCE = compose_last_error("error", "lease_expired", None)


def _pl(suffix: str) -> bytes:
    """A payload unique to the item (``logical_asset.content_sha256`` is unique)."""

    return f"payload for {suffix}\n".encode() * 3


def _attempt_rows(rig: _Rig, item_id: int) -> list[RestoreItemAttempt]:
    """The item's attempt rows by ``attempt_no``, detached with their columns loaded."""

    with session_scope(rig.engine) as session:
        rows = list(
            session.scalars(
                select(RestoreItemAttempt)
                .where(RestoreItemAttempt.restore_request_item_id == item_id)
                .order_by(RestoreItemAttempt.attempt_no)
            )
        )
        session.expunge_all()
    return rows


def _attempt_count(rig: _Rig) -> int:
    with session_scope(rig.engine) as session:
        return int(session.scalar(select(func.count(RestoreItemAttempt.id))) or 0)


def _age_history(rig: _Rig, item_id: int, by: dt.timedelta = dt.timedelta(hours=1)) -> None:
    """Move the item's end times back so the policy's delay (at most 30 min) has passed."""

    with session_scope(rig.engine) as session:
        for row in session.scalars(
            select(RestoreItemAttempt).where(RestoreItemAttempt.restore_request_item_id == item_id)
        ):
            if row.ended_at is not None:
                row.ended_at = row.ended_at - by
            if row.closed_at is not None:
                row.closed_at = row.closed_at - by


def _expire_lease(rig: _Rig, item_id: int) -> None:
    with session_scope(rig.engine) as session:
        lease = session.get(RestoreOpenSession, item_id)
        assert lease is not None
        lease.expires_at = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1)


def _close_attempt(rig: _Rig, attempt_id: int) -> None:
    """Close an attempt as recovery does: ``closed_at`` set, ``end_kind`` lost."""

    with session_scope(rig.engine) as session:
        row = session.get(RestoreItemAttempt, attempt_id)
        assert row is not None
        row.closed_at = dt.datetime.now(dt.UTC)
        row.end_kind = "lost"


def _giveup_of(session: Any, item_id: int) -> RestoreItemGiveup | None:
    return session.scalar(
        select(RestoreItemGiveup).where(RestoreItemGiveup.restore_request_item_id == item_id)
    )


def _open_collect(
    service: RestoreService, item_id: int, context: _Context, **fields: Any
) -> tuple[list[Any], BaseException | None, RestoreService]:
    """Drive one open to its end; return the frames, the exception that ended it (if any), and the service."""

    frames: list[Any] = []
    request = restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id, **fields)
    try:
        frames.extend(
            _receive(
                service.OpenRestore(request, context),
                lambda ack: service.CommitRestore(ack, context),
            )
        )
    except BaseException as exc:
        return frames, exc, service
    return frames, None, service


def _receive(stream: Iterator[Any], commit: Callable[[Any], Any]) -> Iterator[Any]:
    """Drain as the receiver does: acknowledge each verified file before reading on."""

    head = None
    for frame in stream:
        if frame.HasField("manifest_head"):
            head = frame.manifest_head
        elif frame.HasField("file_end"):
            assert head is not None
            commit(
                _commit_request(
                    head.lease_token.restore_request_item_id,
                    head.manifest_sha256,
                    head.lease_token,
                    frame.file_end.index + 1,
                )
            )
        yield frame


def _pull_until_file_end(stream: Iterator[Any]) -> list[Any]:
    """Pause before acknowledgement so a test can inspect or commit staged progress."""

    frames: list[Any] = []
    for frame in stream:
        frames.append(frame)
        if frame.HasField("file_end"):
            return frames
    raise AssertionError("stream ended before file_end")


def _pull_until_job_end(
    stream: Iterator[Any], service: RestoreService, context: _Context
) -> list[Any]:
    """Acknowledge the file, then pause in the post-job_end window used by E5/E6."""

    frames: list[Any] = []
    for frame in _receive(stream, lambda ack: service.CommitRestore(ack, context)):
        frames.append(frame)
        if frame.HasField("job_end"):
            return frames
    raise AssertionError("stream ended before job_end")


def _deliver(rig: _Rig, item_id: int) -> Any:
    """Stream the item to ``sent`` with the rig's service; return its manifest head."""

    frames = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )
    assert frames[-1].HasField("job_end")
    return frames[0].manifest_head


@contextmanager
def _rlr_service(rig: _Rig) -> Iterator[RestoreService]:
    """Exercise the real delivery path with a short lease and no background workers."""

    service = _started(
        RestoreService(
            replace(
                rig.service_config,
                lease_duration=dt.timedelta(seconds=0.6),
                acknowledgement_poll_seconds=0.04,
            )
        )
    )
    try:
        yield service
    finally:
        service.stop()


def _rlr_lease(rig: _Rig, item_id: int) -> RestoreOpenSession:
    """Read a detached lease so every observation comes from the database."""

    with session_scope(rig.engine) as session:
        lease = session.get(RestoreOpenSession, item_id)
        assert lease is not None
        session.expunge(lease)
        return lease


def _rlr_first_chunk(stream: Iterator[Any]) -> list[Any]:
    """Leave the generator suspended at its first data frame."""

    frames = []
    for frame in stream:
        frames.append(frame)
        if frame.HasField("chunk"):
            return frames
    raise AssertionError("no chunk delivered")


@pytest.mark.parametrize("source", ["archive", "cache"])
def test_rlr_long_delivery_and_bounded_ack_reads(
    fresh_rig: _Rig,
    source: str,
) -> None:
    """Proofs 1/7c: renew both sources over three leases, then poll without spinning."""

    rig = fresh_rig
    payload = b"r" * (32 * 256 * 1024)
    item_id = _seed_item(rig, "rlr-long", payload, receiver="receiver")
    if source == "cache":
        _seed_cache_entry(rig, item_id, payload)
    with _rlr_service(rig) as service:
        context = rig.context("receiver")
        stream = service.OpenRestore(
            restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), context
        )
        observations = []
        frames = []
        started = time.monotonic()
        for frame in stream:
            frames.append(frame)
            if frame.HasField("chunk"):
                assert len(frame.chunk.data) == 256 * 1024
                observations.append(_rlr_lease(rig, item_id))
                time.sleep(0.07)
            if frame.HasField("file_end"):
                break
        assert time.monotonic() - started >= 3 * service.config.lease_duration.total_seconds()
        assert len(observations) == 32
        changes = [observations[0].updated_at]
        head = frames[0].manifest_head
        for row in observations:
            assert row.generation == head.lease_token.generation
            assert row.receiver_device_id == "receiver"
            assert row.manifest_sha256 == head.manifest_sha256
            if row.updated_at != changes[-1]:
                assert (row.updated_at - changes[-1]).total_seconds() >= 0.1
                changes.append(row.updated_at)
        reads = []

        def count_reads(_conn: Any, _cursor: Any, statement: str, *_args: Any) -> None:
            if "FROM restore_item_checkpoint JOIN restore_open_session" in statement:
                reads.append(statement)

        event.listen(rig.engine, "before_cursor_execute", count_reads)
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = executor.submit(list, stream)
                try:
                    with pytest.raises(TimeoutError):
                        pending.result(timeout=0.12)
                    service.CommitRestore(
                        _commit_request(item_id, head.manifest_sha256, head.lease_token, 1),
                        rig.context("receiver"),
                    )
                    tail = pending.result(timeout=2)
                finally:
                    context.active = False
        finally:
            event.remove(rig.engine, "before_cursor_execute", count_reads)
        assert 2 <= len(reads) <= 6
        assert _kinds(tail) == ["job_end"]
        assert len(changes) >= 8
        assert len(changes) < len(observations)
        assert _payload_digest(frames) == hashlib.sha256(payload).digest()
        with session_scope(rig.engine) as session:
            assert session.get(RestoreRequestItem, item_id).state == "sent"
        reply = service.CommitRestore(
            _commit_request(item_id, head.manifest_sha256, head.lease_token, 1, revealed=True),
            rig.context("receiver"),
        )
        assert reply.status == "revealed"
        with session_scope(rig.engine) as session:
            assert session.get(RestoreRequestItem, item_id).state == "done"
            assert giveup_agreement_violations(session) == []


def test_rlr_delayed_source_open_renews_before_first_chunk(
    fresh_rig: _Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Proof 2: source opening may outlast acquisition's lease."""

    rig = fresh_rig
    item_id = _seed_item(rig, "rlr-open", _pl("rlr-open"), receiver="receiver")
    with _rlr_service(rig) as service:
        original = service._open_prepared_chunks

        @contextmanager
        def delayed(prepared: Any) -> Iterator[Iterator[bytes]]:
            time.sleep(0.75)
            with original(prepared) as chunks:
                yield chunks

        monkeypatch.setattr(service, "_open_prepared_chunks", delayed)
        stream = service.OpenRestore(
            restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), rig.context("receiver")
        )
        try:
            head = _rlr_first_chunk(stream)[0].manifest_head
            assert _rlr_lease(rig, item_id).expires_at.replace(tzinfo=dt.UTC) > dt.datetime.now(
                dt.UTC
            )
            # _receive needs the manifest head; acknowledge this remainder explicitly.
            _pull_until_file_end(stream)
            service.CommitRestore(
                _commit_request(item_id, head.manifest_sha256, head.lease_token, 1),
                rig.context("receiver"),
            )
            assert _kinds(list(stream)) == ["job_end"]
            assert _attempt_rows(rig, item_id)[0].cause is None
        finally:
            stream.close()  # type: ignore[attr-defined]


@pytest.mark.parametrize("action", ["resume", "cancel_live", "cancel_expired", "recovery"])
def test_rlr_pause_resume_cancel_and_recovery(fresh_rig: _Rig, action: str) -> None:
    """Proofs 3/5/7/7b: pulling renews; a stopped receiver and recovery retain their meanings."""

    rig = fresh_rig
    item_id = _seed_item(rig, "rlr-pause", b"p" * (8 * 256 * 1024), receiver="receiver")
    with _rlr_service(rig) as service:
        stream = service.OpenRestore(
            restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), rig.context("receiver")
        )
        head = _rlr_first_chunk(stream)[0].manifest_head
        time.sleep(0.75)
        if action == "recovery":
            (before,) = _attempt_rows(rig, item_id)
            _close_attempt(rig, before.id)
            closed = _attempt_rows(rig, item_id)[0]
            with patch.object(service, "_end_attempt", wraps=service._end_attempt) as end_attempt:
                with pytest.raises(_Abort) as fenced:
                    next(stream)
                assert fenced.value.code == grpc.StatusCode.UNAVAILABLE
                assert fenced.value.details == "restore attempt was closed by recovery"
                end_attempt.assert_not_called()
            (after,) = _attempt_rows(rig, item_id)
            assert (after.end_kind, after.ended_at, after.closed_at) == (
                "lost",
                None,
                closed.closed_at,
            )
            assert service._registry == {}
            return
        if action == "cancel_expired":
            stream.close()  # type: ignore[attr-defined]
        else:
            assert next(stream).HasField("chunk")
            assert _rlr_lease(rig, item_id).expires_at.replace(tzinfo=dt.UTC) > dt.datetime.now(
                dt.UTC
            )
            if action == "cancel_live":
                stream.close()  # type: ignore[attr-defined]
            else:
                _pull_until_file_end(stream)
                service.CommitRestore(
                    _commit_request(item_id, head.manifest_sha256, head.lease_token, 1),
                    rig.context("receiver"),
                )
                assert _kinds(list(stream)) == ["job_end"]
        (row,) = _attempt_rows(rig, item_id)
        expected = {
            "resume": ("job_end", None, "sent"),
            "cancel_live": ("cancelled", None, "queued"),
            "cancel_expired": ("cancelled", "lease_expired", "failed"),
        }[action]
        assert (row.end_kind, row.cause) == expected[:2]
        with session_scope(rig.engine) as session:
            item = session.get(RestoreRequestItem, item_id)
            assert item.state == expected[2]
            if action == "cancel_expired":
                assert item.detail == _LEASE_EXPIRED_SENTENCE
            assert giveup_agreement_violations(session) == []


@pytest.mark.parametrize("ending", ["complete", "close"])
def test_rlr_transient_renewal_errors_retry_and_complete(
    fresh_rig: _Rig,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    ending: str,
) -> None:
    """Proof 4: spaced retries renew the row and clear failure even on a later close."""

    rig = fresh_rig
    item_id = _seed_item(rig, "rlr-errors", b"e" * (8 * 256 * 1024), receiver="receiver")
    with _rlr_service(rig) as service:
        original = service._renew_lease
        calls = []
        before_success = []

        def flaky(prepared: Any, lease: Any) -> bool:
            calls.append(time.monotonic())
            if len(calls) == 1:
                raise OperationalError("renew", {}, RuntimeError("database is locked"))
            if len(calls) == 2:
                raise RuntimeError("non-lock write error")
            if len(calls) == 3:
                before_success.append(_rlr_lease(rig, item_id).updated_at)
            return original(prepared, lease)

        monkeypatch.setattr(service, "_renew_lease", flaky)
        stream = service.OpenRestore(
            restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), rig.context("receiver")
        )
        frames = []
        for frame in _receive(
            stream, lambda ack: service.CommitRestore(ack, rig.context("receiver"))
        ):
            frames.append(frame)
            if frame.HasField("chunk"):
                if len(calls) == 3:
                    assert len(before_success) == 1
                    assert _rlr_lease(rig, item_id).updated_at > before_success[0]
                    if ending == "close":
                        stream.close()  # type: ignore[attr-defined]
                        break
                time.sleep(0.025)
        if ending == "complete":
            assert frames[-1].HasField("job_end")
        else:
            assert frames[-1].HasField("chunk")
            assert len(calls) == 3
        assert len(calls) >= 3
        assert all(0.02 <= b - a < 0.1 for a, b in pairwise(calls[:3]))
        assert (
            sum(
                record.levelname == "WARNING" and "lease renewal failed" in record.message
                for record in caplog.records
            )
            == 2
        )
        assert (_attempt_rows(rig, item_id)[0].end_kind, _attempt_rows(rig, item_id)[0].cause) == (
            "job_end" if ending == "complete" else "cancelled",
            None,
        )


@pytest.mark.parametrize("ending", ["close", "wait", "staged", "recover"])
def test_rlr_outstanding_renewal_is_server_failure_even_with_staged(
    fresh_rig: _Rig,
    monkeypatch: pytest.MonkeyPatch,
    ending: str,
) -> None:
    """Proof 4a/b/c: a pending write owns the failure and holds an acknowledgement."""

    rig = fresh_rig
    item_id = _seed_item(rig, "rlr-outstanding", b"o" * (8 * 256 * 1024), receiver="receiver")
    with _rlr_service(rig) as service:
        original = service._renew_lease
        calls = []
        in_wait = [False]
        staged = []

        def failing(prepared: Any, lease: Any) -> bool:
            calls.append(time.monotonic())
            if in_wait[0] and ending in {"staged", "recover"} and not staged:
                service.CommitRestore(
                    _commit_request(item_id, head.manifest_sha256, head.lease_token, 1),
                    rig.context("receiver"),
                )
                staged.append(time.monotonic())
                # Hold this failed write past expiry. The next poll sees a
                # durable acknowledgement and an expired row together.
                time.sleep(0.75)
            elif ending == "recover" and staged:
                return original(prepared, lease)
            raise RuntimeError("renewal write unavailable")

        monkeypatch.setattr(service, "_renew_lease", failing)
        context = rig.context("receiver")
        stream = service.OpenRestore(
            restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), context
        )
        head = _rlr_first_chunk(stream)[0].manifest_head
        if ending == "close":
            time.sleep(0.75)
            stream.close()  # type: ignore[attr-defined]
        else:
            _pull_until_file_end(stream)
            expiry = _rlr_lease(rig, item_id).expires_at.replace(tzinfo=dt.UTC)
            in_wait[0] = True
            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = executor.submit(list, stream)
                try:
                    with pytest.raises(TimeoutError):
                        pending.result(timeout=0.7)
                    if ending == "recover":
                        assert _kinds(pending.result(timeout=2)) == ["job_end"]
                    else:
                        with pytest.raises(_Abort) as failed:
                            pending.result(timeout=2)
                        assert failed.value.code == grpc.StatusCode.UNAVAILABLE
                        assert (
                            failed.value.details == "the server could not renew the delivery lease"
                        )
                        assert dt.datetime.now(dt.UTC) > expiry + service.config.lease_duration
                finally:
                    context.active = False
            assert all(b - a >= 0.02 for a, b in pairwise(calls))
            if ending in {"staged", "recover"}:
                assert len(staged) == 1
        (row,) = _attempt_rows(rig, item_id)
        if ending == "recover":
            assert (row.end_kind, row.cause) == ("job_end", None)
        else:
            assert (row.end_kind, row.cause, row.message) == (
                "error",
                "server_error",
                "the server could not renew the delivery lease",
            )
        with session_scope(rig.engine) as session:
            assert session.get(RestoreRequestItem, item_id).state == (
                "sent" if ending == "recover" else "queued"
            )
            assert giveup_agreement_violations(session) == []


@pytest.mark.parametrize("reconciled", [False, True])
def test_rlr_revealed_after_expiry_finishes_sent(fresh_rig: _Rig, reconciled: bool) -> None:
    """Proof 9: the current generation confirms a revealed file even after expiry."""

    rig = fresh_rig
    item_id = _seed_item(rig, "rlr-revealed", _pl("rlr-revealed"), receiver="receiver")
    with _rlr_service(rig) as service:
        frames, error, _ = _open_collect(service, item_id, rig.context("receiver"))
        assert error is None
        assert frames[-1].HasField("job_end")
        head = frames[0].manifest_head
        _expire_lease(rig, item_id)
        with session_scope(rig.engine) as session:
            if reconciled:
                assert reconcile(session, restore_open_reconciler.DOMAIN, cursor=item_id - 1) == (
                    1,
                    1,
                )
            item = session.get(RestoreRequestItem, item_id)
            assert item.state == "sent"
            if reconciled:
                assert item.detail == restore_open_reconciler.REOPENABLE_DETAIL
        lease_before = _rlr_lease(rig, item_id)
        request = _commit_request(item_id, head.manifest_sha256, head.lease_token, 1, revealed=True)
        reply = service.CommitRestore(request, rig.context("receiver"))
        assert (reply.status, reply.committed_index, reply.revealed) == ("revealed", 1, True)
        assert _checkpoint_of(rig, item_id) == (1, True)
        with session_scope(rig.engine) as session:
            item = session.get(RestoreRequestItem, item_id)
            assert (item.state, item.detail, item.request.state) == ("done", None, "completed")
            assert giveup_agreement_violations(session) == []
        lease_after = _rlr_lease(rig, item_id)
        assert (lease_after.generation, lease_after.expires_at, lease_after.updated_at) == (
            lease_before.generation,
            lease_before.expires_at,
            lease_before.updated_at,
        )
        assert service.CommitRestore(request, rig.context("receiver")).status == "already_done"


def _rlr_commit_snapshot(rig: _Rig, item_id: int) -> tuple[Any, ...]:
    """Capture every commit target and the lease to prove a refused commit writes nothing."""

    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        checkpoint = item.checkpoint
        lease = item.open_session
        return tuple(
            tuple(getattr(row, column.key) for column in row.__table__.columns)
            for row in (item, item.request, checkpoint, lease)
        )


@pytest.mark.parametrize("case", ["staged", "streaming_revealed", "old_generation"])
def test_rlr_expiry_preserves_commit_guards(fresh_rig: _Rig, case: str) -> None:
    """Proof 10: expired STAGED, pre-sent REVEALED and superseded tokens stay refused."""

    rig = fresh_rig
    item_id = _seed_item(rig, "rlr-guards", _pl("rlr-guards"), receiver="receiver")
    with _rlr_service(rig) as service:
        stream = service.OpenRestore(
            restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), rig.context("receiver")
        )
        try:
            if case == "old_generation":
                frames = list(
                    _receive(
                        stream, lambda ack: service.CommitRestore(ack, rig.context("receiver"))
                    )
                )
                head = frames[0].manifest_head
                _expire_lease(rig, item_id)
                stream = service.OpenRestore(
                    restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                    rig.context("receiver"),
                )
                new_head = next(stream).manifest_head
                assert new_head.lease_token.generation == head.lease_token.generation + 1
            else:
                head = _pull_until_file_end(stream)[0].manifest_head
                _expire_lease(rig, item_id)
            before = _rlr_commit_snapshot(rig, item_id)
            with pytest.raises(_Abort) as refused:
                service.CommitRestore(
                    _commit_request(
                        item_id,
                        head.manifest_sha256,
                        head.lease_token,
                        1,
                        revealed=case != "staged",
                    ),
                    rig.context("receiver"),
                )
            assert refused.value.code == grpc.StatusCode.FAILED_PRECONDITION
            assert (
                refused.value.details
                == {
                    "staged": "restore lease has expired",
                    "streaming_revealed": "restore item has not reached sent",
                    "old_generation": "manifest, receiver, or lease generation does not match",
                }[case]
            )
            assert _rlr_commit_snapshot(rig, item_id) == before
            assert _checkpoint_of(rig, item_id) == (0, False)
        finally:
            stream.close()  # type: ignore[attr-defined]
        with session_scope(rig.engine) as session:
            assert giveup_agreement_violations(session) == []


def test_rlr_revealed_between_reopen_admission_and_acquisition_wins(fresh_rig: _Rig) -> None:
    """Proof 10's race: confirmation wins, and the pending reopen rolls back its lease CAS."""

    rig = fresh_rig
    item_id = _seed_item(rig, "rlr-reopen-race", _pl("rlr-reopen-race"), receiver="receiver")
    with _rlr_service(rig) as service:
        frames, error, _ = _open_collect(service, item_id, rig.context("receiver"))
        assert error is None
        head = frames[0].manifest_head
        _expire_lease(rig, item_id)
        before = _rlr_lease(rig, item_id)

        def reveal(admitted_id: int) -> None:
            assert admitted_id == item_id
            assert len(_attempt_rows(rig, item_id)) == 2
            assert _attempt_rows(rig, item_id)[-1].lease_generation is None
            reply = service.CommitRestore(
                _commit_request(item_id, head.manifest_sha256, head.lease_token, 1, revealed=True),
                rig.context("receiver"),
            )
            assert reply.status == "revealed"

        reopened = _started(_HookService(service.config, before_lease=reveal))
        try:
            frames, error, _ = _open_collect(reopened, item_id, rig.context("receiver"))
        finally:
            reopened.stop()
        assert frames == []
        assert isinstance(error, RuntimeError)
        assert str(error) == "leased restore item ceased to be streamable"
        delivered, withdrawn = _attempt_rows(rig, item_id)
        assert (delivered.end_kind, delivered.cause) == ("job_end", None)
        assert (withdrawn.end_kind, withdrawn.cause, withdrawn.lease_generation) == (
            "error",
            "item_withdrawn",
            None,
        )
        assert _checkpoint_of(rig, item_id) == (1, True)
        after = _rlr_lease(rig, item_id)
        assert (after.generation, after.expires_at, after.updated_at) == (
            before.generation,
            before.expires_at,
            before.updated_at,
        )
        with session_scope(rig.engine) as session:
            item = session.get(RestoreRequestItem, item_id)
            assert (item.state, item.detail, item.request.state) == ("done", None, "completed")
            assert giveup_agreement_violations(session) == []


def _make_tape_only(rig: _Rig, suffix: str) -> None:
    with session_scope(rig.engine) as session:
        backend = session.scalar(select(Backend).where(Backend.name == f"disk-{suffix}"))
        assert backend is not None
        backend.kind = BackendKind.REM_TAPE


def _corrupt_archive(rig: _Rig, suffix: str) -> None:
    rig.backend.paths[f"object-{suffix}"].write_bytes(b"archive-prefix" + b"corrupted data")


def _set_item(rig: _Rig, item_id: int, **values: Any) -> None:
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        for name, value in values.items():
            setattr(item, name, value)


def _raising_chunks(
    make: Callable[[], BaseException],
) -> Callable[[Iterator[bytes]], Iterator[bytes]]:
    def chunks(_real: Iterator[bytes]) -> Iterator[bytes]:
        raise make()
        yield b""  # a generator that raises on its first pull

    return chunks


_Context_factory = Callable[[str], _Context]
_Driver = Callable[
    [_Rig, pytest.MonkeyPatch, _Context_factory, str], tuple[int, list[Any], Any, RestoreService]
]


@dataclass(frozen=True, kw_only=True)
class _ExitCase:
    """One row of B3's exit table, with the driver that reaches it."""

    drive: _Driver
    end_kind: str
    cause: str | None
    raises: type[BaseException] | None = _Abort
    code: grpc.StatusCode | None = grpc.StatusCode.FAILED_PRECONDITION
    blocked: str = "none"  # none | future | past
    opened_from: str = "queued"
    prior_rows: int = 0
    message_length: int | None = None  # the stored message's exact length, when pinned


def _with_hooks(rig: _Rig, **hooks: Any) -> _HookService:
    return _started(_HookService(rig.service_config, **hooks))  # type: ignore[return-value]


def _d_capacity(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    first_id = _seed_item(
        rig, f"{tag}-capacity-first", _pl(f"{tag}-capacity-first"), receiver="receiver"
    )
    second_id = _seed_item(
        rig, f"{tag}-capacity-second", _pl(f"{tag}-capacity-second"), receiver="receiver"
    )
    service = _started(RestoreService(replace(rig.service_config, max_concurrent_streams=1)))
    holder_context = ctx("receiver")
    try:
        holder = service.OpenRestore(
            restore_pb2.OpenRestoreRequest(restore_request_item_id=first_id), holder_context
        )
        assert next(holder).HasField("manifest_head")
        frames, exc, _ = _open_collect(service, second_id, ctx("receiver"))
        holder_context.active = False
        with pytest.raises(grpc.RpcError):
            list(holder)
    finally:
        service.stop()
    return second_id, frames, exc, service


def _d_destination(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(
        rig,
        f"{tag}-destination",
        _pl(f"{tag}-destination"),
        receiver="receiver",
        destination_id="nowhere",
    )
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))


def _d_can_restore(rig: _Rig, mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(rig, f"{tag}-can-restore", _pl(f"{tag}-can-restore"), receiver="receiver")
    mp.setitem(rig.capabilities, "ada", frozenset())
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))


def _d_privacy_capability(rig: _Rig, mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(
        rig, f"{tag}-privacy", _pl(f"{tag}-privacy"), receiver="receiver", privacy_level="p2"
    )
    mp.setitem(rig.capabilities, "ada", frozenset({"can_restore"}))
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))


def _d_privacy_unmapped(rig: _Rig, mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(
        rig, f"{tag}-unmapped", _pl(f"{tag}-unmapped"), receiver="receiver", privacy_level="p2"
    )
    mp.setattr(
        "sutradhara.grpc.restore_service.hdcache_privacy_capability_map_from_env", lambda: {}
    )
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))


def _d_privacy_map_raises(rig: _Rig, mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(
        rig, f"{tag}-map-raises", _pl(f"{tag}-map-raises"), receiver="receiver", privacy_level="p2"
    )

    def boom() -> dict[str, str]:
        raise ArtifactClassPolicyError(
            "SUTRADHARA_HDCACHE_PRIVACY_CAPABILITIES must be a JSON object"
        )

    mp.setattr("sutradhara.grpc.restore_service.hdcache_privacy_capability_map_from_env", boom)
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))


def _d_capabilities_raise(rig: _Rig, mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(rig, f"{tag}-caps-raise", _pl(f"{tag}-caps-raise"), receiver="receiver")

    def boom(_operator: str) -> frozenset[str]:
        raise RuntimeError("the directory is down")

    mp.setattr(rig.service._live, "capabilities_for", boom)
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))


def _d_incomplete(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(rig, f"{tag}-incomplete", _pl(f"{tag}-incomplete"), receiver="receiver")
    _set_item(rig, item_id, final_rel_path=None)
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))


def _d_bad_path(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(rig, f"{tag}-bad-path", _pl(f"{tag}-bad-path"), receiver="receiver")
    _set_item(rig, item_id, final_rel_path="../escape.bin")
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))


def _d_manifest_changed(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(
        rig, f"{tag}-manifest-changed", _pl(f"{tag}-manifest-changed"), receiver="receiver"
    )
    with session_scope(rig.engine) as session:
        session.add(
            RestoreItemCheckpoint(
                restore_request_item_id=item_id,
                manifest_sha256=b"\x01" * 32,
                committed_index=0,
                revealed=False,
            )
        )
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))


def _d_resume_mismatch(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(
        rig, f"{tag}-resume-mismatch", _pl(f"{tag}-resume-mismatch"), receiver="receiver"
    )
    resume = restore_pb2.ResumeToken(
        restore_request_item_id=item_id, manifest_sha256=hashlib.sha256(b"other").digest()
    )
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"), resume_token=resume)


def _d_token_mismatch(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(
        rig, f"{tag}-token-mismatch", _pl(f"{tag}-token-mismatch"), receiver="receiver"
    )
    token = restore_pb2.LeaseToken(
        restore_request_item_id=item_id,
        receiver_device_id="receiver",
        manifest_sha256=b"\x02" * 32,
        generation=1,
    )
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"), lease_token=token)


def _d_stale_generation(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(
        rig, f"{tag}-stale-generation", _pl(f"{tag}-stale-generation"), receiver="receiver"
    )
    head = _deliver(rig, item_id)
    token = restore_pb2.LeaseToken()
    token.CopyFrom(head.lease_token)
    token.generation = 7
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"), lease_token=token)


def _d_resume_without_lease(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    suffix = f"{tag}-protocol"
    payload = _pl(suffix)
    item_id = _seed_item(rig, suffix, payload, receiver="receiver")
    digest = _reference_manifest_digest(
        [(f"exports/{suffix}.bin", len(payload), hashlib.sha256(payload).digest())]
    )
    resume = restore_pb2.ResumeToken(restore_request_item_id=item_id, manifest_sha256=digest)
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"), resume_token=resume)


def _d_cache_disk_unavailable(rig: _Rig, mp: Any, ctx: _Context_factory, tag: str) -> Any:
    suffix = f"{tag}-cache-disk"
    payload = _pl(suffix)
    item_id = _seed_item(rig, suffix, payload, receiver="receiver")
    _seed_cache_entry(rig, item_id, payload)
    _make_tape_only(rig, suffix)
    mp.setattr("sutradhara.hdcache.manager._probe_cache_disk_recovery", lambda *a, **k: False)
    _set_cache_disk_state(rig, "absent")
    try:
        return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))
    finally:
        _set_cache_disk_state(rig, "active")


def _d_tape_only(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    suffix = f"{tag}-tape-only"
    item_id = _seed_item(rig, suffix, _pl(suffix), receiver="receiver")
    _make_tape_only(rig, suffix)
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))


def _d_source_integrity_probe(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    suffix = f"{tag}-probe-integrity"
    item_id = _seed_item(rig, suffix, _pl(suffix), receiver="receiver")
    _corrupt_archive(rig, suffix)
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))


def _d_source_unavailable_probe(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(
        rig, f"{tag}-probe-unavailable", _pl(f"{tag}-probe-unavailable"), receiver="receiver"
    )
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.size_bytes is not None
        item.size_bytes += 1
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))


def _d_lease_live_still_live(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(rig, f"{tag}-678-live", _pl(f"{tag}-678-live"), receiver="receiver")
    head = _deliver(rig, item_id)
    return item_id, *_open_collect(
        rig.service, item_id, ctx("receiver"), lease_token=head.lease_token
    )


def _d_stale_lease_moved(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(rig, f"{tag}-678-moved", _pl(f"{tag}-678-moved"), receiver="receiver")
    head = _deliver(rig, item_id)

    def bump(moved_id: int) -> None:
        with session_scope(rig.engine) as session:
            lease = session.get(RestoreOpenSession, moved_id)
            assert lease is not None
            lease.generation += 1

    service = _with_hooks(rig, before_lease=bump)
    try:
        return item_id, *_open_collect(
            service, item_id, ctx("receiver"), lease_token=head.lease_token
        )
    finally:
        service.stop()


def _d_lease_live_expired_between(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(rig, f"{tag}-678-expired", _pl(f"{tag}-678-expired"), receiver="receiver")
    head = _deliver(rig, item_id)
    # The race inside one call: the lease expires between the CAS and the re-read.
    service = _with_hooks(rig, lease_row=lambda row: (row[0], row[1] - dt.timedelta(hours=1)))
    try:
        return item_id, *_open_collect(
            service, item_id, ctx("receiver"), lease_token=head.lease_token
        )
    finally:
        service.stop()


def _d_lease_live_698(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(rig, f"{tag}-698", _pl(f"{tag}-698"), receiver="receiver")
    _deliver(rig, item_id)
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))


def _d_item_withdrawn(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    # K6's driver: the item leaves the openable states between ``_prepare_open``
    # and the lease transaction, whose item update then matches no row.
    item_id = _seed_item(rig, f"{tag}-withdrawn", _pl(f"{tag}-withdrawn"), receiver="receiver")
    service = _with_hooks(
        rig, before_lease=lambda withdrawn_id: _set_item(rig, withdrawn_id, state="failed")
    )
    try:
        return item_id, *_open_collect(service, item_id, ctx("receiver"))
    finally:
        service.stop()


def _d_in_stream(make: Callable[[], BaseException] | None, name: str) -> _Driver:
    def drive(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
        item_id = _seed_item(
            rig, f"{tag}-stream-{name}", _pl(f"{tag}-stream-{name}"), receiver="receiver"
        )
        chunks = _raising_chunks(make) if make is not None else (lambda _real: iter([b"x"]))
        service = _with_hooks(rig, chunks=chunks)
        try:
            return item_id, *_open_collect(service, item_id, ctx("receiver"))
        finally:
            service.stop()

    return drive


def _d_delivered(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(rig, f"{tag}-delivered", _pl(f"{tag}-delivered"), receiver="receiver")
    return item_id, *_open_collect(rig.service, item_id, ctx("receiver"))


def _d_revealed_replay(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(rig, f"{tag}-replay", _pl(f"{tag}-replay"), receiver="receiver")
    head = _deliver(rig, item_id)
    rig.service.CommitRestore(
        _commit_request(item_id, head.manifest_sha256, head.lease_token, 1, revealed=True),
        rig.context("receiver"),
    )
    # No slot: a one-slot service whose slot another stream holds still replays.
    holder_id = _seed_item(
        rig, f"{tag}-replay-holder", _pl(f"{tag}-replay-holder"), receiver="receiver"
    )
    service = _started(RestoreService(replace(rig.service_config, max_concurrent_streams=1)))
    holder_context = ctx("receiver")
    try:
        holder = service.OpenRestore(
            restore_pb2.OpenRestoreRequest(restore_request_item_id=holder_id), holder_context
        )
        assert next(holder).HasField("manifest_head")
        frames, exc, _ = _open_collect(service, item_id, ctx("receiver"))
        assert [frame.WhichOneof("payload") for frame in frames] == ["job_end"]
        holder_context.active = False
        with pytest.raises(grpc.RpcError):
            list(holder)
    finally:
        service.stop()
    return item_id, frames, exc, service


def _d_cancelled_inactive(rig: _Rig, _mp: Any, ctx: _Context_factory, tag: str) -> Any:
    item_id = _seed_item(rig, f"{tag}-inactive", _pl(f"{tag}-inactive"), receiver="receiver")
    context = ctx("receiver")
    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), context
    )
    frames = [next(stream)]
    context.active = False
    try:
        frames.extend(stream)
    except BaseException as exc:
        return item_id, frames, exc, rig.service
    return item_id, frames, None, rig.service


_DENIED = grpc.StatusCode.PERMISSION_DENIED
_LONG_TEXT = "x" * 3000
_EXIT_CASES: dict[str, _ExitCase] = {
    "server_error_in_stream_value_error": _ExitCase(
        drive=_d_in_stream(lambda: ValueError(_LONG_TEXT), "valueerror"),
        end_kind="error",
        cause="server_error",
        raises=ValueError,
        code=None,
        message_length=2000,
    ),
    "source_unavailable_in_stream_cache_serve_failed": _ExitCase(
        drive=_d_in_stream(
            lambda: CacheServeFailed("io", "cache read failed mid-stream", mark_lost=False), "csf"
        ),
        end_kind="error",
        cause="source_unavailable",
        raises=None,
        code=None,
    ),
    "capacity": _ExitCase(
        drive=_d_capacity,
        end_kind="error",
        cause="capacity",
        code=grpc.StatusCode.RESOURCE_EXHAUSTED,
    ),
    "not_authorized_destination": _ExitCase(
        drive=_d_destination, end_kind="error", cause="not_authorized", code=_DENIED
    ),
    "not_authorized_can_restore": _ExitCase(
        drive=_d_can_restore, end_kind="error", cause="not_authorized", code=_DENIED
    ),
    "not_authorized_privacy": _ExitCase(
        drive=_d_privacy_capability, end_kind="error", cause="not_authorized", code=_DENIED
    ),
    "configuration_unmapped": _ExitCase(
        drive=_d_privacy_unmapped, end_kind="error", cause="configuration"
    ),
    "configuration_map_raises": _ExitCase(
        drive=_d_privacy_map_raises, end_kind="error", cause="configuration"
    ),
    "configuration_capabilities_raise": _ExitCase(
        drive=_d_capabilities_raise, end_kind="error", cause="configuration"
    ),
    "manifest_invalid_incomplete": _ExitCase(
        drive=_d_incomplete, end_kind="error", cause="manifest_invalid"
    ),
    "manifest_invalid_path": _ExitCase(
        drive=_d_bad_path, end_kind="error", cause="manifest_invalid"
    ),
    "manifest_invalid_changed": _ExitCase(
        drive=_d_manifest_changed, end_kind="error", cause="manifest_invalid"
    ),
    "resume_mismatch": _ExitCase(
        drive=_d_resume_mismatch, end_kind="error", cause="resume_mismatch"
    ),
    "stale_lease_token_mismatch": _ExitCase(
        drive=_d_token_mismatch, end_kind="error", cause="stale_lease"
    ),
    "stale_lease_generation": _ExitCase(
        drive=_d_stale_generation,
        end_kind="error",
        cause="stale_lease",
        opened_from="sent",
        prior_rows=1,
    ),
    "protocol_resume_without_lease": _ExitCase(
        drive=_d_resume_without_lease, end_kind="error", cause="protocol"
    ),
    "cache_disk_unavailable": _ExitCase(
        drive=_d_cache_disk_unavailable, end_kind="error", cause="cache_disk_unavailable"
    ),
    "tape_only": _ExitCase(drive=_d_tape_only, end_kind="error", cause="tape_only"),
    "source_integrity_probe": _ExitCase(
        drive=_d_source_integrity_probe, end_kind="error", cause="source_integrity"
    ),
    "source_unavailable_probe": _ExitCase(
        drive=_d_source_unavailable_probe, end_kind="error", cause="source_unavailable"
    ),
    "lease_live_678_still_live": _ExitCase(
        drive=_d_lease_live_still_live,
        end_kind="error",
        cause="lease_live",
        blocked="future",
        opened_from="sent",
        prior_rows=1,
    ),
    "stale_lease_678_generation_moved": _ExitCase(
        drive=_d_stale_lease_moved,
        end_kind="error",
        cause="stale_lease",
        opened_from="sent",
        prior_rows=1,
    ),
    "lease_live_678_expired_between": _ExitCase(
        drive=_d_lease_live_expired_between,
        end_kind="error",
        cause="lease_live",
        blocked="past",
        opened_from="sent",
        prior_rows=1,
    ),
    "lease_live_698": _ExitCase(
        drive=_d_lease_live_698,
        end_kind="error",
        cause="lease_live",
        code=grpc.StatusCode.ALREADY_EXISTS,
        blocked="future",
        opened_from="sent",
        prior_rows=1,
    ),
    "item_withdrawn_lease_item_update": _ExitCase(
        drive=_d_item_withdrawn,
        end_kind="error",
        cause="item_withdrawn",
        raises=RuntimeError,
        code=None,
    ),
    "source_integrity_in_stream_frame": _ExitCase(
        drive=_d_in_stream(lambda: LogicalMemberIntegrityError("member hash mismatch"), "lmi"),
        end_kind="error",
        cause="source_integrity",
        raises=None,
        code=None,
    ),
    "source_unavailable_in_stream_frame": _ExitCase(
        drive=_d_in_stream(lambda: RestoreSourceUnavailable("the copy vanished"), "rsu"),
        end_kind="error",
        cause="source_unavailable",
        raises=None,
        code=None,
    ),
    "source_integrity_size_mismatch": _ExitCase(
        drive=_d_in_stream(None, "short"),
        end_kind="error",
        cause="source_integrity",
        raises=RestoreSourceUnavailable,
        code=None,
    ),
    "source_unavailable_in_stream_oserror": _ExitCase(
        drive=_d_in_stream(lambda: OSError("the disk fell off"), "oserror"),
        end_kind="error",
        cause="source_unavailable",
        raises=OSError,
        code=None,
    ),
    "source_integrity_in_stream_chunk": _ExitCase(
        drive=_d_in_stream(lambda: ChunkRestoreIntegrityError("chunk hash mismatch"), "chunk"),
        end_kind="error",
        cause="source_integrity",
        raises=ChunkRestoreIntegrityError,
        code=None,
    ),
    "job_end_delivered": _ExitCase(
        drive=_d_delivered, end_kind="job_end", cause=None, raises=None, code=None
    ),
    "job_end_revealed_replay": _ExitCase(
        drive=_d_revealed_replay,
        end_kind="job_end",
        cause=None,
        raises=None,
        code=None,
        opened_from="done",
        prior_rows=1,
    ),
    "cancelled_client_inactive": _ExitCase(
        drive=_d_cancelled_inactive,
        end_kind="cancelled",
        cause=None,
        raises=grpc.RpcError,
        code=None,
    ),
}


def _check_exit_case(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch, name: str, ctx: _Context_factory, *, tag: str
) -> None:
    case = _EXIT_CASES[name]
    item_id, _frames, exc, service = case.drive(rig, monkeypatch, ctx, tag)
    rows = _attempt_rows(rig, item_id)
    assert len(rows) == case.prior_rows + 1, [(r.end_kind, r.cause) for r in rows]
    row = rows[-1]
    assert (row.end_kind, row.cause) == (case.end_kind, case.cause)
    # An attempt that ended normally has left the registry (B4).
    assert row.id not in service._registry
    assert row.message is None or len(row.message) <= 2000
    if case.message_length is not None:
        assert row.message is not None
        assert len(row.message) == case.message_length
    assert row.opened_from_state == case.opened_from
    assert row.ended_at is not None
    assert row.closed_at is None
    assert row.receiver_device_id == "receiver"
    assert row.server_instance_id is not None
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    if case.blocked == "none":
        assert row.blocked_until is None
    elif case.blocked == "future":
        assert row.blocked_until is not None
        assert row.blocked_until > now
    else:
        assert row.blocked_until is not None
        assert row.blocked_until <= now
    if case.raises is None:
        assert exc is None, exc
        return
    assert exc is not None
    if case.raises is _Abort and tag == "e2":
        assert type(exc) is Exception  # production's bare abort
        return
    assert isinstance(exc, case.raises), exc
    if isinstance(exc, _Abort):
        assert exc.code == case.code


@pytest.mark.parametrize("name", sorted(_EXIT_CASES))
def test_e1_every_admitted_call_writes_one_row_with_its_outcome(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    _check_exit_case(rig, monkeypatch, name, rig.context, tag="e1")


@pytest.mark.parametrize("name", sorted(_EXIT_CASES))
def test_e2_the_cell_is_type_free_under_a_bare_exception_abort(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    _check_exit_case(
        rig,
        monkeypatch,
        name,
        lambda device: _BareContext(device, rig.fingerprints[device]),
        tag="e2",
    )


def test_e3_tape_only_item_fails_at_once_with_the_owners_sentence(fresh_rig: _Rig) -> None:
    rig = fresh_rig
    item_id = _seed_item(rig, "e3-tape", _pl("e3-tape"), receiver="receiver")
    _make_tape_only(rig, "e3-tape")
    _frames, exc, _ = _open_collect(rig.service, item_id, rig.context("receiver"))
    assert isinstance(exc, _Abort)
    assert exc.code == grpc.StatusCode.FAILED_PRECONDITION
    (row,) = _attempt_rows(rig, item_id)
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "failed"
        assert item.detail == _TAPE_ONLY_SENTENCE
        assert item.request.state == "completed_with_errors"
        giveup = _giveup_of(session, item_id)
        assert giveup is not None
        assert (giveup.rule, giveup.counted, giveup.threshold) == ("terminal_cause", None, None)
        assert giveup.decisive_attempt_id == row.id
        assert giveup.code_version
        assert giveup_agreement_violations(session) == []


def test_e3_eight_counted_failures_give_up_and_seven_do_not(fresh_rig: _Rig) -> None:
    rig = fresh_rig
    item_id = _seed_item(rig, "e3-eight", _pl("e3-eight"), receiver="receiver")
    _corrupt_archive(rig, "e3-eight")
    for attempt in range(1, 9):
        if attempt > 1:
            _age_history(rig, item_id)
        _frames, exc, _ = _open_collect(rig.service, item_id, rig.context("receiver"))
        assert isinstance(exc, _Abort)
        assert exc.code == grpc.StatusCode.FAILED_PRECONDITION
        rows = _attempt_rows(rig, item_id)
        assert len(rows) == attempt
        assert (rows[-1].end_kind, rows[-1].cause) == ("error", "source_integrity")
        with session_scope(rig.engine) as session:
            item = session.get(RestoreRequestItem, item_id)
            assert item is not None
            giveup = _giveup_of(session, item_id)
            assert giveup_agreement_violations(session) == []
            if attempt < 8:
                assert item.state == "queued"
                assert giveup is None
            else:
                assert item.state == "failed"
                assert giveup is not None
                assert (giveup.rule, giveup.counted, giveup.threshold) == ("max_failures", 8, 8)
                assert giveup.decisive_attempt_id == rows[-1].id
                assert item.detail is not None
                assert item.detail.startswith("gave up after 8 failed tries: ")
                assert item.request.state == "completed_with_errors"
    # A ninth open meets the failed state before admission: no row.
    _frames, exc, _ = _open_collect(rig.service, item_id, rig.context("receiver"))
    assert isinstance(exc, _Abort)
    assert exc.details == "item is not streamable"
    assert len(_attempt_rows(rig, item_id)) == 8


def test_giveup_agreement_check_reports_a_failed_item_without_a_give_up_row(
    fresh_rig: _Rig,
) -> None:
    rig = fresh_rig
    item_id = _seed_item(rig, "agreement", _pl("agreement"), receiver="receiver")
    _set_item(rig, item_id, state="failed", detail="set by hand")
    with session_scope(rig.engine) as session:
        assert giveup_agreement_violations(session) == [
            f"item {item_id} is failed without a give-up row"
        ]


def test_e4_admission_backs_off_after_a_counted_failure_and_not_after_an_interruption(
    rig: _Rig,
) -> None:
    item_id = _seed_item(rig, "e4-backoff", _pl("e4-backoff"), receiver="receiver")
    _corrupt_archive(rig, "e4-backoff")
    _frames, exc, _ = _open_collect(rig.service, item_id, rig.context("receiver"))
    assert isinstance(exc, _Abort)
    assert exc.code == grpc.StatusCode.FAILED_PRECONDITION
    assert len(_attempt_rows(rig, item_id)) == 1
    # The second open is refused before admission: no row.
    _frames, exc, _ = _open_collect(rig.service, item_id, rig.context("receiver"))
    assert isinstance(exc, _Abort)
    assert exc.code == grpc.StatusCode.FAILED_PRECONDITION
    assert exc.details == "restore item is waiting before its next try"
    assert len(_attempt_rows(rig, item_id)) == 1
    # Aged, it is admitted again.
    _age_history(rig, item_id)
    _frames, exc, _ = _open_collect(rig.service, item_id, rig.context("receiver"))
    assert isinstance(exc, _Abort)
    assert exc.details != "restore item is waiting before its next try"
    assert len(_attempt_rows(rig, item_id)) == 2

    # A terminal row whose give-up is pending (as recovery would find it) is refused too.
    pending_id = _seed_item(rig, "e4-pending-giveup", _pl("e4-pending-giveup"), receiver="receiver")
    with session_scope(rig.engine) as session:
        now = dt.datetime.now(dt.UTC)
        session.add(
            RestoreItemAttempt(
                restore_request_item_id=pending_id,
                attempt_no=1,
                receiver_device_id="receiver",
                server_instance_id=rig.service.instance_id,
                opened_from_state="queued",
                opened_at=now,
                ended_at=now,
                end_kind="error",
                cause="tape_only",
                message="tape source deferred to RM3",
                code_version="test",
            )
        )
    _frames, exc, _ = _open_collect(rig.service, pending_id, rig.context("receiver"))
    assert isinstance(exc, _Abort)
    assert exc.code == grpc.StatusCode.FAILED_PRECONDITION
    assert exc.details == "restore item will not be retried"
    assert len(_attempt_rows(rig, pending_id)) == 1

    # An interruption is re-offered at once.
    quick_id = _seed_item(rig, "e4-interrupted", _pl("e4-interrupted"), receiver="receiver")
    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=quick_id), rig.context("receiver")
    )
    assert next(stream).HasField("manifest_head")
    stream.close()  # type: ignore[attr-defined]
    (interrupted,) = _attempt_rows(rig, quick_id)
    assert (interrupted.end_kind, interrupted.cause) == ("cancelled", None)
    frames, exc, _ = _open_collect(rig.service, quick_id, rig.context("receiver"))
    assert exc is None
    assert frames[-1].HasField("job_end")
    assert len(_attempt_rows(rig, quick_id)) == 2


def test_e5_an_end_on_a_closed_attempt_rolls_back_including_the_sent_cas(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    item_id = _seed_item(rig, "e5-closed", _pl("e5-closed"), receiver="receiver")
    statuses: list[str] = []
    real_end = rig.service._end_attempt

    def spy(entry: Any, *, deliver: bool) -> Any:
        result = real_end(entry, deliver=deliver)
        statuses.append(result.status)
        return result

    monkeypatch.setattr(rig.service, "_end_attempt", spy)
    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), rig.context("receiver")
    )
    _pull_until_job_end(stream, rig.service, rig.context("receiver"))
    (open_row,) = _attempt_rows(rig, item_id)
    assert open_row.end_kind is None
    _close_attempt(rig, open_row.id)

    with pytest.raises(_Abort) as aborted:
        next(stream)
    assert aborted.value.code == grpc.StatusCode.ABORTED
    assert aborted.value.details == "the delivery was not recorded"
    assert statuses == [END_ALREADY_CLOSED]
    (row,) = _attempt_rows(rig, item_id)
    assert (row.end_kind, row.ended_at is None, row.closed_at is not None) == ("lost", True, True)
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "streaming"  # the sent CAS rolled back with the end
    assert open_row.id not in rig.service._registry


def test_e6_a_delivery_whose_lease_expired_before_the_client_pulled_past_job_end_ends_non_ok(
    fresh_rig: _Rig,
) -> None:
    rig = fresh_rig
    item_id = _seed_item(rig, "e6-expired", _pl("e6-expired"), receiver="receiver")
    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), rig.context("receiver")
    )
    _pull_until_job_end(stream, rig.service, rig.context("receiver"))
    _expire_lease(rig, item_id)
    with pytest.raises(_Abort) as aborted:
        next(stream)
    assert aborted.value.code == grpc.StatusCode.ABORTED
    assert aborted.value.details == "the delivery was not recorded"
    (row,) = _attempt_rows(rig, item_id)
    assert (row.end_kind, row.cause) == ("job_end", "lease_expired")
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "failed"
        assert item.detail == _LEASE_EXPIRED_SENTENCE
        assert item.request.state == "completed_with_errors"
        giveup = _giveup_of(session, item_id)
        assert giveup is not None
        assert giveup.rule == "terminal_cause"
        assert giveup_agreement_violations(session) == []


def test_e7_a_failing_end_transaction_still_releases_the_slot_and_closes_the_plan(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    item_id = _seed_item(rig, "e7-guarded", _pl("e7-guarded"), receiver="receiver")
    closed: list[int] = []
    real_close = RestorePlan.close

    def counting_close(self: RestorePlan) -> None:
        closed.append(id(self))
        real_close(self)

    monkeypatch.setattr(RestorePlan, "close", counting_close)

    def boom(_entry: Any, *, deliver: bool, margin: Any = None) -> Any:
        raise RuntimeError("the database went away")

    monkeypatch.setattr(rig.service, "_end_transaction", boom)
    slots_before = rig.service._slots._value
    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), rig.context("receiver")
    )
    assert next(stream).HasField("manifest_head")
    stream.close()  # type: ignore[attr-defined]

    assert rig.service._slots._value == slots_before
    assert len(closed) == 1
    (row,) = _attempt_rows(rig, item_id)
    assert row.end_kind is None
    assert row.ended_at is None
    entry = rig.service._registry[row.id]
    assert entry.ended is True
    assert (entry.outcome.end_kind, entry.outcome.cause, entry.klass) == (
        "cancelled",
        None,
        "interrupted",
    )
    # Recovery (RSR-1e) owns this entry; drop it so the module rig stays clean.
    rig.service._registry.pop(row.id)


def test_e8_the_fence_refuses_a_call_whose_attempt_recovery_closed(rig: _Rig) -> None:
    item_id = _seed_item(rig, "e8-fence", _pl("e8-fence"), receiver="receiver")

    def close_the_attempt(fenced_id: int) -> None:
        (row,) = _attempt_rows(rig, fenced_id)
        _close_attempt(rig, row.id)

    service = _with_hooks(rig, before_lease=close_the_attempt)
    try:
        _frames, exc, _ = _open_collect(service, item_id, rig.context("receiver"))
    finally:
        service.stop()
    assert isinstance(exc, _Abort)
    assert exc.code == grpc.StatusCode.UNAVAILABLE
    assert exc.details == "restore attempt was closed by recovery"
    (row,) = _attempt_rows(rig, item_id)
    assert row.end_kind == "lost"
    assert row.ended_at is None
    assert row.lease_generation is None
    assert row.id not in service._registry
    with session_scope(rig.engine) as session:
        assert session.get(RestoreOpenSession, item_id) is None  # the lease write rolled back
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "queued"


def test_e10_closing_the_generator_on_an_active_context_records_cancelled(rig: _Rig) -> None:
    item_id = _seed_item(rig, "e10-close", _pl("e10-close"), receiver="receiver")
    context = rig.context("receiver")
    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), context
    )
    assert next(stream).HasField("manifest_head")
    assert context.is_active()
    stream.close()  # type: ignore[attr-defined]
    (row,) = _attempt_rows(rig, item_id)
    assert (row.end_kind, row.cause) == ("cancelled", None)
    assert row.id not in rig.service._registry
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "queued"
        lease = session.get(RestoreOpenSession, item_id)
        assert lease is not None
        assert lease.expires_at <= dt.datetime.now(dt.UTC).replace(tzinfo=None)


def test_rederive_request_state_sees_a_core_write_to_an_item_held_in_the_session(
    tmp_path: Path,
) -> None:
    """D6 step 4: the request is derived from the rows as written, not from stale held objects."""

    engine = make_engine(f"sqlite:///{tmp_path / 'rederive.db'}")
    create_all(engine)
    try:
        with session_scope(engine) as session:
            request = RestoreRequest(
                id="held",
                identity="ada",
                destination_id="d",
                delivery_mode="server_local",
                state="active",
            )
            for n in (1, 2):
                session.add(LogicalAsset(content_sha256=bytes([n]) * 32, size_bytes=1))
                request.items.append(
                    RestoreRequestItem(
                        content_sha256=bytes([n]) * 32, artifactclass="x", state="sent"
                    )
                )
            session.add(request)
        with session_scope(engine) as session:
            held = session.scalars(select(RestoreRequestItem)).first()
            assert held is not None
            assert len(held.request.items) == 2  # the request and both items are held
            session.connection().execute(
                sql_update(RestoreRequestItem).values(state="done", detail=None)
            )
            rederive_request_state(session, "held")
            assert held.request.state == "completed"
            assert [item.state for item in held.request.items] == ["done", "done"]
        with session_scope(engine) as session:
            stored = session.get(RestoreRequest, "held")
            assert stored is not None
            assert stored.state == "completed"
    finally:
        engine.dispose()


def _fail_tape_only(rig: _Rig, suffix: str) -> int:
    item_id = _seed_item(rig, suffix, _pl(suffix), receiver="receiver")
    _make_tape_only(rig, suffix)
    _frames, exc, _ = _open_collect(rig.service, item_id, rig.context("receiver"))
    assert isinstance(exc, _Abort)
    return item_id


def test_giveup_agreement_check_reports_a_detail_that_is_not_the_composed_text(
    fresh_rig: _Rig,
) -> None:
    rig = fresh_rig
    item_id = _fail_tape_only(rig, "agreement-detail")
    with session_scope(rig.engine) as session:
        assert giveup_agreement_violations(session) == []
    _set_item(rig, item_id, detail="edited by hand")
    with session_scope(rig.engine) as session:
        assert giveup_agreement_violations(session) == [
            f"item {item_id}: detail 'edited by hand' is not the composed text "
            f"{_TAPE_ONLY_SENTENCE!r}"
        ]


def test_giveup_agreement_check_reports_a_give_up_whose_item_is_not_failed(
    fresh_rig: _Rig,
) -> None:
    rig = fresh_rig
    item_id = _fail_tape_only(rig, "agreement-state")
    _set_item(rig, item_id, state="queued")
    with session_scope(rig.engine) as session:
        giveup = _giveup_of(session, item_id)
        assert giveup is not None
        assert giveup_agreement_violations(session) == [
            f"give-up {giveup.id}: item {item_id} is 'queued', not failed"
        ]


def test_e7_a_failing_delivery_end_aborts_and_keeps_the_entry_marked_delivered(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    item_id = _seed_item(rig, "e7-delivery", _pl("e7-delivery"), receiver="receiver")
    real_end = rig.service._end_transaction

    def boom(entry: Any, *, deliver: bool, margin: Any = None) -> Any:
        if deliver:
            raise RuntimeError("the database went away at the delivery")
        return real_end(entry, deliver=deliver, margin=margin)

    monkeypatch.setattr(rig.service, "_end_transaction", boom)
    slots_before = rig.service._slots._value
    frames, exc, _ = _open_collect(rig.service, item_id, rig.context("receiver"))
    assert frames[-1].HasField("job_end")
    assert isinstance(exc, _Abort)
    assert exc.code == grpc.StatusCode.ABORTED
    assert exc.details == "the delivery was not recorded"
    assert rig.service._slots._value == slots_before
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "streaming"
    (row,) = _attempt_rows(rig, item_id)
    assert row.end_kind is None
    entry = rig.service._registry[row.id]
    assert entry.ended is True
    assert (entry.outcome.end_kind, entry.outcome.cause, entry.klass) == (
        "job_end",
        None,
        "delivered",
    )
    # Recovery (RSR-1e) owns this entry; drop it so the module rig stays clean.
    rig.service._registry.pop(row.id)


def test_a_concurrent_admission_between_the_pre_check_and_the_insert_is_refused_without_a_row(
    rig: _Rig,
) -> None:
    item_id = _seed_item(rig, "race-insert", _pl("race-insert"), receiver="receiver")

    def another_open_wins(raced_id: int) -> None:
        with session_scope(rig.engine) as session:
            session.add(
                RestoreItemAttempt(
                    restore_request_item_id=raced_id,
                    attempt_no=1,
                    receiver_device_id="receiver",
                    server_instance_id=rig.service.instance_id,
                    opened_from_state="queued",
                    opened_at=dt.datetime.now(dt.UTC),
                    code_version="test",
                )
            )

    service = _with_hooks(rig, before_insert=another_open_wins)
    try:
        _frames, exc, _ = _open_collect(service, item_id, rig.context("receiver"))
    finally:
        service.stop()
    assert isinstance(exc, _Abort)
    assert exc.code == grpc.StatusCode.ALREADY_EXISTS
    assert exc.details == "another open of this item is in progress"
    (row,) = _attempt_rows(rig, item_id)  # only the other open's row
    assert row.end_kind is None
    assert row.code_version == "test"
    assert service._registry == {}


def test_pre_admission_refusals_for_item_id_missing_and_not_started_write_no_row(rig: _Rig) -> None:
    item_id = _seed_item(rig, "pre-admission", _pl("pre-admission"), receiver="receiver")
    rows_before = _attempt_count(rig)
    unstarted = RestoreService(rig.service_config)
    for service, requested_id, code, details in (
        (rig.service, 0, grpc.StatusCode.FAILED_PRECONDITION, "restore item id must be positive"),
        (rig.service, -1, grpc.StatusCode.FAILED_PRECONDITION, "restore item id must be positive"),
        (rig.service, 10**9, grpc.StatusCode.FAILED_PRECONDITION, "restore item is unavailable"),
        (unstarted, item_id, grpc.StatusCode.UNAVAILABLE, "restore service is not started"),
    ):
        _frames, exc, _ = _open_collect(service, requested_id, rig.context("receiver"))
        assert isinstance(exc, _Abort)
        assert (exc.code, exc.details) == (code, details)
    assert _attempt_count(rig) == rows_before
    assert _attempt_rows(rig, item_id) == []


# ---- RSR-1c: the receiver's claim, one lease transaction, the shared funnel ------


def _checkpoint_of(rig: _Rig, item_id: int) -> tuple[int, bool]:
    with session_scope(rig.engine) as session:
        checkpoint = session.get(RestoreItemCheckpoint, item_id)
        assert checkpoint is not None
        return checkpoint.committed_index, checkpoint.revealed


def _lease_of(rig: _Rig, item_id: int) -> tuple[int, dt.datetime] | None:
    with session_scope(rig.engine) as session:
        lease = session.get(RestoreOpenSession, item_id)
        if lease is None:
            return None
        return lease.generation, lease.expires_at


def _request_state(rig: _Rig, item_id: int) -> str:
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        return str(item.request.state)


def _resume_fields(item_id: int, head: Any, claim: int) -> dict[str, Any]:
    """A resume under the prior lease generation, claiming ``claim`` files held."""

    return {
        "lease_token": head.lease_token,
        "resume_token": restore_pb2.ResumeToken(
            restore_request_item_id=item_id,
            manifest_sha256=head.manifest_sha256,
            committed_index=claim,
        ),
    }


def _acknowledged(rig: _Rig, suffix: str) -> tuple[int, bytes, Any]:
    """Deliver an item and record the receiver's STAGED acknowledgement of its one file."""

    payload = _pl(suffix)
    item_id = _seed_item(rig, suffix, payload, receiver="receiver")
    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), rig.context("receiver")
    )
    frames = _pull_until_file_end(stream)
    head = frames[0].manifest_head
    staged = rig.service.CommitRestore(
        _commit_request(item_id, head.manifest_sha256, head.lease_token, 1),
        rig.context("receiver"),
    )
    assert (staged.status, staged.committed_index) == ("advanced", 1)
    frames.extend(stream)
    assert frames[-1].HasField("job_end")
    assert _checkpoint_of(rig, item_id) == (1, False)
    return item_id, payload, head


def _kinds(frames: list[Any]) -> list[str]:
    return [frame.WhichOneof("payload") for frame in frames]


def _funnel_service(
    rig: _Rig, events: list[RestoreEvent], **cache_overrides: Any
) -> RestoreService:
    """A started service whose cache config records events and has its own breaker."""

    assert rig.service_config.cache_config is not None
    cache_config = replace(
        rig.service_config.cache_config, event_sink=events.append, **cache_overrides
    )
    return _started(RestoreService(replace(rig.service_config, cache_config=cache_config)))


def test_k1_a_lost_acknowledgement_resume_claiming_zero_streams_the_file_and_lowers(
    rig: _Rig,
) -> None:
    item_id, payload, head = _acknowledged(rig, "k1-lost-ack")
    _expire_lease(rig, item_id)

    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(
            restore_request_item_id=item_id, **_resume_fields(item_id, head, 0)
        ),
        rig.context("receiver"),
    )
    frames = _pull_until_file_end(stream)
    # Observe the lowered claim before acknowledging this delivery.
    assert _checkpoint_of(rig, item_id) == (0, False)
    opened_head = frames[0].manifest_head
    rig.service.CommitRestore(
        _commit_request(item_id, opened_head.manifest_sha256, opened_head.lease_token, 1),
        rig.context("receiver"),
    )
    frames.extend(stream)
    assert _checkpoint_of(rig, item_id) == (1, False)

    assert _kinds(frames)[:4] == ["manifest_head", "manifest_entry", "manifest_end", "file_header"]
    assert frames[-1].job_end.files == 1
    assert _payload_digest(frames) == hashlib.sha256(payload).digest()
    assert frames[0].manifest_head.lease_token.generation == head.lease_token.generation + 1
    _delivered, resumed = _attempt_rows(rig, item_id)
    assert (resumed.end_kind, resumed.cause, resumed.lease_generation) == ("job_end", None, 2)


def test_k2_a_fresh_open_after_discarded_staging_streams_the_file_and_lowers(rig: _Rig) -> None:
    item_id, payload, head = _acknowledged(rig, "k2-discarded")
    _expire_lease(rig, item_id)

    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
        rig.context("receiver"),
    )
    frames = _pull_until_file_end(stream)
    # Observe the lowered claim before acknowledging this delivery.
    assert _checkpoint_of(rig, item_id) == (0, False)
    opened_head = frames[0].manifest_head
    rig.service.CommitRestore(
        _commit_request(item_id, opened_head.manifest_sha256, opened_head.lease_token, 1),
        rig.context("receiver"),
    )
    frames.extend(stream)
    assert _checkpoint_of(rig, item_id) == (1, False)

    assert "file_header" in _kinds(frames)
    assert frames[-1].job_end.files == 1
    assert _payload_digest(frames) == hashlib.sha256(payload).digest()
    assert frames[0].manifest_head.lease_token.generation == head.lease_token.generation + 1


def test_k3_a_claim_equal_to_the_stored_value_takes_the_committed_path_without_a_read(
    rig: _Rig,
) -> None:
    item_id, _payload, head = _acknowledged(rig, "k3-equal")
    rig.backend.paths["object-k3-equal"].unlink()  # no source read may happen
    _expire_lease(rig, item_id)
    archive_opens = rig.backend.open_calls

    frames = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(
                    restore_request_item_id=item_id, **_resume_fields(item_id, head, 1)
                ),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )

    assert _kinds(frames) == ["manifest_head", "manifest_entry", "manifest_end", "job_end"]
    assert frames[-1].job_end.files == 0
    assert rig.backend.open_calls == archive_opens
    assert _checkpoint_of(rig, item_id) == (1, False)
    _delivered, resumed = _attempt_rows(rig, item_id)
    # K8: the committed path runs no probe.
    assert (resumed.probe_source, resumed.probe_candidates_tried, resumed.probe_ended_at) == (
        None,
        None,
        None,
    )


def test_k3_a_claim_above_the_stored_value_is_not_adopted(rig: _Rig) -> None:
    item_id, _payload, head = _acknowledged(rig, "k3-above")
    rig.backend.paths["object-k3-above"].unlink()
    _expire_lease(rig, item_id)

    frames = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(
                    restore_request_item_id=item_id, **_resume_fields(item_id, head, 5)
                ),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )

    assert _kinds(frames) == ["manifest_head", "manifest_entry", "manifest_end", "job_end"]
    assert frames[-1].job_end.files == 0
    assert _checkpoint_of(rig, item_id) == (1, False)


def test_k3_a_claim_above_a_stored_zero_streams_the_file_and_stores_zero(rig: _Rig) -> None:
    """A claim of 1 against a stored 0 is not adopted: the plan starts from the
    server's lower value; the stored value stays 0 until the file is acknowledged."""

    payload = _pl("k3-claim-one")
    item_id = _seed_item(rig, "k3-claim-one", payload, receiver="receiver")
    context = rig.context("receiver")
    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), context
    )
    head = next(stream).manifest_head  # the lease is granted; nothing acknowledged
    # Closed while the lease is live: an interruption (``cancelled``), the item
    # returned to ``queued`` and the lease expired by the end, so it reopens at once.
    stream.close()  # type: ignore[attr-defined]
    assert _checkpoint_of(rig, item_id) == (0, False)

    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(
            restore_request_item_id=item_id, **_resume_fields(item_id, head, 1)
        ),
        rig.context("receiver"),
    )
    frames = _pull_until_file_end(stream)
    # Observe the lowered claim before acknowledging this delivery.
    assert _checkpoint_of(rig, item_id) == (0, False)
    opened_head = frames[0].manifest_head
    rig.service.CommitRestore(
        _commit_request(item_id, opened_head.manifest_sha256, opened_head.lease_token, 1),
        rig.context("receiver"),
    )
    frames.extend(stream)
    assert _checkpoint_of(rig, item_id) == (1, False)

    assert "file_header" in _kinds(frames)
    assert frames[-1].job_end.files == 1
    assert _payload_digest(frames) == hashlib.sha256(payload).digest()


class _CountingFactory:
    """Counts the sessions a service opens; delegates to the real factory."""

    def __init__(self, real: Any) -> None:
        self.real = real
        self.calls = 0
        self.begins = 0

    def __call__(self) -> Any:
        self.calls += 1
        return self.real()

    def begin(self) -> Any:
        self.begins += 1
        return self.real.begin()


class _PrepareCountingService(RestoreService):
    """Count the sessions ``_prepare_open`` opens (its own one, and no other)."""

    def __init__(self, config: RestoreServiceConfig) -> None:
        super().__init__(config)
        self.prepare_sessions: list[tuple[int, int]] = []

    def _prepare_open(self, request: Any, identity: Any, context: Any, entry: Any) -> Any:
        counting = _CountingFactory(self._factory)
        self._factory = counting  # type: ignore[assignment]
        try:
            return super()._prepare_open(request, identity, context, entry)
        finally:
            self._factory = counting.real
            self.prepare_sessions.append((counting.calls, counting.begins))


def test_the_probe_path_opens_no_second_session_during_preparation(rig: _Rig) -> None:
    """D-RSRP-14: a cache pass, a corrupt cache entry through the funnel with
    the disk fallback, and a tape-only refusal each use ``_prepare_open``'s one
    session (``_factory()`` once, ``_factory.begin()`` never)."""

    service = _started(_PrepareCountingService(rig.service_config))
    try:
        payload = _pl("no2s-cache")
        cache_id = _seed_item(rig, "no2s-cache", payload, receiver="receiver")
        _seed_cache_entry(rig, cache_id, payload)
        frames = list(
            _receive(
                service.OpenRestore(
                    restore_pb2.OpenRestoreRequest(restore_request_item_id=cache_id),
                    rig.context("receiver"),
                ),
                lambda ack: service.CommitRestore(ack, rig.context("receiver")),
            )
        )
        assert _payload_digest(frames) == hashlib.sha256(payload).digest()

        payload = _pl("no2s-corrupt")
        corrupt_id = _seed_item(rig, "no2s-corrupt", payload, receiver="receiver")
        _seed_cache_entry(rig, corrupt_id, payload).write_bytes(b"corrupt cache bytes")
        frames = list(
            _receive(
                service.OpenRestore(
                    restore_pb2.OpenRestoreRequest(restore_request_item_id=corrupt_id),
                    rig.context("receiver"),
                ),
                lambda ack: service.CommitRestore(ack, rig.context("receiver")),
            )
        )
        assert _payload_digest(frames) == hashlib.sha256(payload).digest()

        payload = _pl("no2s-tape")
        tape_id = _seed_item(rig, "no2s-tape", payload, receiver="receiver")
        _seed_cache_entry(rig, tape_id, payload).write_bytes(b"corrupt cache bytes")
        _make_tape_only(rig, "no2s-tape")
        _frames, exc, _ = _open_collect(service, tape_id, rig.context("receiver"))
        assert isinstance(exc, _Abort)
        (row,) = _attempt_rows(rig, tape_id)
        assert (row.end_kind, row.cause) == ("error", "tape_only")
    finally:
        service.stop()

    assert service.prepare_sessions == [(1, 0), (1, 0), (1, 0)]
    with session_scope(rig.engine) as session:
        for item_id in (cache_id, corrupt_id):
            row = session.scalar(
                select(RestoreItemAttempt).where(
                    RestoreItemAttempt.restore_request_item_id == item_id
                )
            )
            assert row is not None
            assert row.probe_ended_at is not None  # the evidence was committed


def test_k3_an_open_refused_for_a_live_lease_leaves_the_stored_value_unchanged(
    rig: _Rig,
) -> None:
    item_id, _payload, _head = _acknowledged(rig, "k3-live")  # the lease is live

    frames, exc, _ = _open_collect(rig.service, item_id, rig.context("receiver"))

    assert frames == []
    assert isinstance(exc, _Abort)
    assert exc.code == grpc.StatusCode.ALREADY_EXISTS
    assert _checkpoint_of(rig, item_id) == (1, False)


def test_k3_a_revealed_checkpoint_replays_unchanged_and_is_never_lowered(rig: _Rig) -> None:
    item_id, _payload, head = _acknowledged(rig, "k3-revealed")
    revealed = rig.service.CommitRestore(
        _commit_request(item_id, head.manifest_sha256, head.lease_token, 1, revealed=True),
        rig.context("receiver"),
    )
    assert revealed.revealed

    frames = list(
        _receive(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(
                    restore_request_item_id=item_id, **_resume_fields(item_id, head, 0)
                ),
                rig.context("receiver"),
            ),
            lambda ack: rig.service.CommitRestore(ack, rig.context("receiver")),
        )
    )

    assert _kinds(frames) == ["job_end"]
    assert frames[0].job_end.files == 0
    assert _checkpoint_of(rig, item_id) == (1, True)
    lease = _lease_of(rig, item_id)
    assert lease is not None
    assert lease[0] == head.lease_token.generation  # the replay takes no lease


def test_the_698_insert_race_reads_the_live_lease_in_the_same_transaction(rig: _Rig) -> None:
    """C3: a lease that went live between the pre-probe check (C4) and the
    insert makes the insert fail inside its savepoint; the live expiry is read
    in the same transaction and recorded as ``blocked_until``."""

    item_id = _seed_item(rig, "c3-698-race", _pl("c3-698-race"), receiver="receiver")
    live_until = dt.datetime.now(dt.UTC) + dt.timedelta(minutes=3)

    def another_open_leased(raced_id: int) -> None:
        with session_scope(rig.engine) as session:
            session.add(
                RestoreOpenSession(
                    restore_request_item_id=raced_id,
                    receiver_device_id="receiver",
                    manifest_sha256=b"\x02" * 32,
                    generation=1,
                    expires_at=live_until,
                )
            )

    service = _with_hooks(rig, before_lease=another_open_leased)
    try:
        frames, exc, _ = _open_collect(service, item_id, rig.context("receiver"))
    finally:
        service.stop()

    assert frames == []
    assert isinstance(exc, _Abort)
    assert (exc.code, exc.details) == (
        grpc.StatusCode.ALREADY_EXISTS,
        "restore item has a live open",
    )
    (row,) = _attempt_rows(rig, item_id)
    assert (row.end_kind, row.cause) == ("error", "lease_live")
    assert row.blocked_until == live_until.replace(tzinfo=None)
    assert row.lease_generation is None
    assert _lease_of(rig, item_id) == (1, live_until.replace(tzinfo=None))


def test_k6_the_lease_transaction_is_one_when_the_item_is_withdrawn(rig: _Rig) -> None:
    """The item update matches no row: the lease CAS, the fence's generation and
    D10's lowering all roll back; the request state is re-derived, not ``active``."""

    item_id, _payload, head = _acknowledged(rig, "k6-withdrawn")
    _expire_lease(rig, item_id)
    lease_before = _lease_of(rig, item_id)
    assert lease_before is not None
    service = _with_hooks(
        rig, before_lease=lambda withdrawn_id: _set_item(rig, withdrawn_id, state="failed")
    )
    try:
        frames, exc, _ = _open_collect(
            service, item_id, rig.context("receiver"), **_resume_fields(item_id, head, 0)
        )
    finally:
        service.stop()

    assert frames == []
    assert isinstance(exc, RuntimeError)
    _delivered, withdrawn = _attempt_rows(rig, item_id)
    assert (withdrawn.end_kind, withdrawn.cause) == ("error", "item_withdrawn")
    assert withdrawn.lease_generation is None
    assert withdrawn.ended_at is not None
    assert withdrawn.id not in service._registry
    # As before the call: the lease row (generation, expiry) and the checkpoint
    # (the lowering to 0 rolled back with the rest).
    assert _lease_of(rig, item_id) == lease_before
    assert _checkpoint_of(rig, item_id) == (1, False)
    assert _request_state(rig, item_id) == "completed_with_errors"


def test_k7_a_token_less_open_against_a_live_lease_is_refused_before_any_source_read(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _pl("k7-live")
    item_id = _seed_item(rig, "k7-live", payload, receiver="receiver")
    _seed_cache_entry(rig, item_id, payload)
    _deliver(rig, item_id)  # served from the cache; the lease is live
    lease = _lease_of(rig, item_id)
    assert lease is not None
    probes: list[int] = []
    original_probe = RestoreService._probe_cache_entry

    def counting_probe(self: RestoreService, *args: Any, **kwargs: Any) -> bool:
        probes.append(1)
        return original_probe(self, *args, **kwargs)

    monkeypatch.setattr(RestoreService, "_probe_cache_entry", counting_probe)
    archive_opens = rig.backend.open_calls

    frames, exc, _ = _open_collect(rig.service, item_id, rig.context("receiver"))

    assert frames == []
    assert isinstance(exc, _Abort)
    assert (exc.code, exc.details) == (
        grpc.StatusCode.ALREADY_EXISTS,
        "restore item has a live open",
    )
    assert probes == []
    assert rig.backend.open_calls == archive_opens
    _delivered, refused = _attempt_rows(rig, item_id)
    assert (refused.end_kind, refused.cause) == ("error", "lease_live")
    assert refused.blocked_until == lease[1]
    assert (refused.probe_source, refused.probe_candidates_tried, refused.probe_ended_at) == (
        None,
        None,
        None,
    )


def test_k4_a_corrupt_cache_entry_goes_through_the_funnel_once_and_falls_back_to_disk(
    rig: _Rig,
) -> None:
    payload = _pl("k4-corrupt-disk")
    item_id = _seed_item(rig, "k4-corrupt-disk", payload, receiver="receiver")
    cache_path = _seed_cache_entry(rig, item_id, payload)
    cache_path.write_bytes(b"corrupt cache bytes")
    events: list[RestoreEvent] = []
    breaker = DiskCircuitBreaker(failure_threshold=1, window_seconds=60)
    service = _funnel_service(rig, events, breaker=breaker)
    archive_opens = rig.backend.open_calls
    try:
        frames = list(
            _receive(
                service.OpenRestore(
                    restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                    rig.context("receiver"),
                ),
                lambda ack: service.CommitRestore(ack, rig.context("receiver")),
            )
        )
    finally:
        service.stop()

    assert _payload_digest(frames) == hashlib.sha256(payload).digest()
    assert rig.backend.open_calls > archive_opens
    assert [event.code for event in events] == ["cache-fallback:read-failed"]
    assert not breaker.is_open("d-cache")  # a content mismatch is not a breaker failure
    assert not cache_path.exists()
    with session_scope(rig.engine) as session:
        entry = session.get(CacheEntry, hashlib.sha256(payload).digest())
        assert entry is not None
        assert entry.state == "lost"
        disk = session.get(CacheDisk, "d-cache")
        assert disk is not None
        assert disk.state == "active"
    (row,) = _attempt_rows(rig, item_id)
    assert (row.end_kind, row.cause) == ("job_end", None)
    # K8: the cache entry read counts one, the disk copy one; the open streams from disk.
    assert (row.probe_source, row.probe_candidates_tried) == ("disk", 2)
    assert row.probe_ended_at is not None


def test_k4_a_corrupt_cache_entry_on_a_tape_only_item_ends_tape_only_with_the_entry_lost(
    rig: _Rig,
) -> None:
    payload = _pl("k4-corrupt-tape")
    item_id = _seed_item(rig, "k4-corrupt-tape", payload, receiver="receiver")
    cache_path = _seed_cache_entry(rig, item_id, payload)
    cache_path.write_bytes(b"corrupt cache bytes")
    _make_tape_only(rig, "k4-corrupt-tape")
    events: list[RestoreEvent] = []
    service = _funnel_service(rig, events)
    try:
        frames, exc, _ = _open_collect(service, item_id, rig.context("receiver"))
    finally:
        service.stop()

    assert frames == []
    assert isinstance(exc, _Abort)
    assert exc.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "RM3" in exc.details
    assert [event.code for event in events] == ["cache-fallback:read-failed"]
    # Committed with the funnel: the file is gone and the row is lost, so the
    # fresh read gives tape_only (terminal), not cache_disk_unavailable.
    assert not cache_path.exists()
    with session_scope(rig.engine) as session:
        entry = session.get(CacheEntry, hashlib.sha256(payload).digest())
        assert entry is not None
        assert entry.state == "lost"
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "failed"
        assert item.detail == _TAPE_ONLY_SENTENCE
    (row,) = _attempt_rows(rig, item_id)
    assert (row.end_kind, row.cause) == ("error", "tape_only")
    assert (row.probe_source, row.probe_candidates_tried) == (None, 1)
    assert row.probe_ended_at is not None


def test_k4_a_cache_read_deadline_counts_one_breaker_failure_and_leaves_the_entry_present(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _pl("k4-deadline")
    item_id = _seed_item(rig, "k4-deadline", payload, receiver="receiver")
    cache_path = _seed_cache_entry(rig, item_id, payload)

    def timing_out(*_args: Any, **_kwargs: Any) -> Any:
        raise StoreReadTimeout("cache read deadline exceeded")

    monkeypatch.setattr("sutradhara.hdcache.manager.read_entry_verified", timing_out)
    events: list[RestoreEvent] = []
    # One failure trips this breaker: the trip is the proof the failure was recorded.
    breaker = DiskCircuitBreaker(failure_threshold=1, window_seconds=60)
    service = _funnel_service(rig, events, breaker=breaker)
    try:
        frames = list(
            _receive(
                service.OpenRestore(
                    restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                    rig.context("receiver"),
                ),
                lambda ack: service.CommitRestore(ack, rig.context("receiver")),
            )
        )
    finally:
        service.stop()
        _set_cache_disk_state(rig, "active")

    assert _payload_digest(frames) == hashlib.sha256(payload).digest()
    assert [event.code for event in events] == ["cache-fallback:read-deadline", "disk-circuit-open"]
    assert breaker.is_open("d-cache")
    assert cache_path.exists()
    with session_scope(rig.engine) as session:
        entry = session.get(CacheEntry, hashlib.sha256(payload).digest())
        assert entry is not None
        assert entry.state == "present"
    (row,) = _attempt_rows(rig, item_id)
    assert (row.end_kind, row.cause) == ("job_end", None)
    assert (row.probe_source, row.probe_candidates_tried) == ("disk", 2)


def test_k8_probe_evidence_records_the_source_and_the_candidates_read(rig: _Rig) -> None:
    # A cache pass: cache, 1.
    payload = _pl("k8-cache")
    cache_id = _seed_item(rig, "k8-cache", payload, receiver="receiver")
    _seed_cache_entry(rig, cache_id, payload)
    _deliver(rig, cache_id)
    (row,) = _attempt_rows(rig, cache_id)
    assert (row.probe_source, row.probe_candidates_tried) == ("cache", 1)
    assert row.probe_ended_at is not None

    # A plain disk pass: disk, 1.
    disk_id = _seed_item(rig, "k8-disk", _pl("k8-disk"), receiver="receiver")
    _deliver(rig, disk_id)
    (row,) = _attempt_rows(rig, disk_id)
    assert (row.probe_source, row.probe_candidates_tried) == ("disk", 1)

    # Every disk candidate failing after a corrupt cache entry: null, 2, source_integrity.
    payload = _pl("k8-all-fail")
    failing_id = _seed_item(rig, "k8-all-fail", payload, receiver="receiver")
    cache_path = _seed_cache_entry(rig, failing_id, payload)
    cache_path.write_bytes(b"corrupt cache bytes")
    _corrupt_archive(rig, "k8-all-fail")
    frames, exc, _ = _open_collect(rig.service, failing_id, rig.context("receiver"))
    assert frames == []
    assert isinstance(exc, _Abort)
    (row,) = _attempt_rows(rig, failing_id)
    assert (row.end_kind, row.cause) == ("error", "source_integrity")
    assert (row.probe_source, row.probe_candidates_tried) == (None, 2)
    assert row.probe_ended_at is not None

    # The revealed replay runs no probe.
    replay_id = _seed_item(rig, "k8-replay", _pl("k8-replay"), receiver="receiver")
    head = _deliver(rig, replay_id)
    rig.service.CommitRestore(
        _commit_request(replay_id, head.manifest_sha256, head.lease_token, 1, revealed=True),
        rig.context("receiver"),
    )
    frames, exc, _ = _open_collect(
        rig.service, replay_id, rig.context("receiver"), lease_token=head.lease_token
    )
    assert exc is None
    _delivered, replay = _attempt_rows(rig, replay_id)
    assert (replay.probe_source, replay.probe_candidates_tried, replay.probe_ended_at) == (
        None,
        None,
        None,
    )


# ---- RSR-1d: acknowledged delivery and same-stream offers (D5, D11) -----------


def test_d2_delivery_waits_for_the_receivers_staged_checkpoint(rig: _Rig) -> None:
    """Transporting file_end leaves the item streaming until STAGED is durable."""

    item_id = _seed_item(rig, "d2-wait", _pl("d2-wait"), receiver="receiver")
    context = rig.context("receiver")
    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), context
    )
    frames = _pull_until_file_end(stream)
    head = frames[0].manifest_head
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(list, stream)
        try:
            with pytest.raises(TimeoutError):
                pending.result(timeout=0.1)
            assert _checkpoint_of(rig, item_id) == (0, False)
            (row,) = _attempt_rows(rig, item_id)
            assert row.end_kind is None
            reply = rig.service.CommitRestore(
                _commit_request(item_id, head.manifest_sha256, head.lease_token, 1),
                rig.context("receiver"),
            )
            assert (reply.status, reply.committed_index) == ("advanced", 1)
            tail = pending.result(timeout=2)
        finally:
            context.active = False
    assert _kinds(tail) == ["job_end"]
    (row,) = _attempt_rows(rig, item_id)
    assert (row.end_kind, row.cause) == ("job_end", None)
    with session_scope(rig.engine) as session:
        assert session.get(RestoreRequestItem, item_id).state == "sent"


def test_d2_rejecting_file_end_without_acknowledgement_is_an_interruption(rig: _Rig) -> None:
    """A receiver rejecting the file cannot turn transport completion into sent."""

    item_id = _seed_item(rig, "d2-reject", _pl("d2-reject"), receiver="receiver")
    context = rig.context("receiver")
    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), context
    )
    _pull_until_file_end(stream)
    context.active = False
    error = None
    try:
        list(stream)
    except grpc.RpcError as exc:
        error = exc
    with session_scope(rig.engine) as session:
        assert session.get(RestoreRequestItem, item_id).state == "queued"
    assert error is not None
    (row,) = _attempt_rows(rig, item_id)
    assert (row.end_kind, row.cause) == ("cancelled", None)
    assert _checkpoint_of(rig, item_id) == (0, False)


def test_d2_acknowledgement_must_match_the_open_generation(rig: _Rig) -> None:
    """A checkpoint attached to another lease generation cannot unblock this open."""

    item_id = _seed_item(rig, "d2-generation", _pl("d2-generation"), receiver="receiver")
    context = rig.context("receiver")
    stream = rig.service.OpenRestore(
        restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), context
    )
    head = _pull_until_file_end(stream)[0].manifest_head
    with session_scope(rig.engine) as session:
        session.get(RestoreOpenSession, item_id).generation += 1
        session.get(RestoreItemCheckpoint, item_id).committed_index = 1
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(list, stream)
        try:
            with pytest.raises(TimeoutError):
                pending.result(timeout=0.1)
            with session_scope(rig.engine) as session:
                session.get(RestoreOpenSession, item_id).generation = head.lease_token.generation
            assert _kinds(pending.result(timeout=2)) == ["job_end"]
        finally:
            context.active = False


def test_d3_lease_expiry_while_waiting_for_acknowledgement_is_terminal(fresh_rig: _Rig) -> None:
    """No acknowledgement by the lease deadline gives up with the deterministic cause."""

    rig = fresh_rig
    item_id = _seed_item(rig, "d3-expired", _pl("d3-expired"), receiver="receiver")
    service = _started(
        RestoreService(replace(rig.service_config, lease_duration=dt.timedelta(seconds=0.3)))
    )
    try:
        stream = service.OpenRestore(
            restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), rig.context("receiver")
        )
        _pull_until_file_end(stream)
        with pytest.raises(_Abort) as expired:
            next(stream)
        assert expired.value.code == grpc.StatusCode.ABORTED
        assert expired.value.details == (
            "the receiver did not acknowledge the delivery before its lease expired"
        )
        (row,) = _attempt_rows(rig, item_id)
        assert (row.end_kind, row.cause) == ("cancelled", "lease_expired")
        with session_scope(rig.engine) as session:
            item = session.get(RestoreRequestItem, item_id)
            assert item.state == "failed"
            assert item.detail == _LEASE_EXPIRED_SENTENCE
            assert giveup_agreement_violations(session) == []
    finally:
        service.stop()


@pytest.mark.parametrize("interval", [0, -0.01, float("nan")])
def test_d2_acknowledgement_poll_interval_must_be_positive(rig: _Rig, interval: float) -> None:
    """Reject poll intervals that cannot advance the acknowledgement wait."""

    with pytest.raises(ValueError, match="acknowledgement poll interval must be positive"):
        replace(rig.service_config, acknowledgement_poll_seconds=interval)


class _PollingWatchContext(_Context):
    """Bound a watch by poll count and drive state changes between its polls."""

    def __init__(
        self, rig: _Rig, polls: int, before_poll: Callable[[int], None] | None = None
    ) -> None:
        super().__init__("receiver", rig.fingerprints["receiver"])
        self.poll = 0
        self.limit = polls
        self.before_poll = before_poll

    def is_active(self) -> bool:
        self.poll += 1
        if not self.active or self.poll > self.limit:
            return False
        if self.before_poll is not None:
            self.before_poll(self.poll)
        return self.active


def test_d1_counted_failure_reoffers_after_backoff(fresh_rig: _Rig) -> None:
    """One stream suppresses a counted failure during backoff, then offers its new key."""

    rig = fresh_rig
    item_id = _seed_item(rig, "d1-counted", _pl("d1-counted"), receiver="receiver")
    offered: list[int] = []

    def before_poll(poll: int) -> None:
        if poll == 2:
            assert offered == [item_id]
            _corrupt_archive(rig, "d1-counted")
            _frames, exc, _ = _open_collect(rig.service, item_id, rig.context("receiver"))
            assert isinstance(exc, _Abort)
            (row,) = _attempt_rows(rig, item_id)
            assert row.cause == "source_integrity"
        elif poll == 3:
            assert offered == [item_id]  # the preceding backoff poll offered nothing
            _age_history(rig, item_id)

    context = _PollingWatchContext(rig, 4, before_poll)
    offered.extend(
        assignment.restore_request_item_id
        for assignment in rig.service.WatchAssignments(
            restore_pb2.WatchRequest(device_id="receiver"), context
        )
    )
    assert offered == [item_id, item_id]


def test_d1_rejected_delivery_reoffers_immediately(fresh_rig: _Rig) -> None:
    """T-d2's unacknowledged rejection is offered again on the original watch."""

    rig = fresh_rig
    item_id = _seed_item(rig, "d1-rejected", _pl("d1-rejected"), receiver="receiver")
    offered: list[int] = []

    def before_poll(poll: int) -> None:
        if poll == 2:
            assert offered == [item_id]
            receiver = rig.context("receiver")
            stream = rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), receiver
            )
            _pull_until_file_end(stream)
            receiver.active = False
            with pytest.raises(grpc.RpcError):
                list(stream)
            (row,) = _attempt_rows(rig, item_id)
            assert (row.end_kind, row.cause) == ("cancelled", None)
        elif poll == 3:
            assert offered == [item_id, item_id]  # no delay after the first interruption

    context = _PollingWatchContext(rig, 3, before_poll)
    offered.extend(
        assignment.restore_request_item_id
        for assignment in rig.service.WatchAssignments(
            restore_pb2.WatchRequest(device_id="receiver"), context
        )
    )
    assert offered == [item_id, item_id]


@pytest.mark.parametrize("history", ["open", "pending_give_up", "given_up"])
def test_d1_open_and_given_up_items_are_not_offered(fresh_rig: _Rig, history: str) -> None:
    """An open attempt or pending give-up suppresses a queued item without writing it."""

    rig = fresh_rig
    tag = f"d1-{history}"
    if history == "given_up":
        item_id = _fail_tape_only(rig, tag)
    else:
        item_id = _seed_item(rig, tag, _pl(tag), receiver="receiver")
        with session_scope(rig.engine) as session:
            pending = history == "pending_give_up"
            session.add(
                RestoreItemAttempt(
                    restore_request_item_id=item_id,
                    attempt_no=1,
                    receiver_device_id="receiver",
                    server_instance_id=rig.service.instance_id,
                    opened_from_state="queued",
                    opened_at=func.now(),
                    code_version="test-rsr-1d",
                    end_kind="error" if pending else None,
                    cause="tape_only" if pending else None,
                    ended_at=func.now() if pending else None,
                )
            )
    context = _PollingWatchContext(rig, 2)
    assert (
        list(rig.service.WatchAssignments(restore_pb2.WatchRequest(device_id="receiver"), context))
        == []
    )
    if history != "given_up":
        with session_scope(rig.engine) as session:
            assert session.get(RestoreRequestItem, item_id).state == "queued"
            assert _giveup_of(session, item_id) is None  # recovery, not the watch, applies it


def test_d1_delivered_item_stays_suppressed_until_a_new_stream(fresh_rig: _Rig) -> None:
    """A delivery's changing row and its lease expiry do not repeat the original offer."""

    rig = fresh_rig
    item_id = _seed_item(rig, "d1-delivered", _pl("d1-delivered"), receiver="receiver")
    offered: list[int] = []

    def before_poll(poll: int) -> None:
        if poll == 2:
            assert offered == [item_id]
            _deliver(rig, item_id)
        elif poll == 3:
            assert offered == [item_id]  # a new delivery row is still the same offer
            _expire_lease(rig, item_id)

    context = _PollingWatchContext(rig, 4, before_poll)
    offered.extend(
        assignment.restore_request_item_id
        for assignment in rig.service.WatchAssignments(
            restore_pb2.WatchRequest(device_id="receiver"), context
        )
    )
    assert offered == [item_id]
    reconnected = rig.service.WatchAssignments(
        restore_pb2.WatchRequest(device_id="receiver"), _PollingWatchContext(rig, 3)
    )
    assert [assignment.restore_request_item_id for assignment in reconnected] == [item_id]


def test_d1_lease_live_reoffers_after_blocked_until(fresh_rig: _Rig) -> None:
    """A wait row becomes a fresh offer once its recorded lease boundary has passed."""

    rig = fresh_rig
    item_id = _seed_item(rig, "d1-lease-live", _pl("d1-lease-live"), receiver="receiver")
    _deliver(rig, item_id)
    offered: list[int] = []

    def before_poll(poll: int) -> None:
        if poll == 2:
            assert offered == [item_id]
            _frames, exc, _ = _open_collect(rig.service, item_id, rig.context("receiver"))
            assert isinstance(exc, _Abort)
            _delivered, wait = _attempt_rows(rig, item_id)
            assert wait.cause == "lease_live"
            assert wait.blocked_until is not None
        elif poll == 3:
            assert offered == [item_id]
            _expire_lease(rig, item_id)
            with session_scope(rig.engine) as session:
                wait = session.scalars(
                    select(RestoreItemAttempt).where(
                        RestoreItemAttempt.restore_request_item_id == item_id,
                        RestoreItemAttempt.cause == "lease_live",
                    )
                ).one()
                wait.blocked_until = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=2)

    context = _PollingWatchContext(rig, 4, before_poll)
    offered.extend(
        assignment.restore_request_item_id
        for assignment in rig.service.WatchAssignments(
            restore_pb2.WatchRequest(device_id="receiver"), context
        )
    )
    assert offered == [item_id, item_id]


def test_d1_refusal_before_admission_keeps_the_seen_key(fresh_rig: _Rig) -> None:
    """A refusal writing no row cannot mint another offer, even after its delay passes."""

    rig = fresh_rig
    item_id = _seed_item(rig, "d1-no-row", _pl("d1-no-row"), receiver="receiver")
    _corrupt_archive(rig, "d1-no-row")
    _frames, exc, _ = _open_collect(rig.service, item_id, rig.context("receiver"))
    assert isinstance(exc, _Abort)
    _age_history(rig, item_id)
    offered: list[int] = []

    def before_poll(poll: int) -> None:
        if poll == 2:
            assert offered == [item_id]
            # Simulate the admission re-check seeing a later backoff deadline
            # for the same row (a database-clock change); admission writes no row.
            with session_scope(rig.engine) as session:
                session.connection().execute(
                    sql_update(RestoreItemAttempt)
                    .where(RestoreItemAttempt.restore_request_item_id == item_id)
                    .values(ended_at=func.now())
                )
            _frames, refused, _ = _open_collect(rig.service, item_id, rig.context("receiver"))
            assert isinstance(refused, _Abort)
            assert refused.details == "restore item is waiting before its next try"
            assert len(_attempt_rows(rig, item_id)) == 1
        elif poll == 3:
            _age_history(rig, item_id)
        elif poll == 4:
            assert offered == [item_id]  # eligible again, but the key was already offered
            _frames, failed, _ = _open_collect(rig.service, item_id, rig.context("receiver"))
            assert isinstance(failed, _Abort)
            assert len(_attempt_rows(rig, item_id)) == 2
            _age_history(rig, item_id)

    context = _PollingWatchContext(rig, 5, before_poll)
    offered.extend(
        assignment.restore_request_item_id
        for assignment in rig.service.WatchAssignments(
            restore_pb2.WatchRequest(device_id="receiver"), context
        )
    )
    assert offered == [item_id, item_id]


@pytest.mark.parametrize("reverse", [False, True])
def test_d5_sibling_reveals_rederive_under_the_request_lock(rig: _Rig, reverse: bool) -> None:
    """Either sibling reveal order locks and reloads the request before deriving its state."""

    tag = f"d5-siblings-{reverse}"
    first_id = _seed_item(rig, tag + "-a", _pl(tag + "-a"), receiver="receiver")
    second_id = _seed_item(rig, tag + "-b", _pl(tag + "-b"), receiver="receiver")
    with session_scope(rig.engine) as session:
        first = session.get(RestoreRequestItem, first_id)
        second = session.get(RestoreRequestItem, second_id)
        old_request = second.request
        second.request = first.request
        session.flush()
        session.delete(old_request)
    heads = {item_id: _deliver(rig, item_id) for item_id in (first_id, second_id)}
    locked_reads: list[Any] = []

    def observe_request_lock(state: Any) -> None:
        # On SQLite the sibling outcome is right even without the lock; pin the
        # request statement's FOR UPDATE when compiled for PostgreSQL.
        if state.is_select and any(
            description.get("entity") is RestoreRequest
            for description in state.statement.column_descriptions
        ):
            compiled = str(state.statement.compile(dialect=postgresql.dialect()))
            if "FOR UPDATE" in compiled:
                locked_reads.append(state.statement)

    event.listen(Session, "do_orm_execute", observe_request_lock)
    try:
        order = (second_id, first_id) if reverse else (first_id, second_id)
        for index, item_id in enumerate(order):
            head = heads[item_id]
            reply = rig.service.CommitRestore(
                _commit_request(item_id, head.manifest_sha256, head.lease_token, 1, revealed=True),
                rig.context("receiver"),
            )
            assert reply.status == "revealed"
            assert len(locked_reads) == index + 1
            assert _request_state(rig, item_id) == ("active" if index == 0 else "completed")
    finally:
        event.remove(Session, "do_orm_execute", observe_request_lock)
    with session_scope(rig.engine) as session:
        assert session.get(RestoreRequestItem, first_id).state == "done"
        assert session.get(RestoreRequestItem, second_id).state == "done"
        assert session.get(RestoreRequestItem, first_id).request.state == "completed"


@pytest.mark.parametrize("state", ["sent", "failed"])
def test_d5_reconciler_reopens_sent_and_preserves_failed(rig: _Rig, state: str) -> None:
    """Only a sent item's detail changes; terminal items and their request stay intact."""

    tag = f"d5-reconciler-{state}"
    item_id = _seed_item(rig, tag, _pl(tag), receiver="receiver")
    _deliver(rig, item_id)
    _expire_lease(rig, item_id)
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        item.state = state
        item.detail = "prior detail"
        item.updated_at = dt.datetime(2000, 1, 1, tzinfo=dt.UTC)
        item.request.state = "completed_with_errors" if state == "failed" else "completed"
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        before = (item.state, item.detail, item.updated_at, item.request.state)
        restore_open_reconciler.reconcile_target(session, str(item_id))
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        after = (item.state, item.detail, item.updated_at, item.request.state)
        if state == "failed":
            assert after == before
        else:
            assert item.state == "sent"
            assert item.detail == restore_open_reconciler.REOPENABLE_DETAIL
            assert item.updated_at > before[2]
            assert item.request.state == "active"


@pytest.fixture
def worker_limit_socket(fresh_rig: _Rig) -> Iterator[tuple[_Rig, int]]:
    """The production 16-worker mTLS server with a six-second restore lease."""

    rig = fresh_rig
    port = _unused_port()
    parts = make_server(
        GrpcServerConfig(
            engine=rig.engine,
            landing_root=rig.root / "landing-limit",
            pki_dir=rig.pki_dir,
            bind="127.0.0.1",
            port=port,
            validate_artifactclass=False,
            # Watches only hold workers; fast polling starves opens in this single-process test.
            restore=replace(
                rig.service_config,
                lease_duration=dt.timedelta(seconds=6),
                assignment_poll_seconds=0.5,
            ),
        )
    )
    parts.restore_service.start(background=False)
    parts.server.start()
    try:
        yield rig, port
    finally:
        parts.server.stop(grace=0).wait()
        parts.restore_service.stop()


def test_real_socket_d4_commits_progress_when_all_shared_workers_are_occupied(
    worker_limit_socket: tuple[_Rig, int],
) -> None:
    """Twelve watches plus four unacknowledged opens cannot starve their STAGED RPCs.

    If CommitRestore shared the stream workers, the opens would end lease_expired;
    both outcomes are collected first so cleanup cannot hide that. It needs loopback
    sockets and uses make_server's production registration.
    """

    rig, port = worker_limit_socket
    item_ids = [_seed_item(rig, f"d4-{n}", _pl(f"d4-{n}"), receiver="receiver") for n in range(4)]
    channel = _receiver_channel(rig, port)
    stub = restore_pb2_grpc.RestoreServiceStub(channel)  # type: ignore[no-untyped-call]
    watches: list[Any] = []
    streams: list[Any] = []
    try:
        for _ in range(12):
            watch = stub.WatchAssignments(
                restore_pb2.WatchRequest(device_id="receiver"), timeout=20
            )
            watches.append(watch)
            # A returned assignment proves this long-lived handler has a worker.
            assert next(watch).restore_request_item_id in item_ids
        heads: list[Any] = []
        for item_id in item_ids:
            stream = stub.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), timeout=20
            )
            streams.append(stream)
            heads.append(_pull_until_file_end(stream)[0].manifest_head)
        for item_id in item_ids:
            lease = _lease_of(rig, item_id)
            assert lease is not None
            remaining = lease[1].replace(tzinfo=dt.UTC) - dt.datetime.now(dt.UTC)
            assert remaining.total_seconds() > 3
        began = time.monotonic()
        commits = [
            stub.CommitRestore.future(
                _commit_request(item_id, head.manifest_sha256, head.lease_token, 1), timeout=2
            )
            for item_id, head in zip(item_ids, heads, strict=True)
        ]
        replies: list[Any] = []
        for commit in commits:
            try:
                # Commits that queue behind the streams expire here; keep reading
                # the opens so each expiry is recorded.
                replies.append(commit.result(timeout=4))
            except grpc.RpcError as exc:
                replies.append(exc)
        tails: list[Any] = []
        for stream in streams:
            try:
                tails.append(list(stream))
            except grpc.RpcError as exc:
                tails.append(exc)
        elapsed = time.monotonic() - began
        rows = [_attempt_rows(rig, item_id)[0] for item_id in item_ids]
        assert [(row.end_kind, row.cause) for row in rows] == [("job_end", None)] * 4
        assert all(not isinstance(reply, grpc.RpcError) for reply in replies), replies
        assert [(reply.status, reply.committed_index) for reply in replies] == [("advanced", 1)] * 4
        assert all(not isinstance(tail, grpc.RpcError) for tail in tails), tails
        assert all(_kinds(tail) == ["job_end"] for tail in tails)
        assert elapsed < 3  # well before the six-second lease expires
    finally:
        for stream in streams:
            stream.cancel()
        for watch in watches:
            watch.cancel()
        channel.close()
