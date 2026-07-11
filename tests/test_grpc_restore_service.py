"""RM1.2a authorization, lease, and real-socket restore-stream verification."""

from __future__ import annotations

import datetime as dt
import hashlib
import shutil
import socket
import struct
import tracemalloc
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import grpc
import pytest
from sqlalchemy import Engine, func, select

from sutradhara._proto import restore_pb2, restore_pb2_grpc
from sutradhara.api.live_capabilities import LiveCapabilityResolver
from sutradhara.api.routes_restore import _request_payload
from sutradhara.archive_restore import RemArchiveExtractor
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    BundlingPolicy,
    DurabilityPolicy,
    HdcachePolicy,
    PlacementPolicy,
    apply_artifactclass_policy,
)
from sutradhara.backend.port import BackendLocator, ByteRange, CopyRecord, StreamKind, VerifyResult
from sutradhara.catalog.models import (
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
    RestoreService,
    RestoreServiceConfig,
    _manifest_digest,
    _ManifestFile,
)
from sutradhara.grpc.server import GrpcServerConfig, make_server
from sutradhara.hdcache.models import RestoreOpenSession, RestoreRequest, RestoreRequestItem
from sutradhara.jobs.models import Job
from sutradhara.keys import KeyRegistry
from sutradhara.rem_archive_cli import resolve_rem_bin
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import RAO_CHUNK_SIZE, RaoCliSealer


class _DiskArchiveBackend:
    """Range-stream archive fixture that never materializes a whole object."""

    def __init__(self) -> None:
        self.paths: dict[str, Path] = {}
        self.max_chunk_seen = 0
        self.whole_reads = 0

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
        return VerifyResult(ok=True)


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
    root = tmp_path_factory.mktemp("grpc-restore")
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
    keys = KeyRegistry(root / "keys")
    config = RestoreServiceConfig(
        engine=engine,
        live_capabilities=LiveCapabilityResolver(capability_source),
        backend_resolver=lambda _session, _artifactclass: dict(backend_by_id),
        extractor_factory=lambda: RemArchiveExtractor(rem_bin, keys=keys),
        max_concurrent_streams=4,
        lease_duration=dt.timedelta(minutes=5),
        assignment_poll_seconds=0.01,
    )
    test_rig = _Rig(
        root=root,
        engine=engine,
        pki_dir=pki_dir,
        backend=backend,
        backend_by_id=backend_by_id,
        capabilities=capabilities,
        unavailable_operators=unavailable_operators,
        service_config=config,
        service=RestoreService(config),
        receiver_material=receiver_material,
        receiver_cert=receiver_cert,
        fingerprints=fingerprints,
    )
    try:
        yield test_rig
    finally:
        engine.dispose()


@pytest.fixture
def socket_port(rig: _Rig) -> Iterator[int]:
    """Run the production shared mTLS server on a real loopback socket."""

    port = _unused_port()
    server = make_server(
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
    server.start()
    try:
        yield port
    finally:
        server.stop(grace=0).wait()


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
        for frame in stub.OpenRestore(
            restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), timeout=20
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
    assert max_frame <= RAO_CHUNK_SIZE
    assert rig.backend.max_chunk_seen <= RAO_CHUNK_SIZE
    assert rig.backend.whole_reads == 0
    assert peak - baseline < len(payload) // 2


def test_open_restore_real_socket_streams_rao_aead_plaintext(rig: _Rig, socket_port: int) -> None:
    payload = b"authenticated encrypted restore\n" * 20_000
    source = rig.root / "encrypted-source.bin"
    source.write_bytes(payload)
    registry = KeyRegistry(rig.root / "keys")
    epoch = registry.create_epoch()
    stored = rig.root / "encrypted-stored.rao"
    with RaoCliSealer(registry).seal(
        source,
        Representation.RAO_AEAD_V1,
        key_epoch=epoch,
    ) as sealed:
        shutil.copyfile(sealed.sealed_path, stored)
    item_id = _seed_item(
        rig,
        "aead",
        payload,
        receiver="receiver",
        representation=Representation.RAO_AEAD_V1,
        stored_path=stored,
        key_epoch=epoch.key_id,
        member_path=source.name,
    )

    channel = _receiver_channel(rig, socket_port)
    try:
        frames = restore_pb2_grpc.RestoreServiceStub(  # type: ignore[no-untyped-call]
            channel
        ).OpenRestore(restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id), timeout=20)
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
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
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

        rig.capabilities["ada"] = frozenset({"can_restore", "can_restore_p2"})
        frames = list(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
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

    with pytest.raises(_Abort) as denied:
        emitted.extend(
            rig.service.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context(device_id),
            )
        )

    assert denied.value.code == expected
    assert emitted == []


def test_duplicate_open_rejected_then_expired_generation_reopens(rig: _Rig) -> None:
    item_id = _seed_item(rig, "lease", b"lease arbitration", receiver="receiver")
    request = restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id)
    first = rig.service.OpenRestore(request, rig.context("receiver"))
    assert next(first).HasField("manifest_head")

    with pytest.raises(_Abort) as duplicate:
        list(rig.service.OpenRestore(request, rig.context("receiver")))
    assert duplicate.value.code == grpc.StatusCode.ALREADY_EXISTS

    with session_scope(rig.engine) as session:
        lease = session.get(RestoreOpenSession, item_id)
        assert lease is not None
        first_generation = lease.generation
        lease.expires_at = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1)
    reopened = rig.service.OpenRestore(request, rig.context("receiver"))
    assert next(reopened).manifest_head.lease_token.generation == first_generation + 1
    first.close()  # type: ignore[attr-defined]
    remaining = list(reopened)
    assert remaining[-1].HasField("job_end")


def test_restore_capacity_fails_fast_and_cancellation_releases_state(rig: _Rig) -> None:
    first_id = _seed_item(rig, "capacity-first", b"first", receiver="receiver")
    second_id = _seed_item(rig, "capacity-second", b"second", receiver="receiver")
    service = RestoreService(replace(rig.service_config, max_concurrent_streams=1))
    first_context = rig.context("receiver")
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
    with session_scope(rig.engine) as session:
        first_item = session.get(RestoreRequestItem, first_id)
        assert first_item is not None
        assert first_item.state == "queued"
        lease = session.get(RestoreOpenSession, first_id)
        assert lease is not None
        assert lease.expires_at <= dt.datetime.now(dt.UTC).replace(tzinfo=None)


def test_archive_error_frame_does_not_mark_item_sent(rig: _Rig) -> None:
    item_id = _seed_item(rig, "stream-error", b"expected bytes", receiver="receiver")
    path = rig.backend.paths["object-stream-error"]
    path.write_bytes(b"archive-prefix" + b"corrupted data")

    frames = list(
        rig.service.OpenRestore(
            restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
            rig.context("receiver"),
        )
    )

    assert frames[-1].HasField("error")
    assert not any(frame.HasField("job_end") for frame in frames)
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == "queued"


def test_completed_stream_is_sent_without_job_or_local_write_and_console_is_active(
    rig: _Rig,
) -> None:
    payload = b"remote terminal belongs to RM1.3"
    item_id = _seed_item(rig, "sent", payload, receiver="receiver")
    forbidden_local = rig.root / "agent-destinations" / "receiver" / "exports" / "sent.bin"

    frames = list(
        rig.service.OpenRestore(
            restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
            rig.context("receiver"),
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
        assert _request_payload(item.request)["state"] == "active"
        assert session.scalar(select(func.count()).select_from(Job)) == 0
    assert not forbidden_local.exists()


def test_completed_stream_records_actual_tape_source(rig: _Rig) -> None:
    item_id = _seed_item(rig, "source-tape", b"served from tape", receiver="receiver")
    with session_scope(rig.engine) as session:
        backend = session.scalar(select(Backend).where(Backend.name == "disk-source-tape"))
        assert backend is not None
        backend.kind = BackendKind.REM_TAPE

    frames = list(
        rig.service.OpenRestore(
            restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
            rig.context("receiver"),
        )
    )

    assert frames[-1].HasField("job_end")
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.source == "tape"


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


def test_commit_restore_is_explicitly_deferred_to_rm13(rig: _Rig) -> None:
    with pytest.raises(_Abort) as deferred:
        rig.service.CommitRestore(restore_pb2.CommitRestoreRequest(), rig.context("receiver"))
    assert deferred.value.code == grpc.StatusCode.UNIMPLEMENTED
    assert "RM1.3" in deferred.value.details


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
    key_epoch: str | None = None,
    member_path: str = "payload.bin",
    privacy_level: str = "none",
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
                artifactclass=artifactclass,
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
                **({"key_epoch": key_epoch} if key_epoch is not None else {}),
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
        if representation is Representation.RAO_AEAD_V1:
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
            destination_id="restore-dest",
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
