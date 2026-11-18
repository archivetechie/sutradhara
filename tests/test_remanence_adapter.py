"""Tests for the Remanence Layer 5 adapter.

Fixture-mode tests protect local/dev behavior; fake-Catalog tests protect the
live gRPC mapping without requiring the real daemon in unit tests.
"""

from __future__ import annotations

import hashlib
import tempfile
import threading
import time
from collections.abc import Iterator
from concurrent import futures
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import grpc
import pytest

from sutradhara._proto import layer5_pb2, layer5_pb2_grpc
from sutradhara.backend import remanence as remanence_module
from sutradhara.backend.port import (
    BackendError,
    BackendNotFoundError,
    BackendStoredCorruptError,
    BackendTransientError,
    BackendUnavailableError,
    ByteRange,
    CopyRecord,
    StorageBackend,
)
from sutradhara.backend.remanence import (
    CommittedCopy,
    RemanenceBackend,
    RemanenceDigestMismatchError,
    RemanenceWriteResult,
    RemanenceWriteSessionError,
    WrittenReceipt,
)
from sutradhara.catalog.session import locator_key
from sutradhara.catalog.types import content_hash


@contextmanager
def _serve_uds(server: grpc.Server) -> Iterator[str]:
    """Start a test gRPC server on a temporary Unix-domain socket."""

    with tempfile.TemporaryDirectory(prefix="sutradhara-grpc-test-") as root:
        endpoint = f"unix:{Path(root) / 'server.sock'}"
        if server.add_insecure_port(endpoint) != 1:
            raise RuntimeError(f"failed to bind test gRPC server at {endpoint}")
        server.start()
        try:
            yield endpoint
        finally:
            server.stop(grace=None)


def test_tape_finalization_wire_tags_keep_health_and_progress_distinct() -> None:
    fields = {field.name: field.number for field in layer5_pb2.TapeFinalization.DESCRIPTOR.fields}
    assert fields["replica_health"] == 5
    assert fields["replica_progress"] == 11


def test_tape_state_retired_keeps_authoritative_wire_tag() -> None:
    state = layer5_pb2.Tape.State.DESCRIPTOR
    assert state.values_by_name["TAPE_STATE_RETIRED"].number == 10


FIXTURE = Path(__file__).parent / "fixtures" / "remanence_objects.json"


def _committed_object(
    template: layer5_pb2.ObjectRecord,
    caller_object_id: str,
) -> layer5_pb2.ObjectRecord:
    """Clone a catalog record as one generated CHECKPOINTED write result."""

    result = layer5_pb2.ObjectRecord()
    result.CopyFrom(template)
    result.caller_object_id = caller_object_id
    result.append_commit_info.durability = layer5_pb2.APPEND_DURABILITY_CHECKPOINTED
    return result


def _written_ack(
    caller_object_id: str,
    *,
    object_id: bytes,
    ordinal: int = 1,
    batch_id: bytes = b"batch-1",
) -> layer5_pb2.ObjectRecord:
    """Build the real locator-free ObjectRecord shape for a WRITTEN append ack."""

    return layer5_pb2.ObjectRecord(
        object_id=object_id,
        caller_object_id=caller_object_id,
        append_commit_info=layer5_pb2.AppendCommitInfo(
            durability=layer5_pb2.APPEND_DURABILITY_WRITTEN,
            batch_id=batch_id,
            provisional_ordinal=ordinal,
        ),
    )


def _checkpoint_response(
    session_id: bytes,
    tape_uuid: bytes,
    committed: list[layer5_pb2.ObjectRecord],
) -> layer5_pb2.CheckpointSessionResponse:
    """Build a generated checkpoint response with parallel committed sets."""

    copies = [next(cp for cp in obj.copies if cp.tape_uuid == tape_uuid) for obj in committed]
    return layer5_pb2.CheckpointSessionResponse(
        session=layer5_pb2.WriteSession(session_id=session_id, tape_uuid=tape_uuid),
        committed_objects=committed,
        committed_copies=copies,
    )


def _write_session_response(
    session_id: bytes,
    tape_uuid: bytes,
    committed: list[layer5_pb2.ObjectRecord],
) -> layer5_pb2.WriteSession:
    """Build the generated close response carrying any checkpointed objects."""

    copies = [next(cp for cp in obj.copies if cp.tape_uuid == tape_uuid) for obj in committed]
    return layer5_pb2.WriteSession(
        session_id=session_id,
        tape_uuid=tape_uuid,
        checkpointed_objects=committed,
        committed_copies=copies,
    )


@pytest.fixture
def backend() -> RemanenceBackend:
    return RemanenceBackend.from_fixture_file("rem-tape-primary", FIXTURE)


def test_satisfies_storagebackend_protocol(backend: RemanenceBackend) -> None:
    assert isinstance(backend, StorageBackend)
    assert backend.name == "rem-tape-primary"


def test_enumerate_yields_one_record_per_object_copy(backend: RemanenceBackend) -> None:
    records = list(backend.enumerate())
    # Fixture has 3 objects, the third with 2 copies: 4 records total.
    assert len(records) == 4

    # Group by logical_id to verify the multi-copy case.
    by_hash: dict[bytes, list[CopyRecord]] = {}
    for r in records:
        by_hash.setdefault(r.logical_id, []).append(r)

    assert len(by_hash) == 3, "three distinct logical assets"
    copy_counts = sorted(len(v) for v in by_hash.values())
    assert copy_counts == [1, 1, 2], "third asset has two copies"


def test_enumerate_copy_record_shape(backend: RemanenceBackend) -> None:
    records = list(backend.enumerate())
    first = next(r for r in records if r.metadata.get("caller_object_id") == "asset-000")

    expected_hash = content_hash(hashlib.sha256(b"hello world").digest())
    assert first.logical_id == expected_hash
    assert first.integrity_hash == expected_hash
    assert first.size_bytes == len(b"hello world")
    assert first.native_locator["tape_file_number"] == 1
    assert first.native_locator["tape_uuid"].startswith("aaaa0000")
    assert "object_id" in first.native_locator

    assert first.metadata["body_format"] == "rem-tar-v1"
    assert first.metadata["caller_object_id"] == "asset-000"
    assert first.metadata["health"] == "ok"
    assert first.metadata["caller_meta:campaign"] == "fixture"


def test_read_whole_object(backend: RemanenceBackend) -> None:
    records = list(backend.enumerate())
    first = next(r for r in records if r.metadata["caller_object_id"] == "asset-000")
    data = backend.read_range(first.native_locator, ByteRange(0, 0))
    assert data == b"hello world"


def test_read_sub_range(backend: RemanenceBackend) -> None:
    records = list(backend.enumerate())
    second = next(r for r in records if r.metadata["caller_object_id"] == "asset-001")
    data = backend.read_range(second.native_locator, ByteRange(7, 12))
    assert data == b"asset"


def test_read_range_past_end_raises(backend: RemanenceBackend) -> None:
    records = list(backend.enumerate())
    first = next(r for r in records if r.metadata["caller_object_id"] == "asset-000")
    with pytest.raises(ValueError, match="exceeds object size"):
        backend.read_range(first.native_locator, ByteRange(0, 999))


def test_read_unknown_tape_raises(backend: RemanenceBackend) -> None:
    locator = {
        "tape_uuid": "ffffffffffffffffffffffffffffffff",
        "tape_file_number": 1,
    }
    with pytest.raises(BackendNotFoundError, match="no object at tape"):
        backend.read_range(locator, ByteRange(0, 0))


def test_read_malformed_locator_raises(backend: RemanenceBackend) -> None:
    with pytest.raises(BackendNotFoundError, match="locator must have"):
        backend.read_range({"tape_uuid": "not-hex", "tape_file_number": 1}, ByteRange(0, 0))


def test_verify_happy(backend: RemanenceBackend) -> None:
    records = list(backend.enumerate())
    first = next(r for r in records if r.metadata["caller_object_id"] == "asset-000")
    result = backend.verify(first.native_locator)
    assert result.ok
    assert result.measured is True
    assert result.actual_hash is not None


@pytest.mark.parametrize("with_bytes", [True, False])
def test_verify_fixture_observer_only_receives_available_bytes(with_bytes: bool) -> None:
    """T2 fixture contract: catalog-only objects produce no byte observations."""
    data = b"fixture observer"
    digest = hashlib.sha256(data).digest()
    obj: dict[str, Any] = {
        "object_id": "1" * 32,
        "content_sha256": digest.hex(),
        "logical_size_bytes": len(data),
        "copies": [{"tape_uuid": "2" * 32, "tape_file_number": 1, "health": "ok"}],
    }
    if with_bytes:
        obj["content_hex"] = data.hex()
    backend = RemanenceBackend.from_object_dicts("fixture", [obj])
    [record] = list(backend.enumerate())
    observed: list[tuple[int, bytes]] = []
    result = backend.verify(record.native_locator, observe=lambda o, c: observed.append((o, c)))
    assert result.ok
    assert result.measured is with_bytes
    assert result.actual_hash == digest
    assert observed == ([(0, data)] if with_bytes else [])


def test_verify_byte_less_fixture_is_explicitly_unmeasured() -> None:
    """A fixture catalog hash without readable bytes cannot qualify as measured."""

    digest = content_hash(hashlib.sha256(b"catalog-only").digest())
    backend = RemanenceBackend.from_object_dicts(
        "catalog-only",
        [
            {
                "object_id": "1" * 32,
                "content_sha256": digest.hex(),
                "logical_size_bytes": 12,
                "copies": [
                    {
                        "tape_uuid": "2" * 32,
                        "tape_file_number": 1,
                        "health": "ok",
                    }
                ],
            }
        ],
    )
    [record] = list(backend.enumerate())

    result = backend.verify(record.native_locator)

    assert result.ok is True
    assert result.measured is False
    assert result.actual_hash == digest


def test_verify_detects_mismatch_when_fixture_lies() -> None:
    """If the fixture's declared content_sha256 doesn't match the bytes,
    verify() reports the mismatch."""
    h_fake = content_hash(hashlib.sha256(b"not the real content").digest())
    dicts = [
        {
            "object_id": "0" * 32,
            "caller_object_id": "lying-asset",
            "content_sha256": h_fake.hex(),
            "logical_size_bytes": 11,
            "body_format": "rem-tar-v1",
            "content_b64": "aGVsbG8gd29ybGQ=",  # "hello world", not matching h_fake
            "copies": [
                {
                    "tape_uuid": "c" * 32,
                    "tape_file_number": 1,
                    "first_body_lba": 0,
                    "health": "ok",
                }
            ],
        }
    ]
    backend = RemanenceBackend.from_object_dicts("test", dicts)
    [record] = list(backend.enumerate())
    result = backend.verify(record.native_locator)
    assert not result.ok
    assert result.actual_hash != h_fake
    assert "expected" in result.detail


def test_empty_fixture_yields_nothing() -> None:
    backend = RemanenceBackend.from_object_dicts("empty", [])
    assert list(backend.enumerate()) == []


def test_duplicate_locator_in_fixture_raises() -> None:
    """Two copies with the same (tape_uuid, tape_file_number) is malformed."""
    h = content_hash(hashlib.sha256(b"x").digest())
    dicts = [
        {
            "object_id": "0" * 32,
            "content_sha256": h.hex(),
            "logical_size_bytes": 1,
            "copies": [
                {"tape_uuid": "a" * 32, "tape_file_number": 1},
                {"tape_uuid": "a" * 32, "tape_file_number": 1},
            ],
        }
    ]
    with pytest.raises(ValueError, match="duplicate"):
        RemanenceBackend.from_object_dicts("bad", dicts)


def test_uuid_fields_must_be_16_bytes() -> None:
    h = content_hash(hashlib.sha256(b"x").digest())
    dicts = [
        {
            "object_id": "0" * 34,
            "content_sha256": h.hex(),
            "logical_size_bytes": 1,
            "copies": [{"tape_uuid": "a" * 32, "tape_file_number": 1}],
        }
    ]
    with pytest.raises(ValueError, match="object_id must be a 16-byte UUID"):
        RemanenceBackend.from_object_dicts("bad", dicts)


def test_multi_copy_asset_yields_distinct_locators(backend: RemanenceBackend) -> None:
    """The third fixture asset has two copies on different tapes.
    enumerate() must yield two records with the same logical_id but
    distinct native_locators."""
    records = list(backend.enumerate())
    h_multi = content_hash(hashlib.sha256(b"multi-copy content").digest())
    multi = [r for r in records if r.logical_id == h_multi]
    assert len(multi) == 2
    locators = {tuple(sorted(r.native_locator.items())) for r in multi}
    assert len(locators) == 2, "the two copies have distinct locators"


LIVE_ENDPOINT = "localhost:50051"


@pytest.fixture
def proto_object() -> layer5_pb2.ObjectRecord:
    return layer5_pb2.ObjectRecord(
        object_id=bytes.fromhex("1cd8ebd3d70a4998a02ab868b8aafbf3"),
        caller_object_id="771e50cd-bb6a-46a1-ad51-8bd2d36c494d",
        content_sha256=bytes.fromhex(
            "df36100fc9069260ac935a730be234373252074249cbc855fb65a6213dceafa4"
        ),
        logical_size_bytes=123,
        body_format="rem-tar-v1",
        caller_metadata={"campaign": "daemon-test"},
        copies=[
            layer5_pb2.ObjectCopy(
                tape_uuid=bytes.fromhex("b8f6123456784e90aabbccddeeff0011"),
                tape_file_number=1,
                first_body_lba=1,
                health=layer5_pb2.ObjectCopy.OBJECT_COPY_HEALTH_OK,
                pool_id="scenario-a",
            )
        ],
        append_commit_info=layer5_pb2.AppendCommitInfo(
            append_mode=layer5_pb2.APPEND_MODE_FRESH,
            tape_uuid=bytes.fromhex("b8f6123456784e90aabbccddeeff0011"),
            voltag="RMA101L9",
            tape_file_number=1,
            first_body_lba=1,
            position_after_lba=42,
            durability=layer5_pb2.APPEND_DURABILITY_CHECKPOINTED,
        ),
    )


class _Catalog(layer5_pb2_grpc.CatalogServicer):
    def __init__(
        self,
        obj: layer5_pb2.ObjectRecord,
        pools: list[layer5_pb2.TapePool] | None = None,
    ) -> None:
        self.obj = obj
        self.pools = pools or []
        self.get_object_requests: list[layer5_pb2.GetObjectRequest] = []

    def EnumerateObjects(
        self,
        request: layer5_pb2.EnumerateObjectsRequest,
        context: grpc.ServicerContext,
    ) -> Iterator[layer5_pb2.ObjectRecord]:
        yield self.obj

    def GetObject(
        self,
        request: layer5_pb2.GetObjectRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.ObjectRecord:
        self.get_object_requests.append(request)
        if request.object_id == self.obj.object_id:
            return self.obj
        context.abort(grpc.StatusCode.NOT_FOUND, "object not found")
        raise AssertionError("unreachable after context.abort")

    def ListTapePools(
        self,
        request: layer5_pb2.ListTapePoolsRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.ListTapePoolsResponse:
        return layer5_pb2.ListTapePoolsResponse(pools=self.pools)


@pytest.fixture
def catalog_server(
    proto_object: layer5_pb2.ObjectRecord,
) -> Iterator[tuple[str, _Catalog]]:
    servicer = _Catalog(proto_object)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    layer5_pb2_grpc.add_CatalogServicer_to_server(servicer, server)  # type: ignore[no-untyped-call]
    with _serve_uds(server) as endpoint:
        yield endpoint, servicer


@pytest.fixture
def live_backend(catalog_server: tuple[str, _Catalog]) -> RemanenceBackend:
    endpoint, _ = catalog_server
    return RemanenceBackend.from_grpc("primary-tape", endpoint)


def test_from_grpc_builds_live_adapter(live_backend: RemanenceBackend) -> None:
    assert isinstance(live_backend, StorageBackend)
    assert live_backend.name == "primary-tape"


def test_live_enumerate_maps_proto_records_to_copy_records(
    live_backend: RemanenceBackend,
) -> None:
    [record] = list(live_backend.enumerate())
    assert record.logical_id == bytes.fromhex(
        "df36100fc9069260ac935a730be234373252074249cbc855fb65a6213dceafa4"
    )
    assert record.integrity_hash == record.logical_id
    assert record.size_bytes == 123
    assert record.native_locator == {
        "tape_uuid": "b8f6123456784e90aabbccddeeff0011",
        "tape_file_number": 1,
        "first_body_lba": 1,
        "object_id": "1cd8ebd3d70a4998a02ab868b8aafbf3",
        "caller_object_id": "771e50cd-bb6a-46a1-ad51-8bd2d36c494d",
        "content_sha256": "df36100fc9069260ac935a730be234373252074249cbc855fb65a6213dceafa4",
        "pool_id": "scenario-a",
        "body_format": "rem-tar-v1",
    }
    assert record.metadata["health"] == "ok"
    assert record.metadata["caller_meta:campaign"] == "daemon-test"
    assert record.metadata["append_commit_info"] == {
        "append_mode": "fresh",
        "tape_uuid": "b8f6123456784e90aabbccddeeff0011",
        "voltag": "RMA101L9",
        "tape_file_number": 1,
        "first_body_lba": 1,
        "position_before_lba": None,
        "position_after_lba": 42,
        "journal_record_ordinal": None,
        "estimated_remaining_bytes": None,
        "sealed_after_write": None,
    }


def test_proto_native_locator_matches_write_path_locator_key(
    live_backend: RemanenceBackend,
) -> None:
    [record] = list(live_backend.enumerate())
    expected = {
        "tape_uuid": "b8f6123456784e90aabbccddeeff0011",
        "tape_file_number": 1,
        "first_body_lba": 1,
        "object_id": "1cd8ebd3d70a4998a02ab868b8aafbf3",
        "caller_object_id": "771e50cd-bb6a-46a1-ad51-8bd2d36c494d",
        "content_sha256": "df36100fc9069260ac935a730be234373252074249cbc855fb65a6213dceafa4",
        "pool_id": "scenario-a",
        "body_format": "rem-tar-v1",
    }
    assert locator_key(record.native_locator) == locator_key(expected)


_TAPE_UUID_HEX = "b8f6123456784e90aabbccddeeff0011"
_OBJECT_ID_HEX = "1cd8ebd3d70a4998a02ab868b8aafbf3"


def _read_locator(content_sha256_hex: str) -> dict[str, str | int]:
    return {
        "tape_uuid": _TAPE_UUID_HEX,
        "tape_file_number": 1,
        "first_body_lba": 1,
        "object_id": _OBJECT_ID_HEX,
        "caller_object_id": "obj-1",
        "content_sha256": content_sha256_hex,
        "pool_id": "scenario-a",
        "body_format": "rem-tar-v1",
    }


class _QueueLibrary:
    """Thread-safe advisory assignment source for bay-queue tests."""

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self.active = False
        self.loaded_tape_uuid = b""

    def GetLiveStatus(
        self, request: layer5_pb2.GetLiveStatusRequest, timeout: float | None = None
    ) -> layer5_pb2.GetLiveStatusResponse:
        with self._guard:
            return layer5_pb2.GetLiveStatusResponse(
                drive_assignments=[
                    layer5_pb2.DriveAssignment(
                        library_serial="LIB-A",
                        bay=0x101,
                        drive_uuid=bytes.fromhex("11" * 16),
                        state=(
                            layer5_pb2.DriveAssignment.State.Value("DRIVE_ASSIGNMENT_STATE_ACTIVE")
                            if self.active
                            else layer5_pb2.DriveAssignment.State.Value(
                                "DRIVE_ASSIGNMENT_STATE_IDLE"
                            )
                        ),
                        loaded_tape_uuid=self.loaded_tape_uuid,
                    )
                ]
            )

    def set_active(self, active: bool, tape_uuid: bytes = b"") -> None:
        with self._guard:
            self.active = active
            self.loaded_tape_uuid = tape_uuid if active else b""


class _QueueReadClient:
    """Read client that makes Open/Close visible through the advisory fake."""

    def __init__(self, library: _QueueLibrary) -> None:
        self.library = library
        self.open_count = 0
        self.opened = threading.Event()
        self.second_opened = threading.Event()

    def OpenReadSession(
        self, request: layer5_pb2.OpenReadSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.ReadSession:
        self.open_count += 1
        if self.open_count == 2:
            self.second_opened.set()
        tape_uuid = bytes(request.tape_target.tape_uuid)
        self.library.set_active(True, tape_uuid)
        self.opened.set()
        return layer5_pb2.ReadSession(
            session_id=f"session-{self.open_count}".encode(),
            tape_uuid=tape_uuid,
            drive_element_address=0x101,
        )

    def ReadObjectRange(
        self, request: layer5_pb2.ReadObjectRangeRequest
    ) -> Iterator[layer5_pb2.BytesChunk]:
        yield layer5_pb2.BytesChunk(data=b"payload", is_last=True)

    def CloseReadSession(
        self, request: layer5_pb2.CloseReadSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.ReadSession:
        self.library.set_active(False)
        return layer5_pb2.ReadSession(session_id=request.session_id)


class _LostRaceError(grpc.RpcError):  # type: ignore[misc]
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.FAILED_PRECONDITION

    def details(self) -> str:
        return "read session already active"


class _TapeMountedError(_LostRaceError):
    def details(self) -> str:
        return "tape is already mounted"


class _OtherPreconditionError(_LostRaceError):
    def details(self) -> str:
        return "tape is quarantined"


class _LostRaceReadClient(_QueueReadClient):
    def OpenReadSession(
        self, request: layer5_pb2.OpenReadSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.ReadSession:
        self.open_count += 1
        if self.open_count == 1:
            raise _LostRaceError()
        tape_uuid = bytes(request.tape_target.tape_uuid)
        self.library.set_active(True, tape_uuid)
        return layer5_pb2.ReadSession(
            session_id=b"won-after-requeue",
            tape_uuid=tape_uuid,
            drive_element_address=0x101,
        )


class _TapeMountedReadClient(_LostRaceReadClient):
    def OpenReadSession(
        self, request: layer5_pb2.OpenReadSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.ReadSession:
        if self.open_count == 0:
            self.open_count += 1
            raise _TapeMountedError()
        return super().OpenReadSession(request, timeout=timeout)


class _OtherPreconditionReadClient(_QueueReadClient):
    def OpenReadSession(
        self, request: layer5_pb2.OpenReadSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.ReadSession:
        self.open_count += 1
        raise _OtherPreconditionError()


class _RangeFailureReadClient(_QueueReadClient):
    def ReadObjectRange(
        self, request: layer5_pb2.ReadObjectRangeRequest
    ) -> Iterator[layer5_pb2.BytesChunk]:
        raise _LostRaceError()
        yield  # pragma: no cover - make this a streaming RPC-shaped method


def _queue_backend(
    library: _QueueLibrary,
    client: _QueueReadClient,
    *,
    read_open_deadline_seconds: float = 600.0,
) -> RemanenceBackend:
    backend = RemanenceBackend(
        "queued-rem-tape",
        endpoint="queue-test",
        read_session=client,
        read_open_deadline_seconds=read_open_deadline_seconds,
    )
    backend._drive_queue = remanence_module._DriveRestoreQueue(library, poll_seconds=0.001)
    return backend


def test_busy_bay_queues_second_tape_restore_until_first_closes() -> None:
    library = _QueueLibrary()
    client = _QueueReadClient(library)
    queued_backend = _queue_backend(library, client)
    release = [threading.Event(), threading.Event()]
    errors: list[BaseException] = []

    def restore(index: int, tape_byte: int) -> None:
        locator = _read_locator(hashlib.sha256(b"payload").hexdigest())
        locator["tape_uuid"] = (bytes([tape_byte]) * 16).hex()
        try:
            with queued_backend.open_read_session(locator):
                assert release[index].wait(timeout=2), "test did not release queued restore"
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    first = threading.Thread(target=restore, args=(0, 0x21))
    first.start()
    assert client.opened.wait(timeout=1), "first restore did not open"

    second = threading.Thread(target=restore, args=(1, 0x22))
    second.start()
    assert not client.second_opened.wait(timeout=0.05), (
        "second restore opened instead of queueing behind the busy bay"
    )

    release[0].set()
    assert client.second_opened.wait(timeout=1), "queued restore did not proceed after close"
    release[1].set()
    first.join(timeout=1)
    second.join(timeout=1)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert client.open_count == 2


def test_read_error_surfaces_opened_session_without_grpc_server() -> None:
    library = _QueueLibrary()
    client = _RangeFailureReadClient(library)
    backend = _queue_backend(library, client)

    with backend.open_read_session(_read_locator("11" * 32)) as reader:
        assert reader.session_id == b"session-1"
        assert reader.drive_element_address == 0x101
        with pytest.raises(BackendError) as raised:
            reader.read_range(ByteRange(0, 1))

    assert raised.value.session_id == b"session-1"  # type: ignore[attr-defined]
    assert raised.value.drive_element_address == 0x101  # type: ignore[attr-defined]


def test_lost_open_read_session_race_requeues_instead_of_failing() -> None:
    library = _QueueLibrary()
    client = _LostRaceReadClient(library)
    queued_backend = _queue_backend(library, client)
    locator = _read_locator(hashlib.sha256(b"payload").hexdigest())

    with queued_backend.open_read_session(locator) as session:
        assert session.session_id == b"won-after-requeue"
        assert session.drive_element_address == 0x101

    assert client.open_count == 2


def test_tape_already_mounted_by_another_session_requeues_and_releases_lease() -> None:
    library = _QueueLibrary()
    client = _TapeMountedReadClient(library)
    backend = _queue_backend(library, client)
    assert backend._drive_queue is not None
    real_acquire = backend._drive_queue.acquire
    leases: list[Any] = []

    def recording_acquire(tape_uuid: bytes, *args: Any, **kwargs: Any) -> Any:
        lease = real_acquire(tape_uuid, *args, **kwargs)
        leases.append(lease)
        return lease

    backend._drive_queue.acquire = recording_acquire  # type: ignore[method-assign]

    with backend.open_read_session(_read_locator(hashlib.sha256(b"payload").hexdigest())) as s:
        assert s.session_id == b"won-after-requeue"
        # The first attempt's lease was released before the second attempt began.
        assert len(leases) == 2
        assert leases[0]._released is True

    assert client.open_count == 2


def test_other_failed_precondition_on_open_still_fails_on_first_attempt() -> None:
    library = _QueueLibrary()
    client = _OtherPreconditionReadClient(library)
    backend = _queue_backend(library, client)

    with pytest.raises(BackendUnavailableError):
        backend.open_read_session(_read_locator("11" * 32))

    assert client.open_count == 1


def test_drive_uuid_float_does_not_change_library_bay_queue_key() -> None:
    tape_uuid = bytes.fromhex("33" * 16)
    before = [
        layer5_pb2.DriveAssignment(
            library_serial="LIB-A",
            bay=0x101,
            drive_uuid=bytes.fromhex("aa" * 16),
            state=layer5_pb2.DriveAssignment.State.Value("DRIVE_ASSIGNMENT_STATE_IDLE"),
        ),
        layer5_pb2.DriveAssignment(
            library_serial="LIB-A",
            bay=0x102,
            drive_uuid=bytes.fromhex("bb" * 16),
            state=layer5_pb2.DriveAssignment.State.Value("DRIVE_ASSIGNMENT_STATE_ACTIVE"),
            loaded_tape_uuid=tape_uuid,
        ),
    ]
    after_float = [
        layer5_pb2.DriveAssignment(
            library_serial="LIB-A",
            bay=0x101,
            drive_uuid=bytes.fromhex("bb" * 16),
            state=layer5_pb2.DriveAssignment.State.Value("DRIVE_ASSIGNMENT_STATE_IDLE"),
        ),
        layer5_pb2.DriveAssignment(
            library_serial="LIB-A",
            bay=0x102,
            drive_uuid=bytes.fromhex("aa" * 16),
            state=layer5_pb2.DriveAssignment.State.Value("DRIVE_ASSIGNMENT_STATE_ACTIVE"),
            loaded_tape_uuid=tape_uuid,
        ),
    ]

    selected_before = remanence_module._select_drive_assignment(before, tape_uuid)
    selected_after = remanence_module._select_drive_assignment(after_float, tape_uuid)

    assert selected_before is not None
    assert selected_after is not None
    assert (selected_before.library_serial, selected_before.bay) == ("LIB-A", 0x102)
    assert (selected_after.library_serial, selected_after.bay) == ("LIB-A", 0x102)


class _ReadSession(layer5_pb2_grpc.ReadSessionServiceServicer):
    """In-process fake of the daemon ReadSessionService."""

    def __init__(
        self,
        data: bytes,
        *,
        chunk_size: int = 1 << 20,
        read_error: grpc.StatusCode | None = None,
    ) -> None:
        self.data = data
        self.chunk_size = chunk_size
        self.read_error = read_error
        self.session_id = b"read-session-1"
        self.tape_uuid = bytes.fromhex(_TAPE_UUID_HEX)
        self.open_request: layer5_pb2.OpenReadSessionRequest | None = None
        self.read_requests: list[layer5_pb2.ReadObjectRangeRequest] = []
        self.opened = False
        self.closed = False

    def OpenReadSession(
        self,
        request: layer5_pb2.OpenReadSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.ReadSession:
        self.opened = True
        self.open_request = request
        return layer5_pb2.ReadSession(session_id=self.session_id, tape_uuid=self.tape_uuid)

    def ReadObjectRange(
        self,
        request: layer5_pb2.ReadObjectRangeRequest,
        context: grpc.ServicerContext,
    ) -> Iterator[layer5_pb2.BytesChunk]:
        self.read_requests.append(request)
        if self.read_error is not None:
            context.abort(self.read_error, "read failed")
            raise AssertionError("unreachable after context.abort")
        if request.start_byte == 0 and request.end_byte == 0:
            payload = self.data
        else:
            payload = self.data[request.start_byte : request.end_byte]
        for i in range(0, max(len(payload), 1), self.chunk_size):
            piece = payload[i : i + self.chunk_size]
            yield layer5_pb2.BytesChunk(
                data=piece,
                is_last=(i + self.chunk_size >= len(payload)),
            )

    def CloseReadSession(
        self,
        request: layer5_pb2.CloseReadSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.ReadSession:
        self.closed = True
        return layer5_pb2.ReadSession(session_id=self.session_id, tape_uuid=self.tape_uuid)


@contextmanager
def _serve_read(servicer: _ReadSession) -> Iterator[str]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    layer5_pb2_grpc.add_ReadSessionServiceServicer_to_server(servicer, server)  # type: ignore[no-untyped-call]
    with _serve_uds(server) as endpoint:
        yield endpoint


def test_read_range_whole_object() -> None:
    data = b"hello tape world"
    servicer = _ReadSession(data)
    with _serve_read(servicer) as endpoint:
        backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
        out = backend.read_range(
            _read_locator(hashlib.sha256(data).hexdigest()),
            ByteRange(0, 0),
        )

    assert out == data
    assert servicer.opened
    assert servicer.closed
    assert servicer.open_request is not None
    assert servicer.open_request.tape_target.tape_uuid == bytes.fromhex(_TAPE_UUID_HEX)
    req = servicer.read_requests[0]
    assert req.object_id == bytes.fromhex(_OBJECT_ID_HEX)
    assert (req.start_byte, req.end_byte) == (0, 0)


def test_read_range_reassembles_multiple_chunks() -> None:
    data = b"abcdefghij" * 3
    servicer = _ReadSession(data, chunk_size=8)
    with _serve_read(servicer) as endpoint:
        backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
        out = backend.read_range(
            _read_locator(hashlib.sha256(data).hexdigest()),
            ByteRange(0, 0),
        )
    assert out == data


def test_read_range_plumbs_byte_range() -> None:
    data = b"0123456789"
    servicer = _ReadSession(data)
    with _serve_read(servicer) as endpoint:
        backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
        out = backend.read_range(
            _read_locator(hashlib.sha256(data).hexdigest()),
            ByteRange(2, 5),
        )
    assert (servicer.read_requests[0].start_byte, servicer.read_requests[0].end_byte) == (
        2,
        5,
    )
    assert out == data[2:5]


def test_read_range_grpc_error_raises_unavailable() -> None:
    servicer = _ReadSession(b"x", read_error=grpc.StatusCode.INTERNAL)
    with _serve_read(servicer) as endpoint:
        backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
        with pytest.raises(BackendUnavailableError):
            backend.read_range(
                _read_locator(hashlib.sha256(b"x").hexdigest()),
                ByteRange(0, 0),
            )
    assert servicer.closed


def test_read_range_data_loss_raises_stored_corrupt_with_session_identity() -> None:
    servicer = _ReadSession(b"x", read_error=grpc.StatusCode.DATA_LOSS)
    with _serve_read(servicer) as endpoint:
        backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
        with pytest.raises(BackendStoredCorruptError) as caught:
            backend.read_range(
                _read_locator(hashlib.sha256(b"x").hexdigest()),
                ByteRange(0, 0),
            )
    exc = caught.value
    assert not isinstance(exc, BackendUnavailableError)
    assert exc.session_id == servicer.session_id  # type: ignore[attr-defined]
    assert exc.drive_element_address == 0  # type: ignore[attr-defined]
    assert servicer.closed


def test_verify_ok_when_hash_matches() -> None:
    data = b"verify me please"
    servicer = _ReadSession(data)
    with _serve_read(servicer) as endpoint:
        backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
        result = backend.verify(_read_locator(hashlib.sha256(data).hexdigest()))
    assert result.ok is True
    assert result.actual_hash == hashlib.sha256(data).digest()


def test_verify_mismatch_returns_not_ok() -> None:
    data = b"actual bytes on tape"
    servicer = _ReadSession(data)
    with _serve_read(servicer) as endpoint:
        backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
        result = backend.verify(_read_locator(hashlib.sha256(b"different").hexdigest()))
    assert result.ok is False
    assert result.actual_hash == hashlib.sha256(data).digest()


def test_verify_grpc_error_propagates() -> None:
    servicer = _ReadSession(b"x", read_error=grpc.StatusCode.INTERNAL)
    with _serve_read(servicer) as endpoint:
        backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
        with pytest.raises(BackendUnavailableError):
            backend.verify(_read_locator(hashlib.sha256(b"x").hexdigest()))


def test_live_endpoint_unavailable_raises_backend_unavailable(tmp_path: Path) -> None:
    endpoint = f"unix:{tmp_path / 'missing.sock'}"
    backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
    with pytest.raises(BackendUnavailableError, match=r"missing\.sock"):
        list(backend.enumerate())


@pytest.mark.parametrize(
    "endpoint", ["127.0.0.1:50051", "http://localhost:50051", "https://rem:50051"]
)
def test_from_grpc_rejects_tcp_without_mtls(endpoint: str) -> None:
    with pytest.raises(ValueError, match="TCP requires mTLS"):
        RemanenceBackend.from_grpc("primary-tape", endpoint)


def test_unix_grpc_endpoint_sets_dummy_authority() -> None:
    assert remanence_module._grpc_channel_options("unix:/var/lib/replica/rem/rem.sock") == (
        ("grpc.default_authority", "127.0.0.1:50051"),
    )
    assert remanence_module._grpc_channel_options("127.0.0.1:50051") == ()


class _WriteSession(layer5_pb2_grpc.WriteSessionServiceServicer):
    """In-process fake of the daemon WriteSessionService for unit tests."""

    def __init__(
        self,
        obj: layer5_pb2.ObjectRecord,
        tape_uuid: bytes,
        *,
        append_error: grpc.StatusCode | None = None,
    ) -> None:
        self.obj = obj
        self.tape_uuid = tape_uuid
        self.append_error = append_error
        self.session_id = b"session-0001"
        self.open_request: layer5_pb2.OpenWriteSessionRequest | None = None
        self.appended: list[layer5_pb2.AppendObjectMessage] = []
        self.opened = False
        self.checkpointed = False
        self.closed = False
        self.aborted = False
        self.abort_reason: str | None = None
        self.pending: list[layer5_pb2.ObjectRecord] = []

    def OpenWriteSession(
        self,
        request: layer5_pb2.OpenWriteSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.WriteSession:
        self.opened = True
        self.open_request = request
        return layer5_pb2.WriteSession(session_id=self.session_id, tape_uuid=self.tape_uuid)

    def AppendObject(
        self,
        request_iterator: Iterator[layer5_pb2.AppendObjectMessage],
        context: grpc.ServicerContext,
    ) -> layer5_pb2.ObjectRecord:
        caller_object_id = ""
        finished = False
        for msg in request_iterator:
            self.appended.append(msg)
            if msg.HasField("start"):
                caller_object_id = msg.start.caller_object_id
            elif msg.HasField("finish"):
                finished = True
        if not finished:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "missing Finish")
        if self.append_error is not None:
            context.abort(self.append_error, "append failed")
            raise AssertionError("unreachable after context.abort")
        self.pending.append(_committed_object(self.obj, caller_object_id))
        return _written_ack(caller_object_id, object_id=self.obj.object_id)

    def CheckpointSession(
        self,
        request: layer5_pb2.CheckpointSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.CheckpointSessionResponse:
        self.checkpointed = True
        committed = list(self.pending)
        self.pending.clear()
        return _checkpoint_response(self.session_id, self.tape_uuid, committed)

    def CloseWriteSession(
        self,
        request: layer5_pb2.CloseWriteSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.WriteSession:
        self.closed = True
        committed = list(self.pending)
        self.pending.clear()
        return _write_session_response(self.session_id, self.tape_uuid, committed)

    def AbortWriteSession(
        self,
        request: layer5_pb2.AbortWriteSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.WriteSession:
        self.aborted = True
        self.abort_reason = request.reason
        return layer5_pb2.WriteSession(session_id=self.session_id, tape_uuid=self.tape_uuid)


class _DirectWriteError(grpc.RpcError):  # type: ignore[misc]
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.INTERNAL

    def details(self) -> str:
        return "direct append stopped"


class _DirectWriteClient:
    """Protocol-shaped write client that needs no listening socket."""

    def __init__(self, obj: layer5_pb2.ObjectRecord, *, raise_on_append: bool = False) -> None:
        self.obj = obj
        self.raise_on_append = raise_on_append
        self.session_id = b"direct-write-session"
        self.drive_element_address = 0x102
        self.aborted = False
        self.appended: list[layer5_pb2.AppendObjectMessage] = []
        self.pending: list[layer5_pb2.ObjectRecord] = []

    def OpenWriteSession(
        self, request: layer5_pb2.OpenWriteSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.WriteSession:
        return layer5_pb2.WriteSession(
            session_id=self.session_id,
            tape_uuid=self.obj.copies[0].tape_uuid,
            drive_element_address=self.drive_element_address,
        )

    def AppendObject(
        self,
        request_iterator: Iterator[layer5_pb2.AppendObjectMessage],
        timeout: float | None = None,
    ) -> layer5_pb2.ObjectRecord:
        messages = []
        for message in request_iterator:
            messages.append(message)
            self.appended.append(message)
        if self.raise_on_append:
            raise _DirectWriteError()
        caller_object_id = messages[0].start.caller_object_id
        self.pending.append(_committed_object(self.obj, caller_object_id))
        return _written_ack(caller_object_id, object_id=self.obj.object_id)

    def CheckpointSession(
        self, request: layer5_pb2.CheckpointSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.CheckpointSessionResponse | layer5_pb2.WriteSession:
        committed = list(self.pending)
        self.pending.clear()
        return _checkpoint_response(self.session_id, self.obj.copies[0].tape_uuid, committed)

    def CloseWriteSession(
        self, request: layer5_pb2.CloseWriteSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.WriteSession:
        committed = list(self.pending)
        self.pending.clear()
        return _write_session_response(
            self.session_id,
            self.obj.copies[0].tape_uuid,
            committed,
        )

    def AbortWriteSession(
        self, request: layer5_pb2.AbortWriteSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.WriteSession:
        self.aborted = True
        return layer5_pb2.WriteSession(session_id=request.session_id)


class _MalformedCommittedWriteClient(_DirectWriteClient):
    """Return a committed response whose written copy cannot be selected."""

    def CheckpointSession(
        self, request: layer5_pb2.CheckpointSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.CheckpointSessionResponse:
        malformed = layer5_pb2.ObjectRecord(
            object_id=bytes.fromhex("20" * 16),
            caller_object_id=self.pending[0].caller_object_id,
            content_sha256=bytes.fromhex("30" * 32),
            logical_size_bytes=4,
            body_format="raw-bytes",
        )
        self.pending.clear()
        return layer5_pb2.CheckpointSessionResponse(
            session=layer5_pb2.WriteSession(session_id=request.session_id),
            committed_objects=[malformed],
            committed_copies=[],
        )


class _LegacyWriteSessionClient(_DirectWriteClient):
    """Model the documented pre-checkpoint WriteSession response compatibility seam."""

    def __init__(
        self,
        obj: layer5_pb2.ObjectRecord,
        *,
        modern_checkpoint_response: bool = False,
    ) -> None:
        super().__init__(obj)
        self.modern_checkpoint_response = modern_checkpoint_response

    def AppendObject(
        self,
        request_iterator: Iterator[layer5_pb2.AppendObjectMessage],
        timeout: float | None = None,
    ) -> layer5_pb2.ObjectRecord:
        list(request_iterator)
        legacy = layer5_pb2.ObjectRecord()
        legacy.CopyFrom(self.obj)
        legacy.ClearField("append_commit_info")
        return legacy

    def CheckpointSession(
        self, request: layer5_pb2.CheckpointSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.CheckpointSessionResponse | layer5_pb2.WriteSession:
        if self.modern_checkpoint_response:
            return layer5_pb2.CheckpointSessionResponse(
                session=layer5_pb2.WriteSession(session_id=request.session_id)
            )
        return layer5_pb2.WriteSession(session_id=request.session_id)


class _CheckpointBatchClient:
    """Direct generated-type client for checkpoint batch behavior."""

    def __init__(self, *, fail_checkpoint: bool = False) -> None:
        self.session_id = b"checkpoint-session"
        self.tape_uuid = bytes.fromhex("a1" * 16)
        self.fail_checkpoint = fail_checkpoint
        self.pending: list[layer5_pb2.ObjectRecord] = []
        self.aborted = False

    def OpenWriteSession(
        self, request: layer5_pb2.OpenWriteSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.WriteSession:
        return layer5_pb2.WriteSession(
            session_id=self.session_id,
            tape_uuid=self.tape_uuid,
            drive_element_address=0x202,
        )

    def AppendObject(
        self, messages: Iterator[layer5_pb2.AppendObjectMessage], timeout: float | None = None
    ) -> layer5_pb2.ObjectRecord:
        caller_object_id = ""
        data = bytearray()
        for message in messages:
            if message.HasField("start"):
                caller_object_id = message.start.caller_object_id
            elif message.HasField("chunk"):
                data.extend(message.chunk.data)
        ordinal = len(self.pending) + 1
        digest = hashlib.sha256(data).digest()
        self.pending.append(
            layer5_pb2.ObjectRecord(
                object_id=ordinal.to_bytes(16),
                caller_object_id=caller_object_id,
                content_sha256=digest,
                logical_size_bytes=len(data),
                body_format="raw-bytes",
                copies=[
                    layer5_pb2.ObjectCopy(
                        tape_uuid=self.tape_uuid,
                        tape_file_number=ordinal,
                        first_body_lba=1,
                        health=layer5_pb2.ObjectCopy.OBJECT_COPY_HEALTH_OK,
                        pool_id="checkpoint-pool",
                    )
                ],
                append_commit_info=layer5_pb2.AppendCommitInfo(
                    append_mode=layer5_pb2.APPEND_MODE_FRESH,
                    tape_uuid=self.tape_uuid,
                    tape_file_number=ordinal,
                    first_body_lba=1,
                    durability=layer5_pb2.APPEND_DURABILITY_CHECKPOINTED,
                ),
            )
        )
        return _written_ack(
            caller_object_id,
            object_id=ordinal.to_bytes(16),
            ordinal=ordinal,
        )

    def CheckpointSession(
        self, request: layer5_pb2.CheckpointSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.CheckpointSessionResponse:
        if self.fail_checkpoint:
            raise _DirectWriteError()
        committed = list(self.pending)
        self.pending.clear()
        return _checkpoint_response(self.session_id, self.tape_uuid, committed)

    def CloseWriteSession(
        self, request: layer5_pb2.CloseWriteSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.WriteSession:
        committed = list(self.pending)
        self.pending.clear()
        return _write_session_response(self.session_id, self.tape_uuid, committed)

    def AbortWriteSession(
        self, request: layer5_pb2.AbortWriteSessionRequest, timeout: float | None = None
    ) -> layer5_pb2.WriteSession:
        self.aborted = True
        return layer5_pb2.WriteSession(session_id=request.session_id)


@contextmanager
def _serve_write(servicer: _WriteSession) -> Iterator[str]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    layer5_pb2_grpc.add_WriteSessionServiceServicer_to_server(servicer, server)  # type: ignore[no-untyped-call]
    with _serve_uds(server) as endpoint:
        yield endpoint


@pytest.fixture
def write_server(
    proto_object: layer5_pb2.ObjectRecord,
) -> Iterator[tuple[str, _WriteSession]]:
    servicer = _WriteSession(proto_object, tape_uuid=proto_object.copies[0].tape_uuid)
    with _serve_write(servicer) as endpoint:
        yield endpoint, servicer


def test_write_requires_live_mode(backend: RemanenceBackend, tmp_path: Path) -> None:
    src = tmp_path / "obj.bin"
    src.write_bytes(b"data")
    with pytest.raises(BackendError):
        backend.write_object_to_pool(src, "scenario-a")


def test_write_result_and_error_surface_opened_session_without_grpc_server(
    proto_object: layer5_pb2.ObjectRecord,
    tmp_path: Path,
) -> None:
    src = tmp_path / "obj.bin"
    src.write_bytes(b"data")
    success_client = _DirectWriteClient(proto_object)
    backend = RemanenceBackend(
        "direct-rem",
        endpoint="direct",
        write_session=success_client,
    )

    result = backend.write_object_to_pool(src, "scenario-a")

    assert result.session_id == success_client.session_id
    assert result.drive_element_address == success_client.drive_element_address
    assert result.copy_record.native_locator == result.native_locator

    error_client = _DirectWriteClient(proto_object, raise_on_append=True)
    backend = RemanenceBackend(
        "direct-rem",
        endpoint="direct",
        write_session=error_client,
    )
    with pytest.raises(RemanenceWriteSessionError) as raised:
        backend.write_object_to_pool(src, "scenario-a")
    assert raised.value.session_id == error_client.session_id
    assert raised.value.drive_element_address == error_client.drive_element_address
    assert error_client.aborted


def test_pre_checkpoint_write_session_response_uses_legacy_append_record(
    proto_object: layer5_pb2.ObjectRecord,
    tmp_path: Path,
) -> None:
    """The documented legacy WriteSession fallback remains isolated and covered."""

    src = tmp_path / "legacy.bin"
    src.write_bytes(b"legacy checkpoint response")
    client = _LegacyWriteSessionClient(proto_object)
    backend = RemanenceBackend(
        "direct-rem",
        endpoint="direct",
        write_session=client,
    )

    result = backend.write_object_to_pool(src, "scenario-a")

    assert result.session_id == client.session_id
    assert result.native_locator["object_id"] == proto_object.object_id.hex()

    modern_client = _LegacyWriteSessionClient(
        proto_object,
        modern_checkpoint_response=True,
    )
    modern_backend = RemanenceBackend(
        "direct-rem",
        endpoint="direct",
        write_session=modern_client,
    )
    with pytest.raises(
        RemanenceWriteSessionError,
        match="did not commit every pending caller object",
    ):
        modern_backend.write_object_to_pool(src, "scenario-a")


def test_malformed_committed_write_response_keeps_opened_session_identity(
    proto_object: layer5_pb2.ObjectRecord,
    tmp_path: Path,
) -> None:
    src = tmp_path / "obj.bin"
    src.write_bytes(b"data")
    client = _MalformedCommittedWriteClient(proto_object)
    backend = RemanenceBackend(
        "direct-rem",
        endpoint="direct",
        write_session=client,
    )

    with pytest.raises(RemanenceWriteSessionError, match="returned 0 copies") as raised:
        backend.write_object_to_pool(src, "scenario-a")

    assert raised.value.session_id == client.session_id
    assert raised.value.drive_element_address == client.drive_element_address
    assert client.aborted


def test_batch_append_is_advisory_until_checkpoint(tmp_path: Path) -> None:
    first = tmp_path / "first.bin"
    second = tmp_path / "second.bin"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    client = _CheckpointBatchClient()
    backend = RemanenceBackend(
        "direct-rem",
        endpoint="direct",
        write_session=client,
    )

    batch = backend.open_batch("checkpoint-pool")
    first_receipt = batch.append(first, "object-1")
    second_receipt = batch.append(second, "object-2")

    assert first_receipt == WrittenReceipt(batch_id="62617463682d31", provisional_ordinal=1)
    assert second_receipt == WrittenReceipt(batch_id="62617463682d31", provisional_ordinal=2)
    committed = batch.checkpoint()
    assert all(isinstance(item, CommittedCopy) for item in committed)
    assert [item.caller_object_id for item in committed] == ["object-1", "object-2"]
    assert [item.native_locator["tape_file_number"] for item in committed] == [1, 2]
    assert batch.close() == []


def test_batch_failure_requeues_every_outstanding_object_from_byte_zero(
    tmp_path: Path,
) -> None:
    sources = [tmp_path / "one.bin", tmp_path / "two.bin"]
    for index, source in enumerate(sources):
        source.write_bytes(bytes([index]))
    client = _CheckpointBatchClient(fail_checkpoint=True)
    backend = RemanenceBackend(
        "direct-rem",
        endpoint="direct",
        write_session=client,
    )
    batch = backend.open_batch("checkpoint-pool")
    batch.append(sources[0], "object-1")
    batch.append(sources[1], "object-2")

    with pytest.raises(RemanenceWriteSessionError) as raised:
        batch.checkpoint()

    assert client.aborted
    assert [item.caller_object_id for item in raised.value.requeue_objects] == [
        "object-1",
        "object-2",
    ]
    assert [item.source for item in raised.value.requeue_objects] == sources
    assert [item.provisional_ordinal for item in raised.value.requeue_objects] == [1, 2]


def test_batch_of_one_preserves_existing_write_result_contract(tmp_path: Path) -> None:
    source = tmp_path / "one.bin"
    source.write_bytes(b"one object")
    client = _CheckpointBatchClient()
    backend = RemanenceBackend(
        "direct-rem",
        endpoint="direct",
        write_session=client,
    )

    result = backend.write_object_to_pool(source, "checkpoint-pool")

    assert isinstance(result, RemanenceWriteResult)
    assert result.session_id == client.session_id
    assert result.drive_element_address == 0x202
    assert result.logical_id == hashlib.sha256(b"one object").digest()
    assert result.native_locator["tape_file_number"] == 1
    assert client.pending == []


def test_write_object_to_pool_success(
    write_server: tuple[str, _WriteSession],
    tmp_path: Path,
) -> None:
    endpoint, servicer = write_server
    src = tmp_path / "obj.bin"
    src.write_bytes(b"hello world")

    wb = RemanenceBackend.from_grpc("primary-tape", endpoint)
    record = wb.write_object_to_pool(src, "scenario-a")

    assert isinstance(record, RemanenceWriteResult)
    assert isinstance(record.copy_record, CopyRecord)
    assert record.session_id == servicer.session_id
    assert record.drive_element_address == 0
    assert record.metadata["append_commit_info"]["append_mode"] == "fresh"
    assert record.metadata["append_commit_info"]["tape_file_number"] == 1
    assert servicer.opened
    assert servicer.checkpointed
    assert servicer.closed
    assert not servicer.aborted
    assert servicer.open_request is not None
    assert servicer.open_request.pool_target.pool_id == "scenario-a"
    assert servicer.open_request.pool_target.mount_if_needed is True


@pytest.mark.parametrize("with_digest", [False, True])
@pytest.mark.parametrize(
    "data", [b"", b"a" * (2 * 1024 * 1024 + 512 * 1024)], ids=["empty", "multichunk"]
)
def test_append_start_digest_fields(
    proto_object: layer5_pb2.ObjectRecord,
    tmp_path: Path,
    with_digest: bool,
    data: bytes,
) -> None:
    """Both write modes preserve the wire fields, including a declared empty file."""

    client = _DirectWriteClient(proto_object)
    src = tmp_path / "big.bin"
    src.write_bytes(data)
    digest = hashlib.sha256(data).digest()

    RemanenceBackend("primary-tape", write_session=client).write_object_to_pool(
        src, "scenario-a", content_sha256=digest if with_digest else None
    )

    msgs = client.appended
    start = layer5_pb2.AppendObjectStart(
        session_id=client.session_id,
        caller_object_id=src.name,
        declared_size_bytes=len(data),
    )
    if with_digest:
        start.expected_content_sha256 = digest
        start.expected_content_digest.CopyFrom(layer5_pb2.Digest(algorithm="sha256", value=digest))
        start.source_replay_capability = layer5_pb2.SOURCE_REPLAY_CAPABILITY_REPLAY_FROM_START
    assert msgs[0] == layer5_pb2.AppendObjectMessage(start=start)
    assert msgs[-1] == layer5_pb2.AppendObjectMessage(
        finish=layer5_pb2.AppendObjectFinish(
            session_id=client.session_id,
            expected_content_sha256=digest,
            expected_content_digest=layer5_pb2.Digest(algorithm="sha256", value=digest),
        )
    )
    assert sum(1 for msg in msgs if msg.HasField("chunk")) == (3 if data else 0)
    assert b"".join(msg.chunk.data for msg in msgs[1:-1]) == data


@pytest.mark.parametrize("length", [0, 1, 31, 33, 64])
@pytest.mark.parametrize("entrypoint", ["write", "batch", "messages"])
def test_malformed_digest_is_refused_before_any_message(
    proto_object: layer5_pb2.ObjectRecord,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    length: int,
    entrypoint: str,
) -> None:
    """Digest validation precedes session creation, append RPCs and source access."""

    client = _DirectWriteClient(proto_object)
    backend = RemanenceBackend("primary-tape", write_session=client)
    source = tmp_path / "missing.bin"
    digest = b"x" * length
    monkeypatch.setattr(client, "AppendObject", lambda _: pytest.fail("AppendObject is called"))
    if entrypoint == "write":
        monkeypatch.setattr(backend, "open_batch", lambda _: pytest.fail("open_batch is called"))
        with pytest.raises(ValueError, match="content_sha256 must be exactly 32 bytes"):
            backend.write_object_to_pool(source, "scenario-a", content_sha256=digest)
    elif entrypoint == "batch":
        batch = backend.open_batch("scenario-a")
        try:
            with pytest.raises(ValueError, match="content_sha256 must be exactly 32 bytes"):
                batch.append(source, source.name, content_sha256=digest)
        finally:
            batch.abort("validation complete")
    else:
        messages = backend._append_messages(
            client.session_id, source, source.name, content_sha256=digest
        )
        with pytest.raises(ValueError, match="content_sha256 must be exactly 32 bytes"):
            next(messages)


def test_write_streams_start_chunks_finish(
    write_server: tuple[str, _WriteSession],
    tmp_path: Path,
) -> None:
    endpoint, servicer = write_server
    data = b"a" * (2 * 1024 * 1024 + 512 * 1024)
    src = tmp_path / "big.bin"
    src.write_bytes(data)

    RemanenceBackend.from_grpc("primary-tape", endpoint).write_object_to_pool(src, "scenario-a")

    msgs = servicer.appended
    assert msgs[0].HasField("start")
    assert msgs[-1].HasField("finish")
    assert sum(1 for msg in msgs if msg.HasField("chunk")) == 3
    assert msgs[0].start.declared_size_bytes == len(data)
    assert msgs[-1].finish.expected_content_sha256 == hashlib.sha256(data).digest()


@pytest.mark.parametrize("rpc_error", [False, True])
def test_write_digest_mismatch_ends_request_iterator_cleanly(
    proto_object: layer5_pb2.ObjectRecord, tmp_path: Path, rpc_error: bool
) -> None:
    """The iterator ends normally; either RPC outcome exposes the local mismatch."""

    class ExhaustingWriteClient(_DirectWriteClient):
        """Consume the requests and optionally reject the missing Finish."""

        def AppendObject(
            self,
            request_iterator: Iterator[layer5_pb2.AppendObjectMessage],
            timeout: float | None = None,
        ) -> layer5_pb2.ObjectRecord:
            with futures.ThreadPoolExecutor(max_workers=1) as executor:
                self.appended.extend(executor.submit(list, request_iterator).result())
            if rpc_error:
                raise _DirectWriteError()
            return _written_ack(
                self.appended[0].start.caller_object_id, object_id=self.obj.object_id
            )

    client = ExhaustingWriteClient(proto_object)
    backend = RemanenceBackend("primary-tape", write_session=client)
    source = tmp_path / "changed.bin"
    source.write_bytes(b"changed bytes")
    supplied = hashlib.sha256(b"original bytes").digest()
    streamed = hashlib.sha256(source.read_bytes()).digest()

    with pytest.raises(RemanenceDigestMismatchError) as raised:
        backend.write_object_to_pool(source, "scenario-a", content_sha256=supplied)

    assert not isinstance(raised.value, BackendUnavailableError)
    assert supplied.hex() in str(raised.value)
    assert streamed.hex() in str(raised.value)
    assert client.aborted
    assert client.pending == []
    assert [msg.WhichOneof("payload") for msg in client.appended] == ["start", "chunk"]


def test_append_digest_mismatch_omits_finish(tmp_path: Path) -> None:
    """A changed file ends the iterator with both hashes and no Finish."""

    source = tmp_path / "changed.bin"
    supplied = hashlib.sha256(b"original bytes").digest()
    source.write_bytes(b"changed bytes")
    streamed = hashlib.sha256(source.read_bytes()).digest()
    messages = RemanenceBackend("rem")._append_messages(
        b"session", source, source.name, content_sha256=supplied
    )
    seen = []
    with pytest.raises(RemanenceDigestMismatchError) as raised:
        seen.extend(messages)

    assert [msg.WhichOneof("payload") for msg in seen] == ["start", "chunk"]
    assert supplied.hex() in str(raised.value)
    assert streamed.hex() in str(raised.value)


def test_write_digest_mismatch_aborts_over_grpc(
    write_server: tuple[str, _WriteSession], tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The gRPC boundary preserves mismatch details and never checkpoints."""

    endpoint, servicer = write_server
    source = tmp_path / "changed.bin"
    supplied = hashlib.sha256(b"original bytes").digest()
    source.write_bytes(b"changed bytes")
    streamed = hashlib.sha256(source.read_bytes()).digest()
    backend = RemanenceBackend.from_grpc("primary-tape", endpoint)

    with pytest.raises(RemanenceDigestMismatchError) as raised:
        backend.write_object_to_pool(source, "scenario-a", content_sha256=supplied)

    assert not isinstance(raised.value, BackendUnavailableError)
    assert "Exception iterating requests!" not in caplog.text
    assert [msg.WhichOneof("payload") for msg in servicer.appended] == ["start", "chunk"]
    assert supplied.hex() in str(raised.value)
    assert streamed.hex() in str(raised.value)
    assert servicer.aborted
    assert servicer.abort_reason is not None
    assert supplied.hex() in servicer.abort_reason
    assert streamed.hex() in servicer.abort_reason
    assert not any(msg.HasField("finish") for msg in servicer.appended)
    assert not servicer.checkpointed
    assert not servicer.closed
    assert servicer.pending == []
    assert source.read_bytes() == b"changed bytes"


def test_interrupted_append_replays_same_file_from_zero(
    proto_object: layer5_pb2.ObjectRecord, tmp_path: Path
) -> None:
    """A requeued source opens afresh and sends every byte on a new append."""

    class InterruptedWriteClient(_DirectWriteClient):
        """Interrupt the first append after one chunk and accept the replay."""

        def __init__(self) -> None:
            super().__init__(proto_object)
            self.attempts: list[list[layer5_pb2.AppendObjectMessage]] = []

        def AppendObject(
            self,
            request_iterator: Iterator[layer5_pb2.AppendObjectMessage],
            timeout: float | None = None,
        ) -> layer5_pb2.ObjectRecord:
            messages: list[layer5_pb2.AppendObjectMessage] = []
            self.attempts.append(messages)
            for message in request_iterator:
                messages.append(message)
                if len(self.attempts) == 1 and message.HasField("chunk"):
                    raise _DirectWriteError()
            return super().AppendObject(iter(messages))

    client = InterruptedWriteClient()
    backend = RemanenceBackend("primary-tape", write_session=client)
    source = tmp_path / "replay.bin"
    data = b"a" * (1024 * 1024) + b"tail"
    source.write_bytes(data)
    digest = hashlib.sha256(data).digest()
    with pytest.raises(RemanenceWriteSessionError) as raised:
        backend.write_object_to_pool(source, "scenario-a", content_sha256=digest)

    assert client.aborted
    assert source.read_bytes() == data
    [requeued] = raised.value.requeue_objects
    assert requeued.source == source
    backend.write_object_to_pool(
        requeued.source,
        "scenario-a",
        caller_object_id=requeued.caller_object_id,
        content_sha256=digest,
    )

    first, replay = client.attempts
    assert [message.WhichOneof("payload") for message in first] == ["start", "chunk"]
    assert replay[0] == first[0]
    assert b"".join(message.chunk.data for message in replay[1:-1]) == data
    assert replay[-1].finish.expected_content_sha256 == digest
    assert source.read_bytes() == data


def test_write_aborts_on_stream_error(
    proto_object: layer5_pb2.ObjectRecord,
    tmp_path: Path,
) -> None:
    servicer = _WriteSession(
        proto_object,
        tape_uuid=proto_object.copies[0].tape_uuid,
        append_error=grpc.StatusCode.INTERNAL,
    )
    src = tmp_path / "obj.bin"
    src.write_bytes(b"data")

    with _serve_write(servicer) as endpoint:
        backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
        with pytest.raises(RemanenceWriteSessionError) as raised:
            backend.write_object_to_pool(src, "scenario-a")

    assert servicer.aborted is True
    assert raised.value.session_id == servicer.session_id
    assert raised.value.drive_element_address == 0


def test_write_object_locator_parity_with_enumerate(
    write_server: tuple[str, _WriteSession],
    catalog_server: tuple[str, _Catalog],
    proto_object: layer5_pb2.ObjectRecord,
    tmp_path: Path,
) -> None:
    write_endpoint, _ = write_server
    catalog_endpoint, _ = catalog_server
    src = tmp_path / proto_object.caller_object_id
    src.write_bytes(b"x" * 10)

    written = RemanenceBackend.from_grpc("primary-tape", write_endpoint).write_object_to_pool(
        src, "scenario-a"
    )
    [enumerated] = list(RemanenceBackend.from_grpc("primary-tape", catalog_endpoint).enumerate())

    assert written.native_locator == enumerated.native_locator
    assert locator_key(written.native_locator) == locator_key(enumerated.native_locator)
    assert written.copy_record == enumerated


class _RoundTripStore:
    def __init__(self) -> None:
        self.objects: dict[bytes, bytes] = {}
        self.tape_uuid = bytes.fromhex(_TAPE_UUID_HEX)
        self.object_id = bytes.fromhex(_OBJECT_ID_HEX)


class _RTWrite(layer5_pb2_grpc.WriteSessionServiceServicer):
    def __init__(self, store: _RoundTripStore) -> None:
        self.store = store
        self.session_id = b"rt-write-1"
        self.pending: layer5_pb2.ObjectRecord | None = None

    def OpenWriteSession(
        self,
        request: layer5_pb2.OpenWriteSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.WriteSession:
        return layer5_pb2.WriteSession(
            session_id=self.session_id,
            tape_uuid=self.store.tape_uuid,
        )

    def AppendObject(
        self,
        request_iterator: Iterator[layer5_pb2.AppendObjectMessage],
        context: grpc.ServicerContext,
    ) -> layer5_pb2.ObjectRecord:
        buf = bytearray()
        caller_object_id = ""
        for msg in request_iterator:
            if msg.HasField("start"):
                caller_object_id = msg.start.caller_object_id
            elif msg.HasField("chunk"):
                buf.extend(msg.chunk.data)
        data = bytes(buf)
        self.store.objects[self.store.object_id] = data
        self.pending = layer5_pb2.ObjectRecord(
            object_id=self.store.object_id,
            caller_object_id=caller_object_id,
            content_sha256=hashlib.sha256(data).digest(),
            logical_size_bytes=len(data),
            body_format="rem-tar-v1",
            copies=[
                layer5_pb2.ObjectCopy(
                    tape_uuid=self.store.tape_uuid,
                    tape_file_number=1,
                    first_body_lba=1,
                    health=layer5_pb2.ObjectCopy.OBJECT_COPY_HEALTH_OK,
                    pool_id="scenario-a",
                )
            ],
            append_commit_info=layer5_pb2.AppendCommitInfo(
                append_mode=layer5_pb2.APPEND_MODE_FRESH,
                tape_uuid=self.store.tape_uuid,
                tape_file_number=1,
                first_body_lba=1,
                durability=layer5_pb2.APPEND_DURABILITY_CHECKPOINTED,
            ),
        )
        return _written_ack(
            caller_object_id,
            object_id=self.store.object_id,
        )

    def CloseWriteSession(
        self,
        request: layer5_pb2.CloseWriteSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.WriteSession:
        committed = [] if self.pending is None else [self.pending]
        self.pending = None
        return _write_session_response(
            self.session_id,
            self.store.tape_uuid,
            committed,
        )

    def CheckpointSession(
        self,
        request: layer5_pb2.CheckpointSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.CheckpointSessionResponse:
        committed = [] if self.pending is None else [self.pending]
        self.pending = None
        return _checkpoint_response(
            self.session_id,
            self.store.tape_uuid,
            committed,
        )

    def AbortWriteSession(
        self,
        request: layer5_pb2.AbortWriteSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.WriteSession:
        return layer5_pb2.WriteSession(
            session_id=self.session_id,
            tape_uuid=self.store.tape_uuid,
        )


class _RTRead(layer5_pb2_grpc.ReadSessionServiceServicer):
    def __init__(self, store: _RoundTripStore) -> None:
        self.store = store
        self.session_id = b"rt-read-1"

    def OpenReadSession(
        self,
        request: layer5_pb2.OpenReadSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.ReadSession:
        return layer5_pb2.ReadSession(
            session_id=self.session_id,
            tape_uuid=self.store.tape_uuid,
        )

    def ReadObjectRange(
        self,
        request: layer5_pb2.ReadObjectRangeRequest,
        context: grpc.ServicerContext,
    ) -> Iterator[layer5_pb2.BytesChunk]:
        data = self.store.objects.get(request.object_id, b"")
        if request.start_byte == 0 and request.end_byte == 0:
            payload = data
        else:
            payload = data[request.start_byte : request.end_byte]
        yield layer5_pb2.BytesChunk(data=payload, is_last=True)

    def CloseReadSession(
        self,
        request: layer5_pb2.CloseReadSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.ReadSession:
        return layer5_pb2.ReadSession(
            session_id=self.session_id,
            tape_uuid=self.store.tape_uuid,
        )


@contextmanager
def _serve_roundtrip(store: _RoundTripStore) -> Iterator[str]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    layer5_pb2_grpc.add_WriteSessionServiceServicer_to_server(_RTWrite(store), server)  # type: ignore[no-untyped-call]
    layer5_pb2_grpc.add_ReadSessionServiceServicer_to_server(_RTRead(store), server)  # type: ignore[no-untyped-call]
    with _serve_uds(server) as endpoint:
        yield endpoint


def test_write_then_read_roundtrip_byte_equal(tmp_path: Path) -> None:
    data = b"round trip bytes " * 100
    src = tmp_path / "obj.bin"
    src.write_bytes(data)
    store = _RoundTripStore()

    with _serve_roundtrip(store) as endpoint:
        backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
        locator = backend.write_object_to_pool(src, "scenario-a").native_locator
        out = backend.read_range(locator, ByteRange(0, 0))

    assert out == data


def test_commit_point_suspends_batch_observers_during_live_write(tmp_path, monkeypatch):
    """T5b: a real batch-of-one write commits without touching the guarded job row."""
    from copy import deepcopy

    from sutradhara.catalog.session import create_all, make_engine, session_scope
    from sutradhara.jobs import registry
    from sutradhara.jobs.commit_point import commit_point_for
    from sutradhara.jobs.config import WorkerConfig
    from sutradhara.jobs.engine import submit
    from sutradhara.jobs.models import Job
    from sutradhara.jobs.registry import CHECKPOINT_BATCH_STATE_KEY, JobResult
    from sutradhara.jobs.worker import JobWorker

    engine = make_engine(f"sqlite:///{tmp_path / 'observer.db'}")
    create_all(engine)
    source = tmp_path / "artifact.bin"
    source.write_bytes(b"checkpoint window proof")
    client = _CheckpointBatchClient()
    backend = RemanenceBackend("direct-rem", endpoint="direct", write_session=client)
    recorded = []

    def handler(ctx):
        before = deepcopy(ctx.job.step_state)
        with commit_point_for(ctx.session).window():
            result = backend.write_object_to_pool(source, "checkpoint-pool")
            assert result.integrity_hash == hashlib.sha256(source.read_bytes()).digest()
            assert not client.aborted
        assert ctx.job.step_state == before
        assert not ctx.session.info.get("checkpoint_batch_state_touched")
        # Both observer bindings are restored: append records, checkpoint clears.
        with backend.open_batch("checkpoint-pool") as batch:
            batch.append(source, "outside-window")
            assert CHECKPOINT_BATCH_STATE_KEY in ctx.job.step_state
            recorded.append(deepcopy(ctx.job.step_state))
            batch.checkpoint()
            assert CHECKPOINT_BATCH_STATE_KEY not in ctx.job.step_state
        assert ctx.session.info["checkpoint_batch_state_touched"]
        return JobResult(ok=True)

    monkeypatch.setitem(registry._HANDLERS, "_rba_observer", handler)
    with session_scope(engine) as session:
        job_id = submit(session, "_rba_observer").id
    outcome = JobWorker(engine, config=WorkerConfig(executor_workers=1)).drain()[0]
    assert outcome.result.ok, outcome.result.detail
    assert len(recorded) == 1
    assert not client.aborted
    with session_scope(engine) as session:
        assert CHECKPOINT_BATCH_STATE_KEY not in session.get(Job, job_id).step_state
    engine.dispose()


class _PoolLookupError(grpc.RpcError):
    def __init__(self, status):
        self.status = status

    def code(self):
        return self.status

    def details(self):
        return "pool lookup/write proof"


class _PoolLookupCatalog:
    """In-process catalogue retaining the precise pool/name request."""

    def __init__(self, obj, *, status=None):
        self.obj = obj
        self.status = status
        self.requests = []

    def GetObject(self, request, timeout=None):
        self.requests.append(request)
        if self.status is not None:
            raise _PoolLookupError(self.status)
        return self.obj


def test_remanence_pool_lookup_name_pool_and_write_locator(proto_object, tmp_path):
    """T7/M8: GetObject carries both identity fields and returns the write's locator."""
    source = tmp_path / "artifact.rem"
    source.write_bytes(b"pool lookup parity")
    proto_object.content_sha256 = hashlib.sha256(source.read_bytes()).digest()
    proto_object.logical_size_bytes = source.stat().st_size
    catalog = _PoolLookupCatalog(proto_object)
    client = _DirectWriteClient(proto_object)
    backend = RemanenceBackend("rem", endpoint="direct", catalog=catalog, write_session=client)
    written = backend.write_object_to_pool(
        source, "scenario-a", caller_object_id=proto_object.caller_object_id
    )
    found = backend.lookup_pool_object(
        "scenario-a", proto_object.caller_object_id, candidate_digests=[]
    )
    assert found is not None
    assert found.copy_record.native_locator == written.native_locator
    assert found.content_hash == proto_object.content_sha256
    assert found.copy_record.integrity_hash == written.integrity_hash
    assert found.copy_record.size_bytes == source.stat().st_size
    assert len(catalog.requests) == 1
    assert catalog.requests[0].pool_id == "scenario-a"
    assert catalog.requests[0].caller_object_id == proto_object.caller_object_id
    assert catalog.requests[0].object_id == b""
    assert backend.reissue_after_absent is True
    assert backend.holds_names is True


@pytest.mark.parametrize(
    "case", ["not-found", "other-pool", "two-copies", "empty-hash", "bad-hash", "unavailable"]
)
def test_remanence_pool_lookup_absence_and_unavailable(proto_object, case):
    """T7: only authoritative absence is None; ambiguous/malformed answers raise."""
    from sutradhara.backend.port import BackendUnavailableError

    status = None
    if case == "not-found":
        status = grpc.StatusCode.NOT_FOUND
    elif case == "unavailable":
        status = grpc.StatusCode.UNAVAILABLE
    elif case == "other-pool":
        proto_object.copies[0].pool_id = "other"
    elif case == "two-copies":
        proto_object.copies.add().CopyFrom(proto_object.copies[0])
    elif case == "empty-hash":
        proto_object.content_sha256 = b""
    elif case == "bad-hash":
        proto_object.content_sha256 = b"short"
    backend = RemanenceBackend(
        "rem", endpoint="direct", catalog=_PoolLookupCatalog(proto_object, status=status)
    )
    if case in {"not-found", "other-pool"}:
        assert backend.lookup_pool_object("scenario-a", "name", candidate_digests=[]) is None
    else:
        with pytest.raises(BackendUnavailableError):
            backend.lookup_pool_object("scenario-a", "name", candidate_digests=[])


@pytest.mark.parametrize("operation", ["append", "checkpoint", "close"])
@pytest.mark.parametrize(
    "status",
    [
        grpc.StatusCode.ALREADY_EXISTS,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    ],
)
def test_remanence_write_pool_refusal_mapping(tmp_path, monkeypatch, operation, status):
    """T7: every batch operation maps refusal/in-doubt and retains abort behavior."""
    from sutradhara.backend.port import BackendObjectExistsError, BackendWriteInDoubtError
    from sutradhara.backend.remanence import RemanenceWriteInDoubtError

    client = _CheckpointBatchClient()
    backend = RemanenceBackend("rem", endpoint="direct", write_session=client)
    source = tmp_path / "object.bin"
    source.write_bytes(b"data")
    batch = backend.open_batch("checkpoint-pool")

    def fail(*args, **kwargs):
        raise _PoolLookupError(status)

    if operation != "append":
        batch.append(source, "name")
    monkeypatch.setattr(
        client,
        {"append": "AppendObject", "checkpoint": "CheckpointSession", "close": "CloseWriteSession"}[
            operation
        ],
        fail,
    )
    expected = {
        grpc.StatusCode.ALREADY_EXISTS: BackendObjectExistsError,
        grpc.StatusCode.ABORTED: RemanenceWriteInDoubtError,
        grpc.StatusCode.DEADLINE_EXCEEDED: RemanenceWriteInDoubtError,
        grpc.StatusCode.UNAVAILABLE: RemanenceWriteSessionError,
    }[status]
    arguments = (source, "name") if operation == "append" else ()
    with pytest.raises(expected) as error:
        getattr(batch, operation)(*arguments)
    assert client.aborted
    if status in {grpc.StatusCode.ABORTED, grpc.StatusCode.DEADLINE_EXCEEDED}:
        assert isinstance(error.value, RemanenceWriteSessionError)
        assert isinstance(error.value, BackendWriteInDoubtError)
        assert error.value.session_id == client.session_id
    elif status == grpc.StatusCode.UNAVAILABLE:
        assert type(error.value) is RemanenceWriteSessionError


class _BusyUntilLibrary(_QueueLibrary):
    """Stay externally busy for a finite cap, even with the poll guard mutated."""

    def __init__(self, cap_seconds: float) -> None:
        super().__init__()
        self.busy_until = time.monotonic() + cap_seconds
        self.timeouts: list[float | None] = []

    def GetLiveStatus(self, request, timeout: float | None = None):
        self.timeouts.append(timeout)
        self.set_active(time.monotonic() < self.busy_until)
        return super().GetLiveStatus(request, timeout=timeout)


class _RaceUntilClient(_QueueReadClient):
    """Lose reservations until a finite cap so a broken deadline cannot hang."""

    def __init__(self, library: _QueueLibrary, cap_seconds: float) -> None:
        super().__init__(library)
        self.refuse_until = time.monotonic() + cap_seconds
        self.timeouts: list[float | None] = []

    def OpenReadSession(self, request, timeout: float | None = None):
        self.timeouts.append(timeout)
        if time.monotonic() < self.refuse_until:
            raise _LostRaceError()
        return super().OpenReadSession(request, timeout=timeout)


class _OpenRpcError(grpc.RpcError):  # type: ignore[misc]
    def __init__(self, status: grpc.StatusCode) -> None:
        self.status = status

    def code(self) -> grpc.StatusCode:
        return self.status

    def details(self) -> str:
        return "injected open failure"


def test_read_open_busy_poll_has_budget_and_releases_bay() -> None:
    """T5: external ACTIVE polling expires promptly and leaves a usable bay."""
    seconds = 0.2
    library = _BusyUntilLibrary(5 * seconds)
    client = _QueueReadClient(library)
    backend = _queue_backend(library, client, read_open_deadline_seconds=seconds)
    locator = _read_locator("11" * 32)
    started = time.monotonic()
    with pytest.raises(BackendTransientError) as raised:
        backend.open_read_session(locator)
    assert time.monotonic() - started < 2 * seconds
    assert "queue-test" in str(raised.value)
    assert _TAPE_UUID_HEX in str(raised.value)
    assert "0.2s" in str(raised.value)
    assert all(timeout is not None and 0 < timeout <= seconds for timeout in library.timeouts)
    assert backend._drive_queue is not None
    assert not any(lock.locked() for lock in backend._drive_queue._locks.values())
    library.busy_until = 0
    with backend.open_read_session(locator):
        pass
    assert client.open_count == 1


def test_read_open_race_requeues_share_budget(monkeypatch) -> None:
    """T6: lost reservations do not restart the open's time budget."""
    seconds = 0.2
    monkeypatch.setattr(remanence_module, "_DRIVE_ASSIGNMENT_POLL_SECONDS", 5 * seconds)
    library = _QueueLibrary()
    client = _RaceUntilClient(library, 5 * seconds)
    backend = _queue_backend(library, client, read_open_deadline_seconds=seconds)
    started = time.monotonic()
    with pytest.raises(BackendTransientError):
        backend.open_read_session(_read_locator("11" * 32))
    assert time.monotonic() - started < 2 * seconds
    assert client.timeouts
    assert backend._drive_queue is not None
    assert not any(lock.locked() for lock in backend._drive_queue._locks.values())


@pytest.mark.parametrize(
    "status", [None, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE]
)
def test_read_open_rpc_timeout_and_error_mapping(status) -> None:
    """T7: the RPC gets a positive remaining timeout and preserves error typing."""
    seconds = 0.2
    library = _QueueLibrary()
    timeouts = []

    class Client(_QueueReadClient):
        def OpenReadSession(self, request, timeout: float | None = None):
            timeouts.append(timeout)
            if status is not None:
                raise _OpenRpcError(status)
            return super().OpenReadSession(request, timeout=timeout)

    backend = _queue_backend(library, Client(library), read_open_deadline_seconds=seconds)
    if status is None:
        with backend.open_read_session(_read_locator("11" * 32)):
            pass
    else:
        with pytest.raises(BackendUnavailableError) as raised:
            backend.open_read_session(_read_locator("11" * 32))
        assert isinstance(raised.value, BackendTransientError) == (
            status == grpc.StatusCode.DEADLINE_EXCEEDED
        )
        if status == grpc.StatusCode.DEADLINE_EXCEEDED:
            assert "queue-test" in str(raised.value)
            assert _TAPE_UUID_HEX in str(raised.value)
            assert "0.2s" in str(raised.value)
            assert "injected open failure" in str(raised.value)
    assert len(timeouts) == 1
    assert timeouts[0] is not None
    assert 0 < timeouts[0] <= seconds
    assert backend._drive_queue is not None
    assert not any(lock.locked() for lock in backend._drive_queue._locks.values())


def test_read_open_local_lock_wait_is_not_charged() -> None:
    """T8: a second open succeeds after a local session holds the bay for 3 budgets."""
    seconds = 0.2
    library = _QueueLibrary()
    client = _QueueReadClient(library)
    backend = _queue_backend(library, client, read_open_deadline_seconds=seconds)
    errors = []
    opened = threading.Event()

    def hold_session():
        try:
            with backend.open_read_session(_read_locator("11" * 32)):
                opened.set()
                time.sleep(3 * seconds)
        except BaseException as exc:
            errors.append(exc)

    first = threading.Thread(target=hold_session)
    first.start()
    try:
        assert opened.wait(timeout=2)
        started = time.monotonic()
        with backend.open_read_session(_read_locator("11" * 32)):
            assert time.monotonic() - started > 2 * seconds
    finally:
        first.join(timeout=2)
    assert not first.is_alive()
    assert errors == []
    assert client.open_count == 2


def test_read_open_success_after_budget_keeps_session_until_close() -> None:
    """FR1 T1: a successful daemon open survives its budget until the reader closes."""
    seconds = 0.2
    library = _QueueLibrary()

    class Client(_QueueReadClient):
        """Return a live session after the caller's open budget has elapsed."""

        def OpenReadSession(self, request, timeout: float | None = None):
            time.sleep(1.5 * seconds)
            return super().OpenReadSession(request, timeout=timeout)

    client = Client(library)
    backend = _queue_backend(library, client, read_open_deadline_seconds=seconds)
    with backend.open_read_session(_read_locator("11" * 32)):
        assert library.active
        assert client.open_count == 1
    assert not library.active
    assert backend._drive_queue is not None
    assert not any(lock.locked() for lock in backend._drive_queue._locks.values())


@pytest.mark.parametrize("failure_site", ["report", "reader"])
def test_read_open_post_rpc_failure_closes_session_and_releases_bay(monkeypatch, failure_site):
    """An opened daemon session and local lease are cleaned up before propagation."""
    library = _QueueLibrary()
    backend = _queue_backend(library, _QueueReadClient(library), read_open_deadline_seconds=0.2)

    def fail(*args, **kwargs):
        raise RuntimeError("post-open failure")

    if failure_site == "report":
        monkeypatch.setattr(remanence_module, "report_session_open", fail)
    else:
        monkeypatch.setattr(remanence_module.RemanenceReadSession, "live", fail)
    with pytest.raises(RuntimeError, match="post-open failure"):
        backend.open_read_session(_read_locator("11" * 32))
    assert not library.active
    assert backend._drive_queue is not None
    assert not any(lock.locked() for lock in backend._drive_queue._locks.values())


def test_read_open_status_rpc_timeout_remains_advisory() -> None:
    """A failed advisory status call still permits a direct open within the budget."""
    timeouts = []

    class Library(_QueueLibrary):
        def GetLiveStatus(self, request, timeout: float | None = None):
            timeouts.append(timeout)
            raise _OpenRpcError(grpc.StatusCode.DEADLINE_EXCEEDED)

    library = Library()
    backend = _queue_backend(library, _QueueReadClient(library), read_open_deadline_seconds=0.2)
    with backend.open_read_session(_read_locator("11" * 32)):
        pass
    assert len(timeouts) == 1
    assert timeouts[0] is not None
    assert 0 < timeouts[0] <= 0.2


class _DeadlineWriteClient(_CheckpointBatchClient):
    """Record call bounds and optionally interrupt an append after consuming it."""

    def __init__(self, *, append_error=None, open_error=None):
        super().__init__()
        self.timeouts = {}
        self.append_error = append_error
        self.open_error = open_error

    def OpenWriteSession(self, request, timeout: float | None = None):
        self.timeouts["open"] = timeout
        if self.open_error is not None:
            raise _PoolLookupError(self.open_error)
        return super().OpenWriteSession(request)

    def AppendObject(self, messages, timeout: float | None = None):
        self.timeouts["append"] = timeout
        if self.append_error is not None:
            list(messages)
            raise self.append_error
        return super().AppendObject(messages)

    def CheckpointSession(self, request, timeout: float | None = None):
        self.timeouts["checkpoint"] = timeout
        return super().CheckpointSession(request)

    def CloseWriteSession(self, request, timeout: float | None = None):
        self.timeouts["close"] = timeout
        return super().CloseWriteSession(request)

    def AbortWriteSession(self, request, timeout: float | None = None):
        self.timeouts["abort"] = timeout
        return super().AbortWriteSession(request)


class _DeadlineCatalog:
    """Direct metadata client recording GetFile bounds and supplying a stream."""

    def __init__(self, *, status=None, call=None):
        self.status = status
        self.call = call
        self.timeout = None

    def GetFile(self, request, timeout: float | None = None):
        self.timeout = timeout
        if self.status is not None:
            raise _PoolLookupError(self.status)
        return layer5_pb2.FileRecord()

    def EnumerateObjects(self, request):
        return self.call


@pytest.mark.parametrize("operation", ["open", "checkpoint", "close"])
def test_deadline_session_write_bounds(tmp_path, operation):
    """RBA-DL T1(a): batch-of-one forwards each session bound."""
    source = tmp_path / "source"
    source.write_bytes(b"bounded write")
    client = _DeadlineWriteClient()
    backend = RemanenceBackend("rem", endpoint="direct", write_session=client)
    backend.write_object_to_pool(source, "checkpoint-pool")
    assert client.timeouts[operation] == 1800.0


def test_deadline_abort_bound(tmp_path):
    """RBA-DL T1(b): even failed appends bound the best-effort abort."""
    source = tmp_path / "source"
    source.write_bytes(b"failed append")
    client = _DeadlineWriteClient(append_error=_DirectWriteError())
    backend = RemanenceBackend("rem", endpoint="direct", write_session=client)
    with pytest.raises(RemanenceWriteSessionError):
        backend.write_object_to_pool(source, "checkpoint-pool")
    assert client.aborted
    assert client.timeouts["abort"] == 60.0


def test_deadline_read_close_bound():
    """RBA-DL T1(c): the production range route bounds read-session cleanup."""

    class Client(_QueueReadClient):
        timeout = None

        def CloseReadSession(self, request, timeout: float | None = None):
            self.timeout = timeout
            return super().CloseReadSession(request)

    library = _QueueLibrary()
    client = Client(library)
    backend = _queue_backend(library, client)
    backend.read_range(_read_locator("ab" * 32), ByteRange(0, 1))
    assert client.timeout == 60.0


def test_deadline_get_file_bound():
    """RBA-DL T1(d): GetFile uses the metadata bound."""
    catalog = _DeadlineCatalog()
    backend = RemanenceBackend("rem", endpoint="direct", catalog=catalog)
    backend.get_file(_read_locator("ab" * 32), path="member")
    assert catalog.timeout == 15.0


@pytest.mark.parametrize("size", [3 * 1024 * 1024 + 1, 0])
def test_deadline_append_size_bound(tmp_path, size):
    """RBA-DL T2: append allows a session bound plus source transfer time."""
    source = tmp_path / "source"
    source.write_bytes(b"x" * size)
    client = _DeadlineWriteClient()
    backend = RemanenceBackend("rem", endpoint="direct", write_session=client)
    backend.write_object_to_pool(source, "checkpoint-pool")
    assert client.timeouts["append"] == pytest.approx(1800.0 + size / (10 * 1024 * 1024))


@pytest.mark.parametrize("status", [grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE])
def test_deadline_open_class(status):
    """RBA-DL T4: only an open deadline is transient."""
    client = _DeadlineWriteClient(open_error=status)
    backend = RemanenceBackend("rem", endpoint="direct", write_session=client)
    expected = (
        BackendTransientError
        if status == grpc.StatusCode.DEADLINE_EXCEEDED
        else BackendUnavailableError
    )
    with pytest.raises(expected, match="direct") as error:
        backend.open_batch("checkpoint-pool")
    assert type(error.value) is expected


@pytest.mark.parametrize("status", [grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE])
def test_deadline_get_file_class(status):
    """RBA-DL T5: only a GetFile deadline is transient."""
    backend = RemanenceBackend("rem", endpoint="direct", catalog=_DeadlineCatalog(status=status))
    expected = (
        BackendTransientError
        if status == grpc.StatusCode.DEADLINE_EXCEEDED
        else BackendUnavailableError
    )
    with pytest.raises(expected, match="direct") as error:
        backend.get_file(_read_locator("ab" * 32), path="member")
    assert type(error.value) is expected


def test_deadline_funnel_write_outcome_unknown(tmp_path):
    """RBA-DL T6: append deadline leaves the attempt unresolved without a lookup."""
    from sutradhara.archive_fanout import LocalArchiveBuilder, flush_bundle
    from sutradhara.bundle_copy_ledger import identity_attempts
    from sutradhara.catalog.session import create_all, make_engine, session_scope
    from tests.test_bundle_sweeper import _backends, _rba_seed

    engine = make_engine(f"sqlite:///{tmp_path / 'deadline.db'}")
    create_all(engine)
    try:
        bundle_id = _rba_seed(engine, tmp_path, pools=("p-a",))
        client = _DeadlineWriteClient(
            append_error=_PoolLookupError(grpc.StatusCode.DEADLINE_EXCEEDED)
        )
        catalog = _PoolLookupCatalog(layer5_pb2.ObjectRecord(), status=grpc.StatusCode.NOT_FOUND)
        backend = RemanenceBackend("rem", endpoint="direct", write_session=client, catalog=catalog)
        with session_scope(engine) as session:
            result = flush_bundle(
                session,
                bundle_id=bundle_id,
                backends=_backends(session, backend),
                builder=LocalArchiveBuilder(),
            )
            assert result.condition_reason == "write-outcome-unknown"
            attempts = identity_attempts(session, bundle_id=bundle_id, pool_id="p-a", generation=0)
            assert len(attempts) == 1
            assert attempts[0][1] is None
            assert catalog.requests == []
            assert client.aborted
    finally:
        engine.dispose()


class _IdleObjectCall:
    """A catalogue stream whose second wait ends only on cancellation or its cap."""

    def __init__(self, obj):
        self.obj = obj
        self.sent = False
        self.cancelled = threading.Event()

    def __iter__(self):
        return self

    def __next__(self):
        if self.cancelled.is_set():
            raise _PoolLookupError(grpc.StatusCode.CANCELLED)
        if not self.sent:
            self.sent = True
            return self.obj
        if self.cancelled.wait(1.0):
            raise _PoolLookupError(grpc.StatusCode.CANCELLED)
        raise RuntimeError("idle catalogue fake reached its cap")

    def cancel(self):
        self.cancelled.set()
        return True


def test_deadline_enumerate_idle_watch(monkeypatch, proto_object):
    """RBA-DL T8: catalogue stalls use the metadata idle bound and map transient."""
    monkeypatch.setattr(remanence_module, "_PLAN_METADATA_TIMEOUT_SECONDS", 0.2, raising=False)
    call = _IdleObjectCall(proto_object)
    backend = RemanenceBackend("rem", endpoint="direct", catalog=_DeadlineCatalog(call=call))
    started = time.monotonic()
    with pytest.raises(BackendTransientError, match="direct") as error:
        list(backend.enumerate())
    assert time.monotonic() - started < 1.0
    assert "0.2" in str(error.value)
    assert call.cancelled.is_set()
    assert not [t for t in threading.enumerate() if t.name.startswith("remanence-idle-watch")]


@pytest.mark.parametrize("status", [grpc.StatusCode.CANCELLED, grpc.StatusCode.DEADLINE_EXCEEDED])
def test_deadline_enumerate_server_error_class(status):
    """FR1 T2(b): catalogue cancellation is unavailable; a deadline is transient."""

    class Call:
        """Fail on the first next without any idle-watch cancellation."""

        def __iter__(self):
            return self

        def __next__(self):
            raise _PoolLookupError(status)

        def cancel(self):
            return True

    backend = RemanenceBackend("rem", endpoint="direct", catalog=_DeadlineCatalog(call=Call()))
    expected = (
        BackendTransientError
        if status == grpc.StatusCode.DEADLINE_EXCEEDED
        else BackendUnavailableError
    )
    with pytest.raises(expected, match="direct") as error:
        list(backend.enumerate())
    assert type(error.value) is expected


def test_read_idle_watch_ends_a_stalled_real_grpc_stream(monkeypatch):
    """RBA-DL H1: a real gRPC next is interrupted by the idle watch."""
    monkeypatch.setattr(remanence_module, "_READ_IDLE_TIMEOUT_SECONDS", 0.2, raising=False)

    class StalledRead(_ReadSession):
        def ReadObjectRange(self, request, context):
            yield layer5_pb2.BytesChunk(data=b"first")
            cap = time.monotonic() + 5 * 0.2
            while context.is_active() and time.monotonic() < cap:
                time.sleep(0.01)

    servicer = StalledRead(b"first")
    with _serve_read(servicer) as endpoint:
        backend = RemanenceBackend.from_grpc("rem", endpoint=endpoint)
        try:
            started = time.monotonic()
            with pytest.raises(BackendTransientError, match="idle wait exceeded"):
                backend.read_range(_read_locator("ab" * 32), ByteRange(0, 0))
            assert time.monotonic() - started < 1.0
            assert servicer.closed
        finally:
            backend._channel.close()


def test_append_deadline_over_real_grpc_is_in_doubt(monkeypatch, tmp_path, proto_object):
    """RBA-DL H2: a real stream-unary deadline aborts and leaves the write in doubt."""
    from sutradhara.backend.remanence import RemanenceWriteInDoubtError

    monkeypatch.setattr(remanence_module, "_SESSION_CALL_TIMEOUT_SECONDS", 0.2, raising=False)

    class StalledWrite(_WriteSession):
        def AppendObject(self, messages, context):
            self.appended.extend(messages)
            cap = time.monotonic() + 5 * 0.2
            while context.is_active() and time.monotonic() < cap:
                time.sleep(0.01)
            return self.obj

    source = tmp_path / "source"
    source.write_bytes(b"write deadline")
    servicer = StalledWrite(proto_object, bytes.fromhex(_TAPE_UUID_HEX))
    with _serve_write(servicer) as endpoint:
        backend = RemanenceBackend.from_grpc("rem", endpoint=endpoint)
        try:
            started = time.monotonic()
            with pytest.raises(RemanenceWriteInDoubtError):
                backend.write_object_to_pool(source, "scenario-a")
            assert time.monotonic() - started < 1.0
            assert servicer.aborted
        finally:
            backend._channel.close()


class _FindingRpcError(grpc.RpcError):
    """Socket-free read status with controllable trailing metadata."""

    def __init__(self, code, trailers):
        self._code = code
        self._trailers = trailers

    def code(self):
        return self._code

    def details(self):
        return "recorded position unreadable"

    def trailing_metadata(self):
        return self._trailers


class _FindingRangeClient(_QueueReadClient):
    """Preserve the normal session lifecycle while failing its range call."""

    def __init__(self, library, error):
        super().__init__(library)
        self.error = error

    def ReadObjectRange(self, request):
        raise self.error
        yield  # generator-shaped RPC; the error is raised during iteration


@pytest.mark.parametrize("value", ["media-damage", b"media-damage"])
@pytest.mark.parametrize(
    "code",
    [
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.CANCELLED,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    ],
)
def test_media_finding_read_range_keeps_identity(code, value):
    """T1(a,d): the finding outranks transport/session statuses, but preserves identity."""
    from sutradhara.backend.port import BackendMediaError

    library = _QueueLibrary()
    error = _FindingRpcError(code, (("remanence-retry", "media"), ("remanence-finding", value)))
    client = _FindingRangeClient(library, error)
    backend = _queue_backend(library, client)
    with (
        backend.open_read_session(_read_locator("11" * 32)) as reader,
        pytest.raises(BackendMediaError) as caught,
    ):
        reader.read_range(ByteRange(0, 1))
    assert type(caught.value) is BackendMediaError
    assert not isinstance(caught.value, BackendUnavailableError)
    assert caught.value.session_id == b"session-1"
    assert caught.value.drive_element_address == 257
    assert "drive_element_address=257" in str(caught.value)
    assert "queue-test" in str(caught.value)
    assert code.name in str(caught.value)
    assert "recorded position unreadable" in str(caught.value)
    assert "remanence-finding=media-damage" in str(caught.value)


@pytest.mark.parametrize(
    "case",
    [
        "retry-only",
        "absent",
        "raises",
        "other-finding",
        "data-loss",
        "cancelled",
        "none",
        "non-callable",
    ],
)
def test_media_finding_contract_edges(case):
    """T2: only the finding changes a read; DATA_LOSS retains its original class."""
    code = grpc.StatusCode.INTERNAL
    trailers = ()
    if case == "retry-only":
        trailers = (("remanence-retry", "media"),)
    elif case == "other-finding":
        trailers = (("remanence-finding", "another-finding"),)
    elif case == "data-loss":
        code = grpc.StatusCode.DATA_LOSS
        trailers = (("remanence-finding", "media-damage"),)
    elif case == "cancelled":
        code = grpc.StatusCode.CANCELLED
    error = _FindingRpcError(code, None if case == "none" else trailers)
    if case == "absent":

        class NoTrailers(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.INTERNAL

            def details(self):
                return "no trailers"

        error = NoTrailers()
    elif case == "raises":

        def unreadable():
            raise RuntimeError("trailers unavailable")

        error.trailing_metadata = unreadable
    elif case == "non-callable":
        error.trailing_metadata = "unavailable"
    library = _QueueLibrary()
    backend = _queue_backend(library, _FindingRangeClient(library, error))
    with (
        backend.open_read_session(_read_locator("11" * 32)) as reader,
        pytest.raises(BackendError) as caught,
    ):
        reader.read_range(ByteRange(0, 1))
    expected = BackendStoredCorruptError if case == "data-loss" else BackendUnavailableError
    assert type(caught.value) is expected


def test_media_finding_over_real_grpc():
    """T4 hand-back: grpc exposes the daemon finding on the streaming error."""
    from sutradhara.backend.port import BackendMediaError

    class FindingServicer(_ReadSession):
        def ReadObjectRange(self, request, context):
            context.set_trailing_metadata((("remanence-finding", "media-damage"),))
            context.abort(grpc.StatusCode.INTERNAL, "recorded position unreadable")
            yield  # streaming RPC

    servicer = FindingServicer(b"x")
    with _serve_read(servicer) as endpoint:
        backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
        with pytest.raises(BackendMediaError):
            backend.read_range(_read_locator(hashlib.sha256(b"x").hexdigest()), ByteRange(0, 0))
    assert servicer.closed
