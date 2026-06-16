"""Tests for the Remanence Layer 5 adapter.

Fixture-mode tests protect local/dev behavior; fake-Catalog tests protect the
live gRPC mapping without requiring the real daemon in unit tests.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from concurrent import futures
from contextlib import contextmanager
from pathlib import Path

import grpc
import pytest

from sutradhara._proto import layer5_pb2, layer5_pb2_grpc
from sutradhara.backend import remanence as remanence_module
from sutradhara.backend.port import (
    BackendError,
    BackendNotFoundError,
    BackendUnavailableError,
    ByteRange,
    CopyRecord,
    StorageBackend,
)
from sutradhara.backend.remanence import RemanenceBackend
from sutradhara.catalog.session import locator_key
from sutradhara.catalog.types import content_hash

FIXTURE = Path(__file__).parent / "fixtures" / "remanence_objects.json"


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
    assert result.actual_hash is not None


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
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        yield f"127.0.0.1:{port}", servicer
    finally:
        server.stop(grace=None)


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
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        yield f"127.0.0.1:{port}"
    finally:
        server.stop(grace=None)


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


def test_live_endpoint_unavailable_raises_backend_unavailable() -> None:
    backend = RemanenceBackend.from_grpc("primary-tape", "127.0.0.1:1")
    with pytest.raises(BackendUnavailableError, match=r"127\.0\.0\.1:1"):
        list(backend.enumerate())


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
        self.closed = False
        self.aborted = False
        self.abort_reason: str | None = None

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
        for msg in request_iterator:
            self.appended.append(msg)
        if self.append_error is not None:
            context.abort(self.append_error, "append failed")
            raise AssertionError("unreachable after context.abort")
        return self.obj

    def CloseWriteSession(
        self,
        request: layer5_pb2.CloseWriteSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.WriteSession:
        self.closed = True
        return layer5_pb2.WriteSession(session_id=self.session_id, tape_uuid=self.tape_uuid)

    def AbortWriteSession(
        self,
        request: layer5_pb2.AbortWriteSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.WriteSession:
        self.aborted = True
        self.abort_reason = request.reason
        return layer5_pb2.WriteSession(session_id=self.session_id, tape_uuid=self.tape_uuid)


@contextmanager
def _serve_write(servicer: _WriteSession) -> Iterator[str]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    layer5_pb2_grpc.add_WriteSessionServiceServicer_to_server(servicer, server)  # type: ignore[no-untyped-call]
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        yield f"127.0.0.1:{port}"
    finally:
        server.stop(grace=None)


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


def test_write_object_to_pool_success(
    write_server: tuple[str, _WriteSession],
    tmp_path: Path,
) -> None:
    endpoint, servicer = write_server
    src = tmp_path / "obj.bin"
    src.write_bytes(b"hello world")

    wb = RemanenceBackend.from_grpc("primary-tape", endpoint)
    record = wb.write_object_to_pool(src, "scenario-a")

    assert isinstance(record, CopyRecord)
    assert servicer.opened
    assert servicer.closed
    assert not servicer.aborted
    assert servicer.open_request is not None
    assert servicer.open_request.pool_target.pool_id == "scenario-a"
    assert servicer.open_request.pool_target.mount_if_needed is True


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
        with pytest.raises(BackendUnavailableError):
            backend.write_object_to_pool(src, "scenario-a")

    assert servicer.aborted is True


def test_write_object_locator_parity_with_enumerate(
    write_server: tuple[str, _WriteSession],
    catalog_server: tuple[str, _Catalog],
    tmp_path: Path,
) -> None:
    write_endpoint, _ = write_server
    catalog_endpoint, _ = catalog_server
    src = tmp_path / "obj.bin"
    src.write_bytes(b"x" * 10)

    written = RemanenceBackend.from_grpc("primary-tape", write_endpoint).write_object_to_pool(
        src, "scenario-a"
    )
    [enumerated] = list(RemanenceBackend.from_grpc("primary-tape", catalog_endpoint).enumerate())

    assert written.native_locator == enumerated.native_locator
    assert locator_key(written.native_locator) == locator_key(enumerated.native_locator)
    assert written == enumerated


class _RoundTripStore:
    def __init__(self) -> None:
        self.objects: dict[bytes, bytes] = {}
        self.tape_uuid = bytes.fromhex(_TAPE_UUID_HEX)
        self.object_id = bytes.fromhex(_OBJECT_ID_HEX)


class _RTWrite(layer5_pb2_grpc.WriteSessionServiceServicer):
    def __init__(self, store: _RoundTripStore) -> None:
        self.store = store
        self.session_id = b"rt-write-1"

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
        return layer5_pb2.ObjectRecord(
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
        )

    def CloseWriteSession(
        self,
        request: layer5_pb2.CloseWriteSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.WriteSession:
        return layer5_pb2.WriteSession(
            session_id=self.session_id,
            tape_uuid=self.store.tape_uuid,
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
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        yield f"127.0.0.1:{port}"
    finally:
        server.stop(grace=None)


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
