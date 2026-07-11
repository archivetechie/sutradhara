"""RM0.1 tests for lazy backend streams and context-owned teardown."""

from __future__ import annotations

import hashlib
import resource
import threading
import tracemalloc
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import grpc
import pytest

from sutradhara._proto import layer5_pb2
from sutradhara.backend.d2tape import D2TapeBackend
from sutradhara.backend.memory import MemoryBackend
from sutradhara.backend.port import (
    BackendUnavailableError,
    ByteRange,
    StreamingStorageBackend,
    StreamKind,
)
from sutradhara.backend.remanence import RemanenceBackend
from sutradhara.backend.s3 import S3Backend
from sutradhara.backend.ssh_disk import SshDiskBackend

_TAPE_UUID = bytes.fromhex("00112233445566778899aabbccddeeff")
_OBJECT_ID = bytes.fromhex("ffeeddccbbaa99887766554433221100")
_LOCATOR: dict[str, Any] = {
    "tape_uuid": _TAPE_UUID.hex(),
    "tape_file_number": 1,
    "object_id": _OBJECT_ID.hex(),
}


class _BoundedCall:
    """A cancellable one-slot stream whose producer blocks when abandoned."""

    def __init__(self, chunks: list[bytes], events: list[str]) -> None:
        self._chunks = chunks
        self._events = events
        self._condition = threading.Condition()
        self._slot: layer5_pb2.BytesChunk | None = None
        self._cancelled = False
        self._done = False
        self._blocked = threading.Event()
        self.server_yielded = 0
        self._producer = threading.Thread(target=self._produce, daemon=True)
        self._producer.start()

    def _produce(self) -> None:
        try:
            for index, data in enumerate(self._chunks):
                with self._condition:
                    while self._slot is not None and not self._cancelled:
                        self._blocked.set()
                        self._condition.wait()
                    if self._cancelled:
                        return
                    self._slot = layer5_pb2.BytesChunk(
                        data=data,
                        is_last=index == len(self._chunks) - 1,
                    )
                    self.server_yielded += 1
                    self._condition.notify_all()
        finally:
            with self._condition:
                self._done = True
                self._condition.notify_all()

    def __iter__(self) -> _BoundedCall:
        return self

    def __next__(self) -> layer5_pb2.BytesChunk:
        with self._condition:
            while self._slot is None and not self._done:
                self._condition.wait()
            if self._slot is None:
                raise StopIteration
            chunk = self._slot
            self._slot = None
            self._condition.notify_all()
            return chunk

    def wait_until_producer_is_blocked(self) -> None:
        assert self._blocked.wait(timeout=30.0), "producer did not block within timeout"

    def cancel(self) -> bool:
        self._events.append("cancel")
        with self._condition:
            self._cancelled = True
            self._slot = None
            self._condition.notify_all()
        self._producer.join(timeout=30.0)
        assert not self._producer.is_alive(), (
            "producer did not unblock after cancel (cancel wedged)"
        )
        return True

    @property
    def producer_done(self) -> bool:
        return not self._producer.is_alive()


class _GeneratedCall:
    """Generate a large logical response while retaining only the current chunk."""

    def __init__(self, *, chunks: int, chunk_bytes: int) -> None:
        self._remaining = chunks
        self._chunk_bytes = chunk_bytes
        self.cancelled = False

    def __iter__(self) -> _GeneratedCall:
        return self

    def __next__(self) -> layer5_pb2.BytesChunk:
        if not self._remaining:
            raise StopIteration
        self._remaining -= 1
        return layer5_pb2.BytesChunk(
            data=b"x" * self._chunk_bytes,
            is_last=self._remaining == 0,
        )

    def cancel(self) -> bool:
        self.cancelled = True
        return True


class _CancelledRpcError(grpc.RpcError):  # type: ignore[misc]
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.CANCELLED

    def details(self) -> str:
        return "server cancelled stream"


class _ServerCancelledCall:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    def __iter__(self) -> _ServerCancelledCall:
        return self

    def __next__(self) -> layer5_pb2.BytesChunk:
        raise _CancelledRpcError()

    def cancel(self) -> bool:
        self._events.append("cancel")
        return True


class _ReadClient:
    def __init__(self, call: Any, events: list[str]) -> None:
        self.call = call
        self.events = events
        self.closed = False

    def OpenReadSession(self, request: layer5_pb2.OpenReadSessionRequest) -> layer5_pb2.ReadSession:
        return layer5_pb2.ReadSession(session_id=b"session", tape_uuid=_TAPE_UUID)

    def ReadObjectRange(self, request: layer5_pb2.ReadObjectRangeRequest) -> Any:
        return self.call

    def CloseReadSession(
        self, request: layer5_pb2.CloseReadSessionRequest
    ) -> layer5_pb2.ReadSession:
        self.events.append("close")
        if isinstance(self.call, _BoundedCall):
            assert self.call.producer_done, (
                "CloseReadSession ran before cancellation unblocked send"
            )
        self.closed = True
        return layer5_pb2.ReadSession(session_id=b"session", tape_uuid=_TAPE_UUID)


def _live_backend(call: Any, events: list[str]) -> RemanenceBackend:
    return RemanenceBackend(
        "rem-tape",
        endpoint="mock-remanence",
        read_session=_ReadClient(call, events),
    )


def test_remanence_first_byte_is_lazy_and_early_close_cancels_before_close() -> None:
    events: list[str] = []
    call = _BoundedCall([b"first", b"second", b"third", b"fourth"], events)
    backend = _live_backend(call, events)

    with backend.open_range_chunks(_LOCATOR, ByteRange(0, 0), chunk_bytes=5) as chunks:
        assert next(chunks) == b"first"
        call.wait_until_producer_is_blocked()
        assert call.server_yielded < 4

    assert events == ["cancel", "close"]
    assert call.producer_done


def test_remanence_exception_mid_stream_has_the_same_teardown() -> None:
    events: list[str] = []
    call = _BoundedCall([b"first", b"second", b"third"], events)
    backend = _live_backend(call, events)

    with pytest.raises(RuntimeError, match="consumer failed"):
        _fail_during_remanence_consumption(backend, call)

    assert events == ["cancel", "close"]
    assert call.producer_done


def test_remanence_server_cancellation_still_cancels_call_then_closes_session() -> None:
    events: list[str] = []
    backend = _live_backend(_ServerCancelledCall(events), events)

    with (
        pytest.raises(BackendUnavailableError, match="server cancelled stream"),
        backend.open_range_chunks(_LOCATOR, ByteRange(0, 0), chunk_bytes=5) as chunks,
    ):
        next(chunks)

    assert events == ["cancel", "close"]


def test_remanence_large_stream_peak_memory_is_a_few_chunks() -> None:
    chunk_bytes = 256 * 1024
    call = _GeneratedCall(chunks=256, chunk_bytes=chunk_bytes)
    backend = _live_backend(call, [])
    digest = hashlib.sha256()
    consumed_bytes = 0

    rss_before_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    tracemalloc.start()
    try:
        with backend.open_range_chunks(
            _LOCATOR, ByteRange(0, 0), chunk_bytes=chunk_bytes
        ) as chunks:
            for chunk in chunks:
                digest.update(chunk)
                consumed_bytes += len(chunk)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    rss_growth_bytes = (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - rss_before_kib) * 1024

    assert consumed_bytes == 256 * chunk_bytes
    assert peak < 8 * chunk_bytes
    assert rss_growth_bytes < 16 * chunk_bytes


class _StreamingBody:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks
        self.yielded = 0
        self.closed = False
        self.requested_chunk_bytes: int | None = None

    def iter_chunks(self, *, chunk_size: int) -> Iterator[bytes]:
        self.requested_chunk_bytes = chunk_size
        for chunk in self._chunks:
            self.yielded += 1
            yield chunk

    def close(self) -> None:
        self.closed = True


class _S3Client:
    def __init__(self, body: _StreamingBody) -> None:
        self.body = body
        self.kwargs: dict[str, Any] = {}

    def get_object(self, **kwargs: Any) -> dict[str, _StreamingBody]:
        self.kwargs = kwargs
        return {"Body": self.body}


def test_s3_stream_is_lazy_ranged_and_body_is_closed_on_exception() -> None:
    body = _StreamingBody([b"abc", b"def", b"ghi"])
    client = _S3Client(body)
    backend = S3Backend("cloud", bucket="bucket", client=client)

    with pytest.raises(RuntimeError, match="stop"):
        _fail_during_s3_consumption(backend, body)

    assert client.kwargs["Range"] == "bytes=4-11"
    assert body.requested_chunk_bytes == 3
    assert body.closed


def _fail_during_remanence_consumption(
    backend: RemanenceBackend,
    call: _BoundedCall,
) -> None:
    with backend.open_range_chunks(_LOCATOR, ByteRange(0, 0), chunk_bytes=5) as chunks:
        assert next(chunks) == b"first"
        call.wait_until_producer_is_blocked()
        raise RuntimeError("consumer failed")


def _fail_during_s3_consumption(backend: S3Backend, body: _StreamingBody) -> None:
    with backend.open_range_chunks(
        {"bucket": "bucket", "key": "key"}, ByteRange(4, 12), chunk_bytes=3
    ) as chunks:
        assert body.yielded == 0
        assert next(chunks) == b"abc"
        assert body.yielded == 1
        raise RuntimeError("stop")


class _UnusedTransport:
    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"transport method should not be called: {name}")


def test_capability_query_distinguishes_native_from_materialized(tmp_path: Path) -> None:
    memory = MemoryBackend("memory")
    remanence = RemanenceBackend.from_objects("rem", [])
    s3 = S3Backend("s3", bucket="bucket", client=_S3Client(_StreamingBody([])))
    d2 = D2TapeBackend(
        "d2",
        jar_path=tmp_path / "d2.jar",
        java_bin=tmp_path / "java",
        device_env_path=tmp_path / "device.env",
        state_dir=tmp_path / "state",
    )
    ssh = SshDiskBackend(
        "ssh",
        host="unused",
        root="/unused",
        transport=_UnusedTransport(),
    )

    assert isinstance(remanence, StreamingStorageBackend)
    assert isinstance(s3, StreamingStorageBackend)
    assert not isinstance(d2, StreamingStorageBackend)
    assert not isinstance(ssh, StreamingStorageBackend)
    assert not isinstance(memory, StreamingStorageBackend)
    assert remanence.stream_kind is StreamKind.native_stream
    assert s3.stream_kind is StreamKind.native_stream
    assert d2.stream_kind is StreamKind.scratch_stream
    assert ssh.stream_kind is StreamKind.scratch_stream
    assert memory.stream_kind is StreamKind.memory_buffered
    digest = memory.add(b"abcdef")
    with memory.open_materialized_range_chunks(
        {"hash_hex": digest.hex()}, ByteRange(1, 6), chunk_bytes=2
    ) as chunks:
        assert list(chunks) == [b"bc", b"de", b"f"]
