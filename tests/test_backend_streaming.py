"""RM0.1 tests for lazy backend streams and context-owned teardown."""

from __future__ import annotations

import hashlib
import resource
import threading
import tracemalloc
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import grpc
import pytest

from sutradhara._proto import layer5_pb2
from sutradhara.archive_restore import ArchiveRestoreError, _copy_backend_range_to_path
from sutradhara.backend.d2tape import D2TapeBackend
from sutradhara.backend.memory import MemoryBackend
from sutradhara.backend.port import (
    BackendNotFoundError,
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
        self.opens = 0
        self.ranges: list[tuple[int, int]] = []

    def OpenReadSession(self, request: layer5_pb2.OpenReadSessionRequest) -> layer5_pb2.ReadSession:
        self.opens += 1
        return layer5_pb2.ReadSession(session_id=b"session", tape_uuid=_TAPE_UUID)

    def ReadObjectRange(self, request: layer5_pb2.ReadObjectRangeRequest) -> Any:
        self.ranges.append((request.start_byte, request.end_byte))
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
        self.get_calls = 0

    def get_object(self, **kwargs: Any) -> dict[str, _StreamingBody]:
        self.get_calls += 1
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


class _VerifyObservation:
    """Keep O(1) observation state so the memory test measures the adapter."""

    def __init__(self) -> None:
        self.digest = hashlib.sha256()
        self.size = 0
        self.calls = 0
        self.contiguous = True

    def __call__(self, offset: int, chunk: bytes) -> None:
        self.contiguous = self.contiguous and offset == self.size
        self.digest.update(chunk)
        self.size += len(chunk)
        self.calls += 1


def test_remanence_verify_observes_one_session_with_bounded_memory() -> None:
    """T2: a measured verify hashes/observes 64 chunks in one session and range."""
    chunk_bytes = 256 * 1024
    chunk_count = 64
    expected = hashlib.sha256()
    for _ in range(chunk_count):
        expected.update(b"x" * chunk_bytes)
    call = _GeneratedCall(chunks=chunk_count, chunk_bytes=chunk_bytes)
    backend = _live_backend(call, [])
    client = backend._read_session
    assert isinstance(client, _ReadClient)
    observation = _VerifyObservation()

    tracemalloc.start()
    try:
        result = backend.verify(
            {**_LOCATOR, "content_sha256": expected.hexdigest()}, observe=observation
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert result.ok
    assert result.measured
    assert result.actual_hash == expected.digest() == observation.digest.digest()
    assert observation.contiguous
    assert observation.calls == chunk_count
    assert observation.size == chunk_count * chunk_bytes
    assert client.opens == 1
    assert client.ranges == [(0, 0)]
    assert client.closed
    assert call.cancelled
    assert peak < 8 * chunk_bytes


class _GeneratedBody(_StreamingBody):
    """Generate S3 bytes lazily without holding the logical object in memory."""

    def iter_chunks(self, *, chunk_size: int) -> Iterator[bytes]:
        self.requested_chunk_bytes = chunk_size
        for _ in range(64):
            self.yielded += 1
            yield b"s" * chunk_size


def test_s3_verify_observes_one_download_with_bounded_memory() -> None:
    """T3: S3 observes and hashes every byte with memory bounded by chunk size."""
    chunk_bytes = 256 * 1024
    expected = hashlib.sha256()
    for _ in range(64):
        expected.update(b"s" * chunk_bytes)
    body = _GeneratedBody([])
    client = _S3Client(body)
    backend = S3Backend("s3", bucket="bucket", client=client)
    observation = _VerifyObservation()

    tracemalloc.start()
    try:
        result = backend.verify(
            {"key": "object", "sha256": expected.hexdigest()}, observe=observation
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert result.ok
    assert result.measured
    assert result.actual_hash == expected.digest() == observation.digest.digest()
    assert observation.contiguous
    assert observation.calls == body.yielded == 64
    assert observation.size == 64 * chunk_bytes
    assert client.get_calls == 1
    assert "Range" not in client.kwargs
    assert body.closed
    assert peak < 8 * chunk_bytes


def test_memory_verify_observes_its_stored_bytes() -> None:
    """Memory observation carries the same bytes used for the measured digest."""
    backend = MemoryBackend("memory")
    data = b"memory readback"
    expected = backend.add(data)
    observation = _VerifyObservation()
    result = backend.verify({"hash_hex": expected.hex()}, observe=observation)
    assert result.ok
    assert result.measured
    assert observation.contiguous
    assert observation.size == len(data)
    assert result.actual_hash == expected == observation.digest.digest()


@pytest.mark.parametrize(
    "case", ["ok", "mismatch", "invalid", "absent", "download_absent", "no_observer"]
)
def test_ssh_verify_observer_preserves_gates_and_hashes_download(case: str) -> None:
    """Observe one downloaded object only after SSH's existing validity checks."""
    data = b"downloaded bytes"
    expected = hashlib.sha256(data).hexdigest()

    class Transport(_UnusedTransport):
        downloads = 0

        def sha256(self, key: str) -> str | None:
            return None if case == "absent" else expected

        def get(self, key: str, path: Path) -> None:
            self.downloads += 1
            if case == "download_absent":
                raise BackendNotFoundError("gone after remote hash")
            path.write_bytes(b"different" if case == "mismatch" else data)

    transport = Transport()
    backend = SshDiskBackend("ssh", host="unused", root="/unused", transport=transport)
    observation = _VerifyObservation()
    result = backend.verify(
        {"key": "object", "sha256": "invalid" if case == "invalid" else expected},
        observe=None if case == "no_observer" else observation,
    )
    if case in {"invalid", "absent", "download_absent"}:
        assert not result.ok
        assert not result.measured
        assert result.detail == ("invalid hash" if case == "invalid" else "absent")
        assert observation.calls == 0
        assert transport.downloads == (1 if case == "download_absent" else 0)
    else:
        assert result.measured
        assert result.ok is (case != "mismatch")
        if case == "no_observer":
            assert transport.downloads == observation.calls == 0
        else:
            assert transport.downloads == 1
            assert observation.contiguous
            assert result.actual_hash == observation.digest.digest()


@pytest.mark.parametrize("gate_ok", [True, False])
def test_d2_verify_observer_uses_gated_restored_payload(
    gate_ok: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed d2tape hashes gate retains its unmeasured result and reads nothing."""
    backend = D2TapeBackend(
        "d2",
        jar_path=tmp_path / "d2.jar",
        java_bin=tmp_path / "java",
        device_env_path=tmp_path / "device.env",
        state_dir=tmp_path / "state",
    )
    data = b"d2 payload" * 100_000
    digest = hashlib.sha256(data).digest()
    restored: list[Path] = []
    commands: list[list[str]] = []

    def restore(locator: dict[str, Any], scratch: Path) -> Path:
        path = scratch / "payload"
        path.write_bytes(data)
        restored.append(path)
        return path

    def run(argv: list[str], device: Any) -> dict[str, Any]:
        commands.append(argv)
        return {"ok": gate_ok}

    monkeypatch.setattr(backend, "_device_config", lambda: SimpleNamespace(device="unused"))
    monkeypatch.setattr(
        backend,
        "_artifact_for_locator",
        lambda _: {"integrity_hash": digest.hex(), "hashes": {}},
    )
    monkeypatch.setattr(backend, "_restore_payload", restore)
    monkeypatch.setattr(backend, "_run_d2tape", run)
    observation = _VerifyObservation()
    result = backend.verify(
        {"artifact_name": "object", "volume_blocksize": 512, "start_block": 0, "end_block": 1},
        observe=observation,
    )
    assert len(commands) == 1
    assert commands[0][0] == "verify"
    assert "--hashes" in commands[0]
    assert result.ok is gate_ok
    assert result.measured is gate_ok
    if gate_ok:
        assert result.actual_hash == digest == observation.digest.digest()
        assert observation.contiguous
        assert observation.size == len(data)
        assert len(restored) == 1
        assert not restored[0].exists()
    else:
        assert observation.calls == 0
        assert not restored


@pytest.mark.parametrize("position", [0, 17])
def test_ranged_copy_empty_interval_never_opens_backend(position: int, tmp_path: Path) -> None:
    """T5: both [0, 0) and a later empty interval truncate the destination without I/O."""
    backend = SshDiskBackend("ssh", host="unused", root="/unused", transport=_UnusedTransport())
    destination = tmp_path / "empty"
    destination.write_bytes(b"old contents")
    _copy_backend_range_to_path(backend, {"key": "object"}, position, position, destination)
    assert destination.read_bytes() == b""


def test_ranged_copy_truncated_materialized_object_is_restore_error(tmp_path: Path) -> None:
    """T5: a materialized opener's ValueError stays eligible for next-copy fallback."""
    downloads: list[Path] = []

    class Transport(_UnusedTransport):
        def get(self, key: str, destination: Path) -> None:
            downloads.append(destination)
            destination.write_bytes(b"short")

    backend = SshDiskBackend("ssh", host="unused", root="/unused", transport=Transport())
    with pytest.raises(ArchiveRestoreError, match="exceeds object size") as caught:
        _copy_backend_range_to_path(backend, {"key": "object"}, 0, 10, tmp_path / "copy")
    assert isinstance(caught.value.__cause__, ValueError)
    assert len(downloads) == 1
    assert not downloads[0].exists()


@pytest.mark.parametrize("payload", [b"short", b"too many bytes"])
def test_ranged_copy_rejects_native_stream_length_mismatch(payload: bytes, tmp_path: Path) -> None:
    """Finite streams must return the exact interval length, whether short or long."""
    body = _StreamingBody([payload])
    client = _S3Client(body)
    backend = S3Backend("s3", bucket="bucket", client=client)
    with pytest.raises(ArchiveRestoreError, match="expected 10"):
        _copy_backend_range_to_path(backend, {"key": "object"}, 0, 10, tmp_path / "copy")
    assert client.get_calls == 1
    assert body.closed
