"""Tests for the in-process MemoryBackend.

Exercises the StorageBackend Protocol via the simplest possible impl,
to lock in the trait contract before the real Remanence adapter lands.
"""

from __future__ import annotations

import hashlib

import pytest

from sutradhara.backend.memory import MemoryBackend
from sutradhara.backend.port import (
    BackendNotFoundError,
    ByteRange,
    StorageBackend,
)
from sutradhara.catalog.types import content_hash


def test_memory_backend_satisfies_storagebackend_protocol() -> None:
    backend = MemoryBackend("test-mem")
    assert isinstance(backend, StorageBackend)
    assert backend.name == "test-mem"


def test_enumerate_yields_added_objects() -> None:
    backend = MemoryBackend("mem")
    h1 = backend.add(b"hello world")
    h2 = backend.add(b"another asset")

    records = list(backend.enumerate())
    assert len(records) == 2

    seen = {r.logical_id: r for r in records}
    assert h1 in seen
    assert h2 in seen
    assert seen[h1].size_bytes == len(b"hello world")
    assert seen[h1].integrity_hash == h1
    assert seen[h1].native_locator == {"hash_hex": h1.hex()}


def test_enumerate_carries_extra_metadata() -> None:
    backend = MemoryBackend("mem")
    backend.add(b"asset", source="test-fixture", note="hi")
    [record] = list(backend.enumerate())
    assert record.metadata == {"source": "test-fixture", "note": "hi"}


def test_read_whole_object() -> None:
    backend = MemoryBackend("mem")
    h = backend.add(b"the quick brown fox")
    locator = {"hash_hex": h.hex()}

    data = backend.read_range(locator, ByteRange(0, 0))
    assert data == b"the quick brown fox"


def test_read_subrange() -> None:
    backend = MemoryBackend("mem")
    h = backend.add(b"the quick brown fox")
    locator = {"hash_hex": h.hex()}

    data = backend.read_range(locator, ByteRange(4, 9))
    assert data == b"quick"


def test_read_range_past_end_raises() -> None:
    backend = MemoryBackend("mem")
    h = backend.add(b"short")
    locator = {"hash_hex": h.hex()}

    with pytest.raises(ValueError, match="exceeds object size"):
        backend.read_range(locator, ByteRange(0, 99))


def test_read_unknown_locator_raises_backend_not_found() -> None:
    backend = MemoryBackend("mem")
    bogus = content_hash(hashlib.sha256(b"never stored").digest())
    locator = {"hash_hex": bogus.hex()}

    with pytest.raises(BackendNotFoundError):
        backend.read_range(locator, ByteRange(0, 0))


def test_read_malformed_locator_raises() -> None:
    backend = MemoryBackend("mem")
    with pytest.raises(BackendNotFoundError, match="hash_hex"):
        backend.read_range({"wrong_key": "x"}, ByteRange(0, 0))


def test_verify_happy_path() -> None:
    backend = MemoryBackend("mem")
    h = backend.add(b"verifiable bytes")

    result = backend.verify({"hash_hex": h.hex()})
    assert result.ok
    assert result.actual_hash == h
    assert result.detail == ""


def test_verify_detects_corruption() -> None:
    backend = MemoryBackend("mem")
    h = backend.add(b"original content")
    backend.corrupt(h, b"tampered")

    result = backend.verify({"hash_hex": h.hex()})
    assert not result.ok
    assert result.actual_hash is not None
    assert result.actual_hash != h
    assert "expected" in result.detail


def test_corrupt_unknown_hash_raises() -> None:
    backend = MemoryBackend("mem")
    bogus = content_hash(hashlib.sha256(b"never stored").digest())
    with pytest.raises(BackendNotFoundError):
        backend.corrupt(bogus)


def test_byte_range_validates_negative() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        ByteRange(-1, 5)
    with pytest.raises(ValueError, match="non-negative"):
        ByteRange(5, -1)


def test_byte_range_validates_end_before_start() -> None:
    with pytest.raises(ValueError, match=">= start"):
        ByteRange(10, 5)


def test_byte_range_rejects_zero_end_except_whole_object() -> None:
    with pytest.raises(ValueError, match="whole-object"):
        ByteRange(5, 0)


def test_byte_range_whole_object_is_special_zero_zero() -> None:
    r = ByteRange(0, 0)
    assert r.is_whole_object
    assert r.length == 0

    r2 = ByteRange(5, 10)
    assert not r2.is_whole_object
    assert r2.length == 5
