"""Tests for P2.1 whole-copy restore primitives.

These tests exercise the asset-scoped restore boundary without depending on a
live Remanence daemon or the RAO CLI. Stored bytes live in a populated
``MemoryBackend`` instance, while a fake opener models representation reversal
and encrypted key-epoch selection.
"""

from __future__ import annotations

import contextlib
import hashlib
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from sutradhara.backend.memory import MemoryBackend
from sutradhara.catalog.models import Backend, Bundle, Copy, LogicalAsset, VerifyReceipt
from sutradhara.catalog.session import create_all, locator_key, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource, content_hash
from sutradhara.restore import (
    RestoreError,
    RestoreIntegrityError,
    atomic_write_verified_chunks,
    atomic_write_verified_file,
    restore_copy,
)
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


class _FakeOpener:
    def __init__(self) -> None:
        self.calls: list[tuple[Representation, tuple[str, ...] | None]] = []

    @contextlib.contextmanager
    def open(
        self,
        source_path: Path | str,
        representation: Representation,
        *,
        recipient_epochs: tuple[str, ...] | None = None,
        key_domain: str | None = None,
        work_dir: Path | str | None = None,
    ) -> Iterator[Path]:
        del key_domain, work_dir
        self.calls.append((representation, recipient_epochs))
        source = Path(source_path)
        if representation in {Representation.RAW_BYTES, Representation.D2TAR_RAW}:
            yield source
            return

        with tempfile.TemporaryDirectory(prefix="test-restore-open-") as raw:
            temp_dir = Path(raw)
            plaintext = temp_dir / "plain.bin"
            prefix, stored_key, payload = source.read_bytes().split(b":", 2)
            assert prefix.decode("ascii") == representation.value
            if representation is Representation.RAO_AEAD_V1:
                assert recipient_epochs is not None
                assert stored_key.decode("ascii") == recipient_epochs[0]
            plaintext.write_bytes(payload)
            yield plaintext


def _sha(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def test_atomic_write_verified_chunks_commits_only_exact_verified_stream(tmp_path: Path) -> None:
    destination = (tmp_path / "verified.bin").resolve()
    payload = b"verified " * 1000

    atomic_write_verified_chunks(
        iter((payload[:17], payload[17:])),
        destination,
        expected_sha256=_sha(payload),
        expected_size_bytes=len(payload),
    )

    assert destination.read_bytes() == payload
    assert list(tmp_path.glob(".verified.bin.*.tmp")) == []


@pytest.mark.parametrize(
    ("expected_sha256", "expected_size", "match"),
    [
        (_sha(b"wrong"), 7, "SHA-256"),
        (_sha(b"payload"), 8, "size"),
    ],
)
def test_atomic_write_verified_chunks_deletes_temp_on_fixity_failure(
    tmp_path: Path,
    expected_sha256: bytes,
    expected_size: int,
    match: str,
) -> None:
    destination = (tmp_path / "failed.bin").resolve()
    destination.write_bytes(b"existing")

    with pytest.raises(RestoreIntegrityError, match=match):
        atomic_write_verified_chunks(
            iter((b"pay", b"load")),
            destination,
            expected_sha256=expected_sha256,
            expected_size_bytes=expected_size,
        )

    assert destination.read_bytes() == b"existing"
    assert list(tmp_path.glob(".failed.bin.*.tmp")) == []


def test_atomic_write_verified_chunks_deletes_temp_on_source_failure(tmp_path: Path) -> None:
    destination = (tmp_path / "source-failed.bin").resolve()

    def failing_chunks() -> Iterator[bytes]:
        yield b"partial"
        raise OSError("source failed")

    with pytest.raises(OSError, match="source failed"):
        atomic_write_verified_chunks(
            failing_chunks(),
            destination,
            expected_sha256=_sha(b"partial"),
            expected_size_bytes=len(b"partial"),
        )

    assert not destination.exists()
    assert list(tmp_path.glob(".source-failed.bin.*.tmp")) == []


def _stored_bytes(
    data: bytes,
    representation: Representation,
    key_epoch: str = "archive-" + "1" * 32,
) -> bytes:
    if representation in {Representation.RAW_BYTES, Representation.D2TAR_RAW}:
        return data
    return representation.value.encode("ascii") + b":" + key_epoch.encode("ascii") + b":" + data


def _metadata(representation: Representation, *, key_epoch: str | None = None) -> dict[str, object]:
    metadata: dict[str, object] = {"representation": representation.value}
    if representation in {Representation.RAO_PLAIN_V1, Representation.RAO_AEAD_V1}:
        metadata["chunk_size"] = 262144
    if representation is Representation.RAO_AEAD_V1 and key_epoch is not None:
        metadata["recipient_epochs"] = [key_epoch, "recovery-" + "2" * 32]
    return metadata


def _add_backend_row(session: Session) -> Backend:
    count = len(list(session.scalars(select(Backend))))
    row = Backend(
        name=f"mem-{count + 1}",
        kind=BackendKind.MEMORY,
        tier=BackendTier.SELF_DESCRIBING,
    )
    session.add(row)
    session.flush()
    return row


def _add_copy(
    engine: Engine,
    backend: MemoryBackend,
    *,
    data: bytes = b"restore payload",
    representation: Representation = Representation.RAW_BYTES,
    key_epoch: str = "archive-" + "1" * 32,
    storage_metadata: dict[str, object] | None = None,
    health: CopyHealth = CopyHealth.OK,
    stored_bytes: bytes | None = None,
) -> int:
    asset_hash = _sha(data)
    stored = (
        stored_bytes if stored_bytes is not None else _stored_bytes(data, representation, key_epoch)
    )
    stored_hash = backend.add(stored)
    locator = {"hash_hex": stored_hash.hex()}
    with session_scope(engine) as s:
        s.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(data)))
        backend_row = _add_backend_row(s)
        copy = Copy(
            logical_asset_hash=asset_hash,
            backend_id=backend_row.id,
            native_locator=locator,
            native_locator_key=locator_key(locator),
            storage_metadata=storage_metadata
            if storage_metadata is not None
            else _metadata(representation, key_epoch=key_epoch),
            integrity_hash=stored_hash,
            source=CopySource.INGEST,
            health=health,
        )
        s.add(copy)
        s.flush()
        return copy.id


def _restore_to_destination(
    session: Session,
    copy: Copy,
    *,
    backend: MemoryBackend,
    opener: _FakeOpener,
    destination: Path,
) -> None:
    with restore_copy(
        session,
        copy,
        backend=backend,
        opener=opener,
        execution_id=f"restore-test:{copy.id}:{destination.name}",
    ) as result:
        atomic_write_verified_file(result.path, destination)


@pytest.mark.parametrize(
    "representation",
    [Representation.RAW_BYTES, Representation.RAO_PLAIN_V1, Representation.RAO_AEAD_V1],
)
def test_restore_copy_round_trips_asset_per_representation(
    engine: Engine,
    tmp_path: Path,
    representation: Representation,
) -> None:
    backend = MemoryBackend("mem")
    opener = _FakeOpener()
    data = f"payload for {representation.value}".encode()
    copy_id = _add_copy(engine, backend, data=data, representation=representation)
    dest = tmp_path / f"{representation.value}.bin"

    with session_scope(engine) as s:
        copy = s.get(Copy, copy_id)
        assert copy is not None
        with restore_copy(
            s,
            copy,
            backend=backend,
            opener=opener,
            execution_id=f"restore-request:{representation.value}",
        ) as result:
            atomic_write_verified_file(result.path, dest)
        assert copy.last_measured_digest == copy.integrity_hash
        receipt = s.scalars(select(VerifyReceipt).where(VerifyReceipt.copy_id == copy.id)).one()
        assert receipt.source == "restore"
        assert receipt.execution_id == f"restore-request:{representation.value}"

    assert dest.read_bytes() == data
    assert result.sha256 == _sha(data)
    if representation is Representation.RAO_AEAD_V1:
        expected_recipients = (
            ("archive-" + "1" * 32, "recovery-" + "2" * 32)
            if representation is Representation.RAO_AEAD_V1
            else None
        )
        assert opener.calls == [(representation, expected_recipients)]


def test_restore_copy_reads_suspect_direct_copy(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = MemoryBackend("mem")
    opener = _FakeOpener()
    data = b"suspect copy remains break-glass readable"
    copy_id = _add_copy(engine, backend, data=data, health=CopyHealth.SUSPECT)
    dest = tmp_path / "suspect.bin"

    with session_scope(engine) as s:
        copy = s.get(Copy, copy_id)
        assert copy is not None
        with restore_copy(
            s,
            copy,
            backend=backend,
            opener=opener,
            execution_id="restore-test:suspect",
        ) as result:
            atomic_write_verified_file(result.path, dest)

    assert dest.read_bytes() == data
    assert result.sha256 == _sha(data)


def test_restore_fails_closed_for_stored_and_content_corruption(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = MemoryBackend("mem")
    opener = _FakeOpener()
    copy_id = _add_copy(engine, backend, data=b"good", representation=Representation.RAW_BYTES)
    dest = tmp_path / "stored-corrupt.bin"
    dest.write_bytes(b"old")
    with session_scope(engine) as s:
        copy = s.get(Copy, copy_id)
        assert copy is not None
        backend.corrupt(content_hash(bytes.fromhex(copy.native_locator["hash_hex"])), b"corrupt")

    with session_scope(engine) as s:
        copy = s.get(Copy, copy_id)
        assert copy is not None
        with pytest.raises(RestoreError, match="stored-corrupt"):
            _restore_to_destination(s, copy, backend=backend, opener=opener, destination=dest)

    assert dest.read_bytes() == b"old"

    backend2 = MemoryBackend("mem")
    wrong_stored = _stored_bytes(b"wrong", Representation.RAO_PLAIN_V1)
    copy_id2 = _add_copy(
        engine,
        backend2,
        data=b"expected",
        representation=Representation.RAO_PLAIN_V1,
        stored_bytes=wrong_stored,
    )
    dest2 = tmp_path / "content-corrupt.bin"
    with session_scope(engine) as s:
        copy = s.get(Copy, copy_id2)
        assert copy is not None
        with pytest.raises(RestoreError, match="content-corrupt"):
            _restore_to_destination(s, copy, backend=backend2, opener=opener, destination=dest2)

    assert not dest2.exists()


def test_restore_rejects_missing_bundle_and_bad_representation(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = MemoryBackend("mem")
    opener = _FakeOpener()
    missing_copy = _add_copy(
        engine,
        backend,
        data=b"missing",
        health=CopyHealth.MISSING,
    )
    bad_meta_copy = _add_copy(
        engine,
        backend,
        data=b"bad meta",
        storage_metadata={},
    )
    bundle_bytes = b"bundle bytes"
    bundle_hash = backend.add(bundle_bytes)
    with session_scope(engine) as s:
        backend_row = _add_backend_row(s)
        bundle = Bundle(id="bundle-1", artifactclass="o-archive")
        s.add(bundle)
        s.flush()
        locator = {"hash_hex": bundle_hash.hex()}
        bundle_copy = Copy(
            bundle_id=bundle.id,
            backend_id=backend_row.id,
            native_locator=locator,
            native_locator_key=locator_key(locator),
            storage_metadata=_metadata(Representation.RAW_BYTES),
            integrity_hash=bundle_hash,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
        )
        s.add(bundle_copy)
        s.flush()
        bundle_copy_id = bundle_copy.id

    for copy_id, reason in [
        (missing_copy, "missing"),
        (bundle_copy_id, "bundle"),
        (bad_meta_copy, "representation"),
    ]:
        dest = tmp_path / f"{copy_id}.bin"
        with session_scope(engine) as s:
            copy = s.get(Copy, copy_id)
            assert copy is not None
            with pytest.raises(RestoreError, match=reason):
                _restore_to_destination(s, copy, backend=backend, opener=opener, destination=dest)
        assert not dest.exists()


def test_restore_copy_temp_lifetime(engine: Engine) -> None:
    backend = MemoryBackend("mem")
    opener = _FakeOpener()
    copy_id = _add_copy(engine, backend, data=b"temp lifetime")
    with session_scope(engine) as s:
        copy = s.get(Copy, copy_id)
        assert copy is not None
        with restore_copy(
            s,
            copy,
            backend=backend,
            opener=opener,
            execution_id="restore-test:temp-lifetime",
        ) as restored:
            temp_path = restored.path
            assert temp_path.exists()
            assert temp_path.read_bytes() == b"temp lifetime"
        assert not temp_path.exists()
