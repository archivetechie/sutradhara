"""Tests for P2.1 whole-copy restore jobs and primitives.

These tests exercise the asset-scoped restore boundary without depending on a
live Remanence daemon or the RAO CLI. Stored bytes live in a populated
``MemoryBackend`` instance injected through the handler seam, while a fake
opener models representation reversal and encrypted key-epoch selection.
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
from sutradhara.catalog.models import Backend, Bundle, Copy, LogicalAsset
from sutradhara.catalog.session import create_all, locator_key, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource, content_hash
from sutradhara.jobs import handlers as _handlers  # noqa: F401 -- register built-ins
from sutradhara.jobs.dispatch import dispatch_restore
from sutradhara.jobs.engine import run_one, submit
from sutradhara.jobs.models import Job, JobStatus
from sutradhara.keys import KeyEpoch
from sutradhara.restore import restore_copy
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


class _FakeOpener:
    def __init__(self) -> None:
        self.calls: list[tuple[Representation, str | None]] = []

    @contextlib.contextmanager
    def open(
        self,
        source_path: Path | str,
        representation: Representation,
        *,
        key_epoch: KeyEpoch | None = None,
    ) -> Iterator[Path]:
        key_id = key_epoch.key_id if key_epoch is not None else None
        self.calls.append((representation, key_id))
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
                assert stored_key.decode("ascii") == key_id
            plaintext.write_bytes(payload)
            yield plaintext


def _sha(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _stored_bytes(data: bytes, representation: Representation, key_epoch: str = "epoch-1") -> bytes:
    if representation in {Representation.RAW_BYTES, Representation.D2TAR_RAW}:
        return data
    return representation.value.encode("ascii") + b":" + key_epoch.encode("ascii") + b":" + data


def _metadata(representation: Representation, *, key_epoch: str | None = None) -> dict[str, object]:
    metadata: dict[str, object] = {"representation": representation.value}
    if representation in {Representation.RAO_PLAIN_V1, Representation.RAO_AEAD_V1}:
        metadata["chunk_size"] = 262144
    if representation is Representation.RAO_AEAD_V1 and key_epoch is not None:
        metadata["key_epoch"] = key_epoch
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
    key_epoch: str = "epoch-1",
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


@pytest.mark.parametrize(
    "representation",
    [Representation.RAW_BYTES, Representation.RAO_PLAIN_V1, Representation.RAO_AEAD_V1],
)
def test_restore_job_round_trips_asset_per_representation(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    representation: Representation,
) -> None:
    backend = MemoryBackend("mem")
    opener = _FakeOpener()
    data = f"payload for {representation.value}".encode()
    copy_id = _add_copy(engine, backend, data=data, representation=representation)
    dest = tmp_path / f"{representation.value}.bin"

    import sutradhara.jobs.handlers.restore as restore_handler

    monkeypatch.setattr(restore_handler, "resolve_restore_backend", lambda _copy: backend)
    monkeypatch.setattr(restore_handler, "RaoCliOpener", lambda _registry: opener)
    with session_scope(engine) as s:
        handle = dispatch_restore(s, copy_id, dest)
        assert isinstance(handle["params"]["dest_path"], str)
        result = run_one(s, handle["job_id"])
        job = s.get(Job, handle["job_id"])

    assert result.ok
    assert job is not None
    assert job.status == JobStatus.SUCCEEDED
    assert dest.read_bytes() == data
    assert job.step_state["restore"]["sha256"] == _sha(data).hex()
    if representation is Representation.RAO_AEAD_V1:
        assert opener.calls == [(representation, "epoch-1")]


def test_restore_fails_closed_for_stored_and_content_corruption(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sutradhara.jobs.handlers.restore as restore_handler

    backend = MemoryBackend("mem")
    opener = _FakeOpener()
    copy_id = _add_copy(engine, backend, data=b"good", representation=Representation.RAW_BYTES)
    dest = tmp_path / "stored-corrupt.bin"
    dest.write_bytes(b"old")
    with session_scope(engine) as s:
        copy = s.get(Copy, copy_id)
        assert copy is not None
        backend.corrupt(content_hash(bytes.fromhex(copy.native_locator["hash_hex"])), b"corrupt")

    monkeypatch.setattr(restore_handler, "resolve_restore_backend", lambda _copy: backend)
    monkeypatch.setattr(restore_handler, "RaoCliOpener", lambda _registry: opener)
    with session_scope(engine) as s:
        job = submit(s, "restore", {"copy_id": copy_id, "dest_path": str(dest)})
        result = run_one(s, job.id)

    assert not result.ok
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
    monkeypatch.setattr(restore_handler, "resolve_restore_backend", lambda _copy: backend2)
    with session_scope(engine) as s:
        job = submit(s, "restore", {"copy_id": copy_id2, "dest_path": str(dest2)})
        result = run_one(s, job.id)

    assert not result.ok
    assert not dest2.exists()


def test_restore_rejects_missing_bundle_and_bad_representation(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sutradhara.jobs.handlers.restore as restore_handler

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

    monkeypatch.setattr(restore_handler, "resolve_restore_backend", lambda _copy: backend)
    monkeypatch.setattr(restore_handler, "RaoCliOpener", lambda _registry: opener)
    for copy_id, reason in [
        (missing_copy, "missing"),
        (bundle_copy_id, "bundle"),
        (bad_meta_copy, "representation"),
    ]:
        dest = tmp_path / f"{copy_id}.bin"
        with session_scope(engine) as s:
            job = submit(s, "restore", {"copy_id": copy_id, "dest_path": str(dest)})
            result = run_one(s, job.id)
        assert not result.ok
        assert reason in result.detail
        assert not dest.exists()


def test_restore_copy_temp_lifetime(engine: Engine) -> None:
    backend = MemoryBackend("mem")
    opener = _FakeOpener()
    copy_id = _add_copy(engine, backend, data=b"temp lifetime")
    with session_scope(engine) as s:
        copy = s.get(Copy, copy_id)
        assert copy is not None
        with restore_copy(s, copy, backend=backend, opener=opener) as restored:
            temp_path = restored.path
            assert temp_path.exists()
            assert temp_path.read_bytes() == b"temp lifetime"
        assert not temp_path.exists()
