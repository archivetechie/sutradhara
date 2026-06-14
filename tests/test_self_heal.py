"""Tests for multi-copy self-heal from surviving backend copies.

Scenario Q rebuilds missing target pools from an existing healthy copy,
not from an external original source. These tests use a readable in-process
write backend for deterministic control-flow coverage, plus one real RAO round
trip to prove an encrypted copy rebuilt from a plaintext survivor opens to the
original bytes.
"""

from __future__ import annotations

import contextlib
import hashlib
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, select

from sutradhara.backend.port import (
    BackendLocator,
    ByteRange,
    CopyRecord,
    VerifyResult,
)
from sutradhara.catalog.copies import add_copy
from sutradhara.catalog.models import ArtifactClassPool, Backend, Copy, LogicalAsset, Pool
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import (
    BackendKind,
    BackendTier,
    ContentHash,
    CopyHealth,
    CopySource,
    content_hash,
)
from sutradhara.keys import KeyEpoch, KeyRegistry
from sutradhara.replication import (
    ReplicationInvariantError,
    SelfHealUnavailable,
    replicate_asset,
    replication_status,
    self_heal,
)
from sutradhara.sealing.rao import RaoCliOpener, RaoCliSealer, resolve_rem_bin
from sutradhara.sealing.port import Representation, SealResult


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


class _ReadableTaggedWriteBackend:
    def __init__(
        self,
        name: str,
        *,
        tape_by_pool: dict[str, str] | None = None,
    ) -> None:
        self._name = name
        self._tape_by_pool = tape_by_pool or {}
        self._objects: dict[str, bytes] = {}
        self._records: list[CopyRecord] = []
        self.writes: list[str] = []

    @property
    def name(self) -> str:
        return self._name

    def put_object(self, pool: str, data: bytes) -> CopyRecord:
        digest = content_hash(hashlib.sha256(data).digest())
        object_id = f"{len(self._records) + 1:032x}"
        tape_uuid = self._tape_by_pool.get(pool, f"{len(self._records) + 1:032x}")
        locator = {
            "pool_id": pool,
            "tape_uuid": tape_uuid,
            "tape_file_number": len(self._records) + 1,
            "object_id": object_id,
            "content_sha256": digest.hex(),
        }
        record = CopyRecord(
            logical_id=digest,
            native_locator=locator,
            integrity_hash=digest,
            size_bytes=len(data),
        )
        self._objects[object_id] = data
        self._records.append(record)
        return record

    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        self.writes.append(pool)
        return self.put_object(pool, Path(source).read_bytes())

    def enumerate(self) -> Iterator[CopyRecord]:
        return iter(self._records)

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        object_id = str(locator["object_id"])
        data = self._objects[object_id]
        if byte_range.is_whole_object:
            return data
        return data[byte_range.start : byte_range.end]

    def verify(self, locator: BackendLocator) -> VerifyResult:
        actual = content_hash(hashlib.sha256(self.read_range(locator, ByteRange(0, 0))).digest())
        expected = content_hash(bytes.fromhex(str(locator["content_sha256"])))
        return VerifyResult(ok=actual == expected, actual_hash=actual)


class _FakeSealer:
    def __init__(self) -> None:
        self.calls: list[tuple[Representation, str | None]] = []

    @contextlib.contextmanager
    def seal(
        self,
        source_path: Path | str,
        representation: Representation,
        *,
        key_epoch: KeyEpoch | None = None,
    ) -> Iterator[SealResult]:
        source = Path(source_path)
        plaintext_digest = hashlib.sha256(source.read_bytes()).digest()
        key_id = key_epoch.key_id if key_epoch is not None else None
        self.calls.append((representation, key_id))
        if representation is Representation.RAW_BYTES:
            yield SealResult(source, plaintext_digest, plaintext_digest, representation)
            return

        with tempfile.TemporaryDirectory(prefix="test-seal-") as temp_dir_raw:
            sealed_path = Path(temp_dir_raw) / "sealed.bin"
            sealed_path.write_bytes(
                representation.value.encode("ascii")
                + b":"
                + (key_id or "").encode("ascii")
                + b":"
                + source.read_bytes()
            )
            stored_digest = hashlib.sha256(sealed_path.read_bytes()).digest()
            yield SealResult(
                sealed_path,
                stored_digest,
                plaintext_digest,
                representation,
            )


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
        if representation is Representation.RAW_BYTES:
            yield source
            return

        with tempfile.TemporaryDirectory(prefix="test-open-") as temp_dir_raw:
            plaintext_path = Path(temp_dir_raw) / "plain.bin"
            plaintext_path.write_bytes(source.read_bytes().split(b":", 2)[2])
            yield plaintext_path


def _add_backend(engine: Engine) -> int:
    with session_scope(engine) as s:
        row = Backend(
            name="rem",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
            config={"daemon_endpoint": "unix:/fake/rem.sock"},
        )
        s.add(row)
        s.flush()
        return row.id


def _add_asset(engine: Engine, data: bytes) -> ContentHash:
    digest = content_hash(hashlib.sha256(data).digest())
    with session_scope(engine) as s:
        s.add(LogicalAsset(content_sha256=digest, size_bytes=len(data)))
    return digest


def _add_o_archive_pools(engine: Engine, backend_id: int) -> None:
    with session_scope(engine) as s:
        s.add(
            Pool(
                id="o-copy-1-pool",
                backend_id=backend_id,
                representation=Representation.RAO_PLAIN_V1.value,
            )
        )
        s.add(
            Pool(
                id="o-copy-2-pool",
                backend_id=backend_id,
                representation=Representation.RAO_AEAD_V1.value,
            )
        )
        s.add(
            ArtifactClassPool(
                artifactclass="o-archive",
                pool_id="o-copy-1-pool",
                sort_order=1,
            )
        )
        s.add(
            ArtifactClassPool(
                artifactclass="o-archive",
                pool_id="o-copy-2-pool",
                sort_order=2,
            )
        )


def _metadata(representation: Representation) -> dict[str, object]:
    return {"representation": representation.value}


def _backend() -> _ReadableTaggedWriteBackend:
    return _ReadableTaggedWriteBackend(
        "rem",
        tape_by_pool={
            "o-copy-1-pool": "1" * 32,
            "o-copy-2-pool": "2" * 32,
        },
    )


def test_self_heal_noop_when_nothing_is_missing(engine: Engine) -> None:
    data = b"complete asset"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    _add_o_archive_pools(engine, backend_id)
    backend = _backend()
    copy1 = backend.put_object("o-copy-1-pool", b"copy-one")
    copy2 = backend.put_object("o-copy-2-pool", b"copy-two")

    with session_scope(engine) as s:
        for record in (copy1, copy2):
            add_copy(
                s,
                logical_asset_hash=asset_hash,
                backend_id=backend_id,
                native_locator=record.native_locator,
                integrity_hash=record.integrity_hash,
                source=CopySource.INGEST,
                pool_id=str(record.native_locator["pool_id"]),
                storage_metadata=_metadata(
                    Representation.RAO_PLAIN_V1
                    if record.native_locator["pool_id"] == "o-copy-1-pool"
                    else Representation.RAO_AEAD_V1
                ),
            )
        repaired = self_heal(
            s,
            asset_hash,
            "o-archive",
            backends={backend_id: backend},
            key_epoch="1" * 32,
            opener=_FakeOpener(),
            sealer=_FakeSealer(),
        )

    assert repaired == []
    assert backend.writes == []


def test_self_heal_raises_when_no_healthy_source_remains(engine: Engine) -> None:
    asset_hash = _add_asset(engine, b"lost asset")
    backend_id = _add_backend(engine)
    _add_o_archive_pools(engine, backend_id)

    with session_scope(engine) as s, pytest.raises(SelfHealUnavailable, match="no healthy"):
        self_heal(
            s,
            asset_hash,
            "o-archive",
            backends={backend_id: _backend()},
            key_epoch="1" * 32,
            opener=_FakeOpener(),
            sealer=_FakeSealer(),
        )


def test_self_heal_rebuilds_missing_encrypted_copy_from_survivor(
    engine: Engine,
) -> None:
    data = b"heal from surviving copy"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    _add_o_archive_pools(engine, backend_id)
    backend = _backend()
    opener = _FakeOpener()
    sealer = _FakeSealer()
    key_id = "1" * 32

    source_bytes = b"rao-plain-v1::" + data
    source_record = backend.put_object("o-copy-1-pool", source_bytes)
    missing_record = backend.put_object("o-copy-2-pool", b"lost old copy")

    with session_scope(engine) as s:
        add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator=source_record.native_locator,
            integrity_hash=source_record.integrity_hash,
            source=CopySource.INGEST,
            pool_id="o-copy-1-pool",
            storage_metadata=_metadata(Representation.RAO_PLAIN_V1),
        )
        add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator=missing_record.native_locator,
            integrity_hash=missing_record.integrity_hash,
            source=CopySource.INGEST,
            health=CopyHealth.MISSING,
            pool_id="o-copy-2-pool",
            storage_metadata=_metadata(Representation.RAO_AEAD_V1),
        )

        repaired = self_heal(
            s,
            asset_hash,
            "o-archive",
            backends={backend_id: backend},
            key_epoch=key_id,
            opener=opener,
            sealer=sealer,
        )
        status = replication_status(
            s,
            asset_hash,
            "o-archive",
            {backend_id: backend},
            key_epoch=key_id,
        )

    assert len(repaired) == 1
    assert repaired[0].native_locator["pool_id"] == "o-copy-2-pool"
    assert status["complete"] is True
    assert backend.writes == ["o-copy-2-pool"]
    assert opener.calls == [(Representation.RAO_PLAIN_V1, None)]
    assert sealer.calls == [(Representation.RAO_AEAD_V1, key_id)]


def test_self_heal_rejects_source_that_opens_to_wrong_plaintext(
    engine: Engine,
) -> None:
    data = b"original"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    _add_o_archive_pools(engine, backend_id)
    backend = _backend()
    source_record = backend.put_object("o-copy-1-pool", b"rao-plain-v1::tampered")

    with session_scope(engine) as s:
        add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator=source_record.native_locator,
            integrity_hash=source_record.integrity_hash,
            source=CopySource.INGEST,
            pool_id="o-copy-1-pool",
            storage_metadata=_metadata(Representation.RAO_PLAIN_V1),
        )

        with pytest.raises(ReplicationInvariantError, match="source plaintext hash"):
            self_heal(
                s,
                asset_hash,
                "o-archive",
                backends={backend_id: backend},
                key_epoch="1" * 32,
                opener=_FakeOpener(),
                sealer=_FakeSealer(),
            )


def test_self_heal_rebuilt_encrypted_copy_opens_with_epoch_key(
    engine: Engine,
    tmp_path: Path,
) -> None:
    try:
        resolve_rem_bin()
    except FileNotFoundError as exc:
        pytest.skip(str(exc))

    data = b"real rao self heal"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    _add_o_archive_pools(engine, backend_id)
    backend = _backend()
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    registry = KeyRegistry(tmp_path / "keys")
    epoch = registry.create_epoch()

    with session_scope(engine) as s:
        replicate_asset(
            s,
            asset_hash,
            source,
            "o-archive",
            backends={backend_id: backend},
            sealer=RaoCliSealer(registry),
            key_epoch=epoch.key_id,
        )
        copy2 = next(
            copy
            for copy in s.scalars(select(Copy))
            if copy.native_locator["pool_id"] == "o-copy-2-pool"
        )
        copy2.health = CopyHealth.MISSING

    with session_scope(engine) as s:
        repaired = self_heal(
            s,
            asset_hash,
            "o-archive",
            backends={backend_id: backend},
            opener=RaoCliOpener(registry),
            sealer=RaoCliSealer(registry),
            key_epoch=epoch.key_id,
        )
        [rebuilt] = repaired
        stored = backend.read_range(rebuilt.native_locator, ByteRange(0, 0))

    rebuilt_rao = tmp_path / "rebuilt.rao"
    rebuilt_rao.write_bytes(stored)
    with RaoCliOpener(registry).open(
        rebuilt_rao,
        Representation.RAO_AEAD_V1,
        key_epoch=epoch,
    ) as plaintext_path:
        assert plaintext_path.read_bytes() == data
