"""Pool-backed replication tests."""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
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
    CopyHealth,
    CopySource,
    content_hash,
)
from sutradhara.durability import AssetTarget
from sutradhara.jobs.engine import submit
from sutradhara.keys import KeyEpoch
from sutradhara.replication import (
    PoolRepresentationError,
    ReplicationInvariantError,
    repair,
    replicate_asset,
    replication_status,
    select_restore_source,
    select_source,
    target_pools,
)
from sutradhara.sealing.port import Representation, SealResult
from sutradhara.sealing.rao import RAO_CHUNK_SIZE

ARCHIVE_EPOCH = "archive-" + "1" * 32
RECOVERY_EPOCH = "recovery-" + "2" * 32


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


class _PoolWriteBackend:
    def __init__(
        self,
        name: str,
        *,
        tape_by_pool: dict[str, str] | None = None,
    ) -> None:
        self._name = name
        self._tape_by_pool = tape_by_pool or {}
        self.writes: list[str] = []

    @property
    def name(self) -> str:
        return self._name

    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        data = Path(source).read_bytes()
        digest = content_hash(hashlib.sha256(data).digest())
        self.writes.append(pool)
        tape_uuid = self._tape_by_pool.get(pool, f"{len(self.writes):032x}")
        return CopyRecord(
            logical_id=digest,
            native_locator={
                "pool_id": pool,
                "tape_uuid": tape_uuid,
                "tape_file_number": len(self.writes),
                "object_id": f"{len(self.writes):032x}",
                "content_sha256": digest.hex(),
                "body_format": "rem-tar-v1",
            },
            integrity_hash=digest,
            size_bytes=len(data),
        )

    def enumerate(self) -> Iterator[CopyRecord]:
        return iter(())

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        raise NotImplementedError

    def verify(self, locator: BackendLocator) -> VerifyResult:
        raise NotImplementedError


class _WrongHashBackend(_PoolWriteBackend):
    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        record = super().write_object_to_pool(source, pool)
        wrong = content_hash(hashlib.sha256(b"different").digest())
        return CopyRecord(
            logical_id=wrong,
            native_locator=record.native_locator,
            integrity_hash=wrong,
            size_bytes=record.size_bytes,
        )


class _D2TapeFakeBackend(_PoolWriteBackend):
    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        data = Path(source).read_bytes()
        digest = content_hash(hashlib.sha256(data).digest())
        self.writes.append(pool)
        start_block = 2 + (len(self.writes) - 1) * 4
        artifact_name = f"n-{digest.hex()[:16]}"
        return CopyRecord(
            logical_id=digest,
            native_locator={
                "barcode": "D2T002L7",
                "volume_uuid": "00000000-0000-4000-8000-00000000000f",
                "artifact_name": artifact_name,
                "start_block": start_block,
                "end_block": start_block + 3,
                "volume_blocksize": 256000,
                "pool_id": pool,
            },
            integrity_hash=digest,
            size_bytes=len(data),
        )


class _FakeSealer:
    def __init__(
        self,
        tmp_path: Path,
        *,
        plaintext_digest_override: bytes | None = None,
        stored_digest_override: bytes | None = None,
    ) -> None:
        self._tmp_path = tmp_path
        self._plaintext_digest_override = plaintext_digest_override
        self._stored_digest_override = stored_digest_override
        self.calls: list[tuple[Representation, str | None]] = []
        self.results: list[SealResult] = []

    @contextlib.contextmanager
    def seal(
        self,
        source_path: Path | str,
        representation: Representation,
        *,
        key_epoch: KeyEpoch | None = None,
    ) -> Iterator[SealResult]:
        source = Path(source_path)
        plaintext = hashlib.sha256(source.read_bytes()).digest()
        key_id = key_epoch.key_id if key_epoch is not None else None
        self.calls.append((representation, key_id))

        if representation in {Representation.RAW_BYTES, Representation.D2TAR_RAW}:
            result = SealResult(source, plaintext, plaintext, representation)
            self.results.append(result)
            yield result
            return

        sealed_path = self._tmp_path / f"sealed-{len(self.results)}.bin"
        sealed_path.write_bytes(
            representation.value.encode("ascii")
            + b":"
            + (key_id or "").encode("ascii")
            + b":"
            + source.read_bytes()
        )
        stored = hashlib.sha256(sealed_path.read_bytes()).digest()
        result = SealResult(
            sealed_path=sealed_path,
            stored_digest=self._stored_digest_override or stored,
            plaintext_digest=self._plaintext_digest_override or plaintext,
            representation=representation,
            recipient_epochs=(key_id, RECOVERY_EPOCH)
            if representation is Representation.RAO_AEAD_V1 and key_id is not None
            else (),
        )
        self.results.append(result)
        try:
            yield result
        finally:
            with contextlib.suppress(FileNotFoundError):
                sealed_path.unlink()


def _add_backend(
    engine: Engine,
    name: str = "rem",
    kind: BackendKind = BackendKind.REM_TAPE,
) -> int:
    with session_scope(engine) as s:
        row = Backend(
            name=name,
            kind=kind,
            tier=BackendTier.SELF_DESCRIBING,
            config={"daemon_endpoint": "unix:/fake/rem.sock"},
        )
        s.add(row)
        s.flush()
        return row.id


def _add_pool(
    engine: Engine,
    *,
    backend_id: int,
    pool_id: str,
    artifactclass: str,
    representation: Representation,
    sort_order: int = 0,
    accepts_writes: bool = True,
) -> None:
    with session_scope(engine) as s:
        s.add(
            Pool(
                id=pool_id,
                backend_id=backend_id,
                representation=representation.value,
                accepts_writes=accepts_writes,
            )
        )
        s.add(
            ArtifactClassPool(
                artifactclass=artifactclass,
                pool_id=pool_id,
                sort_order=sort_order,
            )
        )


def _add_asset(engine: Engine, data: bytes) -> bytes:
    digest = hashlib.sha256(data).digest()
    with session_scope(engine) as s:
        s.add(LogicalAsset(content_sha256=digest, size_bytes=len(data)))
    return digest


def _metadata(representation: Representation) -> dict[str, object]:
    metadata: dict[str, object] = {"representation": representation.value}
    if representation in {Representation.RAO_PLAIN_V1, Representation.RAO_AEAD_V1}:
        metadata["chunk_size"] = RAO_CHUNK_SIZE
    return metadata


def _qualify_fixture_copy(
    copy: Copy,
    *,
    measured_at: dt.datetime | None = None,
) -> None:
    """Populate explicit read-back evidence for catalog-only fixture copies."""

    at = measured_at or dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    copy.last_checked_at = at
    copy.last_measured_digest = copy.integrity_hash
    copy.last_measured_at = at


def test_target_pools_reads_active_memberships_and_representations(
    engine: Engine,
) -> None:
    backend_id = _add_backend(engine)
    _add_pool(
        engine,
        backend_id=backend_id,
        pool_id="o-copy-1-pool",
        artifactclass="o-archive",
        representation=Representation.RAO_PLAIN_V1,
        sort_order=1,
    )
    _add_pool(
        engine,
        backend_id=backend_id,
        pool_id="o-copy-2-pool",
        artifactclass="o-archive",
        representation=Representation.RAO_AEAD_V1,
        sort_order=2,
    )
    backend = _PoolWriteBackend("rem")

    with session_scope(engine) as s:
        targets = target_pools(
            s,
            "o-archive",
            {backend_id: backend},
            key_epoch=ARCHIVE_EPOCH,
        )

    assert [target.pool_id for _, target in targets] == [
        "o-copy-1-pool",
        "o-copy-2-pool",
    ]
    assert [target.representation for _, target in targets] == [
        Representation.RAO_PLAIN_V1.value,
        Representation.RAO_AEAD_V1.value,
    ]
    assert [target.key_epoch for _, target in targets] == [None, ARCHIVE_EPOCH]


def test_target_pools_excludes_write_fenced_by_default_but_status_keeps_it(
    engine: Engine,
) -> None:
    data = b"fenced source"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    _add_pool(
        engine,
        backend_id=backend_id,
        pool_id="active-pool",
        artifactclass="o-archive",
        representation=Representation.RAW_BYTES,
        sort_order=0,
    )
    _add_pool(
        engine,
        backend_id=backend_id,
        pool_id="fenced-pool",
        artifactclass="o-archive",
        representation=Representation.RAW_BYTES,
        sort_order=1,
        accepts_writes=False,
    )
    backend = _PoolWriteBackend("rem")

    with session_scope(engine) as s:
        active_copy, _ = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            pool_id="active-pool",
            native_locator={"object": "active", "tape_uuid": "active-tape"},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
            storage_metadata=_metadata(Representation.RAW_BYTES),
        )
        _qualify_fixture_copy(active_copy)
        write_targets = target_pools(s, "o-archive", {backend_id: backend})
        status = replication_status(s, asset_hash, "o-archive", {backend_id: backend})

    assert [target.pool_id for _, target in write_targets] == ["active-pool"]
    assert {target.pool_id for target in status["want"]} == {"active-pool", "fenced-pool"}
    assert {target.pool_id for target in status["missing"]} == {"fenced-pool"}


def test_pool_sealing_rejects_hdcache_key_epoch(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"private pool asset"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    _add_pool(
        engine,
        backend_id=backend_id,
        pool_id="private-pool",
        artifactclass="video-priv",
        representation=Representation.RAO_AEAD_V1,
    )
    backend = _PoolWriteBackend("rem")

    with (
        session_scope(engine) as s,
        pytest.raises(
            ReplicationInvariantError,
            match="requires archive key epochs",
        ),
    ):
        replicate_asset(
            s,
            asset_hash,
            source,
            "video-priv",
            backends={backend_id: backend},
            sealer=_FakeSealer(tmp_path),
            key_epoch="hdcache-" + "1" * 32,
        )


def test_replicate_asset_fans_out_and_records_each_pool_copy(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"multi-pool asset"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    for index, pool_id in enumerate(("private-copy-1", "private-copy-2")):
        _add_pool(
            engine,
            backend_id=backend_id,
            pool_id=pool_id,
            artifactclass="video-priv",
            representation=Representation.RAW_BYTES,
            sort_order=index,
        )
    backend = _PoolWriteBackend(
        "rem",
        tape_by_pool={
            "private-copy-1": "1" * 32,
            "private-copy-2": "2" * 32,
        },
    )
    sealer = _FakeSealer(tmp_path)

    with session_scope(engine) as s:
        copies = replicate_asset(
            s,
            asset_hash,
            source,
            "video-priv",
            backends={backend_id: backend},
            sealer=sealer,
        )

    assert backend.writes == ["private-copy-1", "private-copy-2"]
    assert len(copies) == 2
    with session_scope(engine) as s:
        rows = list(s.scalars(select(Copy).order_by(Copy.id)))
        assert {row.pool_id for row in rows} == {"private-copy-1", "private-copy-2"}
        assert {row.storage_metadata["representation"] for row in rows} == {
            Representation.RAW_BYTES.value
        }
        assert {row.native_locator["tape_uuid"] for row in rows} == {
            "1" * 32,
            "2" * 32,
        }
        assert list(s.scalars(select(ArtifactClassPool))).pop().artifactclass == "video-priv"


def test_replicate_asset_records_stored_digest_for_rao_pool_copies(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"scenario-o asset"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    _add_pool(
        engine,
        backend_id=backend_id,
        pool_id="o-copy-1-pool",
        artifactclass="o-archive",
        representation=Representation.RAO_PLAIN_V1,
        sort_order=1,
    )
    _add_pool(
        engine,
        backend_id=backend_id,
        pool_id="o-copy-2-pool",
        artifactclass="o-archive",
        representation=Representation.RAO_AEAD_V1,
        sort_order=2,
    )
    backend = _PoolWriteBackend(
        "rem",
        tape_by_pool={
            "o-copy-1-pool": "1" * 32,
            "o-copy-2-pool": "2" * 32,
        },
    )
    sealer = _FakeSealer(tmp_path)

    with session_scope(engine) as s:
        copies = replicate_asset(
            s,
            asset_hash,
            source,
            "o-archive",
            backends={backend_id: backend},
            sealer=sealer,
            key_epoch=ARCHIVE_EPOCH,
        )

    assert backend.writes == ["o-copy-1-pool", "o-copy-2-pool"]
    assert sealer.calls == [
        (Representation.RAO_PLAIN_V1, None),
        (Representation.RAO_AEAD_V1, ARCHIVE_EPOCH),
    ]
    assert len(copies) == 2
    with session_scope(engine) as s:
        rows = {row.pool_id: row for row in s.scalars(select(Copy).order_by(Copy.id))}
        assert rows["o-copy-1-pool"].integrity_hash == sealer.results[0].stored_digest
        assert rows["o-copy-2-pool"].integrity_hash == sealer.results[1].stored_digest
        assert rows["o-copy-1-pool"].storage_metadata == {
            "representation": Representation.RAO_PLAIN_V1.value,
            "chunk_size": RAO_CHUNK_SIZE,
        }
        assert rows["o-copy-2-pool"].storage_metadata == {
            "representation": Representation.RAO_AEAD_V1.value,
            "chunk_size": RAO_CHUNK_SIZE,
            "recipient_epochs": [ARCHIVE_EPOCH, RECOVERY_EPOCH],
        }


def test_replicate_asset_n_archive_writes_three_copies_across_two_backends(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"scenario-n asset"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    asset_hash = _add_asset(engine, data)
    rem_backend_id = _add_backend(engine, name="mem-rem", kind=BackendKind.MEMORY)
    d2_backend_id = _add_backend(engine, name="d2-tape", kind=BackendKind.D2_TAPE)
    _add_pool(
        engine,
        backend_id=rem_backend_id,
        pool_id="n-copy-1",
        artifactclass="n-archive",
        representation=Representation.RAO_PLAIN_V1,
        sort_order=1,
    )
    _add_pool(
        engine,
        backend_id=rem_backend_id,
        pool_id="n-copy-2",
        artifactclass="n-archive",
        representation=Representation.RAO_AEAD_V1,
        sort_order=2,
    )
    _add_pool(
        engine,
        backend_id=d2_backend_id,
        pool_id="n-copy-3",
        artifactclass="n-archive",
        representation=Representation.D2TAR_RAW,
        sort_order=3,
    )
    rem_backend = _PoolWriteBackend(
        "mem-rem",
        tape_by_pool={"n-copy-1": "1" * 32, "n-copy-2": "2" * 32},
    )
    d2_backend = _D2TapeFakeBackend("d2-tape")
    sealer = _FakeSealer(tmp_path)

    with session_scope(engine) as s:
        copies = replicate_asset(
            s,
            asset_hash,
            source,
            "n-archive",
            backends={rem_backend_id: rem_backend, d2_backend_id: d2_backend},
            sealer=sealer,
            key_epoch=ARCHIVE_EPOCH,
        )
        status = replication_status(
            s,
            asset_hash,
            "n-archive",
            {rem_backend_id: rem_backend, d2_backend_id: d2_backend},
            key_epoch=ARCHIVE_EPOCH,
        )

    assert rem_backend.writes == ["n-copy-1", "n-copy-2"]
    assert d2_backend.writes == ["n-copy-3"]
    assert len(copies) == 3
    assert status["complete"] is True
    assert {target.pool_id for target in status["have"]} == {
        "n-copy-1",
        "n-copy-2",
        "n-copy-3",
    }
    assert sealer.calls == [
        (Representation.RAO_PLAIN_V1, None),
        (Representation.RAO_AEAD_V1, ARCHIVE_EPOCH),
        (Representation.D2TAR_RAW, None),
    ]

    with session_scope(engine) as s:
        rows = list(s.scalars(select(Copy).order_by(Copy.id)))
        assert {row.backend.name for row in rows} == {"mem-rem", "d2-tape"}
        [d2_copy] = [row for row in rows if row.backend.name == "d2-tape"]
        assert d2_copy.storage_metadata == {"representation": Representation.D2TAR_RAW.value}
        assert d2_copy.integrity_hash == asset_hash


def test_replicate_asset_rejects_rao_plaintext_digest_mismatch(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"bad plaintext digest"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    _add_pool(
        engine,
        backend_id=backend_id,
        pool_id="o-copy-1-pool",
        artifactclass="o-archive",
        representation=Representation.RAO_PLAIN_V1,
    )
    backend = _PoolWriteBackend("rem")
    sealer = _FakeSealer(
        tmp_path,
        plaintext_digest_override=hashlib.sha256(b"different").digest(),
    )

    with (
        session_scope(engine) as s,
        pytest.raises(
            ReplicationInvariantError,
            match="plaintext_digest",
        ),
    ):
        replicate_asset(
            s,
            asset_hash,
            source,
            "o-archive",
            backends={backend_id: backend},
            sealer=sealer,
        )


def test_replicate_asset_rejects_rao_stored_digest_mismatch(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"bad stored digest"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    _add_pool(
        engine,
        backend_id=backend_id,
        pool_id="o-copy-1-pool",
        artifactclass="o-archive",
        representation=Representation.RAO_PLAIN_V1,
    )
    backend = _PoolWriteBackend("rem")
    sealer = _FakeSealer(
        tmp_path,
        stored_digest_override=hashlib.sha256(b"different").digest(),
    )

    with (
        session_scope(engine) as s,
        pytest.raises(
            ReplicationInvariantError,
            match="stored bytes",
        ),
    ):
        replicate_asset(
            s,
            asset_hash,
            source,
            "o-archive",
            backends={backend_id: backend},
            sealer=sealer,
        )


def test_replicate_asset_rerun_skips_existing_healthy_pools(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"idempotent fanout"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    for index, pool_id in enumerate(("private-copy-1", "private-copy-2")):
        _add_pool(
            engine,
            backend_id=backend_id,
            pool_id=pool_id,
            artifactclass="video-priv",
            representation=Representation.RAW_BYTES,
            sort_order=index,
        )
    backend = _PoolWriteBackend("rem")

    with session_scope(engine) as s:
        replicate_asset(
            s,
            asset_hash,
            source,
            "video-priv",
            backends={backend_id: backend},
            sealer=_FakeSealer(tmp_path),
        )
    with session_scope(engine) as s:
        copies = replicate_asset(
            s,
            asset_hash,
            source,
            "video-priv",
            backends={backend_id: backend},
            sealer=_FakeSealer(tmp_path),
        )

    assert backend.writes == ["private-copy-1", "private-copy-2"]
    assert len(copies) == 2


def test_replicate_asset_rejects_backend_hash_mismatch(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"wrong hash"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    _add_pool(
        engine,
        backend_id=backend_id,
        pool_id="private-copy-1",
        artifactclass="video-priv",
        representation=Representation.RAW_BYTES,
    )
    backend = _WrongHashBackend("rem")

    with session_scope(engine) as s, pytest.raises(ReplicationInvariantError, match="differs"):
        replicate_asset(
            s,
            asset_hash,
            source,
            "video-priv",
            backends={backend_id: backend},
            sealer=_FakeSealer(tmp_path),
        )


def test_replication_status_rejects_copy_representation_mismatch(
    engine: Engine,
) -> None:
    data = b"representation drift"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    _add_pool(
        engine,
        backend_id=backend_id,
        pool_id="o-copy-1-pool",
        artifactclass="o-archive",
        representation=Representation.RAO_PLAIN_V1,
    )

    with session_scope(engine) as s:
        copy, _ = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            pool_id="o-copy-1-pool",
            native_locator={"pool_id": "o-copy-1-pool", "tape_uuid": "1" * 32},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
            storage_metadata=_metadata(Representation.RAW_BYTES),
        )
        _qualify_fixture_copy(copy)

        with pytest.raises(PoolRepresentationError, match="requires"):
            replication_status(s, asset_hash, "o-archive", {backend_id: _PoolWriteBackend("rem")})


def test_repair_writes_only_missing_pools(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"repair me"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    for index, pool_id in enumerate(("private-copy-1", "private-copy-2")):
        _add_pool(
            engine,
            backend_id=backend_id,
            pool_id=pool_id,
            artifactclass="video-priv",
            representation=Representation.RAW_BYTES,
            sort_order=index,
        )
    backend = _PoolWriteBackend("rem", tape_by_pool={"private-copy-2": "2" * 32})

    with session_scope(engine) as s:
        existing, _ = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            pool_id="private-copy-1",
            native_locator={"pool_id": "private-copy-1", "tape_uuid": "1" * 32},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
            storage_metadata=_metadata(Representation.RAW_BYTES),
        )
        _qualify_fixture_copy(existing)
        repaired = repair(
            s,
            asset_hash,
            source,
            "video-priv",
            backends={backend_id: backend},
            sealer=_FakeSealer(tmp_path),
        )
        status = replication_status(s, asset_hash, "video-priv", {backend_id: backend})

    assert backend.writes == ["private-copy-2"]
    assert len(repaired) == 1
    assert repaired[0].pool_id == "private-copy-2"
    assert status["complete"] is True


def test_replication_status_reports_missing_and_complete(
    engine: Engine,
) -> None:
    data = b"status asset"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    for index, pool_id in enumerate(("private-copy-1", "private-copy-2")):
        _add_pool(
            engine,
            backend_id=backend_id,
            pool_id=pool_id,
            artifactclass="video-priv",
            representation=Representation.RAW_BYTES,
            sort_order=index,
        )
    backend = _PoolWriteBackend("rem")

    with session_scope(engine) as s:
        first, _ = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            pool_id="private-copy-1",
            native_locator={"pool_id": "private-copy-1", "tape_uuid": "1" * 32},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
            storage_metadata=_metadata(Representation.RAW_BYTES),
        )
        _qualify_fixture_copy(first)
        status = replication_status(s, asset_hash, "video-priv", {backend_id: backend})
        assert status["complete"] is False
        assert {p.pool_id for p in status["have"]} == {"private-copy-1"}
        assert {p.pool_id for p in status["missing"]} == {"private-copy-2"}

        second, _ = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            pool_id="private-copy-2",
            native_locator={"pool_id": "private-copy-2", "tape_uuid": "2" * 32},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
            storage_metadata=_metadata(Representation.RAW_BYTES),
        )
        _qualify_fixture_copy(second)
        status = replication_status(s, asset_hash, "video-priv", {backend_id: backend})
        assert status["complete"] is True
        assert status["missing"] == set()


def test_replication_status_counts_fresh_unverified_copy(
    engine: Engine,
) -> None:
    data = b"fresh copy"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    _add_pool(
        engine,
        backend_id=backend_id,
        pool_id="private-copy-1",
        artifactclass="video-priv",
        representation=Representation.RAW_BYTES,
    )

    with session_scope(engine) as s:
        copy, _ = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            pool_id="private-copy-1",
            native_locator={"pool_id": "private-copy-1", "tape_uuid": "1" * 32},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
            storage_metadata=_metadata(Representation.RAW_BYTES),
        )
        assert copy.last_checked_at is None
        assert copy.last_measured_digest is None
        submit(
            s,
            "verify",
            {"copy_id": copy.id},
            dedupe_key=f"verify:copy:{copy.id}",
        )

        status = replication_status(
            s,
            asset_hash,
            "video-priv",
            {backend_id: _PoolWriteBackend("rem")},
        )

    assert status["complete"] is True
    assert {target.pool_id for target in status["have"]} == {"private-copy-1"}


def test_replication_status_rejects_same_tape_for_multiple_pools(
    engine: Engine,
) -> None:
    data = b"same tape"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    for index, pool_id in enumerate(("private-copy-1", "private-copy-2")):
        _add_pool(
            engine,
            backend_id=backend_id,
            pool_id=pool_id,
            artifactclass="video-priv",
            representation=Representation.RAW_BYTES,
            sort_order=index,
        )

    with session_scope(engine) as s:
        for pool in ("private-copy-1", "private-copy-2"):
            copy, _ = add_copy(
                s,
                logical_asset_hash=asset_hash,
                backend_id=backend_id,
                pool_id=pool,
                native_locator={
                    "pool_id": pool,
                    "tape_uuid": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                },
                integrity_hash=asset_hash,
                source=CopySource.INGEST,
                storage_metadata=_metadata(Representation.RAW_BYTES),
            )
            _qualify_fixture_copy(copy)

        with pytest.raises(ReplicationInvariantError, match="distinct tape_uuid"):
            replication_status(
                s,
                asset_hash,
                "video-priv",
                {backend_id: _PoolWriteBackend("rem")},
            )


def test_replication_status_rejects_missing_tape_uuid(
    engine: Engine,
) -> None:
    data = b"missing tape"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    _add_pool(
        engine,
        backend_id=backend_id,
        pool_id="private-copy-1",
        artifactclass="video-priv",
        representation=Representation.RAW_BYTES,
    )

    with session_scope(engine) as s:
        copy, _ = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            pool_id="private-copy-1",
            native_locator={"pool_id": "private-copy-1"},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
            storage_metadata=_metadata(Representation.RAW_BYTES),
        )
        _qualify_fixture_copy(copy)

        with pytest.raises(ReplicationInvariantError, match="missing tape_uuid"):
            replication_status(
                s,
                asset_hash,
                "video-priv",
                {backend_id: _PoolWriteBackend("rem")},
            )


def test_select_restore_source_picks_first_healthy_copy(
    engine: Engine,
) -> None:
    data = b"restore"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)

    with session_scope(engine) as s:
        missing, _ = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator={"pool_id": "private-copy-missing", "tape_uuid": "0" * 32},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
            health=CopyHealth.MISSING,
        )
        first, _ = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator={"pool_id": "private-copy-1", "tape_uuid": "1" * 32},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
        )
        second, _ = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator={"pool_id": "private-copy-2", "tape_uuid": "2" * 32},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
        )

        selected = select_restore_source(s, asset_hash)
        selected_by_custom = select_restore_source(
            s,
            asset_hash,
            chooser=lambda copies: copies[-1],
        )

    assert missing.id < first.id < second.id
    assert selected is not None
    assert selected.id == first.id
    assert selected_by_custom is not None
    assert selected_by_custom.id == second.id


def test_select_source_self_heal_prefers_fresh_aead_over_stale_plain(
    engine: Engine,
) -> None:
    data = b"source order"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    with session_scope(engine) as s:
        s.add_all(
            [
                Pool(
                    id="plain-pool",
                    backend_id=backend_id,
                    representation=Representation.RAO_PLAIN_V1.value,
                    offsite_gate=False,
                ),
                Pool(
                    id="aead-pool",
                    backend_id=backend_id,
                    representation=Representation.RAO_AEAD_V1.value,
                    offsite_gate=False,
                ),
            ]
        )
        s.flush()
        stale_plain, _ = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            pool_id="plain-pool",
            native_locator={"pool_id": "plain-pool", "tape_uuid": "1" * 32},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
            storage_metadata=_metadata(Representation.RAO_PLAIN_V1),
        )
        _qualify_fixture_copy(
            stale_plain,
            measured_at=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
        )
        fresh_aead, _ = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            pool_id="aead-pool",
            native_locator={"pool_id": "aead-pool", "tape_uuid": "2" * 32},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
            storage_metadata=_metadata(Representation.RAO_AEAD_V1),
        )
        _qualify_fixture_copy(
            fresh_aead,
            measured_at=dt.datetime(2026, 1, 2, tzinfo=dt.UTC),
        )

        selected = select_source(
            s,
            AssetTarget(asset_hash, "o-archive"),
            purpose="self_heal",
        )

    assert stale_plain.id < fresh_aead.id
    assert selected is not None
    assert selected.id == fresh_aead.id
