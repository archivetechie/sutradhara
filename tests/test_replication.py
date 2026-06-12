"""Multi-placement replication tests.

These tests exercise sutradhara's first multi-pool fan-out layer: placements are
selected by universal content/copy tags, writes reuse the backend port, copies
are recorded through catalog.add_copy, and completeness is evaluated from
healthy copy rows without promoting tape-specific fields to catalog columns.
"""

from __future__ import annotations

import contextlib
import hashlib
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, select

from sutradhara.backend.port import (
    BackendLocator,
    ByteRange,
    CopyRecord,
    TaggedPlacement,
    VerifyResult,
)
from sutradhara.catalog.copies import add_copy
from sutradhara.catalog.models import Backend, Copy, LogicalAsset, PlacementTagPin
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import (
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
    content_hash,
)
from sutradhara.keys import KeyEpoch
from sutradhara.replication import (
    DuplicatePlacementClass,
    PlacementTagDrift,
    ReplicationInvariantError,
    repair,
    replicate_asset,
    replication_status,
    select_restore_source,
    target_placements,
)
from sutradhara.sealing.policy import n_archive_policy, o_archive_policy
from sutradhara.sealing.port import Representation, SealResult
from sutradhara.sealing.rao import RAO_CHUNK_SIZE


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


class _TaggedWriteBackend:
    def __init__(
        self,
        name: str,
        placements: list[TaggedPlacement],
        *,
        tape_by_placement: dict[str, str] | None = None,
    ) -> None:
        self._name = name
        self._placements = placements
        self._tape_by_placement = tape_by_placement or {}
        self.writes: list[str] = []

    @property
    def name(self) -> str:
        return self._name

    def list_tagged_placements(self) -> list[TaggedPlacement]:
        return list(self._placements)

    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        data = Path(source).read_bytes()
        digest = content_hash(hashlib.sha256(data).digest())
        self.writes.append(pool)
        tape_uuid = self._tape_by_placement.get(pool, f"{len(self.writes):032x}")
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


class _WrongHashBackend(_TaggedWriteBackend):
    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        record = super().write_object_to_pool(source, pool)
        wrong = content_hash(hashlib.sha256(b"different").digest())
        return CopyRecord(
            logical_id=wrong,
            native_locator=record.native_locator,
            integrity_hash=wrong,
            size_bytes=record.size_bytes,
        )


class _D2TapeFakeBackend(_TaggedWriteBackend):
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
            result = SealResult(
                sealed_path=source,
                stored_digest=plaintext,
                plaintext_digest=plaintext,
                representation=representation,
            )
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
        )
        self.results.append(result)
        try:
            yield result
        finally:
            with contextlib.suppress(FileNotFoundError):
                sealed_path.unlink()


def _placement(
    placement_id: str,
    content_class: str,
    copy_class: str,
    backend_name: str = "rem",
) -> TaggedPlacement:
    return TaggedPlacement(
        placement_id=placement_id,
        content_class=content_class,
        copy_class=copy_class,
        backend_name=backend_name,
    )


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


def _add_asset(engine: Engine, data: bytes) -> bytes:
    digest = hashlib.sha256(data).digest()
    with session_scope(engine) as s:
        s.add(LogicalAsset(content_sha256=digest, size_bytes=len(data)))
    return digest


def test_target_placements_filters_by_content_class_and_copy_class() -> None:
    backend = _TaggedWriteBackend(
        "rem",
        [
            _placement("private-copy-1", "video-priv", "copy-1"),
            _placement("private-copy-2", "video-priv", "copy-2"),
            _placement("public-copy-1", "video-pub", "copy-1"),
        ],
    )

    targets = target_placements("video-priv", {1: backend})

    assert {placement.placement_id for _, placement in targets} == {
        "private-copy-1",
        "private-copy-2",
    }
    assert {placement.copy_class for _, placement in targets} == {"copy-1", "copy-2"}


def test_target_placements_applies_o_archive_representation_policy() -> None:
    backend = _TaggedWriteBackend(
        "rem",
        [
            _placement("copy-1-pool", "o-archive", "o-copy-1"),
            _placement("copy-2-pool", "o-archive", "o-copy-2"),
        ],
    )

    targets = target_placements(
        "o-archive",
        {1: backend},
        policy=o_archive_policy(),
        key_epoch="1" * 32,
    )

    by_copy = {placement.copy_class: placement for _, placement in targets}
    assert by_copy["o-copy-1"].representation == Representation.RAO_PLAIN_V1.value
    assert by_copy["o-copy-1"].key_epoch is None
    assert by_copy["o-copy-2"].representation == Representation.RAO_AEAD_V1.value
    assert by_copy["o-copy-2"].key_epoch == "1" * 32


def test_target_placements_applies_n_archive_representation_policy() -> None:
    backend = _TaggedWriteBackend(
        "rem",
        [
            _placement("n-copy-1", "n-archive", "copy-1"),
            _placement("n-copy-2", "n-archive", "copy-2"),
            _placement("n-copy-3", "n-archive", "copy-3"),
        ],
    )

    targets = target_placements(
        "n-archive",
        {1: backend},
        policy=n_archive_policy(),
        key_epoch="1" * 32,
    )

    by_copy = {placement.copy_class: placement for _, placement in targets}
    assert by_copy["copy-1"].representation == Representation.RAO_PLAIN_V1.value
    assert by_copy["copy-1"].key_epoch is None
    assert by_copy["copy-2"].representation == Representation.RAO_AEAD_V1.value
    assert by_copy["copy-2"].key_epoch == "1" * 32
    assert by_copy["copy-3"].representation == Representation.D2TAR_RAW.value
    assert by_copy["copy-3"].key_epoch is None


def test_target_placements_rejects_duplicate_copy_class() -> None:
    backend = _TaggedWriteBackend(
        "rem",
        [
            _placement("private-copy-a", "video-priv", "copy-1"),
            _placement("private-copy-b", "video-priv", "copy-1"),
        ],
    )

    with pytest.raises(DuplicatePlacementClass, match="copy-1"):
        target_placements("video-priv", {1: backend})


def test_replicate_asset_fans_out_and_records_each_copy(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"multi-pool asset"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    backend = _TaggedWriteBackend(
        "rem",
        [
            _placement("private-copy-1", "video-priv", "copy-1"),
            _placement("private-copy-2", "video-priv", "copy-2"),
        ],
        tape_by_placement={
            "private-copy-1": "1" * 32,
            "private-copy-2": "2" * 32,
        },
    )

    with session_scope(engine) as s:
        copies = replicate_asset(
            s,
            asset_hash,
            source,
            "video-priv",
            backends={backend_id: backend},
        )

    assert backend.writes == ["private-copy-1", "private-copy-2"]
    assert len(copies) == 2
    with session_scope(engine) as s:
        rows = list(s.scalars(select(Copy).order_by(Copy.id)))
        assert {row.native_locator["pool_id"] for row in rows} == {
            "private-copy-1",
            "private-copy-2",
        }
        assert {row.native_locator["tape_uuid"] for row in rows} == {
            "1" * 32,
            "2" * 32,
        }
        assert {row.source for row in rows} == {CopySource.INGEST}
        assert {row.health for row in rows} == {CopyHealth.OK}
        pins = list(s.scalars(select(PlacementTagPin).order_by(PlacementTagPin.id)))
        assert [
            (pin.placement_id, pin.content_class, pin.copy_class)
            for pin in pins
        ] == [
            ("private-copy-1", "video-priv", "copy-1"),
            ("private-copy-2", "video-priv", "copy-2"),
        ]


def test_replicate_asset_records_stored_digest_for_o_archive_sealed_copies(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"scenario-o asset"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    backend = _TaggedWriteBackend(
        "rem",
        [
            _placement("o-copy-1-pool", "o-archive", "o-copy-1"),
            _placement("o-copy-2-pool", "o-archive", "o-copy-2"),
        ],
        tape_by_placement={
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
            policy=o_archive_policy(),
            key_epoch="1" * 32,
        )

    assert backend.writes == ["o-copy-1-pool", "o-copy-2-pool"]
    assert sealer.calls == [
        (Representation.RAO_PLAIN_V1, None),
        (Representation.RAO_AEAD_V1, "1" * 32),
    ]
    assert len(copies) == 2
    with session_scope(engine) as s:
        rows = {
            row.native_locator["pool_id"]: row
            for row in s.scalars(select(Copy).order_by(Copy.id))
        }
        assert rows["o-copy-1-pool"].logical_asset_hash == asset_hash
        assert rows["o-copy-2-pool"].logical_asset_hash == asset_hash
        assert rows["o-copy-1-pool"].integrity_hash == sealer.results[0].stored_digest
        assert rows["o-copy-2-pool"].integrity_hash == sealer.results[1].stored_digest
        assert rows["o-copy-1-pool"].storage_metadata == {
            "representation": Representation.RAO_PLAIN_V1.value,
            "chunk_size": RAO_CHUNK_SIZE,
        }
        assert rows["o-copy-2-pool"].storage_metadata == {
            "representation": Representation.RAO_AEAD_V1.value,
            "chunk_size": RAO_CHUNK_SIZE,
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
    rem_backend = _TaggedWriteBackend(
        "mem-rem",
        [
            _placement("n-copy-1", "n-archive", "copy-1", "mem-rem"),
            _placement("n-copy-2", "n-archive", "copy-2", "mem-rem"),
        ],
        tape_by_placement={
            "n-copy-1": "1" * 32,
            "n-copy-2": "2" * 32,
        },
    )
    d2_backend = _D2TapeFakeBackend(
        "d2-tape",
        [_placement("n-copy-3", "n-archive", "copy-3", "d2-tape")],
    )
    sealer = _FakeSealer(tmp_path)

    with session_scope(engine) as s:
        copies = replicate_asset(
            s,
            asset_hash,
            source,
            "n-archive",
            backends={rem_backend_id: rem_backend, d2_backend_id: d2_backend},
            sealer=sealer,
            policy=n_archive_policy(),
            key_epoch="1" * 32,
        )
        status = replication_status(
            s,
            asset_hash,
            "n-archive",
            {rem_backend_id: rem_backend, d2_backend_id: d2_backend},
            policy=n_archive_policy(),
            key_epoch="1" * 32,
        )

    assert rem_backend.writes == ["n-copy-1", "n-copy-2"]
    assert d2_backend.writes == ["n-copy-3"]
    assert len(copies) == 3
    assert status["complete"] is True
    assert {placement.placement_id for placement in status["have"]} == {
        "n-copy-1",
        "n-copy-2",
        "n-copy-3",
    }
    assert sealer.calls == [
        (Representation.RAO_PLAIN_V1, None),
        (Representation.RAO_AEAD_V1, "1" * 32),
        (Representation.D2TAR_RAW, None),
    ]

    with session_scope(engine) as s:
        rows = list(s.scalars(select(Copy).order_by(Copy.id)))
        assert {row.backend.name for row in rows} == {"mem-rem", "d2-tape"}
        rem_rows = [row for row in rows if row.backend.name == "mem-rem"]
        assert [row.storage_metadata["representation"] for row in rem_rows] == [
            Representation.RAO_PLAIN_V1.value,
            Representation.RAO_AEAD_V1.value,
        ]
        assert {row.storage_metadata["chunk_size"] for row in rem_rows} == {
            RAO_CHUNK_SIZE,
        }
        [d2_copy] = [row for row in rows if row.backend.name == "d2-tape"]
        assert set(d2_copy.native_locator) == {
            "barcode",
            "volume_uuid",
            "artifact_name",
            "start_block",
            "end_block",
            "volume_blocksize",
            "pool_id",
        }
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
    backend = _TaggedWriteBackend(
        "rem",
        [_placement("o-copy-1-pool", "o-archive", "o-copy-1")],
    )
    sealer = _FakeSealer(
        tmp_path,
        plaintext_digest_override=hashlib.sha256(b"different").digest(),
    )

    with session_scope(engine) as s, pytest.raises(
        ReplicationInvariantError,
        match="plaintext_digest",
    ):
        replicate_asset(
            s,
            asset_hash,
            source,
            "o-archive",
            backends={backend_id: backend},
            sealer=sealer,
            policy={("o-archive", "o-copy-1"): Representation.RAO_PLAIN_V1.value},
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
    backend = _TaggedWriteBackend(
        "rem",
        [_placement("o-copy-1-pool", "o-archive", "o-copy-1")],
    )
    sealer = _FakeSealer(
        tmp_path,
        stored_digest_override=hashlib.sha256(b"different").digest(),
    )

    with session_scope(engine) as s, pytest.raises(
        ReplicationInvariantError,
        match="stored bytes",
    ):
        replicate_asset(
            s,
            asset_hash,
            source,
            "o-archive",
            backends={backend_id: backend},
            sealer=sealer,
            policy={("o-archive", "o-copy-1"): Representation.RAO_PLAIN_V1.value},
        )


def test_replicate_asset_rerun_skips_existing_healthy_placements(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"idempotent fanout"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    backend = _TaggedWriteBackend(
        "rem",
        [
            _placement("private-copy-1", "video-priv", "copy-1"),
            _placement("private-copy-2", "video-priv", "copy-2"),
        ],
    )

    with session_scope(engine) as s:
        replicate_asset(s, asset_hash, source, "video-priv", backends={backend_id: backend})
    with session_scope(engine) as s:
        copies = replicate_asset(
            s,
            asset_hash,
            source,
            "video-priv",
            backends={backend_id: backend},
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
    backend = _WrongHashBackend(
        "rem",
        [_placement("private-copy-1", "video-priv", "copy-1")],
    )

    with session_scope(engine) as s, pytest.raises(
        ReplicationInvariantError, match="differs"
    ):
        replicate_asset(
            s,
            asset_hash,
            source,
            "video-priv",
            backends={backend_id: backend},
        )


def test_tag_drift_raises_reconciliation_halt(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"drift"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)

    with session_scope(engine) as s:
        replicate_asset(
            s,
            asset_hash,
            source,
            "video-priv",
            backends={
                backend_id: _TaggedWriteBackend(
                    "rem",
                    [_placement("private-copy-1", "video-priv", "copy-1")],
                )
            },
        )

    drifted = _TaggedWriteBackend(
        "rem",
        [_placement("private-copy-1", "video-pub", "copy-1")],
    )
    with session_scope(engine) as s, pytest.raises(PlacementTagDrift, match="halt"):
        replication_status(s, asset_hash, "video-priv", {backend_id: drifted})


def test_repair_writes_only_missing_placements(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"repair me"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    backend = _TaggedWriteBackend(
        "rem",
        [
            _placement("private-copy-1", "video-priv", "copy-1"),
            _placement("private-copy-2", "video-priv", "copy-2"),
        ],
        tape_by_placement={
            "private-copy-2": "2" * 32,
        },
    )

    with session_scope(engine) as s:
        add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator={"pool_id": "private-copy-1", "tape_uuid": "1" * 32},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
        )
        repaired = repair(
            s,
            asset_hash,
            source,
            "video-priv",
            backends={backend_id: backend},
        )
        status = replication_status(s, asset_hash, "video-priv", {backend_id: backend})

    assert backend.writes == ["private-copy-2"]
    assert len(repaired) == 1
    assert repaired[0].native_locator["pool_id"] == "private-copy-2"
    assert status["complete"] is True


def test_replication_status_reports_missing_and_complete(
    engine: Engine,
) -> None:
    data = b"status asset"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    backend = _TaggedWriteBackend(
        "rem",
        [
            _placement("private-copy-1", "video-priv", "copy-1"),
            _placement("private-copy-2", "video-priv", "copy-2"),
        ],
    )

    with session_scope(engine) as s:
        add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator={"pool_id": "private-copy-1", "tape_uuid": "1" * 32},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
        )
        status = replication_status(s, asset_hash, "video-priv", {backend_id: backend})
        assert status["complete"] is False
        assert {p.placement_id for p in status["have"]} == {"private-copy-1"}
        assert {p.placement_id for p in status["missing"]} == {"private-copy-2"}

        add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator={"pool_id": "private-copy-2", "tape_uuid": "2" * 32},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
        )
        status = replication_status(s, asset_hash, "video-priv", {backend_id: backend})
        assert status["complete"] is True
        assert status["missing"] == set()


def test_replication_status_rejects_same_tape_for_multiple_placements(
    engine: Engine,
) -> None:
    data = b"same tape"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    backend = _TaggedWriteBackend(
        "rem",
        [
            _placement("private-copy-1", "video-priv", "copy-1"),
            _placement("private-copy-2", "video-priv", "copy-2"),
        ],
    )

    with session_scope(engine) as s:
        for pool in ("private-copy-1", "private-copy-2"):
            add_copy(
                s,
                logical_asset_hash=asset_hash,
                backend_id=backend_id,
                native_locator={
                    "pool_id": pool,
                    "tape_uuid": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                },
                integrity_hash=asset_hash,
                source=CopySource.INGEST,
            )

        with pytest.raises(ReplicationInvariantError, match="distinct tape_uuid"):
            replication_status(s, asset_hash, "video-priv", {backend_id: backend})


def test_replication_status_rejects_missing_tape_uuid(
    engine: Engine,
) -> None:
    data = b"missing tape"
    asset_hash = _add_asset(engine, data)
    backend_id = _add_backend(engine)
    backend = _TaggedWriteBackend(
        "rem",
        [_placement("private-copy-1", "video-priv", "copy-1")],
    )

    with session_scope(engine) as s:
        add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator={"pool_id": "private-copy-1"},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
        )

        with pytest.raises(ReplicationInvariantError, match="missing tape_uuid"):
            replication_status(s, asset_hash, "video-priv", {backend_id: backend})


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
