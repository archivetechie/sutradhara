"""Archive bundle fan-out and artifactclass restore tests."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, select

from sutradhara.archive_bundle import bundle_due, enqueue_artifact
from sutradhara.archive_fanout import (
    BundleHeld,
    BundleOversize,
    ConformanceScan,
    DeviationCluster,
    LocalArchiveBuilder,
    flush_bundle,
)
from sutradhara.archive_restore import (
    RestoreIntegrityError,
    RestoreSourceUnavailable,
    restore_asset,
)
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    BundlingPolicy,
    PlacementPolicy,
    apply_artifactclass_policy,
    get_artifactclass_policy,
)
from sutradhara.backend.port import BackendLocator, ByteRange, CopyRecord, VerifyResult
from sutradhara.catalog.models import AssetLocator, Backend, Bundle, Copy, LogicalAsset, Pool
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, content_hash
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


@dataclass(frozen=True)
class _ArchiveSetup:
    bundle_id: str
    rem_backend_id: int
    d2_backend_id: int
    assets: dict[bytes, bytes]


class _ArchiveWriteBackend:
    def __init__(self, name: str) -> None:
        self._name = name
        self._objects: dict[str, bytes] = {}
        self._counter = 0
        self.writes: list[str] = []

    @property
    def name(self) -> str:
        return self._name

    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        data = Path(source).read_bytes()
        digest = content_hash(hashlib.sha256(data).digest())
        self._counter += 1
        object_id = f"{self._name}-{self._counter}"
        self._objects[object_id] = data
        self.writes.append(pool)
        return CopyRecord(
            logical_id=digest,
            native_locator={
                "pool_id": pool,
                "object_id": object_id,
                "content_sha256": digest.hex(),
                "tape_uuid": f"{self._counter:032x}",
                "tape_file_number": self._counter,
            },
            integrity_hash=digest,
            size_bytes=len(data),
        )

    def enumerate(self) -> Iterator[CopyRecord]:
        return iter(())

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        data = self._objects[str(locator["object_id"])]
        if byte_range.is_whole_object:
            return data
        return data[byte_range.start : byte_range.end]

    def verify(self, locator: BackendLocator) -> VerifyResult:
        data = self.read_range(locator, ByteRange(0, 0))
        actual = content_hash(hashlib.sha256(data).digest())
        expected = content_hash(bytes.fromhex(str(locator["content_sha256"])))
        return VerifyResult(ok=actual == expected, actual_hash=actual)

    def corrupt(self, locator: BackendLocator) -> None:
        object_id = str(locator["object_id"])
        self._objects[object_id] = b"corrupt" + self._objects[object_id]


class _DeviationBuilder(LocalArchiveBuilder):
    def scan(self, **kwargs: Any) -> ConformanceScan:
        return ConformanceScan(
            clusters=(
                DeviationCluster(
                    prefix="tmp/",
                    reason="unsupported-entry",
                    count=2,
                    samples=("tmp/socket",),
                    proposed_default="exclude",
                ),
            )
        )


def _digest(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _install_policy(
    engine: Engine,
    *,
    expect: str = "messy",
    target_gb: float = 0.000001,
) -> tuple[int, int]:
    with session_scope(engine) as s:
        rem = Backend(
            name="rem",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        d2 = Backend(
            name="d2",
            kind=BackendKind.D2_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add_all([rem, d2])
        s.flush()
        s.add_all(
            [
                Pool(
                    id="o-copy-1-pool",
                    backend_id=rem.id,
                    representation=Representation.RAO_PLAIN_V1.value,
                ),
                Pool(
                    id="d2-shelf-pool",
                    backend_id=d2.id,
                    representation=Representation.D2TAR_RAW.value,
                ),
            ]
        )
        s.flush()
        apply_artifactclass_policy(
            s,
            "o-archive",
            ArtifactClassPolicy(
                ruleset="rao.o.v1",
                placements=(
                    PlacementPolicy("o-copy-1-pool", role="primary"),
                    PlacementPolicy("d2-shelf-pool", role="shelf"),
                ),
                bundling=BundlingPolicy(
                    target_gb=target_gb,
                    max_age_seconds=60,
                ),
                restore_preference=("o-copy-1-pool", "d2-shelf-pool"),
                expect=expect,
            ),
        )
        return rem.id, d2.id


def _create_bundle(
    engine: Engine,
    tmp_path: Path,
    *,
    expect: str = "messy",
    target_gb: float = 0.000001,
) -> _ArchiveSetup:
    rem_id, d2_id = _install_policy(engine, expect=expect, target_gb=target_gb)
    files = {
        "a.bin": b"alpha body",
        "nested/b.bin": b"beta body",
    }
    paths: dict[str, Path] = {}
    for member_path, data in files.items():
        path = tmp_path / member_path.replace("/", "_")
        path.write_bytes(data)
        paths[member_path] = path

    with session_scope(engine) as s:
        for data in files.values():
            s.add(LogicalAsset(content_sha256=_digest(data), size_bytes=len(data)))
        s.flush()
        policy = get_artifactclass_policy(s, "o-archive")
        bundle_id = ""
        assets: dict[bytes, bytes] = {}
        for member_path, data in files.items():
            asset_hash = _digest(data)
            bundle, _, _ = enqueue_artifact(
                s,
                artifactclass="o-archive",
                policy=policy,
                logical_asset_hash=asset_hash,
                source_path=paths[member_path],
                member_path=member_path,
            )
            bundle_id = bundle.id
            assets[asset_hash] = data
        return _ArchiveSetup(bundle_id, rem_id, d2_id, assets)


def test_enqueue_due_and_flush_fans_out_bundle_copies(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_bundle(engine, tmp_path, target_gb=0.000000001)
    rem_backend = _ArchiveWriteBackend("rem")
    d2_backend = _ArchiveWriteBackend("d2")

    with session_scope(engine) as s:
        bundle = s.get(Bundle, setup.bundle_id)
        assert bundle is not None
        assert bundle_due(bundle)
        result = flush_bundle(
            s,
            bundle_id=setup.bundle_id,
            backends={
                setup.rem_backend_id: rem_backend,
                setup.d2_backend_id: d2_backend,
            },
            builder=LocalArchiveBuilder(),
            deliverables_dir=tmp_path / "manifests",
        )

        sealed = s.get(Bundle, setup.bundle_id)
        assert sealed is not None
        assert sealed.status == "sealed"
        assert len(result.copy_ids) == 2
        assert result.manifest_path is not None
        assert Path(result.manifest_path).exists()
        assert sealed.customer_manifest_path == result.manifest_path

        copies = list(s.scalars(select(Copy).order_by(Copy.pool_id)))
        assert {copy.pool_id for copy in copies} == {
            "d2-shelf-pool",
            "o-copy-1-pool",
        }
        assert all(copy.logical_asset_hash is None for copy in copies)
        assert len(list(s.scalars(select(AssetLocator)))) == 4
        assert rem_backend.writes == ["o-copy-1-pool"]
        assert d2_backend.writes == ["d2-shelf-pool"]


def test_compliant_expectation_holds_bundle_for_review(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_bundle(engine, tmp_path, expect="compliant")

    with session_scope(engine) as s, pytest.raises(BundleHeld):
        flush_bundle(
            s,
            bundle_id=setup.bundle_id,
            backends={
                setup.rem_backend_id: _ArchiveWriteBackend("rem"),
                setup.d2_backend_id: _ArchiveWriteBackend("d2"),
            },
            builder=_DeviationBuilder(),
        )

    with session_scope(engine) as s:
        bundle = s.get(Bundle, setup.bundle_id)
        assert bundle is not None
        assert bundle.status == "held"
        assert bundle.review_summary == {
            "clusters": [
                {
                    "bytes_total": 0,
                    "count": 2,
                    "prefix": "tmp/",
                    "proposed_default": "exclude",
                    "reason": "unsupported-entry",
                    "samples": ["tmp/socket"],
                }
            ],
            "exclusions": [],
        }
        assert list(s.scalars(select(Copy))) == []


def test_oversize_member_surfaces_before_writes(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_bundle(engine, tmp_path)
    backend = _ArchiveWriteBackend("rem")

    with session_scope(engine) as s, pytest.raises(BundleOversize, match="oversize"):
        flush_bundle(
            s,
            bundle_id=setup.bundle_id,
            backends={
                setup.rem_backend_id: backend,
                setup.d2_backend_id: _ArchiveWriteBackend("d2"),
            },
            builder=LocalArchiveBuilder(),
            tape_capacity_bytes=1,
        )
    assert backend.writes == []


def test_restore_uses_policy_preference_and_falls_back_to_d2(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_bundle(engine, tmp_path)
    rem_backend = _ArchiveWriteBackend("rem")
    d2_backend = _ArchiveWriteBackend("d2")
    [asset_hash] = list(setup.assets)[:1]

    with session_scope(engine) as s:
        flush_bundle(
            s,
            bundle_id=setup.bundle_id,
            backends={
                setup.rem_backend_id: rem_backend,
                setup.d2_backend_id: d2_backend,
            },
            builder=LocalArchiveBuilder(),
        )
        first = restore_asset(
            s,
            asset_hash=asset_hash,
            artifactclass="o-archive",
            destination=tmp_path / "restore-a.bin",
            backends={
                setup.rem_backend_id: rem_backend,
                setup.d2_backend_id: d2_backend,
            },
        )
        assert first.pool_id == "o-copy-1-pool"
        assert first.output_path.read_bytes() == setup.assets[asset_hash]

        rem_copy = s.scalars(select(Copy).where(Copy.pool_id == "o-copy-1-pool")).one()
        rem_copy.health = CopyHealth.MISSING
        fallback = restore_asset(
            s,
            asset_hash=asset_hash,
            artifactclass="o-archive",
            destination=tmp_path / "restore-fallback.bin",
            backends={
                setup.rem_backend_id: rem_backend,
                setup.d2_backend_id: d2_backend,
            },
        )
        assert fallback.pool_id == "d2-shelf-pool"
        assert fallback.output_path.read_bytes() == setup.assets[asset_hash]


def test_restore_surfaces_unavailable_and_integrity_failures(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_bundle(engine, tmp_path)
    rem_backend = _ArchiveWriteBackend("rem")
    d2_backend = _ArchiveWriteBackend("d2")
    [asset_hash] = list(setup.assets)[:1]

    with session_scope(engine) as s:
        flush_bundle(
            s,
            bundle_id=setup.bundle_id,
            backends={
                setup.rem_backend_id: rem_backend,
                setup.d2_backend_id: d2_backend,
            },
            builder=LocalArchiveBuilder(),
        )
        with pytest.raises(RestoreSourceUnavailable):
            restore_asset(
                s,
                asset_hash=asset_hash,
                artifactclass="o-archive",
                destination=tmp_path / "unavailable.bin",
                backends={},
            )

        rem_copy = s.scalars(select(Copy).where(Copy.pool_id == "o-copy-1-pool")).one()
        rem_backend.corrupt(rem_copy.native_locator)
        with pytest.raises(RestoreIntegrityError):
            restore_asset(
                s,
                asset_hash=asset_hash,
                artifactclass="o-archive",
                destination=tmp_path / "corrupt.bin",
                backends={
                    setup.rem_backend_id: rem_backend,
                    setup.d2_backend_id: d2_backend,
                },
            )
