"""Archive bundle fan-out and artifactclass restore tests."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, select

from sutradhara.archive_bundle import bundle_due, enqueue_artifact
from sutradhara.archive_fanout import (
    ArchiveFanoutError,
    BuiltBlobRoot,
    BuiltExclusion,
    BuildArtifact,
    BundleHeld,
    BundleOversize,
    ConformanceScan,
    DeviationCluster,
    HmacManifestSigner,
    LocalArchiveBuilder,
    ManifestSigningError,
    MemberInput,
    build_bundle_copy_for_pool,
    flush_bundle,
)
from sutradhara.archive_restore import (
    RemArchiveExtractor,
    RestoreIntegrityError,
    RestoreNameError,
    RestoreSourceUnavailable,
    RestoreSuspectAsset,
    read_member_bytes,
    read_member_to_path,
    resolve_member_asset_hash,
    restore_asset,
)
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    BundlingPolicy,
    CompressionStagingPolicy,
    DurabilityPolicy,
    PlacementPolicy,
    StagingPolicy,
    apply_artifactclass_policy,
    get_artifactclass_policy,
)
from sutradhara.backend.port import BackendLocator, ByteRange, CopyRecord, VerifyResult
from sutradhara.catalog.copies import add_bundle_copy
from sutradhara.catalog.models import (
    AssetLocator,
    Backend,
    BlobRoot,
    Bundle,
    Copy,
    ExclusionRecord,
    LogicalAsset,
    Pool,
    StagingTransform,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import (
    AssetValidity,
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
    content_hash,
)
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import RAO_CHUNK_SIZE
from sutradhara.staging import stage_and_enqueue_artifact
from sutradhara.replication import target_pools


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
        self.reads: list[ByteRange] = []

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
        self.reads.append(byte_range)
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


class _BadLocatorBuilder(LocalArchiveBuilder):
    def build(self, **kwargs: Any) -> BuildArtifact:
        artifact = super().build(**kwargs)
        [first, *rest] = artifact.members
        bad_first = replace(
            first,
            native_locator={
                **first.native_locator,
                "offset": int(first.native_locator["offset"]) + 1,
            },
        )
        return replace(artifact, members=(bad_first, *rest))


class _OutputsBuilder(LocalArchiveBuilder):
    def build(self, **kwargs: Any) -> BuildArtifact:
        artifact = super().build(**kwargs)
        return replace(
            artifact,
            blob_roots=(
                BuiltBlobRoot(
                    root_path="blob-root",
                    native_locator={"member_path": "blob-root", "offset": 0},
                ),
            ),
            exclusions=(BuiltExclusion(path="ignored.tmp", reason="test-exclusion"),),
        )


class _FakeKeyRegistry:
    def __init__(self, key_path: Path) -> None:
        self.key_path = key_path
        self.seen: list[str] = []

    @contextmanager
    def materialized_root_key(self, key_id: str) -> Iterator[Path]:
        self.seen.append(key_id)
        yield self.key_path


def _digest(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _install_policy(
    engine: Engine,
    *,
    expect: str = "messy",
    target_gb: float = 0.000001,
    staging: StagingPolicy = StagingPolicy(),
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
                staging=staging,
                durability=DurabilityPolicy(min_copies=2, min_impl_families=2),
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


def _create_rao_plain_copy(
    engine: Engine,
    tmp_path: Path,
    backend: _ArchiveWriteBackend,
    members: list[tuple[str, bytes, int | None]],
) -> tuple[int, dict[bytes, bytes]]:
    rem_id, _ = _install_policy(engine)
    object_size = 0
    for _, data, first_lba in members:
        if first_lba is None:
            continue
        object_size = max(object_size, first_lba * RAO_CHUNK_SIZE + len(data))
    payload = bytearray(b"\0" * object_size)
    for _, data, first_lba in members:
        if first_lba is None:
            continue
        start = first_lba * RAO_CHUNK_SIZE
        payload[start : start + len(data)] = data

    object_path = tmp_path / "manual-plain.rao"
    object_path.write_bytes(payload)
    assets = {_digest(data): data for _, data, _ in members}

    with session_scope(engine) as s:
        s.add(Bundle(id="bundle-plain", artifactclass="o-archive", status="sealed"))
        for asset_hash, data in assets.items():
            s.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(data)))
        s.flush()
        record = backend.write_object_to_pool(object_path, "o-copy-1-pool")
        copy, _ = add_bundle_copy(
            s,
            bundle_id="bundle-plain",
            backend_id=rem_id,
            pool_id="o-copy-1-pool",
            native_locator=record.native_locator,
            integrity_hash=record.integrity_hash,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
            storage_metadata={
                "representation": Representation.RAO_PLAIN_V1.value,
                "chunk_size": RAO_CHUNK_SIZE,
                "stored_size_bytes": record.size_bytes,
            },
        )
        for member_path, data, first_lba in members:
            native_locator: dict[str, object] = {
                "member_path": member_path,
                "size_bytes": len(data),
            }
            if first_lba is not None:
                native_locator["first_chunk_lba"] = first_lba
            s.add(
                AssetLocator(
                    logical_asset_hash=_digest(data),
                    pool_id="o-copy-1-pool",
                    copy_id=copy.id,
                    bundle_id="bundle-plain",
                    member_path=member_path,
                    native_locator=native_locator,
                    representation=Representation.RAO_PLAIN_V1.value,
                )
            )
    return rem_id, assets


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
            manifest_signer=HmacManifestSigner(b"receipt-secret", "test-key"),
        )

        sealed = s.get(Bundle, setup.bundle_id)
        assert sealed is not None
        assert sealed.status == "sealed"
        assert len(result.copy_ids) == 2
        assert result.manifest_path is not None
        assert Path(result.manifest_path).exists()
        assert sealed.customer_manifest_path == result.manifest_path
        receipt = json.loads(Path(result.manifest_path).read_text())
        assert receipt["signature"]["algorithm"] == "hmac-sha256"
        assert receipt["signature"]["key_id"] == "test-key"
        assert receipt["manifest"]["representation"] == Representation.RAO_PLAIN_V1.value

        copies = list(s.scalars(select(Copy).order_by(Copy.pool_id)))
        assert {copy.pool_id for copy in copies} == {
            "d2-shelf-pool",
            "o-copy-1-pool",
        }
        assert all(copy.logical_asset_hash is None for copy in copies)
        assert len(list(s.scalars(select(AssetLocator)))) == 4
        assert rem_backend.writes == ["o-copy-1-pool"]
        assert d2_backend.writes == ["d2-shelf-pool"]


def test_build_bundle_copy_for_pool_records_copy_locators_blob_roots_but_no_exclusions(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_bundle(engine, tmp_path)
    rem_backend = _ArchiveWriteBackend("rem")

    with session_scope(engine) as s:
        bundle = s.get(Bundle, setup.bundle_id)
        assert bundle is not None
        targets = target_pools(
            s,
            "o-archive",
            {
                setup.rem_backend_id: rem_backend,
                setup.d2_backend_id: _ArchiveWriteBackend("d2"),
            },
        )
        backend, target = next(item for item in targets if item[1].pool_id == "o-copy-1-pool")
        members = [
            MemberInput(
                logical_asset_hash=member.logical_asset_hash,
                member_path=member.member_path,
                source_path=Path(str(member.source_path)),
                size_bytes=member.size_bytes,
                file_sha256=member.file_sha256,
            )
            for member in bundle.members
        ]
        work_dir = tmp_path / "primitive-work"
        work_dir.mkdir()

        copy = build_bundle_copy_for_pool(
            s,
            bundle=bundle,
            target=target,
            member_sources=members,
            builder=_OutputsBuilder(),
            backend=backend,
            key_epoch=None,
            work_dir=work_dir,
        )

        assert copy.bundle_id == bundle.id
        assert copy.pool_id == "o-copy-1-pool"
        assert copy.last_verified_at is not None
        assert bundle.status == "open"
        assert len(list(s.scalars(select(AssetLocator).where(AssetLocator.copy_id == copy.id)))) == 2
        assert len(list(s.scalars(select(BlobRoot).where(BlobRoot.copy_id == copy.id)))) == 1
        assert list(s.scalars(select(ExclusionRecord))) == []


def test_local_archive_builder_aead_offsets_verify_without_builder_fallback(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = _ArchiveWriteBackend("aead")
    data = b"encrypted-test-builder-payload"
    source = tmp_path / "asset.bin"
    source.write_bytes(data)

    with session_scope(engine) as s:
        row = Backend(
            name="aead",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(row)
        s.flush()
        s.add(
            Pool(
                id="aead-pool",
                backend_id=row.id,
                representation=Representation.RAO_AEAD_V1.value,
            )
        )
        s.add(LogicalAsset(content_sha256=_digest(data), size_bytes=len(data)))
        s.flush()
        apply_artifactclass_policy(
            s,
            "aead-archive",
            ArtifactClassPolicy(
                ruleset="rao.aead.test",
                placements=(PlacementPolicy("aead-pool", role="offsite"),),
                bundling=BundlingPolicy(target_gb=0.000000001, max_age_seconds=60),
                restore_preference=("aead-pool",),
                expect="messy",
                durability=DurabilityPolicy(min_copies=1, min_impl_families=1),
            ),
        )
        policy = get_artifactclass_policy(s, "aead-archive")
        bundle, _, _ = enqueue_artifact(
            s,
            artifactclass="aead-archive",
            policy=policy,
            logical_asset_hash=_digest(data),
            source_path=source,
            member_path="asset.bin",
        )
        flush_bundle(
            s,
            bundle_id=bundle.id,
            backends={row.id: backend},
            builder=LocalArchiveBuilder(),
            key_epoch="1" * 32,
        )

    assert backend.writes == ["aead-pool"]


def test_manifest_receipt_requires_keyed_signer(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_bundle(engine, tmp_path)

    with session_scope(engine) as s, pytest.raises(ManifestSigningError):
        flush_bundle(
            s,
            bundle_id=setup.bundle_id,
            backends={
                setup.rem_backend_id: _ArchiveWriteBackend("rem"),
                setup.d2_backend_id: _ArchiveWriteBackend("d2"),
            },
            builder=LocalArchiveBuilder(),
            deliverables_dir=tmp_path / "manifests",
        )


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


def test_member_locator_verification_runs_before_bundle_seals(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_bundle(engine, tmp_path)

    with pytest.raises(ArchiveFanoutError, match="member"), session_scope(engine) as s:
        flush_bundle(
            s,
            bundle_id=setup.bundle_id,
            backends={
                setup.rem_backend_id: _ArchiveWriteBackend("rem"),
                setup.d2_backend_id: _ArchiveWriteBackend("d2"),
            },
            builder=_BadLocatorBuilder(),
        )

    with session_scope(engine) as s:
        bundle = s.get(Bundle, setup.bundle_id)
        assert bundle is not None
        assert bundle.status == "open"
        assert list(s.scalars(select(Copy))) == []


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
        get_artifactclass_policy(s, "o-archive").restore_preference = ["o-copy-1-pool"]
        d2_backend.reads.clear()
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
        assert d2_backend.reads
        assert all(not byte_range.is_whole_object for byte_range in d2_backend.reads)


def test_rao_plain_restore_reads_only_member_range(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = _ArchiveWriteBackend("rem")
    target = b"beta body" * 7
    first_lba = 2
    rem_id, assets = _create_rao_plain_copy(
        engine,
        tmp_path,
        backend,
        [
            ("a.bin", b"alpha body", 1),
            ("nested/b.bin", target, first_lba),
        ],
    )
    asset_hash = _digest(target)

    with session_scope(engine) as s:
        backend.reads.clear()
        restored = restore_asset(
            s,
            asset_hash=asset_hash,
            artifactclass="o-archive",
            destination=tmp_path / "restore-beta.bin",
            backends={rem_id: backend},
        )

    start = first_lba * RAO_CHUNK_SIZE
    assert restored.output_path.read_bytes() == assets[asset_hash]
    assert backend.reads == [ByteRange(start, start + len(target))]


def test_zero_byte_rao_plain_member_restores_without_backend_read(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = _ArchiveWriteBackend("rem")
    rem_id, _ = _create_rao_plain_copy(
        engine,
        tmp_path,
        backend,
        [("empty.bin", b"", None)],
    )

    with session_scope(engine) as s:
        backend.reads.clear()
        restored = restore_asset(
            s,
            asset_hash=hashlib.sha256(b"").digest(),
            artifactclass="o-archive",
            destination=tmp_path / "empty.bin",
            backends={rem_id: backend},
        )

    assert restored.output_path.read_bytes() == b""
    assert backend.reads == []


def test_restore_resolves_relative_nested_destination_and_keeps_existing_on_failure(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _ArchiveWriteBackend("rem")
    payload = b"durable restore payload"
    rem_id, _ = _create_rao_plain_copy(
        engine,
        tmp_path,
        backend,
        [("payload.bin", payload, 1)],
    )
    asset_hash = _digest(payload)
    monkeypatch.chdir(tmp_path)

    with session_scope(engine) as s:
        restored = restore_asset(
            s,
            asset_hash=asset_hash,
            artifactclass="o-archive",
            destination=Path("new/rel/dir/payload.bin"),
            backends={rem_id: backend},
        )

    expected = (tmp_path / "new/rel/dir/payload.bin").resolve()
    assert restored.output_path == expected
    assert expected.read_bytes() == payload

    existing = tmp_path / "new/rel/dir/existing.bin"
    existing.write_bytes(b"old bytes")
    with session_scope(engine) as s:
        copy = s.scalars(select(Copy).where(Copy.pool_id == "o-copy-1-pool")).one()
        backend.corrupt(copy.native_locator)
        with pytest.raises(RestoreIntegrityError):
            restore_asset(
                s,
                asset_hash=asset_hash,
                artifactclass="o-archive",
                destination=Path("new/rel/dir/existing.bin"),
                backends={rem_id: backend},
            )

    assert existing.read_bytes() == b"old bytes"
    assert list(existing.parent.glob(f".{existing.name}.*.tmp")) == []


def test_read_member_primitive_matches_bytes_wrapper_and_streams_large_member(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = _ArchiveWriteBackend("rem")
    payload = (b"large member\n" * ((RAO_CHUNK_SIZE // len(b"large member\n")) + 2)) + b"tail"
    first_lba = 1
    _rem_id, _ = _create_rao_plain_copy(
        engine,
        tmp_path,
        backend,
        [("large.bin", payload, first_lba)],
    )

    with session_scope(engine) as s:
        locator = s.scalars(
            select(AssetLocator).where(AssetLocator.logical_asset_hash == _digest(payload))
        ).one()
        copy = locator.copy
        assert copy is not None
        output_path = tmp_path / "primitive-large.bin"
        backend.reads.clear()
        written = read_member_to_path(backend, copy, locator, output_path)
        path_ranges = list(backend.reads)
        backend.reads.clear()
        wrapper_bytes = read_member_bytes(backend, copy, locator, work_dir=tmp_path)
        wrapper_ranges = list(backend.reads)

    start = first_lba * RAO_CHUNK_SIZE
    end = start + len(payload)
    assert written == len(payload)
    assert output_path.read_bytes() == payload
    assert wrapper_bytes == payload
    assert len(path_ranges) > 1
    assert path_ranges == wrapper_ranges
    assert all(not byte_range.is_whole_object for byte_range in path_ranges)
    assert path_ranges[0].start == start
    assert path_ranges[-1].end == end
    assert sum(byte_range.length for byte_range in path_ranges) == len(payload)


def test_restore_refuses_suspect_asset_without_force(
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
        asset = s.get(LogicalAsset, asset_hash)
        assert asset is not None
        asset.validity = AssetValidity.SUSPECT
        asset.validity_note = "decode error via validate"
        with pytest.raises(RestoreSuspectAsset, match="decode error"):
            restore_asset(
                s,
                asset_hash=asset_hash,
                artifactclass="o-archive",
                destination=tmp_path / "blocked.bin",
                backends={
                    setup.rem_backend_id: rem_backend,
                    setup.d2_backend_id: d2_backend,
                },
            )
        forced = restore_asset(
            s,
            asset_hash=asset_hash,
            artifactclass="o-archive",
            destination=tmp_path / "forced.bin",
            backends={
                setup.rem_backend_id: rem_backend,
                setup.d2_backend_id: d2_backend,
            },
            force_suspect=True,
        )
        assert forced.output_path.read_bytes() == setup.assets[asset_hash]


def test_zstd_staged_member_fans_out_manifests_and_restores_original(
    engine: Engine,
    tmp_path: Path,
) -> None:
    original = (b"virtual disk block\n" * 256) + b"tail"
    source = tmp_path / "disk.img"
    source.write_bytes(original)
    rem_id, d2_id = _install_policy(
        engine,
        staging=StagingPolicy(
            compression=CompressionStagingPolicy(
                codec="zstd",
                level=3,
                globs=("**/*.img",),
            )
        ),
    )
    rem_backend = _ArchiveWriteBackend("rem")
    d2_backend = _ArchiveWriteBackend("d2")

    with session_scope(engine) as s:
        policy = get_artifactclass_policy(s, "o-archive")
        staged = stage_and_enqueue_artifact(
            s,
            artifactclass="o-archive",
            policy=policy,
            source_path=source,
            staging_root=tmp_path / "stage",
            member_path="images/disk.img",
            bundle_id="bundle-zstd",
        )

        assert staged.logical_sha256 == _digest(original)
        assert staged.stored_member_path == "images/disk.img.zst"
        assert staged.staged_path.read_bytes() != original

        result = flush_bundle(
            s,
            bundle_id="bundle-zstd",
            backends={rem_id: rem_backend, d2_id: d2_backend},
            builder=LocalArchiveBuilder(),
            deliverables_dir=tmp_path / "manifests",
            manifest_signer=HmacManifestSigner(b"receipt-secret", "test-key"),
        )

        transforms = list(s.scalars(select(StagingTransform)))
        assert [transform.kind for transform in transforms] == ["zstd-file-v1"]
        assert transforms[0].original_member_path == "images/disk.img"
        assert transforms[0].stored_member_path == "images/disk.img.zst"
        assert transforms[0].original_sha256 == _digest(original)
        assert transforms[0].stored_sha256 == staged.staged_sha256

        assert result.manifest_path is not None
        receipt = json.loads(Path(result.manifest_path).read_text())
        assert receipt["members"] == [
            {
                "member_name": "images/disk.img",
                "stored_member_name": "images/disk.img.zst",
                "logical_sha256": _digest(original).hex(),
                "stored_sha256": staged.staged_sha256.hex(),
                "transforms": ["zstd-file-v1"],
                "pfr_original": False,
            }
        ]
        assert resolve_member_asset_hash(
            s,
            artifactclass="o-archive",
            member_name="images/disk.img",
        ) == _digest(original)
        assert resolve_member_asset_hash(
            s,
            artifactclass="o-archive",
            member_name="images/disk.img.zst",
        ) == _digest(original)
        with pytest.raises(RestoreNameError):
            resolve_member_asset_hash(
                s,
                artifactclass="o-archive",
                member_name=r"bad\xFF",
            )

        rem_backend.reads.clear()
        restored = restore_asset(
            s,
            asset_hash=_digest(original),
            artifactclass="o-archive",
            destination=tmp_path / "restored.img",
            backends={rem_id: rem_backend, d2_id: d2_backend},
        )

    assert restored.output_path.read_bytes() == original
    assert rem_backend.reads
    assert all(not byte_range.is_whole_object for byte_range in rem_backend.reads)


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
        fallback = restore_asset(
            s,
            asset_hash=asset_hash,
            artifactclass="o-archive",
            destination=tmp_path / "corrupt-primary.bin",
            backends={
                setup.rem_backend_id: rem_backend,
                setup.d2_backend_id: d2_backend,
            },
        )
        assert fallback.pool_id == "d2-shelf-pool"

        d2_locator = s.scalars(
            select(AssetLocator).where(
                AssetLocator.pool_id == "d2-shelf-pool",
                AssetLocator.logical_asset_hash == asset_hash,
            )
        ).one()
        d2_locator.native_locator = {
            **d2_locator.native_locator,
            "block_range": [0, int(d2_locator.native_locator["size_bytes"])],
        }
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


def test_restore_asset_marks_digest_mismatch_copy_suspect_and_falls_through(
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
        locator = s.scalars(
            select(AssetLocator).where(
                AssetLocator.pool_id == "o-copy-1-pool",
                AssetLocator.logical_asset_hash == asset_hash,
            )
        ).one()
        locator.native_locator = {
            **locator.native_locator,
            "offset": int(locator.native_locator["offset"]) + 1,
        }
        primary_copy = locator.copy
        assert primary_copy is not None

        restored = restore_asset(
            s,
            asset_hash=asset_hash,
            artifactclass="o-archive",
            destination=tmp_path / "fallback.bin",
            backends={
                setup.rem_backend_id: rem_backend,
                setup.d2_backend_id: d2_backend,
            },
        )

        assert restored.pool_id == "d2-shelf-pool"
        assert primary_copy.health == CopyHealth.SUSPECT


def test_encrypted_restore_plumbing_uses_key_epoch_and_rao_range_args(
    engine: Engine,
    tmp_path: Path,
) -> None:
    restored = b"encrypted member"
    asset_hash = _digest(restored)
    key_epoch = "a" * 32
    key_file = tmp_path / "root.key"
    key_file.write_bytes(b"k" * 32)
    rem_script = tmp_path / "fake-rem"
    rem_script.write_text(
        """#!/usr/bin/env python3
import pathlib
import sys
args = sys.argv[1:]
required = ["--path", "--first-chunk-lba", "--file-size-bytes", "--range", "--key-file"]
missing = [flag for flag in required if flag not in args]
if missing:
    raise SystemExit("missing " + ",".join(missing))
dest = pathlib.Path(args[args.index("--dest") + 1])
member = args[args.index("--path") + 1]
out = dest / member
out.parent.mkdir(parents=True, exist_ok=True)
out.write_bytes(b"encrypted member")
""",
        encoding="utf-8",
    )
    rem_script.chmod(0o755)
    keys = _FakeKeyRegistry(key_file)
    backend = _ArchiveWriteBackend("enc")
    object_path = tmp_path / "encrypted.rao"
    object_path.write_bytes(b"not a local archive")

    with session_scope(engine) as s:
        row = Backend(
            name="rem",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(row)
        s.flush()
        s.add(
            Pool(
                id="encrypted-pool",
                backend_id=row.id,
                representation=Representation.RAO_AEAD_V1.value,
            )
        )
        s.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(restored)))
        s.add(Bundle(id="bundle-enc", artifactclass="o-archive", status="sealed"))
        s.flush()
        apply_artifactclass_policy(
            s,
            "o-archive",
            ArtifactClassPolicy(
                ruleset="rao.o.v1",
                placements=(PlacementPolicy("encrypted-pool"),),
                bundling=BundlingPolicy(target_gb=1, max_age_seconds=60),
                restore_preference=("encrypted-pool",),
                expect="messy",
                durability=DurabilityPolicy(min_copies=1, min_impl_families=1),
            ),
        )
        record = backend.write_object_to_pool(object_path, "encrypted-pool")
        copy, _ = add_bundle_copy(
            s,
            bundle_id="bundle-enc",
            backend_id=row.id,
            pool_id="encrypted-pool",
            native_locator=record.native_locator,
            integrity_hash=record.integrity_hash,
            source=CopySource.INGEST,
            storage_metadata={
                "representation": Representation.RAO_AEAD_V1.value,
                "key_epoch": key_epoch,
                "stored_size_bytes": record.size_bytes,
            },
        )
        s.add(
            AssetLocator(
                logical_asset_hash=asset_hash,
                pool_id="encrypted-pool",
                copy_id=copy.id,
                bundle_id="bundle-enc",
                member_path="nested/encrypted.bin",
                native_locator={
                    "member_path": "nested/encrypted.bin",
                    "first_chunk_lba": 4,
                    "size_bytes": len(restored),
                },
                representation=Representation.RAO_AEAD_V1.value,
            )
        )
        result = restore_asset(
            s,
            asset_hash=asset_hash,
            artifactclass="o-archive",
            destination=tmp_path / "encrypted-restore.bin",
            backends={row.id: backend},
            extractor=RemArchiveExtractor(rem_script, keys=keys),  # type: ignore[arg-type]
        )

    assert result.output_path.read_bytes() == restored
    assert keys.seen == [key_epoch]
