"""Bundle-copy repair and reconciler tests."""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pytest
from sqlalchemy import Engine, select

from sutradhara.archive_bundle import add_bundle_member
from sutradhara.archive_fanout import (
    BuildArtifact,
    BuiltBlobRoot,
    BuiltExclusion,
    LocalArchiveBuilder,
    flush_bundle,
)
from sutradhara.archive_restore import read_member_bytes
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    BundlingPolicy,
    DurabilityPolicy,
    PlacementPolicy,
    apply_artifactclass_policy,
)
from sutradhara.backend.port import (
    BackendError,
    BackendLocator,
    ByteRange,
    CopyRecord,
    VerifyResult,
)
from sutradhara.catalog.copies import add_bundle_copy
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    AssetLocator,
    Backend,
    BlobRoot,
    Bundle,
    Copy,
    ExclusionRecord,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource, content_hash
from sutradhara.durability import bundle_replication_status
from sutradhara.jobs import handlers as _handlers  # noqa: F401 -- register bundle-repair
from sutradhara.jobs.engine import run_one, submit
from sutradhara.jobs.handlers import bundle_repair
from sutradhara.jobs.models import Job, JobAttempt, JobStatus, ReconciliationCondition
from sutradhara.jobs.reconcilers import bundle_copy
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    CONDITION_BLOCKED,
    CONDITION_SATISFIED,
    OBSERVED_MISSING,
    OBSERVED_PRESENT,
)
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


class _Backend:
    def __init__(self, *, kind: BackendKind = BackendKind.REM_TAPE) -> None:
        self.kind = kind
        self._counter = 0
        self.objects: dict[str, bytes] = {}
        self.writes: list[str] = []
        self.read_failures: set[str] = set()
        self.fail_next_verify = False

    @property
    def name(self) -> str:
        return "rem"

    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        self._counter += 1
        data = Path(source).read_bytes()
        digest = content_hash(hashlib.sha256(data).digest())
        object_id = f"obj-{self._counter}"
        self.objects[object_id] = data
        self.writes.append(pool)
        media_locator = (
            {"barcode": f"D2BAR{self._counter:06d}"}
            if self.kind == BackendKind.D2_TAPE
            else {"tape_uuid": f"{self._counter:032x}"}
        )
        return CopyRecord(
            logical_id=digest,
            native_locator={
                "pool_id": pool,
                "object_id": object_id,
                "content_sha256": digest.hex(),
                "tape_file_number": self._counter,
                **media_locator,
            },
            integrity_hash=digest,
            size_bytes=len(data),
        )

    def enumerate(self) -> Iterator[CopyRecord]:
        return iter(())

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        object_id = str(locator["object_id"])
        if object_id in self.read_failures:
            raise BackendError(f"transport unavailable for {object_id}")
        data = self.objects[object_id]
        if byte_range.is_whole_object:
            return data
        return data[byte_range.start : byte_range.end]

    def verify(self, locator: BackendLocator) -> VerifyResult:
        if self.fail_next_verify:
            self.fail_next_verify = False
            return VerifyResult(ok=False, detail="forced verify failure")
        actual = content_hash(hashlib.sha256(self.read_range(locator, ByteRange(0, 0))).digest())
        expected = content_hash(bytes.fromhex(str(locator["content_sha256"])))
        return VerifyResult(ok=actual == expected, actual_hash=actual)

    def corrupt_member(self, copy: Copy, locator: AssetLocator) -> None:
        object_id = str(copy.native_locator["object_id"])
        data = bytearray(self.objects[object_id])
        header_len = int.from_bytes(data[:8], "big")
        offset = 8 + header_len + int(locator.native_locator["offset"])
        data[offset] ^= 0x01
        self.objects[object_id] = bytes(data)


class _OutputsBuilder(LocalArchiveBuilder):
    def build(self, **kwargs: object) -> BuildArtifact:
        artifact = super().build(**kwargs)
        return replace(
            artifact,
            blob_roots=(
                BuiltBlobRoot(
                    root_path="root",
                    native_locator={"member_path": "root", "offset": 0},
                ),
            ),
            exclusions=(BuiltExclusion(path="skip.tmp", reason="test-exclusion"),),
        )


def test_bundle_repair_rebuilds_missing_pool_after_staging_purge(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _Backend(kind=BackendKind.D2_TAPE)
    bundle_id, _backend_id, assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))

    with session_scope(engine) as s:
        old_exclusion_count = len(list(s.scalars(select(ExclusionRecord))))
        p2_copy = s.scalars(select(Copy).where(Copy.pool_id == "p2")).one()
        p2_copy.health = CopyHealth.MISSING
        bundle = s.get(Bundle, bundle_id)
        assert bundle is not None
        for member in bundle.members:
            member.source_path = str(tmp_path / "purged" / member.member_path)

        monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda _row: backend)
        monkeypatch.setattr(
            bundle_repair,
            "make_archive_builder",
            lambda rem_bin=None: _OutputsBuilder(),
        )
        job = submit(s, "bundle-repair", {"bundle_id": bundle_id})
        result = run_one(s, job.id)

        assert result.ok
        repaired = s.scalars(
            select(Copy).where(Copy.pool_id == "p2", Copy.health == CopyHealth.OK)
        ).one()
        locators = list(s.scalars(select(AssetLocator).where(AssetLocator.copy_id == repaired.id)))
        assert len(locators) == len(assets)
        assert len(list(s.scalars(select(BlobRoot).where(BlobRoot.copy_id == repaired.id)))) == 1
        assert len(list(s.scalars(select(ExclusionRecord)))) == old_exclusion_count
        for locator in locators:
            assert read_member_bytes(backend, repaired, locator, work_dir=tmp_path) == assets[
                locator.logical_asset_hash
            ]
        attempt = s.scalars(select(JobAttempt).where(JobAttempt.job_id == job.id)).one()
        assert "tape:D2BAR000003" in attempt.detail["components"]


def test_bundle_repair_marks_corrupt_source_suspect_and_falls_back(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _Backend()
    bundle_id, _backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2", "p3"))

    with session_scope(engine) as s:
        p1_copy = s.scalars(select(Copy).where(Copy.pool_id == "p1")).one()
        p1_locator = s.scalars(
            select(AssetLocator).where(AssetLocator.copy_id == p1_copy.id)
        ).first()
        assert p1_locator is not None
        backend.corrupt_member(p1_copy, p1_locator)
        p1_copy.last_verified_at = dt.datetime(2026, 1, 2, tzinfo=dt.UTC)
        p2_copy = s.scalars(select(Copy).where(Copy.pool_id == "p2")).one()
        p2_copy.last_verified_at = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
        p3_copy = s.scalars(select(Copy).where(Copy.pool_id == "p3")).one()
        p3_copy.health = CopyHealth.MISSING

        monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda _row: backend)
        monkeypatch.setattr(
            bundle_repair,
            "make_archive_builder",
            lambda rem_bin=None: _OutputsBuilder(),
        )
        job = submit(s, "bundle-repair", {"bundle_id": bundle_id})
        result = run_one(s, job.id)

        assert result.ok
        assert p1_copy.health == CopyHealth.SUSPECT
        assert s.scalars(
            select(Copy).where(Copy.pool_id == "p3", Copy.health == CopyHealth.OK)
        ).one()


def test_bundle_repair_transport_error_falls_back_without_suspect_latch(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _Backend()
    bundle_id, _backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2", "p3"))

    with session_scope(engine) as s:
        p1_copy = s.scalars(select(Copy).where(Copy.pool_id == "p1")).one()
        p1_copy.last_verified_at = dt.datetime(2026, 1, 2, tzinfo=dt.UTC)
        p2_copy = s.scalars(select(Copy).where(Copy.pool_id == "p2")).one()
        p2_copy.last_verified_at = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
        p3_copy = s.scalars(select(Copy).where(Copy.pool_id == "p3")).one()
        p3_copy.health = CopyHealth.MISSING
        backend.read_failures.add(str(p1_copy.native_locator["object_id"]))

        monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda _row: backend)
        monkeypatch.setattr(
            bundle_repair,
            "make_archive_builder",
            lambda rem_bin=None: _OutputsBuilder(),
        )
        job = submit(s, "bundle-repair", {"bundle_id": bundle_id})
        result = run_one(s, job.id)

        assert result.ok
        assert p1_copy.health == CopyHealth.OK
        assert s.scalars(
            select(Copy).where(Copy.pool_id == "p3", Copy.health == CopyHealth.OK)
        ).one()


def test_bundle_repair_failed_target_verification_commits_suspect_copy_then_reruns(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _Backend()
    bundle_id, _backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))

    with session_scope(engine) as s:
        p2_copy = s.scalars(select(Copy).where(Copy.pool_id == "p2")).one()
        p2_copy.health = CopyHealth.MISSING
        backend.fail_next_verify = True

        monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda _row: backend)
        monkeypatch.setattr(
            bundle_repair,
            "make_archive_builder",
            lambda rem_bin=None: _OutputsBuilder(),
        )
        first_job = submit(s, "bundle-repair", {"bundle_id": bundle_id})
        first_result = run_one(s, first_job.id)

        assert not first_result.ok
        assert first_job.status == JobStatus.FAILED
        suspect = s.scalars(
            select(Copy).where(Copy.pool_id == "p2", Copy.health == CopyHealth.SUSPECT)
        ).one()
        assert suspect.last_verified_at is None
        status = bundle_replication_status(s, bundle_id)
        assert status["complete"] is False
        assert {target.pool_id for target in status["missing"]} == {"p2"}

        second_job = submit(s, "bundle-repair", {"bundle_id": bundle_id})
        second_result = run_one(s, second_job.id)

        assert second_result.ok
        assert second_job.status == JobStatus.SUCCEEDED
        status = bundle_replication_status(s, bundle_id)
        assert status["complete"] is True


def test_bundle_repair_reports_write_fenced_missing_pools(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _Backend()
    bundle_id, _backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))

    with session_scope(engine) as s:
        p2 = s.get(Pool, "p2")
        assert p2 is not None
        p2.accepts_writes = False
        p2_copy = s.scalars(select(Copy).where(Copy.pool_id == "p2")).one()
        p2_copy.health = CopyHealth.MISSING
        policy = s.get(ArtifactClassPolicyRecord, "class-a")
        assert policy is not None
        policy.min_copies = 1
        condition = s.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == "bundle_copy",
                ReconciliationCondition.target_key == bundle_id,
            )
        ).one()
        assert condition.condition == CONDITION_SATISFIED

        monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda _row: backend)
        monkeypatch.setattr(
            bundle_repair,
            "make_archive_builder",
            lambda rem_bin=None: _OutputsBuilder(),
        )
        job = submit(
            s,
            "bundle-repair",
            {"bundle_id": bundle_id},
            recon_domain="bundle_copy",
            recon_target_key=bundle_id,
            dedupe_key=f"bundle_copy:{bundle_id}",
        )
        result = run_one(s, job.id)

        assert result.ok
        assert "write-fenced" in result.detail
        assert "p2" in result.detail
        assert result.step_state["bundle_repair"]["repaired_pools"] == []
        assert result.step_state["bundle_repair"]["remaining_write_fenced_pools"] == ["p2"]
        status = bundle_replication_status(s, bundle_id)
        assert status["complete"] is False
        assert {target.pool_id for target in status["missing"]} == {"p2"}
        assert condition.condition == CONDITION_BACKOFF
        assert condition.reason == "fenced-missing"
        assert condition.observed_state == OBSERVED_MISSING
        assert condition.message is not None
        assert "p2" in condition.message


def test_bundle_copy_reconciler_observes_placement_complete_without_default_floor(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = _Backend()
    bundle_id, _backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))

    with session_scope(engine) as s:
        p2_copy = s.scalars(select(Copy).where(Copy.pool_id == "p2")).one()
        p2_copy.health = CopyHealth.MISSING

        missing = bundle_copy.observe(s, bundle_id)
        assert missing.observed_state == OBSERVED_MISSING
        assert bundle_copy.enumerate_targets(s, None, 100)[0].observed_state == OBSERVED_MISSING

        p2_copy.health = CopyHealth.OK
        present = bundle_copy.observe(s, bundle_id)
        assert present.observed_state == OBSERVED_PRESENT

        bundle_copy.reconcile_target(s, bundle_id)
        job = s.scalars(select(Job).where(Job.kind == "bundle-repair")).one()
        assert job.params == {"bundle_id": bundle_id}
        assert job.recon_domain == "bundle_copy"
        assert job.recon_target_key == bundle_id


def test_flush_bundle_outbox_fast_path_resolves_condition(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = _Backend()
    bundle_id, _backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))

    with session_scope(engine) as s:
        condition = s.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == "bundle_copy",
                ReconciliationCondition.target_key == bundle_id,
            )
        ).one()
        assert condition.condition == CONDITION_SATISFIED
        assert condition.observed_state == OBSERVED_PRESENT
        assert condition.reason is None


def test_bundle_copy_structural_floor_blocks_without_duplicate_write(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = _Backend()
    bundle_id, _backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))

    with session_scope(engine) as s:
        policy = s.get(ArtifactClassPolicyRecord, "class-a")
        assert policy is not None
        policy.min_impl_families = 2
        before = len(list(s.scalars(select(Copy).where(Copy.bundle_id == bundle_id))))

        condition = bundle_copy.refresh_condition(s, bundle_id)

        after = len(list(s.scalars(select(Copy).where(Copy.bundle_id == bundle_id))))
        assert after == before
        assert condition.condition == CONDITION_BLOCKED
        assert condition.reason == "durability-floor-unsatisfiable"


def test_bundle_copy_duplicate_alarm_logs_once(
    engine: Engine,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = _Backend()
    bundle_id, backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))

    with session_scope(engine) as s:
        existing = s.scalars(select(Copy).where(Copy.bundle_id == bundle_id, Copy.pool_id == "p1")).one()
        add_bundle_copy(
            s,
            bundle_id=bundle_id,
            backend_id=backend_id,
            pool_id="p1",
            native_locator={
                "pool_id": "p1",
                "object_id": "duplicate",
                "tape_uuid": "f" * 32,
            },
            integrity_hash=existing.integrity_hash,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
            storage_metadata=existing.storage_metadata,
        )
        s.flush()
        caplog.set_level("ERROR", logger="sutradhara.jobs.reconcilers.bundle_copy")

        first = bundle_copy.refresh_condition(s, bundle_id)
        second = bundle_copy.refresh_condition(s, bundle_id)

        assert first.condition == CONDITION_BLOCKED
        assert second.condition == CONDITION_BLOCKED
        assert second.reason == "duplicate-copy"
        assert sum(
            1 for record in caplog.records if record.message == "bundle_copy_condition_blocked"
        ) == 1


def _flushed_bundle(
    engine: Engine,
    tmp_path: Path,
    backend: _Backend,
    pool_ids: tuple[str, ...],
) -> tuple[str, int, dict[bytes, bytes]]:
    paths = {
        "a.bin": b"alpha",
        "nested/b.bin": b"beta",
    }
    with session_scope(engine) as s:
        row = Backend(name="rem", kind=backend.kind, tier=BackendTier.SELF_DESCRIBING)
        s.add(row)
        s.flush()
        for pool_id in pool_ids:
            s.add(
                Pool(
                    id=pool_id,
                    backend_id=row.id,
                    representation=Representation.RAO_PLAIN_V1.value,
                )
            )
        s.flush()
        apply_artifactclass_policy(
            s,
            "class-a",
            ArtifactClassPolicy(
                ruleset="",
                placements=tuple(PlacementPolicy(pool_id) for pool_id in pool_ids),
                bundling=BundlingPolicy(target_gb=1, max_age_seconds=60),
                restore_preference=pool_ids,
                expect="messy",
                durability=DurabilityPolicy(
                    min_copies=len(pool_ids),
                    min_impl_families=1,
                ),
            ),
        )
        bundle = Bundle(id="bundle-a", artifactclass="class-a", status="open")
        s.add(bundle)
        assets: dict[bytes, bytes] = {}
        for member_path, data in paths.items():
            asset_hash = hashlib.sha256(data).digest()
            source = tmp_path / member_path.replace("/", "_")
            source.write_bytes(data)
            s.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(data)))
            s.flush()
            add_bundle_member(
                s,
                bundle=bundle,
                logical_asset_hash=asset_hash,
                member_path=member_path,
                size_bytes=len(data),
                file_sha256=asset_hash,
                source_path=str(source),
            )
            assets[asset_hash] = data
        flush_bundle(
            s,
            bundle_id=bundle.id,
            backends={row.id: backend},
            builder=_OutputsBuilder(),
        )
        return bundle.id, row.id, assets
