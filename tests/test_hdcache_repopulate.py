"""Hdcache M6 repopulation, drain, drill, and alarm tests."""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, select

import sutradhara.jobs.handlers as _handlers  # noqa: F401
from sutradhara.archive_restore import restore_assets_from_bundle
from sutradhara.backend.port import ByteRange
from sutradhara.backend.memory import MemoryBackend
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    ArtifactClassPool,
    AssetLocator,
    Backend,
    Bundle,
    BundleMember,
    Copy,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, locator_key, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource
from sutradhara.hdcache.alarms import (
    ALARM_DOMAIN,
    HdcacheAlarmConfig,
    evaluate_hdcache_alarm_conditions,
    record_restore_event_alarm,
    record_walker_event_alarm,
)
from sutradhara.hdcache.fill import HdcacheFillConfig
from sutradhara.hdcache.manager import RestoreEvent
from sutradhara.hdcache.models import CacheDisk, CacheEntry
from sutradhara.hdcache.repopulate import (
    RepopulationConfig,
    drain_retiring_disk,
    drill_status,
    enqueue_repopulation,
    execute_repopulation_batch,
)
from sutradhara.hdcache.store import RAW_REPRESENTATION, entry_path, write_entry
from sutradhara.hdcache.walker import HdcacheWalkerEvent
from sutradhara.jobs.engine import pending_candidates, submit
from sutradhara.jobs.models import Job, JobStatus, ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import CONDITION_OPEN, CONDITION_SATISFIED
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    eng = make_engine(f"sqlite:///{tmp_path / 'hdcache-m6.db'}")
    create_all(eng)
    yield eng
    eng.dispose()


def test_repopulation_planner_groups_by_source_tape_and_tags_drill(
    engine: Engine,
    tmp_path: Path,
) -> None:
    memory = MemoryBackend("mem")
    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "d001", state="dead")
        _add_disk(session, "d002", tmp_path / "d002")
        targets = _seed_bundle(
            session,
            memory,
            bundle_id="bundle-a",
            members=[b"clip-a", b"clip-b"],
            tape_uuid="tape-a",
        )
        targets += _seed_bundle(
            session,
            memory,
            bundle_id="bundle-b",
            members=[b"clip-c"],
            tape_uuid="tape-b",
        )
        lost_at = dt.datetime(2026, 7, 3, 8, tzinfo=dt.UTC)
        for target in targets:
            session.add(
                CacheEntry(
                    content_sha256=target["digest"],
                    artifactclass="s-masters",
                    bundle_key=target["bundle_id"],
                    group_key="s-masters:test",
                    disk_id="d001",
                    relpath=f"{target['digest'].hex()[:2]}/{target['digest'].hex()}",
                    size_bytes=target["size"],
                    state="lost",
                    representation=RAW_REPRESENTATION,
                    trusted=True,
                    lost_origin_disk_id="d001",
                    lost_drill_id="d001:20260703T080000Z",
                    lost_at=lost_at,
                )
            )

        plan = enqueue_repopulation(
            session,
            config=RepopulationConfig(
                fill_config=HdcacheFillConfig(live_job_cap=10, scratch_root=tmp_path / "scratch"),
                scratch_root=tmp_path / "scratch",
            ),
        )

        jobs = list(session.scalars(select(Job).where(Job.kind == "hdcache_fill").order_by(Job.id)))
        assert plan.count == 3
        assert plan.scheduled == 2
        assert [job.priority for job in jobs] == [50, 50]
        batch = next(job for job in jobs if job.params.get("repopulate_batch") is True)
        assert batch.params["source_tape"].endswith("tape_uuid:tape-a")
        assert batch.params["origin_drill_ids"] == ["d001:20260703T080000Z"]
        assert {item["content_sha256"] for item in batch.params["items"]} == {
            targets[0]["digest"].hex(),
            targets[1]["digest"].hex(),
        }
        singleton = next(job for job in jobs if job.params["source_tape"].endswith("tape_uuid:tape-b"))
        assert singleton.params["repopulate_batch"] is True
        assert singleton.params["origin_drill_ids"] == ["d001:20260703T080000Z"]
        assert [item["lost_drill_id"] for item in singleton.params["items"]] == [
            "d001:20260703T080000Z"
        ]
        assert singleton.params["source_tape"].endswith("tape_uuid:tape-b")


def test_repopulation_batch_extracts_bundle_once_and_fills_entries(
    engine: Engine,
    tmp_path: Path,
) -> None:
    memory = MemoryBackend("mem")
    extractor = CountingBundleExtractor()
    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "dead", state="dead")
        _add_disk(session, "d002", tmp_path / "active")
        targets = _seed_bundle(
            session,
            memory,
            bundle_id="bundle-a",
            members=[b"clip-a", b"clip-b"],
            tape_uuid="tape-a",
        )
        lost_at = dt.datetime(2026, 7, 3, 8, tzinfo=dt.UTC)
        for target in targets:
            session.add(
                CacheEntry(
                    content_sha256=target["digest"],
                    artifactclass="s-masters",
                    bundle_key="bundle-a",
                    group_key="s-masters:test",
                    disk_id="d001",
                    relpath=f"{target['digest'].hex()[:2]}/{target['digest'].hex()}",
                    size_bytes=target["size"],
                    state="lost",
                    representation=RAW_REPRESENTATION,
                    trusted=True,
                    lost_origin_disk_id="d001",
                    lost_drill_id="d001:20260703T080000Z",
                    lost_at=lost_at,
                )
            )
        plan = enqueue_repopulation(
            session,
            config=RepopulationConfig(
                fill_config=HdcacheFillConfig(live_job_cap=10, scratch_root=tmp_path / "scratch"),
                scratch_root=tmp_path / "scratch",
            ),
        )
        assert plan.scheduled == 1
        job = session.scalars(select(Job).where(Job.kind == "hdcache_fill")).one()

        results = execute_repopulation_batch(
            session,
            job.params,
            config=RepopulationConfig(
                fill_config=HdcacheFillConfig(scratch_root=tmp_path / "scratch"),
                scratch_root=tmp_path / "scratch",
                extractor=extractor,
                restore_backends={1: memory},
            ),
        )

        assert extractor.batch_calls == 1
        assert {result.source for result in results} == {"restore-batch"}
        for target in targets:
            entry = session.get(CacheEntry, target["digest"])
            assert entry is not None
            assert entry.state == "present"
            assert entry.disk_id == "d002"
            assert entry.refilled_at is not None
            assert entry_path(tmp_path / "active", target["digest"]).read_bytes() == target["data"]


def test_singleton_repopulation_uses_restore_not_live_source_path(
    engine: Engine,
    tmp_path: Path,
) -> None:
    memory = MemoryBackend("mem")
    landing = tmp_path / "landing" / "clip.mov"
    landing.parent.mkdir()
    landing.write_bytes(b"archive-only")
    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "dead", state="dead")
        _add_disk(session, "d002", tmp_path / "active")
        target = _seed_bundle(
            session,
            memory,
            bundle_id="bundle-a",
            members=[b"archive-only"],
            tape_uuid="tape-a",
            source_paths=[landing],
        )[0]
        session.add(
            CacheEntry(
                content_sha256=target["digest"],
                artifactclass="s-masters",
                bundle_key="bundle-a",
                group_key="s-masters:test",
                disk_id="d001",
                relpath=f"{target['digest'].hex()[:2]}/{target['digest'].hex()}",
                size_bytes=target["size"],
                state="lost",
                representation=RAW_REPRESENTATION,
                trusted=True,
                lost_origin_disk_id="d001",
                lost_drill_id="d001:20260703T080000Z",
                lost_at=dt.datetime(2026, 7, 3, 8, tzinfo=dt.UTC),
            )
        )
        plan = enqueue_repopulation(
            session,
            config=RepopulationConfig(
                fill_config=HdcacheFillConfig(live_job_cap=10, scratch_root=tmp_path / "scratch"),
                scratch_root=tmp_path / "scratch",
            ),
        )
        job = session.scalars(select(Job).where(Job.kind == "hdcache_fill")).one()

        results = execute_repopulation_batch(
            session,
            job.params,
            config=RepopulationConfig(
                fill_config=HdcacheFillConfig(scratch_root=tmp_path / "scratch"),
                scratch_root=tmp_path / "scratch",
                restore_backends={1: memory},
            ),
        )

        assert plan.scheduled == 1
        assert job.params["repopulate_batch"] is True
        assert results[0].source == "restore"
        assert entry_path(tmp_path / "active", target["digest"]).read_bytes() == b"archive-only"


def test_repopulation_priority_yields_to_restore_and_outranks_migration(
    engine: Engine,
    tmp_path: Path,
) -> None:
    memory = MemoryBackend("mem")
    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "dead", state="dead")
        _add_disk(session, "d002", tmp_path / "active")
        target = _seed_bundle(
            session,
            memory,
            bundle_id="bundle-a",
            members=[b"clip-a"],
            tape_uuid="tape-a",
        )[0]
        session.add(
            CacheEntry(
                content_sha256=target["digest"],
                artifactclass="s-masters",
                bundle_key="bundle-a",
                group_key="s-masters:test",
                disk_id="d001",
                relpath=f"{target['digest'].hex()[:2]}/{target['digest'].hex()}",
                size_bytes=target["size"],
                state="lost",
                representation=RAW_REPRESENTATION,
                trusted=True,
                lost_origin_disk_id="d001",
                lost_drill_id="d001:20260703T080000Z",
                lost_at=dt.datetime(2026, 7, 3, 8, tzinfo=dt.UTC),
            )
        )
        submit(session, "restore", {"restore_request_item_id": 1}, priority=0)
        submit(session, "migration", {}, priority=100)
        enqueue_repopulation(
            session,
            config=RepopulationConfig(
                fill_config=HdcacheFillConfig(live_job_cap=10, scratch_root=tmp_path / "scratch"),
                scratch_root=tmp_path / "scratch",
            ),
        )

        assert [job.kind for job in pending_candidates(session)] == [
            "restore",
            "hdcache_fill",
            "migration",
        ]


def test_drain_retiring_disk_verified_local_move_and_auto_dead(
    engine: Engine,
    tmp_path: Path,
) -> None:
    memory = MemoryBackend("mem")
    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "retiring", state="retiring")
        _add_disk(session, "d002", tmp_path / "active")
        target = _seed_bundle(
            session,
            memory,
            bundle_id="bundle-a",
            members=[b"local-good"],
            tape_uuid="tape-a",
        )[0]
        write_entry(tmp_path / "retiring", target["digest"], target["data"])
        session.get(CacheDisk, "d001").filled_bytes = target["size"]
        session.add(
            CacheEntry(
                content_sha256=target["digest"],
                artifactclass="s-masters",
                bundle_key="bundle-a",
                group_key="s-masters:test",
                disk_id="d001",
                relpath=f"{target['digest'].hex()[:2]}/{target['digest'].hex()}",
                size_bytes=target["size"],
                state="present",
                representation=RAW_REPRESENTATION,
                trusted=True,
            )
        )

        result = drain_retiring_disk(
            session,
            "d001",
            config=RepopulationConfig(
                fill_config=HdcacheFillConfig(scratch_root=tmp_path / "scratch"),
                scratch_root=tmp_path / "scratch",
                restore_backends={1: memory},
            ),
        )

        entry = session.get(CacheEntry, target["digest"])
        assert result == result.__class__(disk_id="d001", moved=1, fallback_to_tape=0, failed=0, auto_dead=True)
        assert entry is not None
        assert entry.disk_id == "d002"
        assert session.get(CacheDisk, "d001").state == "dead"
        assert entry_path(tmp_path / "active", target["digest"]).read_bytes() == target["data"]


def test_drain_falls_back_to_tape_on_corrupt_source(
    engine: Engine,
    tmp_path: Path,
) -> None:
    memory = MemoryBackend("mem")
    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "retiring", state="retiring")
        _add_disk(session, "d002", tmp_path / "active")
        target = _seed_bundle(
            session,
            memory,
            bundle_id="bundle-a",
            members=[b"tape-good"],
            tape_uuid="tape-a",
        )[0]
        bad_path = entry_path(tmp_path / "retiring", target["digest"])
        bad_path.parent.mkdir(parents=True, exist_ok=True)
        bad_path.write_bytes(b"corrupt")
        session.get(CacheDisk, "d001").filled_bytes = target["size"]
        session.add(
            CacheEntry(
                content_sha256=target["digest"],
                artifactclass="s-masters",
                bundle_key="bundle-a",
                group_key="s-masters:test",
                disk_id="d001",
                relpath=f"{target['digest'].hex()[:2]}/{target['digest'].hex()}",
                size_bytes=target["size"],
                state="present",
                representation=RAW_REPRESENTATION,
                trusted=True,
            )
        )

        result = drain_retiring_disk(
            session,
            "d001",
            config=RepopulationConfig(
                fill_config=HdcacheFillConfig(scratch_root=tmp_path / "scratch"),
                scratch_root=tmp_path / "scratch",
                restore_backends={1: memory},
            ),
        )

        assert result.fallback_to_tape == 1
        assert entry_path(tmp_path / "active", target["digest"]).read_bytes() == target["data"]


def test_drill_status_eta_math(engine: Engine, tmp_path: Path) -> None:
    started = dt.datetime(2026, 7, 3, 8, tzinfo=dt.UTC)
    now = started + dt.timedelta(hours=1)
    lost_digest = hashlib.sha256(b"lost").digest()
    refilled_digest = hashlib.sha256(b"refilled").digest()
    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "dead", state="dead")
        _add_disk(session, "d002", tmp_path / "active")
        for digest, size in ((lost_digest, 200), (refilled_digest, 100)):
            session.add(LogicalAsset(content_sha256=digest, size_bytes=size))
        session.add(
            CacheEntry(
                content_sha256=lost_digest,
                artifactclass="s-masters",
                disk_id="d001",
                relpath=f"{lost_digest.hex()[:2]}/{lost_digest.hex()}",
                size_bytes=200,
                state="lost",
                representation=RAW_REPRESENTATION,
                trusted=True,
                lost_origin_disk_id="d001",
                lost_drill_id="d001:20260703T080000Z",
                lost_at=started,
            )
        )
        session.add(
            CacheEntry(
                content_sha256=refilled_digest,
                artifactclass="s-masters",
                disk_id="d002",
                relpath=f"{refilled_digest.hex()[:2]}/{refilled_digest.hex()}",
                size_bytes=100,
                state="present",
                representation=RAW_REPRESENTATION,
                trusted=True,
                lost_origin_disk_id="d001",
                lost_drill_id="d001:20260703T080000Z",
                lost_at=started,
                refilled_at=now,
            )
        )

        status = drill_status(session, "d001", now=now)[0]

        assert status.remaining_entries == 1
        assert status.refilled_entries == 1
        assert status.bytes_per_hour == 100
        assert status.eta_seconds == 7200
        assert not status.completed


def test_alarm_condition_matrix(engine: Engine, tmp_path: Path) -> None:
    now = dt.datetime(2026, 7, 3, 12, tzinfo=dt.UTC)
    lost_digest = hashlib.sha256(b"lost").digest()
    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "full", capacity_bytes=100, filled_bytes=99)
        _add_disk(session, "d002", tmp_path / "absent", state="absent")
        _add_disk(session, "d003", tmp_path / "smart", smart_status="degraded")
        session.add(LogicalAsset(content_sha256=lost_digest, size_bytes=1))
        session.add(
            CacheEntry(
                content_sha256=lost_digest,
                artifactclass="s-masters",
                disk_id="d001",
                relpath=f"{lost_digest.hex()[:2]}/{lost_digest.hex()}",
                size_bytes=1,
                state="lost",
                representation=RAW_REPRESENTATION,
                trusted=True,
            )
        )
        session.add(
            Job(
                kind="hdcache_fill",
                params={},
                status=JobStatus.PENDING,
                created_at=now - dt.timedelta(hours=1),
                not_before=now - dt.timedelta(hours=1),
            )
        )

        evaluate_hdcache_alarm_conditions(
            session,
            config=HdcacheAlarmConfig(
                lost_backlog_threshold=0,
                fill_queue_stalled_seconds=60,
                now=now,
            ),
        )
        record_restore_event_alarm(
            session,
            RestoreEvent(code="privacy-unmapped", severity="alarm", detail="privacy level p4 unmapped"),
        )
        record_walker_event_alarm(
            session,
            HdcacheWalkerEvent(code="walker-tripwire-halt", severity="alarm", disk_id="d001"),
        )

        active = {
            row.target_key: row.reason
            for row in session.scalars(
                select(ReconciliationCondition).where(
                    ReconciliationCondition.domain == ALARM_DOMAIN,
                    ReconciliationCondition.condition == "open",
                )
            )
        }

        assert active["reserve-breach:d001"] == "reserve-breach"
        assert active["disk-unreachable:d002"] == "disk-unreachable"
        assert active["smart-degradation:d003"] == "smart-degradation"
        assert active["lost-backlog"] == "lost-backlog"
        assert active["fill-queue-stalled"] == "fill-queue-stalled"
        assert active["unmapped-privacy-level"] == "unmapped-privacy-level"
        assert active["walker-tripwire:d001"] == "walker-tripwire"


def test_disk_circuit_closed_satisfies_restore_alarm(engine: Engine) -> None:
    with session_scope(engine) as session:
        opened = record_restore_event_alarm(
            session,
            RestoreEvent(
                code="disk-circuit-open",
                severity="alarm",
                detail="cache disk d001 exceeded failure threshold",
            ),
        )

        assert opened is not None
        assert opened.target_key == "disk-unreachable:restore"
        assert opened.condition == CONDITION_OPEN

        closed = record_restore_event_alarm(
            session,
            RestoreEvent(
                code="disk-circuit-closed",
                severity="info",
                detail="cache disk d001 recovered",
            ),
        )

        assert closed is not None
        assert closed.target_key == "disk-unreachable:restore"
        assert closed.condition == CONDITION_SATISFIED
        active = {
            row.target_key
            for row in session.scalars(
                select(ReconciliationCondition).where(
                    ReconciliationCondition.domain == ALARM_DOMAIN,
                    ReconciliationCondition.condition == CONDITION_OPEN,
                )
            )
        }
        assert "disk-unreachable:restore" not in active


def test_restore_assets_from_bundle_uses_one_batch_extractor(
    engine: Engine,
    tmp_path: Path,
) -> None:
    memory = MemoryBackend("mem")
    extractor = CountingBundleExtractor()
    with session_scope(engine) as session:
        targets = _seed_bundle(
            session,
            memory,
            bundle_id="bundle-a",
            members=[b"clip-a", b"clip-b"],
            tape_uuid="tape-a",
        )

        results = restore_assets_from_bundle(
            session,
            asset_hashes=[target["digest"] for target in targets],
            artifactclass="s-masters",
            destination_dir=tmp_path / "restore",
            backends={1: memory},
            extractor=extractor,
        )

        assert extractor.batch_calls == 1
        assert [result.output_path.read_bytes() for result in results] == [b"clip-a", b"clip-b"]


class CountingBundleExtractor:
    def __init__(self) -> None:
        self.batch_calls = 0

    def extract_to_path(self, **_kwargs: Any) -> None:
        raise AssertionError("single-member extraction should not be used")

    def extract_bundle_to_paths(
        self,
        *,
        locators: list[AssetLocator],
        copy: Copy,
        backend: MemoryBackend,
        destinations: dict[bytes, Path],
    ) -> None:
        self.batch_calls += 1
        blob = backend.read_range(copy.native_locator, ByteRange(0, 0))
        for locator in locators:
            offset = int(locator.native_locator["offset"])
            size = int(locator.native_locator["size_bytes"])
            destinations[locator.logical_asset_hash].write_bytes(blob[offset : offset + size])


def _add_disk(
    session: Any,
    disk_id: str,
    mount: Path,
    *,
    state: str = "active",
    capacity_bytes: int = 10_000_000,
    filled_bytes: int = 0,
    smart_status: str | None = None,
) -> None:
    mount.mkdir(parents=True, exist_ok=True)
    session.add(
        CacheDisk(
            disk_id=disk_id,
            serial=f"SER-{disk_id}",
            fs_uuid=f"fs-{disk_id}",
            mount=str(mount),
            state=state,
            capacity_bytes=capacity_bytes,
            filled_bytes=filled_bytes,
            smart_status=smart_status,
        )
    )


def _seed_bundle(
    session: Any,
    memory: MemoryBackend,
    *,
    bundle_id: str,
    members: list[bytes],
    tape_uuid: str,
    source_paths: list[Path] | None = None,
) -> list[dict[str, Any]]:
    backend = _backend(session)
    _policy(session)
    offsets: list[int] = []
    cursor = 0
    for data in members:
        offsets.append(cursor)
        cursor += len(data)
    bundle_bytes = b"".join(members)
    object_hash = memory.add(bundle_bytes)
    bundle = Bundle(
        id=bundle_id,
        artifactclass="s-masters",
        status="sealed",
        target_bytes=1024,
        max_age_seconds=3600,
    )
    session.add(bundle)
    copy = Copy(
        bundle_id=bundle_id,
        backend_id=backend.id,
        pool_id="mem-pool",
        native_locator={
            "hash_hex": object_hash.hex(),
            "tape_uuid": tape_uuid,
            "tape_file_number": 1,
        },
        native_locator_key=locator_key({"hash_hex": object_hash.hex(), "tape_uuid": tape_uuid}),
        storage_metadata={"representation": Representation.RAW_BYTES.value},
        integrity_hash=object_hash,
        health=CopyHealth.OK,
        source=CopySource.INGEST,
    )
    session.add(copy)
    session.flush()
    targets: list[dict[str, Any]] = []
    for index, data in enumerate(members):
        digest = hashlib.sha256(data).digest()
        session.add(LogicalAsset(content_sha256=digest, size_bytes=len(data)))
        member_path = f"{index}-{digest.hex()}.mov"
        session.add(
            BundleMember(
                bundle_id=bundle_id,
                logical_asset_hash=digest,
                member_path=member_path,
                source_path=None if source_paths is None else str(source_paths[index]),
                size_bytes=len(data),
                file_sha256=digest,
            )
        )
        session.add(
            AssetLocator(
                logical_asset_hash=digest,
                pool_id="mem-pool",
                copy_id=copy.id,
                bundle_id=bundle_id,
                native_locator={
                    "member_path": member_path,
                    "offset": offsets[index],
                    "size_bytes": len(data),
                },
                member_path=member_path,
                representation=Representation.RAW_BYTES.value,
            )
        )
        targets.append(
            {
                "digest": digest,
                "size": len(data),
                "data": data,
                "bundle_id": bundle_id,
            }
        )
    session.flush()
    return targets


def _backend(session: Any) -> Backend:
    backend = session.scalar(select(Backend).where(Backend.name == "mem"))
    if backend is None:
        backend = Backend(name="mem", kind=BackendKind.MEMORY, tier=BackendTier.SELF_DESCRIBING)
        session.add(backend)
        session.flush()
    pool = session.get(Pool, "mem-pool")
    if pool is None:
        session.add(
            Pool(
                id="mem-pool",
                backend_id=backend.id,
                representation=Representation.RAW_BYTES.value,
            )
        )
    return backend


def _policy(session: Any) -> None:
    session.merge(
        ArtifactClassPolicyRecord(
            artifactclass="s-masters",
            ruleset="test.rules",
            expect="messy",
            target_bytes=1024,
            max_age_seconds=3600,
            restore_preference=["mem-pool"],
            staging_config={},
            hdcache_config={"enabled": True, "privacy_level": "none"},
        )
    )
    if (
        session.scalar(
            select(ArtifactClassPool).where(
                ArtifactClassPool.artifactclass == "s-masters",
                ArtifactClassPool.pool_id == "mem-pool",
            )
        )
        is None
    ):
        session.add(
            ArtifactClassPool(
                artifactclass="s-masters",
                pool_id="mem-pool",
                active=True,
                sort_order=0,
            )
        )
    session.flush()
