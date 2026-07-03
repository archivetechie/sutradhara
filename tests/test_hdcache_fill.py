"""Hdcache M3 fill, key-domain, and convergence tests."""

from __future__ import annotations

import contextlib
import errno
import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, select

import sutradhara.jobs.handlers as _handlers  # noqa: F401
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
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource
from sutradhara.hdcache.fill import (
    DOMAIN,
    HDCACHE_FILL_PRIORITY,
    JOB_KIND,
    HdcacheFillConfig,
    HdcacheFillTarget,
    dedupe_key,
    enqueue_targets,
    fill_target,
    observe_target,
)
from sutradhara.hdcache.models import CacheDisk, CacheEntry
from sutradhara.hdcache.store import AEAD_REPRESENTATION, RAW_REPRESENTATION, write_entry
from sutradhara.jobs.engine import run_one, submit
from sutradhara.jobs.models import Job, ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BLOCKED,
    OBSERVED_MISSING,
    record_observation,
)
from sutradhara.keys import KeyEpoch, KeyRegistry
from sutradhara.sealing.port import Representation, SealResult


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    eng = make_engine(f"sqlite:///{tmp_path / 'hdcache-fill.db'}")
    create_all(eng)
    yield eng
    eng.dispose()


def test_hdcache_fill_from_landing_and_restore_fallback(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing.mov"
    landing.write_bytes(b"landing bytes")
    fallback_bytes = b"fallback bytes"
    memory = MemoryBackend("mem")

    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "d001")
        landing_target = _seed_archived_asset(
            session,
            data=landing.read_bytes(),
            artifactclass="s-masters",
            source_path=landing,
        )
        fallback_target = _seed_archived_asset(
            session,
            data=fallback_bytes,
            artifactclass="s-masters",
            source_path=tmp_path / "already-purged.mov",
            memory=memory,
        )

        landed = fill_target(session, landing_target, config=_config(tmp_path))
        restored = fill_target(
            session,
            fallback_target,
            config=_config(tmp_path),
            restore_backends={1: memory},
        )

        assert landed.source == "landing"
        assert restored.source == "restore"
        assert session.get(CacheEntry, landing_target.content_sha256).state == "present"
        assert session.get(CacheEntry, fallback_target.content_sha256).state == "present"


def test_hdcache_aead_fill_records_hdcache_epoch_and_stored_digest(
    engine: Engine,
    tmp_path: Path,
) -> None:
    source = tmp_path / "private.mov"
    source.write_bytes(b"private bytes")
    registry = KeyRegistry(tmp_path / "keys")

    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "d001")
        target = _seed_archived_asset(
            session,
            data=source.read_bytes(),
            artifactclass="private",
            source_path=source,
            hdcache_config={"enabled": True, "privacy_level": "p2"},
        )

        result = fill_target(
            session,
            target,
            config=_config(tmp_path),
            key_registry=registry,
            sealer=FakeSealer(),
        )

        entry = session.get(CacheEntry, target.content_sha256)
        assert result.representation == AEAD_REPRESENTATION
        assert entry is not None
        assert entry.representation == AEAD_REPRESENTATION
        assert entry.key_epoch is not None
        assert entry.key_epoch.startswith("hdcache-")
        assert entry.stored_digest == hashlib.sha256(b"sealed:" + source.read_bytes()).digest()


def test_hdcache_fill_adopts_lost_row_and_replaces_dead_disk(
    engine: Engine,
    tmp_path: Path,
) -> None:
    source = tmp_path / "clip.mov"
    source.write_bytes(b"clip bytes")

    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "d001", state="dead")
        _add_disk(session, "d002", tmp_path / "d002")
        target = _seed_archived_asset(session, data=source.read_bytes(), source_path=source)
        session.add(
            CacheEntry(
                content_sha256=target.content_sha256,
                artifactclass=target.artifactclass,
                disk_id="d001",
                relpath=f"{target.sha_hex[:2]}/{target.sha_hex}",
                size_bytes=target.size_bytes,
                state="lost",
                representation=RAW_REPRESENTATION,
                trusted=True,
            )
        )

        result = fill_target(session, target, config=_config(tmp_path))

        assert result.disk_id == "d002"
        assert session.get(CacheEntry, target.content_sha256).disk_id == "d002"


def test_hdcache_fill_enospc_flags_disk_and_replaces(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "clip.mov"
    source.write_bytes(b"clip bytes")
    calls = 0

    def flaky_write(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError(errno.ENOSPC, "no space left")
        return write_entry(*args, **kwargs)

    monkeypatch.setattr("sutradhara.hdcache.fill.write_entry", flaky_write)

    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "d001")
        _add_disk(session, "d002", tmp_path / "d002")
        target = _seed_archived_asset(session, data=source.read_bytes(), source_path=source)

        result = fill_target(session, target, config=_config(tmp_path))

        assert result.disk_id in {"d001", "d002"}
        overfull = session.scalar(select(CacheDisk).where(CacheDisk.smart_status == "over-reserve"))
        assert overfull is not None
        assert session.get(CacheEntry, target.content_sha256).state == "present"
        assert calls == 2


def test_hdcache_enqueue_honors_live_cap_dedupe_and_blocked_targets(
    engine: Engine,
    tmp_path: Path,
) -> None:
    config = HdcacheFillConfig(live_job_cap=7, scratch_root=tmp_path / "scratch")
    targets = [
        HdcacheFillTarget(
            content_sha256=index.to_bytes(32, "big"),
            artifactclass="s-masters",
            size_bytes=1,
        )
        for index in range(10_000)
    ]

    with session_scope(engine) as session:
        record_observation(
            session,
            domain=DOMAIN,
            target_key=targets[0].sha_hex,
            desired=True,
            observed_state=OBSERVED_MISSING,
        )
        row = session.scalar(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == DOMAIN,
                ReconciliationCondition.target_key == targets[0].sha_hex,
            )
        )
        row.condition = CONDITION_BLOCKED
        row.next_eligible_at = None

        plan = enqueue_targets(session, targets, config=config)
        duplicate = submit(
            session,
            JOB_KIND,
            {"content_sha256": targets[1].sha_hex, "artifactclass": "s-masters"},
            dedupe_key=dedupe_key(targets[1].content_sha256),
            priority=HDCACHE_FILL_PRIORITY,
        )

        jobs = list(session.scalars(select(Job).where(Job.kind == JOB_KIND).order_by(Job.id)))
        assert plan.count == 10_000
        assert plan.scheduled == 7
        assert len(jobs) == 7
        assert duplicate.id == jobs[0].id
        assert all(job.recon_target_key != targets[0].sha_hex for job in jobs)


def test_hdcache_convergence_marks_privacy_raise_and_retired_epoch_lost(
    engine: Engine,
    tmp_path: Path,
) -> None:
    registry = KeyRegistry(tmp_path / "keys")
    raw_source = tmp_path / "raw.mov"
    raw_source.write_bytes(b"raw")
    private_source = tmp_path / "private.mov"
    private_source.write_bytes(b"private")

    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "d001")
        raw_target = _seed_archived_asset(
            session,
            data=raw_source.read_bytes(),
            artifactclass="raise",
            source_path=raw_source,
            hdcache_config={"enabled": True, "privacy_level": "none"},
        )
        private_target = _seed_archived_asset(
            session,
            data=private_source.read_bytes(),
            artifactclass="retired",
            source_path=private_source,
            hdcache_config={"enabled": True, "privacy_level": "p2"},
        )
        fill_target(session, raw_target, config=_config(tmp_path))
        fill_target(
            session,
            private_target,
            config=_config(tmp_path),
            key_registry=registry,
            sealer=FakeSealer(),
        )
        raw_policy = session.get(ArtifactClassPolicyRecord, "raise")
        raw_policy.hdcache_config = {"enabled": True, "privacy_level": "p2"}
        private_entry = session.get(CacheEntry, private_target.content_sha256)
        registry.retire_epoch(private_entry.key_epoch)

        assert observe_target(session, raw_target.sha_hex, mutate=True, key_registry=registry) == (
            True,
            OBSERVED_MISSING,
        )
        assert observe_target(
            session,
            private_target.sha_hex,
            mutate=True,
            key_registry=registry,
        ) == (True, OBSERVED_MISSING)

        assert session.get(CacheEntry, raw_target.content_sha256).state == "lost"
        assert session.get(CacheEntry, private_target.content_sha256).state == "lost"


def test_hdcache_fill_handler_projects_blocked_condition(
    engine: Engine,
    tmp_path: Path,
) -> None:
    digest = hashlib.sha256(b"not archived").digest()

    with session_scope(engine) as session:
        session.add(LogicalAsset(content_sha256=digest, size_bytes=12))
        record_observation(
            session,
            domain=DOMAIN,
            target_key=digest.hex(),
            desired=True,
            observed_state=OBSERVED_MISSING,
        )
        job = submit(
            session,
            JOB_KIND,
            {"content_sha256": digest.hex(), "artifactclass": "s-masters"},
            recon_domain=DOMAIN,
            recon_target_key=digest.hex(),
            dedupe_key=dedupe_key(digest),
        )

        result = run_one(session, job.id)

        condition = session.scalar(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == DOMAIN,
                ReconciliationCondition.target_key == digest.hex(),
            )
        )
        assert result.ok is False
        assert condition.condition == CONDITION_BLOCKED


class FakeSealer:
    @contextlib.contextmanager
    def seal(
        self,
        source_path: Path | str,
        representation: Representation,
        *,
        key_epoch: KeyEpoch | None = None,
        work_dir: Path | str | None = None,
    ) -> Iterator[SealResult]:
        assert representation is Representation.RAO_AEAD_V1
        assert key_epoch is not None
        assert key_epoch.key_id.startswith("hdcache-")
        assert work_dir is not None
        source = Path(source_path)
        sealed = Path(work_dir) / f"sealed-{hashlib.sha256(source.read_bytes()).hexdigest()}.rao"
        sealed.write_bytes(b"sealed:" + source.read_bytes())
        yield SealResult(
            sealed_path=sealed,
            stored_digest=hashlib.sha256(sealed.read_bytes()).digest(),
            plaintext_digest=hashlib.sha256(source.read_bytes()).digest(),
            representation=Representation.RAO_AEAD_V1,
        )


def _config(tmp_path: Path) -> HdcacheFillConfig:
    return HdcacheFillConfig(scratch_root=tmp_path / "scratch")


def _add_disk(
    session: Any,
    disk_id: str,
    mount: Path,
    *,
    state: str = "active",
) -> None:
    mount.mkdir(parents=True, exist_ok=True)
    session.add(
        CacheDisk(
            disk_id=disk_id,
            serial=f"SER-{disk_id}",
            fs_uuid=f"fs-{disk_id}",
            mount=str(mount),
            state=state,
            capacity_bytes=10_000_000,
            filled_bytes=0,
        )
    )


def _seed_archived_asset(
    session: Any,
    *,
    data: bytes,
    artifactclass: str = "s-masters",
    source_path: Path | None = None,
    memory: MemoryBackend | None = None,
    hdcache_config: dict[str, object] | None = None,
) -> HdcacheFillTarget:
    digest = hashlib.sha256(data).digest()
    source_text = None if source_path is None else str(source_path)
    session.merge(LogicalAsset(content_sha256=digest, size_bytes=len(data)))
    backend = session.scalar(select(Backend).where(Backend.name == "mem"))
    if backend is None:
        backend = Backend(
            name="mem",
            kind=BackendKind.MEMORY,
            tier=BackendTier.SELF_DESCRIBING,
        )
        session.add(backend)
        session.flush()
    pool = session.get(Pool, "mem-pool")
    if pool is None:
        pool = Pool(
            id="mem-pool",
            backend_id=backend.id,
            representation=Representation.RAW_BYTES.value,
        )
        session.add(pool)
    if session.get(ArtifactClassPolicyRecord, artifactclass) is None:
        session.add(
            ArtifactClassPolicyRecord(
                artifactclass=artifactclass,
                ruleset="test.rules",
                expect="messy",
                target_bytes=1024,
                max_age_seconds=3600,
                restore_preference=["mem-pool"],
                staging_config={},
                hdcache_config=hdcache_config or {"enabled": True, "privacy_level": "none"},
            )
        )
    else:
        session.get(ArtifactClassPolicyRecord, artifactclass).hdcache_config = (
            hdcache_config or {"enabled": True, "privacy_level": "none"}
        )
    if (
        session.scalar(
            select(ArtifactClassPool).where(
                ArtifactClassPool.artifactclass == artifactclass,
                ArtifactClassPool.pool_id == "mem-pool",
            )
        )
        is None
    ):
        session.add(
            ArtifactClassPool(
                artifactclass=artifactclass,
                pool_id="mem-pool",
                active=True,
                sort_order=0,
            )
        )
    bundle_id = f"bundle-{artifactclass}-{digest.hex()[:12]}"
    bundle = Bundle(
        id=bundle_id,
        artifactclass=artifactclass,
        status="sealed",
        target_bytes=1024,
        max_age_seconds=3600,
    )
    session.add(bundle)
    session.add(
        BundleMember(
            bundle_id=bundle_id,
            logical_asset_hash=digest,
            member_path=f"{digest.hex()}.mov",
            source_path=source_text,
            size_bytes=len(data),
            file_sha256=digest,
        )
    )
    if memory is not None:
        memory.add(data)
    copy = Copy(
        bundle_id=bundle_id,
        backend_id=backend.id,
        pool_id="mem-pool",
        native_locator={"hash_hex": digest.hex()},
        native_locator_key=f'{{"hash_hex":"{digest.hex()}"}}',
        storage_metadata={"representation": Representation.RAW_BYTES.value},
        integrity_hash=digest,
        health=CopyHealth.OK,
        source=CopySource.INGEST,
    )
    session.add(copy)
    session.flush()
    session.add(
        AssetLocator(
            logical_asset_hash=digest,
            pool_id="mem-pool",
            copy_id=copy.id,
            bundle_id=bundle_id,
            native_locator={
                "member_path": f"{digest.hex()}.mov",
                "offset": 0,
                "size_bytes": len(data),
            },
            member_path=f"{digest.hex()}.mov",
            representation=Representation.RAW_BYTES.value,
        )
    )
    session.flush()
    return HdcacheFillTarget(
        content_sha256=digest,
        artifactclass=artifactclass,
        size_bytes=len(data),
        bundle_key=bundle_id,
        group_key=f"{artifactclass}:test",
        source_path=source_text,
    )
