"""Hdcache M3 fill, key-domain, and convergence tests."""

from __future__ import annotations

import contextlib
import errno
import hashlib
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, func, select

import sutradhara.hdcache.fill as fill_module
import sutradhara.jobs.handlers as _handlers  # noqa: F401
import sutradhara.jobs.reconcilers.hdcache as _hdcache_reconciler
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
from sutradhara.hdcache.fill import (
    DOMAIN,
    JOB_KIND,
    HdcacheFillBlocked,
    HdcacheFillConfig,
    HdcacheFillTarget,
    count_live_hdcache_jobs,
    dedupe_key,
    enqueue_targets,
    fill_target,
    mark_entry_lost_and_delete,
    observe_target,
    submit_hdcache_fill,
)
from sutradhara.hdcache.models import CacheDisk, CacheEntry
from sutradhara.hdcache.store import (
    AEAD_REPRESENTATION,
    RAW_REPRESENTATION,
    ExpectedDiskIdentity,
    ObservedBlockIdentity,
    StoreError,
    entry_path,
    write_disk_sentinel,
    write_entry,
)
from sutradhara.jobs.config import WorkerConfig
from sutradhara.jobs.engine import run_one, submit
from sutradhara.jobs.models import Job, JobAttempt, JobStatus, ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BLOCKED,
    OBSERVED_MISSING,
    OBSERVED_PRESENT,
    record_observation,
)
from sutradhara.jobs.reconcilers.spine import reconcile
from sutradhara.jobs.registry import JobContext, JobResult
from sutradhara.jobs.worker import JobWorker
from sutradhara.keys import KeyEpoch, KeyRegistry
from sutradhara.sealing.port import Representation, SealResult

TEST_HDCACHE_HMAC_SECRET = b"hdcache-fill-test-secret"


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


def test_hdcache_fill_handler_attempt_records_d2_tape_fallback(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sutradhara.jobs.handlers.hdcache_fill as hdcache_fill_handler

    data = b"d2 fallback bytes"
    memory = MemoryBackend("d2-stub")
    barcode = "D2CACHE01"
    config = _config(tmp_path)

    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "d001")
        target = _seed_archived_asset(
            session,
            data=data,
            artifactclass="s-masters",
            source_path=tmp_path / "purged.mov",
            memory=memory,
        )
        copy = session.scalars(select(Copy).where(Copy.bundle_id == target.bundle_key)).one()
        copy.backend.kind = BackendKind.D2_TAPE
        copy.native_locator = {
            **copy.native_locator,
            "barcode": barcode,
            "volume_uuid": "d2-volume-1",
        }
        copy.native_locator_key = locator_key(copy.native_locator)
        monkeypatch.setattr(fill_module, "backend_from_row", lambda _row: memory)
        monkeypatch.setattr(hdcache_fill_handler, "fill_config_from_env", lambda: config)
        record_observation(
            session,
            domain=DOMAIN,
            target_key=target.sha_hex,
            desired=True,
            observed_state=OBSERVED_MISSING,
        )

        job = submit_hdcache_fill(session, target, config=config)
        assert job is not None
        result = run_one(session, job.id)

        assert result.ok, result.detail
        attempt = session.scalars(select(JobAttempt).where(JobAttempt.job_id == job.id)).one()
        assert f"tape:{barcode}" in attempt.detail["components"]


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
        assert session.get(CacheDisk, result.disk_id).filled_bytes == len(
            b"sealed:" + source.read_bytes()
        )


def test_policy_sha_change_between_reservation_and_finalize_refills_under_new_policy(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "clip.mov"
    source.write_bytes(b"privacy race")
    registry = KeyRegistry(tmp_path / "keys")
    original_write = fill_module._write_source_to_disk
    raised = False

    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "d001")
        target = _seed_archived_asset(
            session,
            data=source.read_bytes(),
            artifactclass="race",
            source_path=source,
            hdcache_config={"enabled": True, "privacy_level": "none"},
        )
        policy = session.get(ArtifactClassPolicyRecord, "race")
        policy.policy_sha256 = "policy-none"
        session.flush([policy])

        def racing_write(*args: Any, **kwargs: Any) -> Any:
            nonlocal raised
            result = original_write(*args, **kwargs)
            if not raised and kwargs["representation"] == RAW_REPRESENTATION:
                with session_scope(engine) as policy_session:
                    policy = policy_session.get(ArtifactClassPolicyRecord, "race")
                    policy.hdcache_config = {"enabled": True, "privacy_level": "p2"}
                    policy.policy_sha256 = "policy-p2"
                    policy_session.flush([policy])
                raised = True
            return result

        monkeypatch.setattr(fill_module, "_write_source_to_disk", racing_write)

        result = fill_target(
            session,
            target,
            config=_config(tmp_path),
            key_registry=registry,
            sealer=FakeSealer(),
        )

        entry = session.get(CacheEntry, target.content_sha256)
        raw_path = entry_path(tmp_path / "d001", target.content_sha256, representation=RAW_REPRESENTATION)
        assert raised is True
        assert result.representation == AEAD_REPRESENTATION
        assert entry.representation == AEAD_REPRESENTATION
        assert entry.key_epoch is not None
        assert not raw_path.exists()


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
        overfull = session.scalar(
            select(CacheDisk).where(CacheDisk.capacity_state == "over_reserve")
        )
        assert overfull is not None
        assert overfull.disk_id != result.disk_id
        assert overfull.smart_status is None
        assert overfull.filled_bytes == 0
        assert session.get(CacheDisk, result.disk_id).filled_bytes == target.size_bytes
        assert session.get(CacheEntry, target.content_sha256).state == "present"
        assert calls == 2


def test_hdcache_reconciler_honors_live_cap_and_tops_up_archived_backlog(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cap = 7
    backlog = 10_000
    monkeypatch.setenv("SUTRADHARA_HDCACHE_LIVE_JOB_CAP", str(cap))
    monkeypatch.setenv("SUTRADHARA_HDCACHE_SCRATCH_ROOT", str(tmp_path / "scratch"))

    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "d001")
        _seed_archived_backlog(session, tmp_path, count=backlog)

        assert session.scalar(select(func.count()).select_from(BundleMember)) == backlog
        discovered, processed = reconcile(session, DOMAIN, batch=cap * 3, limit=cap * 3)
        assert discovered == cap * 3
        assert processed == cap * 3
        assert count_live_hdcache_jobs(session) == cap
        _assert_live_cap(session, cap)

        initial_jobs = _hdcache_jobs(session, status=JobStatus.PENDING)
        assert len(initial_jobs) == cap
        for job in initial_jobs[:3]:
            result = run_one(session, job.id)
            assert result.ok, result.detail
            _assert_live_cap(session, cap)

        assert count_live_hdcache_jobs(session) == cap - 3
        reconcile(session, DOMAIN, batch=cap * 3, limit=cap * 3)
        assert count_live_hdcache_jobs(session) == cap
        _assert_live_cap(session, cap)
        assert len(_hdcache_jobs(session, status=JobStatus.SUCCEEDED)) == 3


def test_enqueue_targets_records_backoff_when_live_cap_drops_work(
    engine: Engine,
    tmp_path: Path,
) -> None:
    with session_scope(engine) as session:
        _add_disk(session, "d001", tmp_path / "d001")
        targets = _seed_archived_backlog(session, tmp_path, count=2)

        plan = enqueue_targets(
            session,
            targets,
            config=HdcacheFillConfig(live_job_cap=1, scratch_root=tmp_path / "scratch"),
        )

        condition = session.scalar(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == DOMAIN,
                ReconciliationCondition.target_key == targets[1].sha_hex,
            )
        )
        assert plan.scheduled == 1
        assert condition is not None
        assert condition.condition == "backoff"
        assert condition.reason == "live-cap"
        assert condition.next_eligible_at is not None


def test_hdcache_fill_jobs_declare_io_lease_and_serialize(
    engine: Engine,
) -> None:
    targets = [
        HdcacheFillTarget(hashlib.sha256(f"fill-{index}".encode()).digest(), "s-masters", 10)
        for index in range(2)
    ]
    with session_scope(engine) as session:
        for target in targets:
            record_observation(
                session,
                domain=DOMAIN,
                target_key=target.sha_hex,
                desired=True,
                observed_state=OBSERVED_MISSING,
            )
            job = submit_hdcache_fill(session, target, config=HdcacheFillConfig())
            assert job is not None
            assert job.required_resources == [{"pool": "io", "count": 1}]

    state = {"active": 0, "max_active": 0}
    lock = threading.Lock()

    def fake_fill(ctx: JobContext) -> JobResult:
        assert ctx.granted_leases == {"io": 1}
        with lock:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
        time.sleep(0.05)
        with lock:
            state["active"] -= 1
        return JobResult(ok=True, detail="filled")

    from sutradhara.jobs import registry as _r

    original = _r._HANDLERS[JOB_KIND]
    _r._HANDLERS[JOB_KIND] = fake_fill
    try:
        worker = JobWorker(
            engine,
            config=WorkerConfig(
                capacities={"cpu": 2, "io": 1, "tape_drive": 0, "gpu": 0},
                executor_workers=2,
            ),
        )
        worker.drain()
    finally:
        _r._HANDLERS[JOB_KIND] = original

    assert state["max_active"] == 1


def test_hdcache_convergence_marks_privacy_raise_and_retired_epoch_lost(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = KeyRegistry(tmp_path / "keys")
    monkeypatch.setenv("SUTRADHARA_KEY_REGISTRY_DIR", str(registry.registry_dir))
    monkeypatch.setenv("SUTRADHARA_HDCACHE_SCRATCH_ROOT", str(tmp_path / "scratch"))
    monkeypatch.setenv("SUTRADHARA_HDCACHE_HMAC_SECRET_HEX", TEST_HDCACHE_HMAC_SECRET.hex())
    monkeypatch.setattr("sutradhara.hdcache.fill.RaoCliSealer", FakeSealer)
    monkeypatch.setattr(_hdcache_reconciler, "fill_config_from_env", lambda: _config(tmp_path))
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
        raw_entry = session.get(CacheEntry, raw_target.content_sha256)
        raw_disk = session.get(CacheDisk, raw_entry.disk_id)
        raw_path = entry_path(
            Path(raw_disk.mount),
            raw_entry.content_sha256,
            representation=RAW_REPRESENTATION,
        )
        assert raw_path.is_file()
        raw_policy = session.get(ArtifactClassPolicyRecord, "raise")
        raw_policy.hdcache_config = {"enabled": True, "privacy_level": "p2"}
        private_entry = session.get(CacheEntry, private_target.content_sha256)
        retired_epoch = private_entry.key_epoch
        registry.retire_epoch(retired_epoch)

        reconcile(session, DOMAIN, batch=10, limit=10)
        jobs = _hdcache_jobs(session, status=JobStatus.PENDING)
        assert {job.recon_target_key for job in jobs} == {
            raw_target.sha_hex,
            private_target.sha_hex,
        }
        assert not raw_path.exists()

        assert session.get(CacheEntry, raw_target.content_sha256).state == "lost"
        assert session.get(CacheEntry, private_target.content_sha256).state == "lost"

        for job in jobs:
            result = run_one(session, job.id)
            assert result.ok, result.detail

        raw_entry = session.get(CacheEntry, raw_target.content_sha256)
        private_entry = session.get(CacheEntry, private_target.content_sha256)
        assert raw_entry.state == "present"
        assert raw_entry.representation == AEAD_REPRESENTATION
        assert raw_entry.key_epoch is not None
        assert raw_entry.key_epoch.startswith("hdcache-")
        assert raw_entry.stored_digest == hashlib.sha256(b"sealed:" + raw_source.read_bytes()).digest()
        assert private_entry.state == "present"
        assert private_entry.representation == AEAD_REPRESENTATION
        assert private_entry.key_epoch is not None
        assert private_entry.key_epoch.startswith("hdcache-")
        assert private_entry.key_epoch != retired_epoch
        assert private_entry.stored_digest == hashlib.sha256(
            b"sealed:" + private_source.read_bytes()
        ).digest()


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(OSError(errno.EACCES, "permission denied"), id="oserror"),
        pytest.param(StoreError("delete failed"), id="storeerror"),
    ],
)
def test_mark_entry_lost_preserves_accounting_when_delete_fails(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
) -> None:
    data = b"stale cache bytes"
    digest = hashlib.sha256(data).digest()

    def fail_delete(*_args: Any, **_kwargs: Any) -> bool:
        raise failure

    monkeypatch.setattr("sutradhara.hdcache.fill.delete_entry", fail_delete)

    with session_scope(engine) as session:
        entry = _seed_present_cache_entry(session, tmp_path / "d001", digest, len(data))

        with pytest.raises(type(failure)):
            mark_entry_lost_and_delete(
                session,
                entry,
                deadline_monotonic=time.monotonic() + 1.0,
            )

        assert session.get(CacheDisk, "d001").filled_bytes == len(data)
        assert session.get(CacheEntry, digest).state == "present"


@pytest.mark.parametrize("file_present", [True, False])
def test_mark_entry_lost_releases_accounting_when_deleted_or_absent(
    engine: Engine,
    tmp_path: Path,
    file_present: bool,
) -> None:
    data = b"stale cache bytes"
    digest = hashlib.sha256(data).digest()

    with session_scope(engine) as session:
        entry = _seed_present_cache_entry(session, tmp_path / "d001", digest, len(data))
        path = entry_path(tmp_path / "d001", digest, representation=RAW_REPRESENTATION)
        if file_present:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)

        mark_entry_lost_and_delete(
            session,
            entry,
            deadline_monotonic=time.monotonic() + 1.0,
        )

        assert session.get(CacheDisk, "d001").filled_bytes == 0
        assert session.get(CacheEntry, digest).state == "lost"
        assert not path.exists()


def test_hung_lost_mark_delete_returns_within_deadline_and_preserves_accounting(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = b"stale private cache bytes"
    delete_started = threading.Event()
    release_delete = threading.Event()
    original_unlink = Path.unlink

    with session_scope(engine) as session:
        target = _seed_archived_asset(
            session,
            data=data,
            artifactclass="private-timeout",
            hdcache_config={"enabled": True, "privacy_level": "p2"},
        )
        _seed_present_cache_entry(
            session,
            tmp_path / "d-timeout",
            target.content_sha256,
            len(data),
            disk_id="d-timeout",
            artifactclass="private-timeout",
        )
        path = entry_path(tmp_path / "d-timeout", target.content_sha256, representation=RAW_REPRESENTATION)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

        def blocking_unlink(path_obj: Path, *args: Any, **kwargs: Any) -> None:
            if path_obj == path:
                delete_started.set()
                release_delete.wait()
            return original_unlink(path_obj, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", blocking_unlink)

        started = time.monotonic()
        try:
            desired, observed = observe_target(
                session,
                target.sha_hex,
                mutate=True,
                config=HdcacheFillConfig(
                    scratch_root=tmp_path / "scratch",
                    hmac_secret=TEST_HDCACHE_HMAC_SECRET,
                    identity_probe=FillIdentityProbe(),
                    delete_deadline_seconds=0.05,
                ),
            )
            elapsed = time.monotonic() - started

            condition = session.scalar(
                select(ReconciliationCondition).where(
                    ReconciliationCondition.domain == DOMAIN,
                    ReconciliationCondition.target_key == target.sha_hex,
                )
            )
            assert desired is True
            assert observed == OBSERVED_MISSING
            assert elapsed < 0.5
            assert delete_started.is_set()
            assert session.get(CacheDisk, "d-timeout").state == "absent"
            assert session.get(CacheDisk, "d-timeout").filled_bytes == len(data)
            assert session.get(CacheEntry, target.content_sha256).state == "present"
            assert condition is not None
            assert condition.reason == "disk-delete-timeout"
        finally:
            release_delete.set()


def test_present_entry_on_absent_disk_is_not_lost_or_replaced(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"absent cache bytes"
    with session_scope(engine) as session:
        target = _seed_archived_asset(session, data=data)
        entry = _seed_present_cache_entry(session, tmp_path / "d001", target.content_sha256, len(data))
        disk = session.get(CacheDisk, "d001")
        disk.state = "absent"
        session.flush([disk])

        desired, observed = observe_target(session, target.sha_hex, mutate=True)

        assert desired is True
        assert observed == OBSERVED_PRESENT
        assert session.get(CacheEntry, target.content_sha256).state == "present"
        assert session.get(CacheDisk, "d001").filled_bytes == len(data)

        with pytest.raises(HdcacheFillBlocked) as excinfo:
            fill_target(session, target, config=_config(tmp_path))

        assert excinfo.value.reason == "disk-unavailable"
        assert session.get(CacheEntry, target.content_sha256).state == "present"
        assert session.get(CacheEntry, target.content_sha256).disk_id == entry.disk_id
        assert session.get(CacheDisk, "d001").filled_bytes == len(data)


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
    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

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


class FillIdentityProbe:
    def observe(self, mount: Path) -> ObservedBlockIdentity:
        disk_id = mount.name
        return ObservedBlockIdentity(
            True,
            serial=f"SER-{disk_id}",
            fs_uuid=f"fs-{disk_id}",
        )


def _assert_live_cap(session: Any, cap: int) -> None:
    assert count_live_hdcache_jobs(session) <= cap


def _hdcache_jobs(session: Any, *, status: JobStatus | None = None) -> list[Job]:
    query = select(Job).where(Job.kind == JOB_KIND)
    if status is not None:
        query = query.where(Job.status == status)
    return list(session.scalars(query.order_by(Job.id)))


def _config(tmp_path: Path) -> HdcacheFillConfig:
    return HdcacheFillConfig(
        scratch_root=tmp_path / "scratch",
        hmac_secret=TEST_HDCACHE_HMAC_SECRET,
        identity_probe=FillIdentityProbe(),
    )


def _add_disk(
    session: Any,
    disk_id: str,
    mount: Path,
    *,
    state: str = "active",
) -> None:
    mount.mkdir(parents=True, exist_ok=True)
    write_disk_sentinel(
        mount,
        ExpectedDiskIdentity(disk_id, f"SER-{disk_id}", f"fs-{disk_id}"),
        hmac_secret=TEST_HDCACHE_HMAC_SECRET,
    )
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


def _seed_present_cache_entry(
    session: Any,
    mount: Path,
    digest: bytes,
    size_bytes: int,
    *,
    disk_id: str = "d001",
    artifactclass: str = "s-masters",
) -> CacheEntry:
    session.merge(LogicalAsset(content_sha256=digest, size_bytes=size_bytes))
    _add_disk(session, disk_id, mount)
    disk = session.get(CacheDisk, disk_id)
    disk.filled_bytes = size_bytes
    entry = CacheEntry(
        content_sha256=digest,
        artifactclass=artifactclass,
        disk_id=disk_id,
        relpath=f"{digest.hex()[:2]}/{digest.hex()}",
        size_bytes=size_bytes,
        state="present",
        representation=RAW_REPRESENTATION,
        trusted=True,
    )
    session.add(entry)
    session.flush()
    return entry


def _seed_archived_backlog(
    session: Any,
    tmp_path: Path,
    *,
    count: int,
    artifactclass: str = "s-masters",
) -> list[HdcacheFillTarget]:
    source_root = tmp_path / "backlog-sources"
    source_root.mkdir(parents=True, exist_ok=True)
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
                hdcache_config={"enabled": True, "privacy_level": "none"},
            )
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
    session.flush()

    copies: list[tuple[Copy, bytes, str, int]] = []
    targets: list[HdcacheFillTarget] = []
    for index in range(count):
        data = f"asset-{index}".encode("ascii")
        digest = hashlib.sha256(data).digest()
        source_path = source_root / f"{index}.mov"
        source_path.write_bytes(data)
        bundle_id = f"bundle-backlog-{index}"
        session.add(LogicalAsset(content_sha256=digest, size_bytes=len(data)))
        session.add(
            Bundle(
                id=bundle_id,
                artifactclass=artifactclass,
                status="sealed",
                target_bytes=1024,
                max_age_seconds=3600,
            )
        )
        session.add(
            BundleMember(
                bundle_id=bundle_id,
                logical_asset_hash=digest,
                member_path=f"{digest.hex()}.mov",
                source_path=str(source_path),
                size_bytes=len(data),
                file_sha256=digest,
            )
        )
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
        copies.append((copy, digest, bundle_id, len(data)))
        targets.append(
            HdcacheFillTarget(
                content_sha256=digest,
                artifactclass=artifactclass,
                size_bytes=len(data),
                bundle_key=bundle_id,
                group_key=f"{artifactclass}:test",
                source_path=str(source_path),
            )
        )
    session.flush()
    for copy, digest, bundle_id, size_bytes in copies:
        session.add(
            AssetLocator(
                logical_asset_hash=digest,
                pool_id="mem-pool",
                copy_id=copy.id,
                bundle_id=bundle_id,
                native_locator={
                    "member_path": f"{digest.hex()}.mov",
                    "offset": 0,
                    "size_bytes": size_bytes,
                },
                member_path=f"{digest.hex()}.mov",
                representation=Representation.RAW_BYTES.value,
            )
        )
    session.flush()
    return targets


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
