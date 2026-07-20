"""Job engine tests — submit, dispatch, verify handler, CLI round-trip."""

from __future__ import annotations

import datetime as dt
import hashlib
import io
import json
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner, Result
from sqlalchemy import Engine, select
from sqlalchemy.exc import IntegrityError

import sutradhara.jobs.engine as job_engine
from sutradhara.api.identity import parse_identity
from sutradhara.backend import factory as backend_factory
from sutradhara.backend.memory import MemoryBackend
from sutradhara.backend.port import StorageBackend
from sutradhara.catalog.models import ArtifactClass, Backend, Copy, LogicalAsset, VerifyReceipt
from sutradhara.catalog.session import (
    create_all,
    locator_key,
    make_engine,
    session_scope,
)
from sutradhara.catalog.types import (
    AssetValidity,
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
)
from sutradhara.cli.main import cli
from sutradhara.hdcache.manager import (
    RestoreConfig,
    RestoreDestination,
    RestoreItemSpec,
    admit_restore_request,
)
from sutradhara.hdcache.models import RestoreRequest, RestoreRequestItem
from sutradhara.jobs import handlers as _handlers  # noqa: F401 -- register built-ins
from sutradhara.jobs.config import RetryPolicy, WorkerConfig
from sutradhara.jobs.engine import (
    claim_job_by_id,
    claim_pending,
    reset_orphaned_running_jobs,
    run_one,
    run_pending,
    submit,
)
from sutradhara.jobs.models import Job, JobAttempt, JobStatus, ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    OBSERVED_MISSING,
    record_observation,
)
from sutradhara.jobs.registry import (
    JobContext,
    JobResult,
    register_handler,
    registered_kinds,
)
from sutradhara.jobs.worker import JobWorker
from sutradhara.structured_logs import configure_structured_stdout_logging


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    with session_scope(eng) as session:
        session.add(ArtifactClass(name="s-masters"))
    yield eng
    eng.dispose()


def _aware_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


# -------------------------------------------------------------------------
# submit + dispatch
# -------------------------------------------------------------------------


def test_submit_creates_pending_job(engine: Engine) -> None:
    with session_scope(engine) as s:
        job = submit(s, "verify", {"copy_id": 1})
        assert job.id is not None
        assert job.status == JobStatus.PENDING
        assert job.kind == "verify"
        assert job.params == {"copy_id": 1}
        assert job.attempts == 0
        assert job.not_before == job.created_at
        assert job.priority == 0


def test_claim_pending_returns_oldest_first(engine: Engine) -> None:
    with session_scope(engine) as s:
        j1 = submit(s, "verify", {"copy_id": 1})
        j2 = submit(s, "verify", {"copy_id": 2})
        first = claim_pending(s)
        assert first is not None
        assert first.id == j1.id
        # Mark first running so claim returns the next one.
        first.status = JobStatus.RUNNING
        s.flush()
        second = claim_pending(s)
        assert second is not None
        assert second.id == j2.id


def test_claim_pending_returns_none_when_empty(engine: Engine) -> None:
    with session_scope(engine) as s:
        assert claim_pending(s) is None


def test_atomic_claim_flips_running_and_second_claim_cannot_regrab(engine: Engine) -> None:
    with session_scope(engine) as s:
        job = submit(s, "verify", {"copy_id": 1})
        claimed = claim_job_by_id(s, job.id)
        assert claimed is not None
        assert claimed.status == JobStatus.RUNNING
        assert claimed.attempts == 1
        assert claim_job_by_id(s, job.id) is None


def test_prerequisites_gate_pending_jobs(engine: Engine) -> None:
    with session_scope(engine) as s:
        prereq = submit(s, "verify", {"copy_id": 1})
        child = submit(s, "verify", {"copy_id": 2}, prerequisites=[prereq.id])

        assert claim_pending(s) is not None
        assert claim_pending(s) is None
        prereq.status = JobStatus.SUCCEEDED
        s.flush()

        claimed = claim_pending(s)
        assert claimed is not None
        assert claimed.id == child.id


@pytest.mark.parametrize(
    "live_status",
    [JobStatus.PENDING, JobStatus.RUNNING, JobStatus.QUEUED],
)
def test_submit_is_idempotent_for_live_dedupe_key(
    engine: Engine,
    live_status: JobStatus,
) -> None:
    with session_scope(engine) as s:
        dedupe_key = f"verify:{live_status.value}"
        first = submit(s, "verify", {"copy_id": 1}, dedupe_key=dedupe_key)
        first.status = live_status
        s.flush()

        second = submit(s, "verify", {"copy_id": 2}, dedupe_key=dedupe_key)

        assert second.id == first.id
        assert second.params == {"copy_id": 1}


@pytest.mark.parametrize("terminal_status", [JobStatus.FAILED, JobStatus.SUCCEEDED])
def test_submit_ignores_terminal_jobs_for_dedupe_key(
    engine: Engine,
    terminal_status: JobStatus,
) -> None:
    with session_scope(engine) as s:
        first = submit(s, "verify", {"copy_id": 1}, dedupe_key="verify:terminal")
        first.status = terminal_status
        first.finished_at = dt.datetime.now(dt.UTC)
        s.flush()

        second = submit(s, "verify", {"copy_id": 2}, dedupe_key="verify:terminal")

        assert second.id != first.id
        assert second.status == JobStatus.PENDING
        assert second.params == {"copy_id": 2}
        rows = list(
            s.scalars(select(Job).where(Job.dedupe_key == "verify:terminal").order_by(Job.id))
        )
        assert [row.id for row in rows] == [first.id, second.id]


@pytest.mark.parametrize(
    "live_status",
    [JobStatus.PENDING, JobStatus.RUNNING, JobStatus.QUEUED],
)
def test_live_dedupe_key_unique_index_blocks_direct_duplicate_insert(
    engine: Engine,
    live_status: JobStatus,
) -> None:
    def insert_duplicate_live_job() -> None:
        with session_scope(engine) as s:
            dedupe_key = f"verify:race:{live_status.value}"
            submit(s, "verify", {"copy_id": 1}, dedupe_key=dedupe_key)
            s.add(
                Job(
                    kind="verify",
                    params={"copy_id": 2},
                    status=live_status,
                    dedupe_key=dedupe_key,
                )
            )
            s.flush()

    with pytest.raises(IntegrityError):
        insert_duplicate_live_job()


def test_submit_dedupe_integrity_error_returns_existing_job(
    engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with session_scope(engine) as s:
        first = submit(s, "verify", {"copy_id": 1}, dedupe_key="verify:race-requery")
        real_lookup = job_engine._live_job_for_dedupe
        calls = {"count": 0}

        def stale_fast_path(session: object, dedupe_key: str) -> Job | None:
            calls["count"] += 1
            if calls["count"] == 1:
                return None
            return real_lookup(session, dedupe_key)

        monkeypatch.setattr(job_engine, "_live_job_for_dedupe", stale_fast_path)

        second = submit(s, "verify", {"copy_id": 2}, dedupe_key="verify:race-requery")

        assert second.id == first.id
        assert second.params == {"copy_id": 1}
        assert calls["count"] >= 2


def test_run_unknown_kind_marks_failed(engine: Engine) -> None:
    with session_scope(engine) as s:
        job = submit(s, "nonexistent_kind", {})

    with session_scope(engine) as s:
        result = run_one(s, job.id)
        assert not result.ok
        assert "no handler registered" in result.detail
        fetched = s.get(Job, job.id)
        assert fetched is not None
        assert fetched.status == JobStatus.FAILED
        assert fetched.attempts == 1
        assert fetched.last_error is not None
        assert "no handler registered" in fetched.last_error


def test_run_pending_bad_kind_does_not_block_later_jobs(engine: Engine) -> None:
    @register_handler("_test_after_bad_kind")
    def _ok(_ctx: JobContext) -> JobResult:
        return JobResult(ok=True, detail="valid ran")

    try:
        with session_scope(engine) as s:
            bad = submit(s, "nonexistent_kind", {})
            good = submit(s, "_test_after_bad_kind", {})

        with session_scope(engine) as s:
            results = run_pending(s, limit=0)
            assert [jid for jid, _ in results] == [bad.id, good.id]
            assert [r.ok for _, r in results] == [False, True]

            bad_row = s.get(Job, bad.id)
            good_row = s.get(Job, good.id)
            assert bad_row is not None
            assert good_row is not None
            assert bad_row.status == JobStatus.FAILED
            assert good_row.status == JobStatus.SUCCEEDED
    finally:
        from sutradhara.jobs import registry as _r

        _r._HANDLERS.pop("_test_after_bad_kind", None)


def test_handler_exception_marks_failed_and_captures_traceback(
    engine: Engine,
) -> None:
    """A handler that raises must leave the job in FAILED with last_error set."""

    @register_handler("_test_raises")
    def _raises(_ctx: JobContext) -> JobResult:
        raise RuntimeError("intentional test failure")

    try:
        with session_scope(engine) as s:
            job = submit(s, "_test_raises", {})
            result = run_one(s, job.id)
            assert not result.ok
            assert "intentional test failure" in result.detail
            assert job.status == JobStatus.FAILED
            assert job.attempts == 1
            assert job.last_error is not None
            assert "RuntimeError" in job.last_error
            assert job.finished_at is not None
            assert job.started_at is not None
    finally:
        registered_kinds()  # touch
        # Best-effort cleanup of the test-only handler so other tests
        # don't see leakage across runs in the same process.
        from sutradhara.jobs import registry as _r

        _r._HANDLERS.pop("_test_raises", None)


def test_run_one_on_terminal_job_raises(engine: Engine) -> None:
    """Re-running a SUCCEEDED job is rejected — terminal states are sticky."""

    @register_handler("_test_succeeds")
    def _ok(_ctx: JobContext) -> JobResult:
        return JobResult(ok=True, detail="ok")

    try:
        with session_scope(engine) as s:
            job = submit(s, "_test_succeeds", {})
            run_one(s, job.id)
            assert job.status == JobStatus.SUCCEEDED
            with pytest.raises(ValueError, match="terminal status"):
                run_one(s, job.id)
    finally:
        from sutradhara.jobs import registry as _r

        _r._HANDLERS.pop("_test_succeeds", None)


def test_run_pending_drains_queue(engine: Engine) -> None:
    """`limit=0` runs everything currently pending."""

    counter = {"n": 0}

    @register_handler("_test_counter")
    def _counter(_ctx: JobContext) -> JobResult:
        counter["n"] += 1
        return JobResult(ok=True)

    try:
        with session_scope(engine) as s:
            for i in range(3):
                submit(s, "_test_counter", {"i": i})

        with session_scope(engine) as s:
            results = run_pending(s, limit=0)
            assert len(results) == 3
            assert all(r.ok for _, r in results)
            assert counter["n"] == 3
    finally:
        from sutradhara.jobs import registry as _r

        _r._HANDLERS.pop("_test_counter", None)


def _worker_engine(tmp_path: Path) -> Engine:
    db_path = tmp_path / f"worker-{time.time_ns()}.db"
    eng = make_engine(f"sqlite:///{db_path}")
    create_all(eng)
    return eng


def test_worker_emits_structured_job_entity_logs(tmp_path: Path) -> None:
    @register_handler("_test_structured_log")
    def _structured_log(ctx: JobContext) -> JobResult:
        assert ctx.job.kind == "_test_structured_log"
        return JobResult(ok=True)

    try:
        stream = io.StringIO()
        configure_structured_stdout_logging(stream)
        eng = _worker_engine(tmp_path)
        with session_scope(eng) as s:
            job = submit(s, "_test_structured_log", {}, required_resources=[])
            job_id = job.id

        JobWorker(eng).drain()

        events = [json.loads(line) for line in stream.getvalue().splitlines()]
        started = next(event for event in events if event["event"] == "sutradhara.job.started")
        finished = next(event for event in events if event["event"] == "sutradhara.job.finished")
        expected_ref = {"kind": "job", "id": str(job_id), "confidence": "high"}
        assert started["job_id"] == job_id
        assert started["job_kind"] == "_test_structured_log"
        assert started["entity_refs"] == [expected_ref]
        assert finished["job_status"] == "succeeded"
        assert finished["outcome"] == "ok"
        assert finished["entity_refs"] == [expected_ref]
        eng.dispose()
    finally:
        from sutradhara.jobs import registry as _r

        _r._HANDLERS.pop("_test_structured_log", None)


def test_worker_enforces_cpu_and_io_lease_caps(tmp_path: Path) -> None:
    state = {"active": 0, "max_active": 0}
    lock = threading.Lock()

    @register_handler("_test_lease_sleep")
    def _lease_sleep(ctx: JobContext) -> JobResult:
        assert ctx.granted_leases
        with lock:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
        time.sleep(0.05)
        with lock:
            state["active"] -= 1
        return JobResult(ok=True)

    try:
        eng = _worker_engine(tmp_path)
        with session_scope(eng) as s:
            for _index in range(4):
                submit(
                    s,
                    "_test_lease_sleep",
                    {},
                    required_resources=[{"pool": "cpu", "count": 8}],
                )
        worker = JobWorker(
            eng,
            config=WorkerConfig.defaults().with_pool_overrides(
                {"cpu": 24, "io": 2, "tape_drive": 0, "gpu": 0}
            ),
        )
        worker.drain()
        assert state["max_active"] == 3
        eng.dispose()

        state.update({"active": 0, "max_active": 0})
        eng = _worker_engine(tmp_path)
        with session_scope(eng) as s:
            for _index in range(3):
                submit(
                    s,
                    "_test_lease_sleep",
                    {},
                    required_resources=[{"pool": "cpu", "count": 8}],
                )
        worker = JobWorker(
            eng,
            config=WorkerConfig.defaults().with_pool_overrides(
                {"cpu": 16, "io": 2, "tape_drive": 0, "gpu": 0}
            ),
        )
        worker.drain()
        assert state["max_active"] == 2
        eng.dispose()

        state.update({"active": 0, "max_active": 0})
        eng = _worker_engine(tmp_path)
        with session_scope(eng) as s:
            for _index in range(3):
                submit(
                    s,
                    "_test_lease_sleep",
                    {},
                    required_resources=[{"pool": "io", "count": 1}],
                )
        worker = JobWorker(
            eng,
            config=WorkerConfig.defaults().with_pool_overrides(
                {"cpu": 24, "io": 1, "tape_drive": 0, "gpu": 0}
            ),
        )
        worker.drain()
        assert state["max_active"] == 1
        eng.dispose()
    finally:
        from sutradhara.jobs import registry as _r

        _r._HANDLERS.pop("_test_lease_sleep", None)


def test_worker_claim_available_is_bounded_by_free_slot_budget(tmp_path: Path) -> None:
    @register_handler("_test_claim_bound")
    def _claim_bound(_ctx: JobContext) -> JobResult:
        return JobResult(ok=True)

    try:
        eng = _worker_engine(tmp_path)
        with session_scope(eng) as s:
            for _index in range(5):
                submit(s, "_test_claim_bound", {})

        worker = JobWorker(
            eng,
            config=WorkerConfig(
                capacities={"cpu": 4, "io": 2, "tape_drive": 0, "gpu": 0},
                executor_workers=2,
            ),
        )
        claimed = worker._claim_available(max_new=2)

        assert len(claimed) == 2
        with session_scope(eng) as s:
            assert len(list(s.scalars(select(Job).where(Job.status == JobStatus.RUNNING)))) == 2
            assert len(list(s.scalars(select(Job).where(Job.status == JobStatus.PENDING)))) == 3
        eng.dispose()
    finally:
        from sutradhara.jobs import registry as _r

        _r._HANDLERS.pop("_test_claim_bound", None)


def test_worker_never_fit_records_attempt_and_reconciler_condition(tmp_path: Path) -> None:
    eng = _worker_engine(tmp_path)
    target_key = "asset:" + "f" * 64 + ":pool-a"
    with session_scope(eng) as s:
        record_observation(
            s,
            domain="copy",
            target_key=target_key,
            desired=True,
            observed_state=OBSERVED_MISSING,
        )
        job = submit(
            s,
            "missing_handler_never_runs",
            {},
            required_resources=[{"pool": "gpu", "count": 1}],
            recon_domain="copy",
            recon_target_key=target_key,
        )

    worker = JobWorker(
        eng,
        config=WorkerConfig(
            capacities={"cpu": 4, "io": 2, "tape_drive": 0, "gpu": 0},
            executor_workers=2,
        ),
    )
    assert worker._claim_available(max_new=1) == []

    with session_scope(eng) as s:
        row = s.get(Job, job.id)
        assert row is not None
        assert row.status == JobStatus.FAILED
        assert row.attempts == 1
        assert "exceed worker capacities" in (row.last_error or "")

        attempt = s.scalars(select(JobAttempt).where(JobAttempt.job_id == job.id)).one()
        assert attempt.outcome == JobStatus.FAILED
        assert attempt.started_at == attempt.finished_at

        condition = s.scalars(select(ReconciliationCondition)).one()
        assert condition.condition == CONDITION_BACKOFF
        assert condition.reason == "never-fit"
        assert condition.last_attempt_id == attempt.id
    eng.dispose()


def test_worker_is_work_conserving_but_aging_blocks_starvation(tmp_path: Path) -> None:
    order: list[str] = []
    lock = threading.Lock()

    @register_handler("_test_aging")
    def _aging(ctx: JobContext) -> JobResult:
        name = str(ctx.job.params["name"])
        with lock:
            order.append(name)
        time.sleep(float(ctx.job.params.get("sleep", 0.01)))
        return JobResult(ok=True)

    try:
        eng = _worker_engine(tmp_path)
        with session_scope(eng) as s:
            submit(
                s,
                "_test_aging",
                {"name": "holder", "sleep": 0.2},
                required_resources=[{"pool": "cpu", "count": 4}],
            )
            submit(
                s,
                "_test_aging",
                {"name": "big"},
                required_resources=[{"pool": "cpu", "count": 8}],
            )
            submit(
                s,
                "_test_aging",
                {"name": "small-1"},
                required_resources=[{"pool": "cpu", "count": 1}],
            )
            submit(
                s,
                "_test_aging",
                {"name": "small-2"},
                required_resources=[{"pool": "cpu", "count": 1}],
            )

        config = WorkerConfig.defaults().with_pool_overrides(
            {"cpu": 8, "io": 2, "tape_drive": 0, "gpu": 0}
        )
        config = WorkerConfig(
            capacities=config.capacities,
            retry=config.retry,
            per_kind_retry=config.per_kind_retry,
            aging_threshold_scans=2,
            executor_workers=8,
        )
        JobWorker(eng, config=config).drain()

        assert order.index("small-1") < order.index("big")
        assert order.index("big") < order.index("small-2")
    finally:
        from sutradhara.jobs import registry as _r

        _r._HANDLERS.pop("_test_aging", None)


def test_worker_retries_with_backoff_then_fails(tmp_path: Path) -> None:
    @register_handler("_test_retry_fails")
    def _retry_fails(_ctx: JobContext) -> JobResult:
        return JobResult(ok=False, detail="try again")

    try:
        eng = _worker_engine(tmp_path)
        with session_scope(eng) as s:
            job = submit(s, "_test_retry_fails", {})
        config = WorkerConfig(
            capacities={"cpu": 4, "io": 2, "tape_drive": 0, "gpu": 0},
            retry=RetryPolicy(max_attempts=2, backoff_seconds=60),
            executor_workers=4,
        )
        JobWorker(eng, config=config).drain()
        with session_scope(eng) as s:
            row = s.get(Job, job.id)
            assert row is not None
            assert row.status == JobStatus.PENDING
            assert row.attempts == 1
            assert _aware_utc(row.not_before) > dt.datetime.now(dt.UTC)
            row.not_before = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1)

        JobWorker(eng, config=config).drain(recover_orphans=False)
        with session_scope(eng) as s:
            row = s.get(Job, job.id)
            assert row is not None
            assert row.status == JobStatus.FAILED
            assert row.attempts == 2
    finally:
        from sutradhara.jobs import registry as _r

        _r._HANDLERS.pop("_test_retry_fails", None)


def test_retry_policy_delay_has_jitter_and_clamp_bounds() -> None:
    retry = RetryPolicy(max_attempts=5, backoff_seconds=100)
    base = 400
    samples = [retry.delay_seconds(3, max_seconds=450) for _ in range(50)]

    assert all(base * 0.8 <= sample <= 450 for sample in samples)
    assert all(sample <= 450 for sample in samples)
    assert retry.delay_seconds(0, max_seconds=450) == 0


def test_worker_startup_resets_orphaned_running_jobs(engine: Engine) -> None:
    with session_scope(engine) as s:
        job = submit(s, "verify", {"copy_id": 1})
        job.status = JobStatus.RUNNING
        job.started_at = dt.datetime.now(dt.UTC)

    with session_scope(engine) as s:
        assert reset_orphaned_running_jobs(s) == 1
        row = s.get(Job, job.id)
        assert row is not None
        assert row.status == JobStatus.PENDING
        assert row.started_at is None
        assert row.last_error is not None
        assert "orphaned RUNNING at startup, reset to PENDING at" in row.last_error
        assert row.step_state["engine_observations"] == [{"note": row.last_error}]


def test_validate_handler_marks_clean_and_decode_invalid_assets(
    engine: Engine,
    tmp_path: Path,
) -> None:
    clean = tmp_path / "clean.txt"
    clean.write_text("valid text", encoding="utf-8")
    invalid = tmp_path / "invalid.bin"
    invalid.write_bytes(b"\xff\xfe\xfa")
    clean_hash = _register_asset(engine, clean.read_bytes())
    invalid_hash = _register_asset(engine, invalid.read_bytes())

    with session_scope(engine) as s:
        clean_job = submit(
            s,
            "validate",
            {"asset_hash": clean_hash.hex(), "path": str(clean), "validator": "utf-8"},
        )
        invalid_job = submit(
            s,
            "validate",
            {
                "asset_hash": invalid_hash.hex(),
                "path": str(invalid),
                "validator": "utf-8",
            },
        )
        clean_result = run_one(s, clean_job.id)
        invalid_result = run_one(s, invalid_job.id)

        assert clean_result.ok
        assert invalid_result.ok
        clean_asset = s.get(LogicalAsset, clean_hash)
        invalid_asset = s.get(LogicalAsset, invalid_hash)
        assert clean_asset is not None
        assert invalid_asset is not None
        assert clean_asset.validity == AssetValidity.OK
        assert invalid_asset.validity == AssetValidity.SUSPECT
        assert invalid_asset.validity_note is not None
        assert "decode error" in invalid_asset.validity_note


def test_validate_handler_read_error_does_not_mark_suspect(
    engine: Engine,
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing.txt"
    asset_hash = _register_asset(engine, b"not read")

    with session_scope(engine) as s:
        job = submit(
            s,
            "validate",
            {"asset_hash": asset_hash.hex(), "path": str(missing), "validator": "utf-8"},
        )
        result = run_one(s, job.id)
        asset = s.get(LogicalAsset, asset_hash)
        assert not result.ok
        assert "read error" in result.detail
        assert asset is not None
        assert asset.validity == AssetValidity.UNVALIDATED


# -------------------------------------------------------------------------
# dispatch_write_to_tape + copy handler
# -------------------------------------------------------------------------


def _register_tape_backend(engine: Engine, name: str = "tape-1") -> None:
    with session_scope(engine) as s:
        s.add(
            Backend(
                name=name,
                kind=BackendKind.REM_TAPE,
                tier=BackendTier.SELF_DESCRIBING,
            )
        )


def _register_asset(engine: Engine, content: bytes) -> bytes:
    h = hashlib.sha256(content).digest()
    with session_scope(engine) as s:
        if s.get(LogicalAsset, h) is None:
            s.add(LogicalAsset(content_sha256=h, size_bytes=len(content)))
    return h


def test_copy_kind_is_registered() -> None:
    assert "copy" in registered_kinds()


def test_copy_handler_fails_loudly_does_not_fake_success(engine: Engine) -> None:
    """A copy job must FAIL (not SUCCEED) — no bytes are actually moved yet."""
    with session_scope(engine) as s:
        job = submit(s, "copy", {"asset_hash": "ab" * 32, "target_backend": "tape-1"})
        result = run_one(s, job.id)
        assert not result.ok
        assert job.status == JobStatus.FAILED
        assert job.last_error is not None
        assert "not implemented" in job.last_error
        assert "NotImplementedError" in job.last_error


def test_dispatch_write_to_tape_creates_pending_copy_job(engine: Engine) -> None:
    from sutradhara.jobs.dispatch import dispatch_write_to_tape

    _register_tape_backend(engine, "tape-1")
    h = _register_asset(engine, b"to-be-archived")

    with session_scope(engine) as s:
        handle = dispatch_write_to_tape(s, h)

    assert handle["kind"] == "copy"
    assert handle["target_backend"] == "tape-1"
    assert handle["params"] == {"asset_hash": h.hex(), "target_backend": "tape-1"}

    with session_scope(engine) as s:
        job = s.get(Job, handle["job_id"])
        assert job is not None
        assert job.status == JobStatus.PENDING  # dispatch does NOT run the job
        assert job.kind == "copy"
        assert job.params["asset_hash"] == h.hex()


def test_dispatch_write_to_tape_uses_explicit_target_backend(engine: Engine) -> None:
    from sutradhara.jobs.dispatch import dispatch_write_to_tape

    _register_tape_backend(engine, "tape-a")
    _register_tape_backend(engine, "tape-b")
    h = _register_asset(engine, b"explicit-library")

    with session_scope(engine) as s:
        handle = dispatch_write_to_tape(s, h, target_backend="tape-b")

    assert handle["target_backend"] == "tape-b"
    assert handle["params"] == {"asset_hash": h.hex(), "target_backend": "tape-b"}


def test_dispatch_requires_explicit_target_for_multiple_tape_backends(
    engine: Engine,
) -> None:
    from sutradhara.jobs.dispatch import AmbiguousBackend, dispatch_write_to_tape

    _register_tape_backend(engine, "tape-a")
    _register_tape_backend(engine, "tape-b")
    h = _register_asset(engine, b"ambiguous-library")

    with session_scope(engine) as s, pytest.raises(AmbiguousBackend) as excinfo:
        dispatch_write_to_tape(s, h)

    message = str(excinfo.value)
    assert "tape-a" in message
    assert "tape-b" in message
    assert "target_backend" in message


def test_dispatch_explicit_target_must_exist(engine: Engine) -> None:
    from sutradhara.jobs.dispatch import NoEligibleBackend, dispatch_write_to_tape

    h = _register_asset(engine, b"missing-target")
    with session_scope(engine) as s, pytest.raises(NoEligibleBackend, match="not registered"):
        dispatch_write_to_tape(s, h, target_backend="missing-tape")


def test_dispatch_explicit_target_must_be_rem_tape(engine: Engine) -> None:
    from sutradhara.jobs.dispatch import NoEligibleBackend, dispatch_write_to_tape

    with session_scope(engine) as s:
        s.add(Backend(name="mem", kind=BackendKind.MEMORY, tier=BackendTier.SELF_DESCRIBING))
    h = _register_asset(engine, b"wrong-kind-target")

    with session_scope(engine) as s, pytest.raises(NoEligibleBackend, match="rem_tape"):
        dispatch_write_to_tape(s, h, target_backend="mem")


def test_dispatch_picks_a_rem_tape_backend_even_among_others(engine: Engine) -> None:
    """Dispatch must target a rem_tape backend, ignoring memory/other kinds."""
    from sutradhara.jobs.dispatch import dispatch_write_to_tape

    with session_scope(engine) as s:
        s.add(Backend(name="mem", kind=BackendKind.MEMORY, tier=BackendTier.SELF_DESCRIBING))
        s.add(Backend(name="tape-x", kind=BackendKind.REM_TAPE, tier=BackendTier.SELF_DESCRIBING))
    h = _register_asset(engine, b"pick-the-tape")

    with session_scope(engine) as s:
        handle = dispatch_write_to_tape(s, h)
    assert handle["target_backend"] == "tape-x"


def test_dispatch_fails_when_no_tape_backend_registered(engine: Engine) -> None:
    from sutradhara.jobs.dispatch import NoEligibleBackend, dispatch_write_to_tape

    h = _register_asset(engine, b"no-backend")
    with session_scope(engine) as s, pytest.raises(NoEligibleBackend, match="no rem_tape backend"):
        dispatch_write_to_tape(s, h)


def test_dispatch_fails_when_asset_not_in_catalog(engine: Engine) -> None:
    from sutradhara.jobs.dispatch import AssetNotInCatalog, dispatch_write_to_tape

    _register_tape_backend(engine, "tape-1")
    phantom = hashlib.sha256(b"never-registered").digest()
    with session_scope(engine) as s, pytest.raises(AssetNotInCatalog, match="no LogicalAsset"):
        dispatch_write_to_tape(s, phantom)


def test_dispatch_rejects_non_content_hash(engine: Engine) -> None:
    from sutradhara.jobs.dispatch import dispatch_write_to_tape

    _register_tape_backend(engine, "tape-1")
    with session_scope(engine) as s, pytest.raises(ValueError, match="32-byte"):
        dispatch_write_to_tape(s, b"too-short")


# -------------------------------------------------------------------------
# verify handler — integration with catalog + MemoryBackend
# -------------------------------------------------------------------------


def _seed_memory_backend(
    engine: Engine,
    content: bytes,
    monkeypatch: pytest.MonkeyPatch,
    *,
    kind: BackendKind = BackendKind.MEMORY,
    locator_extra: dict[str, object] | None = None,
) -> tuple[int, MemoryBackend]:
    """Insert a backend row + an asset/copy row pointing into a MemoryBackend.

    Because the `backend_from_row` factory builds a fresh MemoryBackend per
    call (it has no way to know about pre-seeded test instances), this helper
    also monkeypatches the factory to return the seeded backend.

    Returns (copy_id, in-memory backend instance).
    """
    backend_impl = MemoryBackend("test-mem")
    h = backend_impl.add(content)

    locator = {"hash_hex": h.hex(), **(locator_extra or {})}
    with session_scope(engine) as s:
        backend_row = Backend(
            name="test-mem",
            kind=kind,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(backend_row)
        s.add(LogicalAsset(content_sha256=h, size_bytes=len(content)))
        s.flush()
        copy = Copy(
            logical_asset_hash=h,
            backend_id=backend_row.id,
            native_locator=locator,
            native_locator_key=locator_key(locator),
            integrity_hash=h,
            health=CopyHealth.OK,
            source=CopySource.INGEST,
        )
        s.add(copy)
        s.flush()
        copy_id = copy.id

    monkeypatch.setattr(
        backend_factory,
        "backend_from_row",
        lambda row: backend_impl if row.name == "test-mem" else None,
    )
    return copy_id, backend_impl


def test_verify_happy_marks_copy_ok_and_records_timestamp(
    engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    copy_id, _ = _seed_memory_backend(engine, b"verifiable bytes", monkeypatch)

    with session_scope(engine) as s:
        job = submit(s, "verify", {"copy_id": copy_id})
        result = run_one(s, job.id)
        assert result.ok
        assert result.detail == "verified ok"
        assert job.status == JobStatus.SUCCEEDED

        copy = s.get(Copy, copy_id)
        assert copy is not None
        assert copy.health == CopyHealth.OK
        assert copy.last_checked_at is not None
        assert copy.last_checked_at.tzinfo is dt.UTC

        # step_state captures the verify answer for inspection.
        assert job.step_state["verify_result"]["ok"] is True
        assert job.step_state["copy_health_after"] == "ok"


def test_verify_handler_attempt_records_d2_tape_component(
    engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    barcode = "D2VERIFY01"
    copy_id, _ = _seed_memory_backend(
        engine,
        b"d2 verify bytes",
        monkeypatch,
        kind=BackendKind.D2_TAPE,
        locator_extra={"barcode": barcode},
    )

    with session_scope(engine) as session:
        copy = session.get(Copy, copy_id)
        assert copy is not None
        job = submit(session, "verify", {"copy_id": copy_id})
        assert run_one(session, job.id).ok

        attempt = session.scalars(select(JobAttempt).where(JobAttempt.job_id == job.id)).one()
        assert f"tape:d2tape:{barcode}" in attempt.detail["components"]


def test_verify_detects_corruption_marks_corrupt_and_succeeds(
    engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A measured digest mismatch succeeds as work and marks the copy CORRUPT."""
    from sutradhara.backend.port import (
        BackendLocator,
        ByteRange,
        VerifyResult,
    )
    from sutradhara.catalog.types import content_hash

    h = content_hash(hashlib.sha256(b"original").digest())

    class _AlwaysFailsBackend:
        @property
        def name(self) -> str:
            return "always-fails"

        def enumerate(self) -> Iterator[object]:  # pragma: no cover
            return iter([])

        def read_range(self, _l: BackendLocator, _r: ByteRange) -> bytes:  # pragma: no cover
            raise NotImplementedError

        def verify(self, _l: BackendLocator) -> VerifyResult:
            return VerifyResult(
                ok=False,
                measured=True,
                actual_hash=content_hash(hashlib.sha256(b"tampered").digest()),
                detail="hash mismatch",
            )

    monkeypatch.setattr(
        backend_factory,
        "backend_from_row",
        lambda row: _AlwaysFailsBackend() if row.name == "broken-backend" else None,
    )
    if True:
        with session_scope(engine) as s:
            backend_row = Backend(
                name="broken-backend",
                kind=BackendKind.MEMORY,
                tier=BackendTier.SELF_DESCRIBING,
            )
            s.add(backend_row)
            s.add(LogicalAsset(content_sha256=h, size_bytes=8))
            s.flush()
            locator = {"hash_hex": h.hex()}
            copy = Copy(
                logical_asset_hash=h,
                backend_id=backend_row.id,
                native_locator=locator,
                native_locator_key=locator_key(locator),
                integrity_hash=h,
                health=CopyHealth.OK,
                source=CopySource.INGEST,
            )
            s.add(copy)
            s.flush()
            copy_id = copy.id

        with session_scope(engine) as s:
            job = submit(s, "verify", {"copy_id": copy_id})
            result = run_one(s, job.id)
            # Job machinery succeeded; the bad-integrity outcome lives in catalog.
            assert result.ok
            assert "integrity mismatch" in result.detail
            assert job.status == JobStatus.SUCCEEDED

            refreshed = s.get(Copy, copy_id)
            assert refreshed is not None
            assert refreshed.health == CopyHealth.CORRUPT
            assert refreshed.last_checked_at is not None
            assert job.step_state["verify_result"]["ok"] is False
            assert job.step_state["copy_health_after"] == "corrupt"


def test_verify_with_missing_copy_id_marks_failed(engine: Engine) -> None:
    with session_scope(engine) as s:
        job = submit(s, "verify", {"copy_id": 99999})
        result = run_one(s, job.id)
        assert not result.ok
        assert "no copy with id" in result.detail
        assert job.status == JobStatus.FAILED
        assert job.last_error is not None
        assert "no copy with id" in job.last_error


def test_verify_with_bad_params_marks_failed(engine: Engine) -> None:
    with session_scope(engine) as s:
        job = submit(s, "verify", {})  # missing copy_id
        result = run_one(s, job.id)
        assert not result.ok
        assert "copy_id" in result.detail
        assert job.status == JobStatus.FAILED


def test_verify_recovers_a_previously_missing_copy(
    engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A copy marked MISSING that now verifies clean should be set back to OK."""
    copy_id, _ = _seed_memory_backend(engine, b"recoverable", monkeypatch)

    with session_scope(engine) as s:
        stale = s.get(Copy, copy_id)
        assert stale is not None
        stale.health = CopyHealth.MISSING

    with session_scope(engine) as s:
        job = submit(s, "verify", {"copy_id": copy_id})
        run_one(s, job.id)
        refreshed = s.get(Copy, copy_id)
        assert refreshed is not None
        assert refreshed.health == CopyHealth.OK


def test_trust_verify_promotion_clears_stale_measurement_and_enqueues_remeasure(
    engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A trust-only success cannot requalify a previously suspect copy."""

    from sutradhara.backend.port import BackendLocator, ByteRange, VerifyResult

    copy_id, _ = _seed_memory_backend(engine, b"trust-only", monkeypatch)

    class _TrustOnlyBackend:
        @property
        def name(self) -> str:
            return "trust-only"

        def enumerate(self) -> Iterator[object]:
            return iter(())

        def read_range(self, _locator: BackendLocator, _range: ByteRange) -> bytes:
            raise AssertionError("trust-only verification must not read bytes")

        def verify(self, _locator: BackendLocator) -> VerifyResult:
            return VerifyResult(ok=True, measured=False, detail="catalog echo")

    monkeypatch.setattr(backend_factory, "backend_from_row", lambda _row: _TrustOnlyBackend())
    old = dt.datetime(2025, 1, 1, tzinfo=dt.UTC)
    with session_scope(engine) as session:
        copy = session.get(Copy, copy_id)
        assert copy is not None
        copy.health = CopyHealth.SUSPECT
        copy.last_measured_digest = copy.integrity_hash
        copy.last_measured_at = old

    with session_scope(engine) as session:
        job = submit(session, "verify", {"copy_id": copy_id})
        assert run_one(session, job.id).ok
        copy = session.get(Copy, copy_id)
        assert copy is not None
        assert copy.health == CopyHealth.OK
        assert copy.last_measured_digest is None
        assert copy.last_measured_at is None
        receipt = session.scalars(
            select(VerifyReceipt).where(VerifyReceipt.copy_id == copy_id)
        ).one()
        assert receipt.source == "verify-job"
        assert receipt.measured_digest is None
        assert receipt.failure_kind == "measurement-invalidated"
        pending = list(
            session.scalars(
                select(Job).where(Job.kind == "verify", Job.status == JobStatus.PENDING)
            )
        )
        assert [row.params for row in pending] == [{"copy_id": copy_id}]


# -------------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------------

FIXTURE = Path(__file__).parent / "fixtures" / "remanence_objects.json"


@pytest.fixture
def cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    db_path = tmp_path / "sutradhara.db"
    monkeypatch.setenv("SUTRADHARA_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.chdir(tmp_path)
    return {"db": str(db_path)}


def _run_cli(args: list[str], expect_exit: int = 0) -> Result:
    runner = CliRunner()
    result = runner.invoke(cli, args)
    if result.exit_code != expect_exit:
        pytest.fail(
            f"CLI args {args!r} exited {result.exit_code}, expected {expect_exit}\n"
            f"output:\n{result.output}"
        )
    return result


def test_jobs_help_lists_subcommands(cli_env: dict[str, str]) -> None:
    result = _run_cli(["jobs", "--help"])
    for sub in ("submit", "list", "show", "run"):
        assert sub in result.output


def test_backends_add_merges_config_and_library_uuid(cli_env: dict[str, str]) -> None:
    _run_cli(["db", "init"])
    _run_cli(
        [
            "backends",
            "add",
            "rem-specific",
            "--kind",
            "rem_tape",
            "--fixture",
            str(FIXTURE),
            "--config",
            "priority=7",
            "--config",
            "enabled=true",
            "--config",
            "label=archive",
            "--library-uuid",
            "library-123",
        ]
    )

    eng = make_engine()
    with session_scope(eng) as s:
        row = s.scalars(select(Backend).where(Backend.name == "rem-specific")).one()
        assert row.config == {
            "priority": 7,
            "enabled": True,
            "label": "archive",
            "fixture_path": str(FIXTURE),
            "library_uuid": "library-123",
        }


def test_backends_add_rejects_colliding_config_keys(cli_env: dict[str, str]) -> None:
    _run_cli(["db", "init"])

    fixture_collision = _run_cli(
        [
            "backends",
            "add",
            "rem-fixture-collision",
            "--kind",
            "rem_tape",
            "--config",
            "fixture_path=elsewhere.json",
            "--fixture",
            str(FIXTURE),
        ],
        expect_exit=2,
    )
    assert "fixture_path" in fixture_collision.output
    assert "overwrite" in fixture_collision.output

    library_collision = _run_cli(
        [
            "backends",
            "add",
            "rem-library-collision",
            "--kind",
            "rem_tape",
            "--config",
            "library_uuid=from-config",
            "--library-uuid",
            "from-flag",
        ],
        expect_exit=2,
    )
    assert "library_uuid" in library_collision.output
    assert "overwrite" in library_collision.output

    config_collision = _run_cli(
        [
            "backends",
            "add",
            "rem-config-collision",
            "--kind",
            "rem_tape",
            "--config",
            "priority=1",
            "--config",
            "priority=2",
        ],
        expect_exit=2,
    )
    assert "priority" in config_collision.output
    assert "overwrite" in config_collision.output


def test_jobs_list_empty(cli_env: dict[str, str]) -> None:
    _run_cli(["db", "init"])
    result = _run_cli(["jobs", "list"])
    assert "(no jobs)" in result.output


def test_jobs_submit_unknown_kind_exits_nonzero(cli_env: dict[str, str]) -> None:
    _run_cli(["db", "init"])
    result = _run_cli(["jobs", "submit", "nonexistent_kind"], expect_exit=2)
    assert "no handler registered" in result.output


def test_jobs_submit_accepts_scheduler_fields_and_dedupe_key(
    cli_env: dict[str, str],
) -> None:
    _run_cli(["db", "init"])
    first = _run_cli(
        [
            "jobs",
            "submit",
            "verify",
            "--param",
            "copy_id=1",
            "--resource",
            "cpu=2",
            "--prereq",
            "42",
            "--not-before",
            "2030-01-02T03:04:05Z",
            "--priority",
            "7",
            "--dedupe-key",
            "verify:copy:1",
        ]
    )
    second = _run_cli(
        [
            "jobs",
            "submit",
            "verify",
            "--param",
            "copy_id=2",
            "--dedupe-key",
            "verify:copy:1",
        ]
    )

    assert "id=1" in first.output
    assert "id=1" in second.output
    eng = make_engine()
    with session_scope(eng) as s:
        [job] = list(s.scalars(select(Job)))
        assert job.params == {"copy_id": 1}
        assert job.required_resources == [{"pool": "cpu", "count": 2}]
        assert job.prerequisites == [42]
        assert job.priority == 7
        assert job.dedupe_key == "verify:copy:1"
        assert _aware_utc(job.not_before) == dt.datetime(2030, 1, 2, 3, 4, 5, tzinfo=dt.UTC)


def test_jobs_run_missing_id_exits_nonzero_without_traceback(
    cli_env: dict[str, str],
) -> None:
    _run_cli(["db", "init"])
    result = _run_cli(["jobs", "run", "--id", "999"], expect_exit=2)
    assert "no job with id=999" in result.output
    assert "Traceback" not in result.output


def test_jobs_run_terminal_id_exits_nonzero_without_traceback(
    cli_env: dict[str, str],
) -> None:
    _run_cli(["db", "init"])
    _run_cli(["jobs", "submit", "verify", "-p", "copy_id=999"])
    first = _run_cli(["jobs", "run", "--id", "1"])
    assert "FAILED" in first.output

    second = _run_cli(["jobs", "run", "--id", "1"], expect_exit=2)
    assert "terminal status" in second.output
    assert "Traceback" not in second.output


def test_jobs_round_trip(cli_env: dict[str, str]) -> None:
    """Full CLI round-trip: scrub populates catalog, submit verify, run, show."""
    _run_cli(["db", "init"])
    _run_cli(
        [
            "backends",
            "add",
            "tape-primary",
            "--kind",
            "rem_tape",
            "--tier",
            "self_describing",
            "--fixture",
            str(FIXTURE),
        ]
    )
    _run_cli(["scrub", "--backend", "tape-primary"])

    # Find a copy_id to verify. (List assets JSON doesn't expose copy_id;
    # query the catalog directly.)
    from sqlalchemy import select as _sel

    from sutradhara.catalog.models import Copy as _C

    eng = make_engine()
    with session_scope(eng) as s:
        copy_id = s.scalars(_sel(_C.id).limit(1)).one()

    submit_result = _run_cli(["jobs", "submit", "verify", "-p", f"copy_id={copy_id}"])
    assert "kind='verify'" in submit_result.output
    assert "status=pending" in submit_result.output

    list_result = _run_cli(["jobs", "list"])
    assert "verify" in list_result.output
    assert "pending" in list_result.output

    run_result = _run_cli(["jobs", "run"])
    assert "ok" in run_result.output
    assert "verified ok" in run_result.output

    # show emits JSON detail including step_state.
    show_result = _run_cli(["jobs", "show", "1"])
    payload = json.loads(show_result.output)
    assert payload["kind"] == "verify"
    assert payload["status"] == "succeeded"
    assert payload["step_state"]["verify_result"]["ok"] is True
    assert payload["attempts"] == 1


def test_jobs_run_drains_queue_with_limit_zero(cli_env: dict[str, str]) -> None:
    _run_cli(["db", "init"])
    _run_cli(
        [
            "backends",
            "add",
            "tape-primary",
            "--kind",
            "rem_tape",
            "--fixture",
            str(FIXTURE),
        ]
    )
    _run_cli(["scrub", "--backend", "tape-primary"])

    # Scrub discovery durably submits one verify per copy.
    from sqlalchemy import select as _sel

    from sutradhara.catalog.models import Copy as _C

    eng = make_engine()
    with session_scope(eng) as s:
        copy_ids = list(s.scalars(_sel(_C.id)))

    list_pending = _run_cli(["jobs", "list", "--status", "pending"])
    assert list_pending.output.count("verify") == len(copy_ids)

    run_all = _run_cli(["jobs", "run", "--limit", "0"])
    assert run_all.output.count("verified ok") == len(copy_ids)

    list_succeeded = _run_cli(["jobs", "list", "--status", "succeeded"])
    assert list_succeeded.output.count("verify") == len(copy_ids)
    list_pending_after = _run_cli(["jobs", "list", "--status", "pending"])
    assert list_pending_after.output == "(no jobs)\n"


def test_jobs_submit_with_string_param(cli_env: dict[str, str]) -> None:
    """Non-JSON --param values are passed through as strings."""

    @register_handler("_test_string_param")
    def _h(ctx: JobContext) -> JobResult:
        return JobResult(ok=True, detail=str(ctx.job.params))

    try:
        _run_cli(["db", "init"])
        result = _run_cli(["jobs", "submit", "_test_string_param", "-p", "label=hello-world"])
        assert "submitted job" in result.output
        run_result = _run_cli(["jobs", "run"])
        assert "hello-world" in run_result.output
    finally:
        from sutradhara.jobs import registry as _r

        _r._HANDLERS.pop("_test_string_param", None)


# Use StorageBackend in a type assertion so the import isn't flagged unused.
def test_memory_backend_still_protocol() -> None:
    assert isinstance(MemoryBackend("x"), StorageBackend)


# -------------------------------------------------------------------------
# dispatch_restore + restore handler
# -------------------------------------------------------------------------


def _register_restorable_copy(
    engine: Engine,
    *,
    content: bytes = b"restore-me",
    backend_name: str = "tape-1",
    health: CopyHealth = CopyHealth.OK,
    locator_extra: dict[str, object] | None = None,
    backend_kind: BackendKind = BackendKind.REM_TAPE,
) -> int:
    """Register an asset + backend + one Copy; return the copy's id."""
    asset_hash = hashlib.sha256(content).digest()
    locator = {"hash_hex": asset_hash.hex(), **(locator_extra or {})}
    with session_scope(engine) as s:
        if s.get(LogicalAsset, asset_hash) is None:
            s.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(content)))
        backend = Backend(
            name=backend_name,
            kind=backend_kind,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(backend)
        s.flush()
        copy = Copy(
            logical_asset_hash=asset_hash,
            backend_id=backend.id,
            native_locator=locator,
            native_locator_key=locator_key(locator),
            integrity_hash=asset_hash,
            storage_metadata={"representation": "raw-bytes"},
            source=CopySource.SCRUB,
            health=health,
        )
        s.add(copy)
        s.flush()
        return copy.id


def _register_restore_request_item(
    engine: Engine,
    *,
    content: bytes = b"restore-me",
    state: str = "queued",
    admitted: bool = True,
    delivery_mode: str = "server_local",
) -> int:
    """Register one restore request item and return its id."""
    asset_hash = hashlib.sha256(content).digest()
    with session_scope(engine) as s:
        if s.get(LogicalAsset, asset_hash) is None:
            s.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(content)))
        if admitted:
            request = admit_restore_request(
                s,
                identity=parse_identity(
                    {
                        "X-Authentik-Username": "ada",
                        "X-Authentik-Groups": "sutradhara-ingest",
                    }
                ),
                destination_id="media-server",
                items=[RestoreItemSpec(asset_hash, "s-masters")],
                config=RestoreConfig(
                    destinations={
                        "media-server": RestoreDestination(
                            id="media-server",
                            root=Path("/tmp/sutradhara-restore-root"),
                            label="media-server",
                        )
                    }
                ),
            )
            item = request.items[0]
            item.state = state
            s.flush([item])
            return item.id
        if delivery_mode == "agent":
            from sutradhara.grpc.store import GrpcLogicalDevice

            if s.get(GrpcLogicalDevice, "agent-device") is None:
                s.add(GrpcLogicalDevice(device_id="agent-device", scopes=["ingest", "restore"]))
                s.flush()
        request = RestoreRequest(
            id=f"restore-{asset_hash.hex()[:12]}",
            identity="ada",
            destination_id="media-server",
            state="active",
            delivery_mode=delivery_mode,
            receiver_device_id="agent-device" if delivery_mode == "agent" else None,
        )
        item = RestoreRequestItem(
            content_sha256=asset_hash,
            artifactclass="s-masters",
            state=state,
        )
        request.items.append(item)
        s.add(request)
        s.flush()
        return item.id


def test_dispatch_restore_creates_pending_restore_job(engine: Engine) -> None:
    from sutradhara.jobs.dispatch import dispatch_restore

    item_id = _register_restore_request_item(engine)

    with session_scope(engine) as s:
        handle = dispatch_restore(s, item_id)

    assert handle["kind"] == "restore"
    assert handle["restore_request_item_id"] == item_id
    assert handle["params"] == {"restore_request_item_id": item_id}

    with session_scope(engine) as s:
        job = s.get(Job, handle["job_id"])
        assert job is not None
        assert job.status == JobStatus.PENDING
        assert job.kind == "restore"
        assert job.params == {"restore_request_item_id": item_id}


def test_dispatch_restore_raises_for_unknown_request_item(engine: Engine) -> None:
    from sutradhara.jobs.dispatch import UnknownRestoreRequestItem, dispatch_restore

    with session_scope(engine) as s, pytest.raises(UnknownRestoreRequestItem, match="no Restore"):
        dispatch_restore(s, 999)


def test_dispatch_restore_rejects_nonqueued_item(engine: Engine) -> None:
    from sutradhara.jobs.dispatch import RestoreRequestItemNotRunnable, dispatch_restore

    item_id = _register_restore_request_item(engine, state="denied")

    with session_scope(engine) as s, pytest.raises(RestoreRequestItemNotRunnable, match="denied"):
        dispatch_restore(s, item_id)


def test_dispatch_restore_rejects_agent_delivery_item(engine: Engine) -> None:
    """Defense-in-depth: an agent-delivery item must never dispatch to the local writer."""
    from sutradhara.jobs.dispatch import RestoreRequestItemNotRunnable, dispatch_restore

    item_id = _register_restore_request_item(engine, admitted=False, delivery_mode="agent")

    with (
        session_scope(engine) as s,
        pytest.raises(
            RestoreRequestItemNotRunnable,
            match="agent-delivery",
        ),
    ):
        dispatch_restore(s, item_id)


def test_dispatch_restore_rejects_queued_item_without_admission_inputs(engine: Engine) -> None:
    from sutradhara.jobs.dispatch import RestoreRequestItemNotRunnable, dispatch_restore

    item_id = _register_restore_request_item(engine, admitted=False)

    with (
        session_scope(engine) as s,
        pytest.raises(
            RestoreRequestItemNotRunnable,
            match="missing admission inputs",
        ),
    ):
        dispatch_restore(s, item_id)


def test_jobs_submit_refuses_public_restore_kind(
    engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUTRADHARA_DB_URL", "sqlite:///:memory:")
    runner = CliRunner()
    result = runner.invoke(cli, ["jobs", "submit", "restore", "-p", "restore_request_item_id=1"])
    assert result.exit_code == 2
    assert "restore jobs must be created from gated restore requests" in result.output


def test_restore_kind_is_registered() -> None:
    assert "restore" in registered_kinds()


def test_restore_handler_fails_cleanly_does_not_fake_success(
    engine: Engine,
) -> None:
    """A restore job must FAIL (not SUCCEED) if required params are absent."""
    with session_scope(engine) as s:
        job = submit(s, "restore", {"copy_id": 1})
        result = run_one(s, job.id)
        assert not result.ok
        assert job.status == JobStatus.FAILED
        assert job.last_error is not None
        assert "rejects raw copy_id/dest_path" in job.last_error


def test_restore_handler_refuses_queued_item_without_admission_inputs(engine: Engine) -> None:
    item_id = _register_restore_request_item(engine, admitted=False)

    with session_scope(engine) as s:
        job = submit(s, "restore", {"restore_request_item_id": item_id})
        result = run_one(s, job.id)

        assert not result.ok
        assert job.status == JobStatus.FAILED
        assert job.last_error is not None
        assert "missing admission inputs" in job.last_error
        assert s.get(RestoreRequestItem, item_id).state == "failed"


def test_restore_handler_runs_gated_request_item(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sutradhara.jobs.handlers.restore as restore_handler
    from sutradhara.hdcache.manager import ITEM_DONE, RestoreConfig, ServeResult

    barcode = "D2RESTORE01"
    copy_id = _register_restorable_copy(
        engine,
        locator_extra={"barcode": barcode},
        backend_kind=BackendKind.D2_TAPE,
    )
    item_id = _register_restore_request_item(engine)
    output = tmp_path / "restored.bin"

    def fake_config() -> RestoreConfig:
        return RestoreConfig()

    def fake_serve(session, item, *, gates_already_admitted, config):
        assert item.id == item_id
        assert gates_already_admitted is True
        item.state = ITEM_DONE
        item.detail = None
        return ServeResult(item.id, "tape", output, 12, copy_id=copy_id)

    monkeypatch.setattr(restore_handler, "restore_config_from_env", fake_config)
    monkeypatch.setattr(restore_handler, "serve_restore_item", fake_serve)

    with session_scope(engine) as s:
        job = submit(s, "restore", {"restore_request_item_id": item_id})
        result = run_one(s, job.id)
        assert result.ok
        assert job.status == JobStatus.SUCCEEDED
        assert job.step_state["restore"]["restore_request_item_id"] == item_id
        assert job.step_state["restore"]["source"] == "tape"
        attempt = s.scalars(select(JobAttempt).where(JobAttempt.job_id == job.id)).one()
        assert f"tape:d2tape:{barcode}" in attempt.detail["components"]
