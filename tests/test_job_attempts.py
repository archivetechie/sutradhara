"""Job attempt audit-log tests for completed job-engine runs."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, func, select

from sutradhara.catalog.session import (
    create_all,
    make_engine,
    make_session_factory,
    session_scope,
)
from sutradhara.jobs.attempts import record_attempt
from sutradhara.jobs.config import RetryPolicy, WorkerConfig
from sutradhara.jobs.engine import apply_retry_policy, run_one, submit
from sutradhara.jobs.models import Job, JobAttempt, JobStatus
from sutradhara.jobs.registry import JobContext, JobResult, register_handler


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_attempt_appended_for_successful_run(engine: Engine) -> None:
    @register_handler("_attempt_success")
    def _attempt_success(_ctx: JobContext) -> JobResult:
        return JobResult(ok=True, detail="ok", step_state={"unit": {"kind": "ok"}})

    try:
        with session_scope(engine) as session:
            job = submit(session, "_attempt_success", {})
            result = run_one(session, job.id, granted_leases={"cpu": 2})
            assert result.ok

        with session_scope(engine) as session:
            attempt = session.scalars(select(JobAttempt)).one()
            assert attempt.job_id == job.id
            assert attempt.job_kind == "_attempt_success"
            assert attempt.attempt_number == 1
            assert attempt.outcome == JobStatus.SUCCEEDED
            assert attempt.error is None
            assert attempt.started_at is not None
            assert attempt.finished_at is not None
            assert _aware_utc(attempt.finished_at) >= _aware_utc(attempt.started_at)
            assert attempt.granted_leases == {"cpu": 2}
            assert attempt.worker_id is not None
            assert ":" in attempt.worker_id
            assert attempt.code_version
            assert attempt.detail["step_state"]["unit"]["kind"] == "ok"
            assert attempt.created_at is not None
    finally:
        _unregister("_attempt_success")


def test_multiple_attempts_keep_retry_history(engine: Engine) -> None:
    calls = {"count": 0}

    @register_handler("_attempt_retry")
    def _attempt_retry(_ctx: JobContext) -> JobResult:
        calls["count"] += 1
        if calls["count"] == 1:
            return JobResult(ok=False, detail="first failure")
        return JobResult(ok=True, detail="later success")

    try:
        config = WorkerConfig(
            capacities={"cpu": 1, "io": 1, "tape_drive": 0, "gpu": 0},
            retry=RetryPolicy(max_attempts=3, backoff_seconds=0),
        )
        with session_scope(engine) as session:
            job = submit(session, "_attempt_retry", {})
            first = run_one(session, job.id)
            assert not first.ok
            apply_retry_policy(session, job, config=config)
            assert job.status == JobStatus.PENDING

        with session_scope(engine) as session:
            second = run_one(session, job.id)
            assert second.ok

        with session_scope(engine) as session:
            attempts = list(session.scalars(select(JobAttempt).order_by(JobAttempt.attempt_number)))
            assert [attempt.attempt_number for attempt in attempts] == [1, 2]
            assert [attempt.outcome for attempt in attempts] == [
                JobStatus.FAILED,
                JobStatus.SUCCEEDED,
            ]
            assert attempts[0].error == "first failure"
            assert attempts[1].error is None
            row = session.get(Job, job.id)
            assert row is not None
            assert row.status == JobStatus.SUCCEEDED
            assert row.last_error is None
    finally:
        _unregister("_attempt_retry")


def test_attempts_survive_terminal_job_prune(engine: Engine) -> None:
    @register_handler("_attempt_prune")
    def _attempt_prune(_ctx: JobContext) -> JobResult:
        return JobResult(ok=True, detail="ok")

    try:
        with session_scope(engine) as session:
            job = submit(session, "_attempt_prune", {})
            run_one(session, job.id)

        with session_scope(engine) as session:
            row = session.get(Job, job.id)
            assert row is not None
            session.delete(row)

        with session_scope(engine) as session:
            attempt = session.scalars(select(JobAttempt)).one()
            assert attempt.job_id is None
            assert attempt.job_kind == "_attempt_prune"
            assert attempt.outcome == JobStatus.SUCCEEDED
    finally:
        _unregister("_attempt_prune")


def test_record_attempt_does_not_commit(engine: Engine) -> None:
    with session_scope(engine) as session:
        job = submit(session, "verify", {"copy_id": 1})
        job_id = job.id

    factory = make_session_factory(engine)
    session = factory()
    try:
        tx_job = session.get(Job, job_id)
        assert tx_job is not None
        tx_job.status = JobStatus.SUCCEEDED
        tx_job.attempts = 1
        tx_job.started_at = dt.datetime.now(dt.UTC)
        tx_job.finished_at = dt.datetime.now(dt.UTC)
        tx_job.step_state = {"manual": {"kind": "ok"}}
        attempt = record_attempt(session, tx_job, granted_leases={"io": 1})
        assert attempt.id is not None
        session.rollback()
    finally:
        session.close()

    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(JobAttempt)) == 0


def test_attempts_record_unknown_handler_and_exception_paths(engine: Engine) -> None:
    @register_handler("_attempt_raises")
    def _attempt_raises(_ctx: JobContext) -> JobResult:
        raise RuntimeError("intentional failure")

    try:
        with session_scope(engine) as session:
            unknown = submit(session, "missing_attempt_kind", {})
            raised = submit(session, "_attempt_raises", {})
            unknown_result = run_one(session, unknown.id)
            raised_result = run_one(session, raised.id)
            assert not unknown_result.ok
            assert not raised_result.ok

        with session_scope(engine) as session:
            attempts = {
                attempt.job_kind: attempt
                for attempt in session.scalars(select(JobAttempt).order_by(JobAttempt.job_kind))
            }
            assert set(attempts) == {"_attempt_raises", "missing_attempt_kind"}
            assert attempts["missing_attempt_kind"].outcome == JobStatus.FAILED
            assert attempts["missing_attempt_kind"].error is not None
            assert "no handler registered" in attempts["missing_attempt_kind"].error
            assert attempts["_attempt_raises"].outcome == JobStatus.FAILED
            assert attempts["_attempt_raises"].error is not None
            assert "RuntimeError" in attempts["_attempt_raises"].error
    finally:
        _unregister("_attempt_raises")


def _aware_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _unregister(kind: str) -> None:
    from sutradhara.jobs import registry as _r

    _r._HANDLERS.pop(kind, None)
