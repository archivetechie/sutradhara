"""Exercise worker contention through real sessions with injected SQLite failures."""

import sqlite3

import pytest
from sqlalchemy import event, select
from sqlalchemy.exc import OperationalError

from sutradhara.catalog.models import Backend
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier
from sutradhara.jobs import registry
from sutradhara.jobs.config import WorkerConfig
from sutradhara.jobs.engine import submit
from sutradhara.jobs.models import Job, JobAttempt, JobStatus
from sutradhara.jobs.registry import JobResult, register_handler
from sutradhara.jobs.worker import JobWorker


@pytest.fixture
def setup_worker(tmp_path, monkeypatch):
    """Use a file database so executor threads share the same catalog."""
    engine = make_engine(f"sqlite:///{tmp_path / 'worker.db'}")
    create_all(engine)
    delays = []
    monkeypatch.setattr("sutradhara.jobs.worker.time.sleep", delays.append)

    @register_handler("_lock_test")
    def handler(ctx):
        ctx.session.add(
            Backend(name="lock-backend", kind=BackendKind.MEMORY, tier=BackendTier.SELF_DESCRIBING)
        )
        ctx.session.flush()
        return JobResult(ok=True, detail="done")

    with session_scope(engine) as session:
        job_id = submit(session, "_lock_test").id
    worker = JobWorker(engine, config=WorkerConfig(executor_workers=1))
    yield engine, worker, job_id, delays
    registry._HANDLERS.pop("_lock_test", None)
    engine.dispose()


def lock_error(message="database is locked"):
    return OperationalError("injected", {}, sqlite3.OperationalError(message))


@pytest.mark.parametrize("message", ["database is locked", "database table is locked"])
@pytest.mark.parametrize("target", ["job_attempt", "backend"])
def test_two_failed_flushes_record_one_attempt(setup_worker, message, target):
    engine, worker, job_id, delays = setup_worker
    calls = 0

    def fail_twice(conn, cursor, statement, parameters, context, executemany):
        nonlocal calls
        if statement.startswith(f"INSERT INTO {target} "):
            calls += 1
            if calls <= 2:
                raise lock_error(message)

    event.listen(engine, "before_cursor_execute", fail_twice)
    outcomes = worker.drain()
    assert len(outcomes) == 1
    assert outcomes[0].result.ok
    assert calls == 3
    assert delays == [0.1, 0.5]
    with session_scope(engine) as session:
        attempt = session.scalars(select(JobAttempt)).one()
        assert attempt.attempt_number == 1
        assert attempt.outcome == JobStatus.SUCCEEDED
        assert session.get(Job, job_id).attempts == 1
    assert worker.drain() == []


def test_persistent_handler_lock_records_failure_and_worker_continues(setup_worker):
    engine, worker, job_id, delays = setup_worker

    def always_locked(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO backend "):
            raise lock_error()

    event.listen(engine, "before_cursor_execute", always_locked)
    outcomes = worker.drain()
    assert len(outcomes) == 1
    assert not outcomes[0].result.ok
    assert not outcomes[0].failure_record_pending
    assert delays == [0.1, 0.5, 2, 5]
    with session_scope(engine) as session:
        attempt = session.scalars(select(JobAttempt)).one()
        assert attempt.outcome == JobStatus.FAILED
        assert "database is locked" in attempt.error
        assert session.get(Job, job_id).status == JobStatus.FAILED
    event.remove(engine, "before_cursor_execute", always_locked)
    with session_scope(engine) as session:
        submit(session, "_lock_test")
    assert worker.drain()[0].result.ok


def test_locked_failure_record_is_deferred_until_next_poll(setup_worker):
    engine, worker, job_id, _delays = setup_worker

    def always_locked(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO job_attempt "):
            raise lock_error()

    event.listen(engine, "before_cursor_execute", always_locked)
    outcome = worker.drain()[0]
    assert outcome.failure_record_pending
    assert worker.drain() == []
    event.remove(engine, "before_cursor_execute", always_locked)
    assert worker.drain() == []
    with session_scope(engine) as session:
        attempt = session.scalars(select(JobAttempt)).one()
        assert attempt.outcome == JobStatus.FAILED
        assert attempt.attempt_number == 1
        assert session.get(Job, job_id).status == JobStatus.FAILED


def test_unrelated_operational_error_still_propagates(setup_worker):
    engine, worker, _job_id, delays = setup_worker

    def broken(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO job_attempt "):
            raise lock_error("no such table: job_attempt")

    event.listen(engine, "before_cursor_execute", broken)
    with pytest.raises(OperationalError, match="no such table"):
        worker.drain()
    assert delays == []


def test_commit_lock_retries_entire_attempt(setup_worker):
    engine, worker, _job_id, delays = setup_worker
    commits = 0

    def mark_attempt(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO job_attempt "):
            conn.info["attempt_written"] = True

    def fail_commit(conn):
        nonlocal commits
        if conn.info.pop("attempt_written", False):
            commits += 1
            if commits <= 2:
                raise lock_error()

    event.listen(engine, "before_cursor_execute", mark_attempt)
    event.listen(engine, "commit", fail_commit)
    assert worker.drain()[0].result.ok
    assert commits == 3
    assert delays == [0.1, 0.5]
    with session_scope(engine) as session:
        assert session.scalars(select(JobAttempt)).one().attempt_number == 1
        assert session.scalars(select(Backend)).one().name == "lock-backend"


def test_claim_commit_lock_releases_reserved_leases(setup_worker):
    engine, worker, job_id, _delays = setup_worker
    worker = JobWorker(engine, config=WorkerConfig(capacities={"io": 1}, executor_workers=1))
    with session_scope(engine) as session:
        session.get(Job, job_id).required_resources = [{"pool": "io", "count": 1}]

    def fail_commit(conn):
        raise lock_error()

    event.listen(engine, "commit", fail_commit)
    assert worker.drain(recover_orphans=False) == []
    event.remove(engine, "commit", fail_commit)
    assert worker.drain(recover_orphans=False)[0].result.ok


def test_orphan_recovery_lock_is_retried_on_next_poll(setup_worker):
    engine, worker, _job_id, _delays = setup_worker

    def fail_query(conn, cursor, statement, parameters, context, executemany):
        raise lock_error()

    event.listen(engine, "before_cursor_execute", fail_query)
    assert worker.recover_orphans() == 0
    assert worker.drain(recover_orphans=False) == []
    event.remove(engine, "before_cursor_execute", fail_query)
    assert worker.drain(recover_orphans=False)[0].result.ok
