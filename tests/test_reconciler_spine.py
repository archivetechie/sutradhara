"""Tests for the generic P0.3 reconciler spine.

These focus on the domain-agnostic condition helpers and engine/worker hooks:
Axis A owns observation, Axis B updates existing condition rows after attempts,
and reconciler-backed jobs do not use job-level retry.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, func, select, text

from sutradhara.catalog.session import create_all, make_engine, make_session_factory, session_scope
from sutradhara.jobs.config import RetryPolicy, WorkerConfig
from sutradhara.jobs.engine import submit
from sutradhara.jobs.models import Job, JobStatus, ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    CONDITION_SATISFIED,
    OBSERVED_MISSING,
    ReconciliationInvariantError,
    record_condition,
    record_observation,
)
from sutradhara.jobs.worker import JobWorker


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_record_condition_raises_if_row_absent(engine: Engine) -> None:
    with session_scope(engine) as session, pytest.raises(ReconciliationInvariantError):
        record_condition(
            session,
            domain="copy",
            target_key="asset:" + "a" * 64 + ":pool-a",
            condition=CONDITION_BACKOFF,
            reason="drive-error",
        )


def test_record_observation_and_condition_do_not_commit(engine: Engine) -> None:
    target_key = "asset:" + "b" * 64 + ":pool-a"
    factory = make_session_factory(engine)
    session = factory()
    try:
        row = record_observation(
            session,
            domain="copy",
            target_key=target_key,
            desired=True,
            observed_state=OBSERVED_MISSING,
        )
        record_condition(
            session,
            domain="copy",
            target_key=target_key,
            condition=CONDITION_BACKOFF,
            reason="drive-error",
        )
        assert row.id is not None
        session.rollback()
    finally:
        session.close()

    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(ReconciliationCondition)) == 0


def test_due_backoff_observation_preserves_attempt_count(engine: Engine) -> None:
    target_key = "asset:" + "c" * 64 + ":pool-a"
    with session_scope(engine) as session:
        row = record_observation(
            session,
            domain="copy",
            target_key=target_key,
            desired=True,
            observed_state=OBSERVED_MISSING,
        )
        row.attempt_count = 2
        row.condition = CONDITION_BACKOFF
        row.reason = "drive-error"
        row.next_eligible_at = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1)
        session.flush()

        observed = record_observation(
            session,
            domain="copy",
            target_key=target_key,
            desired=True,
            observed_state=OBSERVED_MISSING,
        )

        assert observed.condition == CONDITION_BACKOFF
        assert observed.attempt_count == 2
        assert observed.reason == "drive-error"


def test_observation_clears_stale_diagnostics_on_satisfied(engine: Engine) -> None:
    target_key = "asset:" + "d" * 64 + ":pool-a"
    with session_scope(engine) as session:
        row = record_observation(
            session,
            domain="copy",
            target_key=target_key,
            desired=True,
            observed_state=OBSERVED_MISSING,
        )
        row.condition = CONDITION_BACKOFF
        row.reason = "drive-error"
        row.message = "drive busy"
        row.attempt_count = 2
        row.next_eligible_at = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=60)
        session.flush()

        satisfied = record_observation(
            session,
            domain="copy",
            target_key=target_key,
            desired=True,
            observed_state="present",
        )

        assert satisfied.condition == CONDITION_SATISFIED
        assert satisfied.reason is None
        assert satisfied.message is None
        assert satisfied.next_eligible_at is None
        assert satisfied.attempt_count == 0


def test_process_worklist_query_uses_condition_index(engine: Engine) -> None:
    with session_scope(engine) as session:
        plan = session.execute(
            text(
                "EXPLAIN QUERY PLAN "
                "SELECT id FROM reconciliation_condition "
                "WHERE domain = :domain "
                "AND condition IN ('open', 'backoff') "
                "AND next_eligible_at <= :now"
            ),
            {"domain": "copy", "now": dt.datetime.now(dt.UTC)},
        ).all()

    assert "ix_condition_work" in " ".join(str(row[-1]) for row in plan)


def test_worker_skips_job_retry_for_reconciler_jobs(tmp_path: Path) -> None:
    engine = make_engine(f"sqlite:///{tmp_path / 'worker-recon.db'}")
    create_all(engine)
    target_key = "asset:" + "e" * 64 + ":pool-a"
    with session_scope(engine) as session:
        record_observation(
            session,
            domain="copy",
            target_key=target_key,
            desired=True,
            observed_state=OBSERVED_MISSING,
        )
        recon_job = submit(
            session,
            "missing_handler_for_recon",
            {},
            recon_domain="copy",
            recon_target_key=target_key,
        )
        imperative_job = submit(session, "missing_handler_for_imperative", {})

    config = WorkerConfig(
        capacities={"cpu": 1, "io": 1, "tape_drive": 0, "gpu": 0},
        retry=RetryPolicy(max_attempts=2, backoff_seconds=3600),
        executor_workers=1,
    )
    worker = JobWorker(engine, config=config)
    worker.drain()

    with session_scope(engine) as session:
        recon = session.get(Job, recon_job.id)
        imperative = session.get(Job, imperative_job.id)
        condition = session.scalars(select(ReconciliationCondition)).one()

        assert recon is not None
        assert recon.status == JobStatus.FAILED
        assert imperative is not None
        assert imperative.status == JobStatus.PENDING
        assert condition.condition == CONDITION_BACKOFF
    engine.dispose()
