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
from click.testing import CliRunner
from sqlalchemy import Engine, func, select, text

from sutradhara.catalog.session import create_all, make_engine, make_session_factory, session_scope
from sutradhara.cli.main import cli
from sutradhara.jobs.config import RetryPolicy, WorkerConfig
from sutradhara.jobs.engine import run_one, submit
from sutradhara.jobs.models import (
    ConditionComponent,
    Job,
    JobAttempt,
    JobStatus,
    ReconciliationCondition,
)
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    CONDITION_BLOCKED,
    CONDITION_OPEN,
    CONDITION_SATISFIED,
    OBSERVED_MISSING,
    ReconciliationInvariantError,
    _default_backoff_due,
    record_condition,
    record_observation,
)
from sutradhara.jobs.reconcilers.spine import reopen_version_bumped
from sutradhara.jobs.registry import (
    ConditionProjection,
    JobContext,
    JobResult,
    register_handler,
)
from sutradhara.jobs.tool_versions import register_tool_version, unregister_tool_version
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


def test_condition_default_backoff_has_jitter_and_clamp_bounds() -> None:
    now = dt.datetime.now(dt.UTC)
    base = 120
    samples = [(_default_backoff_due(now, 2) - now).total_seconds() for _index in range(50)]

    assert all(base * 0.8 <= sample <= base * 1.2 for sample in samples)
    clamped = [(_default_backoff_due(now, 7) - now).total_seconds() for _index in range(50)]
    assert all(sample <= 3600 for sample in clamped)


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


def test_reconcile_cli_lists_and_reopens_blocked_conditions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_url = f"sqlite:///{tmp_path / 'reconcile-cli.db'}"
    monkeypatch.setenv("SUTRADHARA_DB_URL", db_url)
    engine = make_engine(db_url)
    create_all(engine)
    try:
        with session_scope(engine) as session:
            _add_blocked_condition(
                session,
                target_key="asset:" + "1" * 64 + ":pool-a",
                reason="not-implemented",
                tool_name="ffmpeg",
                tool_version="old",
            )
            _add_blocked_condition(
                session,
                target_key="asset:" + "2" * 64 + ":pool-a",
                reason="drive-error",
            )

        runner = CliRunner()
        listed = runner.invoke(cli, ["reconcile", "copy", "--list-blocked"])
        assert listed.exit_code == 0, listed.output
        assert "asset:" + "1" * 64 + ":pool-a" in listed.output
        assert "reason=not-implemented" in listed.output
        assert "blocked_tool_name=ffmpeg" in listed.output
        assert "blocked_tool_version=old" in listed.output
        assert "since=" in listed.output

        reopened = runner.invoke(
            cli,
            ["reconcile", "copy", "--reopen-blocked", "--reason", "not-implemented"],
        )
        assert reopened.exit_code == 0, reopened.output
        assert "reopened 1 blocked condition(s)" in reopened.output
        assert "observed" not in reopened.output

        with session_scope(engine) as session:
            rows = {
                row.target_key: row
                for row in session.scalars(
                    select(ReconciliationCondition).order_by(ReconciliationCondition.target_key)
                )
            }
            reopened_row = rows["asset:" + "1" * 64 + ":pool-a"]
            held_row = rows["asset:" + "2" * 64 + ":pool-a"]
            assert reopened_row.condition == CONDITION_OPEN
            assert reopened_row.reason is None
            assert reopened_row.blocked_tool_name is None
            assert reopened_row.blocked_tool_version is None
            assert reopened_row.attempt_count == 0
            assert reopened_row.next_eligible_at is not None
            assert "reopened by" in (reopened_row.message or "")
            assert held_row.condition == CONDITION_BLOCKED
            assert held_row.reason == "drive-error"
    finally:
        engine.dispose()


def test_record_fix_reopens_only_exact_component_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @register_handler("_component_block")
    def _component_block(ctx: JobContext) -> JobResult:
        component = str(ctx.job.params["component"])
        ctx.touch(component)
        return JobResult(
            ok=True,
            detail="park requested",
            condition=ConditionProjection(
                condition=CONDITION_BLOCKED,
                reason="fixture-block",
                message="park requested",
            ),
        )

    db_url = f"sqlite:///{tmp_path / 'record-fix.db'}"
    monkeypatch.setenv("SUTRADHARA_DB_URL", db_url)
    monkeypatch.setattr("sutradhara.cli.reconcile.getpass.getuser", lambda: "operator-a")
    engine = make_engine(db_url)
    create_all(engine)
    target_components = {
        "match-a": "tape:L80012",
        "other": "tape:L800120",
    }
    try:
        with session_scope(engine) as session:
            jobs: dict[str, Job] = {}
            for target_key, component in target_components.items():
                record_observation(
                    session,
                    domain="copy",
                    target_key=target_key,
                    desired=True,
                    observed_state=OBSERVED_MISSING,
                )
                jobs[target_key] = submit(
                    session,
                    "_component_block",
                    {"component": component},
                    recon_domain="copy",
                    recon_target_key=target_key,
                )
            for job in jobs.values():
                assert run_one(session, job.id).ok

            matching = session.scalars(
                select(ConditionComponent).where(ConditionComponent.component == "tape:L80012")
            ).one()
            assert matching.condition_id is not None
            matching_attempt = session.scalars(
                select(JobAttempt).where(JobAttempt.job_id == jobs["match-a"].id)
            ).one()
            session.delete(matching_attempt)

        result = CliRunner().invoke(
            cli,
            ["reconcile", "record-fix", "tape:L80012", "--note", "tape replaced"],
        )
        assert result.exit_code == 0, result.output
        assert "reopened 1 blocked condition(s)" in result.output

        with session_scope(engine) as session:
            rows = {
                row.target_key: row
                for row in session.scalars(
                    select(ReconciliationCondition).order_by(ReconciliationCondition.target_key)
                )
            }
            assert rows["match-a"].condition == CONDITION_OPEN
            assert rows["match-a"].last_attempt_id is None
            assert "reopened by operator-a" in (rows["match-a"].message or "")
            assert "tape replaced" in (rows["match-a"].message or "")
            assert rows["match-a"].attempt_count == 0
            assert rows["other"].condition == CONDITION_BLOCKED
    finally:
        from sutradhara.jobs import registry as _registry

        _registry._HANDLERS.pop("_component_block", None)
        engine.dispose()


def test_version_bump_reopens_only_changed_known_tool_versions(engine: Engine) -> None:
    register_tool_version("fake-diff", lambda: "2.0")
    register_tool_version("fake-same", lambda: "1.0")
    register_tool_version("fake-unknown", lambda: "unknown")
    try:
        with session_scope(engine) as session:
            _add_blocked_condition(
                session,
                target_key="diff",
                reason="unsupported-source",
                tool_name="fake-diff",
                tool_version="1.0",
            )
            _add_blocked_condition(
                session,
                target_key="same",
                reason="unsupported-source",
                tool_name="fake-same",
                tool_version="1.0",
            )
            _add_blocked_condition(
                session,
                target_key="unknown",
                reason="unsupported-source",
                tool_name="fake-unknown",
                tool_version="1.0",
            )

            assert reopen_version_bumped(session, "copy") == 1

            rows = {row.target_key: row for row in session.scalars(select(ReconciliationCondition))}
            assert rows["diff"].condition == CONDITION_OPEN
            assert rows["diff"].reason is None
            assert rows["diff"].blocked_tool_name is None
            assert rows["diff"].next_eligible_at is not None
            assert "version changed from 1.0 to 2.0" in (rows["diff"].message or "")
            assert rows["same"].condition == CONDITION_BLOCKED
            assert rows["unknown"].condition == CONDITION_BLOCKED
    finally:
        unregister_tool_version("fake-diff")
        unregister_tool_version("fake-same")
        unregister_tool_version("fake-unknown")


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


def _add_blocked_condition(
    session: object,
    *,
    target_key: str,
    reason: str,
    tool_name: str | None = None,
    tool_version: str | None = None,
) -> None:
    session.add(
        ReconciliationCondition(
            domain="copy",
            target_key=target_key,
            observed_state=OBSERVED_MISSING,
            condition=CONDITION_BLOCKED,
            reason=reason,
            message="blocked",
            attempt_count=3,
            next_eligible_at=None,
            blocked_tool_name=tool_name,
            blocked_tool_version=tool_version,
            updated_at=dt.datetime.now(dt.UTC),
        )
    )
