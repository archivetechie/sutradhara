"""Job attempt audit-log tests for completed job-engine runs."""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, func, select

from sutradhara._proto import layer5_pb2
from sutradhara.backend.remanence import RemanenceBackend
from sutradhara.catalog.models import Backend
from sutradhara.catalog.session import (
    create_all,
    make_engine,
    make_session_factory,
    session_scope,
)
from sutradhara.catalog.types import BackendKind, BackendTier
from sutradhara.jobs.attempts import record_attempt
from sutradhara.jobs.config import RetryPolicy, WorkerConfig
from sutradhara.jobs.engine import apply_retry_policy, run_one, submit
from sutradhara.jobs.models import Job, JobAttempt, JobStatus, ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    OBSERVED_MISSING,
    record_observation,
)
from sutradhara.jobs.registry import JobContext, JobResult, register_handler
from sutradhara.jobs.runtime_observations import report_session_open


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
            unknown = submit(session, "missing_attempt_kind", {"asset_hash": "ab" * 32})
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
            assert (
                "asset:sha256:" + "ab" * 32 in attempts["missing_attempt_kind"].detail["components"]
            )
            assert attempts["_attempt_raises"].outcome == JobStatus.FAILED
            assert attempts["_attempt_raises"].error is not None
            assert "RuntimeError" in attempts["_attempt_raises"].error
    finally:
        _unregister("_attempt_raises")


def test_tape_session_observations_do_not_invent_copy_media_components(engine: Engine) -> None:
    @register_handler("_attempt_tape_success")
    def _attempt_tape_success(_ctx: JobContext) -> JobResult:
        report_session_open(
            session_id="session-read-17",
            drive_element_address=257,
            tape_uuid="L80012",
            library="DEC1",
        )
        return JobResult(ok=True, detail="read completed")

    try:
        with session_scope(engine) as session:
            job = submit(session, "_attempt_tape_success", {})
            assert run_one(session, job.id).ok

        with session_scope(engine) as session:
            attempt = session.scalars(select(JobAttempt)).one()
            assert not any(
                component.startswith("tape:") for component in attempt.detail["components"]
            )
            assert "drive:257" in attempt.detail["components"]
            assert "library:DEC1" in attempt.detail["components"]
            assert attempt.detail["component_parents"] == [
                {"component": "drive:257", "parent": "library:DEC1"}
            ]
            assert attempt.detail["observations"] == [
                {
                    "session_id": "session-read-17",
                    "drive_element_address": 257,
                    "tape_uuid": "L80012",
                    "library": "DEC1",
                }
            ]
    finally:
        _unregister("_attempt_tape_success")


def test_tape_session_observation_survives_handler_exception(engine: Engine) -> None:
    @register_handler("_attempt_tape_raises")
    def _attempt_tape_raises(_ctx: JobContext) -> JobResult:
        report_session_open(
            session_id=b"session-before-error",
            drive_element_address=513,
            tape_uuid="L80013",
        )
        raise RuntimeError("read stopped")

    try:
        with session_scope(engine) as session:
            job = submit(session, "_attempt_tape_raises", {})
            assert not run_one(session, job.id).ok

        with session_scope(engine) as session:
            attempt = session.scalars(select(JobAttempt)).one()
            assert attempt.outcome == JobStatus.FAILED
            assert not any(
                component.startswith("tape:") for component in attempt.detail["components"]
            )
            assert "drive:513" in attempt.detail["components"]
            assert attempt.detail["observations"] == [
                {
                    "session_id": b"session-before-error".hex(),
                    "drive_element_address": 513,
                    "tape_uuid": "L80013",
                }
            ]
    finally:
        _unregister("_attempt_tape_raises")


class _AttemptWriteClient:
    """Protocol-shaped Remanence write client for engine composition tests."""

    def __init__(self, data: bytes, *, suffix: int, fail_after_open: bool = False) -> None:
        self.data = data
        self.fail_after_open = fail_after_open
        self.session_id = f"adapter-session-{suffix}".encode()
        self.tape_uuid = bytes([suffix]) * 16
        self.drive_element_address = 0x100 + suffix

    def OpenWriteSession(
        self, request: layer5_pb2.OpenWriteSessionRequest
    ) -> layer5_pb2.WriteSession:
        return layer5_pb2.WriteSession(
            session_id=self.session_id,
            tape_uuid=self.tape_uuid,
            drive_element_address=self.drive_element_address,
        )

    def AppendObject(
        self, request_iterator: Iterator[layer5_pb2.AppendObjectMessage]
    ) -> layer5_pb2.ObjectRecord:
        list(request_iterator)
        if self.fail_after_open:
            raise RuntimeError("append stopped after open")
        digest = hashlib.sha256(self.data).digest()
        return layer5_pb2.ObjectRecord(
            object_id=bytes.fromhex("10" * 16),
            caller_object_id="attempt-adapter",
            content_sha256=digest,
            logical_size_bytes=len(self.data),
            body_format="raw-bytes",
            copies=[
                layer5_pb2.ObjectCopy(
                    tape_uuid=self.tape_uuid,
                    tape_file_number=1,
                    first_body_lba=1,
                    pool_id="attempt-pool",
                )
            ],
        )

    def CloseWriteSession(
        self, request: layer5_pb2.CloseWriteSessionRequest
    ) -> layer5_pb2.WriteSession:
        return layer5_pb2.WriteSession(session_id=request.session_id)

    def AbortWriteSession(
        self, request: layer5_pb2.AbortWriteSessionRequest
    ) -> layer5_pb2.WriteSession:
        return layer5_pb2.WriteSession(session_id=request.session_id)


def test_real_adapter_session_facts_reach_attempt_on_success_and_failure(
    engine: Engine,
    tmp_path: Path,
) -> None:
    data = b"adapter composition"
    source = tmp_path / "adapter.bin"
    source.write_bytes(data)
    success_client = _AttemptWriteClient(data, suffix=1)
    failure_client = _AttemptWriteClient(data, suffix=2, fail_after_open=True)

    @register_handler("_attempt_adapter_success")
    def _attempt_adapter_success(_ctx: JobContext) -> JobResult:
        RemanenceBackend(
            "adapter-rem",
            endpoint="stub",
            write_session=success_client,
        ).write_object_to_pool(source, "attempt-pool")
        return JobResult(ok=True, detail="write completed")

    @register_handler("_attempt_adapter_failure")
    def _attempt_adapter_failure(_ctx: JobContext) -> JobResult:
        RemanenceBackend(
            "adapter-rem",
            endpoint="stub",
            write_session=failure_client,
        ).write_object_to_pool(source, "attempt-pool")
        raise AssertionError("unreachable after adapter failure")

    try:
        with session_scope(engine) as session:
            success_job = submit(session, "_attempt_adapter_success", {})
            failure_job = submit(session, "_attempt_adapter_failure", {})
            assert run_one(session, success_job.id).ok
            assert not run_one(session, failure_job.id).ok

        with session_scope(engine) as session:
            attempts = {
                attempt.job_kind: attempt
                for attempt in session.scalars(select(JobAttempt).order_by(JobAttempt.job_kind))
            }
            for kind, client, outcome in (
                ("_attempt_adapter_success", success_client, JobStatus.SUCCEEDED),
                ("_attempt_adapter_failure", failure_client, JobStatus.FAILED),
            ):
                attempt = attempts[kind]
                assert attempt.outcome == outcome
                assert not any(
                    component.startswith("tape:") for component in attempt.detail["components"]
                )
                assert f"drive:{client.drive_element_address}" in attempt.detail["components"]
                assert attempt.detail["observations"] == [
                    {
                        "session_id": client.session_id.hex(),
                        "drive_element_address": client.drive_element_address,
                        "tape_uuid": client.tape_uuid.hex(),
                    }
                ]
    finally:
        _unregister("_attempt_adapter_success")
        _unregister("_attempt_adapter_failure")


def test_handler_flush_error_rolls_back_to_savepoint_and_records_attempt(engine: Engine) -> None:
    @register_handler("_attempt_flush_error")
    def _attempt_flush_error(ctx: JobContext) -> JobResult:
        ctx.observe({"phase": "before duplicate backend flush"})
        ctx.session.add(
            Backend(
                name="duplicate-backend",
                kind=BackendKind.MEMORY,
                tier=BackendTier.SELF_DESCRIBING,
            )
        )
        ctx.session.flush()
        raise AssertionError("unreachable after unique constraint failure")

    try:
        with session_scope(engine) as session:
            session.add(
                Backend(
                    name="duplicate-backend",
                    kind=BackendKind.MEMORY,
                    tier=BackendTier.SELF_DESCRIBING,
                )
            )
            session.flush()
            record_observation(
                session,
                domain="_attempt_flush_error",
                target_key="duplicate-backend",
                desired=True,
                observed_state=OBSERVED_MISSING,
            )
            job = submit(
                session,
                "_attempt_flush_error",
                {},
                recon_domain="_attempt_flush_error",
                recon_target_key="duplicate-backend",
            )
            result = run_one(session, job.id)

            assert not result.ok
            assert job.status == JobStatus.FAILED
            assert session.scalar(select(func.count()).select_from(JobAttempt)) == 1
            attempt = session.scalars(select(JobAttempt)).one()
            assert attempt.outcome == JobStatus.FAILED
            assert attempt.detail["observations"] == [{"phase": "before duplicate backend flush"}]
            condition = session.scalars(select(ReconciliationCondition)).one()
            assert condition.condition == CONDITION_BACKOFF
            assert condition.last_attempt_id == attempt.id
            assert (
                session.scalar(
                    select(func.count())
                    .select_from(Backend)
                    .where(Backend.name == "duplicate-backend")
                )
                == 1
            )
    finally:
        _unregister("_attempt_flush_error")


def test_observation_persists_when_handler_returns_failure(engine: Engine) -> None:
    @register_handler("_attempt_returns_failure")
    def _attempt_returns_failure(ctx: JobContext) -> JobResult:
        ctx.observe({"exit_status": 9, "stderr_excerpt": "tool stopped\n"})
        return JobResult(ok=False, detail="tool stopped")

    try:
        with session_scope(engine) as session:
            job = submit(session, "_attempt_returns_failure", {})
            assert not run_one(session, job.id).ok

        with session_scope(engine) as session:
            attempt = session.scalars(select(JobAttempt)).one()
            assert attempt.detail["observations"] == [
                {"exit_status": 9, "stderr_excerpt": "tool stopped\n"}
            ]
    finally:
        _unregister("_attempt_returns_failure")


def _aware_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _unregister(kind: str) -> None:
    from sutradhara.jobs import registry as _r

    _r._HANDLERS.pop(kind, None)
