"""Single-node concurrent job worker with counted resource leases.

One process owns the in-memory lease tally. The database remains the durable
queue and source of truth for job status; each claimed job runs in its own
session so handlers can mutate catalog state safely while the scheduler keeps
dispatching other lease-fitting work.
"""

from __future__ import annotations

import datetime as dt
import logging
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass

from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from sutradhara.catalog.session import session_scope
from sutradhara.jobs import handlers as _handlers  # noqa: F401 -- register built-ins
from sutradhara.jobs.attempts import record_attempt
from sutradhara.jobs.config import WorkerConfig
from sutradhara.jobs.engine import (
    AttemptSuperseded,
    _record_reconciler_condition,
    apply_retry_policy,
    attempt_detail,
    claim_job_by_id,
    is_sqlite_lock_error,
    job_context,
    pending_candidates,
    reset_orphaned_running_jobs,
    run_one,
)
from sutradhara.jobs.leases import LeaseManager, normalize_required_resources
from sutradhara.jobs.models import Job, JobStatus
from sutradhara.jobs.reconcilers.conditions import CONDITION_BACKOFF
from sutradhara.jobs.registry import JobResult
from sutradhara.structured_logs import emit_structured_event

_LOCK_BACKOFF = (0.1, 0.5, 2.0, 5.0)
_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerJobOutcome:
    """Result for one job executed by the worker."""

    job_id: int
    result: JobResult
    granted_leases: dict[str, int]
    failure_record_pending: bool = False


class JobWorker:
    """Lease-aware worker for a single Sutradhara process."""

    def __init__(self, engine: Engine, *, config: WorkerConfig | None = None) -> None:
        self.engine = engine
        self.config = config or WorkerConfig.defaults()
        self.leases = LeaseManager(self.config.capacities)
        self._blocked_scans: dict[int, int] = {}
        self._pending_failures: dict[int, WorkerJobOutcome] = {}
        self._recovery_pending = False

    def recover_orphans(self) -> int:
        try:
            with session_scope(self.engine) as session:
                recovered = reset_orphaned_running_jobs(session)
        except OperationalError as error:
            if not is_sqlite_lock_error(error):
                raise
            self._recovery_pending = True
            return 0
        self._recovery_pending = False
        return recovered

    def drain(self, *, recover_orphans: bool = True) -> list[WorkerJobOutcome]:
        """Run pending work until no eligible dispatchable jobs remain."""
        for job_id, outcome in list(self._pending_failures.items()):
            if _persist_lock_failure(self.engine, outcome):
                del self._pending_failures[job_id]
        # Do not reset a job whose failed attempt is still awaiting persistence.
        if (recover_orphans or self._recovery_pending) and not self._pending_failures:
            self.recover_orphans()
        if self._recovery_pending:
            return []
        outcomes: list[WorkerJobOutcome] = []
        max_workers = max(self.config.executor_workers or 1, 1)
        futures: dict[Future[WorkerJobOutcome], dict[str, int]] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                free_slots = max_workers - len(futures)
                claimed = self._claim_available(max_new=free_slots) if free_slots > 0 else []
                for job_id, granted in claimed:
                    future = executor.submit(
                        _execute_job,
                        self.engine,
                        job_id,
                        granted,
                        self.config,
                    )
                    futures[future] = granted
                if not futures:
                    break
                done, _pending = wait(futures, return_when=FIRST_COMPLETED)
                for future in done:
                    granted = futures.pop(future)
                    try:
                        outcome = future.result()
                        outcomes.append(outcome)
                        if outcome.failure_record_pending:
                            self._pending_failures[outcome.job_id] = outcome
                    finally:
                        self.leases.release(granted)
        return outcomes

    def _claim_available(self, *, max_new: int) -> list[tuple[int, dict[str, int]]]:
        if max_new <= 0:
            return []
        # Claims have no attempt yet. Roll back contention and retry on the next
        # scheduler poll, releasing reservations made by the aborted transaction.
        reserved_before: list[dict[str, int]] = []
        try:
            return self._claim_available_bounded(max_new=max_new, reserved=reserved_before)
        except Exception as error:
            for granted in reserved_before:
                self.leases.release(granted)
            if is_sqlite_lock_error(error):
                return []
            raise

    def _claim_available_bounded(
        self, *, max_new: int, reserved: list[dict[str, int]]
    ) -> list[tuple[int, dict[str, int]]]:
        claimed: list[tuple[int, dict[str, int]]] = []
        with session_scope(self.engine) as session:
            while len(claimed) < max_new:
                job = None
                granted: dict[str, int] | None = None
                for candidate in pending_candidates(session):
                    required = normalize_required_resources(candidate.required_resources)
                    if not self.leases.can_ever_fit(required):
                        _mark_never_fits(
                            session,
                            candidate,
                            required,
                            self.config.capacities,
                        )
                        self._blocked_scans.pop(candidate.id, None)
                        session.flush()
                        continue
                    if not self.leases.fits(required):
                        count = self._blocked_scans.get(candidate.id, 0) + 1
                        self._blocked_scans[candidate.id] = count
                        if count >= self.config.aging_threshold_scans:
                            break
                        continue
                    job = claim_job_by_id(session, candidate.id)
                    if job is None:
                        continue
                    granted = self.leases.reserve(required)
                    reserved.append(granted)
                    self._blocked_scans.pop(candidate.id, None)
                    break
                if job is None or granted is None:
                    break
                claimed.append((job.id, granted))
        return claimed


def _mark_never_fits(
    session: Session,
    job: Job,
    required: dict[str, int],
    capacities: dict[str, int],
) -> None:
    now = dt.datetime.now(dt.UTC)
    job.attempts += 1
    job.started_at = now
    job.status = JobStatus.FAILED
    job.finished_at = now
    job.last_error = f"required resources {required!r} exceed worker capacities {capacities!r}"
    ctx = job_context(session, job)
    attempt = record_attempt(session, job, granted_leases={}, detail=attempt_detail(ctx))
    _record_reconciler_condition(
        session,
        job,
        attempt,
        condition=CONDITION_BACKOFF,
        reason="never-fit",
        message=job.last_error,
    )


def _execute_job_once(
    engine: Engine,
    job_id: int,
    granted: dict[str, int],
    config: WorkerConfig,
) -> WorkerJobOutcome:
    with session_scope(engine) as session:
        if engine.dialect.name == "sqlite":
            # sqlite3's legacy transaction mode does not BEGIN for SELECT or
            # SAVEPOINT. Without this, releasing the handler savepoint commits
            # its writes before the attempt transaction can roll them back.
            connection = session.connection()
            dbapi_connection = connection.connection.dbapi_connection
            # session.connection() has just checked out a live connection; no
            # close or invalidation can have occurred before this access.
            if dbapi_connection is None:
                raise RuntimeError("active session must have a DBAPI connection")
            if not dbapi_connection.in_transaction:
                connection.exec_driver_sql("BEGIN")
            session.info["attempt_total_changes"] = dbapi_connection.total_changes
        job = session.get(Job, job_id)
        emit_structured_event(
            "sutradhara.job.started",
            job_id=job_id,
            job_kind=None if job is None else job.kind,
            entity_refs=[{"kind": "job", "id": str(job_id), "confidence": "high"}],
            granted_leases=dict(granted),
        )
        attempt_superseded = False
        try:
            result = run_one(session, job_id, granted_leases=granted)
        except AttemptSuperseded as error:
            session.rollback()
            result = JobResult(ok=False, detail=str(error), superseded=True)
            attempt_superseded = True
        job = session.get(Job, job_id)
        emit_structured_event(
            "sutradhara.job.finished",
            job_id=job_id,
            job_kind=None if job is None else job.kind,
            job_status=None if job is None else job.status,
            outcome="superseded" if result.superseded else "ok" if result.ok else "failed",
            entity_refs=[{"kind": "job", "id": str(job_id), "confidence": "high"}],
            granted_leases=dict(granted),
        )
        if not attempt_superseded and job is not None and job.recon_domain is None:
            apply_retry_policy(session, job, config=config)
        return WorkerJobOutcome(
            job_id=job_id,
            result=result,
            granted_leases=dict(granted),
        )


def _execute_job(
    engine: Engine,
    job_id: int,
    granted: dict[str, int],
    config: WorkerConfig,
) -> WorkerJobOutcome:
    """Retry the entire attempt transaction, including its flushes and commit.

    The claim is already committed, so rollback/replay retains one attempt
    number and writes only one audit row. Lock retries replay no late writer's
    external work: its boundary waits for BEGIN IMMEDIATE before writing.
    Crash recovery still may replay external effects; early writers retain
    their existing transaction and replay semantics.
    """
    for retry in range(len(_LOCK_BACKOFF) + 1):
        try:
            return _execute_job_once(engine, job_id, granted, config)
        except OperationalError as error:
            if not is_sqlite_lock_error(error):
                raise
            detail = str(error)
            if retry < len(_LOCK_BACKOFF):
                time.sleep(_LOCK_BACKOFF[retry])
    outcome = WorkerJobOutcome(job_id, JobResult(ok=False, detail=detail), dict(granted))
    if _persist_lock_failure(engine, outcome):
        return outcome
    return WorkerJobOutcome(job_id, outcome.result, dict(granted), failure_record_pending=True)


def _persist_lock_failure(engine: Engine, outcome: WorkerJobOutcome) -> bool:
    """Record exhaustion without replaying the handler; defer if SQLite is locked.

    An indefinitely locked database cannot accept even a failure record. Keep
    the claimed job RUNNING and let the live worker retry this write next poll.
    """
    try:
        with session_scope(engine) as session:
            job = session.get(Job, outcome.job_id)
            if job is None:
                return True
            job.status = JobStatus.FAILED
            job.finished_at = dt.datetime.now(dt.UTC)
            job.last_error = outcome.result.detail
            ctx = job_context(session, job, granted_leases=outcome.granted_leases)
            attempt = record_attempt(
                session, job, granted_leases=outcome.granted_leases, detail=attempt_detail(ctx)
            )
            _record_reconciler_condition(
                session,
                job,
                attempt,
                condition=CONDITION_BACKOFF,
                reason="sqlite-lock-exhausted",
                message=job.last_error,
            )
    except OperationalError as error:
        if not is_sqlite_lock_error(error):
            raise
        _LOG.warning("Deferring locked SQLite failure record for job %s: %s", outcome.job_id, error)
        return False
    return True
