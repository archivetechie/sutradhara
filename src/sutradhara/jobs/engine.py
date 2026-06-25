"""Job submission, atomic claim, and synchronous in-process execution.

The worker stores jobs in the Sutradhara catalog DB. SQLite is still the queue
substrate, but claims now use a guarded ``PENDING -> RUNNING`` update so the
future Postgres ``SKIP LOCKED`` implementation can be swapped in one place.
"""

from __future__ import annotations

import datetime as dt
import traceback
from typing import Any, cast

from sqlalchemy import select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session

from sutradhara.jobs.attempts import record_attempt
from sutradhara.jobs.config import WorkerConfig
from sutradhara.jobs.leases import LeaseManager, normalize_required_resources
from sutradhara.jobs.models import LIVE_JOB_STATUS_VALUES, TERMINAL_STATUSES, Job, JobStatus
from sutradhara.jobs.registry import (
    HandlerNotRegistered,
    JobContext,
    JobResult,
    get_handler,
)


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def submit(
    session: Session,
    kind: str,
    params: dict[str, Any] | None = None,
    *,
    required_resources: list[dict[str, Any]] | None = None,
    prerequisites: list[int] | None = None,
    not_before: dt.datetime | None = None,
    priority: int = 0,
    dedupe_key: str | None = None,
) -> Job:
    """Create a PENDING job in the catalog. Returns the persisted Job.

    Caller is responsible for `session.commit()`.
    """
    if dedupe_key is not None:
        existing = session.scalars(
            select(Job)
            .where(
                Job.dedupe_key == dedupe_key,
                Job.status.in_(LIVE_JOB_STATUS_VALUES),
            )
            .order_by(Job.id)
            .limit(1)
        ).one_or_none()
        if existing is not None:
            return existing
    now = _utcnow()
    job = Job(
        kind=kind,
        params=params or {},
        required_resources=required_resources or [],
        prerequisites=prerequisites or [],
        status=JobStatus.PENDING,
        created_at=now,
        not_before=not_before or now,
        priority=priority,
        dedupe_key=dedupe_key,
    )
    session.add(job)
    session.flush()  # so caller can read job.id
    return job


def run_one(
    session: Session,
    job_id: int,
    *,
    granted_leases: dict[str, int] | None = None,
) -> JobResult:
    """Run a specific job by id. Marks status, records outcome.

    Returns the `JobResult` from the handler (or a synthesized failure
    result if the handler raised). Caller is responsible for `commit()`.
    """
    job = session.get(Job, job_id)
    if job is None:
        raise ValueError(f"no job with id={job_id}")
    if job.status in TERMINAL_STATUSES:
        raise ValueError(f"job id={job_id} is in terminal status {job.status}; cannot re-run")
    if job.status == JobStatus.PENDING:
        job = _claim_job_by_id(session, job.id, now=_utcnow())
        if job is None:
            raise ValueError(f"job id={job_id} could not be claimed")
    elif job.status != JobStatus.RUNNING:
        raise ValueError(f"job id={job_id} is {job.status}; cannot run")

    try:
        handler = get_handler(job.kind)
    except HandlerNotRegistered as e:
        job.status = JobStatus.FAILED
        job.finished_at = _utcnow()
        job.last_error = str(e)
        record_attempt(session, job, granted_leases=granted_leases)
        return JobResult(ok=False, detail=str(e))

    try:
        result = handler(JobContext(session=session, job=job, granted_leases=granted_leases or {}))
    except Exception as e:
        job.status = JobStatus.FAILED
        job.finished_at = _utcnow()
        job.last_error = f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"
        record_attempt(session, job, granted_leases=granted_leases)
        return JobResult(ok=False, detail=str(e))

    job.status = JobStatus.SUCCEEDED if result.ok else JobStatus.FAILED
    job.finished_at = _utcnow()
    job.last_error = None if result.ok else result.detail
    if result.step_state:
        # Merge over existing step_state so handlers can build up state
        # incrementally without clobbering.
        job.step_state = {**job.step_state, **result.step_state}
    record_attempt(session, job, granted_leases=granted_leases)
    return result


def claim_pending(
    session: Session,
    *,
    leases: LeaseManager | None = None,
    now: dt.datetime | None = None,
) -> Job | None:
    """Atomically claim the first eligible pending job that fits leases."""
    claim_time = now or _utcnow()
    manager = leases or LeaseManager(WorkerConfig.defaults().capacities)
    for job in _pending_candidates(session, now=claim_time):
        required = normalize_required_resources(job.required_resources)
        if not manager.fits(required):
            continue
        claimed = _claim_job_by_id(session, job.id, now=claim_time)
        if claimed is not None:
            return claimed
    return None


def run_pending(session: Session, *, limit: int = 1) -> list[tuple[int, JobResult]]:
    """Run up to `limit` PENDING jobs sequentially. Returns (job_id, result).

    `limit=0` runs everything currently pending.
    """
    results: list[tuple[int, JobResult]] = []
    remaining = limit if limit > 0 else None
    while remaining is None or remaining > 0:
        job = claim_pending(session)
        if job is None:
            break
        result = run_one(session, job.id)
        results.append((job.id, result))
        # Flush so the next claim sees the status transition.
        session.flush()
        if remaining is not None:
            remaining -= 1
    return results


def pending_candidates(session: Session, *, now: dt.datetime | None = None) -> list[Job]:
    """Return pending jobs whose delay and prerequisites allow dispatch."""
    return _pending_candidates(session, now=now or _utcnow())


def claim_job_by_id(
    session: Session,
    job_id: int,
    *,
    now: dt.datetime | None = None,
) -> Job | None:
    """Guarded ``PENDING -> RUNNING`` claim for a scheduler-selected job."""
    return _claim_job_by_id(session, job_id, now=now or _utcnow())


def reset_orphaned_running_jobs(session: Session) -> int:
    """Reset RUNNING jobs to PENDING on single-worker startup."""
    result = cast(
        CursorResult[Any],
        session.execute(
            update(Job)
            .where(Job.status == JobStatus.RUNNING)
            .values(status=JobStatus.PENDING, started_at=None, not_before=_utcnow())
        ),
    )
    return int(result.rowcount or 0)


def apply_retry_policy(
    session: Session,
    job: Job,
    *,
    config: WorkerConfig,
    now: dt.datetime | None = None,
) -> None:
    """Re-enqueue a failed job when its retry policy has attempts remaining."""
    if job.status != JobStatus.FAILED:
        return
    retry = config.retry_for_kind(job.kind)
    if job.attempts >= retry.max_attempts:
        return
    base = now or _utcnow()
    job.status = JobStatus.PENDING
    job.not_before = base + dt.timedelta(seconds=retry.delay_seconds(job.attempts))
    job.started_at = None
    job.finished_at = None
    session.flush()


def _pending_candidates(session: Session, *, now: dt.datetime) -> list[Job]:
    rows = list(
        session.scalars(
            select(Job)
            .where(
                Job.status == JobStatus.PENDING,
                Job.not_before <= now,
            )
            .order_by(Job.priority, Job.created_at, Job.id)
        )
    )
    return [job for job in rows if _prerequisites_succeeded(session, job)]


def _prerequisites_succeeded(session: Session, job: Job) -> bool:
    for prereq_id in job.prerequisites or []:
        prereq = session.get(Job, prereq_id)
        if prereq is None or prereq.status != JobStatus.SUCCEEDED:
            return False
    return True


def _claim_job_by_id(session: Session, job_id: int, *, now: dt.datetime) -> Job | None:
    result = cast(
        CursorResult[Any],
        session.execute(
            update(Job)
            .where(Job.id == job_id, Job.status == JobStatus.PENDING)
            .values(
                status=JobStatus.RUNNING,
                started_at=now,
                finished_at=None,
                attempts=Job.attempts + 1,
            )
        ),
    )
    if result.rowcount != 1:
        return None
    session.flush()
    claimed = session.get(Job, job_id)
    if claimed is not None:
        session.refresh(claimed)
    return claimed
