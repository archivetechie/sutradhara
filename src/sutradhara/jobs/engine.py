"""Job submission and synchronous in-process runner.

Day-1 engine: take the oldest PENDING job, mark RUNNING, dispatch to
handler, record outcome. No queue substrate (Redis / Postgres
LISTEN/NOTIFY), no worker fleets — those land in later slices.
"""

from __future__ import annotations

import datetime as dt
import traceback
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.jobs.models import TERMINAL_STATUSES, Job, JobStatus
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
) -> Job:
    """Create a PENDING job in the catalog. Returns the persisted Job.

    Caller is responsible for `session.commit()`.
    """
    job = Job(
        kind=kind,
        params=params or {},
        required_resources=required_resources or [],
        prerequisites=prerequisites or [],
        status=JobStatus.PENDING,
    )
    session.add(job)
    session.flush()  # so caller can read job.id
    return job


def run_one(session: Session, job_id: int) -> JobResult:
    """Run a specific job by id. Marks status, records outcome.

    Returns the `JobResult` from the handler (or a synthesized failure
    result if the handler raised). Caller is responsible for `commit()`.
    """
    job = session.get(Job, job_id)
    if job is None:
        raise ValueError(f"no job with id={job_id}")
    if job.status in TERMINAL_STATUSES:
        raise ValueError(
            f"job id={job_id} is in terminal status {job.status}; cannot re-run"
        )

    job.status = JobStatus.RUNNING
    job.started_at = _utcnow()
    job.attempts += 1
    session.flush()

    try:
        handler = get_handler(job.kind)
    except HandlerNotRegistered as e:
        job.status = JobStatus.FAILED
        job.finished_at = _utcnow()
        job.last_error = str(e)
        return JobResult(ok=False, detail=str(e))

    try:
        result = handler(JobContext(session=session, job=job))
    except Exception as e:
        job.status = JobStatus.FAILED
        job.finished_at = _utcnow()
        job.last_error = f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"
        return JobResult(ok=False, detail=str(e))

    job.status = JobStatus.SUCCEEDED if result.ok else JobStatus.FAILED
    job.finished_at = _utcnow()
    job.last_error = None if result.ok else result.detail
    if result.step_state:
        # Merge over existing step_state so handlers can build up state
        # incrementally without clobbering.
        job.step_state = {**job.step_state, **result.step_state}
    return result


def claim_pending(session: Session) -> Job | None:
    """Return the oldest PENDING job, or None.

    Day-1 has no concurrency; "claim" is just "fetch oldest pending."
    A real scheduler with multiple workers will need SKIP LOCKED or an
    equivalent.
    """
    return session.scalars(
        select(Job)
        .where(Job.status == JobStatus.PENDING)
        .order_by(Job.created_at, Job.id)
        .limit(1)
    ).one_or_none()


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
