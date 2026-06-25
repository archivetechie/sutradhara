"""Single-node concurrent job worker with counted resource leases.

One process owns the in-memory lease tally. The database remains the durable
queue and source of truth for job status; each claimed job runs in its own
session so handlers can mutate catalog state safely while the scheduler keeps
dispatching other lease-fitting work.
"""

from __future__ import annotations

import datetime as dt
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from sutradhara.catalog.session import session_scope
from sutradhara.jobs import handlers as _handlers  # noqa: F401 -- register built-ins
from sutradhara.jobs.config import WorkerConfig
from sutradhara.jobs.engine import (
    apply_retry_policy,
    claim_job_by_id,
    pending_candidates,
    reset_orphaned_running_jobs,
    run_one,
)
from sutradhara.jobs.leases import LeaseManager, normalize_required_resources
from sutradhara.jobs.models import Job, JobStatus
from sutradhara.jobs.registry import JobResult


@dataclass(frozen=True)
class WorkerJobOutcome:
    """Result for one job executed by the worker."""

    job_id: int
    result: JobResult
    granted_leases: dict[str, int]


class JobWorker:
    """Lease-aware worker for a single Sutradhara process."""

    def __init__(self, engine: Engine, *, config: WorkerConfig | None = None) -> None:
        self.engine = engine
        self.config = config or WorkerConfig.defaults()
        self.leases = LeaseManager(self.config.capacities)
        self._blocked_scans: dict[int, int] = {}

    def recover_orphans(self) -> int:
        with session_scope(self.engine) as session:
            return reset_orphaned_running_jobs(session)

    def drain(self, *, recover_orphans: bool = True) -> list[WorkerJobOutcome]:
        """Run pending work until no eligible dispatchable jobs remain."""
        if recover_orphans:
            self.recover_orphans()
        outcomes: list[WorkerJobOutcome] = []
        max_workers = max(self.config.executor_workers or 1, 1)
        futures: dict[Future[WorkerJobOutcome], dict[str, int]] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                claimed = self._claim_available()
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
                        outcomes.append(future.result())
                    finally:
                        self.leases.release(granted)
        return outcomes

    def _claim_available(self) -> list[tuple[int, dict[str, int]]]:
        claimed: list[tuple[int, dict[str, int]]] = []
        with session_scope(self.engine) as session:
            while True:
                job = None
                granted: dict[str, int] | None = None
                for candidate in pending_candidates(session):
                    required = normalize_required_resources(candidate.required_resources)
                    if not self.leases.can_ever_fit(required):
                        _mark_never_fits(candidate, required, self.config.capacities)
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
                    self._blocked_scans.pop(candidate.id, None)
                    break
                if job is None or granted is None:
                    break
                claimed.append((job.id, granted))
        return claimed


def _mark_never_fits(job: Job, required: dict[str, int], capacities: dict[str, int]) -> None:
    job.status = JobStatus.FAILED
    job.finished_at = dt.datetime.now(dt.UTC)
    job.last_error = f"required resources {required!r} exceed worker capacities {capacities!r}"


def _execute_job(
    engine: Engine,
    job_id: int,
    granted: dict[str, int],
    config: WorkerConfig,
) -> WorkerJobOutcome:
    with session_scope(engine) as session:
        result = run_one(session, job_id, granted_leases=granted)
        job = session.get(Job, job_id)
        if job is not None and job.recon_domain is None:
            apply_retry_policy(session, job, config=config)
        return WorkerJobOutcome(
            job_id=job_id,
            result=result,
            granted_leases=dict(granted),
        )
