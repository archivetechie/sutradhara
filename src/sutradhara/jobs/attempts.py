"""Append completed job runs to the ``job_attempt`` audit log.

The caller owns the transaction: this module flushes but never commits or rolls
back. A process crash during a handler commits nothing and therefore records no
attempt row; orphan recovery resets the live job for a later completed run.
"""

from __future__ import annotations

import datetime as dt
import os
import socket
from collections.abc import Mapping
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from sqlalchemy.orm import Session

from sutradhara.jobs.models import Job, JobAttempt


def record_attempt(
    session: Session,
    job: Job,
    *,
    granted_leases: Mapping[str, int] | None = None,
    worker_id: str | None = None,
    code_version: str | None = None,
    detail: dict[str, Any] | None = None,
) -> JobAttempt:
    """Append one completed ``run_one`` execution to ``job_attempt``."""

    finished_at = job.finished_at or _utcnow()
    started_at = job.started_at or finished_at
    attempt = JobAttempt(
        job_id=job.id,
        job_kind=job.kind,
        subject_job_id=job.id,
        subject_domain=job.recon_domain,
        subject_key=job.recon_target_key,
        params_snapshot=dict(job.params or {}),
        attempt_number=job.attempts,
        outcome=job.status,
        error=job.last_error,
        started_at=started_at,
        finished_at=finished_at,
        granted_leases=dict(granted_leases or {}),
        worker_id=worker_id or default_worker_id(),
        code_version=code_version or default_code_version(),
        detail=detail if detail is not None else {"step_state": dict(job.step_state or {})},
    )
    session.add(attempt)
    session.flush([attempt])
    return attempt


def default_worker_id() -> str:
    """Return the default process identity recorded for job attempts."""

    return f"{socket.gethostname()}:{os.getpid()}"


def default_code_version() -> str:
    """Return the installed Sutradhara version, or ``unknown`` outside packaging."""

    try:
        return version("sutradhara")
    except PackageNotFoundError:
        return "unknown"


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
