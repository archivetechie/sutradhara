"""SQLAlchemy 2.0 model for the job table.

The job row is a unit of work — jobs are data, not classes (spec §6.1
of the project spec). The engine dispatches by `kind` string to a
registered handler.
"""

from __future__ import annotations

import datetime as dt
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from sutradhara.catalog.models import Base


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


class JobStatus(StrEnum):
    PENDING = "pending"  # newly submitted
    QUEUED = "queued"  # scheduler has selected it (reserved for future)
    RUNNING = "running"  # handler is executing
    SUCCEEDED = "succeeded"  # handler returned normally
    FAILED = "failed"  # handler raised
    CANCELLED = "cancelled"  # operator cancelled (reserved for future)


# Terminal states — no further status transitions allowed.
TERMINAL_STATUSES = frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED})


class Job(Base):
    """One unit of work tracked by the job engine."""

    __tablename__ = "job"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Dispatch key. String, not enum, because the set of registered job
    # kinds grows over time (verify, ingest, copy, transcode, ...) and is
    # owned by the registry, not the database.
    kind: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    # Handler-specific. The handler validates the shape on dispatch.
    params: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    # Scheduler inputs: counted pool requirements and prerequisite job IDs.
    required_resources: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list
    )
    prerequisites: Mapped[list[int]] = mapped_column(JSON, nullable=False, default=list)

    status: Mapped[JobStatus] = mapped_column(
        String(16), nullable=False, default=JobStatus.PENDING, index=True
    )

    # Idempotency: handlers record their progress here so a crash mid-job
    # can resume from the last recorded step on the next run. Day-1
    # handlers (verify) are single-step and write the final result here.
    step_state: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    not_before: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, index=True
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    dedupe_key: Mapped[str | None] = mapped_column(String(256), nullable=True, unique=True)

    # Free-form on failure. Structured detail belongs in step_state /
    # the audit log (which doesn't exist yet — TODO once Sutradhara audit
    # surface lands).
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    started_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    def __repr__(self) -> str:
        return f"<Job id={self.id} kind={self.kind!r} status={self.status}>"
