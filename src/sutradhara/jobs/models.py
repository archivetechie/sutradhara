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
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
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
LIVE_JOB_STATUSES = (JobStatus.PENDING, JobStatus.RUNNING, JobStatus.QUEUED)
LIVE_JOB_STATUS_VALUES = tuple(status.value for status in LIVE_JOB_STATUSES)


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
    dedupe_key: Mapped[str | None] = mapped_column(String(256), nullable=True)
    recon_domain: Mapped[str | None] = mapped_column(String(64), nullable=True)
    recon_target_key: Mapped[str | None] = mapped_column(String(256), nullable=True)

    # Free-form current failure reason. Structured per-run detail belongs in
    # step_state and the append-only JobAttempt audit log.
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    started_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    def __repr__(self) -> str:
        return f"<Job id={self.id} kind={self.kind!r} status={self.status}>"


class JobAttempt(Base):
    """Append-only audit transcript for one completed run of a job.

    The live ``job`` row remains the queue/current-state record. ``job_attempt``
    keeps the durable execution history that later condition projections can
    summarize, even after terminal job rows are pruned.
    """

    __tablename__ = "job_attempt"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("job.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    job_kind: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    outcome: Mapped[JobStatus] = mapped_column(String(16), nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    granted_leases: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict, server_default=text("'{}'")
    )
    worker_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    code_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    detail: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict, server_default=text("'{}'")
    )
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    def __repr__(self) -> str:
        return (
            f"<JobAttempt id={self.id} job_id={self.job_id} "
            f"kind={self.job_kind!r} outcome={self.outcome}>"
        )


class ReconciliationCondition(Base):
    """Durable per-target reconciliation projection.

    Rows are keyed by ``(domain, target_key)`` and summarize observed reality plus
    the latest attempt outcome so reconcilers can use a small indexed worklist
    instead of scanning job attempts.
    """

    __tablename__ = "reconciliation_condition"
    __table_args__ = (
        UniqueConstraint("domain", "target_key", name="uq_recon_condition_domain_target"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    domain: Mapped[str] = mapped_column(String(64), nullable=False)
    target_key: Mapped[str] = mapped_column(String(256), nullable=False)
    observed_state: Mapped[str] = mapped_column(String(64), nullable=False)
    condition: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_eligible_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    blocked_tool_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    blocked_tool_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_attempt_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("job_attempt.id", ondelete="SET NULL"),
        nullable=True,
    )
    last_attempt_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_success_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    def __repr__(self) -> str:
        return (
            f"<ReconciliationCondition domain={self.domain!r} "
            f"target={self.target_key!r} condition={self.condition!r}>"
        )


class ConditionComponent(Base):
    """Indexed component snapshot retained independently of job attempts."""

    __tablename__ = "condition_component"
    __table_args__ = (
        UniqueConstraint(
            "condition_id",
            "component",
            name="uq_condition_component_condition_component",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    condition_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("reconciliation_condition.id", ondelete="CASCADE"),
        nullable=False,
    )
    component: Mapped[str] = mapped_column(String(2048), nullable=False, index=True)


Index(
    "uq_job_dedupe_key_live",
    Job.dedupe_key,
    unique=True,
    sqlite_where=Job.status.in_(LIVE_JOB_STATUS_VALUES),
    postgresql_where=Job.status.in_(LIVE_JOB_STATUS_VALUES),
)

Index(
    "ix_job_recon_live",
    Job.recon_domain,
    Job.recon_target_key,
    sqlite_where=Job.status.in_(LIVE_JOB_STATUS_VALUES),
    postgresql_where=Job.status.in_(LIVE_JOB_STATUS_VALUES),
)

Index(
    "ix_condition_work",
    ReconciliationCondition.domain,
    ReconciliationCondition.condition,
    ReconciliationCondition.next_eligible_at,
)
