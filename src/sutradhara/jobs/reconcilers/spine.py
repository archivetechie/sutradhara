"""Generic reconcile loop over durable condition rows.

``discover`` refreshes condition rows from catalog reality, while ``process``
uses the condition index as a bounded worklist, re-observes each due target, and
enqueues one ordinary job when the gate is open.
"""

from __future__ import annotations

import datetime as dt

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.jobs.models import LIVE_JOB_STATUS_VALUES, Job, ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    CONDITION_BLOCKED,
    CONDITION_OPEN,
    record_observation,
    reopen_condition,
)
from sutradhara.jobs.reconcilers.registry import get_reconciler
from sutradhara.jobs.tool_versions import current_tool_version


def discover(
    session: Session,
    domain: str,
    *,
    batch: int = 1000,
    cursor: int | None = None,
) -> int:
    """Refresh condition rows from a bounded batch of observed reality."""

    reconciler = get_reconciler(domain)
    count = 0
    for observation in reconciler.enumerate_targets(session, cursor, batch):
        record_observation(
            session,
            domain=domain,
            target_key=observation.target_key,
            desired=observation.desired,
            observed_state=observation.observed_state,
        )
        count += 1
    return count


def process(
    session: Session,
    domain: str,
    *,
    limit: int = 100,
) -> int:
    """Act on due workable conditions for ``domain``."""

    reconciler = get_reconciler(domain)
    processed = 0
    for condition in due_workable(session, domain, limit=limit):
        observation = reconciler.observe(session, condition.target_key)
        refreshed = record_observation(
            session,
            domain=domain,
            target_key=condition.target_key,
            desired=observation.desired,
            observed_state=observation.observed_state,
        )
        if gate_open(
            session,
            domain=domain,
            target_key=condition.target_key,
            condition=refreshed,
            desired=observation.desired,
        ):
            reconciler.reconcile_target(session, condition.target_key)
        processed += 1
    return processed


def reconcile(
    session: Session,
    domain: str,
    *,
    batch: int = 1000,
    cursor: int | None = None,
    limit: int = 100,
) -> tuple[int, int]:
    """Run one bounded discover pass followed by one bounded process pass."""

    reopen_version_bumped(session, domain)
    discovered = discover(session, domain, batch=batch, cursor=cursor)
    processed = process(session, domain, limit=limit)
    return discovered, processed


def reopen_version_bumped(session: Session, domain: str) -> int:
    """Reopen blocked conditions whose recorded tool version has changed."""

    reopened = 0
    rows = list(
        session.scalars(
            select(ReconciliationCondition)
            .where(
                ReconciliationCondition.domain == domain,
                ReconciliationCondition.condition == CONDITION_BLOCKED,
                ReconciliationCondition.blocked_tool_name.is_not(None),
            )
            .order_by(ReconciliationCondition.id)
        )
    )
    for row in rows:
        tool = row.blocked_tool_name
        if tool is None:
            continue
        current = current_tool_version(tool)
        if current == "unknown" or current == row.blocked_tool_version:
            continue
        previous = row.blocked_tool_version or "unknown"
        reopen_condition(
            session,
            row,
            actor="version-bump",
            note=f"{tool} version changed from {previous} to {current}",
        )
        reopened += 1
    return reopened


def due_workable(
    session: Session,
    domain: str,
    *,
    limit: int,
    now: dt.datetime | None = None,
) -> list[ReconciliationCondition]:
    """Return due open/backoff rows using ``ix_condition_work``."""

    due_at = now or _utcnow()
    return list(
        session.scalars(
            select(ReconciliationCondition)
            .where(
                ReconciliationCondition.domain == domain,
                ReconciliationCondition.condition.in_((CONDITION_OPEN, CONDITION_BACKOFF)),
                ReconciliationCondition.next_eligible_at <= due_at,
            )
            .order_by(ReconciliationCondition.next_eligible_at, ReconciliationCondition.id)
            .limit(limit)
        )
    )


def gate_open(
    session: Session,
    *,
    domain: str,
    target_key: str,
    condition: ReconciliationCondition,
    desired: bool,
    now: dt.datetime | None = None,
) -> bool:
    """Return true when a condition can enqueue a new attempt."""

    if not desired:
        return False
    if condition.condition not in (CONDITION_OPEN, CONDITION_BACKOFF):
        return False
    if condition.next_eligible_at is None:
        return False
    if _as_utc(condition.next_eligible_at) > _as_utc(now or _utcnow()):
        return False
    live_job_id = session.scalars(
        select(Job.id)
        .where(
            Job.recon_domain == domain,
            Job.recon_target_key == target_key,
            Job.status.in_(LIVE_JOB_STATUS_VALUES),
        )
        .limit(1)
    ).first()
    return live_job_id is None


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _as_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)
