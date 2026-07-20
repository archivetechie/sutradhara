"""Condition projection helpers for the reconciler spine.

The helpers implement the P0.3 two-axis contract: observations own reality and
create condition rows; job attempts update only the attempt axis after
``run_one`` records a ``job_attempt``. Callers own transactions; this module
flushes but never commits or rolls back.
"""

from __future__ import annotations

import datetime as dt

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from sutradhara.jobs.config import DEFAULT_MAX_BACKOFF_SECONDS, jittered_backoff_seconds
from sutradhara.jobs.models import (
    ConditionComponent,
    JobAttempt,
    JobStatus,
    ReconciliationCondition,
)

CONDITION_OPEN = "open"
CONDITION_BACKOFF = "backoff"
CONDITION_BLOCKED = "blocked"
CONDITION_SUPPRESSED = "suppressed"
CONDITION_SATISFIED = "satisfied"

OBSERVED_PRESENT = "present"
OBSERVED_MISSING = "missing"

WORKABLE_CONDITIONS = (CONDITION_OPEN, CONDITION_BACKOFF)
HELD_CONDITIONS = (CONDITION_BACKOFF, CONDITION_BLOCKED, CONDITION_SUPPRESSED)

DEFAULT_BACKOFF_SECONDS = 60
DEFAULT_BACKOFF_GIVE_UP_ATTEMPTS = 3
DEFAULT_CONDITION_MAX_BACKOFF_SECONDS = DEFAULT_MAX_BACKOFF_SECONDS


class ReconciliationInvariantError(RuntimeError):
    """A reconciler condition invariant was violated."""


def record_observation(
    session: Session,
    *,
    domain: str,
    target_key: str,
    desired: bool,
    observed_state: str,
    reason: str | None = None,
    message: str | None = None,
) -> ReconciliationCondition:
    """Record Axis-A reality for one reconciliation target.

    ``record_observation`` is the only writer that creates condition rows because
    it always has the non-null ``observed_state``. It never commits.
    """

    now = _utcnow()
    row = _get_condition(session, domain, target_key)

    if row is None:
        row = ReconciliationCondition(
            domain=domain,
            target_key=target_key,
            observed_state=observed_state,
            condition=CONDITION_SATISFIED,
            observed_at=now,
            condition_changed_at=now,
            updated_at=now,
        )
        session.add(row)

    row.observed_state = observed_state
    row.observed_at = now

    if not desired:
        _mark_satisfied(row, observed_state=observed_state, now=now)
        session.flush([row])
        return row

    if observed_state == OBSERVED_PRESENT:
        if row.condition == CONDITION_SUPPRESSED:
            pass
        else:
            _mark_satisfied(row, observed_state=observed_state, now=now)
        session.flush([row])
        return row

    if row.condition in HELD_CONDITIONS:
        session.flush([row])
        return row

    _set_condition(row, CONDITION_OPEN, now)
    row.reason = reason
    row.message = message
    if row.next_eligible_at is None:
        row.next_eligible_at = now
    session.flush([row])
    return row


def record_condition(
    session: Session,
    *,
    domain: str,
    target_key: str,
    condition: str | None = None,
    reason: str | None = None,
    message: str | None = None,
    attempt: JobAttempt | None = None,
    next_eligible_at: dt.datetime | None = None,
    blocked_tool: tuple[str, str] | None = None,
    auto_block: bool = True,
) -> ReconciliationCondition:
    """Record Axis-B attempt outcome for one existing condition row.

    Axis B cannot insert because it has no observed state. A missing row means a
    reconciler-backed job was run without a preceding observation, which is an
    invariant error.
    """

    row = _get_condition(session, domain, target_key)
    if row is None:
        raise ReconciliationInvariantError(
            f"no reconciliation condition for domain={domain!r} target={target_key!r}"
        )

    now = _utcnow()
    was_blocked = row.condition == CONDITION_BLOCKED
    _link_attempt(row, attempt, now=now)

    if condition is None:
        if attempt is not None and attempt.outcome == JobStatus.SUCCEEDED:
            row.last_success_at = attempt.finished_at
        session.flush([row])
        return row

    if condition == CONDITION_BACKOFF:
        next_count = row.attempt_count + 1
        row.attempt_count = next_count
        if auto_block and next_count >= DEFAULT_BACKOFF_GIVE_UP_ATTEMPTS:
            _set_condition(row, CONDITION_BLOCKED, now)
            row.reason = reason or "give-up"
            row.message = message
            row.next_eligible_at = None
            if not was_blocked:
                _snapshot_blocked_components(session, row, attempt)
        else:
            _set_condition(row, CONDITION_BACKOFF, now)
            row.reason = reason
            row.message = message
            row.next_eligible_at = next_eligible_at or _default_backoff_due(now, next_count)
        _set_blocked_tool(row, blocked_tool)
        session.flush([row])
        return row

    if condition == CONDITION_BLOCKED:
        _set_condition(row, CONDITION_BLOCKED, now)
        row.reason = reason
        row.message = message
        row.next_eligible_at = None
        _set_blocked_tool(row, blocked_tool)
        if not was_blocked:
            _snapshot_blocked_components(session, row, attempt)
        session.flush([row])
        return row

    raise ValueError(f"Axis-B condition must be 'backoff', 'blocked', or None; got {condition!r}")


def reopen_condition(
    session: Session,
    row: ReconciliationCondition,
    *,
    actor: str,
    note: str,
) -> ReconciliationCondition:
    """Reopen a blocked condition and make it immediately workable."""

    now = _utcnow()
    _set_condition(row, CONDITION_OPEN, now)
    row.reason = None
    row.message = note
    row.blocked_tool_name = None
    row.blocked_tool_version = None
    row.attempt_count = 0
    row.next_eligible_at = now
    row.reopened_by = actor
    row.reopened_at = now
    session.flush([row])
    return row


def _get_condition(
    session: Session,
    domain: str,
    target_key: str,
) -> ReconciliationCondition | None:
    return session.scalars(
        select(ReconciliationCondition).where(
            ReconciliationCondition.domain == domain,
            ReconciliationCondition.target_key == target_key,
        )
    ).one_or_none()


def _mark_satisfied(
    row: ReconciliationCondition,
    *,
    observed_state: str,
    now: dt.datetime,
) -> None:
    row.observed_state = observed_state
    _set_condition(row, CONDITION_SATISFIED, now)
    row.reason = None
    row.message = None
    row.attempt_count = 0
    row.next_eligible_at = None
    row.blocked_tool_name = None
    row.blocked_tool_version = None


def _set_condition(
    row: ReconciliationCondition,
    value: str,
    now: dt.datetime,
) -> None:
    """Advance the condition clock only for a real value transition."""

    if row.condition != value:
        row.condition = value
        row.condition_changed_at = now


def _link_attempt(
    row: ReconciliationCondition,
    attempt: JobAttempt | None,
    *,
    now: dt.datetime,
) -> None:
    if attempt is None:
        return
    row.last_attempt_id = attempt.id
    row.last_attempt_at = attempt.finished_at or now


def _set_blocked_tool(
    row: ReconciliationCondition,
    blocked_tool: tuple[str, str] | None,
) -> None:
    if blocked_tool is None:
        row.blocked_tool_name = None
        row.blocked_tool_version = None
        return
    row.blocked_tool_name, row.blocked_tool_version = blocked_tool


def _snapshot_blocked_components(
    session: Session,
    row: ReconciliationCondition,
    attempt: JobAttempt | None,
) -> None:
    """Replace the parked condition's indexed component snapshot."""

    session.flush([row])
    session.execute(delete(ConditionComponent).where(ConditionComponent.condition_id == row.id))
    raw_components = [] if attempt is None else attempt.detail.get("components", [])
    if not isinstance(raw_components, list):
        raw_components = []
    components = sorted(
        {component for component in raw_components if isinstance(component, str) and component}
    )
    session.add_all(
        [ConditionComponent(condition_id=row.id, component=component) for component in components]
    )
    session.flush()


def _default_backoff_due(now: dt.datetime, attempt_count: int) -> dt.datetime:
    delay = DEFAULT_BACKOFF_SECONDS * (2 ** max(attempt_count - 1, 0))
    return now + dt.timedelta(
        seconds=jittered_backoff_seconds(
            delay,
            max_seconds=DEFAULT_CONDITION_MAX_BACKOFF_SECONDS,
        )
    )


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
