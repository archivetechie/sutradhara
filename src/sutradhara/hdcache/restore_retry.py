"""Retry policy for agent restores: the cause table, the summary and the rule.

Design RSR (D2, D4, D6, D7). An agent item's history is its rows in
``restore_item_attempt`` ordered by ``attempt_no``. Each ended row has one
**class** from the table below, which is the single source of both
:func:`classify` (Python) and :func:`attempt_class_expression` (the SQL
``CASE``). :func:`attempt_summaries` folds the histories of many items in one
statement; :func:`summarize_rows` is its plain-Python twin over the same rows.
:func:`plan_next_offer` is the only place the retry rule lives. The text
composers are display, not policy; stored text is raw and read surfaces
sanitize it.
"""

from __future__ import annotations

import datetime as dt
import math
import os
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Final

from sqlalchemy import ColumnElement, and_, case, func, null, select
from sqlalchemy.orm import Session

from sutradhara.hdcache.models import RestoreItemAttempt

# ---- A2: the class table ----------------------------------------------------

CLASS_NONE: Final = "none"
CLASS_DELIVERED: Final = "delivered"
CLASS_NEUTRAL: Final = "neutral"
CLASS_WAIT: Final = "wait"
CLASS_COUNTED: Final = "counted"
CLASS_INTERRUPTED: Final = "interrupted"
CLASS_TERMINAL: Final = "terminal"

ATTEMPT_CLASSES: Final = (
    CLASS_NONE,
    CLASS_DELIVERED,
    CLASS_NEUTRAL,
    CLASS_WAIT,
    CLASS_COUNTED,
    CLASS_INTERRUPTED,
    CLASS_TERMINAL,
)

_COUNTED_CAUSES: Final = (
    "not_authorized",
    "configuration",
    "manifest_invalid",
    "resume_mismatch",
    "stale_lease",
    "protocol",
    "cache_disk_unavailable",
    "source_integrity",
    "source_unavailable",
    "server_error",
)
_WAIT_CAUSES: Final = ("capacity", "open_conflict", "lease_live")


@dataclass(frozen=True)
class ClassRow:
    """One row of the class table: an exact match on the end kind and cause.

    ``cause`` None matches a null cause. ``opened_from_done`` None matches
    either opening state; True or False matches rows opened from ``done`` or
    from any other state.
    """

    klass: str
    end_kind: str
    cause: str | None
    opened_from_done: bool | None = None


#: The one table both halves are generated from (design RSR, D4). Every legal
#: ``(end_kind, cause, opened from done)`` triple matches exactly one row.
CLASS_TABLE: Final[tuple[ClassRow, ...]] = (
    ClassRow(CLASS_NONE, "job_end", None, True),
    ClassRow(CLASS_DELIVERED, "job_end", None, False),
    ClassRow(CLASS_NEUTRAL, "job_end", "item_withdrawn"),
    ClassRow(CLASS_NEUTRAL, "error", "item_withdrawn"),
    *(ClassRow(CLASS_WAIT, "error", cause) for cause in _WAIT_CAUSES),
    *(ClassRow(CLASS_COUNTED, "error", cause) for cause in _COUNTED_CAUSES),
    ClassRow(CLASS_INTERRUPTED, "cancelled", None),
    ClassRow(CLASS_INTERRUPTED, "lost", None),
    ClassRow(CLASS_TERMINAL, "error", "tape_only"),
    ClassRow(CLASS_TERMINAL, "cancelled", "lease_expired"),
    ClassRow(CLASS_TERMINAL, "job_end", "lease_expired"),
)


def classify(end_kind: str | None, cause: str | None, opened_from_done: bool) -> str | None:
    """Return the class of an ended row, or None for an open row (``end_kind`` null)."""

    if end_kind is None:
        return None
    for row in CLASS_TABLE:
        if row.end_kind != end_kind or row.cause != cause:
            continue
        if row.opened_from_done is not None and row.opened_from_done != opened_from_done:
            continue
        return row.klass
    return None


def attempt_class_expression(
    end_kind: Any = None,
    cause: Any = None,
    opened_from_state: Any = None,
) -> ColumnElement[Any]:
    """The SQL ``CASE`` generated from :data:`CLASS_TABLE`; null for an open row.

    The three column expressions default to ``restore_item_attempt``'s own.
    """

    kind_col: Any = RestoreItemAttempt.end_kind if end_kind is None else end_kind
    cause_col: Any = RestoreItemAttempt.cause if cause is None else cause
    state_col: Any = (
        RestoreItemAttempt.opened_from_state if opened_from_state is None else opened_from_state
    )
    whens: list[tuple[ColumnElement[bool], str]] = []
    for row in CLASS_TABLE:
        conditions: list[ColumnElement[bool]] = [kind_col == row.end_kind]
        conditions.append(cause_col.is_(None) if row.cause is None else cause_col == row.cause)
        if row.opened_from_done is True:
            conditions.append(state_col == "done")
        elif row.opened_from_done is False:
            conditions.append(state_col != "done")
        whens.append((and_(*conditions), row.klass))
    return case(*whens, else_=null())


# ---- A3: the policy ----------------------------------------------------------


@dataclass(frozen=True)
class RetryPolicy:
    """The owner's numbers (2026-11-09): 30 s doubling to 30 minutes; eight and eight."""

    base_delay: dt.timedelta = dt.timedelta(seconds=30)
    max_delay: dt.timedelta = dt.timedelta(seconds=1800)
    max_failures: int = 8
    max_interruptions: int = 8
    wait_delay: dt.timedelta = dt.timedelta(seconds=30)

    def __post_init__(self) -> None:
        zero = dt.timedelta(0)
        if self.base_delay <= zero or self.max_delay <= zero or self.wait_delay <= zero:
            raise ValueError("restore retry delays must be positive")
        if self.max_delay < self.base_delay:
            raise ValueError("restore retry max delay must not be below the base delay")
        if self.max_failures <= 0 or self.max_interruptions <= 0:
            raise ValueError("restore retry thresholds must be positive")


_ENV_PREFIX: Final = "SUTRADHARA_RESTORE_RETRY_"


def _env_seconds(name: str, default: dt.timedelta) -> dt.timedelta:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number of seconds, got {raw!r}")
    try:
        return dt.timedelta(seconds=value)
    except OverflowError as exc:
        raise ValueError(f"{name} is too large: {raw!r}") from exc


def _env_count(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def retry_policy_from_env() -> RetryPolicy:
    """Build the policy from ``SUTRADHARA_RESTORE_RETRY_*``; refuses bad numbers (ValueError)."""

    defaults = RetryPolicy()
    return RetryPolicy(
        base_delay=_env_seconds(f"{_ENV_PREFIX}BASE_SECONDS", defaults.base_delay),
        max_delay=_env_seconds(f"{_ENV_PREFIX}MAX_SECONDS", defaults.max_delay),
        max_failures=_env_count(f"{_ENV_PREFIX}MAX_FAILURES", defaults.max_failures),
        max_interruptions=_env_count(f"{_ENV_PREFIX}MAX_INTERRUPTIONS", defaults.max_interruptions),
        wait_delay=_env_seconds(f"{_ENV_PREFIX}WAIT_SECONDS", defaults.wait_delay),
    )


# ---- A4: the summary ---------------------------------------------------------


@dataclass(frozen=True)
class SummaryRow:
    """What the summary keeps of one ended row (the API composes its last error from it)."""

    id: int
    end_kind: str
    klass: str
    cause: str | None
    message: str | None
    end_time: dt.datetime
    blocked_until: dt.datetime | None


@dataclass(frozen=True)
class AttemptSummary:
    """One item's history folded for :func:`plan_next_offer` (design RSR, D4).

    ``counted`` is *n* (every counted row), ``interruptions`` is *r* (the
    interrupted rows after the latest counted or delivered row), ``delivered``
    is *d*. ``wait`` and ``neutral`` rows neither count nor break *r*.
    """

    attempts: int = 0
    counted: int = 0
    interruptions: int = 0
    delivered: int = 0
    open: bool = False
    terminal: bool = False
    latest: SummaryRow | None = None
    latest_non_wait: SummaryRow | None = None


def _as_utc(value: dt.datetime) -> dt.datetime:
    """SQLite returns naive UTC; PostgreSQL returns aware. Compare on one footing."""

    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _optional_utc(value: dt.datetime | None) -> dt.datetime | None:
    return None if value is None else _as_utc(value)


def summarize_rows(rows: Iterable[RestoreItemAttempt]) -> AttemptSummary:
    """Plain-Python twin of :func:`attempt_summaries` over one item's rows."""

    ordered = sorted(rows, key=lambda row: row.attempt_no)
    counted = delivered = interruptions = 0
    is_open = terminal = False
    latest: SummaryRow | None = None
    latest_non_wait: SummaryRow | None = None
    for row in ordered:
        if row.end_kind is None:
            is_open = True
            continue
        klass = classify(row.end_kind, row.cause, row.opened_from_state == "done")
        if klass is None:
            raise ValueError(f"attempt {row.id} has no class: {row.end_kind!r}, {row.cause!r}")
        end_time = row.ended_at if row.ended_at is not None else row.closed_at
        if end_time is None:
            raise ValueError(f"attempt {row.id} ended without an end time")
        summary_row = SummaryRow(
            id=row.id,
            end_kind=row.end_kind,
            klass=klass,
            cause=row.cause,
            message=row.message,
            end_time=_as_utc(end_time),
            blocked_until=_optional_utc(row.blocked_until),
        )
        latest = summary_row
        if klass != CLASS_WAIT:
            latest_non_wait = summary_row
        if klass == CLASS_COUNTED:
            counted += 1
            interruptions = 0
        elif klass == CLASS_DELIVERED:
            delivered += 1
            interruptions = 0
        elif klass == CLASS_INTERRUPTED:
            interruptions += 1
        elif klass == CLASS_TERMINAL:
            terminal = True
    return AttemptSummary(
        attempts=len(ordered),
        counted=counted,
        interruptions=interruptions,
        delivered=delivered,
        open=is_open,
        terminal=terminal,
        latest=latest,
        latest_non_wait=latest_non_wait,
    )


_ROW_FIELDS: Final = ("id", "end_kind", "klass", "cause", "message", "end_time", "blocked_until")


def attempt_summaries_statement(item_ids: Iterable[int]) -> Any:
    """The one SELECT behind :func:`attempt_summaries` (exposed for compilation tests)."""

    attempt = RestoreItemAttempt
    classified = (
        select(
            attempt.id.label("id"),
            attempt.restore_request_item_id.label("item_id"),
            attempt.attempt_no.label("attempt_no"),
            attempt.end_kind.label("end_kind"),
            attempt.cause.label("cause"),
            attempt.message.label("message"),
            attempt.blocked_until.label("blocked_until"),
            func.coalesce(attempt.ended_at, attempt.closed_at).label("end_time"),
            attempt_class_expression().label("klass"),
        )
        .where(attempt.restore_request_item_id.in_(list(item_ids)))
        .cte("classified")
    )
    c = classified.c
    ended = c.end_kind.is_not(None)
    # Per item: the latest counted or delivered row (which bounds r), the
    # latest ended row and the latest ended row that is not a wait.
    marks = (
        select(
            c.item_id.label("item_id"),
            func.max(case((c.klass.in_([CLASS_COUNTED, CLASS_DELIVERED]), c.attempt_no))).label(
                "bound_no"
            ),
            func.max(case((ended, c.attempt_no))).label("latest_no"),
            func.max(case((and_(ended, c.klass != CLASS_WAIT), c.attempt_no))).label(
                "latest_non_wait_no"
            ),
        )
        .group_by(c.item_id)
        .cte("marks")
    )
    m = marks.c

    def flag(condition: ColumnElement[Any]) -> ColumnElement[Any]:
        return case((condition, 1), else_=0)

    def projected(mark: ColumnElement[Any], prefix: str) -> list[ColumnElement[Any]]:
        return [
            func.max(case((c.attempt_no == mark, getattr(c, name)))).label(f"{prefix}_{name}")
            for name in _ROW_FIELDS
        ]

    return (
        select(
            c.item_id,
            func.count().label("attempts"),
            func.sum(flag(c.klass == CLASS_COUNTED)).label("counted"),
            func.sum(
                flag(
                    and_(
                        c.klass == CLASS_INTERRUPTED,
                        c.attempt_no > func.coalesce(m.bound_no, 0),
                    )
                )
            ).label("interruptions"),
            func.sum(flag(c.klass == CLASS_DELIVERED)).label("delivered"),
            func.max(flag(c.end_kind.is_(None))).label("open"),
            func.max(flag(c.klass == CLASS_TERMINAL)).label("terminal"),
            *projected(m.latest_no, "latest"),
            *projected(m.latest_non_wait_no, "latest_non_wait"),
        )
        .select_from(classified)
        .join(marks, m.item_id == c.item_id)
        .group_by(c.item_id)
    )


def _row_from_mapping(mapping: Any, prefix: str) -> SummaryRow | None:
    row_id = mapping[f"{prefix}_id"]
    if row_id is None:
        return None
    return SummaryRow(
        id=int(row_id),
        end_kind=str(mapping[f"{prefix}_end_kind"]),
        klass=str(mapping[f"{prefix}_klass"]),
        cause=mapping[f"{prefix}_cause"],
        message=mapping[f"{prefix}_message"],
        end_time=_as_utc(mapping[f"{prefix}_end_time"]),
        blocked_until=_optional_utc(mapping[f"{prefix}_blocked_until"]),
    )


def attempt_summaries(session: Session, item_ids: Iterable[int]) -> dict[int, AttemptSummary]:
    """Summaries of many items in one statement; an item with no rows has ``attempts == 0``."""

    ids = sorted(set(item_ids))
    summaries = {item_id: AttemptSummary() for item_id in ids}
    if not ids:
        return summaries
    for mapping in session.execute(attempt_summaries_statement(ids)).mappings():
        summaries[int(mapping["item_id"])] = AttemptSummary(
            attempts=int(mapping["attempts"]),
            counted=int(mapping["counted"]),
            interruptions=int(mapping["interruptions"]),
            delivered=int(mapping["delivered"]),
            open=bool(mapping["open"]),
            terminal=bool(mapping["terminal"]),
            latest=_row_from_mapping(mapping, "latest"),
            latest_non_wait=_row_from_mapping(mapping, "latest_non_wait"),
        )
    return summaries


# ---- A5: the rule --------------------------------------------------------------

OFFER_OPEN: Final = "open"
OFFER_GIVE_UP: Final = "give_up"
OFFER_AT: Final = "at"
OFFER_NOW: Final = "now"

RULE_TERMINAL_CAUSE: Final = "terminal_cause"
RULE_MAX_FAILURES: Final = "max_failures"
RULE_MAX_INTERRUPTIONS: Final = "max_interruptions"


@dataclass(frozen=True)
class NextOffer:
    """What to do with an item: skip (open), give up, offer at a time, or offer now."""

    kind: str
    not_before: dt.datetime | None = None
    rule: str | None = None
    counted: int | None = None
    threshold: int | None = None
    decisive_attempt_id: int | None = None


def _backoff(policy: RetryPolicy, exponent: int) -> dt.timedelta:
    """``base * 2**exponent`` capped at ``max_delay``; the doubling stops at the ceiling."""

    delay = policy.base_delay
    for _ in range(exponent):
        if delay >= policy.max_delay:
            break
        delay = delay * 2
    return min(delay, policy.max_delay)


def plan_next_offer(summary: AttemptSummary, now: dt.datetime, policy: RetryPolicy) -> NextOffer:
    """The retry rule (design RSR, D4), pure. ``now`` is the database clock (D-RSRP-5)."""

    if summary.open:
        return NextOffer(OFFER_OPEN)
    latest = summary.latest
    # The decisive row of a give-up is the latest ended non-wait row (D-RSRP-9):
    # the terminal, counted or interrupted row that triggered the rule, never a
    # later wait row, so compose_detail renders the cause that ended the item.
    row = summary.latest_non_wait
    if row is not None:
        if summary.terminal:
            return NextOffer(OFFER_GIVE_UP, rule=RULE_TERMINAL_CAUSE, decisive_attempt_id=row.id)
        if summary.counted >= policy.max_failures:
            return NextOffer(
                OFFER_GIVE_UP,
                rule=RULE_MAX_FAILURES,
                counted=summary.counted,
                threshold=policy.max_failures,
                decisive_attempt_id=row.id,
            )
        if summary.interruptions >= policy.max_interruptions:
            return NextOffer(
                OFFER_GIVE_UP,
                rule=RULE_MAX_INTERRUPTIONS,
                counted=summary.interruptions,
                threshold=policy.max_interruptions,
                decisive_attempt_id=row.id,
            )

    not_before: dt.datetime | None = None
    if row is not None:
        if row.klass == CLASS_COUNTED:
            not_before = row.end_time + _backoff(policy, summary.counted - 1)
        elif row.klass == CLASS_INTERRUPTED and summary.interruptions >= 2:
            not_before = row.end_time + _backoff(policy, summary.interruptions - 2)
        elif row.klass == CLASS_NEUTRAL:
            not_before = row.end_time + policy.wait_delay
        # delivered, none: no delay
    if latest is not None and latest.klass == CLASS_WAIT:
        # Its own wait: until blocked_until when set (a past one gives none,
        # D-RSRP-4), else its end plus the wait delay; the item waits for the
        # later of its own wait and the delay above.
        own = (
            latest.blocked_until
            if latest.blocked_until is not None
            else latest.end_time + policy.wait_delay
        )
        not_before = own if not_before is None else max(own, not_before)
    if not_before is None or not_before <= _as_utc(now):
        return NextOffer(OFFER_NOW)
    return NextOffer(OFFER_AT, not_before=not_before)


# ---- A6: the text composer (D-RSRP-6) --------------------------------------------

MESSAGE_LIMIT: Final = 2000

#: Per-cause operator text. ``{message}`` is the raw stored message.
_CAUSE_TEXT: Final[dict[str, str]] = {
    "not_authorized": "not authorized: {message}",
    "configuration": "server configuration: {message}",
    "manifest_invalid": "the item's recorded path or size is invalid: {message}",
    "resume_mismatch": "the receiver's saved progress is for a different version of this item",
    "stale_lease": "the receiver held an outdated delivery lease",
    "protocol": "the receiver sent an invalid resume request",
    # Never the raised text, which says "deferred to RM3".
    "cache_disk_unavailable": "the cache disk holding this item is unavailable",
    "source_integrity": "the archive copies failed their integrity check: {message}",
    "source_unavailable": "no readable source copy: {message}",
    "server_error": "server error: {message}",
    "capacity": "restore capacity is full",
    "open_conflict": "another open of this item was in progress",
    "lease_live": "an earlier delivery's lease is still live",
    "item_withdrawn": "the item was withdrawn while it was being opened",
    # The owner's two sentences, verbatim.
    "tape_only": (
        "this item's usable copies are on tape, which agent delivery cannot read yet; "
        "request server-local delivery"
    ),
    "lease_expired": (
        "the transfer took longer than the 30-minute delivery lease, which is not renewed "
        "yet; request server-local delivery"
    ),
}
_INTERRUPTED_TEXT: Final = "the transfer was interrupted"


def bound_message(text: str) -> str:
    """Bound a stored message to the column's 2000 characters."""

    return text[:MESSAGE_LIMIT]


def compose_last_error(end_kind: str, cause: str | None, message: str | None) -> str | None:
    """Operator text for one ended row; None when nothing went wrong (a ``job_end`` with no cause)."""

    if cause is None:
        return None if end_kind == "job_end" else _INTERRUPTED_TEXT
    template = _CAUSE_TEXT[cause]
    if "{message}" not in template:
        return template
    if message is None or message == "":
        return template.replace(": {message}", "")
    return template.format(message=message)


def compose_detail(rule: str, counted: int | None, decisive: SummaryRow) -> str:
    """The item's ``detail`` once the policy gave up (design RSR, D6)."""

    last_error = compose_last_error(decisive.end_kind, decisive.cause, decisive.message)
    if rule == RULE_TERMINAL_CAUSE:
        return last_error or _INTERRUPTED_TEXT
    if rule == RULE_MAX_FAILURES:
        return f"gave up after {counted} failed tries: {last_error}"
    if rule == RULE_MAX_INTERRUPTIONS:
        return f"gave up after {counted} interrupted transfers in a row"
    raise ValueError(f"unknown give-up rule {rule!r}")
