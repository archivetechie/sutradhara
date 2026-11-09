"""RSR-1a: the attempt ledger's shape rules, the retry policy and the summary query."""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import random
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, delete, insert, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateIndex

from sutradhara.catalog.models import LogicalAsset
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.hdcache.models import (
    RESTORE_ATTEMPT_CAUSES,
    RestoreItemAttempt,
    RestoreItemGiveup,
    RestoreRequest,
    RestoreRequestItem,
    RestoreServerInstance,
)
from sutradhara.hdcache.restore_retry import (
    ATTEMPT_CLASSES,
    CLASS_TABLE,
    RetryPolicy,
    SummaryRow,
    attempt_class_expression,
    attempt_summaries,
    attempt_summaries_statement,
    bound_message,
    classify,
    compose_detail,
    compose_last_error,
    plan_next_offer,
    retry_policy_from_env,
    summarize_rows,
)

T0 = dt.datetime(2026, 11, 9, 12, 0, 0, tzinfo=dt.UTC)


def _legal_end_shapes() -> list[tuple[str, str | None]]:
    """Every (end_kind, cause) pair the D2 CHECKs admit for an ended row."""

    shapes: list[tuple[str, str | None]] = [
        ("job_end", None),
        ("job_end", "lease_expired"),
        ("job_end", "item_withdrawn"),
        ("cancelled", None),
        ("cancelled", "lease_expired"),
        ("lost", None),
    ]
    shapes.extend(("error", cause) for cause in RESTORE_ATTEMPT_CAUSES if cause != "lease_expired")
    return shapes


@pytest.fixture(scope="module")
def engine(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Engine]:
    root: Path = tmp_path_factory.mktemp("restore-retry")
    engine = make_engine(f"sqlite:///{root / 'catalog.db'}")
    create_all(engine)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="module")
def instance_id(engine: Engine) -> int:
    with session_scope(engine) as session:
        row = RestoreServerInstance(
            host="test", pid=1, code_version="test", started_at=T0, heartbeat_at=T0
        )
        session.add(row)
        session.flush()
        return row.id


def _seed_item(engine: Engine, suffix: str) -> int:
    digest = hashlib.sha256(suffix.encode()).digest()
    with session_scope(engine) as session:
        session.add(LogicalAsset(content_sha256=digest, size_bytes=1))
        session.add(
            RestoreRequest(
                id=f"request-{suffix}",
                identity="ada",
                destination_id="dest",
                delivery_mode="server_local",
            )
        )
        session.flush()
        item = RestoreRequestItem(
            request_id=f"request-{suffix}", content_sha256=digest, artifactclass="ac"
        )
        session.add(item)
        session.flush()
        return item.id


def _attempt_values(
    item_id: int, instance_id: int, attempt_no: int, **overrides: Any
) -> dict[str, Any]:
    values: dict[str, Any] = {
        "restore_request_item_id": item_id,
        "attempt_no": attempt_no,
        "receiver_device_id": "receiver",
        "server_instance_id": instance_id,
        "opened_from_state": "queued",
        "opened_at": T0,
        "code_version": "test",
    }
    values.update(overrides)
    return values


def _ended(end_kind: str, cause: str | None, **more: Any) -> dict[str, Any]:
    values: dict[str, Any] = {"end_kind": end_kind, "cause": cause}
    if end_kind == "lost":
        values["closed_at"] = T0
    else:
        values["ended_at"] = T0
    values.update(more)
    return values


def _insert_attempt(engine: Engine, values: dict[str, Any]) -> int:
    with engine.begin() as conn:
        result = conn.execute(insert(RestoreItemAttempt).values(**values))
        return int(result.inserted_primary_key[0])


# ---- T2: the D2 CHECKs -------------------------------------------------------

_VIOLATIONS: list[tuple[str, dict[str, Any]]] = [
    ("attempt_no", {"attempt_no": 0}),
    ("opened_from_state", {"opened_from_state": "failed"}),
    ("probe_source", {"probe_source": "tape"}),
    ("probe_candidates_tried", {"probe_candidates_tried": -1}),
    ("lease_generation", {"lease_generation": 0}),
    ("bytes_sent", {"bytes_sent": -1}),
    ("end_kind_vocabulary", {"end_kind": "finished", "ended_at": T0}),
    ("cause_vocabulary", _ended("error", "something_else")),
    ("message_length", _ended("error", "server_error", message="x" * 2001)),
    ("end_kind_without_time", {"end_kind": "job_end"}),
    ("time_without_end_kind", {"ended_at": T0}),
    # Only ck_restore_item_attempt_one_end_time rejects this one.
    ("lost_both_end_times", {"end_kind": "lost", "ended_at": T0, "closed_at": T0}),
    ("lost_with_ended_at", {"end_kind": "lost", "ended_at": T0}),
    ("job_end_with_closed_at", {"end_kind": "job_end", "closed_at": T0}),
    ("open_with_cause", {"cause": "capacity"}),
    ("error_without_cause", _ended("error", None)),
    ("error_lease_expired", _ended("error", "lease_expired")),
    ("lost_with_cause", _ended("lost", "server_error")),
    ("cancelled_counted_cause", _ended("cancelled", "server_error")),
    ("job_end_counted_cause", _ended("job_end", "server_error")),
    ("blocked_until_without_lease_live", _ended("error", "capacity", blocked_until=T0)),
    # A NULL cause must not slip through the blocked_until rule (SQL three-valued logic).
    ("blocked_until_on_open_row", {"blocked_until": T0}),
    ("blocked_until_on_cancelled_null_cause", _ended("cancelled", None, blocked_until=T0)),
]


@pytest.mark.parametrize(("rule", "overrides"), _VIOLATIONS, ids=[v[0] for v in _VIOLATIONS])
def test_attempt_constraint_rejects_one_violation(
    engine: Engine, instance_id: int, rule: str, overrides: dict[str, Any]
) -> None:
    item_id = _seed_item(engine, f"violation-{rule}")
    with pytest.raises(IntegrityError):
        _insert_attempt(engine, {**_attempt_values(item_id, instance_id, 1), **overrides})
    # The violating row must be the only reason: the same row without it inserts.
    _insert_attempt(engine, _attempt_values(item_id, instance_id, 1))


def test_attempt_constraint_admits_every_legal_shape(engine: Engine, instance_id: int) -> None:
    item_id = _seed_item(engine, "legal-shapes")
    shapes = _legal_end_shapes()
    assert len(shapes) == 21
    for attempt_no, (end_kind, cause) in enumerate(shapes, start=1):
        extra: dict[str, Any] = {}
        if cause == "lease_live":
            extra["blocked_until"] = T0
        _insert_attempt(
            engine,
            _attempt_values(
                item_id,
                instance_id,
                attempt_no,
                probe_source="cache",
                probe_candidates_tried=0,
                probe_ended_at=T0,
                lease_generation=1,
                bytes_sent=0,
                message="m",
                **_ended(end_kind, cause, **extra),
            ),
        )
    # And the open shape, with the optional columns null.
    _insert_attempt(engine, _attempt_values(item_id, instance_id, len(shapes) + 1))


def test_attempt_constraint_blocks_item_delete_and_giveup_shape(
    engine: Engine, instance_id: int
) -> None:
    item_id = _seed_item(engine, "restrict")
    attempt_id = _insert_attempt(
        engine, _attempt_values(item_id, instance_id, 1, **_ended("error", "tape_only"))
    )
    with pytest.raises(IntegrityError), engine.begin() as conn:
        conn.execute(delete(RestoreRequestItem).where(RestoreRequestItem.id == item_id))
    giveup = {
        "restore_request_item_id": item_id,
        "decisive_attempt_id": attempt_id,
        "rule": "terminal_cause",
        "decided_at": T0,
        "code_version": "test",
    }
    with pytest.raises(IntegrityError), engine.begin() as conn:
        conn.execute(insert(RestoreItemGiveup).values(**giveup, counted=1))
    with pytest.raises(IntegrityError), engine.begin() as conn:
        conn.execute(insert(RestoreItemGiveup).values(**{**giveup, "rule": "max_failures"}))
    with engine.begin() as conn:
        conn.execute(insert(RestoreItemGiveup).values(**giveup))
    # A second give-up for the same item is refused (one decision per item).
    with pytest.raises(IntegrityError), engine.begin() as conn:
        conn.execute(
            insert(RestoreItemGiveup).values(
                **{**giveup, "rule": "max_failures", "counted": 8, "threshold": 8}
            )
        )


def test_attempt_constraint_giveup_rule_legal_numbers_and_duplicate_attempt_no(
    engine: Engine, instance_id: int
) -> None:
    item_id = _seed_item(engine, "giveup-legal")
    attempt_id = _insert_attempt(
        engine, _attempt_values(item_id, instance_id, 1, **_ended("error", "server_error"))
    )
    legal = {
        "restore_request_item_id": item_id,
        "decisive_attempt_id": attempt_id,
        "rule": "max_failures",
        "counted": 8,
        "threshold": 8,
        "decided_at": T0,
        "code_version": "test",
    }
    # Only ck_restore_item_giveup_rule rejects an unknown rule with both numbers set.
    with pytest.raises(IntegrityError), engine.begin() as conn:
        conn.execute(insert(RestoreItemGiveup).values(**{**legal, "rule": "other"}))
    with engine.begin() as conn:
        conn.execute(insert(RestoreItemGiveup).values(**legal))
    # uq_restore_item_attempt_item_no: a second row with the same attempt_no is refused.
    with pytest.raises(IntegrityError):
        _insert_attempt(
            engine, _attempt_values(item_id, instance_id, 1, **_ended("cancelled", None))
        )
    _insert_attempt(engine, _attempt_values(item_id, instance_id, 2, **_ended("cancelled", None)))


# ---- T3: one open attempt per item -------------------------------------------


def test_open_index_allows_one_open_attempt_per_item(engine: Engine, instance_id: int) -> None:
    item_id = _seed_item(engine, "open-index")
    first = _insert_attempt(engine, _attempt_values(item_id, instance_id, 1))
    with pytest.raises(IntegrityError):
        _insert_attempt(engine, _attempt_values(item_id, instance_id, 2))
    with engine.begin() as conn:
        conn.execute(
            RestoreItemAttempt.__table__.update()
            .where(RestoreItemAttempt.id == first)
            .values(end_kind="cancelled", ended_at=T0)
        )
    second = _insert_attempt(engine, _attempt_values(item_id, instance_id, 2))
    with engine.connect() as conn:
        rows = conn.execute(
            select(RestoreItemAttempt.id).where(
                RestoreItemAttempt.restore_request_item_id == item_id
            )
        ).scalars()
        assert set(rows) == {first, second}


def test_open_index_is_partial_on_postgresql() -> None:
    index = next(
        index
        for index in RestoreItemAttempt.__table__.indexes
        if index.name == "uq_restore_item_attempt_open"
    )
    ddl = str(CreateIndex(index).compile(dialect=postgresql.dialect()))
    assert "WHERE ended_at IS NULL AND closed_at IS NULL" in ddl
    assert ddl.startswith("CREATE UNIQUE INDEX")


# ---- T4 (Python half): the class table -----------------------------------------


def _legal_triples() -> list[tuple[str, str | None, bool]]:
    return [
        (end_kind, cause, from_done)
        for end_kind, cause in _legal_end_shapes()
        for from_done in (True, False)
    ]


def test_class_table_maps_every_legal_shape_to_exactly_one_class() -> None:
    assert len(_legal_triples()) == 42
    for end_kind, cause, from_done in _legal_triples():
        matching = [
            row
            for row in CLASS_TABLE
            if row.end_kind == end_kind
            and row.cause == cause
            and (row.opened_from_done is None or row.opened_from_done == from_done)
        ]
        assert len(matching) == 1, (end_kind, cause, from_done)
        assert classify(end_kind, cause, from_done) == matching[0].klass
        assert matching[0].klass in ATTEMPT_CLASSES
    assert classify(None, None, False) is None
    assert classify("job_end", None, True) == "none"
    assert classify("job_end", None, False) == "delivered"
    assert classify("error", "tape_only", False) == "terminal"
    assert classify("lost", None, False) == "interrupted"


# ---- T5: the policy ----------------------------------------------------------------


class _Row:
    """A duck-typed attempt row for the Python twin (what summarize_rows reads)."""

    _ids = iter(range(1, 1_000_000))

    def __init__(
        self,
        attempt_no: int,
        end_kind: str | None,
        cause: str | None = None,
        *,
        at: dt.datetime | None = None,
        opened_from_state: str = "queued",
        blocked_until: dt.datetime | None = None,
        message: str | None = None,
    ) -> None:
        self.id = next(self._ids)
        self.attempt_no = attempt_no
        self.end_kind = end_kind
        self.cause = cause
        self.opened_from_state = opened_from_state
        self.blocked_until = blocked_until
        self.message = message
        when = T0 + dt.timedelta(minutes=attempt_no) if at is None else at
        self.ended_at = None
        self.closed_at = None
        if end_kind == "lost":
            self.closed_at = when
        elif end_kind is not None:
            self.ended_at = when


def _history(*shapes: tuple[str | None, str | None], **kw: Any) -> list[_Row]:
    return [_Row(no, kind, cause, **kw) for no, (kind, cause) in enumerate(shapes, start=1)]


def _offer(rows: list[Any], now: dt.datetime | None = None, policy: RetryPolicy | None = None):
    return plan_next_offer(summarize_rows(rows), now or T0, policy or RetryPolicy())


COUNTED = ("error", "server_error")
INTERRUPTED = ("cancelled", None)
LOST = ("lost", None)
DELIVERED = ("job_end", None)
NEUTRAL = ("job_end", "item_withdrawn")
WAIT = ("error", "capacity")


def test_policy_counted_delays_double_to_the_ceiling_and_sum_to_3690() -> None:
    expected = [30, 60, 120, 240, 480, 960, 1800]
    seen = []
    for n in range(1, 8):
        rows = _history(*([COUNTED] * n))
        offer = _offer(rows)
        assert offer.kind == "at"
        assert offer.not_before is not None
        seen.append(int((offer.not_before - rows[-1].ended_at).total_seconds()))
    assert seen == expected
    assert sum(seen) == 3690


def test_policy_counts_every_counted_cause_including_stale_lease() -> None:
    for cause in (
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
    ):
        rows = _history(*([("error", cause)] * 8))
        offer = _offer(rows)
        assert offer.kind == "give_up", cause
        assert (offer.rule, offer.counted, offer.threshold) == ("max_failures", 8, 8)
        assert offer.decisive_attempt_id == rows[-1].id


def test_policy_gives_up_at_the_eighth_counted_failure_not_the_seventh() -> None:
    assert _offer(_history(*([COUNTED] * 7))).kind == "at"
    offer = _offer(_history(*([COUNTED] * 8)))
    assert offer.kind == "give_up"
    assert offer.rule == "max_failures"


def test_policy_gives_up_at_the_eighth_interruption_in_a_row() -> None:
    seven = _history(*([INTERRUPTED] * 7))
    assert _offer(seven).kind == "at"
    offer = _offer(_history(*([INTERRUPTED] * 4), *([LOST] * 4)))
    assert offer.kind == "give_up"
    assert (offer.rule, offer.counted, offer.threshold) == ("max_interruptions", 8, 8)
    # A counted row between them ends the run: no give-up.
    assert _offer(_history(*([INTERRUPTED] * 7), COUNTED, *([INTERRUPTED] * 7))).kind == "at"


@pytest.mark.parametrize(
    "terminal",
    [("error", "tape_only"), ("cancelled", "lease_expired"), ("job_end", "lease_expired")],
)
def test_policy_gives_up_at_once_on_a_terminal_row(terminal: tuple[str, str]) -> None:
    rows = _history(terminal)
    offer = _offer(rows)
    assert offer.kind == "give_up"
    assert offer.rule == "terminal_cause"
    assert (offer.counted, offer.threshold) == (None, None)
    assert offer.decisive_attempt_id == rows[-1].id
    # Even after a delivery and with a later wait row: the decisive row is the
    # latest ended non-wait row (D-RSRP-9), so the detail carries the terminal cause.
    rows = _history(DELIVERED, terminal, WAIT)
    offer = _offer(rows)
    assert offer.kind == "give_up"
    assert offer.decisive_attempt_id == rows[1].id
    decisive = summarize_rows(rows).latest_non_wait
    assert decisive is not None
    assert decisive.id == rows[1].id
    assert compose_detail("terminal_cause", None, decisive) == compose_last_error(
        terminal[0], terminal[1], None
    )


def test_policy_first_interruption_reoffers_now_and_later_ones_back_off() -> None:
    assert _offer(_history(INTERRUPTED)).kind == "now"
    assert _offer(_history(LOST)).kind == "now"
    for r, delay in ((2, 30), (3, 60), (4, 120), (7, 960)):
        rows = _history(*([INTERRUPTED] * r))
        offer = _offer(rows)
        assert offer.kind == "at"
        assert offer.not_before == rows[-1].ended_at + dt.timedelta(seconds=delay)


def test_policy_wait_and_neutral_rows_neither_count_nor_break_a_run() -> None:
    rows = _history(INTERRUPTED, WAIT, INTERRUPTED, NEUTRAL, INTERRUPTED)
    summary = summarize_rows(rows)
    assert (summary.counted, summary.interruptions, summary.delivered) == (0, 3, 0)
    rows = _history(COUNTED, WAIT, NEUTRAL, COUNTED, WAIT)
    summary = summarize_rows(rows)
    assert (summary.counted, summary.interruptions) == (2, 0)
    # Twelve waits never give up.
    assert _offer(_history(*([WAIT] * 12)), now=T0 + dt.timedelta(days=1)).kind == "now"
    assert _offer(_history(*([NEUTRAL] * 12)), now=T0 + dt.timedelta(days=1)).kind == "now"


def test_policy_counted_or_delivered_row_ends_an_interruption_run() -> None:
    rows = _history(INTERRUPTED, INTERRUPTED, INTERRUPTED, COUNTED)
    assert summarize_rows(rows).interruptions == 0
    rows = _history(INTERRUPTED, INTERRUPTED, DELIVERED, INTERRUPTED)
    summary = summarize_rows(rows)
    assert (summary.interruptions, summary.delivered) == (1, 1)
    assert _offer(rows).kind == "now"


def test_policy_neutral_row_waits_the_wait_delay() -> None:
    rows = _history(COUNTED, NEUTRAL)
    offer = _offer(rows)
    assert offer.kind == "at"
    assert offer.not_before == rows[-1].ended_at + dt.timedelta(seconds=30)


def test_policy_wait_row_after_counted_takes_the_later_of_the_two() -> None:
    counted_end = T0
    rows = [
        _Row(1, *COUNTED, at=counted_end),
        _Row(2, *COUNTED, at=counted_end),
        _Row(3, *COUNTED, at=counted_end),  # n = 3: 120 s after this end
        _Row(4, *WAIT, at=counted_end + dt.timedelta(seconds=10)),  # own: +40 s
    ]
    offer = _offer(rows)
    assert offer.kind == "at"
    assert offer.not_before == counted_end + dt.timedelta(seconds=120)
    # The wait's own time wins when it is later.
    rows[3] = _Row(4, *WAIT, at=counted_end + dt.timedelta(seconds=100))
    offer = _offer(rows)
    assert offer.not_before == counted_end + dt.timedelta(seconds=130)
    # The counted delay still applies when the wait row is the only later row.
    assert _offer(rows, now=counted_end + dt.timedelta(seconds=129)).kind == "at"
    assert _offer(rows, now=counted_end + dt.timedelta(seconds=130)).kind == "now"


def test_policy_blocked_until_future_past_and_null() -> None:
    future = T0 + dt.timedelta(minutes=20)
    rows = [_Row(1, "error", "lease_live", at=T0, blocked_until=future)]
    offer = _offer(rows)
    assert (offer.kind, offer.not_before) == ("at", future)
    past = T0 - dt.timedelta(minutes=20)
    rows = [_Row(1, "error", "lease_live", at=T0, blocked_until=past)]
    assert _offer(rows).kind == "now"
    rows = [_Row(1, "error", "lease_live", at=T0)]
    offer = _offer(rows)
    assert (offer.kind, offer.not_before) == ("at", T0 + dt.timedelta(seconds=30))
    assert _offer(rows, now=T0 + dt.timedelta(seconds=30)).kind == "now"
    # A future blocked_until also beats an earlier counted delay.
    rows = [_Row(1, *COUNTED, at=T0), _Row(2, "error", "lease_live", at=T0, blocked_until=future)]
    assert _offer(rows).not_before == future


def test_policy_open_wins_over_everything() -> None:
    rows = _history(*([COUNTED] * 8), ("error", "tape_only"), (None, None))
    summary = summarize_rows(rows)
    assert summary.open is True
    assert _offer(rows).kind == "open"


def test_policy_delivered_and_empty_histories_offer_now() -> None:
    assert _offer([]).kind == "now"
    assert summarize_rows([]).attempts == 0
    rows = _history(*([COUNTED] * 7), DELIVERED)
    assert _offer(rows).kind == "now"
    replay = [_Row(1, "job_end", None, opened_from_state="done")]
    summary = summarize_rows(replay)
    assert summary.latest is not None
    assert summary.latest.klass == "none"
    assert _offer(replay).kind == "now"


def test_policy_naive_now_is_read_as_utc() -> None:
    rows = _history(COUNTED)  # ends at T0 + 1 min; the delay is 30 s
    naive_now = (T0 + dt.timedelta(minutes=1, seconds=31)).replace(tzinfo=None)
    assert _offer(rows, now=naive_now).kind == "now"
    naive_now = (T0 + dt.timedelta(minutes=1, seconds=29)).replace(tzinfo=None)
    assert _offer(rows, now=naive_now).kind == "at"


# ---- T7: text and configuration ---------------------------------------------------


def test_text_owner_sentences_are_verbatim() -> None:
    assert compose_last_error("error", "tape_only", "deferred to RM3") == (
        "this item's usable copies are on tape, which agent delivery cannot read yet; "
        "request server-local delivery"
    )
    assert compose_last_error("cancelled", "lease_expired", None) == (
        "the transfer took longer than the 30-minute delivery lease, which is not renewed "
        "yet; request server-local delivery"
    )
    assert compose_last_error("job_end", "lease_expired", None) == compose_last_error(
        "cancelled", "lease_expired", None
    )


def test_text_cache_disk_unavailable_never_says_rm3() -> None:
    text = compose_last_error("error", "cache_disk_unavailable", "tape source deferred to RM3")
    assert text == "the cache disk holding this item is unavailable"
    assert "RM3" not in text


def test_text_per_cause_and_interruptions() -> None:
    assert compose_last_error("error", "not_authorized", "no grant") == "not authorized: no grant"
    assert compose_last_error("error", "server_error", None) == "server error"
    assert compose_last_error("error", "capacity", None) == "restore capacity is full"
    assert compose_last_error("cancelled", None, None) == "the transfer was interrupted"
    assert compose_last_error("lost", None, None) == "the transfer was interrupted"
    assert compose_last_error("job_end", None, None) is None
    for cause in RESTORE_ATTEMPT_CAUSES:
        kind = "cancelled" if cause == "lease_expired" else "error"
        assert compose_last_error(kind, cause, "m")


def test_text_compose_detail_per_rule() -> None:
    decisive = SummaryRow(
        id=1,
        end_kind="error",
        klass="counted",
        cause="source_unavailable",
        message="all copies suspect",
        end_time=T0,
        blocked_until=None,
    )
    assert compose_detail("max_failures", 8, decisive) == (
        "gave up after 8 failed tries: no readable source copy: all copies suspect"
    )
    assert compose_detail("max_interruptions", 8, decisive) == (
        "gave up after 8 interrupted transfers in a row"
    )
    terminal = SummaryRow(
        id=2,
        end_kind="error",
        klass="terminal",
        cause="tape_only",
        message=None,
        end_time=T0,
        blocked_until=None,
    )
    assert compose_detail("terminal_cause", None, terminal) == compose_last_error(
        "error", "tape_only", None
    )
    with pytest.raises(ValueError, match="unknown give-up rule"):
        compose_detail("other", None, decisive)


def test_text_bound_message_at_2000() -> None:
    assert bound_message("x" * 1999) == "x" * 1999
    assert bound_message("x" * 2000) == "x" * 2000
    assert len(bound_message("x" * 2001)) == 2000
    assert bound_message("") == ""


def test_text_retry_policy_from_env_defaults_overrides_and_refusals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in [key for key in os.environ if key.startswith("SUTRADHARA_RESTORE_RETRY_")]:
        monkeypatch.delenv(name)
    policy = retry_policy_from_env()
    assert policy == RetryPolicy()
    assert policy.base_delay == dt.timedelta(seconds=30)
    assert policy.max_delay == dt.timedelta(seconds=1800)
    assert policy.wait_delay == dt.timedelta(seconds=30)
    assert (policy.max_failures, policy.max_interruptions) == (8, 8)

    monkeypatch.setenv("SUTRADHARA_RESTORE_RETRY_BASE_SECONDS", "5")
    monkeypatch.setenv("SUTRADHARA_RESTORE_RETRY_MAX_SECONDS", "40")
    monkeypatch.setenv("SUTRADHARA_RESTORE_RETRY_MAX_FAILURES", "3")
    monkeypatch.setenv("SUTRADHARA_RESTORE_RETRY_MAX_INTERRUPTIONS", "2")
    monkeypatch.setenv("SUTRADHARA_RESTORE_RETRY_WAIT_SECONDS", "7")
    policy = retry_policy_from_env()
    assert policy == RetryPolicy(
        base_delay=dt.timedelta(seconds=5),
        max_delay=dt.timedelta(seconds=40),
        max_failures=3,
        max_interruptions=2,
        wait_delay=dt.timedelta(seconds=7),
    )
    # The overrides drive the rule: 5, 10, 20, then give up at the third failure.
    assert _offer(_history(COUNTED), policy=policy).not_before == T0 + dt.timedelta(
        minutes=1, seconds=5
    )
    assert _offer(_history(COUNTED, COUNTED, COUNTED), policy=policy).kind == "give_up"

    for name, value in (
        ("BASE_SECONDS", "0"),
        ("BASE_SECONDS", "-1"),
        ("MAX_SECONDS", "4"),
        ("MAX_FAILURES", "0"),
        ("MAX_INTERRUPTIONS", "-3"),
        ("WAIT_SECONDS", "0"),
    ):
        with monkeypatch.context() as context:
            context.setenv(f"SUTRADHARA_RESTORE_RETRY_{name}", value)
            with pytest.raises(ValueError, match="restore retry"):
                retry_policy_from_env()
    with pytest.raises(ValueError, match="max delay must not be below"):
        RetryPolicy(base_delay=dt.timedelta(seconds=60), max_delay=dt.timedelta(seconds=30))


# ---- T4 (SQL half) and T6: the summary statement ---------------------------------------


def test_class_table_sql_case_agrees_with_python_on_sqlite(
    engine: Engine, instance_id: int
) -> None:
    item_id = _seed_item(engine, "class-case")
    expected: dict[int, str | None] = {}
    for attempt_no, (end_kind, cause, from_done) in enumerate(_legal_triples(), start=1):
        extra = {"blocked_until": T0} if cause == "lease_live" else {}
        row_id = _insert_attempt(
            engine,
            _attempt_values(
                item_id,
                instance_id,
                attempt_no,
                opened_from_state="done" if from_done else "sent",
                **_ended(end_kind, cause, **extra),
            ),
        )
        expected[row_id] = classify(end_kind, cause, from_done)
    open_id = _insert_attempt(engine, _attempt_values(item_id, instance_id, 43))
    expected[open_id] = None
    with engine.connect() as conn:
        seen = dict(
            conn.execute(
                select(RestoreItemAttempt.id, attempt_class_expression()).where(
                    RestoreItemAttempt.restore_request_item_id == item_id
                )
            ).all()
        )
    assert seen == expected
    assert set(expected.values()) == {*ATTEMPT_CLASSES, None}


def _random_history(rng: random.Random, item_id: int, instance_id: int) -> list[dict[str, Any]]:
    shapes = _legal_end_shapes()
    count = rng.randint(0, 20)
    rows: list[dict[str, Any]] = []
    for attempt_no in range(1, count + 1):
        values = _attempt_values(
            item_id,
            instance_id,
            attempt_no,
            opened_from_state=rng.choice(["queued", "sent", "streaming", "done"]),
            opened_at=T0 + dt.timedelta(seconds=rng.randint(0, 100_000)),
            message=rng.choice([None, "", "m", "ü" * 50, "x" * 2000]),
        )
        if attempt_no == count and rng.random() < 0.3:
            rows.append(values)  # the open row, last
            continue
        end_kind, cause = rng.choice(shapes)
        when = T0 + dt.timedelta(seconds=rng.randint(0, 100_000))
        extra: dict[str, Any] = {}
        if cause == "lease_live" and rng.random() < 0.7:
            extra["blocked_until"] = when + dt.timedelta(seconds=rng.randint(-600, 600))
        rows.append({**values, **_ended(end_kind, cause, **extra)})
        rows[-1]["ended_at" if end_kind != "lost" else "closed_at"] = when
    return rows


def test_summary_statement_agrees_with_python_twin_on_random_histories(
    engine: Engine, instance_id: int
) -> None:
    rng = random.Random(20261109)
    item_ids = [_seed_item(engine, f"agree-{index}") for index in range(300)]
    with engine.begin() as conn:
        for item_id in item_ids:
            for values in _random_history(rng, item_id, instance_id):
                conn.execute(insert(RestoreItemAttempt).values(**values))
    with session_scope(engine) as session:
        rows_by_item: dict[int, list[RestoreItemAttempt]] = {item_id: [] for item_id in item_ids}
        for row in session.scalars(
            select(RestoreItemAttempt).where(
                RestoreItemAttempt.restore_request_item_id.in_(item_ids)
            )
        ):
            rows_by_item[row.restore_request_item_id].append(row)
        rng.shuffle(item_ids)
        batches = [item_ids[index : index + 37] for index in range(0, len(item_ids), 37)]
        compared = 0
        kinds: set[str] = set()
        for batch in batches:
            summaries = attempt_summaries(session, batch)
            assert set(summaries) == set(batch)
            for item_id in batch:
                twin = summarize_rows(rows_by_item[item_id])
                assert summaries[item_id] == twin, item_id
                kinds.add(plan_next_offer(twin, T0, RetryPolicy()).kind)
                compared += 1
        assert compared == 300
        assert {"now", "at", "give_up", "open"} <= kinds
        assert any(not rows for rows in rows_by_item.values())
        assert attempt_summaries(session, []) == {}
    compiled = str(
        attempt_summaries_statement([1, 2]).compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    assert "WITH classified AS" in compiled
    assert "GROUP BY" in compiled


# ---- FR1 additions: large limits (T5) and non-finite configuration (T7) ---------------


def test_policy_large_limits_walk_to_one_hundred_without_overflow() -> None:
    policy = RetryPolicy(max_failures=100, max_interruptions=100)
    ceiling = dt.timedelta(seconds=1800)
    for n in range(1, 100):
        rows = _history(*([COUNTED] * n))
        offer = _offer(rows, policy=policy)
        assert offer.kind == "at", n
        assert offer.not_before is not None
        if n >= 7:
            assert offer.not_before - rows[-1].ended_at == ceiling
    assert _offer(_history(*([COUNTED] * 100)), policy=policy).kind == "give_up"
    assert _offer(_history(INTERRUPTED), policy=policy).kind == "now"
    for r in range(2, 100):
        rows = _history(*([INTERRUPTED] * r))
        offer = _offer(rows, policy=policy)
        assert offer.kind == "at", r
        assert offer.not_before is not None
        if r >= 8:
            assert offer.not_before - rows[-1].ended_at == ceiling
    offer = _offer(_history(*([INTERRUPTED] * 100)), policy=policy)
    assert (offer.kind, offer.rule, offer.counted) == ("give_up", "max_interruptions", 100)


@pytest.mark.parametrize("value", ["inf", "-inf", "nan", "1e400"])
def test_text_retry_policy_from_env_refuses_non_finite_seconds(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    for name in ("BASE_SECONDS", "MAX_SECONDS", "WAIT_SECONDS"):
        with monkeypatch.context() as context:
            context.setenv(f"SUTRADHARA_RESTORE_RETRY_{name}", value)
            with pytest.raises(ValueError, match=r"finite|too large|positive|not be below"):
                retry_policy_from_env()
