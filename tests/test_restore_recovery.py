"""RSR-1e recovery against a function-scoped catalog, with no other test's leftovers."""

from __future__ import annotations

import datetime as dt
import math
import time
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import grpc
import pytest
from sqlalchemy import event, select, update
from sqlalchemy.dialects import postgresql

from sutradhara._proto import restore_pb2
from sutradhara.catalog.session import session_scope
from sutradhara.grpc import restore_service as service_module
from sutradhara.grpc.restore_service import RestoreService, _Outcome
from sutradhara.hdcache.models import (
    RestoreItemAttempt,
    RestoreItemGiveup,
    RestoreOpenSession,
    RestoreRequestItem,
    RestoreServerInstance,
)
from sutradhara.hdcache.restore_retry import attempt_summaries, giveup_agreement_violations
from tests.test_grpc_restore_service import (
    _Abort,
    _attempt_rows,
    _build_rig,
    _close_attempt,
    _commit_request,
    _kinds,
    _open_collect,
    _pl,
    _Rig,
    _rlr_service,
    _seed_item,
    _with_hooks,
)


@pytest.fixture
def rig(tmp_path: Path) -> Iterator[_Rig]:
    """Own the whole recovery database; reuse only the stream fixture's construction."""

    result = _build_rig(tmp_path / "recovery")
    result.service.stop()
    result.service_config = replace(
        result.service_config,
        heartbeat_seconds=1,
        staleness_threshold=dt.timedelta(seconds=10),
        recovery_seconds=0.02,
    )
    result.service = RestoreService(result.service_config)
    result.service.start(background=False)
    try:
        yield result
    finally:
        result.service.stop()
        result.engine.dispose()


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Advance only the service's monotonic clock; database and lease clocks stay real."""

    value = [time.monotonic()]
    monkeypatch.setattr(
        service_module, "time", SimpleNamespace(monotonic=lambda: value[0], sleep=time.sleep)
    )
    return value


def _healthy(service: RestoreService, clock: list[float]) -> None:
    threshold = service.config.staleness_threshold.total_seconds()
    for _ in range(5):
        clock[0] += threshold * 0.3
        service.heartbeat_once()
    assert service.heartbeat_unbroken_seconds() > threshold


def _item(rig: _Rig, name: str) -> int:
    return _seed_item(rig, name, _pl(name), receiver="receiver")


@pytest.mark.parametrize("pause", [False, True])
def test_rlr_other_instance_respects_renewal_then_fences_stopped_delivery(
    rig: _Rig,
    pause: bool,
) -> None:
    """Proof 6: actual recovery preserves a renewing stale owner, but closes an idle one."""

    item_id = _seed_item(rig, "rlr-recovery", b"r" * (32 * 256 * 1024), receiver="receiver")
    observer = RestoreService(
        replace(
            rig.service_config,
            heartbeat_seconds=0.05,
            staleness_threshold=dt.timedelta(seconds=0.3),
        )
    )
    observer.start(background=False)
    try:
        with _rlr_service(rig) as caller:
            stream = caller.OpenRestore(
                restore_pb2.OpenRestoreRequest(restore_request_item_id=item_id),
                rig.context("receiver"),
            )
            try:
                head = next(stream).manifest_head
                with session_scope(rig.engine) as session:
                    session.get(RestoreServerInstance, caller.instance_id).heartbeat_at = (
                        dt.datetime.now(dt.UTC) - dt.timedelta(hours=1)
                    )
                started = time.monotonic()
                checked = False
                for frame in stream:
                    if frame.HasField("chunk"):
                        time.sleep(0.07)
                        observer.heartbeat_once()
                        if time.monotonic() - started > 1.1 and not checked:
                            assert observer.heartbeat_unbroken_seconds() > 0.3
                            observer.recover_once()
                            (row,) = _attempt_rows(rig, item_id)
                            assert (row.end_kind, row.closed_at, row.ended_at) == (None, None, None)
                            checked = True
                            if pause:
                                for _ in range(14):
                                    time.sleep(0.08)
                                    observer.heartbeat_once()
                                observer.recover_once()
                                _assert_lost(rig, item_id)
                                closed = _attempt_rows(rig, item_id)[0].closed_at
                                with patch.object(
                                    caller, "_end_attempt", wraps=caller._end_attempt
                                ) as end_attempt:
                                    with pytest.raises(_Abort) as fenced:
                                        next(stream)
                                    end_attempt.assert_not_called()
                                assert fenced.value.code == grpc.StatusCode.UNAVAILABLE
                                assert (
                                    fenced.value.details == "restore attempt was closed by recovery"
                                )
                                assert _attempt_rows(rig, item_id)[0].closed_at == closed
                                assert caller._registry == {}
                                return
                    if frame.HasField("file_end"):
                        caller.CommitRestore(
                            _commit_request(item_id, head.manifest_sha256, head.lease_token, 1),
                            rig.context("receiver"),
                        )
                        assert _kinds(list(stream)) == ["job_end"]
                        break
                assert checked
                (row,) = _attempt_rows(rig, item_id)
                assert (row.end_kind, row.cause) == ("job_end", None)
                with session_scope(rig.engine) as session:
                    assert session.get(RestoreRequestItem, item_id).state == "sent"
                    assert giveup_agreement_violations(session) == []
            finally:
                stream.close()  # type: ignore[attr-defined]
    finally:
        observer.stop()


def _abandoned(
    rig: _Rig,
    item_id: int,
    *,
    attempt_no: int = 1,
    instance_id: int | None = None,
    generation: int | None = 1,
    expiry: dt.datetime | None = None,
) -> int:
    """Seed a crashed process's ledger and (when granted) its lease generation."""

    now = dt.datetime.now(dt.UTC)
    with session_scope(rig.engine) as session:
        if instance_id is None:
            owner = RestoreServerInstance(
                host="dead",
                pid=1,
                code_version="test",
                started_at=now - dt.timedelta(hours=2),
                heartbeat_at=now - dt.timedelta(hours=1),
            )
            session.add(owner)
            session.flush()
            instance_id = owner.id
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        if generation is not None:
            session.merge(
                RestoreOpenSession(
                    restore_request_item_id=item_id,
                    receiver_device_id="receiver",
                    manifest_sha256=b"m" * 32,
                    generation=generation,
                    expires_at=expiry or now - dt.timedelta(minutes=5),
                )
            )
            item.state = "streaming"
            item.bytes_restored = 7
        row = RestoreItemAttempt(
            restore_request_item_id=item_id,
            attempt_no=attempt_no,
            receiver_device_id="receiver",
            server_instance_id=instance_id,
            opened_from_state="queued",
            opened_at=now - dt.timedelta(minutes=10),
            lease_generation=generation,
            code_version="test",
        )
        session.add(row)
        session.flush()
        return row.id


def _assert_lost(rig: _Rig, item_id: int, *, state: str = "queued") -> None:
    row = _attempt_rows(rig, item_id)[-1]
    assert (row.end_kind, row.cause, row.ended_at) == ("lost", None, None)
    assert row.closed_at is not None
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item is not None
        assert item.state == state
        assert item.bytes_restored == 0
        assert item.request.state == ("pending" if state == "queued" else "completed_with_errors")
        if item.open_session is not None:
            assert item.open_session.expires_at.replace(tzinfo=dt.UTC) <= dt.datetime.now(dt.UTC)
        assert giveup_agreement_violations(session) == []


def test_r1_dead_instances_require_unbroken_heartbeat_and_stop_after_a_break(
    rig: _Rig, clock: list[float]
) -> None:
    first = _item(rig, "r1-first")
    second = _item(rig, "r1-second")
    _abandoned(rig, first)
    _abandoned(rig, second)
    rig.service.recover_once()
    assert _attempt_rows(rig, first)[0].closed_at is None
    _healthy(rig.service, clock)
    rig.service.recover_once()
    _assert_lost(rig, first)
    _assert_lost(rig, second)
    third = _item(rig, "r1-break")
    _abandoned(rig, third)
    clock[0] += 6
    rig.service.recover_once()
    assert _attempt_rows(rig, third)[0].closed_at is None
    rig.service.heartbeat_once()
    rig.service.recover_once()
    assert _attempt_rows(rig, third)[0].closed_at is None
    _healthy(rig.service, clock)
    rig.service.recover_once()
    _assert_lost(rig, third)


def test_r1_dead_instance_lost_close_has_unknown_bytes_sent(rig: _Rig, clock: list[float]) -> None:
    """Recovery cannot reconstruct a stopped server's transmitted byte count."""

    item_id = _item(rig, "r1-unknown-bytes")
    _abandoned(rig, item_id)
    _healthy(rig.service, clock)
    rig.service.recover_once()
    _assert_lost(rig, item_id)
    with session_scope(rig.engine) as session:
        row = session.scalar(
            select(RestoreItemAttempt).where(
                RestoreItemAttempt.restore_request_item_id == item_id,
                RestoreItemAttempt.end_kind == "lost",
                RestoreItemAttempt.bytes_sent.is_(None),
            )
        )
        assert row is not None
        assert row.bytes_sent is None


def test_r1_fresh_other_instance_is_not_closed_despite_expired_lease(
    rig: _Rig, clock: list[float]
) -> None:
    """A long-expired lease alone does not make another live instance recoverable."""

    item_id = _item(rig, "r1-fresh")
    attempt_id = _abandoned(
        rig,
        item_id,
        expiry=dt.datetime.now(dt.UTC)
        - rig.service.config.staleness_threshold
        - dt.timedelta(minutes=1),
    )
    with session_scope(rig.engine) as session:
        owner_id = session.get(RestoreItemAttempt, attempt_id).server_instance_id
        assert owner_id != rig.service.instance_id
        session.get(RestoreServerInstance, owner_id).heartbeat_at = dt.datetime.now(dt.UTC)
    _healthy(rig.service, clock)
    rig.service.recover_once()
    row = _attempt_rows(rig, item_id)[0]
    assert (row.end_kind, row.ended_at, row.closed_at) == (None, None, None)
    with session_scope(rig.engine) as session:
        assert session.get(RestoreRequestItem, item_id).state == "streaming"
        assert giveup_agreement_violations(session) == []


def test_r1_eight_lost_closes_give_up(rig: _Rig, clock: list[float]) -> None:
    item_id = _item(rig, "r1-eight")
    _healthy(rig.service, clock)
    for number in range(1, 9):
        _abandoned(rig, item_id, attempt_no=number, generation=number)
        rig.service.recover_once()
        _assert_lost(rig, item_id, state="failed" if number == 8 else "queued")
    with session_scope(rig.engine) as session:
        giveup = session.scalar(select(RestoreItemGiveup))
        assert giveup is not None
        assert (giveup.rule, giveup.counted, giveup.threshold) == ("max_interruptions", 8, 8)
        assert giveup.decisive_attempt_id == _attempt_rows(rig, item_id)[-1].id


@pytest.mark.parametrize("seconds", [60, -5])
def test_r2_live_or_recently_expired_lease_is_not_closed(
    rig: _Rig, clock: list[float], seconds: int
) -> None:
    item_id = _item(rig, "r2-margin")
    _abandoned(rig, item_id, expiry=dt.datetime.now(dt.UTC) + dt.timedelta(seconds=seconds))
    _healthy(rig.service, clock)
    rig.service.recover_once()
    assert _attempt_rows(rig, item_id)[0].closed_at is None
    with session_scope(rig.engine) as session:
        assert session.get(RestoreRequestItem, item_id).state == "streaming"


def test_r2_null_generation_closes_and_fences_the_pending_acquisition(
    rig: _Rig, clock: list[float]
) -> None:
    item_id = _item(rig, "r2-fence")
    _healthy(rig.service, clock)

    def recover(_item_id: int) -> None:
        with session_scope(rig.engine) as session:
            session.execute(
                update(RestoreServerInstance)
                .where(RestoreServerInstance.id == caller.instance_id)
                .values(heartbeat_at=dt.datetime.now(dt.UTC) - dt.timedelta(hours=1))
            )
        rig.service.recover_once()

    caller = _with_hooks(rig, before_lease=recover)
    try:
        frames, exc, _ = _open_collect(caller, item_id, rig.context("receiver"))
    finally:
        caller.stop()
    assert frames == []
    assert isinstance(exc, _Abort)
    assert exc.code == grpc.StatusCode.UNAVAILABLE
    _assert_lost(rig, item_id)
    with session_scope(rig.engine) as session:
        assert session.get(RestoreOpenSession, item_id) is None


def test_r2_conditional_end_compiles_positive_lease_rule(rig: _Rig, clock: list[float]) -> None:
    item_id = _item(rig, "r2-sql")
    _abandoned(rig, item_id)
    _healthy(rig.service, clock)
    statements: list[Any] = []

    def capture(_conn: Any, clause: Any, *_args: Any) -> None:
        if getattr(clause, "is_update", False) and clause.table.name == "restore_item_attempt":
            statements.append(clause)

    event.listen(rig.engine, "before_execute", capture)
    try:
        rig.service.recover_once()
    finally:
        event.remove(rig.engine, "before_execute", capture)
    assert len(statements) == 1
    compiled = statements[0].compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert "OR (EXISTS (" in sql or "OR EXISTS (" in sql
    assert "NOT (EXISTS" not in sql
    assert "restore_open_session.expires_at <" in sql.split("EXISTS (", 1)[1]
    assert "restore_open_session.generation = restore_item_attempt.lease_generation" in sql
    assert any(isinstance(value, dt.datetime) for value in compiled.params.values())
    _assert_lost(rig, item_id)


def test_r3_failed_counted_end_retries_outside_registry_lock(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    item_id = _item(rig, "r3-counted")
    entry = rig.service._insert_attempt(
        rig.context("receiver"),
        _Outcome(),
        item_id=item_id,
        request_id="request-r3-counted",
        device_id="receiver",
        opened_from_state="queued",
    )
    entry.outcome.set("error", "source_integrity", "bad archive")
    real_end = rig.service._end_transaction

    def fail(_entry: Any, *, deliver: bool, margin: Any = None) -> Any:
        raise RuntimeError("end write failed")

    with monkeypatch.context() as patch:
        patch.setattr(rig.service, "_end_transaction", fail)
        with pytest.raises(RuntimeError, match="end write failed"):
            rig.service._end_attempt(entry, deliver=False)
    assert entry.ended

    def checked(entry: Any, *, deliver: bool, margin: Any = None) -> Any:
        assert rig.service._registry_lock.acquire(blocking=False)
        rig.service._registry_lock.release()
        return real_end(entry, deliver=deliver, margin=margin)

    monkeypatch.setattr(rig.service, "_end_transaction", checked)
    rig.service.recover_once()
    assert entry.attempt_id not in rig.service._registry
    with session_scope(rig.engine) as session:
        summary = attempt_summaries(session, [item_id])[item_id]
        assert (summary.counted, summary.interruptions, summary.open) == (1, 0, False)
        assert summary.latest.cause == "source_integrity"


def _cancelled_attempt(rig: _Rig, name: str, *, seconds: float = 1) -> service_module._AttemptEntry:
    """Seed a leased cancellation whose first end can fail independently of the stream."""

    item_id = _item(rig, name)
    entry = rig.service._insert_attempt(
        rig.context("receiver"),
        _Outcome(end_kind="cancelled"),
        item_id=item_id,
        request_id=f"request-{name}",
        device_id="receiver",
        opened_from_state="queued",
    )
    expiry = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=seconds)
    entry.lease = service_module._Lease(item_id, "receiver", b"c" * 32, 1, expiry)
    with session_scope(rig.engine) as session:
        session.add(
            RestoreOpenSession(
                restore_request_item_id=item_id,
                receiver_device_id="receiver",
                manifest_sha256=b"c" * 32,
                generation=1,
                expires_at=expiry,
            )
        )
        session.get(RestoreItemAttempt, entry.attempt_id).lease_generation = 1
        session.get(RestoreRequestItem, item_id).state = "streaming"
    return entry


def _assert_cancelled_interruption(rig: _Rig, item_id: int) -> None:
    """Pin the persisted cancellation, retry counts, and give-up agreement together."""

    row = _attempt_rows(rig, item_id)[0]
    assert (row.end_kind, row.cause) == ("cancelled", None)
    with session_scope(rig.engine) as session:
        summary = attempt_summaries(session, [item_id])[item_id]
        assert (summary.counted, summary.interruptions) == (0, 1)
        assert session.get(RestoreRequestItem, item_id).state == "queued"
        assert (
            session.scalar(
                select(RestoreItemGiveup).where(
                    RestoreItemGiveup.restore_request_item_id == item_id
                )
            )
            is None
        )
        assert giveup_agreement_violations(session) == []


def test_r3_cancelled_end_retried_after_its_lease_lapsed_is_an_interruption(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An outage outlasting the lease must preserve the first cancellation's class."""

    entry = _cancelled_attempt(rig, "r3-cancel-lapsed")
    assert entry.lease is not None

    def fail(_entry: Any, *, deliver: bool, margin: Any = None) -> Any:
        raise RuntimeError("end write failed")

    with monkeypatch.context() as patch:
        patch.setattr(rig.service, "_end_transaction", fail)
        with pytest.raises(RuntimeError, match="end write failed"):
            rig.service._end_attempt(entry, deliver=False)
    assert entry.first_end_at is not None
    assert entry.first_end_at < entry.lease.expires_at
    time.sleep(max(0, (entry.lease.expires_at - dt.datetime.now(dt.UTC)).total_seconds()) + 0.05)
    rig.service.recover_once()
    _assert_cancelled_interruption(rig, entry.item_id)


def test_r3_cancelled_end_whose_first_try_outlasted_its_lease_is_an_interruption(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A slow failing transaction must retain the cancellation instant before expiry."""

    entry = _cancelled_attempt(rig, "r3-cancel-slow-end")
    assert entry.lease is not None
    expires = entry.lease.expires_at

    def fail(_entry: Any, *, deliver: bool, margin: Any = None) -> Any:
        time.sleep(max(0, (expires - dt.datetime.now(dt.UTC)).total_seconds()) + 0.05)
        raise RuntimeError("slow end write failed")

    with monkeypatch.context() as patch:
        patch.setattr(rig.service, "_end_transaction", fail)
        with pytest.raises(RuntimeError, match="slow end write failed"):
            rig.service._end_attempt(entry, deliver=False)
    assert entry.first_end_at is not None
    assert entry.first_end_at < expires
    rig.service.recover_once()
    _assert_cancelled_interruption(rig, entry.item_id)


def test_r3_cancelled_end_whose_lease_had_expired_at_the_first_try_stays_lease_expired(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retrying a genuinely expired cancellation must retain its terminal cause."""

    entry = _cancelled_attempt(rig, "r3-cancel-expired", seconds=-1)

    def fail(_entry: Any, *, deliver: bool, margin: Any = None) -> Any:
        raise RuntimeError("end write failed")

    with monkeypatch.context() as patch:
        patch.setattr(rig.service, "_end_transaction", fail)
        with pytest.raises(RuntimeError, match="end write failed"):
            rig.service._end_attempt(entry, deliver=False)
    rig.service.recover_once()
    row = _attempt_rows(rig, entry.item_id)[0]
    assert (row.end_kind, row.cause) == ("cancelled", "lease_expired")
    with session_scope(rig.engine) as session:
        assert giveup_agreement_violations(session) == []


def test_r3_first_end_instant_is_kept_across_failed_retries(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A second failed end after expiry must not advance the cancellation clock."""

    entry = _cancelled_attempt(rig, "r3-cancel-twice")
    assert entry.lease is not None

    def fail(_entry: Any, *, deliver: bool, margin: Any = None) -> Any:
        raise RuntimeError("end write failed")

    with monkeypatch.context() as patch:
        patch.setattr(rig.service, "_end_transaction", fail)
        with pytest.raises(RuntimeError, match="end write failed"):
            rig.service._end_attempt(entry, deliver=False)
        first_end_at = entry.first_end_at
        assert first_end_at is not None
        assert first_end_at < entry.lease.expires_at
        time.sleep(
            max(0, (entry.lease.expires_at - dt.datetime.now(dt.UTC)).total_seconds()) + 0.05
        )
        with pytest.raises(RuntimeError, match="end write failed"):
            rig.service._end_attempt(entry, deliver=False)
        assert entry.first_end_at == first_end_at
    rig.service.recover_once()
    _assert_cancelled_interruption(rig, entry.item_id)


def test_r3_failed_delivery_retries_as_server_error(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    item_id = _item(rig, "r3-delivery")
    real_end = rig.service._end_transaction

    def fail(entry: Any, *, deliver: bool, margin: Any = None) -> Any:
        if deliver:
            raise RuntimeError("delivery write failed")
        return real_end(entry, deliver=deliver, margin=margin)

    with monkeypatch.context() as patch:
        patch.setattr(rig.service, "_end_transaction", fail)
        frames, exc, _ = _open_collect(rig.service, item_id, rig.context("receiver"))
    assert frames[-1].HasField("job_end")  # _open_collect acknowledges STAGED at file_end.
    assert isinstance(exc, _Abort)
    assert exc.code == grpc.StatusCode.ABORTED
    entry = next(iter(rig.service._registry.values()))
    assert entry.klass == "delivered"
    rig.service.recover_once()
    row = _attempt_rows(rig, item_id)[0]
    assert (row.end_kind, row.cause) == ("error", "server_error")
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert item.state == "queued"
        assert item.checkpoint.committed_index == 1
        assert attempt_summaries(session, [item_id])[item_id].counted == 1
    assert entry.attempt_id not in rig.service._registry


def test_r3_own_orphan_closes_even_with_a_live_lease_and_broken_heartbeat(
    rig: _Rig, clock: list[float]
) -> None:
    item_id = _item(rig, "r3-orphan")
    _abandoned(
        rig,
        item_id,
        instance_id=rig.service.instance_id,
        expiry=dt.datetime.now(dt.UTC) + dt.timedelta(hours=1),
    )
    clock[0] += 6
    rig.service.recover_once()
    _assert_lost(rig, item_id)


def test_r3_already_closed_registry_entry_is_dropped(rig: _Rig) -> None:
    item_id = _item(rig, "r3-closed")
    entry = rig.service._insert_attempt(
        rig.context("receiver"),
        _Outcome(end_kind="error", cause="source_integrity"),
        item_id=item_id,
        request_id="request-r3-closed",
        device_id="receiver",
        opened_from_state="queued",
    )
    rig.service._keep_ended(entry)
    _close_attempt(rig, entry.attempt_id)
    rig.service.recover_once()
    assert entry.attempt_id not in rig.service._registry
    assert _attempt_rows(rig, item_id)[0].end_kind == "lost"


@pytest.mark.parametrize("seconds", [0, -1, float("nan"), math.inf, 1e12])
def test_recovery_interval_is_positive(rig: _Rig, seconds: float) -> None:
    with pytest.raises(ValueError, match="recovery interval must be positive and finite"):
        replace(rig.service_config, recovery_seconds=seconds)


def test_r3_active_registry_and_revealed_replay_keep_their_meaning(rig: _Rig) -> None:
    item_id = _item(rig, "r3-replay")
    with session_scope(rig.engine) as session:
        session.get(RestoreRequestItem, item_id).state = "done"
    entry = rig.service._insert_attempt(
        rig.context("receiver"),
        _Outcome(),
        item_id=item_id,
        request_id="request-r3-replay",
        device_id="receiver",
        opened_from_state="done",
    )
    rig.service.recover_once()
    assert _attempt_rows(rig, item_id)[0].closed_at is None
    entry.outcome.set("job_end", None)
    rig.service._keep_ended(entry)
    assert entry.klass == "none"
    rig.service.recover_once()
    row = _attempt_rows(rig, item_id)[0]
    assert (row.end_kind, row.cause, row.closed_at) == ("job_end", None, None)
    assert row.ended_at is not None
    with session_scope(rig.engine) as session:
        summary = attempt_summaries(session, [item_id])[item_id]
        assert (summary.counted, summary.delivered) == (0, 0)
        assert session.get(RestoreRequestItem, item_id).state == "done"


@pytest.mark.parametrize("generation", [None, 1])
def test_r3_lost_end_never_uses_the_items_newer_lease(rig: _Rig, generation: int | None) -> None:
    item_id = _item(rig, "r3-newer")
    _abandoned(rig, item_id, instance_id=rig.service.instance_id, generation=generation)
    expiry = dt.datetime.now(dt.UTC) + dt.timedelta(hours=1)
    with session_scope(rig.engine) as session:
        session.merge(
            RestoreOpenSession(
                restore_request_item_id=item_id,
                receiver_device_id="receiver",
                manifest_sha256=b"n" * 32,
                generation=2,
                expires_at=expiry,
            )
        )
        item = session.get(RestoreRequestItem, item_id)
        item.state = "streaming"
        item.bytes_restored = 7
    rig.service.recover_once()
    assert _attempt_rows(rig, item_id)[0].end_kind == "lost"
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert (item.state, item.bytes_restored) == ("streaming", 7)
        assert item.open_session.generation == 2
        assert item.open_session.expires_at.replace(tzinfo=dt.UTC) == expiry


def _stranded(rig: _Rig, name: str, *, local: bool = False) -> int:
    """A legacy streaming item with an expired lease and no ledger history."""

    item_id = _item(rig, name)
    with session_scope(rig.engine) as session:
        session.add(
            RestoreOpenSession(
                restore_request_item_id=item_id,
                receiver_device_id="receiver",
                manifest_sha256=b"s" * 32,
                generation=1,
                expires_at=dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1),
            )
        )
        item = session.get(RestoreRequestItem, item_id)
        item.state = "streaming"
        item.bytes_restored = 9
        if local:
            item.request.delivery_mode = "server_local"
            item.request.receiver_device_id = None
    return item_id


def test_r4_stranded_items_recover_only_when_heartbeat_is_healthy(
    rig: _Rig, clock: list[float]
) -> None:
    item_id = _stranded(rig, "r4-stranded")
    local_id = _stranded(rig, "r4-local", local=True)
    clock[0] += 6
    rig.service.recover_once()
    with session_scope(rig.engine) as session:
        assert session.get(RestoreRequestItem, item_id).state == "streaming"
        assert giveup_agreement_violations(session) == []
    rig.service.heartbeat_once()
    rig.service.recover_once()  # Step 4 needs health, not a full threshold of successes.
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert (item.state, item.bytes_restored, item.request.state) == ("queued", 0, "pending")
        assert session.get(RestoreRequestItem, local_id).state == "streaming"
        assert attempt_summaries(session, [item_id])[item_id].attempts == 0
        assert giveup_agreement_violations(session) == []


@pytest.mark.parametrize("changed", ["lease", "attempt"])
def test_r4_stranding_rereads_after_lock(rig: _Rig, changed: str) -> None:
    item_id = _stranded(rig, "r4-reread")
    locked: list[str] = []
    item_updates: list[str] = []

    def count_item_updates(
        _conn: Any, _cursor: Any, statement: str, _parameters: Any, _context: Any, _many: bool
    ) -> None:
        if locked and statement.lstrip().upper().startswith("UPDATE RESTORE_REQUEST_ITEM "):
            item_updates.append(statement)

    def race(conn: Any, clause: Any, *_args: Any) -> None:
        if (
            not locked
            and getattr(clause, "_for_update_arg", None) is not None
            and "restore_open_session" in str(clause)
        ):
            locked.append(str(clause.compile(dialect=postgresql.dialect())))
            if changed == "lease":
                conn.execute(
                    update(RestoreOpenSession)
                    .where(RestoreOpenSession.restore_request_item_id == item_id)
                    .values(
                        generation=2, expires_at=dt.datetime.now(dt.UTC) + dt.timedelta(hours=1)
                    )
                )
            else:
                conn.execute(
                    service_module.insert(RestoreItemAttempt).values(
                        restore_request_item_id=item_id,
                        attempt_no=1,
                        receiver_device_id="receiver",
                        server_instance_id=rig.service.instance_id,
                        opened_from_state="queued",
                        opened_at=service_module.func.now(),
                        code_version="race",
                    )
                )

    event.listen(rig.engine, "before_execute", race)
    event.listen(rig.engine, "before_cursor_execute", count_item_updates)
    try:
        rig.service.recover_once()
    finally:
        event.remove(rig.engine, "before_execute", race)
        event.remove(rig.engine, "before_cursor_execute", count_item_updates)
    assert len(locked) == 1
    assert "FOR UPDATE" in locked[0]
    assert item_updates == []
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert (item.state, item.bytes_restored) == ("streaming", 9)
        assert giveup_agreement_violations(session) == []


@pytest.mark.parametrize("state", ["queued", "sent"])
def test_r4_lowered_policy_applies_pending_giveup_with_non_wait_decisive_row(
    rig: _Rig, state: str, clock: list[float]
) -> None:
    item_id = _item(rig, "r4-pending")
    rows = []
    for cause in ("source_integrity", "source_unavailable", "capacity"):
        entry = rig.service._insert_attempt(
            rig.context("receiver"),
            _Outcome(end_kind="error", cause=cause),
            item_id=item_id,
            request_id="request-r4-pending",
            device_id="receiver",
            opened_from_state="queued",
        )
        rig.service._end_attempt(entry, deliver=False)
        rows.append(entry.attempt_id)
    with session_scope(rig.engine) as session:
        session.get(RestoreRequestItem, item_id).state = state
    rig.service.recover_once()
    with session_scope(rig.engine) as session:
        assert session.get(RestoreRequestItem, item_id).state == state
        assert giveup_agreement_violations(session) == []
    rig.service._retry_policy = replace(rig.service._retry_policy, max_failures=2)
    clock[0] += 6
    rig.service.recover_once()
    with session_scope(rig.engine) as session:
        assert session.get(RestoreRequestItem, item_id).state == state
        assert giveup_agreement_violations(session) == []
    rig.service.heartbeat_once()
    rig.service.recover_once()
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, item_id)
        assert (item.state, item.request.state) == ("failed", "completed_with_errors")
        giveup = session.scalar(select(RestoreItemGiveup))
        assert giveup.decisive_attempt_id == rows[1]
        assert (giveup.rule, giveup.counted, giveup.threshold) == ("max_failures", 2, 2)
        assert giveup_agreement_violations(session) == []
    rig.service.recover_once()
    with session_scope(rig.engine) as session:
        assert len(list(session.scalars(select(RestoreItemGiveup)))) == 1
        assert giveup_agreement_violations(session) == []


@pytest.mark.parametrize("leg", ["registry", "own", "stale"])
def test_r3_one_failing_end_does_not_starve_the_pass(
    rig: _Rig,
    clock: list[float],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    leg: str,
) -> None:
    """Each recovery loop contains a bad end and still reaches stranded-item recovery."""

    if leg == "stale":
        _healthy(rig.service, clock)
    entries = []
    attempts = []
    for name in ("r3-failing", "r3-succeeding"):
        item_id = _item(rig, name)
        if leg == "registry":
            entry = rig.service._insert_attempt(
                rig.context("receiver"),
                _Outcome(end_kind="error", cause="source_integrity"),
                item_id=item_id,
                request_id=f"request-{name}",
                device_id="receiver",
                opened_from_state="queued",
            )
            rig.service._keep_ended(entry)
            entries.append(entry)
            attempt_id = entry.attempt_id
        else:
            attempt_id = _abandoned(
                rig,
                item_id,
                instance_id=rig.service.instance_id if leg == "own" else None,
            )
        attempts.append((attempt_id, item_id))
    (first_id, first_item), (second_id, second_item) = attempts
    stranded = _stranded(rig, "r3-beside-ends")
    real_end = rig.service._end_transaction

    def fail_first(entry: Any, *, deliver: bool, margin: Any = None) -> Any:
        if entry.attempt_id == first_id:
            raise RuntimeError("persistent end failure")
        return real_end(entry, deliver=deliver, margin=margin)

    monkeypatch.setattr(rig.service, "_end_transaction", fail_first)
    rig.service.recover_once()
    assert second_id not in rig.service._registry
    row = _attempt_rows(rig, second_item)[0]
    if leg == "registry":
        first, second = entries
        assert second.attempt_id not in rig.service._registry
        assert (row.end_kind, row.cause) == ("error", "source_integrity")
        assert row.ended_at is not None
        assert rig.service._registry[first.attempt_id] is first
        assert first.ended
    else:
        assert (row.end_kind, row.cause, row.ended_at) == ("lost", None, None)
        assert row.closed_at is not None
    first_row = _attempt_rows(rig, first_item)[0]
    assert (first_row.end_kind, first_row.ended_at, first_row.closed_at) == (None, None, None)
    assert f"attempt {first_id} (instance {rig.service.instance_id})" in caplog.text
    with session_scope(rig.engine) as session:
        item = session.get(RestoreRequestItem, stranded)
        assert (item.state, item.bytes_restored) == ("queued", 0)
        assert giveup_agreement_violations(session) == []


def test_r5_recovery_pass_runs_at_start(rig: _Rig) -> None:
    """A stranded item must recover before the first 60-second periodic interval."""

    item_id = _stranded(rig, "r5-start")
    service = RestoreService(replace(rig.service_config, recovery_seconds=60))
    service.start(background=True)
    try:
        deadline = time.monotonic() + 2
        recovered = False
        while time.monotonic() < deadline:
            with session_scope(rig.engine) as session:
                recovered = session.get(RestoreRequestItem, item_id).state == "queued"
            if recovered:
                break
            time.sleep(0.02)
        assert recovered, "start did not recover the stranded item within 2 s"
        with session_scope(rig.engine) as session:
            assert giveup_agreement_violations(session) == []
    finally:
        service.stop()


def test_r5_slow_recovery_does_not_age_heartbeat(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    import threading

    entered = threading.Event()
    release = threading.Event()
    service = RestoreService(replace(rig.service_config, heartbeat_seconds=0.03))
    real_end = service._end_attempt
    item_id = _item(rig, "r5-slow")

    def slow(entry: Any, **kwargs: Any) -> Any:
        entered.set()
        assert release.wait(4), "test did not release its recovery end"
        return real_end(entry, **kwargs)

    monkeypatch.setattr(service, "_end_attempt", slow)
    service.start(background=True)
    heartbeat_thread = service._heartbeat_thread
    recovery_thread = service._recovery_thread
    try:
        _abandoned(rig, item_id, instance_id=service.instance_id)
        assert entered.wait(2), "background recovery never reached the end"
        assert recovery_thread is not None
        assert recovery_thread.daemon
        with session_scope(rig.engine) as session:
            first = session.get(RestoreServerInstance, service.instance_id).heartbeat_at
        before = service.heartbeat_unbroken_seconds()
        deadline = time.monotonic() + 3
        advanced = False
        while time.monotonic() < deadline:
            with session_scope(rig.engine) as session:
                advanced = (
                    session.get(RestoreServerInstance, service.instance_id).heartbeat_at > first
                )
            if advanced:
                break
            time.sleep(0.03)
        assert advanced, "heartbeat_at did not advance while recovery was blocked"
        assert service.heartbeat_unbroken_seconds() > before
    finally:
        release.set()
        service.stop()
    assert not heartbeat_thread.is_alive()
    assert not recovery_thread.is_alive()
    _assert_lost(rig, item_id)


def test_r5_recovery_fault_is_logged_and_next_pass_can_run(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    item_id = _item(rig, "r5-fault")
    _abandoned(rig, item_id, instance_id=rig.service.instance_id)
    with monkeypatch.context() as patch:

        def fail(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("temporary recovery fault")

        patch.setattr(rig.service, "_recovery_entries", fail)
        rig.service.recover_once()
    assert f"restore service recovery failed (instance {rig.service.instance_id})" in caplog.text
    assert "temporary recovery fault" in caplog.text
    assert _attempt_rows(rig, item_id)[0].closed_at is None
    rig.service.recover_once()
    _assert_lost(rig, item_id)
