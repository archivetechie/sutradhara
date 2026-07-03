"""Gap-board alarm projection for hdcache operational conditions.

M6 publishes cache degradation as reconciler-style conditions owned by the
archive operator. The console can read these alongside ordinary reconciliation
gaps instead of scraping logs for reserve, lost-backlog, disk, walker, privacy,
or fallback symptoms.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import Engine
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.hdcache.fill import JOB_KIND
from sutradhara.hdcache.models import CacheDisk, CacheEntry
from sutradhara.hdcache.placement import DiskState, placement_config_from_env
from sutradhara.hdcache.walker import HdcacheWalkerEvent
from sutradhara.jobs.models import Job, LIVE_JOB_STATUS_VALUES, ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_OPEN,
    OBSERVED_MISSING,
    OBSERVED_PRESENT,
    record_observation,
)

if TYPE_CHECKING:
    from sutradhara.hdcache.manager import RestoreEvent

ALARM_DOMAIN = "hdcache_alarm"
ALARM_OWNER = "archive operator"
DEFAULT_LOST_BACKLOG_THRESHOLD = 1
DEFAULT_LOST_GROWTH_SECONDS = 30 * 60
DEFAULT_FILL_QUEUE_STALLED_SECONDS = 15 * 60


@dataclass(frozen=True)
class RestoreEventAlarmSink:
    """DB-backed sink that projects restore events into alarm conditions."""

    engine: Engine | None = None
    session: Session | None = None

    def __call__(self, event: RestoreEvent) -> None:
        if self.session is not None:
            record_restore_event_alarm(self.session, event)
            return
        engine = self.engine or make_engine()
        with session_scope(engine) as session:
            record_restore_event_alarm(session, event)

    def bind(self, session: Session) -> RestoreEventAlarmSink:
        return RestoreEventAlarmSink(session=session)


@dataclass(frozen=True)
class WalkerEventAlarmSink:
    """DB-backed sink that projects walker events into alarm conditions."""

    engine: Engine | None = None
    session: Session | None = None

    def __call__(self, event: HdcacheWalkerEvent) -> None:
        if self.session is not None:
            record_walker_event_alarm(self.session, event)
            return
        engine = self.engine or make_engine()
        with session_scope(engine) as session:
            record_walker_event_alarm(session, event)

    def bind(self, session: Session) -> WalkerEventAlarmSink:
        return WalkerEventAlarmSink(session=session)


def restore_event_alarm_sink(
    *,
    engine: Engine | None = None,
    session: Session | None = None,
) -> RestoreEventAlarmSink:
    """Return a DB-backed restore event sink."""

    return RestoreEventAlarmSink(engine=engine, session=session)


def walker_event_alarm_sink(
    *,
    engine: Engine | None = None,
    session: Session | None = None,
) -> WalkerEventAlarmSink:
    """Return a DB-backed walker event sink."""

    return WalkerEventAlarmSink(engine=engine, session=session)


@dataclass(frozen=True)
class HdcacheAlarmConfig:
    """Thresholds for hdcache alarm condition projection."""

    lost_backlog_threshold: int = DEFAULT_LOST_BACKLOG_THRESHOLD
    lost_growth_seconds: int = DEFAULT_LOST_GROWTH_SECONDS
    fill_queue_stalled_seconds: int = DEFAULT_FILL_QUEUE_STALLED_SECONDS
    now: dt.datetime | None = None

    def __post_init__(self) -> None:
        if self.lost_backlog_threshold < 0:
            raise ValueError("lost_backlog_threshold must be non-negative")
        if self.lost_growth_seconds <= 0:
            raise ValueError("lost_growth_seconds must be positive")
        if self.fill_queue_stalled_seconds <= 0:
            raise ValueError("fill_queue_stalled_seconds must be positive")


def evaluate_hdcache_alarm_conditions(
    session: Session,
    *,
    config: HdcacheAlarmConfig | None = None,
) -> list[ReconciliationCondition]:
    """Refresh threshold-based hdcache alarm conditions."""

    final_config = config or HdcacheAlarmConfig()
    now = _as_utc(final_config.now or dt.datetime.now(dt.UTC))
    rows: list[ReconciliationCondition] = []
    placement_config = placement_config_from_env()
    for disk in session.scalars(select(CacheDisk).order_by(CacheDisk.disk_id)):
        disk_state = DiskState(
            disk_id=disk.disk_id,
            state=disk.state,
            capacity_bytes=disk.capacity_bytes,
            filled_bytes=disk.filled_bytes,
            enclosure=disk.enclosure,
            slot=disk.slot,
        )
        reserve_breached = (
            disk.state == "active"
            and disk.capacity_bytes > 0
            and disk_state.free_bytes < placement_config.reserve_bytes(disk_state)
        )
        rows.append(
            _set_alarm(
                session,
                f"reserve-breach:{disk.disk_id}",
                active=reserve_breached,
                reason="reserve-breach",
                message=f"cache disk {disk.disk_id} is below configured reserve",
            )
        )
        rows.append(
            _set_alarm(
                session,
                f"disk-unreachable:{disk.disk_id}",
                active=disk.state == "absent",
                reason="disk-unreachable",
                message=f"cache disk {disk.disk_id} is absent or circuit-broken",
            )
        )
        smart_degraded = bool(
            disk.smart_status
            and disk.smart_status.strip().lower() not in {"ok", "healthy", "passed"}
        )
        rows.append(
            _set_alarm(
                session,
                f"smart-degradation:{disk.disk_id}",
                active=smart_degraded,
                reason="smart-degradation",
                message=f"cache disk {disk.disk_id} SMART status: {disk.smart_status}",
            )
        )

    lost_count = int(
        session.scalar(select(func.count()).select_from(CacheEntry).where(CacheEntry.state == "lost"))
        or 0
    )
    rows.append(
        _set_alarm(
            session,
            "lost-backlog",
            active=lost_count > final_config.lost_backlog_threshold,
            reason="lost-backlog",
            message=f"{lost_count} lost hdcache entries exceed threshold "
            f"{final_config.lost_backlog_threshold}",
        )
    )
    rows.append(_evaluate_lost_growth(session, lost_count, now=now, config=final_config))
    rows.append(_evaluate_fill_queue_stalled(session, now=now, config=final_config))
    return rows


def record_restore_event_alarm(
    session: Session,
    event: RestoreEvent,
) -> ReconciliationCondition | None:
    """Project a restore/fallback event into a gap-board alarm condition."""

    if event.code == "privacy-unmapped":
        return _set_alarm(
            session,
            "unmapped-privacy-level",
            active=True,
            reason="unmapped-privacy-level",
            message=event.detail or "privacy level is not mapped to a restore capability",
        )
    if event.code == "disk-circuit-open":
        return _set_alarm(
            session,
            "disk-unreachable:restore",
            active=True,
            reason="disk-unreachable",
            message=event.detail or event.code,
        )
    if event.code == "disk-circuit-closed":
        return _set_alarm(
            session,
            "disk-unreachable:restore",
            active=False,
            reason="disk-unreachable",
            message=event.detail or event.code,
        )
    if event.code.startswith("cache-fallback:"):
        reason = event.code.split(":", 1)[1]
        return _set_alarm(
            session,
            f"fallback-reason:{reason}",
            active=True,
            reason="fallback-reason-spike",
            message=event.detail or f"cache fallback reason {reason}",
        )
    return None


def record_walker_event_alarm(
    session: Session,
    event: HdcacheWalkerEvent,
) -> ReconciliationCondition | None:
    """Project walker reason-coded events into gap-board alarm conditions."""

    if event.code == "walker-tripwire-halt":
        return _set_alarm(
            session,
            f"walker-tripwire:{event.disk_id or 'unknown'}",
            active=True,
            reason="walker-tripwire",
            message=event.detail or "hdcache walker tripwire halted destructive mode",
        )
    if event.code in {"disk-identity-mismatch", "walker-disk-absent"}:
        return _set_alarm(
            session,
            f"disk-unreachable:{event.disk_id or 'unknown'}",
            active=True,
            reason="disk-unreachable",
            message=event.detail or event.code,
        )
    return None


def alarm_condition_payload(row: ReconciliationCondition) -> dict[str, object]:
    """Return a JSON-friendly alarm condition with the fixed owner string."""

    return {
        "domain": row.domain,
        "target_key": row.target_key,
        "condition": row.condition,
        "reason": row.reason,
        "message": row.message,
        "owner": ALARM_OWNER,
        "updated_at": row.updated_at.isoformat(),
    }


def _evaluate_lost_growth(
    session: Session,
    lost_count: int,
    *,
    now: dt.datetime,
    config: HdcacheAlarmConfig,
) -> ReconciliationCondition:
    row = _get_alarm(session, "lost-backlog-growth")
    previous_count = _parse_lost_count(None if row is None else row.message)
    active = (
        row is not None
        and previous_count is not None
        and lost_count > previous_count
        and (_as_utc(row.updated_at) + dt.timedelta(seconds=config.lost_growth_seconds)) <= now
    )
    updated = _set_alarm(
        session,
        "lost-backlog-growth",
        active=active,
        reason="lost-backlog-growth",
        message=f"lost_count={lost_count}",
    )
    if not active:
        updated.message = f"lost_count={lost_count}"
        session.flush([updated])
    return updated


def _evaluate_fill_queue_stalled(
    session: Session,
    *,
    now: dt.datetime,
    config: HdcacheAlarmConfig,
) -> ReconciliationCondition:
    threshold = now - dt.timedelta(seconds=config.fill_queue_stalled_seconds)
    stalled = (
        session.scalars(
            select(Job)
            .where(
                Job.kind == JOB_KIND,
                Job.status.in_(LIVE_JOB_STATUS_VALUES),
                Job.created_at <= threshold,
            )
            .limit(1)
        ).first()
        is not None
    )
    return _set_alarm(
        session,
        "fill-queue-stalled",
        active=stalled,
        reason="fill-queue-stalled",
        message="hdcache fill queue has live jobs older than the stall threshold",
    )


def _set_alarm(
    session: Session,
    target_key: str,
    *,
    active: bool,
    reason: str,
    message: str,
) -> ReconciliationCondition:
    row = record_observation(
        session,
        domain=ALARM_DOMAIN,
        target_key=target_key,
        desired=active,
        observed_state=OBSERVED_MISSING if active else OBSERVED_PRESENT,
    )
    if active:
        row.condition = CONDITION_OPEN
        row.reason = reason
        row.message = message
    session.flush([row])
    return row


def _get_alarm(session: Session, target_key: str) -> ReconciliationCondition | None:
    return session.scalars(
        select(ReconciliationCondition).where(
            ReconciliationCondition.domain == ALARM_DOMAIN,
            ReconciliationCondition.target_key == target_key,
        )
    ).one_or_none()


def _parse_lost_count(message: str | None) -> int | None:
    if not message or not message.startswith("lost_count="):
        return None
    try:
        return int(message.split("=", 1)[1])
    except ValueError:
        return None


def _as_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)
