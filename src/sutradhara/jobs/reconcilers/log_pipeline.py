"""Self-monitoring reconciler for the unified logs collection pipeline.

The domain keeps one durable ``ReconciliationCondition`` for the live
VictoriaLogs/collector path.  It observes the newest stored record and the
newest ``source="log_pipeline"`` heartbeat; stale or failed observations open
the condition, and a later healthy probe satisfies it.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from sqlalchemy.orm import Session

from sutradhara.jobs.models import ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_OPEN,
    OBSERVED_MISSING,
    OBSERVED_PRESENT,
    record_observation,
)
from sutradhara.jobs.reconcilers.registry import Reconciler, TargetObservation, register_reconciler
from sutradhara.logs_store import (
    VictoriaLogsClient,
    VictoriaLogsQueryError,
    VictoriaLogsUnavailable,
    log_store_client_from_env,
    parse_vl_timestamp,
)

DOMAIN = "log_pipeline"
TARGET_KEY = "pipeline"
CADENCE_SECONDS = 5 * 60
STALE_AFTER = dt.timedelta(minutes=5)
_CACHE_KEY = "log_pipeline_probe_result"


@dataclass(frozen=True)
class ProbeResult:
    """Observed health of the VictoriaLogs ingest/read path."""

    healthy: bool
    reason: str | None
    message: str | None
    newest_age_seconds: float | None
    heartbeat_age_seconds: float | None


def refresh_condition(
    session: Session,
    *,
    client: VictoriaLogsClient | None = None,
    now: dt.datetime | None = None,
    stale_after: dt.timedelta = STALE_AFTER,
) -> ReconciliationCondition:
    """Probe the log pipeline and update its durable condition row."""

    result = probe_log_pipeline(client=client, now=now, stale_after=stale_after)
    row = record_observation(
        session,
        domain=DOMAIN,
        target_key=TARGET_KEY,
        desired=True,
        observed_state=OBSERVED_PRESENT if result.healthy else OBSERVED_MISSING,
    )
    _classify_from_result(session, row, result)
    return row


def probe_log_pipeline(
    *,
    client: VictoriaLogsClient | None = None,
    now: dt.datetime | None = None,
    stale_after: dt.timedelta = STALE_AFTER,
) -> ProbeResult:
    """Return the current log pipeline health from VictoriaLogs stats queries."""

    final_client = client or log_store_client_from_env()
    observed_at = _as_utc(now or dt.datetime.now(dt.UTC))
    try:
        newest = _newest_time(final_client, "* | stats max(_time) as newest_time")
        heartbeat = _newest_time(
            final_client,
            'source:="log_pipeline" | stats max(_time) as newest_time',
        )
    except (VictoriaLogsUnavailable, VictoriaLogsQueryError) as exc:
        return ProbeResult(
            healthy=False,
            reason="query-failed",
            message=f"VictoriaLogs self-monitor query failed: {type(exc).__name__}",
            newest_age_seconds=None,
            heartbeat_age_seconds=None,
        )

    if newest is None:
        return ProbeResult(
            healthy=False,
            reason="newest-record-missing",
            message="VictoriaLogs has no records visible to the log pipeline probe",
            newest_age_seconds=None,
            heartbeat_age_seconds=None,
        )
    if heartbeat is None:
        newest_age = max((observed_at - newest).total_seconds(), 0.0)
        return ProbeResult(
            healthy=False,
            reason="heartbeat-missing",
            message="No log_pipeline heartbeat record is visible in VictoriaLogs",
            newest_age_seconds=newest_age,
            heartbeat_age_seconds=None,
        )

    newest_age = max((observed_at - newest).total_seconds(), 0.0)
    heartbeat_age = max((observed_at - heartbeat).total_seconds(), 0.0)
    if heartbeat_age > stale_after.total_seconds():
        return ProbeResult(
            healthy=False,
            reason="heartbeat-stale",
            message=f"log_pipeline heartbeat age is {heartbeat_age:.0f}s",
            newest_age_seconds=newest_age,
            heartbeat_age_seconds=heartbeat_age,
        )
    if newest_age > stale_after.total_seconds():
        return ProbeResult(
            healthy=False,
            reason="newest-record-stale",
            message=f"newest VictoriaLogs record age is {newest_age:.0f}s",
            newest_age_seconds=newest_age,
            heartbeat_age_seconds=heartbeat_age,
        )
    return ProbeResult(
        healthy=True,
        reason=None,
        message=None,
        newest_age_seconds=newest_age,
        heartbeat_age_seconds=heartbeat_age,
    )


def enumerate_targets(
    session: Session,
    cursor: int | None,
    batch: int,
) -> list[TargetObservation]:
    """Refresh the singleton target for the generic reconciler discover pass."""

    result = probe_log_pipeline()
    _cache_result(session, result)
    return [_observation_from_result(result)]


def observe(session: Session, target_key: str) -> TargetObservation:
    """Observe the singleton log pipeline target."""

    if target_key != TARGET_KEY:
        return TargetObservation(
            target_key=target_key,
            desired=False,
            observed_state=OBSERVED_MISSING,
        )
    result = probe_log_pipeline()
    _cache_result(session, result)
    return _observation_from_result(result)


def reconcile_target(session: Session, target_key: str) -> None:
    """No-op actuator; this domain projects an alarm but does not enqueue work."""

    del session, target_key


def classify_condition(
    session: Session,
    target_key: str,
    condition: ReconciliationCondition,
) -> None:
    """Attach plain reason/detail to open log pipeline conditions."""

    result = session.info.pop(_CACHE_KEY, None)
    if not isinstance(result, ProbeResult):
        result = probe_log_pipeline()
    _classify_from_result(session, condition, result)


def _classify_from_result(
    session: Session,
    row: ReconciliationCondition,
    result: ProbeResult,
) -> None:
    if result.healthy:
        session.flush([row])
        return
    row.condition = CONDITION_OPEN
    row.reason = result.reason
    row.message = result.message
    session.flush([row])


def _observation_from_result(result: ProbeResult) -> TargetObservation:
    return TargetObservation(
        target_key=TARGET_KEY,
        desired=True,
        observed_state=OBSERVED_PRESENT if result.healthy else OBSERVED_MISSING,
    )


def _cache_result(session: Session, result: ProbeResult) -> None:
    session.info[_CACHE_KEY] = result


def _newest_time(client: VictoriaLogsClient, query: str) -> dt.datetime | None:
    rows = client.query(query)
    if not rows:
        return None
    return parse_vl_timestamp(rows[0].get("newest_time"))


def _as_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


register_reconciler(DOMAIN)(
    Reconciler(
        enumerate_targets=enumerate_targets,
        observe=observe,
        reconcile_target=reconcile_target,
        classify_condition=classify_condition,
    )
)
