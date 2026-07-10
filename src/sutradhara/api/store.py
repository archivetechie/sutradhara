"""Durable idempotency and source-claim storage for the receive HTTP API."""

from __future__ import annotations

import contextlib
import datetime as dt
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    UniqueConstraint,
    and_,
    delete,
    false,
    func,
    or_,
    select,
    update,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column

from sutradhara.catalog.models import Base
from sutradhara.catalog.session import make_session_factory

DEFAULT_TTL = dt.timedelta(minutes=30)
DEFAULT_HEARTBEAT_INTERVAL = dt.timedelta(seconds=5)
ORPHAN_RECONCILE_BATCH = 100
RECEIVE_ENDPOINT = "/api/receive"
DEVICE_RECEIVE_ENDPOINT = "POST /api/devices/receive"
DUPLICATE_WARNED_EVENT = "receive_duplicate_warned"
DUPLICATE_ACKNOWLEDGED_EVENT = "receive_duplicate_acknowledged"
CARD_LEASE_PREFIX = "card-identity:"
LOG = logging.getLogger(__name__)

IdempotencyState = Literal["claimed", "completed", "conflict", "in_progress"]
DeviceIntentState = Literal[
    "warned",
    "authorized",
    "completed",
    "conflict",
    "in_progress",
    "busy",
    "terminal",
]
AckFailureState = Literal["failed", "in_progress", "unchanged"]
LeaseRenewalState = Literal["renewed", "throttled", "lost"]


class IdempotencyRecord(Base):
    """One durable idempotency row scoped by operator, endpoint, and client key."""

    __tablename__ = "idempotency_record"
    __table_args__ = (
        UniqueConstraint(
            "operator_username",
            "endpoint",
            "idempotency_key",
            name="uq_idempotency_scope",
        ),
        CheckConstraint(
            "status IN ('in_progress', 'completed', 'warned', 'authorized', 'started', "
            "'committed', 'aborted', 'quarantined', 'failed')",
            name="ck_idempotency_record_status",
        ),
        Index("ix_idempotency_record_last_heartbeat", "last_heartbeat"),
        Index(
            "ix_idempotency_record_card_intent",
            "endpoint",
            "card_identity",
            "status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    operator_username: Mapped[str] = mapped_column(String(256), nullable=False)
    endpoint: Mapped[str] = mapped_column(String(128), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    intake_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    response_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    device_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    card_identity: Mapped[str | None] = mapped_column(String(128), nullable=True)
    card_label: Mapped[str | None] = mapped_column(String(512), nullable=True)
    duplicate_warning: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    duplicate_acknowledged: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )
    lease_source_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    warned_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    authorized_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    started_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    terminal_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow()
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow()
    )
    last_heartbeat: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow()
    )


class SourceClaim(Base):
    """One durable lease for a receive source while a receive is in progress."""

    __tablename__ = "source_claim"
    __table_args__ = (Index("ix_source_claim_last_heartbeat", "last_heartbeat"),)

    source_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    operator_username: Mapped[str] = mapped_column(String(256), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False)
    intake_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow()
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow()
    )
    last_heartbeat: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow()
    )


@dataclass(frozen=True)
class IdempotencyDecision:
    """Result of attempting to claim or replay an idempotent API request."""

    state: IdempotencyState
    response_json: dict[str, Any] | None = None


@dataclass(frozen=True)
class DeviceIntentDecision:
    """Result of claiming or replaying a card-identity receive intent."""

    state: DeviceIntentState
    response_json: dict[str, Any] | None = None
    terminal_state: str | None = None


@dataclass(frozen=True)
class StartIntentDecision:
    """Atomic authorization result used by the proto-unchanged StartIntake RPC."""

    state: Literal["claimed", "resume", "missing"]
    intake_id: str | None = None


def begin_idempotency(
    engine: Any,
    *,
    operator_username: str,
    endpoint: str,
    idempotency_key: str,
    request_hash: str,
    ttl: dt.timedelta = DEFAULT_TTL,
) -> IdempotencyDecision:
    """Atomically claim an idempotency key or return its replay/conflict state."""

    try:
        return _begin_idempotency_once(
            engine,
            operator_username=operator_username,
            endpoint=endpoint,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            ttl=ttl,
        )
    except IntegrityError:
        return IdempotencyDecision("in_progress")


def begin_device_receive_intent(
    engine: Any,
    *,
    operator_username: str,
    device_id: str,
    card_identity: str,
    card_label: str | None,
    idempotency_key: str,
    request_hash: str,
    acknowledge_duplicate: bool,
    ttl: dt.timedelta = DEFAULT_TTL,
) -> DeviceIntentDecision:
    """Atomically perform the duplicate handshake and card lease transition."""

    try:
        return _begin_device_receive_intent_once(
            engine,
            operator_username=operator_username,
            device_id=device_id,
            card_identity=card_identity,
            card_label=card_label,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            acknowledge_duplicate=acknowledge_duplicate,
            ttl=ttl,
        )
    except IntegrityError:
        try:
            return _begin_device_receive_intent_once(
                engine,
                operator_username=operator_username,
                device_id=device_id,
                card_identity=card_identity,
                card_label=card_label,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                acknowledge_duplicate=acknowledge_duplicate,
                ttl=ttl,
            )
        except IntegrityError:
            return DeviceIntentDecision("in_progress")


def claim_start_intake(
    session: Session,
    *,
    operator_username: str,
    device_id: str,
    idempotency_key: str,
    intake_id: str,
    ttl: dt.timedelta = DEFAULT_TTL,
) -> StartIntentDecision:
    """Link StartIntake to an authorized HTTP intent in the caller transaction."""

    record = _idempotency_record(
        session,
        operator_username=operator_username,
        endpoint=DEVICE_RECEIVE_ENDPOINT,
        idempotency_key=idempotency_key,
    )
    if record is None or record.device_id != device_id:
        return StartIntentDecision("missing")
    now = _utcnow()
    if record.status in {"authorized", "started"} and _is_stale(
        record.last_heartbeat,
        ttl=ttl,
        now=now,
    ):
        record.status = "failed"
        record.terminal_at = now
        record.updated_at = now
        _release_record_lease(session, record)
        return StartIntentDecision("missing")
    if record.status == "started" and record.intake_id:
        return StartIntentDecision("resume", record.intake_id)
    if record.status != "authorized" or record.card_identity is None:
        return StartIntentDecision("missing")
    lease_source_id = record.lease_source_id or card_lease_source_id(record.card_identity)
    if not _claim_source_in_session(
        session,
        source_id=lease_source_id,
        operator_username=operator_username,
        idempotency_key=idempotency_key,
        ttl=ttl,
        now=now,
    ):
        return StartIntentDecision("missing")
    claimed = session.execute(
        update(IdempotencyRecord)
        .where(
            IdempotencyRecord.id == record.id,
            IdempotencyRecord.status == "authorized",
        )
        .values(
            status="started",
            intake_id=intake_id,
            lease_source_id=lease_source_id,
            started_at=now,
            updated_at=now,
            last_heartbeat=now,
        )
    )
    if claimed.rowcount != 1:
        session.expire(record)
        session.refresh(record)
        if record.status == "started" and record.intake_id:
            return StartIntentDecision("resume", record.intake_id)
        return StartIntentDecision("missing")
    claim = session.get(SourceClaim, lease_source_id)
    if claim is not None and claim.idempotency_key == idempotency_key:
        claim.intake_id = intake_id
        claim.updated_at = now
    return StartIntentDecision("claimed", intake_id)


def store_device_receive_response(
    engine: Any,
    *,
    operator_username: str,
    device_id: str,
    idempotency_key: str,
    intake_id: str,
    response_json: dict[str, Any],
) -> bool:
    """Persist the HTTP response without collapsing the intent's started state."""

    now = _utcnow()
    factory = make_session_factory(engine)
    with factory.begin() as session:
        record = _idempotency_record(
            session,
            operator_username=operator_username,
            endpoint=DEVICE_RECEIVE_ENDPOINT,
            idempotency_key=idempotency_key,
        )
        if (
            record is None
            or record.device_id != device_id
            or record.intake_id != intake_id
            or record.status not in {"started", "committed"}
        ):
            return False
        record.response_json = response_json
        record.updated_at = now
        record.last_heartbeat = now
        return True


def transition_device_intent_terminal(
    session: Session,
    *,
    intake_id: str,
    terminal_state: Literal["committed", "aborted", "quarantined", "failed"],
) -> bool:
    """Terminalize an intake-linked intent and release its card lease atomically."""

    record = session.scalars(
        select(IdempotencyRecord).where(
            IdempotencyRecord.endpoint == DEVICE_RECEIVE_ENDPOINT,
            IdempotencyRecord.intake_id == intake_id,
        )
    ).one_or_none()
    if record is None:
        return False
    if record.status == terminal_state:
        return True
    if record.status in {"aborted", "quarantined", "failed"}:
        return False
    if record.status not in {"started", "committed"}:
        return False
    now = _utcnow()
    record.status = terminal_state
    record.terminal_at = now
    record.updated_at = now
    record.last_heartbeat = now
    _release_record_lease(session, record)
    return True


def fail_device_receive_intent(
    engine: Any,
    *,
    operator_username: str,
    device_id: str,
    idempotency_key: str,
) -> bool:
    """Fail an authorized or started intent and release its lease for a clean retry."""

    now = _utcnow()
    factory = make_session_factory(engine)
    with factory.begin() as session:
        record = _idempotency_record(
            session,
            operator_username=operator_username,
            endpoint=DEVICE_RECEIVE_ENDPOINT,
            idempotency_key=idempotency_key,
        )
        if record is None or record.device_id != device_id:
            return False
        if record.status not in {"authorized", "started"}:
            return False
        record.status = "failed"
        record.terminal_at = now
        record.updated_at = now
        record.last_heartbeat = now
        _release_record_lease(session, record)
        return True


def fail_device_receive_intent_if_unstarted(
    engine: Any,
    *,
    operator_username: str,
    device_id: str,
    idempotency_key: str,
) -> AckFailureState:
    """Fail an ack-wait intent only while no live gRPC intake has claimed it.

    ``StartIntake`` and the HTTP ack waiter run independently.  A command-stream
    failure may therefore reach the waiter after the helper has already started
    uploading.  That live receive owns the lease and must be allowed to finish.
    """

    from sutradhara.grpc.store import GrpcIntake

    now = _utcnow()
    factory = make_session_factory(engine)
    with factory.begin() as session:
        record = _idempotency_record(
            session,
            operator_username=operator_username,
            endpoint=DEVICE_RECEIVE_ENDPOINT,
            idempotency_key=idempotency_key,
        )
        if record is None or record.device_id != device_id:
            return "unchanged"
        live_intake = session.scalar(
            select(GrpcIntake.intake_id)
            .where(
                GrpcIntake.operator == operator_username,
                GrpcIntake.device_id == device_id,
                GrpcIntake.idempotency_key == idempotency_key,
                GrpcIntake.state.in_(("streaming", "committing", "committed")),
            )
            .limit(1)
        )
        if record.status == "started" or live_intake is not None:
            return "in_progress"
        if record.status != "authorized":
            return "unchanged"
        failed = session.execute(
            update(IdempotencyRecord)
            .where(
                IdempotencyRecord.id == record.id,
                IdempotencyRecord.status == "authorized",
            )
            .values(
                status="failed",
                terminal_at=now,
                updated_at=now,
                last_heartbeat=now,
            )
            .execution_options(synchronize_session=False)
        )
        if failed.rowcount != 1:
            session.expire(record)
            session.refresh(record)
            return "in_progress" if record.status == "started" else "unchanged"
        _release_record_lease(session, record)
        return "failed"


def renew_device_intake_lease(
    engine: Any,
    *,
    intake_id: str,
    floor: dt.timedelta = DEFAULT_HEARTBEAT_INTERVAL,
) -> LeaseRenewalState:
    """Renew a started intent lease, distinguishing throttling from claim loss."""

    now = _utcnow()
    factory = make_session_factory(engine)
    with factory.begin() as session:
        record = session.scalars(
            select(IdempotencyRecord).where(
                IdempotencyRecord.endpoint == DEVICE_RECEIVE_ENDPOINT,
                IdempotencyRecord.intake_id == intake_id,
                IdempotencyRecord.status == "started",
            )
        ).one_or_none()
        if record is None or record.lease_source_id is None:
            return "lost"
        claim = session.get(SourceClaim, record.lease_source_id)
        if claim is None or not _claim_owned_by_record(claim, record):
            return "lost"
        if _aware(record.last_heartbeat) + floor > now:
            return "throttled"
        record.updated_at = now
        record.last_heartbeat = now
        claim.updated_at = now
        claim.last_heartbeat = now
        return "renewed"


def reconcile_device_receive_leases(
    engine: Any,
    *,
    ttl: dt.timedelta = DEFAULT_TTL,
) -> dict[str, int]:
    """Rebuild live card leases, expire stale owners, and fail stale orphans."""

    now = _utcnow()
    rebuilt = 0
    expired = 0
    orphaned = 0
    factory = make_session_factory(engine)
    with factory.begin() as session:
        records = list(
            session.scalars(
                select(IdempotencyRecord)
                .where(
                    IdempotencyRecord.endpoint == DEVICE_RECEIVE_ENDPOINT,
                    IdempotencyRecord.status.in_(("authorized", "started")),
                    IdempotencyRecord.card_identity.is_not(None),
                )
                .order_by(IdempotencyRecord.created_at, IdempotencyRecord.id)
            )
        )
        for record in records:
            if _is_stale(record.last_heartbeat, ttl=ttl, now=now):
                terminalized = session.execute(
                    update(IdempotencyRecord)
                    .where(
                        IdempotencyRecord.id == record.id,
                        IdempotencyRecord.status.in_(("authorized", "started")),
                        IdempotencyRecord.last_heartbeat < now - ttl,
                    )
                    .values(
                        status="failed",
                        terminal_at=now,
                        updated_at=now,
                    )
                    .execution_options(synchronize_session=False)
                )
                if terminalized.rowcount != 1:
                    continue
                _release_record_lease(session, record)
                expired += 1
                continue
            assert record.card_identity is not None
            lease_source_id = record.lease_source_id or card_lease_source_id(record.card_identity)
            if _claim_source_in_session(
                session,
                source_id=lease_source_id,
                operator_username=record.operator_username,
                idempotency_key=record.idempotency_key,
                ttl=ttl,
                now=now,
            ):
                record.lease_source_id = lease_source_id
                claim = session.get(SourceClaim, lease_source_id)
                if claim is not None and _claim_owned_by_record(claim, record):
                    claim.intake_id = record.intake_id
                rebuilt += 1
            else:
                record.status = "failed"
                record.terminal_at = now
                record.updated_at = now
                expired += 1
        orphaned = _reconcile_orphaned_grpc_intakes(
            session,
            ttl=ttl,
            now=now,
            batch_size=ORPHAN_RECONCILE_BATCH,
        )
    return {"rebuilt": rebuilt, "expired": expired, "orphaned": orphaned}


def card_lease_source_id(card_identity: str) -> str:
    """Namespace a card identity away from `/api/receive` source ids."""

    return f"{CARD_LEASE_PREFIX}{card_identity}"


def duplicate_telemetry_counts(engine: Any) -> dict[str, int]:
    """Count the two named duplicate-warning outcomes from durable intent rows."""

    factory = make_session_factory(engine)
    with factory() as session:
        unacknowledged = session.scalar(
            select(func.count(IdempotencyRecord.id)).where(
                IdempotencyRecord.endpoint == DEVICE_RECEIVE_ENDPOINT,
                IdempotencyRecord.status == "warned",
                IdempotencyRecord.duplicate_acknowledged.is_(False),
            )
        )
        acknowledged = session.scalar(
            select(func.count(IdempotencyRecord.id)).where(
                IdempotencyRecord.endpoint == DEVICE_RECEIVE_ENDPOINT,
                IdempotencyRecord.duplicate_acknowledged.is_(True),
            )
        )
    return {
        "warned_then_never_acknowledged": int(unacknowledged or 0),
        "warned_then_acknowledged": int(acknowledged or 0),
    }


def complete_idempotency(
    engine: Any,
    *,
    operator_username: str,
    endpoint: str,
    idempotency_key: str,
    intake_id: str,
    response_json: dict[str, Any],
) -> None:
    """Persist the completed response for later same-key replays."""

    now = _utcnow()
    factory = make_session_factory(engine)
    with factory.begin() as session:
        record = _idempotency_record(
            session,
            operator_username=operator_username,
            endpoint=endpoint,
            idempotency_key=idempotency_key,
        )
        if record is None:
            return
        record.status = "completed"
        record.intake_id = intake_id
        record.response_json = response_json
        record.updated_at = now
        record.last_heartbeat = now


def abandon_idempotency(
    engine: Any,
    *,
    operator_username: str,
    endpoint: str,
    idempotency_key: str,
) -> None:
    """Remove an in-progress idempotency row after a non-durable failure."""

    factory = make_session_factory(engine)
    with factory.begin() as session:
        record = _idempotency_record(
            session,
            operator_username=operator_username,
            endpoint=endpoint,
            idempotency_key=idempotency_key,
        )
        if record is not None and record.status == "in_progress":
            session.delete(record)


def refresh_idempotency(
    engine: Any,
    *,
    operator_username: str,
    endpoint: str,
    idempotency_key: str,
) -> None:
    """Heartbeat a live idempotency claim without holding a long transaction."""

    now = _utcnow()
    factory = make_session_factory(engine)
    with factory.begin() as session:
        record = _idempotency_record(
            session,
            operator_username=operator_username,
            endpoint=endpoint,
            idempotency_key=idempotency_key,
        )
        if record is not None and record.status == "in_progress":
            record.updated_at = now
            record.last_heartbeat = now


def claim_source(
    engine: Any,
    *,
    source_id: str,
    operator_username: str,
    idempotency_key: str,
    ttl: dt.timedelta = DEFAULT_TTL,
) -> bool:
    """Atomically claim a source, reclaiming only stale heartbeat leases."""

    now = _utcnow()
    factory = make_session_factory(engine)
    try:
        with factory.begin() as session:
            return _claim_source_in_session(
                session,
                source_id=source_id,
                operator_username=operator_username,
                idempotency_key=idempotency_key,
                ttl=ttl,
                now=now,
            )
    except IntegrityError:
        return False


def release_source(
    engine: Any,
    *,
    source_id: str,
    idempotency_key: str,
) -> None:
    """Release only the source lease owned by this receive intent."""

    factory = make_session_factory(engine)
    with factory.begin() as session:
        claim = session.get(SourceClaim, source_id)
        if claim is not None and claim.idempotency_key == idempotency_key:
            session.delete(claim)


def refresh_source_claim(
    engine: Any,
    *,
    source_id: str,
    idempotency_key: str,
) -> None:
    """Heartbeat a source lease without holding the receive transaction open."""

    now = _utcnow()
    factory = make_session_factory(engine)
    with factory.begin() as session:
        claim = session.get(SourceClaim, source_id)
        if claim is not None and claim.idempotency_key == idempotency_key:
            claim.updated_at = now
            claim.last_heartbeat = now


def source_busy_ids(
    engine: Any,
    source_ids: list[str],
    *,
    ttl: dt.timedelta = DEFAULT_TTL,
) -> set[str]:
    """Return active, non-stale source ids for receive options rendering."""

    now = _utcnow()
    factory = make_session_factory(engine)
    with factory.begin() as session:
        _delete_stale_claims(session, ttl=ttl, now=now)
        claims = session.scalars(select(SourceClaim).where(SourceClaim.source_id.in_(source_ids)))
        return {claim.source_id for claim in claims}


def _begin_idempotency_once(
    engine: Any,
    *,
    operator_username: str,
    endpoint: str,
    idempotency_key: str,
    request_hash: str,
    ttl: dt.timedelta,
) -> IdempotencyDecision:
    now = _utcnow()
    factory = make_session_factory(engine)
    with factory.begin() as session:
        record = _idempotency_record(
            session,
            operator_username=operator_username,
            endpoint=endpoint,
            idempotency_key=idempotency_key,
        )
        if record is None:
            session.add(
                IdempotencyRecord(
                    operator_username=operator_username,
                    endpoint=endpoint,
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    status="in_progress",
                    created_at=now,
                    updated_at=now,
                    last_heartbeat=now,
                )
            )
            return IdempotencyDecision("claimed")

        if record.request_hash != request_hash:
            return IdempotencyDecision("conflict")
        if record.status == "completed":
            return IdempotencyDecision("completed", record.response_json)
        if not _is_stale(record.last_heartbeat, ttl=ttl, now=now):
            return IdempotencyDecision("in_progress")

        record.status = "in_progress"
        record.intake_id = None
        record.response_json = None
        record.created_at = now
        record.updated_at = now
        record.last_heartbeat = now
        return IdempotencyDecision("claimed")


def peek_device_receive_intent(
    engine: Any,
    *,
    operator_username: str,
    idempotency_key: str,
    request_hash: str,
    acknowledge_duplicate: bool,
    ttl: dt.timedelta = DEFAULT_TTL,
) -> DeviceIntentDecision | None:
    """Read-only pre-check so idempotency verdicts precede card resolution.

    A same-key/different-body request must surface ``idempotency_conflict`` even
    when the mutated body references a card that no longer resolves, and stored
    warned/completed responses must replay without requiring the card to still be
    mounted. Never creates records, transitions states, or touches leases; every
    other outcome falls through to ``begin_device_receive_intent``.
    """
    factory = make_session_factory(engine)
    with factory.begin() as session:
        record = _idempotency_record(
            session,
            operator_username=operator_username,
            endpoint=DEVICE_RECEIVE_ENDPOINT,
            idempotency_key=idempotency_key,
        )
        if record is None:
            return None
        if record.request_hash != request_hash:
            return DeviceIntentDecision("conflict")
        if record.status == "warned" and not acknowledge_duplicate:
            return DeviceIntentDecision("warned", record.duplicate_warning)
        if record.status in {"aborted", "quarantined", "failed"}:
            return DeviceIntentDecision("terminal", terminal_state=record.status)
        if (
            record.status in {"authorized", "started", "committed"}
            and record.response_json is not None
        ):
            return DeviceIntentDecision("completed", record.response_json)
        if record.status == "committed":
            return DeviceIntentDecision("terminal", terminal_state=record.status)
        if record.status in {"authorized", "started"} and _is_stale(
            record.last_heartbeat, ttl=ttl, now=_utcnow()
        ):
            return None
        return None


def _begin_device_receive_intent_once(
    engine: Any,
    *,
    operator_username: str,
    device_id: str,
    card_identity: str,
    card_label: str | None,
    idempotency_key: str,
    request_hash: str,
    acknowledge_duplicate: bool,
    ttl: dt.timedelta,
) -> DeviceIntentDecision:
    from sutradhara.api.receive_history import latest_card_history

    now = _utcnow()
    factory = make_session_factory(engine)
    with factory.begin() as session:
        record = _idempotency_record(
            session,
            operator_username=operator_username,
            endpoint=DEVICE_RECEIVE_ENDPOINT,
            idempotency_key=idempotency_key,
        )
        if record is None:
            if _source_claim_is_busy(
                session,
                source_id=card_lease_source_id(card_identity),
                operator_username=operator_username,
                idempotency_key=idempotency_key,
                ttl=ttl,
                now=now,
            ):
                return DeviceIntentDecision("busy")
            record = IdempotencyRecord(
                operator_username=operator_username,
                endpoint=DEVICE_RECEIVE_ENDPOINT,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                status="in_progress",
                device_id=device_id,
                card_identity=card_identity,
                card_label=card_label,
                created_at=now,
                updated_at=now,
                last_heartbeat=now,
            )
            session.add(record)
            session.flush()
            history = latest_card_history(
                session,
                card_identity=card_identity,
                requester=operator_username,
                exclude_intent_id=record.id,
            )
            if history is not None:
                warning = {"duplicateWarning": history.warning_payload()}
                record.status = "warned"
                record.duplicate_warning = warning
                record.warned_at = now
                LOG.info(
                    "%s operator=%s device_id=%s card_identity=%s visible=%s",
                    DUPLICATE_WARNED_EVENT,
                    operator_username,
                    device_id,
                    card_identity,
                    history.visible,
                )
                return DeviceIntentDecision("warned", warning)
            if not _authorize_record_lease(session, record, ttl=ttl, now=now):
                session.delete(record)
                return DeviceIntentDecision("busy")
            return DeviceIntentDecision("authorized")

        if record.request_hash != request_hash:
            return DeviceIntentDecision("conflict")
        if record.device_id != device_id or record.card_identity != card_identity:
            return DeviceIntentDecision("conflict")
        if (
            record.status in {"authorized", "started", "committed"}
            and record.response_json is not None
        ):
            return DeviceIntentDecision("completed", record.response_json)
        if record.status in {"authorized", "started"} and _is_stale(
            record.last_heartbeat,
            ttl=ttl,
            now=now,
        ):
            terminalized = session.execute(
                update(IdempotencyRecord)
                .where(
                    IdempotencyRecord.id == record.id,
                    IdempotencyRecord.status.in_(("authorized", "started")),
                    IdempotencyRecord.last_heartbeat < now - ttl,
                )
                .values(
                    status="failed",
                    terminal_at=now,
                    updated_at=now,
                    last_heartbeat=now,
                )
                .execution_options(synchronize_session=False)
            )
            if terminalized.rowcount != 1:
                session.expire(record)
                session.refresh(record)
                if record.response_json is not None:
                    return DeviceIntentDecision("completed", record.response_json)
                if record.status in {"authorized", "started"}:
                    return DeviceIntentDecision("in_progress")
                if record.status in {"committed", "aborted", "quarantined", "failed"}:
                    return DeviceIntentDecision("terminal", terminal_state=record.status)
                return DeviceIntentDecision("conflict")
            _release_record_lease(session, record)
            return DeviceIntentDecision("terminal", terminal_state="failed")
        if record.status == "warned":
            if not acknowledge_duplicate:
                return DeviceIntentDecision("warned", record.duplicate_warning)
            history = latest_card_history(
                session,
                card_identity=card_identity,
                requester=operator_username,
                exclude_intent_id=record.id,
            )
            if history is not None:
                record.duplicate_warning = {"duplicateWarning": history.warning_payload()}
            if not _authorize_record_lease(session, record, ttl=ttl, now=now):
                return DeviceIntentDecision("busy")
            record.duplicate_acknowledged = True
            LOG.info(
                "%s operator=%s device_id=%s card_identity=%s",
                DUPLICATE_ACKNOWLEDGED_EVENT,
                operator_username,
                device_id,
                card_identity,
            )
            return DeviceIntentDecision("authorized")
        if record.status in {"authorized", "started"}:
            return DeviceIntentDecision("in_progress")
        if record.status == "committed":
            return DeviceIntentDecision("terminal", terminal_state=record.status)
        if record.status in {"aborted", "quarantined", "failed"}:
            return DeviceIntentDecision("terminal", terminal_state=record.status)
        return DeviceIntentDecision("conflict")


def _authorize_record_lease(
    session: Session,
    record: IdempotencyRecord,
    *,
    ttl: dt.timedelta,
    now: dt.datetime,
) -> bool:
    assert record.card_identity is not None
    lease_source_id = card_lease_source_id(record.card_identity)
    if not _claim_source_in_session(
        session,
        source_id=lease_source_id,
        operator_username=record.operator_username,
        idempotency_key=record.idempotency_key,
        ttl=ttl,
        now=now,
    ):
        return False
    record.status = "authorized"
    record.lease_source_id = lease_source_id
    record.authorized_at = now
    record.updated_at = now
    record.last_heartbeat = now
    return True


def _claim_source_in_session(
    session: Session,
    *,
    source_id: str,
    operator_username: str,
    idempotency_key: str,
    ttl: dt.timedelta,
    now: dt.datetime,
) -> bool:
    claim = session.get(SourceClaim, source_id)
    if claim is None:
        session.add(
            SourceClaim(
                source_id=source_id,
                operator_username=operator_username,
                idempotency_key=idempotency_key,
                created_at=now,
                updated_at=now,
                last_heartbeat=now,
            )
        )
        return True
    if _claim_owned_by(
        claim,
        operator_username=operator_username,
        idempotency_key=idempotency_key,
    ):
        return True
    if not _is_stale(claim.last_heartbeat, ttl=ttl, now=now):
        return False
    claim.operator_username = operator_username
    claim.idempotency_key = idempotency_key
    claim.intake_id = None
    claim.created_at = now
    claim.updated_at = now
    claim.last_heartbeat = now
    return True


def _source_claim_is_busy(
    session: Session,
    *,
    source_id: str,
    operator_username: str,
    idempotency_key: str,
    ttl: dt.timedelta,
    now: dt.datetime,
) -> bool:
    """Return whether another live owner holds a source before history warning."""

    claim = session.get(SourceClaim, source_id)
    if claim is None:
        return False
    if _claim_owned_by(
        claim,
        operator_username=operator_username,
        idempotency_key=idempotency_key,
    ):
        return False
    return not _is_stale(claim.last_heartbeat, ttl=ttl, now=now)


def _release_record_lease(session: Session, record: IdempotencyRecord) -> None:
    if record.lease_source_id is None:
        return
    claim = session.get(SourceClaim, record.lease_source_id)
    if claim is not None and _claim_owned_by_record(claim, record):
        session.delete(claim)


def _claim_owned_by_record(claim: SourceClaim, record: IdempotencyRecord) -> bool:
    """Return whether a source claim belongs to one durable intent."""

    return _claim_owned_by(
        claim,
        operator_username=record.operator_username,
        idempotency_key=record.idempotency_key,
    )


def _claim_owned_by(
    claim: SourceClaim,
    *,
    operator_username: str,
    idempotency_key: str,
) -> bool:
    """Centralize the operator-and-key source-claim ownership predicate."""

    return (
        claim.operator_username == operator_username
        and claim.idempotency_key == idempotency_key
    )


def _reconcile_orphaned_grpc_intakes(
    session: Session,
    *,
    ttl: dt.timedelta,
    now: dt.datetime,
    batch_size: int,
) -> int:
    """Abort inactive streaming rows whose linked receive intent is terminal or absent."""

    from sutradhara.grpc.store import GrpcIntake

    terminal_states = ("committed", "aborted", "quarantined", "failed")
    cutoff = now - ttl
    rows = list(
        session.scalars(
            select(GrpcIntake)
            .outerjoin(
                IdempotencyRecord,
                and_(
                    IdempotencyRecord.endpoint == DEVICE_RECEIVE_ENDPOINT,
                    IdempotencyRecord.intake_id == GrpcIntake.intake_id,
                ),
            )
            .where(
                GrpcIntake.state.in_(("streaming", "committing")),
                GrpcIntake.updated_at < cutoff,
                or_(
                    IdempotencyRecord.id.is_(None),
                    IdempotencyRecord.status.in_(terminal_states),
                ),
            )
            .order_by(GrpcIntake.updated_at, GrpcIntake.intake_id)
            .limit(batch_size)
        )
        .unique()
    )
    if not rows:
        return 0
    intake_ids = [row.intake_id for row in rows]
    intents = {
        record.intake_id: record
        for record in session.scalars(
            select(IdempotencyRecord).where(
                IdempotencyRecord.endpoint == DEVICE_RECEIVE_ENDPOINT,
                IdempotencyRecord.intake_id.in_(intake_ids),
            )
        )
        if record.intake_id is not None
    }
    orphaned = 0
    for row in rows:
        intent = intents.get(row.intake_id)
        latest_activity = _grpc_landing_last_activity(row)
        if latest_activity + ttl >= now:
            row.updated_at = latest_activity
            continue
        row.state = "aborted"
        row.updated_at = now
        orphaned += 1
        LOG.warning(
            "terminalized inactive orphan grpc intake: intake_id=%s linked_intent=%s",
            row.intake_id,
            "absent" if intent is None else intent.status,
        )
    return orphaned


def _grpc_landing_last_activity(row: Any) -> dt.datetime:
    """Return the latest durable or known landing-path activity for a gRPC intake."""

    latest = _aware(row.updated_at)
    intake_dir = Path(row.landing_root) / row.intake_id
    paths = [
        intake_dir,
        intake_dir / ".receiving.json",
        intake_dir / "receive-receipts.jsonl",
    ]
    incoming = intake_dir / ".incoming"
    with contextlib.suppress(OSError):
        paths.extend(incoming.iterdir())
    for path in paths:
        try:
            modified = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.UTC)
        except OSError:
            continue
        latest = max(latest, modified)
    return latest


def _idempotency_record(
    session: Session,
    *,
    operator_username: str,
    endpoint: str,
    idempotency_key: str,
) -> IdempotencyRecord | None:
    return session.scalars(
        select(IdempotencyRecord).where(
            IdempotencyRecord.operator_username == operator_username,
            IdempotencyRecord.endpoint == endpoint,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
    ).one_or_none()


def _delete_stale_claims(session: Session, *, ttl: dt.timedelta, now: dt.datetime) -> None:
    cutoff = now - ttl
    session.execute(delete(SourceClaim).where(SourceClaim.last_heartbeat < cutoff))


def _is_stale(value: dt.datetime, *, ttl: dt.timedelta, now: dt.datetime) -> bool:
    return _aware(value) + ttl < now


def _aware(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
