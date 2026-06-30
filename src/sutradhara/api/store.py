"""Durable idempotency and source-claim storage for the receive HTTP API."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    UniqueConstraint,
    delete,
    select,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column

from sutradhara.catalog.models import Base
from sutradhara.catalog.session import make_session_factory

DEFAULT_TTL = dt.timedelta(minutes=30)
DEFAULT_HEARTBEAT_INTERVAL = dt.timedelta(seconds=5)
RECEIVE_ENDPOINT = "/api/receive"

IdempotencyState = Literal["claimed", "completed", "conflict", "in_progress"]


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
            "status IN ('in_progress', 'completed')",
            name="ck_idempotency_record_status",
        ),
        Index("ix_idempotency_record_last_heartbeat", "last_heartbeat"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    operator_username: Mapped[str] = mapped_column(String(256), nullable=False)
    endpoint: Mapped[str] = mapped_column(String(128), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    intake_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    response_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
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
    __table_args__ = (
        Index("ix_source_claim_last_heartbeat", "last_heartbeat"),
    )

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


def release_idempotency(
    engine: Any,
    *,
    operator_username: str,
    endpoint: str,
    idempotency_key: str,
) -> None:
    """Delete an idempotency row regardless of completion state.

    gRPC AbortIntake uses this after deleting a not-yet-committed intake so the
    same StartIntake key can mint a fresh id. The HTTP receive endpoint keeps
    using ``abandon_idempotency`` for non-durable in-progress failures.
    """

    factory = make_session_factory(engine)
    with factory.begin() as session:
        record = _idempotency_record(
            session,
            operator_username=operator_username,
            endpoint=endpoint,
            idempotency_key=idempotency_key,
        )
        if record is not None:
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
            if not _is_stale(claim.last_heartbeat, ttl=ttl, now=now):
                return False
            claim.operator_username = operator_username
            claim.idempotency_key = idempotency_key
            claim.intake_id = None
            claim.created_at = now
            claim.updated_at = now
            claim.last_heartbeat = now
            return True
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
