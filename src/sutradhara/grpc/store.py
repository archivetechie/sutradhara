"""Durable state for streaming gRPC intakes and device enrollment.

The gRPC service deliberately keeps owner, StartIntake intent, state, and commit
digest in SQL rather than in ``.receiving.json``. The filesystem marker is only a
watcher/sweep hint and disappears after CommitIntake hands the bag to
``sutra intake watch``.
"""

from __future__ import annotations

import datetime as dt
import secrets
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import Boolean, CheckConstraint, DateTime, Index, String, UniqueConstraint, select
from sqlalchemy.orm import Mapped, Session, mapped_column

from sutradhara.catalog.models import Base

GRPC_START_ENDPOINT = "grpc:StartIntake"
GrpcIntakeState = Literal["streaming", "committing", "committed", "aborted"]


class GrpcIntake(Base):
    """One durable streaming-intake owner/state record."""

    __tablename__ = "grpc_intake"
    __table_args__ = (
        CheckConstraint(
            "state IN ('streaming', 'committing', 'committed', 'aborted')",
            name="ck_grpc_intake_state",
        ),
        Index("ix_grpc_intake_owner", "operator", "device_id"),
    )

    intake_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    operator: Mapped[str] = mapped_column(String(128), nullable=False)
    device_id: Mapped[str] = mapped_column(String(256), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    manifest_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    card_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    source_plan_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    artifactclass: Mapped[str] = mapped_column(String(128), nullable=False)
    source_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    source_ref: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    label: Mapped[str | None] = mapped_column(String(512), nullable=True)
    landing_root: Mapped[str] = mapped_column(String(2048), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow()
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow()
    )


class GrpcDeviceEnrollment(Base):
    """Device-certificate fingerprint mapped to a server-assigned operator."""

    __tablename__ = "grpc_device_enrollment"
    __table_args__ = (
        UniqueConstraint("device_id", "cert_fingerprint", name="uq_grpc_device_fingerprint"),
        Index("ix_grpc_device_fingerprint", "cert_fingerprint"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    device_id: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    cert_fingerprint: Mapped[str] = mapped_column(String(95), nullable=False)
    operator: Mapped[str] = mapped_column(String(128), nullable=False)
    revoked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow()
    )
    revoked_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class GrpcEnrollToken(Base):
    """One-use, short-lived authorization token for CSR signing."""

    __tablename__ = "grpc_enroll_token"

    token: Mapped[str] = mapped_column(String(128), primary_key=True)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow()
    )
    expires_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    operator: Mapped[str] = mapped_column(String(128), nullable=False)
    device_id: Mapped[str] = mapped_column(String(256), nullable=False)
    used_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


@dataclass(frozen=True)
class DeviceIdentity:
    """Resolved device certificate identity."""

    operator: str
    device_id: str
    fingerprint: str


@dataclass(frozen=True)
class EnrollTokenGrant:
    """Validated enrollment-token payload."""

    operator: str
    device_id: str


def insert_intake(
    session: Session,
    *,
    intake_id: str,
    operator: str,
    device_id: str,
    idempotency_key: str,
    source_plan_digest: str,
    artifactclass: str,
    source_kind: str,
    source_ref: str | None,
    label: str | None,
    landing_root: str,
) -> GrpcIntake:
    """Insert a fresh streaming intake in ``streaming`` state."""

    now = _utcnow()
    row = GrpcIntake(
        intake_id=intake_id,
        operator=operator,
        device_id=device_id,
        state="streaming",
        manifest_digest=None,
        idempotency_key=idempotency_key,
        source_plan_digest=source_plan_digest,
        artifactclass=artifactclass,
        source_kind=source_kind,
        source_ref=source_ref,
        label=label,
        landing_root=landing_root,
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    return row


def get_intake(session: Session, intake_id: str) -> GrpcIntake | None:
    """Return one gRPC intake row."""

    return session.get(GrpcIntake, intake_id)


def set_card_id(
    session: Session,
    *,
    intake_id: str,
    operator: str,
    device_id: str,
    card_id: str,
) -> bool:
    """Correlate a relay card id into an owned streaming-intake row."""

    row = session.get(GrpcIntake, intake_id)
    if row is None:
        return False
    if row.operator != operator or row.device_id != device_id:
        return False
    if row.card_id not in {None, card_id}:
        return False
    row.card_id = card_id
    row.updated_at = _utcnow()
    return True


def operator_for_device(session: Session, device_id: str) -> str | None:
    """Return the active operator for a non-revoked enrolled device."""

    operators = {
        row.operator
        for row in session.scalars(
            select(GrpcDeviceEnrollment).where(
                GrpcDeviceEnrollment.device_id == device_id,
                GrpcDeviceEnrollment.revoked.is_(False),
            )
        )
    }
    if not operators:
        return None
    if len(operators) > 1:
        raise PermissionError("device has conflicting active operator enrollments")
    return next(iter(operators))


def compare_and_set_state(
    session: Session,
    intake_id: str,
    *,
    expect: str,
    update: GrpcIntakeState,
) -> bool:
    """Transition state only if the current durable value matches ``expect``."""

    row = session.get(GrpcIntake, intake_id)
    if row is None or row.state != expect:
        return False
    row.state = update
    row.updated_at = _utcnow()
    return True


def set_state(session: Session, intake_id: str, state: GrpcIntakeState) -> None:
    """Set the durable state for an existing gRPC intake."""

    row = session.get(GrpcIntake, intake_id)
    if row is None:
        raise KeyError(intake_id)
    row.state = state
    row.updated_at = _utcnow()


def set_committed_digest(session: Session, intake_id: str, manifest_digest: str) -> None:
    """Mark an intake committed and persist the commit digest."""

    row = session.get(GrpcIntake, intake_id)
    if row is None:
        raise KeyError(intake_id)
    row.state = "committed"
    row.manifest_digest = manifest_digest
    row.updated_at = _utcnow()


def issue_enroll_token(
    session: Session,
    *,
    operator: str,
    device_id: str,
    ttl: dt.timedelta = dt.timedelta(hours=24),
) -> str:
    """Create and persist a one-time operator/device-bound enrollment token."""

    token = secrets.token_urlsafe(32)
    now = _utcnow()
    session.add(
        GrpcEnrollToken(
            token=token,
            created_at=now,
            expires_at=now + ttl,
            operator=operator,
            device_id=device_id,
        )
    )
    return token


def consume_enroll_token(
    session: Session,
    token: str,
    *,
    device_id: str | None = None,
    now: dt.datetime | None = None,
) -> EnrollTokenGrant:
    """Mark an enrollment token used, rejecting missing, expired, reused, or wrong-device tokens."""

    timestamp = now or _utcnow()
    row = session.get(GrpcEnrollToken, token)
    if row is None:
        raise ValueError("enrollment token is unknown")
    if row.used_at is not None:
        raise ValueError("enrollment token was already used")
    if _aware(row.expires_at) < timestamp:
        raise ValueError("enrollment token has expired")
    if device_id is not None and row.device_id != device_id:
        raise ValueError("CSR common name does not match enrollment token device_id")
    row.used_at = timestamp
    return EnrollTokenGrant(operator=row.operator, device_id=row.device_id)


def release_enroll_token(session: Session, token: str) -> bool:
    """Clear a consumed enrollment token after certificate signing fails."""

    row = session.get(GrpcEnrollToken, token)
    if row is None or row.used_at is None:
        return False
    row.used_at = None
    return True


def record_device_enrollment(
    session: Session,
    *,
    device_id: str,
    cert_fingerprint: str,
    operator: str,
) -> GrpcDeviceEnrollment:
    """Record one active certificate fingerprint for a device/operator.

    Re-enrollment by the same operator supersedes any prior active fingerprint
    for the device. Active ownership by a different operator is refused without
    mutating the existing rows.
    """

    normalized = normalize_fingerprint(cert_fingerprint)
    active_rows = list(
        session.scalars(
            select(GrpcDeviceEnrollment).where(
                GrpcDeviceEnrollment.device_id == device_id,
                GrpcDeviceEnrollment.revoked.is_(False),
            )
        )
    )
    if any(row.operator != operator for row in active_rows):
        raise PermissionError("device belongs to a different operator")

    now = _utcnow()
    for row in active_rows:
        if row.cert_fingerprint != normalized:
            row.revoked = True
            row.revoked_at = now

    existing = session.scalars(
        select(GrpcDeviceEnrollment).where(
            GrpcDeviceEnrollment.device_id == device_id,
            GrpcDeviceEnrollment.cert_fingerprint == normalized,
        )
    ).one_or_none()
    if existing is not None:
        existing.operator = operator
        existing.revoked = False
        existing.revoked_at = None
        return existing
    row = GrpcDeviceEnrollment(
        device_id=device_id,
        cert_fingerprint=normalized,
        operator=operator,
        revoked=False,
    )
    session.add(row)
    return row


def revoke_device(session: Session, device_id: str) -> int:
    """Block every known certificate fingerprint for a device."""

    now = _utcnow()
    rows = list(
        session.scalars(
            select(GrpcDeviceEnrollment).where(GrpcDeviceEnrollment.device_id == device_id)
        )
    )
    for row in rows:
        row.revoked = True
        row.revoked_at = now
    return len(rows)


def resolve_device(session: Session, *, device_id: str, cert_fingerprint: str) -> DeviceIdentity:
    """Resolve a peer certificate to the server-assigned operator."""

    normalized = normalize_fingerprint(cert_fingerprint)
    row = session.scalars(
        select(GrpcDeviceEnrollment).where(
            GrpcDeviceEnrollment.device_id == device_id,
            GrpcDeviceEnrollment.cert_fingerprint == normalized,
        )
    ).one_or_none()
    if row is None or row.revoked:
        raise PermissionError("device certificate is not enrolled")
    return DeviceIdentity(operator=row.operator, device_id=row.device_id, fingerprint=normalized)


def normalize_fingerprint(value: str) -> str:
    """Return colon-separated uppercase SHA-256 fingerprint text."""

    compact = value.replace(":", "").replace(" ", "").upper()
    if len(compact) != 64:
        raise ValueError("certificate fingerprint must be a SHA-256 hex digest")
    bytes.fromhex(compact)
    return ":".join(compact[index : index + 2] for index in range(0, len(compact), 2))


def _aware(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
