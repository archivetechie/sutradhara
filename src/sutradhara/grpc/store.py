"""Durable state for streaming gRPC intakes and device enrollment.

The gRPC service deliberately keeps owner, StartIntake intent, state, and commit
digest in SQL rather than in ``.receiving.json``. The filesystem marker is only a
watcher/sweep hint and disappears after CommitIntake hands the bag to
``sutra intake watch``.
"""

from __future__ import annotations

import datetime as dt
import logging
import secrets
from dataclasses import dataclass
from typing import Literal, cast

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    select,
)
from sqlalchemy.orm import Mapped, Session, mapped_column

from sutradhara.catalog.models import Base
from sutradhara.schema_conventions import vocabulary_check_sql

GrpcIntakeState = Literal["streaming", "committing", "committed", "aborted"]
RotationAuthority = Literal["self", "admin"]
DeviceScope = Literal["ingest", "restore"]
VALID_DEVICE_SCOPES = frozenset({"ingest", "restore"})
LOG = logging.getLogger(__name__)


class GrpcIntake(Base):
    """One durable streaming-intake owner/state record."""

    __tablename__ = "grpc_intake"
    __table_args__ = (
        CheckConstraint(
            vocabulary_check_sql("state", "grpc_intake_state"),
            name="ck_grpc_intake_state",
        ),
        Index("ix_grpc_intake_owner", "operator", "device_id"),
        Index("ix_grpc_intake_card_id", "card_id"),
    )

    intake_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    operator: Mapped[str] = mapped_column(String(128), nullable=False)
    device_id: Mapped[str] = mapped_column(String(256), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    manifest_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    card_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    source_plan_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    artifactclass: Mapped[str] = mapped_column(
        String(128), ForeignKey("artifactclass.name", ondelete="RESTRICT"), nullable=False
    )
    source_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    source_ref: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    label: Mapped[str | None] = mapped_column(String(512), nullable=True)
    landing_root: Mapped[str] = mapped_column(String(2048), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow()
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow(),
        onupdate=lambda: _utcnow()
    )


class GrpcLogicalDevice(Base):
    """Stable device identity carrying enrollment scopes across certificate rotations."""

    __tablename__ = "grpc_logical_device"
    __table_args__ = (
        CheckConstraint(
            'CAST(scopes AS TEXT) IN (\'["ingest"]\', \'["restore"]\', \'["ingest", "restore"]\')',
            name="ck_grpc_logical_device_scopes",
        ),
    )

    device_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    scopes: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=lambda: ["ingest"])
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow()
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow(),
        onupdate=lambda: _utcnow()
    )


class GrpcDeviceDestinationGrant(Base):
    """One opaque restore destination binding authorized for a logical device."""

    __tablename__ = "grpc_device_destination_grant"
    __table_args__ = (
        UniqueConstraint(
            "device_id",
            "destination_id",
            name="uq_grpc_device_destination_grant",
        ),
        Index("ix_grpc_device_destination_grant_destination", "destination_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    device_id: Mapped[str] = mapped_column(
        String(256),
        ForeignKey("grpc_logical_device.device_id", ondelete="CASCADE"),
        nullable=False,
    )
    destination_id: Mapped[str] = mapped_column(String(128), nullable=False)
    dest_root: Mapped[str] = mapped_column(String(2048), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
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
    device_id: Mapped[str] = mapped_column(
        String(256),
        ForeignKey("grpc_logical_device.device_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
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
    __table_args__ = (
        CheckConstraint(
            'CAST(scopes AS TEXT) IN (\'["ingest"]\', \'["restore"]\', \'["ingest", "restore"]\')',
            name="ck_grpc_enroll_token_scopes",
        ),
    )

    token: Mapped[str] = mapped_column(String(128), primary_key=True)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: _utcnow()
    )
    expires_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    operator: Mapped[str] = mapped_column(String(128), nullable=False)
    device_id: Mapped[str] = mapped_column(String(256), nullable=False)
    scopes: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=lambda: ["ingest"])
    rotation_authority: Mapped[str | None] = mapped_column(String(32), nullable=True)
    rotation_fingerprint: Mapped[str | None] = mapped_column(String(95), nullable=True)
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
    scopes: tuple[DeviceScope, ...]
    rotation_authority: RotationAuthority | None = None
    rotation_fingerprint: str | None = None


@dataclass(frozen=True)
class RegisteredDevice:
    """Durable active enrollment summary for one operator-owned device."""

    device_id: str
    operator: str
    created_at: dt.datetime


class DeviceOwnershipError(PermissionError):
    """Raised when an enrollment would cross an active device owner boundary."""


class DeviceRotationProofError(PermissionError):
    """Raised when a re-enrollment would replace an active cert without proof."""


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


def registered_devices_for_operator(session: Session, operator: str) -> list[RegisteredDevice]:
    """Return active, durable device registrations for one operator.

    A device may have historical or rotated fingerprints; the API projection is
    one row per active device id, using the newest active enrollment timestamp.
    """

    rows = list(
        session.scalars(
            select(GrpcDeviceEnrollment)
            .where(
                GrpcDeviceEnrollment.operator == operator,
                GrpcDeviceEnrollment.revoked.is_(False),
            )
            .order_by(GrpcDeviceEnrollment.device_id, GrpcDeviceEnrollment.created_at.desc())
        )
    )
    seen: set[str] = set()
    registered: list[RegisteredDevice] = []
    for row in rows:
        if row.device_id in seen:
            continue
        seen.add(row.device_id)
        registered.append(
            RegisteredDevice(
                device_id=row.device_id,
                operator=row.operator,
                created_at=_aware(row.created_at),
            )
        )
    return registered


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
    scopes: tuple[str, ...] = ("ingest",),
    ttl: dt.timedelta = dt.timedelta(hours=24),
    rotation_authority: RotationAuthority | None = None,
    rotation_fingerprint: str | None = None,
) -> str:
    """Create and persist a one-time operator/device-bound enrollment token."""

    normalized_scopes = validate_device_scopes(scopes)
    token = secrets.token_urlsafe(32)
    now = _utcnow()
    if rotation_authority is None and rotation_fingerprint is not None:
        raise ValueError("rotation_fingerprint requires rotation_authority")
    if rotation_authority == "self" and not rotation_fingerprint:
        raise ValueError("self rotation requires old certificate fingerprint proof")
    normalized_rotation_fingerprint = (
        normalize_fingerprint(rotation_fingerprint) if rotation_fingerprint is not None else None
    )
    prior_tokens = session.scalars(
        select(GrpcEnrollToken).where(
            GrpcEnrollToken.operator == operator,
            GrpcEnrollToken.device_id == device_id,
            GrpcEnrollToken.used_at.is_(None),
        )
    )
    for prior_token in prior_tokens:
        prior_token.used_at = now
    session.add(
        GrpcEnrollToken(
            token=token,
            created_at=now,
            expires_at=now + ttl,
            operator=operator,
            device_id=device_id,
            scopes=list(normalized_scopes),
            rotation_authority=rotation_authority,
            rotation_fingerprint=normalized_rotation_fingerprint,
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
    if row.rotation_authority not in {None, "self", "admin"}:
        raise ValueError("enrollment token has invalid rotation authority")
    row.used_at = timestamp
    return EnrollTokenGrant(
        operator=row.operator,
        device_id=row.device_id,
        scopes=validate_device_scopes(row.scopes),
        rotation_authority=cast(RotationAuthority | None, row.rotation_authority),
        rotation_fingerprint=row.rotation_fingerprint,
    )


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
    scopes: tuple[str, ...] = ("ingest",),
    rotation_authority: RotationAuthority | None = None,
    rotation_fingerprint: str | None = None,
) -> GrpcDeviceEnrollment:
    """Record one active certificate fingerprint for a device/operator.

    Re-enrollment by the same operator supersedes a prior active fingerprint only
    when the caller presents old-key proof or an admin-confirmed rotation token.
    Active ownership by a different operator is refused without mutating the
    existing rows.
    """

    normalized_scopes = validate_device_scopes(scopes)
    normalized = normalize_fingerprint(cert_fingerprint)
    normalized_rotation_fingerprint = (
        normalize_fingerprint(rotation_fingerprint) if rotation_fingerprint is not None else None
    )
    active_rows = list(
        session.scalars(
            select(GrpcDeviceEnrollment).where(
                GrpcDeviceEnrollment.device_id == device_id,
                GrpcDeviceEnrollment.revoked.is_(False),
            )
        )
    )
    if any(row.operator != operator for row in active_rows):
        raise DeviceOwnershipError("device belongs to a different operator")
    superseded_rows = [row for row in active_rows if row.cert_fingerprint != normalized]
    if superseded_rows:
        _require_rotation_authority(
            active_rows=superseded_rows,
            rotation_authority=rotation_authority,
            rotation_fingerprint=normalized_rotation_fingerprint,
        )

    now = _utcnow()
    logical_device = _get_or_create_logical_device(
        session,
        device_id=device_id,
        scopes=normalized_scopes,
        now=now,
    )
    # A redeemed rotation token is authoritative: scopes replace, never union.
    logical_device.scopes = list(normalized_scopes)
    logical_device.updated_at = now
    session.flush([logical_device])
    for row in superseded_rows:
        row.revoked = True
        row.revoked_at = now
    if superseded_rows:
        LOG.info(
            "device certificate rotated: device_id=%s operator=%s authority=%s superseded=%d",
            device_id,
            operator,
            rotation_authority,
            len(superseded_rows),
        )

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


def validate_device_scopes(scopes: object) -> tuple[DeviceScope, ...]:
    """Return a canonical non-empty scope tuple, rejecting unknown or duplicate grants."""

    if not isinstance(scopes, (list, tuple)) or not scopes:
        raise ValueError("enrollment scopes must be a non-empty list")
    if any(not isinstance(scope, str) or scope not in VALID_DEVICE_SCOPES for scope in scopes):
        raise ValueError("enrollment scopes must contain only ingest and restore")
    if len(set(scopes)) != len(scopes):
        raise ValueError("enrollment scopes must not contain duplicates")
    return cast(
        tuple[DeviceScope, ...],
        tuple(scope for scope in ("ingest", "restore") if scope in scopes),
    )


def _get_or_create_logical_device(
    session: Session,
    *,
    device_id: str,
    scopes: tuple[DeviceScope, ...],
    now: dt.datetime,
) -> GrpcLogicalDevice:
    """Atomically ensure the certificate's stable parent exists on supported databases."""

    existing = session.get(GrpcLogicalDevice, device_id)
    if existing is not None:
        return existing
    values = {
        "device_id": device_id,
        "scopes": list(scopes),
        "created_at": now,
        "updated_at": now,
    }
    dialect = session.get_bind().dialect.name
    if dialect == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        session.execute(
            sqlite_insert(GrpcLogicalDevice)
            .values(**values)
            .on_conflict_do_nothing(index_elements=["device_id"])
        )
    elif dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as postgresql_insert

        session.execute(
            postgresql_insert(GrpcLogicalDevice)
            .values(**values)
            .on_conflict_do_nothing(index_elements=["device_id"])
        )
    else:
        row = GrpcLogicalDevice(**values)
        session.add(row)
        session.flush([row])
        return row
    inserted = session.get(GrpcLogicalDevice, device_id)
    if inserted is None:
        raise RuntimeError("logical device parent upsert did not produce a row")
    return inserted


def _require_rotation_authority(
    *,
    active_rows: list[GrpcDeviceEnrollment],
    rotation_authority: RotationAuthority | None,
    rotation_fingerprint: str | None,
) -> None:
    if rotation_authority == "admin":
        return
    if (
        rotation_authority == "self"
        and rotation_fingerprint is not None
        and any(row.cert_fingerprint == rotation_fingerprint for row in active_rows)
    ):
        return
    raise DeviceRotationProofError("active device certificate rotation requires old-key proof")


def _aware(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
