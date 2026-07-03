"""ORM models for the hdcache disk-tier inventory.

These tables intentionally model cache state beside the archival catalog. They
do not register storage backends, pools, copies, or artifactclass memberships;
the HD cache is expendable operational state derived from durable archive truth.
"""

from __future__ import annotations

import datetime as dt

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from sutradhara.catalog.models import Base


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


class CacheDisk(Base):
    """One enrolled physical disk in the expendable HD cache tier."""

    __tablename__ = "cache_disk"
    __table_args__ = (
        CheckConstraint(
            "state IN ('active', 'absent', 'retiring', 'dead')",
            name="ck_cache_disk_state",
        ),
        Index("ix_cache_disk_state", "state"),
    )

    disk_id: Mapped[str] = mapped_column(String(16), primary_key=True)
    serial: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)
    wwn: Mapped[str | None] = mapped_column(String(256), nullable=True)
    fs_uuid: Mapped[str] = mapped_column(String(128), nullable=False)
    enclosure: Mapped[str | None] = mapped_column(String(256), nullable=True)
    slot: Mapped[str | None] = mapped_column(String(128), nullable=True)
    mount: Mapped[str] = mapped_column(String(2048), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    capacity_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    filled_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    smart_status: Mapped[str | None] = mapped_column(String(256), nullable=True)
    enrolled_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    last_walk_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    entries: Mapped[list[CacheEntry]] = relationship(
        back_populates="disk",
        cascade="all, delete-orphan",
        lazy="raise",
    )


class CacheEntry(Base):
    """One cache realization of a logical asset on an enrolled disk."""

    __tablename__ = "cache_entry"
    __table_args__ = (
        CheckConstraint(
            "state IN ('filling', 'present', 'lost')",
            name="ck_cache_entry_state",
        ),
        CheckConstraint(
            "representation IN ('raw-bytes', 'rao-aead-v1')",
            name="ck_cache_entry_representation",
        ),
        Index("ix_cache_entry_bundle_key", "bundle_key"),
        Index("ix_cache_entry_group_key", "group_key"),
        Index("ix_cache_entry_disk_id", "disk_id"),
        Index("ix_cache_entry_state", "state"),
    )

    content_sha256: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="CASCADE"),
        primary_key=True,
    )
    artifactclass: Mapped[str] = mapped_column(String(128), nullable=False)
    bundle_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    group_key: Mapped[str | None] = mapped_column(String(256), nullable=True)
    disk_id: Mapped[str] = mapped_column(
        String(16),
        ForeignKey("cache_disk.disk_id", ondelete="CASCADE"),
        nullable=False,
    )
    relpath: Mapped[str] = mapped_column(String(2048), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="filling")
    representation: Mapped[str] = mapped_column(String(64), nullable=False, default="raw-bytes")
    key_epoch: Mapped[str | None] = mapped_column(String(128), nullable=True)
    stored_digest: Mapped[bytes | None] = mapped_column(LargeBinary(32), nullable=True)
    trusted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    placed_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    last_read_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lost_origin_disk_id: Mapped[str | None] = mapped_column(String(16), nullable=True)
    lost_drill_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lost_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refilled_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    disk: Mapped[CacheDisk] = relationship(back_populates="entries")


class RestoreRequest(Base):
    """Operator restore request tracked independently of the cache/tape branch."""

    __tablename__ = "restore_request"
    __table_args__ = (
        CheckConstraint(
            "state IN ('pending', 'active', 'completed', 'completed_with_errors')",
            name="ck_restore_request_state",
        ),
        Index("ix_restore_request_created_at", "created_at"),
        Index("ix_restore_request_state", "state"),
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    identity: Mapped[str] = mapped_column(String(256), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    destination_id: Mapped[str] = mapped_column(String(128), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    admitted_by: Mapped[str | None] = mapped_column(String(256), nullable=True)
    admitted_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    admitted_capabilities: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)

    items: Mapped[list[RestoreRequestItem]] = relationship(
        back_populates="request",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class RestoreRequestItem(Base):
    """One asset-level item inside a persisted restore request."""

    __tablename__ = "restore_request_item"
    __table_args__ = (
        CheckConstraint(
            "state IN ("
            "'queued', 'waking_disk', 'streaming', 'done', "
            "'fell_back_to_tape', 'denied', 'failed'"
            ")",
            name="ck_restore_request_item_state",
        ),
        Index("ix_restore_request_item_request_id", "request_id"),
        Index("ix_restore_request_item_content_sha256", "content_sha256"),
        Index("ix_restore_request_item_state", "state"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    request_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("restore_request.id", ondelete="CASCADE"),
        nullable=False,
    )
    content_sha256: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="CASCADE"),
        nullable=False,
    )
    artifactclass: Mapped[str] = mapped_column(String(128), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    admitted_force_suspect: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    admitted_force_rejected: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    request: Mapped[RestoreRequest] = relationship(back_populates="items")
