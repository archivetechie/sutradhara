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
    UniqueConstraint,
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
        CheckConstraint(
            "capacity_state IN ('ok', 'over_reserve')",
            name="ck_cache_disk_capacity_state",
        ),
        Index("ix_cache_disk_state", "state"),
        Index("ix_cache_disk_capacity_state", "capacity_state"),
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
    capacity_state: Mapped[str] = mapped_column(String(32), nullable=False, default="ok")
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
        CheckConstraint(
            "delivery_mode IN ('server_local', 'agent')",
            name="ck_restore_request_delivery_mode",
        ),
        CheckConstraint(
            "(delivery_mode = 'server_local' AND receiver_device_id IS NULL) OR "
            "(delivery_mode = 'agent' AND receiver_device_id IS NOT NULL)",
            name="ck_restore_request_receiver_binding",
        ),
        Index("ix_restore_request_created_at", "created_at"),
        Index("ix_restore_request_state", "state"),
        UniqueConstraint("idempotency_key", name="uq_restore_request_idempotency_key"),
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    identity: Mapped[str] = mapped_column(String(256), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    destination_id: Mapped[str] = mapped_column(String(128), nullable=False)
    delivery_mode: Mapped[str] = mapped_column(
        String(32), nullable=False, default="server_local", server_default="server_local"
    )
    receiver_device_id: Mapped[str | None] = mapped_column(
        String(256),
        ForeignKey("grpc_logical_device.device_id", ondelete="RESTRICT"),
        nullable=True,
    )
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    admitted_by: Mapped[str | None] = mapped_column(String(256), nullable=True)
    admitted_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    admitted_capabilities: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    idempotency_body_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)

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
            "'queued', 'waking_disk', 'streaming', 'sent', 'done', "
            "'fell_back_to_tape', 'denied', 'failed'"
            ")",
            name="ck_restore_request_item_state",
        ),
        CheckConstraint(
            "denial_kind IS NULL OR denial_kind IN ("
            "'capability', 'privacy_unmapped', 'suspect', 'rejected'"
            ")",
            name="ck_restore_request_item_denial_kind",
        ),
        CheckConstraint(
            "source IS NULL OR source IN ('cache', 'tape')",
            name="ck_restore_request_item_source",
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
    final_rel_path: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    denial_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    bytes_restored: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    source: Mapped[str | None] = mapped_column(String(16), nullable=True)
    admitted_force_suspect: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    admitted_force_rejected: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    request: Mapped[RestoreRequest] = relationship(back_populates="items")
    checkpoint: Mapped[RestoreItemCheckpoint | None] = relationship(
        back_populates="item",
        cascade="all, delete-orphan",
        lazy="selectin",
        uselist=False,
    )
    open_session: Mapped[RestoreOpenSession | None] = relationship(
        back_populates="item",
        cascade="all, delete-orphan",
        uselist=False,
    )


class RestoreItemCheckpoint(Base):
    """Durable per-item staged/revealed progress for agent delivery."""

    __tablename__ = "restore_item_checkpoint"
    __table_args__ = (
        CheckConstraint(
            "committed_index >= 0 AND committed_index <= 2147483647",
            name="ck_restore_item_checkpoint_index",
        ),
        CheckConstraint(
            "revealed = false OR committed_index >= 1",
            name="ck_restore_item_checkpoint_revealed",
        ),
        CheckConstraint(
            "length(manifest_sha256) = 32",
            name="ck_restore_item_checkpoint_manifest_sha256",
        ),
    )

    restore_request_item_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("restore_request_item.id", ondelete="CASCADE"),
        primary_key=True,
    )
    manifest_sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    committed_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    revealed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    item: Mapped[RestoreRequestItem] = relationship(back_populates="checkpoint")


class RestoreOpenSession(Base):
    """Exclusive, expiring generation lease for opening one agent restore item."""

    __tablename__ = "restore_open_session"
    __table_args__ = (
        CheckConstraint("generation >= 1", name="ck_restore_open_session_generation"),
        CheckConstraint(
            "length(manifest_sha256) = 32",
            name="ck_restore_open_session_manifest_sha256",
        ),
        UniqueConstraint(
            "restore_request_item_id",
            name="uq_restore_open_session_item",
        ),
    )

    restore_request_item_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("restore_request_item.id", ondelete="CASCADE"),
        primary_key=True,
    )
    receiver_device_id: Mapped[str] = mapped_column(
        String(256),
        ForeignKey("grpc_logical_device.device_id", ondelete="CASCADE"),
        nullable=False,
    )
    manifest_sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    expires_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    item: Mapped[RestoreRequestItem] = relationship(back_populates="open_session")
