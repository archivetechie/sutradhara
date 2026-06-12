"""SQLAlchemy 2.0 declarative models for the catalog.

Day-1 vertical slice tables only (docs/spec-v0.1.md §4):
  - logical_asset  (content-addressed; PK is the SHA-256 itself)
  - backend        (registered storage backends)
  - copy           (one row per realization of an asset on a backend)

Derivations, recipes, and jobs land in later slices.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)

from sutradhara.catalog.types import (
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
    MediaKind,
)


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


class Base(DeclarativeBase):
    """Declarative base for all catalog ORM models."""


class LogicalAsset(Base):
    """A content-addressed logical asset.

    The primary key is the SHA-256 of the asset's bytes — there is no
    surrogate ID. Same hash means same row (full deduplication, per
    docs/spec-v0.1.md §2 principle 3).
    """

    __tablename__ = "logical_asset"

    content_sha256: Mapped[bytes] = mapped_column(LargeBinary(32), primary_key=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    first_seen_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    # Non-authoritative ergonomic metadata.
    human_label: Mapped[str | None] = mapped_column(String(512), nullable=True)
    media_kind: Mapped[MediaKind | None] = mapped_column(
        String(32), nullable=True
    )
    media_info: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    copies: Mapped[list[Copy]] = relationship(
        back_populates="logical_asset",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<LogicalAsset hash={self.content_sha256.hex()[:12]}… "
            f"size={self.size_bytes} copies={len(self.copies)}>"
        )


class Backend(Base):
    """A registered storage backend (one row per backend instance)."""

    __tablename__ = "backend"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    kind: Mapped[BackendKind] = mapped_column(String(32), nullable=False)
    config: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    tier: Mapped[BackendTier] = mapped_column(String(32), nullable=False)
    added_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    copies: Mapped[list[Copy]] = relationship(
        back_populates="backend",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    placement_tag_pins: Mapped[list[PlacementTagPin]] = relationship(
        back_populates="backend",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return f"<Backend id={self.id} name={self.name!r} kind={self.kind} tier={self.tier}>"


class PlacementTagPin(Base):
    """Pinned placement routing tags for drift detection.

    Placement identity is backend-specific, so the durable key is
    `(backend_id, placement_id)`. The stored tags are sutradhara's routing
    vocabulary and are checked against future backend discovery before acting.
    """

    __tablename__ = "placement_tag_pin"
    __table_args__ = (
        UniqueConstraint(
            "backend_id",
            "placement_id",
            name="uq_placement_tag_pin_backend_placement",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    backend_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("backend.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    placement_id: Mapped[str] = mapped_column(String(128), nullable=False)
    content_class: Mapped[str] = mapped_column(String(128), nullable=False)
    copy_class: Mapped[str] = mapped_column(String(128), nullable=False)
    pinned_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    backend: Mapped[Backend] = relationship(back_populates="placement_tag_pins")

    def __repr__(self) -> str:
        return (
            f"<PlacementTagPin backend={self.backend_id} "
            f"placement={self.placement_id!r} "
            f"content_class={self.content_class!r} copy_class={self.copy_class!r}>"
        )


class Copy(Base):
    """One realization of a logical asset on one backend.

    Many copies per asset (per docs/spec-v0.1.md §4.2). UNIQUE on
    (backend_id, native_locator_key) so a backend cannot register the
    same locator twice; multiplicity is on (asset, backend) not on the
    locator.
    """

    __tablename__ = "copy"
    __table_args__ = (
        UniqueConstraint(
            "backend_id",
            "native_locator_key",
            name="uq_copy_backend_locator",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    logical_asset_hash: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    backend_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("backend.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # The locator itself is structured per-backend; we store the JSON for
    # querying/display AND a deterministic string key for the UNIQUE
    # constraint (SQLite cannot UNIQUE-index on a JSON column directly,
    # and ordering of dict keys in JSON storage is implementation-defined).
    native_locator: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    native_locator_key: Mapped[str] = mapped_column(String(512), nullable=False)
    storage_metadata: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict
    )

    integrity_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    health: Mapped[CopyHealth] = mapped_column(
        String(16), nullable=False, default=CopyHealth.OK
    )
    last_verified_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    first_observed_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    source: Mapped[CopySource] = mapped_column(String(32), nullable=False)

    logical_asset: Mapped[LogicalAsset] = relationship(back_populates="copies")
    backend: Mapped[Backend] = relationship(back_populates="copies")

    def __repr__(self) -> str:
        return (
            f"<Copy id={self.id} "
            f"hash={self.logical_asset_hash.hex()[:12]}… "
            f"backend={self.backend_id} health={self.health}>"
        )
