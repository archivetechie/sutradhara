"""SQLAlchemy 2.0 declarative models for the catalog.

Day-1 vertical slice tables only (docs/spec-v0.1.md §4):
  - logical_asset  (content-addressed; PK is the SHA-256 itself)
  - backend        (registered storage backends)
  - copy           (one row per realization of an asset on a backend)

Pool membership and archive bundle tables are layered on this base as the
archive path moves from scenario-era routing tags to explicit storage pools.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import (
    Boolean,
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
    pools: Mapped[list[Pool]] = relationship(
        back_populates="backend",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return f"<Backend id={self.id} name={self.name!r} kind={self.kind} tier={self.tier}>"


class Pool(Base):
    """A durable storage pool owned by a backend.

    Pool identity is the storage policy surface. A pool owns its byte
    representation; copies point at the pool they were written through.
    """

    __tablename__ = "pool"
    __table_args__ = (
        UniqueConstraint(
            "backend_id",
            "id",
            name="uq_pool_backend_id",
        ),
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    backend_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("backend.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    representation: Mapped[str] = mapped_column(String(64), nullable=False)
    location: Mapped[str] = mapped_column(String(256), nullable=False, default="")
    offsite_gate: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    tier: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    backend: Mapped[Backend] = relationship(back_populates="pools")
    artifactclass_memberships: Mapped[list[ArtifactClassPool]] = relationship(
        back_populates="pool",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    copies: Mapped[list[Copy]] = relationship(
        back_populates="pool",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<Pool id={self.id!r} backend={self.backend_id} "
            f"representation={self.representation!r}>"
        )


class ArtifactClassPool(Base):
    """Active membership from an artifactclass to a storage pool."""

    __tablename__ = "artifactclass_pool"
    __table_args__ = (
        UniqueConstraint(
            "artifactclass",
            "pool_id",
            name="uq_artifactclass_pool_artifactclass_pool",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    artifactclass: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    pool_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("pool.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    role: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    pool: Mapped[Pool] = relationship(back_populates="artifactclass_memberships")

    def __repr__(self) -> str:
        return (
            f"<ArtifactClassPool artifactclass={self.artifactclass!r} "
            f"pool={self.pool_id!r} active={self.active}>"
        )


class Bundle(Base):
    """A synthetic archive object containing one or more logical assets."""

    __tablename__ = "bundle"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    artifactclass: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    representation: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="open")
    total_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    closed_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    members: Mapped[list[BundleMember]] = relationship(
        back_populates="bundle",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    locators: Mapped[list[AssetLocator]] = relationship(
        back_populates="bundle",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<Bundle id={self.id!r} artifactclass={self.artifactclass!r} "
            f"status={self.status!r}>"
        )


class BundleMember(Base):
    """Membership of one logical asset inside a synthetic archive bundle."""

    __tablename__ = "bundle_member"
    __table_args__ = (
        UniqueConstraint(
            "bundle_id",
            "logical_asset_hash",
            name="uq_bundle_member_bundle_asset",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bundle_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("bundle.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    logical_asset_hash: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    member_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    file_sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    added_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    bundle: Mapped[Bundle] = relationship(back_populates="members")
    logical_asset: Mapped[LogicalAsset] = relationship()


class AssetLocator(Base):
    """A per-asset locator, including bundle-derived locations."""

    __tablename__ = "asset_locator"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    logical_asset_hash: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    pool_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("pool.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    copy_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("copy.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    bundle_id: Mapped[str | None] = mapped_column(
        String(128),
        ForeignKey("bundle.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    native_locator: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    representation: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    logical_asset: Mapped[LogicalAsset] = relationship()
    pool: Mapped[Pool] = relationship()
    copy: Mapped[Copy | None] = relationship()
    bundle: Mapped[Bundle | None] = relationship(back_populates="locators")


class BlobRoot(Base):
    """Content root for a generated blob or bundle manifest."""

    __tablename__ = "blob_root"
    __table_args__ = (
        UniqueConstraint(
            "logical_asset_hash",
            "algorithm",
            name="uq_blob_root_asset_algorithm",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    logical_asset_hash: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    algorithm: Mapped[str] = mapped_column(String(64), nullable=False)
    root_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    logical_asset: Mapped[LogicalAsset] = relationship()


class ExclusionRecord(Base):
    """A durable record explaining why material was excluded from bundling."""

    __tablename__ = "exclusion_record"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    artifactclass: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    logical_asset_hash: Mapped[bytes | None] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    reason: Mapped[str] = mapped_column(String(128), nullable=False)
    detail: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    logical_asset: Mapped[LogicalAsset | None] = relationship()


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
    pool_id: Mapped[str | None] = mapped_column(
        String(128),
        ForeignKey("pool.id", ondelete="SET NULL"),
        nullable=True,
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
    pool: Mapped[Pool | None] = relationship(back_populates="copies")

    def __repr__(self) -> str:
        return (
            f"<Copy id={self.id} "
            f"hash={self.logical_asset_hash.hex()[:12]}… "
            f"backend={self.backend_id} health={self.health}>"
        )
