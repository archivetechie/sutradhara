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
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)

from sutradhara.catalog.types import (
    AssetValidity,
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
    IntakeSourceKind,
    IntakeStatus,
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
    __table_args__ = (
        CheckConstraint(
            "validity IN ('ok', 'suspect', 'unvalidated')",
            name="ck_logical_asset_validity",
        ),
    )

    content_sha256: Mapped[bytes] = mapped_column(LargeBinary(32), primary_key=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    first_seen_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    # Non-authoritative ergonomic metadata.
    human_label: Mapped[str | None] = mapped_column(String(512), nullable=True)
    media_kind: Mapped[MediaKind | None] = mapped_column(String(32), nullable=True)
    media_info: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    validity: Mapped[AssetValidity] = mapped_column(
        String(32), nullable=False, default=AssetValidity.UNVALIDATED
    )
    validity_note: Mapped[str | None] = mapped_column(Text, nullable=True)

    copies: Mapped[list[Copy]] = relationship(
        back_populates="logical_asset",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    ingest_items: Mapped[list[IngestItem]] = relationship(
        back_populates="logical_asset",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<LogicalAsset hash={self.content_sha256.hex()[:12]}… "
            f"size={self.size_bytes} copies={len(self.copies)}>"
        )


class Intake(Base):
    """A completed landing batch admitted by the intake scanner.

    The scanner treats an `intake.json` sentinel as the boundary between
    receiving and verifying. A quarantined intake records the failed batch but
    does not register any `ingest_item` rows.
    """

    __tablename__ = "intake"
    __table_args__ = (
        CheckConstraint(
            "source_kind IN ('card', 'drive', 'upload', 'handoff', 'download', 'other')",
            name="ck_intake_source_kind",
        ),
        CheckConstraint(
            "status IN ('receiving', 'verifying', 'quarantined', 'registered')",
            name="ck_intake_status",
        ),
    )

    intake_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    operator: Mapped[str] = mapped_column(String(128), nullable=False)
    source_kind: Mapped[IntakeSourceKind] = mapped_column(String(32), nullable=False)
    source_ref: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    artifactclass: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    label: Mapped[str | None] = mapped_column(String(512), nullable=True)
    manifest_path: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    status: Mapped[IntakeStatus] = mapped_column(
        String(32), nullable=False, default=IntakeStatus.RECEIVING
    )
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    registered_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    quarantined_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    items: Mapped[list[IngestItem]] = relationship(
        back_populates="intake",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<Intake id={self.intake_id!r} source_kind={self.source_kind!r} "
            f"status={self.status!r}>"
        )


class IngestItem(Base):
    """One occurrence of a logical asset within an intake.

    Occurrence identity stays separate from content identity: two card intakes
    may carry the same bytes and still deserve separate provenance rows.
    """

    __tablename__ = "ingest_item"
    __table_args__ = (
        UniqueConstraint(
            "intake_id",
            "as_received_path",
            name="uq_ingest_item_intake_as_received_path",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    intake_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("intake.intake_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    logical_asset_hash: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    as_received_path: Mapped[str] = mapped_column(String(2048), nullable=False)
    virtual_path: Mapped[str] = mapped_column(String(2048), nullable=False)
    st_dev: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    st_ino: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    artifactclass: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    item_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSON,
        nullable=False,
        default=dict,
    )
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    intake: Mapped[Intake] = relationship(back_populates="items")
    logical_asset: Mapped[LogicalAsset] = relationship(back_populates="ingest_items")
    derived_from_edges: Mapped[list[AssetDerivation]] = relationship(
        back_populates="derived_item",
        cascade="all, delete-orphan",
        foreign_keys="AssetDerivation.derived_item_id",
        lazy="selectin",
    )
    source_for_edges: Mapped[list[AssetDerivation]] = relationship(
        back_populates="source_item",
        cascade="all, delete-orphan",
        foreign_keys="AssetDerivation.source_item_id",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return f"<IngestItem id={self.id} intake={self.intake_id!r} path={self.as_received_path!r}>"


class AssetDerivation(Base):
    """A provenance edge from one ingested occurrence to a derived occurrence."""

    __tablename__ = "asset_derivation"
    __table_args__ = (
        UniqueConstraint(
            "derived_item_id",
            "source_item_id",
            "kind",
            name="uq_asset_derivation_derived_source_kind",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    derived_item_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("ingest_item.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source_item_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("ingest_item.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    derived_item: Mapped[IngestItem] = relationship(
        back_populates="derived_from_edges",
        foreign_keys=[derived_item_id],
    )
    source_item: Mapped[IngestItem] = relationship(
        back_populates="source_for_edges",
        foreign_keys=[source_item_id],
    )

    def __repr__(self) -> str:
        return (
            f"<AssetDerivation source_item={self.source_item_id} "
            f"derived_item={self.derived_item_id} kind={self.kind!r}>"
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


class ArtifactClassPolicyRecord(Base):
    """The active strict policy document bound to an artifactclass."""

    __tablename__ = "artifactclass_policy"

    artifactclass: Mapped[str] = mapped_column(String(128), primary_key=True)
    ruleset: Mapped[str] = mapped_column(String(256), nullable=False)
    expect: Mapped[str] = mapped_column(String(32), nullable=False)
    target_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_age_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    restore_preference: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    staging_config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    policy_source: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    policy_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    def __repr__(self) -> str:
        return (
            f"<ArtifactClassPolicy artifactclass={self.artifactclass!r} "
            f"ruleset={self.ruleset!r} expect={self.expect!r}>"
        )


class Bundle(Base):
    """A synthetic archive object containing one or more logical assets."""

    __tablename__ = "bundle"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    artifactclass: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="open")
    total_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    member_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    target_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    max_age_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ruleset: Mapped[str | None] = mapped_column(String(256), nullable=True)
    expect: Mapped[str | None] = mapped_column(String(32), nullable=True)
    archive_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    scan_summary: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    review_summary: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    customer_manifest_path: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    opened_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    flushed_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    sealed_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    held_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    members: Mapped[list[BundleMember]] = relationship(
        back_populates="bundle",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    locators: Mapped[list[AssetLocator]] = relationship(
        back_populates="bundle",
        lazy="selectin",
    )
    copies: Mapped[list[Copy]] = relationship(
        back_populates="bundle",
        lazy="selectin",
    )
    review_decisions: Mapped[list[ReviewDecision]] = relationship(
        back_populates="bundle",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<Bundle id={self.id!r} artifactclass={self.artifactclass!r} status={self.status!r}>"
        )


class BundleMember(Base):
    """Membership of one logical asset inside a synthetic archive bundle."""

    __tablename__ = "bundle_member"
    __table_args__ = (
        UniqueConstraint(
            "bundle_id",
            "member_path",
            name="uq_bundle_member_bundle_path",
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
    source_path: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    file_sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    source_metadata: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    added_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    bundle: Mapped[Bundle] = relationship(back_populates="members")
    logical_asset: Mapped[LogicalAsset] = relationship()
    transforms: Mapped[list[StagingTransform]] = relationship(
        back_populates="bundle_member",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="StagingTransform.step_order",
    )


class StagingTransform(Base):
    """One recorded staging transform applied before bundle fan-out.

    Transform rows are copy-independent. They describe how one bundle member's
    archived bytes differ from the original logical asset and, for reversible
    transforms such as zstd compression, how restore must recover the original
    bytes before verifying the logical asset hash.
    """

    __tablename__ = "staging_transform"
    __table_args__ = (
        UniqueConstraint(
            "bundle_member_id",
            "step_order",
            name="uq_staging_transform_member_step",
        ),
        UniqueConstraint(
            "bundle_id",
            "stored_member_path",
            "step_order",
            name="uq_staging_transform_bundle_stored_step",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bundle_member_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("bundle_member.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
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
    artifactclass: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    step_order: Mapped[int] = mapped_column(Integer, nullable=False)
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    reversible: Mapped[bool] = mapped_column(Boolean, nullable=False)
    original_member_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    stored_member_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    original_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    stored_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    original_sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    stored_sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    parameters: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    result: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    bundle_member: Mapped[BundleMember] = relationship(back_populates="transforms")
    logical_asset: Mapped[LogicalAsset] = relationship()
    bundle: Mapped[Bundle] = relationship()


class AssetLocator(Base):
    """A per-asset locator, including bundle-derived locations."""

    __tablename__ = "asset_locator"
    __table_args__ = (
        UniqueConstraint(
            "copy_id",
            "logical_asset_hash",
            "member_path",
            name="uq_asset_locator_copy_asset_member",
        ),
    )

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
    member_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    representation: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    logical_asset: Mapped[LogicalAsset] = relationship()
    pool: Mapped[Pool] = relationship()
    copy: Mapped[Copy | None] = relationship()
    bundle: Mapped[Bundle | None] = relationship(back_populates="locators")


class BlobRoot(Base):
    """A coarse pointer to a blob entry inside one archive copy."""

    __tablename__ = "blob_root"
    __table_args__ = (
        UniqueConstraint(
            "copy_id",
            "root_path",
            name="uq_blob_root_copy_root",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bundle_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("bundle.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    copy_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("copy.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    pool_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("pool.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    root_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    native_locator: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    archive_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    bundle: Mapped[Bundle] = relationship()
    copy: Mapped[Copy] = relationship()
    pool: Mapped[Pool] = relationship()


class ExclusionRecord(Base):
    """A durable record explaining why material was excluded from bundling."""

    __tablename__ = "exclusion_record"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bundle_id: Mapped[str | None] = mapped_column(
        String(128),
        ForeignKey("bundle.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    artifactclass: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    logical_asset_hash: Mapped[bytes | None] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    reason: Mapped[str] = mapped_column(String(128), nullable=False)
    count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    bytes_total: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    ruleset_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    ruleset_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    detail: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    logical_asset: Mapped[LogicalAsset | None] = relationship()
    bundle: Mapped[Bundle | None] = relationship()


class ReviewDecision(Base):
    """A recorded held-bundle review decision."""

    __tablename__ = "review_decision"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bundle_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("bundle.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    scope: Mapped[str] = mapped_column(String(64), nullable=False)
    subtree: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    reason: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    reviewer: Mapped[str | None] = mapped_column(String(256), nullable=True)
    persisted_rule: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    decided_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    bundle: Mapped[Bundle] = relationship(back_populates="review_decisions")


class Copy(Base):
    """One realization of a logical asset or bundle on one backend.

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
        CheckConstraint(
            "(logical_asset_hash IS NOT NULL AND bundle_id IS NULL) OR "
            "(logical_asset_hash IS NULL AND bundle_id IS NOT NULL)",
            name="ck_copy_asset_xor_bundle",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    logical_asset_hash: Mapped[bytes | None] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    bundle_id: Mapped[str | None] = mapped_column(
        String(128),
        ForeignKey("bundle.id", ondelete="CASCADE"),
        nullable=True,
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
    storage_metadata: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    integrity_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    health: Mapped[CopyHealth] = mapped_column(String(16), nullable=False, default=CopyHealth.OK)
    last_verified_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    first_observed_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    source: Mapped[CopySource] = mapped_column(String(32), nullable=False)

    logical_asset: Mapped[LogicalAsset | None] = relationship(back_populates="copies")
    bundle: Mapped[Bundle | None] = relationship(back_populates="copies")
    backend: Mapped[Backend] = relationship(back_populates="copies")
    pool: Mapped[Pool | None] = relationship(back_populates="copies")

    def __repr__(self) -> str:
        return (
            f"<Copy id={self.id} "
            f"hash={self.logical_asset_hash.hex()[:12] if self.logical_asset_hash else None}… "
            f"backend={self.backend_id} health={self.health}>"
        )
