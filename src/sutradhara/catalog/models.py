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
    DDL,
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    event,
    select,
    text,
)
from sqlalchemy.engine import Connection
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)

from sutradhara.catalog.types import (
    ArrangementStatus,
    AssetValidity,
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
    IngestDisposition,
    IntakeSourceKind,
    IntakeStatus,
    IntegrityHashProvenance,
    MediaKind,
    RetentionState,
    SubmissionStatus,
    implementation_family_for_kind,
)
from sutradhara.schema_conventions import vocabulary_check_sql


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
            vocabulary_check_sql("validity", "asset_validity"),
            name="ck_logical_asset_validity",
        ),
        CheckConstraint(
            vocabulary_check_sql("media_kind", "media_kind"),
            name="ck_logical_asset_media_kind",
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
    rejected_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    rejected_by: Mapped[str | None] = mapped_column(String(256), nullable=True)
    rejection_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    copies: Mapped[list[Copy]] = relationship(
        back_populates="logical_asset",
        lazy="selectin",
        passive_deletes=True,
    )
    ingest_items: Mapped[list[IngestItem]] = relationship(
        back_populates="logical_asset",
        lazy="selectin",
    )
    virtual_members: Mapped[list[VirtualArrangementMember]] = relationship(
        back_populates="logical_asset",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<LogicalAsset hash={self.content_sha256.hex()[:12]}… "
            f"size={self.size_bytes} copies={len(self.copies)}>"
        )


class ArtifactClass(Base):
    """Policy-administered registry of valid artifactclass names."""

    __tablename__ = "artifactclass"

    name: Mapped[str] = mapped_column(String(128), primary_key=True)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )


class Intake(Base):
    """A completed landing batch admitted through explicit intake registration.

    Intake treats an `intake.json` sentinel as the boundary between
    receiving and verifying. A quarantined intake records the failed batch but
    does not register any `ingest_item` rows.
    """

    __tablename__ = "intake"
    __table_args__ = (
        CheckConstraint(
            vocabulary_check_sql("source_kind", "intake_source_kind"),
            name="ck_intake_source_kind",
        ),
        CheckConstraint(
            vocabulary_check_sql("status", "intake_status"),
            name="ck_intake_status",
        ),
        CheckConstraint(
            vocabulary_check_sql("retention_state", "retention_state"),
            name="ck_intake_retention_state",
        ),
        CheckConstraint(
            "(status = 'registered' AND retention_state != 'not_applicable') OR "
            "(status != 'registered' AND retention_state = 'not_applicable')",
            name="ck_intake_retention_ordering",
        ),
        CheckConstraint(
            "(staging_tombstoned_at IS NULL AND staging_tombstone_path IS NULL) OR "
            "(staging_tombstoned_at IS NOT NULL AND staging_tombstone_path IS NOT NULL)",
            name="ck_intake_tombstone_pair",
        ),
        CheckConstraint(
            "retention_state != 'tombstoned' OR "
            "(staging_tombstoned_at IS NOT NULL AND staging_tombstone_path IS NOT NULL)",
            name="ck_intake_tombstoned_state",
        ),
        CheckConstraint(
            "retention_state != 'purged' OR staging_deleted_at IS NOT NULL",
            name="ck_intake_purged_state",
        ),
    )

    intake_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    operator: Mapped[str] = mapped_column(String(128), nullable=False)
    source_kind: Mapped[IntakeSourceKind] = mapped_column(String(32), nullable=False)
    source_ref: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    card_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    device_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    artifactclass: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("artifactclass.name", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    label: Mapped[str | None] = mapped_column(String(512), nullable=True)
    manifest_path: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    manifest_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    requested_profile: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[IntakeStatus] = mapped_column(
        String(32), nullable=False, default=IntakeStatus.VERIFYING
    )
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )
    registered_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    quarantined_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    retention_state: Mapped[RetentionState] = mapped_column(
        String(32),
        nullable=False,
        default=RetentionState.NOT_APPLICABLE,
        server_default=RetentionState.NOT_APPLICABLE.value,
    )
    released_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    release_policy_fingerprint: Mapped[str | None] = mapped_column(String(67), nullable=True)
    staging_tombstoned_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    staging_tombstone_path: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    staging_deleted_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    items: Mapped[list[IngestItem]] = relationship(
        back_populates="intake",
        cascade="all, delete-orphan",
        lazy="selectin",
        foreign_keys="IngestItem.intake_id",
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
        CheckConstraint(
            vocabulary_check_sql("disposition", "ingest_disposition"),
            name="ck_ingest_item_disposition",
        ),
        UniqueConstraint(
            "intake_id",
            "as_received_path",
            name="uq_ingest_item_intake_as_received_path",
        ),
        Index(
            "ix_ingest_item_intake_hash",
            "intake_id",
            "logical_asset_hash",
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
        ForeignKey("logical_asset.content_sha256", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    as_received_path: Mapped[str] = mapped_column(String(2048), nullable=False)
    virtual_path: Mapped[str] = mapped_column(String(2048), nullable=False)
    st_dev: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    st_ino: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    artifactclass: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("artifactclass.name", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    disposition: Mapped[IngestDisposition] = mapped_column(
        String(32),
        nullable=False,
        default=IngestDisposition.LEGACY_UNKNOWN,
        server_default=IngestDisposition.LEGACY_UNKNOWN.value,
    )
    disposition_evaluated_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    disposition_policy_generation: Mapped[str | None] = mapped_column(String(128), nullable=True)
    disposition_evidence: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    prior_intake_id: Mapped[str | None] = mapped_column(
        String(128),
        ForeignKey("intake.intake_id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    item_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSON,
        nullable=False,
        default=dict,
    )
    source_path: Mapped[str | None] = mapped_column(String(4096), nullable=True)
    pfr_sidecar_path: Mapped[str | None] = mapped_column(String(4096), nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    intake: Mapped[Intake] = relationship(back_populates="items", foreign_keys=[intake_id])
    logical_asset: Mapped[LogicalAsset] = relationship(back_populates="ingest_items")
    derived_from_edges: Mapped[list[AssetDerivation]] = relationship(
        back_populates="derived_item",
        foreign_keys="AssetDerivation.derived_item_id",
        lazy="selectin",
        passive_deletes=True,
    )
    source_for_edges: Mapped[list[AssetDerivation]] = relationship(
        back_populates="source_item",
        foreign_keys="AssetDerivation.source_item_id",
        lazy="selectin",
        passive_deletes=True,
    )

    def __repr__(self) -> str:
        return f"<IngestItem id={self.id} intake={self.intake_id!r} path={self.as_received_path!r}>"


@event.listens_for(Intake, "before_insert")
def _initialize_intake_retention_state(
    mapper: object,
    connection: Connection,
    target: Intake,
) -> None:
    """Initialize retention only after the registration boundary is known."""

    del mapper, connection
    if target.status == IntakeStatus.REGISTERED and target.retention_state in (
        None,
        RetentionState.NOT_APPLICABLE,
    ):
        target.retention_state = RetentionState.HELD
    elif target.retention_state is None:
        target.retention_state = RetentionState.NOT_APPLICABLE


class AssetDerivation(Base):
    """A provenance edge from one ingested occurrence to a derived occurrence."""

    __tablename__ = "asset_derivation"
    __table_args__ = (
        CheckConstraint(
            vocabulary_check_sql("kind", "asset_derivation_kind"),
            name="ck_asset_derivation_kind",
        ),
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
        ForeignKey("ingest_item.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    source_item_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("ingest_item.id", ondelete="RESTRICT"),
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


class Arrangement(Base):
    """Mutable pre-archive namespace over registered master ingest items."""

    __tablename__ = "arrangement"
    __table_args__ = (
        CheckConstraint(
            vocabulary_check_sql("status", "arrangement_status"),
            name="ck_arrangement_status",
        ),
        CheckConstraint(
            "(status = 'abandoned' AND abandoned_at IS NOT NULL AND abandoned_by IS NOT NULL) OR "
            "(status != 'abandoned' AND abandoned_at IS NULL AND abandoned_by IS NULL "
            "AND abandonment_reason IS NULL)",
            name="ck_arrangement_abandonment",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    label: Mapped[str] = mapped_column(String(512), nullable=False)
    intake_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("intake.intake_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    artifactclass: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("artifactclass.name", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    status: Mapped[ArrangementStatus] = mapped_column(
        String(32), nullable=False, default=ArrangementStatus.DRAFT, index=True
    )
    submission_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey(
            "submission.id",
            ondelete="SET NULL",
            name="fk_arrangement_submission_id",
            use_alter=True,
        ),
        nullable=True,
        index=True,
    )
    cloned_from_arrangement_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "arrangement.id",
            ondelete="SET NULL",
            name="fk_arrangement_cloned_from_arrangement_id",
            use_alter=True,
        ),
        nullable=True,
        index=True,
    )
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )
    submitted_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    abandoned_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    abandoned_by: Mapped[str | None] = mapped_column(String(256), nullable=True)
    abandonment_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    intake: Mapped[Intake] = relationship()
    members: Mapped[list[ArrangementMember]] = relationship(
        back_populates="arrangement",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="ArrangementMember.member_path",
    )

    def __repr__(self) -> str:
        return f"<Arrangement id={self.id} intake={self.intake_id!r} status={self.status!r}>"


class ArrangementMember(Base):
    """One arranged archive entry in a draft arrangement."""

    __tablename__ = "arrangement_member"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    arrangement_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("arrangement.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    ingest_item_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("ingest_item.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    member_path: Mapped[str] = mapped_column(String(2048), nullable=False)
    excluded: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )

    arrangement: Mapped[Arrangement] = relationship(back_populates="members")
    ingest_item: Mapped[IngestItem] = relationship()

    def __repr__(self) -> str:
        return (
            f"<ArrangementMember arrangement={self.arrangement_id} "
            f"path={self.member_path!r} excluded={self.excluded}>"
        )


class Submission(Base):
    """Frozen source-map payload emitted by arrangement submit."""

    __tablename__ = "submission"
    __table_args__ = (
        CheckConstraint(
            vocabulary_check_sql("status", "submission_status"),
            name="ck_submission_status",
        ),
        UniqueConstraint("arrangement_id", name="uq_submission_arrangement_id"),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    arrangement_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("arrangement.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    artifactclass: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("artifactclass.name", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    source_map_path: Mapped[str] = mapped_column(String(4096), nullable=False)
    manifest_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    member_count: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[SubmissionStatus] = mapped_column(
        String(32), nullable=False, default=SubmissionStatus.PENDING_ARCHIVE, index=True
    )
    archived_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    submitted_by: Mapped[str] = mapped_column(String(256), nullable=False)
    submitted_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    arrangement: Mapped[Arrangement] = relationship(foreign_keys=[arrangement_id])
    members: Mapped[list[SubmissionMember]] = relationship(
        back_populates="submission",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="SubmissionMember.ord",
    )

    def __repr__(self) -> str:
        return (
            f"<Submission id={self.id!r} arrangement={self.arrangement_id} status={self.status!r}>"
        )


class SubmissionMember(Base):
    """DB-queryable mirror of one immutable source-map row."""

    __tablename__ = "submission_member"
    __table_args__ = (
        UniqueConstraint(
            "submission_id",
            "archive_path",
            name="uq_submission_member_submission_archive_path",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    submission_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("submission.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    ingest_item_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("ingest_item.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    archive_path: Mapped[str] = mapped_column(String(2048), nullable=False)
    source_path: Mapped[str] = mapped_column(String(4096), nullable=False, index=True)
    sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ord: Mapped[int] = mapped_column(Integer, nullable=False)

    submission: Mapped[Submission] = relationship(back_populates="members")
    ingest_item: Mapped[IngestItem] = relationship()


Index(
    "uq_arrangement_member_path_active",
    ArrangementMember.arrangement_id,
    ArrangementMember.member_path,
    unique=True,
    sqlite_where=ArrangementMember.excluded.is_(False),
    postgresql_where=ArrangementMember.excluded.is_(False),
)


class VirtualArrangement(Base):
    """A permanently mutable organizational view over archived logical assets."""

    __tablename__ = "virtual_arrangement"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(512), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str] = mapped_column(String(256), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )

    members: Mapped[list[VirtualArrangementMember]] = relationship(
        back_populates="view",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="VirtualArrangementMember.path",
    )
    history: Mapped[list[VirtualArrangementHistory]] = relationship(
        back_populates="view",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return f"<VirtualArrangement id={self.id} name={self.name!r}>"


class VirtualArrangementMember(Base):
    """One archived asset placed at a virtual path within one view."""

    __tablename__ = "virtual_arrangement_member"
    __table_args__ = (
        UniqueConstraint(
            "va_id",
            "logical_asset_hash",
            "artifactclass",
            name="uq_virtual_arrangement_member_asset_class",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    va_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("virtual_arrangement.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    logical_asset_hash: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    artifactclass: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("artifactclass.name", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    path: Mapped[str] = mapped_column(String(2048), nullable=False)
    excluded: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    added_by: Mapped[str] = mapped_column(String(256), nullable=False)
    added_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )

    view: Mapped[VirtualArrangement] = relationship(back_populates="members")
    logical_asset: Mapped[LogicalAsset] = relationship(back_populates="virtual_members")
    history: Mapped[list[VirtualArrangementHistory]] = relationship(
        back_populates="member",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<VirtualArrangementMember va={self.va_id} "
            f"asset={self.logical_asset_hash.hex()[:12]} class={self.artifactclass!r} "
            f"path={self.path!r} excluded={self.excluded}>"
        )


class VirtualArrangementHistory(Base):
    """Append-only audit row for virtual arrangement path moves."""

    __tablename__ = "virtual_arrangement_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    va_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("virtual_arrangement.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    va_member_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("virtual_arrangement_member.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    logical_asset_hash: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    artifactclass: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("artifactclass.name", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    old_path: Mapped[str] = mapped_column(String(2048), nullable=False)
    new_path: Mapped[str] = mapped_column(String(2048), nullable=False)
    actor: Mapped[str] = mapped_column(String(256), nullable=False)
    changed_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    view: Mapped[VirtualArrangement] = relationship(back_populates="history")
    member: Mapped[VirtualArrangementMember | None] = relationship(back_populates="history")
    logical_asset: Mapped[LogicalAsset] = relationship()


class AssetTag(Base):
    """Soft-deleted governance tag attached to a logical asset."""

    __tablename__ = "asset_tag"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    logical_asset_hash: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    tag: Mapped[str] = mapped_column(String(256), nullable=False)
    added_by: Mapped[str] = mapped_column(String(256), nullable=False)
    added_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    removed_by: Mapped[str | None] = mapped_column(String(256), nullable=True)
    removed_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    logical_asset: Mapped[LogicalAsset] = relationship()


Index(
    "uq_virtual_arrangement_member_path_active",
    VirtualArrangementMember.va_id,
    VirtualArrangementMember.path,
    unique=True,
    sqlite_where=VirtualArrangementMember.excluded.is_(False),
    postgresql_where=VirtualArrangementMember.excluded.is_(False),
)

Index(
    "uq_asset_tag_active",
    AssetTag.logical_asset_hash,
    AssetTag.tag,
    unique=True,
    sqlite_where=AssetTag.removed_at.is_(None),
    postgresql_where=AssetTag.removed_at.is_(None),
)


class Backend(Base):
    """A registered storage backend (one row per backend instance)."""

    __tablename__ = "backend"
    __table_args__ = (
        CheckConstraint(
            vocabulary_check_sql("tier", "backend_tier"),
            name="ck_backend_tier",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    kind: Mapped[BackendKind] = mapped_column(String(32), nullable=False)
    implementation_family: Mapped[str] = mapped_column(String(64), nullable=False)
    config: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    tier: Mapped[BackendTier] = mapped_column(String(32), nullable=False)
    added_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    copies: Mapped[list[Copy]] = relationship(
        back_populates="backend",
        lazy="selectin",
        passive_deletes=True,
    )
    pools: Mapped[list[Pool]] = relationship(
        back_populates="backend",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<Backend id={self.id} name={self.name!r} kind={self.kind} "
            f"family={self.implementation_family!r} tier={self.tier}>"
        )


@event.listens_for(Backend, "before_insert")
@event.listens_for(Backend, "before_update")
def _set_backend_implementation_family(
    mapper: object,
    connection: Connection,
    target: Backend,
) -> None:
    """Populate the required durability family from the backend kind registry."""

    del mapper, connection
    target.implementation_family = implementation_family_for_kind(target.kind)


class OffsiteConfirmation(Base):
    """Operator confirmation that one tape/media id is durably offsite."""

    __tablename__ = "offsite_confirmation"
    __table_args__ = (
        CheckConstraint(
            "(revoked_at IS NULL AND revoked_by IS NULL) OR "
            "(revoked_at IS NOT NULL AND revoked_by IS NOT NULL AND length(revoked_by) > 0)",
            name="ck_offsite_confirmation_revocation_pair",
        ),
    )

    media_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    confirmed_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    confirmed_by: Mapped[str] = mapped_column(String(256), nullable=False)
    shipment_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    revoked_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_by: Mapped[str | None] = mapped_column(String(256), nullable=True)

    def __repr__(self) -> str:
        return f"<OffsiteConfirmation media_id={self.media_id!r}>"


class RetentionEvent(Base):
    """Append-only audit event for retention gate and deletion actions."""

    __tablename__ = "retention_event"
    __table_args__ = (
        CheckConstraint(
            vocabulary_check_sql("action", "retention_action"),
            name="ck_retention_event_action",
        ),
        CheckConstraint(
            "(subject_type = 'intake' AND intake_id IS NOT NULL AND "
            "intake_id = subject_id AND action IN "
            "('released', 'cloud_blob_deleted', 'staging_deleted', 'release_attempted', "
            "'purge_attempted', 'staging_tombstoned', 'staging_purge_held', 'abandoned', "
            "'correction_recorded')) OR "
            "(subject_type = 'media' AND intake_id IS NULL AND action IN "
            "('offsite_confirmed', 'correction_recorded')) OR "
            "(subject_type = 'batch' AND intake_id IS NULL AND action IN "
            "('batch_invoked', 'batch_refused', 'grace_overridden')) OR "
            "(subject_type = 'receipt' AND intake_id IS NULL AND "
            "action = 'correction_recorded')",
            name="ck_retention_event_subject",
        ),
        CheckConstraint(
            "(supersedes_source IS NULL AND supersedes_event_id IS NULL) OR "
            "(supersedes_source IN ('verify_receipt', 'retention_event') AND "
            "supersedes_event_id IS NOT NULL AND action = 'correction_recorded')",
            name="ck_retention_event_supersession",
        ),
    )

    event_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    intake_id: Mapped[str | None] = mapped_column(
        String(128),
        ForeignKey("intake.intake_id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    subject_type: Mapped[str] = mapped_column(String(16), nullable=False)
    subject_id: Mapped[str] = mapped_column(String(256), nullable=False)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    operation_id: Mapped[str] = mapped_column(String(512), nullable=False)
    actor: Mapped[str] = mapped_column(String(256), nullable=False)
    occurred_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    detail: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    supersedes_source: Mapped[str | None] = mapped_column(String(32), nullable=True)
    supersedes_event_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    intake: Mapped[Intake | None] = relationship()

    def __repr__(self) -> str:
        return f"<RetentionEvent intake={self.intake_id!r} action={self.action!r}>"


Index(
    "uq_retention_event_action_operation_once",
    RetentionEvent.action,
    RetentionEvent.operation_id,
    unique=True,
    sqlite_where=RetentionEvent.action.in_(
        (
            "release_attempted",
            "cloud_blob_deleted",
            "released",
            "purge_attempted",
            "staging_tombstoned",
            "staging_deleted",
        )
    ),
    postgresql_where=RetentionEvent.action.in_(
        (
            "release_attempted",
            "cloud_blob_deleted",
            "released",
            "purge_attempted",
            "staging_tombstoned",
            "staging_deleted",
        )
    ),
)


class RetentionJournalCheckpoint(Base):
    """Optimization mirror of the last authoritative published journal footer."""

    __tablename__ = "retention_journal_checkpoint"
    __table_args__ = (
        CheckConstraint("id = 1", name="ck_retention_journal_checkpoint_singleton"),
        CheckConstraint("global_sequence >= 0", name="ck_retention_journal_sequence"),
        CheckConstraint(
            "verify_receipt_cursor >= 0 AND retention_event_cursor >= 0",
            name="ck_retention_journal_cursors",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    envelope_id: Mapped[str] = mapped_column(String(128), nullable=False)
    hash_algorithm_id: Mapped[str] = mapped_column(String(32), nullable=False)
    global_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    head_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    verify_receipt_cursor: Mapped[int] = mapped_column(Integer, nullable=False)
    retention_event_cursor: Mapped[int] = mapped_column(Integer, nullable=False)
    published_filename: Mapped[str] = mapped_column(String(512), nullable=False)
    published_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Pool(Base):
    """A durable storage pool owned by a backend.

    Pool identity is the storage policy surface. A pool owns its byte
    representation; copies point at the pool they were written through.
    """

    __tablename__ = "pool"
    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    backend_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("backend.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    representation: Mapped[str] = mapped_column(String(64), nullable=False)
    location: Mapped[str | None] = mapped_column(String(256), nullable=True)
    offsite_gate: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    storage_class: Mapped[str | None] = mapped_column(String(64), nullable=True)
    accepts_writes: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    retired: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    media_generation: Mapped[str | None] = mapped_column(String(128), nullable=True)
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
    artifactclass: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("artifactclass.name", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
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

    artifactclass: Mapped[str] = mapped_column(
        String(128), ForeignKey("artifactclass.name", ondelete="RESTRICT"), primary_key=True
    )
    ruleset: Mapped[str] = mapped_column(String(256), nullable=False)
    expect: Mapped[str] = mapped_column(String(32), nullable=False)
    target_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_age_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    restore_preference: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    min_copies: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    min_impl_families: Mapped[int] = mapped_column(Integer, nullable=False, default=2)
    staging_config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    hdcache_config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    policy_source: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    policy_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )

    def __repr__(self) -> str:
        return (
            f"<ArtifactClassPolicy artifactclass={self.artifactclass!r} "
            f"ruleset={self.ruleset!r} expect={self.expect!r}>"
        )


@event.listens_for(ArtifactClassPolicyRecord, "before_insert")
def _register_policy_artifactclass(
    mapper: object,
    connection: Connection,
    target: ArtifactClassPolicyRecord,
) -> None:
    """Make policy administration the sole artifactclass-registry writer."""

    del mapper
    exists = connection.scalar(
        select(ArtifactClass.name).where(ArtifactClass.name == target.artifactclass)
    )
    if exists is None:
        connection.execute(
            ArtifactClass.__table__.insert().values(
                name=target.artifactclass,
                created_at=_utcnow(),
            )
        )


class Bundle(Base):
    """A synthetic archive object containing one or more logical assets."""

    __tablename__ = "bundle"
    __table_args__ = (
        CheckConstraint(
            vocabulary_check_sql("status", "bundle_status"),
            name="ck_bundle_status",
        ),
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    artifactclass: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("artifactclass.name", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
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
        lazy="selectin",
        passive_deletes=True,
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
        UniqueConstraint(
            "id",
            "bundle_id",
            "logical_asset_hash",
            name="uq_bundle_member_id_bundle_asset",
        ),
        UniqueConstraint(
            "bundle_id",
            "logical_asset_hash",
            "member_path",
            name="uq_bundle_member_bundle_asset_path",
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
        ForeignKey("logical_asset.content_sha256", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    member_path: Mapped[str] = mapped_column(String(2048), nullable=False)
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
        lazy="selectin",
        order_by="StagingTransform.step_order",
        passive_deletes=True,
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
        ForeignKeyConstraint(
            ("bundle_member_id", "bundle_id", "logical_asset_hash"),
            ("bundle_member.id", "bundle_member.bundle_id", "bundle_member.logical_asset_hash"),
            ondelete="RESTRICT",
            name="fk_staging_transform_member_identity",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bundle_member_id: Mapped[int] = mapped_column(
        Integer,
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
        ForeignKey("logical_asset.content_sha256", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    artifactclass: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("artifactclass.name", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
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

    bundle_member: Mapped[BundleMember] = relationship(
        back_populates="transforms", foreign_keys=[bundle_member_id]
    )
    logical_asset: Mapped[LogicalAsset] = relationship(overlaps="transforms")
    bundle: Mapped[Bundle] = relationship(overlaps="transforms")


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
        ForeignKeyConstraint(
            ("copy_id", "pool_id", "bundle_id"),
            ("copy.id", "copy.pool_id", "copy.bundle_id"),
            ondelete="RESTRICT",
            name="fk_asset_locator_copy_pool_bundle",
        ),
        ForeignKeyConstraint(
            ("bundle_id", "logical_asset_hash", "member_path"),
            (
                "bundle_member.bundle_id",
                "bundle_member.logical_asset_hash",
                "bundle_member.member_path",
            ),
            ondelete="RESTRICT",
            name="fk_asset_locator_bundle_member",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    logical_asset_hash: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    pool_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("pool.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    copy_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        index=True,
    )
    bundle_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("bundle.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    native_locator: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    member_path: Mapped[str] = mapped_column(String(2048), nullable=False)
    representation: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    logical_asset: Mapped[LogicalAsset] = relationship(foreign_keys=[logical_asset_hash])
    pool: Mapped[Pool] = relationship(foreign_keys=[pool_id])
    copy: Mapped[Copy] = relationship(
        foreign_keys=[copy_id, pool_id, bundle_id], overlaps="pool,locators"
    )
    bundle: Mapped[Bundle] = relationship(
        back_populates="locators", foreign_keys=[bundle_id], overlaps="copy"
    )


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
    artifactclass: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("artifactclass.name", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
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
    __table_args__ = (
        CheckConstraint(
            vocabulary_check_sql("action", "review_action"),
            name="ck_review_decision_action",
        ),
        CheckConstraint(
            vocabulary_check_sql("scope", "review_scope"),
            name="ck_review_decision_scope",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bundle_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("bundle.id", ondelete="RESTRICT"),
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


class AssetReviewEvent(Base):
    """Append-only reject/unreject decision history for one logical asset."""

    __tablename__ = "asset_review_event"
    __table_args__ = (
        CheckConstraint(
            vocabulary_check_sql("action", "asset_review_action"),
            name="ck_asset_review_event_action",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    logical_asset_hash: Mapped[bytes] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    actor: Mapped[str] = mapped_column(String(256), nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    decided_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )


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
        UniqueConstraint("id", "pool_id", "bundle_id", name="uq_copy_id_pool_bundle"),
        CheckConstraint(
            "(logical_asset_hash IS NOT NULL AND bundle_id IS NULL) OR "
            "(logical_asset_hash IS NULL AND bundle_id IS NOT NULL)",
            name="ck_copy_asset_xor_bundle",
        ),
        CheckConstraint(
            vocabulary_check_sql("health", "copy_health"),
            name="ck_copy_health",
        ),
        CheckConstraint(
            "(last_measured_digest IS NULL AND last_measured_at IS NULL) OR "
            "(last_measured_digest IS NOT NULL AND last_measured_at IS NOT NULL)",
            name="ck_copy_measurement_pair",
        ),
        CheckConstraint(
            vocabulary_check_sql("integrity_hash_provenance", "integrity_hash_provenance"),
            name="ck_copy_integrity_hash_provenance",
        ),
        CheckConstraint(
            vocabulary_check_sql("source", "copy_source"),
            name="ck_copy_source",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    logical_asset_hash: Mapped[bytes | None] = mapped_column(
        LargeBinary(32),
        ForeignKey("logical_asset.content_sha256", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    bundle_id: Mapped[str | None] = mapped_column(
        String(128),
        ForeignKey("bundle.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    backend_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("backend.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    pool_id: Mapped[str | None] = mapped_column(
        String(128),
        ForeignKey("pool.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )

    # The locator itself is structured per-backend; we store the JSON for
    # querying/display AND a deterministic string key for the UNIQUE
    # constraint (SQLite cannot UNIQUE-index on a JSON column directly,
    # and ordering of dict keys in JSON storage is implementation-defined).
    native_locator: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    native_locator_key: Mapped[str] = mapped_column(String(512), nullable=False)
    media_id: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    media_family: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    storage_metadata: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    integrity_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    integrity_hash_provenance: Mapped[IntegrityHashProvenance] = mapped_column(
        String(32),
        nullable=False,
        default=IntegrityHashProvenance.LOCALLY_COMPUTED,
        server_default=IntegrityHashProvenance.LOCALLY_COMPUTED.value,
    )
    health: Mapped[CopyHealth] = mapped_column(String(16), nullable=False, default=CopyHealth.OK)
    health_changed_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )
    last_checked_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_measured_digest: Mapped[bytes | None] = mapped_column(LargeBinary(32), nullable=True)
    last_measured_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deleted_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
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


@event.listens_for(Copy, "before_insert")
def _materialize_copy_media_identity(
    mapper: object,
    connection: Connection,
    target: Copy,
) -> None:
    """Write canonical media columns at the copy-registration boundary."""

    from sutradhara.catalog.media_identity import _copy_media_id

    del mapper
    family = connection.execute(
        select(Backend.implementation_family).where(Backend.id == target.backend_id)
    ).scalar_one_or_none()
    if family is None:
        return
    identity = _copy_media_id(family, target.native_locator, target.backend_id)
    target.media_id = identity.media_id
    target.media_family = identity.media_family


class VerifyReceipt(Base):
    """Append-only audit receipt emitted atomically with measurement projection changes."""

    __tablename__ = "verify_receipt"
    __table_args__ = (
        CheckConstraint(
            vocabulary_check_sql("source", "verify_receipt_source"),
            name="ck_verify_receipt_source",
        ),
        CheckConstraint(
            "source != 'scrub' OR measured_digest IS NULL",
            name="ck_verify_receipt_scrub_unmeasured",
        ),
        UniqueConstraint(
            "source",
            "execution_id",
            "copy_id",
            name="uq_verify_receipt_execution_copy",
        ),
    )

    event_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    copy_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("copy.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    backend_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("backend.id", ondelete="RESTRICT"),
        nullable=False,
    )
    expected_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    measured_digest: Mapped[bytes | None] = mapped_column(LargeBinary(32), nullable=True)
    backend_ok: Mapped[bool] = mapped_column(Boolean, nullable=False)
    failure_kind: Mapped[str | None] = mapped_column(String(64), nullable=True)
    failure_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    execution_id: Mapped[str] = mapped_column(String(512), nullable=False)
    producer_process: Mapped[str] = mapped_column(String(512), nullable=False)
    actor: Mapped[str | None] = mapped_column(String(256), nullable=True)
    recorded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    copy: Mapped[Copy] = relationship()
    backend: Mapped[Backend] = relationship()


event.listen(
    Copy.__table__,
    "after_create",
    DDL(
        "CREATE TRIGGER trg_copy_health_changed_at "
        "AFTER UPDATE OF health ON copy "
        "FOR EACH ROW WHEN NEW.health IS NOT OLD.health "
        "BEGIN UPDATE copy SET health_changed_at = CURRENT_TIMESTAMP WHERE id = NEW.id; END"
    ).execute_if(dialect="sqlite"),
)
