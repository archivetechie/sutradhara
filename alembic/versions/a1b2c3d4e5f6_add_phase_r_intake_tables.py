"""Add Phase R intake and derivation tables.

Revision ID: a1b2c3d4e5f6
Revises: 31d2c8f9a4b7
Create Date: 2026-06-17
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a1b2c3d4e5f6"
down_revision: str | Sequence[str] | None = "31d2c8f9a4b7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "intake",
        sa.Column("intake_id", sa.String(length=128), nullable=False),
        sa.Column("operator", sa.String(length=128), nullable=False),
        sa.Column("source_kind", sa.String(length=32), nullable=False),
        sa.Column("source_ref", sa.String(length=1024), nullable=True),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("label", sa.String(length=512), nullable=True),
        sa.Column("manifest_path", sa.String(length=2048), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("registered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("quarantined_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "source_kind IN ('card', 'drive', 'upload', 'handoff', 'download', 'other')",
            name="ck_intake_source_kind",
        ),
        sa.CheckConstraint(
            "status IN ('receiving', 'verifying', 'quarantined', 'registered')",
            name="ck_intake_status",
        ),
        sa.PrimaryKeyConstraint("intake_id"),
    )
    op.create_index(op.f("ix_intake_artifactclass"), "intake", ["artifactclass"], unique=False)

    op.create_table(
        "ingest_item",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("intake_id", sa.String(length=128), nullable=False),
        sa.Column("logical_asset_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("as_received_path", sa.String(length=2048), nullable=False),
        sa.Column("virtual_path", sa.String(length=2048), nullable=False),
        sa.Column("st_dev", sa.BigInteger(), nullable=True),
        sa.Column("st_ino", sa.BigInteger(), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["intake_id"],
            ["intake.intake_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["logical_asset_hash"],
            ["logical_asset.content_sha256"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "intake_id",
            "as_received_path",
            name="uq_ingest_item_intake_as_received_path",
        ),
    )
    op.create_index(op.f("ix_ingest_item_artifactclass"), "ingest_item", ["artifactclass"])
    op.create_index(op.f("ix_ingest_item_intake_id"), "ingest_item", ["intake_id"])
    op.create_index(
        op.f("ix_ingest_item_logical_asset_hash"),
        "ingest_item",
        ["logical_asset_hash"],
    )

    op.create_table(
        "asset_derivation",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("derived_item_id", sa.Integer(), nullable=False),
        sa.Column("source_item_id", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["derived_item_id"],
            ["ingest_item.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_item_id"],
            ["ingest_item.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "derived_item_id",
            "source_item_id",
            "kind",
            name="uq_asset_derivation_derived_source_kind",
        ),
    )
    op.create_index(
        op.f("ix_asset_derivation_derived_item_id"),
        "asset_derivation",
        ["derived_item_id"],
    )
    op.create_index(
        op.f("ix_asset_derivation_source_item_id"),
        "asset_derivation",
        ["source_item_id"],
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_asset_derivation_source_item_id"), table_name="asset_derivation")
    op.drop_index(op.f("ix_asset_derivation_derived_item_id"), table_name="asset_derivation")
    op.drop_table("asset_derivation")

    op.drop_index(op.f("ix_ingest_item_logical_asset_hash"), table_name="ingest_item")
    op.drop_index(op.f("ix_ingest_item_intake_id"), table_name="ingest_item")
    op.drop_index(op.f("ix_ingest_item_artifactclass"), table_name="ingest_item")
    op.drop_table("ingest_item")

    op.drop_index(op.f("ix_intake_artifactclass"), table_name="intake")
    op.drop_table("intake")
