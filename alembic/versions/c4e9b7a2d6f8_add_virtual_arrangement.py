"""add virtual arrangement tables

Revision ID: c4e9b7a2d6f8
Revises: b2d7f3a8c91e
Create Date: 2026-06-27
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c4e9b7a2d6f8"
down_revision: str | Sequence[str] | None = "b2d7f3a8c91e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    with op.batch_alter_table("logical_asset") as batch:
        batch.add_column(sa.Column("rejected_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("rejected_by", sa.String(length=256), nullable=True))
        batch.add_column(sa.Column("rejection_reason", sa.Text(), nullable=True))

    op.create_table(
        "virtual_arrangement",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=512), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_by", sa.String(length=256), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_virtual_arrangement_name"),
    )

    op.create_table(
        "virtual_arrangement_member",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("va_id", sa.Integer(), nullable=False),
        sa.Column("logical_asset_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("path", sa.String(length=2048), nullable=False),
        sa.Column("excluded", sa.Boolean(), nullable=False),
        sa.Column("added_by", sa.String(length=256), nullable=False),
        sa.Column("added_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["logical_asset_hash"], ["logical_asset.content_sha256"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["va_id"], ["virtual_arrangement.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "va_id",
            "logical_asset_hash",
            "artifactclass",
            name="uq_virtual_arrangement_member_asset_class",
        ),
    )
    op.create_index(
        "ix_virtual_arrangement_member_artifactclass",
        "virtual_arrangement_member",
        ["artifactclass"],
    )
    op.create_index(
        "ix_virtual_arrangement_member_logical_asset_hash",
        "virtual_arrangement_member",
        ["logical_asset_hash"],
    )
    op.create_index("ix_virtual_arrangement_member_va_id", "virtual_arrangement_member", ["va_id"])
    op.create_index(
        "uq_virtual_arrangement_member_path_active",
        "virtual_arrangement_member",
        ["va_id", "path"],
        unique=True,
        sqlite_where=sa.text("excluded = false"),
        postgresql_where=sa.text("excluded = false"),
    )

    op.create_table(
        "virtual_arrangement_history",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("va_id", sa.Integer(), nullable=False),
        sa.Column("va_member_id", sa.Integer(), nullable=True),
        sa.Column("logical_asset_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("old_path", sa.String(length=2048), nullable=False),
        sa.Column("new_path", sa.String(length=2048), nullable=False),
        sa.Column("actor", sa.String(length=256), nullable=False),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["logical_asset_hash"], ["logical_asset.content_sha256"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["va_id"], ["virtual_arrangement.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["va_member_id"],
            ["virtual_arrangement_member.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_virtual_arrangement_history_artifactclass",
        "virtual_arrangement_history",
        ["artifactclass"],
    )
    op.create_index(
        "ix_virtual_arrangement_history_logical_asset_hash",
        "virtual_arrangement_history",
        ["logical_asset_hash"],
    )
    op.create_index(
        "ix_virtual_arrangement_history_va_id",
        "virtual_arrangement_history",
        ["va_id"],
    )
    op.create_index(
        "ix_virtual_arrangement_history_va_member_id",
        "virtual_arrangement_history",
        ["va_member_id"],
    )

    op.create_table(
        "asset_tag",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("logical_asset_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("tag", sa.String(length=256), nullable=False),
        sa.Column("added_by", sa.String(length=256), nullable=False),
        sa.Column("added_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("removed_by", sa.String(length=256), nullable=True),
        sa.Column("removed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["logical_asset_hash"], ["logical_asset.content_sha256"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_asset_tag_logical_asset_hash", "asset_tag", ["logical_asset_hash"])
    op.create_index(
        "uq_asset_tag_active",
        "asset_tag",
        ["logical_asset_hash", "tag"],
        unique=True,
        sqlite_where=sa.text("removed_at IS NULL"),
        postgresql_where=sa.text("removed_at IS NULL"),
    )


def downgrade() -> None:
    """Downgrade schema."""

    op.drop_index("uq_asset_tag_active", table_name="asset_tag")
    op.drop_index("ix_asset_tag_logical_asset_hash", table_name="asset_tag")
    op.drop_table("asset_tag")

    op.drop_index("ix_virtual_arrangement_history_va_member_id", table_name="virtual_arrangement_history")
    op.drop_index("ix_virtual_arrangement_history_va_id", table_name="virtual_arrangement_history")
    op.drop_index(
        "ix_virtual_arrangement_history_logical_asset_hash",
        table_name="virtual_arrangement_history",
    )
    op.drop_index("ix_virtual_arrangement_history_artifactclass", table_name="virtual_arrangement_history")
    op.drop_table("virtual_arrangement_history")

    op.drop_index("uq_virtual_arrangement_member_path_active", table_name="virtual_arrangement_member")
    op.drop_index("ix_virtual_arrangement_member_va_id", table_name="virtual_arrangement_member")
    op.drop_index(
        "ix_virtual_arrangement_member_logical_asset_hash",
        table_name="virtual_arrangement_member",
    )
    op.drop_index(
        "ix_virtual_arrangement_member_artifactclass",
        table_name="virtual_arrangement_member",
    )
    op.drop_table("virtual_arrangement_member")

    op.drop_table("virtual_arrangement")

    with op.batch_alter_table("logical_asset") as batch:
        batch.drop_column("rejection_reason")
        batch.drop_column("rejected_by")
        batch.drop_column("rejected_at")
