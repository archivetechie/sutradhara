"""add arrangement submit tables

Revision ID: 8f1d2c3b4a9e
Revises: 6a0f4c2e9d1b
Create Date: 2026-06-26
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "8f1d2c3b4a9e"
down_revision: str | Sequence[str] | None = "6a0f4c2e9d1b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    inline_submission_fk = op.get_bind().dialect.name == "sqlite"
    arrangement_constraints = [
        sa.CheckConstraint(
            "status IN ('draft', 'pending_derivatives', 'ready', 'submitted', 'abandoned')",
            name="ck_arrangement_status",
        ),
        sa.ForeignKeyConstraint(["intake_id"], ["intake.intake_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    ]
    if inline_submission_fk:
        arrangement_constraints.append(
            sa.ForeignKeyConstraint(
                ["submission_id"],
                ["submission.id"],
                name="fk_arrangement_submission_id",
                ondelete="SET NULL",
            )
        )

    op.create_table(
        "arrangement",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("label", sa.String(length=512), nullable=False),
        sa.Column("intake_id", sa.String(length=128), nullable=False),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("submission_id", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        *arrangement_constraints,
    )
    op.create_index("ix_arrangement_artifactclass", "arrangement", ["artifactclass"])
    op.create_index("ix_arrangement_intake_id", "arrangement", ["intake_id"])
    op.create_index("ix_arrangement_status", "arrangement", ["status"])
    op.create_index("ix_arrangement_submission_id", "arrangement", ["submission_id"])

    op.create_table(
        "arrangement_member",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("arrangement_id", sa.Integer(), nullable=False),
        sa.Column("ingest_item_id", sa.Integer(), nullable=False),
        sa.Column("member_path", sa.String(length=2048), nullable=False),
        sa.Column("excluded", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["arrangement_id"], ["arrangement.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["ingest_item_id"], ["ingest_item.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_arrangement_member_arrangement_id", "arrangement_member", ["arrangement_id"])
    op.create_index("ix_arrangement_member_ingest_item_id", "arrangement_member", ["ingest_item_id"])
    op.create_index(
        "uq_arrangement_member_path_active",
        "arrangement_member",
        ["arrangement_id", "member_path"],
        unique=True,
        sqlite_where=sa.text("excluded = false"),
        postgresql_where=sa.text("excluded = false"),
    )

    op.create_table(
        "submission",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("arrangement_id", sa.Integer(), nullable=False),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("source_map_path", sa.String(length=4096), nullable=False),
        sa.Column("manifest_digest", sa.String(length=64), nullable=False),
        sa.Column("member_count", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("submitted_by", sa.String(length=256), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('pending_archive', 'archived')",
            name="ck_submission_status",
        ),
        sa.ForeignKeyConstraint(["arrangement_id"], ["arrangement.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("arrangement_id", name="uq_submission_arrangement_id"),
    )
    op.create_index("ix_submission_arrangement_id", "submission", ["arrangement_id"])
    op.create_index("ix_submission_artifactclass", "submission", ["artifactclass"])
    op.create_index("ix_submission_status", "submission", ["status"])
    if not inline_submission_fk:
        op.create_foreign_key(
            "fk_arrangement_submission_id",
            "arrangement",
            "submission",
            ["submission_id"],
            ["id"],
            ondelete="SET NULL",
        )

    op.create_table(
        "submission_member",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("submission_id", sa.String(length=64), nullable=False),
        sa.Column("ingest_item_id", sa.Integer(), nullable=True),
        sa.Column("archive_path", sa.String(length=2048), nullable=False),
        sa.Column("source_path", sa.String(length=4096), nullable=False),
        sa.Column("sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("ord", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["ingest_item_id"], ["ingest_item.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["submission_id"], ["submission.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "submission_id",
            "archive_path",
            name="uq_submission_member_submission_archive_path",
        ),
    )
    op.create_index("ix_submission_member_ingest_item_id", "submission_member", ["ingest_item_id"])
    op.create_index("ix_submission_member_source_path", "submission_member", ["source_path"])
    op.create_index("ix_submission_member_submission_id", "submission_member", ["submission_id"])


def downgrade() -> None:
    """Downgrade schema."""

    if op.get_bind().dialect.name != "sqlite":
        op.drop_constraint(
            "fk_arrangement_submission_id",
            "arrangement",
            type_="foreignkey",
        )

    op.drop_index("ix_submission_member_submission_id", table_name="submission_member")
    op.drop_index("ix_submission_member_source_path", table_name="submission_member")
    op.drop_index("ix_submission_member_ingest_item_id", table_name="submission_member")
    op.drop_table("submission_member")

    op.drop_index("ix_submission_status", table_name="submission")
    op.drop_index("ix_submission_artifactclass", table_name="submission")
    op.drop_index("ix_submission_arrangement_id", table_name="submission")
    op.drop_table("submission")

    op.drop_index("uq_arrangement_member_path_active", table_name="arrangement_member")
    op.drop_index("ix_arrangement_member_ingest_item_id", table_name="arrangement_member")
    op.drop_index("ix_arrangement_member_arrangement_id", table_name="arrangement_member")
    op.drop_table("arrangement_member")

    op.drop_index("ix_arrangement_submission_id", table_name="arrangement")
    op.drop_index("ix_arrangement_status", table_name="arrangement")
    op.drop_index("ix_arrangement_intake_id", table_name="arrangement")
    op.drop_index("ix_arrangement_artifactclass", table_name="arrangement")
    op.drop_table("arrangement")
