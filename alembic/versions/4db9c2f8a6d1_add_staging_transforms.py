"""Add artifact staging transform bookkeeping.

Revision ID: 4db9c2f8a6d1
Revises: 2f4a8bb0c2d7
Create Date: 2026-06-15
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "4db9c2f8a6d1"
down_revision: str | Sequence[str] | None = "2f4a8bb0c2d7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("artifactclass_policy") as batch_op:
        batch_op.add_column(
            sa.Column(
                "staging_config",
                sa.JSON(),
                nullable=False,
                server_default=sa.text("'{}'"),
            )
        )
        batch_op.alter_column("staging_config", server_default=None)

    op.create_table(
        "staging_transform",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "bundle_member_id",
            sa.Integer(),
            sa.ForeignKey("bundle_member.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "bundle_id",
            sa.String(length=128),
            sa.ForeignKey("bundle.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "logical_asset_hash",
            sa.LargeBinary(length=32),
            sa.ForeignKey("logical_asset.content_sha256", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("step_order", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column("reversible", sa.Boolean(), nullable=False),
        sa.Column("original_member_path", sa.String(length=1024), nullable=False),
        sa.Column("stored_member_path", sa.String(length=1024), nullable=False),
        sa.Column("original_size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("stored_size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("original_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("stored_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("parameters", sa.JSON(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "bundle_member_id",
            "step_order",
            name="uq_staging_transform_member_step",
        ),
        sa.UniqueConstraint(
            "bundle_id",
            "stored_member_path",
            "step_order",
            name="uq_staging_transform_bundle_stored_step",
        ),
    )
    op.create_index(
        "ix_staging_transform_bundle_member_id",
        "staging_transform",
        ["bundle_member_id"],
    )
    op.create_index("ix_staging_transform_bundle_id", "staging_transform", ["bundle_id"])
    op.create_index(
        "ix_staging_transform_logical_asset_hash",
        "staging_transform",
        ["logical_asset_hash"],
    )
    op.create_index(
        "ix_staging_transform_artifactclass",
        "staging_transform",
        ["artifactclass"],
    )


def downgrade() -> None:
    op.drop_index("ix_staging_transform_artifactclass", table_name="staging_transform")
    op.drop_index("ix_staging_transform_logical_asset_hash", table_name="staging_transform")
    op.drop_index("ix_staging_transform_bundle_id", table_name="staging_transform")
    op.drop_index("ix_staging_transform_bundle_member_id", table_name="staging_transform")
    op.drop_table("staging_transform")

    with op.batch_alter_table("artifactclass_policy") as batch_op:
        batch_op.drop_column("staging_config")
