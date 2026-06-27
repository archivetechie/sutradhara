"""add retention gate tables and tombstones

Revision ID: d7f1c2a9b3e4
Revises: c4e9b7a2d6f8
Create Date: 2026-06-27
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d7f1c2a9b3e4"
down_revision: str | Sequence[str] | None = "c4e9b7a2d6f8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    op.create_table(
        "offsite_confirmation",
        sa.Column("media_id", sa.String(length=256), nullable=False),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("confirmed_by", sa.String(length=256), nullable=False),
        sa.Column("shipment_id", sa.String(length=256), nullable=True),
        sa.PrimaryKeyConstraint("media_id"),
    )

    with op.batch_alter_table("intake") as batch:
        batch.add_column(
            sa.Column(
                "retention_state",
                sa.String(length=32),
                nullable=False,
                server_default="held",
            )
        )
        batch.add_column(sa.Column("released_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(
            sa.Column("staging_deleted_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch.create_check_constraint(
            "ck_intake_retention_state",
            "retention_state IN ('held', 'released', 'purged')",
        )

    with op.batch_alter_table("copy") as batch:
        batch.add_column(sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True))

    op.create_table(
        "retention_event",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("intake_id", sa.String(length=128), nullable=False),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("actor", sa.String(length=256), nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("detail", sa.JSON(), nullable=True),
        sa.CheckConstraint(
            "action IN ('released', 'cloud_blob_deleted', 'staging_deleted')",
            name="ck_retention_event_action",
        ),
        sa.ForeignKeyConstraint(["intake_id"], ["intake.intake_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_retention_event_intake_id", "retention_event", ["intake_id"])


def downgrade() -> None:
    """Downgrade schema."""

    op.drop_index("ix_retention_event_intake_id", table_name="retention_event")
    op.drop_table("retention_event")

    with op.batch_alter_table("copy") as batch:
        batch.drop_column("deleted_at")

    with op.batch_alter_table("intake") as batch:
        batch.drop_constraint("ck_intake_retention_state", type_="check")
        batch.drop_column("staging_deleted_at")
        batch.drop_column("released_at")
        batch.drop_column("retention_state")

    op.drop_table("offsite_confirmation")
