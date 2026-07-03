"""add hdcache repopulation tracking columns

Revision ID: 9f4a6b2c1d8e
Revises: 5d3a9e7b8c4f
Create Date: 2026-07-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "9f4a6b2c1d8e"
down_revision: str | Sequence[str] | None = "5d3a9e7b8c4f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    with op.batch_alter_table("cache_entry") as batch_op:
        batch_op.add_column(sa.Column("lost_origin_disk_id", sa.String(length=16), nullable=True))
        batch_op.add_column(sa.Column("lost_drill_id", sa.String(length=128), nullable=True))
        batch_op.add_column(sa.Column("lost_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("refilled_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""

    with op.batch_alter_table("cache_entry") as batch_op:
        batch_op.drop_column("refilled_at")
        batch_op.drop_column("lost_at")
        batch_op.drop_column("lost_drill_id")
        batch_op.drop_column("lost_origin_disk_id")
