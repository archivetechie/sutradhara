"""add restore admission inputs

Revision ID: 4e6f8a1c2b3d
Revises: 3c7e4a9b1d2f
Create Date: 2026-07-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "4e6f8a1c2b3d"
down_revision: str | Sequence[str] | None = "3c7e4a9b1d2f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    with op.batch_alter_table("restore_request") as batch:
        batch.add_column(sa.Column("admitted_by", sa.String(length=256), nullable=True))
        batch.add_column(sa.Column("admitted_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("admitted_capabilities", sa.JSON(), nullable=True))

    with op.batch_alter_table("restore_request_item") as batch:
        batch.add_column(sa.Column("admitted_force_suspect", sa.Boolean(), nullable=True))
        batch.add_column(sa.Column("admitted_force_rejected", sa.Boolean(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""

    with op.batch_alter_table("restore_request_item") as batch:
        batch.drop_column("admitted_force_rejected")
        batch.drop_column("admitted_force_suspect")

    with op.batch_alter_table("restore_request") as batch:
        batch.drop_column("admitted_capabilities")
        batch.drop_column("admitted_at")
        batch.drop_column("admitted_by")
