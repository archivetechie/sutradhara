"""add placement tag pin table.

Revision ID: d8a4f14b2c6d
Revises: c2bee2e015ab
Create Date: 2026-06-04 00:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d8a4f14b2c6d"
down_revision: str | Sequence[str] | None = "c2bee2e015ab"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "placement_tag_pin",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("backend_id", sa.Integer(), nullable=False),
        sa.Column("placement_id", sa.String(length=128), nullable=False),
        sa.Column("content_class", sa.String(length=128), nullable=False),
        sa.Column("copy_class", sa.String(length=128), nullable=False),
        sa.Column("pinned_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["backend_id"], ["backend.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "backend_id",
            "placement_id",
            name="uq_placement_tag_pin_backend_placement",
        ),
    )
    op.create_index(
        op.f("ix_placement_tag_pin_backend_id"),
        "placement_tag_pin",
        ["backend_id"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        op.f("ix_placement_tag_pin_backend_id"),
        table_name="placement_tag_pin",
    )
    op.drop_table("placement_tag_pin")
