"""Add indexed component snapshots for parked reconciliation conditions.

Revision ID: c8d2e4f6a1b3
Revises: b7c1d9e3f5a2
Create Date: 2026-07-18
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c8d2e4f6a1b3"
down_revision: str | Sequence[str] | None = "b7c1d9e3f5a2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the attempt-independent component lookup."""

    op.create_table(
        "condition_component",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("condition_id", sa.Integer(), nullable=False),
        sa.Column("component", sa.String(length=2048), nullable=False),
        sa.ForeignKeyConstraint(
            ["condition_id"],
            ["reconciliation_condition.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "condition_id",
            "component",
            name="uq_condition_component_condition_component",
        ),
    )
    op.create_index(
        "ix_condition_component_component",
        "condition_component",
        ["component"],
    )


def downgrade() -> None:
    """Remove the parked-condition component lookup."""

    op.drop_index("ix_condition_component_component", table_name="condition_component")
    op.drop_table("condition_component")
