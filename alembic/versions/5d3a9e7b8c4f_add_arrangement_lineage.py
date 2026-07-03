"""add arrangement clone lineage

Revision ID: 5d3a9e7b8c4f
Revises: 4e6f8a1c2b3d
Create Date: 2026-07-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "5d3a9e7b8c4f"
down_revision: str | Sequence[str] | None = "4e6f8a1c2b3d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    with op.batch_alter_table("arrangement") as batch:
        batch.add_column(sa.Column("cloned_from_arrangement_id", sa.Integer(), nullable=True))
        batch.create_index(
            "ix_arrangement_cloned_from_arrangement_id",
            ["cloned_from_arrangement_id"],
        )
        batch.create_foreign_key(
            "fk_arrangement_cloned_from_arrangement_id",
            "arrangement",
            ["cloned_from_arrangement_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    """Downgrade schema."""

    with op.batch_alter_table("arrangement") as batch:
        batch.drop_constraint(
            "fk_arrangement_cloned_from_arrangement_id",
            type_="foreignkey",
        )
        batch.drop_index("ix_arrangement_cloned_from_arrangement_id")
        batch.drop_column("cloned_from_arrangement_id")
