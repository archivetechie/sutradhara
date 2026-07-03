"""add restore admission proof

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

    with op.batch_alter_table("restore_request_item") as batch:
        batch.add_column(sa.Column("admission_proof", sa.JSON(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""

    with op.batch_alter_table("restore_request_item") as batch:
        batch.drop_column("admission_proof")
