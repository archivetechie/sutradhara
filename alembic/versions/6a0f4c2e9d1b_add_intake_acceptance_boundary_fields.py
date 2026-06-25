"""add intake acceptance boundary fields

Revision ID: 6a0f4c2e9d1b
Revises: f3c4d5e6a7b8
Create Date: 2026-06-25
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "6a0f4c2e9d1b"
down_revision: str | Sequence[str] | None = "f3c4d5e6a7b8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    with op.batch_alter_table("intake") as batch_op:
        batch_op.add_column(sa.Column("manifest_digest", sa.String(length=64), nullable=True))
        batch_op.add_column(sa.Column("requested_profile", sa.String(length=128), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""

    with op.batch_alter_table("intake") as batch_op:
        batch_op.drop_column("requested_profile")
        batch_op.drop_column("manifest_digest")
