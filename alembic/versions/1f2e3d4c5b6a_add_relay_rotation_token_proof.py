"""add relay rotation token proof

Revision ID: 1f2e3d4c5b6a
Revises: 0d9c8b7a6e5f
Create Date: 2026-07-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "1f2e3d4c5b6a"
down_revision: str | Sequence[str] | None = "0d9c8b7a6e5f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    with op.batch_alter_table("grpc_enroll_token") as batch:
        batch.add_column(sa.Column("rotation_authority", sa.String(length=32), nullable=True))
        batch.add_column(sa.Column("rotation_fingerprint", sa.String(length=95), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""

    with op.batch_alter_table("grpc_enroll_token") as batch:
        batch.drop_column("rotation_fingerprint")
        batch.drop_column("rotation_authority")
