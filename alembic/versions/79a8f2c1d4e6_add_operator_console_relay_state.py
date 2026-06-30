"""add operator console relay state

Revision ID: 79a8f2c1d4e6
Revises: 5a9d3c7e1b2f
Create Date: 2026-06-30
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "79a8f2c1d4e6"
down_revision: str | Sequence[str] | None = "5a9d3c7e1b2f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    with op.batch_alter_table("grpc_intake") as batch:
        batch.add_column(sa.Column("card_id", sa.String(length=256), nullable=True))

    with op.batch_alter_table("grpc_enroll_token") as batch:
        batch.add_column(
            sa.Column("operator", sa.String(length=128), nullable=False, server_default="")
        )
        batch.add_column(
            sa.Column("device_id", sa.String(length=256), nullable=False, server_default="")
        )
    with op.batch_alter_table("grpc_enroll_token") as batch:
        batch.alter_column("operator", server_default=None)
        batch.alter_column("device_id", server_default=None)


def downgrade() -> None:
    """Downgrade schema."""

    with op.batch_alter_table("grpc_enroll_token") as batch:
        batch.drop_column("device_id")
        batch.drop_column("operator")
    with op.batch_alter_table("grpc_intake") as batch:
        batch.drop_column("card_id")
