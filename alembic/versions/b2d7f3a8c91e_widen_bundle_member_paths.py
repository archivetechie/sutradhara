"""widen bundle member path columns

Revision ID: b2d7f3a8c91e
Revises: 8f1d2c3b4a9e
Create Date: 2026-06-26
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b2d7f3a8c91e"
down_revision: str | Sequence[str] | None = "8f1d2c3b4a9e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    with op.batch_alter_table("bundle_member") as batch:
        batch.alter_column(
            "member_path",
            existing_type=sa.String(length=1024),
            type_=sa.String(length=2048),
            existing_nullable=False,
        )
    with op.batch_alter_table("asset_locator") as batch:
        batch.alter_column(
            "member_path",
            existing_type=sa.String(length=1024),
            type_=sa.String(length=2048),
            existing_nullable=False,
        )


def downgrade() -> None:
    """Downgrade schema."""

    with op.batch_alter_table("asset_locator") as batch:
        batch.alter_column(
            "member_path",
            existing_type=sa.String(length=2048),
            type_=sa.String(length=1024),
            existing_nullable=False,
        )
    with op.batch_alter_table("bundle_member") as batch:
        batch.alter_column(
            "member_path",
            existing_type=sa.String(length=2048),
            type_=sa.String(length=1024),
            existing_nullable=False,
        )
