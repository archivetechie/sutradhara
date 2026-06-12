"""Add copy storage metadata.

Revision ID: 9b2af1cc0e6a
Revises: d8a4f14b2c6d
Create Date: 2026-06-12
"""

from __future__ import annotations

from typing import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "9b2af1cc0e6a"
down_revision: str | None = "d8a4f14b2c6d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "copy",
        sa.Column(
            "storage_metadata",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
    )


def downgrade() -> None:
    op.drop_column("copy", "storage_metadata")
