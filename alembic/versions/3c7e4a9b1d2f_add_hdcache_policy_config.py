"""Add hdcache policy config.

Revision ID: 3c7e4a9b1d2f
Revises: 1f2e3d4c5b6a
Create Date: 2026-07-03 14:15:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "3c7e4a9b1d2f"
down_revision: str | Sequence[str] | None = "1f2e3d4c5b6a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "artifactclass_policy",
        sa.Column(
            "hdcache_config",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
    )


def downgrade() -> None:
    op.drop_column("artifactclass_policy", "hdcache_config")
