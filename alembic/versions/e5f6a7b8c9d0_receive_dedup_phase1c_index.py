"""Add the phase-1c intake/hash anti-join index.

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-07-11
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "e5f6a7b8c9d0"
down_revision: str | Sequence[str] | None = "d4e5f6a7b8c9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Index intake membership and distinct hash lookup together."""

    op.create_index(
        "ix_ingest_item_intake_hash",
        "ingest_item",
        ["intake_id", "logical_asset_hash"],
    )


def downgrade() -> None:
    """Remove the phase-1c anti-join index."""

    op.drop_index("ix_ingest_item_intake_hash", table_name="ingest_item")
