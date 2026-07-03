"""add restore progress and hdcache capacity state

Revision ID: 7c2d4e9f0a1b
Revises: 9f4a6b2c1d8e
Create Date: 2026-07-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "7c2d4e9f0a1b"
down_revision: str | Sequence[str] | None = "9f4a6b2c1d8e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    with op.batch_alter_table("cache_disk") as batch:
        batch.add_column(
            sa.Column("capacity_state", sa.String(length=32), nullable=False, server_default="ok")
        )
        batch.create_check_constraint(
            "ck_cache_disk_capacity_state",
            "capacity_state IN ('ok', 'over_reserve')",
        )
        batch.create_index("ix_cache_disk_capacity_state", ["capacity_state"])

    with op.batch_alter_table("restore_request") as batch:
        batch.add_column(sa.Column("idempotency_key", sa.String(length=128), nullable=True))
        batch.add_column(sa.Column("idempotency_body_hash", sa.String(length=64), nullable=True))
        batch.create_unique_constraint(
            "uq_restore_request_idempotency_key",
            ["idempotency_key"],
        )

    with op.batch_alter_table("restore_request_item") as batch:
        batch.add_column(sa.Column("denial_kind", sa.String(length=32), nullable=True))
        batch.add_column(sa.Column("size_bytes", sa.BigInteger(), nullable=True))
        batch.add_column(
            sa.Column("bytes_restored", sa.BigInteger(), nullable=False, server_default="0")
        )
        batch.add_column(sa.Column("source", sa.String(length=16), nullable=True))
        batch.create_check_constraint(
            "ck_restore_request_item_denial_kind",
            (
                "denial_kind IS NULL OR denial_kind IN "
                "('capability', 'privacy_unmapped', 'suspect', 'rejected')"
            ),
        )
        batch.create_check_constraint(
            "ck_restore_request_item_source",
            "source IS NULL OR source IN ('cache', 'tape')",
        )


def downgrade() -> None:
    """Downgrade schema."""

    with op.batch_alter_table("restore_request_item") as batch:
        batch.drop_constraint("ck_restore_request_item_source", type_="check")
        batch.drop_constraint("ck_restore_request_item_denial_kind", type_="check")
        batch.drop_column("source")
        batch.drop_column("bytes_restored")
        batch.drop_column("size_bytes")
        batch.drop_column("denial_kind")

    with op.batch_alter_table("restore_request") as batch:
        batch.drop_constraint("uq_restore_request_idempotency_key", type_="unique")
        batch.drop_column("idempotency_body_hash")
        batch.drop_column("idempotency_key")

    with op.batch_alter_table("cache_disk") as batch:
        batch.drop_index("ix_cache_disk_capacity_state")
        batch.drop_constraint("ck_cache_disk_capacity_state", type_="check")
        batch.drop_column("capacity_state")
