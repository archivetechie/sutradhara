"""add api idempotency and source claim tables

Revision ID: e9f0a1b2c3d4
Revises: d7f1c2a9b3e4
Create Date: 2026-06-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e9f0a1b2c3d4"
down_revision: str | Sequence[str] | None = "d7f1c2a9b3e4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    op.create_table(
        "idempotency_record",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("operator_username", sa.String(length=256), nullable=False),
        sa.Column("endpoint", sa.String(length=128), nullable=False),
        sa.Column("idempotency_key", sa.String(length=64), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("intake_id", sa.String(length=128), nullable=True),
        sa.Column("response_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_heartbeat", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('in_progress', 'completed')",
            name="ck_idempotency_record_status",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "operator_username",
            "endpoint",
            "idempotency_key",
            name="uq_idempotency_scope",
        ),
    )
    op.create_index(
        "ix_idempotency_record_last_heartbeat",
        "idempotency_record",
        ["last_heartbeat"],
    )

    op.create_table(
        "source_claim",
        sa.Column("source_id", sa.String(length=256), nullable=False),
        sa.Column("operator_username", sa.String(length=256), nullable=False),
        sa.Column("idempotency_key", sa.String(length=64), nullable=False),
        sa.Column("intake_id", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_heartbeat", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("source_id"),
    )
    op.create_index("ix_source_claim_last_heartbeat", "source_claim", ["last_heartbeat"])


def downgrade() -> None:
    """Downgrade schema."""

    op.drop_index("ix_source_claim_last_heartbeat", table_name="source_claim")
    op.drop_table("source_claim")
    op.drop_index("ix_idempotency_record_last_heartbeat", table_name="idempotency_record")
    op.drop_table("idempotency_record")
