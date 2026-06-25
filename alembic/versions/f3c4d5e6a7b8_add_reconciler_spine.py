"""add reconciler spine

Revision ID: f3c4d5e6a7b8
Revises: af859d4ffb71
Create Date: 2026-06-25
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f3c4d5e6a7b8"
down_revision: str | Sequence[str] | None = "af859d4ffb71"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

LIVE_JOB_PREDICATE = "status IN ('pending', 'running', 'queued')"


def upgrade() -> None:
    """Upgrade schema."""

    with op.batch_alter_table("job") as batch_op:
        batch_op.add_column(sa.Column("recon_domain", sa.String(length=64), nullable=True))
        batch_op.add_column(sa.Column("recon_target_key", sa.String(length=256), nullable=True))

    op.create_index(
        "ix_job_recon_live",
        "job",
        ["recon_domain", "recon_target_key"],
        unique=False,
        sqlite_where=sa.text(LIVE_JOB_PREDICATE),
        postgresql_where=sa.text(LIVE_JOB_PREDICATE),
    )

    op.create_table(
        "reconciliation_condition",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("domain", sa.String(length=64), nullable=False),
        sa.Column("target_key", sa.String(length=256), nullable=False),
        sa.Column("observed_state", sa.String(length=64), nullable=False),
        sa.Column("condition", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.String(length=128), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("attempt_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("next_eligible_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("blocked_tool_name", sa.String(length=128), nullable=True),
        sa.Column("blocked_tool_version", sa.String(length=128), nullable=True),
        sa.Column("last_attempt_id", sa.Integer(), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["last_attempt_id"], ["job_attempt.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("domain", "target_key", name="uq_recon_condition_domain_target"),
    )
    op.create_index(
        "ix_condition_work",
        "reconciliation_condition",
        ["domain", "condition", "next_eligible_at"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""

    op.drop_index("ix_condition_work", table_name="reconciliation_condition")
    op.drop_table("reconciliation_condition")
    op.drop_index("ix_job_recon_live", table_name="job")
    with op.batch_alter_table("job") as batch_op:
        batch_op.drop_column("recon_target_key")
        batch_op.drop_column("recon_domain")
