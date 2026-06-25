"""add job attempt log

Revision ID: af859d4ffb71
Revises: a1b2c3d4e5f6
Create Date: 2026-06-25 12:33:07.112476

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "af859d4ffb71"
down_revision: str | Sequence[str] | None = "a1b2c3d4e5f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "job_attempt",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=True),
        sa.Column("job_kind", sa.String(length=64), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("outcome", sa.String(length=16), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("granted_leases", sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("worker_id", sa.String(length=256), nullable=True),
        sa.Column("code_version", sa.String(length=128), nullable=True),
        sa.Column("detail", sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["job.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_job_attempt_job_id"), "job_attempt", ["job_id"], unique=False)
    op.create_index(op.f("ix_job_attempt_job_kind"), "job_attempt", ["job_kind"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_job_attempt_job_kind"), table_name="job_attempt")
    op.drop_index(op.f("ix_job_attempt_job_id"), table_name="job_attempt")
    op.drop_table("job_attempt")
