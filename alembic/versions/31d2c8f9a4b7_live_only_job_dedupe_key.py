"""Make job dedupe keys unique only for live work.

Revision ID: 31d2c8f9a4b7
Revises: 7e14d9af2c31
Create Date: 2026-06-17
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "31d2c8f9a4b7"
down_revision: str | Sequence[str] | None = "7e14d9af2c31"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

LIVE_DEDUPE_PREDICATE = "status IN ('pending', 'running', 'queued')"


def upgrade() -> None:
    with op.batch_alter_table("job") as batch_op:
        batch_op.drop_constraint("uq_job_dedupe_key", type_="unique")

    op.create_index(
        "uq_job_dedupe_key_live",
        "job",
        ["dedupe_key"],
        unique=True,
        sqlite_where=sa.text(LIVE_DEDUPE_PREDICATE),
        postgresql_where=sa.text(LIVE_DEDUPE_PREDICATE),
    )


def downgrade() -> None:
    op.drop_index("uq_job_dedupe_key_live", table_name="job")
    with op.batch_alter_table("job") as batch_op:
        batch_op.create_unique_constraint("uq_job_dedupe_key", ["dedupe_key"])
