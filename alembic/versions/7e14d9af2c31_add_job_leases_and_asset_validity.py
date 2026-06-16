"""Add job lease scheduling fields and asset validity.

Revision ID: 7e14d9af2c31
Revises: 4db9c2f8a6d1
Create Date: 2026-06-16
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "7e14d9af2c31"
down_revision: str | Sequence[str] | None = "4db9c2f8a6d1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("job") as batch_op:
        batch_op.add_column(sa.Column("not_before", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("priority", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("dedupe_key", sa.String(length=256), nullable=True))

    op.execute("UPDATE job SET not_before = created_at WHERE not_before IS NULL")

    with op.batch_alter_table("job") as batch_op:
        batch_op.alter_column("not_before", nullable=False)
        batch_op.alter_column("priority", server_default=None)
        batch_op.create_index("ix_job_not_before", ["not_before"], unique=False)
        batch_op.create_unique_constraint("uq_job_dedupe_key", ["dedupe_key"])

    with op.batch_alter_table("logical_asset") as batch_op:
        batch_op.add_column(
            sa.Column(
                "validity",
                sa.String(length=32),
                nullable=False,
                server_default="unvalidated",
            )
        )
        batch_op.add_column(sa.Column("validity_note", sa.Text(), nullable=True))
        batch_op.alter_column("validity", server_default=None)
        batch_op.create_check_constraint(
            "ck_logical_asset_validity",
            "validity IN ('ok', 'suspect', 'unvalidated')",
        )


def downgrade() -> None:
    with op.batch_alter_table("logical_asset") as batch_op:
        batch_op.drop_constraint("ck_logical_asset_validity", type_="check")
        batch_op.drop_column("validity_note")
        batch_op.drop_column("validity")

    with op.batch_alter_table("job") as batch_op:
        batch_op.drop_constraint("uq_job_dedupe_key", type_="unique")
        batch_op.drop_index("ix_job_not_before")
        batch_op.drop_column("dedupe_key")
        batch_op.drop_column("priority")
        batch_op.drop_column("not_before")
