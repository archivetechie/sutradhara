"""Add immutable receive content-novelty evidence.

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-07-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f6a7b8c9d0e1"
down_revision: str | Sequence[str] | None = "e5f6a7b8c9d0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Persist per-occurrence novelty and its evaluated-at durability evidence."""

    with op.batch_alter_table("ingest_item") as batch:
        batch.add_column(
            sa.Column(
                "disposition",
                sa.String(length=32),
                nullable=False,
                server_default="legacy_unknown",
            )
        )
        batch.add_column(sa.Column("disposition_evaluated_at", sa.DateTime(timezone=True)))
        batch.add_column(sa.Column("disposition_policy_generation", sa.String(length=128)))
        batch.add_column(sa.Column("disposition_evidence", sa.JSON()))
        batch.add_column(sa.Column("prior_intake_id", sa.String(length=128)))
        batch.create_check_constraint(
            "ck_ingest_item_disposition",
            "disposition IN ('new', 'known_durable', 'known_under_durable', "
            "'reverified', 'legacy_unknown')",
        )
        batch.create_foreign_key(
            "fk_ingest_item_prior_intake_id_intake",
            "intake",
            ["prior_intake_id"],
            ["intake_id"],
        )
        batch.create_index("ix_ingest_item_prior_intake_id", ["prior_intake_id"])


def downgrade() -> None:
    """Remove phase-2 novelty evidence."""

    with op.batch_alter_table("ingest_item") as batch:
        batch.drop_index("ix_ingest_item_prior_intake_id")
        batch.drop_constraint("fk_ingest_item_prior_intake_id_intake", type_="foreignkey")
        batch.drop_constraint("ck_ingest_item_disposition", type_="check")
        batch.drop_column("prior_intake_id")
        batch.drop_column("disposition_evidence")
        batch.drop_column("disposition_policy_generation")
        batch.drop_column("disposition_evaluated_at")
        batch.drop_column("disposition")
