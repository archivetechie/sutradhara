"""Add retention-journal checkpoint and supersession targets.

Revision ID: f2a3b4c5d6e7
Revises: e1f2a3b4c5d6
Create Date: 2026-07-20
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f2a3b4c5d6e7"
down_revision: str | Sequence[str] | None = "e1f2a3b4c5d6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Install the emit-only retention-journal catalog state."""

    with op.batch_alter_table("retention_event") as batch:
        batch.drop_constraint("ck_retention_event_subject", type_="check")
        batch.add_column(sa.Column("supersedes_source", sa.String(32), nullable=True))
        batch.add_column(sa.Column("supersedes_event_id", sa.Integer(), nullable=True))
        batch.create_check_constraint(
            "ck_retention_event_subject",
            "(subject_type = 'intake' AND intake_id IS NOT NULL AND "
            "intake_id = subject_id AND action IN "
            "('released', 'cloud_blob_deleted', 'staging_deleted', 'release_attempted', "
            "'purge_attempted', 'staging_tombstoned', 'staging_purge_held', 'abandoned', "
            "'correction_recorded')) OR "
            "(subject_type = 'media' AND intake_id IS NULL AND action IN "
            "('offsite_confirmed', 'correction_recorded')) OR "
            "(subject_type = 'batch' AND intake_id IS NULL AND action IN "
            "('batch_invoked', 'batch_refused', 'grace_overridden')) OR "
            "(subject_type = 'receipt' AND intake_id IS NULL AND "
            "action = 'correction_recorded')",
        )
        batch.create_check_constraint(
            "ck_retention_event_supersession",
            "(supersedes_source IS NULL AND supersedes_event_id IS NULL) OR "
            "(supersedes_source IN ('verify_receipt', 'retention_event') AND "
            "supersedes_event_id IS NOT NULL AND action = 'correction_recorded')",
        )

    op.create_table(
        "retention_journal_checkpoint",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("envelope_id", sa.String(128), nullable=False),
        sa.Column("hash_algorithm_id", sa.String(32), nullable=False),
        sa.Column("global_sequence", sa.Integer(), nullable=False),
        sa.Column("head_hash", sa.LargeBinary(32), nullable=False),
        sa.Column("verify_receipt_cursor", sa.Integer(), nullable=False),
        sa.Column("retention_event_cursor", sa.Integer(), nullable=False),
        sa.Column("published_filename", sa.String(512), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("id = 1", name="ck_retention_journal_checkpoint_singleton"),
        sa.CheckConstraint("global_sequence >= 0", name="ck_retention_journal_sequence"),
        sa.CheckConstraint(
            "verify_receipt_cursor >= 0 AND retention_event_cursor >= 0",
            name="ck_retention_journal_cursors",
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Remove journal export state while leaving receipt rows untouched."""

    op.drop_table("retention_journal_checkpoint")
    with op.batch_alter_table("retention_event") as batch:
        batch.drop_constraint("ck_retention_event_supersession", type_="check")
        batch.drop_constraint("ck_retention_event_subject", type_="check")
        batch.create_check_constraint(
            "ck_retention_event_subject",
            "(subject_type = 'intake' AND intake_id IS NOT NULL AND "
            "intake_id = subject_id AND action IN "
            "('released', 'cloud_blob_deleted', 'staging_deleted', 'release_attempted', "
            "'purge_attempted', 'staging_tombstoned', 'staging_purge_held', 'abandoned', "
            "'correction_recorded')) OR "
            "(subject_type = 'media' AND intake_id IS NULL AND action IN "
            "('offsite_confirmed', 'correction_recorded')) OR "
            "(subject_type = 'batch' AND intake_id IS NULL AND action IN "
            "('batch_invoked', 'batch_refused', 'grace_overridden'))",
        )
        batch.drop_column("supersedes_event_id")
        batch.drop_column("supersedes_source")
