"""Add deletion evidence, retention reservations, and recording tables.

Revision ID: e1f2a3b4c5d6
Revises: c8d2e4f6a1b3
Create Date: 2026-07-19
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e1f2a3b4c5d6"
down_revision: str | Sequence[str] | None = "c8d2e4f6a1b3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INTAKE_ACTIONS = (
    "released",
    "cloud_blob_deleted",
    "staging_deleted",
    "release_attempted",
    "purge_attempted",
    "staging_tombstoned",
    "staging_purge_held",
    "abandoned",
    "correction_recorded",
)
_ONCE_ACTIONS = (
    "release_attempted",
    "cloud_blob_deleted",
    "released",
    "purge_attempted",
    "staging_tombstoned",
    "staging_deleted",
)
_FK_NAMING = {
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
}


def upgrade() -> None:
    """Install the authoritative evidence projection and audit recording schema."""

    with op.batch_alter_table("intake") as batch:
        batch.drop_constraint("ck_intake_retention_state", type_="check")
        batch.add_column(sa.Column("release_policy_fingerprint", sa.String(67), nullable=True))
        batch.add_column(sa.Column("staging_tombstoned_at", sa.DateTime(timezone=True)))
        batch.add_column(sa.Column("staging_tombstone_path", sa.String(2048)))
        batch.create_check_constraint(
            "ck_intake_retention_state",
            "retention_state IN ('held', 'released', 'tombstoned', 'abandoned', 'purged')",
        )
        batch.create_check_constraint(
            "ck_intake_tombstone_pair",
            "(staging_tombstoned_at IS NULL AND staging_tombstone_path IS NULL) OR "
            "(staging_tombstoned_at IS NOT NULL AND staging_tombstone_path IS NOT NULL)",
        )
        batch.create_check_constraint(
            "ck_intake_tombstoned_state",
            "retention_state != 'tombstoned' OR "
            "(staging_tombstoned_at IS NOT NULL AND staging_tombstone_path IS NOT NULL)",
        )
        batch.create_check_constraint(
            "ck_intake_purged_state",
            "retention_state != 'purged' OR staging_deleted_at IS NOT NULL",
        )

    with op.batch_alter_table("offsite_confirmation") as batch:
        batch.add_column(sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("revoked_by", sa.String(256), nullable=True))
        batch.create_check_constraint(
            "ck_offsite_confirmation_revocation_pair",
            "(revoked_at IS NULL AND revoked_by IS NULL) OR "
            "(revoked_at IS NOT NULL AND revoked_by IS NOT NULL AND length(revoked_by) > 0)",
        )

    op.execute("DROP TRIGGER IF EXISTS trg_copy_health_changed_at")
    with op.batch_alter_table("copy") as batch:
        batch.alter_column(
            "last_verified_at",
            existing_type=sa.DateTime(timezone=True),
            new_column_name="last_checked_at",
        )
        batch.add_column(
            sa.Column(
                "integrity_hash_provenance",
                sa.String(32),
                nullable=False,
                server_default=sa.text("'locally_computed'"),
            )
        )
        batch.add_column(
            sa.Column(
                "health_changed_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("CURRENT_TIMESTAMP"),
            )
        )
        batch.add_column(sa.Column("last_measured_digest", sa.LargeBinary(32), nullable=True))
        batch.add_column(sa.Column("last_measured_at", sa.DateTime(timezone=True), nullable=True))
        batch.create_check_constraint(
            "ck_copy_health",
            "health IN ('ok', 'suspect', 'corrupt', 'missing')",
        )
        batch.create_check_constraint(
            "ck_copy_measurement_pair",
            "(last_measured_digest IS NULL AND last_measured_at IS NULL) OR "
            "(last_measured_digest IS NOT NULL AND last_measured_at IS NOT NULL)",
        )
        batch.create_check_constraint(
            "ck_copy_integrity_hash_provenance",
            "integrity_hash_provenance IN ('locally_computed', 'backend_discovered')",
        )
    _create_health_trigger()

    with op.batch_alter_table("retention_event") as batch:
        batch.alter_column(
            "id",
            existing_type=sa.Integer(),
            new_column_name="event_id",
        )
        batch.add_column(sa.Column("subject_type", sa.String(16), nullable=True))
        batch.add_column(sa.Column("subject_id", sa.String(256), nullable=True))
        batch.add_column(sa.Column("operation_id", sa.String(512), nullable=True))
    op.execute(
        "UPDATE retention_event SET subject_type = 'intake', subject_id = intake_id, "
        "operation_id = action || ':' || intake_id || ':legacy:' || event_id"
    )
    with op.batch_alter_table(
        "retention_event",
        naming_convention=_FK_NAMING,
    ) as batch:
        batch.drop_constraint("ck_retention_event_action", type_="check")
        batch.drop_constraint("fk_retention_event_intake_id_intake", type_="foreignkey")
        batch.alter_column("intake_id", existing_type=sa.String(128), nullable=True)
        batch.alter_column("subject_type", existing_type=sa.String(16), nullable=False)
        batch.alter_column("subject_id", existing_type=sa.String(256), nullable=False)
        batch.create_foreign_key(
            "fk_retention_event_intake_id_intake",
            "intake",
            ["intake_id"],
            ["intake_id"],
            ondelete="RESTRICT",
        )
        batch.create_check_constraint(
            "ck_retention_event_action",
            "action IN ('released', 'cloud_blob_deleted', 'staging_deleted', "
            "'release_attempted', 'purge_attempted', 'staging_tombstoned', "
            "'staging_purge_held', 'batch_invoked', 'batch_refused', "
            "'grace_overridden', 'abandoned', 'correction_recorded', 'offsite_confirmed')",
        )
        batch.create_check_constraint(
            "ck_retention_event_subject",
            "(subject_type = 'intake' AND intake_id = subject_id AND action IN "
            "('released', 'cloud_blob_deleted', 'staging_deleted', 'release_attempted', "
            "'purge_attempted', 'staging_tombstoned', 'staging_purge_held', 'abandoned', "
            "'correction_recorded')) OR "
            "(subject_type = 'media' AND intake_id IS NULL AND action IN "
            "('offsite_confirmed', 'correction_recorded')) OR "
            "(subject_type = 'batch' AND intake_id IS NULL AND action IN "
            "('batch_invoked', 'batch_refused', 'grace_overridden'))",
        )
    op.create_index(
        "uq_retention_event_action_operation_once",
        "retention_event",
        ["action", "operation_id"],
        unique=True,
        sqlite_where=sa.text(
            "action IN ('release_attempted', 'cloud_blob_deleted', 'released', "
            "'purge_attempted', 'staging_tombstoned', 'staging_deleted')"
        ),
        postgresql_where=sa.text(
            "action IN ('release_attempted', 'cloud_blob_deleted', 'released', "
            "'purge_attempted', 'staging_tombstoned', 'staging_deleted')"
        ),
    )

    op.create_table(
        "verify_receipt",
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("copy_id", sa.Integer(), nullable=False),
        sa.Column("backend_id", sa.Integer(), nullable=False),
        sa.Column("expected_digest", sa.LargeBinary(32), nullable=False),
        sa.Column("measured_digest", sa.LargeBinary(32), nullable=True),
        sa.Column("backend_ok", sa.Boolean(), nullable=False),
        sa.Column("failure_kind", sa.String(64), nullable=True),
        sa.Column("failure_detail", sa.Text(), nullable=True),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("execution_id", sa.String(512), nullable=False),
        sa.Column("producer_process", sa.String(512), nullable=False),
        sa.Column("actor", sa.String(256), nullable=True),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "source IN ('fanout', 'verify-job', 'restore', 'scrub')",
            name="ck_verify_receipt_source",
        ),
        sa.CheckConstraint(
            "source != 'scrub' OR measured_digest IS NULL",
            name="ck_verify_receipt_scrub_unmeasured",
        ),
        sa.ForeignKeyConstraint(["copy_id"], ["copy.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["backend_id"], ["backend.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("event_id"),
        sa.UniqueConstraint(
            "source",
            "execution_id",
            "copy_id",
            name="uq_verify_receipt_execution_copy",
        ),
    )
    op.create_index("ix_verify_receipt_copy_id", "verify_receipt", ["copy_id"])


def downgrade() -> None:
    """Remove the deletion-evidence recording schema."""

    op.drop_index("ix_verify_receipt_copy_id", table_name="verify_receipt")
    op.drop_table("verify_receipt")

    op.drop_index(
        "uq_retention_event_action_operation_once",
        table_name="retention_event",
    )
    with op.batch_alter_table(
        "retention_event",
        naming_convention=_FK_NAMING,
    ) as batch:
        batch.drop_constraint("ck_retention_event_subject", type_="check")
        batch.drop_constraint("ck_retention_event_action", type_="check")
        batch.drop_constraint("fk_retention_event_intake_id_intake", type_="foreignkey")
        batch.create_foreign_key(
            "fk_retention_event_intake_id_intake",
            "intake",
            ["intake_id"],
            ["intake_id"],
            ondelete="CASCADE",
        )
        batch.alter_column("intake_id", existing_type=sa.String(128), nullable=False)
        batch.create_check_constraint(
            "ck_retention_event_action",
            "action IN ('released', 'cloud_blob_deleted', 'staging_deleted')",
        )
        batch.drop_column("operation_id")
        batch.drop_column("subject_id")
        batch.drop_column("subject_type")
        batch.alter_column(
            "event_id",
            existing_type=sa.Integer(),
            new_column_name="id",
        )

    op.execute("DROP TRIGGER IF EXISTS trg_copy_health_changed_at")
    with op.batch_alter_table("copy") as batch:
        batch.drop_constraint("ck_copy_integrity_hash_provenance", type_="check")
        batch.drop_constraint("ck_copy_measurement_pair", type_="check")
        batch.drop_constraint("ck_copy_health", type_="check")
        batch.drop_column("last_measured_at")
        batch.drop_column("last_measured_digest")
        batch.drop_column("health_changed_at")
        batch.drop_column("integrity_hash_provenance")
        batch.alter_column(
            "last_checked_at",
            existing_type=sa.DateTime(timezone=True),
            new_column_name="last_verified_at",
        )

    with op.batch_alter_table("offsite_confirmation") as batch:
        batch.drop_constraint("ck_offsite_confirmation_revocation_pair", type_="check")
        batch.drop_column("revoked_by")
        batch.drop_column("revoked_at")

    with op.batch_alter_table("intake") as batch:
        batch.drop_constraint("ck_intake_purged_state", type_="check")
        batch.drop_constraint("ck_intake_tombstoned_state", type_="check")
        batch.drop_constraint("ck_intake_tombstone_pair", type_="check")
        batch.drop_constraint("ck_intake_retention_state", type_="check")
        batch.create_check_constraint(
            "ck_intake_retention_state",
            "retention_state IN ('held', 'released', 'purged')",
        )
        batch.drop_column("staging_tombstone_path")
        batch.drop_column("staging_tombstoned_at")
        batch.drop_column("release_policy_fingerprint")


def _create_health_trigger() -> None:
    op.execute(
        "CREATE TRIGGER trg_copy_health_changed_at "
        "AFTER UPDATE OF health ON copy "
        "FOR EACH ROW WHEN NEW.health IS NOT OLD.health "
        "BEGIN UPDATE copy SET health_changed_at = CURRENT_TIMESTAMP WHERE id = NEW.id; END"
    )
