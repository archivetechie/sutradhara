"""Append-only bundle copy write attempts/resolutions and job claim identity.

Revision ID: f8b2d4e6a0c1
Revises: e7a1c3b5d9f2
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f8b2d4e6a0c1"
down_revision: str | Sequence[str] | None = "e7a1c3b5d9f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _alter_bundle(*, downgrade: bool = False) -> None:
    """Preserve referencing rows while SQLite rebuilds the bundle's constraints."""
    bind = op.get_bind()
    context = op.get_context()
    with context.autocommit_block():
        sqlite = bind.dialect.name == "sqlite"
        was_on = bool(bind.exec_driver_sql("PRAGMA foreign_keys").scalar()) if sqlite else False
        if sqlite:
            bind.exec_driver_sql("PRAGMA foreign_keys=OFF")
        try:
            with op.batch_alter_table("bundle") as batch:
                if downgrade:
                    batch.drop_constraint("ck_bundle_claim_job_pair", type_="check")
                    batch.drop_constraint("fk_bundle_claim_job_id", type_="foreignkey")
                    batch.drop_column("claim_job_attempt")
                    batch.drop_column("claim_job_id")
                else:
                    batch.add_column(sa.Column("claim_job_id", sa.Integer(), nullable=True))
                    batch.add_column(sa.Column("claim_job_attempt", sa.Integer(), nullable=True))
                    batch.create_foreign_key(
                        "fk_bundle_claim_job_id",
                        "job",
                        ["claim_job_id"],
                        ["id"],
                        ondelete="RESTRICT",
                    )
                    batch.create_check_constraint(
                        "ck_bundle_claim_job_pair",
                        "(claim_job_id IS NULL AND claim_job_attempt IS NULL) OR "
                        "(claim_job_id IS NOT NULL AND claim_job_attempt IS NOT NULL)",
                    )
            if sqlite and bind.exec_driver_sql("PRAGMA foreign_key_check").fetchall():
                raise RuntimeError("bundle claim migration left foreign key violations")
        finally:
            if was_on:
                bind.exec_driver_sql("PRAGMA foreign_keys=ON")


def upgrade() -> None:
    _alter_bundle()
    op.create_table(
        "bundle_copy_write_attempt",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "bundle_id",
            sa.String(128),
            sa.ForeignKey("bundle.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "pool_id", sa.String(128), sa.ForeignKey("pool.id", ondelete="RESTRICT"), nullable=False
        ),
        sa.Column(
            "backend_id",
            sa.Integer(),
            sa.ForeignKey("backend.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("object_name", sa.Text(), nullable=False),
        sa.Column("digest_algorithm", sa.Text(), nullable=False),
        sa.Column("digest", sa.LargeBinary(32), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("recipient_epochs", sa.JSON(), nullable=True),
        sa.Column("member_layout", sa.JSON(), nullable=False),
        sa.Column(
            "job_id", sa.Integer(), sa.ForeignKey("job.id", ondelete="RESTRICT"), nullable=True
        ),
        sa.Column("job_attempt", sa.Integer(), nullable=True),
        sa.Column("issued_by", sa.Text(), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("generation >= 0", name="ck_bundle_copy_write_attempt_generation"),
        sa.CheckConstraint(
            "digest_algorithm = 'sha256'", name="ck_bundle_copy_write_attempt_algorithm"
        ),
        sa.CheckConstraint("size_bytes >= 0", name="ck_bundle_copy_write_attempt_size"),
        sa.CheckConstraint(
            "(job_id IS NULL AND job_attempt IS NULL) OR "
            "(job_id IS NOT NULL AND job_attempt IS NOT NULL)",
            name="ck_bundle_copy_write_attempt_job_pair",
        ),
    )
    op.create_index(
        "ix_bundle_copy_write_attempt_identity",
        "bundle_copy_write_attempt",
        ["bundle_id", "pool_id", "generation"],
    )
    op.create_table(
        "bundle_copy_write_resolution",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "attempt_id",
            sa.Integer(),
            sa.ForeignKey("bundle_copy_write_attempt.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("learned_by", sa.Text(), nullable=False),
        sa.Column(
            "copy_id", sa.Integer(), sa.ForeignKey("copy.id", ondelete="RESTRICT"), nullable=True
        ),
        sa.Column("observed_hash", sa.LargeBinary(32), nullable=True),
        sa.Column(
            "job_id", sa.Integer(), sa.ForeignKey("job.id", ondelete="RESTRICT"), nullable=True
        ),
        sa.Column("job_attempt", sa.Integer(), nullable=True),
        sa.Column("resolved_by", sa.Text(), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "outcome IN ('landed', 'not-landed', 'conflict')",
            name="ck_bundle_copy_write_resolution_outcome",
        ),
        sa.CheckConstraint(
            "learned_by IN ('write-response', 'lookup')",
            name="ck_bundle_copy_write_resolution_learned_by",
        ),
        sa.CheckConstraint(
            "(outcome = 'landed' AND copy_id IS NOT NULL) OR "
            "(outcome != 'landed' AND copy_id IS NULL)",
            name="ck_bundle_copy_write_resolution_copy",
        ),
        sa.CheckConstraint(
            "outcome != 'conflict' OR observed_hash IS NOT NULL",
            name="ck_bundle_copy_write_resolution_conflict_hash",
        ),
        sa.CheckConstraint(
            "(job_id IS NULL AND job_attempt IS NULL) OR "
            "(job_id IS NOT NULL AND job_attempt IS NOT NULL)",
            name="ck_bundle_copy_write_resolution_job_pair",
        ),
    )
    op.create_index(
        "ix_bundle_copy_write_resolution_attempt", "bundle_copy_write_resolution", ["attempt_id"]
    )
    if op.get_bind().dialect.name == "sqlite":
        for table in ("bundle_copy_write_attempt", "bundle_copy_write_resolution"):
            for operation in ("UPDATE", "DELETE"):
                op.execute(
                    f"CREATE TRIGGER trg_{table}_no_{operation.lower()} "
                    f"BEFORE {operation} ON {table} "
                    "BEGIN SELECT RAISE(ABORT, 'bundle copy write ledger is append-only'); END"
                )
        op.execute(
            "CREATE TRIGGER trg_bundle_copy_write_resolution_transition "
            "BEFORE INSERT ON bundle_copy_write_resolution "
            "WHEN (SELECT outcome FROM bundle_copy_write_resolution "
            "WHERE attempt_id = NEW.attempt_id ORDER BY id DESC LIMIT 1) IN ('landed', 'conflict') "
            "OR ((SELECT outcome FROM bundle_copy_write_resolution "
            "WHERE attempt_id = NEW.attempt_id ORDER BY id DESC LIMIT 1) = 'not-landed' "
            "AND NOT (NEW.outcome = 'landed' AND NEW.learned_by = 'lookup')) "
            "BEGIN SELECT RAISE(ABORT, 'invalid bundle copy write resolution transition'); END"
        )


def downgrade() -> None:
    op.drop_table("bundle_copy_write_resolution")
    op.drop_table("bundle_copy_write_attempt")
    _alter_bundle(downgrade=True)
