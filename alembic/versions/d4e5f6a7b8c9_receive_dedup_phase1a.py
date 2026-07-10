"""Add receive-dedup phase 1a intent and identity fields.

Revision ID: d4e5f6a7b8c9
Revises: c9a0d1e2f3b4
Create Date: 2026-07-10
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d4e5f6a7b8c9"
down_revision: str | Sequence[str] | None = "c9a0d1e2f3b4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Extend the existing idempotency row into the durable receive intent."""

    with op.batch_alter_table("idempotency_record") as batch:
        batch.drop_constraint("ck_idempotency_record_status", type_="check")
        batch.add_column(sa.Column("device_id", sa.String(length=256), nullable=True))
        batch.add_column(sa.Column("card_identity", sa.String(length=128), nullable=True))
        batch.add_column(sa.Column("card_label", sa.String(length=512), nullable=True))
        batch.add_column(sa.Column("duplicate_warning", sa.JSON(), nullable=True))
        batch.add_column(
            sa.Column(
                "duplicate_acknowledged",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
        batch.add_column(sa.Column("lease_source_id", sa.String(length=256), nullable=True))
        batch.add_column(sa.Column("warned_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("authorized_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("started_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("terminal_at", sa.DateTime(timezone=True), nullable=True))
        batch.create_check_constraint(
            "ck_idempotency_record_status",
            "status IN ('in_progress', 'completed', 'warned', 'authorized', 'started', "
            "'committed', 'aborted', 'quarantined', 'failed')",
        )
        batch.create_index(
            "ix_idempotency_record_card_intent",
            ["endpoint", "card_identity", "status"],
        )

    op.execute(
        sa.text(
            "UPDATE idempotency_record SET "
            "device_id = (SELECT grpc_intake.device_id FROM grpc_intake "
            "WHERE grpc_intake.intake_id = idempotency_record.intake_id), "
            "card_identity = (SELECT CASE WHEN length(grpc_intake.card_id) <= 128 "
            "THEN grpc_intake.card_id ELSE NULL END FROM grpc_intake "
            "WHERE grpc_intake.intake_id = idempotency_record.intake_id), "
            "card_label = (SELECT grpc_intake.label FROM grpc_intake "
            "WHERE grpc_intake.intake_id = idempotency_record.intake_id), "
            "lease_source_id = (SELECT CASE WHEN grpc_intake.card_id IS NOT NULL "
            "AND length(grpc_intake.card_id) <= 128 THEN 'card-identity:' || grpc_intake.card_id "
            "ELSE NULL END FROM grpc_intake "
            "WHERE grpc_intake.intake_id = idempotency_record.intake_id), "
            "started_at = (SELECT grpc_intake.created_at FROM grpc_intake "
            "WHERE grpc_intake.intake_id = idempotency_record.intake_id), "
            "status = CASE "
            "WHEN EXISTS (SELECT 1 FROM intake WHERE intake.intake_id = "
            "idempotency_record.intake_id AND intake.status = 'registered') THEN 'committed' "
            "WHEN EXISTS (SELECT 1 FROM intake WHERE intake.intake_id = "
            "idempotency_record.intake_id AND intake.status = 'quarantined') THEN 'quarantined' "
            "WHEN EXISTS (SELECT 1 FROM grpc_intake WHERE grpc_intake.intake_id = "
            "idempotency_record.intake_id AND grpc_intake.state = 'aborted') THEN 'aborted' "
            "ELSE 'started' END, "
            "updated_at = CURRENT_TIMESTAMP, last_heartbeat = CURRENT_TIMESTAMP, "
            "terminal_at = CASE WHEN EXISTS (SELECT 1 FROM intake WHERE intake.intake_id = "
            "idempotency_record.intake_id AND intake.status IN ('registered', 'quarantined')) "
            "OR EXISTS (SELECT 1 FROM grpc_intake WHERE grpc_intake.intake_id = "
            "idempotency_record.intake_id AND grpc_intake.state = 'aborted') "
            "THEN CURRENT_TIMESTAMP ELSE NULL END "
            "WHERE endpoint = 'POST /api/devices/receive' AND intake_id IS NOT NULL "
            "AND EXISTS (SELECT 1 FROM grpc_intake WHERE grpc_intake.intake_id = "
            "idempotency_record.intake_id)"
        )
    )

    with op.batch_alter_table("intake") as batch:
        batch.add_column(sa.Column("card_id", sa.String(length=128), nullable=True))
        batch.add_column(sa.Column("device_id", sa.String(length=256), nullable=True))
        batch.create_index("ix_intake_card_id", ["card_id"])

    op.execute(
        sa.text(
            "UPDATE intake SET "
            "card_id = (SELECT CASE WHEN length(grpc_intake.card_id) <= 128 "
            "THEN grpc_intake.card_id ELSE NULL END FROM grpc_intake "
            "WHERE grpc_intake.intake_id = intake.intake_id), "
            "device_id = (SELECT grpc_intake.device_id FROM grpc_intake "
            "WHERE grpc_intake.intake_id = intake.intake_id) "
            "WHERE EXISTS (SELECT 1 FROM grpc_intake "
            "WHERE grpc_intake.intake_id = intake.intake_id)"
        )
    )

    with op.batch_alter_table("grpc_intake") as batch:
        batch.create_index("ix_grpc_intake_card_id", ["card_id"])


def downgrade() -> None:
    """Remove receive-dedup phase 1a schema additions."""

    op.execute(
        sa.text(
            "UPDATE idempotency_record SET status = CASE "
            "WHEN response_json IS NOT NULL THEN 'completed' ELSE 'in_progress' END "
            "WHERE status NOT IN ('in_progress', 'completed')"
        )
    )

    with op.batch_alter_table("grpc_intake") as batch:
        batch.drop_index("ix_grpc_intake_card_id")

    with op.batch_alter_table("intake") as batch:
        batch.drop_index("ix_intake_card_id")
        batch.drop_column("device_id")
        batch.drop_column("card_id")

    with op.batch_alter_table("idempotency_record") as batch:
        batch.drop_index("ix_idempotency_record_card_intent")
        batch.drop_constraint("ck_idempotency_record_status", type_="check")
        batch.create_check_constraint(
            "ck_idempotency_record_status",
            "status IN ('in_progress', 'completed')",
        )
        batch.drop_column("terminal_at")
        batch.drop_column("started_at")
        batch.drop_column("authorized_at")
        batch.drop_column("warned_at")
        batch.drop_column("lease_source_id")
        batch.drop_column("duplicate_acknowledged")
        batch.drop_column("duplicate_warning")
        batch.drop_column("card_label")
        batch.drop_column("card_identity")
        batch.drop_column("device_id")
