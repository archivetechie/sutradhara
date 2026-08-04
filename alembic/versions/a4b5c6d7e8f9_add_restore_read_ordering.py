"""Add restore read-ordering slots and the ordering-outcome ledger.

Revision ID: a4b5c6d7e8f9
Revises: f2a3b4c5d6e7
Create Date: 2026-08-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a4b5c6d7e8f9"
down_revision: str | Sequence[str] | None = "f2a3b4c5d6e7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OUTCOME_STATUSES = (
    "ok",
    "degraded_ascending_fallback",
    "unavailable_unknown_block_size",
    "unavailable_compression_enabled",
    "unavailable_unknown_compression",
    "unavailable_unsupported_format",
    "unavailable_unknown_format",
    "unavailable_unknown_extent",
    "unavailable_uncalibrated",
    "unavailable_map_stale",
    "unknown_plan_status",
    "unknown_cost_model_basis",
    "rpc_unimplemented",
    "rpc_invalid_argument",
    "rpc_transport_error",
    "tag_collision",
    "no_spanned_targets",
    "read_failure_unordered",
    "planning_error",
)


def upgrade() -> None:
    """Create the release-slot whiteboard and the ordering-outcome ledger."""

    op.create_table(
        "restore_read_plan_slot",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "request_id",
            sa.String(128),
            sa.ForeignKey("restore_request.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("tape_uuid", sa.LargeBinary(16), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column(
            "item_id",
            sa.Integer(),
            sa.ForeignKey("restore_request_item.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("planned", sa.Boolean(), nullable=False),
        sa.Column("tag", sa.LargeBinary(64), nullable=True),
        sa.Column("start_block", sa.BigInteger(), nullable=True),
        sa.Column("end_block", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "(planned AND tag IS NOT NULL AND start_block IS NOT NULL "
            "AND end_block IS NOT NULL) OR "
            "(NOT planned AND tag IS NULL AND start_block IS NULL "
            "AND end_block IS NULL)",
            name="ck_restore_read_plan_slot_planned_shape",
        ),
        sa.UniqueConstraint(
            "request_id",
            "tape_uuid",
            "position",
            name="uq_restore_read_plan_slot_position",
        ),
        sa.UniqueConstraint("item_id", name="uq_restore_read_plan_slot_item"),
        sa.UniqueConstraint("request_id", "tag", name="uq_restore_read_plan_slot_tag"),
    )
    op.create_index(
        "ix_restore_read_plan_slot_request_volume",
        "restore_read_plan_slot",
        ["request_id", "tape_uuid"],
    )

    op.create_table(
        "restore_ordering_outcome",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "request_id",
            sa.String(128),
            sa.ForeignKey("restore_request.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("tape_uuid", sa.LargeBinary(16), nullable=False),
        sa.Column("phase", sa.String(32), nullable=False),
        sa.Column("status", sa.String(64), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("calibration_generation", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "phase IN ('initial', 'post_mount', 'read_failure')",
            name="ck_restore_ordering_outcome_phase",
        ),
        sa.CheckConstraint(
            "status IN (" + ", ".join(f"'{status}'" for status in _OUTCOME_STATUSES) + ")",
            name="ck_restore_ordering_outcome_status",
        ),
    )
    op.create_index(
        "ix_restore_ordering_outcome_request_volume",
        "restore_ordering_outcome",
        ["request_id", "tape_uuid"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_restore_ordering_outcome_request_volume",
        table_name="restore_ordering_outcome",
    )
    op.drop_table("restore_ordering_outcome")
    op.drop_index(
        "ix_restore_read_plan_slot_request_volume",
        table_name="restore_read_plan_slot",
    )
    op.drop_table("restore_read_plan_slot")
