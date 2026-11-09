"""Add the restore attempt ledger, give-up decisions and server instances.

One row of ``restore_item_attempt`` per admitted agent ``OpenRestore``; one
``restore_item_giveup`` per item the retry policy ended; one
``restore_server_instance`` per start of a process hosting the restore
service (design RSR, D2, D6, D7). All three are ledgers.

Revision ID: e7a1c3b5d9f2
Revises: d3e4f5a6b7c8
Create Date: 2026-11-09
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e7a1c3b5d9f2"
down_revision: str | Sequence[str] | None = "d3e4f5a6b7c8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CAUSES = (
    "not_authorized",
    "configuration",
    "capacity",
    "manifest_invalid",
    "resume_mismatch",
    "open_conflict",
    "stale_lease",
    "protocol",
    "cache_disk_unavailable",
    "tape_only",
    "source_integrity",
    "source_unavailable",
    "server_error",
    "lease_live",
    "item_withdrawn",
    "lease_expired",
)
_END_KINDS = ("job_end", "error", "cancelled", "lost")
_RULES = ("terminal_cause", "max_failures", "max_interruptions")
_OPEN_PREDICATE = "ended_at IS NULL AND closed_at IS NULL"


def _sql_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def upgrade() -> None:
    """Create the three ledger tables."""

    op.create_table(
        "restore_server_instance",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("host", sa.String(length=255), nullable=False),
        sa.Column("pid", sa.Integer(), nullable=False),
        sa.Column("code_version", sa.String(length=128), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "restore_item_attempt",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("restore_request_item_id", sa.Integer(), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("receiver_device_id", sa.String(length=256), nullable=False),
        sa.Column("server_instance_id", sa.Integer(), nullable=False),
        sa.Column("opened_from_state", sa.String(length=16), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("probe_source", sa.String(length=16), nullable=True),
        sa.Column("probe_candidates_tried", sa.Integer(), nullable=True),
        sa.Column("probe_ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_generation", sa.Integer(), nullable=True),
        sa.Column("blocked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("bytes_sent", sa.BigInteger(), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_kind", sa.String(length=16), nullable=True),
        sa.Column("cause", sa.String(length=32), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("code_version", sa.String(length=128), nullable=False),
        sa.CheckConstraint("attempt_no >= 1", name="ck_restore_item_attempt_no"),
        sa.UniqueConstraint(
            "restore_request_item_id", "attempt_no", name="uq_restore_item_attempt_item_no"
        ),
        sa.CheckConstraint(
            "opened_from_state IN ('queued', 'sent', 'streaming', 'done')",
            name="ck_restore_item_attempt_opened_from_state",
        ),
        sa.CheckConstraint(
            "probe_source IS NULL OR probe_source IN ('cache', 'disk')",
            name="ck_restore_item_attempt_probe_source",
        ),
        sa.CheckConstraint(
            "probe_candidates_tried IS NULL OR probe_candidates_tried >= 0",
            name="ck_restore_item_attempt_probe_candidates",
        ),
        sa.CheckConstraint(
            "lease_generation IS NULL OR lease_generation >= 1",
            name="ck_restore_item_attempt_lease_generation",
        ),
        sa.CheckConstraint(
            "bytes_sent IS NULL OR bytes_sent >= 0", name="ck_restore_item_attempt_bytes_sent"
        ),
        sa.CheckConstraint(
            f"end_kind IS NULL OR end_kind IN ({_sql_list(_END_KINDS)})",
            name="ck_restore_item_attempt_end_kind",
        ),
        sa.CheckConstraint(
            f"cause IS NULL OR cause IN ({_sql_list(_CAUSES)})",
            name="ck_restore_item_attempt_cause",
        ),
        sa.CheckConstraint(
            "message IS NULL OR length(message) <= 2000",
            name="ck_restore_item_attempt_message_length",
        ),
        sa.CheckConstraint(
            "(end_kind IS NULL) = (ended_at IS NULL AND closed_at IS NULL)",
            name="ck_restore_item_attempt_end_kind_times",
        ),
        sa.CheckConstraint(
            "ended_at IS NULL OR closed_at IS NULL",
            name="ck_restore_item_attempt_one_end_time",
        ),
        sa.CheckConstraint(
            "CASE WHEN end_kind = 'lost' THEN closed_at IS NOT NULL ELSE closed_at IS NULL END",
            name="ck_restore_item_attempt_lost_closed",
        ),
        sa.CheckConstraint(
            "CASE end_kind "
            "WHEN 'error' THEN cause IS NOT NULL AND cause <> 'lease_expired' "
            "WHEN 'lost' THEN cause IS NULL "
            "WHEN 'cancelled' THEN cause IS NULL OR cause = 'lease_expired' "
            "WHEN 'job_end' THEN cause IS NULL OR cause IN ('lease_expired', 'item_withdrawn') "
            "ELSE cause IS NULL END",
            name="ck_restore_item_attempt_cause_by_kind",
        ),
        sa.CheckConstraint(
            "blocked_until IS NULL OR cause = 'lease_live'",
            name="ck_restore_item_attempt_blocked_until",
        ),
        sa.ForeignKeyConstraint(
            ["restore_request_item_id"], ["restore_request_item.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["server_instance_id"], ["restore_server_instance.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_restore_item_attempt_server_instance",
        "restore_item_attempt",
        ["server_instance_id"],
    )
    op.create_index(
        "uq_restore_item_attempt_open",
        "restore_item_attempt",
        ["restore_request_item_id"],
        unique=True,
        sqlite_where=sa.text(_OPEN_PREDICATE),
        postgresql_where=sa.text(_OPEN_PREDICATE),
    )
    op.create_table(
        "restore_item_giveup",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("restore_request_item_id", sa.Integer(), nullable=False),
        sa.Column("decisive_attempt_id", sa.Integer(), nullable=False),
        sa.Column("rule", sa.String(length=32), nullable=False),
        sa.Column("counted", sa.Integer(), nullable=True),
        sa.Column("threshold", sa.Integer(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("code_version", sa.String(length=128), nullable=False),
        sa.UniqueConstraint("restore_request_item_id", name="uq_restore_item_giveup_item"),
        sa.UniqueConstraint("decisive_attempt_id", name="uq_restore_item_giveup_attempt"),
        sa.CheckConstraint(f"rule IN ({_sql_list(_RULES)})", name="ck_restore_item_giveup_rule"),
        sa.CheckConstraint(
            "(rule = 'terminal_cause' AND counted IS NULL AND threshold IS NULL) OR "
            "(rule <> 'terminal_cause' AND counted IS NOT NULL AND threshold IS NOT NULL)",
            name="ck_restore_item_giveup_numbers",
        ),
        sa.ForeignKeyConstraint(
            ["restore_request_item_id"], ["restore_request_item.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["decisive_attempt_id"], ["restore_item_attempt.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Drop the three ledger tables (give-up first: it references attempts)."""

    op.drop_table("restore_item_giveup")
    op.drop_index("uq_restore_item_attempt_open", table_name="restore_item_attempt")
    op.drop_index("ix_restore_item_attempt_server_instance", table_name="restore_item_attempt")
    op.drop_table("restore_item_attempt")
    op.drop_table("restore_server_instance")
