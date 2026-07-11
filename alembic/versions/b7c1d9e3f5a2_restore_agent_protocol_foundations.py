"""Add persisted restore-agent protocol and enrollment foundations.

Revision ID: b7c1d9e3f5a2
Revises: f6a7b8c9d0e1
Create Date: 2026-07-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b7c1d9e3f5a2"
down_revision: str | Sequence[str] | None = "f6a7b8c9d0e1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SCOPE_CHECK = (
    'CAST(scopes AS TEXT) IN (\'["ingest"]\', \'["restore"]\', \'["ingest", "restore"]\')'
)


def upgrade() -> None:
    """Add logical devices, scoped enrollment, agent binding, progress, and leases."""

    op.create_table(
        "grpc_logical_device",
        sa.Column("device_id", sa.String(length=256), nullable=False),
        sa.Column("scopes", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("device_id"),
    )
    bind = op.get_bind()
    logical = sa.table(
        "grpc_logical_device",
        sa.column("device_id", sa.String()),
        sa.column("scopes", sa.JSON()),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    enrollment = sa.table(
        "grpc_device_enrollment",
        sa.column("device_id", sa.String()),
        sa.column("created_at", sa.DateTime(timezone=True)),
    )
    existing_devices = bind.execute(
        sa.select(enrollment.c.device_id, sa.func.min(enrollment.c.created_at)).group_by(
            enrollment.c.device_id
        )
    ).all()
    if existing_devices:
        bind.execute(
            logical.insert(),
            [
                {
                    "device_id": device_id,
                    "scopes": ["ingest"],
                    "created_at": created_at,
                    "updated_at": created_at,
                }
                for device_id, created_at in existing_devices
            ],
        )
    with op.batch_alter_table("grpc_logical_device") as batch:
        batch.alter_column("scopes", existing_type=sa.JSON(), nullable=False)
        batch.create_check_constraint("ck_grpc_logical_device_scopes", _SCOPE_CHECK)

    op.create_table(
        "grpc_device_destination_grant",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("device_id", sa.String(length=256), nullable=False),
        sa.Column("destination_id", sa.String(length=128), nullable=False),
        sa.Column("dest_root", sa.String(length=2048), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["device_id"], ["grpc_logical_device.device_id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "destination_id", name="uq_grpc_device_destination_grant"),
    )
    op.create_index(
        "ix_grpc_device_destination_grant_destination",
        "grpc_device_destination_grant",
        ["destination_id"],
    )
    with op.batch_alter_table("grpc_device_enrollment") as batch:
        batch.create_foreign_key(
            "fk_grpc_device_enrollment_logical_device",
            "grpc_logical_device",
            ["device_id"],
            ["device_id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("grpc_enroll_token") as batch:
        batch.add_column(sa.Column("scopes", sa.JSON(), nullable=True))
    token = sa.table("grpc_enroll_token", sa.column("scopes", sa.JSON()))
    bind.execute(token.update().values(scopes=["ingest"]))
    with op.batch_alter_table("grpc_enroll_token") as batch:
        batch.alter_column("scopes", existing_type=sa.JSON(), nullable=False)
        batch.create_check_constraint("ck_grpc_enroll_token_scopes", _SCOPE_CHECK)

    with op.batch_alter_table("restore_request") as batch:
        batch.add_column(
            sa.Column(
                "delivery_mode",
                sa.String(length=32),
                nullable=False,
                server_default="server_local",
            )
        )
        batch.add_column(sa.Column("receiver_device_id", sa.String(length=256), nullable=True))
        batch.create_foreign_key(
            "fk_restore_request_receiver_device",
            "grpc_logical_device",
            ["receiver_device_id"],
            ["device_id"],
            ondelete="RESTRICT",
        )
        batch.create_check_constraint(
            "ck_restore_request_delivery_mode",
            "delivery_mode IN ('server_local', 'agent')",
        )
        batch.create_check_constraint(
            "ck_restore_request_receiver_binding",
            "(delivery_mode = 'server_local' AND receiver_device_id IS NULL) OR "
            "(delivery_mode = 'agent' AND receiver_device_id IS NOT NULL)",
        )
    with op.batch_alter_table("restore_request_item") as batch:
        batch.add_column(sa.Column("final_rel_path", sa.String(length=2048), nullable=True))
        batch.drop_constraint("ck_restore_request_item_state", type_="check")
        batch.create_check_constraint(
            "ck_restore_request_item_state",
            "state IN ('queued', 'waking_disk', 'streaming', 'sent', 'done', "
            "'fell_back_to_tape', 'denied', 'failed')",
        )

    op.create_table(
        "restore_item_checkpoint",
        sa.Column("restore_request_item_id", sa.Integer(), nullable=False),
        sa.Column("manifest_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("committed_index", sa.Integer(), nullable=False),
        sa.Column("revealed", sa.Boolean(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "committed_index >= 0 AND committed_index <= 2147483647",
            name="ck_restore_item_checkpoint_index",
        ),
        sa.CheckConstraint(
            "revealed = false OR committed_index >= 1",
            name="ck_restore_item_checkpoint_revealed",
        ),
        sa.CheckConstraint(
            "length(manifest_sha256) = 32",
            name="ck_restore_item_checkpoint_manifest_sha256",
        ),
        sa.ForeignKeyConstraint(
            ["restore_request_item_id"], ["restore_request_item.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("restore_request_item_id"),
    )
    op.create_table(
        "restore_open_session",
        sa.Column("restore_request_item_id", sa.Integer(), nullable=False),
        sa.Column("receiver_device_id", sa.String(length=256), nullable=False),
        sa.Column("manifest_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("generation >= 1", name="ck_restore_open_session_generation"),
        sa.CheckConstraint(
            "length(manifest_sha256) = 32",
            name="ck_restore_open_session_manifest_sha256",
        ),
        sa.ForeignKeyConstraint(
            ["receiver_device_id"], ["grpc_logical_device.device_id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["restore_request_item_id"], ["restore_request_item.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("restore_request_item_id"),
        sa.UniqueConstraint("restore_request_item_id", name="uq_restore_open_session_item"),
    )

    valid_caps = ", ".join(
        f"'{cap}'"
        for cap in (
            "can_view",
            "can_receive",
            "can_restore",
            "can_logs",
            "can_admin",
            "can_restore_p2",
            "can_restore_p3",
        )
    )
    op.create_table(
        "operator_capability_sync",
        sa.Column("operator", sa.String(length=256), nullable=False),
        sa.Column("synchronized_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_until", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("operator"),
    )
    op.create_table(
        "operator_live_capability",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("operator", sa.String(length=256), nullable=False),
        sa.Column("capability", sa.String(length=64), nullable=False),
        sa.CheckConstraint(
            f"capability IN ({valid_caps})", name="ck_operator_live_capability_value"
        ),
        sa.ForeignKeyConstraint(
            ["operator"], ["operator_capability_sync.operator"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("operator", "capability", name="uq_operator_live_capability"),
    )
    op.create_index(
        "ix_operator_live_capability_operator",
        "operator_live_capability",
        ["operator"],
    )


def downgrade() -> None:
    """Remove restore-agent foundations and return to ingest-only enrollment."""

    op.drop_index("ix_operator_live_capability_operator", table_name="operator_live_capability")
    op.drop_table("operator_live_capability")
    op.drop_table("operator_capability_sync")
    op.drop_table("restore_open_session")
    op.drop_table("restore_item_checkpoint")

    op.execute("UPDATE restore_request_item SET state = 'failed' WHERE state = 'sent'")
    with op.batch_alter_table("restore_request_item") as batch:
        batch.drop_constraint("ck_restore_request_item_state", type_="check")
        batch.create_check_constraint(
            "ck_restore_request_item_state",
            "state IN ('queued', 'waking_disk', 'streaming', 'done', "
            "'fell_back_to_tape', 'denied', 'failed')",
        )
        batch.drop_column("final_rel_path")

    with op.batch_alter_table("restore_request") as batch:
        batch.drop_constraint("ck_restore_request_receiver_binding", type_="check")
        batch.drop_constraint("ck_restore_request_delivery_mode", type_="check")
        batch.drop_constraint("fk_restore_request_receiver_device", type_="foreignkey")
        batch.drop_column("receiver_device_id")
        batch.drop_column("delivery_mode")

    with op.batch_alter_table("grpc_enroll_token") as batch:
        batch.drop_constraint("ck_grpc_enroll_token_scopes", type_="check")
        batch.drop_column("scopes")
    with op.batch_alter_table("grpc_device_enrollment") as batch:
        batch.drop_constraint("fk_grpc_device_enrollment_logical_device", type_="foreignkey")
    op.drop_index(
        "ix_grpc_device_destination_grant_destination",
        table_name="grpc_device_destination_grant",
    )
    op.drop_table("grpc_device_destination_grant")
    op.drop_table("grpc_logical_device")
