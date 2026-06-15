"""Tighten archive locator and copy invariants.

Revision ID: 2f4a8bb0c2d7
Revises: b6f0a8d2c9e1
Create Date: 2026-06-15
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "2f4a8bb0c2d7"
down_revision: str | Sequence[str] | None = "b6f0a8d2c9e1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("asset_locator") as batch_op:
        batch_op.add_column(
            sa.Column(
                "member_path",
                sa.String(length=1024),
                nullable=False,
                server_default="",
            )
        )
        batch_op.alter_column("member_path", server_default=None)
        batch_op.create_unique_constraint(
            "uq_asset_locator_copy_asset_member",
            ["copy_id", "logical_asset_hash", "member_path"],
        )

    with op.batch_alter_table("copy") as batch_op:
        batch_op.drop_constraint("ck_copy_asset_or_bundle", type_="check")
        batch_op.create_check_constraint(
            "ck_copy_asset_xor_bundle",
            "(logical_asset_hash IS NOT NULL AND bundle_id IS NULL) OR "
            "(logical_asset_hash IS NULL AND bundle_id IS NOT NULL)",
        )


def downgrade() -> None:
    with op.batch_alter_table("copy") as batch_op:
        batch_op.drop_constraint("ck_copy_asset_xor_bundle", type_="check")
        batch_op.create_check_constraint(
            "ck_copy_asset_or_bundle",
            "logical_asset_hash IS NOT NULL OR bundle_id IS NOT NULL",
        )

    with op.batch_alter_table("asset_locator") as batch_op:
        batch_op.drop_constraint("uq_asset_locator_copy_asset_member", type_="unique")
        batch_op.drop_column("member_path")
