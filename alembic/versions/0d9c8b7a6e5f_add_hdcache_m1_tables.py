"""add hdcache m1 tables

Revision ID: 0d9c8b7a6e5f
Revises: 79a8f2c1d4e6
Create Date: 2026-07-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0d9c8b7a6e5f"
down_revision: str | Sequence[str] | None = "79a8f2c1d4e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    op.create_table(
        "cache_disk",
        sa.Column("disk_id", sa.String(length=16), nullable=False),
        sa.Column("serial", sa.String(length=256), nullable=False),
        sa.Column("wwn", sa.String(length=256), nullable=True),
        sa.Column("fs_uuid", sa.String(length=128), nullable=False),
        sa.Column("enclosure", sa.String(length=256), nullable=True),
        sa.Column("slot", sa.String(length=128), nullable=True),
        sa.Column("mount", sa.String(length=2048), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("capacity_bytes", sa.BigInteger(), nullable=False),
        sa.Column("filled_bytes", sa.BigInteger(), nullable=False),
        sa.Column("smart_status", sa.String(length=256), nullable=True),
        sa.Column("enrolled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_walk_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "state IN ('active', 'absent', 'retiring', 'dead')",
            name="ck_cache_disk_state",
        ),
        sa.PrimaryKeyConstraint("disk_id"),
        sa.UniqueConstraint("serial"),
    )
    op.create_index("ix_cache_disk_state", "cache_disk", ["state"])

    op.create_table(
        "cache_entry",
        sa.Column("content_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("bundle_key", sa.String(length=128), nullable=True),
        sa.Column("group_key", sa.String(length=256), nullable=True),
        sa.Column("disk_id", sa.String(length=16), nullable=False),
        sa.Column("relpath", sa.String(length=2048), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("representation", sa.String(length=64), nullable=False),
        sa.Column("key_epoch", sa.String(length=128), nullable=True),
        sa.Column("stored_digest", sa.LargeBinary(length=32), nullable=True),
        sa.Column("trusted", sa.Boolean(), nullable=False),
        sa.Column("placed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_read_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "state IN ('filling', 'present', 'lost')",
            name="ck_cache_entry_state",
        ),
        sa.CheckConstraint(
            "representation IN ('raw-bytes', 'rao-aead-v1')",
            name="ck_cache_entry_representation",
        ),
        sa.ForeignKeyConstraint(
            ["content_sha256"],
            ["logical_asset.content_sha256"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["disk_id"], ["cache_disk.disk_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("content_sha256"),
    )
    op.create_index("ix_cache_entry_bundle_key", "cache_entry", ["bundle_key"])
    op.create_index("ix_cache_entry_group_key", "cache_entry", ["group_key"])
    op.create_index("ix_cache_entry_disk_id", "cache_entry", ["disk_id"])
    op.create_index("ix_cache_entry_state", "cache_entry", ["state"])

    op.create_table(
        "restore_request",
        sa.Column("id", sa.String(length=128), nullable=False),
        sa.Column("identity", sa.String(length=256), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("destination_id", sa.String(length=128), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.CheckConstraint(
            "state IN ('pending', 'active', 'completed', 'completed_with_errors')",
            name="ck_restore_request_state",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_restore_request_created_at", "restore_request", ["created_at"])
    op.create_index("ix_restore_request_state", "restore_request", ["state"])

    op.create_table(
        "restore_request_item",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("request_id", sa.String(length=128), nullable=False),
        sa.Column("content_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "state IN ("
            "'queued', 'waking_disk', 'streaming', 'done', "
            "'fell_back_to_tape', 'denied', 'failed'"
            ")",
            name="ck_restore_request_item_state",
        ),
        sa.ForeignKeyConstraint(["request_id"], ["restore_request.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["content_sha256"],
            ["logical_asset.content_sha256"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_restore_request_item_request_id",
        "restore_request_item",
        ["request_id"],
    )
    op.create_index(
        "ix_restore_request_item_content_sha256",
        "restore_request_item",
        ["content_sha256"],
    )
    op.create_index("ix_restore_request_item_state", "restore_request_item", ["state"])


def downgrade() -> None:
    """Downgrade schema."""

    op.drop_index("ix_restore_request_item_state", table_name="restore_request_item")
    op.drop_index("ix_restore_request_item_content_sha256", table_name="restore_request_item")
    op.drop_index("ix_restore_request_item_request_id", table_name="restore_request_item")
    op.drop_table("restore_request_item")

    op.drop_index("ix_restore_request_state", table_name="restore_request")
    op.drop_index("ix_restore_request_created_at", table_name="restore_request")
    op.drop_table("restore_request")

    op.drop_index("ix_cache_entry_state", table_name="cache_entry")
    op.drop_index("ix_cache_entry_disk_id", table_name="cache_entry")
    op.drop_index("ix_cache_entry_group_key", table_name="cache_entry")
    op.drop_index("ix_cache_entry_bundle_key", table_name="cache_entry")
    op.drop_table("cache_entry")

    op.drop_index("ix_cache_disk_state", table_name="cache_disk")
    op.drop_table("cache_disk")
