"""Add storage pools and artifactclass memberships.

Revision ID: b6f0a8d2c9e1
Revises: 9b2af1cc0e6a
Create Date: 2026-06-14
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "b6f0a8d2c9e1"
down_revision: str | Sequence[str] | None = "9b2af1cc0e6a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pool",
        sa.Column("id", sa.String(length=128), nullable=False),
        sa.Column("backend_id", sa.Integer(), nullable=False),
        sa.Column("representation", sa.String(length=64), nullable=False),
        sa.Column("location", sa.String(length=256), nullable=False),
        sa.Column("offsite_gate", sa.Boolean(), nullable=False),
        sa.Column("tier", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["backend_id"], ["backend.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("backend_id", "id", name="uq_pool_backend_id"),
    )
    op.create_index(op.f("ix_pool_backend_id"), "pool", ["backend_id"], unique=False)

    op.create_table(
        "artifactclass_pool",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("pool_id", sa.String(length=128), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False),
        sa.Column("role", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["pool_id"], ["pool.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "artifactclass",
            "pool_id",
            name="uq_artifactclass_pool_artifactclass_pool",
        ),
    )
    op.create_index(
        op.f("ix_artifactclass_pool_artifactclass"),
        "artifactclass_pool",
        ["artifactclass"],
        unique=False,
    )
    op.create_index(
        op.f("ix_artifactclass_pool_pool_id"),
        "artifactclass_pool",
        ["pool_id"],
        unique=False,
    )

    with op.batch_alter_table("copy") as batch_op:
        batch_op.add_column(
            sa.Column(
                "pool_id",
                sa.String(length=128),
                nullable=True,
            )
        )
        batch_op.create_foreign_key(
            "fk_copy_pool_id_pool",
            "pool",
            ["pool_id"],
            ["id"],
            ondelete="SET NULL",
        )
        batch_op.create_index(op.f("ix_copy_pool_id"), ["pool_id"], unique=False)

    op.create_table(
        "bundle",
        sa.Column("id", sa.String(length=128), nullable=False),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("representation", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("total_bytes", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_bundle_artifactclass"),
        "bundle",
        ["artifactclass"],
        unique=False,
    )

    op.create_table(
        "bundle_member",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("bundle_id", sa.String(length=128), nullable=False),
        sa.Column("logical_asset_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("member_path", sa.String(length=1024), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("file_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("added_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["bundle_id"], ["bundle.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["logical_asset_hash"],
            ["logical_asset.content_sha256"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "bundle_id",
            "logical_asset_hash",
            name="uq_bundle_member_bundle_asset",
        ),
    )
    op.create_index(
        op.f("ix_bundle_member_bundle_id"),
        "bundle_member",
        ["bundle_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_bundle_member_logical_asset_hash"),
        "bundle_member",
        ["logical_asset_hash"],
        unique=False,
    )

    op.create_table(
        "asset_locator",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("logical_asset_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("pool_id", sa.String(length=128), nullable=False),
        sa.Column("copy_id", sa.Integer(), nullable=True),
        sa.Column("bundle_id", sa.String(length=128), nullable=True),
        sa.Column("native_locator", sa.JSON(), nullable=False),
        sa.Column("representation", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["bundle_id"],
            ["bundle.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(["copy_id"], ["copy.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["logical_asset_hash"],
            ["logical_asset.content_sha256"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["pool_id"], ["pool.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_asset_locator_bundle_id"),
        "asset_locator",
        ["bundle_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_asset_locator_copy_id"),
        "asset_locator",
        ["copy_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_asset_locator_logical_asset_hash"),
        "asset_locator",
        ["logical_asset_hash"],
        unique=False,
    )
    op.create_index(
        op.f("ix_asset_locator_pool_id"),
        "asset_locator",
        ["pool_id"],
        unique=False,
    )

    op.create_table(
        "blob_root",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("logical_asset_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("algorithm", sa.String(length=64), nullable=False),
        sa.Column("root_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["logical_asset_hash"],
            ["logical_asset.content_sha256"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "logical_asset_hash",
            "algorithm",
            name="uq_blob_root_asset_algorithm",
        ),
    )
    op.create_index(
        op.f("ix_blob_root_logical_asset_hash"),
        "blob_root",
        ["logical_asset_hash"],
        unique=False,
    )

    op.create_table(
        "exclusion_record",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("logical_asset_hash", sa.LargeBinary(length=32), nullable=True),
        sa.Column("path", sa.String(length=1024), nullable=True),
        sa.Column("reason", sa.String(length=128), nullable=False),
        sa.Column("detail", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["logical_asset_hash"],
            ["logical_asset.content_sha256"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_exclusion_record_artifactclass"),
        "exclusion_record",
        ["artifactclass"],
        unique=False,
    )
    op.create_index(
        op.f("ix_exclusion_record_logical_asset_hash"),
        "exclusion_record",
        ["logical_asset_hash"],
        unique=False,
    )

    op.drop_index(
        op.f("ix_placement_tag_pin_backend_id"),
        table_name="placement_tag_pin",
    )
    op.drop_table("placement_tag_pin")


def downgrade() -> None:
    op.create_table(
        "placement_tag_pin",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("backend_id", sa.Integer(), nullable=False),
        sa.Column("placement_id", sa.String(length=128), nullable=False),
        sa.Column("content_class", sa.String(length=128), nullable=False),
        sa.Column("copy_class", sa.String(length=128), nullable=False),
        sa.Column("pinned_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["backend_id"], ["backend.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "backend_id",
            "placement_id",
            name="uq_placement_tag_pin_backend_placement",
        ),
    )
    op.create_index(
        op.f("ix_placement_tag_pin_backend_id"),
        "placement_tag_pin",
        ["backend_id"],
        unique=False,
    )

    with op.batch_alter_table("copy") as batch_op:
        batch_op.drop_constraint("fk_copy_pool_id_pool", type_="foreignkey")
        batch_op.drop_index(op.f("ix_copy_pool_id"))
        batch_op.drop_column("pool_id")

    op.drop_index(
        op.f("ix_exclusion_record_logical_asset_hash"),
        table_name="exclusion_record",
    )
    op.drop_index(
        op.f("ix_exclusion_record_artifactclass"),
        table_name="exclusion_record",
    )
    op.drop_table("exclusion_record")

    op.drop_index(op.f("ix_blob_root_logical_asset_hash"), table_name="blob_root")
    op.drop_table("blob_root")

    op.drop_index(op.f("ix_asset_locator_pool_id"), table_name="asset_locator")
    op.drop_index(
        op.f("ix_asset_locator_logical_asset_hash"),
        table_name="asset_locator",
    )
    op.drop_index(op.f("ix_asset_locator_copy_id"), table_name="asset_locator")
    op.drop_index(op.f("ix_asset_locator_bundle_id"), table_name="asset_locator")
    op.drop_table("asset_locator")

    op.drop_index(
        op.f("ix_bundle_member_logical_asset_hash"),
        table_name="bundle_member",
    )
    op.drop_index(op.f("ix_bundle_member_bundle_id"), table_name="bundle_member")
    op.drop_table("bundle_member")

    op.drop_index(op.f("ix_bundle_artifactclass"), table_name="bundle")
    op.drop_table("bundle")

    op.drop_index(op.f("ix_artifactclass_pool_pool_id"), table_name="artifactclass_pool")
    op.drop_index(
        op.f("ix_artifactclass_pool_artifactclass"),
        table_name="artifactclass_pool",
    )
    op.drop_table("artifactclass_pool")

    op.drop_index(op.f("ix_pool_backend_id"), table_name="pool")
    op.drop_table("pool")
