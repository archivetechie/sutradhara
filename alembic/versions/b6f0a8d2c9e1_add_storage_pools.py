"""Add storage pools and RAO archive catalog tables.

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

    op.create_table(
        "artifactclass_policy",
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("ruleset", sa.String(length=256), nullable=False),
        sa.Column("expect", sa.String(length=32), nullable=False),
        sa.Column("target_bytes", sa.BigInteger(), nullable=False),
        sa.Column("max_age_seconds", sa.Integer(), nullable=False),
        sa.Column("restore_preference", sa.JSON(), nullable=False),
        sa.Column("policy_source", sa.String(length=1024), nullable=True),
        sa.Column("policy_sha256", sa.String(length=64), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("artifactclass"),
    )

    op.create_table(
        "bundle",
        sa.Column("id", sa.String(length=128), nullable=False),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("total_bytes", sa.BigInteger(), nullable=False),
        sa.Column("member_count", sa.Integer(), nullable=False),
        sa.Column("target_bytes", sa.BigInteger(), nullable=False),
        sa.Column("max_age_seconds", sa.Integer(), nullable=False),
        sa.Column("ruleset", sa.String(length=256), nullable=True),
        sa.Column("expect", sa.String(length=32), nullable=True),
        sa.Column("archive_id", sa.String(length=128), nullable=True),
        sa.Column("scan_summary", sa.JSON(), nullable=True),
        sa.Column("review_summary", sa.JSON(), nullable=True),
        sa.Column("customer_manifest_path", sa.String(length=2048), nullable=True),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("flushed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sealed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("held_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_bundle_artifactclass"),
        "bundle",
        ["artifactclass"],
        unique=False,
    )

    with op.batch_alter_table("copy") as batch_op:
        batch_op.add_column(sa.Column("pool_id", sa.String(length=128), nullable=True))
        batch_op.add_column(
            sa.Column("bundle_id", sa.String(length=128), nullable=True)
        )
        batch_op.alter_column(
            "logical_asset_hash",
            existing_type=sa.LargeBinary(length=32),
            nullable=True,
        )
        batch_op.create_foreign_key(
            "fk_copy_pool_id_pool",
            "pool",
            ["pool_id"],
            ["id"],
            ondelete="SET NULL",
        )
        batch_op.create_foreign_key(
            "fk_copy_bundle_id_bundle",
            "bundle",
            ["bundle_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_check_constraint(
            "ck_copy_asset_or_bundle",
            "logical_asset_hash IS NOT NULL OR bundle_id IS NOT NULL",
        )
        batch_op.create_index(op.f("ix_copy_pool_id"), ["pool_id"], unique=False)
        batch_op.create_index(op.f("ix_copy_bundle_id"), ["bundle_id"], unique=False)

    op.create_table(
        "bundle_member",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("bundle_id", sa.String(length=128), nullable=False),
        sa.Column("logical_asset_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("member_path", sa.String(length=1024), nullable=False),
        sa.Column("source_path", sa.String(length=2048), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("file_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("source_metadata", sa.JSON(), nullable=True),
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
            "member_path",
            name="uq_bundle_member_bundle_path",
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
        sa.ForeignKeyConstraint(["bundle_id"], ["bundle.id"], ondelete="SET NULL"),
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
        sa.Column("bundle_id", sa.String(length=128), nullable=False),
        sa.Column("copy_id", sa.Integer(), nullable=False),
        sa.Column("pool_id", sa.String(length=128), nullable=False),
        sa.Column("root_path", sa.String(length=1024), nullable=False),
        sa.Column("native_locator", sa.JSON(), nullable=False),
        sa.Column("archive_id", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["bundle_id"], ["bundle.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["copy_id"], ["copy.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["pool_id"], ["pool.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("copy_id", "root_path", name="uq_blob_root_copy_root"),
    )
    op.create_index(op.f("ix_blob_root_bundle_id"), "blob_root", ["bundle_id"])
    op.create_index(op.f("ix_blob_root_copy_id"), "blob_root", ["copy_id"])
    op.create_index(op.f("ix_blob_root_pool_id"), "blob_root", ["pool_id"])

    op.create_table(
        "exclusion_record",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("bundle_id", sa.String(length=128), nullable=True),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("logical_asset_hash", sa.LargeBinary(length=32), nullable=True),
        sa.Column("path", sa.String(length=1024), nullable=True),
        sa.Column("reason", sa.String(length=128), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.Column("bytes_total", sa.BigInteger(), nullable=False),
        sa.Column("ruleset_name", sa.String(length=256), nullable=True),
        sa.Column("ruleset_hash", sa.String(length=64), nullable=True),
        sa.Column("detail", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["bundle_id"], ["bundle.id"], ondelete="SET NULL"),
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
    )
    op.create_index(
        op.f("ix_exclusion_record_bundle_id"),
        "exclusion_record",
        ["bundle_id"],
    )
    op.create_index(
        op.f("ix_exclusion_record_logical_asset_hash"),
        "exclusion_record",
        ["logical_asset_hash"],
    )

    op.create_table(
        "review_decision",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("bundle_id", sa.String(length=128), nullable=False),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("scope", sa.String(length=64), nullable=False),
        sa.Column("subtree", sa.String(length=1024), nullable=True),
        sa.Column("reason", sa.String(length=2048), nullable=True),
        sa.Column("reviewer", sa.String(length=256), nullable=True),
        sa.Column("persisted_rule", sa.JSON(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["bundle_id"], ["bundle.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_review_decision_bundle_id"),
        "review_decision",
        ["bundle_id"],
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

    op.drop_index(op.f("ix_review_decision_bundle_id"), table_name="review_decision")
    op.drop_table("review_decision")

    op.drop_index(
        op.f("ix_exclusion_record_logical_asset_hash"),
        table_name="exclusion_record",
    )
    op.drop_index(
        op.f("ix_exclusion_record_bundle_id"),
        table_name="exclusion_record",
    )
    op.drop_index(
        op.f("ix_exclusion_record_artifactclass"),
        table_name="exclusion_record",
    )
    op.drop_table("exclusion_record")

    op.drop_index(op.f("ix_blob_root_pool_id"), table_name="blob_root")
    op.drop_index(op.f("ix_blob_root_copy_id"), table_name="blob_root")
    op.drop_index(op.f("ix_blob_root_bundle_id"), table_name="blob_root")
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

    with op.batch_alter_table("copy") as batch_op:
        batch_op.drop_index(op.f("ix_copy_bundle_id"))
        batch_op.drop_index(op.f("ix_copy_pool_id"))
        batch_op.drop_constraint("ck_copy_asset_or_bundle", type_="check")
        batch_op.drop_constraint("fk_copy_bundle_id_bundle", type_="foreignkey")
        batch_op.drop_constraint("fk_copy_pool_id_pool", type_="foreignkey")
        batch_op.alter_column(
            "logical_asset_hash",
            existing_type=sa.LargeBinary(length=32),
            nullable=False,
        )
        batch_op.drop_column("bundle_id")
        batch_op.drop_column("pool_id")

    op.drop_index(op.f("ix_bundle_artifactclass"), table_name="bundle")
    op.drop_table("bundle")
    op.drop_table("artifactclass_policy")

    op.drop_index(op.f("ix_artifactclass_pool_pool_id"), table_name="artifactclass_pool")
    op.drop_index(
        op.f("ix_artifactclass_pool_artifactclass"),
        table_name="artifactclass_pool",
    )
    op.drop_table("artifactclass_pool")

    op.drop_index(op.f("ix_pool_backend_id"), table_name="pool")
    op.drop_table("pool")
