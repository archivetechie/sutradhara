"""Copy-grain M3 durability schema and pool lifecycle.

Revision ID: c9a0d1e2f3b4
Revises: 7c2d4e9f0a1b
Create Date: 2026-07-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c9a0d1e2f3b4"
down_revision: str | Sequence[str] | None = "7c2d4e9f0a1b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

BACKEND_FAMILIES = {
    "rem_tape": "tape",
    "d2_tape": "d2tape",
    "rem_disk": "disk",
    "plain_disk": "disk",
    "ssh_disk": "disk",
    "s3": "cloud",
    "gcs": "cloud",
    "azure_blob": "cloud",
    "memory": "memory",
}


def upgrade() -> None:
    op.add_column(
        "backend",
        sa.Column("implementation_family", sa.String(length=64), nullable=True, server_default=""),
    )
    for kind, family in BACKEND_FAMILIES.items():
        op.execute(
            sa.text("UPDATE backend SET implementation_family = :family WHERE kind = :kind").bindparams(
                family=family,
                kind=kind,
            )
        )
    _assert_all_backend_kinds_mapped()
    _recreate_referenced_table(_backend_table())

    with op.batch_alter_table("pool") as batch:
        batch.add_column(
            sa.Column(
                "accepts_writes",
                sa.Boolean(),
                nullable=False,
                server_default=sa.true(),
            )
        )
        batch.add_column(
            sa.Column(
                "retired",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
        batch.add_column(sa.Column("media_generation", sa.String(length=128), nullable=True))

    op.add_column(
        "artifactclass_policy",
        sa.Column("min_copies", sa.Integer(), nullable=False, server_default="3"),
    )
    op.add_column(
        "artifactclass_policy",
        sa.Column("min_impl_families", sa.Integer(), nullable=False, server_default="2"),
    )
    with op.batch_alter_table(
        "artifactclass_policy",
        recreate="always",
        copy_from=_artifactclass_policy_table(include_durability=True),
    ):
        pass

    with op.batch_alter_table(
        "asset_locator",
        recreate="always",
        copy_from=_asset_locator_table(pool_ondelete="RESTRICT"),
    ):
        pass
    with op.batch_alter_table(
        "blob_root",
        recreate="always",
        copy_from=_blob_root_table(pool_ondelete="RESTRICT"),
    ):
        pass


def downgrade() -> None:
    with op.batch_alter_table(
        "blob_root",
        recreate="always",
        copy_from=_blob_root_table(pool_ondelete="CASCADE"),
    ):
        pass
    with op.batch_alter_table(
        "asset_locator",
        recreate="always",
        copy_from=_asset_locator_table(pool_ondelete="CASCADE"),
    ):
        pass

    with op.batch_alter_table(
        "artifactclass_policy",
        recreate="always",
        copy_from=_artifactclass_policy_table(include_durability=False),
    ):
        pass

    _recreate_referenced_table(_pool_table(include_lifecycle=False), table_name="pool")
    _recreate_referenced_table(_backend_table(include_family=False), table_name="backend")


def _assert_all_backend_kinds_mapped() -> None:
    connection = op.get_bind()
    unmapped = list(
        connection.execute(
            sa.text(
                "SELECT kind FROM backend "
                "WHERE implementation_family IS NULL OR implementation_family = '' "
                "GROUP BY kind ORDER BY kind"
            )
        )
    )
    if unmapped:
        kinds = ", ".join(str(row[0]) for row in unmapped)
        raise RuntimeError(f"backend kind(s) have no implementation family mapping: {kinds}")


def _recreate_referenced_table(table: sa.Table, *, table_name: str | None = None) -> None:
    name = table_name or table.name
    context = op.get_context()
    if op.get_bind().dialect.name != "sqlite":
        with op.batch_alter_table(name, recreate="always", copy_from=table):
            pass
        return
    with context.autocommit_block():
        op.execute("PRAGMA foreign_keys=OFF")
        with op.batch_alter_table(name, recreate="always", copy_from=table):
            pass
        op.execute("PRAGMA foreign_keys=ON")
        op.execute("PRAGMA foreign_key_check")


def _backend_table(*, include_family: bool = True) -> sa.Table:
    metadata = sa.MetaData()
    columns: list[sa.Column[object]] = [
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
    ]
    if include_family:
        columns.append(sa.Column("implementation_family", sa.String(length=64), nullable=False))
    columns.extend(
        [
            sa.Column("config", sa.JSON(), nullable=True),
            sa.Column("tier", sa.String(length=32), nullable=False),
            sa.Column("added_at", sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint("name"),
        ]
    )
    return sa.Table("backend", metadata, *columns)


def _pool_table(*, include_lifecycle: bool = True) -> sa.Table:
    metadata = sa.MetaData()
    table = sa.Table(
        "pool",
        metadata,
        sa.Column("id", sa.String(length=128), primary_key=True),
        sa.Column("backend_id", sa.Integer(), nullable=False),
        sa.Column("representation", sa.String(length=64), nullable=False),
        sa.Column("location", sa.String(length=256), nullable=False),
        sa.Column("offsite_gate", sa.Boolean(), nullable=False),
        sa.Column("tier", sa.String(length=64), nullable=False),
        *(
            [
                sa.Column("accepts_writes", sa.Boolean(), nullable=False),
                sa.Column("retired", sa.Boolean(), nullable=False),
                sa.Column("media_generation", sa.String(length=128), nullable=True),
            ]
            if include_lifecycle
            else []
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["backend_id"], ["backend.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("backend_id", "id", name="uq_pool_backend_id"),
    )
    sa.Index("ix_pool_backend_id", table.c.backend_id)
    return table


def _artifactclass_policy_table(*, include_durability: bool) -> sa.Table:
    metadata = sa.MetaData()
    columns: list[sa.Column[object]] = [
        sa.Column("artifactclass", sa.String(length=128), primary_key=True),
        sa.Column("ruleset", sa.String(length=256), nullable=False),
        sa.Column("expect", sa.String(length=32), nullable=False),
        sa.Column("target_bytes", sa.BigInteger(), nullable=False),
        sa.Column("max_age_seconds", sa.Integer(), nullable=False),
        sa.Column("restore_preference", sa.JSON(), nullable=False),
    ]
    if include_durability:
        columns.extend(
            [
                sa.Column("min_copies", sa.Integer(), nullable=False),
                sa.Column("min_impl_families", sa.Integer(), nullable=False),
            ]
        )
    columns.extend(
        [
            sa.Column("staging_config", sa.JSON(), nullable=False),
            sa.Column("hdcache_config", sa.JSON(), nullable=False),
            sa.Column("policy_source", sa.String(length=1024), nullable=True),
            sa.Column("policy_sha256", sa.String(length=64), nullable=True),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        ]
    )
    return sa.Table("artifactclass_policy", metadata, *columns)


def _asset_locator_table(*, pool_ondelete: str) -> sa.Table:
    metadata = sa.MetaData()
    table = sa.Table(
        "asset_locator",
        metadata,
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("logical_asset_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("pool_id", sa.String(length=128), nullable=False),
        sa.Column("copy_id", sa.Integer(), nullable=True),
        sa.Column("bundle_id", sa.String(length=128), nullable=True),
        sa.Column("native_locator", sa.JSON(), nullable=False),
        sa.Column("member_path", sa.String(length=2048), nullable=False),
        sa.Column("representation", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["bundle_id"], ["bundle.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["copy_id"], ["copy.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["logical_asset_hash"],
            ["logical_asset.content_sha256"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["pool_id"], ["pool.id"], ondelete=pool_ondelete),
        sa.UniqueConstraint(
            "copy_id",
            "logical_asset_hash",
            "member_path",
            name="uq_asset_locator_copy_asset_member",
        ),
    )
    sa.Index("ix_asset_locator_bundle_id", table.c.bundle_id)
    sa.Index("ix_asset_locator_copy_id", table.c.copy_id)
    sa.Index("ix_asset_locator_logical_asset_hash", table.c.logical_asset_hash)
    sa.Index("ix_asset_locator_pool_id", table.c.pool_id)
    return table


def _blob_root_table(*, pool_ondelete: str) -> sa.Table:
    metadata = sa.MetaData()
    table = sa.Table(
        "blob_root",
        metadata,
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("bundle_id", sa.String(length=128), nullable=False),
        sa.Column("copy_id", sa.Integer(), nullable=False),
        sa.Column("pool_id", sa.String(length=128), nullable=False),
        sa.Column("root_path", sa.String(length=1024), nullable=False),
        sa.Column("native_locator", sa.JSON(), nullable=False),
        sa.Column("archive_id", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["bundle_id"], ["bundle.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["copy_id"], ["copy.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["pool_id"], ["pool.id"], ondelete=pool_ondelete),
        sa.UniqueConstraint("copy_id", "root_path", name="uq_blob_root_copy_root"),
    )
    sa.Index("ix_blob_root_bundle_id", table.c.bundle_id)
    sa.Index("ix_blob_root_copy_id", table.c.copy_id)
    sa.Index("ix_blob_root_pool_id", table.c.pool_id)
    return table
