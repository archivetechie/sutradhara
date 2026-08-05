"""Bundle groups: member class, group fingerprint/basis, projection, floor.

Migration order follows design-bundle-groups §7 exactly:

1. Add ``bundle_member.artifactclass``; backfill from ``bundle.artifactclass``.
2. Add ``bundle.bundle_group`` + ``bundle.group_basis``; backfill fingerprints
   from each bundle's class's *current* pool set with
   ``basis_source: backfilled`` — an honest guess, marked as one.
3. Add ``artifactclass_policy.bundle_group``; backfill it for every existing
   policy row (recomputable from ``artifactclass_pool`` + ``pool``).
4. Assert no two open accumulators share a fingerprint; abort otherwise (the
   operational drain is the steward's pre-step, not this migration's).
4b. Add ``pool.min_object_bytes`` (nullable, non-negative; no backfill —
   NULL = no floor declared).
5. Drop ``bundle.artifactclass``, ``bundle.ruleset``, ``bundle.expect``;
   create the one-open-accumulator partial unique index.
6. Rewrite ``ck_submission_status`` to admit ``accumulated``. The new
   ``bundle`` terminal status ``void`` is a vocabulary declaration only —
   ``bundle`` has no status CHECK.

Revision ID: b9c8d7e6f5a4
Revises: a4b5c6d7e8f9
Create Date: 2026-08-05
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b9c8d7e6f5a4"
down_revision: str | Sequence[str] | None = "a4b5c6d7e8f9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_GROUP_BASIS_WRITER_VERSION = 1


def _canonical_basis_json(basis: list[dict[str, str]]) -> str:
    return json.dumps(basis, sort_keys=True, separators=(",", ":"))


def _fingerprint(basis: list[dict[str, str]]) -> str:
    return hashlib.sha256(_canonical_basis_json(basis).encode("utf-8")).hexdigest()


def _class_bases(bind: sa.Connection) -> dict[str, list[dict[str, str]]]:
    """Current per-class canonical basis from artifactclass_pool + pool."""
    bases: dict[str, list[dict[str, str]]] = {}
    rows = bind.execute(
        sa.text(
            "SELECT acp.artifactclass, acp.pool_id, p.representation "
            "FROM artifactclass_pool acp JOIN pool p ON p.id = acp.pool_id "
            "WHERE acp.active ORDER BY acp.artifactclass, acp.pool_id"
        )
    )
    for artifactclass, pool_id, representation in rows:
        entry: dict[str, str] = {"pool": pool_id}
        if representation is not None:  # NULLs canonicalise as absent keys
            entry["representation"] = representation
        bases.setdefault(artifactclass, []).append(entry)
    # A class with no active memberships hashes the empty basis (callers use
    # bases.get(artifactclass, [])).
    return bases


def upgrade() -> None:
    bind = op.get_bind()

    # -- 1. bundle_member.artifactclass, backfilled from the owning bundle ----
    with op.batch_alter_table("bundle_member") as batch:
        batch.add_column(sa.Column("artifactclass", sa.String(128), nullable=True))
    op.execute(
        "UPDATE bundle_member SET artifactclass = "
        "(SELECT artifactclass FROM bundle WHERE bundle.id = bundle_member.bundle_id)"
    )
    orphan = bind.execute(
        sa.text("SELECT COUNT(*) FROM bundle_member WHERE artifactclass IS NULL")
    ).scalar()
    if orphan:
        raise RuntimeError(
            f"bundle-groups migration aborted: {orphan} bundle_member rows "
            "have no owning bundle artifactclass to backfill"
        )
    with op.batch_alter_table("bundle_member") as batch:
        batch.alter_column(
            "artifactclass", existing_type=sa.String(128), nullable=False
        )
    op.create_index(
        "ix_bundle_member_artifactclass", "bundle_member", ["artifactclass"]
    )

    # -- 2. bundle.bundle_group + group_basis, backfilled (basis_source =
    #       'backfilled': the class's *current* pool set is an honest guess) --
    with op.batch_alter_table("bundle") as batch:
        batch.add_column(sa.Column("bundle_group", sa.String(64), nullable=True))
        batch.add_column(sa.Column("group_basis", sa.JSON(), nullable=True))
    bases = _class_bases(bind)
    bundles = bind.execute(
        sa.text("SELECT id, artifactclass, target_bytes, max_age_seconds FROM bundle")
    ).fetchall()
    for bundle_id, artifactclass, target_bytes, max_age_seconds in bundles:
        basis = bases.get(artifactclass, [])
        document = {
            "basis": basis,
            "basis_source": "backfilled",
            "writer_version": _GROUP_BASIS_WRITER_VERSION,
            "effective": {
                "target_bytes": target_bytes,
                "max_age_seconds": max_age_seconds,
            },
        }
        bind.execute(
            sa.text(
                "UPDATE bundle SET bundle_group = :fp, group_basis = :doc "
                "WHERE id = :id"
            ),
            {
                "fp": _fingerprint(basis),
                "doc": json.dumps(document),
                "id": bundle_id,
            },
        )
    with op.batch_alter_table("bundle") as batch:
        batch.alter_column("bundle_group", existing_type=sa.String(64), nullable=False)
        batch.alter_column("group_basis", existing_type=sa.JSON(), nullable=False)
    op.create_index("ix_bundle_bundle_group", "bundle", ["bundle_group"])

    # -- 3. artifactclass_policy.bundle_group projection, backfilled for every
    #       existing policy row (it must never sit NULL on a live estate) -----
    with op.batch_alter_table("artifactclass_policy") as batch:
        batch.add_column(sa.Column("bundle_group", sa.String(64), nullable=True))
    for artifactclass in bind.execute(
        sa.text("SELECT artifactclass FROM artifactclass_policy")
    ).scalars():
        bind.execute(
            sa.text(
                "UPDATE artifactclass_policy SET bundle_group = :fp "
                "WHERE artifactclass = :ac"
            ),
            {"fp": _fingerprint(bases.get(artifactclass, [])), "ac": artifactclass},
        )
    op.create_index(
        "ix_artifactclass_policy_bundle_group",
        "artifactclass_policy",
        ["bundle_group"],
    )

    # -- 4. Assert no two open accumulators share a fingerprint; abort
    #       otherwise. The pre-migration operational drain (flushing every
    #       open per-class accumulator through the existing per-class path)
    #       is the steward's step, never this migration's — it never merges
    #       member sets or fakes a seal. --------------------------------------
    duplicates = bind.execute(
        sa.text(
            "SELECT bundle_group, COUNT(*) AS n FROM bundle "
            "WHERE status = 'open' AND archive_id IS NULL "
            "GROUP BY bundle_group HAVING COUNT(*) > 1"
        )
    ).fetchall()
    if duplicates:
        detail = "; ".join(f"{group}: {count} open" for group, count in duplicates)
        raise RuntimeError(
            "bundle-groups migration aborted: open accumulators share a "
            f"fingerprint ({detail}). Drain (flush) the open per-class "
            "accumulators through the existing per-class path, then rerun."
        )

    # -- 4b. pool.min_object_bytes (nullable; NULL = no floor declared) -------
    with op.batch_alter_table("pool") as batch:
        batch.add_column(sa.Column("min_object_bytes", sa.BigInteger(), nullable=True))
        batch.create_check_constraint(
            "ck_pool_min_object_bytes_non_negative",
            "min_object_bytes IS NULL OR min_object_bytes >= 0",
        )

    # -- 5. Drop bundle.artifactclass/ruleset/expect; partial unique index ----
    op.drop_index("ix_bundle_artifactclass", table_name="bundle")
    with op.batch_alter_table("bundle") as batch:
        batch.drop_column("artifactclass")
        batch.drop_column("ruleset")
        batch.drop_column("expect")
    op.create_index(
        "uq_bundle_open_accumulator_per_group",
        "bundle",
        ["bundle_group"],
        unique=True,
        sqlite_where=sa.text("status = 'open' AND archive_id IS NULL"),
        postgresql_where=sa.text("status = 'open' AND archive_id IS NULL"),
    )

    # -- 6. ck_submission_status admits 'accumulated' --------------------------
    _recreate_referenced_table(_submission_table())


def downgrade() -> None:
    bind = op.get_bind()

    _recreate_referenced_table(_submission_table(legacy_check=True))

    op.drop_index("uq_bundle_open_accumulator_per_group", table_name="bundle")
    with op.batch_alter_table("bundle") as batch:
        batch.add_column(sa.Column("artifactclass", sa.String(128), nullable=True))
        batch.add_column(sa.Column("ruleset", sa.String(256), nullable=True))
        batch.add_column(sa.Column("expect", sa.String(32), nullable=True))
    # Best-effort restore: the representative member class, else any class
    # whose projection derives the bundle's group.
    op.execute(
        "UPDATE bundle SET artifactclass = COALESCE("
        "(SELECT bm.artifactclass FROM bundle_member bm "
        " WHERE bm.bundle_id = bundle.id ORDER BY bm.id LIMIT 1), "
        "(SELECT acp.artifactclass FROM artifactclass_policy acp "
        " WHERE acp.bundle_group = bundle.bundle_group "
        " ORDER BY acp.artifactclass LIMIT 1))"
    )
    op.execute(
        "UPDATE bundle SET ruleset = "
        "(SELECT p.ruleset FROM artifactclass_policy p "
        " WHERE p.artifactclass = bundle.artifactclass), "
        "expect = "
        "(SELECT p.expect FROM artifactclass_policy p "
        " WHERE p.artifactclass = bundle.artifactclass)"
    )
    unresolved = bind.execute(
        sa.text("SELECT COUNT(*) FROM bundle WHERE artifactclass IS NULL")
    ).scalar()
    if unresolved:
        raise RuntimeError(
            f"bundle-groups downgrade aborted: {unresolved} bundles have no "
            "resolvable artifactclass"
        )
    with op.batch_alter_table("bundle") as batch:
        batch.alter_column(
            "artifactclass", existing_type=sa.String(128), nullable=False
        )
    op.create_index("ix_bundle_artifactclass", "bundle", ["artifactclass"])

    with op.batch_alter_table("pool") as batch:
        batch.drop_constraint("ck_pool_min_object_bytes_non_negative", type_="check")
        batch.drop_column("min_object_bytes")

    op.drop_index(
        "ix_artifactclass_policy_bundle_group", table_name="artifactclass_policy"
    )
    with op.batch_alter_table("artifactclass_policy") as batch:
        batch.drop_column("bundle_group")

    op.drop_index("ix_bundle_bundle_group", table_name="bundle")
    with op.batch_alter_table("bundle") as batch:
        batch.drop_column("bundle_group")
        batch.drop_column("group_basis")

    op.drop_index("ix_bundle_member_artifactclass", table_name="bundle_member")
    with op.batch_alter_table("bundle_member") as batch:
        batch.drop_column("artifactclass")


def _recreate_referenced_table(table: sa.Table) -> None:
    """Recreate a referenced table so a rewritten CHECK constraint lands.

    Mirrors the copygrain-M3 pattern: on SQLite the rebuild runs with foreign
    keys off (referencing tables keep their rows) and re-checks them after.
    The pragma is restored to its prior value — leaving it ON would make a
    later migration's table rebuild in the same invocation cascade-delete
    referencing rows when it drops the old table.
    """
    name = table.name
    context = op.get_context()
    bind = op.get_bind()
    if bind.dialect.name != "sqlite":
        with op.batch_alter_table(name, recreate="always", copy_from=table):
            pass
        return
    with context.autocommit_block():
        was_on = bool(bind.exec_driver_sql("PRAGMA foreign_keys").scalar())
        op.execute("PRAGMA foreign_keys=OFF")
        with op.batch_alter_table(name, recreate="always", copy_from=table):
            pass
        op.execute("PRAGMA foreign_key_check")
        if was_on:
            op.execute("PRAGMA foreign_keys=ON")


def _submission_table(*, legacy_check: bool = False) -> sa.Table:
    """Explicit copy_from table so SQLite batch recreate carries the CHECK."""
    statuses = (
        "('pending_archive', 'archived')"
        if legacy_check
        else "('pending_archive', 'accumulated', 'archived')"
    )
    metadata = sa.MetaData()
    return sa.Table(
        "submission",
        metadata,
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column(
            "arrangement_id",
            sa.Integer(),
            sa.ForeignKey("arrangement.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("artifactclass", sa.String(128), nullable=False),
        sa.Column("source_map_path", sa.String(4096), nullable=False),
        sa.Column("manifest_digest", sa.String(64), nullable=False),
        sa.Column("member_count", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("submitted_by", sa.String(256), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            f"status IN {statuses}",
            name="ck_submission_status",
        ),
        sa.UniqueConstraint("arrangement_id", name="uq_submission_arrangement_id"),
        sa.Index("ix_submission_arrangement_id", "arrangement_id"),
        sa.Index("ix_submission_artifactclass", "artifactclass"),
        sa.Index("ix_submission_status", "status"),
    )
