"""Rename Sutradhara object representations to the REM-* family.

The Remanence format rename was a pre-production clean break. Sutradhara's
catalog labels nevertheless retained the former object name, including inside
copy metadata and frozen bundle-group witnesses. This migration rewrites those
policy labels and recomputes every fingerprint derived from them.

Revision ID: d3e4f5a6b7c8
Revises: c2d3e4f5a6b7
Create Date: 2026-09-05
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

import sqlalchemy as sa
from alembic import op

revision: str = "d3e4f5a6b7c8"
down_revision: str | Sequence[str] | None = "c2d3e4f5a6b7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UPGRADE_NAMES = {
    "rao-plain-v1": "rem-object-v1",
    "rao-aead-v1": "rem-encrypt-v1",
}
_DOWNGRADE_NAMES = {new: old for old, new in _UPGRADE_NAMES.items()}

_pool = sa.table(
    "pool",
    sa.column("id", sa.String()),
    sa.column("representation", sa.String()),
)
_asset_locator = sa.table(
    "asset_locator",
    sa.column("id", sa.Integer()),
    sa.column("representation", sa.String()),
)
_copy = sa.table(
    "copy",
    sa.column("id", sa.Integer()),
    sa.column("storage_metadata", sa.JSON()),
)
_cache_entry = sa.table(
    "cache_entry",
    sa.column("content_sha256", sa.LargeBinary()),
    sa.column("disk_id", sa.String()),
    sa.column("size_bytes", sa.BigInteger()),
    sa.column("state", sa.String()),
    sa.column("representation", sa.String()),
    sa.column("trusted", sa.Boolean()),
    sa.column("lost_origin_disk_id", sa.String()),
    sa.column("lost_drill_id", sa.String()),
    sa.column("lost_at", sa.DateTime()),
    sa.column("refilled_at", sa.DateTime()),
)
_bundle = sa.table(
    "bundle",
    sa.column("id", sa.String()),
    sa.column("bundle_group", sa.String()),
    sa.column("group_basis", sa.JSON()),
)
_artifactclass_policy = sa.table(
    "artifactclass_policy",
    sa.column("artifactclass", sa.String()),
    sa.column("bundle_group", sa.String()),
)


def _canonical_basis_json(basis: list[dict[str, str]]) -> str:
    """Serialize a basis exactly as the runtime fingerprint contract does."""

    normalized = [
        {key: value for key, value in entry.items() if value is not None} for entry in basis
    ]
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


def _fingerprint(basis: list[dict[str, str]]) -> str:
    """Return the canonical SHA-256 bundle-group fingerprint."""

    return hashlib.sha256(_canonical_basis_json(basis).encode("utf-8")).hexdigest()


def _rewrite_json(value: Any, names: Mapping[str, str]) -> Any:
    """Recursively replace exact legacy representation values in JSON data."""

    if isinstance(value, dict):
        return {key: _rewrite_json(item, names) for key, item in value.items()}
    if isinstance(value, list):
        return [_rewrite_json(item, names) for item in value]
    if isinstance(value, str):
        return names.get(value, value)
    return value


def _basis_from_document(document: object) -> list[dict[str, str]]:
    """Return a validated canonical-basis payload from a bundle witness."""

    if not isinstance(document, dict) or not isinstance(document.get("basis"), list):
        raise RuntimeError("REM-OBJECT rename found an invalid bundle group_basis document")
    basis: list[dict[str, str]] = []
    for entry in document["basis"]:
        if not isinstance(entry, dict) or not isinstance(entry.get("pool"), str):
            raise RuntimeError("REM-OBJECT rename found an invalid bundle basis entry")
        normalized = {"pool": entry["pool"]}
        representation = entry.get("representation")
        if representation is not None:
            if not isinstance(representation, str):
                raise RuntimeError("REM-OBJECT rename found a non-string bundle representation")
            normalized["representation"] = representation
        basis.append(normalized)
    return basis


def _rewrite_scalar_columns(bind: sa.Connection, names: Mapping[str, str]) -> None:
    """Rewrite authoritative scalar representation columns."""

    for old, new in names.items():
        bind.execute(_pool.update().where(_pool.c.representation == old).values(representation=new))
        bind.execute(
            _asset_locator.update()
            .where(_asset_locator.c.representation == old)
            .values(representation=new)
        )


def _invalidate_and_rewrite_cache_entries(
    bind: sa.Connection,
    names: Mapping[str, str],
) -> None:
    """Rename cache rows and invalidate only files that may still be live."""

    for old, new in names.items():
        bind.execute(
            _cache_entry.update()
            .where(
                _cache_entry.c.representation == old,
                _cache_entry.c.state.in_(("filling", "present")),
            )
            .values(
                representation=new,
                state="lost",
                trusted=False,
                lost_origin_disk_id=_cache_entry.c.disk_id,
                lost_drill_id=None,
                lost_at=sa.func.current_timestamp(),
                refilled_at=None,
            )
        )
        bind.execute(
            _cache_entry.update()
            .where(_cache_entry.c.representation == old)
            .values(representation=new)
        )

    # Keep filled_bytes conservative while the representation-derived legacy
    # paths still occupy the disk.  The walker recognizes each row's recorded
    # path, removes it outside the unknown-file tripwire, then reconciles the
    # counter from the remaining filling/present rows.


def _rewrite_copy_metadata(bind: sa.Connection, names: Mapping[str, str]) -> None:
    """Rewrite representation labels embedded in copy storage metadata."""

    for copy_id, metadata in bind.execute(sa.select(_copy.c.id, _copy.c.storage_metadata)):
        rewritten = _rewrite_json(metadata, names)
        if rewritten != metadata:
            bind.execute(
                _copy.update().where(_copy.c.id == copy_id).values(storage_metadata=rewritten)
            )


def _class_bases(bind: sa.Connection) -> dict[str, list[dict[str, str]]]:
    """Return current canonical pool bases for artifactclass projections."""

    bases: dict[str, list[dict[str, str]]] = {}
    rows = bind.execute(
        sa.text(
            "SELECT acp.artifactclass, acp.pool_id, p.representation "
            "FROM artifactclass_pool acp JOIN pool p ON p.id = acp.pool_id "
            "WHERE acp.active"
        )
    )
    for artifactclass, pool_id, representation in rows:
        entry = {"pool": pool_id}
        if representation is not None:
            entry["representation"] = representation
        bases.setdefault(artifactclass, []).append(entry)
    for basis in bases.values():
        basis.sort(key=lambda entry: entry["pool"])
    return bases


def _rewrite_bundle_groups(bind: sa.Connection, names: Mapping[str, str]) -> None:
    """Rewrite frozen bases and every projection derived from their spelling."""

    for bundle_id, document in bind.execute(sa.select(_bundle.c.id, _bundle.c.group_basis)):
        rewritten = _rewrite_json(document, names)
        if rewritten == document:
            continue
        basis = _basis_from_document(rewritten)
        bind.execute(
            _bundle.update()
            .where(_bundle.c.id == bundle_id)
            .values(group_basis=rewritten, bundle_group=_fingerprint(basis))
        )

    bases = _class_bases(bind)
    for (artifactclass,) in bind.execute(sa.select(_artifactclass_policy.c.artifactclass)):
        bind.execute(
            _artifactclass_policy.update()
            .where(_artifactclass_policy.c.artifactclass == artifactclass)
            .values(bundle_group=_fingerprint(bases.get(artifactclass, [])))
        )


def _drop_cache_representation_constraint() -> None:
    """Drop the cache representation check before rewriting constrained rows."""

    with op.batch_alter_table("cache_entry") as batch:
        batch.drop_constraint("ck_cache_entry_representation", type_="check")


def _create_cache_representation_constraint(encrypted_name: str) -> None:
    """Recreate the cache representation check with the selected vocabulary."""

    with op.batch_alter_table("cache_entry") as batch:
        batch.create_check_constraint(
            "ck_cache_entry_representation",
            f"representation IN ('raw-bytes', '{encrypted_name}')",
        )


def _rewrite(names: Mapping[str, str], *, encrypted_name: str) -> None:
    """Apply one direction of the representation rename consistently."""

    bind = op.get_bind()
    _drop_cache_representation_constraint()
    _rewrite_scalar_columns(bind, names)
    _invalidate_and_rewrite_cache_entries(bind, names)
    _rewrite_copy_metadata(bind, names)
    _rewrite_bundle_groups(bind, names)
    _create_cache_representation_constraint(encrypted_name)


def upgrade() -> None:
    """Adopt REM-OBJECT and REM-ENCRYPT as canonical policy names."""

    _rewrite(_UPGRADE_NAMES, encrypted_name="rem-encrypt-v1")


def downgrade() -> None:
    """Restore the former pre-production policy names."""

    _rewrite(_DOWNGRADE_NAMES, encrypted_name="rao-aead-v1")
