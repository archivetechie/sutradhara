"""Bundle-group identity: fingerprint, canonical basis, effective thresholds.

A bundle group is the set of artifactclasses whose *storage placement* is
identical (design-bundle-groups §2). Its identity is derived, never declared:
the SHA-256 hex of a canonical JSON serialization of the class's sorted active
``(pool, representation)`` set — from ``artifactclass_pool`` rows with
``active = True`` joined to ``Pool.representation``, sorted by pool id,
computed from catalog rows only. Runtime and operational state
(``accepts_writes``, backend availability, ``sort_order``) never enters the
fingerprint, so a maintenance fence can never re-partition groups.

This module is the one place the fingerprint is computed. Its writers are
``apply_artifactclass_policy`` (the ``artifactclass_policy.bundle_group``
projection), ``set_pool_representation`` (which mutates a fingerprint input
outside apply), and the schema migration backfill.

Canonicalization, stated once: the basis is a JSON array of objects sorted by
pool id, each object carrying ``pool`` and ``representation`` keys; a NULL
value canonicalises as an absent key; the serialization is ``json.dumps`` with
``sort_keys=True`` and compact separators (ASCII-escaped); the fingerprint is
the SHA-256 hex digest of that text encoded as UTF-8.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    ArtifactClassPool,
    Pool,
)

GROUP_BASIS_WRITER_VERSION = 1
BASIS_SOURCE_DERIVED = "derived"
BASIS_SOURCE_BACKFILLED = "backfilled"


class BundleGroupError(Exception):
    """A bundle-group derivation failed or produced an unusable result."""


class EmptyBundleGroupError(BundleGroupError):
    """A class with no active pool placements cannot open an accumulator."""


def compute_group_basis(session: Session, artifactclass: str) -> list[dict[str, Any]]:
    """Return the canonical basis for one artifactclass from catalog rows only."""
    rows = session.execute(
        select(ArtifactClassPool.pool_id, Pool.representation)
        .join(Pool, Pool.id == ArtifactClassPool.pool_id)
        .where(
            ArtifactClassPool.artifactclass == artifactclass,
            ArtifactClassPool.active.is_(True),
        )
    ).all()
    basis: list[dict[str, Any]] = []
    # The canonical order is Python codepoint sort, never a SQL ORDER BY —
    # collation-independent by construction (F9); for ASCII pool ids it equals
    # the previous BINARY-collation order, so no fingerprint changes (parity
    # test pins the golden value across both code paths).
    for pool_id, representation in sorted(rows, key=lambda row: row[0]):
        entry: dict[str, Any] = {"pool": pool_id}
        # NULLs canonicalise as absent keys (design §7.7).
        if representation is not None:
            entry["representation"] = representation
        basis.append(entry)
    return basis


def canonical_basis_json(basis: list[dict[str, Any]]) -> str:
    """Serialize a basis deterministically; this text is what the fingerprint hashes."""
    return json.dumps(
        [{key: value for key, value in entry.items() if value is not None} for entry in basis],
        sort_keys=True,
        separators=(",", ":"),
    )


def fingerprint_basis(basis: list[dict[str, Any]]) -> str:
    """Return the SHA-256 hex fingerprint of a canonical basis."""
    return hashlib.sha256(canonical_basis_json(basis).encode("utf-8")).hexdigest()


def compute_bundle_group(
    session: Session,
    artifactclass: str,
) -> tuple[str, list[dict[str, Any]]]:
    """Return ``(fingerprint, basis)`` for one artifactclass, live from the catalog."""
    basis = compute_group_basis(session, artifactclass)
    return fingerprint_basis(basis), basis


def group_basis_document(
    basis: list[dict[str, Any]],
    *,
    basis_source: str,
    target_bytes: int,
    max_age_seconds: int,
) -> dict[str, Any]:
    """Build the per-bundle ``group_basis`` witness document.

    The document carries the canonical serialization the fingerprint hashes,
    provenance (``basis_source`` + writer version), and the effective
    thresholds frozen at open — so a sealed bundle stays self-describing after
    policies drift. The typed columns remain authoritative for ``bundle_due``;
    the open-time assert enforces they equal this witness.
    """
    if basis_source not in {BASIS_SOURCE_DERIVED, BASIS_SOURCE_BACKFILLED}:
        raise BundleGroupError(f"unknown basis_source {basis_source!r}")
    return {
        "basis": basis,
        "basis_source": basis_source,
        "writer_version": GROUP_BASIS_WRITER_VERSION,
        "effective": {
            "target_bytes": target_bytes,
            "max_age_seconds": max_age_seconds,
        },
    }


@dataclass(frozen=True)
class GroupThresholds:
    """The threshold arithmetic for one bundle group, with its inputs shown.

    The open path wants only the two effective numbers; the policy-apply
    report wants the arithmetic that produced them — which classes declared
    what, which pools declared a floor, and whether the floor clamp fired. Both
    read this one computation, so the report can never drift from what the
    accumulator actually freezes.
    """

    declared: dict[str, tuple[int, int]]
    """Class -> (declared target_bytes, declared max_age_seconds)."""
    declared_target_bytes: int
    """Max-over-classes target, *before* the generation-floor clamp."""
    max_age_seconds: int
    """Min-over-classes latency ceiling."""
    pool_floors: dict[str, int | None]
    """Basis pool -> declared ``min_object_bytes``; ``None`` = no floor declared."""
    target_bytes: int
    """Effective target after clamping up to the strictest declared floor."""

    @property
    def clamp_active(self) -> bool:
        """True when a pool floor lifted the declared max-over-classes target."""
        return self.target_bytes > self.declared_target_bytes

    @property
    def clamping_pools(self) -> list[str]:
        """Basis pools whose declared floor sits above the declared target.

        §2 warns **when the clamp activates**, on the declared value: after
        clamping the effective target can never undershoot, so a warning keyed
        on the effective value would be unreachable.
        """
        return sorted(
            pool_id
            for pool_id, floor in self.pool_floors.items()
            if floor is not None and floor > self.declared_target_bytes
        )

    @property
    def floorless_pools(self) -> list[str]:
        """Basis pools with no declared floor — never an implicit zero (§3)."""
        return sorted(pool_id for pool_id, floor in self.pool_floors.items() if floor is None)


def group_thresholds(
    session: Session,
    *,
    artifactclass: str,
    policy: ArtifactClassPolicyRecord,
    fingerprint: str,
    basis: list[dict[str, Any]],
) -> GroupThresholds:
    """Compute one group's threshold arithmetic from catalog rows.

    The declared class set is every ``artifactclass_policy`` row whose
    ``bundle_group`` projection equals this fingerprint, **unioned with the
    opening class's live-derived row** so "includes the opener by
    construction" holds even if a projection writer was ever missed.
    ``max_age_seconds`` is a latency ceiling — the group takes the minimum;
    ``target_bytes`` is an accumulation goal — the group takes the maximum,
    clamped up by the strictest member pool's declared ``min_object_bytes``.
    """
    declared: dict[str, tuple[int, int]] = {}
    for row in session.scalars(
        select(ArtifactClassPolicyRecord).where(
            ArtifactClassPolicyRecord.bundle_group == fingerprint
        )
    ):
        declared[row.artifactclass] = (row.target_bytes, row.max_age_seconds)
    # Opener-union: the opening class is in the set by construction, from its
    # live-derived fingerprint, even when its stored projection is stale — so
    # the declared set is never empty here (F7 removed the dead empty check).
    declared[artifactclass] = (policy.target_bytes, policy.max_age_seconds)
    declared_target_bytes = max(target for target, _ in declared.values())
    max_age_seconds = min(age for _, age in declared.values())

    pool_ids = [str(entry["pool"]) for entry in basis]
    pool_floors: dict[str, int | None] = {pool_id: None for pool_id in pool_ids}
    if pool_ids:
        for pool_id, floor in session.execute(
            select(Pool.id, Pool.min_object_bytes).where(Pool.id.in_(pool_ids))
        ).all():
            pool_floors[str(pool_id)] = floor
    floors = [floor for floor in pool_floors.values() if floor is not None]
    target_bytes = max(declared_target_bytes, max(floors)) if floors else declared_target_bytes

    return GroupThresholds(
        declared=declared,
        declared_target_bytes=declared_target_bytes,
        max_age_seconds=max_age_seconds,
        pool_floors=pool_floors,
        target_bytes=target_bytes,
    )


def effective_group_thresholds(
    session: Session,
    *,
    artifactclass: str,
    policy: ArtifactClassPolicyRecord,
    fingerprint: str,
    basis: list[dict[str, Any]],
) -> tuple[int, int]:
    """Return the effective ``(target_bytes, max_age_seconds)`` for a group open.

    The open path's view of :func:`group_thresholds`. An empty basis is an
    error; zero thresholds are never written.
    """
    if not basis:
        raise EmptyBundleGroupError(
            f"artifactclass {artifactclass!r} has no active pool placements; "
            "an accumulator cannot open for an empty bundle group"
        )
    thresholds = group_thresholds(
        session,
        artifactclass=artifactclass,
        policy=policy,
        fingerprint=fingerprint,
        basis=basis,
    )
    if thresholds.target_bytes <= 0 or thresholds.max_age_seconds <= 0:
        raise BundleGroupError(
            f"bundle group {fingerprint!r} derived zero thresholds "
            f"(target_bytes={thresholds.target_bytes}, "
            f"max_age_seconds={thresholds.max_age_seconds}); "
            "zero thresholds are never written"
        )
    return thresholds.target_bytes, thresholds.max_age_seconds


def basis_entries(group_basis: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return the canonical ``(pool, representation)`` entries of a frozen witness.

    The per-bundle ``group_basis`` document is the placement promised at open;
    entries come back in canonical basis order, which is also the declared
    fan-out order for a group bundle (§2/§5).
    """
    entries = (group_basis or {}).get("basis") or []
    return [entry for entry in entries if isinstance(entry, dict) and "pool" in entry]


def basis_pool_ids(group_basis: dict[str, Any] | None) -> list[str]:
    """Return the pool ids of a frozen ``group_basis`` witness, in basis order."""
    return [str(entry["pool"]) for entry in basis_entries(group_basis)]


def refresh_bundle_group_projections(
    session: Session,
    artifactclasses: list[str],
) -> None:
    """Recompute the ``artifactclass_policy.bundle_group`` projection for classes.

    Called by every path that can change a fingerprint input. Classes without
    an applied policy record are skipped — there is no projection row to write.
    """
    for artifactclass in artifactclasses:
        record = session.get(ArtifactClassPolicyRecord, artifactclass)
        if record is None:
            continue
        record.bundle_group = fingerprint_basis(compute_group_basis(session, artifactclass))
    session.flush()
