"""Shared helpers for bundle-group-era tests.

Direct ORM ``Bundle(...)`` constructions need the group identity columns the
class column used to provide. ``bundle_kwargs`` builds a consistent
``bundle_group``/``group_basis`` pair from a basis (or a seed string when the
group identity is incidental to the test — two open accumulators must never
share a fingerprint, so seeding with the bundle id keeps fixtures distinct).
"""

from __future__ import annotations

from typing import Any

from sutradhara.bundle_group import (
    BASIS_SOURCE_DERIVED,
    fingerprint_basis,
    group_basis_document,
)


def bundle_kwargs(
    *,
    basis: list[dict[str, Any]] | None = None,
    seed: str | None = None,
    target_bytes: int = 0,
    max_age_seconds: int = 0,
) -> dict[str, Any]:
    """Return the ``bundle_group``/``group_basis`` kwargs for a test Bundle."""
    if basis is None:
        basis = [] if seed is None else [{"pool": seed}]
    return {
        "bundle_group": fingerprint_basis(basis),
        "group_basis": group_basis_document(
            basis,
            basis_source=BASIS_SOURCE_DERIVED,
            target_bytes=target_bytes,
            max_age_seconds=max_age_seconds,
        ),
    }
