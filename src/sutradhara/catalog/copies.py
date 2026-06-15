"""Catalog copy APIs for recording and querying materialized assets.

`add_copy` is the single, content-addressed funnel through which every Copy
row is created, shared by the reconciliation scrub and the future write/ingest
path. Backend selection and provenance (`source`, `health`) are the caller's
responsibility, passed in as parameters; this module owns only the copy-row
identity and idempotency rules.

Transaction ownership stays with the caller's session scope: these helpers
flush but never commit.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, TypedDict

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import Bundle, Copy, LogicalAsset
from sutradhara.catalog.session import locator_key
from sutradhara.catalog.types import CopyHealth, CopySource, is_content_hash


class CatalogError(Exception):
    """Base class for catalog-layer errors."""


class UnknownLogicalAsset(CatalogError):
    """The given content hash does not name a registered LogicalAsset."""


class UnknownBundle(CatalogError):
    """The given bundle id does not name a registered Bundle."""


class CopyPoolMismatch(CatalogError):
    """An existing backend locator is already associated with another pool."""


class CopyLookup(TypedDict):
    locator: dict[str, Any]
    integrity_hash: bytes
    health: str
    backend: str


class CopyLookupResult(TypedDict):
    id: bytes
    copies: list[CopyLookup]


def add_copy(
    session: Session,
    *,
    logical_asset_hash: bytes,
    backend_id: int,
    pool_id: str | None = None,
    native_locator: dict[str, Any],
    integrity_hash: bytes,
    source: CopySource,
    health: CopyHealth = CopyHealth.OK,
    storage_metadata: dict[str, Any] | None = None,
    last_verified_at: dt.datetime | None = None,
    first_observed_at: dt.datetime | None = None,
) -> tuple[Copy, bool]:
    """Record one Copy of an existing asset on a backend.

    Idempotent on `(backend_id, native_locator_key)`. Returns `(copy, created)`:
    `created=True` when a new row was inserted; `False` when a copy already
    existed for that backend+locator, in which case the existing row is
    returned **unmutated**.

    Backend selection is the caller's job - pass a resolved `backend_id`.
    `source` is required (declare provenance); `health` defaults to OK. Extra
    `storage_metadata` is non-authoritative representation/geometry context,
    not part of the locator identity. `first_observed_at` / `last_verified_at`
    fall back to the model defaults when None.

    Raises `UnknownLogicalAsset` if `logical_asset_hash` names no asset.
    Does not commit; the caller owns the transaction.
    """
    _require_logical_asset(session, logical_asset_hash, field_name="logical_asset_hash")

    key = locator_key(native_locator)
    existing = session.scalars(
        select(Copy).where(
            Copy.backend_id == backend_id,
            Copy.native_locator_key == key,
        )
    ).one_or_none()
    if existing is not None:
        if pool_id is not None and existing.pool_id != pool_id:
            raise CopyPoolMismatch(
                f"copy id={existing.id} locator already belongs to pool "
                f"{existing.pool_id!r}, not {pool_id!r}"
            )
        return existing, False

    copy = Copy(
        logical_asset_hash=logical_asset_hash,
        backend_id=backend_id,
        pool_id=pool_id,
        native_locator=native_locator,
        native_locator_key=key,
        storage_metadata=storage_metadata or {},
        integrity_hash=integrity_hash,
        source=source,
        health=health,
    )
    if last_verified_at is not None:
        copy.last_verified_at = last_verified_at
    if first_observed_at is not None:
        copy.first_observed_at = first_observed_at

    session.add(copy)
    session.flush()
    return copy, True


def add_bundle_copy(
    session: Session,
    *,
    bundle_id: str,
    backend_id: int,
    pool_id: str,
    native_locator: dict[str, Any],
    integrity_hash: bytes,
    source: CopySource,
    health: CopyHealth = CopyHealth.OK,
    storage_metadata: dict[str, Any] | None = None,
    last_verified_at: dt.datetime | None = None,
    first_observed_at: dt.datetime | None = None,
) -> tuple[Copy, bool]:
    """Record one materialized bundle copy on a backend pool.

    Bundle copies deliberately use ``Copy.bundle_id`` instead of
    ``Copy.logical_asset_hash``. Per-asset restore goes through ``asset_locator``
    rows that point at this copy.
    """
    if session.get(Bundle, bundle_id) is None:
        raise UnknownBundle(f"no Bundle with id {bundle_id!r}")

    key = locator_key(native_locator)
    existing = session.scalars(
        select(Copy).where(
            Copy.backend_id == backend_id,
            Copy.native_locator_key == key,
        )
    ).one_or_none()
    if existing is not None:
        if existing.bundle_id != bundle_id:
            raise CopyPoolMismatch(
                f"copy id={existing.id} locator already belongs to bundle "
                f"{existing.bundle_id!r}, not {bundle_id!r}"
            )
        if existing.pool_id != pool_id:
            raise CopyPoolMismatch(
                f"copy id={existing.id} locator already belongs to pool "
                f"{existing.pool_id!r}, not {pool_id!r}"
            )
        return existing, False

    copy = Copy(
        bundle_id=bundle_id,
        backend_id=backend_id,
        pool_id=pool_id,
        native_locator=native_locator,
        native_locator_key=key,
        storage_metadata=storage_metadata or {},
        integrity_hash=integrity_hash,
        source=source,
        health=health,
    )
    if last_verified_at is not None:
        copy.last_verified_at = last_verified_at
    if first_observed_at is not None:
        copy.first_observed_at = first_observed_at

    session.add(copy)
    session.flush()
    return copy, True


def lookup_by_hash(session: Session, content_hash: bytes) -> CopyLookupResult:
    """Return an asset lookup document with all known copies ordered by copy id."""
    _require_logical_asset(session, content_hash, field_name="content_hash")

    copies = [
        CopyLookup(
            locator=copy.native_locator,
            integrity_hash=copy.integrity_hash,
            health=_health_value(copy.health),
            backend=copy.backend.name,
        )
        for copy in session.scalars(
            select(Copy).where(Copy.logical_asset_hash == content_hash).order_by(Copy.id)
        )
    ]
    return CopyLookupResult(id=content_hash, copies=copies)


def _require_logical_asset(
    session: Session,
    asset_id: bytes,
    *,
    field_name: str,
) -> LogicalAsset:
    if not is_content_hash(asset_id):
        raise ValueError(
            f"{field_name} must be a 32-byte SHA-256 content hash; "
            f"got {len(asset_id) if isinstance(asset_id, bytes) else type(asset_id)!r}"
        )

    asset = session.get(LogicalAsset, asset_id)
    if asset is None:
        raise UnknownLogicalAsset(
            f"no LogicalAsset with content hash {asset_id.hex()}; "
            "register the asset before recording or looking up copies"
        )
    return asset


def _health_value(health: CopyHealth | str) -> str:
    if isinstance(health, CopyHealth):
        return health.value
    return health
