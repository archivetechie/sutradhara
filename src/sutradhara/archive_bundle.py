"""Catalog helpers for durable archive bundle bookkeeping."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import (
    AssetLocator,
    BlobRoot,
    Bundle,
    BundleMember,
    ExclusionRecord,
    LogicalAsset,
    Pool,
)


class ArchiveBundleError(Exception):
    """Base class for archive bundle catalog errors."""


class UnknownBundleAsset(ArchiveBundleError):
    """A bundle operation referenced an unknown logical asset."""


class UnknownBundlePool(ArchiveBundleError):
    """A locator operation referenced an unknown pool."""


def get_or_create_open_bundle(
    session: Session,
    *,
    artifactclass: str,
    representation: str,
    bundle_id: str | None = None,
) -> tuple[Bundle, bool]:
    """Return the open bundle for an artifactclass/representation pair.

    If ``bundle_id`` is supplied it is used for a newly-created bundle. Without
    it, a synthetic id is generated.
    """
    existing = session.scalars(
        select(Bundle)
        .where(
            Bundle.artifactclass == artifactclass,
            Bundle.representation == representation,
            Bundle.status == "open",
        )
        .order_by(Bundle.created_at, Bundle.id)
    ).first()
    if existing is not None:
        return existing, False

    bundle = Bundle(
        id=bundle_id or f"bundle-{uuid.uuid4().hex}",
        artifactclass=artifactclass,
        representation=representation,
    )
    session.add(bundle)
    session.flush()
    return bundle, True


def close_bundle(session: Session, bundle: Bundle) -> Bundle:
    """Mark an open bundle as closed."""
    bundle.status = "closed"
    session.flush()
    return bundle


def add_bundle_member(
    session: Session,
    *,
    bundle: Bundle,
    logical_asset_hash: bytes,
    member_path: str,
    size_bytes: int,
    file_sha256: bytes,
) -> tuple[BundleMember, bool]:
    """Add a logical asset to a bundle and update bundle byte totals."""
    if session.get(LogicalAsset, logical_asset_hash) is None:
        raise UnknownBundleAsset(
            f"no LogicalAsset with content hash {logical_asset_hash.hex()}"
        )
    existing = session.scalars(
        select(BundleMember).where(
            BundleMember.bundle_id == bundle.id,
            BundleMember.logical_asset_hash == logical_asset_hash,
        )
    ).one_or_none()
    if existing is not None:
        return existing, False

    member = BundleMember(
        bundle_id=bundle.id,
        logical_asset_hash=logical_asset_hash,
        member_path=member_path,
        size_bytes=size_bytes,
        file_sha256=file_sha256,
    )
    bundle.total_bytes += size_bytes
    session.add(member)
    session.flush()
    return member, True


def record_asset_locator(
    session: Session,
    *,
    logical_asset_hash: bytes,
    pool_id: str,
    native_locator: dict[str, Any],
    representation: str,
    copy_id: int | None = None,
    bundle_id: str | None = None,
) -> AssetLocator:
    """Record a concrete locator for an asset, including bundle locators."""
    if session.get(LogicalAsset, logical_asset_hash) is None:
        raise UnknownBundleAsset(
            f"no LogicalAsset with content hash {logical_asset_hash.hex()}"
        )
    if session.get(Pool, pool_id) is None:
        raise UnknownBundlePool(f"no Pool with id {pool_id!r}")
    locator = AssetLocator(
        logical_asset_hash=logical_asset_hash,
        pool_id=pool_id,
        native_locator=native_locator,
        representation=representation,
        copy_id=copy_id,
        bundle_id=bundle_id,
    )
    session.add(locator)
    session.flush()
    return locator


def record_blob_root(
    session: Session,
    *,
    logical_asset_hash: bytes,
    algorithm: str,
    root_hash: bytes,
) -> BlobRoot:
    """Record a blob root for an asset and algorithm."""
    if session.get(LogicalAsset, logical_asset_hash) is None:
        raise UnknownBundleAsset(
            f"no LogicalAsset with content hash {logical_asset_hash.hex()}"
        )
    root = BlobRoot(
        logical_asset_hash=logical_asset_hash,
        algorithm=algorithm,
        root_hash=root_hash,
    )
    session.add(root)
    session.flush()
    return root


def record_exclusion(
    session: Session,
    *,
    artifactclass: str,
    reason: str,
    logical_asset_hash: bytes | None = None,
    path: str | None = None,
    detail: dict[str, Any] | None = None,
) -> ExclusionRecord:
    """Record why a candidate was excluded from archive bundling."""
    exclusion = ExclusionRecord(
        artifactclass=artifactclass,
        reason=reason,
        logical_asset_hash=logical_asset_hash,
        path=path,
        detail=detail,
    )
    session.add(exclusion)
    session.flush()
    return exclusion
