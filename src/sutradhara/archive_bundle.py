"""Durable RAO archive bundle bookkeeping.

This module owns the sutradhara-side accumulator state: an open bundle per
artifactclass, its pending member set, flush thresholds copied from the applied
artifactclass policy, per-copy asset locators, blob-root pointers, exclusion
records, and held-bundle review decisions.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import uuid
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    AssetLocator,
    BlobRoot,
    Bundle,
    BundleMember,
    Copy,
    ExclusionRecord,
    LogicalAsset,
    Pool,
    ReviewDecision,
    StagingTransform,
)
from sutradhara.catalog.types import is_content_hash
from sutradhara.member_name import escape_path_name, escape_path_text


class ArchiveBundleError(Exception):
    """Base class for archive bundle catalog errors."""


class UnknownBundleAsset(ArchiveBundleError):
    """A bundle operation referenced an unknown logical asset."""


class UnknownBundlePool(ArchiveBundleError):
    """A locator operation referenced an unknown pool."""


class UnknownBundleCopy(ArchiveBundleError):
    """A locator operation referenced an unknown copy."""


class AssetLocatorError(ArchiveBundleError):
    """An asset locator is malformed."""


class BundleStateError(ArchiveBundleError):
    """A bundle operation was requested in the wrong lifecycle state."""


class StagingTransformError(ArchiveBundleError):
    """A staging transform record is inconsistent with its bundle member."""


def get_or_create_open_bundle(
    session: Session,
    *,
    artifactclass: str,
    policy: ArtifactClassPolicyRecord,
    bundle_id: str | None = None,
    now: dt.datetime | None = None,
) -> tuple[Bundle, bool]:
    """Return the durable open accumulator for an artifactclass."""
    existing = session.scalars(
        select(Bundle)
        .where(Bundle.artifactclass == artifactclass, Bundle.status == "open")
        .order_by(Bundle.opened_at, Bundle.id)
    ).first()
    if existing is not None:
        return existing, False

    bundle = Bundle(
        id=bundle_id or f"bundle-{uuid.uuid4().hex}",
        artifactclass=artifactclass,
        status="open",
        target_bytes=policy.target_bytes,
        max_age_seconds=policy.max_age_seconds,
        ruleset=policy.ruleset,
        expect=policy.expect,
        opened_at=now or dt.datetime.now(dt.UTC),
    )
    session.add(bundle)
    session.flush()
    return bundle, True


def enqueue_artifact(
    session: Session,
    *,
    artifactclass: str,
    policy: ArtifactClassPolicyRecord,
    logical_asset_hash: bytes,
    source_path: Path | str,
    member_path: str | None = None,
    member_path_is_escaped: bool = False,
    bundle_id: str | None = None,
    now: dt.datetime | None = None,
    source_metadata: dict[str, Any] | None = None,
) -> tuple[Bundle, BundleMember, bool]:
    """Add one asset to the open accumulator for ``artifactclass``."""
    _require_asset(session, logical_asset_hash)
    source = Path(source_path)
    size_bytes = source.stat().st_size
    if member_path is None:
        path_in_bundle = escape_path_name(source)
    elif member_path_is_escaped:
        path_in_bundle = member_path
    else:
        path_in_bundle = escape_path_text(member_path)
    source_path_text = str(source)
    stored_source_path: str | None = source_path_text
    metadata = dict(source_metadata or {})
    try:
        source_path_text.encode("utf-8")
    except UnicodeEncodeError:
        metadata["source_path_bytes_hex"] = os.fsencode(source).hex()
        stored_source_path = None
    bundle, _ = get_or_create_open_bundle(
        session,
        artifactclass=artifactclass,
        policy=policy,
        bundle_id=bundle_id,
        now=now,
    )
    member, created = add_bundle_member(
        session,
        bundle=bundle,
        logical_asset_hash=logical_asset_hash,
        member_path=path_in_bundle,
        source_path=stored_source_path,
        size_bytes=size_bytes,
        file_sha256=_sha256_file(source),
        source_metadata=metadata or None,
    )
    return bundle, member, created


def bundle_due(
    bundle: Bundle,
    *,
    now: dt.datetime | None = None,
    force: bool = False,
) -> bool:
    """Return whether an open bundle should be flushed."""
    if bundle.status != "open":
        return False
    if force:
        return bundle.member_count > 0
    if bundle.member_count == 0:
        return False
    if bundle.target_bytes and bundle.total_bytes >= bundle.target_bytes:
        return True
    if not bundle.max_age_seconds:
        return False
    reference = now or dt.datetime.now(dt.UTC)
    return (reference - bundle.opened_at).total_seconds() >= bundle.max_age_seconds


def close_bundle(session: Session, bundle: Bundle) -> Bundle:
    """Mark a bundle as sealed after all copy materialisations are verified."""
    bundle.status = "sealed"
    bundle.sealed_at = dt.datetime.now(dt.UTC)
    session.flush()
    return bundle


def hold_bundle(
    session: Session,
    bundle: Bundle,
    *,
    summary: dict[str, Any],
) -> Bundle:
    """Mark a bundle as held for human review."""
    bundle.status = "held"
    bundle.held_at = dt.datetime.now(dt.UTC)
    bundle.review_summary = summary
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
    source_path: str | None = None,
    source_metadata: dict[str, Any] | None = None,
) -> tuple[BundleMember, bool]:
    """Add a logical asset to a bundle and update accumulator totals."""
    if bundle.status != "open":
        raise BundleStateError(f"bundle {bundle.id!r} is not open")
    _require_asset(session, logical_asset_hash)
    existing = session.scalars(
        select(BundleMember).where(
            BundleMember.bundle_id == bundle.id,
            BundleMember.member_path == member_path,
        )
    ).one_or_none()
    if existing is not None:
        return existing, False

    member = BundleMember(
        bundle_id=bundle.id,
        logical_asset_hash=logical_asset_hash,
        member_path=member_path,
        source_path=source_path,
        size_bytes=size_bytes,
        file_sha256=file_sha256,
        source_metadata=source_metadata,
    )
    bundle.total_bytes += size_bytes
    bundle.member_count += 1
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
    copy_id: int,
    bundle_id: str,
    member_path: str | None = None,
) -> AssetLocator:
    """Record a concrete per-copy locator for an asset in a bundle."""
    _require_asset(session, logical_asset_hash)
    if session.get(Pool, pool_id) is None:
        raise UnknownBundlePool(f"no Pool with id {pool_id!r}")
    copy = session.get(Copy, copy_id)
    if copy is None:
        raise UnknownBundleCopy(f"no Copy with id={copy_id}")
    resolved_member_path = member_path or native_locator.get("member_path")
    if not isinstance(resolved_member_path, str) or not resolved_member_path:
        raise AssetLocatorError("asset locator requires a non-empty member_path")
    locator = AssetLocator(
        logical_asset_hash=logical_asset_hash,
        pool_id=pool_id,
        native_locator=native_locator,
        member_path=resolved_member_path,
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
    bundle_id: str,
    copy_id: int,
    pool_id: str,
    root_path: str,
    native_locator: dict[str, Any],
    archive_id: str | None = None,
) -> BlobRoot:
    """Record a coarse blob-root pointer for single-file restore from blobs."""
    if session.get(Bundle, bundle_id) is None:
        raise BundleStateError(f"no Bundle with id {bundle_id!r}")
    if session.get(Copy, copy_id) is None:
        raise UnknownBundleCopy(f"no Copy with id={copy_id}")
    if session.get(Pool, pool_id) is None:
        raise UnknownBundlePool(f"no Pool with id {pool_id!r}")
    root = BlobRoot(
        bundle_id=bundle_id,
        copy_id=copy_id,
        pool_id=pool_id,
        root_path=root_path,
        native_locator=native_locator,
        archive_id=archive_id,
    )
    session.add(root)
    session.flush()
    return root


def record_staging_transform(
    session: Session,
    *,
    member: BundleMember,
    artifactclass: str,
    step_order: int,
    kind: str,
    reversible: bool,
    original_member_path: str,
    stored_member_path: str,
    original_size_bytes: int,
    stored_size_bytes: int,
    original_sha256: bytes,
    stored_sha256: bytes,
    parameters: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
    is_final: bool = True,
) -> StagingTransform:
    """Record one ordered staging transform for a bundle member."""
    if is_final and stored_member_path != member.member_path:
        raise StagingTransformError(
            f"transform stored path {stored_member_path!r} does not match "
            f"bundle member {member.member_path!r}"
        )
    if step_order == 0 and original_sha256 != member.logical_asset_hash:
        raise StagingTransformError(
            "first transform original_sha256 must match the member logical asset hash"
        )
    if is_final and stored_sha256 != member.file_sha256:
        raise StagingTransformError(
            f"transform stored sha256 for {stored_member_path!r} does not match "
            "bundle member file_sha256"
        )
    transform = StagingTransform(
        bundle_member_id=member.id,
        bundle_id=member.bundle_id,
        logical_asset_hash=member.logical_asset_hash,
        artifactclass=artifactclass,
        step_order=step_order,
        kind=kind,
        reversible=reversible,
        original_member_path=original_member_path,
        stored_member_path=stored_member_path,
        original_size_bytes=original_size_bytes,
        stored_size_bytes=stored_size_bytes,
        original_sha256=original_sha256,
        stored_sha256=stored_sha256,
        parameters=parameters or {},
        result=result or {},
    )
    session.add(transform)
    session.flush()
    return transform


def record_exclusion(
    session: Session,
    *,
    artifactclass: str,
    reason: str,
    bundle_id: str | None = None,
    logical_asset_hash: bytes | None = None,
    path: str | None = None,
    count: int = 1,
    bytes_total: int = 0,
    ruleset_name: str | None = None,
    ruleset_hash: str | None = None,
    detail: dict[str, Any] | None = None,
) -> ExclusionRecord:
    """Record why a candidate was excluded from archive bundling."""
    if logical_asset_hash is not None:
        _require_asset(session, logical_asset_hash)
    exclusion = ExclusionRecord(
        bundle_id=bundle_id,
        artifactclass=artifactclass,
        reason=reason,
        logical_asset_hash=logical_asset_hash,
        path=path,
        count=count,
        bytes_total=bytes_total,
        ruleset_name=ruleset_name,
        ruleset_hash=ruleset_hash,
        detail=detail,
    )
    session.add(exclusion)
    session.flush()
    return exclusion


def record_review_decision(
    session: Session,
    *,
    bundle_id: str,
    action: str,
    scope: str,
    subtree: str | None = None,
    reason: str | None = None,
    reviewer: str | None = None,
    persisted_rule: dict[str, Any] | None = None,
) -> ReviewDecision:
    """Record a held-bundle human review decision."""
    bundle = session.get(Bundle, bundle_id)
    if bundle is None:
        raise BundleStateError(f"no Bundle with id {bundle_id!r}")
    decision = ReviewDecision(
        bundle_id=bundle_id,
        action=action,
        scope=scope,
        subtree=subtree,
        reason=reason,
        reviewer=reviewer,
        persisted_rule=persisted_rule,
    )
    session.add(decision)
    session.flush()
    return decision


def _require_asset(session: Session, logical_asset_hash: bytes) -> LogicalAsset:
    if not is_content_hash(logical_asset_hash):
        raise ValueError("logical_asset_hash must be a 32-byte SHA-256 hash")
    asset = session.get(LogicalAsset, logical_asset_hash)
    if asset is None:
        raise UnknownBundleAsset(f"no LogicalAsset with content hash {logical_asset_hash.hex()}")
    return asset


def _sha256_file(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()
