"""Durability predicates shared by replication, retention, and bundle status.

The catalog records durable bytes at two grains: whole-asset ``Copy`` rows and
bundle-scoped ``Copy`` rows reached through ``AssetLocator``. This module keeps
that grain choice explicit so callers can ask for the accounting view
(``durable_placements``) or the physical whole-copy restore view
(``direct_copies``) without re-implementing subtly different health predicates.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict

from sqlalchemy import and_, false, func, or_, select
from sqlalchemy.orm import Session, joinedload

from sutradhara.backend.port import BackendLocator, ByteRange, StorageBackend, VerifyResult
from sutradhara.catalog.models import (
    ArtifactClassPool,
    AssetLocator,
    Backend,
    Bundle,
    BundleMember,
    Copy,
    IngestItem,
    Intake,
    Pool,
    VirtualArrangementMember,
)
from sutradhara.catalog.types import CopyHealth, IntakeStatus
from sutradhara.replication import PoolTarget, target_pools


@dataclass(frozen=True)
class AssetTarget:
    """A logical asset viewed under one artifactclass."""

    asset_hash: bytes
    artifactclass: str


@dataclass(frozen=True)
class BundleTarget:
    """A sealed bundle viewed as the durability target."""

    bundle_id: str


Target = AssetTarget | BundleTarget


@dataclass(frozen=True)
class PlacementTarget(PoolTarget):
    """One wanted pool target annotated with realized-copy status."""

    have: bool = False
    duplicate_count: int = 0
    is_duplicate: bool = False


class PlacementStatus(TypedDict):
    complete: bool
    have: set[PlacementTarget]
    want: set[PlacementTarget]
    missing: set[PlacementTarget]


BundlePlacementCounts = dict[str, dict[tuple[int, str], int]]


class DurabilityMediaIdentityError(ValueError):
    """A copy lacks the media identity required for its implementation family."""


@dataclass(frozen=True)
class CopyMediaIdentity:
    """Per-family media identity used for durability diversity checks."""

    family: str
    media_id: str


@dataclass(frozen=True)
class BundleCopyAggregate:
    """Healthy bundle-copy facts grouped for one bundle."""

    counts: dict[tuple[int, str], int]
    family_by_key: dict[tuple[int, str], str]
    media_identity_by_key: dict[tuple[int, str], CopyMediaIdentity]
    media_errors: tuple[str, ...]

    @property
    def duplicate_keys(self) -> tuple[tuple[int, str, int], ...]:
        return tuple(
            (backend_id, pool_id, count)
            for (backend_id, pool_id), count in sorted(self.counts.items())
            if count > 1
        )


class _TargetBackend(StorageBackend):
    """Read-only placeholder used to expand PoolTarget rows through target_pools."""

    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def enumerate(self) -> Any:
        return iter(())

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        raise NotImplementedError

    def verify(self, locator: BackendLocator) -> VerifyResult:
        raise NotImplementedError


def durable_placements(
    session: Session,
    target: Target,
    *,
    require_verified: bool,
    artifactclass: str | None,
    pool_id: str | None = None,
) -> list[Copy]:
    """Return healthy durability-accounting copies for an asset or bundle target.

    For an ``AssetTarget`` this is asset-grain copies plus bundle copies reached
    through ``AssetLocator`` rows, de-duplicated by ``Copy.id``. The
    ``artifactclass`` argument filters the bundle-locator leg; ``pool_id``
    applies the pool predicate at the grain where each copy is proven.
    """
    if isinstance(target, BundleTarget):
        query = select(Copy).where(
            Copy.bundle_id == target.bundle_id,
            Copy.health == CopyHealth.OK,
            Copy.deleted_at.is_(None),
        )
        if pool_id is not None:
            query = query.where(Copy.pool_id == pool_id)
        if require_verified:
            query = _measurement_filter(query)
        return list(session.scalars(query.order_by(Copy.id)))

    asset_query = _copy_health_filter(
        select(Copy).where(Copy.logical_asset_hash == target.asset_hash),
        require_verified=require_verified,
    )
    if pool_id is not None:
        asset_query = asset_query.where(Copy.pool_id == pool_id)
    asset_copies = list(session.scalars(asset_query.order_by(Copy.id)))
    bundle_query = (
        select(Copy)
        .join(AssetLocator, AssetLocator.copy_id == Copy.id)
        .where(AssetLocator.logical_asset_hash == target.asset_hash)
    )
    bundle_query = _copy_health_filter(bundle_query, require_verified=require_verified)
    if pool_id is not None:
        bundle_query = bundle_query.where(
            AssetLocator.pool_id == pool_id,
            Copy.pool_id == pool_id,
        )
    if artifactclass is not None:
        bundle_query = bundle_query.outerjoin(Bundle, AssetLocator.bundle_id == Bundle.id).where(
            _locator_artifactclass_filter(session, target.asset_hash, artifactclass)
        )
    bundle_copies = list(session.scalars(bundle_query.order_by(Copy.id)))
    return _dedupe_by_copy_id([*asset_copies, *bundle_copies])


def direct_copies(
    session: Session,
    asset_hash: bytes,
    *,
    require_verified: bool = False,
) -> list[Copy]:
    """Return healthy asset-grain copies that whole-copy restore can use today."""
    query = select(Copy).where(
        Copy.logical_asset_hash == asset_hash,
        Copy.health == CopyHealth.OK,
        Copy.deleted_at.is_(None),
    )
    if require_verified:
        query = _measurement_filter(query)
    return list(session.scalars(query.order_by(Copy.id)))


def placement_status(
    session: Session,
    target: Target,
    *,
    require_verified: bool = False,
) -> PlacementStatus:
    """Report wanted pools versus realized durable placements for a target.

    Entries extend ``PoolTarget`` with ``have``, ``duplicate_count``, and
    ``is_duplicate`` while keeping the original PoolTarget attributes available
    for harness seams that getattr-map pool/backend/representation fields.
    """
    if isinstance(target, BundleTarget):
        bundle = session.get(Bundle, target.bundle_id)
        if bundle is None:
            raise ValueError(f"bundle {target.bundle_id!r} does not exist")
        targets = _bundle_policy_targets(session, bundle)
        counts_by_key = bundle_copy_counts_by_pool(
            session,
            [target.bundle_id],
            require_verified=require_verified,
        )[target.bundle_id]
    else:
        artifactclass = target.artifactclass
        targets = [pool_target for _, pool_target in _policy_targets(session, artifactclass)]
        counts_by_key: dict[tuple[int, str], int] = {}
        for copy in durable_placements(
            session,
            target,
            require_verified=require_verified,
            artifactclass=artifactclass,
        ):
            if copy.pool_id is None:
                continue
            key = (copy.backend_id, copy.pool_id)
            counts_by_key[key] = counts_by_key.get(key, 0) + 1

    want: set[PlacementTarget] = set()
    have: set[PlacementTarget] = set()
    missing: set[PlacementTarget] = set()
    for pool_target in targets:
        count = counts_by_key.get((pool_target.backend_id, pool_target.pool_id), 0)
        entry = _placement_entry(pool_target, count)
        want.add(entry)
        if entry.have:
            have.add(entry)
        else:
            missing.add(entry)
    return {
        "complete": not missing,
        "have": have,
        "want": want,
        "missing": missing,
    }


def bundle_replication_status(
    session: Session,
    bundle_id: str,
    *,
    require_verified: bool = True,
) -> PlacementStatus:
    """Return replication-style status for one sealed bundle target."""
    return placement_status(
        session,
        BundleTarget(bundle_id),
        require_verified=require_verified,
    )


def bundle_copy_counts_by_pool(
    session: Session,
    bundle_ids: list[str] | tuple[str, ...],
    *,
    require_verified: bool = False,
) -> BundlePlacementCounts:
    """Return healthy bundle-copy counts grouped by bundle/backend/pool.

    The predicate intentionally matches ``durable_placements(BundleTarget)`` so
    batch observers can avoid N+1 scans while using the same durability view.
    """

    if not bundle_ids:
        return {}
    query = (
        select(Copy.bundle_id, Copy.backend_id, Copy.pool_id, func.count(Copy.id))
        .where(
            Copy.bundle_id.in_(bundle_ids),
            Copy.health == CopyHealth.OK,
            Copy.deleted_at.is_(None),
            Copy.pool_id.is_not(None),
        )
        .group_by(Copy.bundle_id, Copy.backend_id, Copy.pool_id)
    )
    if require_verified:
        query = _measurement_filter(query)
    counts: BundlePlacementCounts = {bundle_id: {} for bundle_id in bundle_ids}
    for bundle_id, backend_id, pool_id, count in session.execute(query):
        if bundle_id is None or pool_id is None:
            continue
        counts[str(bundle_id)][(int(backend_id), str(pool_id))] = int(count)
    return counts


def bundle_copy_aggregates_by_bundle(
    session: Session,
    bundle_ids: list[str] | tuple[str, ...],
    *,
    require_verified: bool = False,
) -> dict[str, BundleCopyAggregate]:
    """Return per-bundle placement counts, families, and media identities."""

    aggregates = {bundle_id: BundleCopyAggregate({}, {}, {}, ()) for bundle_id in bundle_ids}
    if not bundle_ids:
        return {}
    mutable_errors: dict[str, list[str]] = {bundle_id: [] for bundle_id in bundle_ids}
    query = (
        select(Copy)
        .options(joinedload(Copy.backend))
        .where(
            Copy.bundle_id.in_(bundle_ids),
            Copy.health == CopyHealth.OK,
            Copy.deleted_at.is_(None),
            Copy.pool_id.is_not(None),
        )
        .order_by(Copy.bundle_id, Copy.backend_id, Copy.pool_id, Copy.id)
    )
    if require_verified:
        query = _measurement_filter(query)
    for copy in session.scalars(query):
        if copy.bundle_id is None or copy.pool_id is None:
            continue
        aggregate = aggregates[str(copy.bundle_id)]
        key = (copy.backend_id, copy.pool_id)
        aggregate.counts[key] = aggregate.counts.get(key, 0) + 1
        aggregate.family_by_key.setdefault(key, _copy_implementation_family(copy))
        if key in aggregate.media_identity_by_key:
            continue
        try:
            identity = copy_media_identity(copy)
        except DurabilityMediaIdentityError as exc:
            mutable_errors[str(copy.bundle_id)].append(str(exc))
            continue
        if identity is not None:
            aggregate.media_identity_by_key[key] = identity

    return {
        bundle_id: BundleCopyAggregate(
            counts=aggregate.counts,
            family_by_key=aggregate.family_by_key,
            media_identity_by_key=aggregate.media_identity_by_key,
            media_errors=tuple(mutable_errors[bundle_id]),
        )
        for bundle_id, aggregate in aggregates.items()
    }


def copy_media_identity(copy: Copy) -> CopyMediaIdentity | None:
    """Return the per-family media identity for one copy, or None when exempt."""

    family = _copy_implementation_family(copy)
    if family == "memory":
        return None
    if family == "tape":
        value = copy.native_locator.get("tape_uuid")
        if isinstance(value, str) and value:
            return CopyMediaIdentity(family=family, media_id=value)
        raise DurabilityMediaIdentityError(
            f"copy id={copy.id} on backend_id={copy.backend_id} is missing tape_uuid"
        )
    if family == "d2tape":
        value = copy.native_locator.get("volume_uuid")
        if isinstance(value, str) and value:
            return CopyMediaIdentity(family=family, media_id=value)
        value = copy.native_locator.get("barcode")
        if isinstance(value, str) and value:
            return CopyMediaIdentity(family=family, media_id=value)
        raise DurabilityMediaIdentityError(
            f"copy id={copy.id} on backend_id={copy.backend_id} is missing volume_uuid/barcode"
        )
    if family in {"disk", "cloud"}:
        return CopyMediaIdentity(family=family, media_id=f"backend:{copy.backend_id}")
    raise DurabilityMediaIdentityError(
        f"copy id={copy.id} uses unsupported implementation_family={family!r}"
    )


def copy_media_id(copy: Copy) -> str | None:
    """Return a stable string media id for compatibility with replication status."""

    identity = copy_media_identity(copy)
    if identity is None:
        return f"memory:exempt:{copy.id}"
    return f"{identity.family}:{identity.media_id}"


def asset_has_artifactclass_membership(
    session: Session,
    asset_hash: bytes,
    artifactclass: str,
) -> bool:
    """Return whether catalog membership places an asset in ``artifactclass``.

    The D5.1 legacy-locator fallback uses ``IngestItem.artifactclass`` from
    registered intakes as the primary per-asset class occurrence. It also honors
    active ``VirtualArrangementMember`` rows, which are the catalog's mutable
    archived-asset class memberships. The fallback is used only when an
    ``AssetLocator`` has ``bundle_id`` set to NULL and can no longer be joined
    to its bundle's artifactclass.
    """
    ingest_match = session.scalar(
        select(IngestItem.id)
        .join(Intake, IngestItem.intake_id == Intake.intake_id)
        .where(
            IngestItem.logical_asset_hash == asset_hash,
            IngestItem.artifactclass == artifactclass,
            Intake.status == IntakeStatus.REGISTERED,
        )
        .limit(1)
    )
    if ingest_match is not None:
        return True
    virtual_match = session.scalar(
        select(VirtualArrangementMember.id)
        .where(
            VirtualArrangementMember.logical_asset_hash == asset_hash,
            VirtualArrangementMember.artifactclass == artifactclass,
            VirtualArrangementMember.excluded.is_(False),
        )
        .limit(1)
    )
    return virtual_match is not None


def locator_artifactclass_filter(
    session: Session,
    asset_hash: bytes,
    artifactclass: str,
) -> Any:
    """Return the D5.1 locator class filter for one asset and artifactclass."""
    return _locator_artifactclass_filter(session, asset_hash, artifactclass)


def _copy_health_filter(query: Any, *, require_verified: bool) -> Any:
    query = query.where(
        Copy.health == CopyHealth.OK,
        Copy.deleted_at.is_(None),
    )
    if require_verified:
        query = _measurement_filter(query)
    return query


def _measurement_filter(query: Any) -> Any:
    """Apply the one meaning of ``require_verified`` at every copy grain."""

    return query.where(
        Copy.last_measured_digest.is_not(None),
        Copy.last_measured_digest == Copy.integrity_hash,
    )


def pending_verification_copy_ids(session: Session) -> set[int]:
    """Return copies with a live verification job in the caller's transaction."""

    from sutradhara.jobs.models import LIVE_JOB_STATUSES, Job

    ids: set[int] = set()
    for params in session.scalars(
        select(Job.params).where(
            Job.kind == "verify",
            Job.status.in_(LIVE_JOB_STATUSES),
        )
    ):
        copy_id = params.get("copy_id") if isinstance(params, dict) else None
        if isinstance(copy_id, int):
            ids.add(copy_id)
    return ids


def _locator_artifactclass_filter(
    session: Session,
    asset_hash: bytes,
    artifactclass: str,
) -> Any:
    legacy_ok = asset_has_artifactclass_membership(session, asset_hash, artifactclass)
    legacy_clause = AssetLocator.bundle_id.is_(None) if legacy_ok else false()
    # Member grain (§5): the locator's asset must itself sit in the locator's
    # bundle under this class — hash + class, never the bundle's co-residents.
    # A bundle may hold several classes, and duplicate-content members (the
    # Sony split) resolve through their own class membership alone.
    member_clause = (
        select(BundleMember.id)
        .where(
            BundleMember.bundle_id == AssetLocator.bundle_id,
            BundleMember.logical_asset_hash == asset_hash,
            BundleMember.artifactclass == artifactclass,
        )
        .exists()
    )
    return or_(
        and_(AssetLocator.bundle_id.is_not(None), member_clause),
        legacy_clause,
    )


def _bundle_policy_targets(session: Session, bundle: Bundle) -> list[PoolTarget]:
    """Member-grain want-set for a bundle: the union over its member classes'
    live active pool sets (§5). Each member's class policy speaks for that
    member, so the union satisfies them all; classes that coalesced shared a
    pool set at open, and post-seal policy drift widens (never silently
    narrows) what the reconcile/repair machinery demands. Deterministic order:
    sorted class, then class membership order; first occurrence wins on
    shared pools.
    """
    from sutradhara.archive_bundle import bundle_artifactclasses

    targets: list[PoolTarget] = []
    seen: set[tuple[int, str]] = set()
    for artifactclass in bundle_artifactclasses(session, bundle):
        for _, pool_target in _policy_targets(session, artifactclass):
            key = (pool_target.backend_id, pool_target.pool_id)
            if key in seen:
                continue
            seen.add(key)
            targets.append(pool_target)
    return targets


def _policy_targets(
    session: Session, artifactclass: str
) -> list[tuple[StorageBackend, PoolTarget]]:
    backend_rows: dict[int, StorageBackend] = {}
    for backend_id, backend_name in session.execute(
        select(Backend.id, Backend.name)
        .join(Pool, Pool.backend_id == Backend.id)
        .join(ArtifactClassPool, ArtifactClassPool.pool_id == Pool.id)
        .where(
            ArtifactClassPool.artifactclass == artifactclass,
            ArtifactClassPool.active.is_(True),
        )
    ):
        backend_rows[int(backend_id)] = _TargetBackend(str(backend_name))
    return target_pools(session, artifactclass, backend_rows, write_eligible_only=False)


def _placement_entry(target: PoolTarget, duplicate_count: int) -> PlacementTarget:
    return PlacementTarget(
        pool_id=target.pool_id,
        artifactclass=target.artifactclass,
        backend_id=target.backend_id,
        backend_name=target.backend_name,
        representation=target.representation,
        key_epoch=target.key_epoch,
        location=target.location,
        offsite_gate=target.offsite_gate,
        tier=target.tier,
        sort_order=target.sort_order,
        have=duplicate_count > 0,
        duplicate_count=duplicate_count,
        is_duplicate=duplicate_count > 1,
    )


def _dedupe_by_copy_id(copies: list[Copy]) -> list[Copy]:
    seen: set[int] = set()
    result: list[Copy] = []
    for copy in copies:
        if copy.id in seen:
            continue
        seen.add(copy.id)
        result.append(copy)
    return result


def _copy_implementation_family(copy: Copy) -> str:
    family = copy.backend.implementation_family
    if isinstance(family, str) and family:
        return family
    from sutradhara.catalog.types import implementation_family_for_kind

    return implementation_family_for_kind(copy.backend.kind)
