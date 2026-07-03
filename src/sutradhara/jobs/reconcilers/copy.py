"""Copy-domain reconciler.

The copy reconciler derives desired asset-pool placements from registered
``IngestItem`` memberships and active ``ArtifactClassPool`` rows, observes
healthy ``Copy`` rows, and enqueues the existing stub ``copy`` job for missing
placements. It does not move bytes; it proves the controller.
"""

from __future__ import annotations

import logging

from sqlalchemy import distinct, select
from sqlalchemy.orm import Session, joinedload

from sutradhara.catalog.models import ArtifactClassPool, IngestItem, Intake, Pool
from sutradhara.catalog.types import IntakeStatus
from sutradhara.jobs.engine import submit
from sutradhara.jobs.reconcilers.conditions import OBSERVED_MISSING, OBSERVED_PRESENT
from sutradhara.jobs.reconcilers.registry import Reconciler, TargetObservation, register_reconciler
from sutradhara.replication import ReplicationPolicyMissing, _healthy_copies

LOGGER = logging.getLogger(__name__)
DOMAIN = "copy"
TARGET_PREFIX = "asset"


def make_target_key(asset_hash: bytes | str, pool_id: str) -> str:
    """Return the opaque copy target key for one asset/pool placement."""

    sha_hex = asset_hash if isinstance(asset_hash, str) else asset_hash.hex()
    return f"{TARGET_PREFIX}:{sha_hex}:{pool_id}"


def observe(session: Session, target_key: str) -> TargetObservation:
    """Observe one concrete ``asset:{sha}:{pool}`` target."""

    sha_hex, pool_id = parse_target_key(target_key)
    asset_hash = bytes.fromhex(sha_hex)
    desired_pool_ids = _desired_pool_ids_for_asset(session, asset_hash)
    observed_pool_ids = _observed_pool_ids(session, asset_hash)
    return TargetObservation(
        target_key=target_key,
        desired=pool_id in desired_pool_ids,
        observed_state=OBSERVED_PRESENT if pool_id in observed_pool_ids else OBSERVED_MISSING,
    )


def enumerate_targets(
    session: Session,
    cursor: int | None,
    batch: int,
) -> list[TargetObservation]:
    """Enumerate desired copy targets from a bounded live-ingest batch."""

    rows = list(_live_ingest_rows(session, cursor=cursor, batch=batch))
    missing_policy_counts: dict[str, int] = {}
    pool_ids_by_class: dict[str, set[str]] = {}
    classes_by_asset: dict[bytes, set[str]] = {}

    for _item_id, asset_hash, artifactclass in rows:
        classes_by_asset.setdefault(asset_hash, set()).add(artifactclass)
        if artifactclass in pool_ids_by_class or artifactclass in missing_policy_counts:
            continue
        try:
            pool_ids_by_class[artifactclass] = _active_pool_ids_for_class(session, artifactclass)
        except ReplicationPolicyMissing:
            missing_policy_counts[artifactclass] = 0

    for _item_id, _asset_hash, artifactclass in rows:
        if artifactclass in missing_policy_counts:
            missing_policy_counts[artifactclass] += 1

    for artifactclass, count in sorted(missing_policy_counts.items()):
        LOGGER.warning(
            "class %s registered but no active pools - %s items cannot be reconciled",
            artifactclass,
            count,
        )

    observations: dict[str, TargetObservation] = {}
    for asset_hash, classes in classes_by_asset.items():
        desired_pool_ids: set[str] = set()
        for artifactclass in classes:
            desired_pool_ids.update(pool_ids_by_class.get(artifactclass, set()))
        observed_pool_ids = _observed_pool_ids(session, asset_hash)
        for pool_id in sorted(desired_pool_ids):
            target_key = make_target_key(asset_hash, pool_id)
            observations[target_key] = TargetObservation(
                target_key=target_key,
                desired=True,
                observed_state=(
                    OBSERVED_PRESENT if pool_id in observed_pool_ids else OBSERVED_MISSING
                ),
            )

    return list(observations.values())


def reconcile_target(session: Session, target_key: str) -> None:
    """Enqueue the stub ``copy`` job for one missing asset/pool placement."""

    sha_hex, pool_id = parse_target_key(target_key)
    pool = session.scalars(
        select(Pool).options(joinedload(Pool.backend)).where(Pool.id == pool_id)
    ).one_or_none()
    if pool is None:
        raise ValueError(f"no pool with id={pool_id!r}; cannot reconcile {target_key!r}")

    submit(
        session,
        "copy",
        {
            "asset_hash": sha_hex,
            "target_backend": pool.backend.name,
            "pool_id": pool_id,
        },
        recon_domain=DOMAIN,
        recon_target_key=target_key,
        dedupe_key=f"{DOMAIN}:{target_key}",
    )


def parse_target_key(target_key: str) -> tuple[str, str]:
    """Parse ``asset:{sha}:{pool}``, preserving colons inside pool ids."""

    prefix, sep, rest = target_key.partition(":")
    if prefix != TARGET_PREFIX or not sep:
        raise ValueError(f"copy target key must start with 'asset:'; got {target_key!r}")
    sha_hex, sep, pool_id = rest.partition(":")
    if not sep or len(sha_hex) != 64:
        raise ValueError(f"copy target key must be asset:<sha256_hex>:<pool>; got {target_key!r}")
    try:
        bytes.fromhex(sha_hex)
    except ValueError as exc:
        raise ValueError(f"copy target key has invalid sha256 hex {sha_hex!r}") from exc
    if not pool_id:
        raise ValueError(f"copy target key has empty pool id: {target_key!r}")
    return sha_hex, pool_id


def _desired_pool_ids_for_asset(session: Session, asset_hash: bytes) -> set[str]:
    desired: set[str] = set()
    for artifactclass in _live_classes_for_asset(session, asset_hash):
        try:
            desired.update(_active_pool_ids_for_class(session, artifactclass))
        except ReplicationPolicyMissing:
            continue
    return desired


def _active_pool_ids_for_class(session: Session, artifactclass: str) -> set[str]:
    pool_ids = set(
        session.scalars(
            select(ArtifactClassPool.pool_id).where(
                ArtifactClassPool.artifactclass == artifactclass,
                ArtifactClassPool.active.is_(True),
                ArtifactClassPool.pool.has(Pool.accepts_writes.is_(True)),
            )
        )
    )
    if not pool_ids:
        raise ReplicationPolicyMissing(
            f"artifactclass {artifactclass!r} has no active pool memberships"
        )
    return pool_ids


def _observed_pool_ids(session: Session, asset_hash: bytes) -> set[str]:
    return {
        copy.pool_id for copy in _healthy_copies(session, asset_hash) if copy.pool_id is not None
    }


def _live_classes_for_asset(session: Session, asset_hash: bytes) -> set[str]:
    return set(
        session.scalars(
            select(distinct(IngestItem.artifactclass))
            .join(Intake, IngestItem.intake_id == Intake.intake_id)
            .where(
                IngestItem.logical_asset_hash == asset_hash,
                Intake.status == IntakeStatus.REGISTERED,
            )
        )
    )


def _live_ingest_rows(
    session: Session,
    *,
    cursor: int | None,
    batch: int,
) -> list[tuple[int, bytes, str]]:
    query = (
        select(IngestItem.id, IngestItem.logical_asset_hash, IngestItem.artifactclass)
        .join(Intake, IngestItem.intake_id == Intake.intake_id)
        .where(Intake.status == IntakeStatus.REGISTERED)
        .order_by(IngestItem.id)
        .limit(batch)
    )
    if cursor is not None:
        query = query.where(IngestItem.id > cursor)
    return [
        (item_id, asset_hash, artifactclass)
        for item_id, asset_hash, artifactclass in session.execute(query)
    ]


register_reconciler(DOMAIN)(
    Reconciler(
        enumerate_targets=enumerate_targets,
        observe=observe,
        reconcile_target=reconcile_target,
    )
)
