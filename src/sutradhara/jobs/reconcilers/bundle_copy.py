"""Bundle-copy reconciler.

This domain observes sealed bundles against the active write-eligible pools for
their artifactclass and enqueues ``bundle-repair`` when a bundle is missing a
healthy placement. Target keys are bundle ids, so one condition represents the
whole bundle-copy convergence state.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from sutradhara.catalog.models import ArtifactClassPool, Bundle, Pool
from sutradhara.durability import bundle_copy_counts_by_pool
from sutradhara.jobs.engine import submit
from sutradhara.jobs.reconcilers.conditions import OBSERVED_MISSING, OBSERVED_PRESENT
from sutradhara.jobs.reconcilers.registry import Reconciler, TargetObservation, register_reconciler
from sutradhara.replication import PoolTarget

DOMAIN = "bundle_copy"


@dataclass(frozen=True)
class DurabilityFloor:
    """Declared bundle durability floor; absent fields mean no floor yet."""

    min_copies: int | None = None
    min_impl_families: int | None = None


def enumerate_targets(
    session: Session,
    cursor: int | None,
    batch: int,
) -> list[TargetObservation]:
    """Enumerate sealed bundle-copy targets from one bounded bundle batch."""

    rows = _sealed_bundle_rows(session, cursor=cursor, batch=batch)
    if not rows:
        return []
    bundle_ids = [bundle_id for bundle_id, _artifactclass in rows]
    classes = {artifactclass for _bundle_id, artifactclass in rows}
    desired_by_class = _desired_targets_by_class(session, classes)
    floors_by_class = _floors_for_classes(session, classes)
    placement_counts = bundle_copy_counts_by_pool(session, bundle_ids)

    observations: list[TargetObservation] = []
    for bundle_id, artifactclass in rows:
        desired_targets = desired_by_class.get(artifactclass, {})
        if not desired_targets:
            continue
        observations.append(
            _observation_for_bundle(
                bundle_id=bundle_id,
                desired_targets=desired_targets,
                counts=placement_counts.get(bundle_id, {}),
                floor=floors_by_class[artifactclass],
            )
        )
    return observations


def observe(session: Session, target_key: str) -> TargetObservation:
    """Observe one sealed bundle target by bundle id."""

    bundle = session.get(Bundle, target_key)
    if bundle is None or bundle.status != "sealed":
        return TargetObservation(
            target_key=target_key,
            desired=False,
            observed_state=OBSERVED_MISSING,
        )
    desired_targets = _desired_targets_by_class(session, {bundle.artifactclass}).get(
        bundle.artifactclass,
        {},
    )
    if not desired_targets:
        return TargetObservation(
            target_key=target_key,
            desired=False,
            observed_state=OBSERVED_MISSING,
        )
    counts = bundle_copy_counts_by_pool(session, [bundle.id]).get(bundle.id, {})
    return _observation_for_bundle(
        bundle_id=bundle.id,
        desired_targets=desired_targets,
        counts=counts,
        floor=_floor_for_class(session, bundle.artifactclass),
    )


def reconcile_target(session: Session, target_key: str) -> None:
    """Enqueue one bundle repair job for a bundle-copy condition."""

    submit(
        session,
        "bundle-repair",
        {"bundle_id": target_key},
        recon_domain=DOMAIN,
        recon_target_key=target_key,
        dedupe_key=f"{DOMAIN}:{target_key}",
    )


def _sealed_bundle_rows(
    session: Session,
    *,
    cursor: int | None,
    batch: int,
) -> list[tuple[str, str]]:
    query = (
        select(Bundle.id, Bundle.artifactclass)
        .where(Bundle.status == "sealed")
        .order_by(Bundle.id)
        .limit(batch)
    )
    if cursor is not None:
        # Bundle ids are strings in the current schema; use cursor as a stable
        # batch offset until M3 introduces a better typed cursor for this domain.
        query = query.offset(cursor)
    return [
        (str(bundle_id), str(artifactclass))
        for bundle_id, artifactclass in session.execute(query)
    ]


def _desired_targets_by_class(
    session: Session,
    artifactclasses: set[str],
) -> dict[str, dict[tuple[int, str], PoolTarget]]:
    if not artifactclasses:
        return {}
    rows = list(
        session.scalars(
            select(ArtifactClassPool)
            .options(joinedload(ArtifactClassPool.pool).joinedload(Pool.backend))
            .where(
                ArtifactClassPool.artifactclass.in_(artifactclasses),
                ArtifactClassPool.active.is_(True),
                # M3/D3 accepts_writes: add ArtifactClassPool.accepts_writes here.
            )
            .order_by(ArtifactClassPool.artifactclass, ArtifactClassPool.sort_order)
        )
    )
    result: dict[str, dict[tuple[int, str], PoolTarget]] = {}
    for membership in rows:
        pool = membership.pool
        target = PoolTarget(
            pool_id=pool.id,
            artifactclass=membership.artifactclass,
            backend_id=pool.backend_id,
            backend_name=pool.backend.name,
            representation=pool.representation,
            location=pool.location,
            offsite_gate=pool.offsite_gate,
            tier=pool.tier,
            sort_order=membership.sort_order,
        )
        result.setdefault(membership.artifactclass, {})[
            (target.backend_id, target.pool_id)
        ] = target
    return result


def _floors_for_classes(
    session: Session,
    artifactclasses: set[str],
) -> dict[str, DurabilityFloor]:
    return {
        artifactclass: _floor_for_class(session, artifactclass)
        for artifactclass in artifactclasses
    }


def _floor_for_class(session: Session, artifactclass: str) -> DurabilityFloor:
    _ = (session, artifactclass)
    # M3/D2 floor: read declared [durability] min_copies/min_impl_families here.
    return DurabilityFloor()


def _observation_for_bundle(
    *,
    bundle_id: str,
    desired_targets: dict[tuple[int, str], PoolTarget],
    counts: dict[tuple[int, str], int],
    floor: DurabilityFloor,
) -> TargetObservation:
    placement_complete = set(desired_targets).issubset(
        {key for key, count in counts.items() if count > 0}
    )
    floor_satisfied = _floor_satisfied(floor, counts)
    return TargetObservation(
        target_key=bundle_id,
        desired=True,
        observed_state=(
            OBSERVED_PRESENT
            if placement_complete and floor_satisfied
            else OBSERVED_MISSING
        ),
    )


def _floor_satisfied(
    floor: DurabilityFloor,
    counts: dict[tuple[int, str], int],
) -> bool:
    if floor.min_copies is None and floor.min_impl_families is None:
        return True
    realized_copies = sum(1 for count in counts.values() if count > 0)
    if floor.min_copies is not None and realized_copies < floor.min_copies:
        return False
    # M3/D2 floor: compute implementation families from pool/backend family maps.
    return True


register_reconciler(DOMAIN)(
    Reconciler(
        enumerate_targets=enumerate_targets,
        observe=observe,
        reconcile_target=reconcile_target,
    )
)
