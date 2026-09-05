"""Bundle-copy reconciler.

This domain observes sealed bundles against the active write-eligible pools for
their artifactclass and enqueues ``bundle-repair`` when a bundle is missing a
healthy placement. Target keys are bundle ids, so one condition represents the
whole bundle-copy convergence state.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from sutradhara.archive_bundle import bundle_artifactclasses
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    ArtifactClassPool,
    Backend,
    Bundle,
    BundleMember,
    Copy,
    Pool,
)
from sutradhara.catalog.types import CopyHealth
from sutradhara.durability import (
    BundleCopyAggregate,
    DurabilityMediaIdentityError,
    bundle_copy_aggregates_by_bundle,
    copy_media_identity,
    pending_verification_copy_ids,
)
from sutradhara.jobs.engine import submit
from sutradhara.jobs.models import ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BLOCKED,
    OBSERVED_MISSING,
    OBSERVED_PRESENT,
    record_condition,
    record_observation,
)
from sutradhara.jobs.reconcilers.registry import Reconciler, TargetObservation, register_reconciler
from sutradhara.replication import PoolTarget

DOMAIN = "bundle_copy"
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class DurabilityFloor:
    """Declared bundle durability floor for one artifactclass."""

    min_copies: int = 3
    min_impl_families: int = 2


def enumerate_targets(
    session: Session,
    cursor: int | None,
    batch: int,
) -> list[TargetObservation]:
    """Enumerate sealed bundle-copy targets from one bounded bundle batch."""

    rows = _sealed_bundle_rows(session, cursor=cursor, batch=batch)
    if not rows:
        return []
    bundle_ids = [bundle_id for bundle_id, _classes in rows]
    classes = {
        artifactclass for _bundle_id, bundle_classes in rows for artifactclass in bundle_classes
    }
    desired_by_class = _desired_targets_by_class(session, classes)
    floors_by_class = _floors_for_classes(session, classes)
    placement_aggregates = _reconciler_aggregates(session, bundle_ids)

    observations: list[TargetObservation] = []
    for bundle_id, bundle_classes in rows:
        desired_targets = _merge_desired_targets(desired_by_class, bundle_classes)
        if not desired_targets:
            continue
        observations.append(
            _observation_for_bundle(
                bundle_id=bundle_id,
                desired_targets=desired_targets,
                aggregate=placement_aggregates[bundle_id],
                floor=_strictest_floor(
                    [floors_by_class[artifactclass] for artifactclass in bundle_classes]
                ),
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
    desired_targets, floor = _bundle_desired_and_floor(session, bundle)
    if not desired_targets:
        return TargetObservation(
            target_key=target_key,
            desired=False,
            observed_state=OBSERVED_MISSING,
        )
    aggregate = _reconciler_aggregates(session, [bundle.id])[bundle.id]
    return _observation_for_bundle(
        bundle_id=bundle.id,
        desired_targets=desired_targets,
        aggregate=aggregate,
        floor=floor,
    )


def refresh_condition(session: Session, target_key: str) -> ReconciliationCondition:
    """Observe and classify one bundle-copy condition row in the caller's transaction."""

    observation = observe(session, target_key)
    condition = record_observation(
        session,
        domain=DOMAIN,
        target_key=target_key,
        desired=observation.desired,
        observed_state=observation.observed_state,
        reason="durability-unverified"
        if observation.desired and observation.observed_state == OBSERVED_MISSING
        else None,
    )
    classify_condition(session, target_key, condition)
    return condition


def blocked_projection_for_bundle(session: Session, bundle_id: str) -> tuple[str, str] | None:
    """Return a blocked-condition reason/message for non-repairable bundle states."""

    bundle = session.get(Bundle, bundle_id)
    if bundle is None or bundle.status != "sealed":
        return None
    aggregate = _reconciler_aggregates(session, [bundle.id])[bundle.id]
    duplicate_message = _duplicate_message(bundle.id, aggregate)
    if duplicate_message is not None:
        return "duplicate-copy", duplicate_message
    desired_targets, floor = _bundle_desired_and_floor(session, bundle)
    structural_message = _structural_floor_message(
        session,
        desired_targets=desired_targets,
        floor=floor,
        aggregate=aggregate,
    )
    if structural_message is None:
        return None
    return "durability-floor-unsatisfiable", structural_message


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
) -> list[tuple[str, tuple[str, ...]]]:
    """One bounded batch of sealed bundles with their member classes (§5).

    Every member class speaks for its members; memberless funnel bundles fall
    back to the classes whose policy projection derives their group —
    identical pool sets by fingerprint construction.
    """
    query = (
        select(Bundle.id, Bundle.bundle_group)
        .where(Bundle.status == "sealed")
        .order_by(Bundle.id)
        .limit(batch)
    )
    if cursor is not None:
        # Bundle ids are strings in the current schema; use cursor as a stable
        # batch offset until M3 introduces a better typed cursor for this domain.
        query = query.offset(cursor)
    bundles = [(str(bundle_id), str(group)) for bundle_id, group in session.execute(query)]
    if not bundles:
        return []

    classes_by_bundle: dict[str, set[str]] = {bundle_id: set() for bundle_id, _ in bundles}
    for bundle_id, artifactclass in session.execute(
        select(BundleMember.bundle_id, BundleMember.artifactclass)
        .where(BundleMember.bundle_id.in_(list(classes_by_bundle)))
        .distinct()
    ):
        classes_by_bundle[str(bundle_id)].add(str(artifactclass))

    memberless_groups = {group for bundle_id, group in bundles if not classes_by_bundle[bundle_id]}
    projection_classes: dict[str, set[str]] = {group: set() for group in memberless_groups}
    if memberless_groups:
        for group, artifactclass in session.execute(
            select(
                ArtifactClassPolicyRecord.bundle_group,
                ArtifactClassPolicyRecord.artifactclass,
            ).where(ArtifactClassPolicyRecord.bundle_group.in_(list(memberless_groups)))
        ):
            projection_classes[str(group)].add(str(artifactclass))

    rows: list[tuple[str, tuple[str, ...]]] = []
    for bundle_id, group in bundles:
        classes = classes_by_bundle[bundle_id] or projection_classes.get(group, set())
        if classes:
            rows.append((bundle_id, tuple(sorted(classes))))
    return rows


def _merge_desired_targets(
    desired_by_class: dict[str, dict[tuple[int, str], PoolTarget]],
    bundle_classes: tuple[str, ...] | list[str],
) -> dict[tuple[int, str], PoolTarget]:
    """Union of the member classes' desired target maps; first class wins on
    shared pools (deterministic — classes arrive sorted)."""
    merged: dict[tuple[int, str], PoolTarget] = {}
    for artifactclass in bundle_classes:
        for key, target in desired_by_class.get(artifactclass, {}).items():
            merged.setdefault(key, target)
    return merged


def _strictest_floor(floors: list[DurabilityFloor]) -> DurabilityFloor:
    """The strictest declared floor across a bundle's member classes."""
    if not floors:
        return DurabilityFloor()
    return DurabilityFloor(
        min_copies=max(floor.min_copies for floor in floors),
        min_impl_families=max(floor.min_impl_families for floor in floors),
    )


def _bundle_desired_and_floor(
    session: Session,
    bundle: Bundle,
) -> tuple[dict[tuple[int, str], PoolTarget], DurabilityFloor]:
    """Member-grain desired targets and floor for one bundle (§5): the union
    over member classes' live write-eligible pool sets, under the strictest
    member class's declared durability floor."""
    classes = bundle_artifactclasses(session, bundle)
    if not classes:
        return {}, DurabilityFloor()
    desired_by_class = _desired_targets_by_class(session, set(classes))
    return (
        _merge_desired_targets(desired_by_class, classes),
        _strictest_floor([_floor_for_class(session, artifactclass) for artifactclass in classes]),
    )


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
                ArtifactClassPool.pool.has(Pool.accepts_writes.is_(True)),
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
        result.setdefault(membership.artifactclass, {})[(target.backend_id, target.pool_id)] = (
            target
        )
    return result


def _floors_for_classes(
    session: Session,
    artifactclasses: set[str],
) -> dict[str, DurabilityFloor]:
    return {
        artifactclass: _floor_for_class(session, artifactclass) for artifactclass in artifactclasses
    }


def _floor_for_class(session: Session, artifactclass: str) -> DurabilityFloor:
    record = session.get(ArtifactClassPolicyRecord, artifactclass)
    if record is None:
        return DurabilityFloor()
    return DurabilityFloor(
        min_copies=record.min_copies,
        min_impl_families=record.min_impl_families,
    )


def _observation_for_bundle(
    *,
    bundle_id: str,
    desired_targets: dict[tuple[int, str], PoolTarget],
    aggregate: BundleCopyAggregate,
    floor: DurabilityFloor,
) -> TargetObservation:
    placement_complete = set(desired_targets).issubset(
        {key for key, count in aggregate.counts.items() if count > 0}
    )
    floor_satisfied = _floor_satisfied(floor, aggregate)
    no_persistent_duplicates = not aggregate.duplicate_keys
    return TargetObservation(
        target_key=bundle_id,
        desired=True,
        observed_state=(
            OBSERVED_PRESENT
            if placement_complete and floor_satisfied and no_persistent_duplicates
            else OBSERVED_MISSING
        ),
    )


def _floor_satisfied(
    floor: DurabilityFloor,
    aggregate: BundleCopyAggregate,
) -> bool:
    realized_keys = {key for key, count in aggregate.counts.items() if count > 0}
    realized_copies = len(realized_keys)
    if realized_copies < floor.min_copies:
        return False
    families = {aggregate.family_by_key[key] for key in realized_keys}
    if len(families) < floor.min_impl_families:
        return False
    if aggregate.media_errors:
        return False
    return _media_identities_are_distinct(aggregate, realized_keys)


def classify_condition(
    session: Session,
    target_key: str,
    condition: ReconciliationCondition,
) -> None:
    """Project structural bundle-copy deficiencies into blocked conditions."""

    bundle = session.get(Bundle, target_key)
    if bundle is None or bundle.status != "sealed":
        return
    desired_targets, floor = _bundle_desired_and_floor(session, bundle)
    aggregate = _reconciler_aggregates(session, [bundle.id])[bundle.id]
    duplicate_message = _duplicate_message(bundle.id, aggregate)
    if duplicate_message is not None:
        _record_blocked_alarm(
            session,
            condition,
            target_key=target_key,
            reason="duplicate-copy",
            message=duplicate_message,
        )
        return
    structural_message = _structural_floor_message(
        session,
        desired_targets=desired_targets,
        floor=floor,
        aggregate=aggregate,
    )
    if structural_message is not None:
        _record_blocked_alarm(
            session,
            condition,
            target_key=target_key,
            reason="durability-floor-unsatisfiable",
            message=structural_message,
        )


def _media_identities_are_distinct(
    aggregate: BundleCopyAggregate,
    realized_keys: set[tuple[int, str]],
) -> bool:
    seen: dict[tuple[str, str], tuple[int, str]] = {}
    for key in sorted(realized_keys):
        family = aggregate.family_by_key[key]
        if family == "memory":
            continue
        identity = aggregate.media_identity_by_key.get(key)
        if identity is None:
            return False
        media_key = (identity.family, identity.media_id)
        if media_key in seen:
            return False
        seen[media_key] = key
    return True


def _reconciler_aggregates(
    session: Session,
    bundle_ids: list[str],
) -> dict[str, BundleCopyAggregate]:
    """Count measured copies plus OK copies whose deep verification is pending."""

    aggregates = bundle_copy_aggregates_by_bundle(
        session,
        bundle_ids,
        require_verified=True,
    )
    pending_ids = pending_verification_copy_ids(session)
    if not pending_ids:
        return aggregates
    errors = {
        bundle_id: list(aggregate.media_errors) for bundle_id, aggregate in aggregates.items()
    }
    pending = session.scalars(
        select(Copy)
        .options(joinedload(Copy.backend))
        .where(
            Copy.bundle_id.in_(bundle_ids),
            Copy.id.in_(pending_ids),
            Copy.health == CopyHealth.OK,
            Copy.deleted_at.is_(None),
            Copy.pool_id.is_not(None),
        )
        .order_by(Copy.id)
    )
    for copy in pending:
        if copy.bundle_id is None or copy.pool_id is None:
            continue
        if (
            copy.last_measured_digest is not None
            and copy.last_measured_digest == copy.integrity_hash
        ):
            continue
        aggregate = aggregates[copy.bundle_id]
        key = (copy.backend_id, copy.pool_id)
        aggregate.counts[key] = aggregate.counts.get(key, 0) + 1
        aggregate.family_by_key.setdefault(key, str(copy.backend.implementation_family))
        try:
            identity = copy_media_identity(copy)
        except DurabilityMediaIdentityError as exc:
            errors[copy.bundle_id].append(str(exc))
        else:
            if identity is not None:
                aggregate.media_identity_by_key.setdefault(key, identity)
    return {
        bundle_id: BundleCopyAggregate(
            counts=aggregate.counts,
            family_by_key=aggregate.family_by_key,
            media_identity_by_key=aggregate.media_identity_by_key,
            media_errors=tuple(errors[bundle_id]),
        )
        for bundle_id, aggregate in aggregates.items()
    }


def _structural_floor_message(
    session: Session,
    *,
    desired_targets: dict[tuple[int, str], PoolTarget],
    floor: DurabilityFloor,
    aggregate: BundleCopyAggregate,
) -> str | None:
    desired_families = _desired_families(session, desired_targets)
    defects: list[str] = []
    if len(desired_targets) < floor.min_copies:
        defects.append(
            f"write-eligible pools {len(desired_targets)} < min_copies {floor.min_copies}"
        )
    if len(set(desired_families.values())) < floor.min_impl_families:
        defects.append(
            "implementation families "
            f"{len(set(desired_families.values()))} < min_impl_families {floor.min_impl_families}"
        )
    media_conflicts = _realized_media_conflicts(aggregate)
    defects.extend(media_conflicts)
    if aggregate.media_errors:
        defects.extend(aggregate.media_errors)
    if not defects:
        return None
    return "bundle-copy durability floor cannot be satisfied: " + "; ".join(defects)


def _desired_families(
    session: Session,
    desired_targets: dict[tuple[int, str], PoolTarget],
) -> dict[tuple[int, str], str]:
    if not desired_targets:
        return {}
    pool_ids = [pool_id for _backend_id, pool_id in desired_targets]
    rows = session.execute(
        select(Pool.backend_id, Pool.id, Backend.implementation_family)
        .join(Backend, Pool.backend_id == Backend.id)
        .where(Pool.id.in_(pool_ids))
    )
    return {(int(backend_id), str(pool_id)): str(family) for backend_id, pool_id, family in rows}


def _realized_media_conflicts(aggregate: BundleCopyAggregate) -> list[str]:
    seen: dict[tuple[str, str], tuple[int, str]] = {}
    conflicts: list[str] = []
    for key, identity in sorted(aggregate.media_identity_by_key.items()):
        media_key = (identity.family, identity.media_id)
        other = seen.get(media_key)
        if other is not None and other != key:
            conflicts.append(
                "media identity reused by "
                f"{other[0]}/{other[1]} and {key[0]}/{key[1]}: "
                f"{identity.family}:{identity.media_id}"
            )
        seen[media_key] = key
    return conflicts


def _duplicate_message(
    bundle_id: str,
    aggregate: BundleCopyAggregate,
) -> str | None:
    if not aggregate.duplicate_keys:
        return None
    detail = ", ".join(
        f"backend_id={backend_id}/pool={pool_id} count={count}"
        for backend_id, pool_id, count in aggregate.duplicate_keys
    )
    return f"bundle {bundle_id} has persistent duplicate bundle copies: {detail}"


def _record_blocked_alarm(
    session: Session,
    condition: ReconciliationCondition,
    *,
    target_key: str,
    reason: str,
    message: str,
) -> None:
    already_blocked = condition.condition == CONDITION_BLOCKED and condition.reason == reason
    if not already_blocked:
        LOGGER.error(
            "bundle_copy_condition_blocked",
            extra={"target_key": target_key, "reason": reason, "detail": message},
        )
    record_condition(
        session,
        domain=DOMAIN,
        target_key=target_key,
        condition=CONDITION_BLOCKED,
        reason=reason,
        message=message,
    )


register_reconciler(DOMAIN)(
    Reconciler(
        enumerate_targets=enumerate_targets,
        observe=observe,
        reconcile_target=reconcile_target,
        classify_condition=classify_condition,
    )
)
