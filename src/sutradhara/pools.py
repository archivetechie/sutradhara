"""Pool catalog mutation helpers.

Pools are the storage-policy surface. A pool owns its representation, so once a
copy has landed in the pool, changing that representation would silently retag
stored bytes. This module provides the explicit mutation API that enforces the
immutability rule.
"""

from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    ArtifactClassPool,
    AssetLocator,
    Copy,
    Pool,
)
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BLOCKED,
    OBSERVED_MISSING,
    record_condition,
    record_observation,
)
from sutradhara.sealing.port import Representation

LOGGER = logging.getLogger(__name__)


class PoolError(Exception):
    """Base class for pool catalog errors."""


class UnknownPool(PoolError):
    """The requested pool id is not in the catalog."""


class PoolRepresentationImmutable(PoolError):
    """A pool with existing copies cannot change representation."""


class PoolWriteFenceWouldBreakDurability(PoolError):
    """A write-fence change would leave an active class below its floor."""


class PoolRetirementHasLiveLocators(PoolError):
    """A pool cannot retire while restore locator rows still point at it."""


def set_pool_representation(
    session: Session,
    pool_id: str,
    representation: Representation | str,
) -> Pool:
    """Set a pool representation, enforcing immutability after first copy."""
    pool = session.get(Pool, pool_id)
    if pool is None:
        raise UnknownPool(f"no Pool with id {pool_id!r}")
    new_value = (
        representation.value
        if isinstance(representation, Representation)
        else Representation(representation).value
    )
    if pool.representation == new_value:
        return pool
    copy_exists = session.scalars(select(Copy.id).where(Copy.pool_id == pool_id).limit(1)).first()
    if copy_exists is not None:
        raise PoolRepresentationImmutable(
            f"pool {pool_id!r} already contains copies; representation is immutable"
        )
    pool.representation = new_value
    session.flush()
    return pool


def set_pool_write_fence(
    session: Session,
    pool_id: str,
    *,
    accepts_writes: bool,
    force: bool = False,
) -> Pool:
    """Set the pool write fence, refusing floor-breaking drains unless forced."""

    pool = session.get(Pool, pool_id)
    if pool is None:
        raise UnknownPool(f"no Pool with id {pool_id!r}")
    if pool.accepts_writes == accepts_writes:
        return pool

    if not accepts_writes:
        violations = _write_fence_floor_violations(session, pool_id)
        if violations and not force:
            raise PoolWriteFenceWouldBreakDurability(
                f"pool {pool_id!r} cannot be write-fenced; active artifactclass floor "
                "would be unsatisfied: "
                + "; ".join(violations)
            )
        if violations:
            message = (
                f"FORCED write fence for pool {pool_id!r} leaves durability floor "
                "unsatisfied: "
                + "; ".join(violations)
            )
            LOGGER.error(message, extra={"pool_id": pool_id, "violations": violations})
            _record_forced_write_fence_alarm(session, pool_id, message)

    pool.accepts_writes = accepts_writes
    session.flush([pool])
    return pool


def set_pool_retired(session: Session, pool_id: str, *, retired: bool) -> Pool:
    """Set the descriptive retired flag, guarding pools with live locators."""

    pool = session.get(Pool, pool_id)
    if pool is None:
        raise UnknownPool(f"no Pool with id {pool_id!r}")
    if pool.retired == retired:
        return pool
    if retired and _pool_has_live_locators(session, pool_id):
        raise PoolRetirementHasLiveLocators(
            f"pool {pool_id!r} still has live AssetLocator rows"
        )
    pool.retired = retired
    session.flush([pool])
    return pool


def _write_fence_floor_violations(session: Session, pool_id: str) -> list[str]:
    classes = sorted(
        set(
            session.scalars(
                select(ArtifactClassPool.artifactclass).where(
                    ArtifactClassPool.pool_id == pool_id,
                    ArtifactClassPool.active.is_(True),
                )
            )
        )
    )
    violations: list[str] = []
    for artifactclass in classes:
        record = session.get(ArtifactClassPolicyRecord, artifactclass)
        if record is None:
            continue
        pools = list(
            session.scalars(
                select(Pool)
                .join(ArtifactClassPool, ArtifactClassPool.pool_id == Pool.id)
                .options(joinedload(Pool.backend))
                .where(
                    ArtifactClassPool.artifactclass == artifactclass,
                    ArtifactClassPool.active.is_(True),
                    Pool.accepts_writes.is_(True),
                    Pool.id != pool_id,
                )
            )
        )
        families = {pool.backend.implementation_family for pool in pools}
        if len(pools) >= record.min_copies and len(families) >= record.min_impl_families:
            continue
        violations.append(
            f"{artifactclass}: eligible_pools={len(pools)}/{record.min_copies}, "
            f"implementation_families={len(families)}/{record.min_impl_families}"
        )
    return violations


def _pool_has_live_locators(session: Session, pool_id: str) -> bool:
    asset_locator_id = session.scalars(
        select(AssetLocator.id).where(AssetLocator.pool_id == pool_id).limit(1)
    ).first()
    return asset_locator_id is not None


def _record_forced_write_fence_alarm(
    session: Session,
    pool_id: str,
    message: str,
) -> None:
    target_key = f"pool:{pool_id}"
    record_observation(
        session,
        domain="pool_lifecycle",
        target_key=target_key,
        desired=True,
        observed_state=OBSERVED_MISSING,
    )
    record_condition(
        session,
        domain="pool_lifecycle",
        target_key=target_key,
        condition=CONDITION_BLOCKED,
        reason="forced-write-fence-below-floor",
        message=message,
    )
