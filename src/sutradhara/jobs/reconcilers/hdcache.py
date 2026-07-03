"""Hdcache convergence reconciler.

The hdcache domain observes cacheable archived assets and enqueues bounded
``hdcache_fill`` jobs for missing or policy-nonconformant entries. Condition
rows are keyed by content sha256 hex, matching the live job dedupe domain.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from sutradhara.hdcache.fill import (
    DOMAIN,
    desired_target_for_asset,
    enumerate_desired_targets,
    fill_config_from_env,
    observe_target,
    submit_hdcache_fill,
)
from sutradhara.jobs.reconcilers.registry import Reconciler, TargetObservation, register_reconciler


def enumerate_targets(
    session: Session,
    cursor: int | None,
    batch: int,
) -> list[TargetObservation]:
    """Enumerate desired hdcache entries from a bounded bundle-member batch."""

    observations: list[TargetObservation] = []
    for target in enumerate_desired_targets(session, cursor=cursor, batch=batch):
        desired, observed = observe_target(session, target.sha_hex, mutate=False)
        observations.append(
            TargetObservation(
                target_key=target.sha_hex,
                desired=desired,
                observed_state=observed,
            )
        )
    return observations


def observe(session: Session, target_key: str) -> TargetObservation:
    """Observe one hdcache target, converging stale present rows to lost."""

    desired, observed = observe_target(
        session,
        target_key,
        mutate=True,
        config=fill_config_from_env(),
    )
    return TargetObservation(
        target_key=target_key,
        desired=desired,
        observed_state=observed,
    )


def reconcile_target(session: Session, target_key: str) -> None:
    """Enqueue a fill job for one missing hdcache target if the live cap allows it."""

    target = desired_target_for_asset(session, bytes.fromhex(target_key))
    if target is None:
        return
    submit_hdcache_fill(session, target)


register_reconciler(DOMAIN)(
    Reconciler(
        enumerate_targets=enumerate_targets,
        observe=observe,
        reconcile_target=reconcile_target,
    )
)
