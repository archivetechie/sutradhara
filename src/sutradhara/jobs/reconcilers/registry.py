"""Registry for reconciliation domains.

Reconcilers translate catalog desired-state gaps into ordinary jobs. The
registry mirrors the job-handler registry but exposes the three operations the
spine needs: bulk observation, single-target observation, and enqueue.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from sqlalchemy.orm import Session


@dataclass(frozen=True)
class TargetObservation:
    """Observed reality for one reconciliation target."""

    target_key: str
    desired: bool
    observed_state: str


@dataclass(frozen=True)
class Reconciler:
    """Domain-specific hooks used by the generic reconciler spine."""

    enumerate_targets: Callable[[Session, int | None, int], Iterable[TargetObservation]]
    observe: Callable[[Session, str], TargetObservation]
    reconcile_target: Callable[[Session, str], None]


class ReconcilerNotRegistered(Exception):
    """No reconciler is registered for this domain."""


_RECONCILERS: dict[str, Reconciler] = {}


def register_reconciler(domain: str) -> Callable[[Reconciler], Reconciler]:
    """Decorator to register a reconciler under ``domain``."""

    def decorator(reconciler: Reconciler) -> Reconciler:
        if domain in _RECONCILERS:
            raise ValueError(f"reconciler already registered for domain {domain!r}")
        _RECONCILERS[domain] = reconciler
        return reconciler

    return decorator


def get_reconciler(domain: str) -> Reconciler:
    try:
        return _RECONCILERS[domain]
    except KeyError as exc:
        raise ReconcilerNotRegistered(
            f"no reconciler registered for domain {domain!r}; known domains: {sorted(_RECONCILERS)}"
        ) from exc


def registered_reconcilers() -> Mapping[str, Reconciler]:
    """Return a snapshot of registered reconcilers."""

    return dict(_RECONCILERS)
