"""In-memory counted resource leases for the single-node job worker.

The database records each job's requested resources. One worker process owns the
live lease tally, so reserving and releasing resources is an in-process invariant:
``sum(leased[pool]) <= capacity[pool]``.
"""

from __future__ import annotations

import threading
from collections.abc import Mapping
from typing import Any


class LeaseError(ValueError):
    """A resource declaration or lease operation is invalid."""


def normalize_required_resources(raw: list[dict[str, Any]] | None) -> dict[str, int]:
    """Return ``{pool: count}`` from the stored JSON resource declaration."""
    resources: dict[str, int] = {}
    for item in raw or []:
        if not isinstance(item, dict):
            raise LeaseError(f"resource entry must be an object: {item!r}")
        pool = item.get("pool")
        count = item.get("count")
        if not isinstance(pool, str) or not pool:
            raise LeaseError(f"resource pool must be a non-empty string: {item!r}")
        if isinstance(count, bool) or not isinstance(count, int):
            raise LeaseError(f"resource count must be an integer: {item!r}")
        if count < 0:
            raise LeaseError(f"resource count must be non-negative: {item!r}")
        if count == 0:
            continue
        resources[pool] = resources.get(pool, 0) + count
    return resources


class LeaseManager:
    """Thread-safe counted lease accounting for one worker process."""

    def __init__(self, capacities: Mapping[str, int]) -> None:
        self._capacities = {str(pool): int(count) for pool, count in capacities.items()}
        for pool, count in self._capacities.items():
            if count < 0:
                raise LeaseError(f"capacity for pool {pool!r} must be non-negative")
        self._leased = {pool: 0 for pool in self._capacities}
        self._lock = threading.Lock()

    @property
    def capacities(self) -> dict[str, int]:
        return dict(self._capacities)

    @property
    def leased(self) -> dict[str, int]:
        with self._lock:
            return dict(self._leased)

    def fits(self, required: Mapping[str, int]) -> bool:
        with self._lock:
            return self._fits_unlocked(required)

    def can_ever_fit(self, required: Mapping[str, int]) -> bool:
        return all(count <= self._capacities.get(pool, 0) for pool, count in required.items())

    def reserve(self, required: Mapping[str, int]) -> dict[str, int]:
        with self._lock:
            if not self._fits_unlocked(required):
                raise LeaseError(f"resource request does not fit available leases: {required!r}")
            for pool, count in required.items():
                self._leased[pool] = self._leased.get(pool, 0) + count
            return dict(required)

    def release(self, granted: Mapping[str, int]) -> None:
        with self._lock:
            for pool, count in granted.items():
                current = self._leased.get(pool, 0)
                if count < 0 or count > current:
                    raise LeaseError(f"release for pool {pool!r} exceeds leased count")
                self._leased[pool] = current - count

    def _fits_unlocked(self, required: Mapping[str, int]) -> bool:
        for pool, count in required.items():
            capacity = self._capacities.get(pool, 0)
            if count > capacity - self._leased.get(pool, 0):
                return False
        return True
