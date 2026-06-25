"""Worker configuration for the single-node lease scheduler.

The job worker has one source of truth for resource capacities, retry limits,
and backoff. The in-memory lease manager consumes this config; handlers receive
the actual granted leases through ``JobContext``.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_IO_CAPACITY = 2
DEFAULT_DERIVATION_CACHE_ROOT = Path("/var/lib/replica/cache")
_DERIVATION_CACHE_ROOT_OVERRIDE: ContextVar[Path | None] = ContextVar(
    "derivation_cache_root_override",
    default=None,
)


@dataclass(frozen=True)
class RetryPolicy:
    """Retry limits and exponential backoff for failed jobs."""

    max_attempts: int = 1
    backoff_seconds: int = 30

    def delay_seconds(self, attempts: int) -> int:
        if attempts <= 0:
            return 0
        multiplier = 1 << max(0, attempts - 1)
        return self.backoff_seconds * multiplier


@dataclass(frozen=True)
class WorkerConfig:
    """Lease scheduler knobs for one worker process."""

    capacities: dict[str, int] = field(default_factory=dict)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    per_kind_retry: dict[str, RetryPolicy] = field(default_factory=dict)
    aging_threshold_scans: int = 3
    executor_workers: int | None = None

    @classmethod
    def defaults(cls) -> WorkerConfig:
        cpu = os.cpu_count() or 1
        return cls(
            capacities={
                "cpu": cpu,
                "io": DEFAULT_IO_CAPACITY,
                "tape_drive": 0,
                "gpu": 0,
            },
            executor_workers=max(cpu, DEFAULT_IO_CAPACITY, 1),
        )

    def with_pool_overrides(self, overrides: dict[str, int]) -> WorkerConfig:
        capacities = {**self.capacities, **overrides}
        executor_workers = self.executor_workers
        if executor_workers is None:
            executor_workers = max(max(capacities.values(), default=1), 1)
        return WorkerConfig(
            capacities=capacities,
            retry=self.retry,
            per_kind_retry=dict(self.per_kind_retry),
            aging_threshold_scans=self.aging_threshold_scans,
            executor_workers=executor_workers,
        )

    def retry_for_kind(self, kind: str) -> RetryPolicy:
        return self.per_kind_retry.get(kind, self.retry)


def parse_pool_overrides(raw: tuple[str, ...] | list[str]) -> dict[str, int]:
    """Parse CLI ``pool=count`` overrides."""
    overrides: dict[str, int] = {}
    for item in raw:
        if "=" not in item:
            raise ValueError(f"pool override {item!r} must be pool=count")
        pool, _, count_raw = item.partition("=")
        pool = pool.strip()
        if not pool:
            raise ValueError(f"pool override {item!r} has an empty pool name")
        try:
            count = int(count_raw)
        except ValueError as exc:
            raise ValueError(f"pool override {item!r} count must be an integer") from exc
        if count < 0:
            raise ValueError(f"pool override {item!r} count must be non-negative")
        overrides[pool] = count
    return overrides


def config_from_json(raw: dict[str, Any] | None = None) -> WorkerConfig:
    """Build a worker config from a small JSON-like mapping."""
    config = WorkerConfig.defaults()
    if not raw:
        return config
    capacities = raw.get("capacities")
    if isinstance(capacities, dict):
        config = config.with_pool_overrides(
            {str(pool): int(count) for pool, count in capacities.items()}
        )
    retry_raw = raw.get("retry")
    if isinstance(retry_raw, dict):
        config = WorkerConfig(
            capacities=dict(config.capacities),
            retry=RetryPolicy(
                max_attempts=int(retry_raw.get("max_attempts", config.retry.max_attempts)),
                backoff_seconds=int(retry_raw.get("backoff_seconds", config.retry.backoff_seconds)),
            ),
            per_kind_retry=dict(config.per_kind_retry),
            aging_threshold_scans=int(
                raw.get("aging_threshold_scans", config.aging_threshold_scans)
            ),
            executor_workers=config.executor_workers,
        )
    return config


def derivation_cache_root() -> Path:
    """Return the cache root used by derivation reconcilers and handlers."""

    override = _DERIVATION_CACHE_ROOT_OVERRIDE.get()
    if override is not None:
        return override
    raw = os.environ.get("SUTRADHARA_CACHE_ROOT")
    if raw:
        return Path(raw).expanduser().resolve()
    return DEFAULT_DERIVATION_CACHE_ROOT


@contextmanager
def override_derivation_cache_root(path: str | Path | None) -> Iterator[None]:
    """Temporarily override ``derivation_cache_root`` for one reconcile run."""

    if path is None:
        yield
        return
    token = _DERIVATION_CACHE_ROOT_OVERRIDE.set(Path(path).expanduser().resolve())
    try:
        yield
    finally:
        _DERIVATION_CACHE_ROOT_OVERRIDE.reset(token)
