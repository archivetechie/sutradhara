"""Deterministic disk placement for the expendable hdcache tier.

The placement engine is catalog-side policy only: it reads ``cache_disk`` and
``cache_entry`` state, chooses an enrolled disk id, and never touches archival
backend, pool, or copy tables. Fill orchestration in later milestones owns the
write, DB state transition, and retry loop.
"""

from __future__ import annotations

import hashlib
import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from sutradhara.hdcache.models import CacheDisk, CacheEntry

GIB = 1024**3
DEFAULT_SPREAD_MIN_BYTES = GIB
DEFAULT_RESERVE_FRACTION = 0.02
LIVE_ENTRY_STATES = ("present", "filling")


class PlacementError(RuntimeError):
    """Raised when no active hdcache disk can accept a placement."""


@dataclass(frozen=True)
class PlacementConfig:
    """Tunable knobs for the default hdcache disk placement policy."""

    spread_min_bytes: int = DEFAULT_SPREAD_MIN_BYTES
    reserve_largest_expected_file_bytes: int = 0
    reserve_tmp_headroom_bytes: int = 0
    reserve_fraction: float = DEFAULT_RESERVE_FRACTION
    enclosure_spread: bool = False

    def __post_init__(self) -> None:
        if self.spread_min_bytes < 0:
            raise ValueError("spread_min_bytes must be non-negative")
        if self.reserve_largest_expected_file_bytes < 0:
            raise ValueError("reserve_largest_expected_file_bytes must be non-negative")
        if self.reserve_tmp_headroom_bytes < 0:
            raise ValueError("reserve_tmp_headroom_bytes must be non-negative")
        if self.reserve_fraction < 0:
            raise ValueError("reserve_fraction must be non-negative")

    def reserve_bytes(self, disk: DiskState) -> int:
        """Return the per-disk reserve required before accepting a new fill."""

        configured_floor = (
            self.reserve_largest_expected_file_bytes + self.reserve_tmp_headroom_bytes
        )
        fractional_floor = math.ceil(disk.capacity_bytes * self.reserve_fraction)
        return max(configured_floor, fractional_floor)


@dataclass(frozen=True)
class DiskState:
    """Placement-relevant state for one enrolled hdcache disk."""

    disk_id: str
    state: str
    capacity_bytes: int
    filled_bytes: int
    filling_bytes: int = 0
    capacity_state: str = "ok"
    enclosure: str | None = None
    slot: str | None = None

    @property
    def committed_bytes(self) -> int:
        """Bytes that placement must treat as already committed.

        Catalog placement trusts ``cache_disk.filled_bytes``. Fill code updates
        that disk-row counter at reservation time, so ``filling_bytes`` is only
        an in-memory policy input used by direct policy callers/tests.
        """

        return max(0, self.filled_bytes) + max(0, self.filling_bytes)

    @property
    def free_bytes(self) -> int:
        """Free bytes after durable and in-flight cache entries are counted."""

        return max(0, self.capacity_bytes - self.committed_bytes)

    @property
    def filled_ratio(self) -> float:
        """Committed fill ratio used for balance sorting."""

        if self.capacity_bytes <= 0:
            return 1.0
        return self.committed_bytes / self.capacity_bytes


@dataclass(frozen=True)
class PlacementContext:
    """Context used by hdcache placement; built from indexed cache_entry lookups."""

    content_sha256: bytes
    size_bytes: int
    artifactclass: str
    bundle_key: str | None
    sibling_disks: frozenset[str]
    group_key: str | None
    group_disk_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        if len(self.content_sha256) != 32:
            raise ValueError("content_sha256 must be 32 bytes")
        if self.size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")
        object.__setattr__(self, "sibling_disks", frozenset(self.sibling_disks))
        object.__setattr__(
            self,
            "group_disk_counts",
            MappingProxyType(dict(self.group_disk_counts)),
        )


class DiskPlacementPolicy(Protocol):
    """Policy port for hdcache disk selection."""

    def choose(self, candidates: Sequence[DiskState], ctx: PlacementContext) -> str:
        """Return the selected ``cache_disk.disk_id`` for ``ctx``."""


class DefaultDiskPlacementPolicy:
    """Default size-gated anti-affinity placement engine from the hdcache design."""

    def __init__(self, config: PlacementConfig | None = None) -> None:
        self.config = config or PlacementConfig()

    def choose(self, candidates: Sequence[DiskState], ctx: PlacementContext) -> str:
        """Choose one active disk using reserve, affinity, balance, and tiebreak rules."""

        viable = self._viable_candidates(candidates, ctx)
        if not viable:
            raise PlacementError(
                f"no active hdcache disk has free space for {ctx.size_bytes} bytes plus reserve"
            )
        if ctx.size_bytes < self.config.spread_min_bytes:
            return self._choose_small(viable, ctx).disk_id
        return self._choose_large(viable, candidates, ctx).disk_id

    def _viable_candidates(
        self,
        candidates: Sequence[DiskState],
        ctx: PlacementContext,
    ) -> list[DiskState]:
        return [
            disk
            for disk in candidates
            if disk.state == "active"
            and disk.capacity_state == "ok"
            and disk.free_bytes >= ctx.size_bytes + self.config.reserve_bytes(disk)
        ]

    def _choose_small(
        self,
        candidates: Sequence[DiskState],
        ctx: PlacementContext,
    ) -> DiskState:
        same_bundle = [disk for disk in candidates if disk.disk_id in ctx.sibling_disks]
        if same_bundle:
            return min(same_bundle, key=lambda disk: self._small_group_key(disk, ctx))

        same_group = [disk for disk in candidates if ctx.group_disk_counts.get(disk.disk_id, 0) > 0]
        if same_group:
            return min(same_group, key=lambda disk: self._small_group_key(disk, ctx))

        return min(candidates, key=lambda disk: self._balance_key(disk, ctx))

    def _choose_large(
        self,
        candidates: Sequence[DiskState],
        all_disks: Sequence[DiskState],
        ctx: PlacementContext,
    ) -> DiskState:
        non_sibling = [disk for disk in candidates if disk.disk_id not in ctx.sibling_disks]
        narrowed = non_sibling or list(candidates)

        if self.config.enclosure_spread and ctx.sibling_disks:
            used_enclosures = {
                disk.enclosure
                for disk in all_disks
                if disk.disk_id in ctx.sibling_disks and disk.enclosure
            }
            if used_enclosures:
                enclosure_spread = [
                    disk
                    for disk in narrowed
                    if disk.enclosure and disk.enclosure not in used_enclosures
                ]
                if enclosure_spread:
                    narrowed = enclosure_spread

        return min(narrowed, key=lambda disk: self._large_group_key(disk, ctx))

    def _small_group_key(self, disk: DiskState, ctx: PlacementContext) -> tuple[int, float, bytes]:
        return (
            -ctx.group_disk_counts.get(disk.disk_id, 0),
            disk.filled_ratio,
            _tiebreak(disk.disk_id, ctx.content_sha256),
        )

    def _large_group_key(self, disk: DiskState, ctx: PlacementContext) -> tuple[int, float, bytes]:
        return (
            ctx.group_disk_counts.get(disk.disk_id, 0),
            disk.filled_ratio,
            _tiebreak(disk.disk_id, ctx.content_sha256),
        )

    def _balance_key(self, disk: DiskState, ctx: PlacementContext) -> tuple[float, bytes]:
        return (disk.filled_ratio, _tiebreak(disk.disk_id, ctx.content_sha256))


def build_placement_context(
    session: Session,
    *,
    content_sha256: bytes,
    size_bytes: int,
    artifactclass: str,
    bundle_key: str | None,
    group_key: str | None,
) -> PlacementContext:
    """Build placement context with at most one indexed query per grouping signal."""

    sibling_disks: frozenset[str] = frozenset()
    if bundle_key is not None:
        sibling_disks = frozenset(
            session.scalars(
                select(CacheEntry.disk_id)
                .where(
                    CacheEntry.bundle_key == bundle_key,
                    CacheEntry.state.in_(LIVE_ENTRY_STATES),
                    CacheEntry.content_sha256 != content_sha256,
                )
                .distinct()
            )
        )

    group_disk_counts: dict[str, int] = {}
    if group_key is not None:
        group_disk_counts = {
            str(disk_id): int(count)
            for disk_id, count in session.execute(
                select(CacheEntry.disk_id, func.count())
                .where(
                    CacheEntry.group_key == group_key,
                    CacheEntry.state.in_(LIVE_ENTRY_STATES),
                    CacheEntry.content_sha256 != content_sha256,
                )
                .group_by(CacheEntry.disk_id)
            )
        }

    return PlacementContext(
        content_sha256=content_sha256,
        size_bytes=size_bytes,
        artifactclass=artifactclass,
        bundle_key=bundle_key,
        sibling_disks=sibling_disks,
        group_key=group_key,
        group_disk_counts=group_disk_counts,
    )


def disk_states_from_catalog(session: Session) -> list[DiskState]:
    """Return hdcache disk state using the disk-row committed-byte counter."""

    rows = session.scalars(select(CacheDisk).order_by(CacheDisk.disk_id))
    return [
        DiskState(
            disk_id=row.disk_id,
            state=row.state,
            capacity_bytes=row.capacity_bytes,
            filled_bytes=row.filled_bytes,
            capacity_state=row.capacity_state,
            enclosure=row.enclosure,
            slot=row.slot,
        )
        for row in rows
    ]


def choose_placement(
    session: Session,
    *,
    content_sha256: bytes,
    size_bytes: int,
    artifactclass: str,
    bundle_key: str | None,
    group_key: str | None,
    policy: DiskPlacementPolicy | None = None,
) -> str:
    """Build catalog context and choose one hdcache disk for a future fill."""

    ctx = build_placement_context(
        session,
        content_sha256=content_sha256,
        size_bytes=size_bytes,
        artifactclass=artifactclass,
        bundle_key=bundle_key,
        group_key=group_key,
    )
    final_policy = policy or DefaultDiskPlacementPolicy(placement_config_from_env())
    return final_policy.choose(disk_states_from_catalog(session), ctx)


def placement_config_from_env() -> PlacementConfig:
    """Load hdcache placement knobs from environment variables."""

    return PlacementConfig(
        spread_min_bytes=_env_int(
            "SUTRADHARA_HDCACHE_SPREAD_MIN_BYTES",
            DEFAULT_SPREAD_MIN_BYTES,
        ),
        reserve_largest_expected_file_bytes=_env_int(
            "SUTRADHARA_HDCACHE_RESERVE_LARGEST_EXPECTED_FILE_BYTES",
            0,
        ),
        reserve_tmp_headroom_bytes=_env_int(
            "SUTRADHARA_HDCACHE_RESERVE_TMP_HEADROOM_BYTES",
            0,
        ),
        reserve_fraction=_env_float(
            "SUTRADHARA_HDCACHE_RESERVE_FRACTION",
            DEFAULT_RESERVE_FRACTION,
        ),
        enclosure_spread=_env_bool("SUTRADHARA_HDCACHE_ENCLOSURE_SPREAD", False),
    )


def _tiebreak(disk_id: str, content_sha256: bytes) -> bytes:
    return hashlib.sha256(disk_id.encode("utf-8") + content_sha256).digest()


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")
