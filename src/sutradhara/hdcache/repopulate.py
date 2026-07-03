"""Repopulation, retire-drain, and drill tracking for the hdcache tier.

This module closes the cache lifecycle after disk death or retirement. Dead
disks create drill-tagged lost rows that are repopulated from archival tape in
source-tape batches. Retiring disks are drained by verified local reads first,
falling back to tape only when the local cache stream cannot be trusted.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import os
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, joinedload

from sutradhara.archive_restore import (
    ArchiveExtractor,
    ArchiveRestoreError,
    RestoreResult,
    restore_asset,
    restore_assets_from_bundle,
)
from sutradhara.artifactclass_policy import get_artifactclass_policy
from sutradhara.backend.port import StorageBackend
from sutradhara.catalog.models import ArtifactClassPool, AssetLocator, Copy
from sutradhara.catalog.types import CopyHealth
from sutradhara.hdcache.fill import (
    DOMAIN,
    HdcacheFillBlocked,
    HdcacheFillConfig,
    HdcacheFillPlan,
    HdcacheFillResult,
    HdcacheFillTarget,
    count_live_hdcache_jobs,
    desired_target_for_asset,
    fill_config_from_env,
    fill_target_from_plaintext,
    resolve_restore_backends,
)
from sutradhara.hdcache.models import CacheDisk, CacheEntry
from sutradhara.hdcache.store import (
    AEAD_REPRESENTATION,
    RAW_REPRESENTATION,
    StoreContentMismatch,
    StoreError,
    StoreReadTimeout,
    delete_entry,
    read_entry_verified,
)
from sutradhara.jobs.engine import submit
from sutradhara.jobs.models import LIVE_JOB_STATUS_VALUES, Job
from sutradhara.jobs.reconcilers.conditions import OBSERVED_MISSING, record_observation
from sutradhara.keys import KEY_DOMAIN_HDCACHE, KeyEpoch, KeyRegistry, assert_key_epoch_domain
from sutradhara.restore import sha256_file
from sutradhara.sealing.port import Opener, Representation
from sutradhara.sealing.rao import RaoCliOpener

DEFAULT_REPOP_SCRATCH_ROOT = Path("/var/lib/replica/hdcache-repopulate-scratch")
DEFAULT_REPOP_BATCH_SECONDS = 30 * 60
DEFAULT_REPOP_TAPE_BYTES_PER_SECOND = 300 * 1024 * 1024
DEFAULT_DRAIN_READ_DEADLINE_SECONDS = 70.0
REPOP_BATCH_DEDUPE_PREFIX = "hdcache:repop-batch:"

RestoreBackendResolver = Callable[[Session, bytes], dict[int, StorageBackend]]


class RepopulationError(RuntimeError):
    """Raised when a repopulation or drain operation cannot complete safely."""


@dataclass(frozen=True)
class RepopulationConfig:
    """Runtime knobs for tape repopulation and verified retire-drain."""

    fill_config: HdcacheFillConfig = field(default_factory=fill_config_from_env)
    scratch_root: Path = DEFAULT_REPOP_SCRATCH_ROOT
    batch_seconds: int = DEFAULT_REPOP_BATCH_SECONDS
    tape_bytes_per_second: int = DEFAULT_REPOP_TAPE_BYTES_PER_SECOND
    extractor: ArchiveExtractor | None = None
    restore_backends: dict[int, StorageBackend] | None = None
    restore_backend_resolver: RestoreBackendResolver = resolve_restore_backends
    read_deadline_seconds: float = DEFAULT_DRAIN_READ_DEADLINE_SECONDS
    key_registry: KeyRegistry | None = None
    opener: Opener | None = None
    sealer: Any | None = None
    key_epoch: KeyEpoch | None = None

    def __post_init__(self) -> None:
        if self.batch_seconds <= 0:
            raise ValueError("batch_seconds must be positive")
        if self.tape_bytes_per_second <= 0:
            raise ValueError("tape_bytes_per_second must be positive")
        if self.read_deadline_seconds <= 0:
            raise ValueError("read_deadline_seconds must be positive")
        object.__setattr__(self, "scratch_root", Path(self.scratch_root))

    @property
    def max_batch_bytes(self) -> int:
        return self.batch_seconds * self.tape_bytes_per_second

    def registry(self) -> KeyRegistry:
        return self.key_registry or KeyRegistry()


@dataclass(frozen=True)
class SourceTape:
    """Primary archive source used to group repopulation work."""

    token: str
    pool_id: str
    copy_id: int
    bundle_id: str | None


@dataclass(frozen=True)
class LostTarget:
    """One lost cache entry resolved to a fill target and source tape."""

    target: HdcacheFillTarget
    source_tape: SourceTape
    origin_disk_id: str | None
    lost_drill_id: str | None
    lost_at: dt.datetime | None


@dataclass(frozen=True)
class RepopulationBatch:
    """One bounded tape batch submitted as a fill job."""

    source_tape: str
    items: tuple[LostTarget, ...]

    @property
    def bytes_total(self) -> int:
        return sum(item.target.size_bytes for item in self.items)

    @property
    def batch_id(self) -> str:
        digest = hashlib.sha256()
        digest.update(self.source_tape.encode("utf-8"))
        for item in self.items:
            digest.update(item.target.content_sha256)
        return digest.hexdigest()[:24]


@dataclass(frozen=True)
class DrillStatus:
    """Progress view for one dead-disk repopulation drill."""

    disk_id: str
    drill_id: str
    started_at: dt.datetime | None
    remaining_entries: int
    remaining_bytes: int
    refilled_entries: int
    refilled_bytes: int
    bytes_per_hour: float | None
    eta_seconds: float | None
    completed: bool


@dataclass(frozen=True)
class DrainResult:
    """Summary of one retire-drain pass."""

    disk_id: str
    moved: int
    fallback_to_tape: int
    failed: int
    auto_dead: bool


def repopulation_config_from_env() -> RepopulationConfig:
    """Load repopulation knobs from environment variables."""

    return RepopulationConfig(
        fill_config=fill_config_from_env(),
        scratch_root=Path(
            os.environ.get("SUTRADHARA_HDCACHE_REPOP_SCRATCH_ROOT")
            or DEFAULT_REPOP_SCRATCH_ROOT
        ),
        batch_seconds=_env_int("SUTRADHARA_HDCACHE_REPOP_BATCH_SECONDS", DEFAULT_REPOP_BATCH_SECONDS),
        tape_bytes_per_second=_env_int(
            "SUTRADHARA_HDCACHE_REPOP_TAPE_BYTES_PER_SECOND",
            DEFAULT_REPOP_TAPE_BYTES_PER_SECOND,
        ),
        read_deadline_seconds=_env_float(
            "SUTRADHARA_HDCACHE_DRAIN_READ_DEADLINE_SECONDS",
            DEFAULT_DRAIN_READ_DEADLINE_SECONDS,
        ),
    )


def enqueue_repopulation(
    session: Session,
    *,
    config: RepopulationConfig | None = None,
) -> HdcacheFillPlan:
    """Plan and submit tape-grouped fill work for currently lost entries."""

    final_config = config or repopulation_config_from_env()
    total_slots = final_config.fill_config.live_job_cap - count_live_hdcache_jobs(session)
    repop_slots = (
        final_config.fill_config.effective_repopulation_live_job_cap
        - count_live_repopulation_jobs(session)
    )
    slots = min(total_slots, repop_slots)
    if slots <= 0:
        return HdcacheFillPlan(count=0, bytes_total=0, scheduled=0)
    lost_targets = list(_lost_targets(session))
    if not lost_targets:
        return HdcacheFillPlan(count=0, bytes_total=0, scheduled=0)
    batches = _make_batches(lost_targets, max_batch_bytes=final_config.max_batch_bytes)
    scheduled = 0
    for batch in batches:
        if scheduled >= slots:
            break
        if _submit_repopulation_batch(session, batch, config=final_config) is not None:
            scheduled += 1
    return HdcacheFillPlan(
        count=len(lost_targets),
        bytes_total=sum(item.target.size_bytes for item in lost_targets),
        scheduled=scheduled,
    )


def execute_repopulation_batch(
    session: Session,
    params: dict[str, Any],
    *,
    config: RepopulationConfig | None = None,
) -> list[HdcacheFillResult]:
    """Execute one hdcache_fill job carrying a repopulation batch payload."""

    final_config = config or repopulation_config_from_env()
    raw_items = _batch_items(params)
    items_by_sha = {
        _target_from_item(item).content_sha256: item
        for item in raw_items
    }
    targets = [
        target
        for item in raw_items
        if (target := _revalidate_repopulation_item(session, item)) is not None
    ]
    if not targets:
        return []
    final_config.scratch_root.mkdir(parents=True, exist_ok=True)
    os.chmod(final_config.scratch_root, 0o700)
    results: list[HdcacheFillResult] = []
    with tempfile.TemporaryDirectory(prefix="hdcache-repop-batch-", dir=final_config.scratch_root) as raw:
        root = Path(raw)
        for group in _group_targets_for_restore(targets):
            group = [
                target
                for target in group
                if _revalidate_repopulation_item(
                    session,
                    items_by_sha[target.content_sha256],
                )
                is not None
            ]
            if not group:
                continue
            if len(group) > 1 and group[0].bundle_key:
                restored = _restore_bundle_group(session, group, root, config=final_config)
                for target in group:
                    current_target = _revalidate_repopulation_item(
                        session,
                        items_by_sha[target.content_sha256],
                    )
                    if current_target is None:
                        continue
                    result = restored[target.content_sha256]
                    results.append(
                        fill_target_from_plaintext(
                            session,
                            current_target,
                            source_path=result.output_path,
                            source_kind="restore-batch",
                            config=final_config.fill_config,
                            key_registry=final_config.key_registry,
                            sealer=final_config.sealer,
                            key_epoch=final_config.key_epoch,
                        )
                    )
                continue
            target = group[0]
            current_target = _revalidate_repopulation_item(
                session,
                items_by_sha[target.content_sha256],
            )
            if current_target is None:
                continue
            restored_path = root / target.sha_hex
            _restore_single_target(session, current_target, restored_path, config=final_config)
            current_target = _revalidate_repopulation_item(
                session,
                items_by_sha[target.content_sha256],
            )
            if current_target is None:
                continue
            results.append(
                fill_target_from_plaintext(
                    session,
                    current_target,
                    source_path=restored_path,
                    source_kind="restore",
                    config=final_config.fill_config,
                    key_registry=final_config.key_registry,
                    sealer=final_config.sealer,
                    key_epoch=final_config.key_epoch,
                )
            )
    return results


def drain_retiring_disk(
    session: Session,
    disk_id: str,
    *,
    config: RepopulationConfig | None = None,
    limit: int | None = None,
) -> DrainResult:
    """Move entries off one retiring disk via verified local reads, then tape fallback."""

    final_config = config or repopulation_config_from_env()
    disk = session.get(CacheDisk, disk_id)
    if disk is None:
        raise RepopulationError(f"unknown cache disk: {disk_id}")
    if disk.state != "retiring":
        raise RepopulationError(f"cache disk {disk_id} is state={disk.state!r}, not retiring")
    query = (
        select(CacheEntry)
        .where(CacheEntry.disk_id == disk_id, CacheEntry.state == "present")
        .order_by(CacheEntry.content_sha256)
    )
    if limit is not None:
        query = query.limit(limit)
    entries = [
        entry.content_sha256
        for entry in session.scalars(query)
    ]
    moved = 0
    fallback = 0
    failed = 0
    final_config.scratch_root.mkdir(parents=True, exist_ok=True)
    os.chmod(final_config.scratch_root, 0o700)
    for digest in entries:
        disk = _fresh_retiring_disk(session, disk_id)
        entry = _fresh_drain_entry(session, digest, disk_id)
        if entry is None:
            continue
        target = desired_target_for_asset(session, entry.content_sha256)
        if target is None:
            failed += 1
            continue
        old_representation = entry.representation
        old_key_epoch = entry.key_epoch
        with tempfile.TemporaryDirectory(prefix="hdcache-drain-", dir=final_config.scratch_root) as raw:
            plaintext = Path(raw) / "plain"
            try:
                _read_entry_plaintext(session, disk, entry, plaintext, config=final_config)
                source_kind = "drain-local"
            except (StoreContentMismatch, StoreReadTimeout, StoreError, OSError, RuntimeError, ValueError):
                disk = _fresh_retiring_disk(session, disk_id)
                entry = _fresh_drain_entry(session, digest, disk_id)
                if entry is None:
                    continue
                _restore_single_target(session, target, plaintext, config=final_config)
                source_kind = "drain-tape"
                fallback += 1
            try:
                disk = _fresh_retiring_disk(session, disk_id)
                entry = _fresh_drain_entry(session, digest, disk_id)
                if entry is None:
                    continue
                fill_target_from_plaintext(
                    session,
                    target,
                    source_path=plaintext,
                    source_kind=source_kind,
                    config=final_config.fill_config,
                    key_registry=final_config.key_registry,
                    sealer=final_config.sealer,
                    key_epoch=final_config.key_epoch,
                )
            except RepopulationError:
                raise
            except (HdcacheFillBlocked, OSError, RuntimeError, ValueError):
                failed += 1
                continue
        disk = _fresh_retiring_disk(session, disk_id)
        with contextlib.suppress(Exception):
            delete_entry(
                Path(disk.mount),
                digest,
                representation=old_representation,
                key_epoch=old_key_epoch,
            )
        moved += 1
    auto_dead = _maybe_auto_dead(session, disk)
    return DrainResult(
        disk_id=disk_id,
        moved=moved,
        fallback_to_tape=fallback,
        failed=failed,
        auto_dead=auto_dead,
    )


def drain_retiring_disks(
    session: Session,
    *,
    config: RepopulationConfig | None = None,
    limit_per_disk: int | None = None,
) -> list[DrainResult]:
    """Run one drain pass for every retiring disk."""

    return [
        drain_retiring_disk(session, disk.disk_id, config=config, limit=limit_per_disk)
        for disk in session.scalars(
            select(CacheDisk).where(CacheDisk.state == "retiring").order_by(CacheDisk.disk_id)
        )
    ]


def drill_status(
    session: Session,
    disk_id: str | None = None,
    *,
    now: dt.datetime | None = None,
) -> list[DrillStatus]:
    """Return latest drill progress for one disk or all disks with drill history."""

    query = select(CacheEntry).where(CacheEntry.lost_drill_id.is_not(None))
    if disk_id is not None:
        query = query.where(CacheEntry.lost_origin_disk_id == disk_id)
    entries = list(session.scalars(query))
    latest_by_disk: dict[str, str] = {}
    for entry in entries:
        if entry.lost_origin_disk_id is None or entry.lost_drill_id is None:
            continue
        current = latest_by_disk.get(entry.lost_origin_disk_id)
        if current is None or entry.lost_drill_id > current:
            latest_by_disk[entry.lost_origin_disk_id] = entry.lost_drill_id
    local_now = _as_utc(now or dt.datetime.now(dt.UTC))
    statuses = [
        _status_for_drill(
            disk,
            drill_id,
            [entry for entry in entries if entry.lost_drill_id == drill_id],
            now=local_now,
        )
        for disk, drill_id in sorted(latest_by_disk.items())
    ]
    return statuses


def repopulation_batch_payload(batch: RepopulationBatch) -> dict[str, Any]:
    """Return the JSON params carried by a repopulation batch fill job."""

    return {
        "repopulate_batch": True,
        "batch_id": batch.batch_id,
        "source_tape": batch.source_tape,
        "origin_drill_ids": sorted(
            {
                item.lost_drill_id
                for item in batch.items
                if item.lost_drill_id is not None
            }
        ),
        "items": [_batch_item_payload(item) for item in batch.items],
    }


def count_live_repopulation_jobs(session: Session) -> int:
    """Count live hdcache-fill jobs that carry repopulation batch payloads."""

    rows = session.scalars(
        select(Job).where(Job.kind == "hdcache_fill", Job.status.in_(LIVE_JOB_STATUS_VALUES))
    )
    return sum(1 for job in rows if job.params.get("repopulate_batch") is True)


def _lost_targets(session: Session) -> Iterable[LostTarget]:
    rows = list(
        session.scalars(
            select(CacheEntry)
            .where(CacheEntry.state == "lost")
            .order_by(CacheEntry.lost_at, CacheEntry.content_sha256)
        )
    )
    for entry in rows:
        target = desired_target_for_asset(session, entry.content_sha256)
        if target is None:
            continue
        source_tape = _source_tape_for_target(session, target)
        if source_tape is None:
            continue
        yield LostTarget(
            target=target,
            source_tape=source_tape,
            origin_disk_id=entry.lost_origin_disk_id,
            lost_drill_id=entry.lost_drill_id,
            lost_at=entry.lost_at,
        )


def _source_tape_for_target(session: Session, target: HdcacheFillTarget) -> SourceTape | None:
    pool_order = _restore_pool_order(session, target.artifactclass)
    pool_rank = {pool_id: index for index, pool_id in enumerate(pool_order)}
    locators = list(
        session.scalars(
            select(AssetLocator)
            .options(joinedload(AssetLocator.copy))
            .where(AssetLocator.logical_asset_hash == target.content_sha256)
        )
    )
    candidates: list[tuple[int, int, SourceTape]] = []
    for locator in locators:
        copy = locator.copy
        if (
            copy is None
            or copy.health != CopyHealth.OK
            or copy.deleted_at is not None
            or locator.pool_id not in pool_rank
        ):
            continue
        candidates.append(
            (
                pool_rank[locator.pool_id],
                locator.id,
                SourceTape(
                    token=_source_tape_token(copy),
                    pool_id=locator.pool_id,
                    copy_id=copy.id,
                    bundle_id=locator.bundle_id,
                ),
            )
        )
    if not candidates:
        return None
    return min(candidates, key=lambda item: (item[0], item[1]))[2]


def _restore_pool_order(session: Session, artifactclass: str) -> list[str]:
    policy = get_artifactclass_policy(session, artifactclass)
    order: list[str] = []
    seen: set[str] = set()
    for pool_id in policy.restore_preference:
        if pool_id not in seen:
            seen.add(pool_id)
            order.append(pool_id)
    for membership in session.scalars(
        select(ArtifactClassPool)
        .where(
            ArtifactClassPool.artifactclass == artifactclass,
            ArtifactClassPool.active.is_(True),
        )
        .order_by(ArtifactClassPool.sort_order, ArtifactClassPool.pool_id)
    ):
        if membership.pool_id not in seen:
            seen.add(membership.pool_id)
            order.append(membership.pool_id)
    return order


def _source_tape_token(copy: Copy) -> str:
    locator = dict(copy.native_locator or {})
    pool = copy.pool_id or locator.get("pool_id") or "unknown-pool"
    for key in ("tape_uuid", "barcode", "volume_uuid"):
        value = locator.get(key)
        if isinstance(value, str) and value:
            return f"backend:{copy.backend_id}:pool:{pool}:{key}:{value}"
    return f"backend:{copy.backend_id}:pool:{pool}:copy:{copy.id}"


def _make_batches(
    targets: list[LostTarget],
    *,
    max_batch_bytes: int,
) -> list[RepopulationBatch]:
    grouped: dict[str, list[LostTarget]] = defaultdict(list)
    for target in targets:
        grouped[target.source_tape.token].append(target)
    batches: list[RepopulationBatch] = []
    for source_tape in sorted(grouped):
        current: list[LostTarget] = []
        current_bytes = 0
        ordered = sorted(
            grouped[source_tape],
            key=lambda item: (
                item.target.bundle_key or "",
                item.lost_at or dt.datetime.min.replace(tzinfo=dt.UTC),
                item.target.sha_hex,
            ),
        )
        for item in ordered:
            if current and current_bytes + item.target.size_bytes > max_batch_bytes:
                batches.append(RepopulationBatch(source_tape=source_tape, items=tuple(current)))
                current = []
                current_bytes = 0
            current.append(item)
            current_bytes += item.target.size_bytes
        if current:
            batches.append(RepopulationBatch(source_tape=source_tape, items=tuple(current)))
    return batches


def _submit_repopulation_batch(
    session: Session,
    batch: RepopulationBatch,
    *,
    config: RepopulationConfig,
) -> Job | None:
    target_key = _repopulation_recon_target_key(batch)
    record_observation(
        session,
        domain=DOMAIN,
        target_key=target_key,
        desired=True,
        observed_state=OBSERVED_MISSING,
    )
    return submit(
        session,
        "hdcache_fill",
        repopulation_batch_payload(batch),
        required_resources=[{"pool": "io", "count": 1}],
        priority=config.fill_config.effective_repopulation_priority,
        dedupe_key=f"{REPOP_BATCH_DEDUPE_PREFIX}{batch.batch_id}",
        recon_domain=DOMAIN,
        recon_target_key=target_key,
    )


def _repopulation_recon_target_key(batch: RepopulationBatch) -> str:
    drills = sorted({item.lost_drill_id for item in batch.items if item.lost_drill_id})
    drill_part = ",".join(drills) if drills else "untracked"
    if len(drill_part) > 96:
        drill_part = hashlib.sha256(drill_part.encode("utf-8")).hexdigest()[:24]
    return f"repop:{drill_part}:{batch.batch_id}"


def _batch_item_payload(item: LostTarget) -> dict[str, Any]:
    return {
        "content_sha256": item.target.sha_hex,
        "artifactclass": item.target.artifactclass,
        "size_bytes": item.target.size_bytes,
        "bundle_key": item.target.bundle_key,
        "group_key": item.target.group_key,
        "source_path": item.target.source_path,
        "origin_disk_id": item.origin_disk_id,
        "lost_drill_id": item.lost_drill_id,
        "lost_at": None if item.lost_at is None else _as_utc(item.lost_at).isoformat(),
    }


def _batch_items(params: dict[str, Any]) -> list[dict[str, Any]]:
    items = params.get("items")
    if not isinstance(items, list):
        raise RepopulationError("repopulation batch requires params.items")
    if not all(isinstance(item, dict) for item in items):
        raise RepopulationError("repopulation batch items must be objects")
    return items


def _target_from_item(item: dict[str, Any]) -> HdcacheFillTarget:
    raw_sha = item.get("content_sha256")
    artifactclass = item.get("artifactclass")
    size_bytes = item.get("size_bytes")
    if not isinstance(raw_sha, str) or len(raw_sha) != 64:
        raise RepopulationError("repopulation item requires content_sha256")
    if not isinstance(artifactclass, str) or not artifactclass:
        raise RepopulationError("repopulation item requires artifactclass")
    if not isinstance(size_bytes, int) or size_bytes < 0:
        raise RepopulationError("repopulation item requires non-negative size_bytes")
    return HdcacheFillTarget(
        content_sha256=bytes.fromhex(raw_sha),
        artifactclass=artifactclass,
        size_bytes=size_bytes,
        bundle_key=_optional_str(item.get("bundle_key")),
        group_key=_optional_str(item.get("group_key")),
        source_path=_optional_str(item.get("source_path")),
    )


def _revalidate_repopulation_item(
    session: Session,
    item: dict[str, Any],
) -> HdcacheFillTarget | None:
    payload_target = _target_from_item(item)
    entry = session.get(CacheEntry, payload_target.content_sha256)
    if entry is None:
        return None
    session.refresh(entry)
    if entry.state != "lost":
        return None
    expected_drill_id = _optional_str(item.get("lost_drill_id"))
    if expected_drill_id is not None and entry.lost_drill_id != expected_drill_id:
        return None
    expected_origin_disk = _optional_str(item.get("origin_disk_id"))
    if expected_origin_disk is not None and entry.lost_origin_disk_id != expected_origin_disk:
        return None
    return desired_target_for_asset(session, payload_target.content_sha256)


def _group_targets_for_restore(targets: list[HdcacheFillTarget]) -> list[list[HdcacheFillTarget]]:
    grouped: dict[tuple[str, str | None], list[HdcacheFillTarget]] = defaultdict(list)
    for target in targets:
        grouped[(target.artifactclass, target.bundle_key)].append(target)
    return [grouped[key] for key in sorted(grouped, key=lambda item: (item[0], item[1] or ""))]


def _restore_bundle_group(
    session: Session,
    targets: list[HdcacheFillTarget],
    root: Path,
    *,
    config: RepopulationConfig,
) -> dict[bytes, RestoreResult]:
    backends = _backends_for_targets(session, targets, config=config)
    results = restore_assets_from_bundle(
        session,
        asset_hashes=[target.content_sha256 for target in targets],
        artifactclass=targets[0].artifactclass,
        destination_dir=root / f"bundle-{targets[0].bundle_key}",
        backends=backends,
        extractor=config.extractor,
    )
    return {result.asset_hash: result for result in results}


def _restore_single_target(
    session: Session,
    target: HdcacheFillTarget,
    destination: Path,
    *,
    config: RepopulationConfig,
) -> RestoreResult:
    backends = config.restore_backends or config.restore_backend_resolver(
        session,
        target.content_sha256,
    )
    if not backends:
        raise RepopulationError(f"no restore backend available for {target.sha_hex}")
    try:
        return restore_asset(
            session,
            asset_hash=target.content_sha256,
            artifactclass=target.artifactclass,
            destination=destination,
            backends=backends,
            extractor=config.extractor,
        )
    except ArchiveRestoreError as exc:
        raise RepopulationError(str(exc)) from exc


def _backends_for_targets(
    session: Session,
    targets: list[HdcacheFillTarget],
    *,
    config: RepopulationConfig,
) -> dict[int, StorageBackend]:
    if config.restore_backends is not None:
        return dict(config.restore_backends)
    backends: dict[int, StorageBackend] = {}
    for target in targets:
        backends.update(config.restore_backend_resolver(session, target.content_sha256))
    return backends


def _read_entry_plaintext(
    session: Session,
    disk: CacheDisk,
    entry: CacheEntry,
    destination: Path,
    *,
    config: RepopulationConfig,
) -> None:
    deadline = time.monotonic() + config.read_deadline_seconds
    if entry.representation == RAW_REPRESENTATION:
        with destination.open("wb") as output:
            read_entry_verified(
                Path(disk.mount),
                entry.content_sha256,
                representation=RAW_REPRESENTATION,
                output=output,
                deadline_monotonic=deadline,
                disk_id=disk.disk_id,
            )
        return
    if entry.representation != AEAD_REPRESENTATION:
        raise StoreError(f"unsupported cache representation {entry.representation!r}")
    if entry.key_epoch is None or entry.stored_digest is None:
        raise StoreError("AEAD cache entry lacks key epoch or stored digest")
    assert_key_epoch_domain(entry.key_epoch, KEY_DOMAIN_HDCACHE, context="hdcache drain")
    sealed = destination.with_suffix(".sealed")
    with sealed.open("wb") as output:
        read_entry_verified(
            Path(disk.mount),
            entry.content_sha256,
            representation=AEAD_REPRESENTATION,
            key_epoch=entry.key_epoch,
            expected_stream_sha256=entry.stored_digest,
            output=output,
            deadline_monotonic=deadline,
            disk_id=disk.disk_id,
        )
    opener = config.opener or RaoCliOpener(config.registry(), work_dir=config.scratch_root)
    with opener.open(
        sealed,
        Representation.RAO_AEAD_V1,
        key_epoch=KeyEpoch(key_id=entry.key_epoch, created_at="", active=True),
        work_dir=config.scratch_root,
    ) as plaintext:
        digest = sha256_file(plaintext)
        if digest != entry.content_sha256:
            raise StoreContentMismatch("opened cache plaintext digest mismatch")
        with plaintext.open("rb") as raw_in, destination.open("wb") as raw_out:
            for chunk in iter(lambda: raw_in.read(1024 * 1024), b""):
                raw_out.write(chunk)


def _fresh_retiring_disk(session: Session, disk_id: str) -> CacheDisk:
    disk = session.get(CacheDisk, disk_id)
    if disk is None:
        raise RepopulationError(f"unknown cache disk: {disk_id}")
    session.refresh(disk)
    if disk.state != "retiring":
        raise RepopulationError(
            f"cache disk {disk_id} changed to state={disk.state!r}; aborting retire drain"
        )
    return disk


def _fresh_drain_entry(
    session: Session,
    digest: bytes,
    disk_id: str,
) -> CacheEntry | None:
    entry = session.get(CacheEntry, digest)
    if entry is None:
        return None
    session.refresh(entry)
    if entry.state != "present" or entry.disk_id != disk_id:
        return None
    return entry


def _maybe_auto_dead(session: Session, disk: CacheDisk) -> bool:
    remaining = session.scalar(
        select(func.count()).select_from(CacheEntry).where(CacheEntry.disk_id == disk.disk_id)
    )
    if int(remaining or 0) != 0:
        return False
    disk.state = "dead"
    session.flush([disk])
    return True


def _status_for_drill(
    disk_id: str,
    drill_id: str,
    entries: list[CacheEntry],
    *,
    now: dt.datetime,
) -> DrillStatus:
    started_values = [_as_utc(entry.lost_at) for entry in entries if entry.lost_at is not None]
    started_at = min(started_values) if started_values else None
    remaining = [entry for entry in entries if entry.state == "lost"]
    refilled = [entry for entry in entries if entry.state == "present" and entry.refilled_at is not None]
    remaining_bytes = sum(entry.size_bytes for entry in remaining)
    refilled_bytes = sum(entry.size_bytes for entry in refilled)
    bytes_per_hour: float | None = None
    eta_seconds: float | None = None
    if started_at is not None:
        elapsed_hours = max((now - started_at).total_seconds() / 3600, 0.0)
        if elapsed_hours > 0 and refilled_bytes > 0:
            bytes_per_hour = refilled_bytes / elapsed_hours
            eta_seconds = None if remaining_bytes == 0 else remaining_bytes / (bytes_per_hour / 3600)
    return DrillStatus(
        disk_id=disk_id,
        drill_id=drill_id,
        started_at=started_at,
        remaining_entries=len(remaining),
        remaining_bytes=remaining_bytes,
        refilled_entries=len(refilled),
        refilled_bytes=refilled_bytes,
        bytes_per_hour=bytes_per_hour,
        eta_seconds=eta_seconds,
        completed=bool(entries) and not remaining,
    )


def _optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _as_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


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
