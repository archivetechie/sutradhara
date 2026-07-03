"""Bounded hdcache fill orchestration.

The fill path is the only M3 code that writes bytes onto cache disks. It keeps
the disk tier outside archival copy truth, verifies every written stream against
the logical asset digest, and uses the reconciler/job surface to avoid dumping a
large desired-state diff into the worker queue.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import errno
import hashlib
import os
import tempfile
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, joinedload

from sutradhara.archive_restore import ArchiveRestoreError, restore_asset
from sutradhara.artifactclass_policy import hdcache_policy_from_json
from sutradhara.backend.factory import backend_from_row
from sutradhara.backend.port import StorageBackend
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    AssetLocator,
    Bundle,
    BundleMember,
    Copy,
    IngestItem,
    LogicalAsset,
)
from sutradhara.catalog.types import CopyHealth, is_content_hash
from sutradhara.hdcache.models import CacheDisk, CacheEntry
from sutradhara.hdcache.placement import PlacementError, choose_placement
from sutradhara.hdcache.store import (
    AEAD_REPRESENTATION,
    RAW_REPRESENTATION,
    StoreError,
    delete_entry,
    write_entry,
)
from sutradhara.jobs.engine import submit
from sutradhara.jobs.models import LIVE_JOB_STATUS_VALUES, Job, ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    CONDITION_BLOCKED,
    OBSERVED_MISSING,
    OBSERVED_PRESENT,
)
from sutradhara.keys import KEY_DOMAIN_HDCACHE, KeyEpoch, KeyRegistry, assert_key_epoch_domain
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import RaoCliSealer

DOMAIN = "hdcache"
JOB_KIND = "hdcache_fill"
DEDUPE_PREFIX = "hdcache:"

OPERATOR_RESTORE_PRIORITY = 0
HDCACHE_FILL_PRIORITY = 50
MIGRATION_PRIORITY = 100
DEFAULT_LIVE_JOB_CAP = 500
DEFAULT_SCRATCH_ROOT = Path("/var/lib/replica/hdcache-scratch")


class HdcacheFillError(RuntimeError):
    """A hdcache fill failed after it was admitted as work."""


class HdcacheFillBlocked(HdcacheFillError):
    """A hdcache fill cannot make progress without external repair."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class HdcacheFillConfig:
    """Scheduling and scratch knobs for hdcache fills.

    Priority integers are intentionally stated here: lower values run sooner in
    the job engine, so hdcache fill (50) sits below operator restores (0) and
    above migration-style work (100).
    """

    live_job_cap: int = DEFAULT_LIVE_JOB_CAP
    priority: int = HDCACHE_FILL_PRIORITY
    operator_restore_priority: int = OPERATOR_RESTORE_PRIORITY
    migration_priority: int = MIGRATION_PRIORITY
    scratch_root: Path = DEFAULT_SCRATCH_ROOT

    def __post_init__(self) -> None:
        if self.live_job_cap < 0:
            raise ValueError("live_job_cap must be non-negative")
        if self.operator_restore_priority >= self.priority:
            raise ValueError("hdcache priority must be below operator restores")
        if self.priority >= self.migration_priority:
            raise ValueError("hdcache priority must be above migration")
        object.__setattr__(self, "scratch_root", Path(self.scratch_root))


@dataclass(frozen=True)
class HdcacheFillTarget:
    """One desired hdcache entry, derived from archive catalog truth."""

    content_sha256: bytes
    artifactclass: str
    size_bytes: int
    bundle_key: str | None = None
    group_key: str | None = None
    source_path: str | None = None

    @property
    def sha_hex(self) -> str:
        return self.content_sha256.hex()


@dataclass(frozen=True)
class HdcacheFillPlan:
    """Dry-run or enqueue summary for one hdcache fill request."""

    count: int
    bytes_total: int
    scheduled: int = 0


@dataclass(frozen=True)
class HdcacheFillResult:
    """Completed fill details for one cache entry."""

    content_sha256: bytes
    disk_id: str
    relpath: str
    size_bytes: int
    representation: str
    key_epoch: str | None
    stored_digest: bytes | None
    source: str
    already_present: bool = False


RestoreBackendResolver = Callable[[Session, bytes], dict[int, StorageBackend]]


def fill_config_from_env() -> HdcacheFillConfig:
    """Load hdcache fill config from environment variables."""

    return HdcacheFillConfig(
        live_job_cap=_env_int("SUTRADHARA_HDCACHE_LIVE_JOB_CAP", DEFAULT_LIVE_JOB_CAP),
        priority=_env_int("SUTRADHARA_HDCACHE_FILL_PRIORITY", HDCACHE_FILL_PRIORITY),
        operator_restore_priority=_env_int(
            "SUTRADHARA_OPERATOR_RESTORE_PRIORITY",
            OPERATOR_RESTORE_PRIORITY,
        ),
        migration_priority=_env_int("SUTRADHARA_MIGRATION_PRIORITY", MIGRATION_PRIORITY),
        scratch_root=Path(
            os.environ.get("SUTRADHARA_HDCACHE_SCRATCH_ROOT") or DEFAULT_SCRATCH_ROOT
        ),
    )


def make_target_key(content_sha256: bytes | str) -> str:
    """Return the hdcache reconciler target key for one asset."""

    return _sha_bytes(content_sha256).hex()


def dedupe_key(content_sha256: bytes | str) -> str:
    """Return the live-job dedupe key for one hdcache fill."""

    return f"{DEDUPE_PREFIX}{make_target_key(content_sha256)}"


def fill_target_from_params(session: Session, params: dict[str, Any]) -> HdcacheFillTarget:
    """Hydrate a fill target from a job payload, deriving omitted context if needed."""

    raw_sha = params.get("content_sha256")
    if not isinstance(raw_sha, str):
        raise HdcacheFillBlocked("bad-params", "hdcache_fill requires params.content_sha256")
    digest = _sha_bytes(raw_sha)
    artifactclass = params.get("artifactclass")
    if isinstance(artifactclass, str) and artifactclass:
        asset = session.get(LogicalAsset, digest)
        if asset is None:
            raise HdcacheFillBlocked("unknown-asset", f"no logical asset {digest.hex()}")
        return HdcacheFillTarget(
            content_sha256=digest,
            artifactclass=artifactclass,
            size_bytes=asset.size_bytes,
            bundle_key=_optional_str(params.get("bundle_key")),
            group_key=_optional_str(params.get("group_key")),
            source_path=_optional_str(params.get("source_path")),
        )
    target = desired_target_for_asset(session, digest)
    if target is None:
        raise HdcacheFillBlocked(
            "not-cacheable",
            f"asset {digest.hex()} is not currently a cacheable archived asset",
        )
    return target


def resolve_restore_backends(session: Session, content_sha256: bytes) -> dict[int, StorageBackend]:
    """Instantiate backends that have healthy archival locators for one asset."""

    backends: dict[int, StorageBackend] = {}
    for locator in session.scalars(
        select(AssetLocator)
        .options(joinedload(AssetLocator.copy).joinedload(Copy.backend))
        .where(AssetLocator.logical_asset_hash == content_sha256)
    ):
        copy = locator.copy
        if (
            copy is None
            or copy.backend is None
            or copy.health != CopyHealth.OK
            or copy.deleted_at is not None
        ):
            continue
        if copy.backend_id not in backends:
            backends[copy.backend_id] = backend_from_row(copy.backend)
    for copy in session.scalars(
        select(Copy)
        .options(joinedload(Copy.backend))
        .where(
            Copy.logical_asset_hash == content_sha256,
            Copy.health == CopyHealth.OK,
            Copy.deleted_at.is_(None),
        )
    ):
        if copy.backend_id not in backends:
            backends[copy.backend_id] = backend_from_row(copy.backend)
    return backends


def submit_hdcache_fill(
    session: Session,
    target: HdcacheFillTarget,
    *,
    config: HdcacheFillConfig | None = None,
) -> Job | None:
    """Submit one hdcache fill if live cap and condition gates allow it."""

    final_config = config or fill_config_from_env()
    if count_live_hdcache_jobs(session) >= final_config.live_job_cap:
        return None
    if _target_is_held(session, target.sha_hex):
        return None
    return submit(
        session,
        JOB_KIND,
        _target_payload(target),
        priority=final_config.priority,
        dedupe_key=dedupe_key(target.content_sha256),
        recon_domain=DOMAIN,
        recon_target_key=target.sha_hex,
    )


def enqueue_post_flush_hdcache_fills(
    session: Session,
    bundle_id: str,
    *,
    config: HdcacheFillConfig | None = None,
) -> HdcacheFillPlan:
    """Enqueue cache fills for cacheable members after a bundle flush."""

    bundle = session.get(Bundle, bundle_id)
    if bundle is None:
        return HdcacheFillPlan(count=0, bytes_total=0, scheduled=0)
    targets = _desired_targets_for_bundle(session, bundle)
    return enqueue_targets(session, targets, config=config)


def enqueue_targets(
    session: Session,
    targets: Sequence[HdcacheFillTarget],
    *,
    config: HdcacheFillConfig | None = None,
) -> HdcacheFillPlan:
    """Enqueue targets until the live cap is reached."""

    final_config = config or fill_config_from_env()
    scheduled = 0
    bytes_total = sum(target.size_bytes for target in targets)
    for target in targets:
        if submit_hdcache_fill(session, target, config=final_config) is None:
            if count_live_hdcache_jobs(session) >= final_config.live_job_cap:
                break
            continue
        scheduled += 1
    return HdcacheFillPlan(count=len(targets), bytes_total=bytes_total, scheduled=scheduled)


def enqueue_requested_fill(
    session: Session,
    selector: str,
    *,
    config: HdcacheFillConfig | None = None,
    dry_run: bool = False,
) -> HdcacheFillPlan:
    """Resolve a CLI selector (sha256 or artifactclass) and optionally enqueue fills."""

    targets = requested_targets(session, selector)
    if dry_run:
        return HdcacheFillPlan(
            count=len(targets),
            bytes_total=sum(target.size_bytes for target in targets),
            scheduled=0,
        )
    return enqueue_targets(session, targets, config=config)


def requested_targets(session: Session, selector: str) -> list[HdcacheFillTarget]:
    """Resolve one CLI fill selector to desired targets."""

    if _looks_sha_hex(selector):
        target = desired_target_for_asset(session, bytes.fromhex(selector))
        return [] if target is None else [target]
    return desired_targets_for_class(session, selector)


def top_up_lost_entries(
    session: Session,
    *,
    config: HdcacheFillConfig | None = None,
) -> HdcacheFillPlan:
    """Top up hdcache fill jobs for currently lost entries without exceeding the cap."""

    final_config = config or fill_config_from_env()
    remaining = final_config.live_job_cap - count_live_hdcache_jobs(session)
    if remaining <= 0:
        return HdcacheFillPlan(count=0, bytes_total=0, scheduled=0)
    targets: list[HdcacheFillTarget] = []
    for digest in session.scalars(
        select(CacheEntry.content_sha256)
        .where(CacheEntry.state == "lost")
        .order_by(CacheEntry.content_sha256)
        .limit(remaining)
    ):
        target = desired_target_for_asset(session, digest)
        if target is not None:
            targets.append(target)
    return enqueue_targets(session, targets, config=final_config)


def desired_targets_for_class(session: Session, artifactclass: str) -> list[HdcacheFillTarget]:
    """Return all currently desired hdcache targets for one artifactclass."""

    rows = list(
        session.scalars(
            select(BundleMember)
            .join(Bundle, Bundle.id == BundleMember.bundle_id)
            .where(Bundle.artifactclass == artifactclass)
            .order_by(BundleMember.id)
        )
    )
    return _unique_targets(
        target
        for member in rows
        if (target := desired_target_for_asset(session, member.logical_asset_hash)) is not None
        and target.artifactclass == artifactclass
    )


def enumerate_desired_targets(
    session: Session,
    *,
    cursor: int | None,
    batch: int,
) -> list[HdcacheFillTarget]:
    """Enumerate desired hdcache targets from a bounded bundle-member cursor."""

    query = select(BundleMember).order_by(BundleMember.id).limit(batch)
    if cursor is not None:
        query = query.where(BundleMember.id > cursor)
    return _unique_targets(
        target
        for member in session.scalars(query)
        if (target := desired_target_for_asset(session, member.logical_asset_hash)) is not None
    )


def desired_target_for_asset(
    session: Session,
    content_sha256: bytes,
) -> HdcacheFillTarget | None:
    """Return the canonical desired fill target for one archived cacheable asset."""

    if not is_content_hash(content_sha256):
        raise ValueError("content_sha256 must be a 32-byte SHA-256 hash")
    asset = session.get(LogicalAsset, content_sha256)
    if asset is None:
        return None
    candidates: list[tuple[int, str, int, HdcacheFillTarget]] = []
    for member, bundle, policy in session.execute(
        select(BundleMember, Bundle, ArtifactClassPolicyRecord)
        .join(Bundle, Bundle.id == BundleMember.bundle_id)
        .join(
            ArtifactClassPolicyRecord,
            ArtifactClassPolicyRecord.artifactclass == Bundle.artifactclass,
        )
        .where(BundleMember.logical_asset_hash == content_sha256)
        .order_by(BundleMember.id)
    ):
        if bundle.status != "sealed":
            continue
        if not hdcache_policy_from_json(policy.hdcache_config).enabled:
            continue
        if not has_archival_copy(session, content_sha256):
            continue
        target = HdcacheFillTarget(
            content_sha256=content_sha256,
            artifactclass=bundle.artifactclass,
            size_bytes=asset.size_bytes,
            bundle_key=bundle.id,
            group_key=_fallback_group_key(bundle),
            source_path=member.source_path,
        )
        privacy = hdcache_policy_from_json(policy.hdcache_config).privacy_level
        candidates.append((_privacy_rank(privacy), bundle.artifactclass, member.id, target))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1], -item[2]))[3]


def has_archival_copy(session: Session, content_sha256: bytes) -> bool:
    """Return true when at least one healthy archival copy records this asset."""

    locator_count = session.scalar(
        select(func.count())
        .select_from(AssetLocator)
        .join(Copy, AssetLocator.copy_id == Copy.id)
        .where(
            AssetLocator.logical_asset_hash == content_sha256,
            Copy.health == CopyHealth.OK,
            Copy.deleted_at.is_(None),
        )
    )
    if int(locator_count or 0) > 0:
        return True
    direct_count = session.scalar(
        select(func.count())
        .select_from(Copy)
        .where(
            Copy.logical_asset_hash == content_sha256,
            Copy.health == CopyHealth.OK,
            Copy.deleted_at.is_(None),
        )
    )
    return int(direct_count or 0) > 0


def observe_target(
    session: Session,
    target_key: str,
    *,
    mutate: bool,
    key_registry: KeyRegistry | None = None,
) -> tuple[bool, str]:
    """Return desired/observed state for one hdcache reconciler target."""

    digest = _sha_bytes(target_key)
    target = desired_target_for_asset(session, digest)
    if target is None:
        return False, OBSERVED_MISSING
    entry = session.get(CacheEntry, digest)
    if entry is None:
        return True, OBSERVED_MISSING
    if entry.state != "present":
        return True, OBSERVED_MISSING
    disk = session.get(CacheDisk, entry.disk_id)
    if disk is None or disk.state != "active":
        if mutate:
            entry.state = "lost"
            session.flush([entry])
        return True, OBSERVED_MISSING
    if entry_policy_conformant(session, entry, key_registry=key_registry):
        return True, OBSERVED_PRESENT
    if mutate:
        mark_entry_lost_and_delete(session, entry)
    return True, OBSERVED_MISSING


def fill_target(
    session: Session,
    target: HdcacheFillTarget,
    *,
    config: HdcacheFillConfig | None = None,
    key_registry: KeyRegistry | None = None,
    sealer: RaoCliSealer | None = None,
    key_epoch: KeyEpoch | None = None,
    restore_backends: dict[int, StorageBackend] | None = None,
    restore_backend_resolver: RestoreBackendResolver | None = None,
) -> HdcacheFillResult:
    """Fill one hdcache entry from landing first, then archive restore fallback."""

    final_config = config or fill_config_from_env()
    registry = key_registry or KeyRegistry()
    if not has_archival_copy(session, target.content_sha256):
        raise HdcacheFillBlocked(
            "not-archived",
            f"asset {target.sha_hex} has no healthy archival copy",
        )
    asset = session.get(LogicalAsset, target.content_sha256)
    if asset is None:
        raise HdcacheFillBlocked("unknown-asset", f"no logical asset {target.sha_hex}")
    privacy = effective_privacy_level(session, target.content_sha256)
    representation = AEAD_REPRESENTATION if privacy != "none" else RAW_REPRESENTATION
    epoch = key_epoch
    if representation == AEAD_REPRESENTATION:
        epoch = epoch or registry.create_epoch(domain=KEY_DOMAIN_HDCACHE)
        assert_key_epoch_domain(epoch, KEY_DOMAIN_HDCACHE, context="hdcache fill")

    existing = session.get(CacheEntry, target.content_sha256)
    if existing is not None and existing.state == "present":
        disk = session.get(CacheDisk, existing.disk_id)
        if (
            disk is not None
            and disk.state == "active"
            and entry_policy_conformant(session, existing, key_registry=registry)
        ):
            return HdcacheFillResult(
                content_sha256=target.content_sha256,
                disk_id=existing.disk_id,
                relpath=existing.relpath,
                size_bytes=existing.size_bytes,
                representation=existing.representation,
                key_epoch=existing.key_epoch,
                stored_digest=existing.stored_digest,
                source="cache",
                already_present=True,
            )
        mark_entry_lost_and_delete(session, existing)

    last_enospc: OSError | None = None
    for _attempt in range(2):
        entry = _prepare_filling_entry(
            session,
            target,
            stored_size_hint=asset.size_bytes,
            representation=representation,
            key_epoch=None if epoch is None else epoch.key_id,
        )
        disk = session.get(CacheDisk, entry.disk_id)
        if disk is None:
            raise HdcacheFillBlocked("missing-disk", f"cache disk {entry.disk_id!r} is missing")
        try:
            with _plaintext_source(
                session,
                target,
                config=final_config,
                restore_backends=restore_backends,
                restore_backend_resolver=restore_backend_resolver or resolve_restore_backends,
            ) as source:
                write_result = _write_source_to_disk(
                    target,
                    disk,
                    source.path,
                    representation=representation,
                    key_epoch=epoch,
                    config=final_config,
                    registry=registry,
                    sealer=sealer,
                )
                source_kind = source.kind
            entry.relpath = write_result.relpath
            entry.size_bytes = write_result.size_bytes
            entry.state = "present"
            entry.representation = representation
            entry.key_epoch = None if epoch is None else epoch.key_id
            entry.stored_digest = write_result.stored_digest
            entry.trusted = True
            disk.filled_bytes += write_result.size_bytes
            session.flush([entry, disk])
            return HdcacheFillResult(
                content_sha256=target.content_sha256,
                disk_id=disk.disk_id,
                relpath=entry.relpath,
                size_bytes=entry.size_bytes,
                representation=entry.representation,
                key_epoch=entry.key_epoch,
                stored_digest=entry.stored_digest,
                source=source_kind,
            )
        except OSError as exc:
            if not _is_enospc(exc):
                raise
            last_enospc = exc
            _flag_disk_over_reserve(session, disk)
            entry.state = "lost"
            session.flush([entry, disk])
            continue

    detail = f"cache disk write ran out of space for {target.sha_hex}: {last_enospc}"
    raise HdcacheFillError(detail)


def entry_policy_conformant(
    session: Session,
    entry: CacheEntry,
    *,
    key_registry: KeyRegistry | None = None,
) -> bool:
    """Return true when a present row satisfies current privacy and key policy."""

    expected = (
        AEAD_REPRESENTATION
        if effective_privacy_level(session, entry.content_sha256) != "none"
        else RAW_REPRESENTATION
    )
    if entry.representation != expected:
        return False
    if expected == RAW_REPRESENTATION:
        return entry.key_epoch is None and entry.stored_digest is None
    if entry.key_epoch is None or entry.stored_digest is None:
        return False
    try:
        assert_key_epoch_domain(entry.key_epoch, KEY_DOMAIN_HDCACHE, context="hdcache entry")
    except ValueError:
        return False
    registry = key_registry or KeyRegistry()
    try:
        return registry.get_epoch(entry.key_epoch).active
    except (KeyError, ValueError, RuntimeError):
        return False


def mark_entry_lost_and_delete(session: Session, entry: CacheEntry) -> None:
    """Delete a nonconforming cache file and mark the row lost."""

    disk = session.get(CacheDisk, entry.disk_id)
    deleted = False
    if disk is not None:
        with contextlib.suppress(OSError, StoreError):
            deleted = delete_entry(
                Path(disk.mount),
                entry.content_sha256,
                representation=entry.representation,
                key_epoch=entry.key_epoch,
            )
    if deleted and disk is not None:
        disk.filled_bytes = max(0, disk.filled_bytes - max(0, entry.size_bytes))
    entry.state = "lost"
    session.flush([obj for obj in (entry, disk) if obj is not None])


def effective_privacy_level(session: Session, content_sha256: bytes) -> str:
    """Return the strictest hdcache privacy level across containing classes."""

    classes = set(
        session.scalars(
            select(Bundle.artifactclass)
            .join(BundleMember, BundleMember.bundle_id == Bundle.id)
            .where(BundleMember.logical_asset_hash == content_sha256)
        )
    )
    classes.update(
        session.scalars(
            select(IngestItem.artifactclass).where(IngestItem.logical_asset_hash == content_sha256)
        )
    )
    if not classes:
        return "none"
    levels: list[str] = []
    for record in session.scalars(
        select(ArtifactClassPolicyRecord).where(
            ArtifactClassPolicyRecord.artifactclass.in_(classes)
        )
    ):
        levels.append(hdcache_policy_from_json(record.hdcache_config).privacy_level)
    return max(levels or ["none"], key=_privacy_rank)


def count_live_hdcache_jobs(session: Session) -> int:
    """Count live hdcache fill jobs under the engine's live-status definition."""

    count = session.scalar(
        select(func.count())
        .select_from(Job)
        .where(Job.kind == JOB_KIND, Job.status.in_(LIVE_JOB_STATUS_VALUES))
    )
    return int(count or 0)


def _desired_targets_for_bundle(session: Session, bundle: Bundle) -> list[HdcacheFillTarget]:
    return _unique_targets(
        target
        for member in bundle.members
        if (target := desired_target_for_asset(session, member.logical_asset_hash)) is not None
        and target.bundle_key == bundle.id
    )


def _prepare_filling_entry(
    session: Session,
    target: HdcacheFillTarget,
    *,
    stored_size_hint: int,
    representation: str,
    key_epoch: str | None,
) -> CacheEntry:
    existing = session.get(CacheEntry, target.content_sha256)
    disk_id = _usable_existing_disk(session, existing)
    if disk_id is None:
        try:
            disk_id = choose_placement(
                session,
                content_sha256=target.content_sha256,
                size_bytes=stored_size_hint,
                artifactclass=target.artifactclass,
                bundle_key=target.bundle_key,
                group_key=target.group_key,
            )
        except PlacementError as exc:
            raise HdcacheFillBlocked("no-placement", str(exc)) from exc
    relpath = _entry_relpath(target.content_sha256, representation, key_epoch)
    if existing is None:
        existing = CacheEntry(
            content_sha256=target.content_sha256,
            artifactclass=target.artifactclass,
            bundle_key=target.bundle_key,
            group_key=target.group_key,
            disk_id=disk_id,
            relpath=relpath,
            size_bytes=stored_size_hint,
            state="filling",
            representation=representation,
            key_epoch=key_epoch,
            trusted=True,
        )
        session.add(existing)
    else:
        existing.artifactclass = target.artifactclass
        existing.bundle_key = target.bundle_key
        existing.group_key = target.group_key
        existing.disk_id = disk_id
        existing.relpath = relpath
        existing.size_bytes = stored_size_hint
        existing.state = "filling"
        existing.representation = representation
        existing.key_epoch = key_epoch
        existing.stored_digest = None
        existing.trusted = True
    session.flush([existing])
    return existing


def _usable_existing_disk(session: Session, entry: CacheEntry | None) -> str | None:
    if entry is None or entry.state != "filling":
        return None
    disk = session.get(CacheDisk, entry.disk_id)
    if disk is None or disk.state != "active":
        return None
    return entry.disk_id


@dataclass(frozen=True)
class _PlaintextSource:
    path: Path
    kind: str


@contextlib.contextmanager
def _plaintext_source(
    session: Session,
    target: HdcacheFillTarget,
    *,
    config: HdcacheFillConfig,
    restore_backends: dict[int, StorageBackend] | None,
    restore_backend_resolver: RestoreBackendResolver,
) -> Iterator[_PlaintextSource]:
    landing = Path(target.source_path) if target.source_path else None
    if landing is not None:
        copied = _copy_landing_to_verified_temp(landing, target, config=config)
        if copied is not None:
            with copied:
                yield _PlaintextSource(copied.path, "landing")
                return

    config.scratch_root.mkdir(parents=True, exist_ok=True)
    os.chmod(config.scratch_root, 0o700)
    with tempfile.TemporaryDirectory(prefix="hdcache-restore-", dir=config.scratch_root) as raw:
        restored = Path(raw) / "asset"
        backends = restore_backends or restore_backend_resolver(session, target.content_sha256)
        if not backends:
            raise HdcacheFillBlocked(
                "no-restore-backend",
                f"no restore backend is available for {target.sha_hex}",
            )
        try:
            restore_asset(
                session,
                asset_hash=target.content_sha256,
                artifactclass=target.artifactclass,
                destination=restored,
                backends=backends,
            )
        except ArchiveRestoreError as exc:
            raise HdcacheFillBlocked("restore-unavailable", str(exc)) from exc
        if _sha256_file(restored) != target.content_sha256:
            raise HdcacheFillBlocked(
                "restore-integrity",
                f"restore fallback produced wrong digest for {target.sha_hex}",
            )
        yield _PlaintextSource(restored, "restore")


def _write_source_to_disk(
    target: HdcacheFillTarget,
    disk: CacheDisk,
    source_path: Path,
    *,
    representation: str,
    key_epoch: KeyEpoch | None,
    config: HdcacheFillConfig,
    registry: KeyRegistry,
    sealer: RaoCliSealer | None,
) -> Any:
    if representation == RAW_REPRESENTATION:
        with source_path.open("rb") as handle:
            return write_entry(
                Path(disk.mount),
                target.content_sha256,
                handle,
                representation=RAW_REPRESENTATION,
            )
    if representation != AEAD_REPRESENTATION:
        raise HdcacheFillError(f"unsupported hdcache representation {representation!r}")
    if key_epoch is None:
        raise HdcacheFillError("AEAD hdcache fill requires a key epoch")
    assert_key_epoch_domain(key_epoch, KEY_DOMAIN_HDCACHE, context="hdcache fill")
    final_sealer = sealer or RaoCliSealer(registry, work_dir=config.scratch_root)
    with final_sealer.seal(
        source_path,
        Representation.RAO_AEAD_V1,
        key_epoch=key_epoch,
        work_dir=config.scratch_root,
    ) as sealed:
        if sealed.plaintext_digest != target.content_sha256:
            raise HdcacheFillBlocked(
                "source-integrity",
                f"source bytes for {target.sha_hex} changed during seal",
            )
        with sealed.sealed_path.open("rb") as handle:
            return write_entry(
                Path(disk.mount),
                target.content_sha256,
                handle,
                representation=AEAD_REPRESENTATION,
                key_epoch=key_epoch.key_id,
                expected_stream_sha256=sealed.stored_digest,
            )


@dataclass
class _CopiedLanding:
    path: Path
    _tempdir: tempfile.TemporaryDirectory[str]

    def __enter__(self) -> _CopiedLanding:
        return self

    def __exit__(self, *_exc: object) -> None:
        self._tempdir.cleanup()


def _copy_landing_to_verified_temp(
    landing: Path,
    target: HdcacheFillTarget,
    *,
    config: HdcacheFillConfig,
) -> _CopiedLanding | None:
    config.scratch_root.mkdir(parents=True, exist_ok=True)
    os.chmod(config.scratch_root, 0o700)
    tmp = tempfile.TemporaryDirectory(prefix="hdcache-landing-", dir=config.scratch_root)
    copied = Path(tmp.name) / "asset"
    digest = hashlib.sha256()
    try:
        if not landing.is_file():
            tmp.cleanup()
            return None
        with landing.open("rb") as raw_in, copied.open("wb") as raw_out:
            for chunk in iter(lambda: raw_in.read(1024 * 1024), b""):
                digest.update(chunk)
                raw_out.write(chunk)
            raw_out.flush()
            os.fsync(raw_out.fileno())
        if digest.digest() != target.content_sha256:
            tmp.cleanup()
            return None
        return _CopiedLanding(copied, tmp)
    except OSError:
        tmp.cleanup()
        return None


def _flag_disk_over_reserve(session: Session, disk: CacheDisk) -> None:
    disk.smart_status = "over-reserve"
    disk.filled_bytes = max(disk.filled_bytes, disk.capacity_bytes)
    session.flush([disk])


def _target_payload(target: HdcacheFillTarget) -> dict[str, Any]:
    return {
        "content_sha256": target.sha_hex,
        "artifactclass": target.artifactclass,
        "bundle_key": target.bundle_key,
        "group_key": target.group_key,
        "source_path": target.source_path,
    }


def _target_is_held(session: Session, target_key: str) -> bool:
    row = session.scalars(
        select(ReconciliationCondition).where(
            ReconciliationCondition.domain == DOMAIN,
            ReconciliationCondition.target_key == target_key,
        )
    ).one_or_none()
    if row is None:
        return False
    if row.condition == CONDITION_BLOCKED:
        return True
    if row.condition != CONDITION_BACKOFF:
        return False
    if row.next_eligible_at is None:
        return True
    return _as_utc(row.next_eligible_at) > dt.datetime.now(dt.UTC)


def _unique_targets(targets: Iterator[HdcacheFillTarget]) -> list[HdcacheFillTarget]:
    result: dict[bytes, HdcacheFillTarget] = {}
    for target in targets:
        result.setdefault(target.content_sha256, target)
    return list(result.values())


def _fallback_group_key(bundle: Bundle) -> str:
    stamp = bundle.sealed_at or bundle.flushed_at or bundle.opened_at
    return f"{bundle.artifactclass}:{stamp.date().isoformat()}"


def _entry_relpath(content_sha256: bytes, representation: str, key_epoch: str | None) -> str:
    sha = content_sha256.hex()
    if representation == RAW_REPRESENTATION:
        filename = sha
    elif representation == AEAD_REPRESENTATION and key_epoch:
        filename = f"{sha}.{AEAD_REPRESENTATION}.{key_epoch}"
    else:
        raise StoreError("AEAD relpath requires key_epoch")
    return f"{sha[:2]}/{filename}"


def _sha_bytes(value: bytes | str) -> bytes:
    if isinstance(value, bytes):
        if not is_content_hash(value):
            raise ValueError("content_sha256 must be 32 bytes")
        return value
    if not isinstance(value, str) or not _looks_sha_hex(value):
        raise ValueError(f"content_sha256 must be lowercase sha256 hex, got {value!r}")
    return bytes.fromhex(value)


def _looks_sha_hex(value: str) -> bool:
    if len(value) != 64 or value.lower() != value:
        return False
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True


def _optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _privacy_rank(level: str) -> int:
    if level == "none":
        return 0
    if level.startswith("p") and level[1:].isdigit():
        return int(level[1:])
    return 0


def _sha256_file(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()


def _is_enospc(exc: OSError) -> bool:
    return exc.errno == errno.ENOSPC


def _as_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)
