"""Walker and rebuild routines for the hdcache disk tier.

M5 keeps these operations cache-local: the walker reconciles expendable disk
state against ``cache_entry`` rows, while rebuild lets disk filenames propose
untrusted rows only after catalog cross-checks. Neither path writes archival
copy truth.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import os
import re
import tempfile
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import LogicalAsset
from sutradhara.hdcache.fill import (
    JOB_KIND,
    dedupe_key,
    desired_target_for_asset,
    effective_privacy_level,
    mark_entry_lost_and_delete,
    top_up_lost_entries,
)
from sutradhara.hdcache.models import CacheDisk, CacheEntry
from sutradhara.hdcache.store import (
    AEAD_REPRESENTATION,
    RAW_REPRESENTATION,
    DiskIdentityProbe,
    EnumeratedEntry,
    ExpectedDiskIdentity,
    StoreError,
    StoreReadTimeout,
    entries_root,
    entry_path,
    enumerate_entries,
    enumerate_rejected_entry_files,
    probe_disk_liveness_with_deadline,
    run_disk_io_with_deadline,
    tmp_root,
    verify_disk_identity_with_deadline,
)
from sutradhara.jobs.models import Job, LIVE_JOB_STATUS_VALUES
from sutradhara.keys import KEY_DOMAIN_HDCACHE, KeyEpoch, KeyRegistry, assert_key_epoch_domain
from sutradhara.restore import sha256_file
from sutradhara.sealing.port import Opener, Representation
from sutradhara.sealing.rao import RaoCliOpener

DEFAULT_UNKNOWN_FILE_TRIPWIRE = 100
DEFAULT_TMP_GC_AGE_SECONDS = 6 * 60 * 60
DEFAULT_FILLING_YOUNG_SECONDS = 6 * 60 * 60
DEFAULT_WALKER_DISK_IO_DEADLINE_SECONDS = 70.0
DEFAULT_WALKER_IDENTITY_DEADLINE_SECONDS = 2.0
SHA_HEX_RE = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class HdcacheWalkerEvent:
    """Reason-coded event emitted by walker and rebuild operations."""

    code: str
    severity: str
    disk_id: str | None = None
    content_sha256: str | None = None
    detail: str | None = None


class DiskMetadataRefresher(Protocol):
    """Optional provisioner/refresher hook used after a successful disk walk."""

    def refresh_disk_metadata(self, disk: CacheDisk) -> MappingLike | None:
        """Return fresh enclosure/slot/SMART metadata for ``disk``."""


class MappingLike(Protocol):
    """Tiny structural protocol for dataclass or mapping refresh payloads."""

    def __getitem__(self, key: str) -> Any: ...


EventSink = Callable[[HdcacheWalkerEvent], None]


@dataclass(frozen=True)
class HdcacheWalkerConfig:
    """Runtime knobs for the M5 walker and rebuild pass."""

    hmac_secret: bytes
    identity_probe: DiskIdentityProbe | None = None
    event_sink: EventSink | None = None
    unknown_file_tripwire: int = DEFAULT_UNKNOWN_FILE_TRIPWIRE
    tmp_gc_age_seconds: float = DEFAULT_TMP_GC_AGE_SECONDS
    filling_young_seconds: float = DEFAULT_FILLING_YOUNG_SECONDS
    verify_untrusted: bool = False
    enqueue_repopulation: bool = True
    sampled_rehash_count: int = 0
    key_registry: KeyRegistry | None = None
    opener: Opener | None = None
    scratch_root: Path = Path("/var/lib/replica/hdcache-walker-scratch")
    metadata_refresher: Any | None = None
    disk_io_deadline_seconds: float = DEFAULT_WALKER_DISK_IO_DEADLINE_SECONDS
    identity_probe_deadline_seconds: float = DEFAULT_WALKER_IDENTITY_DEADLINE_SECONDS

    def __post_init__(self) -> None:
        if not self.hmac_secret:
            raise ValueError("hmac_secret is required")
        if self.unknown_file_tripwire < 0:
            raise ValueError("unknown_file_tripwire must be non-negative")
        if self.tmp_gc_age_seconds < 0:
            raise ValueError("tmp_gc_age_seconds must be non-negative")
        if self.filling_young_seconds < 0:
            raise ValueError("filling_young_seconds must be non-negative")
        if self.sampled_rehash_count < 0:
            raise ValueError("sampled_rehash_count must be non-negative")
        if self.disk_io_deadline_seconds <= 0:
            raise ValueError("disk_io_deadline_seconds must be positive")
        if self.identity_probe_deadline_seconds <= 0:
            raise ValueError("identity_probe_deadline_seconds must be positive")
        object.__setattr__(self, "scratch_root", Path(self.scratch_root))

    def registry(self) -> KeyRegistry:
        return self.key_registry or KeyRegistry()


@dataclass(frozen=True)
class HdcacheWalkResult:
    """Summary for one disk walker pass."""

    disk_id: str
    destructive: bool
    halted: bool = False
    unknown_files: int = 0
    unknown_deleted: int = 0
    tmp_deleted: int = 0
    entries_lost: int = 0
    entries_present: int = 0
    entries_promoted: int = 0
    filled_bytes: int = 0


@dataclass(frozen=True)
class RebuildFailure:
    """One disk file withheld during rebuild with the disk file left untouched."""

    disk_id: str
    relpath: str
    reason: str
    content_sha256: str | None = None


@dataclass(frozen=True)
class HdcacheRebuildDiskResult:
    """Progress record for one disk rebuild pass."""

    disk_id: str
    index: int
    total: int
    entries: int
    elapsed_seconds: float
    rejected: bool = False
    detail: str | None = None
    failures: list[RebuildFailure] = field(default_factory=list)


@dataclass(frozen=True)
class HdcacheRebuildResult:
    """Summary for a complete sequential rebuild."""

    disks: list[HdcacheRebuildDiskResult]

    @property
    def entries(self) -> int:
        return sum(disk.entries for disk in self.disks)

    @property
    def failures(self) -> list[RebuildFailure]:
        return [failure for disk in self.disks for failure in disk.failures]


def walk_disk(
    session: Session,
    disk: CacheDisk,
    *,
    config: HdcacheWalkerConfig,
    destructive: bool = True,
) -> HdcacheWalkResult:
    """Walk one cache disk according to the design §8.2 matrix."""

    if disk.state == "absent":
        if not _probe_absent_disk_recovery(session, disk, config=config):
            _emit(config, "walker-disk-absent", "info", disk.disk_id, detail="disk state is absent")
            return HdcacheWalkResult(disk_id=disk.disk_id, destructive=False)

    try:
        identity = _verify_identity(disk, config)
    except (DiskWalkAborted, StoreReadTimeout) as exc:
        if isinstance(exc, StoreReadTimeout):
            _mark_disk_absent(session, disk, config=config, detail=str(exc))
        return HdcacheWalkResult(disk_id=disk.disk_id, destructive=False, halted=True)
    destructive = destructive and identity.ok
    if not identity.ok:
        _emit(
            config,
            "disk-identity-mismatch",
            "alarm",
            disk.disk_id,
            detail=f"{identity.status}: {identity.detail}",
        )
        if identity.status == "not_mounted":
            _mark_disk_absent(
                session,
                disk,
                config=config,
                detail=f"{identity.status}: {identity.detail}",
            )
        return HdcacheWalkResult(disk_id=disk.disk_id, destructive=False)

    mount = Path(disk.mount)
    root = entries_root(mount)
    db_entries = list(
        session.scalars(
            select(CacheEntry)
            .where(CacheEntry.disk_id == disk.disk_id)
            .order_by(CacheEntry.content_sha256)
        )
    )
    known_paths = {_entry_final_path(mount, entry) for entry in db_entries}
    try:
        unknown = _disk_io(
            disk,
            config,
            lambda: _unknown_files(root, known_paths),
            "walker unknown-file enumeration deadline exceeded",
        )
    except (DiskWalkAborted, StoreReadTimeout) as exc:
        if isinstance(exc, StoreReadTimeout):
            _mark_disk_absent(session, disk, config=config, detail=str(exc))
        return HdcacheWalkResult(disk_id=disk.disk_id, destructive=False, halted=True)
    if len(unknown) > config.unknown_file_tripwire:
        _emit(
            config,
            "walker-tripwire-halt",
            "alarm",
            disk.disk_id,
            detail=(
                f"{len(unknown)} unknown file(s) over threshold "
                f"{config.unknown_file_tripwire}; run sutra hdcache rebuild?"
            ),
        )
        return HdcacheWalkResult(
            disk_id=disk.disk_id,
            destructive=destructive,
            halted=True,
            unknown_files=len(unknown),
        )

    try:
        unknown_deleted = _delete_unknown_files(session, unknown, config, disk) if destructive else 0
        tmp_deleted = _gc_tmp_files(session, disk, mount, config=config) if destructive else 0
    except (DiskWalkAborted, StoreReadTimeout) as exc:
        if isinstance(exc, StoreReadTimeout):
            _mark_disk_absent(session, disk, config=config, detail=str(exc))
        return HdcacheWalkResult(disk_id=disk.disk_id, destructive=False, halted=True)

    lost = 0
    present = 0
    promoted = 0
    try:
        for entry in db_entries:
            changed = _walk_entry(session, disk, entry, config=config, destructive=destructive)
            lost += changed == "lost"
            present += changed == "present"
            promoted += changed == "promoted"
    except (DiskWalkAborted, StoreReadTimeout) as exc:
        if isinstance(exc, StoreReadTimeout):
            _mark_disk_absent(session, disk, config=config, detail=str(exc))
        return HdcacheWalkResult(
            disk_id=disk.disk_id,
            destructive=False,
            halted=True,
            unknown_files=len(unknown),
            unknown_deleted=unknown_deleted,
            tmp_deleted=tmp_deleted,
            entries_lost=lost,
            entries_present=present,
            entries_promoted=promoted,
        )

    if config.sampled_rehash_count:
        try:
            for entry in db_entries[: config.sampled_rehash_count]:
                if entry.state == "present":
                    should_promote = not entry.trusted
                    verified = _verify_entry(
                        session,
                        disk,
                        entry,
                        config=config,
                        promote=should_promote,
                        destructive=destructive,
                    )
                    if destructive and should_promote and verified:
                        promoted += 1
        except (DiskWalkAborted, StoreReadTimeout) as exc:
            if isinstance(exc, StoreReadTimeout):
                _mark_disk_absent(session, disk, config=config, detail=str(exc))
            return HdcacheWalkResult(
                disk_id=disk.disk_id,
                destructive=False,
                halted=True,
                unknown_files=len(unknown),
                unknown_deleted=unknown_deleted,
                tmp_deleted=tmp_deleted,
                entries_lost=lost,
                entries_present=present,
                entries_promoted=promoted,
            )

    if config.enqueue_repopulation and lost:
        top_up_lost_entries(session)

    _correct_filled_bytes(session, disk)
    _refresh_disk_metadata(disk, config)
    disk.last_walk_at = _utcnow()
    session.flush([disk, *db_entries])
    return HdcacheWalkResult(
        disk_id=disk.disk_id,
        destructive=destructive,
        unknown_files=len(unknown),
        unknown_deleted=unknown_deleted,
        tmp_deleted=tmp_deleted,
        entries_lost=lost,
        entries_present=present,
        entries_promoted=promoted,
        filled_bytes=disk.filled_bytes,
    )


def walk_all_disks(
    session: Session,
    *,
    config: HdcacheWalkerConfig,
    destructive: bool = True,
) -> list[HdcacheWalkResult]:
    """Walk enrolled disks sequentially, one spindle at a time."""

    results: list[HdcacheWalkResult] = []
    for disk in session.scalars(select(CacheDisk).order_by(CacheDisk.disk_id)):
        results.append(walk_disk(session, disk, config=config, destructive=destructive))
    return results


def rebuild_hdcache(
    session: Session,
    *,
    config: HdcacheWalkerConfig,
) -> HdcacheRebuildResult:
    """Rebuild cache_entry rows from disk filenames after catalog cross-checks."""

    disks = list(
        session.scalars(
            select(CacheDisk).where(CacheDisk.state != "dead").order_by(CacheDisk.disk_id)
        )
    )
    total = len(disks)
    results: list[HdcacheRebuildDiskResult] = []
    for index, disk in enumerate(disks, start=1):
        started = time.monotonic()
        try:
            identity = _verify_identity(disk, config)
        except StoreReadTimeout as exc:
            detail = str(exc)
            _mark_disk_absent(session, disk, config=config, detail=detail)
            results.append(
                HdcacheRebuildDiskResult(
                    disk_id=disk.disk_id,
                    index=index,
                    total=total,
                    entries=0,
                    elapsed_seconds=time.monotonic() - started,
                    rejected=True,
                    detail=detail,
                )
            )
            continue
        if not identity.ok:
            detail = f"{identity.status}: {identity.detail}"
            _emit(config, "disk-identity-mismatch", "alarm", disk.disk_id, detail=detail)
            if identity.status == "not_mounted":
                _mark_disk_absent(session, disk, config=config, detail=detail)
            results.append(
                HdcacheRebuildDiskResult(
                    disk_id=disk.disk_id,
                    index=index,
                    total=total,
                    entries=0,
                    elapsed_seconds=time.monotonic() - started,
                    rejected=True,
                    detail=detail,
                )
            )
            continue

        mount = Path(disk.mount)
        try:
            entries = _disk_io(
                disk,
                config,
                lambda: enumerate_entries(mount),
                "rebuild entry enumeration deadline exceeded",
            )
            rejected_entries = _disk_io(
                disk,
                config,
                lambda: enumerate_rejected_entry_files(mount),
                "rebuild rejected-entry enumeration deadline exceeded",
            )
        except StoreReadTimeout as exc:
            detail = str(exc)
            _mark_disk_absent(session, disk, config=config, detail=detail)
            results.append(
                HdcacheRebuildDiskResult(
                    disk_id=disk.disk_id,
                    index=index,
                    total=total,
                    entries=0,
                    elapsed_seconds=time.monotonic() - started,
                    rejected=True,
                    detail=detail,
                )
            )
            continue
        inserted = 0
        failures = [
            RebuildFailure(
                disk_id=disk.disk_id,
                relpath=rejected.relpath,
                reason=rejected.reason,
                content_sha256=(
                    None if rejected.content_sha256 is None else rejected.content_sha256.hex()
                ),
            )
            for rejected in rejected_entries
        ]
        for observed in entries:
            try:
                if _rebuild_observed_entry(session, disk, observed, config=config):
                    inserted += 1
            except RebuildFailureError as exc:
                failures.append(exc.failure)
            except StoreReadTimeout as exc:
                detail = str(exc)
                _mark_disk_absent(session, disk, config=config, detail=detail)
                results.append(
                    HdcacheRebuildDiskResult(
                        disk_id=disk.disk_id,
                        index=index,
                        total=total,
                        entries=inserted,
                        elapsed_seconds=time.monotonic() - started,
                        rejected=True,
                        detail=detail,
                        failures=failures,
                    )
                )
                break
        else:
            _correct_filled_bytes(session, disk)
            results.append(
                HdcacheRebuildDiskResult(
                    disk_id=disk.disk_id,
                    index=index,
                    total=total,
                    entries=inserted,
                    elapsed_seconds=time.monotonic() - started,
                    failures=failures,
                )
            )
            continue
        continue
    return HdcacheRebuildResult(results)


class RebuildFailureError(RuntimeError):
    def __init__(self, failure: RebuildFailure) -> None:
        self.failure = failure
        super().__init__(failure.reason)


class DiskWalkAborted(RuntimeError):
    """Raised when disk liveness changes during a destructive walk."""


def _walk_entry(
    session: Session,
    disk: CacheDisk,
    entry: CacheEntry,
    *,
    config: HdcacheWalkerConfig,
    destructive: bool,
) -> str | None:
    final_path = _entry_final_path(Path(disk.mount), entry)
    exists, size_bytes = _entry_file_status(disk, final_path, config=config)
    if entry.state == "filling":
        if exists:
            if size_bytes == entry.size_bytes:
                if destructive:
                    entry.state = "present"
                    entry.trusted = True
                    session.flush([entry])
                return "present"
            if destructive:
                _mark_entry_lost_after_identity_proof(session, disk, entry, config=config)
                return "lost"
            return None
        if _filling_is_live_or_young(session, entry, disk, config=config):
            return None
        if destructive:
            _mark_entry_lost_after_identity_proof(session, disk, entry, config=config)
            return "lost"
        return None

    if entry.state != "present":
        return None
    if not exists:
        if destructive:
            _mark_entry_lost_after_identity_proof(session, disk, entry, config=config)
            return "lost"
        return None
    if size_bytes != entry.size_bytes:
        if destructive:
            _mark_entry_lost_after_identity_proof(session, disk, entry, config=config)
            return "lost"
        return None
    if config.verify_untrusted and not entry.trusted:
        verified = _verify_entry(
            session,
            disk,
            entry,
            config=config,
            promote=True,
            destructive=destructive,
        )
        if not destructive:
            return None
        return "promoted" if verified else "lost"
    return None


def _verify_entry(
    session: Session,
    disk: CacheDisk,
    entry: CacheEntry,
    *,
    config: HdcacheWalkerConfig,
    promote: bool,
    destructive: bool,
) -> bool:
    try:
        if entry.representation == RAW_REPRESENTATION:
            from sutradhara.hdcache.store import read_entry_verified

            read_entry_verified(
                Path(disk.mount),
                entry.content_sha256,
                deadline_monotonic=_deadline(config.disk_io_deadline_seconds),
                disk_id=disk.disk_id,
            )
        elif entry.representation == AEAD_REPRESENTATION:
            _verify_aead_entry(disk, entry, config=config)
        else:
            raise StoreError(f"unsupported cache representation {entry.representation!r}")
    except Exception as exc:
        _emit(
            config,
            "walker-untrusted-verify-failed",
            "warning",
            disk.disk_id,
            content_sha256=entry.content_sha256.hex(),
            detail=str(exc),
        )
        if destructive:
            _mark_entry_lost_after_identity_proof(session, disk, entry, config=config)
        return False
    if promote and destructive:
        entry.trusted = True
        session.flush([entry])
    return True


def _verify_aead_entry(disk: CacheDisk, entry: CacheEntry, *, config: HdcacheWalkerConfig) -> None:
    from sutradhara.hdcache.store import read_entry_verified

    if entry.key_epoch is None or entry.stored_digest is None:
        raise StoreError("AEAD cache entry lacks key epoch or stored digest")
    assert_key_epoch_domain(entry.key_epoch, KEY_DOMAIN_HDCACHE, context="hdcache walker")
    config.scratch_root.mkdir(parents=True, exist_ok=True)
    os.chmod(config.scratch_root, 0o700)
    with tempfile.TemporaryDirectory(prefix="hdcache-walk-", dir=config.scratch_root) as raw:
        sealed = Path(raw) / "sealed"
        with sealed.open("wb") as output:
            read_entry_verified(
                Path(disk.mount),
                entry.content_sha256,
                representation=AEAD_REPRESENTATION,
                key_epoch=entry.key_epoch,
                expected_stream_sha256=entry.stored_digest,
                output=output,
                deadline_monotonic=_deadline(config.disk_io_deadline_seconds),
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
                raise StoreError("opened cache plaintext digest mismatch")


def _rebuild_observed_entry(
    session: Session,
    disk: CacheDisk,
    observed: EnumeratedEntry,
    *,
    config: HdcacheWalkerConfig,
) -> bool:
    target = desired_target_for_asset(session, observed.content_sha256)
    asset = session.get(LogicalAsset, observed.content_sha256)
    if target is None or asset is None:
        raise _rebuild_failure(disk, observed, "not-cacheable-or-unknown")
    expected_representation = (
        AEAD_REPRESENTATION
        if effective_privacy_level(session, observed.content_sha256) != "none"
        else RAW_REPRESENTATION
    )
    if observed.representation != expected_representation:
        raise _rebuild_failure(disk, observed, "representation-mismatch")
    if observed.representation == RAW_REPRESENTATION and observed.size_bytes != target.size_bytes:
        raise _rebuild_failure(disk, observed, "size-mismatch")

    existing = session.get(CacheEntry, observed.content_sha256)
    if existing is not None and existing.state == "present" and existing.disk_id != disk.disk_id:
        raise _rebuild_failure(disk, observed, "duplicate-present-entry")
    stored_digest = (
        _disk_io(
            disk,
            config,
            lambda: _file_sha256(observed.path),
            "rebuild AEAD digest deadline exceeded",
        )
        if observed.representation == AEAD_REPRESENTATION
        else None
    )
    if existing is None:
        existing = CacheEntry(content_sha256=observed.content_sha256)
        session.add(existing)
    existing.artifactclass = target.artifactclass
    existing.bundle_key = target.bundle_key
    existing.group_key = target.group_key
    existing.disk_id = disk.disk_id
    existing.relpath = observed.relpath
    existing.size_bytes = observed.size_bytes
    existing.state = "present"
    existing.representation = observed.representation
    existing.key_epoch = observed.key_epoch
    existing.stored_digest = stored_digest
    existing.trusted = False
    session.flush([existing])
    return True


def _rebuild_failure(disk: CacheDisk, observed: EnumeratedEntry, reason: str) -> RebuildFailureError:
    return RebuildFailureError(
        RebuildFailure(
            disk_id=disk.disk_id,
            relpath=observed.relpath,
            reason=reason,
            content_sha256=observed.content_sha256.hex(),
        )
    )


def _verify_identity(disk: CacheDisk, config: HdcacheWalkerConfig):
    return verify_disk_identity_with_deadline(
        Path(disk.mount),
        ExpectedDiskIdentity(
            disk_id=disk.disk_id,
            serial=disk.serial,
            fs_uuid=disk.fs_uuid,
            wwn=disk.wwn,
        ),
        hmac_secret=config.hmac_secret,
        disk_id=disk.disk_id,
        probe=config.identity_probe,
        deadline_monotonic=_deadline(config.identity_probe_deadline_seconds),
    )


def _mark_entry_lost_after_identity_proof(
    session: Session,
    disk: CacheDisk,
    entry: CacheEntry,
    *,
    config: HdcacheWalkerConfig,
) -> None:
    try:
        identity = _verify_identity(disk, config)
    except StoreReadTimeout as exc:
        _mark_disk_absent(session, disk, config=config, detail=str(exc))
        raise DiskWalkAborted(str(exc)) from exc
    if not identity.ok:
        detail = f"{identity.status}: {identity.detail}"
        if identity.status == "not_mounted":
            _mark_disk_absent(session, disk, config=config, detail=detail)
        _emit(
            config,
            "walker-lost-mark-aborted",
            "alarm",
            disk.disk_id,
            content_sha256=entry.content_sha256.hex(),
            detail=f"{detail} before lost mark",
        )
        raise DiskWalkAborted(detail)
    mark_entry_lost_and_delete(session, entry)


def _entry_file_status(
    disk: CacheDisk,
    path: Path,
    *,
    config: HdcacheWalkerConfig,
) -> tuple[bool, int | None]:
    def operation() -> tuple[bool, int | None]:
        exists = path.is_file()
        return exists, path.stat().st_size if exists else None

    return _disk_io(
        disk,
        config,
        operation,
        "walker entry stat deadline exceeded",
    )


def _disk_io(
    disk: CacheDisk,
    config: HdcacheWalkerConfig,
    operation: Callable[[], Any],
    timeout_message: str,
) -> Any:
    return run_disk_io_with_deadline(
        disk.disk_id,
        operation,
        deadline_monotonic=_deadline(config.disk_io_deadline_seconds),
        timeout_message=timeout_message,
    )


def _mark_disk_absent(
    session: Session,
    disk: CacheDisk,
    *,
    config: HdcacheWalkerConfig,
    detail: str,
) -> None:
    disk.state = "absent"
    session.flush([disk])
    _emit(config, "walker-disk-absent", "alarm", disk.disk_id, detail=detail)


def _probe_absent_disk_recovery(
    session: Session,
    disk: CacheDisk,
    *,
    config: HdcacheWalkerConfig,
) -> bool:
    if disk.state != "absent":
        return False
    try:
        result = probe_disk_liveness_with_deadline(
            Path(disk.mount),
            ExpectedDiskIdentity(
                disk_id=disk.disk_id,
                serial=disk.serial,
                fs_uuid=disk.fs_uuid,
                wwn=disk.wwn,
            ),
            hmac_secret=config.hmac_secret,
            disk_id=disk.disk_id,
            probe=config.identity_probe,
            deadline_monotonic=_deadline(config.identity_probe_deadline_seconds),
        )
    except StoreReadTimeout as exc:
        _emit(
            config,
            "walker-disk-recovery-timeout",
            "alarm",
            disk.disk_id,
            detail=str(exc),
        )
        return False
    except Exception as exc:
        _emit(
            config,
            "walker-disk-recovery-failed",
            "alarm",
            disk.disk_id,
            detail=str(exc),
        )
        return False
    if not result.ok:
        _emit(
            config,
            "walker-disk-recovery-failed",
            "info",
            disk.disk_id,
            detail=f"{result.status}: {result.detail}",
        )
        return False
    disk.state = "active"
    session.flush([disk])
    _emit(config, "walker-disk-recovered", "info", disk.disk_id, detail="absent disk recovered")
    return True


def _deadline(seconds: float) -> float:
    return time.monotonic() + seconds


def _unknown_files(root: Path, known_paths: set[Path]) -> list[Path]:
    if not root.exists():
        return []
    tmp = tmp_root(root.parent.parent)
    unknown: list[Path] = []
    for path in sorted(root.rglob("*")):
        if _is_relative_to(path, tmp):
            continue
        if path in known_paths:
            continue
        try:
            is_file = path.is_file() or path.is_symlink()
        except OSError:
            continue
        if is_file:
            unknown.append(path)
    return unknown


def _delete_unknown_files(
    session: Session,
    paths: Iterable[Path],
    config: HdcacheWalkerConfig,
    disk: CacheDisk,
) -> int:
    deleted = 0
    for path in paths:
        try:
            _disk_io(
                disk,
                config,
                path.unlink,
                "walker unknown-file delete deadline exceeded",
            )
            deleted += 1
        except OSError as exc:
            _emit(config, "walker-delete-failed", "alarm", disk.disk_id, detail=str(exc))
        except StoreReadTimeout as exc:
            _mark_disk_absent(session, disk, config=config, detail=str(exc))
            raise DiskWalkAborted(str(exc)) from exc
    return deleted


def _gc_tmp_files(session: Session, disk: CacheDisk, mount: Path, *, config: HdcacheWalkerConfig) -> int:
    tmp = tmp_root(mount)
    if not _disk_io(
        disk,
        config,
        tmp.exists,
        "walker tmp-exists deadline exceeded",
    ):
        return 0
    deleted = 0
    now = time.time()
    paths = _disk_io(
        disk,
        config,
        lambda: sorted(tmp.iterdir()),
        "walker tmp enumeration deadline exceeded",
    )
    for path in paths:
        is_stale_file = _disk_io(
            disk,
            config,
            lambda path=path: path.is_file() or path.is_symlink(),
            "walker tmp file-status deadline exceeded",
        )
        if not is_stale_file:
            continue
        try:
            age = now - _disk_io(
                disk,
                config,
                lambda path=path: path.stat().st_mtime,
                "walker tmp stat deadline exceeded",
            )
        except OSError:
            continue
        if age < config.tmp_gc_age_seconds:
            continue
        if _tmp_job_live(session, path):
            continue
        try:
            _disk_io(disk, config, path.unlink, "walker tmp delete deadline exceeded")
            deleted += 1
        except OSError as exc:
            _emit(config, "walker-delete-failed", "alarm", disk.disk_id, detail=str(exc))
    return deleted


def _tmp_job_live(session: Session, path: Path) -> bool:
    match = SHA_HEX_RE.search(path.name)
    if match is not None:
        return _live_hdcache_job(session, bytes.fromhex(match.group(0)))
    count = session.scalar(
        select(func.count()).select_from(Job).where(
            Job.kind == JOB_KIND,
            Job.status.in_(LIVE_JOB_STATUS_VALUES),
        )
    )
    return int(count or 0) > 0


def _filling_is_live_or_young(
    session: Session,
    entry: CacheEntry,
    disk: CacheDisk,
    *,
    config: HdcacheWalkerConfig,
) -> bool:
    if _live_hdcache_job(session, entry.content_sha256):
        return True
    placed_at = _as_utc(entry.placed_at)
    if (_utcnow() - placed_at).total_seconds() < config.filling_young_seconds:
        return True
    tmp = tmp_root(Path(disk.mount))
    if _disk_io(disk, config, tmp.exists, "walker filling tmp-exists deadline exceeded"):
        now = time.time()
        with contextlib.suppress(OSError):
            paths = _disk_io(
                disk,
                config,
                lambda: list(tmp.iterdir()),
                "walker filling tmp enumeration deadline exceeded",
            )
            return any(
                now
                - _disk_io(
                    disk,
                    config,
                    lambda path=path: path.stat().st_mtime,
                    "walker filling tmp stat deadline exceeded",
                )
                < config.tmp_gc_age_seconds
                for path in paths
            )
    return False


def _live_hdcache_job(session: Session, digest: bytes) -> bool:
    count = session.scalar(
        select(func.count()).select_from(Job).where(
            Job.kind == JOB_KIND,
            Job.status.in_(LIVE_JOB_STATUS_VALUES),
            Job.dedupe_key == dedupe_key(digest),
        )
    )
    return int(count or 0) > 0


def _entry_final_path(mount: Path, entry: CacheEntry) -> Path:
    try:
        return entry_path(
            mount,
            entry.content_sha256,
            representation=entry.representation,
            key_epoch=entry.key_epoch,
        )
    except StoreError:
        return entries_root(mount) / entry.relpath


def _correct_filled_bytes(session: Session, disk: CacheDisk) -> None:
    total = session.scalar(
        select(func.coalesce(func.sum(CacheEntry.size_bytes), 0)).where(
            CacheEntry.disk_id == disk.disk_id,
            CacheEntry.state.in_(("filling", "present")),
        )
    )
    disk.filled_bytes = int(total or 0)
    session.flush([disk])


def _refresh_disk_metadata(disk: CacheDisk, config: HdcacheWalkerConfig) -> None:
    refresher = config.metadata_refresher
    if refresher is None or not hasattr(refresher, "refresh_disk_metadata"):
        return
    try:
        payload = refresher.refresh_disk_metadata(disk)
    except Exception as exc:
        _emit(config, "walker-metadata-refresh-failed", "warning", disk.disk_id, detail=str(exc))
        return
    if payload is None:
        return
    for attr in ("enclosure", "slot", "smart_status"):
        value = _payload_get(payload, attr)
        if value is not None:
            setattr(disk, attr, value)


def _payload_get(payload: Any, attr: str) -> Any:
    if isinstance(payload, dict):
        return payload.get(attr)
    return getattr(payload, attr, None)


def _file_sha256(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()


def _emit(
    config: HdcacheWalkerConfig,
    code: str,
    severity: str,
    disk_id: str | None,
    *,
    content_sha256: str | None = None,
    detail: str | None = None,
) -> None:
    if config.event_sink is not None:
        config.event_sink(
            HdcacheWalkerEvent(
                code=code,
                severity=severity,
                disk_id=disk_id,
                content_sha256=content_sha256,
                detail=detail,
            )
        )


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _as_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
