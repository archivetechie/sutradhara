"""Disk lifecycle manager for the hdcache M1 command surface.

This module owns database state transitions for enrolled cache disks while
delegating privileged hardware operations to a ``DiskProvisioner`` port. Tests
use a tmpdir-backed fake; production provisioning can provide the LUKS/mkfs/SES
implementation without changing the catalog-facing lifecycle semantics.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import os
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import Engine, func, select

from sutradhara.catalog.session import make_session_factory
from sutradhara.hdcache.models import CacheDisk, CacheEntry
from sutradhara.hdcache.store import (
    ExpectedDiskIdentity,
    write_disk_sentinel,
)

DEFAULT_MOUNT_ROOT = Path("/srv/hdcache")
LOST_MARK_BATCH_SIZE = 1000


class LifecycleError(RuntimeError):
    """Raised when a disk lifecycle command is refused without partial mutation."""


@dataclass(frozen=True)
class BlockDeviceCandidate:
    """Provisionable block device discovered by the hdcache provisioner."""

    block_dev: str
    serial: str
    wwn: str | None = None
    enclosure: str | None = None
    slot: str | None = None
    capacity_bytes: int = 0


@dataclass(frozen=True)
class ProvisionedDisk:
    """Result of provisioning and mounting one cache disk."""

    block_dev: str
    serial: str
    fs_uuid: str
    mount: Path
    wwn: str | None = None
    enclosure: str | None = None
    slot: str | None = None
    capacity_bytes: int = 0
    smart_status: str | None = None


@dataclass(frozen=True)
class DiskAddResult:
    """Enrollment result emitted by disk add commands."""

    disk_id: str
    serial: str
    fs_uuid: str
    mount: str
    enclosure: str | None
    slot: str | None
    smart_status: str | None


@dataclass(frozen=True)
class DeadDiskResult:
    """Result of marking a disk dead."""

    disk_id: str
    entries_lost: int
    batches: int
    luks_key_drop: str | None


@dataclass(frozen=True)
class HdcacheStatus:
    """Summary of hdcache disk state."""

    disks_total: int
    capacity_bytes: int
    filled_bytes: int
    by_state: dict[str, int]
    worst_disks: list[dict[str, Any]]


class DiskProvisioner(Protocol):
    """Port for privileged disk lifecycle operations."""

    def scan_devices(self) -> Sequence[BlockDeviceCandidate]:
        """Return block devices that could be enrolled."""

    def provision(
        self,
        block_dev: str,
        *,
        disk_id: str,
        mount_root: Path,
    ) -> ProvisionedDisk:
        """Create LUKS2, mkfs, mount, and return observed provisioned identity."""

    def locate(self, disk: CacheDisk) -> str:
        """Blink or identify one physical disk, best effort."""

    def drop_luks_key_slot(self, disk: CacheDisk) -> str | None:
        """Drop the LUKS key-slot association for a dead disk, if available."""


class ShellDiskProvisioner:
    """Best-effort shell-backed provisioner boundary.

    M1 keeps destructive provisioning behind this port. The default implementation
    can scan with ``lsblk`` but refuses destructive provisioning unless replaced
    by deployment-specific code.
    """

    def scan_devices(self) -> Sequence[BlockDeviceCandidate]:
        try:
            output = subprocess.check_output(
                [
                    "lsblk",
                    "--json",
                    "--bytes",
                    "--output",
                    "PATH,SERIAL,WWN,SIZE,TYPE",
                ],
                text=True,
                stderr=subprocess.DEVNULL,
            )
            payload = json.loads(output)
        except Exception:
            return ()
        devices: list[BlockDeviceCandidate] = []
        for node in payload.get("blockdevices", []):
            if not isinstance(node, dict) or node.get("type") != "disk":
                continue
            path = str(node.get("path") or "")
            serial = str(node.get("serial") or "").strip()
            if not path or not serial:
                continue
            devices.append(
                BlockDeviceCandidate(
                    block_dev=path,
                    serial=serial,
                    wwn=str(node.get("wwn") or "") or None,
                    capacity_bytes=int(node.get("size") or 0),
                )
            )
        return devices

    def provision(
        self,
        block_dev: str,
        *,
        disk_id: str,
        mount_root: Path,
    ) -> ProvisionedDisk:
        raise LifecycleError(
            "hardware provisioning requires a deployment DiskProvisioner "
            f"(refusing destructive setup for {block_dev} as {disk_id} under {mount_root})"
        )

    def locate(self, disk: CacheDisk) -> str:
        return f"locate requested for {disk.disk_id}; no SES locator configured"

    def drop_luks_key_slot(self, disk: CacheDisk) -> str | None:
        return f"no LUKS key-slot dropper configured for {disk.disk_id}"


class HdcacheLifecycleManager:
    """Stateful hdcache disk lifecycle operations."""

    def __init__(
        self,
        engine: Engine,
        *,
        provisioner: DiskProvisioner | None = None,
        mount_root: Path = DEFAULT_MOUNT_ROOT,
        hmac_secret: bytes,
        on_entries_lost: Callable[[str, int], None] | None = None,
    ) -> None:
        self.engine = engine
        self.provisioner = provisioner or ShellDiskProvisioner()
        self.mount_root = mount_root
        self.hmac_secret = hmac_secret
        self.on_entries_lost = on_entries_lost or (lambda _disk_id, _count: None)

    def scan(self) -> list[BlockDeviceCandidate]:
        """Return unenrolled device candidates."""

        enrolled = self._enrolled_serials()
        return [
            candidate
            for candidate in self.provisioner.scan_devices()
            if candidate.serial not in enrolled
        ]

    def add_disk(self, block_dev: str) -> DiskAddResult:
        """Provision one disk and insert its cache_disk row."""

        candidate = self._candidate_for_block_dev(block_dev)
        if candidate is not None and candidate.serial in self._enrolled_serials():
            raise LifecycleError(f"cache disk serial is already enrolled: {candidate.serial}")
        disk_id = self._next_disk_id()
        provisioned = self.provisioner.provision(
            block_dev,
            disk_id=disk_id,
            mount_root=self.mount_root,
        )
        expected = ExpectedDiskIdentity(
            disk_id=disk_id,
            serial=provisioned.serial,
            fs_uuid=provisioned.fs_uuid,
            wwn=provisioned.wwn,
        )
        write_disk_sentinel(provisioned.mount, expected, hmac_secret=self.hmac_secret)
        now = dt.datetime.now(dt.UTC)
        factory = make_session_factory(self.engine)
        with factory.begin() as session:
            if self._serial_exists(session, provisioned.serial):
                raise LifecycleError(f"cache disk serial is already enrolled: {provisioned.serial}")
            row = CacheDisk(
                disk_id=disk_id,
                serial=provisioned.serial,
                wwn=provisioned.wwn,
                fs_uuid=provisioned.fs_uuid,
                enclosure=provisioned.enclosure,
                slot=provisioned.slot,
                mount=str(provisioned.mount),
                state="active",
                capacity_bytes=provisioned.capacity_bytes,
                filled_bytes=0,
                smart_status=provisioned.smart_status,
                enrolled_at=now,
            )
            session.add(row)
        return DiskAddResult(
            disk_id=disk_id,
            serial=provisioned.serial,
            fs_uuid=provisioned.fs_uuid,
            mount=str(provisioned.mount),
            enclosure=provisioned.enclosure,
            slot=provisioned.slot,
            smart_status=provisioned.smart_status,
        )

    def add_scan(self) -> list[DiskAddResult]:
        """Provision every currently unenrolled scan candidate."""

        return [self.add_disk(candidate.block_dev) for candidate in self.scan()]

    def disks(self, *, include_dead: bool = False) -> list[CacheDisk]:
        """Return enrolled disks sorted by disk_id."""

        factory = make_session_factory(self.engine)
        with factory() as session:
            query = select(CacheDisk).order_by(CacheDisk.disk_id)
            if not include_dead:
                query = query.where(CacheDisk.state != "dead")
            rows = list(session.scalars(query))
            for row in rows:
                session.expunge(row)
            return rows

    def locate(self, disk_id: str) -> str:
        """Run the provisioner's best-effort locator for one disk."""

        disk = self._disk_or_error(disk_id)
        return self.provisioner.locate(disk)

    def retire(self, disk_id: str) -> CacheDisk:
        """Mark a disk retiring so placement stops using it."""

        return self._set_disk_state(disk_id, "retiring")

    def mark_dead(
        self,
        disk_id: str,
        *,
        batch_size: int = LOST_MARK_BATCH_SIZE,
    ) -> DeadDiskResult:
        """Mark a disk dead and flip its entries to lost in bounded transactions."""

        disk = self._set_disk_state(disk_id, "dead")
        total = 0
        batches = 0
        while True:
            changed = self._mark_lost_batch(disk_id, batch_size=batch_size)
            if changed == 0:
                break
            total += changed
            batches += 1
            self.on_entries_lost(disk_id, changed)
        luks_result = self.provisioner.drop_luks_key_slot(disk)
        return DeadDiskResult(
            disk_id=disk_id,
            entries_lost=total,
            batches=batches,
            luks_key_drop=luks_result,
        )

    def forget(self, disk_id: str) -> CacheDisk:
        """Validate that a dead disk has no entries and keep its id as a tombstone."""

        disk = self._disk_or_error(disk_id)
        if disk.state != "dead":
            raise LifecycleError("only dead disks may be forgotten")
        factory = make_session_factory(self.engine)
        with factory() as session:
            count = session.scalar(
                select(func.count()).select_from(CacheEntry).where(CacheEntry.disk_id == disk_id)
            )
        if count:
            raise LifecycleError(f"disk {disk_id} still has {count} cache entries")
        return disk

    def status(self, *, worst_limit: int = 5) -> HdcacheStatus:
        """Return summary counts and worst disks."""

        rows = self.disks(include_dead=True)
        by_state: dict[str, int] = {}
        for row in rows:
            by_state[row.state] = by_state.get(row.state, 0) + 1
        worst = sorted(
            rows,
            key=lambda row: (
                row.state != "dead",
                row.state != "absent",
                -(row.filled_bytes / row.capacity_bytes if row.capacity_bytes else 0.0),
                row.disk_id,
            ),
        )[:worst_limit]
        return HdcacheStatus(
            disks_total=len(rows),
            capacity_bytes=sum(row.capacity_bytes for row in rows),
            filled_bytes=sum(row.filled_bytes for row in rows),
            by_state=by_state,
            worst_disks=[_disk_payload(row) for row in worst],
        )

    def _next_disk_id(self) -> str:
        rows = self.disks(include_dead=True)
        max_number = 0
        for row in rows:
            if row.disk_id.startswith("d") and row.disk_id[1:].isdigit():
                max_number = max(max_number, int(row.disk_id[1:]))
        return f"d{max_number + 1:03d}"

    def _enrolled_serials(self) -> set[str]:
        factory = make_session_factory(self.engine)
        with factory() as session:
            return set(session.scalars(select(CacheDisk.serial)))

    def _candidate_for_block_dev(self, block_dev: str) -> BlockDeviceCandidate | None:
        for candidate in self.provisioner.scan_devices():
            if candidate.block_dev == block_dev:
                return candidate
        return None

    def _disk_or_error(self, disk_id: str) -> CacheDisk:
        factory = make_session_factory(self.engine)
        with factory() as session:
            row = session.get(CacheDisk, disk_id)
            if row is None:
                raise LifecycleError(f"unknown cache disk: {disk_id}")
            session.expunge(row)
            return row

    def _set_disk_state(self, disk_id: str, state: str) -> CacheDisk:
        factory = make_session_factory(self.engine)
        with factory.begin() as session:
            row = session.get(CacheDisk, disk_id)
            if row is None:
                raise LifecycleError(f"unknown cache disk: {disk_id}")
            row.state = state
            session.flush()
            session.expunge(row)
            return row

    def _mark_lost_batch(self, disk_id: str, *, batch_size: int) -> int:
        factory = make_session_factory(self.engine)
        with factory.begin() as session:
            rows = list(
                session.scalars(
                    select(CacheEntry)
                    .where(CacheEntry.disk_id == disk_id, CacheEntry.state != "lost")
                    .order_by(CacheEntry.content_sha256)
                    .limit(batch_size)
                )
            )
            for row in rows:
                row.state = "lost"
            return len(rows)

    @staticmethod
    def _serial_exists(session: Any, serial: str) -> bool:
        return (
            session.scalar(select(func.count()).select_from(CacheDisk).where(CacheDisk.serial == serial))
            or 0
        ) > 0


def disk_payload(disk: CacheDisk) -> dict[str, Any]:
    """Return a JSON-friendly disk payload."""

    return _disk_payload(disk)


def add_result_payload(result: DiskAddResult) -> dict[str, Any]:
    """Return a JSON-friendly add result."""

    return dataclasses.asdict(result)


def dead_result_payload(result: DeadDiskResult) -> dict[str, Any]:
    """Return a JSON-friendly dead result."""

    return dataclasses.asdict(result)


def status_payload(status: HdcacheStatus) -> dict[str, Any]:
    """Return a JSON-friendly status payload."""

    return dataclasses.asdict(status)


def _disk_payload(row: CacheDisk) -> dict[str, Any]:
    return {
        "disk_id": row.disk_id,
        "serial": row.serial,
        "wwn": row.wwn,
        "fs_uuid": row.fs_uuid,
        "enclosure": row.enclosure,
        "slot": row.slot,
        "mount": row.mount,
        "state": row.state,
        "capacity_bytes": row.capacity_bytes,
        "filled_bytes": row.filled_bytes,
        "smart_status": row.smart_status,
        "enrolled_at": row.enrolled_at.isoformat() if row.enrolled_at else None,
        "last_walk_at": row.last_walk_at.isoformat() if row.last_walk_at else None,
    }


def load_hmac_secret_from_env() -> bytes:
    """Load the hdcache disk sentinel HMAC secret from env or key file."""

    inline = os.environ.get("SUTRADHARA_HDCACHE_HMAC_SECRET")
    if inline:
        return inline.encode("utf-8")
    from sutradhara.hdcache.store import read_hmac_secret

    return read_hmac_secret()
