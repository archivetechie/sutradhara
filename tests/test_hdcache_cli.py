"""CLI tests for hdcache disk lifecycle commands."""

from __future__ import annotations

import json
import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner
from sqlalchemy import Engine, delete, event, select

from sutradhara.catalog.models import LogicalAsset
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.cli.hdcache import hdcache_group, set_manager_factory
from sutradhara.hdcache.lifecycle import (
    BlockDeviceCandidate,
    HdcacheLifecycleManager,
    ProvisionedDisk,
)
from sutradhara.hdcache.models import CacheDisk, CacheEntry


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    eng = make_engine(f"sqlite:///{tmp_path / 'hdcache-cli.db'}")
    create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_hdcache_disk_add_scan_list_status_and_locate(
    engine: Engine,
    tmp_path: Path,
    runner: CliRunner,
) -> None:
    provisioner = FakeProvisioner(
        tmp_path,
        [
            BlockDeviceCandidate("/dev/sda", "SER001", enclosure="shelf-a", slot="01", capacity_bytes=1000),
            BlockDeviceCandidate("/dev/sdb", "SER002", enclosure="shelf-a", slot="02", capacity_bytes=2000),
        ],
    )
    manager = HdcacheLifecycleManager(
        engine,
        provisioner=provisioner,
        mount_root=tmp_path,
        hmac_secret=b"secret",
    )
    set_manager_factory(lambda: manager)
    try:
        scan = runner.invoke(hdcache_group, ["disk", "add", "--scan", "--json"])
        assert scan.exit_code == 0
        assert [item["serial"] for item in json.loads(scan.output)["candidates"]] == [
            "SER001",
            "SER002",
        ]

        add = runner.invoke(hdcache_group, ["disk", "add", "--scan", "--yes", "--json"])
        assert add.exit_code == 0
        assert [item["disk_id"] for item in json.loads(add.output)] == ["d001", "d002"]
        assert (tmp_path / "d001" / "hdcache-disk.json").is_file()
        assert (tmp_path / "d002" / "hdcache-disk.json").is_file()
        assert provisioner.scan_count == 2

        listing = runner.invoke(hdcache_group, ["disk", "list", "--json"])
        assert listing.exit_code == 0
        listed = json.loads(listing.output)["disks"]
        assert [(row["disk_id"], row["smart_status"]) for row in listed] == [
            ("d001", "ok"),
            ("d002", "ok"),
        ]

        status = runner.invoke(hdcache_group, ["status", "--json"])
        assert status.exit_code == 0
        assert json.loads(status.output)["summary"]["by_state"] == {"active": 2}

        locate = runner.invoke(hdcache_group, ["disk", "locate", "d001"])
        assert locate.exit_code == 0
        assert "locating d001 serial=SER001" in locate.output
    finally:
        set_manager_factory(None)


def test_hdcache_dead_marks_entries_lost_in_batches_and_forget_checks_references(
    engine: Engine,
    tmp_path: Path,
    runner: CliRunner,
) -> None:
    lost_batches: list[tuple[str, int]] = []
    provisioner = FakeProvisioner(
        tmp_path,
        [BlockDeviceCandidate("/dev/sda", "SER001", slot="01", capacity_bytes=1000)],
    )
    manager = HdcacheLifecycleManager(
        engine,
        provisioner=provisioner,
        mount_root=tmp_path,
        hmac_secret=b"secret",
        on_entries_lost=lambda disk_id, count: lost_batches.append((disk_id, count)),
    )
    set_manager_factory(lambda: manager)
    try:
        add = runner.invoke(hdcache_group, ["disk", "add", "/dev/sda", "--json"])
        assert add.exit_code == 0
        _seed_entries(engine, "d001", count=1005)

        retire = runner.invoke(hdcache_group, ["disk", "retire", "d001"])
        assert retire.exit_code == 0
        assert "state=retiring" in retire.output

        dry = runner.invoke(hdcache_group, ["disk", "dead", "d001"])
        assert dry.exit_code == 0
        assert "pass --yes" in dry.output

        dead = runner.invoke(hdcache_group, ["disk", "dead", "d001", "--yes", "--json"])
        assert dead.exit_code == 0
        dead_payload = json.loads(dead.output)
        assert dead_payload["entries_lost"] == 1005
        assert dead_payload["batches"] == 2
        assert lost_batches == [("d001", 1000), ("d001", 5)]
        assert provisioner.dropped_slots == ["d001"]

        with session_scope(engine) as session:
            assert {
                row.state for row in session.scalars(select(CacheEntry).where(CacheEntry.disk_id == "d001"))
            } == {"lost"}

        hidden = runner.invoke(hdcache_group, ["disk", "list", "--json"])
        assert hidden.exit_code == 0
        assert json.loads(hidden.output)["disks"] == []
        all_rows = runner.invoke(hdcache_group, ["disk", "list", "--all", "--json"])
        assert json.loads(all_rows.output)["disks"][0]["state"] == "dead"

        refused = runner.invoke(hdcache_group, ["disk", "forget", "d001"])
        assert refused.exit_code != 0
        assert "still has 1005 cache entries" in refused.output

        with session_scope(engine) as session:
            session.execute(delete(CacheEntry).where(CacheEntry.disk_id == "d001"))
        forgotten = runner.invoke(hdcache_group, ["disk", "forget", "d001"])
        assert forgotten.exit_code == 0
        assert "tombstone" in forgotten.output
    finally:
        set_manager_factory(None)


def test_hdcache_add_cleans_up_post_provision_failure(
    engine: Engine,
    tmp_path: Path,
    runner: CliRunner,
) -> None:
    provisioner = FakeProvisioner(
        tmp_path,
        [BlockDeviceCandidate("/dev/sdb", "SER-NEW", slot="02", capacity_bytes=1000)],
    )
    provisioner.serial_overrides["/dev/sdb"] = "SER-DUP"
    manager = HdcacheLifecycleManager(
        engine,
        provisioner=provisioner,
        mount_root=tmp_path,
        hmac_secret=b"secret",
    )
    with session_scope(engine) as session:
        session.add(
            CacheDisk(
                disk_id="d001",
                serial="SER-DUP",
                fs_uuid="fs-dup",
                mount=str(tmp_path / "d001"),
                state="active",
                capacity_bytes=1000,
                filled_bytes=0,
            )
        )

    set_manager_factory(lambda: manager)
    try:
        add = runner.invoke(hdcache_group, ["disk", "add", "/dev/sdb"])

        assert add.exit_code != 0
        assert "cache disk serial is already enrolled: SER-DUP" in add.output
        assert [disk.mount.name for disk in provisioner.cleanup_calls] == ["d002"]
        assert not (tmp_path / "d002").exists()
        with session_scope(engine) as session:
            assert session.get(CacheDisk, "d002") is None
    finally:
        set_manager_factory(None)


def test_hdcache_disk_queries_do_not_eager_load_entries(
    engine: Engine,
    tmp_path: Path,
) -> None:
    provisioner = FakeProvisioner(
        tmp_path,
        [BlockDeviceCandidate("/dev/sda", "SER001", slot="01", capacity_bytes=1000)],
    )
    manager = HdcacheLifecycleManager(
        engine,
        provisioner=provisioner,
        mount_root=tmp_path,
        hmac_secret=b"secret",
    )
    manager.add_disk("/dev/sda")
    _seed_entries(engine, "d001", count=3)
    statements: list[str] = []

    def _collect_select(
        _conn: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: object,
    ) -> None:
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append(statement)

    event.listen(engine, "before_cursor_execute", _collect_select)
    try:
        rows = manager.disks(include_dead=True)
    finally:
        event.remove(engine, "before_cursor_execute", _collect_select)

    assert [row.disk_id for row in rows] == ["d001"]
    assert not any("cache_entry" in statement.lower() for statement in statements)


def test_hdcache_dead_requires_confirm_when_disk_still_mounted(
    engine: Engine,
    tmp_path: Path,
    runner: CliRunner,
) -> None:
    provisioner = FakeProvisioner(
        tmp_path,
        [BlockDeviceCandidate("/dev/sda", "SER001", slot="01", capacity_bytes=1000)],
    )
    manager = HdcacheLifecycleManager(
        engine,
        provisioner=provisioner,
        mount_root=tmp_path,
        hmac_secret=b"secret",
    )
    set_manager_factory(lambda: manager)
    try:
        add = runner.invoke(hdcache_group, ["disk", "add", "/dev/sda", "--json"])
        assert add.exit_code == 0
        provisioner.mounted_disk_ids.add("d001")

        blocked = runner.invoke(hdcache_group, ["disk", "dead", "d001", "--yes"])

        assert blocked.exit_code != 0
        assert "--confirm-mounted" in blocked.output

        confirmed = runner.invoke(
            hdcache_group,
            ["disk", "dead", "d001", "--yes", "--confirm-mounted", "--json"],
        )
        assert confirmed.exit_code == 0
        assert json.loads(confirmed.output)["disk_id"] == "d001"
    finally:
        set_manager_factory(None)


def test_hdcache_dead_reports_luks_drop_failure_without_rolling_back_entry_loss(
    engine: Engine,
    tmp_path: Path,
    runner: CliRunner,
) -> None:
    provisioner = FakeProvisioner(
        tmp_path,
        [BlockDeviceCandidate("/dev/sda", "SER001", slot="01", capacity_bytes=1000)],
    )
    manager = HdcacheLifecycleManager(
        engine,
        provisioner=provisioner,
        mount_root=tmp_path,
        hmac_secret=b"secret",
    )
    set_manager_factory(lambda: manager)
    try:
        add = runner.invoke(hdcache_group, ["disk", "add", "/dev/sda", "--json"])
        assert add.exit_code == 0
        _seed_entries(engine, "d001", count=1)
        provisioner.drop_error = OSError("slot busy")

        dead = runner.invoke(hdcache_group, ["disk", "dead", "d001", "--yes", "--json"])

        assert dead.exit_code == 0
        payload = json.loads(dead.output)
        assert payload["entries_lost"] == 1
        assert payload["luks_key_drop"] == "WARNING: failed to drop LUKS key slot for d001: slot busy"
        with session_scope(engine) as session:
            assert session.scalar(select(CacheEntry.state)) == "lost"
    finally:
        set_manager_factory(None)


def _seed_entries(engine: Engine, disk_id: str, *, count: int) -> None:
    with session_scope(engine) as session:
        for index in range(count):
            digest = index.to_bytes(32, "big")
            session.add(LogicalAsset(content_sha256=digest, size_bytes=1))
            session.add(
                CacheEntry(
                    content_sha256=digest,
                    artifactclass="s-masters",
                    disk_id=disk_id,
                    relpath=f"{digest.hex()[:2]}/{digest.hex()}",
                    size_bytes=1,
                    state="present",
                    representation="raw-bytes",
                    trusted=True,
                )
            )


class FakeProvisioner:
    def __init__(self, root: Path, candidates: list[BlockDeviceCandidate]) -> None:
        self.root = root
        self.candidates = {candidate.block_dev: candidate for candidate in candidates}
        self.cleanup_calls: list[ProvisionedDisk] = []
        self.dropped_slots: list[str] = []
        self.drop_error: OSError | None = None
        self.mounted_disk_ids: set[str] = set()
        self.scan_count = 0
        self.serial_overrides: dict[str, str] = {}

    def scan_devices(self) -> list[BlockDeviceCandidate]:
        self.scan_count += 1
        return list(self.candidates.values())

    def provision(
        self,
        block_dev: str,
        *,
        disk_id: str,
        mount_root: Path,
    ) -> ProvisionedDisk:
        candidate = self.candidates[block_dev]
        serial = self.serial_overrides.get(block_dev, candidate.serial)
        mount = mount_root / disk_id
        mount.mkdir(parents=True, exist_ok=True)
        return ProvisionedDisk(
            block_dev=block_dev,
            serial=serial,
            wwn=candidate.wwn,
            fs_uuid=f"fs-{serial}",
            enclosure=candidate.enclosure,
            slot=candidate.slot,
            mount=mount,
            capacity_bytes=candidate.capacity_bytes,
            smart_status="ok",
        )

    def locate(self, disk: CacheDisk) -> str:
        return f"locating {disk.disk_id} serial={disk.serial}"

    def drop_luks_key_slot(self, disk: CacheDisk) -> str:
        if self.drop_error is not None:
            raise self.drop_error
        self.dropped_slots.append(disk.disk_id)
        return f"dropped luks slot for {disk.disk_id}"

    def cleanup_failed_enrollment(self, disk: ProvisionedDisk) -> None:
        self.cleanup_calls.append(disk)
        shutil.rmtree(disk.mount, ignore_errors=True)

    def is_mounted(self, disk: CacheDisk) -> bool:
        return disk.disk_id in self.mounted_disk_ids
