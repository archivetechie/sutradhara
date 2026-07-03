"""Hdcache M5 walker and rebuild tests."""

from __future__ import annotations

import datetime as dt
import hashlib
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, select

from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    AssetLocator,
    Backend,
    Bundle,
    BundleMember,
    Copy,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, locator_key, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource
from sutradhara.hdcache.models import CacheDisk, CacheEntry
from sutradhara.hdcache.store import (
    RAW_REPRESENTATION,
    ExpectedDiskIdentity,
    ObservedBlockIdentity,
    entry_path,
    tmp_root,
    write_disk_sentinel,
    write_entry,
)
from sutradhara.hdcache.walker import (
    HdcacheWalkerConfig,
    HdcacheWalkerEvent,
    rebuild_hdcache,
    walk_disk,
)
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    eng = make_engine(f"sqlite:///{tmp_path / 'hdcache-walker.db'}")
    create_all(eng)
    yield eng
    eng.dispose()


def test_walker_table_matrix_repairs_and_marks_lost(
    engine: Engine,
    tmp_path: Path,
) -> None:
    secret = b"walker-secret"
    mount = _mount_with_identity(tmp_path, secret=secret)
    old = dt.datetime.now(dt.UTC) - dt.timedelta(hours=8)
    with session_scope(engine) as session:
        disk = _disk(session, mount)
        present = _seed_archived_asset(session, data=b"present")
        _entry(session, disk, present, b"present")
        missing = _seed_archived_asset(session, data=b"missing")
        _entry(session, disk, missing, b"missing", write_file=False)
        mismatch = _seed_archived_asset(session, data=b"mismatch")
        mismatch_entry = _entry(session, disk, mismatch, b"mismatch")
        entry_path(mount, mismatch).write_bytes(b"bad")
        filling_final = _seed_archived_asset(session, data=b"filling-final")
        _entry(session, disk, filling_final, b"filling-final", state="filling", placed_at=old)
        filling_young = _seed_archived_asset(session, data=b"filling-young")
        _entry(
            session,
            disk,
            filling_young,
            b"filling-young",
            state="filling",
            write_file=False,
        )
        unknown = mount / "hdcache" / "v1" / "aa" / "unknown.bin"
        unknown.parent.mkdir(parents=True, exist_ok=True)
        unknown.write_bytes(b"unknown")
        tmp_dir = tmp_root(mount)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        old_tmp = tmp_dir / "old.tmp"
        young_tmp = tmp_dir / "young.tmp"
        old_tmp.write_bytes(b"old")
        young_tmp.write_bytes(b"young")
        old_stamp = (dt.datetime.now(dt.UTC) - dt.timedelta(hours=8)).timestamp()
        os.utime(old_tmp, (old_stamp, old_stamp))
        disk.filled_bytes = 999
        session.flush()

        result = walk_disk(
            session,
            disk,
            config=HdcacheWalkerConfig(
                hmac_secret=secret,
                identity_probe=FakeProbe(),
                enqueue_repopulation=False,
                tmp_gc_age_seconds=60,
                filling_young_seconds=60,
            ),
        )

        assert result.unknown_deleted == 1
        assert result.tmp_deleted == 1
        assert result.entries_lost == 2
        assert result.entries_present == 1
        assert not unknown.exists()
        assert not old_tmp.exists()
        assert young_tmp.exists()
        assert session.get(CacheEntry, missing).state == "lost"
        assert session.get(CacheEntry, mismatch).state == "lost"
        assert session.get(CacheEntry, filling_final).state == "present"
        assert session.get(CacheEntry, filling_young).state == "filling"
        assert disk.filled_bytes == (
            len(b"present") + len(b"filling-final") + len(b"filling-young")
        )
        assert mismatch_entry.state == "lost"


def test_walker_tripwire_and_identity_mismatch_are_non_destructive(
    engine: Engine,
    tmp_path: Path,
) -> None:
    secret = b"walker-secret"
    mount = _mount_with_identity(tmp_path, secret=secret)
    events: list[HdcacheWalkerEvent] = []
    unknown = mount / "hdcache" / "v1" / "aa" / "unknown.bin"
    unknown.parent.mkdir(parents=True, exist_ok=True)
    unknown.write_bytes(b"unknown")
    with session_scope(engine) as session:
        disk = _disk(session, mount)
        halted = walk_disk(
            session,
            disk,
            config=HdcacheWalkerConfig(
                hmac_secret=secret,
                identity_probe=FakeProbe(),
                unknown_file_tripwire=0,
                event_sink=events.append,
            ),
        )
        assert halted.halted is True
        assert unknown.exists()
        assert events[-1].code == "walker-tripwire-halt"

        events.clear()
        read_only = walk_disk(
            session,
            disk,
            config=HdcacheWalkerConfig(
                hmac_secret=secret,
                identity_probe=FakeProbe(serial="OTHER"),
                event_sink=events.append,
            ),
        )
        assert read_only.destructive is False
        assert unknown.exists()
        assert events[-1].code == "disk-identity-mismatch"


def test_rebuild_inserts_untrusted_rows_and_walker_promotes(
    engine: Engine,
    tmp_path: Path,
) -> None:
    secret = b"walker-secret"
    mount = _mount_with_identity(tmp_path, secret=secret)
    with session_scope(engine) as session:
        disk = _disk(session, mount)
        digest = _seed_archived_asset(session, data=b"rebuild me")
        write_entry(mount, digest, b"rebuild me", representation=RAW_REPRESENTATION)
        foreign = hashlib.sha256(b"foreign").digest()
        foreign_path = entry_path(mount, foreign)
        foreign_path.parent.mkdir(parents=True, exist_ok=True)
        foreign_path.write_bytes(b"foreign")
        malformed = mount / "hdcache" / "v1" / "aa" / "not-a-cache-entry"
        malformed.parent.mkdir(parents=True, exist_ok=True)
        malformed.write_bytes(b"malformed")

        result = rebuild_hdcache(
            session,
            config=HdcacheWalkerConfig(hmac_secret=secret, identity_probe=FakeProbe()),
        )

        assert result.entries == 1
        assert [(failure.relpath, failure.content_sha256, failure.reason) for failure in result.failures] == [
            ("aa/not-a-cache-entry", None, "malformed-name"),
            (
                str(foreign_path.relative_to(mount / "hdcache" / "v1")),
                foreign.hex(),
                "not-cacheable-or-unknown",
            ),
        ]
        assert foreign_path.exists()
        assert malformed.exists()
        row = session.get(CacheEntry, digest)
        assert row is not None
        assert row.trusted is False
        assert row.artifactclass == "s-masters"
        assert row.bundle_key is not None

        walk = walk_disk(
            session,
            disk,
            config=HdcacheWalkerConfig(
                hmac_secret=secret,
                identity_probe=FakeProbe(),
                verify_untrusted=True,
                enqueue_repopulation=False,
            ),
        )

        assert walk.entries_promoted == 1
        assert session.get(CacheEntry, digest).trusted is True


def test_read_only_walk_verification_alarms_without_promotion_or_lost(
    engine: Engine,
    tmp_path: Path,
) -> None:
    secret = b"walker-secret"
    mount = _mount_with_identity(tmp_path, secret=secret)
    events: list[HdcacheWalkerEvent] = []
    with session_scope(engine) as session:
        disk = _disk(session, mount)
        digest = _seed_archived_asset(session, data=b"good")
        entry = _entry(session, disk, digest, b"good")
        entry.trusted = False
        entry_path(mount, digest).write_bytes(b"badd")
        session.flush([entry])

        result = walk_disk(
            session,
            disk,
            config=HdcacheWalkerConfig(
                hmac_secret=secret,
                identity_probe=FakeProbe(),
                verify_untrusted=True,
                event_sink=events.append,
                enqueue_repopulation=False,
            ),
            destructive=False,
        )

        assert result.destructive is False
        assert result.entries_lost == 0
        assert result.entries_promoted == 0
        assert session.get(CacheEntry, digest).state == "present"
        assert session.get(CacheEntry, digest).trusted is False
        assert [event.code for event in events] == ["walker-untrusted-verify-failed"]


def test_rebuild_rejects_spoofed_sentinel_without_rows(
    engine: Engine,
    tmp_path: Path,
) -> None:
    secret = b"walker-secret"
    mount = _mount_with_identity(tmp_path, secret=secret)
    payload = (mount / "hdcache-disk.json").read_text(encoding="utf-8")
    (mount / "hdcache-disk.json").write_text(payload.replace("d001", "d999"), encoding="utf-8")
    events: list[HdcacheWalkerEvent] = []
    with session_scope(engine) as session:
        _disk(session, mount)

        result = rebuild_hdcache(
            session,
            config=HdcacheWalkerConfig(
                hmac_secret=secret,
                identity_probe=FakeProbe(),
                event_sink=events.append,
            ),
        )

        assert result.entries == 0
        assert result.disks[0].rejected is True
        assert events[-1].code == "disk-identity-mismatch"


def _mount_with_identity(tmp_path: Path, *, secret: bytes) -> Path:
    mount = tmp_path / "d001"
    mount.mkdir()
    write_disk_sentinel(
        mount,
        ExpectedDiskIdentity("d001", "SER001", "fs-001"),
        hmac_secret=secret,
    )
    return mount


def _disk(session, mount: Path) -> CacheDisk:
    disk = CacheDisk(
        disk_id="d001",
        serial="SER001",
        fs_uuid="fs-001",
        mount=str(mount),
        state="active",
        capacity_bytes=1000,
        filled_bytes=0,
    )
    session.add(disk)
    session.flush()
    return disk


def _entry(
    session,
    disk: CacheDisk,
    digest: bytes,
    data: bytes,
    *,
    state: str = "present",
    write_file: bool = True,
    placed_at: dt.datetime | None = None,
) -> CacheEntry:
    if write_file:
        result = write_entry(Path(disk.mount), digest, data, representation=RAW_REPRESENTATION)
        relpath = result.relpath
        size = result.size_bytes
    else:
        relpath = f"{digest.hex()[:2]}/{digest.hex()}"
        size = len(data)
    entry = CacheEntry(
        content_sha256=digest,
        artifactclass="s-masters",
        disk_id=disk.disk_id,
        relpath=relpath,
        size_bytes=size,
        state=state,
        representation=RAW_REPRESENTATION,
        trusted=True,
        placed_at=placed_at or dt.datetime.now(dt.UTC),
    )
    session.add(entry)
    disk.filled_bytes += size if state in {"filling", "present"} else 0
    session.flush([entry, disk])
    return entry


def _seed_archived_asset(session, *, data: bytes) -> bytes:
    digest = hashlib.sha256(data).digest()
    session.merge(LogicalAsset(content_sha256=digest, size_bytes=len(data)))
    backend = session.scalar(select(Backend).where(Backend.name == "mem"))
    if backend is None:
        backend = Backend(name="mem", kind=BackendKind.MEMORY, tier=BackendTier.SELF_DESCRIBING)
        session.add(backend)
        session.flush()
    if session.get(Pool, "mem-pool") is None:
        session.add(
            Pool(
                id="mem-pool",
                backend_id=backend.id,
                representation=Representation.RAW_BYTES.value,
            )
        )
    session.merge(
        ArtifactClassPolicyRecord(
            artifactclass="s-masters",
            ruleset="test.rules",
            expect="messy",
            target_bytes=1024,
            max_age_seconds=3600,
            restore_preference=["mem-pool"],
            staging_config={},
            hdcache_config={"enabled": True, "privacy_level": "none"},
        )
    )
    bundle_id = f"bundle-{digest.hex()[:12]}"
    if session.get(Bundle, bundle_id) is None:
        session.add(
            Bundle(
                id=bundle_id,
                artifactclass="s-masters",
                status="sealed",
                target_bytes=1024,
                max_age_seconds=3600,
                sealed_at=dt.datetime.now(dt.UTC),
            )
        )
        session.add(
            BundleMember(
                bundle_id=bundle_id,
                logical_asset_hash=digest,
                member_path=f"{digest.hex()}.mov",
                size_bytes=len(data),
                file_sha256=digest,
            )
        )
    copy = Copy(
        bundle_id=bundle_id,
        backend_id=backend.id,
        pool_id="mem-pool",
        native_locator={"hash_hex": digest.hex()},
        native_locator_key=locator_key({"hash_hex": digest.hex()}),
        storage_metadata={"representation": Representation.RAW_BYTES.value},
        integrity_hash=digest,
        health=CopyHealth.OK,
        source=CopySource.INGEST,
    )
    session.add(copy)
    session.flush()
    session.add(
        AssetLocator(
            logical_asset_hash=digest,
            pool_id="mem-pool",
            copy_id=copy.id,
            bundle_id=bundle_id,
            native_locator={
                "member_path": f"{digest.hex()}.mov",
                "offset": 0,
                "size_bytes": len(data),
            },
            member_path=f"{digest.hex()}.mov",
            representation=Representation.RAW_BYTES.value,
        )
    )
    session.flush()
    return digest


class FakeProbe:
    def __init__(self, *, serial: str = "SER001") -> None:
        self.serial = serial

    def observe(self, _mount: Path) -> ObservedBlockIdentity:
        return ObservedBlockIdentity(True, serial=self.serial, fs_uuid="fs-001")
