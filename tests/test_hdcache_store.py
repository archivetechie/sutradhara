"""HD cache schema and disk-store primitive tests."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, select

from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    Backend,
    Copy,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.hdcache.models import CacheDisk, CacheEntry, RestoreRequest, RestoreRequestItem
from sutradhara.hdcache.store import (
    AEAD_REPRESENTATION,
    RAW_REPRESENTATION,
    DiskIdentityResult,
    EntryWriteResult,
    ExpectedDiskIdentity,
    ObservedBlockIdentity,
    StoreError,
    enumerate_entries,
    read_entry_verified,
    verify_disk_identity,
    write_disk_sentinel,
    write_entry,
)


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    eng = make_engine(f"sqlite:///{tmp_path / 'hdcache.db'}")
    create_all(eng)
    yield eng
    eng.dispose()


def test_hdcache_schema_round_trips_inventory_and_restore_rows(engine: Engine, tmp_path: Path) -> None:
    digest = hashlib.sha256(b"clip").digest()
    with session_scope(engine) as session:
        session.add(LogicalAsset(content_sha256=digest, size_bytes=4))
        session.add(
            CacheDisk(
                disk_id="d001",
                serial="SER001",
                fs_uuid="fs-001",
                mount=str(tmp_path / "d001"),
                state="active",
                capacity_bytes=1000,
                filled_bytes=4,
                smart_status="ok",
            )
        )
        session.add(
            CacheEntry(
                content_sha256=digest,
                artifactclass="s-masters",
                bundle_key="bundle-1",
                group_key="event-1",
                disk_id="d001",
                relpath=f"{digest.hex()[:2]}/{digest.hex()}",
                size_bytes=4,
                state="present",
                representation=RAW_REPRESENTATION,
                trusted=True,
            )
        )
        request = RestoreRequest(
            id="restore-1",
            identity="owner",
            destination_id="media-server",
            state="active",
        )
        request.items.append(
            RestoreRequestItem(
                content_sha256=digest,
                artifactclass="s-masters",
                state="queued",
            )
        )
        session.add(request)

    with session_scope(engine) as session:
        disk = session.get(CacheDisk, "d001")
        assert disk is not None
        assert disk.serial == "SER001"
        entry = session.get(CacheEntry, digest)
        assert entry is not None
        assert entry.disk_id == "d001"
        assert entry.bundle_key == "bundle-1"
        request = session.get(RestoreRequest, "restore-1")
        assert request is not None
        assert request.items[0].state == "queued"


def test_hdcache_inv1_cache_never_registers_archival_backend_rows(
    engine: Engine,
    tmp_path: Path,
) -> None:
    with session_scope(engine) as session:
        session.add(
            CacheDisk(
                disk_id="d001",
                serial="SER001",
                fs_uuid="fs-001",
                mount=str(tmp_path / "d001"),
                state="active",
                capacity_bytes=1000,
                filled_bytes=0,
            )
        )
        session.add(
            ArtifactClassPolicyRecord(
                artifactclass="s-masters",
                ruleset="test.rules.v1",
                expect="messy",
                target_bytes=1000,
                max_age_seconds=3600,
                restore_preference=[],
                staging_config={},
            )
        )

    with session_scope(engine) as session:
        assert list(session.scalars(select(Backend))) == []
        assert list(session.scalars(select(Pool))) == []
        assert list(session.scalars(select(Copy))) == []
        assert session.execute(select(CacheDisk)).scalar_one().disk_id == "d001"


def test_store_write_read_delete_and_crash_windows(tmp_path: Path) -> None:
    mount = tmp_path / "d001"
    mount.mkdir()
    payload = b"hello cache"
    digest = hashlib.sha256(payload).digest()

    result = write_entry(mount, digest, [payload])

    assert isinstance(result, EntryWriteResult)
    assert result.path.exists()
    assert result.relpath == f"{digest.hex()[:2]}/{digest.hex()}"
    assert result.stored_digest is None
    read = read_entry_verified(mount, digest)
    assert read.data == payload
    assert read.size_bytes == len(payload)

    # File-present but DB-not-yet-flipped is a representable crash window.
    assert result.path.is_file()

    assert write_entry(mount, digest, [payload]).path == result.path
    assert _tmp_files(mount) == []

    class SimulatedCrash(BaseException):
        pass

    other = hashlib.sha256(b"other").digest()
    with pytest.raises(SimulatedCrash):
        write_entry(
            mount,
            other,
            [b"other"],
            before_rename=lambda _tmp, _final: (_ for _ in ()).throw(SimulatedCrash()),
        )
    assert len(_tmp_files(mount)) == 1
    assert not read_entry_path(mount, other).exists()

    assert read_entry_verified(mount, digest).stream_digest == digest
    assert result.path.exists()
    assert read_entry_verified(mount, digest).data == payload
    assert result.path.exists()


def test_store_refuses_stream_hash_mismatch_and_bad_reads(tmp_path: Path) -> None:
    mount = tmp_path / "d001"
    mount.mkdir()
    payload = b"actual"
    wrong = hashlib.sha256(b"expected").digest()

    with pytest.raises(StoreError, match="stream digest mismatch"):
        write_entry(mount, wrong, [payload])

    digest = hashlib.sha256(payload).digest()
    result = write_entry(mount, digest, [payload])
    result.path.write_bytes(b"corrupt")
    with pytest.raises(StoreError, match="stored stream digest mismatch"):
        read_entry_verified(mount, digest)

    target_payload = b"symlink target"
    target = tmp_path / "outside"
    target.write_bytes(target_payload)
    symlink_digest = hashlib.sha256(target_payload).digest()
    symlink_path = read_entry_path(mount, symlink_digest)
    symlink_path.parent.mkdir(parents=True, exist_ok=True)
    symlink_path.symlink_to(target)
    with pytest.raises(StoreError, match="regular file"):
        read_entry_verified(mount, symlink_digest)


def test_store_enumerates_raw_and_aead_filenames(tmp_path: Path) -> None:
    mount = tmp_path / "d001"
    mount.mkdir()
    raw = b"raw bytes"
    raw_digest = hashlib.sha256(raw).digest()
    sealed = b"sealed bytes"
    plaintext_digest = hashlib.sha256(b"plaintext").digest()
    sealed_digest = hashlib.sha256(sealed).digest()

    write_entry(mount, raw_digest, [raw])
    write_entry(
        mount,
        plaintext_digest,
        [sealed],
        representation=AEAD_REPRESENTATION,
        key_epoch="hdcache-epoch-1",
        expected_stream_sha256=sealed_digest,
    )
    with pytest.raises(StoreError, match="key_epoch"):
        write_entry(
            mount,
            plaintext_digest,
            [sealed],
            representation=AEAD_REPRESENTATION,
            key_epoch="bad.epoch",
            expected_stream_sha256=sealed_digest,
        )
    ignored_digest = hashlib.sha256(b"directory-placeholder").digest()
    ignored_dir = mount / "hdcache" / "v1" / ignored_digest.hex()[:2] / ignored_digest.hex()
    ignored_dir.mkdir(parents=True)

    entries = enumerate_entries(mount)

    assert sorted(
        (entry.content_sha256, entry.representation, entry.key_epoch) for entry in entries
    ) == sorted(
        [
            (raw_digest, RAW_REPRESENTATION, None),
            (plaintext_digest, AEAD_REPRESENTATION, "hdcache-epoch-1"),
        ]
    )


def test_disk_identity_matrix(tmp_path: Path) -> None:
    mount = tmp_path / "d001"
    mount.mkdir()
    secret = b"server-held-secret"
    expected = ExpectedDiskIdentity(
        disk_id="d001",
        serial="SER001",
        fs_uuid="fs-001",
        wwn="wwn-001",
    )
    ok_probe = FakeProbe(ObservedBlockIdentity(True, serial="SER001", fs_uuid="fs-001", wwn="wwn-001"))
    write_disk_sentinel(mount, expected, hmac_secret=secret)

    assert verify_disk_identity(mount, expected, hmac_secret=secret, probe=ok_probe).ok
    assert _identity_status(
        mount,
        expected,
        secret,
        FakeProbe(ObservedBlockIdentity(False)),
    ) == "not_mounted"
    assert _identity_status(
        mount,
        expected,
        secret,
        FakeProbe(ObservedBlockIdentity(True, serial="OTHER", fs_uuid="fs-001")),
    ) == "wrong_serial"
    assert _identity_status(
        mount,
        expected,
        secret,
        FakeProbe(ObservedBlockIdentity(True, serial="SER001", fs_uuid="other")),
    ) == "wrong_fs_uuid"
    assert _identity_status(
        mount,
        expected,
        secret,
        FakeProbe(ObservedBlockIdentity(True, serial="SER001", fs_uuid="fs-001", wwn="other")),
    ) == "wrong_wwn"
    assert _identity_status(
        mount,
        expected,
        secret,
        FakeProbe(ObservedBlockIdentity(True, serial="SER001", fs_uuid="fs-001")),
    ) == "identity_unavailable"

    (mount / "hdcache-disk.json").unlink()
    assert _identity_status(mount, expected, secret, ok_probe) == "missing_sentinel"
    write_disk_sentinel(mount, expected, hmac_secret=secret)
    payload = json.loads((mount / "hdcache-disk.json").read_text(encoding="utf-8"))
    payload["serial"] = "SER001"
    payload["hmac"] = "0" * 64
    (mount / "hdcache-disk.json").write_text(json.dumps(payload), encoding="utf-8")
    assert _identity_status(mount, expected, secret, ok_probe) == "bad_hmac"


def read_entry_path(mount: Path, digest: bytes) -> Path:
    return mount / "hdcache" / "v1" / digest.hex()[:2] / digest.hex()


def _tmp_files(mount: Path) -> list[Path]:
    tmp = mount / "hdcache" / "v1" / "tmp"
    return sorted(path for path in tmp.iterdir() if path.is_file()) if tmp.exists() else []


def _identity_status(
    mount: Path,
    expected: ExpectedDiskIdentity,
    secret: bytes,
    probe: "FakeProbe",
) -> str:
    result = verify_disk_identity(mount, expected, hmac_secret=secret, probe=probe)
    assert isinstance(result, DiskIdentityResult)
    return result.status


class FakeProbe:
    def __init__(self, observed: ObservedBlockIdentity) -> None:
        self.observed = observed

    def observe(self, _mount: Path) -> ObservedBlockIdentity:
        return self.observed
