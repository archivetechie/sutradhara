"""Front-door receive BagIt filesystem contract tests."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import shutil
import tarfile
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from sqlalchemy import Engine, func, select

from sutradhara.catalog.models import IngestItem
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import IntakeStatus
from sutradhara.cli.main import cli
from sutradhara.intake import register_landing_root
from sutradhara_receive import (
    BAG_PROFILE,
    CANONICALIZATION_VERSION,
    PACKAGE_INDEX_NAME,
    PACKAGE_PROFILE_HASH,
    PACKAGE_PROFILE_VERSION,
    RECEIVE_PACKAGE,
    RECEIVE_VERSION,
    VERIFY_SIDECAR_NAME,
    AtomicWriteObserver,
    CollisionError,
    DestinationVerificationError,
    ReceiveError,
    SourceMutationError,
    bag_info_metadata,
    read_bag_info,
    read_manifest_sha256,
    read_package_index,
    receive_source,
    safe_payload_path,
    sweep_orphans,
    validate_bag,
    verify_destination,
    verify_pending,
    wait_for_server_confirmation,
    write_bagit_files,
)
from sutradhara_receive import core as receive_core
from sutradhara_receive.cli import main as receive_cli_main


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_bagit_writer_round_trips_to_shared_reader_and_payload_oxum(tmp_path: Path) -> None:
    bag = tmp_path / "bag"
    data = bag / "data"
    data.mkdir(parents=True)
    payload = b"hello"
    (data / "clip%.mov").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()

    result = write_bagit_files(
        bag,
        entries={"clip%.mov": digest},
        metadata=bag_info_metadata(
            intake_id="bag-001",
            source_kind="card",
            operator="op",
            source_ref="A001",
            artifactclass="camera-original",
            label="shoot",
            started_at=dt.datetime(2026, 6, 18, tzinfo=dt.UTC),
            file_count=1,
            total_bytes=len(payload),
            skipped_count=0,
        ),
    )

    assert (bag / "bagit.txt").read_text(encoding="utf-8") == (
        "BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n"
    )
    assert result.manifest_path.read_text(encoding="utf-8") == (f"{digest}  data/clip%25.mov\n")
    assert read_manifest_sha256(result.manifest_path) == {"clip%.mov": digest}
    bag_info = read_bag_info(result.bag_info_path)
    assert bag_info["Payload-Oxum"] == f"{len(payload)}.1"
    validation = validate_bag(bag)
    assert validation.complete is True
    assert validation.valid is True


def test_bagit_writer_uses_native_atomic_observer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert receive_core._native is not None
    bag = tmp_path / "bag"
    data = bag / "data"
    data.mkdir(parents=True)
    payload = b"hello"
    (data / "clip.mov").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    observer = _RecordingObserver()

    def fail_python_atomic_writer(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("pure Python atomic writer should not be called")

    monkeypatch.setattr(receive_core, "_atomic_write_text", fail_python_atomic_writer)

    result = write_bagit_files(
        bag,
        entries={"clip.mov": digest},
        metadata=bag_info_metadata(
            intake_id="bag-001",
            source_kind="card",
            operator="op",
            source_ref="A001",
            artifactclass="camera-original",
            label="shoot",
            started_at=dt.datetime(2026, 6, 18, tzinfo=dt.UTC),
            file_count=1,
            total_bytes=len(payload),
            skipped_count=0,
        ),
        observer=observer,
    )

    assert result.manifest_path == bag / "manifest-sha256.txt"
    assert observer.destinations == [
        bag / "bagit.txt",
        bag / "bag-info.txt",
        bag / "manifest-sha256.txt",
        bag / "tagmanifest-sha256.txt",
    ]
    assert validate_bag(bag).valid is True


def test_receive_source_uses_native_write_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert receive_core._native is not None
    assert hasattr(receive_core._native, "receive_source_json")
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")

    def fail_python_copy_path(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("pure Python receive copy path should not be called")

    monkeypatch.setattr(receive_core, "_copy_or_verify_entries", fail_python_copy_path)

    result = receive_source(source, landing=landing, source_kind="card", operator="op")

    assert (result.intake_dir / "data" / "clip.mov").read_bytes() == b"video"
    assert validate_bag(result.intake_dir).valid is True


def test_legacy_receive_core_import_aliases_extracted_package() -> None:
    import sutradhara.receive.core as legacy_core
    import sutradhara_receive.core as extracted_core

    assert legacy_core is extracted_core
    assert extracted_core._native is not None


def test_receive_output_passes_reference_bagit_validator_when_installed(tmp_path: Path) -> None:
    bagit = pytest.importorskip("bagit")
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")

    result = receive_source(source, landing=landing, source_kind="card", operator="op")

    bagit.Bag(str(result.intake_dir)).validate()


def test_validate_bag_rejects_payload_manifest_path_outside_data(tmp_path: Path) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    result = receive_source(source, landing=landing, source_kind="card", operator="op")
    result.manifest_path.write_text(
        result.manifest_path.read_text(encoding="utf-8").replace(
            "data/clip.mov",
            "clip.mov",
        ),
        encoding="utf-8",
    )

    validation = validate_bag(result.intake_dir)

    assert validation.complete is False
    assert any("must start with data/" in item for item in validation.errors)


def test_receive_writes_slim_sentinel_last_and_bag_tags(tmp_path: Path) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    observer = _RecordingObserver()

    result = receive_source(
        source,
        landing=landing,
        source_kind="card",
        operator="Camera Op",
        source_ref="A001",
        artifactclass="camera-original",
        atomic_observer=observer,
    )

    sentinel = json.loads(result.sentinel_path.read_text(encoding="utf-8"))
    assert sentinel == {
        "bag_profile": BAG_PROFILE,
        "created_at": sentinel["created_at"],
        "intake_id": result.intake_id,
        "status": "complete",
    }
    assert "manifest_sha256" not in sentinel
    assert not (result.intake_dir / ".receiving.json").exists()
    assert observer.intake_checked is True
    assert observer.destinations[-1].name == "intake.json"
    bag_info = read_bag_info(result.bag_info_path)
    assert bag_info["Bag-Software-Agent"] == f"sutradhara-receive/{RECEIVE_VERSION}"
    assert bag_info["Receive-Package"] == RECEIVE_PACKAGE
    assert bag_info["Canonicalization-Version"] == CANONICALIZATION_VERSION
    assert bag_info["Payload-Oxum"] == f"{len(b'video')}.1"
    assert read_manifest_sha256(result.manifest_path) == {
        "clip.mov": hashlib.sha256(b"video").hexdigest()
    }


def test_receive_rejects_nfc_and_case_collisions_before_payload_copy(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    for names in (("Café.mov", "Cafe\u0301.mov"), ("A.mov", "a.mov")):
        source = tmp_path / f"source-{len(names[0])}-{names[0][0]}"
        source.mkdir()
        for name in names:
            (source / name).write_bytes(name.encode())

        with pytest.raises(CollisionError):
            receive_source(source, landing=landing, source_kind="card", operator="op")

    assert not list(landing.glob("*/data"))
    assert not list(landing.glob("*/intake.json"))


def test_receive_escapes_invalid_source_bytes(tmp_path: Path) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    raw_path = os.fsencode(source) + b"/bad_\xff.bin"
    fd = os.open(raw_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        os.write(fd, b"legacy")
    finally:
        os.close(fd)

    result = receive_source(source, landing=landing, source_kind="drive", operator="op")

    escaped = "bad_\\xff.bin"
    assert (result.intake_dir / "data" / escaped).read_bytes() == b"legacy"
    assert read_manifest_sha256(result.manifest_path) == {
        escaped: hashlib.sha256(b"legacy").hexdigest()
    }


def test_blocking_receive_detects_corrupt_landed_destination_before_return(tmp_path: Path) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")

    def corrupt_payload(_payload: Path, receipts: tuple[Any, ...]) -> None:
        receipts[0].destination_path.write_bytes(b"corrupt")

    with pytest.raises(DestinationVerificationError):
        receive_source(
            source,
            landing=landing,
            source_kind="card",
            operator="op",
            verify="blocking",
            after_copy_hook=corrupt_payload,
        )

    failed = next(landing.iterdir())
    assert (failed / "intake.json").exists()
    sidecar = json.loads((failed / VERIFY_SIDECAR_NAME).read_text(encoding="utf-8"))
    assert sidecar["stage"] == "failed"


def test_staged_receive_returns_at_transfer_and_verify_destination_writes_sidecar(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")

    result = receive_source(source, landing=landing, source_kind="card", operator="op")

    transfer = json.loads((result.intake_dir / VERIFY_SIDECAR_NAME).read_text(encoding="utf-8"))
    assert transfer["stage"] == "transfer"
    assert transfer["mismatches"] == []
    full = verify_destination(result.intake_dir)
    assert full.verified is True
    sidecar = json.loads((result.intake_dir / VERIFY_SIDECAR_NAME).read_text(encoding="utf-8"))
    assert sidecar["stage"] == "full"
    assert sidecar["mismatches"] == []
    log = (result.intake_dir / "receive.log").read_text(encoding="utf-8")
    assert re.search(r' copy \{"relpath":"clip\.mov","bytes":5,"copy_wall_ns":\d+\}', log)
    assert re.search(r' release \{"release_offset_ns":\d+\}', log)
    assert re.search(r' verify \{"stage2_wall_ns":\d+\}', log)


def test_receive_detects_source_mutation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    original = receive_core._stat_snapshot
    calls = 0

    def changed_once(path: Path) -> Any:
        nonlocal calls
        snapshot = original(path)
        if path.name == "clip.mov":
            calls += 1
            if calls == 2:
                return replace(snapshot, mtime_ns=snapshot.mtime_ns + 1)
        return snapshot

    monkeypatch.setattr(receive_core, "_stat_snapshot", changed_once)

    with pytest.raises(SourceMutationError):
        receive_source(source, landing=landing, source_kind="card", operator="op")

    failed = next(landing.iterdir())
    assert not (failed / "intake.json").exists()


def test_receive_records_skipped_symlink_and_fifo(tmp_path: Path) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    (source / "link.mov").symlink_to(source / "clip.mov")
    os.mkfifo(source / "pipe")

    result = receive_source(source, landing=landing, source_kind="card", operator="op")

    bag_info = read_bag_info(result.bag_info_path)
    log = (result.intake_dir / "receive.log").read_text(encoding="utf-8")
    assert bag_info["Skipped-Count"] == "2"
    assert "link.mov: symlink" in log
    assert "pipe: fifo" in log


def test_receive_wraps_package_dir_as_single_tar_with_inner_index(tmp_path: Path) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    bundle = source / "A001.fcpbundle"
    nested = bundle / "Event"
    nested.mkdir(parents=True)
    (nested / "clip.mov").write_bytes(b"video")
    (nested / "._clip.mov").write_bytes(b"appledouble")
    try:
        (bundle / "clip-link.mov").symlink_to("Event/clip.mov")
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    result = receive_source(source, landing=landing, source_kind="card", operator="op")

    data_root = result.intake_dir / "data"
    package_tar = data_root / "A001.fcpbundle.tar"
    assert package_tar.is_file()
    assert not (data_root / "A001.fcpbundle").exists()
    assert read_manifest_sha256(result.manifest_path) == {
        "A001.fcpbundle.tar": hashlib.sha256(package_tar.read_bytes()).hexdigest()
    }
    bag_info = read_bag_info(result.bag_info_path)
    assert bag_info["Package-Profile-Version"] == PACKAGE_PROFILE_VERSION
    assert bag_info["Package-Profile-Hash"] == PACKAGE_PROFILE_HASH

    package_index = read_package_index(result.intake_dir / PACKAGE_INDEX_NAME)
    package = package_index["packages"][0]
    assert package["logical_member_path"] == "A001.fcpbundle"
    assert package["stored_member_path"] == "A001.fcpbundle.tar"
    assert package["profile"] == PACKAGE_PROFILE_VERSION
    assert package["sha256"] == hashlib.sha256(package_tar.read_bytes()).hexdigest()

    by_member = {member["member"]: member for member in package["members"]}
    assert by_member["A001.fcpbundle/Event/clip.mov"]["type"] == "file"
    assert by_member["A001.fcpbundle/Event/._clip.mov"]["type"] == "file"
    assert by_member["A001.fcpbundle/clip-link.mov"]["type"] == "symlink"
    assert by_member["A001.fcpbundle/clip-link.mov"]["linkname"] == "Event/clip.mov"
    assert by_member["A001.fcpbundle/clip-link.mov"]["data_offset"] is None
    clip = by_member["A001.fcpbundle/Event/clip.mov"]
    with package_tar.open("rb") as handle:
        handle.seek(clip["data_offset"])
        extracted = handle.read(clip["length"])
    assert extracted == b"video"
    assert hashlib.sha256(extracted).hexdigest() == clip["sha256"]

    with tarfile.open(package_tar, mode="r:") as tar:
        assert tar.getmember("A001.fcpbundle").isdir()
        assert tar.getmember("A001.fcpbundle/clip-link.mov").issym()
        extracted_member = tar.extractfile("A001.fcpbundle/Event/clip.mov")
        assert extracted_member is not None
        assert extracted_member.read() == b"video"

    validation = validate_bag(result.intake_dir)
    assert validation.valid is True
    assert validation.actual_records[0].relpath == "A001.fcpbundle.tar"
    assert validation.actual_records[0].as_received_relpath == "A001.fcpbundle"


def test_package_tar_profile_is_stable_across_receive_runs(tmp_path: Path) -> None:
    source = tmp_path / "source"
    bundle = source / "A001.fcpbundle"
    (bundle / "Event").mkdir(parents=True)
    (bundle / "Event" / "clip.mov").write_bytes(b"video")
    (bundle / "Event" / "notes.txt").write_bytes(b"notes")

    first = receive_source(
        source, landing=tmp_path / "landing-a", source_kind="card", operator="op"
    )
    second = receive_source(
        source, landing=tmp_path / "landing-b", source_kind="card", operator="op"
    )

    first_tar = first.intake_dir / "data" / "A001.fcpbundle.tar"
    second_tar = second.intake_dir / "data" / "A001.fcpbundle.tar"
    assert first_tar.read_bytes() == second_tar.read_bytes()
    assert read_manifest_sha256(first.manifest_path) == read_manifest_sha256(second.manifest_path)


# A fixed package fixture must hash to this exact value. If a tarfile/Python change
# alters the bytes, this fails on purpose: the package tar's sha256 is the asset's
# durable identity, so a format change must be a conscious `package-tar-v2` bump and
# re-pin, never a silent re-identification of every package across an upgrade.
PACKAGE_TAR_GOLDEN_SHA256 = "621c2c7fc235a8243034b209d9f46bfcd3a2a505a853d6d643746ef80a20b0cc"


def _write_golden_package(source: Path) -> None:
    bundle = source / "GOLDEN.fcpbundle"
    (bundle / "Render").mkdir(parents=True)
    (bundle / "Render" / "clip01.mov").write_bytes(b"clip-one")
    (bundle / "Render" / "clip02.mov").write_bytes(b"clip-two")
    (bundle / "._meta").write_bytes(b"appledouble")
    (bundle / "library.plist").write_bytes(b"plist")


def test_package_tar_matches_pinned_golden_vector(tmp_path: Path) -> None:
    source = tmp_path / "source"
    _write_golden_package(source)

    result = receive_source(source, landing=tmp_path / "landing", source_kind="card", operator="op")

    package_tar = result.intake_dir / "data" / "GOLDEN.fcpbundle.tar"
    actual = hashlib.sha256(package_tar.read_bytes()).hexdigest()
    assert actual == PACKAGE_TAR_GOLDEN_SHA256, (
        "package-tar-v1 bytes changed: if intentional, bump the profile version and "
        f"re-pin this golden; otherwise a tarfile/Python change is silently "
        f"re-identifying every package (got {actual})"
    )


def test_package_bag_passes_reference_bagit_validator(tmp_path: Path) -> None:
    bagit = pytest.importorskip("bagit")
    source = tmp_path / "source"
    _write_golden_package(source)

    result = receive_source(source, landing=tmp_path / "landing", source_kind="card", operator="op")

    # RFC 8493 conformance with a package `.tar` payload + the package-index tag file.
    bagit.Bag(str(result.intake_dir)).validate()


def test_receive_rejects_package_stored_member_collision(tmp_path: Path) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    bundle = source / "A001.fcpbundle"
    bundle.mkdir(parents=True)
    (bundle / "clip.mov").write_bytes(b"video")
    (source / "A001.fcpbundle.tar").write_bytes(b"existing tar")

    with pytest.raises(CollisionError, match="A001\\.fcpbundle\\.tar"):
        receive_source(source, landing=landing, source_kind="card", operator="op")


def test_resume_removes_stale_package_index_when_package_disappears(tmp_path: Path) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    bundle = source / "A001.fcpbundle"
    bundle.mkdir(parents=True)
    (bundle / "clip.mov").write_bytes(b"video")

    class FailOnManifest(AtomicWriteObserver):
        def before_rename(self, temp_path: Path, final_path: Path) -> None:
            if final_path.name == "manifest-sha256.txt":
                raise ReceiveError("simulated crash after package index")

    with pytest.raises(ReceiveError, match="simulated crash"):
        receive_source(
            source,
            landing=landing,
            source_kind="card",
            operator="op",
            atomic_observer=FailOnManifest(),
        )

    intake_id = next(path.name for path in landing.iterdir() if path.is_dir())
    failed = landing / intake_id
    assert (failed / PACKAGE_INDEX_NAME).exists()
    shutil.rmtree(bundle)
    (source / "notes.txt").write_bytes(b"notes")

    resumed = receive_source(
        None,
        landing=landing,
        source_kind="card",
        operator="ignored",
        resume=intake_id,
    )

    assert not (resumed.intake_dir / PACKAGE_INDEX_NAME).exists()
    assert read_manifest_sha256(resumed.manifest_path) == {
        "notes.txt": hashlib.sha256(b"notes").hexdigest()
    }
    assert validate_bag(resumed.intake_dir).valid is True


def test_payload_path_and_source_relationship_guards(tmp_path: Path) -> None:
    with pytest.raises(ReceiveError):
        safe_payload_path(tmp_path / "data", "../escape.mov")
    with pytest.raises(ReceiveError):
        safe_payload_path(tmp_path / "data", "/absolute.mov")

    source = tmp_path / "source"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    with pytest.raises(ReceiveError):
        receive_source(source, landing=source / "landing", source_kind="card", operator="op")

    existing = tmp_path / "landing" / "done"
    data = existing / "data"
    data.mkdir(parents=True)
    (existing / "intake.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ReceiveError):
        receive_source(data, landing=tmp_path / "landing", source_kind="card", operator="op")


def test_explicit_resume_rehashes_present_files_and_bare_rerun_mints_new_id(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "a.mov").write_bytes(b"a")
    (source / "b.mov").write_bytes(b"b")

    def crash_after_copy(_payload: Path, _receipts: tuple[Any, ...]) -> None:
        raise ReceiveError("simulated crash")

    with pytest.raises(ReceiveError):
        receive_source(
            source,
            landing=landing,
            source_kind="card",
            operator="op",
            after_copy_hook=crash_after_copy,
        )

    failed_id = next(path.name for path in landing.iterdir() if path.is_dir())
    failed_data = landing / failed_id / "data"
    (failed_data / "a.mov").write_bytes(b"bad")

    rerun = receive_source(source, landing=landing, source_kind="card", operator="op")
    assert rerun.intake_id != failed_id

    resumed = receive_source(
        None,
        landing=landing,
        source_kind="card",
        operator="ignored",
        resume=failed_id,
    )
    assert resumed.intake_id == failed_id
    assert (failed_data / "a.mov").read_bytes() == b"a"
    assert not (landing / failed_id / ".receiving.json").exists()
    assert (landing / failed_id / "intake.json").exists()


def test_resume_prunes_stale_data_files(tmp_path: Path) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "keep.mov").write_bytes(b"keep")
    (source / "drop.mov").write_bytes(b"drop")

    def crash_after_copy(_payload: Path, _receipts: tuple[Any, ...]) -> None:
        raise ReceiveError("simulated crash")

    with pytest.raises(ReceiveError):
        receive_source(
            source,
            landing=landing,
            source_kind="card",
            operator="op",
            after_copy_hook=crash_after_copy,
        )

    intake_id = next(path.name for path in landing.iterdir() if path.is_dir())
    (source / "drop.mov").unlink()

    result = receive_source(
        None,
        landing=landing,
        source_kind="card",
        operator="ignored",
        resume=intake_id,
    )

    assert not (result.intake_dir / "data" / "drop.mov").exists()
    assert validate_bag(result.intake_dir).valid is True


def test_resume_rejects_existing_special_destination(tmp_path: Path) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")

    def crash_after_copy(_payload: Path, _receipts: tuple[Any, ...]) -> None:
        raise ReceiveError("simulated crash")

    with pytest.raises(ReceiveError):
        receive_source(
            source,
            landing=landing,
            source_kind="card",
            operator="op",
            after_copy_hook=crash_after_copy,
        )

    intake_id = next(path.name for path in landing.iterdir() if path.is_dir())
    destination = landing / intake_id / "data" / "clip.mov"
    destination.unlink()
    os.mkfifo(destination)

    with pytest.raises(ReceiveError, match="unsupported fifo"):
        receive_source(
            None,
            landing=landing,
            source_kind="card",
            operator="ignored",
            resume=intake_id,
        )

    assert not (landing / intake_id / "intake.json").exists()


def test_sweep_orphans_removes_only_stale_receiving_dirs(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    stale = landing / "stale"
    fresh = landing / "fresh"
    complete = landing / "complete"
    for path in (stale, fresh, complete):
        path.mkdir(parents=True)
        (path / ".receiving.json").write_text("{}", encoding="utf-8")
    (complete / "intake.json").write_text("{}", encoding="utf-8")
    old = dt.datetime.now().timestamp() - 48 * 3600
    os.utime(stale / ".receiving.json", (old, old))

    result = sweep_orphans(landing, older_than=dt.timedelta(hours=24))

    assert result.removed == (stale,)
    assert not stale.exists()
    assert fresh.exists()
    assert complete.exists()


def test_sweep_orphans_uses_native_core(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert receive_core._native is not None
    landing = tmp_path / "landing"
    stale = landing / "stale"
    fresh = landing / "fresh"
    complete = landing / "complete"
    for path in (stale, fresh, complete):
        path.mkdir(parents=True)
        (path / ".receiving.json").write_text("{}", encoding="utf-8")
    (complete / "intake.json").write_text("{}", encoding="utf-8")
    now = dt.datetime(2026, 6, 18, 12, tzinfo=dt.UTC)
    old = (now - dt.timedelta(hours=48)).timestamp()
    os.utime(stale / ".receiving.json", (old, old))

    def fail_python_rmtree(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("pure Python sweep should not delete paths")

    monkeypatch.setattr(receive_core.shutil, "rmtree", fail_python_rmtree)

    result = sweep_orphans(
        landing,
        older_than=dt.timedelta(hours=24),
        now=now,
    )

    assert result.removed == (stale,)
    assert not stale.exists()
    assert fresh.exists()
    assert complete.exists()


def test_verify_pending_rechecks_absent_transfer_and_failed_sidecars(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    clean_source = tmp_path / "clean-source"
    bad_source = tmp_path / "bad-source"
    clean_source.mkdir()
    bad_source.mkdir()
    (clean_source / "clip.mov").write_bytes(b"video")
    (bad_source / "clip.mov").write_bytes(b"video")
    clean = receive_source(clean_source, landing=landing, source_kind="card", operator="op")
    bad = receive_source(bad_source, landing=landing, source_kind="card", operator="op")
    (clean.intake_dir / VERIFY_SIDECAR_NAME).unlink()
    (bad.intake_dir / "data" / "clip.mov").write_bytes(b"corrupt")
    failed_first = verify_destination(bad.intake_dir)
    assert failed_first.verified is False

    result = verify_pending([landing])

    assert set(result.checked) == {clean.intake_dir, bad.intake_dir}
    assert result.failed == (bad.intake_dir,)
    assert json.loads((clean.intake_dir / VERIFY_SIDECAR_NAME).read_text())["stage"] == "full"
    assert json.loads((bad.intake_dir / VERIFY_SIDECAR_NAME).read_text())["stage"] == "failed"


def test_confirmation_is_fail_safe_for_verified_quarantine_and_timeout(tmp_path: Path) -> None:
    verified = tmp_path / "verified"
    quarantined = tmp_path / "quarantined"
    discrepancy = tmp_path / "discrepancy"
    invalid_verified = tmp_path / "invalid-verified"
    timeout = tmp_path / "timeout"
    for path in (verified, quarantined, discrepancy, invalid_verified, timeout):
        path.mkdir()
    (verified / "intake.verified.json").write_text('{"ok": true}', encoding="utf-8")
    (quarantined / "intake.quarantined.json").write_text(
        '{"details": {"missing": ["clip.mov"]}}',
        encoding="utf-8",
    )
    (quarantined / "intake.verified.json").write_text('{"stale": true}', encoding="utf-8")
    (discrepancy / "intake.discrepancy.json").write_text(
        '{"status": "registered"}',
        encoding="utf-8",
    )
    (discrepancy / "intake.verified.json").write_text('{"stale": true}', encoding="utf-8")
    (invalid_verified / "intake.verified.json").write_text("not json", encoding="utf-8")

    assert wait_for_server_confirmation(verified, timeout_seconds=0).release_ok is True
    quarantine = wait_for_server_confirmation(quarantined, timeout_seconds=0)
    assert quarantine.release_ok is False
    assert quarantine.status == "quarantined"
    assert quarantine.detail == {"details": {"missing": ["clip.mov"]}}
    discrepancy_result = wait_for_server_confirmation(discrepancy, timeout_seconds=0)
    assert discrepancy_result.release_ok is False
    assert discrepancy_result.status == "discrepancy"
    assert discrepancy_result.detail == {"status": "registered"}
    bad_verified = wait_for_server_confirmation(invalid_verified, timeout_seconds=0)
    assert bad_verified.release_ok is False
    assert bad_verified.status == "pending"
    deadline = wait_for_server_confirmation(timeout, timeout_seconds=0)
    assert deadline.release_ok is False
    assert deadline.status == "timeout"


def test_receive_then_intake_register_accepts_nfd_source_name(
    engine: Engine,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "Cafe\u0301.mov").write_bytes(b"video")

    result = receive_source(source, landing=landing, source_kind="card", operator="Op/../Name")

    with session_scope(engine) as session:
        outcomes = register_landing_root(session, landing, cache_root=tmp_path / "cache")

    assert outcomes[0].status == IntakeStatus.REGISTERED.value
    assert re.fullmatch(r"\d{8}-op-name-[0-9a-f]{32}", result.intake_id)
    assert not (result.intake_dir / "intake.quarantined.json").exists()
    with session_scope(engine) as session:
        item = session.scalars(select(IngestItem)).one()
        assert item.as_received_path == "Café.mov"


def test_intake_quarantines_if_tagmanifest_catches_manifest_tamper(
    engine: Engine,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    result = receive_source(source, landing=landing, source_kind="card", operator="op")
    result.manifest_path.write_text(
        result.manifest_path.read_text(encoding="utf-8").replace(
            hashlib.sha256(b"video").hexdigest(),
            "0" * 64,
        ),
        encoding="utf-8",
    )

    with session_scope(engine) as session:
        outcomes = register_landing_root(session, landing)

    assert outcomes[0].status == IntakeStatus.QUARANTINED.value
    assert outcomes[0].reason == "bag-invalid"
    assert outcomes[0].details["tag_mismatched"][0]["path"] == "manifest-sha256.txt"


def test_intake_quarantines_unsupported_receive_package_version(
    engine: Engine,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    result = receive_source(source, landing=landing, source_kind="card", operator="op")
    metadata = read_bag_info(result.bag_info_path)
    metadata["Receive-Package"] = "sutradhara-receive/999.0.0"
    write_bagit_files(
        result.intake_dir,
        entries=read_manifest_sha256(result.manifest_path),
        metadata=metadata,
    )

    validation = validate_bag(result.intake_dir)
    assert validation.complete is True
    assert validation.valid is False
    assert validation.details()["errors"] == [
        f"Receive-Package mismatch: expected {RECEIVE_PACKAGE}, actual 'sutradhara-receive/999.0.0'"
    ]

    with session_scope(engine) as session:
        outcomes = register_landing_root(session, landing)

    assert outcomes[0].status == IntakeStatus.QUARANTINED.value
    assert outcomes[0].reason == "bag-invalid"
    assert outcomes[0].details["errors"] == validation.details()["errors"]


def test_missing_receive_package_label_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    result = receive_source(source, landing=landing, source_kind="card", operator="op")
    metadata = read_bag_info(result.bag_info_path)
    assert metadata.pop("Receive-Package") == RECEIVE_PACKAGE
    write_bagit_files(
        result.intake_dir,
        entries=read_manifest_sha256(result.manifest_path),
        metadata=metadata,
    )

    validation = validate_bag(result.intake_dir)

    assert validation.complete is True
    assert validation.valid is False
    assert validation.details()["errors"] == [
        f"Receive-Package mismatch: expected {RECEIVE_PACKAGE}, actual None"
    ]


def test_validate_bag_rejects_unsafe_tagmanifest_path(tmp_path: Path) -> None:
    bag = tmp_path / "bag"
    data = bag / "data"
    data.mkdir(parents=True)
    (data / "clip.mov").write_bytes(b"video")
    digest = hashlib.sha256(b"video").hexdigest()
    write_bagit_files(
        bag,
        entries={"clip.mov": digest},
        metadata=bag_info_metadata(
            intake_id="bad-tag-path",
            source_kind="card",
            operator="op",
            source_ref=None,
            artifactclass="camera-original",
            label=None,
            started_at=dt.datetime(2026, 6, 18, tzinfo=dt.UTC),
            file_count=1,
            total_bytes=len(b"video"),
            skipped_count=0,
        ),
    )
    (bag / "tagmanifest-sha256.txt").write_text(f"{'0' * 64}  .\n", encoding="utf-8")

    validation = validate_bag(bag)

    assert validation.complete is True
    assert validation.valid is False
    assert validation.tag_mismatched == [
        {"path": "bagit.txt", "expected": "listed", "actual": None},
        {"path": "bag-info.txt", "expected": "listed", "actual": None},
        {"path": "manifest-sha256.txt", "expected": "listed", "actual": None},
        {"path": ".", "expected": "0" * 64, "actual": "unsafe path"},
    ]


def test_intake_quarantines_bag_payload_symlink_without_following(
    engine: Engine,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    outside = tmp_path / "outside.txt"
    source.mkdir()
    outside.write_text("outside", encoding="utf-8")
    (source / "clip.mov").write_bytes(b"video")
    result = receive_source(source, landing=landing, source_kind="card", operator="op")
    try:
        (result.intake_dir / "data" / "link.txt").symlink_to(outside)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    with session_scope(engine) as session:
        outcomes = register_landing_root(session, landing)

    assert outcomes[0].status == IntakeStatus.QUARANTINED.value
    assert outcomes[0].reason == "bag-incomplete"
    assert any("unsupported symlink" in item for item in outcomes[0].details["errors"])
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 0


def test_intake_distinguishes_incomplete_missing_extra_and_invalid_payload(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    missing = _received_fixture(tmp_path, landing, "missing")
    (missing / "data" / "clip.mov").unlink()
    extra = _received_fixture(tmp_path, landing, "extra")
    (extra / "data" / "extra.mov").write_bytes(b"extra")
    invalid = _received_fixture(tmp_path, landing, "invalid")
    (invalid / "data" / "clip.mov").write_bytes(b"corrupt")

    with session_scope(engine) as session:
        outcomes = register_landing_root(session, landing)

    by_id = {row.intake_id: row for row in outcomes}
    assert by_id[missing.name].reason == "bag-incomplete"
    assert by_id[missing.name].details["missing"] == ["clip.mov"]
    assert by_id[extra.name].reason == "bag-incomplete"
    assert by_id[extra.name].details["extra"] == ["extra.mov"]
    assert by_id[invalid.name].reason == "bag-invalid"
    assert by_id[invalid.name].details["mismatched"][0]["path"] == "clip.mov"
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 0


def test_cli_receive_fake_source_and_confirm_timeout(tmp_path: Path) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")

    result = CliRunner().invoke(
        cli,
        [
            "receive",
            "--fake-source",
            str(source),
            "--landing",
            str(landing),
            "--source-kind",
            "card",
            "--operator",
            "Op",
            "--confirm-timeout",
            "0",
            "--json",
        ],
    )

    assert result.exit_code == 3
    payload = json.loads(result.stdout)
    assert "CARD SAFE TO REMOVE — deep verify continuing" in result.stderr
    assert payload["bag_profile"] == BAG_PROFILE
    assert payload["file_count"] == 1
    assert payload["confirmation"]["status"] == "timeout"
    assert payload["confirmation"]["release_ok"] is False


def test_standalone_receive_cli_fake_source_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")

    exit_code = receive_cli_main(
        [
            "--fake-source",
            str(source),
            "--landing",
            str(landing),
            "--source-kind",
            "card",
            "--operator",
            "Op",
            "--json",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert captured.err == "CARD SAFE TO REMOVE — deep verify continuing\n"
    assert payload["bag_profile"] == BAG_PROFILE
    assert payload["file_count"] == 1
    assert payload["total_bytes"] == len(b"video")
    assert validate_bag(Path(payload["intake_dir"])).valid is True


def test_standalone_receive_cli_confirm_timeout_exits_3(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")

    exit_code = receive_cli_main(
        [
            "--fake-source",
            str(source),
            "--landing",
            str(landing),
            "--source-kind",
            "card",
            "--operator",
            "Op",
            "--confirm-timeout",
            "0",
            "--json",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 3
    assert captured.err == "CARD SAFE TO REMOVE — deep verify continuing\n"
    assert payload["confirmation"]["status"] == "timeout"
    assert payload["confirmation"]["release_ok"] is False


def test_standalone_receive_cli_rejects_source_and_fake_source(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "source"
    fake = tmp_path / "fake"
    landing = tmp_path / "landing"
    source.mkdir()
    fake.mkdir()

    exit_code = receive_cli_main(
        [
            str(source),
            "--fake-source",
            str(fake),
            "--landing",
            str(landing),
            "--source-kind",
            "card",
            "--operator",
            "Op",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert "pass either SOURCE or --fake-source" in captured.err


def test_standalone_receive_cli_sweep_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    landing = tmp_path / "landing"
    stale = landing / "stale"
    fresh = landing / "fresh"
    complete = landing / "complete"
    for path in (stale, fresh, complete):
        path.mkdir(parents=True)
        (path / ".receiving.json").write_text("{}", encoding="utf-8")
    (complete / "intake.json").write_text("{}", encoding="utf-8")
    old = dt.datetime.now().timestamp() - 48 * 3600
    os.utime(stale / ".receiving.json", (old, old))

    exit_code = receive_cli_main(
        [
            "sweep",
            "--landing",
            str(landing),
            "--older-than-hours",
            "24",
            "--json",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert captured.err == ""
    assert payload == {"removed": [str(stale)]}
    assert not stale.exists()
    assert fresh.exists()
    assert complete.exists()


def test_standalone_receive_cli_verify_pending_exit_4(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    result = receive_source(source, landing=landing, source_kind="card", operator="op")
    (result.intake_dir / "data" / "clip.mov").write_bytes(b"corrupt")

    exit_code = receive_cli_main(["verify-pending", "--landing", str(landing), "--json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 4
    assert payload == {"checked": [str(result.intake_dir)], "failed": [str(result.intake_dir)]}
    assert f"destination verification failed: {result.intake_dir}" in captured.err


@pytest.mark.parametrize("source_kind", ["card", "drive", "upload"])
def test_receive_source_kind_is_carried_to_bag_info(source_kind: str, tmp_path: Path) -> None:
    source = tmp_path / source_kind
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "file.bin").write_bytes(source_kind.encode())

    result = receive_source(source, landing=landing, source_kind=source_kind, operator="op")

    assert read_bag_info(result.bag_info_path)["Source-Kind"] == source_kind


def test_manifest_mismatch_uses_bagit_missing_extra_labels() -> None:
    digest = hashlib.sha256(b"video").hexdigest()

    assert receive_core.manifest_mismatch({"Café.mov": digest}, {"Cafe\u0301.mov": digest}) == {}
    assert receive_core.manifest_mismatch({"extra.mov": digest}, {})["extra"] == ["extra.mov"]
    assert receive_core.manifest_mismatch({}, {"missing.mov": digest})["missing"] == ["missing.mov"]


def _received_fixture(tmp_path: Path, landing: Path, name: str) -> Path:
    source = tmp_path / f"source-{name}"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    result = receive_source(
        source,
        landing=landing,
        source_kind="card",
        operator="op",
        now=dt.datetime(2026, 6, 18, tzinfo=dt.UTC),
    )
    fixed = landing / name
    result.intake_dir.rename(fixed)
    bag_info = read_bag_info(fixed / "bag-info.txt")
    bag_info["Intake-Id"] = name
    write_bagit_files(
        fixed,
        entries=read_manifest_sha256(fixed / "manifest-sha256.txt"),
        metadata=bag_info,
    )
    (fixed / "intake.json").write_text(
        json.dumps(
            {
                "bag_profile": BAG_PROFILE,
                "created_at": "2026-06-18T00:00:00+00:00",
                "intake_id": name,
                "status": "complete",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return fixed


class _RecordingObserver(AtomicWriteObserver):
    def __init__(self) -> None:
        self.destinations: list[Path] = []
        self.intake_checked = False

    def before_rename(self, temp_path: Path, final_path: Path) -> None:
        assert temp_path.exists()
        assert not final_path.exists()
        if final_path.name == "intake.json":
            assert (final_path.parent / "tagmanifest-sha256.txt").exists()
            self.intake_checked = True
        self.destinations.append(final_path)
