"""Front-door receive filesystem contract tests."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from sqlalchemy import Engine, select

from sutradhara.catalog.models import IngestItem
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import IntakeStatus
from sutradhara.cli.main import cli
from sutradhara.intake import scan_landing_root
from sutradhara.receive import (
    CANONICALIZATION_VERSION,
    RECEIVE_VERSION,
    AtomicWriteObserver,
    CollisionError,
    DestinationVerificationError,
    ReceiveError,
    SourceMutationError,
    manifest_mismatch,
    read_manifest_sha256,
    receive_source,
    safe_payload_path,
    sha256_file,
    sweep_orphans,
    wait_for_server_confirmation,
    write_mhl_manifest,
)
from sutradhara.receive import core as receive_core


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_mhl_writer_round_trips_to_shared_reader(tmp_path: Path) -> None:
    digest = hashlib.sha256(b"hello").hexdigest()
    manifest = tmp_path / "manifest.mhl"

    manifest_digest = write_mhl_manifest(manifest, {"clip.mov": digest})

    assert manifest.read_text(encoding="utf-8") == (
        "<hashlist>\n"
        "  <hash>\n"
        "    <file>payload/clip.mov</file>\n"
        f"    <sha256>{digest}</sha256>\n"
        "  </hash>\n"
        "</hashlist>\n"
    )
    assert sha256_file(manifest) == manifest_digest
    assert read_manifest_sha256(manifest) == {"clip.mov": digest}


def test_receive_writes_sentinel_last_and_manifest_digest(tmp_path: Path) -> None:
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
    assert sentinel["receive_version"] == RECEIVE_VERSION
    assert sentinel["canonicalization_version"] == CANONICALIZATION_VERSION
    assert sentinel["manifest_sha256"] == sha256_file(result.manifest_path)
    assert sentinel["file_count"] == 1
    assert sentinel["total_bytes"] == len(b"video")
    assert not (result.intake_dir / ".receiving.json").exists()
    assert observer.intake_checked is True
    assert observer.destinations[-1].name == "intake.json"
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

    assert not list(landing.glob("*/payload"))
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
    assert (result.intake_dir / "payload" / escaped).read_bytes() == b"legacy"
    assert read_manifest_sha256(result.manifest_path) == {
        escaped: hashlib.sha256(b"legacy").hexdigest()
    }


def test_receive_detects_corrupt_landed_destination_before_sentinel(tmp_path: Path) -> None:
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
            after_copy_hook=corrupt_payload,
        )

    failed = next(landing.iterdir())
    assert not (failed / "intake.json").exists()
    assert (failed / ".receiving.json").exists()


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

    sentinel = json.loads(result.sentinel_path.read_text(encoding="utf-8"))
    log = (result.intake_dir / "receive.log").read_text(encoding="utf-8")
    assert sentinel["skipped_count"] == 2
    assert "link.mov: symlink" in log
    assert "pipe: fifo" in log


def test_payload_path_and_source_relationship_guards(tmp_path: Path) -> None:
    with pytest.raises(ReceiveError):
        safe_payload_path(tmp_path / "payload", "../escape.mov")
    with pytest.raises(ReceiveError):
        safe_payload_path(tmp_path / "payload", "/absolute.mov")

    source = tmp_path / "source"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    with pytest.raises(ReceiveError):
        receive_source(source, landing=source / "landing", source_kind="card", operator="op")

    existing = tmp_path / "landing" / "done"
    payload = existing / "payload"
    payload.mkdir(parents=True)
    (existing / "intake.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ReceiveError):
        receive_source(payload, landing=tmp_path / "landing", source_kind="card", operator="op")


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
    failed_payload = landing / failed_id / "payload"
    (failed_payload / "a.mov").write_bytes(b"bad")

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
    assert (failed_payload / "a.mov").read_bytes() == b"a"
    assert not (landing / failed_id / ".receiving.json").exists()
    assert (landing / failed_id / "intake.json").exists()


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


def test_confirmation_is_fail_safe_for_verified_quarantine_and_timeout(tmp_path: Path) -> None:
    verified = tmp_path / "verified"
    quarantined = tmp_path / "quarantined"
    timeout = tmp_path / "timeout"
    for path in (verified, quarantined, timeout):
        path.mkdir()
    (verified / "intake.verified.json").write_text('{"ok": true}', encoding="utf-8")
    (quarantined / "intake.quarantined.json").write_text(
        '{"details": {"missing": ["clip.mov"]}}',
        encoding="utf-8",
    )

    assert wait_for_server_confirmation(verified, timeout_seconds=0).release_ok is True
    quarantine = wait_for_server_confirmation(quarantined, timeout_seconds=0)
    assert quarantine.release_ok is False
    assert quarantine.status == "quarantined"
    assert quarantine.detail == {"details": {"missing": ["clip.mov"]}}
    deadline = wait_for_server_confirmation(timeout, timeout_seconds=0)
    assert deadline.release_ok is False
    assert deadline.status == "timeout"


def test_receive_then_intake_scan_accepts_nfd_source_name(engine: Engine, tmp_path: Path) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "Cafe\u0301.mov").write_bytes(b"video")

    result = receive_source(source, landing=landing, source_kind="card", operator="Op/../Name")

    with session_scope(engine) as session:
        outcomes = scan_landing_root(session, landing, cache_root=tmp_path / "cache")

    assert outcomes[0].status == IntakeStatus.REGISTERED.value
    assert re.fullmatch(r"\d{8}-op-name-[0-9a-f]{32}", result.intake_id)
    assert not (result.intake_dir / "intake.quarantined.json").exists()
    with session_scope(engine) as session:
        item = session.scalars(select(IngestItem)).one()
        assert item.as_received_path == "Café.mov"


def test_intake_quarantines_if_manifest_digest_changes(engine: Engine, tmp_path: Path) -> None:
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
        outcomes = scan_landing_root(session, landing)

    assert outcomes[0].status == IntakeStatus.QUARANTINED.value
    assert outcomes[0].reason == "manifest-sha256-mismatch"


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
    payload = json.loads(result.output)
    assert payload["file_count"] == 1
    assert payload["confirmation"]["status"] == "timeout"
    assert payload["confirmation"]["release_ok"] is False


@pytest.mark.parametrize("source_kind", ["card", "drive", "upload"])
def test_receive_source_kind_is_carried_to_sentinel(source_kind: str, tmp_path: Path) -> None:
    source = tmp_path / source_kind
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "file.bin").write_bytes(source_kind.encode())

    result = receive_source(source, landing=landing, source_kind=source_kind, operator="op")

    sentinel = json.loads(result.sentinel_path.read_text(encoding="utf-8"))
    assert sentinel["source_kind"] == source_kind


def test_manifest_mismatch_normalizes_both_sides() -> None:
    digest = hashlib.sha256(b"video").hexdigest()

    assert manifest_mismatch({"Café.mov": digest}, {"Cafe\u0301.mov": digest}) == {}


class _RecordingObserver(AtomicWriteObserver):
    def __init__(self) -> None:
        self.destinations: list[Path] = []
        self.intake_checked = False

    def before_rename(self, temp_path: Path, final_path: Path) -> None:
        assert temp_path.exists()
        assert not final_path.exists()
        if final_path.name == "intake.json":
            assert (final_path.parent / "manifest.mhl").exists()
            self.intake_checked = True
        self.destinations.append(final_path)
