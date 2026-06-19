"""Phase R intake scanner tests."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, func, select

from sutradhara.catalog.models import IngestItem, Intake, LogicalAsset
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import IntakeStatus
from sutradhara.intake import scan_landing_root
from sutradhara.jobs.models import Job
from sutradhara.receive import (
    BAG_PROFILE,
    PACKAGE_INDEX_NAME,
    bag_info_metadata,
    receive_source,
    write_bagit_files,
)


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_scan_good_manifest_registers_items_and_jobs(engine: Engine, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    clip = _write_intake(
        landing,
        "card-001",
        source_kind="card",
        files={"clip.mov": b"video bytes", "notes.txt": b"slate notes"},
        manifest=True,
    )

    with session_scope(engine) as session:
        outcomes = scan_landing_root(session, landing, cache_root=tmp_path / "cache")

    assert len(outcomes) == 1
    assert outcomes[0].status == IntakeStatus.REGISTERED.value
    assert outcomes[0].item_count == 2
    assert outcomes[0].jobs_submitted == 3
    assert (landing / "card-001" / "intake.verified.json").exists()
    receipt = json.loads((landing / "card-001" / "intake.verified.json").read_text())
    assert receipt["release_signal"] is True

    with session_scope(engine) as session:
        intake = session.get(Intake, "card-001")
        assert intake is not None
        assert intake.status == IntakeStatus.REGISTERED
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 2
        assert session.scalar(select(func.count()).select_from(LogicalAsset)) == 2
        jobs = list(session.scalars(select(Job).order_by(Job.kind)))
        assert [job.kind for job in jobs] == ["cloud-blob", "pfr-index", "transcode"]
        assert {job.dedupe_key for job in jobs} == {
            f"cloud-blob:{intake.intake_id}",
            "pfr-index:1",
            "transcode:1",
        }

    assert clip.exists()


def test_scan_receive_package_registers_one_logical_item(
    engine: Engine,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    bundle = source / "A001.fcpbundle"
    (bundle / "Event").mkdir(parents=True)
    (bundle / "Event" / "clip.mov").write_bytes(b"video bytes")

    result = receive_source(
        source,
        landing=landing,
        source_kind="card",
        operator="tester",
        artifactclass="camera-original",
    )

    with session_scope(engine) as session:
        outcomes = scan_landing_root(session, landing, cache_root=tmp_path / "cache")

    assert outcomes[0].status == IntakeStatus.REGISTERED.value
    assert outcomes[0].item_count == 1
    assert outcomes[0].jobs_submitted == 1
    with session_scope(engine) as session:
        item = session.scalars(select(IngestItem)).one()
        asset = session.scalars(select(LogicalAsset)).one()
        assert item.as_received_path == "A001.fcpbundle"
        assert item.virtual_path == "A001.fcpbundle"
        assert item.item_metadata["stored_member_path"] == "A001.fcpbundle.tar"
        assert item.item_metadata["logical_member_path"] == "A001.fcpbundle"
        assert item.item_metadata["package_index_path"] == str(
            result.intake_dir / PACKAGE_INDEX_NAME
        )
        assert item.item_metadata["source_path"] == str(
            result.intake_dir / "data" / "A001.fcpbundle.tar"
        )
        assert asset.media_info["path"] == "A001.fcpbundle"
        assert asset.media_info["stored_member_path"] == "A001.fcpbundle.tar"
        jobs = list(session.scalars(select(Job)))
        assert [job.kind for job in jobs] == ["cloud-blob"]


def test_scan_bad_manifest_quarantines_without_registration(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    _write_intake(
        landing,
        "card-002",
        source_kind="card",
        files={"clip.mov": b"video bytes"},
        manifest=True,
        corrupt_manifest=True,
    )

    with session_scope(engine) as session:
        outcomes = scan_landing_root(session, landing)

    assert outcomes[0].status == IntakeStatus.QUARANTINED.value
    assert outcomes[0].reason == "bag-invalid"
    assert (landing / "card-002" / "intake.quarantined.json").exists()
    with session_scope(engine) as session:
        intake = session.get(Intake, "card-002")
        assert intake is not None
        assert intake.status == IntakeStatus.QUARANTINED
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 0
        assert session.scalar(select(func.count()).select_from(LogicalAsset)) == 0
        assert session.scalar(select(func.count()).select_from(Job)) == 0


def test_scan_quarantines_unnormalized_native_package_directory(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    _write_intake(
        landing,
        "card-native-package",
        source_kind="card",
        files={"A001.fcpbundle/Event/clip.mov": b"video bytes"},
        manifest=True,
    )

    with session_scope(engine) as session:
        outcomes = scan_landing_root(session, landing, cache_root=tmp_path / "cache")

    assert outcomes[0].status == IntakeStatus.QUARANTINED.value
    assert outcomes[0].reason == "bag-incomplete"
    assert any("un-normalized package directory" in item for item in outcomes[0].details["errors"])
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 0
        assert session.scalar(select(func.count()).select_from(LogicalAsset)) == 0
        assert session.scalar(select(func.count()).select_from(Job)) == 0


def test_scan_without_manifest_registers_baseline(engine: Engine, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    _write_intake(
        landing,
        "upload-001",
        source_kind="upload",
        files={"raw.bin": b"opaque upload"},
        manifest=False,
    )

    with session_scope(engine) as session:
        outcomes = scan_landing_root(session, landing)

    assert outcomes[0].status == IntakeStatus.REGISTERED.value
    assert outcomes[0].jobs_submitted == 1
    receipt = json.loads((landing / "upload-001" / "intake.verified.json").read_text())
    assert receipt["release_signal"] is False
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 1
        jobs = list(session.scalars(select(Job)))
        assert [job.kind for job in jobs] == ["cloud-blob"]


def test_scan_rescan_is_idempotent_for_live_jobs(engine: Engine, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    _write_intake(
        landing,
        "card-003",
        source_kind="card",
        files={"clip.mov": b"video bytes"},
        manifest=True,
    )

    with session_scope(engine) as session:
        first = scan_landing_root(session, landing)
    with session_scope(engine) as session:
        second = scan_landing_root(session, landing)

    assert first[0].jobs_submitted == 3
    assert second[0].jobs_submitted == 0
    assert second[0].reason == "already-registered"
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 1
        assert session.scalar(select(func.count()).select_from(Job)) == 3


def test_identical_bytes_dedup_to_one_logical_asset(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    payload = b"same content"
    _write_intake(
        landing,
        "upload-a",
        source_kind="upload",
        files={"a.bin": payload},
        manifest=False,
    )
    _write_intake(
        landing,
        "upload-b",
        source_kind="upload",
        files={"b.bin": payload},
        manifest=False,
    )

    with session_scope(engine) as session:
        outcomes = scan_landing_root(session, landing)

    assert [row.status for row in outcomes] == [
        IntakeStatus.REGISTERED.value,
        IntakeStatus.REGISTERED.value,
    ]
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(LogicalAsset)) == 1
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 2


def _write_intake(
    landing: Path,
    intake_id: str,
    *,
    source_kind: str,
    files: dict[str, bytes],
    manifest: bool,
    corrupt_manifest: bool = False,
) -> Path:
    root = landing / intake_id
    payload = root / "payload"
    if manifest:
        payload = root / "data"
    payload.mkdir(parents=True)
    first_path: Path | None = None
    entries: dict[str, str] = {}
    for relpath, content in files.items():
        path = payload / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        first_path = first_path or path
        entries[relpath] = hashlib.sha256(content).hexdigest()

    if manifest:
        write_bagit_files(
            root,
            entries=entries,
            metadata=bag_info_metadata(
                intake_id=intake_id,
                source_kind=source_kind,
                operator="tester",
                source_ref=None,
                artifactclass="video-master",
                label=intake_id,
                started_at=dt.datetime(2026, 6, 18, tzinfo=dt.UTC),
                file_count=len(entries),
                total_bytes=sum(len(content) for content in files.values()),
                skipped_count=0,
            ),
        )
        if corrupt_manifest:
            first = next(iter(files))
            (payload / first).write_bytes(b"corrupt payload")

    (root / "intake.json").write_text(
        json.dumps(
            _sentinel_payload(
                intake_id,
                source_kind=source_kind,
                manifest=manifest,
            ),
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    assert first_path is not None
    return first_path


def _sentinel_payload(intake_id: str, *, source_kind: str, manifest: bool) -> dict[str, str]:
    if manifest:
        return {
            "bag_profile": BAG_PROFILE,
            "created_at": "2026-06-18T00:00:00+00:00",
            "intake_id": intake_id,
            "status": "complete",
        }
    return {
        "intake_id": intake_id,
        "operator": "tester",
        "source_kind": source_kind,
        "artifactclass": "video-master",
        "label": intake_id,
    }
