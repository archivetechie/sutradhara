"""P1.1 intake inspect/register/prepare lifecycle tests."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, func, select

import sutradhara.intake as intake_module
from sutradhara.catalog.models import ArtifactClass, IngestItem, Intake, LogicalAsset
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import IntakeStatus
from sutradhara.intake import (
    IntakeDiscrepancyError,
    IntakeMarker,
    accept_intake,
    inspect_intake,
    prepare_intake,
    publish_intake_marker,
    register_intake,
    register_landing_root,
)
from sutradhara.jobs.models import Job, JobStatus
from sutradhara_receive import (
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
    with session_scope(eng) as session:
        session.add_all(
            ArtifactClass(name=name) for name in ("camera-original", "other-class", "video-master")
        )
    yield eng
    eng.dispose()


def test_inspect_creates_no_rows(engine: Engine, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    valid = _write_intake(
        landing,
        "card-001",
        source_kind="card",
        files={"clip.mov": b"video bytes"},
        manifest=True,
    )
    invalid = _write_intake(
        landing,
        "card-002",
        source_kind="card",
        files={"clip.mov": b"video bytes"},
        manifest=True,
        corrupt_manifest=True,
    )

    with session_scope(engine) as session:
        ready = inspect_intake(session, valid.parents[1])
        bad = inspect_intake(session, invalid.parents[1])

    assert ready.status == "ready"
    assert ready.item_count == 1
    assert ready.manifest_digest is not None
    assert bad.status == "invalid"
    assert bad.reason == "bag-invalid"
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(Intake)) == 0
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 0
        assert session.scalar(select(func.count()).select_from(LogicalAsset)) == 0
        assert session.scalar(select(func.count()).select_from(Job)) == 0


def test_publish_intake_marker_is_atomic(tmp_path: Path) -> None:
    marker = IntakeMarker(tmp_path / "intake.verified.json", {"ok": True})
    observed: list[Path] = []

    class Observer:
        def before_rename(self, temp_path: Path, final_path: Path) -> None:
            assert temp_path.exists()
            assert not final_path.exists()
            observed.append(temp_path)

    publish_intake_marker(marker, observer=Observer())

    assert observed
    assert marker.path.is_file()
    assert json.loads(marker.path.read_text(encoding="utf-8")) == {"ok": True}
    assert not any(path.exists() for path in observed)


def test_register_good_manifest_creates_catalog_and_cloud_only(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    clip = _write_intake(
        landing,
        "card-001",
        source_kind="card",
        files={"clip.mov": b"video bytes", "notes.txt": b"slate notes"},
        manifest=True,
    )

    with session_scope(engine) as session:
        outcome = register_intake(session, clip.parents[1], cache_root=tmp_path / "cache")

    assert outcome.status == IntakeStatus.REGISTERED.value
    assert outcome.item_count == 2
    assert outcome.jobs_submitted == 1
    assert outcome.marker is not None
    assert not (landing / "card-001" / "intake.verified.json").exists()
    publish_intake_marker(outcome.marker)
    assert (landing / "card-001" / "intake.verified.json").exists()
    receipt = json.loads((landing / "card-001" / "intake.verified.json").read_text())
    assert receipt["release_signal"] is True
    assert receipt["manifest_digest"] == outcome.manifest_digest

    with session_scope(engine) as session:
        intake = session.get(Intake, "card-001")
        assert intake is not None
        assert intake.status == IntakeStatus.REGISTERED
        assert intake.manifest_digest == outcome.manifest_digest
        assert intake.requested_profile is None
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 2
        assert session.scalar(select(func.count()).select_from(LogicalAsset)) == 2
        jobs = list(session.scalars(select(Job).order_by(Job.kind)))
        assert [job.kind for job in jobs] == ["cloud-blob"]
        assert {job.dedupe_key for job in jobs} == {f"cloud-blob:{intake.intake_id}"}

    assert clip.exists()


def test_register_receive_package_registers_one_logical_item(
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
        outcome = register_intake(session, result.intake_dir, cache_root=tmp_path / "cache")

    assert outcome.status == IntakeStatus.REGISTERED.value
    assert outcome.item_count == 1
    assert outcome.jobs_submitted == 1
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
        assert item.source_path == str(result.intake_dir / "data" / "A001.fcpbundle.tar")
        assert "source_path" not in item.item_metadata
        assert "sha256" not in item.item_metadata
        assert asset.media_info is not None
        assert asset.media_info["path"] == "A001.fcpbundle"
        assert asset.media_info["stored_member_path"] == "A001.fcpbundle.tar"
        jobs = list(session.scalars(select(Job)))
        assert [job.kind for job in jobs] == ["cloud-blob"]


def test_register_bad_manifest_quarantines_without_registration(
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
        outcome = register_intake(session, landing / "card-002")

    assert outcome.status == IntakeStatus.QUARANTINED.value
    assert outcome.reason == "bag-invalid"
    publish_intake_marker(outcome.marker)
    assert (landing / "card-002" / "intake.quarantined.json").exists()
    with session_scope(engine) as session:
        intake = session.get(Intake, "card-002")
        assert intake is not None
        assert intake.status == IntakeStatus.QUARANTINED
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 0
        assert session.scalar(select(func.count()).select_from(LogicalAsset)) == 0
        assert session.scalar(select(func.count()).select_from(Job)) == 0


def test_register_quarantines_unnormalized_native_package_directory(
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
        outcome = register_intake(session, landing / "card-native-package")

    assert outcome.status == IntakeStatus.QUARANTINED.value
    assert outcome.reason == "bag-incomplete"
    assert any("un-normalized package directory" in item for item in outcome.details["errors"])
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 0
        assert session.scalar(select(func.count()).select_from(LogicalAsset)) == 0
        assert session.scalar(select(func.count()).select_from(Job)) == 0


def test_register_legacy_payload_requires_explicit_artifactclass(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    _write_intake(
        landing,
        "upload-001",
        source_kind="upload",
        files={"raw.bin": b"opaque upload"},
        manifest=False,
    )

    with (
        pytest.raises(ValueError, match="artifactclass is required"),
        session_scope(engine) as session,
    ):
        register_intake(session, landing / "upload-001")

    with session_scope(engine) as session:
        outcome = register_intake(
            session,
            landing / "upload-001",
            artifactclass="video-master",
        )

    assert outcome.status == IntakeStatus.REGISTERED.value
    assert outcome.jobs_submitted == 1
    publish_intake_marker(outcome.marker)
    receipt = json.loads((landing / "upload-001" / "intake.verified.json").read_text())
    assert receipt["release_signal"] is False
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 1
        jobs = list(session.scalars(select(Job)))
        assert [job.kind for job in jobs] == ["cloud-blob"]


def test_register_same_fingerprint_is_catalog_noop_and_cloud_repair(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    _write_intake(
        landing,
        "card-003",
        source_kind="card",
        files={"clip.mov": b"video bytes"},
        manifest=True,
    )

    with session_scope(engine) as session:
        first = register_intake(session, landing / "card-003", cache_root=tmp_path / "cache")
        intake = session.get(Intake, "card-003")
        assert intake is not None
        updated_at = intake.updated_at
    with session_scope(engine) as session:
        job = session.scalars(select(Job).where(Job.kind == "cloud-blob")).one()
        job.status = JobStatus.FAILED
    with session_scope(engine) as session:
        second = register_intake(session, landing / "card-003", cache_root=tmp_path / "cache")
        intake = session.get(Intake, "card-003")
        assert intake is not None
        assert intake.updated_at == updated_at

    assert first.jobs_submitted == 1
    assert second.jobs_submitted == 1
    assert second.reason == "already-registered"
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 1
        assert session.scalar(select(func.count()).select_from(Job)) == 2


def test_registered_changed_payload_rejects_and_preserves_truth(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    clip = _write_intake(
        landing,
        "card-change",
        source_kind="card",
        files={"clip.mov": b"video bytes"},
        manifest=True,
    )
    with session_scope(engine) as session:
        register_intake(session, clip.parents[1])
    clip.write_bytes(b"tampered bytes")

    with (
        pytest.raises(IntakeDiscrepancyError, match="no longer validates") as raised,
        session_scope(engine) as session,
    ):
        register_intake(session, clip.parents[1])

    assert raised.value.marker.payload["status"] == "discrepancy"
    assert raised.value.reason == "registered-intake-invalid"
    assert not (clip.parents[1] / "intake.discrepancy.json").exists()
    publish_intake_marker(raised.value.marker)
    assert (clip.parents[1] / "intake.discrepancy.json").exists()
    with session_scope(engine) as session:
        intake = session.get(Intake, "card-change")
        assert intake is not None
        assert intake.status == IntakeStatus.REGISTERED
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 1
        assert session.scalar(select(func.count()).select_from(LogicalAsset)) == 1


def test_registered_changed_artifactclass_rejects(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    clip = _write_intake(
        landing,
        "card-class",
        source_kind="card",
        files={"clip.mov": b"video bytes"},
        manifest=True,
    )
    with session_scope(engine) as session:
        register_intake(session, clip.parents[1], artifactclass="video-master")

    with (
        pytest.raises(ValueError, match="artifactclass mismatch"),
        session_scope(engine) as session,
    ):
        register_intake(session, clip.parents[1], artifactclass="other-class")

    with session_scope(engine) as session:
        intake = session.get(Intake, "card-class")
        assert intake is not None
        assert intake.status == IntakeStatus.REGISTERED
        assert intake.artifactclass == "video-master"
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 1


def test_register_then_prepare_splits_cloud_from_derivatives(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    _write_intake(
        landing,
        "card-prepare",
        source_kind="card",
        files={"clip.mov": b"video bytes"},
        manifest=True,
    )

    with session_scope(engine) as session:
        registered = register_intake(
            session, landing / "card-prepare", cache_root=tmp_path / "cache"
        )
    with session_scope(engine) as session:
        prepared = prepare_intake(
            session,
            "card-prepare",
            profile="hd-review",
        )

    assert registered.jobs_submitted == 1
    assert prepared.jobs_submitted == 0
    with session_scope(engine) as session:
        intake = session.get(Intake, "card-prepare")
        assert intake is not None
        assert intake.requested_profile == "hd-review"
        jobs = list(session.scalars(select(Job).order_by(Job.kind)))
        assert [job.kind for job in jobs] == ["cloud-blob"]


def test_prepare_requires_registered_and_known_profile(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    _write_intake(
        landing,
        "card-bad",
        source_kind="card",
        files={"clip.mov": b"video bytes"},
        manifest=True,
        corrupt_manifest=True,
    )
    with session_scope(engine) as session:
        register_intake(session, landing / "card-bad")

    with pytest.raises(ValueError, match="register first"), session_scope(engine) as session:
        prepare_intake(
            session,
            "missing",
            profile="hd-review",
        )
    with (
        pytest.raises(ValueError, match="prepare requires registered"),
        session_scope(engine) as session,
    ):
        prepare_intake(
            session,
            "card-bad",
            profile="hd-review",
        )
    with (
        pytest.raises(ValueError, match="unknown prepare profile"),
        session_scope(engine) as session,
    ):
        prepare_intake(
            session,
            "card-bad",
            profile="typo",
        )


def test_prepare_idempotent_and_profile_overwrite(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    _write_intake(
        landing,
        "card-overwrite",
        source_kind="card",
        files={"clip.mov": b"video bytes"},
        manifest=True,
    )
    with session_scope(engine) as session:
        register_intake(session, landing / "card-overwrite", cache_root=tmp_path / "cache")
    with session_scope(engine) as session:
        first = prepare_intake(
            session,
            "card-overwrite",
            profile="hd-review",
        )
    with session_scope(engine) as session:
        second = prepare_intake(
            session,
            "card-overwrite",
            profile="hd-review",
        )
    with session_scope(engine) as session:
        third = prepare_intake(
            session,
            "card-overwrite",
            profile="proxy-review",
        )

    assert first.jobs_submitted == 0
    assert second.jobs_submitted == 0
    assert third.jobs_submitted == 0
    with session_scope(engine) as session:
        intake = session.get(Intake, "card-overwrite")
        assert intake is not None
        assert intake.requested_profile == "proxy-review"
        assert session.scalar(select(func.count()).select_from(Job)) == 1


def test_accept_equals_register_plus_prepare(engine: Engine, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    clip = _write_intake(
        landing,
        "card-accept",
        source_kind="card",
        files={"clip.mov": b"video bytes"},
        manifest=True,
    )

    with session_scope(engine) as session:
        outcome = accept_intake(
            session,
            clip.parents[1],
            prepare_profile="hd-review",
            cache_root=tmp_path / "cache",
        )

    assert outcome.status == IntakeStatus.REGISTERED.value
    assert outcome.jobs_submitted == 1
    assert outcome.marker is not None
    assert outcome.marker.payload["requested_profile"] == "hd-review"
    with session_scope(engine) as session:
        intake = session.get(Intake, "card-accept")
        assert intake is not None
        assert intake.requested_profile == "hd-review"
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 1
        assert session.scalar(select(func.count()).select_from(Job)) == 1


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
        outcomes = register_landing_root(session, landing, artifactclass="video-master")

    assert [row.status for row in outcomes] == [
        IntakeStatus.REGISTERED.value,
        IntakeStatus.REGISTERED.value,
    ]
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(LogicalAsset)) == 1
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 2


def test_scan_symbols_are_gone() -> None:
    assert not hasattr(intake_module, "scan_intake")
    assert not hasattr(intake_module, "scan_landing_root")
    assert not hasattr(intake_module, "IntakeScanOutcome")


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
