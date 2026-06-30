"""Bag assembly tests for streaming gRPC intake."""

from __future__ import annotations

import datetime as dt
import hashlib
from types import SimpleNamespace

from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.grpc.assembly import assemble_committed_bag
from sutradhara.intake import inspect_intake
from sutradhara_receive import (
    CANONICALIZATION_VERSION,
    PACKAGE_INDEX_NAME,
    PACKAGE_PROFILE_VERSION,
    plan_payload_units,
    read_bag_info,
    read_package_index,
    validate_bag,
)


def test_assembly_writes_valid_non_package_bag_from_stored_intent(tmp_path) -> None:
    intake_dir = tmp_path / "landing" / "intake-1"
    data = intake_dir / "data"
    data.mkdir(parents=True)
    payload = b"video"
    (data / "clip.mov").write_bytes(payload)
    row = _row(tmp_path, intake_dir)
    files = [_entry("clip.mov", hashlib.sha256(payload).hexdigest(), len(payload))]

    assemble_committed_bag(
        intake_dir,
        row=row,
        files=files,
        receive_facts=_facts(skipped_count=0, package_profile_version=""),
        package_indexes=[],
    )

    validation = validate_bag(intake_dir)
    assert validation.valid
    metadata = read_bag_info(intake_dir / "bag-info.txt")
    assert metadata["Operator"] == "owner"
    assert metadata["Artifactclass"] == "video-master"
    assert metadata["Package-Profile-Version"] == PACKAGE_PROFILE_VERSION
    assert metadata["Canonicalization-Version"] == CANONICALIZATION_VERSION

    engine = make_engine("sqlite:///:memory:")
    create_all(engine)
    try:
        with session_scope(engine) as session:
            report = inspect_intake(session, intake_dir)
        assert report.status == "ready"
    finally:
        engine.dispose()


def test_assembly_writes_single_package_index_with_null_non_file_members(tmp_path) -> None:
    source = tmp_path / "source"
    package = source / "A.fcpbundle"
    (package / "Media").mkdir(parents=True)
    (package / "Media" / "clip.mov").write_bytes(b"clip")
    plan = plan_payload_units(source)
    unit = plan.units[0]
    tar_bytes = b"".join(unit.byte_chunks(1024))
    tar_sha = hashlib.sha256(tar_bytes).hexdigest()
    package_index = unit.package_index(tar_sha)

    intake_dir = tmp_path / "landing" / "intake-1"
    data = intake_dir / "data"
    data.mkdir(parents=True)
    (data / unit.relpath).write_bytes(tar_bytes)
    row = _row(tmp_path, intake_dir)

    assemble_committed_bag(
        intake_dir,
        row=row,
        files=[_entry(unit.relpath, tar_sha, len(tar_bytes))],
        receive_facts=_facts(
            skipped_count=0,
            package_profile_version=PACKAGE_PROFILE_VERSION,
        ),
        package_indexes=[_package_proto(package_index)],
    )

    index = read_package_index(intake_dir / PACKAGE_INDEX_NAME)
    assert len(index["packages"]) == 1
    directory = next(member for member in index["packages"][0]["members"] if member["type"] == "directory")
    assert directory["sha256"] is None
    assert directory["data_offset"] is None
    assert validate_bag(intake_dir).valid


def _row(tmp_path, intake_dir):
    return SimpleNamespace(
        intake_id=intake_dir.name,
        operator="owner",
        device_id="mac-1",
        state="streaming",
        manifest_digest=None,
        idempotency_key="key",
        source_plan_digest="a" * 64,
        artifactclass="video-master",
        source_kind="card",
        source_ref="card-a",
        label="Card A",
        landing_root=str(tmp_path / "landing"),
        created_at=dt.datetime.now(dt.UTC),
    )


def _entry(relpath: str, digest: str, size: int):
    return SimpleNamespace(relpath=relpath, client_sha256=digest, bytes=size)


def _facts(*, skipped_count: int, package_profile_version: str):
    return SimpleNamespace(
        canonicalization_version=CANONICALIZATION_VERSION,
        skipped_count=skipped_count,
        package_profile_version=package_profile_version,
    )


def _package_proto(payload):
    return SimpleNamespace(
        logical_member_path=payload["logical_member_path"],
        stored_member_path=payload["stored_member_path"],
        sha256=payload["sha256"],
        members=[SimpleNamespace(**member) for member in payload["members"]],
    )
