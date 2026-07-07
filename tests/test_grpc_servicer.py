"""Servicer and in-process gRPC flow tests for streaming intake."""

from __future__ import annotations

import datetime as dt
import hashlib
import os
from collections.abc import Iterator
from pathlib import Path

import grpc
import pytest
from sqlalchemy import Engine

from sutradhara._proto import intake_pb2
from sutradhara.catalog.models import Intake
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.grpc import store
from sutradhara.grpc.assembly import manifest_digest
from sutradhara.grpc.server import sweep_landing_once, validate_bind_address
from sutradhara.grpc.servicer import GrpcIntakeConfig, IntakeServicer
from sutradhara.intake_watch import process_landing_once


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_servicer_start_upload_commit_watch_and_owner_check(engine: Engine, tmp_path: Path) -> None:
    fingerprint = "AA" * 32
    other_fingerprint = "BB" * 32
    with session_scope(engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint=fingerprint,
            operator="ada",
        )
        store.record_device_enrollment(
            session,
            device_id="mac-2",
            cert_fingerprint=other_fingerprint,
            operator="other",
        )
    landing = tmp_path / "landing"
    servicer = IntakeServicer(
        GrpcIntakeConfig(engine=engine, landing_root=landing, validate_artifactclass=False)
    )
    context = _FakeContext("mac-1", fingerprint)

    start = servicer.StartIntake(
        intake_pb2.StartIntakeRequest(
            idempotency_key="key-1",
            artifactclass="video-master",
            source_kind="card",
            source_ref="card-a",
            label="Card A",
            source_plan_digest="a" * 64,
        ),
        context,
    )
    same = servicer.StartIntake(
        intake_pb2.StartIntakeRequest(
            idempotency_key="key-1",
            artifactclass="video-master",
            source_kind="card",
            source_ref="card-a",
            label="Card A",
            source_plan_digest="a" * 64,
        ),
        context,
    )
    assert same.intake_id == start.intake_id
    with pytest.raises(_Abort) as conflict:
        servicer.StartIntake(
            intake_pb2.StartIntakeRequest(
                idempotency_key="key-1",
                artifactclass="other-class",
                source_kind="card",
                source_plan_digest="a" * 64,
            ),
            context,
    )
    assert conflict.value.code == grpc.StatusCode.FAILED_PRECONDITION

    with pytest.raises(_Abort) as changed_source:
        servicer.StartIntake(
            intake_pb2.StartIntakeRequest(
                idempotency_key="key-1",
                artifactclass="video-master",
                source_kind="card",
                source_ref="card-a",
                label="Card A",
                source_plan_digest="b" * 64,
            ),
            context,
        )
    assert changed_source.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "source changed" in changed_source.value.details

    payload = b"video"
    receipt = servicer.UploadFile(
        iter(
            [
                intake_pb2.FileChunk(
                    intake_id=start.intake_id,
                    relpath="clip.mov",
                    data=payload,
                    offset=0,
                    is_last=False,
                    file_size=len(payload),
                ),
                intake_pb2.FileChunk(
                    intake_id=start.intake_id,
                    relpath="clip.mov",
                    offset=len(payload),
                    is_last=True,
                ),
            ]
        ),
        context,
    )
    assert receipt.server_sha256 == hashlib.sha256(payload).hexdigest()
    leftover = landing / start.intake_id / ".incoming" / "crash.tmp"
    leftover.write_bytes(b"partial")
    listed = servicer.ListIntakeFiles(
        intake_pb2.ListIntakeFilesRequest(intake_id=start.intake_id),
        context,
    )
    assert [item.relpath for item in listed.files] == ["clip.mov"]
    assert not leftover.exists()
    with pytest.raises(_Abort) as owner_error:
        servicer.ListIntakeFiles(
            intake_pb2.ListIntakeFilesRequest(intake_id=start.intake_id),
            _FakeContext("mac-2", other_fingerprint),
        )
    assert owner_error.value.code == grpc.StatusCode.PERMISSION_DENIED

    files = [
        intake_pb2.ManifestEntry(
            relpath="clip.mov",
            client_sha256=receipt.server_sha256,
            bytes=len(payload),
        )
    ]
    commit = servicer.CommitIntake(
        intake_pb2.CommitIntakeRequest(
            intake_id=start.intake_id,
            files=files,
            receive_facts=intake_pb2.ReceiveFacts(
                canonicalization_version="receive-bagit-path-v2",
                skipped_count=0,
                package_profile_version="",
            ),
            manifest_digest=manifest_digest(files),
        ),
        context,
    )
    assert commit.status == "verifying"
    assert not (landing / start.intake_id / ".receiving.json").exists()
    assert servicer.GetIntakeStatus(
        intake_pb2.IntakeStatusRequest(intake_id=start.intake_id),
        context,
    ).status == "verifying"

    events = process_landing_once(
        landing,
        engine=engine,
        settle_seconds=0,
        stable_polls=1,
        cache_root=tmp_path / "cache",
        use_lock=False,
    )
    assert [event.event for event in events] == ["intake-registered"]
    assert (landing / start.intake_id / "intake.verified.json").is_file()
    with session_scope(engine) as session:
        assert session.get(Intake, start.intake_id) is not None


def test_servicer_rejects_leading_data_relpath_and_abort_after_commit(
    engine: Engine,
    tmp_path: Path,
) -> None:
    with session_scope(engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
    servicer = IntakeServicer(
        GrpcIntakeConfig(engine=engine, landing_root=tmp_path / "landing", validate_artifactclass=False)
    )
    context = _FakeContext("mac-1", "AA" * 32)
    start = servicer.StartIntake(
        intake_pb2.StartIntakeRequest(
            idempotency_key="key-2",
            artifactclass="video-master",
            source_kind="card",
            source_plan_digest="a" * 64,
        ),
        context,
    )
    with pytest.raises(_Abort) as rejected:
        servicer.UploadFile(
            iter(
                [
                    intake_pb2.FileChunk(
                        intake_id=start.intake_id,
                        relpath="data/clip.mov",
                        is_last=True,
                    )
                ]
            ),
            context,
        )
    assert rejected.value.code == grpc.StatusCode.INVALID_ARGUMENT


def test_bind_validation_rejects_wildcard_and_public() -> None:
    validate_bind_address("127.0.0.1")
    validate_bind_address("100.81.52.26")
    with pytest.raises(ValueError, match="wildcard"):
        validate_bind_address("0.0.0.0")
    with pytest.raises(ValueError, match="loopback, LAN, or Tailscale"):
        validate_bind_address("8.8.8.8")


def test_server_sweep_removes_stale_receiving_and_incoming(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    stale = landing / "stale"
    active = landing / "active"
    stale_incoming = active / ".incoming"
    stale.mkdir(parents=True)
    stale_incoming.mkdir(parents=True)
    receiving = stale / ".receiving.json"
    receiving.write_text("{}", encoding="utf-8")
    temp = stale_incoming / "old.tmp"
    temp.write_bytes(b"partial")
    old_time = (dt.datetime.now(dt.UTC) - dt.timedelta(hours=25)).timestamp()
    os.utime(receiving, (old_time, old_time))
    os.utime(temp, (old_time, old_time))

    sweep_landing_once(landing)

    assert not stale.exists()
    assert stale_incoming.is_dir()
    assert not temp.exists()


class _Abort(Exception):
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        super().__init__(details)
        self.code = code
        self.details = details


class _FakeContext:
    def __init__(self, device_id: str, fingerprint: str) -> None:
        self.device_id = device_id
        self.fingerprint = fingerprint

    def abort(self, code: grpc.StatusCode, details: str) -> None:
        raise _Abort(code, details)
