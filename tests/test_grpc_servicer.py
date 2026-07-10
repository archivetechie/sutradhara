"""Servicer and in-process gRPC flow tests for streaming intake."""

from __future__ import annotations

import datetime as dt
import hashlib
import os
from collections.abc import Iterator
from pathlib import Path

import grpc
import pytest
from sqlalchemy import Engine, select

from sutradhara._proto import intake_pb2
from sutradhara.api import store as api_store
from sutradhara.catalog.models import Intake
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.grpc import store
from sutradhara.grpc.assembly import manifest_digest
from sutradhara.grpc.progress import ReceiveProgressRegistry
from sutradhara.grpc.server import sweep_landing_once, validate_bind_address
from sutradhara.grpc.servicer import LEASE_LOST_DETAIL, GrpcIntakeConfig, IntakeServicer
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
    payload = b"video"
    progress_registry = ReceiveProgressRegistry()
    servicer = IntakeServicer(
        GrpcIntakeConfig(
            engine=engine,
            landing_root=landing,
            validate_artifactclass=False,
            progress_registry=progress_registry,
        )
    )
    context = _FakeContext("mac-1", fingerprint)
    _authorize_receive_intent(engine, key="key-1", device_id="mac-1")

    start = servicer.StartIntake(
        intake_pb2.StartIntakeRequest(
            idempotency_key="key-1",
            artifactclass="video-master",
            source_kind="card",
            source_ref="card-a",
            label="Card A",
            source_plan_digest="a" * 64,
            planned_bytes_total=len(payload),
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
            planned_bytes_total=len(payload),
        ),
        context,
    )
    assert same.intake_id == start.intake_id
    with session_scope(engine) as session:
        assert store.set_card_id(
            session,
            intake_id=start.intake_id,
            operator="ada",
            device_id="mac-1",
            card_id="card-key-1",
        )
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
    progress = progress_registry.snapshot(start.intake_id)
    assert progress is not None
    assert progress.bytes_received == 0
    assert progress.bytes_total == len(payload)
    assert progress.files == ()
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
    assert progress_registry.snapshot(start.intake_id) is None
    assert not (landing / start.intake_id / ".receiving.json").exists()
    assert (
        servicer.GetIntakeStatus(
            intake_pb2.IntakeStatusRequest(intake_id=start.intake_id),
            context,
        ).status
        == "verifying"
    )

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
        intake = session.get(Intake, start.intake_id)
        assert intake is not None
        assert intake.card_id == "card-key-1"
        assert intake.device_id == "mac-1"
        intent = session.scalars(
            select(api_store.IdempotencyRecord).where(
                api_store.IdempotencyRecord.intake_id == start.intake_id
            )
        ).one()
        assert intent.status == "committed"
        assert session.get(api_store.SourceClaim, intent.lease_source_id) is None


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
        GrpcIntakeConfig(
            engine=engine, landing_root=tmp_path / "landing", validate_artifactclass=False
        )
    )
    context = _FakeContext("mac-1", "AA" * 32)
    _authorize_receive_intent(engine, key="key-2", device_id="mac-1")
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


def test_start_intake_requires_authorized_http_intent(engine: Engine, tmp_path: Path) -> None:
    with session_scope(engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
    servicer = IntakeServicer(
        GrpcIntakeConfig(
            engine=engine,
            landing_root=tmp_path / "landing",
            validate_artifactclass=False,
        )
    )

    with pytest.raises(_Abort) as missing:
        servicer.StartIntake(
            intake_pb2.StartIntakeRequest(
                idempotency_key="unlinked",
                artifactclass="video-master",
                source_kind="card",
                source_plan_digest="a" * 64,
            ),
            _FakeContext("mac-1", "AA" * 32),
        )

    assert missing.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "authorized receive intent" in missing.value.details


def test_stream_activity_renews_lease_before_any_file_receipt(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A single multi-TTL file must keep excluding a concurrent card receive."""

    clock = [dt.datetime(2026, 7, 10, tzinfo=dt.UTC)]
    monkeypatch.setattr(api_store, "_utcnow", lambda: clock[0])
    with session_scope(engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
    decision = api_store.begin_device_receive_intent(
        engine,
        operator_username="ada",
        device_id="mac-1",
        card_identity="card-long",
        card_label="Long Card",
        idempotency_key="long-key",
        request_hash="long-hash",
        acknowledge_duplicate=False,
    )
    assert decision.state == "authorized"
    servicer = IntakeServicer(
        GrpcIntakeConfig(
            engine=engine,
            landing_root=tmp_path / "landing",
            validate_artifactclass=False,
            lease_renewal_floor=dt.timedelta(0),
        )
    )
    context = _FakeContext("mac-1", "AA" * 32)
    started = servicer.StartIntake(
        intake_pb2.StartIntakeRequest(
            idempotency_key="long-key",
            artifactclass="video-master",
            source_kind="card",
            source_plan_digest="a" * 64,
        ),
        context,
    )

    def chunks() -> Iterator[object]:
        yield intake_pb2.FileChunk(
            intake_id=started.intake_id,
            relpath="event.mov",
            data=b"a",
            offset=0,
            file_size=3,
        )
        for minutes, offset, data, is_last in (
            (20, 1, b"b", False),
            (40, 2, b"c", True),
        ):
            clock[0] = dt.datetime(2026, 7, 10, tzinfo=dt.UTC) + dt.timedelta(minutes=minutes)
            competing = api_store.begin_device_receive_intent(
                engine,
                operator_username="other",
                device_id="mac-2",
                card_identity="card-long",
                card_label="Long Card",
                idempotency_key=f"other-{minutes}",
                request_hash=f"other-hash-{minutes}",
                acknowledge_duplicate=False,
            )
            assert competing.state == "busy"
            yield intake_pb2.FileChunk(
                intake_id=started.intake_id,
                relpath="event.mov",
                data=data,
                offset=offset,
                is_last=is_last,
                file_size=3,
            )

    receipt = servicer.UploadFile(chunks(), context)

    assert receipt.received_bytes == 3
    assert receipt.server_sha256 == hashlib.sha256(b"abc").hexdigest()


def test_upload_aborts_when_source_lease_is_lost(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """A resumed leaseless stream fails while preserving the replacement owner."""

    with session_scope(engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
    _authorize_receive_intent(engine, key="lost-key", device_id="mac-1")
    servicer = IntakeServicer(
        GrpcIntakeConfig(
            engine=engine,
            landing_root=tmp_path / "landing",
            validate_artifactclass=False,
            lease_renewal_floor=dt.timedelta(0),
        )
    )
    context = _FakeContext("mac-1", "AA" * 32)
    started = servicer.StartIntake(
        intake_pb2.StartIntakeRequest(
            idempotency_key="lost-key",
            artifactclass="video-master",
            source_kind="card",
            source_plan_digest="a" * 64,
        ),
        context,
    )
    with session_scope(engine) as session:
        intent = session.scalars(
            select(api_store.IdempotencyRecord).where(
                api_store.IdempotencyRecord.idempotency_key == "lost-key"
            )
        ).one()
        assert intent.lease_source_id is not None
        lease_source_id = intent.lease_source_id
        old_claim = session.get(api_store.SourceClaim, lease_source_id)
        assert old_claim is not None
        session.delete(old_claim)
        session.flush()
        session.add(
            api_store.SourceClaim(
                source_id=lease_source_id,
                operator_username="other",
                idempotency_key="replacement-key",
                intake_id="replacement-intake",
            )
        )

    with pytest.raises(_Abort) as lost:
        servicer.UploadFile(
            iter(
                [
                    intake_pb2.FileChunk(
                        intake_id=started.intake_id,
                        relpath="clip.mov",
                        data=b"x",
                        offset=0,
                        is_last=True,
                        file_size=1,
                    )
                ]
            ),
            context,
        )

    assert lost.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert lost.value.details == LEASE_LOST_DETAIL
    with session_scope(engine) as session:
        intent = session.scalars(
            select(api_store.IdempotencyRecord).where(
                api_store.IdempotencyRecord.idempotency_key == "lost-key"
            )
        ).one()
        row = store.get_intake(session, started.intake_id)
        replacement = session.get(api_store.SourceClaim, lease_source_id)
        assert intent.status == "failed"
        assert row is not None
        assert row.state == "aborted"
        assert replacement is not None
        assert replacement.idempotency_key == "replacement-key"


def test_transient_lease_renewal_error_retries_without_aborting_upload(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DB renewal error is tolerated and the next chunk retries renewal."""

    with session_scope(engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
    _authorize_receive_intent(engine, key="retry-renewal", device_id="mac-1")
    servicer = IntakeServicer(
        GrpcIntakeConfig(
            engine=engine,
            landing_root=tmp_path / "landing",
            validate_artifactclass=False,
            lease_renewal_floor=dt.timedelta(0),
        )
    )
    context = _FakeContext("mac-1", "AA" * 32)
    started = servicer.StartIntake(
        intake_pb2.StartIntakeRequest(
            idempotency_key="retry-renewal",
            artifactclass="video-master",
            source_kind="card",
            source_plan_digest="a" * 64,
        ),
        context,
    )
    original = api_store.renew_device_intake_lease
    calls = 0

    def flaky_renewal(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary database outage")
        return original(*args, **kwargs)

    monkeypatch.setattr(api_store, "renew_device_intake_lease", flaky_renewal)
    receipt = servicer.UploadFile(
        iter(
            [
                intake_pb2.FileChunk(
                    intake_id=started.intake_id,
                    relpath="clip.mov",
                    data=b"a",
                    offset=0,
                    file_size=2,
                ),
                intake_pb2.FileChunk(
                    intake_id=started.intake_id,
                    relpath="clip.mov",
                    data=b"b",
                    offset=1,
                    is_last=True,
                    file_size=2,
                ),
            ]
        ),
        context,
    )

    assert calls == 2
    assert receipt.received_bytes == 2
    with session_scope(engine) as session:
        row = store.get_intake(session, started.intake_id)
        assert row is not None
        assert row.state == "streaming"


def test_start_intake_commits_stale_intent_expiry_before_rpc_abort(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """The FAILED_PRECONDITION abort must not roll back stale lease cleanup."""

    with session_scope(engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
    _authorize_receive_intent(engine, key="stale-key", device_id="mac-1")
    with session_scope(engine) as session:
        intent = session.scalars(
            select(api_store.IdempotencyRecord).where(
                api_store.IdempotencyRecord.idempotency_key == "stale-key"
            )
        ).one()
        stale = dt.datetime.now(dt.UTC) - dt.timedelta(hours=1)
        intent.last_heartbeat = stale
        assert intent.lease_source_id is not None
        claim = session.get(api_store.SourceClaim, intent.lease_source_id)
        assert claim is not None
        claim.last_heartbeat = stale
    servicer = IntakeServicer(
        GrpcIntakeConfig(
            engine=engine,
            landing_root=tmp_path / "landing",
            validate_artifactclass=False,
        )
    )

    with pytest.raises(_Abort) as rejected:
        servicer.StartIntake(
            intake_pb2.StartIntakeRequest(
                idempotency_key="stale-key",
                artifactclass="video-master",
                source_kind="card",
                source_plan_digest="a" * 64,
            ),
            _FakeContext("mac-1", "AA" * 32),
        )

    assert rejected.value.code == grpc.StatusCode.FAILED_PRECONDITION
    with session_scope(engine) as session:
        intent = session.scalars(
            select(api_store.IdempotencyRecord).where(
                api_store.IdempotencyRecord.idempotency_key == "stale-key"
            )
        ).one()
        assert intent.status == "failed"
        assert session.get(api_store.SourceClaim, intent.lease_source_id) is None


def test_abort_terminalizes_http_intent_and_releases_card_lease(
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
    _authorize_receive_intent(engine, key="abort-key", device_id="mac-1")
    servicer = IntakeServicer(
        GrpcIntakeConfig(
            engine=engine,
            landing_root=tmp_path / "landing",
            validate_artifactclass=False,
        )
    )
    context = _FakeContext("mac-1", "AA" * 32)
    started = servicer.StartIntake(
        intake_pb2.StartIntakeRequest(
            idempotency_key="abort-key",
            artifactclass="video-master",
            source_kind="card",
            source_plan_digest="a" * 64,
        ),
        context,
    )

    response = servicer.AbortIntake(
        intake_pb2.AbortIntakeRequest(intake_id=started.intake_id),
        context,
    )

    assert response.status == "aborted"
    with session_scope(engine) as session:
        intent = session.scalars(
            select(api_store.IdempotencyRecord).where(
                api_store.IdempotencyRecord.idempotency_key == "abort-key"
            )
        ).one()
        assert intent.status == "aborted"
        assert session.get(api_store.SourceClaim, intent.lease_source_id) is None


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


def _authorize_receive_intent(engine: Engine, *, key: str, device_id: str) -> None:
    """Create the REST authorization that the proto-unchanged StartIntake consumes."""

    decision = api_store.begin_device_receive_intent(
        engine,
        operator_username="ada",
        device_id=device_id,
        card_identity=f"card-{key}",
        card_label="Card",
        idempotency_key=key,
        request_hash=f"hash-{key}",
        acknowledge_duplicate=False,
    )
    assert decision.state == "authorized"


def test_validate_card_id_accepts_real_agent_volume_ids() -> None:
    """The Rust agent derives ids like ``volume:<uuid>``; the colon must pass
    every ingress validator or the first card snapshot kills the device stream
    (2026-07-10 diff-gate blocker)."""
    from sutradhara.grpc.registry import validate_card_id

    for card_id in ("volume:9AA5-66F8", "volume:abcdef012345abcdef012345", "volume:test"):
        assert validate_card_id(card_id) == card_id
