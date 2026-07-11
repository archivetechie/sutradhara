"""HTTP operator-console device route tests."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, select

from sutradhara._proto import device_pb2
from sutradhara.api import store as api_store
from sutradhara.api.routes_devices import DeviceReceiveRequest, _device_receive_hash
from sutradhara.catalog.session import session_scope
from sutradhara.grpc import store as grpc_store
from sutradhara.grpc.progress import ReceiveProgressRegistry
from sutradhara.grpc.registry import Card, CommandAck, ConnectedDeviceRegistry, StreamClosed
from sutradhara.grpc.store import DeviceIdentity
from sutradhara.verification_progress import write_verification_progress
from tests.api.conftest import make_api_app, post_headers


def test_get_devices_filters_online_devices_and_includes_durable_receives(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    registry = ConnectedDeviceRegistry()
    stream = registry.register(
        DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")],
        capabilities=["browse"],
    )
    other = registry.register(
        DeviceIdentity(operator="other", device_id="mac-2", fingerprint="BB" * 32)
    )
    other.update_cards(
        [Card(card_id="card-2", label="Card 2", kind="card", size_bytes=10, status="available")]
    )
    with session_scope(api_engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-offline",
            cert_fingerprint="CC" * 32,
            operator="ada",
        )
        grpc_store.record_device_enrollment(
            session,
            device_id="other-offline",
            cert_fingerprint="DD" * 32,
            operator="other",
        )
        grpc_store.insert_intake(
            session,
            intake_id="intake-1",
            operator="ada",
            device_id="mac-1",
            idempotency_key="key-1",
            source_plan_digest="a" * 64,
            artifactclass="s-masters",
            source_kind="card",
            source_ref="card-1",
            label="Card 1",
            landing_root=str(tmp_path),
        )
        grpc_store.set_card_id(
            session,
            intake_id="intake-1",
            operator="ada",
            device_id="mac-1",
            card_id="card-1",
        )

    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)

    response = client.get("/api/devices", headers=post_headers("operator"))

    assert response.status_code == 200
    body = response.json()
    assert body["registeredDevices"] == [
        {
            "deviceId": "mac-1",
            "enrolledAs": "ada",
            "enrollmentStatus": "active",
            "online": True,
            "lastSeenAt": body["devices"][0]["lastSeenAt"],
        },
        {
            "deviceId": "mac-offline",
            "enrolledAs": "ada",
            "enrollmentStatus": "active",
            "online": False,
            "lastSeenAt": None,
        },
    ]
    assert [device["deviceId"] for device in body["devices"]] == ["mac-1"]
    assert body["devices"][0]["enrolledAs"] == "ada"
    assert body["devices"][0]["online"] is True
    assert body["devices"][0]["capabilities"] == ["browse"]
    assert body["devices"][0]["cards"][0]["cardId"] == "card-1"
    assert body["receives"] == [
        {
            "intakeId": "intake-1",
            "deviceId": "mac-1",
            "cardId": "card-1",
            "status": "streaming",
            "releaseSafe": False,
            "destinationPath": str(tmp_path / "intake-1"),
            "bytesReceived": None,
            "bytesTotal": None,
            "verificationBytesVerified": None,
            "verificationBytesTotal": None,
        }
    ]


def test_streaming_receive_reports_live_byte_progress(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    progress = ReceiveProgressRegistry()
    with session_scope(api_engine) as session:
        grpc_store.insert_intake(
            session,
            intake_id="intake-streaming",
            operator="ada",
            device_id="mac-1",
            idempotency_key="key-streaming",
            source_plan_digest="a" * 64,
            artifactclass="s-masters",
            source_kind="card",
            source_ref="card-1",
            label="Card 1",
            landing_root=str(tmp_path),
        )
    progress.start("intake-streaming", planned_bytes_total=100)
    progress.update_file(
        "intake-streaming",
        relpath="clip.mov",
        bytes_received=25,
        bytes_total=100,
    )

    app = make_api_app(api_engine)
    app.state.grpc_progress_registry = progress
    client = TestClient(app)

    devices = client.get("/api/devices", headers=post_headers("operator"))
    status = client.get("/api/intake/intake-streaming/status", headers=post_headers("operator"))

    expected = {
        "destinationPath": str(tmp_path / "intake-streaming"),
        "bytesReceived": 25,
        "bytesTotal": 100,
        "verificationBytesVerified": None,
        "verificationBytesTotal": None,
    }
    assert devices.status_code == 200
    assert devices.json()["receives"] == [
        {
            "intakeId": "intake-streaming",
            "deviceId": "mac-1",
            "cardId": None,
            "status": "streaming",
            "releaseSafe": False,
            **expected,
        }
    ]
    assert status.status_code == 200
    assert status.json() == {
        "intakeId": "intake-streaming",
        "status": "streaming",
        "errors": [],
        "releaseSafe": False,
        "source_release_safe": False,
        "novelty": {
            "total": 0,
            "new": 0,
            "known_durable": 0,
            "known_under_durable": 0,
            "reverified": 0,
        },
        **expected,
    }


def test_status_and_devices_mark_committed_card_receive_release_safe(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    registry = ConnectedDeviceRegistry()
    stream = registry.register(
        DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="busy")]
    )
    with session_scope(api_engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
        grpc_store.insert_intake(
            session,
            intake_id="intake-committed",
            operator="ada",
            device_id="mac-1",
            idempotency_key="key-1",
            source_plan_digest="a" * 64,
            artifactclass="s-masters",
            source_kind="card",
            source_ref="card-1",
            label="Card 1",
            landing_root=str(tmp_path),
        )
        grpc_store.set_card_id(
            session,
            intake_id="intake-committed",
            operator="ada",
            device_id="mac-1",
            card_id="card-1",
        )
        grpc_store.set_committed_digest(session, "intake-committed", "b" * 64)
    _write_receipts(tmp_path, "intake-committed", [5, 7])
    write_verification_progress(
        tmp_path / "intake-committed",
        state="running",
        bytes_verified=6,
        bytes_total=24,
    )

    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)

    devices = client.get("/api/devices", headers=post_headers("operator"))
    status = client.get("/api/intake/intake-committed/status", headers=post_headers("operator"))

    assert devices.status_code == 200
    assert devices.json()["receives"] == [
        {
            "intakeId": "intake-committed",
            "deviceId": "mac-1",
            "cardId": "card-1",
            "status": "verifying",
            "releaseSafe": True,
            "destinationPath": str(tmp_path / "intake-committed"),
            "bytesReceived": 12,
            "bytesTotal": 12,
            "verificationBytesVerified": 6,
            "verificationBytesTotal": 24,
        }
    ]
    assert status.status_code == 200
    assert status.json() == {
        "intakeId": "intake-committed",
        "status": "verifying",
        "errors": [],
        "releaseSafe": True,
        "source_release_safe": True,
        "novelty": {
            "total": 0,
            "new": 0,
            "known_durable": 0,
            "known_under_durable": 0,
            "reverified": 0,
        },
        "destinationPath": str(tmp_path / "intake-committed"),
        "bytesReceived": 12,
        "bytesTotal": 12,
        "verificationBytesVerified": 6,
        "verificationBytesTotal": 24,
    }


def test_get_device_browse_returns_listing_for_browse_capable_helper(api_engine: Engine) -> None:
    registry = _online_registry(api_engine, capabilities=["browse"])
    original = registry.request_directory_listing

    def auto_listing(**kwargs):
        pending = original(**kwargs)
        pending.future.set_result(
            device_pb2.DirectoryListing(
                request_id=pending.request_id,
                status=device_pb2.DIR_STATUS_OK,
                truncated=True,
                entries=[
                    device_pb2.DirectoryEntry(
                        name="A001.fcpbundle",
                        is_dir=True,
                        is_package=True,
                    ),
                    device_pb2.DirectoryEntry(
                        name="clip.mov",
                        is_dir=False,
                        size_bytes=12,
                    ),
                ],
            )
        )
        return pending

    registry.request_directory_listing = auto_listing  # type: ignore[method-assign]
    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)

    response = client.get(
        "/api/devices/mac-1/browse",
        headers=post_headers("operator"),
        params={"card_id": "card-1", "path": "DCIM"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "path": "DCIM",
        "entries": [
            {
                "name": "A001.fcpbundle",
                "isDir": True,
                "sizeBytes": 0,
                "isPackage": True,
            },
            {
                "name": "clip.mov",
                "isDir": False,
                "sizeBytes": 12,
                "isPackage": False,
            },
        ],
        "truncated": True,
    }


def test_get_device_browse_rejects_legacy_helper_without_capability(api_engine: Engine) -> None:
    app = make_api_app(api_engine)
    app.state.registry = _online_registry(api_engine)
    client = TestClient(app)

    response = client.get(
        "/api/devices/mac-1/browse",
        headers=post_headers("operator"),
        params={"card_id": "card-1"},
    )

    assert response.status_code == 409
    assert response.json()["error"] == "browse_unsupported"


@pytest.mark.parametrize(
    ("directory_status", "http_status", "error"),
    [
        (device_pb2.DIR_STATUS_NOT_FOUND, 404, "not_found"),
        (device_pb2.DIR_STATUS_NOT_A_DIRECTORY, 422, "not_a_directory"),
        (device_pb2.DIR_STATUS_PERMISSION_DENIED, 403, "permission_denied"),
        (device_pb2.DIR_STATUS_CONFINEMENT_VIOLATION, 400, "confinement_violation"),
        (device_pb2.DIR_STATUS_CARD_UNAVAILABLE, 409, "card_unavailable"),
        (device_pb2.DIR_STATUS_IO_ERROR, 502, "io_error"),
    ],
)
def test_get_device_browse_maps_typed_status(
    api_engine: Engine,
    directory_status: int,
    http_status: int,
    error: str,
) -> None:
    registry = _online_registry(api_engine, capabilities=["browse"])
    original = registry.request_directory_listing

    def listing_with_status(**kwargs):
        pending = original(**kwargs)
        pending.future.set_result(
            device_pb2.DirectoryListing(
                request_id=pending.request_id,
                status=directory_status,
                detail="DCIM/problem",
            )
        )
        return pending

    registry.request_directory_listing = listing_with_status  # type: ignore[method-assign]
    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)

    response = client.get(
        "/api/devices/mac-1/browse",
        headers=post_headers("operator"),
        params={"card_id": "card-1", "path": "DCIM/clip.mov"},
    )

    assert response.status_code == http_status
    assert response.json() == {"error": error, "detail": "DCIM/problem"}


def test_get_device_browse_timeout_and_stream_close(api_engine: Engine) -> None:
    registry = _online_registry(api_engine, capabilities=["browse"])
    app = make_api_app(api_engine)
    app.state.registry = registry
    app.state.directory_listing_timeout = 0.01
    client = TestClient(app)

    timeout = client.get(
        "/api/devices/mac-1/browse",
        headers=post_headers("operator"),
        params={"card_id": "card-1"},
    )
    assert timeout.status_code == 504
    assert timeout.json()["error"] == "browse_timeout"

    original = registry.request_directory_listing

    def closed(**kwargs):
        pending = original(**kwargs)
        pending.future.set_exception(StreamClosed("device stream ended"))
        return pending

    registry.request_directory_listing = closed  # type: ignore[method-assign]
    closed_response = client.get(
        "/api/devices/mac-1/browse",
        headers=post_headers("operator"),
        params={"card_id": "card-1"},
    )
    assert closed_response.status_code == 409
    assert closed_response.json()["error"] == "device_unavailable"


def test_post_device_receive_early_ack_completes_idempotency_and_replays(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    registry = _online_registry(api_engine)
    call_count = 0
    original = registry.send_start_receive

    def auto_ack(**kwargs):
        nonlocal call_count
        call_count += 1
        pending = original(**kwargs)
        with session_scope(api_engine) as session:
            intent = api_store.claim_start_intake(
                session,
                operator_username="ada",
                device_id="mac-1",
                idempotency_key=kwargs["idempotency_key"],
                intake_id="intake-1",
            )
            assert intent.state == "claimed"
            grpc_store.insert_intake(
                session,
                intake_id="intake-1",
                operator="ada",
                device_id="mac-1",
                idempotency_key=kwargs["idempotency_key"],
                source_plan_digest="a" * 64,
                artifactclass=kwargs["artifactclass"],
                source_kind="card",
                source_ref=kwargs["card_id"],
                label=kwargs["label"],
                landing_root=str(tmp_path),
            )
        pending.future.set_result(
            CommandAck(
                command_id=pending.command_id,
                accepted=True,
                reason=None,
                intake_id="intake-1",
            )
        )
        return pending

    registry.send_start_receive = auto_ack  # type: ignore[method-assign]
    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)
    key = str(uuid4())
    payload = {
        "card_id": "card-1",
        "artifactclass": "s-masters",
        "label": "Card 1",
        "idempotencyKey": key,
    }

    first = client.post(
        "/api/devices/mac-1/receive", headers=post_headers("operator"), json=payload
    )
    replay = client.post(
        "/api/devices/mac-1/receive", headers=post_headers("operator"), json=payload
    )
    conflict = client.post(
        "/api/devices/mac-1/receive",
        headers=post_headers("operator"),
        json={**payload, "card_id": "missing"},
    )

    assert first.status_code == 200
    assert first.json() == {"intakeId": "intake-1", "status": "streaming"}
    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert conflict.status_code == 409
    assert conflict.json()["error"] == "idempotency_conflict"
    assert call_count == 1
    with session_scope(api_engine) as session:
        row = grpc_store.get_intake(session, "intake-1")
        assert row is not None
        assert row.card_id == "card-1"


def test_post_device_receive_in_progress_does_not_recommand(
    api_engine: Engine,
) -> None:
    registry = _online_registry(api_engine)
    call_count = 0
    original = registry.send_start_receive

    def no_ack(**kwargs):
        nonlocal call_count
        call_count += 1
        return original(**kwargs)

    registry.send_start_receive = no_ack  # type: ignore[method-assign]
    app = make_api_app(api_engine)
    app.state.registry = registry
    app.state.command_ack_timeout = 0.01
    client = TestClient(app)
    payload = {
        "card_id": "card-1",
        "artifactclass": "s-masters",
        "idempotencyKey": str(uuid4()),
    }

    first = client.post(
        "/api/devices/mac-1/receive", headers=post_headers("operator"), json=payload
    )
    retry = client.post(
        "/api/devices/mac-1/receive", headers=post_headers("operator"), json=payload
    )

    assert first.status_code == 409
    assert first.json()["error"] == "ack_timeout"
    assert retry.status_code == 409
    assert retry.json()["error"] == "already_in_progress"
    assert call_count == 1


def test_stream_drop_releases_card_lease_for_immediate_retry(api_engine: Engine) -> None:
    """A dropped command stream must not strand its authorized card lease."""

    registry = _online_registry(api_engine)
    original = registry.send_start_receive
    calls = 0

    def drop_then_reject(**kwargs):
        nonlocal calls
        calls += 1
        pending = original(**kwargs)
        if calls == 1:
            pending.future.set_exception(StreamClosed("device stream ended"))
        else:
            pending.future.set_result(
                CommandAck(
                    command_id=pending.command_id,
                    accepted=False,
                    reason="operator retry reached device",
                    intake_id=None,
                )
            )
        return pending

    registry.send_start_receive = drop_then_reject  # type: ignore[method-assign]
    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)
    payload = {
        "card_id": "card-1",
        "artifactclass": "s-masters",
        "idempotencyKey": str(uuid4()),
    }

    dropped = client.post(
        "/api/devices/mac-1/receive", headers=post_headers("operator"), json=payload
    )
    retried = client.post(
        "/api/devices/mac-1/receive",
        headers=post_headers("operator"),
        json={**payload, "idempotencyKey": str(uuid4())},
    )

    assert dropped.status_code == 409
    assert dropped.json()["error"] == "device_unavailable"
    assert retried.status_code == 409
    assert retried.json() == {
        "error": "receive_rejected",
        "detail": "operator retry reached device",
    }
    assert calls == 2
    with session_scope(api_engine) as session:
        records = list(session.scalars(select(api_store.IdempotencyRecord)))
        assert [record.status for record in records] == ["failed", "failed"]
        assert list(session.scalars(select(api_store.SourceClaim))) == []


def test_stream_drop_after_start_keeps_live_receive_and_lease(api_engine: Engine, tmp_path: Path) -> None:
    """An ack-stream failure cannot terminalize a StartIntake-claimed receive."""

    registry = _online_registry(api_engine)
    original = registry.send_start_receive

    def start_then_drop(**kwargs):
        pending = original(**kwargs)
        with session_scope(api_engine) as session:
            linked = api_store.claim_start_intake(
                session,
                operator_username="ada",
                device_id="mac-1",
                idempotency_key=kwargs["idempotency_key"],
                intake_id="live-intake",
            )
            assert linked.state == "claimed"
            grpc_store.insert_intake(
                session,
                intake_id="live-intake",
                operator="ada",
                device_id="mac-1",
                idempotency_key=kwargs["idempotency_key"],
                source_plan_digest="a" * 64,
                artifactclass=kwargs["artifactclass"],
                source_kind="card",
                source_ref=kwargs["source_ref"],
                label=kwargs["label"],
                landing_root=str(tmp_path),
            )
        pending.future.set_exception(StreamClosed("device stream ended after StartIntake"))
        return pending

    registry.send_start_receive = start_then_drop  # type: ignore[method-assign]
    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)
    payload = {
        "card_id": "card-1",
        "artifactclass": "s-masters",
        "idempotencyKey": str(uuid4()),
    }

    response = client.post(
        "/api/devices/mac-1/receive",
        headers=post_headers("operator"),
        json=payload,
    )

    assert response.status_code == 409
    assert response.json()["error"] == "already_in_progress"
    with session_scope(api_engine) as session:
        intent = session.scalars(select(api_store.IdempotencyRecord)).one()
        intake = grpc_store.get_intake(session, "live-intake")
        assert intent.status == "started"
        assert intake is not None
        assert intake.state == "streaming"
        assert intent.lease_source_id is not None
        claim = session.get(api_store.SourceClaim, intent.lease_source_id)
        assert claim is not None
        assert claim.intake_id == "live-intake"


def test_post_device_receive_canonicalizes_source_ref_before_idempotency(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    registry = _online_registry(api_engine)
    call_count = 0
    seen_source_refs: list[str] = []
    original = registry.send_start_receive

    def auto_ack(**kwargs):
        nonlocal call_count
        call_count += 1
        seen_source_refs.append(kwargs["source_ref"])
        pending = original(**kwargs)
        with session_scope(api_engine) as session:
            intent = api_store.claim_start_intake(
                session,
                operator_username="ada",
                device_id="mac-1",
                idempotency_key=kwargs["idempotency_key"],
                intake_id="intake-1",
            )
            assert intent.state == "claimed"
            grpc_store.insert_intake(
                session,
                intake_id="intake-1",
                operator="ada",
                device_id="mac-1",
                idempotency_key=kwargs["idempotency_key"],
                source_plan_digest="a" * 64,
                artifactclass=kwargs["artifactclass"],
                source_kind="card",
                source_ref=kwargs["source_ref"],
                label=kwargs["label"],
                landing_root=str(tmp_path),
            )
        pending.future.set_result(
            CommandAck(
                command_id=pending.command_id,
                accepted=True,
                reason=None,
                intake_id="intake-1",
            )
        )
        return pending

    registry.send_start_receive = auto_ack  # type: ignore[method-assign]
    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)
    key = str(uuid4())
    base_payload = {
        "card_id": "card-1",
        "artifactclass": "s-masters",
        "idempotencyKey": key,
    }

    first = client.post(
        "/api/devices/mac-1/receive", headers=post_headers("operator"), json=base_payload
    )
    replay = client.post(
        "/api/devices/mac-1/receive",
        headers=post_headers("operator"),
        json={**base_payload, "source_ref": ""},
    )

    assert first.status_code == 200
    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert call_count == 1
    assert seen_source_refs == [""]


def test_post_device_receive_rejects_bad_source_ref_before_claim_or_dispatch(
    api_engine: Engine,
) -> None:
    registry = _online_registry(api_engine)
    call_count = 0

    def unexpected_dispatch(**_kwargs):
        nonlocal call_count
        call_count += 1
        raise AssertionError("receive should not dispatch")

    registry.send_start_receive = unexpected_dispatch  # type: ignore[method-assign]
    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)

    response = client.post(
        "/api/devices/mac-1/receive",
        headers=post_headers("operator"),
        json={
            "card_id": "card-1",
            "artifactclass": "s-masters",
            "idempotencyKey": str(uuid4()),
            "source_ref": "../DCIM",
        },
    )

    assert response.status_code == 400
    assert response.json()["error"] == "bad_path"
    assert call_count == 0
    with session_scope(api_engine) as session:
        assert list(session.scalars(select(api_store.IdempotencyRecord))) == []


def test_post_device_receive_does_not_complete_when_card_correlation_fails(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    registry = _online_registry(api_engine)
    original = registry.send_start_receive

    def bad_ack(**kwargs):
        pending = original(**kwargs)
        with session_scope(api_engine) as session:
            linked = api_store.claim_start_intake(
                session,
                operator_username="ada",
                device_id="mac-1",
                idempotency_key=kwargs["idempotency_key"],
                intake_id="owned-intake",
            )
            assert linked.state == "claimed"
            grpc_store.insert_intake(
                session,
                intake_id="owned-intake",
                operator="ada",
                device_id="mac-1",
                idempotency_key=kwargs["idempotency_key"],
                source_plan_digest="a" * 64,
                artifactclass=kwargs["artifactclass"],
                source_kind="card",
                source_ref=kwargs["source_ref"],
                label=kwargs["label"],
                landing_root=str(tmp_path),
            )
        pending.future.set_result(
            CommandAck(
                command_id=pending.command_id,
                accepted=True,
                reason=None,
                intake_id="missing-intake",
            )
        )
        return pending

    registry.send_start_receive = bad_ack  # type: ignore[method-assign]
    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)

    response = client.post(
        "/api/devices/mac-1/receive",
        headers=post_headers("operator"),
        json={
            "card_id": "card-1",
            "artifactclass": "s-masters",
            "idempotencyKey": str(uuid4()),
        },
    )

    assert response.status_code == 409
    assert response.json()["error"] == "correlation_failed"
    with session_scope(api_engine) as session:
        records = list(session.scalars(select(api_store.IdempotencyRecord)))
        assert len(records) == 1
        assert records[0].status == "failed"
        assert session.get(api_store.SourceClaim, records[0].lease_source_id) is None


def test_terminal_replay_precedes_ejected_card_resolution(api_engine: Engine) -> None:
    """A stored terminal verdict must replay even after the source card is gone."""

    registry = ConnectedDeviceRegistry()
    stream = registry.register(
        DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")]
    )
    with session_scope(api_engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
    original = registry.send_start_receive

    def reject(**kwargs):
        pending = original(**kwargs)
        pending.future.set_result(
            CommandAck(
                command_id=pending.command_id,
                accepted=False,
                reason="receive refused",
                intake_id=None,
            )
        )
        return pending

    registry.send_start_receive = reject  # type: ignore[method-assign]
    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)
    payload = {
        "card_id": "card-1",
        "artifactclass": "s-masters",
        "idempotencyKey": str(uuid4()),
    }

    first = client.post(
        "/api/devices/mac-1/receive", headers=post_headers("operator"), json=payload
    )
    stream.update_cards([])
    replay = client.post(
        "/api/devices/mac-1/receive", headers=post_headers("operator"), json=payload
    )

    assert first.status_code == 409
    assert first.json()["error"] == "receive_rejected"
    assert replay.status_code == 409
    assert replay.json()["error"] == "receive_terminal"
    assert replay.json()["retryable"] is True
    assert "failed" in replay.json()["detail"]


def test_stale_receive_terminal_is_retryable_and_fresh_key_ignores_identity_history(
    api_engine: Engine,
) -> None:
    """Stale same-key replay fails; identity alone does not block a fresh key."""

    registry = _online_registry(api_engine)
    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)
    key = str(uuid4())
    payload = {
        "card_id": "card-1",
        "artifactclass": "s-masters",
        "idempotencyKey": key,
    }
    body = DeviceReceiveRequest.model_validate(payload)
    request_hash = _device_receive_hash("mac-1", body, source_ref="")
    decision = api_store.begin_device_receive_intent(
        api_engine,
        operator_username="ada",
        device_id="mac-1",
        card_identity="card-1",
        card_label="Card 1",
        idempotency_key=key,
        request_hash=request_hash,
        acknowledge_duplicate=False,
    )
    assert decision.state == "authorized"
    with session_scope(api_engine) as session:
        linked = api_store.claim_start_intake(
            session,
            operator_username="ada",
            device_id="mac-1",
            idempotency_key=key,
            intake_id="stale-intake",
        )
        assert linked.state == "claimed"
        intent = session.scalars(select(api_store.IdempotencyRecord)).one()
        stale = dt.datetime.now(dt.UTC) - dt.timedelta(hours=1)
        intent.last_heartbeat = stale
        assert intent.lease_source_id is not None
        claim = session.get(api_store.SourceClaim, intent.lease_source_id)
        assert claim is not None
        claim.last_heartbeat = stale

    terminal = client.post(
        "/api/devices/mac-1/receive", headers=post_headers("operator"), json=payload
    )
    fresh = client.post(
        "/api/devices/mac-1/receive",
        headers=post_headers("operator"),
        json={**payload, "idempotencyKey": str(uuid4())},
    )

    assert terminal.status_code == 409
    assert terminal.json()["error"] == "receive_terminal"
    assert terminal.json()["retryable"] is True
    assert fresh.status_code == 409
    assert fresh.json()["error"] == "ack_timeout"


def test_stored_started_response_and_committed_verdict_replay_after_eject(
    api_engine: Engine,
) -> None:
    """Stored response wins over stale-skip; committed-without-response is terminal."""

    registry = _online_registry(api_engine)
    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)

    def seed(key: str, *, intake_id: str, store_response: bool) -> dict[str, str]:
        payload = {
            "card_id": "card-1",
            "artifactclass": "s-masters",
            "idempotencyKey": key,
        }
        body = DeviceReceiveRequest.model_validate(payload)
        request_hash = _device_receive_hash("mac-1", body, source_ref="")
        decision = api_store.begin_device_receive_intent(
            api_engine,
            operator_username="ada",
            device_id="mac-1",
            card_identity="card-1",
            card_label="Card 1",
            idempotency_key=key,
            request_hash=request_hash,
            acknowledge_duplicate=False,
        )
        if decision.state == "warned":
            decision = api_store.begin_device_receive_intent(
                api_engine,
                operator_username="ada",
                device_id="mac-1",
                card_identity="card-1",
                card_label="Card 1",
                idempotency_key=key,
                request_hash=request_hash,
                acknowledge_duplicate=True,
            )
        assert decision.state == "authorized"
        with session_scope(api_engine) as session:
            linked = api_store.claim_start_intake(
                session,
                operator_username="ada",
                device_id="mac-1",
                idempotency_key=key,
                intake_id=intake_id,
            )
            assert linked.state == "claimed"
            if not store_response:
                assert api_store.transition_device_intent_terminal(
                    session,
                    intake_id=intake_id,
                    terminal_state="committed",
                )
        if store_response:
            assert api_store.store_device_receive_response(
                api_engine,
                operator_username="ada",
                device_id="mac-1",
                idempotency_key=key,
                intake_id=intake_id,
                response_json={"intakeId": intake_id, "status": "streaming"},
            )
            with session_scope(api_engine) as session:
                intent = session.scalars(
                    select(api_store.IdempotencyRecord).where(
                        api_store.IdempotencyRecord.idempotency_key == key
                    )
                ).one()
                intent.last_heartbeat = dt.datetime.now(dt.UTC) - dt.timedelta(hours=1)
        return payload

    committed_payload = seed(str(uuid4()), intake_id="committed-intake", store_response=False)
    stored_payload = seed(str(uuid4()), intake_id="stored-intake", store_response=True)
    view = registry.devices_for("ada")[0]
    registry.update_cards("mac-1", view.generation, [])

    stored = client.post(
        "/api/devices/mac-1/receive", headers=post_headers("operator"), json=stored_payload
    )
    committed = client.post(
        "/api/devices/mac-1/receive", headers=post_headers("operator"), json=committed_payload
    )

    assert stored.status_code == 200
    assert stored.json() == {"intakeId": "stored-intake", "status": "streaming"}
    assert committed.status_code == 409
    assert committed.json()["error"] == "receive_terminal"
    assert committed.json()["retryable"] is True
    assert "committed" in committed.json()["detail"]


def test_device_status_reads_same_grpc_marker_logic(api_engine: Engine, tmp_path: Path) -> None:
    with session_scope(api_engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
        grpc_store.insert_intake(
            session,
            intake_id="intake-1",
            operator="ada",
            device_id="mac-1",
            idempotency_key="key-1",
            source_plan_digest="a" * 64,
            artifactclass="s-masters",
            source_kind="card",
            source_ref="card-1",
            label="Card 1",
            landing_root=str(tmp_path),
        )
    intake_dir = tmp_path / "intake-1"
    intake_dir.mkdir()
    (intake_dir / "intake.verified.json").write_text("{}", encoding="utf-8")
    client = TestClient(make_api_app(api_engine))

    response = client.get("/api/intake/intake-1/status", headers=post_headers("operator"))

    assert response.status_code == 200
    assert response.json() == {
        "intakeId": "intake-1",
        "status": "verified",
        "errors": [],
        "releaseSafe": True,
        "source_release_safe": True,
        "novelty": {
            "total": 0,
            "new": 0,
            "known_durable": 0,
            "known_under_durable": 0,
            "reverified": 0,
        },
        "destinationPath": str(tmp_path / "intake-1"),
        "bytesReceived": None,
        "bytesTotal": None,
        "verificationBytesVerified": None,
        "verificationBytesTotal": None,
    }


def test_post_device_receive_rejects_cross_operator_device(api_engine: Engine) -> None:
    registry = ConnectedDeviceRegistry()
    stream = registry.register(
        DeviceIdentity(operator="other", device_id="mac-2", fingerprint="BB" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")]
    )
    with session_scope(api_engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-2",
            cert_fingerprint="BB" * 32,
            operator="other",
        )
    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)

    response = client.post(
        "/api/devices/mac-2/receive",
        headers=post_headers("operator"),
        json={
            "card_id": "card-1",
            "artifactclass": "s-masters",
            "idempotencyKey": str(uuid4()),
        },
    )

    assert response.status_code == 403


def _online_registry(
    engine: Engine, *, capabilities: list[str] | None = None
) -> ConnectedDeviceRegistry:
    registry = ConnectedDeviceRegistry()
    stream = registry.register(
        DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")],
        capabilities=capabilities,
    )
    with session_scope(engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
    return registry


def _write_receipts(landing_root: Path, intake_id: str, sizes: list[int]) -> None:
    intake_dir = landing_root / intake_id
    intake_dir.mkdir(parents=True, exist_ok=True)
    lines = [
        json.dumps(
            {
                "relpath": f"clip-{index}.mov",
                "server_sha256": f"{index}".zfill(64),
                "bytes": size,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        for index, size in enumerate(sizes)
    ]
    (intake_dir / "receive-receipts.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_stale_intent_with_ejected_card_terminalizes_before_card_resolution(
    api_engine: Engine,
) -> None:
    """A stale same-key replay must 409 receive_terminal even when the card is
    gone — never device_unavailable (2026-07-11 gate finding)."""

    registry = ConnectedDeviceRegistry()
    stream = registry.register(
        DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")],
        capabilities=None,
    )
    with session_scope(api_engine) as session:
        grpc_store.record_device_enrollment(
            session, device_id="mac-1", cert_fingerprint="AA" * 32, operator="ada"
        )
    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)
    key = str(uuid4())
    payload = {
        "card_id": "card-1",
        "artifactclass": "s-masters",
        "idempotencyKey": key,
    }
    body = DeviceReceiveRequest.model_validate(payload)
    request_hash = _device_receive_hash("mac-1", body, source_ref="")
    decision = api_store.begin_device_receive_intent(
        api_engine,
        operator_username="ada",
        device_id="mac-1",
        card_identity="card-1",
        card_label="Card 1",
        idempotency_key=key,
        request_hash=request_hash,
        acknowledge_duplicate=False,
    )
    assert decision.state == "authorized"
    with session_scope(api_engine) as session:
        intent = session.scalars(select(api_store.IdempotencyRecord)).one()
        stale = dt.datetime.now(dt.UTC) - dt.timedelta(hours=1)
        intent.last_heartbeat = stale
        assert intent.lease_source_id is not None
        claim = session.get(api_store.SourceClaim, intent.lease_source_id)
        if claim is not None:
            claim.last_heartbeat = stale

    # Eject the card: the device stays online but reports no cards.
    stream.update_cards([], capabilities=None)

    terminal = client.post(
        "/api/devices/mac-1/receive", headers=post_headers("operator"), json=payload
    )
    assert terminal.status_code == 409
    assert terminal.json()["error"] == "receive_terminal"
    assert terminal.json()["retryable"] is True

    with session_scope(api_engine) as session:
        record = session.scalars(select(api_store.IdempotencyRecord)).one()
        assert record.status == "failed"
        assert session.get(api_store.SourceClaim, "card-identity:card-1") is None
