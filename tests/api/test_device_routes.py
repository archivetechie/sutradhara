"""HTTP operator-console device route tests."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import Engine, select

from sutradhara.api import store as api_store
from sutradhara.catalog.session import session_scope
from sutradhara.grpc import store as grpc_store
from sutradhara.grpc.registry import Card, CommandAck, ConnectedDeviceRegistry
from sutradhara.grpc.store import DeviceIdentity
from tests.api.conftest import make_api_app, post_headers


def test_get_devices_filters_online_devices_and_includes_durable_receives(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    registry = ConnectedDeviceRegistry()
    stream = registry.register(
        DeviceIdentity(operator="owner", device_id="mac-1", fingerprint="AA" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")]
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
            operator="owner",
        )
        grpc_store.insert_intake(
            session,
            intake_id="intake-1",
            operator="owner",
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
            operator="owner",
            device_id="mac-1",
            card_id="card-1",
        )

    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)

    response = client.get("/api/devices", headers=post_headers("operator"))

    assert response.status_code == 200
    body = response.json()
    assert [device["deviceId"] for device in body["devices"]] == ["mac-1"]
    assert body["devices"][0]["cards"][0]["cardId"] == "card-1"
    assert body["receives"] == [
        {
            "intakeId": "intake-1",
            "deviceId": "mac-1",
            "cardId": "card-1",
            "status": "streaming",
        }
    ]


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
            grpc_store.insert_intake(
                session,
                intake_id="intake-1",
                operator="owner",
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

    first = client.post("/api/devices/mac-1/receive", headers=post_headers("operator"), json=payload)
    replay = client.post("/api/devices/mac-1/receive", headers=post_headers("operator"), json=payload)
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

    first = client.post("/api/devices/mac-1/receive", headers=post_headers("operator"), json=payload)
    retry = client.post("/api/devices/mac-1/receive", headers=post_headers("operator"), json=payload)

    assert first.status_code == 409
    assert first.json()["error"] == "ack_timeout"
    assert retry.status_code == 409
    assert retry.json()["error"] == "already_in_progress"
    assert call_count == 1


def test_post_device_receive_does_not_complete_when_card_correlation_fails(
    api_engine: Engine,
) -> None:
    registry = _online_registry(api_engine)
    original = registry.send_start_receive

    def bad_ack(**kwargs):
        pending = original(**kwargs)
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
        assert records == []


def test_device_status_reads_same_grpc_marker_logic(api_engine: Engine, tmp_path: Path) -> None:
    with session_scope(api_engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="owner",
        )
        grpc_store.insert_intake(
            session,
            intake_id="intake-1",
            operator="owner",
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
    assert response.json() == {"intakeId": "intake-1", "status": "verified", "errors": []}


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


def _online_registry(engine: Engine) -> ConnectedDeviceRegistry:
    registry = ConnectedDeviceRegistry()
    stream = registry.register(
        DeviceIdentity(operator="owner", device_id="mac-1", fingerprint="AA" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")]
    )
    with session_scope(engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="owner",
        )
    return registry
