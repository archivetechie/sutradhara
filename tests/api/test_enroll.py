"""Enrollment endpoint tests for the operator console relay."""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import Engine

from sutradhara.catalog.session import session_scope
from sutradhara.grpc import ca, store
from sutradhara.grpc.registry import ConnectedDeviceRegistry
from sutradhara.grpc.store import DeviceIdentity
from tests.api.conftest import make_api_app, post_headers


def test_enroll_token_is_origin_guarded_and_operator_scoped(api_engine: Engine) -> None:
    client = TestClient(make_api_app(api_engine))

    missing_origin = client.post(
        "/api/enroll/token",
        headers={
            **post_headers("operator"),
            "Origin": "",
        },
        json={"device_id": "mac-1"},
    )
    ok = client.post(
        "/api/enroll/token",
        headers=post_headers("operator"),
        json={"device_id": "mac-1"},
    )

    assert missing_origin.status_code == 403
    assert ok.status_code == 200
    assert ok.json()["deviceId"] == "mac-1"


def test_enroll_token_refuses_duplicate_without_reenroll(api_engine: Engine) -> None:
    with session_scope(api_engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="owner",
        )
    client = TestClient(make_api_app(api_engine))

    response = client.post(
        "/api/enroll/token",
        headers=post_headers("operator"),
        json={"device_id": "mac-1"},
    )

    assert response.status_code == 409
    assert response.json()["error"] == "device_already_enrolled"


def test_enroll_token_allows_same_operator_reenroll(api_engine: Engine) -> None:
    with session_scope(api_engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="owner",
        )
    client = TestClient(make_api_app(api_engine))

    response = client.post(
        "/api/enroll/token",
        headers=post_headers("operator"),
        json={"device_id": "mac-1", "reenroll": True},
    )

    assert response.status_code == 200
    assert response.json()["deviceId"] == "mac-1"
    assert response.json()["token"]


def test_enroll_token_refuses_different_operator_device(api_engine: Engine) -> None:
    with session_scope(api_engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="other",
        )
    client = TestClient(make_api_app(api_engine))

    response = client.post(
        "/api/enroll/token",
        headers=post_headers("operator"),
        json={"device_id": "mac-1"},
    )

    assert response.status_code == 409
    assert response.json()["error"] == "device_other_operator"


def test_enroll_csr_is_reachable_without_authentik_or_origin_and_rejects_reuse(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    app = make_api_app(api_engine)
    app.state.grpc_pki_dir = tmp_path / "pki"
    client = TestClient(app)
    token = client.post(
        "/api/enroll/token",
        headers=post_headers("operator"),
        json={"device_id": "mac-1"},
    ).json()["token"]
    material = ca.generate_device_csr(tmp_path / "device", device_id="mac-1")

    signed = client.post(
        "/api/enroll/csr",
        headers={"Host": "testserver", "Content-Type": "application/json"},
        json={"csr_pem": material.csr_path.read_text(encoding="utf-8"), "token": token},
    )
    reused = client.post(
        "/api/enroll/csr",
        headers={"Host": "testserver", "Content-Type": "application/json"},
        json={"csr_pem": material.csr_path.read_text(encoding="utf-8"), "token": token},
    )

    assert signed.status_code == 200
    assert "BEGIN CERTIFICATE" in signed.json()["cert_pem"]
    assert "BEGIN CERTIFICATE" in signed.json()["ca_pem"]
    assert reused.status_code == 400
    assert "already used" in reused.json()["detail"]


def test_enroll_csr_rejects_token_device_mismatch(api_engine: Engine, tmp_path: Path) -> None:
    app = make_api_app(api_engine)
    app.state.grpc_pki_dir = tmp_path / "pki"
    client = TestClient(app)
    token = client.post(
        "/api/enroll/token",
        headers=post_headers("operator"),
        json={"device_id": "mac-2"},
    ).json()["token"]
    material = ca.generate_device_csr(tmp_path / "device", device_id="mac-1")

    response = client.post(
        "/api/enroll/csr",
        headers={"Host": "testserver", "Content-Type": "application/json"},
        json={"csr_pem": material.csr_path.read_text(encoding="utf-8"), "token": token},
    )

    assert response.status_code == 400
    assert "common name" in response.json()["detail"]


def test_enroll_csr_maps_other_operator_refusal_to_conflict(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    app = make_api_app(api_engine)
    app.state.grpc_pki_dir = tmp_path / "pki"
    client = TestClient(app)
    with session_scope(api_engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="owner",
        )
        token = store.issue_enroll_token(session, operator="other", device_id="mac-1")
    material = ca.generate_device_csr(tmp_path / "device", device_id="mac-1")

    response = client.post(
        "/api/enroll/csr",
        headers={"Host": "testserver", "Content-Type": "application/json"},
        json={"csr_pem": material.csr_path.read_text(encoding="utf-8"), "token": token},
    )

    assert response.status_code == 409
    assert response.json()["error"] == "device_other_operator"
    with session_scope(api_engine) as session:
        row = session.get(store.GrpcEnrollToken, token)
        assert row is not None
        assert row.used_at is None


def test_revoke_device_can_evict_live_registry_stream(api_engine: Engine) -> None:
    registry = ConnectedDeviceRegistry()
    registry.register(DeviceIdentity(operator="owner", device_id="mac-1", fingerprint="AA" * 32))
    with session_scope(api_engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="owner",
        )
    app = make_api_app(api_engine)
    app.state.registry = registry
    client = TestClient(app)

    response = client.post("/api/devices/mac-1/revoke", headers=post_headers("admin"))

    assert response.status_code == 200
    assert response.json() == {
        "deviceId": "mac-1",
        "revokedEnrollments": 1,
        "evicted": True,
    }
    assert registry.devices_for("owner") == []
