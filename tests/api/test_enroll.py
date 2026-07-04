"""Enrollment endpoint tests for the operator console relay."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, func, select

from sutradhara.catalog.session import session_scope
from sutradhara.grpc import ca, store
from sutradhara.grpc.registry import ConnectedDeviceRegistry
from sutradhara.grpc.store import DeviceIdentity
from tests.api.conftest import make_api_app, post_headers


ENROLL_URL = "https://system-ui.dvarapala.internal/api/enroll/csr"
CONSOLE_URL = "https://system-ui.dvarapala.internal/"
_MISSING = object()


def _agent_bundle_config(ca_cert: Path, **overrides: object) -> dict[str, object]:
    config: dict[str, object] = {
        "enroll_url": ENROLL_URL,
        "endpoints": [
            {
                "address": "https://sutradhara.archive.lan:50051",
                "server_name": "sutradhara.archive.lan",
            }
        ],
        "enroll_ca_path": ca_cert,
        "console_url": CONSOLE_URL,
    }
    for key, value in overrides.items():
        if value is _MISSING:
            config.pop(key, None)
        else:
            config[key] = value
    return config


def _configure_agent_bundle(app: object, tmp_path: Path, **overrides: object) -> Path:
    pki_dir = tmp_path / "pki"
    ca_cert, _ = ca.ensure_ca(pki_dir)
    app.state.grpc_pki_dir = pki_dir
    app.state.agent_bundle = _agent_bundle_config(ca_cert, **overrides)
    return ca_cert


def _enroll_token_count(engine: Engine) -> int:
    with session_scope(engine) as session:
        count = session.scalar(select(func.count()).select_from(store.GrpcEnrollToken))
    return int(count or 0)


@pytest.mark.parametrize(
    "device_id",
    [
        "",
        "a" * 129,
        "bad/slash",
        "..",
        "bad\r\nx",
    ],
)
@pytest.mark.parametrize("path", ["/api/enroll/token", "/api/enroll/bundle"])
def test_enroll_mint_rejects_invalid_device_id_charset(
    api_engine: Engine,
    tmp_path: Path,
    path: str,
    device_id: str,
) -> None:
    app = make_api_app(api_engine)
    if path == "/api/enroll/bundle":
        _configure_agent_bundle(app, tmp_path)
    client = TestClient(app)

    response = client.post(path, headers=post_headers("operator"), json={"device_id": device_id})

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_device_id"
    assert "^[A-Za-z0-9._-]{1,128}$" in response.json()["detail"]
    assert _enroll_token_count(api_engine) == 0


def test_enroll_bundle_authenticates_before_device_id_or_config_checks(
    api_engine: Engine,
) -> None:
    client = TestClient(make_api_app(api_engine))
    unauthenticated_headers = {
        "Origin": "http://testserver",
        "Host": "testserver",
        "Content-Type": "application/json",
    }

    token_response = client.post(
        "/api/enroll/token",
        headers=unauthenticated_headers,
        json={"device_id": "bad/slash"},
    )
    bundle_response = client.post(
        "/api/enroll/bundle",
        headers=unauthenticated_headers,
        json={"device_id": "bad/slash"},
    )

    assert token_response.status_code == 403
    assert bundle_response.status_code == token_response.status_code
    assert bundle_response.json() == token_response.json()
    assert _enroll_token_count(api_engine) == 0


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


def test_enroll_token_refuses_same_operator_reenroll_without_old_key_proof(
    api_engine: Engine,
) -> None:
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

    assert response.status_code == 409
    assert response.json()["error"] == "old_key_proof_required"


def test_enroll_token_allows_same_operator_reenroll_with_live_old_key_proof(
    api_engine: Engine,
) -> None:
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

    response = client.post(
        "/api/enroll/token",
        headers=post_headers("operator"),
        json={"device_id": "mac-1", "reenroll": True},
    )

    assert response.status_code == 200
    assert response.json()["deviceId"] == "mac-1"
    token = response.json()["token"]
    with session_scope(api_engine) as session:
        row = session.get(store.GrpcEnrollToken, token)
        assert row is not None
        assert row.rotation_authority == "self"
        assert row.rotation_fingerprint == store.normalize_fingerprint("AA" * 32)


def test_enroll_token_refuses_stale_live_stream_proof(api_engine: Engine) -> None:
    registry = ConnectedDeviceRegistry()
    registry.register(DeviceIdentity(operator="owner", device_id="mac-1", fingerprint="BB" * 32))
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

    response = client.post(
        "/api/enroll/token",
        headers=post_headers("operator"),
        json={"device_id": "mac-1", "reenroll": True},
    )

    assert response.status_code == 409
    assert response.json()["error"] == "old_key_proof_required"


def test_enroll_token_allows_admin_confirmed_rotation_without_live_stream(
    api_engine: Engine,
) -> None:
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
        headers=post_headers("sutradhara-admin|sutradhara-ingest"),
        json={"device_id": "mac-1", "reenroll": True},
    )

    assert response.status_code == 200
    token = response.json()["token"]
    with session_scope(api_engine) as session:
        row = session.get(store.GrpcEnrollToken, token)
        assert row is not None
        assert row.rotation_authority == "admin"
        assert row.rotation_fingerprint is None


def test_enroll_bundle_returns_downloadable_bundle_and_redeemable_token(
    api_engine: Engine,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = make_api_app(api_engine)
    ca_cert = _configure_agent_bundle(app, tmp_path)
    client = TestClient(app, base_url="http://request-scope.example")
    material = ca.generate_device_csr(tmp_path / "device", device_id="mac-1")

    caplog.set_level(logging.DEBUG)
    response = client.post(
        "/api/enroll/bundle",
        headers={
            **post_headers("operator"),
            "Host": "request-scope.example",
            "Origin": "http://request-scope.example",
        },
        json={"device_id": "mac-1"},
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json; charset=utf-8"
    assert response.headers["content-disposition"] == 'attachment; filename="mac-1.sutra-enroll"'
    assert response.headers["cache-control"] == "no-store, private"
    bundle = response.json()
    assert bundle == {
        "format": "sutra-enroll-bundle-v1",
        "device_id": "mac-1",
        "enroll_url": ENROLL_URL,
        "enroll_ca_pem": ca_cert.read_text(encoding="utf-8"),
        "token": bundle["token"],
        "expires_at": bundle["expires_at"],
        "endpoints": [
            {
                "address": "https://sutradhara.archive.lan:50051",
                "server_name": "sutradhara.archive.lan",
            }
        ],
        "console_url": CONSOLE_URL,
    }
    assert bundle["token"] not in caplog.text

    signed = client.post(
        bundle["enroll_url"],
        headers={"Host": "system-ui.dvarapala.internal", "Content-Type": "application/json"},
        json={"csr_pem": material.csr_path.read_text(encoding="utf-8"), "token": bundle["token"]},
    )
    assert signed.status_code == 200
    assert "BEGIN CERTIFICATE" in signed.json()["cert_pem"]


def test_enroll_bundle_returns_503_when_unconfigured(api_engine: Engine) -> None:
    client = TestClient(make_api_app(api_engine), base_url="https://testserver")

    response = client.post(
        "/api/enroll/bundle",
        headers=post_headers("operator"),
        json={"device_id": "mac-1"},
    )

    assert response.status_code == 503
    assert response.json()["error"] == "bundle_not_configured"
    assert _enroll_token_count(api_engine) == 0


@pytest.mark.parametrize(
    "case",
    [
        "missing_endpoints",
        "empty_endpoints",
        "missing_endpoint_address",
        "missing_ca_path",
        "unreadable_ca_path",
        "missing_enroll_url",
        "empty_enroll_url",
        "http_enroll_url",
        "relative_enroll_url",
    ],
)
def test_enroll_bundle_returns_503_for_incomplete_required_agent_bundle_config(
    api_engine: Engine,
    tmp_path: Path,
    case: str,
) -> None:
    app = make_api_app(api_engine)
    ca_cert = _configure_agent_bundle(app, tmp_path)
    config = dict(app.state.agent_bundle)
    if case == "missing_endpoints":
        config.pop("endpoints")
    elif case == "empty_endpoints":
        config["endpoints"] = []
    elif case == "missing_endpoint_address":
        config["endpoints"] = [{"server_name": "sutradhara.archive.lan"}]
    elif case == "missing_ca_path":
        config.pop("enroll_ca_path")
    elif case == "unreadable_ca_path":
        config["enroll_ca_path"] = ca_cert.parent
    elif case == "missing_enroll_url":
        config.pop("enroll_url")
    elif case == "empty_enroll_url":
        config["enroll_url"] = ""
    elif case == "http_enroll_url":
        config["enroll_url"] = "http://system-ui.dvarapala.internal/api/enroll/csr"
    elif case == "relative_enroll_url":
        config["enroll_url"] = "/api/enroll/csr"
    app.state.agent_bundle = config
    client = TestClient(app)

    response = client.post(
        "/api/enroll/bundle",
        headers=post_headers("operator"),
        json={"device_id": "mac-1"},
    )

    assert response.status_code == 503
    assert response.json()["error"] == "bundle_not_configured"
    assert _enroll_token_count(api_engine) == 0


def test_enroll_bundle_allows_missing_optional_console_url(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    app = make_api_app(api_engine)
    _configure_agent_bundle(app, tmp_path, console_url=_MISSING)
    client = TestClient(app)

    response = client.post(
        "/api/enroll/bundle",
        headers=post_headers("operator"),
        json={"device_id": "mac-1"},
    )

    assert response.status_code == 200
    assert "console_url" not in response.json()


def test_enroll_bundle_second_download_supersedes_first_unredeemed_token(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    app = make_api_app(api_engine)
    _configure_agent_bundle(app, tmp_path)
    client = TestClient(app)
    material = ca.generate_device_csr(tmp_path / "device", device_id="mac-1")

    first = client.post(
        "/api/enroll/bundle",
        headers=post_headers("operator"),
        json={"device_id": "mac-1"},
    )
    second = client.post(
        "/api/enroll/bundle",
        headers=post_headers("operator"),
        json={"device_id": "mac-1"},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    first_token = first.json()["token"]
    second_token = second.json()["token"]
    assert first_token != second_token
    with session_scope(api_engine) as session:
        first_row = session.get(store.GrpcEnrollToken, first_token)
        second_row = session.get(store.GrpcEnrollToken, second_token)
        assert first_row is not None
        assert first_row.used_at is not None
        assert second_row is not None
        assert second_row.used_at is None

    first_redeem = client.post(
        "/api/enroll/csr",
        headers={"Host": "testserver", "Content-Type": "application/json"},
        json={"csr_pem": material.csr_path.read_text(encoding="utf-8"), "token": first_token},
    )
    second_redeem = client.post(
        "/api/enroll/csr",
        headers={"Host": "testserver", "Content-Type": "application/json"},
        json={"csr_pem": material.csr_path.read_text(encoding="utf-8"), "token": second_token},
    )

    assert first_redeem.status_code == 400
    assert "already used" in first_redeem.json()["detail"]
    assert second_redeem.status_code == 200
    assert "BEGIN CERTIFICATE" in second_redeem.json()["cert_pem"]


def test_enroll_bundle_does_not_log_bodies_or_tokens_on_failure_paths(
    api_engine: Engine,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.NOTSET)
    missing_config_client = TestClient(make_api_app(api_engine))
    config_missing = missing_config_client.post(
        "/api/enroll/bundle",
        headers=post_headers("operator"),
        json={"device_id": "mac-config-missing"},
    )

    with session_scope(api_engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-duplicate",
            cert_fingerprint="AA" * 32,
            operator="owner",
        )
    app = make_api_app(api_engine)
    _configure_agent_bundle(app, tmp_path)
    client = TestClient(app)
    mint_failure = client.post(
        "/api/enroll/bundle",
        headers=post_headers("operator"),
        json={"device_id": "mac-duplicate"},
    )
    bundle_response = client.post(
        "/api/enroll/bundle",
        headers=post_headers("operator"),
        json={"device_id": "mac-redeem"},
    )
    bundle = bundle_response.json()
    wrong_material = ca.generate_device_csr(tmp_path / "wrong-device", device_id="mac-other")
    redeem_failure = client.post(
        "/api/enroll/csr",
        headers={"Host": "testserver", "Content-Type": "application/json"},
        json={
            "csr_pem": wrong_material.csr_path.read_text(encoding="utf-8"),
            "token": bundle["token"],
        },
    )

    assert config_missing.status_code == 503
    assert mint_failure.status_code == 409
    assert bundle_response.status_code == 200
    assert redeem_failure.status_code == 400
    logged = caplog.text
    assert config_missing.text not in logged
    assert mint_failure.text not in logged
    assert bundle_response.text not in logged
    assert redeem_failure.text not in logged
    assert bundle["token"] not in logged


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


def test_enroll_bundle_reenroll_mints_with_rotation_authority(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
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
    _configure_agent_bundle(
        app,
        tmp_path,
        endpoints=[{"address": "https://sutradhara.archive.lan:50051"}],
    )
    client = TestClient(app, base_url="https://testserver")

    response = client.post(
        "/api/enroll/bundle",
        headers=post_headers("operator"),
        json={"device_id": "mac-1", "reenroll": True},
    )

    assert response.status_code == 200
    token = response.json()["token"]
    with session_scope(api_engine) as session:
        row = session.get(store.GrpcEnrollToken, token)
        assert row is not None
        assert row.rotation_authority == "self"
        assert row.rotation_fingerprint == store.normalize_fingerprint("AA" * 32)


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


def test_enroll_csr_with_old_key_proof_rotates_and_evicts_live_stream(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
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
    app.state.grpc_pki_dir = tmp_path / "pki"
    client = TestClient(app)
    token = client.post(
        "/api/enroll/token",
        headers=post_headers("operator"),
        json={"device_id": "mac-1", "reenroll": True},
    ).json()["token"]
    material = ca.generate_device_csr(tmp_path / "device", device_id="mac-1")

    response = client.post(
        "/api/enroll/csr",
        headers={"Host": "testserver", "Content-Type": "application/json"},
        json={"csr_pem": material.csr_path.read_text(encoding="utf-8"), "token": token},
    )

    assert response.status_code == 200
    assert registry.devices_for("owner") == []
    cert_path = tmp_path / "signed.crt"
    cert_path.write_text(response.json()["cert_pem"], encoding="utf-8")
    new_fingerprint = ca.cert_fingerprint(cert_path)
    with session_scope(api_engine) as session:
        with pytest.raises(PermissionError):
            store.resolve_device(session, device_id="mac-1", cert_fingerprint="AA" * 32)
        assert store.resolve_device(
            session,
            device_id="mac-1",
            cert_fingerprint=new_fingerprint,
        ).operator == "owner"


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
