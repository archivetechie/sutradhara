"""Session route tests for the operator HTTP API."""

from __future__ import annotations

from fastapi.testclient import TestClient
from sqlalchemy import Engine

from tests.api.conftest import auth_headers, make_api_app


def test_session_returns_identity_without_raw_groups(api_engine: Engine) -> None:
    client = TestClient(make_api_app(api_engine))

    response = client.get("/api/session", headers=auth_headers("operator"))

    assert response.status_code == 200
    body = response.json()
    assert body == {
        "operatorUsername": "owner",
        "displayName": "Ada Operator",
        "role": "operator",
        "capabilities": ["can_view", "can_receive"],
    }
    assert "groups" not in body


def test_session_missing_groups_is_forbidden(api_engine: Engine) -> None:
    client = TestClient(make_api_app(api_engine))

    response = client.get(
        "/api/session",
        headers={"X-Authentik-Username": "owner", "X-Authentik-Name": "Ada Operator"},
    )

    assert response.status_code == 403
    assert response.json()["error"] == "forbidden"
