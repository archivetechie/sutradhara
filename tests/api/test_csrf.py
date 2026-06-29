"""JSON-only and same-origin guard tests for mutating API requests."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine

from tests.api.conftest import make_api_app, post_headers


def test_form_encoded_post_is_rejected(api_engine: Engine) -> None:
    client = TestClient(make_api_app(api_engine))

    response = client.post(
        "/api/receive",
        headers={
            **post_headers("operator"),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"sourceId": "card-a"},
    )

    assert response.status_code == 415
    assert response.json()["error"] == "unsupported_media_type"


def test_foreign_origin_is_rejected(api_engine: Engine) -> None:
    client = TestClient(make_api_app(api_engine))

    response = client.post(
        "/api/receive",
        headers={**post_headers("operator"), "Origin": "https://attacker.example"},
        json={
            "sourceId": "card-a",
            "landingId": "main",
            "artifactclass": "s-masters",
            "idempotencyKey": str(uuid4()),
        },
    )

    assert response.status_code == 403
    assert response.json()["error"] == "forbidden_origin"


def test_correct_json_and_origin_reach_route(
    api_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sources = tmp_path / "sources"
    landings = tmp_path / "landings"
    (sources / "card-a").mkdir(parents=True)
    (sources / "card-a" / "clip.mov").write_bytes(b"video")
    (landings / "main").mkdir(parents=True)
    monkeypatch.setenv("SUTRA_RECEIVE_SOURCE_ROOT", str(sources))
    monkeypatch.setenv("SUTRA_RECEIVE_LANDING_ROOT", str(landings))
    client = TestClient(make_api_app(api_engine))

    response = client.post(
        "/api/receive",
        headers=post_headers("operator"),
        json={
            "sourceId": "card-a",
            "landingId": "main",
            "artifactclass": "s-masters",
            "idempotencyKey": str(uuid4()),
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "received"
