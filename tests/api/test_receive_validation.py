"""Receive API validation tests."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine

from tests.api.conftest import make_api_app, post_headers


def test_unknown_artifactclass_is_400(
    api_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sources = tmp_path / "sources"
    landings = tmp_path / "landings"
    (sources / "card-a").mkdir(parents=True)
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
            "artifactclass": "unknown",
            "idempotencyKey": str(uuid4()),
        },
    )

    assert response.status_code == 400
    assert response.json()["error"] == "bad_artifactclass"
