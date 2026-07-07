"""End-to-end receive route tests with injected Authentik headers."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, select

from sutradhara.catalog.models import Intake
from sutradhara.catalog.session import session_scope
from tests.api.conftest import make_api_app, post_headers


def test_viewer_cannot_post_receive(
    api_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _prepare_roots(tmp_path, monkeypatch)
    client = TestClient(make_api_app(api_engine))

    response = client.post(
        "/api/receive",
        headers=post_headers("viewer"),
        json={
            "sourceId": "card-a",
            "landingId": "main",
            "artifactclass": "s-masters",
            "idempotencyKey": str(uuid4()),
        },
    )

    assert response.status_code == 403
    assert response.json()["error"] == "forbidden"


def test_operator_receive_stamps_header_operator_and_ignores_body_operator(
    api_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _prepare_roots(tmp_path, monkeypatch)
    client = TestClient(make_api_app(api_engine))

    response = client.post(
        "/api/receive",
        headers=post_headers("operator"),
        json={
            "sourceId": "card-a",
            "landingId": "main",
            "artifactclass": "s-masters",
            "label": "MSR Day 1",
            "operator": "attacker",
            "idempotencyKey": str(uuid4()),
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["intakeId"]
    assert body["status"] == "received"
    with session_scope(api_engine) as session:
        intake = session.scalars(select(Intake)).one()
        assert intake.intake_id == body["intakeId"]
        assert intake.operator == "ada"
        assert intake.label == "MSR Day 1"
        assert intake.artifactclass == "s-masters"


def _prepare_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sources = tmp_path / "sources"
    landings = tmp_path / "landings"
    (sources / "card-a").mkdir(parents=True)
    (sources / "card-a" / "clip.mov").write_bytes(b"video")
    (landings / "main").mkdir(parents=True)
    monkeypatch.setenv("SUTRA_RECEIVE_SOURCE_ROOT", str(sources))
    monkeypatch.setenv("SUTRA_RECEIVE_LANDING_ROOT", str(landings))
