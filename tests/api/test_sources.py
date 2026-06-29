"""Source and landing catalog confinement tests for the operator API."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine

from sutradhara.api import store
from sutradhara.api.sources import CatalogError, resolve_landing, resolve_source
from tests.api.conftest import auth_headers, make_api_app


def test_resolve_source_and_landing_known_ids(
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

    assert resolve_source("card-a").path == (sources / "card-a").resolve()
    assert resolve_landing("main").path == (landings / "main").resolve()


def test_catalog_rejects_paths_dotfiles_and_symlink_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sources = tmp_path / "sources"
    outside = tmp_path / "outside"
    sources.mkdir()
    outside.mkdir()
    (sources / "escape").symlink_to(outside, target_is_directory=True)
    monkeypatch.setenv("SUTRA_RECEIVE_SOURCE_ROOT", str(sources))

    for source_id in ("/etc/passwd", "../outside", ".hidden", str(tmp_path / "api.db")):
        with pytest.raises(CatalogError):
            resolve_source(source_id)
    with pytest.raises(CatalogError):
        resolve_source("escape")


def test_receive_options_return_opaque_ids_and_busy_status(
    api_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sources = tmp_path / "sources"
    landings = tmp_path / "landings"
    (sources / "card-a").mkdir(parents=True)
    (sources / "handoff-b").mkdir()
    (landings / "main").mkdir(parents=True)
    monkeypatch.setenv("SUTRA_RECEIVE_SOURCE_ROOT", str(sources))
    monkeypatch.setenv("SUTRA_RECEIVE_LANDING_ROOT", str(landings))
    assert store.claim_source(
        api_engine,
        source_id="card-a",
        operator_username="other",
        idempotency_key=str(uuid4()),
    )
    client = TestClient(make_api_app(api_engine))

    response = client.get("/api/receive/options", headers=auth_headers("operator"))

    assert response.status_code == 200
    body = response.json()
    assert body["sources"] == [
        {"sourceId": "card-a", "label": "Card A", "kind": "card", "status": "busy"},
        {
            "sourceId": "handoff-b",
            "label": "Handoff B",
            "kind": "handoff",
            "status": "available",
        },
    ]
    assert body["landings"] == [
        {"landingId": "main", "label": "Main", "status": "available"},
    ]
    assert body["artifactclasses"] == [{"artifactclass": "s-masters", "label": "s-masters"}]
    assert all("/" not in source["sourceId"] for source in body["sources"])
