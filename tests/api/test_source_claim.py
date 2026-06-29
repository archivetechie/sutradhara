"""Durable source-claim tests for the receive HTTP API."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine

from sutradhara.api import store
from sutradhara.catalog.session import session_scope
from tests.api.conftest import make_api_app, post_headers


def test_claimed_source_blocks_post_and_options_show_busy(
    api_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _prepare_roots(tmp_path, monkeypatch)
    assert store.claim_source(
        api_engine,
        source_id="card-a",
        operator_username="other",
        idempotency_key=str(uuid4()),
    )
    client = TestClient(make_api_app(api_engine))

    options = client.get("/api/receive/options", headers=post_headers("operator"))
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

    assert options.status_code == 200
    assert options.json()["sources"][0]["status"] == "busy"
    assert response.status_code == 409
    assert response.json()["error"] == "source_busy"


def test_claim_released_after_success(
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
            "idempotencyKey": str(uuid4()),
        },
    )

    assert response.status_code == 200
    with session_scope(api_engine) as session:
        assert session.get(store.SourceClaim, "card-a") is None


def test_recent_source_claim_is_not_reclaimed(api_engine: Engine) -> None:
    assert store.claim_source(
        api_engine,
        source_id="card-a",
        operator_username="operator-a",
        idempotency_key="key-a",
    )

    claimed = store.claim_source(
        api_engine,
        source_id="card-a",
        operator_username="operator-b",
        idempotency_key="key-b",
        ttl=dt.timedelta(minutes=30),
    )

    assert claimed is False


def test_stale_source_claim_is_reclaimable(api_engine: Engine) -> None:
    old = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
    with session_scope(api_engine) as session:
        session.add(
            store.SourceClaim(
                source_id="card-a",
                operator_username="operator-a",
                idempotency_key="key-a",
                created_at=old,
                updated_at=old,
                last_heartbeat=old,
            )
        )

    claimed = store.claim_source(
        api_engine,
        source_id="card-a",
        operator_username="operator-b",
        idempotency_key="key-b",
        ttl=dt.timedelta(minutes=30),
    )

    assert claimed is True
    with session_scope(api_engine) as session:
        claim = session.get(store.SourceClaim, "card-a")
        assert claim is not None
        assert claim.operator_username == "operator-b"


def _prepare_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sources = tmp_path / "sources"
    landings = tmp_path / "landings"
    (sources / "card-a").mkdir(parents=True)
    (sources / "card-a" / "clip.mov").write_bytes(b"video")
    (landings / "main").mkdir(parents=True)
    monkeypatch.setenv("SUTRA_RECEIVE_SOURCE_ROOT", str(sources))
    monkeypatch.setenv("SUTRA_RECEIVE_LANDING_ROOT", str(landings))
