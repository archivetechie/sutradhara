"""Durable idempotency tests for the receive HTTP API."""

from __future__ import annotations

import datetime as dt
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, inspect

from sutradhara.api import store
from sutradhara.catalog.session import create_all, make_engine, session_scope
from tests.api.conftest import make_api_app, post_headers


def test_same_key_same_body_replays_without_second_receive(
    api_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _fake_receive_app(api_engine, tmp_path, monkeypatch)
    client = TestClient(app)
    key = str(uuid4())
    body = _body(key)

    first = client.post("/api/receive", headers=post_headers("operator"), json=body)
    second = client.post("/api/receive", headers=post_headers("operator"), json=body)

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json() == {"intakeId": "intake-001", "status": "received"}
    assert app.state.receive_calls == 1


def test_same_key_different_body_is_409(
    api_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _fake_receive_app(api_engine, tmp_path, monkeypatch)
    client = TestClient(app)
    key = str(uuid4())

    assert client.post("/api/receive", headers=post_headers("operator"), json=_body(key)).status_code == 200
    response = client.post(
        "/api/receive",
        headers=post_headers("operator"),
        json={**_body(key), "label": "changed"},
    )

    assert response.status_code == 409
    assert response.json()["error"] == "idempotency_conflict"
    assert app.state.receive_calls == 1


def test_concurrent_same_key_runs_receive_once(
    api_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _fake_receive_app(api_engine, tmp_path, monkeypatch, delay=0.2)
    client = TestClient(app)
    body = _body(str(uuid4()))
    responses = []

    def post() -> None:
        responses.append(client.post("/api/receive", headers=post_headers("operator"), json=body))

    threads = [threading.Thread(target=post), threading.Thread(target=post)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert [response.status_code for response in responses] == [200, 200]
    assert {response.json()["intakeId"] for response in responses} == {"intake-001"}
    assert app.state.receive_calls == 1


def test_recent_in_progress_idempotency_is_not_reclaimed(api_engine: Engine) -> None:
    key = str(uuid4())
    now = dt.datetime.now(dt.UTC)
    with session_scope(api_engine) as session:
        session.add(
            store.IdempotencyRecord(
                operator_username="owner",
                endpoint=store.RECEIVE_ENDPOINT,
                idempotency_key=key,
                request_hash="abc",
                status="in_progress",
                created_at=now - dt.timedelta(hours=2),
                updated_at=now,
                last_heartbeat=now,
            )
        )

    decision = store.begin_idempotency(
        api_engine,
        operator_username="owner",
        endpoint=store.RECEIVE_ENDPOINT,
        idempotency_key=key,
        request_hash="abc",
        ttl=dt.timedelta(minutes=30),
    )

    assert decision.state == "in_progress"


def test_stale_in_progress_idempotency_is_reclaimable(api_engine: Engine) -> None:
    key = str(uuid4())
    old = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
    with session_scope(api_engine) as session:
        session.add(
            store.IdempotencyRecord(
                operator_username="owner",
                endpoint=store.RECEIVE_ENDPOINT,
                idempotency_key=key,
                request_hash="abc",
                status="in_progress",
                created_at=old,
                updated_at=old,
                last_heartbeat=old,
            )
        )

    decision = store.begin_idempotency(
        api_engine,
        operator_username="owner",
        endpoint=store.RECEIVE_ENDPOINT,
        idempotency_key=key,
        request_hash="abc",
        ttl=dt.timedelta(minutes=30),
    )

    assert decision.state == "claimed"


def test_create_all_registers_api_store_tables(tmp_path: Path) -> None:
    engine = make_engine(f"sqlite:///{tmp_path / 'schema.db'}")
    try:
        create_all(engine)
        tables = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()

    assert {"idempotency_record", "source_claim"} <= tables


def _fake_receive_app(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    delay: float = 0.0,
):
    sources = tmp_path / "sources"
    landings = tmp_path / "landings"
    (sources / "card-a").mkdir(parents=True, exist_ok=True)
    (landings / "main").mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("SUTRA_RECEIVE_SOURCE_ROOT", str(sources))
    monkeypatch.setenv("SUTRA_RECEIVE_LANDING_ROOT", str(landings))
    app = make_api_app(engine)
    app.state.receive_calls = 0
    lock = threading.Lock()

    def fake_receive(*_args: object, **_kwargs: object) -> SimpleNamespace:
        with lock:
            app.state.receive_calls += 1
        if delay:
            time.sleep(delay)
        return SimpleNamespace(intake_id="intake-001", intake_dir=tmp_path / "intake-001")

    app.state.receive_source = fake_receive
    app.state.register_intake = lambda *_args, **_kwargs: None
    return app


def _body(key: str) -> dict[str, str]:
    return {
        "sourceId": "card-a",
        "landingId": "main",
        "artifactclass": "s-masters",
        "idempotencyKey": key,
    }
