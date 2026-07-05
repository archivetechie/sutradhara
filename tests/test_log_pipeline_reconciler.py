"""Tests for the log_pipeline self-monitoring reconciler."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine

from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.hdcache import models as _hdcache_models  # noqa: F401
from sutradhara.jobs.models import ReconciliationCondition
from sutradhara.jobs.reconcilers import log_pipeline
from sutradhara.jobs.reconcilers.conditions import CONDITION_OPEN, CONDITION_SATISFIED
from sutradhara.logs_store import VictoriaLogsUnavailable
from tests.api.conftest import auth_headers, make_api_app


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    db_engine = make_engine(f"sqlite:///{tmp_path / 'log_pipeline.db'}")
    create_all(db_engine)
    yield db_engine
    db_engine.dispose()


def test_log_pipeline_reconciler_opens_and_satisfies_condition(engine: Engine) -> None:
    now = dt.datetime(2026, 7, 5, 12, 0, tzinfo=dt.UTC)

    with session_scope(engine) as session:
        opened = log_pipeline.refresh_condition(
            session,
            client=_ProbeClient(
                newest=now - dt.timedelta(seconds=30),
                heartbeat=now - dt.timedelta(minutes=7),
            ),
            now=now,
        )
        assert opened.domain == log_pipeline.DOMAIN
        assert opened.target_key == log_pipeline.TARGET_KEY
        assert opened.condition == CONDITION_OPEN
        assert opened.reason == "heartbeat-stale"
        assert opened.message == "log_pipeline heartbeat age is 420s"

    with session_scope(engine) as session:
        recovered = log_pipeline.refresh_condition(
            session,
            client=_ProbeClient(
                newest=now - dt.timedelta(seconds=10),
                heartbeat=now - dt.timedelta(seconds=20),
            ),
            now=now,
        )
        assert recovered.condition == CONDITION_SATISFIED
        assert recovered.reason is None
        assert recovered.message is None


def test_log_pipeline_reconciler_query_failure_opens_condition(engine: Engine) -> None:
    now = dt.datetime(2026, 7, 5, 12, 0, tzinfo=dt.UTC)

    with session_scope(engine) as session:
        row = log_pipeline.refresh_condition(session, client=_ProbeClient(fail=True), now=now)

        assert row.condition == CONDITION_OPEN
        assert row.reason == "query-failed"
        assert row.message == "VictoriaLogs self-monitor query failed: VictoriaLogsUnavailable"


def test_log_pipeline_condition_surfaces_with_admin_only_detail(engine: Engine) -> None:
    with session_scope(engine) as session:
        session.add(
            ReconciliationCondition(
                domain=log_pipeline.DOMAIN,
                target_key=log_pipeline.TARGET_KEY,
                observed_state="missing",
                condition=CONDITION_OPEN,
                reason="heartbeat-stale",
                message="log_pipeline heartbeat age is 420s",
                updated_at=dt.datetime(2026, 7, 5, 12, 0, tzinfo=dt.UTC),
            )
        )

    client = TestClient(make_api_app(engine))
    viewer = client.get("/api/ui/reconciliation", headers=auth_headers("viewer"))
    admin = client.get("/api/ui/reconciliation", headers=auth_headers("admin"))

    assert viewer.status_code == 200
    condition = viewer.json()["conditions"][0]
    assert condition["domain"] == log_pipeline.DOMAIN
    assert condition["cause"] == "The log pipeline heartbeat is stale"
    assert condition["owner"] == "archive operator"
    assert "message" not in condition

    assert admin.status_code == 200
    admin_condition = admin.json()["conditions"][0]
    assert admin_condition["message"] == "log_pipeline heartbeat age is 420s"


class _ProbeClient:
    def __init__(
        self,
        *,
        newest: dt.datetime | None = None,
        heartbeat: dt.datetime | None = None,
        fail: bool = False,
    ) -> None:
        self.newest = newest
        self.heartbeat = heartbeat
        self.fail = fail

    def query(self, logsql: str) -> list[dict[str, Any]]:
        if self.fail:
            raise VictoriaLogsUnavailable("down")
        value = self.heartbeat if 'source:="log_pipeline"' in logsql else self.newest
        return [{"newest_time": value.isoformat().replace("+00:00", "Z") if value else ""}]
