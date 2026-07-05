"""HTTP contract tests for the unified logs read-model route."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine

from sutradhara.logs_store import VictoriaLogsClient, VictoriaLogsUnavailable
from tests.api.conftest import auth_headers, make_api_app


def test_logs_route_contract_shape_and_non_admin_tier(api_engine: Engine) -> None:
    app = make_api_app(api_engine)
    app.state.log_store_client = _FakeLogStore(rows=[_row(seq=2), _row(seq=1)])
    client = TestClient(app)

    response = client.get("/api/ui/logs?limit=10", headers=auth_headers("troubleshoot"))

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"rows", "total", "truncated", "next_cursor", "prev_cursor", "histogram"}
    assert body["total"] == 2
    assert body["truncated"] is False
    assert body["next_cursor"] is None
    assert body["prev_cursor"] is None
    assert body["histogram"] == []
    row = body["rows"][0]
    assert set(row) == {
        "ts",
        "ingest_ts",
        "seq",
        "source",
        "host",
        "unit",
        "container",
        "severity",
        "level",
        "trace_id",
        "message",
        "attrs",
        "entity_refs",
    }
    assert row["ts"] == "2026-07-05T12:00:01.000000Z"
    assert row["attrs"]["path"] == "<path>"
    assert "raw" not in row


def test_logs_route_admin_with_logs_sees_raw_and_unsanitized_attrs(api_engine: Engine) -> None:
    app = make_api_app(api_engine)
    app.state.log_store_client = _FakeLogStore(rows=[_row(seq=1)])
    client = TestClient(app)

    response = client.get(
        "/api/ui/logs?limit=1",
        headers=auth_headers("sutradhara-admin|sutradhara-troubleshoot"),
    )

    assert response.status_code == 200
    row = response.json()["rows"][0]
    assert row["raw"] == "raw /vault/private.mov"
    assert row["attrs"]["path"] == "/vault/private.mov"


def test_logs_route_requires_can_logs_not_admin_or_plain_view(api_engine: Engine) -> None:
    client = TestClient(make_api_app(api_engine))

    admin = client.get("/api/ui/logs", headers=auth_headers("admin"))
    viewer = client.get("/api/ui/logs", headers=auth_headers("viewer"))

    assert admin.status_code == 403
    assert admin.json() == {
        "detail": {"error": "forbidden", "detail": "operator lacks logs capability"}
    }
    assert viewer.status_code == 403
    assert viewer.json()["detail"]["error"] == "forbidden"


def test_logs_route_per_source_floors_are_rendered_as_safe_logsql(
    api_engine: Engine,
) -> None:
    fake = _FakeLogStore(rows=[])
    app = make_api_app(api_engine)
    app.state.log_store_client = fake
    client = TestClient(app)

    response = client.get(
        "/api/ui/logs?sources=sutra-worker,remanence&level=warn,remanence:error",
        headers=auth_headers("troubleshoot"),
    )

    assert response.status_code == 200
    row_query = fake.row_queries[-1]
    assert 'source:in("sutra-worker", "remanence")' in row_query
    assert '(source:="sutra-worker" level:in("warn", "error", "fatal"))' in row_query
    assert '(source:="remanence" level:in("error", "fatal"))' in row_query


def test_logs_route_cursor_pages_round_trip_ts_seq_boundary(api_engine: Engine) -> None:
    fake = _FakeLogStore(
        rows=[_row(seq=3), _row(seq=2), _row(seq=1)],
        older_rows=[_row(seq=1)],
        newer_rows=[_row(seq=2), _row(seq=3)],
    )
    app = make_api_app(api_engine)
    app.state.log_store_client = fake
    client = TestClient(app)

    first = client.get("/api/ui/logs?limit=2", headers=auth_headers("troubleshoot"))
    assert first.status_code == 200
    first_body = first.json()
    assert [row["seq"] for row in first_body["rows"]] == [2, 3]

    older = client.get(
        f"/api/ui/logs?limit=2&cursor={first_body['prev_cursor']}",
        headers=auth_headers("troubleshoot"),
    )
    assert older.status_code == 200
    older_body = older.json()
    assert [row["seq"] for row in older_body["rows"]] == [1]

    newer = client.get(
        f"/api/ui/logs?limit=2&cursor={older_body['next_cursor']}",
        headers=auth_headers("troubleshoot"),
    )
    assert newer.status_code == 200
    assert [row["seq"] for row in newer.json()["rows"]] == [2, 3]


def test_logs_route_histogram_shape(api_engine: Engine) -> None:
    app = make_api_app(api_engine)
    app.state.log_store_client = _FakeLogStore(
        rows=[_row(seq=1)],
        histogram_rows=[
            {
                "_time": "2026-07-05T12:00:00Z",
                "source": "sutra-worker",
                "level": "warn",
                "count": "12",
            }
        ],
    )
    client = TestClient(app)

    response = client.get(
        "/api/ui/logs?from=2026-07-05T12:00:00Z&to=2026-07-05T12:10:00Z&histogram=source,level",
        headers=auth_headers("troubleshoot"),
    )

    assert response.status_code == 200
    assert response.json()["histogram"] == [
        {
            "bucket_start": "2026-07-05T12:00:00.000000Z",
            "bucket_end": "2026-07-05T12:00:10.000000Z",
            "source": "sutra-worker",
            "level": "warn",
            "count": 12,
        }
    ]


def test_logs_route_escapes_search_text_in_logsql(api_engine: Engine) -> None:
    fake = _FakeLogStore(rows=[])
    app = make_api_app(api_engine)
    app.state.log_store_client = fake
    client = TestClient(app)
    attack = 'x" | stats count() by (raw) {source="bad"}'

    response = client.get(
        "/api/ui/logs", params={"q": attack}, headers=auth_headers("troubleshoot")
    )

    assert response.status_code == 200
    row_query = fake.row_queries[-1]
    assert 'message:*"x\\" | stats count() by (raw) {source=\\"bad\\"}"*' in row_query
    assert '_msg:*"x\\" | stats count() by (raw) {source=\\"bad\\"}"*' in row_query
    assert 'attrs.*:*"x\\" | stats count() by (raw) {source=\\"bad\\"}"*' in row_query


def test_logs_route_invalid_regex_and_store_down_errors(api_engine: Engine) -> None:
    app = make_api_app(api_engine)
    app.state.log_store_client = _FakeLogStore(rows=[])
    client = TestClient(app)

    bad_regex = client.get(
        "/api/ui/logs?q=[&regex=true",
        headers=auth_headers("troubleshoot"),
    )
    assert bad_regex.status_code == 400
    assert bad_regex.json() == {"detail": {"error": "bad_request", "detail": "invalid regex"}}

    down_app = make_api_app(api_engine)
    down_app.state.log_store_client = _FakeLogStore(rows=[], fail=True)
    down_client = TestClient(down_app)
    unavailable = down_client.get("/api/ui/logs", headers=auth_headers("troubleshoot"))
    assert unavailable.status_code == 503
    assert unavailable.json() == {
        "detail": {"error": "unavailable", "detail": "log store unavailable"}
    }


def test_logs_route_live_smoke_against_local_victorialogs(api_engine: Engine) -> None:
    probe = VictoriaLogsClient(timeout_seconds=0.5)
    try:
        probe.query("* | limit 1")
    except VictoriaLogsUnavailable:
        pytest.skip("local VictoriaLogs is not reachable")
    app = make_api_app(api_engine)
    app.state.log_store_client = VictoriaLogsClient(timeout_seconds=5.0)
    client = TestClient(app)

    response = client.get("/api/ui/logs?from=1d&limit=1", headers=auth_headers("troubleshoot"))

    assert response.status_code == 200
    assert response.json()["rows"]


class _FakeLogStore:
    def __init__(
        self,
        *,
        rows: list[dict[str, Any]],
        older_rows: list[dict[str, Any]] | None = None,
        newer_rows: list[dict[str, Any]] | None = None,
        histogram_rows: list[dict[str, Any]] | None = None,
        fail: bool = False,
    ) -> None:
        self.rows = rows
        self.older_rows = older_rows or []
        self.newer_rows = newer_rows or []
        self.histogram_rows = histogram_rows or []
        self.fail = fail
        self.queries: list[str] = []
        self.row_queries: list[str] = []

    def query(self, logsql: str) -> list[dict[str, Any]]:
        self.queries.append(logsql)
        if self.fail:
            raise VictoriaLogsUnavailable("down")
        if "| stats count() as total" in logsql:
            all_rows = list(reversed(self.rows)) if self.rows else []
            return [
                {
                    "total": str(len(all_rows)),
                    "min_time": all_rows[0]["_time"] if all_rows else "",
                    "max_time": all_rows[-1]["_time"] if all_rows else "",
                }
            ]
        if "| stats by (_time:" in logsql:
            return self.histogram_rows
        self.row_queries.append(logsql)
        if "(_time:<" in logsql:
            return self.older_rows
        if "(_time:>" in logsql:
            return self.newer_rows
        return self.rows


def _row(*, seq: int) -> dict[str, Any]:
    return {
        "_time": f"2026-07-05T12:00:0{seq}.000000Z",
        "ingest_ts": f"2026-07-05T12:00:1{seq}.000000Z",
        "seq": str(seq),
        "source": "sutra-worker" if seq != 3 else "remanence",
        "host": "akash",
        "unit": "sutra-worker.service",
        "severity": "4",
        "level": "warn",
        "message": f"job retry scheduled {seq}",
        "attrs.path": "/vault/private.mov",
        "attrs.job_id": "102",
        "entity_refs": '[{"kind":"job","id":"102","confidence":"high"}]',
        "raw": "raw /vault/private.mov",
    }
