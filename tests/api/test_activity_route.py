"""HTTP tests for ``GET /api/activity`` on the operator-console API."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import Engine

from sutradhara.catalog.session import session_scope
from sutradhara.grpc import store as grpc_store
from tests.api.conftest import auth_headers, make_api_app

BERLIN_SUMMER = dt.timezone(dt.timedelta(hours=2))


def test_activity_route_viewer_gets_contract_shape(api_engine: Engine, tmp_path: Path) -> None:
    now = dt.datetime(2026, 7, 2, 12, 0, tzinfo=BERLIN_SUMMER)
    _insert_intake(
        api_engine,
        tmp_path,
        intake_id="intake-1",
        operator="ada",
        device_id="mac-1",
        card_id="card-1",
        source_ref="DCIM/100APPLE",
        label="Morning shoot",
        state="committed",
        created_at=dt.datetime(2026, 7, 2, 7, 0, tzinfo=dt.UTC),
    )
    _write_receipts(tmp_path, "intake-1", [5])
    _write_marker(
        tmp_path,
        "intake-1",
        "intake.verified.json",
        {"status": "registered", "registered_at": "2026-07-02T08:30:00+00:00"},
    )
    app = make_api_app(api_engine)
    app.state.activity_now = now
    client = TestClient(app)

    response = client.get("/api/activity", headers=auth_headers("viewer"))

    assert response.status_code == 200
    assert response.json() == {
        "summary": {
            "receivesToday": 1,
            "bytesVerifiedToday": 5,
            "openDiscrepancies": 0,
        },
        "intakes": [
            {
                "intakeId": "intake-1",
                "batchLabel": "Morning shoot",
                "sourceLabel": "card-1",
                "deviceId": "mac-1",
                "operator": "ada",
                "artifactclass": "s-masters",
                "status": "verified",
                "startedAt": "2026-07-02T07:00:00+00:00",
                "completedAt": "2026-07-02T08:30:00+00:00",
                "bytesTotal": 5,
                "bytesReceived": 5,
                "errors": [],
            }
        ],
    }


def test_activity_route_requires_view_capability(api_engine: Engine) -> None:
    app = make_api_app(api_engine)
    app.state.activity_now = dt.datetime(2026, 7, 2, 12, 0, tzinfo=BERLIN_SUMMER)
    client = TestClient(app)

    response = client.get(
        "/api/activity",
        headers={
            "X-Authentik-Username": "ada",
            "X-Authentik-Name": "Ada Operator",
            "X-Authentik-Groups": "",
        },
    )

    assert response.status_code == 403
    assert response.json() == {"error": "forbidden", "detail": "operator has no sutradhara role"}


def test_activity_route_validates_days_range(api_engine: Engine) -> None:
    app = make_api_app(api_engine)
    app.state.activity_now = dt.datetime(2026, 7, 2, 12, 0, tzinfo=BERLIN_SUMMER)
    client = TestClient(app)

    too_low = client.get("/api/activity", headers=auth_headers("viewer"), params={"days": 0})
    too_high = client.get("/api/activity", headers=auth_headers("viewer"), params={"days": 31})

    assert too_low.status_code == 400
    assert too_low.json() == {"error": "bad_request", "detail": "days must be between 1 and 30"}
    assert too_high.status_code == 400
    assert too_high.json() == {"error": "bad_request", "detail": "days must be between 1 and 30"}


def test_activity_route_is_cross_operator_read_model(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    now = dt.datetime(2026, 7, 2, 12, 0, tzinfo=BERLIN_SUMMER)
    _insert_intake(
        api_engine,
        tmp_path,
        intake_id="ada-intake",
        operator="ada",
        created_at=dt.datetime(2026, 7, 2, 7, 0, tzinfo=dt.UTC),
    )
    _insert_intake(
        api_engine,
        tmp_path,
        intake_id="other-intake",
        operator="other",
        device_id="mac-2",
        card_id="card-2",
        created_at=dt.datetime(2026, 7, 2, 8, 0, tzinfo=dt.UTC),
    )
    app = make_api_app(api_engine)
    app.state.activity_now = now
    client = TestClient(app)

    response = client.get("/api/activity", headers=auth_headers("viewer"))

    assert response.status_code == 200
    assert {row["operator"] for row in response.json()["intakes"]} == {"ada", "other"}


def _insert_intake(
    engine: Engine,
    landing_root: Path,
    *,
    intake_id: str,
    operator: str = "ada",
    device_id: str = "mac-1",
    card_id: str | None = "card-1",
    source_kind: str = "card",
    source_ref: str | None = "card-1",
    label: str | None = "Batch",
    artifactclass: str = "s-masters",
    state: str = "streaming",
    created_at: dt.datetime,
) -> None:
    with session_scope(engine) as session:
        row = grpc_store.insert_intake(
            session,
            intake_id=intake_id,
            operator=operator,
            device_id=device_id,
            idempotency_key=f"key-{intake_id}",
            source_plan_digest=f"{intake_id}".encode().hex().ljust(64, "0")[:64],
            artifactclass=artifactclass,
            source_kind=source_kind,
            source_ref=source_ref,
            label=label,
            landing_root=str(landing_root),
        )
        row.state = state
        row.card_id = card_id
        row.created_at = created_at
        row.updated_at = created_at + dt.timedelta(minutes=10)


def _write_receipts(landing_root: Path, intake_id: str, sizes: list[int]) -> None:
    intake_dir = landing_root / intake_id
    intake_dir.mkdir(parents=True, exist_ok=True)
    lines = [
        json.dumps(
            {
                "relpath": f"clip-{index}.mov",
                "server_sha256": f"{index}".zfill(64),
                "bytes": size,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        for index, size in enumerate(sizes)
    ]
    (intake_dir / "receive-receipts.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_marker(
    landing_root: Path,
    intake_id: str,
    marker_name: str,
    payload: dict[str, object],
) -> None:
    intake_dir = landing_root / intake_id
    intake_dir.mkdir(parents=True, exist_ok=True)
    (intake_dir / marker_name).write_text(json.dumps(payload), encoding="utf-8")
