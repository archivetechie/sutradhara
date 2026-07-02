"""Read-model tests for the operator-console activity API.

The activity model is intentionally derived from existing ``grpc_intake`` rows
and on-disk receipt/marker files. These tests pin the contract-facing behavior
without adding any new durable schema.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from sqlalchemy import Engine

from sutradhara.api.activity import MAX_ACTIVITY_ROWS, read_activity
from sutradhara.catalog.session import session_scope
from sutradhara.grpc import store as grpc_store

BERLIN_SUMMER = dt.timezone(dt.timedelta(hours=2))


def test_activity_read_model_materializes_contract_fields_and_aggregates(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    now = dt.datetime(2026, 7, 2, 12, 0, tzinfo=BERLIN_SUMMER)
    verified_at = dt.datetime(2026, 7, 2, 8, 30, tzinfo=dt.UTC)
    _insert_intake(
        api_engine,
        tmp_path,
        intake_id="intake-verified",
        operator="owner",
        device_id="mac-1",
        card_id="card-1",
        source_kind="card",
        source_ref="DCIM/100APPLE",
        label="Morning shoot",
        artifactclass="s-masters",
        state="committed",
        created_at=dt.datetime(2026, 7, 2, 7, 0, tzinfo=dt.UTC),
    )
    _write_receipts(tmp_path, "intake-verified", [5, 7])
    _write_marker(
        tmp_path,
        "intake-verified",
        "intake.verified.json",
        {"status": "registered", "registered_at": verified_at.isoformat()},
    )
    _insert_intake(
        api_engine,
        tmp_path,
        intake_id="intake-discrepancy",
        operator="other",
        device_id="mac-2",
        card_id=None,
        source_kind="drive",
        source_ref="Projects/Event",
        label=None,
        artifactclass="s-masters",
        state="committed",
        created_at=dt.datetime(2026, 7, 2, 9, 0, tzinfo=dt.UTC),
    )
    _write_marker(
        tmp_path,
        "intake-discrepancy",
        "intake.discrepancy.json",
        {"status": "discrepancy", "details": {"missing": ["clip.mov"]}},
    )

    payload = read_activity(api_engine, days=7, now=now)

    assert payload["summary"] == {
        "receivesToday": 2,
        "bytesVerifiedToday": 12,
        "openDiscrepancies": 1,
    }
    intakes = payload["intakes"]
    assert isinstance(intakes, list)
    assert [row["intakeId"] for row in intakes] == ["intake-discrepancy", "intake-verified"]
    assert set(intakes[0]) == {
        "intakeId",
        "batchLabel",
        "sourceLabel",
        "deviceId",
        "operator",
        "artifactclass",
        "status",
        "startedAt",
        "completedAt",
        "bytesTotal",
        "bytesReceived",
        "errors",
    }
    assert intakes[0]["sourceLabel"] == "mac-2"
    assert intakes[0]["operator"] == "other"
    assert intakes[0]["status"] == "discrepancy"
    assert intakes[0]["errors"] == ["missing: ['clip.mov']"]
    assert "/" not in str(intakes[0]["sourceLabel"])
    assert intakes[1] == {
        "intakeId": "intake-verified",
        "batchLabel": "Morning shoot",
        "sourceLabel": "card-1",
        "deviceId": "mac-1",
        "operator": "owner",
        "artifactclass": "s-masters",
        "status": "verified",
        "startedAt": "2026-07-02T07:00:00+00:00",
        "completedAt": verified_at.isoformat(),
        "bytesTotal": 12,
        "bytesReceived": 12,
        "errors": [],
    }


def test_activity_read_model_day_boundaries_are_server_local(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    now = dt.datetime(2026, 7, 2, 12, 0, tzinfo=BERLIN_SUMMER)
    _insert_intake(
        api_engine,
        tmp_path,
        intake_id="previous-local-day",
        created_at=dt.datetime(2026, 7, 1, 21, 59, tzinfo=dt.UTC),
    )
    _insert_intake(
        api_engine,
        tmp_path,
        intake_id="start-of-local-day",
        created_at=dt.datetime(2026, 7, 1, 22, 0, tzinfo=dt.UTC),
    )

    payload = read_activity(api_engine, days=1, now=now)

    assert payload["summary"]["receivesToday"] == 1
    assert [row["intakeId"] for row in payload["intakes"]] == ["start-of-local-day"]


def test_activity_read_model_caps_newest_rows(api_engine: Engine, tmp_path: Path) -> None:
    now = dt.datetime(2026, 7, 2, 12, 0, tzinfo=BERLIN_SUMMER)
    for index in range(MAX_ACTIVITY_ROWS + 5):
        _insert_intake(
            api_engine,
            tmp_path,
            intake_id=f"intake-{index:03d}",
            created_at=dt.datetime(2026, 7, 2, 0, 0, tzinfo=dt.UTC)
            + dt.timedelta(minutes=index),
        )

    payload = read_activity(api_engine, days=1, now=now)

    intakes = payload["intakes"]
    assert len(intakes) == MAX_ACTIVITY_ROWS
    assert intakes[0]["intakeId"] == f"intake-{MAX_ACTIVITY_ROWS + 4:03d}"
    assert intakes[-1]["intakeId"] == "intake-005"
    assert payload["summary"]["receivesToday"] == MAX_ACTIVITY_ROWS + 5


def test_activity_read_model_bytes_verified_today_null_when_receipts_absent(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    now = dt.datetime(2026, 7, 2, 12, 0, tzinfo=BERLIN_SUMMER)
    _insert_intake(
        api_engine,
        tmp_path,
        intake_id="verified-no-receipts",
        state="committed",
        created_at=dt.datetime(2026, 7, 2, 8, 0, tzinfo=dt.UTC),
    )
    _write_marker(
        tmp_path,
        "verified-no-receipts",
        "intake.verified.json",
        {"status": "registered", "registered_at": "2026-07-02T08:30:00+00:00"},
    )

    payload = read_activity(api_engine, days=1, now=now)

    assert payload["summary"]["bytesVerifiedToday"] is None
    assert payload["intakes"][0]["bytesTotal"] is None
    assert payload["intakes"][0]["bytesReceived"] is None


def test_activity_read_model_open_discrepancies_counts_only_bad_terminal_window(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    now = dt.datetime(2026, 7, 2, 12, 0, tzinfo=BERLIN_SUMMER)
    _insert_intake(
        api_engine,
        tmp_path,
        intake_id="quarantined",
        state="committed",
        created_at=dt.datetime(2026, 7, 2, 8, 0, tzinfo=dt.UTC),
    )
    _write_marker(tmp_path, "quarantined", "intake.quarantined.json", {"status": "quarantined"})
    _insert_intake(
        api_engine,
        tmp_path,
        intake_id="discrepancy",
        state="committed",
        created_at=dt.datetime(2026, 7, 2, 9, 0, tzinfo=dt.UTC),
    )
    _write_marker(tmp_path, "discrepancy", "intake.discrepancy.json", {"status": "discrepancy"})
    _insert_intake(
        api_engine,
        tmp_path,
        intake_id="verified",
        state="committed",
        created_at=dt.datetime(2026, 7, 2, 10, 0, tzinfo=dt.UTC),
    )
    _write_marker(tmp_path, "verified", "intake.verified.json", {"status": "registered"})
    _insert_intake(
        api_engine,
        tmp_path,
        intake_id="old-quarantined",
        state="committed",
        created_at=dt.datetime(2026, 6, 30, 10, 0, tzinfo=dt.UTC),
    )
    _write_marker(
        tmp_path,
        "old-quarantined",
        "intake.quarantined.json",
        {"status": "quarantined"},
    )

    payload = read_activity(api_engine, days=1, now=now)

    assert payload["summary"]["openDiscrepancies"] == 2
    assert {row["intakeId"] for row in payload["intakes"]} == {
        "quarantined",
        "discrepancy",
        "verified",
    }


def _insert_intake(
    engine: Engine,
    landing_root: Path,
    *,
    intake_id: str,
    operator: str = "owner",
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
            source_plan_digest=f"{intake_id}".encode("utf-8").hex().ljust(64, "0")[:64],
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
    payload: dict[str, Any],
) -> None:
    intake_dir = landing_root / intake_id
    intake_dir.mkdir(parents=True, exist_ok=True)
    (intake_dir / marker_name).write_text(json.dumps(payload), encoding="utf-8")
