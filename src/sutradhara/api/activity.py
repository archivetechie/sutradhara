"""Read-model query for the operator-console activity view.

The activity API is a monitoring surface over durable gRPC receive state. It
does not create catalog rows or add new storage; fields without an existing
durable source are returned as ``None`` so the console can render them honestly.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, select

from sutradhara.catalog.session import make_session_factory
from sutradhara.grpc import store as grpc_store
from sutradhara.grpc.status import intake_status

MAX_ACTIVITY_ROWS = 200
MIN_ACTIVITY_DAYS = 1
MAX_ACTIVITY_DAYS = 30
BAD_TERMINAL_STATUSES = {"quarantined", "discrepancy"}
TERMINAL_STATUSES = {"verified", "quarantined", "discrepancy", "aborted"}
_PATH_SEPARATORS = {"/", "\\"}


def read_activity(
    engine: Engine,
    *,
    days: int = 7,
    now: dt.datetime | None = None,
) -> dict[str, object]:
    """Return the ``GET /api/activity`` read model from durable receive state."""

    if days < MIN_ACTIVITY_DAYS or days > MAX_ACTIVITY_DAYS:
        raise ValueError(f"days must be between {MIN_ACTIVITY_DAYS} and {MAX_ACTIVITY_DAYS}")

    local_now = _local_now(now)
    today_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_start = today_start + dt.timedelta(days=1)
    window_start = today_start - dt.timedelta(days=days - 1)

    rows = _grpc_intake_rows(engine)
    window_rows: list[tuple[grpc_store.GrpcIntake, dt.datetime]] = []
    today_rows: list[tuple[grpc_store.GrpcIntake, dt.datetime]] = []
    for row in rows:
        started_local = _aware(row.created_at).astimezone(local_now.tzinfo)
        if today_start <= started_local < tomorrow_start:
            today_rows.append((row, started_local))
        if window_start <= started_local < tomorrow_start:
            window_rows.append((row, started_local))

    ordered_window = sorted(
        window_rows,
        key=lambda item: (item[1], item[0].intake_id),
        reverse=True,
    )
    modeled_rows = [_activity_row(row) for row, _started in ordered_window]
    intakes = modeled_rows[:MAX_ACTIVITY_ROWS]
    verified_today = [
        _activity_row(row)
        for row, _started in today_rows
        if _status_for(row)[0] == "verified"
    ]
    return {
        "summary": {
            "receivesToday": len(today_rows),
            "bytesVerifiedToday": _verified_bytes_today(verified_today),
            "openDiscrepancies": sum(
                1 for item in modeled_rows if item["status"] in BAD_TERMINAL_STATUSES
            ),
        },
        "intakes": intakes,
    }


def _grpc_intake_rows(engine: Engine) -> list[grpc_store.GrpcIntake]:
    factory = make_session_factory(engine)
    with factory() as session:
        rows = list(session.scalars(select(grpc_store.GrpcIntake)))
        for row in rows:
            session.expunge(row)
    return rows


def _activity_row(row: grpc_store.GrpcIntake) -> dict[str, object]:
    status, errors = _status_for(row)
    receipt_bytes = _receipt_bytes(row)
    started_at = _aware(row.created_at)
    return {
        "intakeId": row.intake_id,
        "batchLabel": row.label,
        "sourceLabel": _source_label(row),
        "deviceId": row.device_id or None,
        "operator": row.operator,
        "artifactclass": row.artifactclass,
        "status": status,
        "startedAt": _iso(started_at),
        "completedAt": _completed_at(row, status),
        "bytesTotal": receipt_bytes,
        "bytesReceived": receipt_bytes,
        "errors": errors,
    }


def _status_for(row: grpc_store.GrpcIntake) -> tuple[str, list[str]]:
    status, errors = intake_status(row)
    return status, errors


def _source_label(row: grpc_store.GrpcIntake) -> str:
    if row.card_id:
        return row.card_id
    if row.device_id and row.source_kind in {"card", "drive"}:
        return row.device_id
    if row.source_ref and not _looks_like_path(row.source_ref):
        return row.source_ref
    return row.source_kind or row.intake_id


def _looks_like_path(value: str) -> bool:
    return any(separator in value for separator in _PATH_SEPARATORS)


def _receipt_bytes(row: grpc_store.GrpcIntake) -> int | None:
    path = Path(row.landing_root) / row.intake_id / "receive-receipts.jsonl"
    if not path.exists():
        return None
    total = 0
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line:
                continue
            payload = json.loads(line)
            total += int(payload["bytes"])
    except Exception:
        return None
    return total


def _verified_bytes_today(rows: list[dict[str, object]]) -> int | None:
    if not rows:
        return 0
    total = 0
    for row in rows:
        value = row["bytesTotal"]
        if value is None:
            return None
        total += int(value)
    return total


def _completed_at(row: grpc_store.GrpcIntake, status: str) -> str | None:
    if status not in TERMINAL_STATUSES:
        return None
    if status == "verified":
        marker_value = _marker_timestamp(row, "intake.verified.json", "registered_at")
        if marker_value is not None:
            return marker_value
    if status == "quarantined":
        marker_value = _marker_timestamp(row, "intake.quarantined.json", "quarantined_at")
        if marker_value is not None:
            return marker_value
    if status == "discrepancy":
        marker_value = _marker_timestamp(row, "intake.discrepancy.json", None)
        if marker_value is not None:
            return marker_value
    return _iso(_aware(row.updated_at))


def _marker_timestamp(
    row: grpc_store.GrpcIntake,
    marker_name: str,
    payload_key: str | None,
) -> str | None:
    marker = Path(row.landing_root) / row.intake_id / marker_name
    if not marker.exists():
        return None
    if payload_key is not None:
        try:
            payload = json.loads(marker.read_text(encoding="utf-8"))
        except Exception:
            payload = None
        if isinstance(payload, dict):
            value = payload.get(payload_key)
            if isinstance(value, str) and value:
                try:
                    return _iso(_parse_datetime(value))
                except ValueError:
                    pass
    try:
        return _iso(dt.datetime.fromtimestamp(marker.stat().st_mtime, tz=dt.UTC))
    except OSError:
        return None


def _parse_datetime(value: str) -> dt.datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    return _aware(dt.datetime.fromisoformat(normalized))


def _local_now(now: dt.datetime | None) -> dt.datetime:
    if now is None:
        return dt.datetime.now().astimezone()
    if now.tzinfo is None:
        return now.replace(tzinfo=dt.datetime.now().astimezone().tzinfo)
    return now


def _aware(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _iso(value: dt.datetime) -> str:
    return _aware(value).isoformat()
