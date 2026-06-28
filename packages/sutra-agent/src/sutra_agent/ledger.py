"""Durable local receive ledger for the Sutradhara edge agent.

The server catalog remains authoritative for archive custody. This ledger is
only edge-side operator state: what this agent attempted, where the intake lives,
and whether the server has written a release-safe verification marker.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sutradhara_receive import ConfirmationResult, ReceiveResult

LEDGER_SCHEMA = "sutra-agent-ledger-v1"
CONFIRMATION_PENDING = "pending"
CONFIRMATION_VERIFIED = "verified"
CONFIRMATION_QUARANTINED = "quarantined"
CONFIRMATION_STATUSES = {
    CONFIRMATION_PENDING,
    CONFIRMATION_VERIFIED,
    CONFIRMATION_QUARANTINED,
}


class AgentLedgerError(ValueError):
    """Raised when the local agent ledger cannot be read or updated."""


@dataclass(frozen=True)
class ConfirmationSnapshot:
    """Current server-confirmation state for one intake."""

    status: str
    release_ok: bool
    marker_path: Path | None = None
    detail: dict[str, Any] | None = None

    def payload(self) -> dict[str, Any]:
        """Return the JSON representation stored in the ledger and CLI output."""

        return {
            "status": self.status,
            "release_ok": self.release_ok,
            "marker_path": str(self.marker_path) if self.marker_path else None,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class ReceiveRunRecord:
    """One receive attempt tracked by the edge agent."""

    intake_id: str
    intake_dir: Path
    landing: Path
    source: str | None
    source_kind: str
    artifactclass: str
    operator: str
    file_count: int
    total_bytes: int
    skipped_count: int
    started_at: str
    updated_at: str
    confirmation: ConfirmationSnapshot
    resume_of: str | None = None

    def payload(self) -> dict[str, Any]:
        """Return the JSON representation stored in the ledger and CLI output."""

        return {
            "intake_id": self.intake_id,
            "intake_dir": str(self.intake_dir),
            "landing": str(self.landing),
            "source": self.source,
            "source_kind": self.source_kind,
            "artifactclass": self.artifactclass,
            "operator": self.operator,
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
            "skipped_count": self.skipped_count,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "resume_of": self.resume_of,
            "confirmation": self.confirmation.payload(),
        }


def now_iso() -> str:
    """Return a UTC timestamp suitable for ledger records."""

    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


def load_ledger(path: Path) -> dict[str, Any]:
    """Load a ledger payload, returning an empty v1 ledger if it does not exist."""

    if not path.exists():
        return {"schema": LEDGER_SCHEMA, "updated_at": None, "runs": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise AgentLedgerError(f"agent ledger is not valid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise AgentLedgerError(f"agent ledger must be a JSON object: {path}")
    if payload.get("schema") != LEDGER_SCHEMA:
        raise AgentLedgerError(
            f"agent ledger schema mismatch: expected {LEDGER_SCHEMA}, "
            f"actual {payload.get('schema')!r}"
        )
    runs = payload.get("runs")
    if not isinstance(runs, list):
        raise AgentLedgerError("agent ledger field 'runs' must be a list")
    return payload


def ledger_records(path: Path) -> list[ReceiveRunRecord]:
    """Return all receive records from a ledger."""

    payload = load_ledger(path)
    return [_record_from_payload(item) for item in payload["runs"]]


def upsert_receive_record(path: Path, record: ReceiveRunRecord) -> None:
    """Insert or replace one receive record in the local ledger."""

    payload = load_ledger(path)
    runs = [_record_from_payload(item) for item in payload["runs"]]
    replaced = False
    next_runs: list[ReceiveRunRecord] = []
    for existing in runs:
        if existing.intake_id == record.intake_id:
            next_runs.append(record)
            replaced = True
        else:
            next_runs.append(existing)
    if not replaced:
        next_runs.append(record)
    _write_ledger(path, next_runs)


def refresh_confirmation_records(
    path: Path,
    *,
    intake_id: str | None = None,
) -> list[ReceiveRunRecord]:
    """Refresh ledger records from server marker files and persist the result."""

    records = ledger_records(path)
    refreshed: list[ReceiveRunRecord] = []
    now = now_iso()
    for record in records:
        if intake_id is not None and record.intake_id != intake_id:
            refreshed.append(record)
            continue
        marker = inspect_confirmation_marker(record.intake_dir)
        refreshed.append(
            ReceiveRunRecord(
                intake_id=record.intake_id,
                intake_dir=record.intake_dir,
                landing=record.landing,
                source=record.source,
                source_kind=record.source_kind,
                artifactclass=record.artifactclass,
                operator=record.operator,
                file_count=record.file_count,
                total_bytes=record.total_bytes,
                skipped_count=record.skipped_count,
                started_at=record.started_at,
                updated_at=now,
                confirmation=marker,
                resume_of=record.resume_of,
            )
        )
    if intake_id is not None and not any(record.intake_id == intake_id for record in records):
        raise AgentLedgerError(f"intake is not tracked in agent ledger: {intake_id}")
    _write_ledger(path, refreshed)
    return [record for record in refreshed if intake_id is None or record.intake_id == intake_id]


def record_from_receive_result(
    result: ReceiveResult,
    *,
    landing: Path,
    source: Path | None,
    source_kind: str,
    artifactclass: str,
    operator: str,
    confirmation: ConfirmationSnapshot,
    resume_of: str | None,
) -> ReceiveRunRecord:
    """Build a ledger record from a completed `sutradhara_receive` result."""

    timestamp = now_iso()
    return ReceiveRunRecord(
        intake_id=result.intake_id,
        intake_dir=result.intake_dir,
        landing=landing,
        source=str(source) if source is not None else None,
        source_kind=source_kind,
        artifactclass=artifactclass,
        operator=operator,
        file_count=result.file_count,
        total_bytes=result.total_bytes,
        skipped_count=result.skipped_count,
        started_at=timestamp,
        updated_at=timestamp,
        confirmation=confirmation,
        resume_of=resume_of,
    )


def confirmation_from_wait_result(
    confirmation: ConfirmationResult | None,
) -> ConfirmationSnapshot:
    """Convert receive's polling result into the agent's durable status model."""

    if confirmation is None or confirmation.status == "timeout":
        return ConfirmationSnapshot(status=CONFIRMATION_PENDING, release_ok=False)
    if confirmation.release_ok:
        return ConfirmationSnapshot(
            status=CONFIRMATION_VERIFIED,
            release_ok=True,
            marker_path=confirmation.marker_path,
            detail=confirmation.detail,
        )
    return ConfirmationSnapshot(
        status=CONFIRMATION_QUARANTINED,
        release_ok=False,
        marker_path=confirmation.marker_path,
        detail=confirmation.detail,
    )


def inspect_confirmation_marker(intake_dir: Path) -> ConfirmationSnapshot:
    """Inspect server marker files for one received intake."""

    quarantined = intake_dir / "intake.quarantined.json"
    if quarantined.is_file():
        return ConfirmationSnapshot(
            status=CONFIRMATION_QUARANTINED,
            release_ok=False,
            marker_path=quarantined,
            detail=_read_json_object(quarantined),
        )
    verified = intake_dir / "intake.verified.json"
    if verified.is_file():
        return ConfirmationSnapshot(
            status=CONFIRMATION_VERIFIED,
            release_ok=True,
            marker_path=verified,
            detail=_read_json_object(verified),
        )
    return ConfirmationSnapshot(status=CONFIRMATION_PENDING, release_ok=False)


def _write_ledger(path: Path, records: list[ReceiveRunRecord]) -> None:
    payload: dict[str, Any] = {
        "schema": LEDGER_SCHEMA,
        "updated_at": now_iso(),
        "runs": [record.payload() for record in records],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def _record_from_payload(payload: Any) -> ReceiveRunRecord:
    if not isinstance(payload, dict):
        raise AgentLedgerError("agent ledger run entries must be JSON objects")
    confirmation_payload = payload.get("confirmation")
    if not isinstance(confirmation_payload, dict):
        raise AgentLedgerError("agent ledger run missing confirmation object")
    return ReceiveRunRecord(
        intake_id=_required_string(payload, "intake_id"),
        intake_dir=Path(_required_string(payload, "intake_dir")),
        landing=Path(_required_string(payload, "landing")),
        source=_optional_string(payload, "source"),
        source_kind=_required_string(payload, "source_kind"),
        artifactclass=_required_string(payload, "artifactclass"),
        operator=_required_string(payload, "operator"),
        file_count=_required_int(payload, "file_count"),
        total_bytes=_required_int(payload, "total_bytes"),
        skipped_count=_required_int(payload, "skipped_count"),
        started_at=_required_string(payload, "started_at"),
        updated_at=_required_string(payload, "updated_at"),
        resume_of=_optional_string(payload, "resume_of"),
        confirmation=_confirmation_from_payload(confirmation_payload),
    )


def _confirmation_from_payload(payload: dict[str, Any]) -> ConfirmationSnapshot:
    status = _required_string(payload, "status")
    if status not in CONFIRMATION_STATUSES:
        raise AgentLedgerError(f"unknown confirmation status in agent ledger: {status!r}")
    release_ok = payload.get("release_ok")
    if not isinstance(release_ok, bool):
        raise AgentLedgerError("confirmation.release_ok must be a boolean")
    marker = _optional_string(payload, "marker_path")
    detail = payload.get("detail")
    if detail is not None and not isinstance(detail, dict):
        raise AgentLedgerError("confirmation.detail must be an object when present")
    return ConfirmationSnapshot(
        status=status,
        release_ok=release_ok,
        marker_path=Path(marker) if marker else None,
        detail=detail,
    )


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise AgentLedgerError(f"agent ledger field {key!r} must be a non-empty string")
    return value


def _optional_string(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise AgentLedgerError(f"agent ledger field {key!r} must be a non-empty string")
    return value


def _required_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int):
        raise AgentLedgerError(f"agent ledger field {key!r} must be an integer")
    return value


def _read_json_object(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None
