"""Shared status reader for streaming gRPC intakes.

Both the gRPC ``GetIntakeStatus`` RPC and the HTTP operator console status
endpoint report the durable row state, refined by watcher terminal markers when
the landing registrar has finished verification.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

from sutradhara.grpc import store as grpc_store

RECEIPTS_NAME = "receive-receipts.jsonl"


@dataclass(frozen=True)
class IntakeStatusView:
    """Externally visible intake verification state for an online receive."""

    status: str
    errors: list[str]
    release_safe: bool


def intake_status(row: grpc_store.GrpcIntake) -> IntakeStatusView:
    """Return the externally visible status and marker errors for an intake row."""

    status, errors = marker_status(intake_landing_path(row))
    if status is not None:
        return IntakeStatusView(
            status=status,
            errors=errors,
            release_safe=release_safe_for_status(row, status),
        )
    if row.state == "committed":
        status = "verifying"
    else:
        status = row.state
    return IntakeStatusView(
        status=status,
        errors=[],
        release_safe=release_safe_for_status(row, status),
    )


def intake_landing_path(row: grpc_store.GrpcIntake) -> Path:
    """Return the server-side landing directory for a gRPC intake row."""

    return Path(row.landing_root) / row.intake_id


def intake_receipt_bytes(row: grpc_store.GrpcIntake) -> int | None:
    """Return bytes copied to the landing receipt ledger, if the ledger is readable."""

    intake_dir = intake_landing_path(row)
    path = intake_dir / RECEIPTS_NAME
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


def release_safe_for_status(row: grpc_store.GrpcIntake, status: str) -> bool:
    """Return whether an online-card source may be released for this status."""

    return row.source_kind == "card" and status in {"committed", "verifying", "verified"}


def marker_status(intake_dir: Path) -> tuple[str | None, list[str]]:
    """Read a watcher terminal marker, if one exists."""

    for name, status in (
        ("intake.discrepancy.json", "discrepancy"),
        ("intake.quarantined.json", "quarantined"),
        ("intake.verified.json", "verified"),
    ):
        marker = intake_dir / name
        if marker.exists():
            return status, _marker_errors(marker)
    return None, []


def _marker_errors(path: Path) -> list[str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return [f"unreadable marker: {path.name}"]
    details = payload.get("details") if isinstance(payload, dict) else None
    if not isinstance(details, dict):
        return []
    errors: list[str] = []
    for key in ("missing", "extra", "mismatched", "errors"):
        value = details.get(key)
        if value:
            errors.append(f"{key}: {value}")
    return errors
