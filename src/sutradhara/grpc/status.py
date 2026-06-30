"""Shared status reader for streaming gRPC intakes.

Both the gRPC ``GetIntakeStatus`` RPC and the HTTP operator console status
endpoint report the durable row state, refined by watcher terminal markers when
the landing registrar has finished verification.
"""

from __future__ import annotations

import json
from pathlib import Path

from sutradhara.grpc import store as grpc_store


def intake_status(row: grpc_store.GrpcIntake) -> tuple[str, list[str]]:
    """Return the externally visible status and marker errors for an intake row."""

    status, errors = marker_status(Path(row.landing_root) / row.intake_id)
    if status is not None:
        return status, errors
    if row.state == "committed":
        return "verifying", []
    return row.state, []


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
