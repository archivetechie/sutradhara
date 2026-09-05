"""Durable verification progress sidecar for landing-root intakes.

The intake watcher may run outside the HTTP API process, so verification
progress has to cross that process boundary through the landing directory. The
sidecar is advisory operator feedback only; catalog state and terminal markers
remain authoritative.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

VERIFICATION_PROGRESS_NAME = ".verification-progress.json"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VerificationProgress:
    """Byte progress for server-side checksum/catalog verification."""

    state: str
    bytes_verified: int
    bytes_total: int
    updated_at: str | None = None


def write_verification_progress(
    intake_dir: Path,
    *,
    state: str,
    bytes_verified: int,
    bytes_total: int,
) -> None:
    """Atomically write verification progress for API readers."""

    total = max(0, int(bytes_total))
    verified = min(total, max(0, int(bytes_verified)))
    payload = {
        "state": state,
        "bytesVerified": verified,
        "bytesTotal": total,
        "updatedAt": dt.datetime.now(dt.UTC).isoformat(),
    }
    path = intake_dir / VERIFICATION_PROGRESS_NAME
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")), encoding="utf-8"
    )
    tmp_path.replace(path)


def read_verification_progress(intake_dir: Path) -> VerificationProgress | None:
    """Read verification progress from an intake landing directory, if present."""

    path = intake_dir / VERIFICATION_PROGRESS_NAME
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception:
        logger.debug("failed to read verification progress from %s", path, exc_info=True)
        return None
    return _progress_from_payload(payload)


def _progress_from_payload(payload: Any) -> VerificationProgress | None:
    if not isinstance(payload, dict):
        return None
    try:
        state = str(payload.get("state") or "running")
        verified = int(payload.get("bytesVerified") or 0)
        total = int(payload.get("bytesTotal") or 0)
    except (TypeError, ValueError):
        return None
    updated_at = payload.get("updatedAt")
    return VerificationProgress(
        state=state,
        bytes_verified=max(0, verified),
        bytes_total=max(0, total),
        updated_at=str(updated_at) if updated_at is not None else None,
    )
