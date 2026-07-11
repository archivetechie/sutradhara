"""Shared helpers for Sutradhara operator-console API routes.

The console read models all share the restore contract's nested error envelope
and public-output rule: server-local absolute paths are redacted before JSON
leaves the API, including nested detail objects from job attempts.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Mapping
from typing import Any, NoReturn

from fastapi import HTTPException

from sutradhara.api.identity import Identity

ABSOLUTE_PATH_RE = re.compile(
    r"(?<![\w.-])(?:[A-Za-z]:[\\/][^\s'\"<>\]\[{}(),;]*|/(?!/)[^\s'\"<>\]\[{}(),;]*)"
)


def require_view(identity: Identity) -> Identity:
    """Require the baseline console read capability."""

    if not identity.has_capability("can_view"):
        raise_console_error(403, "forbidden", "operator has no sutradhara role")
    return identity


def raise_console_error(status_code: int, error: str, detail: str) -> NoReturn:
    """Raise a FastAPI error using the restore-console nested detail shape."""

    raise HTTPException(status_code=status_code, detail={"error": error, "detail": detail})


def iso_utc(value: dt.datetime) -> str:
    """Return an ISO-8601 UTC timestamp for contract JSON."""

    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC).isoformat()


def sanitize_text(detail: str) -> str:
    """Redact server-local absolute paths from a public string."""

    return ABSOLUTE_PATH_RE.sub("<path>", detail)


def sanitize_json(value: Any) -> Any:
    """Recursively redact absolute paths from JSON-like values."""

    if isinstance(value, str):
        return sanitize_text(value)
    if isinstance(value, Mapping):
        return {
            sanitize_text(key) if isinstance(key, str) else key: sanitize_json(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [sanitize_json(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_json(item) for item in value]
    return value
