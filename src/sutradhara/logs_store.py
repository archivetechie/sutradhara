"""VictoriaLogs HTTP client and timestamp helpers for the logs read model.

The operator-console logs route and the ``log_pipeline`` reconciler both talk
to the localhost-only VictoriaLogs LogsQL API.  This module keeps the transport
small, synchronous, and dependency-free so tests can replace it with a canned
client while production uses the same JSON-line parsing path.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_LOG_STORE_URL = "http://127.0.0.1:9428"
LOG_STORE_URL_ENV = "SUTRADHARA_LOG_STORE_URL"
LOG_STORE_TIMEOUT_SECONDS = 5.0


class VictoriaLogsUnavailable(RuntimeError):
    """The localhost VictoriaLogs read path could not be reached."""


class VictoriaLogsQueryError(RuntimeError):
    """VictoriaLogs rejected a LogsQL query."""


@dataclass(frozen=True)
class VictoriaLogsClient:
    """Minimal client for VictoriaLogs' ``/select/logsql/query`` endpoint."""

    base_url: str = DEFAULT_LOG_STORE_URL
    timeout_seconds: float = LOG_STORE_TIMEOUT_SECONDS

    def query(self, logsql: str) -> list[dict[str, Any]]:
        """Run one LogsQL query and return parsed JSON-line rows."""

        endpoint = f"{self.base_url.rstrip('/')}/select/logsql/query"
        body = urlencode({"query": logsql, "timeout": f"{self.timeout_seconds:g}s"}).encode("utf-8")
        request = Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = response.read()
        except HTTPError as exc:
            if exc.code == 400:
                raise VictoriaLogsQueryError("log query rejected by store") from exc
            raise VictoriaLogsUnavailable("log store unavailable") from exc
        except (TimeoutError, URLError, OSError) as exc:
            raise VictoriaLogsUnavailable("log store unavailable") from exc
        return parse_json_lines(payload)


def log_store_client_from_env() -> VictoriaLogsClient:
    """Build a VictoriaLogs client from environment defaults."""

    return VictoriaLogsClient(os.environ.get(LOG_STORE_URL_ENV, DEFAULT_LOG_STORE_URL))


def parse_json_lines(payload: bytes | str) -> list[dict[str, Any]]:
    """Parse VictoriaLogs' JSON-line response into dictionaries."""

    text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    rows: list[dict[str, Any]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        parsed = json.loads(line)
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def parse_vl_timestamp(value: object) -> dt.datetime | None:
    """Parse a VictoriaLogs timestamp as an aware UTC datetime."""

    if not isinstance(value, str) or not value:
        return None
    normalized = value
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def format_vl_timestamp(value: dt.datetime) -> str:
    """Return UTC RFC3339 with microsecond precision and ``Z`` suffix."""

    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)
    utc = value.astimezone(dt.UTC)
    return utc.isoformat(timespec="microseconds").replace("+00:00", "Z")
