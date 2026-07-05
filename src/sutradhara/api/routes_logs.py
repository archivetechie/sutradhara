"""Logs read-model proxy for the operator console.

``GET /api/ui/logs`` is a thin, capability-gated VictoriaLogs proxy that
normalizes the frozen console record shape, keeps user values out of LogsQL
syntax via quoted literals, and applies the P-L1 content tier: ``raw`` only for
admins and recursive ``attrs`` sanitization for non-admin log viewers.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import math
import re
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Query, Request

from sutradhara.api.console import raise_console_error, sanitize_json, sanitize_text
from sutradhara.api.identity import Identity, parse_identity
from sutradhara.logs_store import (
    VictoriaLogsClient,
    VictoriaLogsQueryError,
    VictoriaLogsUnavailable,
    format_vl_timestamp,
    log_store_client_from_env,
    parse_vl_timestamp,
)

router = APIRouter()

DEFAULT_LIMIT = 200
MAX_LIMIT = 500
LEVELS = ("trace", "debug", "info", "warn", "error", "fatal")
LEVEL_RANK = {level: index for index, level in enumerate(LEVELS)}
ENTITY_KINDS = frozenset(
    {"job", "intake", "disk", "tape", "device", "request", "condition", "user"}
)
ENTITY_RE = re.compile(r"^(?P<kind>[a-z_]+):(?P<id>[a-z0-9_-]+)$")
RELATIVE_TIME_RE = re.compile(r"^(?P<sign>[+-]?)(?P<value>[1-9][0-9]*)(?P<unit>ms|s|m|h|d|w)$")
NOW_RELATIVE_RE = re.compile(
    r"^now(?:(?P<sign>[+-])(?P<value>[1-9][0-9]*)(?P<unit>ms|s|m|h|d|w))?$"
)
RELATIVE_MULTIPLIERS = {
    "ms": 0.001,
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
}
FIELD_PIPE = (
    "| fields _time, ingest_ts, seq, source, host, unit, container, severity, "
    "level, trace_id, message, _msg, attrs, attrs.*, entity_refs, raw"
)
STORE_INTERNAL_FIELDS = {
    "_msg",
    "_stream",
    "_stream_id",
    "_time",
    "dedup_key",
    "entity_keys",
    "entity_refs",
    "ingest_ts",
    "raw",
    "seq",
    "severity",
    "source",
    "host",
    "unit",
    "container",
    "trace_id",
    "level",
    "message",
    "attrs",
}
INFRA_AUTH_SOURCE_PREFIXES = (
    "auth",
    "authentik",
    "collector",
    "docker",
    "kernel",
    "log_pipeline",
    "quadstor",
    "remanence",
    "smart",
    "smartd",
    "sshd",
    "systemd",
    "vector",
)
CONTENT_REVEALING_ATTR_KEYS = frozenset(
    {
        "asset_label",
        "asset_labels",
        "file_name",
        "filename",
        "filenames",
        "logical_path",
        "member_path",
        "source_ref",
        "stored_member_path",
        "target_summary",
        "virtual_path",
    }
)


@dataclass(frozen=True)
class _LevelFilter:
    global_floor: str | None
    per_source: dict[str, str]


@dataclass(frozen=True)
class _Cursor:
    direction: str
    ts: str
    seq: int


@dataclass(frozen=True)
class _QueryPlan:
    base_query: str
    start: dt.datetime | None
    end: dt.datetime | None
    cursor: _Cursor | None
    limit: int
    histogram: bool


@router.get("/api/ui/logs")
def get_logs(
    request: Request,
    sources: str | None = None,
    level: str | None = None,
    from_: str | None = Query(default=None, alias="from"),
    to: str | None = None,
    q: str | None = None,
    regex: str | None = None,
    entity: str | None = None,
    cursor: str | None = None,
    limit: str | None = None,
    histogram: str | None = None,
) -> dict[str, object]:
    """Return normalized log rows from VictoriaLogs using the frozen envelope."""

    identity = _require_logs(parse_identity(request.headers))
    plan = _build_query_plan(
        sources=sources,
        level=level,
        from_=from_,
        to=to,
        q=q,
        regex=regex,
        entity=entity,
        cursor=cursor,
        limit=limit,
        histogram=histogram,
        now=_route_now(request),
    )
    client = _log_client(request)
    try:
        stats = _query_stats(client, plan.base_query)
        rows, has_adjacent = _query_page(client, plan)
        shaped = [
            _record_payload(row, is_admin=identity.has_capability("can_admin")) for row in rows
        ]
        hist = _query_histogram(client, plan, stats) if plan.histogram else []
    except VictoriaLogsQueryError:
        raise_console_error(400, "bad_request", "log query rejected")
    except VictoriaLogsUnavailable:
        raise_console_error(503, "unavailable", "log store unavailable")

    return {
        "rows": shaped,
        "total": stats.total,
        "truncated": stats.total > len(shaped),
        "next_cursor": _next_cursor(shaped, plan, has_adjacent),
        "prev_cursor": _prev_cursor(shaped, plan, has_adjacent),
        "histogram": hist,
    }


def _require_logs(identity: Identity) -> Identity:
    if not identity.has_capability("can_logs"):
        raise_console_error(403, "forbidden", "operator lacks logs capability")
    return identity


def _build_query_plan(
    *,
    sources: str | None,
    level: str | None,
    from_: str | None,
    to: str | None,
    q: str | None,
    regex: str | None,
    entity: str | None,
    cursor: str | None,
    limit: str | None,
    histogram: str | None,
    now: dt.datetime,
) -> _QueryPlan:
    source_values = _parse_sources(sources)
    level_filter = _parse_level_filter(level)
    start = _parse_time_bound(from_, field="from", now=now)
    end = _parse_time_bound(to, field="to", now=now)
    if start is not None and end is not None and start > end:
        raise_console_error(400, "bad_request", "from must be before or equal to to")
    entity_value = _parse_entity(entity)
    page_limit = _parse_limit(limit)
    cursor_value = _parse_cursor(cursor)
    histogram_requested = _parse_histogram(histogram)
    filters = ["*"]
    time_filter = _time_filter(start, end)
    if time_filter is not None:
        filters.append(time_filter)
    source_filter = _source_filter(source_values)
    if source_filter is not None:
        filters.append(source_filter)
    level_clause = _level_filter(source_values, level_filter)
    if level_clause is not None:
        filters.append(level_clause)
    search_filter = _search_filter(q, regex=_parse_bool(regex, field="regex"))
    if search_filter is not None:
        filters.append(search_filter)
    entity_filter = _entity_filter(entity_value)
    if entity_filter is not None:
        filters.append(entity_filter)
    return _QueryPlan(
        base_query=" ".join(filters),
        start=start,
        end=end,
        cursor=cursor_value,
        limit=page_limit,
        histogram=histogram_requested,
    )


def _parse_sources(raw: str | None) -> list[str]:
    if raw is None or not raw.strip():
        return []
    values = [part.strip() for part in raw.split(",")]
    if any(not value for value in values):
        raise_console_error(400, "bad_request", "sources must be comma-separated source ids")
    return values


def _parse_level_filter(raw: str | None) -> _LevelFilter:
    if raw is None or not raw.strip():
        return _LevelFilter(global_floor=None, per_source={})
    global_floor: str | None = None
    per_source: dict[str, str] = {}
    for token in [part.strip() for part in raw.split(",")]:
        if not token:
            raise_console_error(400, "bad_request", "invalid level floor grammar")
        if ":" in token:
            source, floor = token.split(":", 1)
            source = source.strip()
            floor = floor.strip()
            if not source or floor not in LEVEL_RANK:
                raise_console_error(400, "bad_request", "invalid level floor grammar")
            per_source[source] = floor
            continue
        if global_floor is not None or token not in LEVEL_RANK:
            raise_console_error(400, "bad_request", "invalid level floor grammar")
        global_floor = token
    return _LevelFilter(global_floor=global_floor, per_source=per_source)


def _parse_time_bound(raw: str | None, *, field: str, now: dt.datetime) -> dt.datetime | None:
    if raw is None or not raw.strip():
        return None
    value = raw.strip()
    relative_now = NOW_RELATIVE_RE.match(value)
    if relative_now is not None:
        sign = relative_now.group("sign")
        if sign is None:
            return now
        delta = _relative_delta(relative_now.group("value"), relative_now.group("unit"))
        return now + delta if sign == "+" else now - delta
    relative = RELATIVE_TIME_RE.match(value)
    if relative is not None:
        delta = _relative_delta(relative.group("value"), relative.group("unit"))
        return now + delta if relative.group("sign") == "+" else now - delta
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise_console_error(400, "bad_request", f"{field} must be RFC3339 or relative")
    if parsed.tzinfo is None:
        raise_console_error(400, "bad_request", f"{field} must include timezone")
    return parsed.astimezone(dt.UTC)


def _relative_delta(value: str, unit: str) -> dt.timedelta:
    return dt.timedelta(seconds=int(value) * RELATIVE_MULTIPLIERS[unit])


def _parse_entity(raw: str | None) -> str | None:
    if raw is None or not raw.strip():
        return None
    value = raw.strip()
    match = ENTITY_RE.match(value)
    if match is None or match.group("kind") not in ENTITY_KINDS:
        raise_console_error(400, "bad_request", "entity must be kind:id")
    return value


def _parse_limit(raw: str | None) -> int:
    if raw is None or not raw.strip():
        return DEFAULT_LIMIT
    try:
        parsed = int(raw)
    except ValueError:
        raise_console_error(400, "bad_request", "limit must be an integer")
    if parsed <= 0:
        raise_console_error(400, "bad_request", "limit must be positive")
    return min(parsed, MAX_LIMIT)


def _parse_bool(raw: str | None, *, field: str) -> bool:
    if raw is None or not raw.strip():
        return False
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise_console_error(400, "bad_request", f"{field} must be boolean")
    raise AssertionError("unreachable")


def _parse_histogram(raw: str | None) -> bool:
    if raw is None or not raw.strip():
        return False
    if raw.strip() != "source,level":
        raise_console_error(400, "bad_request", "unsupported histogram dimensions")
    return True


def _parse_cursor(raw: str | None) -> _Cursor | None:
    if raw is None or not raw.strip():
        return None
    try:
        padded = raw + "=" * (-len(raw) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except (ValueError, json.JSONDecodeError, UnicodeError):
        raise_console_error(400, "bad_request", "invalid cursor")
    if not isinstance(payload, dict):
        raise_console_error(400, "bad_request", "invalid cursor")
    direction = payload.get("d")
    ts = payload.get("ts")
    seq = payload.get("seq")
    if direction not in {"next", "prev"} or not isinstance(ts, str):
        raise_console_error(400, "bad_request", "invalid cursor")
    try:
        parsed_seq = int(seq)
    except (TypeError, ValueError):
        raise_console_error(400, "bad_request", "invalid cursor")
    if parse_vl_timestamp(ts) is None:
        raise_console_error(400, "bad_request", "invalid cursor")
    return _Cursor(direction=direction, ts=ts, seq=parsed_seq)


def _time_filter(start: dt.datetime | None, end: dt.datetime | None) -> str | None:
    if start is None and end is None:
        return None
    if start is not None and end is not None:
        return f"_time:[{format_vl_timestamp(start)}, {format_vl_timestamp(end)}]"
    if start is not None:
        return f"_time:>={format_vl_timestamp(start)}"
    assert end is not None
    return f"_time:<={format_vl_timestamp(end)}"


def _source_filter(sources: list[str]) -> str | None:
    if not sources:
        return None
    return f"source:in({', '.join(_quote(value) for value in sources)})"


def _level_filter(sources: list[str], level_filter: _LevelFilter) -> str | None:
    if level_filter.global_floor is None and not level_filter.per_source:
        return None
    if sources:
        clauses = []
        for source in sources:
            floor = level_filter.per_source.get(source, level_filter.global_floor or LEVELS[0])
            clauses.append(
                f"(source:={_quote(source)} level:in({', '.join(_quote(v) for v in _levels_at_or_above(floor))}))"
            )
        return f"({' OR '.join(clauses)})"
    clauses = [
        f"(source:={_quote(source)} level:in({', '.join(_quote(v) for v in _levels_at_or_above(floor))}))"
        for source, floor in sorted(level_filter.per_source.items())
    ]
    override_sources = sorted(level_filter.per_source)
    if level_filter.global_floor is not None:
        fallback = f"level:in({', '.join(_quote(v) for v in _levels_at_or_above(level_filter.global_floor))})"
        if override_sources:
            fallback = f"(-source:in({', '.join(_quote(v) for v in override_sources)}) {fallback})"
        clauses.append(fallback)
    elif override_sources:
        clauses.append(f"-source:in({', '.join(_quote(v) for v in override_sources)})")
    return f"({' OR '.join(clauses)})"


def _levels_at_or_above(floor: str) -> tuple[str, ...]:
    return LEVELS[LEVEL_RANK[floor] :]


def _search_filter(raw: str | None, *, regex: bool) -> str | None:
    if raw is None or raw == "":
        return None
    if regex:
        try:
            re.compile(raw)
        except re.error:
            raise_console_error(400, "bad_request", "invalid regex")
        quoted = _quote(raw)
        return f"(message:~{quoted} OR _msg:~{quoted} OR attrs.*:~{quoted})"
    quoted = _quote(raw)
    return f"(message:*{quoted}* OR _msg:*{quoted}* OR attrs.*:*{quoted}*)"


def _entity_filter(entity: str | None) -> str | None:
    if entity is None:
        return None
    return f"entity_keys:~{_quote(f'(^|,){re.escape(entity)}(,|$)')}"


def _quote(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


@dataclass(frozen=True)
class _Stats:
    total: int
    min_time: dt.datetime | None
    max_time: dt.datetime | None


def _query_stats(client: VictoriaLogsClient, base_query: str) -> _Stats:
    rows = client.query(
        f"{base_query} | stats count() as total, min(_time) as min_time, max(_time) as max_time"
    )
    first = rows[0] if rows else {}
    return _Stats(
        total=_int_value(first.get("total")),
        min_time=parse_vl_timestamp(first.get("min_time")),
        max_time=parse_vl_timestamp(first.get("max_time")),
    )


def _query_page(client: VictoriaLogsClient, plan: _QueryPlan) -> tuple[list[dict[str, Any]], bool]:
    order_desc = plan.cursor is None or plan.cursor.direction == "prev"
    filters = [plan.base_query]
    if plan.cursor is not None:
        filters.append(_cursor_filter(plan.cursor))
    order = (
        "sort by (_time desc, source desc, seq desc)"
        if order_desc
        else "sort by (_time, source, seq)"
    )
    rows = client.query(f"{' '.join(filters)} | {order} {FIELD_PIPE} | limit {plan.limit + 1}")
    has_adjacent = len(rows) > plan.limit
    page = rows[: plan.limit]
    if order_desc:
        page = list(reversed(page))
    return page, has_adjacent


def _cursor_filter(cursor: _Cursor) -> str:
    op = ">" if cursor.direction == "next" else "<"
    return f"(_time:{op}{cursor.ts} OR (_time:={cursor.ts} seq:{op}{cursor.seq}))"


def _query_histogram(
    client: VictoriaLogsClient,
    plan: _QueryPlan,
    stats: _Stats,
) -> list[dict[str, object]]:
    start = plan.start or stats.min_time
    end = plan.end or stats.max_time
    if start is None or end is None or end <= start:
        return []
    bucket_seconds = _bucket_seconds(start, end)
    rows = client.query(
        f"{plan.base_query} | stats by (_time:{bucket_seconds}s, source, level) "
        "count() as count | sort by (_time, source, level)"
    )
    buckets: list[dict[str, object]] = []
    for row in rows:
        bucket_start = parse_vl_timestamp(row.get("_time"))
        if bucket_start is None:
            continue
        bucket_end = bucket_start + dt.timedelta(seconds=bucket_seconds)
        buckets.append(
            {
                "bucket_start": format_vl_timestamp(bucket_start),
                "bucket_end": format_vl_timestamp(bucket_end),
                "source": _text(row.get("source")),
                "level": _text(row.get("level")),
                "count": _int_value(row.get("count")),
            }
        )
    return buckets


def _bucket_seconds(start: dt.datetime, end: dt.datetime) -> int:
    seconds = max((end - start).total_seconds(), 1.0)
    return max(1, min(300, math.ceil(seconds / 60)))


def _record_payload(row: dict[str, Any], *, is_admin: bool) -> dict[str, object]:
    attrs = _attrs_payload(row)
    if not is_admin:
        attrs = _omit_content_attrs(attrs)
        attrs = sanitize_json(attrs)
    source = _text(row.get("source"))
    payload: dict[str, object] = {
        "ts": _time_text(row.get("_time")),
        "ingest_ts": _time_text(row.get("ingest_ts")),
        "seq": _int_value(row.get("seq")),
        "source": source,
        "host": _text(row.get("host")),
        "unit": _optional_text(row.get("unit")),
        "container": _optional_text(row.get("container")),
        "severity": _int_value(row.get("severity")),
        "level": _text(row.get("level")),
        "trace_id": _optional_text(row.get("trace_id")),
        "message": _message_payload(row, source=source, is_admin=is_admin),
        "attrs": attrs,
        "entity_refs": _entity_refs(row.get("entity_refs")),
    }
    if is_admin:
        payload["raw"] = _text(row.get("raw"))
    return payload


def _message_payload(row: dict[str, Any], *, source: str, is_admin: bool) -> str:
    message = _text(row.get("message", row.get("_msg")))
    if is_admin or _is_infra_auth_source(source):
        return message
    return sanitize_text(message)


def _is_infra_auth_source(source: str) -> bool:
    normalized = source.lower()
    return any(
        normalized == prefix or normalized.startswith(f"{prefix}-")
        for prefix in INFRA_AUTH_SOURCE_PREFIXES
    )


def _attrs_payload(row: dict[str, Any]) -> dict[str, Any]:
    attrs: dict[str, Any] = {}
    raw_attrs = row.get("attrs")
    if isinstance(raw_attrs, dict):
        attrs.update(raw_attrs)
    elif isinstance(raw_attrs, str) and raw_attrs:
        try:
            parsed = json.loads(raw_attrs)
        except json.JSONDecodeError:
            attrs["value"] = raw_attrs
        else:
            if isinstance(parsed, dict):
                attrs.update(parsed)
            else:
                attrs["value"] = parsed
    for key, value in row.items():
        if key.startswith("attrs."):
            _assign_attr(attrs, key.removeprefix("attrs."), value)
        elif key not in STORE_INTERNAL_FIELDS and not key.startswith("_"):
            attrs.setdefault(key, value)
    return attrs


def _omit_content_attrs(attrs: dict[str, Any]) -> dict[str, Any]:
    redacted_fields = attrs.get("redacted_fields", [])
    if not isinstance(redacted_fields, list):
        redacted_fields = []
    explicit = {
        _normalize_attr_key(item)
        for item in redacted_fields
        if isinstance(item, str) and item.strip()
    }
    return _omit_content_value(attrs, explicit)


def _omit_content_value(value: Any, explicit: set[str]) -> Any:
    if isinstance(value, dict):
        shaped: dict[str, Any] = {}
        for key, item in value.items():
            normalized = _normalize_attr_key(key)
            if normalized in CONTENT_REVEALING_ATTR_KEYS or normalized in explicit:
                continue
            shaped[key] = _omit_content_value(item, explicit)
        return shaped
    if isinstance(value, list):
        return [_omit_content_value(item, explicit) for item in value]
    return value


def _normalize_attr_key(key: object) -> str:
    return str(key).strip().lower().replace("-", "_")


def _assign_attr(attrs: dict[str, Any], key: str, value: Any) -> None:
    parts = [part for part in key.split(".") if part]
    if not parts:
        return
    current = attrs
    for part in parts[:-1]:
        existing = current.get(part)
        if not isinstance(existing, dict):
            existing = {}
            current[part] = existing
        current = existing
    current[parts[-1]] = value


def _entity_refs(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _time_text(raw: object) -> str:
    parsed = parse_vl_timestamp(raw)
    return format_vl_timestamp(parsed) if parsed is not None else ""


def _text(raw: object) -> str:
    if raw is None:
        return ""
    return str(raw)


def _optional_text(raw: object) -> str | None:
    if raw is None:
        return None
    text = str(raw)
    return text or None


def _int_value(raw: object) -> int:
    try:
        return int(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _next_cursor(rows: list[dict[str, object]], plan: _QueryPlan, has_adjacent: bool) -> str | None:
    if not rows:
        return None
    if plan.cursor is None:
        return None
    if plan.cursor.direction == "next" and not has_adjacent:
        return None
    boundary = rows[-1]
    return _encode_cursor("next", ts=str(boundary["ts"]), seq=int(boundary["seq"]))


def _prev_cursor(rows: list[dict[str, object]], plan: _QueryPlan, has_adjacent: bool) -> str | None:
    if not rows:
        return None
    if plan.cursor is None and not has_adjacent:
        return None
    if plan.cursor is not None and plan.cursor.direction == "prev" and not has_adjacent:
        return None
    boundary = rows[0]
    return _encode_cursor("prev", ts=str(boundary["ts"]), seq=int(boundary["seq"]))


def _encode_cursor(direction: str, *, ts: str, seq: int) -> str:
    payload = json.dumps({"d": direction, "ts": ts, "seq": seq}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii").rstrip("=")


def _log_client(request: Request) -> VictoriaLogsClient:
    configured = getattr(request.app.state, "log_store_client", None)
    if configured is not None:
        return configured
    return log_store_client_from_env()


def _route_now(request: Request) -> dt.datetime:
    configured = getattr(request.app.state, "logs_now", None)
    if isinstance(configured, dt.datetime):
        return (
            configured.astimezone(dt.UTC)
            if configured.tzinfo
            else configured.replace(tzinfo=dt.UTC)
        )
    return dt.datetime.now(dt.UTC)
