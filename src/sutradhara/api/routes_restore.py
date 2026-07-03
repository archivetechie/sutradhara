"""Restore-console HTTP API for hdcache restore requests and conditions."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from dataclasses import replace
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import Engine, select
from sqlalchemy.exc import IntegrityError

from sutradhara.api.identity import Identity, parse_identity
from sutradhara.catalog.session import session_scope
from sutradhara.catalog.types import is_content_hash
from sutradhara.hdcache.alarms import (
    ALARM_DOMAIN,
    RestoreEventAlarmSink,
    alarm_condition_payload,
    evaluate_hdcache_alarm_conditions,
    restore_event_alarm_sink,
)
from sutradhara.hdcache.fill import OPERATOR_RESTORE_PRIORITY
from sutradhara.hdcache.manager import (
    ITEM_QUEUED,
    REQUEST_ACTIVE,
    REQUEST_COMPLETED,
    REQUEST_COMPLETED_WITH_ERRORS,
    REQUEST_PENDING,
    InvalidRestoreDestination,
    RestoreAdmissionInvalid,
    RestoreConfig,
    RestoreItemSpec,
    RestoreManagerError,
    UnknownRestoreDestination,
    admit_restore_request,
    configured_destinations,
    restore_config_from_env,
)
from sutradhara.hdcache.models import RestoreRequest, RestoreRequestItem
from sutradhara.jobs.engine import submit
from sutradhara.jobs.models import ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import CONDITION_SATISFIED

router = APIRouter()

REQUEST_STATES = {
    REQUEST_PENDING,
    REQUEST_ACTIVE,
    REQUEST_COMPLETED,
    REQUEST_COMPLETED_WITH_ERRORS,
}
MAX_RESTORE_REQUEST_LIMIT = 200
INVALID_RESTORE_DESTINATION_DETAIL = "restore destination is invalid"
_ABSOLUTE_PATH_RE = re.compile(
    r"(?<![\w.-])(?:[A-Za-z]:[\\/][^\s'\"<>\]\[{}(),;]*|/(?!/)[^\s'\"<>\]\[{}(),;]*)"
)


@router.get("/api/ui/restore-destinations")
def get_restore_destinations(request: Request) -> dict[str, object]:
    """Return configured opaque restore destinations."""

    _require_view(parse_identity(request.headers))
    return {"destinations": configured_destinations(_restore_config(request))}


@router.post("/api/ui/restores")
def post_restore(request: Request, payload: dict[str, Any]) -> JSONResponse:
    """Create a restore request; inadmissible cart items become denied rows."""

    identity = _require_view(parse_identity(request.headers))
    destination_id = payload.get("destination_id")
    if not isinstance(destination_id, str) or not destination_id:
        _raise(400, "bad_request", "destination_id is required")
    items = _parse_items(payload.get("items"))
    force = _optional_bool(payload.get("force"), default=False, field="force")
    force_rejected = _optional_bool(
        payload.get("force_rejected"),
        default=False,
        field="force_rejected",
    )
    idempotency_key = _optional_idempotency_key(payload.get("idempotency_key"))
    body_hash = _request_body_hash(
        destination_id=destination_id,
        items=items,
        force=force,
        force_rejected=force_rejected,
    )
    config = _restore_config(request)
    try:
        with session_scope(request.app.state.engine) as session:
            if idempotency_key is not None:
                existing = _restore_request_for_idempotency(session, idempotency_key)
                if existing is not None:
                    if existing.idempotency_body_hash != body_hash:
                        _raise(
                            409,
                            "restore_request_invalid",
                            "same idempotency_key used with a different body",
                        )
                    return JSONResponse({"request_id": existing.id}, status_code=200)
            restore_request = admit_restore_request(
                session,
                identity=identity,
                destination_id=destination_id,
                items=items,
                force_suspect=force,
                force_rejected=force_rejected,
                idempotency_key=idempotency_key,
                idempotency_body_hash=body_hash if idempotency_key is not None else None,
                config=config,
            )
            for item in restore_request.items:
                if item.state == ITEM_QUEUED and item.id is not None:
                    submit(
                        session,
                        "restore",
                        {"restore_request_item_id": item.id},
                        required_resources=[{"pool": "io", "count": 1}],
                        priority=OPERATOR_RESTORE_PRIORITY,
                    )
            request_id = restore_request.id
    except UnknownRestoreDestination as exc:
        _raise(404, "unknown_destination", str(exc))
    except InvalidRestoreDestination:
        _raise(400, "invalid_restore_destination", INVALID_RESTORE_DESTINATION_DETAIL)
    except RestoreAdmissionInvalid as exc:
        _raise(400, "bad_request", _sanitize_detail(str(exc)))
    except RestoreManagerError as exc:
        _raise(400, "restore_request_invalid", _sanitize_detail(str(exc)))
    except IntegrityError:
        if idempotency_key is not None:
            with session_scope(request.app.state.engine) as session:
                existing = _restore_request_for_idempotency(session, idempotency_key)
                if existing is not None and existing.idempotency_body_hash == body_hash:
                    return JSONResponse({"request_id": existing.id}, status_code=200)
            _raise(
                409,
                "restore_request_invalid",
                "same idempotency_key is already in use",
            )
        raise
    except (ValueError, RuntimeError) as exc:
        _raise(400, "bad_request", _sanitize_detail(str(exc)))
    return JSONResponse({"request_id": request_id}, status_code=201)


@router.get("/api/ui/restore-requests")
def get_restore_requests(
    request: Request,
    state: str | None = None,
    limit: int = 50,
) -> dict[str, object]:
    """Return restore requests newest-first."""

    _require_view(parse_identity(request.headers))
    if state is not None and state not in REQUEST_STATES:
        _raise(400, "bad_request", f"unknown restore request state {state!r}")
    if limit < 1 or limit > MAX_RESTORE_REQUEST_LIMIT:
        _raise(400, "bad_request", f"limit must be between 1 and {MAX_RESTORE_REQUEST_LIMIT}")
    with session_scope(request.app.state.engine) as session:
        query = select(RestoreRequest).order_by(
            RestoreRequest.created_at.desc(),
            RestoreRequest.id.desc(),
        )
        if state is not None:
            query = query.where(RestoreRequest.state == state)
        rows = list(session.scalars(query.limit(limit)))
        return {"requests": [_request_payload(row) for row in rows]}


@router.get("/api/ui/restore-requests/{request_id}")
def get_restore_request(request: Request, request_id: str) -> dict[str, object]:
    """Return one restore request by id."""

    _require_view(parse_identity(request.headers))
    with session_scope(request.app.state.engine) as session:
        row = session.get(RestoreRequest, request_id)
        if row is None:
            _raise(404, "not_found", f"unknown restore request {request_id!r}")
        return _request_payload(row)


@router.get("/api/ui/reconciliation")
def get_reconciliation(request: Request) -> dict[str, object]:
    """Return active reconciliation/gap-board conditions."""

    _require_view(parse_identity(request.headers))
    with session_scope(request.app.state.engine) as session:
        evaluate_hdcache_alarm_conditions(session)
        rows = list(
            session.scalars(
                select(ReconciliationCondition)
                .where(ReconciliationCondition.condition != CONDITION_SATISFIED)
                .order_by(ReconciliationCondition.domain, ReconciliationCondition.target_key)
            )
        )
        return {"conditions": [_condition_payload(row) for row in rows]}


def _restore_config(request: Request) -> RestoreConfig:
    configured = getattr(request.app.state, "restore_config", None)
    engine = request.app.state.engine
    if isinstance(configured, RestoreConfig):
        return _with_app_alarm_sink(configured, engine)
    return _with_app_alarm_sink(restore_config_from_env(), engine)


def _with_app_alarm_sink(config: RestoreConfig, engine: Engine) -> RestoreConfig:
    sink = config.event_sink
    if sink is None:
        return replace(config, event_sink=restore_event_alarm_sink(engine=engine))
    if isinstance(sink, RestoreEventAlarmSink) and sink.engine is None and sink.session is None:
        return replace(config, event_sink=restore_event_alarm_sink(engine=engine))
    return config


def _require_view(identity: Identity) -> Identity:
    if not identity.has_capability("can_view"):
        _raise(403, "forbidden", "operator has no sutradhara role")
    return identity


def _parse_items(raw: Any) -> list[RestoreItemSpec]:
    if not isinstance(raw, list) or not raw:
        _raise(400, "bad_request", "items must be a non-empty list")
    items: list[RestoreItemSpec] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            _raise(400, "bad_request", f"items[{index}] must be an object")
        raw_hash = item.get("content_sha256")
        artifactclass = item.get("artifactclass")
        if not isinstance(raw_hash, str) or raw_hash.lower() != raw_hash:
            _raise(400, "bad_request", f"items[{index}].content_sha256 must be lowercase hex")
        try:
            digest = bytes.fromhex(raw_hash)
        except ValueError:
            _raise(400, "bad_request", f"items[{index}].content_sha256 must be lowercase hex")
        if not is_content_hash(digest):
            _raise(400, "bad_request", f"items[{index}].content_sha256 must be sha256")
        if not isinstance(artifactclass, str) or not artifactclass:
            _raise(400, "bad_request", f"items[{index}].artifactclass is required")
        items.append(RestoreItemSpec(digest, artifactclass))
    return items


def _optional_bool(raw: Any, *, default: bool, field: str) -> bool:
    if raw is None:
        return default
    if not isinstance(raw, bool):
        _raise(400, "bad_request", f"{field} must be boolean")
    return raw


def _optional_idempotency_key(raw: Any) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip():
        _raise(400, "bad_request", "idempotency_key must be a non-empty string")
    value = raw.strip()
    if len(value) > 128:
        _raise(400, "bad_request", "idempotency_key must be at most 128 characters")
    return value


def _request_body_hash(
    *,
    destination_id: str,
    items: list[RestoreItemSpec],
    force: bool,
    force_rejected: bool,
) -> str:
    payload = {
        "destination_id": destination_id,
        "items": [
            {
                "content_sha256": item.content_sha256.hex(),
                "artifactclass": item.artifactclass,
            }
            for item in items
        ],
        "force": force,
        "force_rejected": force_rejected,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _restore_request_for_idempotency(session: Any, key: str) -> RestoreRequest | None:
    return session.scalars(
        select(RestoreRequest).where(RestoreRequest.idempotency_key == key)
    ).one_or_none()


def _request_payload(row: RestoreRequest) -> dict[str, object]:
    item_payloads = [_item_payload(item) for item in row.items]
    bytes_total = sum(item.size_bytes or 0 for item in row.items)
    bytes_restored = sum(item.bytes_restored or 0 for item in row.items)
    return {
        "id": row.id,
        "identity": row.identity,
        "created_at": _iso(row.created_at),
        "destination_id": row.destination_id,
        "state": row.state,
        "bytes_total": bytes_total,
        "bytes_restored": bytes_restored,
        "items": item_payloads,
    }


def _item_payload(item: RestoreRequestItem) -> dict[str, object]:
    return {
        "content_sha256": item.content_sha256.hex(),
        "artifactclass": item.artifactclass,
        "state": item.state,
        "detail": None if item.detail is None else _sanitize_detail(item.detail),
        "denial_kind": item.denial_kind,
        "size_bytes": item.size_bytes,
        "bytes_restored": item.bytes_restored,
        "source": item.source,
        "updated_at": _iso(item.updated_at),
    }


def _condition_payload(row: ReconciliationCondition) -> dict[str, object]:
    if row.domain == ALARM_DOMAIN:
        return alarm_condition_payload(row)
    return {
        "domain": row.domain,
        "target_key": row.target_key,
        "condition": row.condition,
        "reason": row.reason,
        "message": row.message,
        "owner": None,
        "updated_at": _iso(row.updated_at),
    }


def _iso(value: dt.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC).isoformat()


def _raise(status_code: int, error: str, detail: str) -> None:
    raise HTTPException(status_code=status_code, detail={"error": error, "detail": detail})


def _sanitize_detail(detail: str) -> str:
    """Remove host-local absolute paths from public restore API error details."""

    return _ABSOLUTE_PATH_RE.sub("<path>", detail)
