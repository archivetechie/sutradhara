"""Receive options and receive-start endpoints for the operator console API."""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import hashlib
import json
from collections.abc import Callable
from typing import Any
from uuid import UUID

import anyio
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select

from sutradhara.api import store
from sutradhara.api.identity import Identity, parse_identity
from sutradhara.api.sources import CatalogError, LandingEntry, SourceEntry
from sutradhara.api.sources import list_landings as default_list_landings
from sutradhara.api.sources import list_sources as default_list_sources
from sutradhara.api.sources import resolve_landing as default_resolve_landing
from sutradhara.api.sources import resolve_source as default_resolve_source
from sutradhara.artifactclass_policy import ArtifactClassPolicyError, get_artifactclass_policy
from sutradhara.catalog.models import ArtifactClassPolicyRecord
from sutradhara.catalog.session import make_session_factory, session_scope
from sutradhara.intake import register_intake
from sutradhara_receive import ReceiveError, ReceiveResult, receive_source

router = APIRouter()


class ReceiveRequest(BaseModel):
    """JSON body accepted by POST /api/receive."""

    sourceId: str
    landingId: str
    artifactclass: str
    idempotencyKey: UUID
    label: str | None = None


@router.get("/api/receive/options")
def receive_options(request: Request) -> dict[str, list[dict[str, str]]]:
    """Return opaque source/landing/class options for the receive screen."""

    _require_view(parse_identity(request.headers))
    engine = request.app.state.engine
    sources = _list_sources(request)
    busy = store.source_busy_ids(
        engine,
        [source.source_id for source in sources],
        ttl=request.app.state.claim_ttl,
    )
    return {
        "sources": [
            {
                "sourceId": source.source_id,
                "label": source.label,
                "kind": source.kind,
                "status": "busy" if source.source_id in busy else "available",
            }
            for source in sources
        ],
        "landings": [
            {
                "landingId": landing.landing_id,
                "label": landing.label,
                "status": "available",
            }
            for landing in _list_landings(request)
        ],
        "artifactclasses": _artifactclasses(engine),
    }


@router.post("/api/receive")
async def post_receive(request: Request, body: ReceiveRequest) -> dict[str, str]:
    """Start one synchronous receive after authZ, validation, leases, and idempotency."""

    identity = _require_receive(parse_identity(request.headers))
    engine = request.app.state.engine
    source = _resolve_source(request, body.sourceId)
    landing = _resolve_landing(request, body.landingId)
    try:
        _validate_artifactclass(engine, body.artifactclass)
    except ArtifactClassPolicyError as exc:
        _raise(400, "bad_artifactclass", str(exc))
    idempotency_key = str(body.idempotencyKey)
    request_hash = _request_hash(body)

    decision = await _claim_or_wait_for_idempotency(
        request,
        identity=identity,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
    )
    if decision.state == "conflict":
        _raise(409, "idempotency_conflict", "same idempotencyKey used with a different body")
    if decision.state == "completed":
        if decision.response_json is None:
            _raise(409, "idempotency_conflict", "completed request has no stored response")
        return {str(key): str(value) for key, value in decision.response_json.items()}
    if decision.state != "claimed":
        _raise(409, "already_in_progress", "receive is already in progress")

    if not store.claim_source(
        engine,
        source_id=source.source_id,
        operator_username=identity.operator_username,
        idempotency_key=idempotency_key,
        ttl=request.app.state.claim_ttl,
    ):
        store.abandon_idempotency(
            engine,
            operator_username=identity.operator_username,
            endpoint=store.RECEIVE_ENDPOINT,
            idempotency_key=idempotency_key,
        )
        _raise(409, "source_busy", "source busy / already in progress")

    heartbeat = asyncio.create_task(
        _heartbeat_loop(
            request,
            identity=identity,
            source_id=source.source_id,
            idempotency_key=idempotency_key,
        )
    )
    try:
        response = await anyio.to_thread.run_sync(
            _receive_and_register,
            request,
            identity,
            source,
            landing,
            body,
        )
        store.complete_idempotency(
            engine,
            operator_username=identity.operator_username,
            endpoint=store.RECEIVE_ENDPOINT,
            idempotency_key=idempotency_key,
            intake_id=response["intakeId"],
            response_json=response,
        )
        return response
    except (CatalogError, ArtifactClassPolicyError, ReceiveError, ValueError) as exc:
        store.abandon_idempotency(
            engine,
            operator_username=identity.operator_username,
            endpoint=store.RECEIVE_ENDPOINT,
            idempotency_key=idempotency_key,
        )
        _raise(400, "validation_error", str(exc))
    finally:
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat
        store.release_source(engine, source_id=source.source_id, idempotency_key=idempotency_key)


def _require_view(identity: Identity) -> Identity:
    if not identity.has_capability("can_view"):
        _raise(403, "forbidden", "operator has no sutradhara role")
    return identity


def _require_receive(identity: Identity) -> Identity:
    _require_view(identity)
    if not identity.has_capability("can_receive"):
        _raise(403, "forbidden", "your group doesn't permit this")
    return identity


async def _claim_or_wait_for_idempotency(
    request: Request,
    *,
    identity: Identity,
    idempotency_key: str,
    request_hash: str,
) -> store.IdempotencyDecision:
    deadline = asyncio.get_running_loop().time() + request.app.state.idempotency_wait_seconds
    while True:
        decision = store.begin_idempotency(
            request.app.state.engine,
            operator_username=identity.operator_username,
            endpoint=store.RECEIVE_ENDPOINT,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            ttl=request.app.state.idempotency_ttl,
        )
        if decision.state != "in_progress":
            return decision
        if asyncio.get_running_loop().time() >= deadline:
            return decision
        await asyncio.sleep(0.05)


async def _heartbeat_loop(
    request: Request,
    *,
    identity: Identity,
    source_id: str,
    idempotency_key: str,
) -> None:
    while True:
        await asyncio.sleep(request.app.state.heartbeat_interval.total_seconds())
        store.refresh_idempotency(
            request.app.state.engine,
            operator_username=identity.operator_username,
            endpoint=store.RECEIVE_ENDPOINT,
            idempotency_key=idempotency_key,
        )
        store.refresh_source_claim(
            request.app.state.engine,
            source_id=source_id,
            idempotency_key=idempotency_key,
        )


def _receive_and_register(
    request: Request,
    identity: Identity,
    source: SourceEntry,
    landing: LandingEntry,
    body: ReceiveRequest,
) -> dict[str, str]:
    receive_impl: Callable[..., ReceiveResult] = request.app.state.receive_source
    register_impl: Callable[..., Any] = request.app.state.register_intake
    result = receive_impl(
        source.path,
        landing=landing.path,
        source_kind=source.kind,
        operator=identity.operator_username,
        source_ref=source.source_id,
        artifactclass=body.artifactclass,
        label=body.label,
    )
    with session_scope(request.app.state.engine) as session:
        register_impl(session, result.intake_dir, artifactclass=body.artifactclass)
    return {"intakeId": result.intake_id, "status": "received"}


def _artifactclasses(engine: Any) -> list[dict[str, str]]:
    factory = make_session_factory(engine)
    with factory() as session:
        records = session.scalars(
            select(ArtifactClassPolicyRecord).order_by(ArtifactClassPolicyRecord.artifactclass)
        )
        return [
            {"artifactclass": record.artifactclass, "label": record.artifactclass}
            for record in records
        ]


def _validate_artifactclass(engine: Any, artifactclass: str) -> None:
    factory = make_session_factory(engine)
    with factory() as session:
        get_artifactclass_policy(session, artifactclass)


def _list_sources(request: Request) -> list[SourceEntry]:
    return request.app.state.list_sources()


def _list_landings(request: Request) -> list[LandingEntry]:
    return request.app.state.list_landings()


def _resolve_source(request: Request, source_id: str) -> SourceEntry:
    try:
        return request.app.state.resolve_source(source_id)
    except CatalogError as exc:
        _raise(400, "bad_source", str(exc))


def _resolve_landing(request: Request, landing_id: str) -> LandingEntry:
    try:
        return request.app.state.resolve_landing(landing_id)
    except CatalogError as exc:
        _raise(400, "bad_landing", str(exc))


def _request_hash(body: ReceiveRequest) -> str:
    payload = {
        "artifactclass": body.artifactclass,
        "idempotencyKey": str(body.idempotencyKey),
        "label": body.label,
        "landingId": body.landingId,
        "sourceId": body.sourceId,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _raise(status_code: int, error: str, detail: str) -> None:
    raise HTTPException(status_code=status_code, detail={"error": error, "detail": detail})


def install_default_state(app: Any) -> None:
    """Install swappable receive dependencies on a FastAPI app instance."""

    app.state.receive_source = receive_source
    app.state.register_intake = register_intake
    app.state.list_sources = default_list_sources
    app.state.list_landings = default_list_landings
    app.state.resolve_source = default_resolve_source
    app.state.resolve_landing = default_resolve_landing
    app.state.idempotency_ttl = store.DEFAULT_TTL
    app.state.claim_ttl = store.DEFAULT_TTL
    app.state.heartbeat_interval = store.DEFAULT_HEARTBEAT_INTERVAL
    app.state.idempotency_wait_seconds = 10.0
