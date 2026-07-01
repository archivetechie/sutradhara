"""Operator-console device relay and enrollment endpoints.

These routes are the HTTP half of the server-brokered relay. They expose only
the authenticated operator's live helper streams, combine that with durable
``grpc_intake`` receive state, and relay receive-start commands through the
shared in-memory registry.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import tempfile
import threading
from functools import partial
from pathlib import Path
from uuid import UUID

import anyio
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select

from sutradhara.api import store as api_store
from sutradhara.api.identity import Identity, parse_identity
from sutradhara.artifactclass_policy import ArtifactClassPolicyError, get_artifactclass_policy
from sutradhara.catalog.session import make_session_factory
from sutradhara.grpc import ca as grpc_ca
from sutradhara.grpc import store as grpc_store
from sutradhara.grpc.registry import (
    CardUnavailable,
    CommandAck,
    ConnectedDeviceRegistry,
    DeviceOffline,
    DeviceOwnerMismatch,
)
from sutradhara.grpc.status import intake_status

router = APIRouter()


class DeviceReceiveRequest(BaseModel):
    """JSON body accepted by POST /api/devices/{device_id}/receive."""

    card_id: str
    artifactclass: str
    idempotencyKey: UUID
    label: str | None = None
    source_ref: str | None = None


class EnrollTokenRequest(BaseModel):
    """JSON body accepted by POST /api/enroll/token."""

    device_id: str
    reenroll: bool = False


class EnrollCsrRequest(BaseModel):
    """JSON body accepted by POST /api/enroll/csr."""

    csr_pem: str
    token: str


class SimpleRateLimiter:
    """Small in-process fixed-window rate limiter for the pre-cert CSR endpoint."""

    def __init__(self, *, limit: int = 10, window: dt.timedelta = dt.timedelta(minutes=1)) -> None:
        self.limit = limit
        self.window = window
        self._lock = threading.Lock()
        self._hits: dict[str, list[dt.datetime]] = {}

    def allow(self, key: str, *, now: dt.datetime | None = None) -> bool:
        """Return whether ``key`` may perform one more request in the current window."""

        current = now or dt.datetime.now(dt.UTC)
        cutoff = current - self.window
        with self._lock:
            hits = [item for item in self._hits.get(key, []) if item >= cutoff]
            if len(hits) >= self.limit:
                self._hits[key] = hits
                return False
            hits.append(current)
            self._hits[key] = hits
            return True


@router.get("/api/devices")
async def get_devices(request: Request) -> dict[str, object]:
    """Return online devices and durable in-flight receives for the operator."""

    identity = _require_view(parse_identity(request.headers))
    registry = _registry(request)
    devices = await anyio.to_thread.run_sync(registry.devices_for, identity.operator_username)
    receives = await anyio.to_thread.run_sync(
        _receive_payloads_for_operator,
        request.app.state.engine,
        identity.operator_username,
    )
    return {
        "devices": [_device_payload(device) for device in devices],
        "receives": receives,
    }


@router.post("/api/devices/{device_id}/receive")
async def post_device_receive(
    device_id: str,
    request: Request,
    body: DeviceReceiveRequest,
) -> dict[str, str]:
    """Relay a receive-start command to an operator-owned helper device."""

    identity = _require_receive(parse_identity(request.headers))
    engine = request.app.state.engine
    try:
        await anyio.to_thread.run_sync(
            partial(
                _validate_receive_start,
                engine,
                operator=identity.operator_username,
                device_id=device_id,
                artifactclass=body.artifactclass,
            )
        )
    except PermissionError as exc:
        _raise(403, "forbidden", str(exc))
    except ArtifactClassPolicyError as exc:
        _raise(400, "bad_artifactclass", str(exc))

    idempotency_key = str(body.idempotencyKey)
    request_hash = _device_receive_hash(device_id, body)
    decision = await anyio.to_thread.run_sync(
        partial(
            api_store.begin_idempotency,
            engine,
            operator_username=identity.operator_username,
            endpoint=api_store.DEVICE_RECEIVE_ENDPOINT,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            ttl=request.app.state.idempotency_ttl,
        )
    )
    if decision.state == "conflict":
        _raise(409, "idempotency_conflict", "same idempotencyKey used with a different body")
    if decision.state == "completed":
        if decision.response_json is None:
            _raise(409, "idempotency_conflict", "completed request has no stored response")
        return {str(key): str(value) for key, value in decision.response_json.items()}
    if decision.state == "in_progress":
        _raise(409, "already_in_progress", "receive is already in progress")

    try:
        pending = await anyio.to_thread.run_sync(
            partial(
                _registry(request).send_start_receive,
                operator=identity.operator_username,
                device_id=device_id,
                card_id=body.card_id,
                artifactclass=body.artifactclass,
                label=body.label,
                source_ref=body.source_ref,
                idempotency_key=idempotency_key,
                abandon_on_reject=True,
            )
        )
    except DeviceOwnerMismatch as exc:
        await _abandon_if_new_claim(request, identity, idempotency_key, decision.state)
        _raise(403, "forbidden", str(exc))
    except (DeviceOffline, CardUnavailable) as exc:
        await _abandon_if_new_claim(request, identity, idempotency_key, decision.state)
        _raise(409, "device_unavailable", str(exc))

    try:
        ack = await _await_ack(pending.future, timeout=request.app.state.command_ack_timeout)
    except TimeoutError:
        _raise(409, "ack_timeout", "device did not ack before the advisory timeout")
    except PermissionError as exc:
        await _abandon_if_new_claim(request, identity, idempotency_key, decision.state)
        _raise(403, "forbidden", str(exc))
    except RuntimeError as exc:
        _raise(409, "device_unavailable", str(exc))

    if not ack.accepted or not ack.intake_id:
        if pending.abandon_on_reject:
            await anyio.to_thread.run_sync(
                partial(
                    api_store.abandon_idempotency,
                    engine,
                    operator_username=identity.operator_username,
                    endpoint=api_store.DEVICE_RECEIVE_ENDPOINT,
                    idempotency_key=idempotency_key,
                )
            )
        _raise(409, "receive_rejected", ack.reason or "device rejected receive")

    completed = await anyio.to_thread.run_sync(
        partial(
            _complete_http_receive,
            engine,
            operator=identity.operator_username,
            device_id=device_id,
            card_id=body.card_id,
            idempotency_key=idempotency_key,
            intake_id=ack.intake_id,
            abandon_on_failure=pending.abandon_on_reject,
        )
    )
    if not completed:
        _raise(409, "correlation_failed", "device ack did not match an owned intake")
    return {"intakeId": ack.intake_id, "status": "streaming"}


@router.post("/api/devices/{device_id}/revoke")
async def post_revoke_device(device_id: str, request: Request) -> dict[str, object]:
    """Revoke a device and evict its live stream from this server process."""

    _require_admin(parse_identity(request.headers))
    return await anyio.to_thread.run_sync(
        partial(_revoke_device, request.app.state.engine, _registry(request), device_id)
    )


@router.get("/api/intake/{intake_id}/status")
def get_intake_status(intake_id: str, request: Request) -> dict[str, object]:
    """Return HTTP status for an operator-owned gRPC intake."""

    identity = _require_view(parse_identity(request.headers))
    factory = make_session_factory(request.app.state.engine)
    with factory() as session:
        row = grpc_store.get_intake(session, intake_id)
        if row is None:
            _raise(404, "not_found", "unknown intake")
        if row.operator != identity.operator_username:
            _raise(403, "forbidden", "intake owner mismatch")
        session.expunge(row)
    status, errors = intake_status(row)
    return {"intakeId": row.intake_id, "status": status, "errors": errors}


@router.post("/api/enroll/token")
def post_enroll_token(request: Request, body: EnrollTokenRequest) -> dict[str, str]:
    """Mint a one-time operator-scoped, device-bound enrollment token."""

    identity = _require_receive(parse_identity(request.headers))
    factory = make_session_factory(request.app.state.engine)
    ttl = request.app.state.enroll_token_ttl
    with factory.begin() as session:
        try:
            owner = grpc_store.operator_for_device(session, body.device_id)
        except PermissionError:
            _raise(
                409,
                "device_other_operator",
                "device is enrolled to a different operator; an admin must revoke it first",
            )
        if owner == identity.operator_username and not body.reenroll:
            _raise(
                409,
                "device_already_enrolled",
                "device already enrolled - re-enroll to rotate its certificate",
            )
        if owner is not None and owner != identity.operator_username:
            _raise(
                409,
                "device_other_operator",
                "device is enrolled to a different operator; an admin must revoke it first",
            )
        token = grpc_store.issue_enroll_token(
            session,
            operator=identity.operator_username,
            device_id=body.device_id,
            ttl=ttl,
        )
    expires = dt.datetime.now(dt.UTC) + ttl
    return {"token": token, "deviceId": body.device_id, "expiresAt": expires.isoformat()}


@router.post("/api/enroll/csr")
def post_enroll_csr(request: Request, body: EnrollCsrRequest) -> dict[str, str]:
    """Redeem a token from a helper and return a signed device certificate."""

    client = request.client.host if request.client else "unknown"
    if not request.app.state.enroll_csr_limiter.allow(client):
        _raise(429, "rate_limited", "too many enrollment attempts")

    pki_dir = Path(request.app.state.grpc_pki_dir)
    with tempfile.TemporaryDirectory(prefix="sutra-enroll-") as temp:
        root = Path(temp)
        csr_path = root / "device.csr"
        cert_path = root / "device.crt"
        csr_path.write_text(body.csr_pem, encoding="utf-8")
        try:
            signed = grpc_ca.sign_device_csr(
                request.app.state.engine,
                pki_dir=pki_dir,
                csr_path=csr_path,
                token=body.token,
                cert_path=cert_path,
            )
        except grpc_ca.CertificateError as exc:
            if "different operator" in str(exc):
                _raise(409, "device_other_operator", str(exc))
            _raise(400, "bad_enrollment", str(exc))
        except ValueError as exc:
            _raise(400, "bad_enrollment", str(exc))
        ca_pem = (pki_dir / grpc_ca.CA_CERT_NAME).read_text(encoding="utf-8")
        cert_pem = signed.cert_path.read_text(encoding="utf-8")
    return {"cert_pem": cert_pem, "ca_pem": ca_pem}


def install_default_state(app: object) -> None:
    """Install swappable operator-console relay dependencies."""

    app.state.registry = getattr(app.state, "registry", ConnectedDeviceRegistry())
    app.state.command_ack_timeout = 5.0
    app.state.enroll_token_ttl = dt.timedelta(hours=24)
    app.state.enroll_csr_limiter = SimpleRateLimiter()
    app.state.grpc_pki_dir = getattr(app.state, "grpc_pki_dir", grpc_ca.DEFAULT_PKI_DIR)


def _registry(request: Request) -> ConnectedDeviceRegistry:
    return request.app.state.registry


def _require_view(identity: Identity) -> Identity:
    if not identity.has_capability("can_view"):
        _raise(403, "forbidden", "operator has no sutradhara role")
    return identity


def _require_receive(identity: Identity) -> Identity:
    _require_view(identity)
    if not identity.has_capability("can_receive"):
        _raise(403, "forbidden", "your group doesn't permit this")
    return identity


def _require_admin(identity: Identity) -> Identity:
    _require_view(identity)
    if not identity.has_capability("can_admin"):
        _raise(403, "forbidden", "your group doesn't permit this")
    return identity


def _validate_receive_start(
    engine: object,
    *,
    operator: str,
    device_id: str,
    artifactclass: str,
) -> None:
    _validate_device_owner(engine, operator=operator, device_id=device_id)
    _validate_artifactclass(engine, artifactclass)


def _validate_device_owner(engine: object, *, operator: str, device_id: str) -> None:
    factory = make_session_factory(engine)
    with factory() as session:
        owner = grpc_store.operator_for_device(session, device_id)
    if owner is None:
        raise PermissionError("device is not enrolled")
    if owner != operator:
        raise PermissionError("device belongs to a different operator")


def _validate_artifactclass(engine: object, artifactclass: str) -> None:
    factory = make_session_factory(engine)
    with factory() as session:
        get_artifactclass_policy(session, artifactclass)


def _device_payload(device: object) -> dict[str, object]:
    return {
        "deviceId": device.device_id,
        "cards": [
            {
                "cardId": card.card_id,
                "label": card.label,
                "kind": card.kind,
                "sizeBytes": card.size_bytes,
                "status": card.status,
            }
            for card in device.cards
        ],
    }


def _receive_payloads_for_operator(engine: object, operator_username: str) -> list[dict[str, str | None]]:
    factory = make_session_factory(engine)
    with factory() as session:
        rows = list(
            session.scalars(
                select(grpc_store.GrpcIntake)
                .where(
                    grpc_store.GrpcIntake.operator == operator_username,
                    grpc_store.GrpcIntake.state.in_(("streaming", "committing", "committed")),
                )
                .order_by(grpc_store.GrpcIntake.created_at)
            )
        )
        for row in rows:
            session.expunge(row)
    payloads: list[dict[str, str | None]] = []
    for row in rows:
        status, _errors = intake_status(row)
        payloads.append(
            {
                "intakeId": row.intake_id,
                "deviceId": row.device_id,
                "cardId": row.card_id,
                "status": status,
            }
        )
    return payloads


def _device_receive_hash(device_id: str, body: DeviceReceiveRequest) -> str:
    payload = {
        "artifactclass": body.artifactclass,
        "card_id": body.card_id,
        "device_id": device_id,
        "label": body.label,
        "source_ref": body.source_ref,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def _await_ack(future: object, *, timeout: float) -> CommandAck:
    wrapped = asyncio.wrap_future(future)
    return await asyncio.wait_for(asyncio.shield(wrapped), timeout=timeout)


async def _abandon_if_new_claim(
    request: Request,
    identity: Identity,
    idempotency_key: str,
    state: str,
) -> None:
    if state != "claimed":
        return
    await anyio.to_thread.run_sync(
        partial(
            api_store.abandon_idempotency,
            request.app.state.engine,
            operator_username=identity.operator_username,
            endpoint=api_store.DEVICE_RECEIVE_ENDPOINT,
            idempotency_key=idempotency_key,
        )
    )


def _complete_http_receive(
    engine: object,
    *,
    operator: str,
    device_id: str,
    card_id: str,
    idempotency_key: str,
    intake_id: str,
    abandon_on_failure: bool,
) -> bool:
    factory = make_session_factory(engine)
    with factory.begin() as session:
        correlated = grpc_store.set_card_id(
            session,
            intake_id=intake_id,
            operator=operator,
            device_id=device_id,
            card_id=card_id,
        )
    if not correlated:
        if abandon_on_failure:
            api_store.abandon_idempotency(
                engine,
                operator_username=operator,
                endpoint=api_store.DEVICE_RECEIVE_ENDPOINT,
                idempotency_key=idempotency_key,
            )
        return False
    api_store.complete_idempotency(
        engine,
        operator_username=operator,
        endpoint=api_store.DEVICE_RECEIVE_ENDPOINT,
        idempotency_key=idempotency_key,
        intake_id=intake_id,
        response_json={"intakeId": intake_id, "status": "streaming"},
    )
    return True


def _revoke_device(
    engine: object,
    registry: ConnectedDeviceRegistry,
    device_id: str,
) -> dict[str, object]:
    factory = make_session_factory(engine)
    with factory.begin() as session:
        revoked = grpc_store.revoke_device(session, device_id)
    return {
        "deviceId": device_id,
        "revokedEnrollments": revoked,
        "evicted": registry.evict(device_id),
    }


def _raise(status_code: int, error: str, detail: str) -> None:
    raise HTTPException(status_code=status_code, detail={"error": error, "detail": detail})
