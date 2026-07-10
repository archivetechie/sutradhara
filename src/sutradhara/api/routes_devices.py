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
import logging
import re
import tempfile
import threading
from functools import partial
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID

import anyio
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy import select

from sutradhara._proto import device_pb2
from sutradhara.api import store as api_store
from sutradhara.api.identity import Identity, parse_identity
from sutradhara.api.paths import DevicePathError, canonical_device_rel_path
from sutradhara.api.receive_history import ReceiveHistoryMatch, latest_card_history
from sutradhara.artifactclass_policy import ArtifactClassPolicyError, get_artifactclass_policy
from sutradhara.catalog.session import make_session_factory
from sutradhara.grpc import ca as grpc_ca
from sutradhara.grpc import store as grpc_store
from sutradhara.grpc.progress import ReceiveProgressRegistry
from sutradhara.grpc.registry import (
    CardUnavailable,
    CommandAck,
    ConnectedDeviceRegistry,
    DeviceOffline,
    DeviceOwnerMismatch,
    StreamClosed,
    validate_card_id,
    validate_card_label,
)
from sutradhara.grpc.status import intake_landing_path, intake_receipt_summary, intake_status
from sutradhara.verification_progress import read_verification_progress

router = APIRouter()
LOG = logging.getLogger(__name__)
BUNDLE_FORMAT = "sutra-enroll-bundle-v1"
DEVICE_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


class DeviceReceiveRequest(BaseModel):
    """JSON body accepted by POST /api/devices/{device_id}/receive."""

    model_config = ConfigDict(extra="forbid")

    card_id: str
    artifactclass: str
    idempotencyKey: UUID
    label: str | None = None
    source_ref: str | None = None
    acknowledge_duplicate: bool = False

    @field_validator("card_id")
    @classmethod
    def _valid_card_id(cls, value: str) -> str:
        return validate_card_id(value)

    @field_validator("label")
    @classmethod
    def _valid_label(cls, value: str | None) -> str | None:
        return validate_card_label(value)


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
    """Return durable registrations, online devices, and in-flight receives."""

    identity = _require_view(parse_identity(request.headers))
    registry = _registry(request)
    devices = await anyio.to_thread.run_sync(registry.devices_for, identity.operator_username)
    registered_devices = await anyio.to_thread.run_sync(
        _registered_device_payloads_for_operator,
        request.app.state.engine,
        identity.operator_username,
        devices,
    )
    receives = await anyio.to_thread.run_sync(
        _receive_payloads_for_operator,
        request.app.state.engine,
        identity.operator_username,
        _progress_registry(request),
    )
    device_payloads = await anyio.to_thread.run_sync(
        _device_payloads_with_history,
        request.app.state.engine,
        identity.operator_username,
        devices,
    )
    return {
        "registeredDevices": registered_devices,
        "devices": device_payloads,
        "receives": receives,
    }


@router.get("/api/devices/{device_id}/browse")
async def get_device_browse(
    device_id: str,
    request: Request,
    card_id: str,
    path: str | None = None,
) -> dict[str, object]:
    """Relay one directory-listing request to a browse-capable helper."""

    identity = _require_receive(parse_identity(request.headers))
    try:
        validate_card_id(card_id)
    except ValueError as exc:
        _raise(400, "invalid_card_id", str(exc))
    rel_path = _canonical_device_path_or_400(path)
    try:
        await anyio.to_thread.run_sync(
            partial(
                _validate_device_owner,
                request.app.state.engine,
                operator=identity.operator_username,
                device_id=device_id,
            )
        )
    except PermissionError as exc:
        _raise(403, "forbidden", str(exc))

    registry = _registry(request)
    try:
        device = await anyio.to_thread.run_sync(
            partial(
                registry.device_for,
                operator=identity.operator_username,
                device_id=device_id,
            )
        )
    except DeviceOwnerMismatch as exc:
        _raise(403, "forbidden", str(exc))
    except DeviceOffline as exc:
        _raise(404, "device_not_found", str(exc))
    if "browse" not in device.capabilities:
        _raise(409, "browse_unsupported", "device helper does not support folder browse")
    if not any(card.card_id == card_id for card in device.cards):
        _raise(404, "card_not_found", "card is not present on the device")

    try:
        pending = await anyio.to_thread.run_sync(
            partial(
                registry.request_directory_listing,
                operator=identity.operator_username,
                device_id=device_id,
                card_id=card_id,
                rel_path=rel_path,
            )
        )
    except DeviceOwnerMismatch as exc:
        _raise(403, "forbidden", str(exc))
    except (DeviceOffline, CardUnavailable) as exc:
        _raise(404, "device_not_found", str(exc))

    try:
        listing = await _await_listing(
            pending.future,
            timeout=request.app.state.directory_listing_timeout,
        )
    except TimeoutError:
        await anyio.to_thread.run_sync(
            partial(
                registry.fail_listing,
                device_id,
                pending.generation,
                pending.request_id,
                TimeoutError("directory listing timed out"),
            )
        )
        _raise(504, "browse_timeout", "device did not return a directory listing in time")
    except StreamClosed as exc:
        _raise(409, "device_unavailable", str(exc))
    except RuntimeError as exc:
        _raise(409, "device_unavailable", str(exc))

    _raise_for_directory_status(listing)
    return {
        "path": rel_path,
        "entries": [
            {
                "name": entry.name,
                "isDir": entry.is_dir,
                "sizeBytes": int(entry.size_bytes),
                "isPackage": entry.is_package,
            }
            for entry in listing.entries
        ],
        "truncated": listing.truncated,
    }


@router.post("/api/devices/{device_id}/receive", response_model=None)
async def post_device_receive(
    device_id: str,
    request: Request,
    body: DeviceReceiveRequest,
) -> dict[str, str] | JSONResponse:
    """Relay a receive-start command to an operator-owned helper device."""

    identity = _require_receive(parse_identity(request.headers))
    engine = request.app.state.engine
    canonical_source_ref = _canonical_device_path_or_400(body.source_ref)
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
    request_hash = _device_receive_hash(device_id, body, source_ref=canonical_source_ref)
    # Idempotency verdicts precede card resolution: a mutated-body replay must
    # surface idempotency_conflict (not device_unavailable), and stored
    # warned/completed responses replay even after the card is ejected.
    peeked = await anyio.to_thread.run_sync(
        partial(
            api_store.peek_device_receive_intent,
            engine,
            operator_username=identity.operator_username,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            acknowledge_duplicate=body.acknowledge_duplicate,
            ttl=request.app.state.idempotency_ttl,
        )
    )
    if peeked is not None:
        if peeked.state == "conflict":
            _raise(409, "idempotency_conflict", "same idempotencyKey used with a different body")
        if peeked.state == "warned" and peeked.response_json is not None:
            return JSONResponse(status_code=409, content=peeked.response_json)
        if peeked.state == "completed" and peeked.response_json is not None:
            return {str(key): str(value) for key, value in peeked.response_json.items()}
        if peeked.state == "terminal":
            _raise(
                409,
                "receive_terminal",
                f"prior receive attempt is {peeked.terminal_state}; start a new receive intent",
                retryable=True,
            )

    try:
        card = await anyio.to_thread.run_sync(
            partial(
                _registry(request).card_for,
                operator=identity.operator_username,
                device_id=device_id,
                card_id=body.card_id,
            )
        )
    except DeviceOwnerMismatch as exc:
        _raise(403, "forbidden", str(exc))
    except (DeviceOffline, CardUnavailable) as exc:
        _raise(409, "device_unavailable", str(exc))
    decision = await anyio.to_thread.run_sync(
        partial(
            api_store.begin_device_receive_intent,
            engine,
            operator_username=identity.operator_username,
            device_id=device_id,
            card_identity=card.card_id,
            card_label=card.label,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            acknowledge_duplicate=body.acknowledge_duplicate,
            ttl=request.app.state.idempotency_ttl,
        )
    )
    if decision.state == "conflict":
        _raise(409, "idempotency_conflict", "same idempotencyKey used with a different body")
    if decision.state == "warned":
        if decision.response_json is None:
            _raise(409, "idempotency_conflict", "warned intent has no stored warning")
        return JSONResponse(status_code=409, content=decision.response_json)
    if decision.state == "completed":
        if decision.response_json is None:
            _raise(409, "idempotency_conflict", "completed request has no stored response")
        return {str(key): str(value) for key, value in decision.response_json.items()}
    if decision.state == "busy":
        _raise(409, "source_busy", "card is busy / already in progress")
    if decision.state == "terminal":
        _raise(
            409,
            "receive_terminal",
            f"prior receive attempt is {decision.terminal_state}; start a new receive intent",
            retryable=True,
        )
    if decision.state == "in_progress":
        _raise(409, "already_in_progress", "receive is already in progress")
    if decision.state != "authorized":
        _raise(409, "idempotency_conflict", "receive intent could not be authorized")

    try:
        pending = await anyio.to_thread.run_sync(
            partial(
                _registry(request).send_start_receive,
                operator=identity.operator_username,
                device_id=device_id,
                card_id=body.card_id,
                artifactclass=body.artifactclass,
                label=body.label,
                source_ref=canonical_source_ref,
                idempotency_key=idempotency_key,
                abandon_on_reject=True,
            )
        )
    except DeviceOwnerMismatch as exc:
        await _fail_device_intent(request, identity, device_id, idempotency_key)
        _raise(403, "forbidden", str(exc))
    except (DeviceOffline, CardUnavailable) as exc:
        await _fail_device_intent(request, identity, device_id, idempotency_key)
        _raise(409, "device_unavailable", str(exc))

    try:
        ack = await _await_ack(pending.future, timeout=request.app.state.command_ack_timeout)
    except TimeoutError:
        _raise(409, "ack_timeout", "device did not ack before the advisory timeout")
    except PermissionError as exc:
        await _fail_device_intent(request, identity, device_id, idempotency_key)
        _raise(403, "forbidden", str(exc))
    except RuntimeError as exc:
        failure_state = await anyio.to_thread.run_sync(
            partial(
                api_store.fail_device_receive_intent_if_unstarted,
                engine,
                operator_username=identity.operator_username,
                device_id=device_id,
                idempotency_key=idempotency_key,
            )
        )
        if failure_state == "in_progress":
            _raise(409, "already_in_progress", "receive is already in progress")
        _raise(409, "device_unavailable", str(exc))

    if not ack.accepted or not ack.intake_id:
        if pending.abandon_on_reject:
            await anyio.to_thread.run_sync(
                partial(
                    api_store.fail_device_receive_intent,
                    engine,
                    operator_username=identity.operator_username,
                    device_id=device_id,
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
    view = intake_status(row)
    return {
        "intakeId": row.intake_id,
        "status": view.status,
        "errors": view.errors,
        "releaseSafe": view.release_safe,
        **_receive_progress_payload(row, view.status, _progress_registry(request)),
    }


@router.post("/api/enroll/token")
def post_enroll_token(request: Request, body: EnrollTokenRequest) -> dict[str, str]:
    """Mint a one-time operator-scoped, device-bound enrollment token."""

    token, expires = mint_enroll_token(request, body)
    return {"token": token, "deviceId": body.device_id, "expiresAt": expires.isoformat()}


@router.post("/api/enroll/bundle")
def post_enroll_bundle(request: Request, body: EnrollTokenRequest) -> Response:
    """Mint and package a downloadable enrollment bundle."""

    identity = _require_receive(parse_identity(request.headers))
    config = _agent_bundle_config_or_503(request)
    token, expires = mint_enroll_token(request, body, identity=identity)
    bundle: dict[str, object] = {
        "format": BUNDLE_FORMAT,
        "device_id": body.device_id,
        "enroll_url": config.enroll_url,
        "enroll_ca_pem": config.enroll_ca_pem,
        "token": token,
        "expires_at": expires.isoformat(),
        "endpoints": [
            {
                "address": endpoint.address,
                **(
                    {"server_name": endpoint.server_name}
                    if endpoint.server_name is not None
                    else {}
                ),
            }
            for endpoint in config.endpoints
        ],
    }
    if config.console_url is not None:
        bundle["console_url"] = config.console_url
    response = Response(content=json.dumps(bundle), media_type="application/json; charset=utf-8")
    response.headers["Content-Disposition"] = (
        f'attachment; filename="{body.device_id}.sutra-enroll"'
    )
    response.headers["Cache-Control"] = "no-store, private"
    return response


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
        except grpc_ca.DeviceOwnershipCertificateError as exc:
            _raise(409, "device_other_operator", str(exc))
        except grpc_ca.DeviceRotationProofCertificateError as exc:
            _raise(409, "old_key_proof_required", str(exc))
        except grpc_ca.CertificateError as exc:
            _raise(400, "bad_enrollment", str(exc))
        except ValueError as exc:
            _raise(400, "bad_enrollment", str(exc))
        evicted = _registry(request).evict(
            signed.device_id,
            reason=StreamClosed("device certificate rotated"),
        )
        if evicted:
            LOG.info("evicted live device stream after certificate rotation: %s", signed.device_id)
        ca_pem = (pki_dir / grpc_ca.CA_CERT_NAME).read_text(encoding="utf-8")
        cert_pem = signed.cert_path.read_text(encoding="utf-8")
    return {"cert_pem": cert_pem, "ca_pem": ca_pem}


def install_default_state(app: object) -> None:
    """Install swappable operator-console relay dependencies."""

    app.state.registry = getattr(app.state, "registry", ConnectedDeviceRegistry())
    app.state.grpc_progress_registry = getattr(
        app.state,
        "grpc_progress_registry",
        ReceiveProgressRegistry(),
    )
    app.state.command_ack_timeout = 5.0
    app.state.directory_listing_timeout = 5.0
    app.state.enroll_token_ttl = dt.timedelta(hours=24)
    app.state.enroll_csr_limiter = SimpleRateLimiter()
    app.state.grpc_pki_dir = getattr(app.state, "grpc_pki_dir", grpc_ca.DEFAULT_PKI_DIR)
    app.state.agent_bundle = getattr(app.state, "agent_bundle", None)
    api_store.reconcile_device_receive_leases(
        app.state.engine,
        ttl=app.state.idempotency_ttl,
    )


def _registry(request: Request) -> ConnectedDeviceRegistry:
    return request.app.state.registry


def _progress_registry(request: Request) -> ReceiveProgressRegistry:
    return request.app.state.grpc_progress_registry


def mint_enroll_token(
    request: Request,
    body: EnrollTokenRequest,
    *,
    identity: Identity | None = None,
) -> tuple[str, dt.datetime]:
    """Apply the shared enroll-token mint guard and return ``(token, expires_at)``."""

    if identity is None:
        identity = _require_receive(parse_identity(request.headers))
    _validate_device_id(body.device_id)
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
        try:
            rotation_authority, rotation_fingerprint = _rotation_authorization(
                _registry(request),
                identity,
                device_id=body.device_id,
                owner=owner,
                reenroll=body.reenroll,
            )
        except (DeviceOffline, DeviceOwnerMismatch):
            _raise(
                409,
                "old_key_proof_required",
                "re-enroll requires the current device stream or an admin rotation",
            )
        if rotation_authority == "self" and rotation_fingerprint is not None:
            try:
                proof_identity = grpc_store.resolve_device(
                    session,
                    device_id=body.device_id,
                    cert_fingerprint=rotation_fingerprint,
                )
            except (PermissionError, ValueError):
                _raise(
                    409,
                    "old_key_proof_required",
                    "re-enroll requires the current active device certificate",
                )
            if proof_identity.operator != identity.operator_username:
                _raise(
                    409,
                    "old_key_proof_required",
                    "re-enroll requires the current active device certificate",
                )
        token = grpc_store.issue_enroll_token(
            session,
            operator=identity.operator_username,
            device_id=body.device_id,
            ttl=ttl,
            rotation_authority=rotation_authority,
            rotation_fingerprint=rotation_fingerprint,
        )
    expires = dt.datetime.now(dt.UTC) + ttl
    return token, expires


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


def _validate_device_id(device_id: str) -> None:
    if not DEVICE_ID_PATTERN.fullmatch(device_id) or device_id in {".", ".."}:
        _raise(
            400,
            "invalid_device_id",
            "device_id must match ^[A-Za-z0-9._-]{1,128}$",
        )


class _AgentBundleEndpoint(BaseModel):
    address: str
    server_name: str | None = None


class _AgentBundleConfig(BaseModel):
    endpoints: list[_AgentBundleEndpoint]
    enroll_url: str
    enroll_ca_pem: str
    console_url: str | None = None


class AgentBundleConfigError(ValueError):
    """The configured enrollment bundle source is incomplete or invalid."""


def parse_agent_bundle_config(raw: object) -> _AgentBundleConfig:
    """Validate an enrollment bundle config and read the configured enrollment CA."""

    if raw is None:
        raise AgentBundleConfigError("agent bundle config is missing")
    try:
        endpoints = _agent_bundle_endpoints(raw)
        enroll_url = _agent_bundle_https_url(raw, "enroll_url")
        enroll_ca_path = _agent_bundle_path(raw, "enroll_ca_path")
        console_url = _agent_bundle_text(raw, "console_url")
    except (KeyError, TypeError, ValueError) as exc:
        raise AgentBundleConfigError("agent bundle config is incomplete") from exc
    if not endpoints or enroll_url is None or enroll_ca_path is None:
        raise AgentBundleConfigError("agent bundle config is incomplete")
    try:
        enroll_ca_pem = enroll_ca_path.read_text(encoding="utf-8")
    except (OSError, ValueError) as exc:
        raise AgentBundleConfigError(f"enroll_ca_path is unreadable: {enroll_ca_path}") from exc
    return _AgentBundleConfig(
        endpoints=endpoints,
        enroll_url=enroll_url,
        enroll_ca_pem=enroll_ca_pem,
        console_url=console_url,
    )


def _agent_bundle_config_or_503(request: Request) -> _AgentBundleConfig:
    raw = getattr(request.app.state, "agent_bundle", None)
    try:
        return parse_agent_bundle_config(raw)
    except AgentBundleConfigError:
        _raise(503, "bundle_not_configured", "enrollment bundle config is incomplete")


def _agent_bundle_endpoints(raw: object) -> list[_AgentBundleEndpoint]:
    endpoints_raw = _agent_bundle_value(raw, "endpoints")
    if not isinstance(endpoints_raw, list):
        raise TypeError("endpoints must be a list")
    endpoints: list[_AgentBundleEndpoint] = []
    for item in endpoints_raw:
        address = _agent_bundle_text(item, "address")
        if address is None:
            raise ValueError("endpoint address missing")
        server_name = _agent_bundle_text(item, "server_name")
        endpoints.append(_AgentBundleEndpoint(address=address, server_name=server_name))
    return endpoints


def _agent_bundle_path(raw: object, key: str) -> Path | None:
    value = _agent_bundle_value(raw, key)
    if value is None:
        return None
    return Path(str(value))


def _agent_bundle_https_url(raw: object, key: str) -> str | None:
    text = _agent_bundle_text(raw, key)
    if text is None:
        return None
    parsed = urlparse(text)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError(f"{key} must be an absolute https URL")
    return text


def _agent_bundle_text(raw: object, key: str) -> str | None:
    value = _agent_bundle_value(raw, key)
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _agent_bundle_value(raw: object, key: str) -> object | None:
    if isinstance(raw, dict):
        return raw.get(key)
    return getattr(raw, key, None)


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


def _rotation_authorization(
    registry: ConnectedDeviceRegistry,
    identity: Identity,
    *,
    device_id: str,
    owner: str | None,
    reenroll: bool,
) -> tuple[grpc_store.RotationAuthority | None, str | None]:
    if owner is None or owner != identity.operator_username:
        return None, None
    if not reenroll:
        return None, None
    if identity.has_capability("can_admin"):
        return "admin", None
    return (
        "self",
        registry.active_fingerprint_for(
            operator=identity.operator_username,
            device_id=device_id,
        ),
    )


def _device_payload(
    device: object,
    history_by_card: dict[str, ReceiveHistoryMatch | None],
) -> dict[str, object]:
    return {
        "deviceId": device.device_id,
        "enrolledAs": device.operator,
        "online": True,
        "lastSeenAt": _datetime_payload(device.last_seen),
        "capabilities": list(device.capabilities),
        "cards": [
            {
                "cardId": card.card_id,
                "label": card.label,
                "kind": card.kind,
                "sizeBytes": card.size_bytes,
                "status": card.status,
                "receivedBefore": _received_before_payload(history_by_card.get(card.card_id)),
            }
            for card in device.cards
        ],
    }


def _device_payloads_with_history(
    engine: object,
    operator_username: str,
    devices: list[object],
) -> list[dict[str, object]]:
    """Attach the authorization-scoped history projection to live card rows."""

    factory = make_session_factory(engine)
    with factory() as session:
        history_by_card = {
            card.card_id: latest_card_history(
                session,
                card_identity=card.card_id,
                requester=operator_username,
            )
            for device in devices
            for card in device.cards
        }
    return [_device_payload(device, history_by_card) for device in devices]


def _received_before_payload(
    match: ReceiveHistoryMatch | None,
) -> dict[str, object] | None:
    if match is None:
        return None
    if not match.visible:
        return {"state": None, "receivedAt": None, "visible": False}
    return {
        "state": match.state,
        "receivedAt": _datetime_payload(match.received_at),
        "visible": True,
    }


def _registered_device_payloads_for_operator(
    engine: object, operator_username: str, online_devices: list[object]
) -> list[dict[str, object]]:
    factory = make_session_factory(engine)
    with factory() as session:
        registered = grpc_store.registered_devices_for_operator(session, operator_username)
    online_by_id = {device.device_id: device for device in online_devices}
    return [
        {
            "deviceId": device.device_id,
            "enrolledAs": device.operator,
            "enrollmentStatus": "active",
            "online": device.device_id in online_by_id,
            "lastSeenAt": _datetime_payload(online_by_id[device.device_id].last_seen)
            if device.device_id in online_by_id
            else None,
        }
        for device in registered
    ]


def _receive_payloads_for_operator(
    engine: object,
    operator_username: str,
    progress_registry: ReceiveProgressRegistry,
) -> list[dict[str, object]]:
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
    payloads: list[dict[str, object]] = []
    for row in rows:
        view = intake_status(row)
        payloads.append(
            {
                "intakeId": row.intake_id,
                "deviceId": row.device_id,
                "cardId": row.card_id,
                "status": view.status,
                "releaseSafe": view.release_safe,
                **_receive_progress_payload(row, view.status, progress_registry),
            }
        )
    return payloads


def _receive_progress_payload(
    row: grpc_store.GrpcIntake,
    status: str,
    progress_registry: ReceiveProgressRegistry,
) -> dict[str, object]:
    """Return additive operator-console progress metadata for a durable receive."""

    receipt = intake_receipt_summary(row)
    receipt_bytes = receipt.bytes_total if receipt is not None else None
    live = progress_registry.snapshot(row.intake_id)
    if live is not None:
        live_received = sum(item.bytes_received for item in live.files)
        live_total = sum(item.bytes_total for item in live.files)
        bytes_received = (receipt_bytes or 0) + live_received
        live_planned = live.bytes_total
        total_candidates = [
            value
            for value in (
                live_planned,
                (receipt_bytes or 0) + live_total if live_total > 0 else None,
                receipt_bytes,
            )
            if value is not None
        ]
        bytes_total = max(total_candidates) if total_candidates else None
    else:
        bytes_received = receipt_bytes
        bytes_total = (
            receipt_bytes
            if receipt_bytes is not None
            and status in {"verifying", "verified", "quarantined", "discrepancy"}
            else None
        )
    verification = read_verification_progress(intake_landing_path(row))
    return {
        "destinationPath": str(intake_landing_path(row)),
        "bytesReceived": bytes_received,
        "bytesTotal": bytes_total,
        "verificationBytesVerified": (
            verification.bytes_verified if verification is not None else None
        ),
        "verificationBytesTotal": verification.bytes_total if verification is not None else None,
    }


def _device_receive_hash(device_id: str, body: DeviceReceiveRequest, *, source_ref: str) -> str:
    payload = {
        "artifactclass": body.artifactclass,
        "card_id": body.card_id,
        "device_id": device_id,
        "label": body.label,
        "source_ref": source_ref,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _datetime_payload(value: dt.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC).isoformat()


async def _await_ack(future: object, *, timeout: float) -> CommandAck:
    wrapped = asyncio.wrap_future(future)
    return await asyncio.wait_for(asyncio.shield(wrapped), timeout=timeout)


async def _await_listing(future: object, *, timeout: float) -> object:
    wrapped = asyncio.wrap_future(future)
    return await asyncio.wait_for(asyncio.shield(wrapped), timeout=timeout)


def _canonical_device_path_or_400(value: str | None) -> str:
    try:
        return canonical_device_rel_path(value)
    except DevicePathError as exc:
        _raise(400, "bad_path", str(exc))


def _raise_for_directory_status(listing: object) -> None:
    status = listing.status
    if status == device_pb2.DIR_STATUS_OK:
        return
    http_status, error, fallback = {
        device_pb2.DIR_STATUS_NOT_FOUND: (404, "not_found", "directory not found"),
        device_pb2.DIR_STATUS_NOT_A_DIRECTORY: (
            422,
            "not_a_directory",
            "path is not a directory",
        ),
        device_pb2.DIR_STATUS_PERMISSION_DENIED: (
            403,
            "permission_denied",
            "permission denied",
        ),
        device_pb2.DIR_STATUS_CONFINEMENT_VIOLATION: (
            400,
            "confinement_violation",
            "path is outside the card",
        ),
        device_pb2.DIR_STATUS_CARD_UNAVAILABLE: (
            409,
            "card_unavailable",
            "card is unavailable",
        ),
        device_pb2.DIR_STATUS_IO_ERROR: (502, "io_error", "device I/O error"),
    }.get(status, (502, "listing_failed", "directory listing failed"))
    detail = listing.detail or fallback
    _raise(http_status, error, detail)


async def _fail_device_intent(
    request: Request,
    identity: Identity,
    device_id: str,
    idempotency_key: str,
) -> None:
    await anyio.to_thread.run_sync(
        partial(
            api_store.fail_device_receive_intent,
            request.app.state.engine,
            operator_username=identity.operator_username,
            device_id=device_id,
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
            api_store.fail_device_receive_intent(
                engine,
                operator_username=operator,
                device_id=device_id,
                idempotency_key=idempotency_key,
            )
        return False
    return api_store.store_device_receive_response(
        engine,
        operator_username=operator,
        device_id=device_id,
        idempotency_key=idempotency_key,
        intake_id=intake_id,
        response_json={"intakeId": intake_id, "status": "streaming"},
    )


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


def _raise(status_code: int, error: str, detail: str, **extra: object) -> None:
    raise HTTPException(
        status_code=status_code,
        detail={"error": error, "detail": detail, **extra},
    )
