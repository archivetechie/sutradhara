"""FastAPI application factory for Sutradhara's operator console API."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from sqlalchemy import Engine

from sutradhara.api.routes_activity import router as activity_router
from sutradhara.api.routes_devices import AgentBundleConfigError, parse_agent_bundle_config
from sutradhara.api.routes_devices import install_default_state as install_device_state
from sutradhara.api.routes_devices import router as devices_router
from sutradhara.api.routes_intake_archive import router as intake_archive_router
from sutradhara.api.routes_jobs import router as jobs_router
from sutradhara.api.routes_library import router as library_router
from sutradhara.api.routes_logs import router as logs_router
from sutradhara.api.routes_receive import install_default_state
from sutradhara.api.routes_receive import router as receive_router
from sutradhara.api.routes_restore import router as restore_router
from sutradhara.api.routes_session import router as session_router
from sutradhara.archive_predicate import archived_all_semantics_enabled
from sutradhara.catalog.session import create_all, make_engine

AGENT_BUNDLE_CONFIG_ENV = "SUTRA_AGENT_BUNDLE_CONFIG"
UNSAFE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
ORIGIN_GUARD_EXEMPT_PATHS = {"/api/enroll/csr"}
LOG = logging.getLogger(__name__)


def create_app(
    engine: Engine | None = None,
    *,
    ensure_schema: bool = True,
    registry: object | None = None,
    grpc_pki_dir: object | None = None,
    grpc_progress_registry: object | None = None,
) -> FastAPI:
    """Create the HTTP API with catalog-backed state and strict edge assumptions."""

    try:
        archive_all_semantics = archived_all_semantics_enabled()
    except ValueError as exc:
        raise RuntimeError(f"invalid configuration: {exc}") from exc
    final_engine = engine or make_engine()
    if ensure_schema:
        create_all(final_engine)
    app = FastAPI(title="sutradhara API")
    app.state.engine = final_engine
    app.state.archived_all_semantics = archive_all_semantics
    if registry is not None:
        app.state.registry = registry
    if grpc_pki_dir is not None:
        app.state.grpc_pki_dir = grpc_pki_dir
    if grpc_progress_registry is not None:
        app.state.grpc_progress_registry = grpc_progress_registry
    agent_bundle = _load_agent_bundle_config_from_env()
    if agent_bundle is not None:
        app.state.agent_bundle = agent_bundle
    install_default_state(app)
    install_device_state(app)

    @app.middleware("http")
    async def _json_origin_guard(request: Request, call_next: object) -> Response:
        if request.url.path.startswith("/api/") and request.method in UNSAFE_METHODS:
            media_type = request.headers.get("content-type", "").split(";", 1)[0].strip()
            if media_type.lower() != "application/json":
                return _error_response(
                    415,
                    "unsupported_media_type",
                    "mutating API requests must be application/json",
                )
            if request.url.path not in ORIGIN_GUARD_EXEMPT_PATHS:
                host = request.headers.get("host")
                origin = request.headers.get("origin")
                if not host or not origin or not _same_origin(origin, host):
                    return _error_response(403, "forbidden_origin", "Origin must match Host")
        return await call_next(request)  # type: ignore[misc]

    @app.exception_handler(HTTPException)
    async def _http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        detail = exc.detail
        if (
            _uses_nested_ui_envelope(request.url.path)
            and isinstance(detail, dict)
            and {"error", "detail"} <= set(detail)
        ):
            return JSONResponse(
                {"detail": detail},
                status_code=exc.status_code,
                headers=exc.headers,
            )
        if isinstance(detail, dict) and {"error", "detail"} <= set(detail):
            return JSONResponse(detail, status_code=exc.status_code, headers=exc.headers)
        return _error_response(exc.status_code, "http_error", str(detail), headers=exc.headers)

    @app.exception_handler(RequestValidationError)
    async def _validation_exception_handler(
        _request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        return _error_response(400, "validation_error", str(exc))

    app.include_router(session_router)
    app.include_router(receive_router)
    app.include_router(devices_router)
    app.include_router(activity_router)
    app.include_router(restore_router)
    app.include_router(jobs_router)
    app.include_router(intake_archive_router)
    app.include_router(library_router)
    app.include_router(logs_router)
    return app


def _load_agent_bundle_config_from_env() -> dict[str, object] | None:
    """Load and validate the optional enrollment-bundle deployment config."""

    config_path_text = os.environ.get(AGENT_BUNDLE_CONFIG_ENV)
    if not config_path_text:
        LOG.warning(
            "%s is unset; POST /api/enroll/bundle will return bundle_not_configured",
            AGENT_BUNDLE_CONFIG_ENV,
        )
        return None
    config_path = Path(config_path_text).expanduser()
    if not config_path.exists():
        LOG.warning(
            "%s points to missing file %s; POST /api/enroll/bundle will return "
            "bundle_not_configured",
            AGENT_BUNDLE_CONFIG_ENV,
            config_path,
        )
        return None
    if not config_path.is_file():
        raise RuntimeError(f"{AGENT_BUNDLE_CONFIG_ENV} path is not a file: {config_path}")
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"invalid JSON in {AGENT_BUNDLE_CONFIG_ENV} {config_path}: {exc}"
        ) from exc
    except OSError as exc:
        raise RuntimeError(
            f"could not read {AGENT_BUNDLE_CONFIG_ENV} {config_path}: {exc}"
        ) from exc
    if not isinstance(raw, dict):
        raise RuntimeError(f"{AGENT_BUNDLE_CONFIG_ENV} {config_path} must contain a JSON object")
    try:
        parse_agent_bundle_config(raw)
    except AgentBundleConfigError as exc:
        raise RuntimeError(f"invalid {AGENT_BUNDLE_CONFIG_ENV} {config_path}: {exc}") from exc
    return raw


def _same_origin(origin: str, host: str) -> bool:
    parsed = urlparse(origin)
    if parsed.scheme not in {"http", "https"}:
        return False
    return parsed.netloc.lower() == host.lower()


def _uses_nested_ui_envelope(path: str) -> bool:
    return (
        path.startswith("/api/ui/restore")
        or path == "/api/ui/reconciliation"
        or path.startswith("/api/ui/jobs")
        or path == "/api/ui/resources"
        or path.startswith("/api/ui/intakes")
        or path.startswith("/api/ui/archive")
        or path == "/api/ui/catalog/assets"
        or path.startswith("/api/ui/library")
        or path == "/api/ui/logs"
    )


def _error_response(
    status_code: int,
    error: str,
    detail: str,
    *,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(
        {"error": error, "detail": detail},
        status_code=status_code,
        headers=headers,
    )
