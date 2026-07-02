"""FastAPI application factory for Sutradhara's operator console API."""

from __future__ import annotations

from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from sqlalchemy import Engine

from sutradhara.api.routes_activity import router as activity_router
from sutradhara.api.routes_devices import install_default_state as install_device_state
from sutradhara.api.routes_devices import router as devices_router
from sutradhara.api.routes_receive import install_default_state
from sutradhara.api.routes_receive import router as receive_router
from sutradhara.api.routes_session import router as session_router
from sutradhara.catalog.session import create_all, make_engine

UNSAFE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
ORIGIN_GUARD_EXEMPT_PATHS = {"/api/enroll/csr"}


def create_app(
    engine: Engine | None = None,
    *,
    ensure_schema: bool = True,
    registry: object | None = None,
    grpc_pki_dir: object | None = None,
) -> FastAPI:
    """Create the HTTP API with catalog-backed state and strict edge assumptions."""

    final_engine = engine or make_engine()
    if ensure_schema:
        create_all(final_engine)
    app = FastAPI(title="sutradhara API")
    app.state.engine = final_engine
    if registry is not None:
        app.state.registry = registry
    if grpc_pki_dir is not None:
        app.state.grpc_pki_dir = grpc_pki_dir
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
    async def _http_exception_handler(_request: Request, exc: HTTPException) -> JSONResponse:
        detail = exc.detail
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
    return app


def _same_origin(origin: str, host: str) -> bool:
    parsed = urlparse(origin)
    if parsed.scheme not in {"http", "https"}:
        return False
    return parsed.netloc.lower() == host.lower()


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
