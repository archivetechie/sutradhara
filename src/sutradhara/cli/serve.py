"""Merged operator server command.

``sutra serve`` hosts the mTLS gRPC intake/control port and the Authentik-fronted
HTTP API in one process so the HTTP routes can command the live helper streams
held by ``DeviceService.Connect``.
"""

from __future__ import annotations

import os
from pathlib import Path

import click
import uvicorn

from sutradhara.api.app import create_app
from sutradhara.catalog.session import create_all, make_engine
from sutradhara.cli.api import (
    DEFAULT_API_SOCKET,
    _bind_unix_socket,
    _parse_socket_mode,
    validate_tcp_host,
)
from sutradhara.grpc import ca
from sutradhara.grpc.registry import ConnectedDeviceRegistry
from sutradhara.grpc.server import (
    DEFAULT_GRPC_PORT,
    DEFAULT_LANDING_ROOT,
    GrpcServerConfig,
    make_server,
    start_sweep_loop,
)


@click.command("serve")
@click.option("--grpc-bind", default="127.0.0.1", show_default=True)
@click.option("--grpc-port", default=DEFAULT_GRPC_PORT, show_default=True, type=int)
@click.option(
    "--landing-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=DEFAULT_LANDING_ROOT,
    show_default=True,
)
@click.option(
    "--pki-dir",
    type=click.Path(path_type=Path, file_okay=False),
    default=ca.DEFAULT_PKI_DIR,
    show_default=True,
)
@click.option(
    "--api-socket",
    type=click.Path(path_type=Path),
    default=lambda: Path(os.environ.get("SUTRA_API_SOCKET", DEFAULT_API_SOCKET)),
    show_default="$SUTRA_API_SOCKET or /run/sutradhara/api.sock",
)
@click.option("--api-tcp", is_flag=True, default=False, help="Serve loopback TCP for local dev.")
@click.option("--api-host", default="127.0.0.1", show_default=True)
@click.option("--api-port", default=8770, show_default=True, type=int)
@click.option("--socket-mode", default="660", show_default=True)
@click.option(
    "--skip-artifactclass-validation",
    is_flag=True,
    default=False,
    help="Development/testing only: allow unknown artifactclasses.",
)
def serve_cmd(
    grpc_bind: str,
    grpc_port: int,
    landing_root: Path,
    pki_dir: Path,
    api_socket: Path,
    api_tcp: bool,
    api_host: str,
    api_port: int,
    socket_mode: str,
    skip_artifactclass_validation: bool,
) -> None:
    """Serve the operator HTTP API and mTLS gRPC relay in one process."""

    engine = make_engine()
    create_all(engine)
    registry = ConnectedDeviceRegistry()
    landing_root.mkdir(parents=True, exist_ok=True)

    grpc_server = make_server(
        GrpcServerConfig(
            engine=engine,
            landing_root=landing_root,
            pki_dir=pki_dir,
            bind=grpc_bind,
            port=grpc_port,
            validate_artifactclass=not skip_artifactclass_validation,
            registry=registry,
        )
    )
    app = create_app(engine, ensure_schema=False, registry=registry, grpc_pki_dir=pki_dir)
    grpc_server.start()
    sweep_stop, sweep_thread = start_sweep_loop(landing_root, registry=registry)
    click.echo(f"serving gRPC intake/control on {grpc_bind}:{grpc_port}")
    try:
        if api_tcp:
            validate_tcp_host(api_host)
            click.echo(f"serving HTTP API on {api_host}:{api_port}")
            uvicorn.run(app, host=api_host, port=api_port, workers=1, reload=False)
            return

        sock = _bind_unix_socket(api_socket, mode=_parse_socket_mode(socket_mode))
        try:
            click.echo(f"serving HTTP API on {api_socket}")
            uvicorn.run(app, fd=sock.fileno(), workers=1, reload=False)
        finally:
            sock.close()
            if api_socket.exists():
                api_socket.unlink()
    finally:
        grpc_server.stop(grace=5)
        sweep_stop.set()
        sweep_thread.join(timeout=5)
