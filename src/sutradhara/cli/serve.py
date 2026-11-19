"""Merged operator server command.

``sutra serve`` hosts the mTLS gRPC intake/control port and the Authentik-fronted
HTTP API in one process so the HTTP routes can command the live helper streams
held by ``DeviceService.Connect``.
"""

from __future__ import annotations

import os
import socket
import threading
from pathlib import Path

import click
import uvicorn

from sutradhara.api.app import create_app
from sutradhara.catalog.session import create_all, make_engine
from sutradhara.cli._server_shutdown import ServerShutdown
from sutradhara.cli.api import (
    DEFAULT_API_SOCKET,
    _bind_unix_socket,
    _parse_socket_mode,
    validate_tcp_host,
)
from sutradhara.grpc import ca
from sutradhara.grpc.progress import ReceiveProgressRegistry
from sutradhara.grpc.registry import ConnectedDeviceRegistry
from sutradhara.grpc.server import (
    DEFAULT_GRPC_PORT,
    DEFAULT_LANDING_ROOT,
    GrpcServerConfig,
    GrpcServerParts,
    make_server,
    start_registry_sweep_loop,
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

    parts: GrpcServerParts | None = None
    sweep_stop: threading.Event | None = None
    sweep_thread: threading.Thread | None = None
    registry_stop: threading.Event | None = None
    registry_thread: threading.Thread | None = None
    sock: socket.socket | None = None
    gate = ServerShutdown()

    def run_body() -> None:
        nonlocal parts, sweep_stop, sweep_thread, registry_stop, registry_thread, sock
        engine = make_engine()
        create_all(engine)
        registry = ConnectedDeviceRegistry()
        progress_registry = ReceiveProgressRegistry()
        landing_root.mkdir(parents=True, exist_ok=True)
        if gate.check_pending():
            return

        parts = make_server(
            GrpcServerConfig(
                engine=engine,
                landing_root=landing_root,
                pki_dir=pki_dir,
                bind=grpc_bind,
                port=grpc_port,
                validate_artifactclass=not skip_artifactclass_validation,
                registry=registry,
                progress_registry=progress_registry,
            )
        )
        if gate.check_pending():
            return
        app = create_app(
            engine,
            ensure_schema=False,
            registry=registry,
            grpc_pki_dir=pki_dir,
            grpc_progress_registry=progress_registry,
        )
        if gate.check_pending():
            return
        # The restore service must be started before any call can reach it (RSR D7).
        parts.restore_service.start()
        if gate.check_pending():
            return
        grpc_server = parts.server
        grpc_server.start()
        if gate.check_pending():
            return
        sweep_stop, sweep_thread = start_sweep_loop(landing_root)
        if gate.check_pending():
            return
        registry_stop, registry_thread = start_registry_sweep_loop(registry)
        if gate.check_pending():
            return
        click.echo(f"serving gRPC intake/control on {grpc_bind}:{grpc_port}")
        if api_tcp:
            validate_tcp_host(api_host)
            click.echo(f"serving HTTP API on {api_host}:{api_port}")
            if gate.check_pending():
                return
            config = uvicorn.Config(app, host=api_host, port=api_port, workers=1, reload=False)
        else:
            sock = _bind_unix_socket(api_socket, mode=_parse_socket_mode(socket_mode))
            if gate.check_pending():
                return
            click.echo(f"serving HTTP API on {api_socket}")
            if gate.check_pending():
                return
            config = uvicorn.Config(app, fd=sock.fileno(), workers=1, reload=False)
        config.load_app()
        server = uvicorn.Server(config=config)
        gate.attach(server)
        server.run()
        if not server.started:
            raise SystemExit(3)

    try:
        gate.install()
        try:
            run_body()
        finally:
            try:
                if sock is not None:
                    sock.close()
            finally:
                try:
                    if sock is not None and api_socket.exists():
                        api_socket.unlink()
                finally:
                    try:
                        # Wait for the server to stop before the restore service. The event fires
                        # once no call is in flight in gRPC's core, which can be before a handler's
                        # own clean-up has returned; an end that fails after that point is closed
                        # `lost` by the next instance's recovery.
                        if parts is not None:
                            parts.server.stop(grace=5).wait()
                    finally:
                        try:
                            if parts is not None:
                                parts.restore_service.stop()
                        finally:
                            try:
                                if sweep_stop is not None:
                                    sweep_stop.set()
                            finally:
                                try:
                                    if registry_stop is not None:
                                        registry_stop.set()
                                finally:
                                    try:
                                        if sweep_thread is not None:
                                            sweep_thread.join(timeout=5)
                                    finally:
                                        if registry_thread is not None:
                                            registry_thread.join(timeout=5)
        click.echo("stopped: gRPC drained, restore service stopped")
    finally:
        gate.restore()
