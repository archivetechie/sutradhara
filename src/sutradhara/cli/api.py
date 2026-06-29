"""CLI entry point for serving the Sutradhara operator HTTP API."""

from __future__ import annotations

import ipaddress
import os
from pathlib import Path

import click
import uvicorn

from sutradhara.api.app import create_app

DEFAULT_API_SOCKET = "/run/sutradhara/api.sock"


@click.command("serve-api")
@click.option(
    "--socket",
    "socket_path",
    type=click.Path(path_type=Path),
    default=lambda: Path(os.environ.get("SUTRA_API_SOCKET", DEFAULT_API_SOCKET)),
    show_default="$SUTRA_API_SOCKET or /run/sutradhara/api.sock",
    help="Unix domain socket path for Caddy to proxy.",
)
@click.option("--tcp", is_flag=True, default=False, help="Serve loopback TCP for local dev.")
@click.option("--host", default="127.0.0.1", show_default=True, help="Loopback TCP host.")
@click.option("--port", default=8770, show_default=True, type=int, help="Loopback TCP port.")
@click.option("--socket-mode", default="660", show_default=True, help="Octal mode via umask.")
def serve_api_cmd(socket_path: Path, tcp: bool, host: str, port: int, socket_mode: str) -> None:
    """Serve the operator API on a UDS by default, never on a tailnet/public bind."""

    app = create_app()
    if tcp:
        validate_tcp_host(host)
        uvicorn.run(app, host=host, port=port, workers=1, reload=False)
        return

    socket_path.parent.mkdir(parents=True, exist_ok=True)
    if socket_path.exists():
        socket_path.unlink()
    old_umask = os.umask(_umask_for_socket_mode(socket_mode))
    try:
        uvicorn.run(app, uds=str(socket_path), workers=1, reload=False)
    finally:
        os.umask(old_umask)


def validate_tcp_host(host: str) -> None:
    """Reject wildcard, public, and tailnet API binds; loopback only."""

    if host == "localhost":
        return
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise click.ClickException(f"API TCP host must be loopback, got {host!r}") from exc
    if not address.is_loopback or address.is_unspecified:
        raise click.ClickException(f"API TCP host must be loopback, got {host!r}")


def _umask_for_socket_mode(mode_text: str) -> int:
    try:
        mode = int(mode_text, 8)
    except ValueError as exc:
        raise click.ClickException(f"invalid socket mode {mode_text!r}") from exc
    if mode < 0 or mode > 0o777:
        raise click.ClickException(f"invalid socket mode {mode_text!r}")
    return 0o777 ^ mode
