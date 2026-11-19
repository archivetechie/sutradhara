"""CLI for the streaming-intake gRPC server and enrollment admin actions."""

from __future__ import annotations

import threading
from pathlib import Path

import click

from sutradhara.catalog.session import create_all, make_engine, make_session_factory
from sutradhara.cli._server_shutdown import ServerShutdown
from sutradhara.grpc import ca
from sutradhara.grpc.admin import revoke_device as revoke_device_admin
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
from sutradhara.grpc.store import issue_enroll_token as issue_enroll_token_row


@click.command("serve-grpc")
@click.option("--bind", default="127.0.0.1", show_default=True, help="LAN/Tailscale bind address.")
@click.option("--port", default=DEFAULT_GRPC_PORT, show_default=True, type=int)
@click.option(
    "--landing-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=DEFAULT_LANDING_ROOT,
    show_default=True,
    help="Landing root where streamed intakes are assembled.",
)
@click.option(
    "--pki-dir",
    type=click.Path(path_type=Path, file_okay=False),
    default=ca.DEFAULT_PKI_DIR,
    show_default=True,
    help="Sutradhara gRPC PKI directory.",
)
@click.option(
    "--issue-enroll-token", is_flag=True, default=False, help="Mint a 24h enrollment token."
)
@click.option("--device-id", default=None, help="Device id for --issue-enroll-token.")
@click.option("--revoke-device", default=None, help="Revoke all certificates for DEVICE_ID.")
@click.option("--sign-csr", type=click.Path(path_type=Path, dir_okay=False), default=None)
@click.option(
    "--operator", "operator_name", default=None, help="Operator for --issue-enroll-token."
)
@click.option("--cert-out", type=click.Path(path_type=Path, dir_okay=False), default=None)
@click.option("--token", default=None, help="Enrollment token for --sign-csr.")
@click.option(
    "--skip-artifactclass-validation",
    is_flag=True,
    default=False,
    help="Development/testing only: allow unknown artifactclasses.",
)
def serve_grpc_cmd(
    bind: str,
    port: int,
    landing_root: Path,
    pki_dir: Path,
    issue_enroll_token: bool,
    device_id: str | None,
    revoke_device: str | None,
    sign_csr: Path | None,
    operator_name: str | None,
    cert_out: Path | None,
    token: str | None,
    skip_artifactclass_validation: bool,
) -> None:
    """Serve streaming intake over gRPC+mTLS, or run enrollment admin actions."""

    if issue_enroll_token or revoke_device is not None or sign_csr is not None:
        engine = make_engine()
        create_all(engine)
        factory = make_session_factory(engine)
        if issue_enroll_token:
            if not operator_name:
                raise click.ClickException("--operator is required with --issue-enroll-token")
            if not device_id:
                raise click.ClickException("--device-id is required with --issue-enroll-token")
            with factory.begin() as session:
                minted = issue_enroll_token_row(
                    session,
                    operator=operator_name,
                    device_id=device_id,
                )
            click.echo(minted)
            return
        if revoke_device is not None:
            count = revoke_device_admin(engine, revoke_device)
            click.echo(f"revoked {count} enrollment(s) for {revoke_device}")
            return
        if sign_csr is not None:
            if not token:
                raise click.ClickException("--token is required with --sign-csr")
            signed = ca.sign_device_csr(
                engine,
                pki_dir=pki_dir,
                csr_path=sign_csr,
                token=token,
                cert_path=cert_out,
            )
            click.echo(f"signed {signed.device_id} for {signed.operator}: {signed.cert_path}")
            click.echo(f"fingerprint: {signed.fingerprint}")
            return

    parts: GrpcServerParts | None = None
    sweep_stop: threading.Event | None = None
    sweep_thread: threading.Thread | None = None
    registry_stop: threading.Event | None = None
    registry_thread: threading.Thread | None = None
    gate = ServerShutdown()

    def run_body() -> None:
        nonlocal parts, sweep_stop, sweep_thread, registry_stop, registry_thread
        engine = make_engine()
        create_all(engine)
        make_session_factory(engine)
        if gate.check_pending():
            return

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
                bind=bind,
                port=port,
                validate_artifactclass=not skip_artifactclass_validation,
                registry=registry,
                progress_registry=progress_registry,
            )
        )
        if gate.check_pending():
            return
        # The restore service must be started before any call can reach it (RSR D7).
        parts.restore_service.start()
        if gate.check_pending():
            return
        server = parts.server
        server.start()
        if gate.check_pending():
            return
        sweep_stop, sweep_thread = start_sweep_loop(landing_root)
        if gate.check_pending():
            return
        registry_stop, registry_thread = start_registry_sweep_loop(registry)
        if gate.check_pending():
            return
        click.echo(f"serving gRPC intake on {bind}:{port}")
        if gate.check_pending():
            return
        while not gate.stop_requested:
            if not server.wait_for_termination(timeout=0.5):
                break

    try:
        gate.install()
        try:
            run_body()
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
