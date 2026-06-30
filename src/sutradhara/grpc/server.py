"""gRPC server lifecycle and bind-safety checks for streaming intake."""

from __future__ import annotations

import datetime as dt
import ipaddress
import shutil
import threading
from concurrent import futures
from dataclasses import dataclass
from pathlib import Path

import grpc
from sqlalchemy import Engine

from sutradhara._proto import device_pb2_grpc, intake_pb2_grpc
from sutradhara.grpc.ca import load_server_credentials
from sutradhara.grpc.device_service import DeviceService, DeviceServiceConfig
from sutradhara.grpc.registry import ConnectedDeviceRegistry
from sutradhara.grpc.servicer import GrpcIntakeConfig, IntakeServicer
from sutradhara_receive import sweep_orphans

DEFAULT_GRPC_PORT = 50051
DEFAULT_LANDING_ROOT = Path("/replica/landing")


@dataclass(frozen=True)
class GrpcServerConfig:
    """Configuration for the streaming-intake gRPC server."""

    engine: Engine
    landing_root: Path
    pki_dir: Path
    bind: str = "127.0.0.1"
    port: int = DEFAULT_GRPC_PORT
    validate_artifactclass: bool = True
    registry: ConnectedDeviceRegistry | None = None


def make_server(config: GrpcServerConfig) -> grpc.Server:
    """Create a configured mTLS gRPC server."""

    validate_bind_address(config.bind)
    ca_cert, server_cert, server_key = load_server_credentials(config.pki_dir)
    registry = config.registry or ConnectedDeviceRegistry()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    intake_pb2_grpc.add_IntakeServiceServicer_to_server(
        IntakeServicer(
            GrpcIntakeConfig(
                engine=config.engine,
                landing_root=config.landing_root,
                validate_artifactclass=config.validate_artifactclass,
            )
        ),
        server,
    )
    device_pb2_grpc.add_DeviceServiceServicer_to_server(
        DeviceService(DeviceServiceConfig(engine=config.engine, registry=registry)),
        server,
    )
    creds = grpc.ssl_server_credentials(
        [(server_key, server_cert)],
        root_certificates=ca_cert,
        require_client_auth=True,
    )
    server.add_secure_port(f"{config.bind}:{config.port}", creds)
    return server


def validate_bind_address(bind: str) -> None:
    """Reject wildcard and public gRPC binds."""

    if bind == "localhost":
        return
    try:
        address = ipaddress.ip_address(bind)
    except ValueError as exc:
        raise ValueError(f"gRPC bind must be an IP address or localhost: {bind!r}") from exc
    tailscale = ipaddress.ip_network("100.64.0.0/10")
    if address.is_unspecified:
        raise ValueError("gRPC bind must not be a wildcard address")
    if address.is_loopback or address.is_private or address in tailscale:
        return
    raise ValueError(f"gRPC bind must be loopback, LAN, or Tailscale, got {bind!r}")


def sweep_landing_once(
    landing_root: Path,
    *,
    older_than: dt.timedelta = dt.timedelta(hours=24),
    now: dt.datetime | None = None,
) -> None:
    """Sweep stale gRPC receive filesystem state."""

    current = now or dt.datetime.now(dt.UTC)
    sweep_orphans(landing_root, older_than=older_than, now=current)
    if not landing_root.exists():
        return
    cutoff = current - older_than
    for incoming in landing_root.glob("*/.incoming"):
        if not incoming.is_dir():
            continue
        for child in incoming.iterdir():
            try:
                mtime = dt.datetime.fromtimestamp(child.stat().st_mtime, tz=dt.UTC)
            except FileNotFoundError:
                continue
            if mtime >= cutoff:
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink(missing_ok=True)


def start_sweep_loop(
    landing_root: Path,
    *,
    interval_seconds: float = 3600.0,
    older_than: dt.timedelta = dt.timedelta(hours=24),
) -> tuple[threading.Event, threading.Thread]:
    """Start a background stale-receive sweep loop for ``serve-grpc``."""

    stop = threading.Event()

    def run() -> None:
        while not stop.wait(interval_seconds):
            sweep_landing_once(landing_root, older_than=older_than)

    thread = threading.Thread(target=run, name="sutra-grpc-sweep", daemon=True)
    thread.start()
    return stop, thread
