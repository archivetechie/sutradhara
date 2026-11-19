"""Run the real serving CLI with bounded, pass-through lifecycle observations.

Only this child harness wraps real acquisition and return boundaries. Signals,
server waits, cleanup and restore end paths remain the product implementations.
"""

from __future__ import annotations

import importlib
import json
import signal
import sys
import threading
import time
from pathlib import Path

import grpc._server
import uvicorn

from sutradhara.cli.main import cli
from sutradhara.grpc.restore_service import RestoreService


def main() -> None:
    root = Path(sys.argv[1])
    route, scenario = sys.argv[2:4]

    def mark(name, value=True):
        path = root / name
        temporary = path.with_suffix(".writing")
        temporary.write_text(json.dumps(value))
        temporary.replace(path)

    def hold(name, *, observe_latch=False):
        deadline = time.monotonic() + 20
        mark(name)
        observed = not observe_latch
        while not (observed and (root / "release").exists()):
            if observe_latch and not observed:
                # Observe the actual gate without importing the not-yet-existing
                # helper during K1, or substituting any handler.
                handler = signal.getsignal(signal.SIGTERM)
                gate = getattr(handler, "__self__", None)
                if getattr(gate, "stop_requested", False):
                    mark("latched", True)
                    observed = True
            if time.monotonic() >= deadline:
                raise TimeoutError(f"child control deadline: {name}")
            time.sleep(0.01)

    original_port = grpc._server._Server.add_secure_port

    def add_secure_port(self, *args, **kwargs):
        result = original_port(self, *args, **kwargs)
        mark("grpc-port", result)
        return result

    grpc._server._Server.add_secure_port = add_secure_port
    original_start = grpc._server._Server.start

    def start(self, *args, **kwargs):
        mark("entered-grpc-start")
        return original_start(self, *args, **kwargs)

    grpc._server._Server.start = start
    original_stop = grpc._server._Server.stop

    def stop(self, *args, **kwargs):
        event = original_stop(self, *args, **kwargs)
        original_wait = event.wait

        def wait(*wait_args, **wait_kwargs):
            # grpcio's grace thread must never be held by this handshake.
            if scenario == "repeated" and threading.current_thread() is threading.main_thread():
                hold("draining")
            return original_wait(*wait_args, **wait_kwargs)

        event.wait = wait
        return event

    grpc._server._Server.stop = stop
    original_restore_start = RestoreService.start

    def restore_start(self, *args, **kwargs):
        result = original_restore_start(self, *args, **kwargs)
        if scenario == "startup":
            hold("restore-started", observe_latch=True)
        return result

    RestoreService.start = restore_start

    def wrap_acquisition(original, name):
        def acquire(*args, **kwargs):
            mark(name)
            return original(*args, **kwargs)

        return acquire

    def wrap_make_server(original):
        def make_server(*args, **kwargs):
            parts = original(*args, **kwargs)
            event = parts.server._state.termination_event
            original_wait = event.wait

            def wait(*wait_args, **wait_kwargs):
                # This callback runs inside grpcio's real termination wait.
                mark("inside-serving-wait")
                return original_wait(*wait_args, **wait_kwargs)

            event.wait = wait
            return parts

        return make_server

    for module_name in ("serve", "grpc"):
        module = importlib.import_module(f"sutradhara.cli.{module_name}")
        module.make_server = wrap_make_server(module.make_server)
        module.start_sweep_loop = wrap_acquisition(module.start_sweep_loop, "entered-landing-sweep")
        module.start_registry_sweep_loop = wrap_acquisition(
            module.start_registry_sweep_loop, "entered-registry-sweep"
        )

    original_http_startup = uvicorn.Server.startup
    original_http_run = uvicorn.Server.run

    def http_run(self, *args, **kwargs):
        mark("entered-http")
        return original_http_run(self, *args, **kwargs)

    uvicorn.Server.run = http_run

    async def http_startup(self, *args, **kwargs):
        mark("entered-http")
        result = await original_http_startup(self, *args, **kwargs)
        if route == "tcp":
            mark("http-port", self.servers[0].sockets[0].getsockname()[1])
        return result

    uvicorn.Server.startup = http_startup
    originals = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    mark("handlers-before", {str(sig): repr(handler) for sig, handler in originals.items()})
    common = ["--pki-dir", str(root / "pki"), "--landing-root", str(root / "landing")]
    if route == "grpc":
        args = ["serve-grpc", "--bind", "127.0.0.1", "--port", "0", *common]
    else:
        args = [
            "serve",
            "--grpc-bind",
            "127.0.0.1",
            "--grpc-port",
            "0",
            *common,
            "--api-socket",
            str(root / "api.sock"),
        ]
        if route == "tcp":
            args += ["--api-tcp", "--api-host", "127.0.0.1", "--api-port", "0"]
    cli(args, standalone_mode=False)
    mark(
        "handlers-after",
        {
            str(sig): {
                "identity_restored": signal.getsignal(sig) is handler,
                "before": repr(handler),
                "after": repr(signal.getsignal(sig)),
            }
            for sig, handler in originals.items()
        },
    )


if __name__ == "__main__":
    main()
