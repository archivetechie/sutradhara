"""Prove the serving commands reach their existing cleanup funnel on signals.

Child cases exercise the registered CLI and real HTTP, mTLS gRPC, restore and
sweep lifecycles. Deterministic cases cover the individual ownership boundaries.
"""

from __future__ import annotations

import http.client
import importlib
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import grpc
import pytest
from click.testing import CliRunner

from sutradhara.catalog.session import create_all, make_engine, make_session_factory
from sutradhara.grpc import ca, store

STOPPED = "stopped: gRPC drained, restore service stopped"
CHILD = Path(__file__).with_name("server_shutdown_child.py")


def _child_case(tmp_path, monkeypatch, route, scenario):
    # Short names are necessary for Unix sockets. Each invocation owns its PKI,
    # database, control files, logs and process; nothing uses deployment state.
    root = Path(tempfile.mkdtemp(prefix="sd-", dir=tmp_path))
    assert len(str(root / "api.sock").encode()) < 100, (
        f"Unix socket path too long: {root / 'api.sock'}"
    )
    timings = {}
    child = channel = None
    setup_start = time.monotonic()
    phase, phase_start = "setup", setup_start
    stdout_path, stderr_path = root / "stdout", root / "stderr"
    try:
        with monkeypatch.context() as patch:
            patch.setattr(tempfile, "tempdir", str(root))
            engine = make_engine(f"sqlite:///{root / 'catalog.db'}")
            try:
                create_all(engine)
                ca.ensure_server_certificate(root / "pki")
                material = ca.generate_device_csr(root / "client", device_id="shutdown-test")
                with make_session_factory(engine).begin() as session:
                    token = store.issue_enroll_token(
                        session, operator="shutdown-test", device_id="shutdown-test"
                    )
                certificate = ca.sign_device_csr(
                    engine, pki_dir=root / "pki", csr_path=material.csr_path, token=token
                )
            finally:
                engine.dispose()
        credentials = grpc.ssl_channel_credentials(
            root_certificates=(root / "pki" / ca.CA_CERT_NAME).read_bytes(),
            private_key=material.key_path.read_bytes(),
            certificate_chain=certificate.cert_path.read_bytes(),
        )
        timings["setup"] = time.monotonic() - setup_start
        env = dict(
            os.environ, SUTRADHARA_DB_URL=f"sqlite:///{root / 'catalog.db'}", TMPDIR=str(root)
        )
        ready_start = time.monotonic()
        phase, phase_start = "readiness", ready_start
        with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
            child = subprocess.Popen(
                [sys.executable, str(CHILD), str(root), route, scenario],
                cwd=root,
                env=env,
                stdout=stdout,
                stderr=stderr,
            )

        def await_marker(name, deadline):
            while not (root / name).exists():
                if child.poll() is not None:
                    return False
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"parent control deadline: {name}; {stderr_path.read_text()}"
                    )
                time.sleep(0.01)
            return True

        ready_deadline = ready_start + 20
        if scenario == "startup":
            assert await_marker("restore-started", ready_deadline), stderr_path.read_text()
        else:
            assert await_marker("grpc-port", ready_deadline), stderr_path.read_text()
            port = json.loads((root / "grpc-port").read_text())
            channel = grpc.secure_channel(f"127.0.0.1:{port}", credentials)
            ready = grpc.channel_ready_future(channel)
            while True:
                remaining = ready_deadline - time.monotonic()
                assert remaining > 0, stderr_path.read_text()
                try:
                    ready.result(timeout=min(2, remaining))
                    break
                except grpc.FutureTimeoutError:
                    assert child.poll() is None, stderr_path.read_text()
            if route == "grpc":
                assert await_marker("inside-serving-wait", ready_deadline), stderr_path.read_text()
            else:
                if route == "tcp":
                    assert await_marker("http-port", ready_deadline), stderr_path.read_text()
                    http_port = json.loads((root / "http-port").read_text())
                while True:
                    remaining = ready_deadline - time.monotonic()
                    assert remaining > 0, stderr_path.read_text()
                    connection = http.client.HTTPConnection(
                        "127.0.0.1",
                        http_port if route == "tcp" else 80,
                        timeout=min(2, remaining),
                    )
                    try:
                        if route == "fd":
                            connection.sock = socket.socket(socket.AF_UNIX)
                            connection.sock.settimeout(min(2, remaining))
                            connection.sock.connect(str(root / "api.sock"))
                        connection.request("GET", f"/shutdown-test-unused-{root.name}")
                        response = connection.getresponse()
                        response.read()
                        assert response.status == 404
                        break
                    except (OSError, http.client.HTTPException):
                        assert child.poll() is None, stderr_path.read_text()
                        time.sleep(0.01)
                    finally:
                        connection.close()
            if route == "fd":
                assert (root / "api.sock").exists()
        timings["readiness"] = time.monotonic() - ready_start
        stop_start = time.monotonic()
        phase, phase_start = "stop_control", stop_start
        sigint_sent = release_seen = False
        os.kill(child.pid, signal.SIGTERM)
        if scenario == "repeated":
            os.kill(child.pid, signal.SIGTERM)
            if await_marker("draining", time.monotonic() + 20):
                os.kill(child.pid, signal.SIGINT)
                sigint_sent = True
                (root / "release").touch()
        elif scenario == "startup":
            if await_marker("latched", time.monotonic() + 20):
                (root / "release").touch()
                release_seen = True
        timings["stop_control"] = time.monotonic() - stop_start
        exit_start = time.monotonic()
        phase, phase_start = "exit", exit_start
        child.wait(timeout=30)
        timings["exit"] = time.monotonic() - exit_start
        # Reap and assert A1 before reading restoration/later-entry evidence.
        # These diagnostics deliberately do not substitute for the A1 oracle.
        diagnostic = {
            "returncode": child.returncode,
            "stdout": stdout_path.read_text(),
            "socket_exists": (root / "api.sock").exists(),
        }
        (root / "outcome.json").write_text(json.dumps(diagnostic, indent=2))
        assert child.returncode == 0, diagnostic  # A1
        if scenario == "repeated":
            assert sigint_sent
        elif scenario == "startup":
            assert release_seen
        stdout = stdout_path.read_text()
        assert stdout.splitlines().count(STOPPED) == 1  # A2: stdout alone
        assert not (root / "api.sock").exists()  # A3
        restored = json.loads((root / "handlers-after").read_text())
        assert all(row["identity_restored"] for row in restored.values())  # A4
        if scenario == "startup":
            assert json.loads((root / "latched").read_text()) is True
            assert not any(
                (root / name).exists()
                for name in (
                    "entered-grpc-start",
                    "entered-landing-sweep",
                    "entered-registry-sweep",
                    "entered-http",
                    "inside-serving-wait",
                )
            )  # A5
    finally:
        timings.setdefault(phase, time.monotonic() - phase_start)
        for name in ("setup", "readiness", "stop_control", "exit"):
            timings.setdefault(name, None)
        (root / "release").touch()
        if channel is not None:
            channel.close()
        if child is not None:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=15)
        timings["total"] = time.monotonic() - setup_start
        (root / "timings.json").write_text(json.dumps(timings, indent=2))
        print(f"child evidence: {root}; timings: {timings}")
        if sys.exc_info()[0] is not None and stderr_path.exists():
            print(f"child stderr:\n{stderr_path.read_text()}")


@pytest.mark.parametrize("route", ["fd", "tcp", "grpc"])
def test_server_sigterm_after_ready(tmp_path, monkeypatch, route):
    _child_case(tmp_path, monkeypatch, route, "ready")


@pytest.mark.parametrize("route", ["fd", "tcp", "grpc"])
def test_server_repeated_signals_drain(tmp_path, monkeypatch, route):
    _child_case(tmp_path, monkeypatch, route, "repeated")


@pytest.mark.parametrize("route", ["fd", "tcp", "grpc"])
def test_server_sigterm_during_startup(tmp_path, monkeypatch, route):
    _child_case(tmp_path, monkeypatch, route, "startup")


def _handlers():
    return {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}


def _deliver(sig):
    handler = signal.getsignal(sig)
    handler(sig, None)


def _exercise(
    tmp_path,
    monkeypatch,
    route="fd",
    *,
    inject=None,
    sig=signal.SIGTERM,
    failures=None,
    started=True,
    wait_returns=(False,),
    wait_signal_at=None,
    admin=None,
    admin_extra=(),
    install_failure=None,
    restore_failure=None,
):
    """Capture a registered command's result before making any test assertions.

    Every row owns fresh doubles and a fresh real gate. An independent finally
    restores process handlers even when the product's restoration is mutated.
    """
    from sutradhara.cli._server_shutdown import ServerShutdown
    from sutradhara.cli.main import cli

    module = importlib.import_module(f"sutradhara.cli.{'grpc' if route == 'grpc' else 'serve'}")
    before = _handlers()
    raw_signal = signal.signal
    trace, calls, arguments, deliveries = [], [], {}, []
    owned = set()
    failures = failures or {}
    socket_path = tmp_path / "unit.sock"
    landing = tmp_path / "landing"
    engine, registry, progress, app = object(), object(), object(), object()
    row = SimpleNamespace()

    def step(name, *args, **kwargs):
        calls.append(name)
        arguments.setdefault(name, []).append((args, kwargs))
        if name == "make_engine":
            row.handlers_at_engine = _handlers()
        if inject == name:
            _deliver(sig)
            deliveries.append((name, signal.getsignal(sig).__self__.stop_requested))
        if name in failures:
            raise failures[name]

    def cleanup(name, *args, **kwargs):
        trace.append((name, args, kwargs))
        step(name, *args, **kwargs)

    def acquire(name, value, ownership=None):
        def call(*args, **kwargs):
            step(name, *args, **kwargs)
            if ownership:
                owned.add(ownership)
            return value

        return call

    class GrpcServer:
        def start(self):
            step("grpc.start")

        def stop(self, *, grace):
            cleanup("grpc.stop", grace=grace)
            return SimpleNamespace(wait=lambda: cleanup("grpc.wait"))

        def wait_for_termination(self, *, timeout):
            number = len(arguments.get("grpc.wait_for_termination", []))
            step("grpc.wait_for_termination", timeout=timeout)
            if wait_signal_at == number:
                _deliver(sig)
                deliveries.append(("wait-slice", signal.getsignal(sig).__self__.stop_requested))
            return wait_returns[number]

    grpc_server = GrpcServer()
    parts = SimpleNamespace(
        server=grpc_server,
        restore_service=SimpleNamespace(
            start=lambda: step("restore.start"), stop=lambda: cleanup("restore.stop")
        ),
    )

    def sweeps(prefix):
        return (
            SimpleNamespace(set=lambda: cleanup(f"{prefix}.set")),
            SimpleNamespace(join=lambda **kw: cleanup(f"{prefix}.join", **kw)),
        )

    class Socket:
        def fileno(self):
            return 71

        def close(self):
            cleanup("fd.close")

    def bind(path, *, mode):
        step("fd.bind", path, mode=mode)
        socket_path.touch()
        owned.add("fd")
        return Socket()

    class Config:
        def __init__(self, *args, **kwargs):
            step("Config", *args, **kwargs)
            try:
                raise KeyError("handled library lookup")
            except KeyError:
                step("Config.handled")

        def load_app(self):
            step("load_app")

    class HttpServer:
        def __init__(self, *, config):
            self.should_exit = False
            step("Server", config=config)

        def run(self):
            row.should_exit_at_run = self.should_exit
            step("run")

        @property
        def started(self):
            step("started")
            return started

    old_attach, old_setattr = ServerShutdown.attach, ServerShutdown.__setattr__
    old_restore = ServerShutdown.restore

    def attach(self, server):
        step("attach.before")
        old_attach(self, server)
        step("attach.after")

    def set_attribute(self, name, value):
        attaching = name == "_server" and isinstance(value, HttpServer)
        if attaching:
            step("attach.before_store")
        old_setattr(self, name, value)
        if attaching:
            step("attach.after_store")

    def restore(self):
        step("restore.enter")
        old_restore(self)

    installs = []

    def install(signum, handler):
        if getattr(handler, "__self__", None) is not None and isinstance(
            handler.__self__, ServerShutdown
        ):
            installs.append(signum)
            if signum == signal.SIGINT and install_failure is not None:
                raise install_failure
        elif restore_failure is not None:
            raise restore_failure
        return raw_signal(signum, handler)

    original_mkdir, original_unlink = Path.mkdir, Path.unlink

    def mkdir(path, *args, **kwargs):
        result = original_mkdir(path, *args, **kwargs)
        if path == landing:
            step("landing.mkdir")
        return result

    def unlink(path, *args, **kwargs):
        if path == socket_path:
            cleanup("fd.unlink")
        return original_unlink(path, *args, **kwargs)

    original_echo = module.click.echo

    def echo(message, *args, **kwargs):
        original_echo(message, *args, **kwargs)
        name = (
            "output"
            if message == STOPPED
            else (
                "announcement.http"
                if str(message).startswith("serving HTTP")
                else "announcement.grpc"
            )
        )
        step(name)

    factory = SimpleNamespace(begin=lambda: nullcontext(object()))
    admin_actions = []

    def admin_action(name, value):
        def action(*args, **kwargs):
            admin_actions.append((name, args, kwargs))
            return value

        return action

    try:
        with monkeypatch.context() as patch:
            patch.setattr(signal, "signal", install)
            patch.setattr(ServerShutdown, "attach", attach)
            patch.setattr(ServerShutdown, "__setattr__", set_attribute)
            patch.setattr(ServerShutdown, "restore", restore)
            patch.setattr(Path, "mkdir", mkdir)
            patch.setattr(Path, "unlink", unlink)
            patch.setattr(module.click, "echo", echo)
            patch.setattr(module, "make_engine", acquire("make_engine", engine))
            patch.setattr(module, "create_all", acquire("create_all", None))
            patch.setattr(module, "ConnectedDeviceRegistry", acquire("registry", registry))
            patch.setattr(module, "ReceiveProgressRegistry", acquire("progress", progress))
            patch.setattr(module, "make_server", acquire("make_server", parts, "parts"))
            patch.setattr(
                module, "start_sweep_loop", acquire("landing.sweep", sweeps("landing"), "landing")
            )
            patch.setattr(
                module,
                "start_registry_sweep_loop",
                acquire("registry.sweep", sweeps("registry"), "registry"),
            )
            common = ["--landing-root", str(landing), "--pki-dir", str(tmp_path / "pki")]
            if route == "grpc":
                patch.setattr(module, "make_session_factory", acquire("factory", factory))
                patch.setattr(module, "issue_enroll_token_row", admin_action("issue", "test-token"))
                patch.setattr(module, "revoke_device_admin", admin_action("revoke", 2))
                patch.setattr(
                    module.ca,
                    "sign_device_csr",
                    admin_action(
                        "sign",
                        SimpleNamespace(
                            device_id="device",
                            operator="operator",
                            cert_path=tmp_path / "signed.crt",
                            fingerprint="test-fingerprint",
                        ),
                    ),
                )
                args = ["serve-grpc", *common]
                if admin == "issue":
                    args += [
                        "--issue-enroll-token",
                        "--operator",
                        "operator",
                        "--device-id",
                        "device",
                    ]
                elif admin == "revoke":
                    args += ["--revoke-device", "device"]
                elif admin == "sign":
                    args += ["--sign-csr", str(tmp_path / "test.csr"), "--token", "test-token"]
                args += list(admin_extra)
            else:
                patch.setattr(module, "create_app", acquire("create_app", app))
                patch.setattr(module, "_bind_unix_socket", bind)
                patch.setattr(module, "validate_tcp_host", acquire("validate_tcp_host", None))
                patch.setattr(module.uvicorn, "Config", Config)
                patch.setattr(module.uvicorn, "Server", HttpServer)
                args = ["serve", *common, "--api-socket", str(socket_path)]
                if route == "tcp":
                    args += ["--api-tcp", "--api-host", "127.0.0.1", "--api-port", "0"]
            result = CliRunner().invoke(cli, args)
            after = _handlers()
    finally:
        for signum, handler in before.items():
            raw_signal(signum, handler)
        socket_path.unlink(missing_ok=True)
    row.result, row.trace, row.calls, row.owned = result, trace, calls, owned
    row.arguments, row.deliveries, row.installs = arguments, deliveries, installs
    row.before, row.after, row.admin_actions = before, after, admin_actions
    row.app, row.registry, row.progress, row.engine = app, registry, progress, engine
    with (tmp_path / "unit-rows.jsonl").open("a") as log:
        log.write(
            json.dumps(
                {
                    "route": route,
                    "inject": inject,
                    "signal": int(sig),
                    "failures": {k: repr(v) for k, v in failures.items()},
                    "started": started,
                    "admin": admin,
                    "calls": calls,
                    "trace": trace,
                    "owned": sorted(owned),
                    "exit": result.exit_code,
                    "exception": repr(result.exception),
                    "line_count": result.stdout.splitlines().count(STOPPED),
                    "restored": before == after,
                    "deliveries": deliveries,
                    "wait_returns": wait_returns,
                    "wait_signal_at": wait_signal_at,
                }
            )
            + "\n"
        )
    return row


def _expected_cleanup(owned, *, failed_stop=False):
    trace = []
    if "fd" in owned:
        trace += [("fd.close", (), {}), ("fd.unlink", (), {})]
    if "parts" in owned:
        trace += [("grpc.stop", (), {"grace": 5})]
        if not failed_stop:
            trace += [("grpc.wait", (), {})]
        trace += [("restore.stop", (), {})]
    for name in ("landing", "registry"):
        if name in owned:
            trace += [(f"{name}.set", (), {})]
    for name in ("landing", "registry"):
        if name in owned:
            trace += [(f"{name}.join", (), {"timeout": 5})]
    return trace


def _assert_result(row, error=None, *, failed_stop=False):
    assert row.trace == _expected_cleanup(row.owned, failed_stop=failed_stop)
    assert row.after == row.before
    if error is None:
        assert row.result.exit_code == 0, row.result.exception
        assert row.result.stdout.splitlines().count(STOPPED) == 1
        assert row.calls.index("output") < row.calls.index("restore.enter")
    else:
        assert row.result.exception is error
        assert row.result.exit_code == (error.code if isinstance(error, SystemExit) else 1)
        assert STOPPED not in row.result.stdout
    assert all(latched for _, latched in row.deliveries)


def test_shutdown_latch_contract(monkeypatch):
    from sutradhara.cli._server_shutdown import ServerShutdown

    class RecordingServer:
        def __init__(self):
            self.writes = []

        @property
        def should_exit(self):
            return bool(self.writes)

        @should_exit.setter
        def should_exit(self, value):
            self.writes.append(value)

    originals, raw_signal = _handlers(), signal.signal
    try:
        # Named witnesses come first, each with a fresh gate.
        for signals in ((signal.SIGTERM, signal.SIGINT), (signal.SIGINT, signal.SIGTERM)):
            gate, server = ServerShutdown(), RecordingServer()
            outcomes, latches, writes = [], [], []
            try:
                gate.install()
                gate.attach(server)
                for signum in signals:
                    try:
                        _deliver(signum)
                        outcomes.append("returned")
                    except BaseException as exc:
                        outcomes.append(exc)
                    latches.append(gate.stop_requested)
                    writes.append(len(server.writes))
            finally:
                gate.restore()
            exit_requests = len(server.writes)
            assert exit_requests == 1
            assert outcomes == ["returned", "returned"]
            assert latches == [True, True]
            assert writes == [1, 1]
            assert server.writes == [True]
        for signum in (signal.SIGTERM, signal.SIGINT):
            gate, server = ServerShutdown(), RecordingServer()
            assert gate.stop_requested is False
            assert gate.check_pending() is False
            assert _handlers() == originals
            gate.restore()
            try:
                gate.install()
                assert signal.getsignal(signal.SIGTERM) is signal.getsignal(signal.SIGINT)
                _deliver(signum)
                assert gate.stop_requested is True
                assert gate.check_pending() is True
                gate.attach(server)
                _deliver(signum)
                # A later delivery during the cleanup window also returns.
                _deliver(signal.SIGINT if signum == signal.SIGTERM else signal.SIGTERM)
                assert server.writes == [True]
                with pytest.raises(AttributeError):
                    gate.stop_requested = False
            finally:
                gate.restore()
            gate.restore()
            assert _handlers() == originals
        gate = ServerShutdown()
        errors, changes = [], []

        def record_signal(*args):
            changes.append(args)
            return raw_signal(*args)

        def background_install():
            try:
                gate.install()
            except BaseException as exc:
                errors.append(exc)

        with monkeypatch.context() as patch:
            patch.setattr(signal, "signal", record_signal)
            thread = threading.Thread(target=background_install)
            thread.start()
            thread.join(timeout=5)
        assert not thread.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], RuntimeError)
        assert str(errors[0]) == "server shutdown signals require the main thread"
        assert changes == []
        assert _handlers() == originals
    finally:
        for signum, handler in originals.items():
            raw_signal(signum, handler)


def test_shutdown_pending_boundaries(tmp_path, monkeypatch):
    witness = _exercise(tmp_path, monkeypatch, inject="restore.start")
    later_start_calls = [
        name
        for name in witness.calls
        if name in ("grpc.start", "landing.sweep", "registry.sweep", "Config", "run")
    ]
    assert later_start_calls == []
    _assert_result(witness)
    assert witness.owned == {"parts"}
    for route in ("fd", "tcp", "grpc"):
        row = _exercise(tmp_path, monkeypatch, route, inject="make_engine")
        handlers = list(row.handlers_at_engine.values())
        assert all(callable(handler) for handler in handlers)
        assert handlers[0] is handlers[1]
        assert row.handlers_at_engine != row.before
        assert row.deliveries == [("make_engine", True)]
        assert "make_server" not in row.calls
        assert row.owned == set()
        assert row.calls == (
            ["make_engine", "create_all", "factory", "output", "restore.enter"]
            if route == "grpc"
            else [
                "make_engine",
                "create_all",
                "registry",
                "progress",
                "landing.mkdir",
                "output",
                "restore.enter",
            ]
        )
        _assert_result(row)
    for action in ("issue", "revoke", "sign"):
        row = _exercise(tmp_path, monkeypatch, "grpc", admin=action)
        assert row.handlers_at_engine == row.before
        assert row.installs == []
        assert row.result.exit_code == 0
    common = [
        "landing.mkdir",
        "make_server",
        "restore.start",
        "grpc.start",
        "landing.sweep",
        "registry.sweep",
    ]
    for route in ("fd", "tcp", "grpc"):
        boundaries = list(common)
        if route == "grpc":
            boundaries.insert(0, "factory")
            boundaries += ["announcement.grpc"]
        else:
            boundaries.insert(2, "create_app")
            if route == "fd":
                boundaries += ["fd.bind"]
            boundaries += ["announcement.http"]
        for boundary in boundaries:
            row = _exercise(tmp_path, monkeypatch, route, inject=boundary)
            position = row.calls.index(boundary)
            assert row.calls[position + 1 :] == [
                name for name, _, _ in _expected_cleanup(row.owned)
            ] + ["output", "restore.enter"]
            assert not any(
                name in row.calls[position + 1 :]
                for name in (
                    "make_server",
                    "create_app",
                    "restore.start",
                    "grpc.start",
                    "landing.sweep",
                    "registry.sweep",
                    "fd.bind",
                    "Config",
                    "load_app",
                    "Server",
                    "run",
                    "grpc.wait_for_termination",
                )
            )
            expected_owned = set()
            if boundary not in ("factory", "landing.mkdir"):
                expected_owned.add("parts")
            if boundary in (
                "landing.sweep",
                "registry.sweep",
                "fd.bind",
                "announcement.http",
                "announcement.grpc",
            ):
                expected_owned.add("landing")
            if boundary in ("registry.sweep", "fd.bind", "announcement.http", "announcement.grpc"):
                expected_owned.add("registry")
            if route == "fd" and boundary in ("fd.bind", "announcement.http"):
                expected_owned.add("fd")
            assert row.owned == expected_owned, (route, boundary)
            assert row.deliveries == [(boundary, True)]
            _assert_result(row)


def test_shutdown_cleanup_and_failures(tmp_path, monkeypatch):
    error = RuntimeError("saved fd close failure")
    witness = _exercise(tmp_path, monkeypatch, failures={"fd.close": error}, inject="fd.unlink")
    cleanup_trace = witness.trace
    expected_trace = _expected_cleanup({"parts", "fd", "landing", "registry"})
    assert cleanup_trace == expected_trace
    _assert_result(witness, error)
    for route in ("fd", "tcp", "grpc"):
        serving = "grpc.wait_for_termination" if route == "grpc" else "run"
        operations = [
            name
            for name, _, _ in _expected_cleanup(
                {"parts", "landing", "registry"} | ({"fd"} if route == "fd" else set())
            )
        ]
        for sig in (signal.SIGTERM, signal.SIGINT):
            for error in (None, RuntimeError("body failed"), SystemExit(1), SystemExit(3)):
                for injection in [serving, *operations]:
                    row = _exercise(
                        tmp_path,
                        monkeypatch,
                        route,
                        inject=injection,
                        sig=sig,
                        failures={} if error is None else {serving: error},
                    )
                    _assert_result(row, error)
                    assert row.deliveries == [(injection, True)]
            if route != "grpc":
                for injection in ("Config.handled", "started"):
                    row = _exercise(tmp_path, monkeypatch, route, inject=injection, sig=sig)
                    _assert_result(row)
                    assert row.deliveries == [(injection, True)]
        for operation in operations:
            error = RuntimeError(f"failed {operation}")
            row = _exercise(tmp_path, monkeypatch, route, failures={operation: error})
            _assert_result(row, error, failed_stop=operation == "grpc.stop")
        # A later cleanup error supersedes an earlier one by normal finally rules.
        first, last = RuntimeError("first cleanup failure"), RuntimeError("last cleanup failure")
        row = _exercise(
            tmp_path, monkeypatch, route, failures={"restore.stop": first, "registry.join": last}
        )
        _assert_result(row, last)


def test_shutdown_restores_handlers(tmp_path, monkeypatch):
    witness = _exercise(tmp_path, monkeypatch)
    handlers_after, handlers_before = witness.after, witness.before
    assert handlers_after == handlers_before
    _assert_result(witness)
    for route in ("fd", "tcp", "grpc"):
        for point in (
            "make_engine",
            "make_server",
            "restore.start",
            "grpc.start",
            "landing.sweep",
            "registry.sweep",
            "restore.stop",
        ):
            error = RuntimeError(f"failure at {point}")
            row = _exercise(tmp_path, monkeypatch, route, failures={point: error})
            _assert_result(row, error)
        error = RuntimeError("second install failed")
        row = _exercise(tmp_path, monkeypatch, route, install_failure=error)
        assert row.before == row.after
        assert row.result.exception is error
        assert row.owned == set()
        assert row.trace == []
        assert row.installs == [signal.SIGTERM, signal.SIGINT]
        assert STOPPED not in row.result.stdout
        error = RuntimeError("restoration failed after output")
        row = _exercise(tmp_path, monkeypatch, route, restore_failure=error)
        assert row.result.exception is error
        assert row.result.stdout.splitlines().count(STOPPED) == 1
        assert row.calls.index("output") < row.calls.index("restore.enter")
    from sutradhara.cli._server_shutdown import ServerShutdown

    # An interrupt after the real SIGTERM installation must not strand its handler.
    originals, raw_signal = _handlers(), signal.signal
    original_term, original_int = originals[signal.SIGTERM], originals[signal.SIGINT]
    gate, interrupted = ServerShutdown(), False

    def interrupt_after_term(signum, handler):
        result = raw_signal(signum, handler)
        if signum == signal.SIGTERM and getattr(handler, "__self__", None) is gate:
            raise KeyboardInterrupt
        return result

    try:
        with monkeypatch.context() as patch:
            patch.setattr(signal, "signal", interrupt_after_term)
            try:
                gate.install()
            except KeyboardInterrupt:
                interrupted = True
            finally:
                gate.restore()
        assert signal.getsignal(signal.SIGTERM) is original_term
        assert signal.getsignal(signal.SIGINT) is original_int
        assert interrupted
    finally:
        for signum, handler in originals.items():
            raw_signal(signum, handler)

    # A non-Python disposition must be refused before either handler changes.
    originals, raw_signal, raw_getsignal = _handlers(), signal.signal, signal.getsignal
    gate, raised, message, changes = ServerShutdown(), None, None, []

    def non_python_handler(signum):
        return None if signum == signal.SIGINT else raw_getsignal(signum)

    def record_change(signum, handler):
        changes.append((signum, handler))
        return raw_signal(signum, handler)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(signal, "getsignal", non_python_handler)
            patch.setattr(signal, "signal", record_change)
            try:
                try:
                    gate.install()
                finally:
                    gate.restore()
            except (RuntimeError, TypeError) as error:
                raised, message = type(error), str(error)
        assert raised is RuntimeError
        assert message == "server shutdown cannot restore a handler not installed from Python"
        assert changes == []
        assert _handlers() == originals
    finally:
        for signum, handler in originals.items():
            raw_signal(signum, handler)

    # Refusing a second installation must preserve the first saved originals.
    originals, raw_signal = _handlers(), signal.signal
    gate, second_install_raised, message = ServerShutdown(), False, None
    try:
        gate.install()
        installed_handlers = _handlers()
        try:
            gate.install()
        except RuntimeError as error:
            second_install_raised, message = True, str(error)
        assert second_install_raised
        assert message == "server shutdown signals are already installed"
        assert _handlers() == installed_handlers
        assert all(getattr(handler, "__self__", None) is gate for handler in _handlers().values())
        gate.restore()
        assert _handlers() == originals
    finally:
        for signum, handler in originals.items():
            raw_signal(signum, handler)

    originals, raw_signal = _handlers(), signal.signal
    try:
        for partial in (False, True):
            gate, changes = ServerShutdown(), []

            def recorder(signum, handler, *, changes=changes, partial=partial):
                changes.append((signum, handler))
                if partial and signum == signal.SIGINT and handler != originals[signum]:
                    raise RuntimeError("partial installation")
                return raw_signal(signum, handler)

            with monkeypatch.context() as patch:
                patch.setattr(signal, "signal", recorder)
                if partial:
                    with pytest.raises(RuntimeError, match="partial installation"):
                        gate.install()
                gate.restore()
                after_first_restore = list(changes)
                gate.restore()
                assert changes == after_first_restore
            assert _handlers() == originals
            # F1 records SIGINT before its failed install, so restoring its original is call four.
            assert len(changes) == (4 if partial else 0)
    finally:
        for signum, handler in originals.items():
            raw_signal(signum, handler)
    for action, extra in (
        ("issue", ("--revoke-device", "ignored", "--sign-csr", str(tmp_path / "ignored.csr"))),
        ("revoke", ("--sign-csr", str(tmp_path / "ignored.csr"))),
        ("sign", ()),
    ):
        row = _exercise(tmp_path, monkeypatch, "grpc", admin=action, admin_extra=extra)
        assert row.before == row.after == row.handlers_at_engine
        assert row.installs == []
        assert row.trace == []
        assert row.owned == set()
        assert row.result.exit_code == 0, row.result.exception
        assert [name for name, _, _ in row.admin_actions] == [action]
        assert STOPPED not in row.result.stdout
        if action == "issue":
            assert row.result.stdout == "test-token\n"
            assert row.admin_actions[0][2] == {"operator": "operator", "device_id": "device"}
        elif action == "revoke":
            assert row.result.stdout == "revoked 2 enrollment(s) for device\n"
            assert row.admin_actions[0][1] == (row.engine, "device")
        else:
            assert f"signed device for operator: {tmp_path / 'signed.crt'}\n" in row.result.stdout
            assert "fingerprint: test-fingerprint\n" in row.result.stdout
            assert row.admin_actions[0][2]["token"] == "test-token"
    # Validation stays on the admin branch, with no signal installation.
    for extra, message in (
        (("--issue-enroll-token",), "--operator is required"),
        (("--issue-enroll-token", "--operator", "operator"), "--device-id is required"),
        (("--sign-csr", str(tmp_path / "unsigned.csr")), "--token is required"),
    ):
        row = _exercise(tmp_path, monkeypatch, "grpc", admin_extra=extra)
        assert row.result.exit_code == 1
        assert message in row.result.output
        assert row.installs == []
        assert row.after == row.before


def test_shutdown_server_handoff_and_wait(tmp_path, monkeypatch):
    witness = _exercise(tmp_path, monkeypatch, inject="Config")
    should_exit_at_run = witness.should_exit_at_run
    assert should_exit_at_run is True
    _assert_result(witness)
    for route in ("fd", "tcp"):
        for point in (
            "Config",
            "load_app",
            "Server",
            "attach.before",
            "attach.before_store",
            "attach.after_store",
            "attach.after",
            "run",
        ):
            row = _exercise(tmp_path, monkeypatch, route, inject=point)
            _assert_result(row)
            # A delivery in run occurs immediately after the recorded entry.
            if point != "run":
                assert row.should_exit_at_run is True
            assert row.deliveries == [(point, True)]
            args, kwargs = row.arguments["Config"][0]
            assert args == (row.app,)
            assert kwargs == (
                {"fd": 71, "workers": 1, "reload": False}
                if route == "fd"
                else {"host": "127.0.0.1", "port": 0, "workers": 1, "reload": False}
            )
            assert (
                row.calls.index("load_app")
                < row.calls.index("Server")
                < row.calls.index("attach.before")
                < row.calls.index("run")
            )
            assert row.arguments["create_app"] == [
                (
                    (row.engine,),
                    {
                        "ensure_schema": False,
                        "registry": row.registry,
                        "grpc_pki_dir": tmp_path / "pki",
                        "grpc_progress_registry": row.progress,
                    },
                )
            ]
        for started in (True, False):
            for requested in (True, False):
                row = _exercise(
                    tmp_path,
                    monkeypatch,
                    route,
                    started=started,
                    inject="Config" if requested else None,
                )
                if started:
                    _assert_result(row)
                else:
                    assert isinstance(row.result.exception, SystemExit)
                    assert row.result.exception.code == 3
                    _assert_result(row, row.result.exception)
                assert row.should_exit_at_run is requested
        for point in ("Config", "load_app", "Server", "run"):
            for error in (SystemExit(3), SystemExit(1), RuntimeError("raised startup failure")):
                row = _exercise(tmp_path, monkeypatch, route, inject=point, failures={point: error})
                _assert_result(row, error)
    # Before the sliced wait, inside its first slice, after multiple timeouts,
    # and actual termination with no signal are four distinct loop exits.
    for inject, returns, signal_at, expected_waits in (
        ("announcement.grpc", (False,), None, 0),
        (None, (True,), 0, 1),
        (None, (True, True, True), 2, 3),
        (None, (True, True, False), None, 3),
        (None, (False,), None, 1),
    ):
        row = _exercise(
            tmp_path,
            monkeypatch,
            "grpc",
            inject=inject,
            wait_returns=returns,
            wait_signal_at=signal_at,
        )
        _assert_result(row)
        assert (
            row.arguments.get("grpc.wait_for_termination", [])
            == [((), {"timeout": 0.5})] * expected_waits
        )
        assert "attach.before" not in row.calls
