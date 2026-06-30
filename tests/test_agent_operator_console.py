"""Operator-console relay behavior for the local sutra-agent helper."""

from __future__ import annotations

import hashlib
import json
import queue
import socket
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from sutra_agent._proto import device_pb2, intake_pb2
from sutra_agent.cli import main as agent_main
from sutra_agent.config import AgentConfig
from sutra_agent.controld import ControlDaemon, card_snapshot_message
from sutra_agent.enroll_client import EnrollmentError, enroll_device, enroll_url
from sutra_agent.grpc_client import StreamReceiveResult, stream_source
from sutra_agent.ledger import ConfirmationSnapshot, active_receive_records, record_active_receive
from sutra_agent.mounts import MountedCard, MountInfo, card_from_mount
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.grpc import ca, store
from sutradhara.grpc.ca import csr_common_name
from sutradhara.grpc.registry import ConnectedDeviceRegistry
from sutradhara.grpc.server import GrpcServerConfig, make_server
from sutradhara.intake_watch import process_landing_once


def test_mount_card_id_uses_volume_identity_and_payload_omits_path(tmp_path: Path) -> None:
    mount_path = tmp_path / "Volumes" / "CARD_A"
    mount_path.mkdir(parents=True)
    card = card_from_mount(
        MountInfo(
            mount_path=mount_path,
            label="CARD_A",
            source="/dev/disk4s1",
            volume_uuid="A1B2-C3D4",
            removable=True,
            size_bytes=123,
        )
    )

    message = card_snapshot_message([card])

    assert card.card_id == "volume:A1B2-C3D4"
    assert message.card_snapshot.cards[0].card_id == "volume:A1B2-C3D4"
    assert str(mount_path) not in str(message)
    assert str(mount_path).encode() not in message.SerializeToString()


def test_stream_source_on_started_fires_before_upload(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    config = _streaming_config(tmp_path)
    events: list[str] = []

    class FakeChannel:
        def __enter__(self) -> FakeChannel:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    class FakeStub:
        def __init__(self, _channel: object) -> None:
            pass

        def StartIntake(self, _request: object) -> object:
            return _Obj(intake_id="intake-1")

        def ListIntakeFiles(self, _request: object) -> object:
            return _Obj(files=[])

        def UploadFile(self, chunks: object) -> object:
            assert events == ["started:intake-1"]
            data = b"".join(chunk.data for chunk in chunks)
            return intake_pb2.FileReceipt(
                relpath="clip.mov",
                server_sha256=hashlib.sha256(data).hexdigest(),
                received_bytes=len(data),
            )

        def CommitIntake(self, _request: object) -> object:
            return _Obj(reupload_relpaths=[])

        def GetIntakeStatus(self, _request: object) -> object:
            return _Obj(status="streaming", errors=[])

    monkeypatch.setattr("sutra_agent.grpc_client._channel", lambda _config: FakeChannel())
    monkeypatch.setattr("sutra_agent.grpc_client.intake_pb2_grpc.IntakeServiceStub", FakeStub)

    result = stream_source(
        source,
        config=config,
        idempotency_key="key-1",
        on_started=lambda intake_id: events.append(f"started:{intake_id}"),
    )

    assert result.intake_id == "intake-1"
    assert events == ["started:intake-1"]


def test_control_daemon_early_acks_then_background_streams(tmp_path: Path) -> None:
    card = _card(tmp_path)
    release = threading.Event()
    seen_source_kinds: list[str] = []
    outbox: queue.Queue[device_pb2.DeviceMessage | None] = queue.Queue()

    def fake_stream_source(_source: Path, **kwargs: Any) -> StreamReceiveResult:
        seen_source_kinds.append(kwargs["config"].source_kind)
        kwargs["on_started"]("intake-1")
        release.wait(timeout=2)
        return _stream_result("intake-1", "key-1")

    daemon = ControlDaemon(
        _streaming_config(tmp_path),
        card_source=lambda: [card],
        stream_source_fn=fake_stream_source,
        abort_intake_fn=lambda _config, _intake_id: None,
    )
    daemon._send_card_snapshot([card], outbox)
    _drain(outbox)

    daemon.handle_command(_start_command("cmd-1", card.card_id, "key-1"), outbox)
    ack = _next_ack(outbox)

    assert ack.command_ack.status == device_pb2.COMMAND_ACK_STATUS_ACCEPTED
    assert ack.command_ack.intake_id == "intake-1"
    assert release.is_set() is False
    assert seen_source_kinds == ["drive"]
    active = outbox.get(timeout=1)
    assert active.WhichOneof("payload") == "active_receives"
    assert active.active_receives.receives[0].intake_id == "intake-1"
    release.set()
    _eventually(lambda: active_receive_records(_streaming_config(tmp_path).resolved_ledger_path()) == [])


def test_control_daemon_same_key_reacks_and_different_key_busy(tmp_path: Path) -> None:
    card = _card(tmp_path)
    config = _streaming_config(tmp_path)
    record_active_receive(
        config.resolved_ledger_path(),
        card_id=card.card_id,
        idempotency_key="key-1",
        intake_id="intake-1",
    )
    daemon = ControlDaemon(config, card_source=lambda: [card])
    outbox: queue.Queue[device_pb2.DeviceMessage | None] = queue.Queue()
    daemon._send_card_snapshot([card], outbox)
    _drain(outbox)

    daemon.handle_command(_start_command("cmd-1", card.card_id, "key-1"), outbox)
    daemon.handle_command(_start_command("cmd-2", card.card_id, "key-2"), outbox)

    same_key = _next_ack(outbox)
    busy = _next_ack(outbox)
    assert same_key.command_ack.status == device_pb2.COMMAND_ACK_STATUS_ACCEPTED
    assert same_key.command_ack.intake_id == "intake-1"
    assert busy.command_ack.status == device_pb2.COMMAND_ACK_STATUS_REJECTED
    assert busy.command_ack.reason == "card busy"


def test_control_daemon_aborts_started_receive_on_background_failure(tmp_path: Path) -> None:
    card = _card(tmp_path)
    aborted: list[str] = []
    outbox: queue.Queue[device_pb2.DeviceMessage | None] = queue.Queue()

    def fake_stream_source(_source: Path, **kwargs: Any) -> StreamReceiveResult:
        kwargs["on_started"]("intake-1")
        raise RuntimeError("card pulled")

    daemon = ControlDaemon(
        _streaming_config(tmp_path),
        card_source=lambda: [card],
        stream_source_fn=fake_stream_source,
        abort_intake_fn=lambda _config, intake_id: aborted.append(intake_id),
    )
    daemon._send_card_snapshot([card], outbox)
    _drain(outbox)

    daemon.handle_command(_start_command("cmd-1", card.card_id, "key-1"), outbox)

    ack = _next_ack(outbox)
    assert ack.command_ack.status == device_pb2.COMMAND_ACK_STATUS_ACCEPTED
    _eventually(lambda: aborted == ["intake-1"])


def test_control_daemon_reports_active_receives_from_ledger(tmp_path: Path) -> None:
    config = _streaming_config(tmp_path)
    record_active_receive(
        config.resolved_ledger_path(),
        card_id="card-1",
        idempotency_key="key-1",
        intake_id="intake-1",
    )
    daemon = ControlDaemon(config)
    outbox: queue.Queue[device_pb2.DeviceMessage | None] = queue.Queue()

    daemon._send_active_receives(outbox)

    message = outbox.get(timeout=1)
    receive = message.active_receives.receives[0]
    assert receive.card_id == "card-1"
    assert receive.idempotency_key == "key-1"
    assert receive.intake_id == "intake-1"


def test_control_daemon_run_forever_reconnects_after_stream_end(tmp_path: Path) -> None:
    daemon = ControlDaemon(
        _streaming_config(tmp_path),
        reconnect_initial_seconds=0.01,
        reconnect_max_seconds=0.01,
    )
    stop = threading.Event()
    calls = 0

    def fake_run_once(*, stop: threading.Event) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            stop.set()

    daemon.run_once = fake_run_once  # type: ignore[method-assign]

    daemon.run_forever(stop=stop)

    assert calls == 2


def test_agent_serve_status_reports_streaming_config(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = agent_main(
        [
            "serve",
            "--server",
            "localhost:50051",
            "--client-cert",
            str(tmp_path / "client.crt"),
            "--client-key",
            str(tmp_path / "client.key"),
            "--ca-cert",
            str(tmp_path / "ca.crt"),
            "--device-id",
            "mac-1",
            "--ledger",
            str(tmp_path / "ledger.json"),
            "--status",
            "--json",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["server"] == "localhost:50051"
    assert payload["device_id"] == "mac-1"


def test_real_control_daemon_relays_receive_end_to_end(tmp_path: Path) -> None:
    engine = make_engine(f"sqlite:///{tmp_path / 'relay.db'}")
    create_all(engine)
    pki = tmp_path / "pki"
    ca.ensure_server_certificate(pki, common_name="localhost")
    material = ca.generate_device_csr(tmp_path / "device", device_id="mac-1")
    with session_scope(engine) as session:
        token = store.issue_enroll_token(session, operator="owner", device_id="mac-1")
    signed = ca.sign_device_csr(engine, pki_dir=pki, csr_path=material.csr_path, token=token)
    registry = ConnectedDeviceRegistry()
    port = _free_port()
    landing = tmp_path / "landing"
    server = make_server(
        GrpcServerConfig(
            engine=engine,
            landing_root=landing,
            pki_dir=pki,
            bind="127.0.0.1",
            port=port,
            validate_artifactclass=False,
            registry=registry,
        )
    )
    source = tmp_path / "mounted-card"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    card = card_from_mount(
        MountInfo(
            mount_path=source,
            label="Mounted Card",
            volume_uuid="real-card",
            removable=True,
            size_bytes=5,
        )
    )
    config = AgentConfig(
        server_address=f"localhost:{port}",
        client_cert=signed.cert_path,
        client_key=material.key_path,
        ca_cert=pki / "ca.crt",
        device_id="mac-1",
        ledger_path=tmp_path / "ledger.json",
    )
    daemon = ControlDaemon(
        config,
        card_source=lambda: [card],
        heartbeat_seconds=0.1,
        reconnect_initial_seconds=0.1,
        reconnect_max_seconds=0.1,
    )
    stop = threading.Event()
    thread = threading.Thread(target=lambda: daemon.run_forever(stop=stop), daemon=True)
    try:
        server.start()
        thread.start()
        _eventually(lambda: len(registry.devices_for("owner")) == 1)

        pending = registry.send_start_receive(
            operator="owner",
            device_id="mac-1",
            card_id=card.card_id,
            artifactclass="video-master",
            label="Mounted Card",
            source_ref=None,
            idempotency_key="relay-key-1",
        )
        ack = pending.future.result(timeout=10)

        assert ack.accepted is True
        assert ack.intake_id is not None
        intake_dir = landing / str(ack.intake_id)
        _eventually(
            lambda: (intake_dir / "intake.json").is_file()
            and not (intake_dir / ".receiving.json").exists()
        )
        events = process_landing_once(
            landing,
            engine=engine,
            settle_seconds=0,
            stable_polls=1,
            cache_root=tmp_path / "cache",
            use_lock=False,
        )
        assert [event.event for event in events] == ["intake-registered"]
        assert (intake_dir / "intake.verified.json").is_file()
    finally:
        stop.set()
        thread.join(timeout=5)
        server.stop(grace=None)
        engine.dispose()


def test_enroll_device_posts_csr_with_pinned_ca_and_stores_response(tmp_path: Path) -> None:
    pinned_ca = tmp_path / "pinned-ca.crt"
    pinned_ca.write_text("PINNED CA", encoding="utf-8")
    seen: dict[str, object] = {}

    def fake_post(url: str, payload: dict[str, str], ca_cert: Path) -> dict[str, str]:
        seen["url"] = url
        seen["ca_cert"] = ca_cert
        seen["cn"] = csr_common_name(_write_csr(tmp_path, payload["csr_pem"]))
        return {"cert_pem": "CERT", "ca_pem": "RETURNED CA"}

    result = enroll_device(
        server="https://sutra.example",
        token="token-1",
        device_id="mac-1",
        output_dir=tmp_path / "device",
        ca_cert=pinned_ca,
        post_json=fake_post,
    )

    assert seen == {
        "url": "https://sutra.example/api/enroll/csr",
        "ca_cert": pinned_ca,
        "cn": "mac-1",
    }
    assert result.client_cert.read_text(encoding="utf-8") == "CERT"
    assert result.ca_cert.read_text(encoding="utf-8") == "RETURNED CA"


def test_enroll_device_surfaces_server_token_mismatch(tmp_path: Path) -> None:
    pinned_ca = tmp_path / "pinned-ca.crt"
    pinned_ca.write_text("PINNED CA", encoding="utf-8")

    def fake_post(_url: str, _payload: dict[str, str], _ca_cert: Path) -> dict[str, str]:
        raise EnrollmentError("CSR common name does not match enrollment token device_id")

    with pytest.raises(EnrollmentError, match="common name"):
        enroll_device(
            server="https://sutra.example",
            token="token-1",
            device_id="mac-1",
            output_dir=tmp_path / "device",
            ca_cert=pinned_ca,
            post_json=fake_post,
        )


def test_enroll_url_rejects_non_https() -> None:
    with pytest.raises(EnrollmentError, match="https"):
        enroll_url("http://sutra.example")


class _Obj:
    def __init__(self, **kwargs: object) -> None:
        self.__dict__.update(kwargs)


def _streaming_config(tmp_path: Path) -> AgentConfig:
    return AgentConfig(
        server_address="localhost:50051",
        client_cert=tmp_path / "client.crt",
        client_key=tmp_path / "client.key",
        ca_cert=tmp_path / "ca.crt",
        device_id="mac-1",
        ledger_path=tmp_path / "ledger.json",
    )


def _card(tmp_path: Path) -> MountedCard:
    mount = tmp_path / "card"
    mount.mkdir(exist_ok=True)
    return card_from_mount(
        MountInfo(
            mount_path=mount,
            label="Card",
            volume_uuid="card-key",
            removable=False,
            size_bytes=5,
        )
    )


def _start_command(command_id: str, card_id: str, idempotency_key: str) -> device_pb2.ServerCommand:
    return device_pb2.ServerCommand(
        start_receive=device_pb2.StartReceive(
            command_id=command_id,
            card_id=card_id,
            artifactclass="video-master",
            label="Card",
            idempotency_key=idempotency_key,
        )
    )


def _stream_result(intake_id: str, key: str) -> StreamReceiveResult:
    return StreamReceiveResult(
        intake_id=intake_id,
        file_count=1,
        total_bytes=5,
        skipped_count=0,
        confirmation=ConfirmationSnapshot(status="pending", release_ok=False),
        idempotency_key=key,
        plan_digest="a" * 64,
    )


def _next_ack(outbox: queue.Queue[device_pb2.DeviceMessage | None]) -> device_pb2.DeviceMessage:
    message = outbox.get(timeout=2)
    assert message is not None
    assert message.WhichOneof("payload") == "command_ack"
    return message


def _drain(outbox: queue.Queue[device_pb2.DeviceMessage | None]) -> None:
    while True:
        try:
            outbox.get_nowait()
        except queue.Empty:
            return


def _eventually(predicate: Callable[[], bool]) -> None:
    import time

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    assert predicate()


def _write_csr(tmp_path: Path, pem: str) -> Path:
    path = tmp_path / "seen.csr"
    path.write_text(pem, encoding="utf-8")
    return path


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
