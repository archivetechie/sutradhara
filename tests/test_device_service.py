"""DeviceService relay tests."""

from __future__ import annotations

import contextlib
import datetime as dt
import queue
import threading
from collections.abc import Callable, Iterator
from pathlib import Path

import grpc
import pytest
from sqlalchemy import Engine, select

from sutradhara._proto import device_pb2
from sutradhara.api import store as api_store
from sutradhara.catalog.models import ArtifactClassPolicyRecord
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.grpc import store as grpc_store
from sutradhara.grpc.device_service import DeviceService, DeviceServiceConfig
from sutradhara.grpc.registry import ConnectedDeviceRegistry


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    eng = make_engine(f"sqlite:///{tmp_path / 'device-service.db'}")
    create_all(eng)
    with session_scope(eng) as session:
        session.add(
            ArtifactClassPolicyRecord(
                artifactclass="s-masters",
                ruleset="test.s-masters.v1",
                expect="compliant",
                target_bytes=0,
                max_age_seconds=0,
                restore_preference=[],
            )
        )
    yield eng
    eng.dispose()


def test_device_service_connect_yields_handshake_before_queued_command(engine: Engine) -> None:
    registry = ConnectedDeviceRegistry()
    servicer = DeviceService(
        DeviceServiceConfig(engine=engine, registry=registry, command_poll_seconds=0.01)
    )
    with session_scope(engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )

    messages = _BlockingIterator()
    responses = servicer.Connect(messages, _FakeContext("mac-1", "AA" * 32))
    _assert_handshake(_next_response(responses))
    messages.put(
        device_pb2.DeviceMessage(
            card_snapshot=device_pb2.CardSnapshot(
                cards=[
                    device_pb2.Card(
                        card_id="card-1",
                        label="Card 1",
                        kind=device_pb2.CARD_KIND_CARD,
                        size_bytes=10,
                        status="available",
                    )
                ]
            )
        )
    )
    _eventually(lambda: registry.devices_for("ada")[0].cards[0].card_id == "card-1")

    pending = registry.send_start_receive(
        operator="ada",
        device_id="mac-1",
        card_id="card-1",
        artifactclass="s-masters",
        label="Card 1",
        source_ref=None,
        idempotency_key="key-1",
    )
    response = _next_response(responses)

    assert response.start_receive.command_id == pending.command_id
    messages.close()
    responses.close()


def test_device_service_idle_heartbeats_keep_stream_past_ttl(engine: Engine) -> None:
    registry = ConnectedDeviceRegistry()
    servicer = DeviceService(
        DeviceServiceConfig(engine=engine, registry=registry, command_poll_seconds=0.01)
    )
    with session_scope(engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )

    messages = _BlockingIterator()
    responses = servicer.Connect(messages, _FakeContext("mac-1", "AA" * 32))
    _assert_handshake(_next_response(responses))
    initial_seen = registry.devices_for("ada")[0].last_seen

    messages.put(device_pb2.DeviceMessage(heartbeat=device_pb2.Heartbeat()))
    _eventually(lambda: registry.devices_for("ada")[0].last_seen > initial_seen)
    heartbeat_seen = registry.devices_for("ada")[0].last_seen
    ttl = dt.timedelta(microseconds=1)
    sweep_time = heartbeat_seen + ttl

    assert sweep_time > initial_seen + ttl
    assert registry.evict_stale(ttl=ttl, now=sweep_time) == []
    assert registry.devices_for("ada")[0].device_id == "mac-1"
    messages.close()
    responses.close()


def test_device_service_ack_correlates_card_id_and_completes_http_idempotency(
    engine: Engine,
    tmp_path: Path,
) -> None:
    registry = ConnectedDeviceRegistry()
    servicer = DeviceService(DeviceServiceConfig(engine=engine, registry=registry))
    with session_scope(engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
        grpc_store.insert_intake(
            session,
            intake_id="intake-1",
            operator="ada",
            device_id="mac-1",
            idempotency_key="key-1",
            source_plan_digest="a" * 64,
            artifactclass="s-masters",
            source_kind="card",
            source_ref="card-1",
            label="Card 1",
            landing_root=str(tmp_path),
        )
    _add_started_http_intent(engine, intake_id="intake-1")

    messages = _BlockingIterator()
    responses = servicer.Connect(messages, _FakeContext("mac-1", "AA" * 32))
    _assert_handshake(_next_response(responses))
    messages.put(
        device_pb2.DeviceMessage(
            card_snapshot=device_pb2.CardSnapshot(
                cards=[
                    device_pb2.Card(
                        card_id="card-1",
                        label="Card 1",
                        kind=device_pb2.CARD_KIND_CARD,
                        size_bytes=10,
                        status="available",
                    )
                ]
            )
        )
    )
    _eventually(lambda: registry.devices_for("ada")[0].cards[0].card_id == "card-1")
    pending = registry.send_start_receive(
        operator="ada",
        device_id="mac-1",
        card_id="card-1",
        artifactclass="s-masters",
        label="Card 1",
        source_ref=None,
        idempotency_key="key-1",
    )
    response = _next_response(responses)
    assert response.start_receive.command_id == pending.command_id

    messages.put(
        device_pb2.DeviceMessage(
            command_ack=device_pb2.CommandAck(
                command_id=pending.command_id,
                status=device_pb2.COMMAND_ACK_STATUS_ACCEPTED,
                intake_id="intake-1",
            )
        )
    )
    assert pending.future.result(timeout=2).intake_id == "intake-1"
    _eventually(lambda: _stored_card_id(engine) == "card-1")
    _eventually(
        lambda: _idempotency_response(engine) == {"intakeId": "intake-1", "status": "streaming"}
    )
    with session_scope(engine) as session:
        record = session.scalars(select(api_store.IdempotencyRecord)).one()
        assert record.status == "started"
        assert record.response_json == {"intakeId": "intake-1", "status": "streaming"}
    messages.close()
    responses.close()


def test_active_receives_rebuilds_card_correlation_after_restart(
    engine: Engine,
    tmp_path: Path,
) -> None:
    registry = ConnectedDeviceRegistry()
    servicer = DeviceService(DeviceServiceConfig(engine=engine, registry=registry))
    with session_scope(engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
        grpc_store.insert_intake(
            session,
            intake_id="intake-1",
            operator="ada",
            device_id="mac-1",
            idempotency_key="key-1",
            source_plan_digest="a" * 64,
            artifactclass="s-masters",
            source_kind="card",
            source_ref="card-1",
            label="Card 1",
            landing_root=str(tmp_path),
        )
    _add_started_http_intent(engine, intake_id="intake-1")

    messages = _BlockingIterator()
    responses = servicer.Connect(messages, _FakeContext("mac-1", "AA" * 32))
    _assert_handshake(_next_response(responses))
    messages.put(
        device_pb2.DeviceMessage(
            active_receives=device_pb2.ActiveReceives(
                receives=[
                    device_pb2.ActiveReceive(
                        card_id="card-1",
                        idempotency_key="key-1",
                        intake_id="intake-1",
                        state="streaming",
                    )
                ]
            )
        )
    )

    _eventually(lambda: _stored_card_id(engine) == "card-1")
    messages.close()
    responses.close()


def test_device_service_dispatches_directory_listing_and_routes_reply(engine: Engine) -> None:
    registry = ConnectedDeviceRegistry()
    servicer = DeviceService(DeviceServiceConfig(engine=engine, registry=registry))
    with session_scope(engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )

    messages = _BlockingIterator()
    responses = servicer.Connect(messages, _FakeContext("mac-1", "AA" * 32))
    _assert_handshake(_next_response(responses))
    messages.put(
        device_pb2.DeviceMessage(
            card_snapshot=device_pb2.CardSnapshot(
                capabilities=["browse"],
                cards=[
                    device_pb2.Card(
                        card_id="card-1",
                        label="Card 1",
                        kind=device_pb2.CARD_KIND_CARD,
                        size_bytes=10,
                        status="available",
                    )
                ],
            )
        )
    )
    _eventually(lambda: registry.devices_for("ada")[0].capabilities == ("browse",))
    pending = registry.request_directory_listing(
        operator="ada",
        device_id="mac-1",
        card_id="card-1",
        rel_path="DCIM",
    )
    response = _next_response(responses)
    assert response.list_directory.request_id == pending.request_id
    assert response.list_directory.rel_path == "DCIM"

    messages.put(
        device_pb2.DeviceMessage(
            directory_listing=device_pb2.DirectoryListing(
                request_id=pending.request_id,
                status=device_pb2.DIR_STATUS_OK,
                entries=[
                    device_pb2.DirectoryEntry(
                        name="100MEDIA",
                        is_dir=True,
                        is_package=False,
                    )
                ],
            )
        )
    )
    assert pending.future.result(timeout=2).entries[0].name == "100MEDIA"
    messages.close()
    responses.close()


def test_device_service_revocation_evicts_on_next_heartbeat(engine: Engine) -> None:
    registry = ConnectedDeviceRegistry()
    servicer = DeviceService(DeviceServiceConfig(engine=engine, registry=registry))
    with session_scope(engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )

    messages = _BlockingIterator()
    responses = servicer.Connect(messages, _FakeContext("mac-1", "AA" * 32))
    _assert_handshake(_next_response(responses))
    messages.put(
        device_pb2.DeviceMessage(
            card_snapshot=device_pb2.CardSnapshot(
                cards=[
                    device_pb2.Card(
                        card_id="card-1",
                        label="Card 1",
                        kind=device_pb2.CARD_KIND_CARD,
                        size_bytes=10,
                        status="available",
                    )
                ]
            )
        )
    )
    _eventually(lambda: registry.devices_for("ada")[0].cards[0].card_id == "card-1")
    with session_scope(engine) as session:
        grpc_store.revoke_device(session, "mac-1")

    messages.put(device_pb2.DeviceMessage(heartbeat=device_pb2.Heartbeat()))

    _eventually(lambda: registry.devices_for("ada") == [])
    messages.close()
    responses.close()


def test_device_service_max_stream_lifetime_evicts_stream(engine: Engine) -> None:
    registry = ConnectedDeviceRegistry()
    servicer = DeviceService(
        DeviceServiceConfig(
            engine=engine,
            registry=registry,
            command_poll_seconds=0.01,
            max_stream_lifetime=dt.timedelta(seconds=0),
        )
    )
    with session_scope(engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )

    messages = _BlockingIterator()
    responses = servicer.Connect(messages, _FakeContext("mac-1", "AA" * 32))
    _assert_handshake(_next_response(responses))
    thread = threading.Thread(target=lambda: _ignore_stop_iteration(responses), daemon=True)
    thread.start()

    _eventually(lambda: registry.devices_for("ada") == [])
    messages.close()
    thread.join(timeout=2)


def test_device_service_ack_does_not_complete_when_card_correlation_fails(
    engine: Engine,
) -> None:
    registry = ConnectedDeviceRegistry()
    servicer = DeviceService(DeviceServiceConfig(engine=engine, registry=registry))
    with session_scope(engine) as session:
        grpc_store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
    _add_started_http_intent(engine, intake_id="intake-expected")

    messages = _BlockingIterator()
    responses = servicer.Connect(messages, _FakeContext("mac-1", "AA" * 32))
    _assert_handshake(_next_response(responses))
    messages.put(
        device_pb2.DeviceMessage(
            card_snapshot=device_pb2.CardSnapshot(
                cards=[
                    device_pb2.Card(
                        card_id="card-1",
                        label="Card 1",
                        kind=device_pb2.CARD_KIND_CARD,
                        size_bytes=10,
                        status="available",
                    )
                ]
            )
        )
    )
    _eventually(lambda: registry.devices_for("ada")[0].cards[0].card_id == "card-1")
    pending = registry.send_start_receive(
        operator="ada",
        device_id="mac-1",
        card_id="card-1",
        artifactclass="s-masters",
        label="Card 1",
        source_ref=None,
        idempotency_key="key-1",
    )
    response = _next_response(responses)
    assert response.start_receive.command_id == pending.command_id

    messages.put(
        device_pb2.DeviceMessage(
            command_ack=device_pb2.CommandAck(
                command_id=pending.command_id,
                status=device_pb2.COMMAND_ACK_STATUS_ACCEPTED,
                intake_id="missing-intake",
            )
        )
    )
    assert pending.future.result(timeout=2).intake_id == "missing-intake"
    _eventually(lambda: _idempotency_status(engine) == "failed")
    assert _idempotency_response(engine) is None
    messages.close()
    responses.close()


def _stored_card_id(engine: Engine) -> str | None:
    with session_scope(engine) as session:
        row = grpc_store.get_intake(session, "intake-1")
        return None if row is None else row.card_id


def _idempotency_status(engine: Engine) -> str | None:
    with session_scope(engine) as session:
        record = session.scalars(select(api_store.IdempotencyRecord)).one_or_none()
        return None if record is None else record.status


def _idempotency_response(engine: Engine) -> dict[str, object] | None:
    with session_scope(engine) as session:
        record = session.scalars(select(api_store.IdempotencyRecord)).one_or_none()
        return None if record is None else record.response_json


def _add_started_http_intent(engine: Engine, *, intake_id: str) -> None:
    """Create the durable HTTP authorization consumed before a device ack."""

    decision = api_store.begin_device_receive_intent(
        engine,
        operator_username="ada",
        device_id="mac-1",
        card_identity="card-1",
        card_label="Card 1",
        idempotency_key="key-1",
        request_hash="a" * 64,
        acknowledge_duplicate=False,
    )
    assert decision.state == "authorized"
    with session_scope(engine) as session:
        linked = api_store.claim_start_intake(
            session,
            operator_username="ada",
            device_id="mac-1",
            idempotency_key="key-1",
            intake_id=intake_id,
        )
        assert linked.state == "claimed"


def _ignore_stop_iteration(iterator: Iterator[object]) -> None:
    with contextlib.suppress(StopIteration):
        next(iterator)


def _next_response(iterator: Iterator[object], *, timeout: float = 2.0) -> object:
    result: queue.Queue[object] = queue.Queue()

    def read() -> None:
        try:
            result.put(next(iterator))
        except Exception as exc:
            result.put(exc)

    thread = threading.Thread(target=read, daemon=True)
    thread.start()
    item = result.get(timeout=timeout)
    thread.join(timeout=0.1)
    if isinstance(item, Exception):
        raise item
    return item


def _assert_handshake(response: object) -> None:
    assert isinstance(response, device_pb2.ServerCommand)
    assert response.WhichOneof("payload") is None


class _BlockingIterator:
    def __init__(self) -> None:
        self._queue: queue.Queue[object | None] = queue.Queue()

    def __iter__(self) -> _BlockingIterator:
        return self

    def __next__(self) -> object:
        item = self._queue.get(timeout=5)
        if item is None:
            raise StopIteration
        return item

    def put(self, item: object) -> None:
        self._queue.put(item)

    def close(self) -> None:
        self._queue.put(None)


class _FakeContext:
    def __init__(self, device_id: str, fingerprint: str) -> None:
        self.device_id = device_id
        self.fingerprint = fingerprint

    def abort(self, code: grpc.StatusCode, details: str) -> None:
        raise RuntimeError(f"{code}: {details}")


def _eventually(predicate: Callable[[], bool]) -> None:
    import time

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (IndexError, AssertionError):
            pass
        time.sleep(0.01)
    assert predicate()
