"""ConnectedDeviceRegistry tests for the operator relay."""

from __future__ import annotations

import datetime as dt

import pytest

from sutradhara.grpc.registry import (
    Card,
    CommandAck,
    ConnectedDeviceRegistry,
    DeviceOffline,
    StreamClosed,
)
from sutradhara.grpc.store import DeviceIdentity


def test_registry_isolates_operator_devices_and_delivers_command() -> None:
    registry = ConnectedDeviceRegistry()
    stream = registry.register(
        DeviceIdentity(operator="owner", device_id="mac-1", fingerprint="AA" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")]
    )

    assert [device.device_id for device in registry.devices_for("owner")] == ["mac-1"]
    assert registry.devices_for("other") == []

    pending = registry.send_start_receive(
        operator="owner",
        device_id="mac-1",
        card_id="card-1",
        artifactclass="s-masters",
        label="Card 1",
        source_ref=None,
        idempotency_key="key-1",
    )
    delivered = stream.next_command(timeout=0.01)
    assert delivered is pending
    assert delivered.command.card_id == "card-1"

    ack = CommandAck(
        command_id=pending.command_id,
        accepted=True,
        reason=None,
        intake_id="intake-1",
    )
    assert stream.ack(ack) is pending
    assert pending.future.result(timeout=0).intake_id == "intake-1"


def test_duplicate_register_replaces_stream_and_fails_pending_ack() -> None:
    registry = ConnectedDeviceRegistry()
    old_closed = False

    def close_old() -> None:
        nonlocal old_closed
        old_closed = True

    old = registry.register(
        DeviceIdentity(operator="owner", device_id="mac-1", fingerprint="AA" * 32),
        close_stream=close_old,
    )
    old.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")]
    )
    pending = registry.send_start_receive(
        operator="owner",
        device_id="mac-1",
        card_id="card-1",
        artifactclass="s-masters",
        label=None,
        source_ref=None,
        idempotency_key="key-1",
    )

    new = registry.register(
        DeviceIdentity(operator="owner", device_id="mac-1", fingerprint="AA" * 32)
    )

    assert old_closed
    with pytest.raises(StreamClosed):
        pending.future.result(timeout=0)
    with pytest.raises(StreamClosed):
        old.next_command(timeout=0.01)
    assert new.generation == old.generation + 1


def test_ttl_eviction_removes_device_and_fails_commands() -> None:
    registry = ConnectedDeviceRegistry()
    stream = registry.register(
        DeviceIdentity(operator="owner", device_id="mac-1", fingerprint="AA" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")]
    )
    pending = registry.send_start_receive(
        operator="owner",
        device_id="mac-1",
        card_id="card-1",
        artifactclass="s-masters",
        label=None,
        source_ref=None,
        idempotency_key="key-1",
    )

    evicted = registry.evict_stale(
        ttl=dt.timedelta(seconds=1),
        now=dt.datetime.now(dt.UTC) + dt.timedelta(seconds=2),
    )

    assert evicted == ["mac-1"]
    with pytest.raises(StreamClosed):
        pending.future.result(timeout=0)
    with pytest.raises(DeviceOffline):
        registry.send_start_receive(
            operator="owner",
            device_id="mac-1",
            card_id="card-1",
            artifactclass="s-masters",
            label=None,
            source_ref=None,
            idempotency_key="key-2",
        )
