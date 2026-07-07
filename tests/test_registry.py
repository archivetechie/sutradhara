"""ConnectedDeviceRegistry tests for the operator relay."""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable

import pytest

from sutradhara._proto import device_pb2
from sutradhara.grpc.registry import (
    Card,
    CommandAck,
    ConnectedDeviceRegistry,
    DeviceOffline,
    PendingListing,
    StreamClosed,
)
from sutradhara.grpc.server import start_registry_sweep_loop, sweep_registry_once
from sutradhara.grpc.store import DeviceIdentity


def test_registry_isolates_operator_devices_and_delivers_command() -> None:
    registry = ConnectedDeviceRegistry()
    stream = registry.register(
        DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")]
    )

    assert [device.device_id for device in registry.devices_for("ada")] == ["mac-1"]
    assert registry.devices_for("other") == []

    pending = registry.send_start_receive(
        operator="ada",
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
        DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32),
        close_stream=close_old,
    )
    old.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")]
    )
    pending = registry.send_start_receive(
        operator="ada",
        device_id="mac-1",
        card_id="card-1",
        artifactclass="s-masters",
        label=None,
        source_ref=None,
        idempotency_key="key-1",
    )

    new = registry.register(
        DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32)
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
        DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")]
    )
    pending = registry.send_start_receive(
        operator="ada",
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
            operator="ada",
            device_id="mac-1",
            card_id="card-1",
            artifactclass="s-masters",
            label=None,
            source_ref=None,
            idempotency_key="key-2",
        )


def test_registry_directory_listing_resolves_matching_future() -> None:
    registry = ConnectedDeviceRegistry()
    stream = registry.register(
        DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")],
        capabilities=["browse"],
    )

    pending = registry.request_directory_listing(
        operator="ada",
        device_id="mac-1",
        card_id="card-1",
        rel_path="DCIM",
    )
    delivered = stream.next_command(timeout=0.01)
    assert isinstance(delivered, PendingListing)
    assert delivered is pending
    assert delivered.command.rel_path == "DCIM"

    listing = device_pb2.DirectoryListing(
        request_id=pending.request_id,
        status=device_pb2.DIR_STATUS_OK,
    )
    assert stream.directory_listing(listing) is pending
    assert pending.future.result(timeout=0).request_id == pending.request_id


def test_registry_drops_stale_or_timed_out_directory_listing() -> None:
    registry = ConnectedDeviceRegistry()
    old = registry.register(
        DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32)
    )
    old.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")]
    )
    pending = registry.request_directory_listing(
        operator="ada",
        device_id="mac-1",
        card_id="card-1",
        rel_path="DCIM",
    )
    registry.fail_listing(
        "mac-1",
        old.generation,
        pending.request_id,
        TimeoutError("timed out"),
    )

    stale_listing = device_pb2.DirectoryListing(
        request_id=pending.request_id,
        status=device_pb2.DIR_STATUS_OK,
    )
    assert old.directory_listing(stale_listing) is None
    with pytest.raises(TimeoutError):
        pending.future.result(timeout=0)


def test_registry_close_fails_pending_commands_and_listings() -> None:
    registry = ConnectedDeviceRegistry()
    stream = registry.register(
        DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card 1", kind="card", size_bytes=10, status="available")]
    )
    command = registry.send_start_receive(
        operator="ada",
        device_id="mac-1",
        card_id="card-1",
        artifactclass="s-masters",
        label=None,
        source_ref=None,
        idempotency_key="key-1",
    )
    listing = registry.request_directory_listing(
        operator="ada",
        device_id="mac-1",
        card_id="card-1",
        rel_path="",
    )

    stream.close(StreamClosed("closed"))

    with pytest.raises(StreamClosed):
        command.future.result(timeout=0)
    with pytest.raises(StreamClosed):
        listing.future.result(timeout=0)


def test_sweep_registry_once_evicts_stale_registry_stream() -> None:
    registry = ConnectedDeviceRegistry()
    registry.register(DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32))

    evicted = sweep_registry_once(
        registry=registry,
        heartbeat_ttl=dt.timedelta(seconds=1),
        now=dt.datetime.now(dt.UTC) + dt.timedelta(seconds=2),
    )

    assert evicted == ["mac-1"]
    assert registry.devices_for("ada") == []


def test_registry_sweep_loop_uses_fast_liveness_tick() -> None:
    registry = ConnectedDeviceRegistry()
    registry.register(DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32))

    stop, thread = start_registry_sweep_loop(
        registry,
        interval_seconds=0.01,
        heartbeat_ttl=dt.timedelta(seconds=0),
    )
    try:
        _eventually(lambda: registry.devices_for("ada") == [])
    finally:
        stop.set()
        thread.join(timeout=1)


def _eventually(predicate: Callable[[], bool]) -> None:
    import time

    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    assert predicate()
