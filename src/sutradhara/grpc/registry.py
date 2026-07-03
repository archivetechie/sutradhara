"""Thread-safe connected-device registry for the operator console relay.

The registry is deliberately in-memory: an open ``DeviceService.Connect`` stream
is the liveness signal, while durable receive state stays in ``grpc_intake``.
It owns short critical sections, per-stream command queues, pending ack futures,
and generation checks so a superseded helper stream cannot satisfy commands for
the replacement stream.
"""

from __future__ import annotations

import datetime as dt
import queue
import threading
import uuid
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any

from sutradhara._proto import device_pb2
from sutradhara.grpc.store import DeviceIdentity


class RegistryError(RuntimeError):
    """Base class for connected-device registry failures."""


class DeviceOffline(RegistryError):
    """Raised when a command targets a device with no live stream."""


class DeviceOwnerMismatch(RegistryError):
    """Raised when an operator tries to use another operator's device."""


class CardUnavailable(RegistryError):
    """Raised when a command targets a card not in the current snapshot."""


class StreamClosed(RegistryError):
    """Raised when a registered stream is closed or superseded."""


@dataclass(frozen=True)
class Card:
    """One helper-reported card/drive entry."""

    card_id: str
    label: str
    kind: str
    size_bytes: int
    status: str


@dataclass(frozen=True)
class CommandAck:
    """Result of a helper command ack."""

    command_id: str
    accepted: bool
    reason: str | None
    intake_id: str | None


@dataclass(frozen=True)
class PendingCommand:
    """A StartReceive command plus the metadata needed to complete its ack."""

    command_id: str
    operator: str
    device_id: str
    card_id: str
    idempotency_key: str
    abandon_on_reject: bool
    generation: int
    command: device_pb2.StartReceive
    future: Future[CommandAck]
    created_at: dt.datetime


@dataclass(frozen=True)
class PendingListing:
    """A ListDirectory request plus the metadata needed to complete its reply."""

    request_id: str
    operator: str
    device_id: str
    card_id: str
    rel_path: str
    generation: int
    command: device_pb2.ListDirectory
    future: Future[Any]
    created_at: dt.datetime


@dataclass(frozen=True)
class DeviceView:
    """Snapshot returned to the HTTP API."""

    device_id: str
    operator: str
    generation: int
    cards: tuple[Card, ...]
    capabilities: tuple[str, ...]
    last_seen: dt.datetime


@dataclass
class _DeviceEntry:
    identity: DeviceIdentity
    generation: int
    command_queue: queue.Queue[PendingCommand | PendingListing | None] = field(default_factory=queue.Queue)
    pending: dict[str, PendingCommand] = field(default_factory=dict)
    pending_listings: dict[str, PendingListing] = field(default_factory=dict)
    cards: dict[str, Card] = field(default_factory=dict)
    capabilities: tuple[str, ...] = ()
    last_seen: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.UTC))
    close_stream: Any | None = None
    closed: bool = False


class RegisteredDeviceStream:
    """Handle owned by one live ``DeviceService.Connect`` stream."""

    def __init__(
        self,
        registry: ConnectedDeviceRegistry,
        *,
        device_id: str,
        generation: int,
    ) -> None:
        self._registry = registry
        self.device_id = device_id
        self.generation = generation

    def update_cards(self, cards: list[Card], capabilities: list[str] | None = None) -> None:
        """Replace this stream's card snapshot."""

        self._registry.update_cards(
            self.device_id,
            self.generation,
            cards,
            capabilities=capabilities,
        )

    def heartbeat(self) -> None:
        """Mark this stream alive."""

        self._registry.heartbeat(self.device_id, self.generation)

    def ack(self, ack: CommandAck) -> PendingCommand | None:
        """Resolve a pending command if this stream generation is still current."""

        return self._registry.ack_command(self.device_id, self.generation, ack)

    def directory_listing(self, listing: Any) -> PendingListing | None:
        """Resolve a pending directory listing for this stream."""

        return self._registry.complete_directory_listing(
            self.device_id,
            self.generation,
            listing,
        )

    def next_command(self, *, timeout: float = 0.25) -> PendingCommand | PendingListing | None:
        """Return the next queued command for this stream."""

        return self._registry.next_command(self.device_id, self.generation, timeout=timeout)

    def fail_pending(
        self,
        pending: PendingCommand | PendingListing,
        exc: BaseException,
    ) -> PendingCommand | PendingListing | None:
        """Fail one pending item on this stream."""

        return self._registry.fail_pending(self.device_id, self.generation, pending, exc)

    def fail_command(self, command_id: str, exc: BaseException) -> PendingCommand | None:
        """Fail one pending command on this stream."""

        return self._registry.fail_command(self.device_id, self.generation, command_id, exc)

    def close(self, reason: BaseException | None = None) -> None:
        """Close this stream if it is still current."""

        self._registry.close_stream(self.device_id, self.generation, reason=reason)


class ConnectedDeviceRegistry:
    """In-memory registry of online helper streams and their pending commands."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, _DeviceEntry] = {}

    def register(
        self,
        identity: DeviceIdentity,
        *,
        close_stream: Any | None = None,
    ) -> RegisteredDeviceStream:
        """Register a helper stream, replacing any prior stream for the device."""

        with self._lock:
            old = self._entries.get(identity.device_id)
            generation = (old.generation + 1) if old is not None else 1
            if old is not None:
                self._close_entry_locked(
                    old,
                    reason=StreamClosed("device stream was superseded"),
                    call_close=True,
                )
            self._entries[identity.device_id] = _DeviceEntry(
                identity=identity,
                generation=generation,
                close_stream=close_stream,
            )
            return RegisteredDeviceStream(
                self,
                device_id=identity.device_id,
                generation=generation,
            )

    def update_cards(
        self,
        device_id: str,
        generation: int,
        cards: list[Card],
        *,
        capabilities: list[str] | None = None,
    ) -> None:
        """Replace the current card snapshot for a live stream."""

        with self._lock:
            entry = self._current_entry_locked(device_id, generation)
            entry.cards = {card.card_id: card for card in cards}
            entry.capabilities = tuple(sorted(set(capabilities or ())))
            entry.last_seen = dt.datetime.now(dt.UTC)

    def heartbeat(self, device_id: str, generation: int) -> None:
        """Refresh a stream's liveness timestamp."""

        with self._lock:
            entry = self._current_entry_locked(device_id, generation)
            entry.last_seen = dt.datetime.now(dt.UTC)

    def devices_for(self, operator: str) -> list[DeviceView]:
        """Return online devices owned by one operator."""

        with self._lock:
            devices = [
                DeviceView(
                    device_id=device_id,
                    operator=entry.identity.operator,
                    generation=entry.generation,
                    cards=tuple(sorted(entry.cards.values(), key=lambda card: card.card_id)),
                    capabilities=entry.capabilities,
                    last_seen=entry.last_seen,
                )
                for device_id, entry in self._entries.items()
                if not entry.closed and entry.identity.operator == operator
            ]
        return sorted(devices, key=lambda item: item.device_id)

    def device_for(self, *, operator: str, device_id: str) -> DeviceView:
        """Return one online device view after owner checks."""

        with self._lock:
            entry = self._entry_for_operator_locked(operator, device_id)
            return DeviceView(
                device_id=device_id,
                operator=entry.identity.operator,
                generation=entry.generation,
                cards=tuple(sorted(entry.cards.values(), key=lambda card: card.card_id)),
                capabilities=entry.capabilities,
                last_seen=entry.last_seen,
            )

    def card_for(self, *, operator: str, device_id: str, card_id: str) -> Card:
        """Return one online card after owner and availability checks."""

        with self._lock:
            entry = self._entry_for_operator_locked(operator, device_id)
            card = entry.cards.get(card_id)
            if card is None:
                raise CardUnavailable("card is not present on the device")
            return card

    def active_fingerprint_for(self, *, operator: str, device_id: str) -> str:
        """Return the live stream's authenticated cert fingerprint after owner checks."""

        with self._lock:
            entry = self._entry_for_operator_locked(operator, device_id)
            return entry.identity.fingerprint

    def send_start_receive(
        self,
        *,
        operator: str,
        device_id: str,
        card_id: str,
        artifactclass: str,
        label: str | None,
        source_ref: str | None,
        idempotency_key: str,
        abandon_on_reject: bool = True,
    ) -> PendingCommand:
        """Queue a StartReceive command for a live, operator-owned device."""

        with self._lock:
            entry = self._entry_for_operator_locked(operator, device_id)
            if card_id not in entry.cards:
                raise CardUnavailable("card is not present on the device")
            command_id = uuid.uuid4().hex
            future: Future[CommandAck] = Future()
            pending = PendingCommand(
                command_id=command_id,
                operator=operator,
                device_id=device_id,
                card_id=card_id,
                idempotency_key=idempotency_key,
                abandon_on_reject=abandon_on_reject,
                generation=entry.generation,
                command=device_pb2.StartReceive(
                    command_id=command_id,
                    card_id=card_id,
                    artifactclass=artifactclass,
                    label=label or "",
                    source_ref=source_ref or "",
                    idempotency_key=idempotency_key,
                ),
                future=future,
                created_at=dt.datetime.now(dt.UTC),
            )
            entry.pending[command_id] = pending
            entry.command_queue.put(pending)
            return pending

    def request_directory_listing(
        self,
        *,
        operator: str,
        device_id: str,
        card_id: str,
        rel_path: str,
    ) -> PendingListing:
        """Queue a ListDirectory request for a live, operator-owned device."""

        with self._lock:
            entry = self._entry_for_operator_locked(operator, device_id)
            if card_id not in entry.cards:
                raise CardUnavailable("card is not present on the device")
            request_id = uuid.uuid4().hex
            future: Future[Any] = Future()
            pending = PendingListing(
                request_id=request_id,
                operator=operator,
                device_id=device_id,
                card_id=card_id,
                rel_path=rel_path,
                generation=entry.generation,
                command=device_pb2.ListDirectory(
                    request_id=request_id,
                    card_id=card_id,
                    rel_path=rel_path,
                ),
                future=future,
                created_at=dt.datetime.now(dt.UTC),
            )
            entry.pending_listings[request_id] = pending
            entry.command_queue.put(pending)
            return pending

    def next_command(
        self,
        device_id: str,
        generation: int,
        *,
        timeout: float,
    ) -> PendingCommand | PendingListing | None:
        """Drain the next command for a stream, respecting generation changes."""

        with self._lock:
            entry = self._current_entry_locked(device_id, generation)
            command_queue = entry.command_queue
        try:
            pending = command_queue.get(timeout=timeout)
        except queue.Empty:
            return None
        if pending is None:
            raise StreamClosed("device stream closed")
        with self._lock:
            self._current_entry_locked(device_id, generation)
            if pending.generation != generation:
                raise StreamClosed("device stream was superseded")
            entry = self._entries[device_id]
            if isinstance(pending, PendingCommand):
                if pending.command_id not in entry.pending:
                    return None
            elif pending.request_id not in entry.pending_listings:
                return None
        return pending

    def ack_command(
        self,
        device_id: str,
        generation: int,
        ack: CommandAck,
    ) -> PendingCommand | None:
        """Resolve a command ack only if it belongs to the current stream generation."""

        with self._lock:
            try:
                entry = self._current_entry_locked(device_id, generation)
            except StreamClosed:
                return None
            pending = entry.pending.pop(ack.command_id, None)
            if pending is None:
                return None
            if not pending.future.done():
                pending.future.set_result(ack)
            entry.last_seen = dt.datetime.now(dt.UTC)
            return pending

    def complete_directory_listing(
        self,
        device_id: str,
        generation: int,
        listing: Any,
    ) -> PendingListing | None:
        """Resolve a directory listing only if it belongs to the current generation."""

        with self._lock:
            try:
                entry = self._current_entry_locked(device_id, generation)
            except StreamClosed:
                return None
            pending = entry.pending_listings.pop(listing.request_id, None)
            if pending is None:
                return None
            if not pending.future.done():
                pending.future.set_result(listing)
            entry.last_seen = dt.datetime.now(dt.UTC)
            return pending

    def fail_pending(
        self,
        device_id: str,
        generation: int,
        pending: PendingCommand | PendingListing,
        exc: BaseException,
    ) -> PendingCommand | PendingListing | None:
        """Fail a pending command or listing if it belongs to the current stream."""

        if isinstance(pending, PendingCommand):
            return self.fail_command(device_id, generation, pending.command_id, exc)
        return self.fail_listing(device_id, generation, pending.request_id, exc)

    def fail_command(
        self,
        device_id: str,
        generation: int,
        command_id: str,
        exc: BaseException,
    ) -> PendingCommand | None:
        """Fail a pending command if it belongs to the current stream."""

        with self._lock:
            try:
                entry = self._current_entry_locked(device_id, generation)
            except StreamClosed:
                return None
            pending = entry.pending.pop(command_id, None)
            if pending is not None and not pending.future.done():
                pending.future.set_exception(exc)
            return pending

    def fail_listing(
        self,
        device_id: str,
        generation: int,
        request_id: str,
        exc: BaseException,
    ) -> PendingListing | None:
        """Fail a pending directory listing if it belongs to the current stream."""

        with self._lock:
            try:
                entry = self._current_entry_locked(device_id, generation)
            except StreamClosed:
                return None
            pending = entry.pending_listings.pop(request_id, None)
            if pending is not None and not pending.future.done():
                pending.future.set_exception(exc)
            return pending

    def evict(self, device_id: str, *, reason: BaseException | None = None) -> bool:
        """Close and remove one live device stream."""

        with self._lock:
            entry = self._entries.pop(device_id, None)
            if entry is None:
                return False
            self._close_entry_locked(entry, reason=reason or StreamClosed("device evicted"))
            return True

    def evict_stale(
        self,
        *,
        ttl: dt.timedelta,
        now: dt.datetime | None = None,
    ) -> list[str]:
        """Evict streams whose heartbeat is older than ``ttl``."""

        current = now or dt.datetime.now(dt.UTC)
        evicted: list[str] = []
        with self._lock:
            for device_id, entry in list(self._entries.items()):
                if entry.last_seen + ttl >= current:
                    continue
                evicted.append(device_id)
                self._entries.pop(device_id, None)
                self._close_entry_locked(entry, reason=StreamClosed("device heartbeat expired"))
        return evicted

    def close_stream(
        self,
        device_id: str,
        generation: int,
        *,
        reason: BaseException | None = None,
    ) -> None:
        """Close a stream only if it is still the current generation."""

        with self._lock:
            entry = self._entries.get(device_id)
            if entry is None or entry.generation != generation:
                return
            self._entries.pop(device_id, None)
            self._close_entry_locked(entry, reason=reason or StreamClosed("device stream closed"))

    def _entry_for_operator_locked(self, operator: str, device_id: str) -> _DeviceEntry:
        entry = self._entries.get(device_id)
        if entry is None or entry.closed:
            raise DeviceOffline("device is offline")
        if entry.identity.operator != operator:
            raise DeviceOwnerMismatch("device belongs to a different operator")
        return entry

    def _current_entry_locked(self, device_id: str, generation: int) -> _DeviceEntry:
        entry = self._entries.get(device_id)
        if entry is None or entry.closed or entry.generation != generation:
            raise StreamClosed("device stream is not current")
        return entry

    def _close_entry_locked(
        self,
        entry: _DeviceEntry,
        *,
        reason: BaseException,
        call_close: bool = True,
    ) -> None:
        if entry.closed:
            return
        entry.closed = True
        for pending in list(entry.pending.values()):
            if not pending.future.done():
                pending.future.set_exception(reason)
        entry.pending.clear()
        for pending in list(entry.pending_listings.values()):
            if not pending.future.done():
                pending.future.set_exception(reason)
        entry.pending_listings.clear()
        entry.command_queue.put(None)
        if call_close and entry.close_stream is not None:
            entry.close_stream()
