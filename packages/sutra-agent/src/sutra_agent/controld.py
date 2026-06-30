"""Outbound control daemon for the operator-console helper.

The daemon keeps a single mTLS ``DeviceService.Connect`` stream open to the
Sutradhara server. It reports local cards by opaque id, accepts server commands
for those ids, starts streaming receives in background threads, and reports
early command acknowledgements as soon as ``StartIntake`` returns.
"""

from __future__ import annotations

import queue
import threading
from dataclasses import dataclass, field, replace
from typing import Any

import grpc

from sutra_agent._proto import device_pb2, device_pb2_grpc
from sutra_agent.config import AgentConfig
from sutra_agent.grpc_client import abort_stream_intake, open_channel, stream_source
from sutra_agent.ledger import (
    active_receive_records,
    clear_active_receive,
    record_active_receive,
)
from sutra_agent.mounts import MountedCard, current_cards, default_mount_watcher

DEFAULT_HEARTBEAT_SECONDS = 20.0
DEFAULT_RECONNECT_INITIAL_SECONDS = 1.0
DEFAULT_RECONNECT_MAX_SECONDS = 30.0


class ControlDaemonError(RuntimeError):
    """Raised when the control daemon cannot run."""


@dataclass
class _InFlightReceive:
    card_id: str
    idempotency_key: str
    intake_id: str | None = None
    thread: threading.Thread | None = None
    started: threading.Event = field(default_factory=threading.Event)


class ControlDaemon:
    """Maintain the server-brokered helper control stream."""

    def __init__(
        self,
        config: AgentConfig,
        *,
        card_source: Any = current_cards,
        stream_source_fn: Any = stream_source,
        abort_intake_fn: Any = abort_stream_intake,
        channel_factory: Any = open_channel,
        stub_factory: Any = device_pb2_grpc.DeviceServiceStub,
        heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
        reconnect_initial_seconds: float = DEFAULT_RECONNECT_INITIAL_SECONDS,
        reconnect_max_seconds: float = DEFAULT_RECONNECT_MAX_SECONDS,
    ) -> None:
        if not config.streaming_enabled:
            raise ControlDaemonError("sutra-agent serve requires streaming config")
        self.config = config
        self.card_source = card_source
        self.stream_source = stream_source_fn
        self.abort_intake = abort_intake_fn
        self.channel_factory = channel_factory
        self.stub_factory = stub_factory
        self.heartbeat_seconds = heartbeat_seconds
        self.reconnect_initial_seconds = reconnect_initial_seconds
        self.reconnect_max_seconds = reconnect_max_seconds
        self._lock = threading.Lock()
        self._cards: dict[str, MountedCard] = {}
        self._active_by_key: dict[str, _InFlightReceive] = {}
        self._restore_active_from_ledger()

    def status_payload(self) -> dict[str, object]:
        """Return a local health/status snapshot for ``sutra-agent serve --status``."""

        with self._lock:
            active = [
                {
                    "card_id": item.card_id,
                    "idempotency_key": item.idempotency_key,
                    "intake_id": item.intake_id,
                }
                for item in self._active_by_key.values()
            ]
        return {
            "configured": True,
            "server": self.config.server_address,
            "device_id": self.config.device_id,
            "active_receives": active,
        }

    def run_forever(self, *, stop: threading.Event | None = None) -> None:
        """Run the control stream until stopped, reconnecting with backoff."""

        stop_event = stop or threading.Event()
        backoff = self.reconnect_initial_seconds
        while not stop_event.is_set():
            try:
                self.run_once(stop=stop_event)
                if stop_event.wait(backoff):
                    return
                backoff = min(self.reconnect_max_seconds, backoff * 2)
            except grpc.RpcError:
                if stop_event.wait(backoff):
                    return
                backoff = min(self.reconnect_max_seconds, backoff * 2)

    def run_once(self, *, stop: threading.Event | None = None) -> None:
        """Open one Connect stream and return when it ends."""

        outer_stop = stop or threading.Event()
        connection_stop = threading.Event()
        outbox: queue.Queue[device_pb2.DeviceMessage | None] = queue.Queue()
        self._restore_active_from_ledger()
        self._send_card_snapshot(self.card_source(), outbox)
        self._send_active_receives(outbox)
        heartbeat = threading.Thread(
            target=self._heartbeat_loop,
            args=(outbox, connection_stop),
            name="sutra-agent-heartbeat",
            daemon=True,
        )
        watcher = threading.Thread(
            target=self._watch_mounts,
            args=(outbox, connection_stop),
            name="sutra-agent-mounts",
            daemon=True,
        )
        heartbeat.start()
        watcher.start()
        try:
            with self.channel_factory(self.config) as channel:
                stub = self.stub_factory(channel)
                responses = stub.Connect(_outgoing_messages(outbox, connection_stop))
                for command in responses:
                    if outer_stop.is_set():
                        break
                    self.handle_command(command, outbox)
        finally:
            connection_stop.set()
            outbox.put(None)
            heartbeat.join(timeout=1)
            watcher.join(timeout=1)

    def handle_command(
        self,
        command: Any,
        outbox: queue.Queue[device_pb2.DeviceMessage | None],
    ) -> None:
        """Handle one server command from ``DeviceService.Connect``."""

        if command.WhichOneof("payload") != "start_receive":
            return
        self._handle_start_receive(command.start_receive, outbox)

    def _handle_start_receive(
        self,
        command: Any,
        outbox: queue.Queue[device_pb2.DeviceMessage | None],
    ) -> None:
        with self._lock:
            card = self._cards.get(command.card_id)
        if card is None or card.status != "available":
            outbox.put(_ack(command.command_id, accepted=False, reason="card not mounted"))
            return
        with self._lock:
            same_key = self._active_by_key.get(command.idempotency_key)
        if same_key is not None:
            if same_key.started.is_set() or same_key.started.wait(timeout=2.0):
                with self._lock:
                    intake_id = same_key.intake_id
                outbox.put(
                    _ack(
                        command.command_id,
                        accepted=True,
                        intake_id=intake_id,
                    )
                )
            else:
                outbox.put(_ack(command.command_id, accepted=False, reason="receive starting"))
            return
        with self._lock:
            busy = next(
                (
                    item
                    for item in self._active_by_key.values()
                    if item.card_id == command.card_id
                ),
                None,
            )
            if busy is not None:
                outbox.put(_ack(command.command_id, accepted=False, reason="card busy"))
                return
            active = _InFlightReceive(
                card_id=command.card_id,
                idempotency_key=command.idempotency_key,
                started=threading.Event(),
            )
            self._active_by_key[command.idempotency_key] = active

        thread = threading.Thread(
            target=self._run_receive,
            args=(active, card, command, outbox),
            name=f"sutra-agent-receive-{command.card_id}",
            daemon=True,
        )
        active.thread = thread
        thread.start()

    def _run_receive(
        self,
        active: _InFlightReceive,
        card: MountedCard,
        command: Any,
        outbox: queue.Queue[device_pb2.DeviceMessage | None],
    ) -> None:
        acked = False

        def on_started(intake_id: str) -> None:
            nonlocal acked
            with self._lock:
                active.intake_id = intake_id
                active.started.set()
            record_active_receive(
                self.config.resolved_ledger_path(),
                card_id=active.card_id,
                idempotency_key=active.idempotency_key,
                intake_id=intake_id,
            )
            outbox.put(_ack(command.command_id, accepted=True, intake_id=intake_id))
            acked = True
            self._send_active_receives(outbox)

        receive_config = replace(
            self.config,
            source_kind=card.kind,
            artifactclass=command.artifactclass or self.config.artifactclass,
        )
        try:
            result = self.stream_source(
                card.mount_path,
                config=receive_config,
                source_ref=command.source_ref or None,
                label=command.label or card.label,
                idempotency_key=command.idempotency_key,
                confirm_timeout=0,
                on_started=on_started,
            )
            if not active.started.is_set():
                on_started(result.intake_id)
        except Exception as exc:
            intake_id = active.intake_id
            if intake_id:
                self.abort_intake(receive_config, intake_id)
            elif not acked:
                outbox.put(_ack(command.command_id, accepted=False, reason=str(exc)))
        finally:
            clear_active_receive(
                self.config.resolved_ledger_path(),
                idempotency_key=active.idempotency_key,
            )
            self._send_active_receives(outbox)
            with self._lock:
                self._active_by_key.pop(active.idempotency_key, None)

    def _send_card_snapshot(
        self,
        cards: list[MountedCard],
        outbox: queue.Queue[device_pb2.DeviceMessage | None],
    ) -> None:
        with self._lock:
            self._cards = {card.card_id: card for card in cards}
        outbox.put(card_snapshot_message(cards))

    def _send_active_receives(self, outbox: queue.Queue[device_pb2.DeviceMessage | None]) -> None:
        records = active_receive_records(self.config.resolved_ledger_path())
        outbox.put(
            device_pb2.DeviceMessage(
                active_receives=device_pb2.ActiveReceives(
                    receives=[
                        device_pb2.ActiveReceive(
                            card_id=record.card_id,
                            idempotency_key=record.idempotency_key,
                            intake_id=record.intake_id,
                            state=record.state,
                        )
                        for record in records
                    ]
                )
            )
        )

    def _restore_active_from_ledger(self) -> None:
        records = active_receive_records(self.config.resolved_ledger_path())
        with self._lock:
            for record in records:
                if record.idempotency_key not in self._active_by_key:
                    self._active_by_key[record.idempotency_key] = _InFlightReceive(
                        card_id=record.card_id,
                        idempotency_key=record.idempotency_key,
                        intake_id=record.intake_id,
                        started=threading.Event(),
                    )
                    self._active_by_key[record.idempotency_key].started.set()

    def _heartbeat_loop(
        self,
        outbox: queue.Queue[device_pb2.DeviceMessage | None],
        stop: threading.Event,
    ) -> None:
        while not stop.wait(self.heartbeat_seconds):
            outbox.put(device_pb2.DeviceMessage(heartbeat=device_pb2.Heartbeat()))

    def _watch_mounts(
        self,
        outbox: queue.Queue[device_pb2.DeviceMessage | None],
        stop: threading.Event,
    ) -> None:
        watcher = default_mount_watcher(self.card_source)
        watcher.run(lambda cards: self._send_card_snapshot(cards, outbox), stop=stop)


def card_snapshot_message(cards: list[MountedCard]) -> device_pb2.DeviceMessage:
    """Build an outbound card snapshot without leaking local mount paths."""

    return device_pb2.DeviceMessage(
        card_snapshot=device_pb2.CardSnapshot(
            cards=[
                device_pb2.Card(
                    card_id=card.card_id,
                    label=card.label,
                    kind=_card_kind_proto(card.kind),
                    size_bytes=card.size_bytes,
                    status=card.status,
                )
                for card in cards
            ]
        )
    )


def _card_kind_proto(kind: str) -> int:
    if kind == "card":
        return device_pb2.CARD_KIND_CARD
    if kind == "drive":
        return device_pb2.CARD_KIND_DRIVE
    return device_pb2.CARD_KIND_OTHER


def _ack(
    command_id: str,
    *,
    accepted: bool,
    reason: str | None = None,
    intake_id: str | None = None,
) -> device_pb2.DeviceMessage:
    return device_pb2.DeviceMessage(
        command_ack=device_pb2.CommandAck(
            command_id=command_id,
            status=(
                device_pb2.COMMAND_ACK_STATUS_ACCEPTED
                if accepted
                else device_pb2.COMMAND_ACK_STATUS_REJECTED
            ),
            reason=reason or "",
            intake_id=intake_id or "",
        )
    )


def _outgoing_messages(
    outbox: queue.Queue[device_pb2.DeviceMessage | None],
    stop: threading.Event,
) -> Any:
    while not stop.is_set():
        item = outbox.get()
        if item is None:
            return
        yield item
