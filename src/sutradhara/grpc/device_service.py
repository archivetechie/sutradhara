"""DeviceService relay for operator-console controlled local receives.

The service holds one bidirectional stream per helper. Upstream helper messages
update the connected-device registry or settle command acks; downstream commands
are drained from the registry queue by the stream-owning thread, which keeps
gRPC writes single-threaded while HTTP handlers can enqueue work safely.
"""

from __future__ import annotations

import datetime as dt
import logging
import threading
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import grpc
from sqlalchemy import Engine

from sutradhara._proto import device_pb2, device_pb2_grpc
from sutradhara.api import store as api_store
from sutradhara.catalog.session import make_session_factory
from sutradhara.grpc import ca as grpc_ca
from sutradhara.grpc import store as grpc_store
from sutradhara.grpc.registry import (
    Card,
    CommandAck,
    ConnectedDeviceRegistry,
    PendingCommand,
    PendingListing,
    RegisteredDeviceStream,
    StreamClosed,
    validate_card_id,
)

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeviceServiceConfig:
    """Runtime dependencies for ``DeviceService``."""

    engine: Engine
    registry: ConnectedDeviceRegistry
    command_poll_seconds: float = 0.25
    max_stream_lifetime: dt.timedelta = dt.timedelta(hours=24)


class DeviceService(device_pb2_grpc.DeviceServiceServicer):
    """gRPC relay service used by local helper daemons."""

    def __init__(self, config: DeviceServiceConfig) -> None:
        self.config = config

    def Connect(self, request_iterator: Iterable[Any], context: Any) -> Iterable[Any]:
        """Maintain one helper control stream."""

        identity = self._identity(context)
        stop = threading.Event()
        connected_at = dt.datetime.now(dt.UTC)
        stream = self.config.registry.register(identity, close_stream=stop.set)

        reader = threading.Thread(
            target=self._read_messages,
            args=(stream, request_iterator, stop, identity, connected_at),
            name=f"sutra-device-reader-{identity.device_id}",
            daemon=True,
        )
        reader.start()
        try:
            # Tonic completes this bidi call only after the first response; an
            # empty command is the relay handshake and the Rust client no-ops it.
            yield device_pb2.ServerCommand()
            while not stop.is_set():
                if self._stream_expired(connected_at):
                    reason = PermissionError("device stream lifetime expired")
                    stream.close(reason)
                    break
                try:
                    pending = stream.next_command(timeout=self.config.command_poll_seconds)
                except StreamClosed:
                    break
                if pending is None:
                    continue
                if not self._dispatch_authorized(identity):
                    stream.fail_pending(
                        pending,
                        PermissionError("device certificate is no longer enrolled"),
                    )
                    stream.close(PermissionError("device certificate is no longer enrolled"))
                    break
                if isinstance(pending, PendingCommand):
                    yield device_pb2.ServerCommand(start_receive=pending.command)
                elif isinstance(pending, PendingListing):
                    yield device_pb2.ServerCommand(list_directory=pending.command)
        finally:
            stop.set()
            stream.close(StreamClosed("device stream ended"))
            reader.join(timeout=1.0)

    def _read_messages(
        self,
        stream: RegisteredDeviceStream,
        request_iterator: Iterable[Any],
        stop: threading.Event,
        identity: grpc_store.DeviceIdentity,
        connected_at: dt.datetime,
    ) -> None:
        try:
            for message in request_iterator:
                if stop.is_set():
                    break
                if self._stream_expired(connected_at):
                    stop.set()
                    stream.close(PermissionError("device stream lifetime expired"))
                    break
                if not self._dispatch_authorized(identity):
                    stop.set()
                    stream.close(PermissionError("device certificate is no longer enrolled"))
                    break
                self._handle_message(stream, message)
        except Exception:
            stop.set()
            stream.close(StreamClosed("device stream reader failed"))
        else:
            stop.set()

    def _handle_message(self, stream: RegisteredDeviceStream, message: Any) -> None:
        kind = message.WhichOneof("payload")
        if kind == "card_snapshot":
            stream.update_cards(
                [_card_from_proto(card) for card in message.card_snapshot.cards],
                capabilities=list(message.card_snapshot.capabilities),
            )
            return
        if kind == "heartbeat":
            stream.heartbeat()
            return
        if kind == "command_ack":
            pending = stream.ack(_ack_from_proto(message.command_ack))
            if pending is not None:
                self._complete_ack(pending, message.command_ack)
            return
        if kind == "active_receives":
            for receive in message.active_receives.receives:
                self._rebuild_active_receive(stream, receive)
            return
        if kind == "directory_listing":
            stream.directory_listing(message.directory_listing)

    def _complete_ack(self, pending: PendingCommand, ack: Any) -> None:
        if ack.status == device_pb2.COMMAND_ACK_STATUS_ACCEPTED and ack.intake_id:
            completed = self._complete_receive(
                operator=pending.operator,
                device_id=pending.device_id,
                card_id=pending.card_id,
                idempotency_key=pending.idempotency_key,
                intake_id=ack.intake_id,
                abandon_on_failure=pending.abandon_on_reject,
            )
            if not completed:
                LOG.warning(
                    "dropped device receive ack with failed card correlation: "
                    "device_id=%s intake_id=%s card_id=%s",
                    pending.device_id,
                    ack.intake_id,
                    pending.card_id,
                )
            return
        if pending.abandon_on_reject:
            api_store.fail_device_receive_intent(
                self.config.engine,
                operator_username=pending.operator,
                device_id=pending.device_id,
                idempotency_key=pending.idempotency_key,
            )

    def _rebuild_active_receive(self, stream: RegisteredDeviceStream, receive: Any) -> None:
        identity = self._stream_identity(stream)
        if identity is None or not receive.intake_id or not receive.idempotency_key:
            return
        try:
            validate_card_id(receive.card_id)
        except ValueError:
            LOG.warning(
                "ignored active receive with invalid card identity: device_id=%s intake_id=%s",
                identity.device_id,
                receive.intake_id,
            )
            return
        self._complete_receive(
            operator=identity.operator,
            device_id=identity.device_id,
            card_id=receive.card_id,
            idempotency_key=receive.idempotency_key,
            intake_id=receive.intake_id,
            abandon_on_failure=False,
        )

    def _complete_receive(
        self,
        *,
        operator: str,
        device_id: str,
        card_id: str,
        idempotency_key: str,
        intake_id: str,
        abandon_on_failure: bool,
    ) -> bool:
        factory = make_session_factory(self.config.engine)
        with factory.begin() as session:
            correlated = grpc_store.set_card_id(
                session,
                intake_id=intake_id,
                operator=operator,
                device_id=device_id,
                card_id=card_id,
            )
        if not correlated:
            if abandon_on_failure:
                api_store.fail_device_receive_intent(
                    self.config.engine,
                    operator_username=operator,
                    device_id=device_id,
                    idempotency_key=idempotency_key,
                )
            return False
        return api_store.store_device_receive_response(
            self.config.engine,
            operator_username=operator,
            device_id=device_id,
            idempotency_key=idempotency_key,
            intake_id=intake_id,
            response_json={"intakeId": intake_id, "status": "streaming"},
        )

    def _identity(self, context: Any) -> grpc_store.DeviceIdentity:
        try:
            return grpc_ca.resolve_peer_identity(self.config.engine, context)
        except PermissionError as exc:
            _abort(context, grpc.StatusCode.UNAUTHENTICATED, str(exc))

    def _dispatch_authorized(self, identity: grpc_store.DeviceIdentity) -> bool:
        factory = make_session_factory(self.config.engine)
        with factory() as session:
            try:
                grpc_store.resolve_device(
                    session,
                    device_id=identity.device_id,
                    cert_fingerprint=identity.fingerprint,
                )
            except PermissionError:
                return False
        return True

    def _stream_expired(self, connected_at: dt.datetime) -> bool:
        return connected_at + self.config.max_stream_lifetime < dt.datetime.now(dt.UTC)

    def _stream_identity(self, stream: RegisteredDeviceStream) -> grpc_store.DeviceIdentity | None:
        factory = make_session_factory(self.config.engine)
        with factory() as session:
            try:
                operator = grpc_store.operator_for_device(session, stream.device_id)
            except PermissionError:
                return None
        if operator is None:
            return None
        return grpc_store.DeviceIdentity(
            operator=operator, device_id=stream.device_id, fingerprint=""
        )


def _card_from_proto(card: Any) -> Card:
    return Card(
        card_id=card.card_id,
        label=card.label,
        kind=_kind_name(card.kind),
        size_bytes=int(card.size_bytes),
        status=card.status,
    )


def _kind_name(value: int) -> str:
    if value == device_pb2.CARD_KIND_CARD:
        return "card"
    if value == device_pb2.CARD_KIND_DRIVE:
        return "drive"
    if value == device_pb2.CARD_KIND_OTHER:
        return "other"
    return "other"


def _ack_from_proto(ack: Any) -> CommandAck:
    return CommandAck(
        command_id=ack.command_id,
        accepted=ack.status == device_pb2.COMMAND_ACK_STATUS_ACCEPTED,
        reason=ack.reason or None,
        intake_id=ack.intake_id or None,
    )


def _abort(context: Any, code: grpc.StatusCode, message: str) -> Any:
    context.abort(code, message)
    raise RuntimeError(message)
