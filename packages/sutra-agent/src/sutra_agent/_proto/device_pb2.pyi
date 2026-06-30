from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CardKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CARD_KIND_UNSPECIFIED: _ClassVar[CardKind]
    CARD_KIND_CARD: _ClassVar[CardKind]
    CARD_KIND_DRIVE: _ClassVar[CardKind]
    CARD_KIND_OTHER: _ClassVar[CardKind]

class CommandAckStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COMMAND_ACK_STATUS_UNSPECIFIED: _ClassVar[CommandAckStatus]
    COMMAND_ACK_STATUS_ACCEPTED: _ClassVar[CommandAckStatus]
    COMMAND_ACK_STATUS_REJECTED: _ClassVar[CommandAckStatus]
CARD_KIND_UNSPECIFIED: CardKind
CARD_KIND_CARD: CardKind
CARD_KIND_DRIVE: CardKind
CARD_KIND_OTHER: CardKind
COMMAND_ACK_STATUS_UNSPECIFIED: CommandAckStatus
COMMAND_ACK_STATUS_ACCEPTED: CommandAckStatus
COMMAND_ACK_STATUS_REJECTED: CommandAckStatus

class DeviceMessage(_message.Message):
    __slots__ = ("card_snapshot", "heartbeat", "command_ack", "active_receives")
    CARD_SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ACK_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_RECEIVES_FIELD_NUMBER: _ClassVar[int]
    card_snapshot: CardSnapshot
    heartbeat: Heartbeat
    command_ack: CommandAck
    active_receives: ActiveReceives
    def __init__(self, card_snapshot: _Optional[_Union[CardSnapshot, _Mapping]] = ..., heartbeat: _Optional[_Union[Heartbeat, _Mapping]] = ..., command_ack: _Optional[_Union[CommandAck, _Mapping]] = ..., active_receives: _Optional[_Union[ActiveReceives, _Mapping]] = ...) -> None: ...

class CardSnapshot(_message.Message):
    __slots__ = ("cards",)
    CARDS_FIELD_NUMBER: _ClassVar[int]
    cards: _containers.RepeatedCompositeFieldContainer[Card]
    def __init__(self, cards: _Optional[_Iterable[_Union[Card, _Mapping]]] = ...) -> None: ...

class Card(_message.Message):
    __slots__ = ("card_id", "label", "kind", "size_bytes", "status")
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    card_id: str
    label: str
    kind: CardKind
    size_bytes: int
    status: str
    def __init__(self, card_id: _Optional[str] = ..., label: _Optional[str] = ..., kind: _Optional[_Union[CardKind, str]] = ..., size_bytes: _Optional[int] = ..., status: _Optional[str] = ...) -> None: ...

class Heartbeat(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CommandAck(_message.Message):
    __slots__ = ("command_id", "status", "reason", "intake_id")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    INTAKE_ID_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    status: CommandAckStatus
    reason: str
    intake_id: str
    def __init__(self, command_id: _Optional[str] = ..., status: _Optional[_Union[CommandAckStatus, str]] = ..., reason: _Optional[str] = ..., intake_id: _Optional[str] = ...) -> None: ...

class ActiveReceives(_message.Message):
    __slots__ = ("receives",)
    RECEIVES_FIELD_NUMBER: _ClassVar[int]
    receives: _containers.RepeatedCompositeFieldContainer[ActiveReceive]
    def __init__(self, receives: _Optional[_Iterable[_Union[ActiveReceive, _Mapping]]] = ...) -> None: ...

class ActiveReceive(_message.Message):
    __slots__ = ("card_id", "idempotency_key", "intake_id", "state")
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    INTAKE_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    card_id: str
    idempotency_key: str
    intake_id: str
    state: str
    def __init__(self, card_id: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., intake_id: _Optional[str] = ..., state: _Optional[str] = ...) -> None: ...

class ServerCommand(_message.Message):
    __slots__ = ("start_receive",)
    START_RECEIVE_FIELD_NUMBER: _ClassVar[int]
    start_receive: StartReceive
    def __init__(self, start_receive: _Optional[_Union[StartReceive, _Mapping]] = ...) -> None: ...

class StartReceive(_message.Message):
    __slots__ = ("command_id", "card_id", "artifactclass", "label", "source_ref", "idempotency_key")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    ARTIFACTCLASS_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    SOURCE_REF_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    card_id: str
    artifactclass: str
    label: str
    source_ref: str
    idempotency_key: str
    def __init__(self, command_id: _Optional[str] = ..., card_id: _Optional[str] = ..., artifactclass: _Optional[str] = ..., label: _Optional[str] = ..., source_ref: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...
