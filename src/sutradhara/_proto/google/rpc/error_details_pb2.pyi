from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BadRequest(_message.Message):
    __slots__ = ("field_violations",)
    class FieldViolation(_message.Message):
        __slots__ = ("field", "description")
        FIELD_FIELD_NUMBER: _ClassVar[int]
        DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
        field: str
        description: str
        def __init__(self, field: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...
    FIELD_VIOLATIONS_FIELD_NUMBER: _ClassVar[int]
    field_violations: _containers.RepeatedCompositeFieldContainer[BadRequest.FieldViolation]
    def __init__(self, field_violations: _Optional[_Iterable[_Union[BadRequest.FieldViolation, _Mapping]]] = ...) -> None: ...
