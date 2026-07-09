from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StartIntakeRequest(_message.Message):
    __slots__ = ("idempotency_key", "artifactclass", "source_kind", "source_ref", "label", "source_plan_digest", "planned_bytes_total")
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    ARTIFACTCLASS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_KIND_FIELD_NUMBER: _ClassVar[int]
    SOURCE_REF_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    SOURCE_PLAN_DIGEST_FIELD_NUMBER: _ClassVar[int]
    PLANNED_BYTES_TOTAL_FIELD_NUMBER: _ClassVar[int]
    idempotency_key: str
    artifactclass: str
    source_kind: str
    source_ref: str
    label: str
    source_plan_digest: str
    planned_bytes_total: int
    def __init__(self, idempotency_key: _Optional[str] = ..., artifactclass: _Optional[str] = ..., source_kind: _Optional[str] = ..., source_ref: _Optional[str] = ..., label: _Optional[str] = ..., source_plan_digest: _Optional[str] = ..., planned_bytes_total: _Optional[int] = ...) -> None: ...

class StartIntakeResponse(_message.Message):
    __slots__ = ("intake_id",)
    INTAKE_ID_FIELD_NUMBER: _ClassVar[int]
    intake_id: str
    def __init__(self, intake_id: _Optional[str] = ...) -> None: ...

class FileChunk(_message.Message):
    __slots__ = ("intake_id", "relpath", "data", "offset", "is_last", "file_size")
    INTAKE_ID_FIELD_NUMBER: _ClassVar[int]
    RELPATH_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    IS_LAST_FIELD_NUMBER: _ClassVar[int]
    FILE_SIZE_FIELD_NUMBER: _ClassVar[int]
    intake_id: str
    relpath: str
    data: bytes
    offset: int
    is_last: bool
    file_size: int
    def __init__(self, intake_id: _Optional[str] = ..., relpath: _Optional[str] = ..., data: _Optional[bytes] = ..., offset: _Optional[int] = ..., is_last: _Optional[bool] = ..., file_size: _Optional[int] = ...) -> None: ...

class FileReceipt(_message.Message):
    __slots__ = ("relpath", "server_sha256", "received_bytes")
    RELPATH_FIELD_NUMBER: _ClassVar[int]
    SERVER_SHA256_FIELD_NUMBER: _ClassVar[int]
    RECEIVED_BYTES_FIELD_NUMBER: _ClassVar[int]
    relpath: str
    server_sha256: str
    received_bytes: int
    def __init__(self, relpath: _Optional[str] = ..., server_sha256: _Optional[str] = ..., received_bytes: _Optional[int] = ...) -> None: ...

class ListIntakeFilesRequest(_message.Message):
    __slots__ = ("intake_id",)
    INTAKE_ID_FIELD_NUMBER: _ClassVar[int]
    intake_id: str
    def __init__(self, intake_id: _Optional[str] = ...) -> None: ...

class ListIntakeFilesResponse(_message.Message):
    __slots__ = ("files",)
    FILES_FIELD_NUMBER: _ClassVar[int]
    files: _containers.RepeatedCompositeFieldContainer[FileRecord]
    def __init__(self, files: _Optional[_Iterable[_Union[FileRecord, _Mapping]]] = ...) -> None: ...

class FileRecord(_message.Message):
    __slots__ = ("relpath", "server_sha256", "bytes")
    RELPATH_FIELD_NUMBER: _ClassVar[int]
    SERVER_SHA256_FIELD_NUMBER: _ClassVar[int]
    BYTES_FIELD_NUMBER: _ClassVar[int]
    relpath: str
    server_sha256: str
    bytes: int
    def __init__(self, relpath: _Optional[str] = ..., server_sha256: _Optional[str] = ..., bytes: _Optional[int] = ...) -> None: ...

class CommitIntakeRequest(_message.Message):
    __slots__ = ("intake_id", "files", "receive_facts", "package_indexes", "manifest_digest")
    INTAKE_ID_FIELD_NUMBER: _ClassVar[int]
    FILES_FIELD_NUMBER: _ClassVar[int]
    RECEIVE_FACTS_FIELD_NUMBER: _ClassVar[int]
    PACKAGE_INDEXES_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_DIGEST_FIELD_NUMBER: _ClassVar[int]
    intake_id: str
    files: _containers.RepeatedCompositeFieldContainer[ManifestEntry]
    receive_facts: ReceiveFacts
    package_indexes: _containers.RepeatedCompositeFieldContainer[PackageIndex]
    manifest_digest: str
    def __init__(self, intake_id: _Optional[str] = ..., files: _Optional[_Iterable[_Union[ManifestEntry, _Mapping]]] = ..., receive_facts: _Optional[_Union[ReceiveFacts, _Mapping]] = ..., package_indexes: _Optional[_Iterable[_Union[PackageIndex, _Mapping]]] = ..., manifest_digest: _Optional[str] = ...) -> None: ...

class ManifestEntry(_message.Message):
    __slots__ = ("relpath", "client_sha256", "bytes")
    RELPATH_FIELD_NUMBER: _ClassVar[int]
    CLIENT_SHA256_FIELD_NUMBER: _ClassVar[int]
    BYTES_FIELD_NUMBER: _ClassVar[int]
    relpath: str
    client_sha256: str
    bytes: int
    def __init__(self, relpath: _Optional[str] = ..., client_sha256: _Optional[str] = ..., bytes: _Optional[int] = ...) -> None: ...

class ReceiveFacts(_message.Message):
    __slots__ = ("canonicalization_version", "skipped_count", "package_profile_version")
    CANONICALIZATION_VERSION_FIELD_NUMBER: _ClassVar[int]
    SKIPPED_COUNT_FIELD_NUMBER: _ClassVar[int]
    PACKAGE_PROFILE_VERSION_FIELD_NUMBER: _ClassVar[int]
    canonicalization_version: str
    skipped_count: int
    package_profile_version: str
    def __init__(self, canonicalization_version: _Optional[str] = ..., skipped_count: _Optional[int] = ..., package_profile_version: _Optional[str] = ...) -> None: ...

class PackageIndex(_message.Message):
    __slots__ = ("logical_member_path", "stored_member_path", "sha256", "members")
    LOGICAL_MEMBER_PATH_FIELD_NUMBER: _ClassVar[int]
    STORED_MEMBER_PATH_FIELD_NUMBER: _ClassVar[int]
    SHA256_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    logical_member_path: str
    stored_member_path: str
    sha256: str
    members: _containers.RepeatedCompositeFieldContainer[PackageMemberEntry]
    def __init__(self, logical_member_path: _Optional[str] = ..., stored_member_path: _Optional[str] = ..., sha256: _Optional[str] = ..., members: _Optional[_Iterable[_Union[PackageMemberEntry, _Mapping]]] = ...) -> None: ...

class PackageMemberEntry(_message.Message):
    __slots__ = ("member", "type", "length", "sha256", "data_offset", "linkname")
    MEMBER_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    SHA256_FIELD_NUMBER: _ClassVar[int]
    DATA_OFFSET_FIELD_NUMBER: _ClassVar[int]
    LINKNAME_FIELD_NUMBER: _ClassVar[int]
    member: str
    type: str
    length: int
    sha256: str
    data_offset: int
    linkname: str
    def __init__(self, member: _Optional[str] = ..., type: _Optional[str] = ..., length: _Optional[int] = ..., sha256: _Optional[str] = ..., data_offset: _Optional[int] = ..., linkname: _Optional[str] = ...) -> None: ...

class CommitIntakeResponse(_message.Message):
    __slots__ = ("intake_id", "status", "reupload_relpaths")
    INTAKE_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    REUPLOAD_RELPATHS_FIELD_NUMBER: _ClassVar[int]
    intake_id: str
    status: str
    reupload_relpaths: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, intake_id: _Optional[str] = ..., status: _Optional[str] = ..., reupload_relpaths: _Optional[_Iterable[str]] = ...) -> None: ...

class IntakeStatusRequest(_message.Message):
    __slots__ = ("intake_id",)
    INTAKE_ID_FIELD_NUMBER: _ClassVar[int]
    intake_id: str
    def __init__(self, intake_id: _Optional[str] = ...) -> None: ...

class IntakeStatusResponse(_message.Message):
    __slots__ = ("intake_id", "status", "errors")
    INTAKE_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ERRORS_FIELD_NUMBER: _ClassVar[int]
    intake_id: str
    status: str
    errors: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, intake_id: _Optional[str] = ..., status: _Optional[str] = ..., errors: _Optional[_Iterable[str]] = ...) -> None: ...

class AbortIntakeRequest(_message.Message):
    __slots__ = ("intake_id",)
    INTAKE_ID_FIELD_NUMBER: _ClassVar[int]
    intake_id: str
    def __init__(self, intake_id: _Optional[str] = ...) -> None: ...

class AbortIntakeResponse(_message.Message):
    __slots__ = ("intake_id", "status")
    INTAKE_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    intake_id: str
    status: str
    def __init__(self, intake_id: _Optional[str] = ..., status: _Optional[str] = ...) -> None: ...
