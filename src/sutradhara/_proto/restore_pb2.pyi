from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DurableState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DURABLE_STATE_UNSPECIFIED: _ClassVar[DurableState]
    DURABLE_STATE_STAGED: _ClassVar[DurableState]
    DURABLE_STATE_REVEALED: _ClassVar[DurableState]
DURABLE_STATE_UNSPECIFIED: DurableState
DURABLE_STATE_STAGED: DurableState
DURABLE_STATE_REVEALED: DurableState

class OpenRestoreRequest(_message.Message):
    __slots__ = ("restore_request_item_id", "lease_token", "resume_token")
    RESTORE_REQUEST_ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    RESUME_TOKEN_FIELD_NUMBER: _ClassVar[int]
    restore_request_item_id: int
    lease_token: LeaseToken
    resume_token: ResumeToken
    def __init__(self, restore_request_item_id: _Optional[int] = ..., lease_token: _Optional[_Union[LeaseToken, _Mapping]] = ..., resume_token: _Optional[_Union[ResumeToken, _Mapping]] = ...) -> None: ...

class LeaseToken(_message.Message):
    __slots__ = ("restore_request_item_id", "receiver_device_id", "manifest_sha256", "generation", "expires_unix_ms")
    RESTORE_REQUEST_ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    RECEIVER_DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_SHA256_FIELD_NUMBER: _ClassVar[int]
    GENERATION_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    restore_request_item_id: int
    receiver_device_id: str
    manifest_sha256: bytes
    generation: int
    expires_unix_ms: int
    def __init__(self, restore_request_item_id: _Optional[int] = ..., receiver_device_id: _Optional[str] = ..., manifest_sha256: _Optional[bytes] = ..., generation: _Optional[int] = ..., expires_unix_ms: _Optional[int] = ...) -> None: ...

class ResumeToken(_message.Message):
    __slots__ = ("restore_request_item_id", "manifest_sha256", "committed_index")
    RESTORE_REQUEST_ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_SHA256_FIELD_NUMBER: _ClassVar[int]
    COMMITTED_INDEX_FIELD_NUMBER: _ClassVar[int]
    restore_request_item_id: int
    manifest_sha256: bytes
    committed_index: int
    def __init__(self, restore_request_item_id: _Optional[int] = ..., manifest_sha256: _Optional[bytes] = ..., committed_index: _Optional[int] = ...) -> None: ...

class RestoreFrame(_message.Message):
    __slots__ = ("manifest_head", "manifest_entry", "file_header", "chunk", "file_end", "manifest_end", "job_end", "error")
    MANIFEST_HEAD_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_ENTRY_FIELD_NUMBER: _ClassVar[int]
    FILE_HEADER_FIELD_NUMBER: _ClassVar[int]
    CHUNK_FIELD_NUMBER: _ClassVar[int]
    FILE_END_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_END_FIELD_NUMBER: _ClassVar[int]
    JOB_END_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    manifest_head: ManifestHead
    manifest_entry: ManifestEntry
    file_header: FileHeader
    chunk: Chunk
    file_end: FileEnd
    manifest_end: ManifestEnd
    job_end: JobEnd
    error: RestoreError
    def __init__(self, manifest_head: _Optional[_Union[ManifestHead, _Mapping]] = ..., manifest_entry: _Optional[_Union[ManifestEntry, _Mapping]] = ..., file_header: _Optional[_Union[FileHeader, _Mapping]] = ..., chunk: _Optional[_Union[Chunk, _Mapping]] = ..., file_end: _Optional[_Union[FileEnd, _Mapping]] = ..., manifest_end: _Optional[_Union[ManifestEnd, _Mapping]] = ..., job_end: _Optional[_Union[JobEnd, _Mapping]] = ..., error: _Optional[_Union[RestoreError, _Mapping]] = ...) -> None: ...

class ManifestHead(_message.Message):
    __slots__ = ("total_bytes", "file_count", "single_top_level", "top_component", "manifest_sha256", "lease_token")
    TOTAL_BYTES_FIELD_NUMBER: _ClassVar[int]
    FILE_COUNT_FIELD_NUMBER: _ClassVar[int]
    SINGLE_TOP_LEVEL_FIELD_NUMBER: _ClassVar[int]
    TOP_COMPONENT_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_SHA256_FIELD_NUMBER: _ClassVar[int]
    LEASE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    total_bytes: int
    file_count: int
    single_top_level: bool
    top_component: str
    manifest_sha256: bytes
    lease_token: LeaseToken
    def __init__(self, total_bytes: _Optional[int] = ..., file_count: _Optional[int] = ..., single_top_level: _Optional[bool] = ..., top_component: _Optional[str] = ..., manifest_sha256: _Optional[bytes] = ..., lease_token: _Optional[_Union[LeaseToken, _Mapping]] = ...) -> None: ...

class ManifestEntry(_message.Message):
    __slots__ = ("index", "final_rel_path", "size", "content_sha256", "mode", "uid", "gid", "mtime_unix_seconds")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    FINAL_REL_PATH_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_SHA256_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    UID_FIELD_NUMBER: _ClassVar[int]
    GID_FIELD_NUMBER: _ClassVar[int]
    MTIME_UNIX_SECONDS_FIELD_NUMBER: _ClassVar[int]
    index: int
    final_rel_path: str
    size: int
    content_sha256: bytes
    mode: int
    uid: int
    gid: int
    mtime_unix_seconds: int
    def __init__(self, index: _Optional[int] = ..., final_rel_path: _Optional[str] = ..., size: _Optional[int] = ..., content_sha256: _Optional[bytes] = ..., mode: _Optional[int] = ..., uid: _Optional[int] = ..., gid: _Optional[int] = ..., mtime_unix_seconds: _Optional[int] = ...) -> None: ...

class FileHeader(_message.Message):
    __slots__ = ("index", "final_rel_path", "size", "content_sha256", "mode", "uid", "gid", "mtime_unix_seconds")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    FINAL_REL_PATH_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_SHA256_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    UID_FIELD_NUMBER: _ClassVar[int]
    GID_FIELD_NUMBER: _ClassVar[int]
    MTIME_UNIX_SECONDS_FIELD_NUMBER: _ClassVar[int]
    index: int
    final_rel_path: str
    size: int
    content_sha256: bytes
    mode: int
    uid: int
    gid: int
    mtime_unix_seconds: int
    def __init__(self, index: _Optional[int] = ..., final_rel_path: _Optional[str] = ..., size: _Optional[int] = ..., content_sha256: _Optional[bytes] = ..., mode: _Optional[int] = ..., uid: _Optional[int] = ..., gid: _Optional[int] = ..., mtime_unix_seconds: _Optional[int] = ...) -> None: ...

class Chunk(_message.Message):
    __slots__ = ("data", "offset")
    DATA_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    offset: int
    def __init__(self, data: _Optional[bytes] = ..., offset: _Optional[int] = ...) -> None: ...

class FileEnd(_message.Message):
    __slots__ = ("index", "final_rel_path", "bytes", "content_sha256")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    FINAL_REL_PATH_FIELD_NUMBER: _ClassVar[int]
    BYTES_FIELD_NUMBER: _ClassVar[int]
    CONTENT_SHA256_FIELD_NUMBER: _ClassVar[int]
    index: int
    final_rel_path: str
    bytes: int
    content_sha256: bytes
    def __init__(self, index: _Optional[int] = ..., final_rel_path: _Optional[str] = ..., bytes: _Optional[int] = ..., content_sha256: _Optional[bytes] = ...) -> None: ...

class ManifestEnd(_message.Message):
    __slots__ = ("manifest_sha256", "file_count")
    MANIFEST_SHA256_FIELD_NUMBER: _ClassVar[int]
    FILE_COUNT_FIELD_NUMBER: _ClassVar[int]
    manifest_sha256: bytes
    file_count: int
    def __init__(self, manifest_sha256: _Optional[bytes] = ..., file_count: _Optional[int] = ...) -> None: ...

class JobEnd(_message.Message):
    __slots__ = ("files", "bytes", "manifest_sha256")
    FILES_FIELD_NUMBER: _ClassVar[int]
    BYTES_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_SHA256_FIELD_NUMBER: _ClassVar[int]
    files: int
    bytes: int
    manifest_sha256: bytes
    def __init__(self, files: _Optional[int] = ..., bytes: _Optional[int] = ..., manifest_sha256: _Optional[bytes] = ...) -> None: ...

class RestoreError(_message.Message):
    __slots__ = ("code", "message")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: str
    message: str
    def __init__(self, code: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class CommitRestoreRequest(_message.Message):
    __slots__ = ("restore_request_item_id", "manifest_sha256", "committed_index", "durable_state", "lease_token")
    RESTORE_REQUEST_ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_SHA256_FIELD_NUMBER: _ClassVar[int]
    COMMITTED_INDEX_FIELD_NUMBER: _ClassVar[int]
    DURABLE_STATE_FIELD_NUMBER: _ClassVar[int]
    LEASE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    restore_request_item_id: int
    manifest_sha256: bytes
    committed_index: int
    durable_state: DurableState
    lease_token: LeaseToken
    def __init__(self, restore_request_item_id: _Optional[int] = ..., manifest_sha256: _Optional[bytes] = ..., committed_index: _Optional[int] = ..., durable_state: _Optional[_Union[DurableState, str]] = ..., lease_token: _Optional[_Union[LeaseToken, _Mapping]] = ...) -> None: ...

class CommitRestoreReply(_message.Message):
    __slots__ = ("restore_request_item_id", "status", "committed_index", "revealed")
    RESTORE_REQUEST_ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    COMMITTED_INDEX_FIELD_NUMBER: _ClassVar[int]
    REVEALED_FIELD_NUMBER: _ClassVar[int]
    restore_request_item_id: int
    status: str
    committed_index: int
    revealed: bool
    def __init__(self, restore_request_item_id: _Optional[int] = ..., status: _Optional[str] = ..., committed_index: _Optional[int] = ..., revealed: _Optional[bool] = ...) -> None: ...

class WatchRequest(_message.Message):
    __slots__ = ("device_id",)
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    device_id: str
    def __init__(self, device_id: _Optional[str] = ...) -> None: ...

class Assignment(_message.Message):
    __slots__ = ("restore_request_item_id", "restore_request_id", "manifest_sha256", "final_rel_path", "size", "artifactclass", "destination_id", "state")
    RESTORE_REQUEST_ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    RESTORE_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_SHA256_FIELD_NUMBER: _ClassVar[int]
    FINAL_REL_PATH_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    ARTIFACTCLASS_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    restore_request_item_id: int
    restore_request_id: str
    manifest_sha256: bytes
    final_rel_path: str
    size: int
    artifactclass: str
    destination_id: str
    state: str
    def __init__(self, restore_request_item_id: _Optional[int] = ..., restore_request_id: _Optional[str] = ..., manifest_sha256: _Optional[bytes] = ..., final_rel_path: _Optional[str] = ..., size: _Optional[int] = ..., artifactclass: _Optional[str] = ..., destination_id: _Optional[str] = ..., state: _Optional[str] = ...) -> None: ...
