import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OperationState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OPERATION_STATE_UNSPECIFIED: _ClassVar[OperationState]
    OPERATION_STATE_QUEUED: _ClassVar[OperationState]
    OPERATION_STATE_RUNNING: _ClassVar[OperationState]
    OPERATION_STATE_SUCCEEDED: _ClassVar[OperationState]
    OPERATION_STATE_FAILED: _ClassVar[OperationState]
    OPERATION_STATE_CANCELLED: _ClassVar[OperationState]
    OPERATION_STATE_UNKNOWN: _ClassVar[OperationState]

class CatalogUnitOriginKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_UNIT_ORIGIN_KIND_UNSPECIFIED: _ClassVar[CatalogUnitOriginKind]
    CATALOG_UNIT_ORIGIN_KIND_NATIVE_OBJECT: _ClassVar[CatalogUnitOriginKind]
    CATALOG_UNIT_ORIGIN_KIND_FOREIGN_ARCHIVE: _ClassVar[CatalogUnitOriginKind]

class CatalogScanConfidence(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_SCAN_CONFIDENCE_UNSPECIFIED: _ClassVar[CatalogScanConfidence]
    CATALOG_SCAN_CONFIDENCE_LOW: _ClassVar[CatalogScanConfidence]
    CATALOG_SCAN_CONFIDENCE_MEDIUM: _ClassVar[CatalogScanConfidence]
    CATALOG_SCAN_CONFIDENCE_HIGH: _ClassVar[CatalogScanConfidence]

class CatalogUnitOriginFilter(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_UNIT_ORIGIN_FILTER_UNSPECIFIED: _ClassVar[CatalogUnitOriginFilter]
    CATALOG_UNIT_ORIGIN_FILTER_NATIVE_OBJECTS: _ClassVar[CatalogUnitOriginFilter]
    CATALOG_UNIT_ORIGIN_FILTER_FOREIGN_ARCHIVES: _ClassVar[CatalogUnitOriginFilter]

class CatalogEntryKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_ENTRY_KIND_UNSPECIFIED: _ClassVar[CatalogEntryKind]
    CATALOG_ENTRY_KIND_REGULAR_FILE: _ClassVar[CatalogEntryKind]
    CATALOG_ENTRY_KIND_DIRECTORY: _ClassVar[CatalogEntryKind]
    CATALOG_ENTRY_KIND_SYMLINK: _ClassVar[CatalogEntryKind]
    CATALOG_ENTRY_KIND_HARDLINK: _ClassVar[CatalogEntryKind]
    CATALOG_ENTRY_KIND_SPECIAL: _ClassVar[CatalogEntryKind]

class CatalogEntryState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_ENTRY_STATE_UNSPECIFIED: _ClassVar[CatalogEntryState]
    CATALOG_ENTRY_STATE_COMPLETE: _ClassVar[CatalogEntryState]
    CATALOG_ENTRY_STATE_PARTIAL: _ClassVar[CatalogEntryState]
    CATALOG_ENTRY_STATE_DAMAGED: _ClassVar[CatalogEntryState]
    CATALOG_ENTRY_STATE_UNSUPPORTED: _ClassVar[CatalogEntryState]
    CATALOG_ENTRY_STATE_UNKNOWN: _ClassVar[CatalogEntryState]

class IntegrityBasis(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INTEGRITY_BASIS_UNSPECIFIED: _ClassVar[IntegrityBasis]
    INTEGRITY_BASIS_UNKNOWN: _ClassVar[IntegrityBasis]
    INTEGRITY_BASIS_CONTENT_HASH: _ClassVar[IntegrityBasis]
    INTEGRITY_BASIS_FORMAT_CHECKSUM: _ClassVar[IntegrityBasis]
    INTEGRITY_BASIS_PARITY_CONSISTENCY: _ClassVar[IntegrityBasis]

class ArchiveGapCause(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ARCHIVE_GAP_CAUSE_UNSPECIFIED: _ClassVar[ArchiveGapCause]
    ARCHIVE_GAP_CAUSE_UNRECOGNIZED_DATA: _ClassVar[ArchiveGapCause]
    ARCHIVE_GAP_CAUSE_READ_ERROR: _ClassVar[ArchiveGapCause]
    ARCHIVE_GAP_CAUSE_MISSING: _ClassVar[ArchiveGapCause]
    ARCHIVE_GAP_CAUSE_RESYNC: _ClassVar[ArchiveGapCause]
    ARCHIVE_GAP_CAUSE_UNSUPPORTED: _ClassVar[ArchiveGapCause]
OPERATION_STATE_UNSPECIFIED: OperationState
OPERATION_STATE_QUEUED: OperationState
OPERATION_STATE_RUNNING: OperationState
OPERATION_STATE_SUCCEEDED: OperationState
OPERATION_STATE_FAILED: OperationState
OPERATION_STATE_CANCELLED: OperationState
OPERATION_STATE_UNKNOWN: OperationState
CATALOG_UNIT_ORIGIN_KIND_UNSPECIFIED: CatalogUnitOriginKind
CATALOG_UNIT_ORIGIN_KIND_NATIVE_OBJECT: CatalogUnitOriginKind
CATALOG_UNIT_ORIGIN_KIND_FOREIGN_ARCHIVE: CatalogUnitOriginKind
CATALOG_SCAN_CONFIDENCE_UNSPECIFIED: CatalogScanConfidence
CATALOG_SCAN_CONFIDENCE_LOW: CatalogScanConfidence
CATALOG_SCAN_CONFIDENCE_MEDIUM: CatalogScanConfidence
CATALOG_SCAN_CONFIDENCE_HIGH: CatalogScanConfidence
CATALOG_UNIT_ORIGIN_FILTER_UNSPECIFIED: CatalogUnitOriginFilter
CATALOG_UNIT_ORIGIN_FILTER_NATIVE_OBJECTS: CatalogUnitOriginFilter
CATALOG_UNIT_ORIGIN_FILTER_FOREIGN_ARCHIVES: CatalogUnitOriginFilter
CATALOG_ENTRY_KIND_UNSPECIFIED: CatalogEntryKind
CATALOG_ENTRY_KIND_REGULAR_FILE: CatalogEntryKind
CATALOG_ENTRY_KIND_DIRECTORY: CatalogEntryKind
CATALOG_ENTRY_KIND_SYMLINK: CatalogEntryKind
CATALOG_ENTRY_KIND_HARDLINK: CatalogEntryKind
CATALOG_ENTRY_KIND_SPECIAL: CatalogEntryKind
CATALOG_ENTRY_STATE_UNSPECIFIED: CatalogEntryState
CATALOG_ENTRY_STATE_COMPLETE: CatalogEntryState
CATALOG_ENTRY_STATE_PARTIAL: CatalogEntryState
CATALOG_ENTRY_STATE_DAMAGED: CatalogEntryState
CATALOG_ENTRY_STATE_UNSUPPORTED: CatalogEntryState
CATALOG_ENTRY_STATE_UNKNOWN: CatalogEntryState
INTEGRITY_BASIS_UNSPECIFIED: IntegrityBasis
INTEGRITY_BASIS_UNKNOWN: IntegrityBasis
INTEGRITY_BASIS_CONTENT_HASH: IntegrityBasis
INTEGRITY_BASIS_FORMAT_CHECKSUM: IntegrityBasis
INTEGRITY_BASIS_PARITY_CONSISTENCY: IntegrityBasis
ARCHIVE_GAP_CAUSE_UNSPECIFIED: ArchiveGapCause
ARCHIVE_GAP_CAUSE_UNRECOGNIZED_DATA: ArchiveGapCause
ARCHIVE_GAP_CAUSE_READ_ERROR: ArchiveGapCause
ARCHIVE_GAP_CAUSE_MISSING: ArchiveGapCause
ARCHIVE_GAP_CAUSE_RESYNC: ArchiveGapCause
ARCHIVE_GAP_CAUSE_UNSUPPORTED: ArchiveGapCause

class IdempotencyKey(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class OperationRef(_message.Message):
    __slots__ = ("operation_id",)
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    operation_id: bytes
    def __init__(self, operation_id: _Optional[bytes] = ...) -> None: ...

class OperationStatus(_message.Message):
    __slots__ = ("operation_id", "operation_kind", "state", "created_at", "updated_at", "progress", "error_summary")
    class ProgressEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATION_KIND_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    PROGRESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_SUMMARY_FIELD_NUMBER: _ClassVar[int]
    operation_id: bytes
    operation_kind: str
    state: OperationState
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    progress: _containers.ScalarMap[str, str]
    error_summary: str
    def __init__(self, operation_id: _Optional[bytes] = ..., operation_kind: _Optional[str] = ..., state: _Optional[_Union[OperationState, str]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., progress: _Optional[_Mapping[str, str]] = ..., error_summary: _Optional[str] = ...) -> None: ...

class PageToken(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class HealthResponse(_message.Message):
    __slots__ = ("status", "components", "detail")
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STATUS_UNSPECIFIED: _ClassVar[HealthResponse.Status]
        STATUS_HEALTHY: _ClassVar[HealthResponse.Status]
        STATUS_READ_ONLY: _ClassVar[HealthResponse.Status]
        STATUS_DEGRADED: _ClassVar[HealthResponse.Status]
        STATUS_FAILED: _ClassVar[HealthResponse.Status]
    STATUS_UNSPECIFIED: HealthResponse.Status
    STATUS_HEALTHY: HealthResponse.Status
    STATUS_READ_ONLY: HealthResponse.Status
    STATUS_DEGRADED: HealthResponse.Status
    STATUS_FAILED: HealthResponse.Status
    class ComponentsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    STATUS_FIELD_NUMBER: _ClassVar[int]
    COMPONENTS_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    status: HealthResponse.Status
    components: _containers.ScalarMap[str, str]
    detail: str
    def __init__(self, status: _Optional[_Union[HealthResponse.Status, str]] = ..., components: _Optional[_Mapping[str, str]] = ..., detail: _Optional[str] = ...) -> None: ...

class VersionResponse(_message.Message):
    __slots__ = ("daemon_version", "api_version", "rust_target")
    DAEMON_VERSION_FIELD_NUMBER: _ClassVar[int]
    API_VERSION_FIELD_NUMBER: _ClassVar[int]
    RUST_TARGET_FIELD_NUMBER: _ClassVar[int]
    daemon_version: str
    api_version: str
    rust_target: str
    def __init__(self, daemon_version: _Optional[str] = ..., api_version: _Optional[str] = ..., rust_target: _Optional[str] = ...) -> None: ...

class GetOperationRequest(_message.Message):
    __slots__ = ("operation_id",)
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    operation_id: bytes
    def __init__(self, operation_id: _Optional[bytes] = ...) -> None: ...

class ListOperationsRequest(_message.Message):
    __slots__ = ("filter", "page_token", "page_size")
    class FilterEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    FILTER_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    filter: _containers.ScalarMap[str, str]
    page_token: PageToken
    page_size: int
    def __init__(self, filter: _Optional[_Mapping[str, str]] = ..., page_token: _Optional[_Union[PageToken, _Mapping]] = ..., page_size: _Optional[int] = ...) -> None: ...

class ListOperationsResponse(_message.Message):
    __slots__ = ("operations", "next_page_token")
    OPERATIONS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    operations: _containers.RepeatedCompositeFieldContainer[OperationStatus]
    next_page_token: PageToken
    def __init__(self, operations: _Optional[_Iterable[_Union[OperationStatus, _Mapping]]] = ..., next_page_token: _Optional[_Union[PageToken, _Mapping]] = ...) -> None: ...

class CancelOperationRequest(_message.Message):
    __slots__ = ("operation_id", "idempotency_key", "force")
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    FORCE_FIELD_NUMBER: _ClassVar[int]
    operation_id: bytes
    idempotency_key: IdempotencyKey
    force: bool
    def __init__(self, operation_id: _Optional[bytes] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ..., force: _Optional[bool] = ...) -> None: ...

class CancelOperationResponse(_message.Message):
    __slots__ = ("resulting_state", "detail")
    RESULTING_STATE_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    resulting_state: OperationState
    detail: str
    def __init__(self, resulting_state: _Optional[_Union[OperationState, str]] = ..., detail: _Optional[str] = ...) -> None: ...

class Library(_message.Message):
    __slots__ = ("library_serial", "vendor", "product", "product_revision", "library_uuid")
    LIBRARY_SERIAL_FIELD_NUMBER: _ClassVar[int]
    VENDOR_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_REVISION_FIELD_NUMBER: _ClassVar[int]
    LIBRARY_UUID_FIELD_NUMBER: _ClassVar[int]
    library_serial: str
    vendor: str
    product: str
    product_revision: str
    library_uuid: bytes
    def __init__(self, library_serial: _Optional[str] = ..., vendor: _Optional[str] = ..., product: _Optional[str] = ..., product_revision: _Optional[str] = ..., library_uuid: _Optional[bytes] = ...) -> None: ...

class LibraryState(_message.Message):
    __slots__ = ("library", "drives", "slots", "import_export_ports", "last_inventory_at", "managed")
    LIBRARY_FIELD_NUMBER: _ClassVar[int]
    DRIVES_FIELD_NUMBER: _ClassVar[int]
    SLOTS_FIELD_NUMBER: _ClassVar[int]
    IMPORT_EXPORT_PORTS_FIELD_NUMBER: _ClassVar[int]
    LAST_INVENTORY_AT_FIELD_NUMBER: _ClassVar[int]
    MANAGED_FIELD_NUMBER: _ClassVar[int]
    library: Library
    drives: _containers.RepeatedCompositeFieldContainer[Drive]
    slots: _containers.RepeatedCompositeFieldContainer[Slot]
    import_export_ports: _containers.RepeatedCompositeFieldContainer[PortalSlot]
    last_inventory_at: _timestamp_pb2.Timestamp
    managed: str
    def __init__(self, library: _Optional[_Union[Library, _Mapping]] = ..., drives: _Optional[_Iterable[_Union[Drive, _Mapping]]] = ..., slots: _Optional[_Iterable[_Union[Slot, _Mapping]]] = ..., import_export_ports: _Optional[_Iterable[_Union[PortalSlot, _Mapping]]] = ..., last_inventory_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., managed: _Optional[str] = ...) -> None: ...

class Drive(_message.Message):
    __slots__ = ("element_address", "drive_serial", "host_device_path", "vendor", "product", "loaded_tape_uuid", "status", "drive_uuid", "cleaning_due", "fenced", "lifetime_read_bytes", "lifetime_write_bytes", "counter_epoch", "session_id", "active_alert_names")
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DRIVE_STATUS_UNSPECIFIED: _ClassVar[Drive.Status]
        DRIVE_STATUS_IDLE: _ClassVar[Drive.Status]
        DRIVE_STATUS_LOADED: _ClassVar[Drive.Status]
        DRIVE_STATUS_BUSY: _ClassVar[Drive.Status]
        DRIVE_STATUS_UNREACHABLE: _ClassVar[Drive.Status]
        DRIVE_STATUS_CLEANING: _ClassVar[Drive.Status]
        DRIVE_STATUS_FENCED: _ClassVar[Drive.Status]
    DRIVE_STATUS_UNSPECIFIED: Drive.Status
    DRIVE_STATUS_IDLE: Drive.Status
    DRIVE_STATUS_LOADED: Drive.Status
    DRIVE_STATUS_BUSY: Drive.Status
    DRIVE_STATUS_UNREACHABLE: Drive.Status
    DRIVE_STATUS_CLEANING: Drive.Status
    DRIVE_STATUS_FENCED: Drive.Status
    ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    DRIVE_SERIAL_FIELD_NUMBER: _ClassVar[int]
    HOST_DEVICE_PATH_FIELD_NUMBER: _ClassVar[int]
    VENDOR_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_FIELD_NUMBER: _ClassVar[int]
    LOADED_TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    DRIVE_UUID_FIELD_NUMBER: _ClassVar[int]
    CLEANING_DUE_FIELD_NUMBER: _ClassVar[int]
    FENCED_FIELD_NUMBER: _ClassVar[int]
    LIFETIME_READ_BYTES_FIELD_NUMBER: _ClassVar[int]
    LIFETIME_WRITE_BYTES_FIELD_NUMBER: _ClassVar[int]
    COUNTER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_ALERT_NAMES_FIELD_NUMBER: _ClassVar[int]
    element_address: int
    drive_serial: str
    host_device_path: str
    vendor: str
    product: str
    loaded_tape_uuid: bytes
    status: Drive.Status
    drive_uuid: bytes
    cleaning_due: str
    fenced: bool
    lifetime_read_bytes: int
    lifetime_write_bytes: int
    counter_epoch: int
    session_id: bytes
    active_alert_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, element_address: _Optional[int] = ..., drive_serial: _Optional[str] = ..., host_device_path: _Optional[str] = ..., vendor: _Optional[str] = ..., product: _Optional[str] = ..., loaded_tape_uuid: _Optional[bytes] = ..., status: _Optional[_Union[Drive.Status, str]] = ..., drive_uuid: _Optional[bytes] = ..., cleaning_due: _Optional[str] = ..., fenced: _Optional[bool] = ..., lifetime_read_bytes: _Optional[int] = ..., lifetime_write_bytes: _Optional[int] = ..., counter_epoch: _Optional[int] = ..., session_id: _Optional[bytes] = ..., active_alert_names: _Optional[_Iterable[str]] = ...) -> None: ...

class DriveCatalogEntry(_message.Message):
    __slots__ = ("drive_uuid", "serial", "identity_source", "actionable", "vendor", "product", "firmware_rev", "managed", "state", "cleaning_due", "fenced", "first_seen_utc", "last_seen_utc", "last_library_serial", "last_element_address", "purchase_date", "warranty_until", "cost", "notes", "retired_at_utc", "retire_reason", "correlation_rollups")
    DRIVE_UUID_FIELD_NUMBER: _ClassVar[int]
    SERIAL_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_SOURCE_FIELD_NUMBER: _ClassVar[int]
    ACTIONABLE_FIELD_NUMBER: _ClassVar[int]
    VENDOR_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_FIELD_NUMBER: _ClassVar[int]
    FIRMWARE_REV_FIELD_NUMBER: _ClassVar[int]
    MANAGED_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CLEANING_DUE_FIELD_NUMBER: _ClassVar[int]
    FENCED_FIELD_NUMBER: _ClassVar[int]
    FIRST_SEEN_UTC_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_UTC_FIELD_NUMBER: _ClassVar[int]
    LAST_LIBRARY_SERIAL_FIELD_NUMBER: _ClassVar[int]
    LAST_ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    PURCHASE_DATE_FIELD_NUMBER: _ClassVar[int]
    WARRANTY_UNTIL_FIELD_NUMBER: _ClassVar[int]
    COST_FIELD_NUMBER: _ClassVar[int]
    NOTES_FIELD_NUMBER: _ClassVar[int]
    RETIRED_AT_UTC_FIELD_NUMBER: _ClassVar[int]
    RETIRE_REASON_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ROLLUPS_FIELD_NUMBER: _ClassVar[int]
    drive_uuid: bytes
    serial: str
    identity_source: str
    actionable: bool
    vendor: str
    product: str
    firmware_rev: str
    managed: str
    state: str
    cleaning_due: str
    fenced: bool
    first_seen_utc: _timestamp_pb2.Timestamp
    last_seen_utc: _timestamp_pb2.Timestamp
    last_library_serial: str
    last_element_address: int
    purchase_date: str
    warranty_until: str
    cost: str
    notes: str
    retired_at_utc: _timestamp_pb2.Timestamp
    retire_reason: str
    correlation_rollups: _containers.RepeatedCompositeFieldContainer[DriveCorrelationRollup]
    def __init__(self, drive_uuid: _Optional[bytes] = ..., serial: _Optional[str] = ..., identity_source: _Optional[str] = ..., actionable: _Optional[bool] = ..., vendor: _Optional[str] = ..., product: _Optional[str] = ..., firmware_rev: _Optional[str] = ..., managed: _Optional[str] = ..., state: _Optional[str] = ..., cleaning_due: _Optional[str] = ..., fenced: _Optional[bool] = ..., first_seen_utc: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_seen_utc: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_library_serial: _Optional[str] = ..., last_element_address: _Optional[int] = ..., purchase_date: _Optional[str] = ..., warranty_until: _Optional[str] = ..., cost: _Optional[str] = ..., notes: _Optional[str] = ..., retired_at_utc: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., retire_reason: _Optional[str] = ..., correlation_rollups: _Optional[_Iterable[_Union[DriveCorrelationRollup, _Mapping]]] = ...) -> None: ...

class DriveCorrelationRollup(_message.Message):
    __slots__ = ("tape_uuid", "voltag", "drive_uuid", "drive_serial", "session_count", "snapshot_count", "write_errors_corrected", "write_errors_uncorrected", "read_errors_corrected", "read_errors_uncorrected", "first_session_utc", "last_session_utc")
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    VOLTAG_FIELD_NUMBER: _ClassVar[int]
    DRIVE_UUID_FIELD_NUMBER: _ClassVar[int]
    DRIVE_SERIAL_FIELD_NUMBER: _ClassVar[int]
    SESSION_COUNT_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_COUNT_FIELD_NUMBER: _ClassVar[int]
    WRITE_ERRORS_CORRECTED_FIELD_NUMBER: _ClassVar[int]
    WRITE_ERRORS_UNCORRECTED_FIELD_NUMBER: _ClassVar[int]
    READ_ERRORS_CORRECTED_FIELD_NUMBER: _ClassVar[int]
    READ_ERRORS_UNCORRECTED_FIELD_NUMBER: _ClassVar[int]
    FIRST_SESSION_UTC_FIELD_NUMBER: _ClassVar[int]
    LAST_SESSION_UTC_FIELD_NUMBER: _ClassVar[int]
    tape_uuid: bytes
    voltag: str
    drive_uuid: bytes
    drive_serial: str
    session_count: int
    snapshot_count: int
    write_errors_corrected: int
    write_errors_uncorrected: int
    read_errors_corrected: int
    read_errors_uncorrected: int
    first_session_utc: _timestamp_pb2.Timestamp
    last_session_utc: _timestamp_pb2.Timestamp
    def __init__(self, tape_uuid: _Optional[bytes] = ..., voltag: _Optional[str] = ..., drive_uuid: _Optional[bytes] = ..., drive_serial: _Optional[str] = ..., session_count: _Optional[int] = ..., snapshot_count: _Optional[int] = ..., write_errors_corrected: _Optional[int] = ..., write_errors_uncorrected: _Optional[int] = ..., read_errors_corrected: _Optional[int] = ..., read_errors_uncorrected: _Optional[int] = ..., first_session_utc: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_session_utc: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class DriveHistoryEvent(_message.Message):
    __slots__ = ("event_id", "drive_uuid", "event_kind", "at_utc", "library_serial", "element_address", "tape_uuid", "detail")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    DRIVE_UUID_FIELD_NUMBER: _ClassVar[int]
    EVENT_KIND_FIELD_NUMBER: _ClassVar[int]
    AT_UTC_FIELD_NUMBER: _ClassVar[int]
    LIBRARY_SERIAL_FIELD_NUMBER: _ClassVar[int]
    ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    drive_uuid: bytes
    event_kind: str
    at_utc: _timestamp_pb2.Timestamp
    library_serial: str
    element_address: int
    tape_uuid: bytes
    detail: str
    def __init__(self, event_id: _Optional[int] = ..., drive_uuid: _Optional[bytes] = ..., event_kind: _Optional[str] = ..., at_utc: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., library_serial: _Optional[str] = ..., element_address: _Optional[int] = ..., tape_uuid: _Optional[bytes] = ..., detail: _Optional[str] = ...) -> None: ...

class DriveHealthSnapshot(_message.Message):
    __slots__ = ("snapshot_id", "drive_uuid", "at_utc", "trigger", "session_id", "tape_alert_flags", "write_errors_corrected", "write_errors_uncorrected", "read_errors_corrected", "read_errors_uncorrected", "raw_pages")
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    DRIVE_UUID_FIELD_NUMBER: _ClassVar[int]
    AT_UTC_FIELD_NUMBER: _ClassVar[int]
    TRIGGER_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    TAPE_ALERT_FLAGS_FIELD_NUMBER: _ClassVar[int]
    WRITE_ERRORS_CORRECTED_FIELD_NUMBER: _ClassVar[int]
    WRITE_ERRORS_UNCORRECTED_FIELD_NUMBER: _ClassVar[int]
    READ_ERRORS_CORRECTED_FIELD_NUMBER: _ClassVar[int]
    READ_ERRORS_UNCORRECTED_FIELD_NUMBER: _ClassVar[int]
    RAW_PAGES_FIELD_NUMBER: _ClassVar[int]
    snapshot_id: int
    drive_uuid: bytes
    at_utc: _timestamp_pb2.Timestamp
    trigger: str
    session_id: str
    tape_alert_flags: str
    write_errors_corrected: int
    write_errors_uncorrected: int
    read_errors_corrected: int
    read_errors_uncorrected: int
    raw_pages: str
    def __init__(self, snapshot_id: _Optional[int] = ..., drive_uuid: _Optional[bytes] = ..., at_utc: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., trigger: _Optional[str] = ..., session_id: _Optional[str] = ..., tape_alert_flags: _Optional[str] = ..., write_errors_corrected: _Optional[int] = ..., write_errors_uncorrected: _Optional[int] = ..., read_errors_corrected: _Optional[int] = ..., read_errors_uncorrected: _Optional[int] = ..., raw_pages: _Optional[str] = ...) -> None: ...

class Alarm(_message.Message):
    __slots__ = ("alarm_id", "condition_key", "kind", "severity", "state", "first_seen_utc", "last_seen_utc", "acked_by", "acked_at_utc", "detail")
    ALARM_ID_FIELD_NUMBER: _ClassVar[int]
    CONDITION_KEY_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    FIRST_SEEN_UTC_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_UTC_FIELD_NUMBER: _ClassVar[int]
    ACKED_BY_FIELD_NUMBER: _ClassVar[int]
    ACKED_AT_UTC_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    alarm_id: int
    condition_key: str
    kind: str
    severity: str
    state: str
    first_seen_utc: _timestamp_pb2.Timestamp
    last_seen_utc: _timestamp_pb2.Timestamp
    acked_by: str
    acked_at_utc: _timestamp_pb2.Timestamp
    detail: str
    def __init__(self, alarm_id: _Optional[int] = ..., condition_key: _Optional[str] = ..., kind: _Optional[str] = ..., severity: _Optional[str] = ..., state: _Optional[str] = ..., first_seen_utc: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_seen_utc: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., acked_by: _Optional[str] = ..., acked_at_utc: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., detail: _Optional[str] = ...) -> None: ...

class ListDrivesRequest(_message.Message):
    __slots__ = ("include_foreign", "include_retired", "page_token", "page_size")
    INCLUDE_FOREIGN_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_RETIRED_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    include_foreign: bool
    include_retired: bool
    page_token: PageToken
    page_size: int
    def __init__(self, include_foreign: _Optional[bool] = ..., include_retired: _Optional[bool] = ..., page_token: _Optional[_Union[PageToken, _Mapping]] = ..., page_size: _Optional[int] = ...) -> None: ...

class ListDrivesResponse(_message.Message):
    __slots__ = ("drives", "next_page_token")
    DRIVES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    drives: _containers.RepeatedCompositeFieldContainer[DriveCatalogEntry]
    next_page_token: PageToken
    def __init__(self, drives: _Optional[_Iterable[_Union[DriveCatalogEntry, _Mapping]]] = ..., next_page_token: _Optional[_Union[PageToken, _Mapping]] = ...) -> None: ...

class GetDriveRequest(_message.Message):
    __slots__ = ("drive",)
    DRIVE_FIELD_NUMBER: _ClassVar[int]
    drive: str
    def __init__(self, drive: _Optional[str] = ...) -> None: ...

class GetDriveHistoryRequest(_message.Message):
    __slots__ = ("drive", "include_events", "include_snapshots", "page_token", "page_size")
    DRIVE_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_EVENTS_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_SNAPSHOTS_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    drive: str
    include_events: bool
    include_snapshots: bool
    page_token: PageToken
    page_size: int
    def __init__(self, drive: _Optional[str] = ..., include_events: _Optional[bool] = ..., include_snapshots: _Optional[bool] = ..., page_token: _Optional[_Union[PageToken, _Mapping]] = ..., page_size: _Optional[int] = ...) -> None: ...

class GetDriveHistoryResponse(_message.Message):
    __slots__ = ("drive", "events", "snapshots", "next_page_token")
    DRIVE_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    drive: DriveCatalogEntry
    events: _containers.RepeatedCompositeFieldContainer[DriveHistoryEvent]
    snapshots: _containers.RepeatedCompositeFieldContainer[DriveHealthSnapshot]
    next_page_token: PageToken
    def __init__(self, drive: _Optional[_Union[DriveCatalogEntry, _Mapping]] = ..., events: _Optional[_Iterable[_Union[DriveHistoryEvent, _Mapping]]] = ..., snapshots: _Optional[_Iterable[_Union[DriveHealthSnapshot, _Mapping]]] = ..., next_page_token: _Optional[_Union[PageToken, _Mapping]] = ...) -> None: ...

class AnnotateDriveRequest(_message.Message):
    __slots__ = ("drive_uuid", "purchase_date", "warranty_until", "cost", "note", "notes_set", "allow_derived_identity")
    DRIVE_UUID_FIELD_NUMBER: _ClassVar[int]
    PURCHASE_DATE_FIELD_NUMBER: _ClassVar[int]
    WARRANTY_UNTIL_FIELD_NUMBER: _ClassVar[int]
    COST_FIELD_NUMBER: _ClassVar[int]
    NOTE_FIELD_NUMBER: _ClassVar[int]
    NOTES_SET_FIELD_NUMBER: _ClassVar[int]
    ALLOW_DERIVED_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    drive_uuid: bytes
    purchase_date: str
    warranty_until: str
    cost: str
    note: str
    notes_set: str
    allow_derived_identity: bool
    def __init__(self, drive_uuid: _Optional[bytes] = ..., purchase_date: _Optional[str] = ..., warranty_until: _Optional[str] = ..., cost: _Optional[str] = ..., note: _Optional[str] = ..., notes_set: _Optional[str] = ..., allow_derived_identity: _Optional[bool] = ...) -> None: ...

class RetireDriveRequest(_message.Message):
    __slots__ = ("drive_uuid", "reason", "i_understand_fleet_removal_is_permanent", "allow_derived_identity")
    DRIVE_UUID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    I_UNDERSTAND_FLEET_REMOVAL_IS_PERMANENT_FIELD_NUMBER: _ClassVar[int]
    ALLOW_DERIVED_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    drive_uuid: bytes
    reason: str
    i_understand_fleet_removal_is_permanent: bool
    allow_derived_identity: bool
    def __init__(self, drive_uuid: _Optional[bytes] = ..., reason: _Optional[str] = ..., i_understand_fleet_removal_is_permanent: _Optional[bool] = ..., allow_derived_identity: _Optional[bool] = ...) -> None: ...

class RetireDriveResponse(_message.Message):
    __slots__ = ("drive", "newly_retired")
    DRIVE_FIELD_NUMBER: _ClassVar[int]
    NEWLY_RETIRED_FIELD_NUMBER: _ClassVar[int]
    drive: DriveCatalogEntry
    newly_retired: bool
    def __init__(self, drive: _Optional[_Union[DriveCatalogEntry, _Mapping]] = ..., newly_retired: _Optional[bool] = ...) -> None: ...

class PollDriveRequest(_message.Message):
    __slots__ = ("drive", "allow_derived_identity")
    DRIVE_FIELD_NUMBER: _ClassVar[int]
    ALLOW_DERIVED_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    drive: str
    allow_derived_identity: bool
    def __init__(self, drive: _Optional[str] = ..., allow_derived_identity: _Optional[bool] = ...) -> None: ...

class CleanDriveRequest(_message.Message):
    __slots__ = ("drive_uuid", "allow_derived_identity", "idempotency_key")
    DRIVE_UUID_FIELD_NUMBER: _ClassVar[int]
    ALLOW_DERIVED_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    drive_uuid: bytes
    allow_derived_identity: bool
    idempotency_key: IdempotencyKey
    def __init__(self, drive_uuid: _Optional[bytes] = ..., allow_derived_identity: _Optional[bool] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ...) -> None: ...

class ListAlarmsRequest(_message.Message):
    __slots__ = ("include_cleared", "page_token", "page_size")
    INCLUDE_CLEARED_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    include_cleared: bool
    page_token: PageToken
    page_size: int
    def __init__(self, include_cleared: _Optional[bool] = ..., page_token: _Optional[_Union[PageToken, _Mapping]] = ..., page_size: _Optional[int] = ...) -> None: ...

class ListAlarmsResponse(_message.Message):
    __slots__ = ("alarms", "next_page_token")
    ALARMS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    alarms: _containers.RepeatedCompositeFieldContainer[Alarm]
    next_page_token: PageToken
    def __init__(self, alarms: _Optional[_Iterable[_Union[Alarm, _Mapping]]] = ..., next_page_token: _Optional[_Union[PageToken, _Mapping]] = ...) -> None: ...

class AckAlarmRequest(_message.Message):
    __slots__ = ("condition_key", "idempotency_key")
    CONDITION_KEY_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    condition_key: str
    idempotency_key: IdempotencyKey
    def __init__(self, condition_key: _Optional[str] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ...) -> None: ...

class GetLiveStatusRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetLiveStatusResponse(_message.Message):
    __slots__ = ("libraries", "operations", "alarms", "snapshot_at_utc", "daemon_epoch")
    LIBRARIES_FIELD_NUMBER: _ClassVar[int]
    OPERATIONS_FIELD_NUMBER: _ClassVar[int]
    ALARMS_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_AT_UTC_FIELD_NUMBER: _ClassVar[int]
    DAEMON_EPOCH_FIELD_NUMBER: _ClassVar[int]
    libraries: _containers.RepeatedCompositeFieldContainer[LibraryState]
    operations: _containers.RepeatedCompositeFieldContainer[OperationRef]
    alarms: _containers.RepeatedCompositeFieldContainer[Alarm]
    snapshot_at_utc: str
    daemon_epoch: int
    def __init__(self, libraries: _Optional[_Iterable[_Union[LibraryState, _Mapping]]] = ..., operations: _Optional[_Iterable[_Union[OperationRef, _Mapping]]] = ..., alarms: _Optional[_Iterable[_Union[Alarm, _Mapping]]] = ..., snapshot_at_utc: _Optional[str] = ..., daemon_epoch: _Optional[int] = ...) -> None: ...

class Slot(_message.Message):
    __slots__ = ("element_address", "voltag", "tape_uuid")
    ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    VOLTAG_FIELD_NUMBER: _ClassVar[int]
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    element_address: int
    voltag: str
    tape_uuid: bytes
    def __init__(self, element_address: _Optional[int] = ..., voltag: _Optional[str] = ..., tape_uuid: _Optional[bytes] = ...) -> None: ...

class PortalSlot(_message.Message):
    __slots__ = ("element_address", "voltag", "tape_uuid", "last_direction")
    class Direction(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        PORTAL_DIRECTION_UNSPECIFIED: _ClassVar[PortalSlot.Direction]
        PORTAL_DIRECTION_IMPORT: _ClassVar[PortalSlot.Direction]
        PORTAL_DIRECTION_EXPORT: _ClassVar[PortalSlot.Direction]
    PORTAL_DIRECTION_UNSPECIFIED: PortalSlot.Direction
    PORTAL_DIRECTION_IMPORT: PortalSlot.Direction
    PORTAL_DIRECTION_EXPORT: PortalSlot.Direction
    ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    VOLTAG_FIELD_NUMBER: _ClassVar[int]
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    LAST_DIRECTION_FIELD_NUMBER: _ClassVar[int]
    element_address: int
    voltag: str
    tape_uuid: bytes
    last_direction: PortalSlot.Direction
    def __init__(self, element_address: _Optional[int] = ..., voltag: _Optional[str] = ..., tape_uuid: _Optional[bytes] = ..., last_direction: _Optional[_Union[PortalSlot.Direction, str]] = ...) -> None: ...

class ListLibrariesResponse(_message.Message):
    __slots__ = ("libraries",)
    LIBRARIES_FIELD_NUMBER: _ClassVar[int]
    libraries: _containers.RepeatedCompositeFieldContainer[Library]
    def __init__(self, libraries: _Optional[_Iterable[_Union[Library, _Mapping]]] = ...) -> None: ...

class GetLibraryRequest(_message.Message):
    __slots__ = ("library_uuid",)
    LIBRARY_UUID_FIELD_NUMBER: _ClassVar[int]
    library_uuid: bytes
    def __init__(self, library_uuid: _Optional[bytes] = ...) -> None: ...

class RefreshInventoryRequest(_message.Message):
    __slots__ = ("library_uuid", "idempotency_key")
    LIBRARY_UUID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    library_uuid: bytes
    idempotency_key: IdempotencyKey
    def __init__(self, library_uuid: _Optional[bytes] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ...) -> None: ...

class MoveMediumRequest(_message.Message):
    __slots__ = ("library_uuid", "source_element_address", "destination_element_address", "idempotency_key")
    LIBRARY_UUID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    library_uuid: bytes
    source_element_address: int
    destination_element_address: int
    idempotency_key: IdempotencyKey
    def __init__(self, library_uuid: _Optional[bytes] = ..., source_element_address: _Optional[int] = ..., destination_element_address: _Optional[int] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ...) -> None: ...

class LoadDriveRequest(_message.Message):
    __slots__ = ("library_uuid", "slot_element_address", "drive_element_address", "idempotency_key")
    LIBRARY_UUID_FIELD_NUMBER: _ClassVar[int]
    SLOT_ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    DRIVE_ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    library_uuid: bytes
    slot_element_address: int
    drive_element_address: int
    idempotency_key: IdempotencyKey
    def __init__(self, library_uuid: _Optional[bytes] = ..., slot_element_address: _Optional[int] = ..., drive_element_address: _Optional[int] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ...) -> None: ...

class UnloadDriveRequest(_message.Message):
    __slots__ = ("library_uuid", "drive_element_address", "destination_slot_address", "idempotency_key")
    LIBRARY_UUID_FIELD_NUMBER: _ClassVar[int]
    DRIVE_ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_SLOT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    library_uuid: bytes
    drive_element_address: int
    destination_slot_address: int
    idempotency_key: IdempotencyKey
    def __init__(self, library_uuid: _Optional[bytes] = ..., drive_element_address: _Optional[int] = ..., destination_slot_address: _Optional[int] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ...) -> None: ...

class ImportElementRequest(_message.Message):
    __slots__ = ("library_uuid", "portal_element_address", "destination_slot_address", "idempotency_key")
    LIBRARY_UUID_FIELD_NUMBER: _ClassVar[int]
    PORTAL_ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_SLOT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    library_uuid: bytes
    portal_element_address: int
    destination_slot_address: int
    idempotency_key: IdempotencyKey
    def __init__(self, library_uuid: _Optional[bytes] = ..., portal_element_address: _Optional[int] = ..., destination_slot_address: _Optional[int] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ...) -> None: ...

class ExportElementRequest(_message.Message):
    __slots__ = ("library_uuid", "source_slot_address", "portal_element_address", "idempotency_key")
    LIBRARY_UUID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_SLOT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    PORTAL_ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    library_uuid: bytes
    source_slot_address: int
    portal_element_address: int
    idempotency_key: IdempotencyKey
    def __init__(self, library_uuid: _Optional[bytes] = ..., source_slot_address: _Optional[int] = ..., portal_element_address: _Optional[int] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ...) -> None: ...

class StreamLibraryEventsRequest(_message.Message):
    __slots__ = ("library_uuid", "since")
    LIBRARY_UUID_FIELD_NUMBER: _ClassVar[int]
    SINCE_FIELD_NUMBER: _ClassVar[int]
    library_uuid: bytes
    since: _timestamp_pb2.Timestamp
    def __init__(self, library_uuid: _Optional[bytes] = ..., since: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class LibraryEvent(_message.Message):
    __slots__ = ("at", "kind", "detail")
    class Kind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        LIBRARY_EVENT_UNSPECIFIED: _ClassVar[LibraryEvent.Kind]
        LIBRARY_EVENT_INVENTORY_DELTA: _ClassVar[LibraryEvent.Kind]
        LIBRARY_EVENT_HOTPLUG: _ClassVar[LibraryEvent.Kind]
        LIBRARY_EVENT_DRIVE_STATE: _ClassVar[LibraryEvent.Kind]
        LIBRARY_EVENT_HARDWARE_WARNING: _ClassVar[LibraryEvent.Kind]
        LIBRARY_EVENT_UNREACHABLE: _ClassVar[LibraryEvent.Kind]
    LIBRARY_EVENT_UNSPECIFIED: LibraryEvent.Kind
    LIBRARY_EVENT_INVENTORY_DELTA: LibraryEvent.Kind
    LIBRARY_EVENT_HOTPLUG: LibraryEvent.Kind
    LIBRARY_EVENT_DRIVE_STATE: LibraryEvent.Kind
    LIBRARY_EVENT_HARDWARE_WARNING: LibraryEvent.Kind
    LIBRARY_EVENT_UNREACHABLE: LibraryEvent.Kind
    class DetailEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    AT_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    at: _timestamp_pb2.Timestamp
    kind: LibraryEvent.Kind
    detail: _containers.ScalarMap[str, str]
    def __init__(self, at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., kind: _Optional[_Union[LibraryEvent.Kind, str]] = ..., detail: _Optional[_Mapping[str, str]] = ...) -> None: ...

class Tape(_message.Message):
    __slots__ = ("tape_uuid", "voltag", "body_format", "block_size_bytes", "data_blocks_per_stripe", "parity_blocks_per_stripe", "stripes_per_neighborhood", "last_committed_tape_file", "state", "updated_at", "pool_id", "correlation_rollups")
    class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        TAPE_STATE_UNSPECIFIED: _ClassVar[Tape.State]
        TAPE_STATE_INVENTORIED: _ClassVar[Tape.State]
        TAPE_STATE_READY: _ClassVar[Tape.State]
        TAPE_STATE_DEGRADED: _ClassVar[Tape.State]
        TAPE_STATE_FAILED: _ClassVar[Tape.State]
        TAPE_STATE_SEALED: _ClassVar[Tape.State]
    TAPE_STATE_UNSPECIFIED: Tape.State
    TAPE_STATE_INVENTORIED: Tape.State
    TAPE_STATE_READY: Tape.State
    TAPE_STATE_DEGRADED: Tape.State
    TAPE_STATE_FAILED: Tape.State
    TAPE_STATE_SEALED: Tape.State
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    VOLTAG_FIELD_NUMBER: _ClassVar[int]
    BODY_FORMAT_FIELD_NUMBER: _ClassVar[int]
    BLOCK_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    DATA_BLOCKS_PER_STRIPE_FIELD_NUMBER: _ClassVar[int]
    PARITY_BLOCKS_PER_STRIPE_FIELD_NUMBER: _ClassVar[int]
    STRIPES_PER_NEIGHBORHOOD_FIELD_NUMBER: _ClassVar[int]
    LAST_COMMITTED_TAPE_FILE_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    POOL_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ROLLUPS_FIELD_NUMBER: _ClassVar[int]
    tape_uuid: bytes
    voltag: str
    body_format: str
    block_size_bytes: int
    data_blocks_per_stripe: int
    parity_blocks_per_stripe: int
    stripes_per_neighborhood: int
    last_committed_tape_file: int
    state: Tape.State
    updated_at: _timestamp_pb2.Timestamp
    pool_id: str
    correlation_rollups: _containers.RepeatedCompositeFieldContainer[DriveCorrelationRollup]
    def __init__(self, tape_uuid: _Optional[bytes] = ..., voltag: _Optional[str] = ..., body_format: _Optional[str] = ..., block_size_bytes: _Optional[int] = ..., data_blocks_per_stripe: _Optional[int] = ..., parity_blocks_per_stripe: _Optional[int] = ..., stripes_per_neighborhood: _Optional[int] = ..., last_committed_tape_file: _Optional[int] = ..., state: _Optional[_Union[Tape.State, str]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., pool_id: _Optional[str] = ..., correlation_rollups: _Optional[_Iterable[_Union[DriveCorrelationRollup, _Mapping]]] = ...) -> None: ...

class TapePool(_message.Message):
    __slots__ = ("pool_id", "display_name", "copy_class", "content_class")
    POOL_ID_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    COPY_CLASS_FIELD_NUMBER: _ClassVar[int]
    CONTENT_CLASS_FIELD_NUMBER: _ClassVar[int]
    pool_id: str
    display_name: str
    copy_class: str
    content_class: str
    def __init__(self, pool_id: _Optional[str] = ..., display_name: _Optional[str] = ..., copy_class: _Optional[str] = ..., content_class: _Optional[str] = ...) -> None: ...

class TapeFile(_message.Message):
    __slots__ = ("tape_uuid", "tape_file_number", "kind", "block_count", "object_id")
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    TAPE_FILE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    BLOCK_COUNT_FIELD_NUMBER: _ClassVar[int]
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    tape_uuid: bytes
    tape_file_number: int
    kind: str
    block_count: int
    object_id: bytes
    def __init__(self, tape_uuid: _Optional[bytes] = ..., tape_file_number: _Optional[int] = ..., kind: _Optional[str] = ..., block_count: _Optional[int] = ..., object_id: _Optional[bytes] = ...) -> None: ...

class ObjectRecord(_message.Message):
    __slots__ = ("object_id", "caller_object_id", "content_sha256", "logical_size_bytes", "body_format", "caller_metadata", "created_at", "copies")
    class CallerMetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CALLER_OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_SHA256_FIELD_NUMBER: _ClassVar[int]
    LOGICAL_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    BODY_FORMAT_FIELD_NUMBER: _ClassVar[int]
    CALLER_METADATA_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    COPIES_FIELD_NUMBER: _ClassVar[int]
    object_id: bytes
    caller_object_id: str
    content_sha256: bytes
    logical_size_bytes: int
    body_format: str
    caller_metadata: _containers.ScalarMap[str, str]
    created_at: _timestamp_pb2.Timestamp
    copies: _containers.RepeatedCompositeFieldContainer[ObjectCopy]
    def __init__(self, object_id: _Optional[bytes] = ..., caller_object_id: _Optional[str] = ..., content_sha256: _Optional[bytes] = ..., logical_size_bytes: _Optional[int] = ..., body_format: _Optional[str] = ..., caller_metadata: _Optional[_Mapping[str, str]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., copies: _Optional[_Iterable[_Union[ObjectCopy, _Mapping]]] = ...) -> None: ...

class ObjectCopy(_message.Message):
    __slots__ = ("tape_uuid", "tape_file_number", "first_body_lba", "last_verified_at", "health", "pool_id")
    class Health(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        OBJECT_COPY_HEALTH_UNSPECIFIED: _ClassVar[ObjectCopy.Health]
        OBJECT_COPY_HEALTH_OK: _ClassVar[ObjectCopy.Health]
        OBJECT_COPY_HEALTH_SUSPECT: _ClassVar[ObjectCopy.Health]
        OBJECT_COPY_HEALTH_DEGRADED: _ClassVar[ObjectCopy.Health]
        OBJECT_COPY_HEALTH_LOST: _ClassVar[ObjectCopy.Health]
    OBJECT_COPY_HEALTH_UNSPECIFIED: ObjectCopy.Health
    OBJECT_COPY_HEALTH_OK: ObjectCopy.Health
    OBJECT_COPY_HEALTH_SUSPECT: ObjectCopy.Health
    OBJECT_COPY_HEALTH_DEGRADED: ObjectCopy.Health
    OBJECT_COPY_HEALTH_LOST: ObjectCopy.Health
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    TAPE_FILE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    FIRST_BODY_LBA_FIELD_NUMBER: _ClassVar[int]
    LAST_VERIFIED_AT_FIELD_NUMBER: _ClassVar[int]
    HEALTH_FIELD_NUMBER: _ClassVar[int]
    POOL_ID_FIELD_NUMBER: _ClassVar[int]
    tape_uuid: bytes
    tape_file_number: int
    first_body_lba: int
    last_verified_at: _timestamp_pb2.Timestamp
    health: ObjectCopy.Health
    pool_id: str
    def __init__(self, tape_uuid: _Optional[bytes] = ..., tape_file_number: _Optional[int] = ..., first_body_lba: _Optional[int] = ..., last_verified_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., health: _Optional[_Union[ObjectCopy.Health, str]] = ..., pool_id: _Optional[str] = ...) -> None: ...

class FileRecord(_message.Message):
    __slots__ = ("object_id", "file_id", "path", "size_bytes", "file_sha256", "first_chunk_body_lba", "chunk_count")
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    FILE_SHA256_FIELD_NUMBER: _ClassVar[int]
    FIRST_CHUNK_BODY_LBA_FIELD_NUMBER: _ClassVar[int]
    CHUNK_COUNT_FIELD_NUMBER: _ClassVar[int]
    object_id: bytes
    file_id: bytes
    path: str
    size_bytes: int
    file_sha256: bytes
    first_chunk_body_lba: int
    chunk_count: int
    def __init__(self, object_id: _Optional[bytes] = ..., file_id: _Optional[bytes] = ..., path: _Optional[str] = ..., size_bytes: _Optional[int] = ..., file_sha256: _Optional[bytes] = ..., first_chunk_body_lba: _Optional[int] = ..., chunk_count: _Optional[int] = ...) -> None: ...

class ListTapesRequest(_message.Message):
    __slots__ = ("library_uuid", "page_token", "page_size", "pool_id")
    LIBRARY_UUID_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    POOL_ID_FIELD_NUMBER: _ClassVar[int]
    library_uuid: bytes
    page_token: PageToken
    page_size: int
    pool_id: str
    def __init__(self, library_uuid: _Optional[bytes] = ..., page_token: _Optional[_Union[PageToken, _Mapping]] = ..., page_size: _Optional[int] = ..., pool_id: _Optional[str] = ...) -> None: ...

class ListTapesResponse(_message.Message):
    __slots__ = ("tapes", "next_page_token")
    TAPES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tapes: _containers.RepeatedCompositeFieldContainer[Tape]
    next_page_token: PageToken
    def __init__(self, tapes: _Optional[_Iterable[_Union[Tape, _Mapping]]] = ..., next_page_token: _Optional[_Union[PageToken, _Mapping]] = ...) -> None: ...

class GetTapeRequest(_message.Message):
    __slots__ = ("tape_uuid",)
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    tape_uuid: bytes
    def __init__(self, tape_uuid: _Optional[bytes] = ...) -> None: ...

class ListTapeFilesRequest(_message.Message):
    __slots__ = ("tape_uuid", "page_token", "page_size")
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    tape_uuid: bytes
    page_token: PageToken
    page_size: int
    def __init__(self, tape_uuid: _Optional[bytes] = ..., page_token: _Optional[_Union[PageToken, _Mapping]] = ..., page_size: _Optional[int] = ...) -> None: ...

class ListTapeFilesResponse(_message.Message):
    __slots__ = ("tape_files", "next_page_token")
    TAPE_FILES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tape_files: _containers.RepeatedCompositeFieldContainer[TapeFile]
    next_page_token: PageToken
    def __init__(self, tape_files: _Optional[_Iterable[_Union[TapeFile, _Mapping]]] = ..., next_page_token: _Optional[_Union[PageToken, _Mapping]] = ...) -> None: ...

class ListTapePoolsRequest(_message.Message):
    __slots__ = ("page_token", "page_size")
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    page_token: PageToken
    page_size: int
    def __init__(self, page_token: _Optional[_Union[PageToken, _Mapping]] = ..., page_size: _Optional[int] = ...) -> None: ...

class ListTapePoolsResponse(_message.Message):
    __slots__ = ("pools", "next_page_token")
    POOLS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    pools: _containers.RepeatedCompositeFieldContainer[TapePool]
    next_page_token: PageToken
    def __init__(self, pools: _Optional[_Iterable[_Union[TapePool, _Mapping]]] = ..., next_page_token: _Optional[_Union[PageToken, _Mapping]] = ...) -> None: ...

class GetTapePoolRequest(_message.Message):
    __slots__ = ("pool_id",)
    POOL_ID_FIELD_NUMBER: _ClassVar[int]
    pool_id: str
    def __init__(self, pool_id: _Optional[str] = ...) -> None: ...

class EnumerateObjectsRequest(_message.Message):
    __slots__ = ("tape_uuid", "library_uuid", "all", "reconcile_from_tape")
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    LIBRARY_UUID_FIELD_NUMBER: _ClassVar[int]
    ALL_FIELD_NUMBER: _ClassVar[int]
    RECONCILE_FROM_TAPE_FIELD_NUMBER: _ClassVar[int]
    tape_uuid: bytes
    library_uuid: bytes
    all: _empty_pb2.Empty
    reconcile_from_tape: bool
    def __init__(self, tape_uuid: _Optional[bytes] = ..., library_uuid: _Optional[bytes] = ..., all: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ..., reconcile_from_tape: _Optional[bool] = ...) -> None: ...

class GetObjectRequest(_message.Message):
    __slots__ = ("object_id", "content_sha256", "caller_object_id")
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_SHA256_FIELD_NUMBER: _ClassVar[int]
    CALLER_OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    object_id: bytes
    content_sha256: bytes
    caller_object_id: str
    def __init__(self, object_id: _Optional[bytes] = ..., content_sha256: _Optional[bytes] = ..., caller_object_id: _Optional[str] = ...) -> None: ...

class FindObjectCopiesRequest(_message.Message):
    __slots__ = ("object_id", "content_sha256", "caller_object_id")
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_SHA256_FIELD_NUMBER: _ClassVar[int]
    CALLER_OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    object_id: bytes
    content_sha256: bytes
    caller_object_id: str
    def __init__(self, object_id: _Optional[bytes] = ..., content_sha256: _Optional[bytes] = ..., caller_object_id: _Optional[str] = ...) -> None: ...

class FindObjectCopiesResponse(_message.Message):
    __slots__ = ("object", "copies")
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    COPIES_FIELD_NUMBER: _ClassVar[int]
    object: ObjectRecord
    copies: _containers.RepeatedCompositeFieldContainer[ObjectCopy]
    def __init__(self, object: _Optional[_Union[ObjectRecord, _Mapping]] = ..., copies: _Optional[_Iterable[_Union[ObjectCopy, _Mapping]]] = ...) -> None: ...

class ReconcileTapeRequest(_message.Message):
    __slots__ = ("tape_uuid", "idempotency_key")
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    tape_uuid: bytes
    idempotency_key: IdempotencyKey
    def __init__(self, tape_uuid: _Optional[bytes] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ...) -> None: ...

class ListFilesInObjectRequest(_message.Message):
    __slots__ = ("object_id", "page_token", "page_size")
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    object_id: bytes
    page_token: PageToken
    page_size: int
    def __init__(self, object_id: _Optional[bytes] = ..., page_token: _Optional[_Union[PageToken, _Mapping]] = ..., page_size: _Optional[int] = ...) -> None: ...

class ListFilesInObjectResponse(_message.Message):
    __slots__ = ("files", "next_page_token")
    FILES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    files: _containers.RepeatedCompositeFieldContainer[FileRecord]
    next_page_token: PageToken
    def __init__(self, files: _Optional[_Iterable[_Union[FileRecord, _Mapping]]] = ..., next_page_token: _Optional[_Union[PageToken, _Mapping]] = ...) -> None: ...

class GetFileRequest(_message.Message):
    __slots__ = ("object_id", "file_id", "path")
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    object_id: bytes
    file_id: bytes
    path: str
    def __init__(self, object_id: _Optional[bytes] = ..., file_id: _Optional[bytes] = ..., path: _Optional[str] = ...) -> None: ...

class CatalogUnit(_message.Message):
    __slots__ = ("unit_id", "tape_uuid", "format_id", "origin_kind", "discovered_at", "native", "foreign")
    UNIT_ID_FIELD_NUMBER: _ClassVar[int]
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    FORMAT_ID_FIELD_NUMBER: _ClassVar[int]
    ORIGIN_KIND_FIELD_NUMBER: _ClassVar[int]
    DISCOVERED_AT_FIELD_NUMBER: _ClassVar[int]
    NATIVE_FIELD_NUMBER: _ClassVar[int]
    FOREIGN_FIELD_NUMBER: _ClassVar[int]
    unit_id: bytes
    tape_uuid: bytes
    format_id: str
    origin_kind: CatalogUnitOriginKind
    discovered_at: _timestamp_pb2.Timestamp
    native: NativeUnitSummary
    foreign: ForeignArchiveSummary
    def __init__(self, unit_id: _Optional[bytes] = ..., tape_uuid: _Optional[bytes] = ..., format_id: _Optional[str] = ..., origin_kind: _Optional[_Union[CatalogUnitOriginKind, str]] = ..., discovered_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., native: _Optional[_Union[NativeUnitSummary, _Mapping]] = ..., foreign: _Optional[_Union[ForeignArchiveSummary, _Mapping]] = ...) -> None: ...

class NativeUnitSummary(_message.Message):
    __slots__ = ("object_id",)
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    object_id: bytes
    def __init__(self, object_id: _Optional[bytes] = ...) -> None: ...

class ForeignArchiveSummary(_message.Message):
    __slots__ = ("scan_id", "source_kind", "source_id", "confidence", "last_scan_at", "entry_count", "damage_event_count")
    SCAN_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_KIND_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    LAST_SCAN_AT_FIELD_NUMBER: _ClassVar[int]
    ENTRY_COUNT_FIELD_NUMBER: _ClassVar[int]
    DAMAGE_EVENT_COUNT_FIELD_NUMBER: _ClassVar[int]
    scan_id: bytes
    source_kind: str
    source_id: str
    confidence: CatalogScanConfidence
    last_scan_at: _timestamp_pb2.Timestamp
    entry_count: int
    damage_event_count: int
    def __init__(self, scan_id: _Optional[bytes] = ..., source_kind: _Optional[str] = ..., source_id: _Optional[str] = ..., confidence: _Optional[_Union[CatalogScanConfidence, str]] = ..., last_scan_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., entry_count: _Optional[int] = ..., damage_event_count: _Optional[int] = ...) -> None: ...

class EnumerateUnitsRequest(_message.Message):
    __slots__ = ("tape_uuid", "library_uuid", "all", "origin_filter", "refresh_from_source")
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    LIBRARY_UUID_FIELD_NUMBER: _ClassVar[int]
    ALL_FIELD_NUMBER: _ClassVar[int]
    ORIGIN_FILTER_FIELD_NUMBER: _ClassVar[int]
    REFRESH_FROM_SOURCE_FIELD_NUMBER: _ClassVar[int]
    tape_uuid: bytes
    library_uuid: bytes
    all: _empty_pb2.Empty
    origin_filter: CatalogUnitOriginFilter
    refresh_from_source: bool
    def __init__(self, tape_uuid: _Optional[bytes] = ..., library_uuid: _Optional[bytes] = ..., all: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ..., origin_filter: _Optional[_Union[CatalogUnitOriginFilter, str]] = ..., refresh_from_source: _Optional[bool] = ...) -> None: ...

class GetCatalogUnitRequest(_message.Message):
    __slots__ = ("unit_id",)
    UNIT_ID_FIELD_NUMBER: _ClassVar[int]
    unit_id: bytes
    def __init__(self, unit_id: _Optional[bytes] = ...) -> None: ...

class CatalogEntry(_message.Message):
    __slots__ = ("unit_id", "entry_id", "path", "kind", "size_bytes", "mtime", "state", "integrity_basis")
    UNIT_ID_FIELD_NUMBER: _ClassVar[int]
    ENTRY_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    MTIME_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    INTEGRITY_BASIS_FIELD_NUMBER: _ClassVar[int]
    unit_id: bytes
    entry_id: bytes
    path: str
    kind: CatalogEntryKind
    size_bytes: int
    mtime: _timestamp_pb2.Timestamp
    state: CatalogEntryState
    integrity_basis: IntegrityBasis
    def __init__(self, unit_id: _Optional[bytes] = ..., entry_id: _Optional[bytes] = ..., path: _Optional[str] = ..., kind: _Optional[_Union[CatalogEntryKind, str]] = ..., size_bytes: _Optional[int] = ..., mtime: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., state: _Optional[_Union[CatalogEntryState, str]] = ..., integrity_basis: _Optional[_Union[IntegrityBasis, str]] = ...) -> None: ...

class ListEntriesInUnitRequest(_message.Message):
    __slots__ = ("unit_id", "page_token", "page_size", "refresh_from_source")
    UNIT_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    REFRESH_FROM_SOURCE_FIELD_NUMBER: _ClassVar[int]
    unit_id: bytes
    page_token: PageToken
    page_size: int
    refresh_from_source: bool
    def __init__(self, unit_id: _Optional[bytes] = ..., page_token: _Optional[_Union[PageToken, _Mapping]] = ..., page_size: _Optional[int] = ..., refresh_from_source: _Optional[bool] = ...) -> None: ...

class ListEntriesInUnitResponse(_message.Message):
    __slots__ = ("entries", "next_page_token", "archive_gaps")
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    ARCHIVE_GAPS_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[CatalogEntry]
    next_page_token: PageToken
    archive_gaps: _containers.RepeatedCompositeFieldContainer[ArchiveGap]
    def __init__(self, entries: _Optional[_Iterable[_Union[CatalogEntry, _Mapping]]] = ..., next_page_token: _Optional[_Union[PageToken, _Mapping]] = ..., archive_gaps: _Optional[_Iterable[_Union[ArchiveGap, _Mapping]]] = ...) -> None: ...

class ArchiveGap(_message.Message):
    __slots__ = ("unit_id", "source_start", "source_end", "cause")
    UNIT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_START_FIELD_NUMBER: _ClassVar[int]
    SOURCE_END_FIELD_NUMBER: _ClassVar[int]
    CAUSE_FIELD_NUMBER: _ClassVar[int]
    unit_id: bytes
    source_start: int
    source_end: int
    cause: ArchiveGapCause
    def __init__(self, unit_id: _Optional[bytes] = ..., source_start: _Optional[int] = ..., source_end: _Optional[int] = ..., cause: _Optional[_Union[ArchiveGapCause, str]] = ...) -> None: ...

class WriteSession(_message.Message):
    __slots__ = ("session_id", "tape_uuid", "drive_element_address", "body_format", "state", "objects_committed", "bytes_committed", "opened_at", "last_checkpoint_at", "target_kind", "tape_sequence", "current_tape_index")
    class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        WRITE_SESSION_STATE_UNSPECIFIED: _ClassVar[WriteSession.State]
        WRITE_SESSION_STATE_OPEN: _ClassVar[WriteSession.State]
        WRITE_SESSION_STATE_CHECKPOINTED: _ClassVar[WriteSession.State]
        WRITE_SESSION_STATE_CLOSED: _ClassVar[WriteSession.State]
        WRITE_SESSION_STATE_ABORTED: _ClassVar[WriteSession.State]
        WRITE_SESSION_STATE_ORPHANED: _ClassVar[WriteSession.State]
        WRITE_SESSION_STATE_LOST: _ClassVar[WriteSession.State]
    WRITE_SESSION_STATE_UNSPECIFIED: WriteSession.State
    WRITE_SESSION_STATE_OPEN: WriteSession.State
    WRITE_SESSION_STATE_CHECKPOINTED: WriteSession.State
    WRITE_SESSION_STATE_CLOSED: WriteSession.State
    WRITE_SESSION_STATE_ABORTED: WriteSession.State
    WRITE_SESSION_STATE_ORPHANED: WriteSession.State
    WRITE_SESSION_STATE_LOST: WriteSession.State
    class TargetKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        WRITE_SESSION_TARGET_KIND_UNSPECIFIED: _ClassVar[WriteSession.TargetKind]
        WRITE_SESSION_TARGET_KIND_PINNED_TAPE: _ClassVar[WriteSession.TargetKind]
        WRITE_SESSION_TARGET_KIND_POOL: _ClassVar[WriteSession.TargetKind]
    WRITE_SESSION_TARGET_KIND_UNSPECIFIED: WriteSession.TargetKind
    WRITE_SESSION_TARGET_KIND_PINNED_TAPE: WriteSession.TargetKind
    WRITE_SESSION_TARGET_KIND_POOL: WriteSession.TargetKind
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    DRIVE_ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    BODY_FORMAT_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    OBJECTS_COMMITTED_FIELD_NUMBER: _ClassVar[int]
    BYTES_COMMITTED_FIELD_NUMBER: _ClassVar[int]
    OPENED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_CHECKPOINT_AT_FIELD_NUMBER: _ClassVar[int]
    TARGET_KIND_FIELD_NUMBER: _ClassVar[int]
    TAPE_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    CURRENT_TAPE_INDEX_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    tape_uuid: bytes
    drive_element_address: int
    body_format: str
    state: WriteSession.State
    objects_committed: int
    bytes_committed: int
    opened_at: _timestamp_pb2.Timestamp
    last_checkpoint_at: _timestamp_pb2.Timestamp
    target_kind: WriteSession.TargetKind
    tape_sequence: _containers.RepeatedScalarFieldContainer[bytes]
    current_tape_index: int
    def __init__(self, session_id: _Optional[bytes] = ..., tape_uuid: _Optional[bytes] = ..., drive_element_address: _Optional[int] = ..., body_format: _Optional[str] = ..., state: _Optional[_Union[WriteSession.State, str]] = ..., objects_committed: _Optional[int] = ..., bytes_committed: _Optional[int] = ..., opened_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_checkpoint_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., target_kind: _Optional[_Union[WriteSession.TargetKind, str]] = ..., tape_sequence: _Optional[_Iterable[bytes]] = ..., current_tape_index: _Optional[int] = ...) -> None: ...

class OpenWriteSessionRequest(_message.Message):
    __slots__ = ("drive_target", "tape_target", "pool_target", "body_format", "idempotency_key", "recover_session_id")
    DRIVE_TARGET_FIELD_NUMBER: _ClassVar[int]
    TAPE_TARGET_FIELD_NUMBER: _ClassVar[int]
    POOL_TARGET_FIELD_NUMBER: _ClassVar[int]
    BODY_FORMAT_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    RECOVER_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    drive_target: DriveTarget
    tape_target: TapeTarget
    pool_target: TapePoolTarget
    body_format: str
    idempotency_key: IdempotencyKey
    recover_session_id: bytes
    def __init__(self, drive_target: _Optional[_Union[DriveTarget, _Mapping]] = ..., tape_target: _Optional[_Union[TapeTarget, _Mapping]] = ..., pool_target: _Optional[_Union[TapePoolTarget, _Mapping]] = ..., body_format: _Optional[str] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ..., recover_session_id: _Optional[bytes] = ...) -> None: ...

class DriveTarget(_message.Message):
    __slots__ = ("library_uuid", "drive_element_address", "required_pool_id")
    LIBRARY_UUID_FIELD_NUMBER: _ClassVar[int]
    DRIVE_ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_POOL_ID_FIELD_NUMBER: _ClassVar[int]
    library_uuid: bytes
    drive_element_address: int
    required_pool_id: str
    def __init__(self, library_uuid: _Optional[bytes] = ..., drive_element_address: _Optional[int] = ..., required_pool_id: _Optional[str] = ...) -> None: ...

class TapeTarget(_message.Message):
    __slots__ = ("tape_uuid", "mount_if_needed", "required_pool_id")
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    MOUNT_IF_NEEDED_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_POOL_ID_FIELD_NUMBER: _ClassVar[int]
    tape_uuid: bytes
    mount_if_needed: bool
    required_pool_id: str
    def __init__(self, tape_uuid: _Optional[bytes] = ..., mount_if_needed: _Optional[bool] = ..., required_pool_id: _Optional[str] = ...) -> None: ...

class TapePoolTarget(_message.Message):
    __slots__ = ("pool_id", "library_uuid", "mount_if_needed")
    POOL_ID_FIELD_NUMBER: _ClassVar[int]
    LIBRARY_UUID_FIELD_NUMBER: _ClassVar[int]
    MOUNT_IF_NEEDED_FIELD_NUMBER: _ClassVar[int]
    pool_id: str
    library_uuid: bytes
    mount_if_needed: bool
    def __init__(self, pool_id: _Optional[str] = ..., library_uuid: _Optional[bytes] = ..., mount_if_needed: _Optional[bool] = ...) -> None: ...

class AppendObjectMessage(_message.Message):
    __slots__ = ("start", "chunk", "finish")
    START_FIELD_NUMBER: _ClassVar[int]
    CHUNK_FIELD_NUMBER: _ClassVar[int]
    FINISH_FIELD_NUMBER: _ClassVar[int]
    start: AppendObjectStart
    chunk: AppendObjectChunk
    finish: AppendObjectFinish
    def __init__(self, start: _Optional[_Union[AppendObjectStart, _Mapping]] = ..., chunk: _Optional[_Union[AppendObjectChunk, _Mapping]] = ..., finish: _Optional[_Union[AppendObjectFinish, _Mapping]] = ...) -> None: ...

class AppendObjectStart(_message.Message):
    __slots__ = ("session_id", "caller_object_id", "caller_metadata", "declared_size_bytes", "body_format_manifest")
    class CallerMetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CALLER_OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CALLER_METADATA_FIELD_NUMBER: _ClassVar[int]
    DECLARED_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    BODY_FORMAT_MANIFEST_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    caller_object_id: str
    caller_metadata: _containers.ScalarMap[str, str]
    declared_size_bytes: int
    body_format_manifest: bytes
    def __init__(self, session_id: _Optional[bytes] = ..., caller_object_id: _Optional[str] = ..., caller_metadata: _Optional[_Mapping[str, str]] = ..., declared_size_bytes: _Optional[int] = ..., body_format_manifest: _Optional[bytes] = ...) -> None: ...

class AppendObjectChunk(_message.Message):
    __slots__ = ("session_id", "data")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    data: bytes
    def __init__(self, session_id: _Optional[bytes] = ..., data: _Optional[bytes] = ...) -> None: ...

class AppendObjectFinish(_message.Message):
    __slots__ = ("session_id", "expected_content_sha256")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_CONTENT_SHA256_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    expected_content_sha256: bytes
    def __init__(self, session_id: _Optional[bytes] = ..., expected_content_sha256: _Optional[bytes] = ...) -> None: ...

class CheckpointSessionRequest(_message.Message):
    __slots__ = ("session_id", "idempotency_key")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    idempotency_key: IdempotencyKey
    def __init__(self, session_id: _Optional[bytes] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ...) -> None: ...

class CloseWriteSessionRequest(_message.Message):
    __slots__ = ("session_id", "idempotency_key")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    idempotency_key: IdempotencyKey
    def __init__(self, session_id: _Optional[bytes] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ...) -> None: ...

class AbortWriteSessionRequest(_message.Message):
    __slots__ = ("session_id", "idempotency_key", "reason")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    idempotency_key: IdempotencyKey
    reason: str
    def __init__(self, session_id: _Optional[bytes] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class GetWriteSessionRequest(_message.Message):
    __slots__ = ("session_id",)
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    def __init__(self, session_id: _Optional[bytes] = ...) -> None: ...

class ReadSession(_message.Message):
    __slots__ = ("session_id", "tape_uuid", "drive_element_address", "state", "opened_at")
    class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        READ_SESSION_STATE_UNSPECIFIED: _ClassVar[ReadSession.State]
        READ_SESSION_STATE_OPEN: _ClassVar[ReadSession.State]
        READ_SESSION_STATE_CLOSED: _ClassVar[ReadSession.State]
        READ_SESSION_STATE_LOST: _ClassVar[ReadSession.State]
    READ_SESSION_STATE_UNSPECIFIED: ReadSession.State
    READ_SESSION_STATE_OPEN: ReadSession.State
    READ_SESSION_STATE_CLOSED: ReadSession.State
    READ_SESSION_STATE_LOST: ReadSession.State
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    TAPE_UUID_FIELD_NUMBER: _ClassVar[int]
    DRIVE_ELEMENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    OPENED_AT_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    tape_uuid: bytes
    drive_element_address: int
    state: ReadSession.State
    opened_at: _timestamp_pb2.Timestamp
    def __init__(self, session_id: _Optional[bytes] = ..., tape_uuid: _Optional[bytes] = ..., drive_element_address: _Optional[int] = ..., state: _Optional[_Union[ReadSession.State, str]] = ..., opened_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class OpenReadSessionRequest(_message.Message):
    __slots__ = ("drive_target", "tape_target", "idempotency_key")
    DRIVE_TARGET_FIELD_NUMBER: _ClassVar[int]
    TAPE_TARGET_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    drive_target: DriveTarget
    tape_target: TapeTarget
    idempotency_key: IdempotencyKey
    def __init__(self, drive_target: _Optional[_Union[DriveTarget, _Mapping]] = ..., tape_target: _Optional[_Union[TapeTarget, _Mapping]] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ...) -> None: ...

class CloseReadSessionRequest(_message.Message):
    __slots__ = ("session_id", "idempotency_key")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    idempotency_key: IdempotencyKey
    def __init__(self, session_id: _Optional[bytes] = ..., idempotency_key: _Optional[_Union[IdempotencyKey, _Mapping]] = ...) -> None: ...

class GetReadSessionRequest(_message.Message):
    __slots__ = ("session_id",)
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    def __init__(self, session_id: _Optional[bytes] = ...) -> None: ...

class ReadObjectRangeRequest(_message.Message):
    __slots__ = ("session_id", "object_id", "file_id", "start_byte", "end_byte", "stream_chunk_bytes")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    START_BYTE_FIELD_NUMBER: _ClassVar[int]
    END_BYTE_FIELD_NUMBER: _ClassVar[int]
    STREAM_CHUNK_BYTES_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    object_id: bytes
    file_id: bytes
    start_byte: int
    end_byte: int
    stream_chunk_bytes: int
    def __init__(self, session_id: _Optional[bytes] = ..., object_id: _Optional[bytes] = ..., file_id: _Optional[bytes] = ..., start_byte: _Optional[int] = ..., end_byte: _Optional[int] = ..., stream_chunk_bytes: _Optional[int] = ...) -> None: ...

class ReadFileRequest(_message.Message):
    __slots__ = ("session_id", "object_id", "file_id", "stream_chunk_bytes")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    STREAM_CHUNK_BYTES_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    object_id: bytes
    file_id: bytes
    stream_chunk_bytes: int
    def __init__(self, session_id: _Optional[bytes] = ..., object_id: _Optional[bytes] = ..., file_id: _Optional[bytes] = ..., stream_chunk_bytes: _Optional[int] = ...) -> None: ...

class BytesChunk(_message.Message):
    __slots__ = ("data", "is_last")
    DATA_FIELD_NUMBER: _ClassVar[int]
    IS_LAST_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    is_last: bool
    def __init__(self, data: _Optional[bytes] = ..., is_last: _Optional[bool] = ...) -> None: ...

class QueryAuditRequest(_message.Message):
    __slots__ = ("since", "until", "filter")
    class FilterEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SINCE_FIELD_NUMBER: _ClassVar[int]
    UNTIL_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    since: _timestamp_pb2.Timestamp
    until: _timestamp_pb2.Timestamp
    filter: _containers.ScalarMap[str, str]
    def __init__(self, since: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., until: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., filter: _Optional[_Mapping[str, str]] = ...) -> None: ...

class AuditEntry(_message.Message):
    __slots__ = ("sequence", "timestamp", "actor", "source_layer", "operation_id", "session_id", "event_kind", "detail_json")
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    SOURCE_LAYER_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_KIND_FIELD_NUMBER: _ClassVar[int]
    DETAIL_JSON_FIELD_NUMBER: _ClassVar[int]
    sequence: int
    timestamp: _timestamp_pb2.Timestamp
    actor: str
    source_layer: str
    operation_id: bytes
    session_id: bytes
    event_kind: str
    detail_json: str
    def __init__(self, sequence: _Optional[int] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., actor: _Optional[str] = ..., source_layer: _Optional[str] = ..., operation_id: _Optional[bytes] = ..., session_id: _Optional[bytes] = ..., event_kind: _Optional[str] = ..., detail_json: _Optional[str] = ...) -> None: ...
