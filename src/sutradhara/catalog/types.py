"""Domain types used across the catalog."""

from __future__ import annotations

from enum import StrEnum
from typing import NewType

# A SHA-256 content hash. 32 raw bytes. This is the logical-asset identity
# (docs/spec-v0.1.md §2, §4.1) — there is no surrogate ID.
ContentHash = NewType("ContentHash", bytes)

CONTENT_HASH_LEN = 32


def is_content_hash(value: bytes) -> bool:
    """True iff `value` is the right length to be a SHA-256 content hash."""
    return isinstance(value, bytes) and len(value) == CONTENT_HASH_LEN


def content_hash(value: bytes) -> ContentHash:
    """Construct a `ContentHash`, validating length."""
    if not is_content_hash(value):
        raise ValueError(f"content_sha256 must be {CONTENT_HASH_LEN} bytes; got {len(value)}")
    return ContentHash(value)


class BackendKind(StrEnum):
    """The kind of storage a backend represents.

    The set is not closed — new kinds land alongside new adapters. This
    enum is the registered set as of spec-v0.1.md §4.5.
    """

    REM_TAPE = "rem_tape"
    D2_TAPE = "d2_tape"
    REM_DISK = "rem_disk"
    PLAIN_DISK = "plain_disk"
    SSH_DISK = "ssh_disk"
    S3 = "s3"
    GCS = "gcs"
    AZURE_BLOB = "azure_blob"
    MEMORY = "memory"  # in-process test backend (sutradhara.backend.memory)


BACKEND_IMPLEMENTATION_FAMILIES: dict[BackendKind, str] = {
    BackendKind.REM_TAPE: "tape",
    BackendKind.D2_TAPE: "d2tape",
    BackendKind.REM_DISK: "disk",
    BackendKind.PLAIN_DISK: "disk",
    BackendKind.SSH_DISK: "disk",
    BackendKind.S3: "cloud",
    BackendKind.GCS: "cloud",
    BackendKind.AZURE_BLOB: "cloud",
    BackendKind.MEMORY: "memory",
}


def implementation_family_for_kind(kind: BackendKind | str) -> str:
    """Return the durability implementation family for a registered backend kind."""

    backend_kind = BackendKind(kind)
    try:
        return BACKEND_IMPLEMENTATION_FAMILIES[backend_kind]
    except KeyError as exc:
        raise ValueError(
            f"backend kind {backend_kind.value!r} has no implementation family mapping"
        ) from exc


class BackendTier(StrEnum):
    """First-class distinction from spec-v0.1.md §5.2 / §5.3.

    Tier-1 (self_describing) backends carry the content hash on the medium
    itself; a scan rebuilds the catalog without any central state.

    Tier-2 (catalog_authoritative) backends require DB backup for recovery
    because linkage exists only in the Sutradhara DB. The spec calls for
    keeping this tier as small as possible.
    """

    SELF_DESCRIBING = "self_describing"
    CATALOG_AUTHORITATIVE = "catalog_authoritative"


class CopyHealth(StrEnum):
    """Health of a single copy, updated by scrub and verify jobs."""

    OK = "ok"
    SUSPECT = "suspect"
    CORRUPT = "corrupt"
    MISSING = "missing"


class AssetValidity(StrEnum):
    """Decode/parse validity of the content-addressed asset bytes."""

    OK = "ok"
    SUSPECT = "suspect"
    UNVALIDATED = "unvalidated"


class CopySource(StrEnum):
    """How this copy first came to be known to the catalog."""

    INGEST = "ingest"  # first-write path
    SCRUB = "scrub"  # discovered by a scrub re-enumeration
    MANUAL_IMPORT = "manual_import"


class MediaKind(StrEnum):
    """Coarse classification of asset content. Derivable, non-authoritative."""

    VIDEO = "video"
    AUDIO = "audio"
    IMAGE = "image"
    DOCUMENT = "document"
    OTHER = "other"


class IntakeSourceKind(StrEnum):
    """Where an intake arrived from before catalog registration."""

    CARD = "card"
    DRIVE = "drive"
    UPLOAD = "upload"
    HANDOFF = "handoff"
    DOWNLOAD = "download"
    OTHER = "other"


class IntakeStatus(StrEnum):
    """Registration state for a landing intake."""

    RECEIVING = "receiving"
    VERIFYING = "verifying"
    QUARANTINED = "quarantined"
    REGISTERED = "registered"


class IngestDisposition(StrEnum):
    """Immutable content-novelty verdict recorded when an item is registered."""

    NEW = "new"
    KNOWN_DURABLE = "known_durable"
    KNOWN_UNDER_DURABLE = "known_under_durable"
    REVERIFIED = "reverified"
    LEGACY_UNKNOWN = "legacy_unknown"


class ArrangementStatus(StrEnum):
    """Lifecycle for a mutable pre-archive arrangement workspace."""

    DRAFT = "draft"
    PENDING_DERIVATIVES = "pending_derivatives"
    READY = "ready"
    SUBMITTED = "submitted"
    ABANDONED = "abandoned"


class SubmissionStatus(StrEnum):
    """Archive lifecycle for an immutable submitted source-map payload."""

    PENDING_ARCHIVE = "pending_archive"
    ARCHIVED = "archived"


class RetentionState(StrEnum):
    """Per-intake lifecycle for temporary-byte retention."""

    HELD = "held"
    RELEASED = "released"
    PURGED = "purged"
