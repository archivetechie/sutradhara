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
        raise ValueError(
            f"content_sha256 must be {CONTENT_HASH_LEN} bytes; got {len(value)}"
        )
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
    S3 = "s3"
    GCS = "gcs"
    AZURE_BLOB = "azure_blob"
    MEMORY = "memory"  # in-process test backend (sutradhara.backend.memory)


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


class CopySource(StrEnum):
    """How this copy first came to be known to the catalog."""

    INGEST = "ingest"            # first-write path
    SCRUB = "scrub"              # discovered by a scrub re-enumeration
    MANUAL_IMPORT = "manual_import"


class MediaKind(StrEnum):
    """Coarse classification of asset content. Derivable, non-authoritative."""

    VIDEO = "video"
    AUDIO = "audio"
    IMAGE = "image"
    DOCUMENT = "document"
    OTHER = "other"
