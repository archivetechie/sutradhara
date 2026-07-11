"""Storage backend contract — the trait every backend adapter implements.

See docs/spec-v0.1.md §5 (backend adapter contract). Each adapter is one
thin layer per backend kind. Sutradhara treats every backend uniformly:
enumerate() yields the per-copy identity, read_range() reads bytes,
verify() re-checks integrity.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from sutradhara.catalog.types import ContentHash

# A locator is backend-specific structured data. Each backend defines its
# own shape (e.g. tape: {tape_uuid, tape_file_number}; s3: {bucket, key};
# disk: {path}). The catalog stores it as JSON; adapters interpret it.
BackendLocator = dict[str, Any]


class StreamKind(StrEnum):
    """How a backend makes range chunks available to a consumer."""

    native_stream = "native_stream"
    scratch_stream = "scratch_stream"
    memory_buffered = "memory_buffered"


@dataclass(frozen=True)
class ByteRange:
    """Half-open byte range [start, end). Matches the gRPC ReadObjectRange
    contract (proto/layer5.proto). start == end == 0 means "whole object."
    """

    start: int
    end: int

    def __post_init__(self) -> None:
        if self.start < 0 or self.end < 0:
            raise ValueError(f"byte range must be non-negative: [{self.start}, {self.end})")
        if self.end == 0 and self.start != 0:
            raise ValueError("byte range end=0 is reserved for whole-object reads; use [0, 0)")
        if self.end != 0 and self.end < self.start:
            raise ValueError(f"byte range end must be >= start: [{self.start}, {self.end})")

    @property
    def is_whole_object(self) -> bool:
        return self.start == 0 and self.end == 0

    @property
    def length(self) -> int:
        """Length of the requested slice, or 0 if whole-object."""
        if self.is_whole_object:
            return 0
        return self.end - self.start


@dataclass(frozen=True)
class CopyRecord:
    """One copy of a logical asset on one backend.

    The shape returned by `StorageBackend.enumerate()`. This is what the
    catalog turns into rows in `copy` (sutradhara.catalog.models.Copy).

    See spec-v0.1.md §5.1: the union of every backend's enumerate() output
    IS the rebuilt catalog.
    """

    logical_id: ContentHash
    native_locator: BackendLocator
    integrity_hash: ContentHash
    size_bytes: int
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VerifyResult:
    """Outcome of a backend.verify() call."""

    ok: bool
    actual_hash: ContentHash | None = None
    detail: str = ""


@runtime_checkable
class StorageBackend(Protocol):
    """The trait every storage backend implements.

    Implementations: see sutradhara.backend.memory (in-process test backend) and
    sutradhara.backend.remanence (gRPC adapter to Remanence Layer 5, with an
    explicit dev fixture mode).
    """

    @property
    def name(self) -> str:
        """Operator-visible backend name. Matches the `backend.name` column."""
        ...

    def enumerate(self) -> Iterator[CopyRecord]:
        """Yield every copy this backend holds.

        Each `CopyRecord` carries `logical_id` (the content hash) so the
        catalog can be rebuilt from the union of every backend's
        enumeration. This is the load-bearing operation that makes the
        rebuildable-index discipline real (spec-v0.1.md §2 principle 1).
        """
        ...

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        """Read a byte range from one copy.

        `byte_range.is_whole_object` is True → read the whole object.
        Otherwise read exactly `[start, end)` (half-open, like the proto).
        Raises `BackendNotFoundError` if the locator does not address a
        known copy on this backend.
        """
        ...

    def verify(self, locator: BackendLocator) -> VerifyResult:
        """Re-check the integrity of one copy.

        Implementation strategy is backend-specific:
          - rem-tape: re-read + re-hash, or trust 3c parity scrub.
          - cloud: re-hash via download (expensive) or trust provider ETag.
          - memory: re-hash the stored bytes.

        Returns a `VerifyResult.ok=False` (with `actual_hash` if known) on
        mismatch; does NOT raise on integrity failure.
        """
        ...


@runtime_checkable
class DeletableStorageBackend(StorageBackend, Protocol):
    """Storage backend capability for idempotent object deletion."""

    def delete_object(self, locator: BackendLocator) -> None:
        """Delete one backend object.

        Implementations must treat an already-absent object as success so
        callers can safely retry after a delete-before-DB crash window.
        """
        ...


@runtime_checkable
class StreamingStorageBackend(StorageBackend, Protocol):
    """Optional capability for genuinely lazy, context-owned range streams."""

    @property
    def stream_kind(self) -> StreamKind:
        """Describe the storage behavior behind ``open_range_chunks``."""
        ...

    def open_range_chunks(
        self,
        locator: BackendLocator,
        byte_range: ByteRange,
        *,
        chunk_bytes: int,
    ) -> AbstractContextManager[Iterator[bytes]]:
        """Open a lazy byte stream whose context structurally owns cleanup."""
        ...


class BackendError(Exception):
    """Base for adapter-level errors."""


class BackendNotFoundError(BackendError):
    """The supplied locator does not address any copy on this backend."""


class BackendUnavailableError(BackendError):
    """The backend itself is unreachable (network, mount, etc.)."""


class BackendTransientError(BackendUnavailableError):
    """A backend operation failed with a retryable transport condition."""


class BackendSessionInvalidatedError(BackendError):
    """A session-scoped backend read was invalidated before bytes were returned."""
