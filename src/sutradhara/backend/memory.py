"""In-process test backend.

Holds a `{content_sha256 → bytes}` map. Useful for unit tests of the
scrub flow and for exercising the StorageBackend trait independent of
any real backend. Not a production backend.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from typing import Any

from sutradhara.backend.port import (
    BackendLocator,
    BackendNotFoundError,
    ByteRange,
    CopyRecord,
    TaggedPlacement,
    VerifyResult,
)
from sutradhara.catalog.types import ContentHash, content_hash

PlacementConfig = TaggedPlacement | dict[str, Any]


class MemoryBackend:
    """In-process backend storing raw bytes keyed by content hash."""

    def __init__(
        self,
        name: str,
        placements: list[PlacementConfig] | None = None,
    ) -> None:
        self._name = name
        self._objects: dict[ContentHash, bytes] = {}
        self._extra_metadata: dict[ContentHash, dict[str, Any]] = {}
        self._placements = tuple(
            _placement_from_config(name, placement)
            for placement in (placements or [])
        )

    @property
    def name(self) -> str:
        return self._name

    # --- test helpers -----------------------------------------------------

    def add(self, content: bytes, **metadata: Any) -> ContentHash:
        """Store `content`, return its hash. Convenience for tests."""
        h = content_hash(hashlib.sha256(content).digest())
        self._objects[h] = content
        if metadata:
            self._extra_metadata[h] = metadata
        return h

    def corrupt(self, hash_: ContentHash, replacement: bytes = b"\x00") -> None:
        """Overwrite the stored bytes for `hash_` to simulate corruption.

        Subsequent `verify()` on this object will report `ok=False`.
        """
        if hash_ not in self._objects:
            raise BackendNotFoundError(f"no object with hash {hash_.hex()[:12]}…")
        self._objects[hash_] = replacement

    # --- StorageBackend protocol -----------------------------------------

    def enumerate(self) -> Iterator[CopyRecord]:
        for h, data in self._objects.items():
            yield CopyRecord(
                logical_id=h,
                native_locator={"hash_hex": h.hex()},
                integrity_hash=h,
                size_bytes=len(data),
                metadata=dict(self._extra_metadata.get(h, {})),
            )

    def list_tagged_placements(self) -> list[TaggedPlacement]:
        return list(self._placements)

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        h = self._locator_to_hash(locator)
        data = self._objects[h]

        if byte_range.is_whole_object:
            return data

        if byte_range.end > len(data):
            raise ValueError(
                f"byte range end {byte_range.end} exceeds object size {len(data)}"
            )
        return data[byte_range.start : byte_range.end]

    def verify(self, locator: BackendLocator) -> VerifyResult:
        h = self._locator_to_hash(locator)
        data = self._objects[h]
        actual = content_hash(hashlib.sha256(data).digest())
        if actual == h:
            return VerifyResult(ok=True, actual_hash=actual)
        return VerifyResult(
            ok=False,
            actual_hash=actual,
            detail=f"expected {h.hex()[:12]}…, got {actual.hex()[:12]}…",
        )

    # --- helpers ---------------------------------------------------------

    def _locator_to_hash(self, locator: BackendLocator) -> ContentHash:
        hex_value = locator.get("hash_hex")
        if not isinstance(hex_value, str):
            raise BackendNotFoundError(
                f"memory backend locator must have 'hash_hex' key; got {locator!r}"
            )
        try:
            h = content_hash(bytes.fromhex(hex_value))
        except ValueError as e:
            raise BackendNotFoundError(
                f"invalid hash_hex {hex_value!r}: {e}"
            ) from e
        if h not in self._objects:
            raise BackendNotFoundError(f"no object with hash {hex_value[:12]}…")
        return h


def _placement_from_config(
    backend_name: str,
    placement: PlacementConfig,
) -> TaggedPlacement:
    if isinstance(placement, TaggedPlacement):
        return TaggedPlacement(
            placement.placement_id,
            placement.content_class,
            placement.copy_class,
            backend_name,
            placement.representation,
            placement.key_epoch,
        )
    try:
        return TaggedPlacement(
            placement_id=str(placement["placement_id"]),
            content_class=str(placement["content_class"]),
            copy_class=str(placement["copy_class"]),
            backend_name=backend_name,
            representation=str(placement.get("representation", "raw-bytes")),
            key_epoch=(
                None
                if placement.get("key_epoch") is None
                else str(placement.get("key_epoch"))
            ),
        )
    except KeyError as e:
        raise ValueError(
            "memory backend placement config must include placement_id, "
            "content_class, and copy_class"
        ) from e
