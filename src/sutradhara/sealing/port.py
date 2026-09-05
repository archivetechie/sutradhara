"""Sealing and opening ports for per-copy stored representations.

Replication owns logical asset identity as a plaintext SHA-256, while this
port turns a local plaintext file into the exact byte representation that a
backend stores for one copy. Implementations return a context manager so any
temporary sealed objects can be removed after the backend write succeeds or
fails. The opener is the inverse used by self-heal: it recovers plaintext from
a stored copy before the missing placement is re-sealed.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from sutradhara.keys.registry import KeyEpoch


class Representation(StrEnum):
    """Supported per-copy storage-policy representations.

    ``rem-object-v1`` is also the canonical Remanence stream format id.
    ``rem-encrypt-v1`` is Sutradhara's policy label for a REM-ENCRYPT envelope
    whose decrypted inner stream is still ``rem-object-v1``.
    """

    RAW_BYTES = "raw-bytes"
    REM_OBJECT_V1 = "rem-object-v1"
    REM_ENCRYPT_V1 = "rem-encrypt-v1"
    D2TAR_RAW = "d2tar-raw"


@dataclass(frozen=True)
class SealResult:
    """Result of sealing one local source file for backend storage."""

    sealed_path: Path
    stored_digest: bytes
    plaintext_digest: bytes
    representation: Representation
    recipient_epochs: tuple[str, ...] = ()


class Sealer(Protocol):
    """Transforms a local source file into a stored representation."""

    def seal(
        self,
        source_path: Path | str,
        representation: Representation,
        *,
        key_epoch: KeyEpoch | None = None,
        work_dir: Path | str | None = None,
    ) -> AbstractContextManager[SealResult]:
        """Yield a sealed local file, cleaning temporary files on exit."""
        ...


class Opener(Protocol):
    """Recovers plaintext from a local stored representation file."""

    def open(
        self,
        source_path: Path | str,
        representation: Representation,
        *,
        recipient_epochs: Sequence[str] | None = None,
        key_domain: str | None = None,
        work_dir: Path | str | None = None,
    ) -> AbstractContextManager[Path]:
        """Yield a local plaintext file, cleaning temporary files on exit."""
        ...
