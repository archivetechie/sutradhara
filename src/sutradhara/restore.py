"""Whole-copy restore primitives shared by jobs and replication self-heal.

P2.1 restore is deliberately narrow: read one asset-scoped ``Copy`` from a
storage backend, reverse the copy's recorded representation, verify both stored
and plaintext digests, and expose a verified plaintext file only for the
duration of a context manager. Durable placement at an operator destination is
kept separate so replication can reuse the same read/open/verify path.
"""

from __future__ import annotations

import contextlib
import contextvars
import hashlib
import os
import tempfile
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.orm import Session

from sutradhara.backend.port import ByteRange, StorageBackend, VerifyResult
from sutradhara.catalog.models import Copy, LogicalAsset
from sutradhara.catalog.types import CopyHealth, content_hash
from sutradhara.evidence_recorder import record_measured
from sutradhara.sealing.port import Opener, Representation

_PROGRESS_CALLBACK: contextvars.ContextVar[Callable[[int], None] | None] = contextvars.ContextVar(
    "sutradhara_restore_progress_callback",
    default=None,
)


class RestoreError(Exception):
    """Base class for whole-copy restore failures."""


class RestoreIntegrityError(RestoreError):
    """Stored or opened bytes do not match the catalog verification anchors."""


class RestoreUnsupported(RestoreError):
    """The copy cannot be restored by the P2.1 whole-asset path."""


@dataclass(frozen=True)
class RestoreResult:
    """Verified plaintext made available inside ``restore_copy``'s context."""

    path: Path
    sha256: bytes
    size_bytes: int
    representation: Representation
    copy_id: int | None


@contextlib.contextmanager
def restore_copy(
    session: Session,
    copy: Copy,
    *,
    backend: StorageBackend,
    opener: Opener,
    execution_id: str | None = None,
    actor: str | None = None,
) -> Iterator[RestoreResult]:
    """Yield a verified plaintext temp file for one asset-scoped copy.

    Representation and encrypted-copy key epoch are read only from
    ``copy.storage_metadata``. The yielded path is valid only while the context
    is open; callers that need a durable file must copy it before exit.
    """
    expected_hash = _expected_asset_hash(session, copy)
    representation = _copy_representation(copy)
    recipient_epochs = _copy_recipient_epochs(copy, representation)

    with tempfile.TemporaryDirectory(prefix="sutradhara-restore-copy-") as temp_dir_raw:
        temp_dir = Path(temp_dir_raw)
        stored_path = temp_dir / "stored-copy.bin"
        stored_path.write_bytes(backend.read_range(copy.native_locator, ByteRange(0, 0)))

        stored_digest = sha256_file(stored_path)
        measured = content_hash(stored_digest)
        record_measured(
            session,
            copy,
            VerifyResult(
                ok=stored_digest == copy.integrity_hash,
                measured=True,
                actual_hash=measured,
                detail=(
                    ""
                    if stored_digest == copy.integrity_hash
                    else "stored bytes differ from Copy.integrity_hash"
                ),
            ),
            source="restore",
            execution_id=execution_id or f"restore-{uuid.uuid4()}",
            actor=actor,
        )
        if stored_digest != copy.integrity_hash:
            raise RestoreIntegrityError(
                "stored-corrupt: stored bytes digest differs from Copy.integrity_hash "
                f"for copy id={copy.id}: {stored_digest.hex()} != {copy.integrity_hash.hex()}"
            )

        with opener.open(
            stored_path,
            representation,
            recipient_epochs=recipient_epochs,
        ) as plaintext_path:
            plaintext_digest = sha256_file(plaintext_path)
            if plaintext_digest != expected_hash:
                raise RestoreIntegrityError(
                    "content-corrupt: opened plaintext digest differs from LogicalAsset "
                    f"for copy id={copy.id}: {plaintext_digest.hex()} != {expected_hash.hex()}"
                )
            try:
                size_bytes = plaintext_path.stat().st_size
            except OSError as exc:
                raise RestoreIntegrityError(
                    f"content-unreadable: restored plaintext is unavailable: {exc}"
                ) from exc
            yield RestoreResult(
                path=plaintext_path,
                sha256=plaintext_digest,
                size_bytes=size_bytes,
                representation=representation,
                copy_id=copy.id,
            )


def atomic_write_verified_file(
    source: Path,
    destination: Path,
    *,
    progress_callback: Callable[[int], None] | None = None,
) -> None:
    """Durably replace ``destination`` with bytes from a verified source file."""
    if not destination.is_absolute():
        raise ValueError("restore destination path must be absolute")
    parent = destination.parent
    if not parent.is_dir():
        raise ValueError(f"restore destination parent does not exist: {parent}")

    fd, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=parent)
    temp_path = Path(temp_name)
    try:
        callback = progress_callback or _PROGRESS_CALLBACK.get()
        with os.fdopen(fd, "wb") as out_handle:
            with source.open("rb") as in_handle:
                for chunk in iter(lambda: in_handle.read(1024 * 1024), b""):
                    out_handle.write(chunk)
                    if callback is not None:
                        callback(len(chunk))
            out_handle.flush()
            os.fsync(out_handle.fileno())
        os.replace(temp_path, destination)
        _fsync_directory(parent)
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            temp_path.unlink()
        raise


def atomic_write_verified_chunks(
    chunks: Iterator[bytes],
    destination: Path,
    *,
    expected_sha256: bytes,
    expected_size_bytes: int,
    progress_callback: Callable[[int], None] | None = None,
) -> None:
    """Atomically publish a chunk stream only after size and SHA-256 verification.

    The destination-side temporary file is exclusively created and removed on
    every producer, write, size, or digest failure.  The caller's producer is
    responsible for any representation-layer verification before clean EOF.
    """

    if not destination.is_absolute():
        raise ValueError("restore destination path must be absolute")
    if len(expected_sha256) != hashlib.sha256().digest_size:
        raise ValueError("expected_sha256 must be a 32-byte SHA-256 hash")
    if expected_size_bytes < 0:
        raise ValueError("expected_size_bytes must be non-negative")
    parent = destination.parent
    if not parent.is_dir():
        raise ValueError(f"restore destination parent does not exist: {parent}")

    fd, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=parent)
    temp_path = Path(temp_name)
    digest = hashlib.sha256()
    size_bytes = 0
    try:
        callback = progress_callback or _PROGRESS_CALLBACK.get()
        with os.fdopen(fd, "wb") as out_handle:
            for chunk in chunks:
                if not isinstance(chunk, bytes):
                    raise TypeError("restore stream yielded a non-bytes chunk")
                out_handle.write(chunk)
                digest.update(chunk)
                size_bytes += len(chunk)
                if callback is not None:
                    callback(len(chunk))
            if size_bytes != expected_size_bytes:
                raise RestoreIntegrityError(
                    f"restored size {size_bytes} != expected {expected_size_bytes}"
                )
            actual_sha256 = digest.digest()
            if actual_sha256 != expected_sha256:
                raise RestoreIntegrityError(
                    f"restored SHA-256 {actual_sha256.hex()} != expected {expected_sha256.hex()}"
                )
            out_handle.flush()
            os.fsync(out_handle.fileno())
        os.replace(temp_path, destination)
        _fsync_directory(parent)
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            temp_path.unlink()
        raise


@contextlib.contextmanager
def restore_progress_context(callback: Callable[[int], None] | None) -> Iterator[None]:
    """Apply a byte-progress callback to nested atomic restore publishes."""

    token = _PROGRESS_CALLBACK.set(callback)
    try:
        yield
    finally:
        _PROGRESS_CALLBACK.reset(token)


def validate_restore_destination(value: object) -> Path:
    """Validate a JSON-bound restore destination parameter."""
    if not isinstance(value, str) or not value:
        raise ValueError("restore destination must be a non-empty string")
    destination = Path(value)
    if not destination.is_absolute():
        raise ValueError("restore destination path must be absolute")
    if not destination.parent.is_dir():
        raise ValueError(f"restore destination parent does not exist: {destination.parent}")
    return destination


def sha256_file(path: Path) -> bytes:
    """Return SHA-256 digest bytes for a local file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()


def _expected_asset_hash(session: Session, copy: Copy) -> bytes:
    if copy.deleted_at is not None:
        raise RestoreUnsupported(f"copy id={copy.id} has been tombstoned by retention")
    if copy.health == CopyHealth.MISSING:
        raise RestoreUnsupported(f"copy id={copy.id} has health=missing")
    if copy.bundle_id is not None or copy.logical_asset_hash is None:
        raise RestoreUnsupported("bundle restore is not supported by P2.1 whole-asset restore")
    asset = session.get(LogicalAsset, copy.logical_asset_hash)
    if asset is None:
        raise RestoreUnsupported(f"copy id={copy.id} does not reference a known LogicalAsset")
    return asset.content_sha256


def _copy_representation(copy: Copy) -> Representation:
    value = copy.storage_metadata.get("representation")
    if not isinstance(value, str) or not value:
        raise RestoreUnsupported(f"copy id={copy.id} has no recorded representation")
    try:
        return Representation(value)
    except ValueError as exc:
        raise RestoreUnsupported(
            f"copy id={copy.id} has unsupported representation {value!r}"
        ) from exc


def _copy_recipient_epochs(copy: Copy, representation: Representation) -> tuple[str, ...] | None:
    if representation is not Representation.RAO_AEAD_V1:
        return None
    value = copy.storage_metadata.get("recipient_epochs")
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(epoch, str) or not epoch for epoch in value)
    ):
        raise RestoreUnsupported(
            f"encrypted copy id={copy.id} has no valid recipient_epochs; cannot restore"
        )
    if len(set(value)) != len(value):
        raise RestoreUnsupported(
            f"encrypted copy id={copy.id} has duplicate recipient_epochs; cannot restore"
        )
    return tuple(value)


def _fsync_directory(path: Path) -> None:
    flags = getattr(os, "O_DIRECTORY", 0) | os.O_RDONLY
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
