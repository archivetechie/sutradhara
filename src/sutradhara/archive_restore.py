"""Artifactclass-aware restore from bundle asset locators.

Restore policy is copy-independent: choose the first healthy locator according
to the artifactclass policy's ordered pool preference, extract bytes from that
copy's representation, and verify the plaintext SHA-256 against the logical
asset identity.
"""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from sutradhara.artifactclass_policy import get_artifactclass_policy
from sutradhara.backend.port import ByteRange, StorageBackend
from sutradhara.catalog.models import ArtifactClassPool, AssetLocator, Copy
from sutradhara.catalog.types import CopyHealth, is_content_hash
from sutradhara.keys import KeyRegistry
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import RAO_CHUNK_SIZE


class ArchiveRestoreError(Exception):
    """Base class for archive restore errors."""


class RestoreSourceUnavailable(ArchiveRestoreError):
    """No healthy copy could satisfy a restore request."""


class RestoreIntegrityError(ArchiveRestoreError):
    """Restored bytes do not match the logical asset hash."""


@dataclass(frozen=True)
class RestoreResult:
    """Completed restore details."""

    asset_hash: bytes
    pool_id: str
    copy_id: int
    output_path: Path
    size_bytes: int


class ArchiveExtractor(Protocol):
    """Per-copy archive extraction boundary."""

    def extract(
        self,
        *,
        locator: AssetLocator,
        copy: Copy,
        backend: StorageBackend,
    ) -> bytes:
        """Return plaintext bytes for one locator from one stored copy."""
        ...


class LocalArchiveExtractor:
    """Extractor for d2 tar copies, local test archives, and offset locators."""

    def extract(
        self,
        *,
        locator: AssetLocator,
        copy: Copy,
        backend: StorageBackend,
    ) -> bytes:
        representation = Representation(locator.representation)
        if representation is Representation.D2TAR_RAW:
            return _extract_d2(locator, copy, backend)
        if (
            representation in {Representation.RAO_PLAIN_V1, Representation.RAO_AEAD_V1}
            and "offset" not in locator.native_locator
        ):
            raise ArchiveRestoreError(
                f"copy id={copy.id} representation {representation.value!r} "
                "requires a RAO restore adapter"
            )

        container = _read_whole(backend, copy)
        if _is_local_archive(container):
            return _extract_from_local_archive(container, locator.native_locator)
        if "offset" in locator.native_locator:
            offset = int(locator.native_locator["offset"])
            size = int(locator.native_locator["size_bytes"])
            return backend.read_range(
                copy.native_locator,
                ByteRange(offset, offset + size),
            )
        raise ArchiveRestoreError(
            f"copy id={copy.id} representation {representation.value!r} requires "
            "a RAO restore adapter; no local locator offset was available"
        )


class RemArchiveExtractor(LocalArchiveExtractor):
    """Extractor that delegates RAO bundle extraction to the rem CLI."""

    def __init__(
        self,
        rem_bin: str | Path = "rem",
        *,
        keys: KeyRegistry | None = None,
    ) -> None:
        self._rem_bin = str(rem_bin)
        self._keys = keys or KeyRegistry()

    def extract(
        self,
        *,
        locator: AssetLocator,
        copy: Copy,
        backend: StorageBackend,
    ) -> bytes:
        try:
            return super().extract(locator=locator, copy=copy, backend=backend)
        except ArchiveRestoreError:
            representation = Representation(locator.representation)
            if representation not in {
                Representation.RAO_PLAIN_V1,
                Representation.RAO_AEAD_V1,
            }:
                raise
            return self._extract_with_rem(locator, copy, backend, representation)

    def _extract_with_rem(
        self,
        locator: AssetLocator,
        copy: Copy,
        backend: StorageBackend,
        representation: Representation,
    ) -> bytes:
        member_path = _member_path(locator.native_locator)
        with tempfile.TemporaryDirectory(prefix="sutradhara-restore-") as raw:
            temp_dir = Path(raw)
            object_path = temp_dir / "bundle.rao"
            dest_dir = temp_dir / "out"
            dest_dir.mkdir()
            _materialize_copy_to_path(backend, copy, object_path)
            cmd = [
                self._rem_bin,
                "archive",
                "extract",
                "--object",
                str(object_path),
                "--dest",
                str(dest_dir),
                "--path",
                member_path,
                "--first-chunk-lba",
                str(_first_chunk_lba(locator.native_locator)),
                "--file-size-bytes",
                str(_size_bytes(locator.native_locator)),
                "--range",
                f"0:{_size_bytes(locator.native_locator)}",
                "--overwrite",
            ]
            if representation is Representation.RAO_PLAIN_V1:
                cmd.extend(["--chunk-size", str(RAO_CHUNK_SIZE)])
                _run_rem(cmd)
            else:
                key_epoch = _key_epoch(copy.storage_metadata)
                with self._keys.materialized_root_key(key_epoch) as key_file:
                    cmd.extend(["--key-file", str(key_file)])
                    _run_rem(cmd)
            return _read_restored_member(dest_dir, member_path)


def restore_asset(
    session: Session,
    *,
    asset_hash: bytes,
    artifactclass: str,
    destination: Path | str,
    backends: dict[int, StorageBackend],
    extractor: ArchiveExtractor | None = None,
) -> RestoreResult:
    """Restore one asset using the artifactclass ordered pool preference."""
    if not is_content_hash(asset_hash):
        raise ValueError("asset_hash must be a 32-byte SHA-256 hash")
    archive_extractor = extractor or LocalArchiveExtractor()
    policy = get_artifactclass_policy(session, artifactclass)
    pool_order = _restore_pool_order(session, artifactclass, policy.restore_preference)
    locators = list(
        session.scalars(
            select(AssetLocator)
            .options(joinedload(AssetLocator.copy).joinedload(Copy.backend))
            .where(AssetLocator.logical_asset_hash == asset_hash)
        )
    )
    by_pool = {pool_id: [] for pool_id in pool_order}
    for locator in locators:
        if locator.pool_id in by_pool:
            by_pool[locator.pool_id].append(locator)

    integrity_errors: list[str] = []
    for pool_id in pool_order:
        for locator in by_pool.get(pool_id, []):
            copy = locator.copy
            if copy is None or copy.health != CopyHealth.OK:
                continue
            backend = backends.get(copy.backend_id)
            if backend is None:
                continue
            try:
                data = archive_extractor.extract(
                    locator=locator,
                    copy=copy,
                    backend=backend,
                )
            except ArchiveRestoreError as exc:
                integrity_errors.append(f"copy id={copy.id} pool={pool_id}: {exc}")
                continue
            actual = hashlib.sha256(data).digest()
            if actual != asset_hash:
                integrity_errors.append(
                    f"copy id={copy.id} pool={pool_id}: {actual.hex()} != {asset_hash.hex()}"
                )
                continue
            output_path = Path(destination)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(data)
            return RestoreResult(
                asset_hash=asset_hash,
                pool_id=pool_id,
                copy_id=copy.id,
                output_path=output_path,
                size_bytes=len(data),
            )

    if integrity_errors:
        raise RestoreIntegrityError(
            f"all candidate restores for asset {asset_hash.hex()} failed integrity: "
            + "; ".join(integrity_errors)
        )
    raise RestoreSourceUnavailable(
        f"no healthy locator for asset {asset_hash.hex()} in artifactclass {artifactclass!r}"
    )


def _restore_pool_order(
    session: Session,
    artifactclass: str,
    restore_preference: list[str],
) -> list[str]:
    order: list[str] = []
    seen: set[str] = set()
    for pool_id in restore_preference:
        if pool_id not in seen:
            seen.add(pool_id)
            order.append(pool_id)
    memberships = list(
        session.scalars(
            select(ArtifactClassPool)
            .where(
                ArtifactClassPool.artifactclass == artifactclass,
                ArtifactClassPool.active.is_(True),
            )
            .order_by(ArtifactClassPool.sort_order, ArtifactClassPool.pool_id)
        )
    )
    for membership in memberships:
        if membership.pool_id not in seen:
            seen.add(membership.pool_id)
            order.append(membership.pool_id)
    return order


def _extract_d2(
    locator: AssetLocator,
    copy: Copy,
    backend: StorageBackend,
) -> bytes:
    if "block_range" in locator.native_locator:
        raw_range = locator.native_locator["block_range"]
        if (
            isinstance(raw_range, list)
            and len(raw_range) == 2
            and "size_bytes" in locator.native_locator
        ):
            data_start = int(raw_range[0])
            size = int(locator.native_locator["size_bytes"])
            return backend.read_range(
                copy.native_locator,
                ByteRange(data_start, data_start + size),
            )
    container = _read_whole(backend, copy)
    return _extract_from_tar(container, locator.member_path)


def _extract_from_tar(container: bytes, member_path: str) -> bytes:
    with tarfile.open(fileobj=io.BytesIO(container), mode="r:*") as tar:
        handle = tar.extractfile(member_path)
        if handle is None:
            raise ArchiveRestoreError(f"tar member {member_path!r} is not a file")
        return handle.read()


def _extract_from_local_archive(container: bytes, locator: dict[str, object]) -> bytes:
    header_len = int.from_bytes(container[:8], "big")
    payload_start = 8 + header_len
    json.loads(container[8:payload_start].decode("utf-8"))
    offset = int(locator["offset"])
    size = int(locator["size_bytes"])
    return container[payload_start + offset : payload_start + offset + size]


def _is_local_archive(container: bytes) -> bool:
    if len(container) < 8:
        return False
    header_len = int.from_bytes(container[:8], "big")
    if header_len <= 0 or 8 + header_len > len(container):
        return False
    try:
        header = json.loads(container[8 : 8 + header_len].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    return isinstance(header, dict) and header.get("format") == "sutradhara-local-archive-v1"


def _read_whole(backend: StorageBackend, copy: Copy) -> bytes:
    return backend.read_range(copy.native_locator, ByteRange(0, 0))


def _materialize_copy_to_path(
    backend: StorageBackend,
    copy: Copy,
    destination: Path,
) -> None:
    size = copy.storage_metadata.get("stored_size_bytes")
    if isinstance(size, int) and size >= 0:
        with destination.open("wb") as handle:
            for start in range(0, size, RAO_CHUNK_SIZE):
                end = min(start + RAO_CHUNK_SIZE, size)
                handle.write(backend.read_range(copy.native_locator, ByteRange(start, end)))
        return
    destination.write_bytes(_read_whole(backend, copy))


def _member_path(locator: dict[str, Any]) -> str:
    value = locator.get("member_path")
    if not isinstance(value, str) or not value:
        raise ArchiveRestoreError("asset locator is missing member_path")
    return value


def _first_chunk_lba(locator: dict[str, Any]) -> int:
    value = locator.get("first_chunk_lba")
    if value is None:
        raise ArchiveRestoreError("RAO asset locator is missing first_chunk_lba")
    result = int(value)
    if result < 0:
        raise ArchiveRestoreError(f"invalid first_chunk_lba {value!r}")
    return result


def _size_bytes(locator: dict[str, Any]) -> int:
    value = locator.get("size_bytes")
    if value is None:
        raise ArchiveRestoreError("asset locator is missing size_bytes")
    result = int(value)
    if result < 0:
        raise ArchiveRestoreError(f"invalid size_bytes {value!r}")
    return result


def _key_epoch(storage_metadata: dict[str, Any]) -> str:
    value = storage_metadata.get("key_epoch")
    if not isinstance(value, str) or not value:
        raise ArchiveRestoreError("encrypted archive copy is missing key_epoch")
    return value


def _run_rem(cmd: list[str]) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise ArchiveRestoreError(
            f"rem archive extract failed (exit {result.returncode}): "
            f"stdout={result.stdout.strip()[:500]!r} "
            f"stderr={result.stderr.strip()[:500]!r}"
        )


def _read_restored_member(dest_dir: Path, member_path: str) -> bytes:
    candidate = dest_dir / member_path if member_path else None
    if candidate is not None and candidate.is_file():
        return candidate.read_bytes()
    files = [path for path in dest_dir.rglob("*") if path.is_file()]
    if len(files) != 1:
        raise ArchiveRestoreError(
            f"rem restore expected one file for member {member_path!r}, found {len(files)}"
        )
    return files[0].read_bytes()
