"""Artifactclass-aware restore from bundle asset locators.

Restore policy is copy-independent: choose the first healthy locator according
to the artifactclass policy's ordered pool preference, extract bytes from that
copy's representation, and verify the plaintext SHA-256 against the logical
asset identity.
"""

from __future__ import annotations

import json
import shutil
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
from sutradhara.catalog.models import (
    ArtifactClassPool,
    AssetLocator,
    Bundle,
    BundleMember,
    Copy,
    LogicalAsset,
    StagingTransform,
)
from sutradhara.catalog.types import AssetValidity, CopyHealth, is_content_hash
from sutradhara.keys import KeyRegistry
from sutradhara.member_name import MemberNameError, escape_member_name, unescape_member_name
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import RAO_CHUNK_SIZE
from sutradhara.staging import StagingError, reverse_transforms_to_path

_MAX_LOCAL_ARCHIVE_HEADER_BYTES = 16 * 1024 * 1024


class ArchiveRestoreError(Exception):
    """Base class for archive restore errors."""


class RestoreSourceUnavailable(ArchiveRestoreError):
    """No healthy copy could satisfy a restore request."""


class RestoreIntegrityError(ArchiveRestoreError):
    """Restored bytes do not match the logical asset hash."""


class RestoreNameError(ArchiveRestoreError):
    """A customer member-name restore request could not be resolved."""


class RestoreSuspectAsset(ArchiveRestoreError):
    """A normal restore was refused because the asset is flagged suspect."""


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

    def extract_to_path(
        self,
        *,
        locator: AssetLocator,
        copy: Copy,
        backend: StorageBackend,
        destination: Path,
    ) -> None:
        """Write the stored member bytes for one locator from one stored copy."""
        ...


class LocalArchiveExtractor:
    """Extractor for d2 tar copies, local test archives, and offset locators."""

    def extract_to_path(
        self,
        *,
        locator: AssetLocator,
        copy: Copy,
        backend: StorageBackend,
        destination: Path,
    ) -> None:
        representation = Representation(locator.representation)
        if representation is Representation.D2TAR_RAW:
            _extract_d2_to_path(locator, copy, backend, destination)
            return
        if (
            representation in {Representation.RAO_PLAIN_V1, Representation.RAO_AEAD_V1}
            and "offset" not in locator.native_locator
        ):
            raise ArchiveRestoreError(
                f"copy id={copy.id} representation {representation.value!r} "
                "requires a RAO restore adapter"
            )

        if _try_extract_from_local_archive_to_path(backend, copy, locator, destination):
            return
        if "offset" in locator.native_locator:
            offset = int(locator.native_locator["offset"])
            size = int(locator.native_locator["size_bytes"])
            _copy_backend_range_to_path(
                backend,
                copy.native_locator,
                offset,
                offset + size,
                destination,
            )
            return
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

    def extract_to_path(
        self,
        *,
        locator: AssetLocator,
        copy: Copy,
        backend: StorageBackend,
        destination: Path,
    ) -> None:
        try:
            super().extract_to_path(
                locator=locator,
                copy=copy,
                backend=backend,
                destination=destination,
            )
            return
        except ArchiveRestoreError:
            representation = Representation(locator.representation)
            if representation not in {
                Representation.RAO_PLAIN_V1,
                Representation.RAO_AEAD_V1,
            }:
                raise
            self._extract_with_rem_to_path(locator, copy, backend, representation, destination)

    def _extract_with_rem_to_path(
        self,
        locator: AssetLocator,
        copy: Copy,
        backend: StorageBackend,
        representation: Representation,
        destination: Path,
    ) -> None:
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
            format_plugin = _format_plugin(copy.storage_metadata)
            if format_plugin is not None:
                cmd.extend(["--format", format_plugin])
            if representation is Representation.RAO_PLAIN_V1:
                cmd.extend(["--chunk-size", str(RAO_CHUNK_SIZE)])
                _run_rem(cmd)
            else:
                key_epoch = _key_epoch(copy.storage_metadata)
                with self._keys.materialized_root_key(key_epoch) as key_file:
                    cmd.extend(["--key-file", str(key_file)])
                    _run_rem(cmd)
            _copy_restored_member(dest_dir, member_path, destination)


def restore_asset(
    session: Session,
    *,
    asset_hash: bytes,
    artifactclass: str,
    destination: Path | str,
    backends: dict[int, StorageBackend],
    extractor: ArchiveExtractor | None = None,
    force_suspect: bool = False,
) -> RestoreResult:
    """Restore one asset using the artifactclass ordered pool preference."""
    if not is_content_hash(asset_hash):
        raise ValueError("asset_hash must be a 32-byte SHA-256 hash")
    _check_asset_restore_allowed(session, asset_hash, force_suspect=force_suspect)
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
    by_pool: dict[str, list[AssetLocator]] = {pool_id: [] for pool_id in pool_order}
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
            output_path = Path(destination)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with tempfile.TemporaryDirectory(
                    prefix=".sutradhara-restore-",
                    dir=output_path.parent,
                ) as raw_tmp:
                    temp_dir = Path(raw_tmp)
                    stored_path = temp_dir / "stored-member"
                    restored_path = temp_dir / "restored-member"
                    archive_extractor.extract_to_path(
                        locator=locator,
                        copy=copy,
                        backend=backend,
                        destination=stored_path,
                    )
                    restored = reverse_transforms_to_path(
                        stored_path,
                        restored_path,
                        _locator_transforms(session, locator),
                    )
                    if restored.sha256 != asset_hash:
                        integrity_errors.append(
                            f"copy id={copy.id} pool={pool_id}: "
                            f"{restored.sha256.hex()} != {asset_hash.hex()}"
                        )
                        continue
                    restored_path.replace(output_path)
                    return RestoreResult(
                        asset_hash=asset_hash,
                        pool_id=pool_id,
                        copy_id=copy.id,
                        output_path=output_path,
                        size_bytes=restored.size_bytes,
                    )
            except ArchiveRestoreError as exc:
                integrity_errors.append(f"copy id={copy.id} pool={pool_id}: {exc}")
                continue
            except StagingError as exc:
                integrity_errors.append(f"copy id={copy.id} pool={pool_id}: {exc}")
                continue

    if integrity_errors:
        raise RestoreIntegrityError(
            f"all candidate restores for asset {asset_hash.hex()} failed integrity: "
            + "; ".join(integrity_errors)
        )
    raise RestoreSourceUnavailable(
        f"no healthy locator for asset {asset_hash.hex()} in artifactclass {artifactclass!r}"
    )


def _check_asset_restore_allowed(
    session: Session,
    asset_hash: bytes,
    *,
    force_suspect: bool,
) -> None:
    asset = session.get(LogicalAsset, asset_hash)
    if asset is None:
        return
    if asset.validity != AssetValidity.SUSPECT or force_suspect:
        return
    note = f": {asset.validity_note}" if asset.validity_note else ""
    raise RestoreSuspectAsset(
        f"asset {asset_hash.hex()} is flagged suspect{note}; use --force to restore anyway"
    )


def resolve_member_asset_hash(
    session: Session,
    *,
    artifactclass: str,
    member_name: str,
) -> bytes:
    """Resolve a customer escaped member name to a logical asset hash."""
    try:
        canonical = escape_member_name(unescape_member_name(member_name))
    except MemberNameError as exc:
        raise RestoreNameError(f"invalid escaped member name {member_name!r}: {exc}") from exc

    hashes: set[bytes] = set(
        session.scalars(
            select(StagingTransform.logical_asset_hash).where(
                StagingTransform.artifactclass == artifactclass,
                (
                    (StagingTransform.original_member_path == canonical)
                    | (StagingTransform.stored_member_path == canonical)
                ),
            )
        )
    )
    hashes.update(
        session.scalars(
            select(BundleMember.logical_asset_hash)
            .join(Bundle, Bundle.id == BundleMember.bundle_id)
            .where(
                Bundle.artifactclass == artifactclass,
                BundleMember.member_path == canonical,
            )
        )
    )
    if not hashes:
        raise RestoreNameError(
            f"no catalog member {member_name!r} in artifactclass {artifactclass!r}"
        )
    if len(hashes) > 1:
        raise RestoreNameError(
            f"member name {member_name!r} is ambiguous in artifactclass {artifactclass!r}"
        )
    return next(iter(hashes))


def _locator_transforms(
    session: Session,
    locator: AssetLocator,
) -> list[StagingTransform]:
    if locator.bundle_id is None:
        return []
    member_id = session.scalar(
        select(StagingTransform.bundle_member_id)
        .where(
            StagingTransform.bundle_id == locator.bundle_id,
            StagingTransform.logical_asset_hash == locator.logical_asset_hash,
            StagingTransform.stored_member_path == locator.member_path,
        )
        .order_by(StagingTransform.step_order.desc())
        .limit(1)
    )
    if member_id is None:
        return []
    return list(
        session.scalars(
            select(StagingTransform)
            .where(
                StagingTransform.bundle_member_id == member_id,
            )
            .order_by(StagingTransform.step_order)
        )
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


def _extract_d2_to_path(
    locator: AssetLocator,
    copy: Copy,
    backend: StorageBackend,
    destination: Path,
) -> None:
    if "block_range" in locator.native_locator:
        raw_range = locator.native_locator["block_range"]
        if (
            isinstance(raw_range, list)
            and len(raw_range) == 2
            and "size_bytes" in locator.native_locator
        ):
            data_start = int(raw_range[0])
            size = int(locator.native_locator["size_bytes"])
            _copy_backend_range_to_path(
                backend,
                copy.native_locator,
                data_start,
                data_start + size,
                destination,
            )
            return
    with tempfile.TemporaryDirectory(prefix="sutradhara-d2-restore-") as raw_tmp:
        object_path = Path(raw_tmp) / "copy.tar"
        _materialize_copy_to_path(backend, copy, object_path)
        _extract_tar_member_to_path(object_path, locator.member_path, destination)


def _extract_tar_member_to_path(
    tar_path: Path,
    member_path: str,
    destination: Path,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, mode="r:*") as tar:
        handle = tar.extractfile(member_path)
        if handle is None:
            raise ArchiveRestoreError(f"tar member {member_path!r} is not a file")
        with destination.open("wb") as raw_out:
            shutil.copyfileobj(handle, raw_out, length=1024 * 1024)


def _try_extract_from_local_archive_to_path(
    backend: StorageBackend,
    copy: Copy,
    locator: AssetLocator,
    destination: Path,
) -> bool:
    header_len_raw = backend.read_range(copy.native_locator, ByteRange(0, 8))
    if len(header_len_raw) != 8:
        return False
    header_len = int.from_bytes(header_len_raw, "big")
    if header_len <= 0 or header_len > _MAX_LOCAL_ARCHIVE_HEADER_BYTES:
        return False
    try:
        header_bytes = backend.read_range(copy.native_locator, ByteRange(8, 8 + header_len))
        header = json.loads(header_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return False
    if not isinstance(header, dict) or header.get("format") != "sutradhara-local-archive-v1":
        return False
    if "offset" not in locator.native_locator or "size_bytes" not in locator.native_locator:
        raise ArchiveRestoreError("local archive locator is missing offset/size_bytes")
    offset = int(locator.native_locator["offset"])
    size = int(locator.native_locator["size_bytes"])
    if offset < 0 or size < 0:
        raise ArchiveRestoreError("local archive locator has negative offset/size_bytes")
    payload_start = 8 + header_len
    _copy_backend_range_to_path(
        backend,
        copy.native_locator,
        payload_start + offset,
        payload_start + offset + size,
        destination,
    )
    return True


def _copy_backend_range_to_path(
    backend: StorageBackend,
    locator: dict[str, Any],
    start: int,
    end: int,
    destination: Path,
) -> None:
    if start < 0 or end < start:
        raise ArchiveRestoreError(f"invalid backend byte range [{start}, {end})")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as handle:
        for cursor in range(start, end, RAO_CHUNK_SIZE):
            chunk_end = min(cursor + RAO_CHUNK_SIZE, end)
            chunk = backend.read_range(locator, ByteRange(cursor, chunk_end))
            expected = chunk_end - cursor
            if len(chunk) != expected:
                raise ArchiveRestoreError(
                    f"backend returned {len(chunk)} bytes for range "
                    f"[{cursor}, {chunk_end}), expected {expected}"
                )
            handle.write(chunk)


def _read_whole(backend: StorageBackend, copy: Copy) -> bytes:
    return backend.read_range(copy.native_locator, ByteRange(0, 0))


def _materialize_copy_to_path(
    backend: StorageBackend,
    copy: Copy,
    destination: Path,
) -> None:
    size = copy.storage_metadata.get("stored_size_bytes")
    if isinstance(size, int) and size >= 0:
        _copy_backend_range_to_path(backend, copy.native_locator, 0, size, destination)
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


def _format_plugin(storage_metadata: dict[str, Any]) -> str | None:
    value = storage_metadata.get("format_plugin")
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ArchiveRestoreError("copy format_plugin metadata must be a non-empty string")
    return value


def _run_rem(cmd: list[str]) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise ArchiveRestoreError(
            f"rem archive extract failed (exit {result.returncode}): "
            f"stdout={result.stdout.strip()[:500]!r} "
            f"stderr={result.stderr.strip()[:500]!r}"
        )


def _copy_restored_member(dest_dir: Path, member_path: str, destination: Path) -> None:
    candidate = dest_dir / member_path if member_path else None
    if candidate is not None and candidate.is_file():
        source = candidate
    else:
        files = [path for path in dest_dir.rglob("*") if path.is_file()]
        if len(files) != 1:
            raise ArchiveRestoreError(
                f"rem restore expected one file for member {member_path!r}, found {len(files)}"
            )
        source = files[0]
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as raw_in, destination.open("wb") as raw_out:
        shutil.copyfileobj(raw_in, raw_out, length=1024 * 1024)
