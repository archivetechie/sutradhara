"""Staging-time transforms for archive ingest and restore.

The archive accumulator stores logical identity separately from the bytes sent
to rem/d2. This module owns the copy-on-write stage that can merge AppleDouble
sidecars or compress selected members before enqueue, plus the inverse restore
steps for reversible transforms.
"""

from __future__ import annotations

import dataclasses
import hashlib
import os
import shutil
import struct
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path, PurePosixPath
from typing import Any

import zstandard as zstd
from sqlalchemy.orm import Session

from sutradhara.archive_bundle import (
    enqueue_artifact,
    extract_member_tag,
    get_or_create_open_bundle,
    hold_bundle,
    record_staging_transform,
    tag_member_path,
)
from sutradhara.artifactclass_policy import (
    AppleDoubleStagingPolicy,
    ArtifactClassPolicyError,
    CompressionStagingPolicy,
    StagingPolicy,
    staging_policy_from_json,
)
from sutradhara.catalog.models import ArtifactClassPolicyRecord, LogicalAsset, StagingTransform
from sutradhara_receive.member_name import escape_path_name, escape_path_text

APPLEDOUBLE_MERGE_KIND = "appledouble-merge-v1"
ZSTD_FILE_KIND = "zstd-file-v1"
_APPLEDOUBLE_MAGIC = 0x00051607
_APPLEDOUBLE_VERSION = 0x00020000
_APPLEDOUBLE_RESOURCE_FORK = 2
_APPLEDOUBLE_FINDER_INFO = 9
_APPLEDOUBLE_FINDER_INFO_BYTES = 32
_APPLEDOUBLE_ATTR_MAGIC = 0x41545452
_APPLEDOUBLE_ATTR_MAGIC_BYTES = b"ATTR"
_APPLEDOUBLE_ATTR_HEADER_BYTES = 36
_APPLEDOUBLE_ATTR_ENTRY_FIXED_BYTES = 11
_ZSTD_SUFFIX = ".zst"


class StagingError(Exception):
    """Base class for staging transform failures."""


class StagingHeld(StagingError):
    """A staging failure should hold the bundle for review."""

    def __init__(self, summary: dict[str, Any]) -> None:
        super().__init__("staging transform held bundle")
        self.summary = summary


@dataclass(frozen=True)
class TransformSpec:
    """Pending transform record created before the bundle member row exists."""

    kind: str
    reversible: bool
    original_member_path: str
    stored_member_path: str
    original_size_bytes: int
    stored_size_bytes: int
    original_sha256: bytes
    stored_sha256: bytes
    parameters: dict[str, Any]
    result: dict[str, Any]


@dataclass(frozen=True)
class StagedArtifact:
    """The staged artifact that should be enqueued into the open bundle."""

    original_path: Path
    staged_path: Path
    logical_member_path: str
    stored_member_path: str
    logical_sha256: bytes
    staged_sha256: bytes
    logical_size_bytes: int
    staged_size_bytes: int
    transforms: tuple[TransformSpec, ...]
    pfr_original: bool
    # The bundle the member actually landed in. An include-alone member routes
    # to its own funnel bundle, not the group accumulator, so callers must
    # report this id rather than re-deriving the accumulator (P1 residue F6).
    bundle_id: str | None = None


@dataclass(frozen=True)
class RestoredFile:
    """A streamed restore transform result."""

    sha256: bytes
    size_bytes: int


@dataclass(frozen=True)
class _AppleDoubleEntry:
    offset: int
    data: bytes


def stage_and_enqueue_artifact(
    session: Session,
    *,
    artifactclass: str,
    policy: ArtifactClassPolicyRecord,
    source_path: Path | str,
    staging_root: Path | str,
    member_path: str | None = None,
    bundle_id: str | None = None,
) -> StagedArtifact:
    """Stage one source path, enqueue it, and persist transform records.

    The original file's SHA-256 is the `LogicalAsset` identity. Any staged bytes
    produced by transforms become the bundle member's `file_sha256`.
    """
    staging_policy = staging_policy_from_json(policy.staging_config)
    try:
        staged = stage_artifact(
            source_path=source_path,
            staging_root=staging_root,
            policy=staging_policy,
            member_path=member_path,
        )
    except StagingHeld as exc:
        bundle, _ = get_or_create_open_bundle(
            session,
            artifactclass=artifactclass,
            policy=policy,
            bundle_id=bundle_id,
        )
        hold_bundle(session, bundle, summary=exc.summary)
        raise

    asset = session.get(LogicalAsset, staged.logical_sha256)
    if asset is None:
        session.add(
            LogicalAsset(content_sha256=staged.logical_sha256, size_bytes=staged.logical_size_bytes)
        )
        session.flush()

    source_metadata = {
        "logical_path": staged.logical_member_path,
        "stored_member_path": staged.stored_member_path,
        "pfr_original": staged.pfr_original,
        "transforms": [{"kind": transform.kind} for transform in staged.transforms],
    }
    _, member, created = enqueue_artifact(
        session,
        artifactclass=artifactclass,
        policy=policy,
        logical_asset_hash=staged.logical_sha256,
        source_path=staged.staged_path,
        member_path=staged.stored_member_path,
        member_path_is_escaped=True,
        bundle_id=bundle_id,
        source_metadata=source_metadata,
    )
    staged = dataclasses.replace(staged, bundle_id=member.bundle_id)
    if member.member_path != staged.stored_member_path:
        # The naming ladder disambiguated this member. Staged files keep their
        # on-disk names; the transform chain and the source_metadata copy are
        # re-keyed onto the enqueue-returned name before recording.
        #
        # The tag is a *catalog-name* artifact, so it goes on every catalog
        # name in the chain and on nothing else:
        #
        # * every step's ``stored_member_path`` is tagged — not just the final
        #   one. The final step needs it for the member equality
        #   `record_staging_transform` asserts, and every step needs it for the
        #   (bundle_id, stored_member_path, step_order) unique surface: two
        #   co-resident classes staging the same logical path through the same
        #   two-step chain (AppleDouble merge then zstd) collide at
        #   ``step_order = 0`` unless the intermediate name is tagged too.
        # * ``original_member_path`` is tagged for steps after the first, so
        #   step N still links to step N-1's stored name.
        # * **step 0's ``original_member_path`` is left untagged.** It is the
        #   one name in the chain that is not a catalog name: it is the
        #   source's own logical name, the same string
        #   ``source_metadata["logical_path"]`` holds and the customer manifest
        #   prints as ``member_name`` (design §5). Tagging it recorded a
        #   filename no file ever carried, and made the chain contradict the
        #   receipt about the member's logical name.
        tag = extract_member_tag(staged.stored_member_path, member.member_path)
        if tag is None:
            raise StagingError(
                f"cannot re-key transforms: {staged.stored_member_path!r} does not "
                f"map onto recorded member name {member.member_path!r}"
            )
        staged = dataclasses.replace(
            staged,
            stored_member_path=member.member_path,
            transforms=tuple(
                dataclasses.replace(
                    transform,
                    original_member_path=(
                        transform.original_member_path
                        if index == 0
                        else tag_member_path(transform.original_member_path, tag)
                    ),
                    stored_member_path=tag_member_path(transform.stored_member_path, tag),
                )
                for index, transform in enumerate(staged.transforms)
            ),
        )
        if created:
            member.source_metadata = {
                **(member.source_metadata or {}),
                "stored_member_path": member.member_path,
            }
            session.flush()
    if not created and member.transforms:
        return staged

    transform_refs: list[dict[str, Any]] = []
    final_index = len(staged.transforms) - 1
    for index, transform in enumerate(staged.transforms):
        row = record_staging_transform(
            session,
            member=member,
            artifactclass=artifactclass,
            step_order=index,
            kind=transform.kind,
            reversible=transform.reversible,
            original_member_path=transform.original_member_path,
            stored_member_path=transform.stored_member_path,
            original_size_bytes=transform.original_size_bytes,
            stored_size_bytes=transform.stored_size_bytes,
            original_sha256=transform.original_sha256,
            stored_sha256=transform.stored_sha256,
            parameters=transform.parameters,
            result=transform.result,
            is_final=index == final_index,
        )
        transform_refs.append({"kind": row.kind, "id": row.id})
    if transform_refs:
        member.source_metadata = {
            **(member.source_metadata or {}),
            "transforms": transform_refs,
        }
        session.flush()
    return staged


def stage_artifact(
    *,
    source_path: Path | str,
    staging_root: Path | str,
    policy: StagingPolicy,
    member_path: str | None = None,
) -> StagedArtifact:
    """Return the staged file and transform specs for one source file."""
    source = Path(source_path)
    if not source.is_file():
        raise StagingError(f"staging currently requires a regular file: {source}")
    if policy.appledouble.action == "merge-to-xattrs" and _is_appledouble_sidecar(source):
        _handle_appledouble_sidecar_source(policy.appledouble, source)
    logical_member_path = (
        escape_path_name(source) if member_path is None else escape_path_text(member_path)
    )
    original_size = source.stat().st_size
    original_hash = _sha256_file(source)
    transforms: list[TransformSpec] = []

    needs_copy = policy.appledouble.action == "merge-to-xattrs" or _should_compress(
        policy.compression, logical_member_path, original_size
    )
    current_path = source
    current_member_path = logical_member_path
    current_hash = original_hash
    current_size = original_size
    root = Path(staging_root)

    if needs_copy:
        current_path = _safe_staging_path(root, current_member_path)
        current_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, current_path)

    if policy.appledouble.action == "merge-to-xattrs":
        transform = _merge_appledouble(
            source=source,
            staged=current_path,
            member_path=current_member_path,
            original_hash=original_hash,
            original_size=original_size,
            policy=policy.appledouble,
        )
        transforms.append(transform)

    if _should_compress(policy.compression, logical_member_path, original_size):
        compressed_member_path = f"{logical_member_path}{_ZSTD_SUFFIX}"
        compressed_path = _safe_staging_path(root, compressed_member_path)
        compressed_path.parent.mkdir(parents=True, exist_ok=True)
        _compress_zstd(
            current_path,
            compressed_path,
            level=_required_compression_level(policy.compression),
        )
        compressed_hash = _sha256_file(compressed_path)
        compressed_size = compressed_path.stat().st_size
        transforms.append(
            TransformSpec(
                kind=ZSTD_FILE_KIND,
                reversible=True,
                original_member_path=current_member_path,
                stored_member_path=compressed_member_path,
                original_size_bytes=current_size,
                stored_size_bytes=compressed_size,
                original_sha256=current_hash,
                stored_sha256=compressed_hash,
                parameters={
                    "codec": "zstd",
                    "level": _required_compression_level(policy.compression),
                    "threads": 0,
                    "suffix": _ZSTD_SUFFIX,
                    "implementation": "python-zstandard",
                    "implementation_version": zstd.__version__,
                },
                result={"pfr_original": False},
            )
        )
        current_path = compressed_path
        current_member_path = compressed_member_path
        current_hash = compressed_hash
        current_size = compressed_size

    return StagedArtifact(
        original_path=source,
        staged_path=current_path,
        logical_member_path=logical_member_path,
        stored_member_path=current_member_path,
        logical_sha256=original_hash,
        staged_sha256=current_hash,
        logical_size_bytes=original_size,
        staged_size_bytes=current_size,
        transforms=tuple(transforms),
        pfr_original=not any(transform.kind == ZSTD_FILE_KIND for transform in transforms),
    )


def reverse_transforms_to_path(
    source: Path,
    destination: Path,
    transforms: Iterable[StagingTransform],
) -> RestoredFile:
    """Apply reversible transform inverses from one file path to another."""
    reversible = [
        transform
        for transform in sorted(transforms, key=lambda item: item.step_order, reverse=True)
        if transform.reversible
    ]
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not reversible:
        _copy_file(source, destination)
        return RestoredFile(
            sha256=_sha256_file(destination),
            size_bytes=destination.stat().st_size,
        )

    with tempfile.TemporaryDirectory(
        prefix=".sutradhara-transform-",
        dir=destination.parent,
    ) as raw_tmp:
        temp_dir = Path(raw_tmp)
        current = source
        for index, transform in enumerate(reversible):
            target = destination if index == len(reversible) - 1 else temp_dir / f"step-{index}"
            if transform.kind == ZSTD_FILE_KIND:
                try:
                    _decompress_zstd_to_path(current, target)
                except zstd.ZstdError as exc:
                    raise StagingError("zstd decompression failed during restore") from exc
                current = target
                continue
            raise StagingError(f"unsupported reversible transform {transform.kind!r}")
    return RestoredFile(
        sha256=_sha256_file(destination),
        size_bytes=destination.stat().st_size,
    )


def _merge_appledouble(
    *,
    source: Path,
    staged: Path,
    member_path: str,
    original_hash: bytes,
    original_size: int,
    policy: AppleDoubleStagingPolicy,
) -> TransformSpec:
    sidecar = source.with_name(f"._{source.name}")
    if not sidecar.exists():
        return TransformSpec(
            kind=APPLEDOUBLE_MERGE_KIND,
            reversible=False,
            original_member_path=member_path,
            stored_member_path=member_path,
            original_size_bytes=original_size,
            stored_size_bytes=original_size,
            original_sha256=original_hash,
            stored_sha256=original_hash,
            parameters={"action": "merge-to-xattrs", "tool": policy.tool},
            result={"merged": False, "reason": "sidecar-not-found"},
        )
    if not sidecar.is_file():
        _handle_appledouble_error(policy, f"AppleDouble sidecar is not a file: {sidecar}")

    raw = sidecar.read_bytes()
    try:
        entries = _parse_appledouble(raw)
        xattrs = _apply_appledouble_entries(staged, raw, entries)
    except Exception as exc:
        _handle_appledouble_error(policy, f"AppleDouble merge failed for {sidecar}: {exc}")
    staged_sidecar = staged.with_name(f"._{staged.name}")
    if staged_sidecar.exists():
        staged_sidecar.unlink()
    resource_fork = entries.get(_APPLEDOUBLE_RESOURCE_FORK)
    resource_fork_bytes = len(resource_fork.data) if resource_fork is not None else 0
    return TransformSpec(
        kind=APPLEDOUBLE_MERGE_KIND,
        reversible=False,
        original_member_path=member_path,
        stored_member_path=member_path,
        original_size_bytes=original_size,
        stored_size_bytes=original_size,
        original_sha256=original_hash,
        stored_sha256=original_hash,
        parameters={
            "action": "merge-to-xattrs",
            "tool": policy.tool,
            "sidecar_path": str(sidecar),
            "sidecar_sha256": hashlib.sha256(raw).hexdigest(),
        },
        result={
            "merged": True,
            "consumed_sidecar": True,
            "sidecar_path": str(sidecar),
            "xattrs": xattrs,
            "resource_fork_bytes": resource_fork_bytes,
        },
    )


def _is_appledouble_sidecar(source: Path) -> bool:
    return source.name.startswith("._") and len(source.name) > 2


def _handle_appledouble_sidecar_source(policy: AppleDoubleStagingPolicy, source: Path) -> None:
    sibling = source.with_name(source.name[2:])
    reason = (
        "appledouble-sidecar-consumed"
        if sibling.exists()
        else "appledouble-sidecar-without-data-fork"
    )
    _handle_appledouble_error(
        policy,
        f"AppleDouble sidecar {source} is metadata for {sibling}, not an archive member",
        reason=reason,
    )


def _handle_appledouble_error(
    policy: AppleDoubleStagingPolicy,
    message: str,
    *,
    reason: str = "appledouble-merge-failed",
) -> None:
    summary = {
        "clusters": [
            {
                "prefix": "",
                "reason": reason,
                "count": 1,
                "bytes_total": 0,
                "samples": [message],
                "proposed_default": "review",
            }
        ],
        "exclusions": [],
    }
    if policy.on_error == "hold":
        raise StagingHeld(summary)
    raise StagingError(message)


def _parse_appledouble(raw: bytes) -> dict[int, _AppleDoubleEntry]:
    if len(raw) < 26:
        raise StagingError("AppleDouble sidecar is too short")
    magic, version = struct.unpack_from(">II", raw, 0)
    if magic != _APPLEDOUBLE_MAGIC or version != _APPLEDOUBLE_VERSION:
        raise StagingError("AppleDouble sidecar has invalid magic/version")
    entry_count = struct.unpack_from(">H", raw, 24)[0]
    table_end = 26 + entry_count * 12
    if table_end > len(raw):
        raise StagingError("AppleDouble entry table is truncated")
    entries: dict[int, _AppleDoubleEntry] = {}
    for index in range(entry_count):
        entry_id, offset, length = struct.unpack_from(">III", raw, 26 + index * 12)
        end = offset + length
        if offset > len(raw) or end > len(raw) or end < offset:
            raise StagingError(f"AppleDouble entry {entry_id} points outside the sidecar")
        entries[entry_id] = _AppleDoubleEntry(offset=offset, data=raw[offset:end])
    if not any(
        entry in entries for entry in {_APPLEDOUBLE_RESOURCE_FORK, _APPLEDOUBLE_FINDER_INFO}
    ):
        raise StagingError("AppleDouble sidecar has no supported metadata entries")
    return entries


def _apply_appledouble_entries(
    path: Path,
    raw: bytes,
    entries: dict[int, _AppleDoubleEntry],
) -> list[str]:
    applied: list[str] = []
    if _APPLEDOUBLE_RESOURCE_FORK in entries:
        name = "user.com.apple.ResourceFork"
        os.setxattr(path, name, entries[_APPLEDOUBLE_RESOURCE_FORK].data)
        applied.append(name)
    if _APPLEDOUBLE_FINDER_INFO in entries:
        name = "user.com.apple.FinderInfo"
        finder_entry = entries[_APPLEDOUBLE_FINDER_INFO]
        if len(finder_entry.data) < _APPLEDOUBLE_FINDER_INFO_BYTES:
            raise StagingError("AppleDouble FinderInfo entry is shorter than 32 bytes")
        os.setxattr(path, name, finder_entry.data[:_APPLEDOUBLE_FINDER_INFO_BYTES])
        applied.append(name)
        for attr_name, value in _parse_appledouble_finder_xattrs(raw, finder_entry):
            os.setxattr(path, attr_name, value)
            applied.append(attr_name)
    return applied


def _parse_appledouble_finder_xattrs(
    raw: bytes,
    finder_entry: _AppleDoubleEntry,
) -> list[tuple[str, bytes]]:
    trailer_start = finder_entry.offset + _APPLEDOUBLE_FINDER_INFO_BYTES
    trailer_end = finder_entry.offset + len(finder_entry.data)
    trailer = raw[trailer_start:trailer_end]
    if not trailer or not trailer.strip(b"\0"):
        return []

    attr_start = trailer_start
    if not trailer.startswith(_APPLEDOUBLE_ATTR_MAGIC_BYTES):
        if trailer.startswith(b"\0\0" + _APPLEDOUBLE_ATTR_MAGIC_BYTES):
            attr_start += 2
        else:
            raise StagingError("AppleDouble FinderInfo ATTR trailer has invalid magic")
    return _parse_appledouble_attrs(raw, attr_start)


def _parse_appledouble_attrs(raw: bytes, attr_start: int) -> list[tuple[str, bytes]]:
    header_end = attr_start + _APPLEDOUBLE_ATTR_HEADER_BYTES
    if header_end > len(raw):
        raise StagingError("AppleDouble ATTR header is truncated")
    (
        magic,
        _debug_tag,
        _total_size,
        data_start,
        data_length,
        _reserved0,
        _reserved1,
        _reserved2,
        _flags,
        attr_count,
    ) = struct.unpack_from(">IIIII3IHH", raw, attr_start)
    if magic != _APPLEDOUBLE_ATTR_MAGIC:
        raise StagingError("AppleDouble ATTR header has invalid magic")
    if data_start > len(raw) or data_start + data_length > len(raw):
        raise StagingError("AppleDouble ATTR data points outside the sidecar")

    attrs: list[tuple[str, bytes]] = []
    entry_offset = header_end
    for _index in range(attr_count):
        fixed_end = entry_offset + _APPLEDOUBLE_ATTR_ENTRY_FIXED_BYTES
        if fixed_end > len(raw):
            raise StagingError("AppleDouble ATTR entry is truncated")
        value_offset, value_length, _entry_flags, name_length = struct.unpack_from(
            ">IIHB",
            raw,
            entry_offset,
        )
        if name_length < 2 or name_length > 129:
            raise StagingError("AppleDouble ATTR entry has invalid name length")
        name_start = fixed_end
        name_end = name_start + name_length
        if name_end > len(raw):
            raise StagingError("AppleDouble ATTR entry name is truncated")
        raw_name = raw[name_start:name_end]
        if raw_name[-1] != 0:
            raise StagingError("AppleDouble ATTR entry name is not NUL-terminated")
        try:
            name = raw_name[:-1].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise StagingError("AppleDouble ATTR entry name is not UTF-8") from exc
        value_start = value_offset
        value_end = value_start + value_length
        if value_start > len(raw) or value_end > len(raw) or value_end < value_start:
            value_start = attr_start + value_offset
            value_end = value_start + value_length
        if value_start > len(raw) or value_end > len(raw) or value_end < value_start:
            raise StagingError("AppleDouble ATTR entry data points outside the sidecar")
        attrs.append((_linux_xattr_name(name), raw[value_start:value_end]))
        entry_offset += _align4(_APPLEDOUBLE_ATTR_ENTRY_FIXED_BYTES + name_length)
    return attrs


def _linux_xattr_name(name: str) -> str:
    if name.startswith(("user.", "system.", "security.", "trusted.")):
        return name
    return f"user.{name}"


def _align4(value: int) -> int:
    return (value + 3) & ~3


def _should_compress(
    policy: CompressionStagingPolicy,
    member_path: str,
    size_bytes: int,
) -> bool:
    if policy.codec == "off":
        return False
    if policy.codec != "zstd":
        raise ArtifactClassPolicyError(f"unsupported compression codec {policy.codec!r}")
    if policy.min_bytes is not None and size_bytes < policy.min_bytes:
        return False
    if not policy.globs:
        return True
    return any(_glob_matches(member_path, pattern) for pattern in policy.globs)


def _glob_matches(member_path: str, pattern: str) -> bool:
    normalized = PurePosixPath(member_path).as_posix()
    if fnmatchcase(normalized, pattern):
        return True
    if pattern.startswith("**/") and fnmatchcase(normalized, pattern[3:]):
        return True
    return PurePosixPath(normalized).match(pattern)


def _required_compression_level(policy: CompressionStagingPolicy) -> int:
    if policy.level is None:
        raise StagingError("zstd compression requires a pinned level")
    return policy.level


def _compress_zstd(source: Path, destination: Path, *, level: int) -> None:
    compressor = zstd.ZstdCompressor(
        level=level,
        threads=0,
        write_checksum=True,
        write_content_size=True,
    )
    with source.open("rb") as raw_in, destination.open("wb") as raw_out:
        compressor.copy_stream(raw_in, raw_out)


def _decompress_zstd_to_path(source: Path, destination: Path) -> None:
    with source.open("rb") as raw_in, destination.open("wb") as raw_out:
        zstd.ZstdDecompressor().copy_stream(raw_in, raw_out)


def _copy_file(source: Path, destination: Path) -> None:
    if source == destination:
        return
    with source.open("rb") as raw_in, destination.open("wb") as raw_out:
        shutil.copyfileobj(raw_in, raw_out, length=1024 * 1024)


def _safe_staging_path(root: Path, member_path: str) -> Path:
    pure = PurePosixPath(member_path)
    if pure.is_absolute() or any(part == ".." for part in pure.parts):
        raise StagingError(f"unsafe member path for staging: {member_path!r}")
    return root.joinpath(*pure.parts)


def _sha256_file(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()
