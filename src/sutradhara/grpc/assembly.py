"""Assemble a streamed gRPC intake into the standard receive BagIt layout.

CommitIntake calls this after every streamed payload unit has a receipt. The
resulting bag is intentionally indistinguishable from a local ``sutra receive``
bag: tag files are written with ``sutradhara_receive`` helpers, ``intake.json``
is the final handoff sentinel, and no terminal verification marker is created.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
from collections.abc import Iterable, Mapping
from contextlib import suppress
from pathlib import Path
from typing import Any

from sutradhara.grpc.store import GrpcIntake
from sutradhara_receive import (
    BAG_PROFILE,
    CANONICALIZATION_VERSION,
    PACKAGE_INDEX_NAME,
    PACKAGE_PROFILE_HASH,
    PACKAGE_PROFILE_VERSION,
    ReceiveError,
    bag_info_metadata,
    build_package_index,
    canonicalize_manifest_path,
    manifest_digest as receive_manifest_digest,
    write_bagit_files,
)


class AssemblyError(ReceiveError):
    """Raised when streamed commit facts cannot produce a valid receive bag."""


def assemble_committed_bag(
    intake_dir: Path,
    *,
    row: GrpcIntake,
    files: Iterable[Any],
    receive_facts: Any,
    package_indexes: Iterable[Any],
) -> None:
    """Write BagIt tag files and the final intake sentinel for a gRPC intake."""

    _check_receive_fact_skew(receive_facts)
    manifest_entries = {
        canonicalize_manifest_path(str(item.relpath)): str(item.client_sha256).lower()
        for item in files
    }
    packages = [_package_index_entry(item) for item in package_indexes]
    extra_tags: tuple[str, ...] = ()
    if packages:
        package_index = build_package_index(packages)
        if package_index is None:
            raise AssemblyError("package index unexpectedly empty")
        _atomic_write_json(
            intake_dir / PACKAGE_INDEX_NAME,
            package_index,
        )
        extra_tags = (PACKAGE_INDEX_NAME,)
    else:
        with suppress(FileNotFoundError):
            (intake_dir / PACKAGE_INDEX_NAME).unlink()

    total_bytes = sum(int(item.bytes) for item in files)
    file_count = len(manifest_entries)
    started_at = _aware(row.created_at)
    write_bagit_files(
        intake_dir,
        entries=manifest_entries,
        metadata=bag_info_metadata(
            intake_id=row.intake_id,
            source_kind=row.source_kind,
            operator=row.operator,
            source_ref=row.source_ref,
            artifactclass=row.artifactclass,
            label=row.label,
            started_at=started_at,
            file_count=file_count,
            total_bytes=total_bytes,
            skipped_count=int(receive_facts.skipped_count),
        ),
        extra_tag_files=extra_tags,
    )

    incoming = intake_dir / ".incoming"
    if incoming.exists():
        shutil.rmtree(incoming)
    incoming.mkdir(exist_ok=True)
    _fsync_dir(incoming)

    sentinel = {
        "intake_id": row.intake_id,
        "status": "complete",
        "bag_profile": BAG_PROFILE,
        "created_at": dt.datetime.now(dt.UTC).isoformat(),
        "transport": "grpc-stream",
    }
    _atomic_write_json(intake_dir / "intake.json", sentinel)
    _fsync_dir(intake_dir)


def manifest_digest(files: Iterable[Any]) -> str:
    """Return sha256 over sorted manifest commit entries."""

    return receive_manifest_digest(files)


def _check_receive_fact_skew(receive_facts: Any) -> None:
    if receive_facts.canonicalization_version != CANONICALIZATION_VERSION:
        raise AssemblyError(
            "canonicalization version mismatch: "
            f"expected {CANONICALIZATION_VERSION}, "
            f"actual {receive_facts.canonicalization_version!r}"
        )
    profile = str(receive_facts.package_profile_version or "")
    if profile and profile != PACKAGE_PROFILE_VERSION:
        raise AssemblyError(
            "package profile mismatch: "
            f"expected {PACKAGE_PROFILE_VERSION}, actual {profile!r}"
        )


def _package_index_entry(package: Any) -> dict[str, Any]:
    stored = canonicalize_manifest_path(str(package.stored_member_path))
    logical = canonicalize_manifest_path(str(package.logical_member_path))
    members = [_member_entry(member) for member in package.members]
    return {
        "logical_member_path": logical,
        "stored_member_path": stored,
        "sha256": str(package.sha256).lower(),
        "profile": PACKAGE_PROFILE_VERSION,
        "members": sorted(members, key=lambda item: item["member"]),
    }


def _member_entry(member: Any) -> dict[str, Any]:
    member_type = str(member.type)
    base: dict[str, Any] = {
        "member": canonicalize_manifest_path(str(member.member)),
        "type": member_type,
    }
    if member_type == "file":
        base["length"] = int(member.length)
        base["sha256"] = str(member.sha256)
        base["data_offset"] = int(member.data_offset)
    elif member_type in {"directory", "symlink"}:
        base["length"] = 0
        base["sha256"] = None
        base["data_offset"] = None
        if member_type == "symlink":
            base["linkname"] = str(member.linkname)
    else:
        raise AssemblyError(f"unsupported package member type: {member_type!r}")
    return base


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    data = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    _atomic_write_bytes(path, data.encode("utf-8"))


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _fsync_dir(path.parent)
    temp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temp.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        temp.replace(path)
        _fsync_dir(path.parent)
    except Exception:
        with suppress(FileNotFoundError):
            temp.unlink()
        raise


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _aware(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)
