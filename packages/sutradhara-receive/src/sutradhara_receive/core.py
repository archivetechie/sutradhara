"""Filesystem receive core for Sutradhara's source-agnostic intake front door.

This module is shared by edge-side `sutra receive` and server-side intake registration.
It keeps the contract-critical parts in one dependency-light place: canonical
member paths, BagIt manifests, safe payload paths, atomic writes, resumable
landing directories, source quiescence checks, and server release markers.
"""

from __future__ import annotations

import datetime as dt
import fnmatch
import hashlib
import json
import os
import posixpath
import queue
import re
import shutil
import stat
import tarfile
import threading
import time
import unicodedata
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, cast

from sutradhara_receive.member_name import (
    MemberNameError,
    escape_member_name,
    unescape_member_name,
)

try:
    from sutradhara_receive import _native
except ImportError:  # pragma: no cover - source-tree fallback before native build.
    _native = None

RECEIVE_VERSION = "receive-v2"
RECEIVE_PACKAGE_NAME = "sutradhara-receive"
RECEIVE_PACKAGE_VERSION = "0.1.0"
with suppress(PackageNotFoundError):
    RECEIVE_PACKAGE_VERSION = version(RECEIVE_PACKAGE_NAME)
if _native is not None and RECEIVE_PACKAGE_VERSION != _native.RECEIVE_PACKAGE_VERSION:
    raise RuntimeError(
        "sutradhara-receive package metadata version "
        f"{RECEIVE_PACKAGE_VERSION!r} does not match native core version "
        f"{_native.RECEIVE_PACKAGE_VERSION!r}"
    )
RECEIVE_PACKAGE = f"{RECEIVE_PACKAGE_NAME}/{RECEIVE_PACKAGE_VERSION}"
SUPPORTED_RECEIVE_PACKAGES = frozenset({RECEIVE_PACKAGE})
CANONICALIZATION_VERSION = "receive-bagit-path-v2"
PACKAGE_PROFILE_VERSION = "package-tar-v1"
PACKAGE_GLOBS = ("*.fcpbundle", "*.photoslibrary", "*.imovielibrary", "*.app")
BAG_PROFILE = "bagit-1.0"
BAGIT_TEXT = "BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n"
DATA_DIR_NAME = "data"
MANIFEST_NAME = "manifest-sha256.txt"
BAG_INFO_NAME = "bag-info.txt"
BAGIT_NAME = "bagit.txt"
TAGMANIFEST_NAME = "tagmanifest-sha256.txt"
PACKAGE_INDEX_NAME = "package-index.json"
VERIFY_SIDECAR_NAME = "verify.json"
MAX_DEVICE_REL_PATH = 1024
_DEVICE_DRIVE_PREFIX = re.compile(r"^[A-Za-z]:")
_BAG_TAG_FILES = (BAGIT_NAME, BAG_INFO_NAME, MANIFEST_NAME)
_COPY_BUFFER_BYTES = 1024 * 1024
_DEFAULT_ORPHAN_AGE = dt.timedelta(hours=24)
_PACKAGE_FILE_MODE = 0o644
_PACKAGE_DIR_MODE = 0o755
_PACKAGE_SYMLINK_MODE = 0o777
_PACKAGE_MTIME = 0
PACKAGE_PROFILE_HASH = hashlib.sha256(
    json.dumps(
        {
            "format": "tar-pax",
            "globs": PACKAGE_GLOBS,
            "mtime": _PACKAGE_MTIME,
            "profile": PACKAGE_PROFILE_VERSION,
            "regular_file_mode": _PACKAGE_FILE_MODE,
            "directory_mode": _PACKAGE_DIR_MODE,
            "symlink_mode": _PACKAGE_SYMLINK_MODE,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()


class ReceiveError(Exception):
    """Base class for front-door receive failures."""


class CollisionError(ReceiveError):
    """Two source relpaths map to the same destination-safe canonical path."""


class SourceScanError(ReceiveError):
    """The source tree cannot be received safely."""


class SourceMutationError(ReceiveError):
    """A source file changed while receive was reading it."""


class DestinationVerificationError(ReceiveError):
    """A landed payload file did not match the first-contact digest."""


class AtomicWriteObserver:
    """Test hook called between temp-file fsync and atomic rename."""

    def before_rename(self, temp_path: Path, final_path: Path) -> None:
        """Observe an atomic write before the final name becomes visible."""


@dataclass(frozen=True)
class FileReceipt:
    """One payload object copied or verified in an intake payload."""

    source_path: Path
    relpath: str
    destination_path: Path
    sha256_hex: str
    size_bytes: int
    st_dev: int | None
    st_ino: int | None
    copied: bool
    logical_relpath: str | None = None
    stored_relpath: str | None = None
    package_profile: str | None = None
    package_index: str | None = None
    package_members: tuple[dict[str, Any], ...] = ()

    @property
    def sha256_bytes(self) -> bytes:
        """Return the file digest as raw SHA-256 bytes."""

        return bytes.fromhex(self.sha256_hex)

    @property
    def as_received_relpath(self) -> str:
        """Return the operator-facing relpath for this payload object."""

        return self.logical_relpath or self.relpath


@dataclass
class PayloadUnit:
    """One planned payload object for local or streaming receive.

    A unit is either a regular file or a normalized package tar. Planning is
    metadata-only; ``byte_chunks`` performs the single source read under the same
    stat-before/stat-after mutation guard used by local ``receive_source``.
    """

    source_path: Path
    relpath: str
    entry_type: str
    logical_relpath: str | None
    hint_size: int
    plan_size: int
    mtime_ns: int
    _package_members_cache: tuple[dict[str, Any], ...] = field(
        default_factory=tuple,
        init=False,
        repr=False,
    )

    @property
    def is_package(self) -> bool:
        """Return true when this unit streams a normalized package tar."""

        return self.entry_type == "package"

    def byte_chunks(self, chunk_bytes: int = _COPY_BUFFER_BYTES) -> Iterator[bytes]:
        """Yield source bytes once, failing if the source changes during read."""

        if chunk_bytes <= 0:
            raise ReceiveError("chunk_bytes must be positive")
        if self.is_package:
            yield from _stream_package_unit(self, chunk_bytes=chunk_bytes)
        else:
            yield from _stream_file_unit(self, chunk_bytes=chunk_bytes)

    def package_index(self, tar_sha256: str) -> dict[str, Any] | None:
        """Return the package-index entry for this unit after streaming."""

        if not self.is_package:
            return None
        if self.logical_relpath is None:
            raise ReceiveError(f"package unit missing logical relpath: {self.source_path}")
        if not self._package_members_cache:
            raise ReceiveError(f"package unit was not streamed yet: {self.relpath}")
        return {
            "logical_member_path": self.logical_relpath,
            "stored_member_path": self.relpath,
            "sha256": tar_sha256,
            "members": list(self._package_members_cache),
        }


@dataclass(frozen=True)
class PayloadPlan:
    """Metadata-only source plan shared by local and streaming receives."""

    units: tuple[PayloadUnit, ...]
    rejected: tuple[RejectedEntry, ...]

    @property
    def skipped_count(self) -> int:
        """Return the number of source entries intentionally skipped."""

        return len(self.rejected)

    def source_plan_digest(self) -> str:
        """Return the metadata-only digest bound into StartIntake."""

        return payload_plan_digest(self)


@dataclass(frozen=True)
class RejectedEntry:
    """A source entry deliberately not copied by v1 receive policy."""

    relpath: str
    source_path: Path
    reason: str


@dataclass(frozen=True)
class ReceiveResult:
    """Completed receive summary returned to the CLI and tests."""

    intake_id: str
    intake_dir: Path
    manifest_path: Path
    bag_info_path: Path
    tagmanifest_path: Path
    sentinel_path: Path
    file_count: int
    total_bytes: int
    skipped_count: int
    bag_profile: str


@dataclass(frozen=True)
class BagWriteResult:
    """BagIt tag files written for a completed receive."""

    manifest_path: Path
    bag_info_path: Path
    tagmanifest_path: Path


@dataclass(frozen=True)
class BagValidationResult:
    """Completeness and validity evidence for an intake BagIt bag."""

    bag_root: Path
    data_root: Path
    metadata: dict[str, str]
    manifest: dict[str, str]
    actual: dict[str, str]
    actual_records: tuple[FileReceipt, ...]
    missing: list[str]
    extra: list[str]
    mismatched: list[dict[str, str]]
    tag_mismatched: list[dict[str, str | None]]
    errors: list[str]

    @property
    def complete(self) -> bool:
        """True when manifest and payload inventory agree exactly."""

        return (
            not self.missing
            and not self.extra
            and not any(error.startswith("complete:") for error in self.errors)
        )

    @property
    def valid(self) -> bool:
        """True when the bag is complete and all payload/tag checksums verify."""

        return self.complete and not self.mismatched and not self.tag_mismatched and not self.errors

    def details(self) -> dict[str, Any]:
        """Return a quarantine-ready detail dictionary."""

        payload: dict[str, Any] = {
            "missing": self.missing,
            "extra": self.extra,
            "mismatched": self.mismatched,
            "tag_mismatched": self.tag_mismatched,
            "errors": self.errors,
        }
        return {key: value for key, value in payload.items() if value}


@dataclass(frozen=True)
class OrphanSweepResult:
    """Summary of stale `.receiving.json` directories removed from landing."""

    removed: tuple[Path, ...]


@dataclass(frozen=True)
class VerifyMismatch:
    """One payload-vs-manifest mismatch recorded by destination verification."""

    path: str
    expected: str | None
    actual: str | None


@dataclass(frozen=True)
class VerifyResult:
    """Outcome of a post-release destination re-read."""

    bag_path: Path
    sidecar_path: Path
    stage: str
    checked_at: str
    mismatches: tuple[VerifyMismatch, ...] = ()

    @property
    def verified(self) -> bool:
        """True when the destination re-read completed without mismatches."""

        return self.stage == "full" and not self.mismatches


@dataclass(frozen=True)
class VerifyPendingResult:
    """Summary of landing bags swept by `verify-pending`."""

    checked: tuple[Path, ...]
    failed: tuple[Path, ...]


@dataclass(frozen=True)
class ConfirmationResult:
    """Fail-safe server confirmation result for removable-source release."""

    release_ok: bool
    status: str
    marker_path: Path | None
    detail: dict[str, Any] | None = None


@dataclass(frozen=True)
class _SourceEntry:
    source_path: Path
    relpath: str
    entry_type: str = "file"
    logical_relpath: str | None = None


@dataclass(frozen=True)
class _StatSnapshot:
    size: int
    mtime_ns: int
    inode: int | None
    device: int | None


@dataclass(frozen=True)
class _PackageMember:
    source_path: Path
    member_name: str
    mode: int
    size: int
    type_name: str
    linkname: str | None = None


@dataclass(frozen=True)
class _PackageTarResult:
    digest: str
    size_bytes: int
    members: tuple[dict[str, Any], ...]


def receive_source(
    source: Path | str | None,
    *,
    landing: Path | str,
    source_kind: str,
    operator: str,
    source_ref: str | None = None,
    artifactclass: str = "default",
    label: str | None = None,
    resume: str | None = None,
    now: dt.datetime | None = None,
    verify: str = "staged",
    atomic_observer: AtomicWriteObserver | None = None,
    after_copy_hook: Callable[[Path, tuple[FileReceipt, ...]], None] | None = None,
) -> ReceiveResult:
    """Receive one source tree into a contract-compliant landing intake.

    Bare receives always mint a new intake id. Resume is explicit and must name a
    prior sentinel-less intake whose `.receiving.json` records the original
    source and parameters. Sources are treated as read-only and must be quiescent:
    each regular file is stat-guarded before and after the read.
    """

    verify = _normalize_verify_mode(verify)
    receive_started_ns = time.monotonic_ns()
    if _native is not None and _stat_snapshot is globals().get("_ORIGINAL_STAT_SNAPSHOT"):
        return _receive_source_native(
            source,
            landing=landing,
            source_kind=source_kind,
            operator=operator,
            source_ref=source_ref,
            artifactclass=artifactclass,
            label=label,
            resume=resume,
            now=now,
            verify=verify,
            atomic_observer=atomic_observer,
            after_copy_hook=after_copy_hook,
        )

    started_at = now or _utcnow()
    landing_root = Path(landing).resolve()
    landing_root.mkdir(parents=True, exist_ok=True)
    _fsync_dir(landing_root)

    events: list[str] = []
    if resume is None:
        if source is None:
            raise ReceiveError("receive requires SOURCE unless --resume is used")
        source_root = Path(source).resolve()
        _validate_source_root(source_root)
        _validate_landing_relationship(source_root, landing_root)
        intake_id = _mint_intake_id(operator, started_at, landing_root)
        intake_dir = landing_root / intake_id
        intake_dir.mkdir(mode=0o755)
        receiving = _receiving_payload(
            intake_id=intake_id,
            source=source_root,
            landing=landing_root,
            source_kind=source_kind,
            operator=operator,
            source_ref=source_ref,
            artifactclass=artifactclass,
            label=label,
            started_at=started_at,
        )
        _atomic_write_json(intake_dir / ".receiving.json", receiving, observer=atomic_observer)
        events.append(f"started intake {intake_id} from {source_root}")
    else:
        intake_id = resume
        intake_dir = landing_root / intake_id
        receiving_path = intake_dir / ".receiving.json"
        if (intake_dir / "intake.json").exists():
            raise ReceiveError(f"intake {intake_id!r} is already complete")
        receiving = _read_json(receiving_path)
        source_root = Path(str(receiving.get("source") or "")).resolve()
        _validate_source_root(source_root)
        _validate_landing_relationship(source_root, landing_root)
        source_kind = str(receiving.get("source_kind") or source_kind)
        operator = str(receiving.get("operator") or operator)
        source_ref = _optional_str(receiving.get("source_ref"))
        artifactclass = str(receiving.get("artifactclass") or artifactclass)
        label = _optional_str(receiving.get("label"))
        events.append(f"resumed intake {intake_id} from {source_root}")

    data_root = intake_dir / DATA_DIR_NAME
    try:
        regulars, rejected = _scan_source(source_root)
        _check_collisions(regulars)
        data_root.mkdir(parents=True, exist_ok=True)
        _fsync_dir(data_root)
        if resume is not None:
            _prune_stale_payload_files(
                data_root,
                desired_relpaths={entry.relpath for entry in regulars},
                events=events,
            )
        receipts = _copy_or_verify_entries(
            regulars,
            payload_root=data_root,
            observer=atomic_observer,
            events=events,
        )
        if after_copy_hook is not None:
            after_copy_hook(data_root, receipts)
        _fsync_payload_files(receipts)
        entries = {receipt.relpath: receipt.sha256_hex for receipt in receipts}
        package_index = _package_index_payload(receipts)
        extra_tag_files: tuple[str, ...] = ()
        if package_index is not None:
            _atomic_write_json(
                intake_dir / PACKAGE_INDEX_NAME,
                package_index,
                observer=atomic_observer,
            )
            extra_tag_files = (PACKAGE_INDEX_NAME,)
        else:
            with suppress(FileNotFoundError):
                (intake_dir / PACKAGE_INDEX_NAME).unlink()
                _fsync_dir(intake_dir)
        total_bytes = sum(receipt.size_bytes for receipt in receipts)
        for item in rejected:
            events.append(f"skipped {item.relpath}: {item.reason}")
        events.append(
            f"verified {len(receipts)} file(s), {total_bytes} byte(s), {len(rejected)} skipped"
        )
        _write_receive_log(intake_dir / "receive.log", events, observer=atomic_observer)

        bag_files = write_bagit_files(
            intake_dir,
            entries=entries,
            metadata=bag_info_metadata(
                intake_id=intake_id,
                source_kind=source_kind,
                operator=operator,
                source_ref=source_ref,
                artifactclass=artifactclass,
                label=label,
                started_at=started_at,
                file_count=len(receipts),
                total_bytes=total_bytes,
                skipped_count=len(rejected),
            ),
            extra_tag_files=extra_tag_files,
            observer=atomic_observer,
        )
        sentinel = {
            "intake_id": intake_id,
            "status": "complete",
            "bag_profile": BAG_PROFILE,
            "created_at": started_at.isoformat(),
        }
        _atomic_write_json(intake_dir / "intake.json", sentinel, observer=atomic_observer)
        _fsync_dir(intake_dir)
        with suppress(FileNotFoundError):
            (intake_dir / ".receiving.json").unlink()
        _fsync_dir(intake_dir)
        events.append(f'release {{"release_offset_ns":{time.monotonic_ns() - receive_started_ns}}}')
        _write_receive_log(intake_dir / "receive.log", events, observer=None)
        if verify == "staged":
            _write_transfer_verify_sidecar(intake_dir)
        else:
            verify_result = verify_destination(intake_dir)
            if not verify_result.verified:
                raise DestinationVerificationError(
                    f"destination verification failed: {_mismatch_payload(verify_result.mismatches)}"
                )
        return ReceiveResult(
            intake_id=intake_id,
            intake_dir=intake_dir,
            manifest_path=bag_files.manifest_path,
            bag_info_path=bag_files.bag_info_path,
            tagmanifest_path=bag_files.tagmanifest_path,
            sentinel_path=intake_dir / "intake.json",
            file_count=len(receipts),
            total_bytes=total_bytes,
            skipped_count=len(rejected),
            bag_profile=BAG_PROFILE,
        )
    except ReceiveError as exc:
        events.append(f"failed: {exc}")
        if intake_dir.exists():
            _write_receive_log(intake_dir / "receive.log", events, observer=atomic_observer)
        raise


def _receive_source_native(
    source: Path | str | None,
    *,
    landing: Path | str,
    source_kind: str,
    operator: str,
    source_ref: str | None,
    artifactclass: str,
    label: str | None,
    resume: str | None,
    now: dt.datetime | None,
    verify: str,
    atomic_observer: AtomicWriteObserver | None,
    after_copy_hook: Callable[[Path, tuple[FileReceipt, ...]], None] | None,
) -> ReceiveResult:
    started_at = now or _utcnow()
    landing_root = Path(landing).resolve()
    landing_root.mkdir(parents=True, exist_ok=True)
    _fsync_dir(landing_root)
    if resume is None:
        if source is None:
            raise ReceiveError("receive requires SOURCE unless --resume is used")
        source_arg: Path | None = Path(source).resolve()
        intake_id = _mint_intake_id(operator, started_at, landing_root)
    else:
        source_arg = None
        intake_id = resume
    try:
        payload = cast(
            dict[str, Any],
            json.loads(
                _native.receive_source_json(
                    source_arg,
                    landing_root,
                    intake_id,
                    started_at.isoformat(),
                    started_at.date().isoformat(),
                    source_kind,
                    operator,
                    source_ref,
                    artifactclass,
                    label,
                    verify,
                    resume,
                    atomic_observer,
                    after_copy_hook,
                )
            ),
        )
    except RuntimeError as exc:
        _raise_native_receive_error(exc)
    return _receive_result_from_native(payload)


def plan_payload_units(source: Path | str) -> PayloadPlan:
    """Return a metadata-only receive plan for a source tree.

    The plan reuses the same package-boundary, symlink/special-file, collision,
    and canonicalization logic as ``receive_source``. It does not read file
    contents, so streaming receive still reads each source unit exactly once.
    """

    source_root = Path(source).resolve()
    _validate_source_root(source_root)
    if _native is not None:
        try:
            return _payload_plan_from_native(
                cast(dict[str, Any], json.loads(_native.plan_payload_units_json(source_root)))
            )
        except RuntimeError as exc:
            _raise_native_receive_error(exc)
    entries, rejected = _scan_source(source_root)
    _check_collisions(entries)
    units = tuple(_payload_unit_from_entry(entry) for entry in entries)
    return PayloadPlan(units=units, rejected=rejected)


def payload_plan_digest(plan: PayloadPlan | Iterable[PayloadUnit]) -> str:
    """Return sha256 over sorted ``{relpath,size,mtime_ns}`` plan metadata."""

    units = plan.units if isinstance(plan, PayloadPlan) else tuple(plan)
    if _native is not None and hasattr(_native, "payload_plan_digest"):
        return str(
            _native.payload_plan_digest(
                [(unit.relpath, int(unit.plan_size), int(unit.mtime_ns)) for unit in units]
            )
        )
    payload = [
        {"relpath": unit.relpath, "size": unit.plan_size, "mtime_ns": unit.mtime_ns}
        for unit in sorted(units, key=lambda item: item.relpath)
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def source_plan_digest(plan: PayloadPlan | Iterable[PayloadUnit]) -> str:
    """Alias for the gRPC ``source_plan_digest`` wire field encoding."""

    return payload_plan_digest(plan)


def manifest_digest(files: Iterable[Any]) -> str:
    """Return sha256 over sorted gRPC commit manifest entries."""

    entries = [
        (
            str(_field(item, "relpath")),
            str(_field(item, "client_sha256")),
            int(_field(item, "bytes")),
        )
        for item in files
    ]
    if _native is not None and hasattr(_native, "manifest_digest"):
        try:
            return str(_native.manifest_digest(entries))
        except ValueError as exc:
            raise ReceiveError(str(exc)) from exc
    payload = [
        {
            "relpath": canonicalize_manifest_path(relpath),
            "client_sha256": client_sha256.lower(),
            "bytes": size_bytes,
        }
        for relpath, client_sha256, size_bytes in entries
    ]
    return hashlib.sha256(
        json.dumps(sorted(payload, key=lambda item: item["relpath"]), sort_keys=True).encode(
            "utf-8"
        )
    ).hexdigest()


def build_package_index(packages: Iterable[Mapping[str, Any]]) -> dict[str, Any] | None:
    """Return the shared package-index tag payload for normalized package records."""

    package_list = [dict(package) for package in packages]
    if _native is not None and hasattr(_native, "build_package_index_json"):
        payload = json.loads(
            _native.build_package_index_json(
                json.dumps(package_list, sort_keys=True, separators=(",", ":"))
            )
        )
        return None if payload is None else cast(dict[str, Any], payload)
    if not package_list:
        return None
    return {
        "profile": PACKAGE_PROFILE_VERSION,
        "profile_hash": PACKAGE_PROFILE_HASH,
        "package_globs": list(PACKAGE_GLOBS),
        "packages": sorted(package_list, key=lambda item: item["stored_member_path"]),
    }


def _payload_plan_from_native(payload: Mapping[str, Any]) -> PayloadPlan:
    units = tuple(_payload_unit_from_native(item) for item in payload.get("units", ()))
    rejected = tuple(_rejected_entry_from_native(item) for item in payload.get("rejected", ()))
    return PayloadPlan(units=units, rejected=rejected)


def _receive_result_from_native(payload: Mapping[str, Any]) -> ReceiveResult:
    return ReceiveResult(
        intake_id=str(payload["intake_id"]),
        intake_dir=_path_from_native_payload(payload["intake_dir"]),
        manifest_path=_path_from_native_payload(payload["manifest_path"]),
        bag_info_path=_path_from_native_payload(payload["bag_info_path"]),
        tagmanifest_path=_path_from_native_payload(payload["tagmanifest_path"]),
        sentinel_path=_path_from_native_payload(payload["sentinel_path"]),
        file_count=int(payload["file_count"]),
        total_bytes=int(payload["total_bytes"]),
        skipped_count=int(payload["skipped_count"]),
        bag_profile=str(payload["bag_profile"]),
    )


def _payload_unit_from_native(payload: Mapping[str, Any]) -> PayloadUnit:
    return PayloadUnit(
        source_path=_path_from_native_payload(payload["source_path"]),
        relpath=str(payload["relpath"]),
        entry_type=str(payload["entry_type"]),
        logical_relpath=_optional_str(payload.get("logical_relpath")),
        hint_size=int(payload["hint_size"]),
        plan_size=int(payload["plan_size"]),
        mtime_ns=int(payload["mtime_ns"]),
    )


def _rejected_entry_from_native(payload: Mapping[str, Any]) -> RejectedEntry:
    return RejectedEntry(
        relpath=str(payload["relpath"]),
        source_path=_path_from_native_payload(payload["source_path"]),
        reason=str(payload["reason"]),
    )


def _path_from_native_payload(payload: Any) -> Path:
    if isinstance(payload, Mapping):
        if "os_hex" in payload:
            return Path(os.fsdecode(bytes.fromhex(str(payload["os_hex"]))))
        if "text" in payload:
            return Path(str(payload["text"]))
    return Path(str(payload))


def _raise_native_receive_error(exc: RuntimeError) -> None:
    message = str(exc)
    if message.startswith("canonical receive path collision:"):
        raise CollisionError(message) from exc
    if message.startswith("source changed during receive:"):
        raise SourceMutationError(message) from exc
    if message.startswith("destination verification failed:"):
        raise DestinationVerificationError(message) from exc
    if "during scan" in message:
        raise SourceScanError(message) from exc
    raise ReceiveError(message) from exc


def sweep_orphans(
    landing: Path | str,
    *,
    older_than: dt.timedelta = _DEFAULT_ORPHAN_AGE,
    now: dt.datetime | None = None,
) -> OrphanSweepResult:
    """Remove stale sentinel-less receive directories from a landing root."""

    landing_root = Path(landing)
    current = now or _utcnow()
    if _native is not None:
        try:
            return _orphan_sweep_from_native(
                cast(
                    dict[str, Any],
                    json.loads(
                        _native.sweep_orphans_json(
                            landing_root,
                            older_than.total_seconds(),
                            current.timestamp(),
                        )
                    ),
                )
            )
        except RuntimeError as exc:
            raise ReceiveError(str(exc)) from exc
    removed: list[Path] = []
    if not landing_root.exists():
        return OrphanSweepResult(removed=())
    for child in sorted(path for path in landing_root.iterdir() if path.is_dir()):
        receiving = child / ".receiving.json"
        if not receiving.exists() or (child / "intake.json").exists():
            continue
        mtime = dt.datetime.fromtimestamp(receiving.stat().st_mtime, tz=dt.UTC)
        if current - mtime >= older_than:
            shutil.rmtree(child)
            removed.append(child)
    return OrphanSweepResult(removed=tuple(removed))


def _orphan_sweep_from_native(payload: Mapping[str, Any]) -> OrphanSweepResult:
    return OrphanSweepResult(
        removed=tuple(_path_from_native_payload(item) for item in payload.get("removed", ()))
    )


def verify_destination(bag_path: Path | str) -> VerifyResult:
    """Re-read a completed bag payload against its manifest and write `verify.json`."""

    bag = Path(bag_path)
    if _native is not None:
        try:
            return _verify_result_from_native(
                cast(dict[str, Any], json.loads(_native.verify_destination_json(bag)))
            )
        except RuntimeError as exc:
            raise ReceiveError(str(exc)) from exc
    stage2_started_ns = time.monotonic_ns()
    mismatches = tuple(_destination_mismatches(bag))
    result = VerifyResult(
        bag_path=bag,
        sidecar_path=bag / VERIFY_SIDECAR_NAME,
        stage="full" if not mismatches else "failed",
        checked_at=_utcnow().isoformat(),
        mismatches=mismatches,
    )
    _atomic_write_json(result.sidecar_path, _verify_sidecar_payload(result), observer=None)
    _append_receive_log_event(
        bag / "receive.log",
        f'verify {{"stage2_wall_ns":{time.monotonic_ns() - stage2_started_ns}}}',
    )
    return result


def verify_pending(landing_roots: Iterable[Path | str]) -> VerifyPendingResult:
    """Verify completed landing bags whose sidecar is absent, transfer, or failed."""

    roots = tuple(Path(root) for root in landing_roots)
    if _native is not None:
        try:
            return _verify_pending_result_from_native(
                cast(
                    dict[str, Any],
                    json.loads(_native.verify_pending_json([str(root) for root in roots])),
                )
            )
        except RuntimeError as exc:
            raise ReceiveError(str(exc)) from exc
    checked: list[Path] = []
    failed: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for bag in sorted(path for path in root.iterdir() if path.is_dir()):
            if not (bag / "intake.json").exists() or (bag / ".receiving.json").exists():
                continue
            if not _verify_sidecar_is_pending(bag):
                continue
            result = verify_destination(bag)
            checked.append(bag)
            if not result.verified:
                failed.append(bag)
    return VerifyPendingResult(checked=tuple(checked), failed=tuple(failed))


def _verify_result_from_native(payload: Mapping[str, Any]) -> VerifyResult:
    return VerifyResult(
        bag_path=_path_from_native_payload(payload["bag_path"]),
        sidecar_path=_path_from_native_payload(payload["sidecar_path"]),
        stage=str(payload["stage"]),
        checked_at=str(payload["checked_at"]),
        mismatches=tuple(_verify_mismatch_from_native(item) for item in payload["mismatches"]),
    )


def _verify_pending_result_from_native(payload: Mapping[str, Any]) -> VerifyPendingResult:
    return VerifyPendingResult(
        checked=tuple(_path_from_native_payload(item) for item in payload.get("checked", ())),
        failed=tuple(_path_from_native_payload(item) for item in payload.get("failed", ())),
    )


def _verify_mismatch_from_native(payload: Mapping[str, Any]) -> VerifyMismatch:
    return VerifyMismatch(
        path=str(payload["path"]),
        expected=_optional_str(payload.get("expected")),
        actual=_optional_str(payload.get("actual")),
    )


def wait_for_server_confirmation(
    intake_dir: Path | str,
    *,
    timeout_seconds: float,
    poll_interval_seconds: float = 1.0,
) -> ConfirmationResult:
    """Poll server markers and default to no release on timeout or quarantine."""

    root = Path(intake_dir)
    deadline = time.monotonic() + timeout_seconds
    while True:
        discrepancy = root / "intake.discrepancy.json"
        if discrepancy.exists():
            return ConfirmationResult(
                release_ok=False,
                status="discrepancy",
                marker_path=discrepancy,
                detail=_read_json_or_none(discrepancy),
            )
        quarantined = root / "intake.quarantined.json"
        if quarantined.exists():
            return ConfirmationResult(
                release_ok=False,
                status="quarantined",
                marker_path=quarantined,
                detail=_read_json_or_none(quarantined),
            )
        verified = root / "intake.verified.json"
        if verified.exists():
            detail = _read_json_or_none(verified)
            return ConfirmationResult(
                release_ok=detail is not None,
                status="verified" if detail is not None else "pending",
                marker_path=verified,
                detail=detail,
            )
        if time.monotonic() >= deadline:
            return ConfirmationResult(
                release_ok=False,
                status="timeout",
                marker_path=None,
                detail=None,
            )
        time.sleep(poll_interval_seconds)


def hash_payload_tree(
    payload_root: Path | str,
    *,
    reject_native_packages: bool = False,
) -> list[FileReceipt]:
    """Hash a payload tree using the same canonical relpaths as receive."""

    root = Path(payload_root)
    if _native is not None:
        try:
            records = json.loads(
                _native.hash_payload_tree_json(
                    root,
                    reject_native_packages=reject_native_packages,
                )
            )
        except RuntimeError as exc:
            raise ReceiveError(str(exc)) from exc
        receipts: list[FileReceipt] = []
        actual_paths: dict[str, Path] = {}
        for candidate in sorted(root.rglob("*")):
            if candidate.is_file():
                actual_paths[canonicalize_filesystem_path(candidate, root)] = candidate
        for record in records:
            relpath = str(record["relpath"])
            destination = actual_paths.get(relpath, safe_payload_path(root, relpath))
            try:
                stat_result = destination.lstat()
            except FileNotFoundError:
                st_dev = None
                st_ino = None
            else:
                st_dev = getattr(stat_result, "st_dev", None)
                st_ino = getattr(stat_result, "st_ino", None)
            receipts.append(
                FileReceipt(
                    source_path=destination,
                    relpath=relpath,
                    destination_path=destination,
                    sha256_hex=str(record["sha256_hex"]),
                    size_bytes=int(record["size_bytes"]),
                    st_dev=st_dev,
                    st_ino=st_ino,
                    copied=False,
                )
            )
        return receipts

    receipts: list[FileReceipt] = []
    for path in sorted(root.rglob("*")):
        relpath = canonicalize_filesystem_path(path, root)
        try:
            stat_result = path.lstat()
        except FileNotFoundError as exc:
            raise SourceScanError(f"payload entry disappeared during hash: {path}") from exc
        if stat.S_ISDIR(stat_result.st_mode):
            if reject_native_packages and _is_package_boundary(relpath):
                raise ReceiveError(
                    "payload contains un-normalized package directory "
                    f"{relpath!r}; re-run sutra receive so it is stored as a "
                    f"{PACKAGE_PROFILE_VERSION} tar"
                )
            continue
        if stat.S_ISLNK(stat_result.st_mode):
            raise ReceiveError(f"payload contains unsupported symlink: {relpath}")
        if not stat.S_ISREG(stat_result.st_mode):
            raise ReceiveError(
                f"payload contains unsupported {_special_file_reason(stat_result.st_mode)}: "
                f"{relpath}"
            )
        receipts.append(
            FileReceipt(
                source_path=path,
                relpath=relpath,
                destination_path=path,
                sha256_hex=sha256_file(path),
                size_bytes=stat_result.st_size,
                st_dev=getattr(stat_result, "st_dev", None),
                st_ino=getattr(stat_result, "st_ino", None),
                copied=False,
            )
        )
    return receipts


def read_package_index(path: str | Path) -> dict[str, Any]:
    """Read and lightly validate a receive package index tag file."""

    if _native is not None:
        try:
            return cast(dict[str, Any], json.loads(_native.read_package_index_json(Path(path))))
        except RuntimeError as exc:
            raise ReceiveError(str(exc)) from exc

    payload = _read_json(Path(path))
    if payload.get("profile") != PACKAGE_PROFILE_VERSION:
        raise ReceiveError(
            "Package-Profile-Version mismatch: "
            f"expected {PACKAGE_PROFILE_VERSION}, actual {payload.get('profile')!r}"
        )
    if payload.get("profile_hash") != PACKAGE_PROFILE_HASH:
        raise ReceiveError(
            "Package-Profile-Hash mismatch: "
            f"expected {PACKAGE_PROFILE_HASH}, actual {payload.get('profile_hash')!r}"
        )
    packages = payload.get("packages")
    if not isinstance(packages, list):
        raise ReceiveError(f"{PACKAGE_INDEX_NAME} packages must be a list")
    for package in packages:
        if not isinstance(package, dict):
            raise ReceiveError(f"{PACKAGE_INDEX_NAME} package entries must be objects")
        for key in ("logical_member_path", "stored_member_path", "sha256", "members"):
            if key not in package:
                raise ReceiveError(f"{PACKAGE_INDEX_NAME} package missing {key}")
        canonicalize_manifest_path(str(package["logical_member_path"]))
        canonicalize_manifest_path(str(package["stored_member_path"]))
        if not _is_sha256_hex(str(package["sha256"])):
            raise ReceiveError(f"{PACKAGE_INDEX_NAME} package has invalid sha256")
        if not isinstance(package["members"], list):
            raise ReceiveError(f"{PACKAGE_INDEX_NAME} package members must be a list")
    return payload


def read_manifest_sha256(path: str | Path) -> dict[str, str]:
    """Read BagIt `manifest-sha256.txt` as canonical relpath -> SHA-256 hex.

    Paths are serialized as BagIt bag-relative POSIX paths under `data/`. The
    BagIt RFC 8493 percent layer is reversed first (`%25`, `%0D`, `%0A`), then the
    receive member-name canonicalization is applied.
    """

    if _native is not None:
        try:
            return cast(dict[str, str], _native.read_manifest_sha256(Path(path)))
        except RuntimeError as exc:
            raise ReceiveError(str(exc)) from exc

    return _read_checksum_manifest(Path(path), payload_manifest=True)


def read_bag_info(path: str | Path) -> dict[str, str]:
    """Read a BagIt `bag-info.txt` file into a label dictionary."""

    if _native is not None:
        try:
            return cast(dict[str, str], _native.read_bag_info(Path(path)))
        except RuntimeError as exc:
            raise ReceiveError(str(exc)) from exc

    result: dict[str, str] = {}
    current_key: str | None = None
    for line_number, raw_line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not raw_line:
            continue
        if raw_line[0].isspace() and current_key is not None:
            result[current_key] = f"{result[current_key]}\n{raw_line.strip()}"
            continue
        if ":" not in raw_line:
            raise ReceiveError(f"invalid bag-info line {line_number}: {raw_line!r}")
        key, value = raw_line.split(":", 1)
        key = key.strip()
        if not key:
            raise ReceiveError(f"invalid empty bag-info label at line {line_number}")
        result[key] = value.strip()
        current_key = key
    return result


def bag_info_metadata(
    *,
    intake_id: str,
    source_kind: str,
    operator: str,
    source_ref: str | None,
    artifactclass: str,
    label: str | None,
    started_at: dt.datetime,
    file_count: int,
    total_bytes: int,
    skipped_count: int,
) -> dict[str, str]:
    """Return the BagIt metadata labels that are authoritative for intake."""

    return {
        "Bagging-Date": started_at.date().isoformat(),
        "Payload-Oxum": f"{total_bytes}.{file_count}",
        "Bag-Software-Agent": f"sutradhara-receive/{RECEIVE_VERSION}",
        "Receive-Package": RECEIVE_PACKAGE,
        "Intake-Id": intake_id,
        "Operator": operator,
        "Source-Kind": source_kind,
        "Source-Ref": source_ref or "",
        "Artifactclass": artifactclass,
        "Label": label or "",
        "Canonicalization-Version": CANONICALIZATION_VERSION,
        "Package-Profile-Version": PACKAGE_PROFILE_VERSION,
        "Package-Profile-Hash": PACKAGE_PROFILE_HASH,
        "Skipped-Count": str(skipped_count),
    }


def write_bagit_files(
    bag_root: Path | str,
    *,
    entries: Mapping[str, str],
    metadata: Mapping[str, str],
    extra_tag_files: Iterable[str] = (),
    observer: AtomicWriteObserver | None = None,
) -> BagWriteResult:
    """Write BagIt tag files for a completed receive and return their paths."""

    root = Path(bag_root)
    manifest_path = root / MANIFEST_NAME
    bag_info_path = root / BAG_INFO_NAME
    bagit_path = root / BAGIT_NAME
    tagmanifest_path = root / TAGMANIFEST_NAME
    if _native is not None:
        try:
            payload = cast(
                dict[str, Any],
                json.loads(
                    _native.write_bagit_files(
                        root,
                        dict(entries),
                        dict(metadata),
                        list(extra_tag_files),
                        observer,
                    )
                ),
            )
        except RuntimeError as exc:
            raise ReceiveError(str(exc)) from exc
        return BagWriteResult(
            manifest_path=_path_from_native_payload(payload["manifest_path"]),
            bag_info_path=_path_from_native_payload(payload["bag_info_path"]),
            tagmanifest_path=_path_from_native_payload(payload["tagmanifest_path"]),
        )
    _atomic_write_text(bagit_path, BAGIT_TEXT, observer=observer)
    _atomic_write_text(bag_info_path, bag_info_text(metadata), observer=observer)
    _atomic_write_text(manifest_path, bagit_manifest_text(entries), observer=observer)
    tag_files = (*_BAG_TAG_FILES, *tuple(extra_tag_files))
    _atomic_write_text(
        tagmanifest_path,
        tagmanifest_text(root, tag_files),
        observer=observer,
    )
    return BagWriteResult(
        manifest_path=manifest_path,
        bag_info_path=bag_info_path,
        tagmanifest_path=tagmanifest_path,
    )


def bagit_manifest_text(entries: Mapping[str, str]) -> str:
    """Return deterministic BagIt `manifest-sha256.txt` text."""

    if _native is not None:
        try:
            return str(_native.bagit_manifest_text(dict(entries)))
        except ValueError as exc:
            raise ReceiveError(str(exc)) from exc

    lines = []
    for relpath in sorted(entries):
        digest = entries[relpath].lower()
        if not _is_sha256_hex(digest):
            raise ReceiveError(f"invalid sha256 for {relpath!r}: {entries[relpath]!r}")
        canonical = canonicalize_manifest_path(relpath)
        lines.append(f"{digest}  {_encode_bagit_path(f'{DATA_DIR_NAME}/{canonical}')}")
    return "\n".join(lines) + ("\n" if lines else "")


def bag_info_text(metadata: Mapping[str, str]) -> str:
    """Return deterministic BagIt `bag-info.txt` text for intake metadata."""

    if _native is not None:
        return str(_native.bag_info_text(dict(metadata)))

    order = [
        "Bagging-Date",
        "Payload-Oxum",
        "Bag-Software-Agent",
        "Receive-Package",
        "Intake-Id",
        "Operator",
        "Source-Kind",
        "Source-Ref",
        "Artifactclass",
        "Label",
        "Canonicalization-Version",
        "Package-Profile-Version",
        "Package-Profile-Hash",
        "Skipped-Count",
    ]
    lines: list[str] = []
    for key in order:
        if key in metadata:
            lines.append(f"{key}: {_bag_info_value(metadata[key])}")
    for key in sorted(set(metadata) - set(order)):
        lines.append(f"{key}: {_bag_info_value(metadata[key])}")
    return "\n".join(lines) + "\n"


def tagmanifest_text(bag_root: Path | str, tag_files: Iterable[str]) -> str:
    """Return deterministic BagIt `tagmanifest-sha256.txt` text."""

    if _native is not None:
        try:
            return str(_native.tagmanifest_text(Path(bag_root), list(tag_files)))
        except RuntimeError as exc:
            raise ReceiveError(str(exc)) from exc

    root = Path(bag_root)
    lines = []
    for relpath in sorted(tag_files):
        path = root / relpath
        lines.append(f"{sha256_file(path)}  {_encode_bagit_path(relpath)}")
    return "\n".join(lines) + ("\n" if lines else "")


def validate_bag(bag_root: Path | str) -> BagValidationResult:
    """Validate BagIt completeness and checksum validity for an intake bag."""

    root = Path(bag_root)
    data_root = root / DATA_DIR_NAME
    errors: list[str] = []
    metadata: dict[str, str] = {}
    manifest: dict[str, str] = {}
    actual_records: tuple[FileReceipt, ...] = ()
    if not data_root.is_dir():
        errors.append(f"complete: missing {DATA_DIR_NAME}/ directory")
    else:
        try:
            actual_records = tuple(hash_payload_tree(data_root, reject_native_packages=True))
        except ReceiveError as exc:
            errors.append(f"complete: cannot hash {DATA_DIR_NAME}/: {exc}")
    actual = {record.relpath: record.sha256_hex for record in actual_records}

    try:
        manifest = read_manifest_sha256(root / MANIFEST_NAME)
    except (OSError, ReceiveError, ValueError) as exc:
        errors.append(f"complete: cannot read {MANIFEST_NAME}: {exc}")

    try:
        metadata = read_bag_info(root / BAG_INFO_NAME)
    except (OSError, ReceiveError, ValueError) as exc:
        errors.append(f"cannot read {BAG_INFO_NAME}: {exc}")

    package_index_path = root / PACKAGE_INDEX_NAME
    package_index: dict[str, Any] | None = None
    if package_index_path.exists():
        try:
            package_index = read_package_index(package_index_path)
            actual_records = _annotate_package_records(actual_records, package_index)
        except (OSError, ReceiveError, ValueError) as exc:
            errors.append(f"cannot read {PACKAGE_INDEX_NAME}: {exc}")

    actual = {record.relpath: record.sha256_hex for record in actual_records}
    mismatch = manifest_mismatch(actual, manifest)
    missing = list(mismatch.get("missing", []))
    extra = list(mismatch.get("extra", []))
    mismatched = list(mismatch.get("mismatched", []))
    if metadata:
        oxum = metadata.get("Payload-Oxum")
        expected_oxum = (
            f"{sum(record.size_bytes for record in actual_records)}.{len(actual_records)}"
        )
        if oxum != expected_oxum:
            errors.append(f"Payload-Oxum mismatch: expected {expected_oxum}, actual {oxum!r}")
        receive_package_error = _receive_package_error(metadata)
        if receive_package_error is not None:
            errors.append(receive_package_error)
        if metadata.get("Canonicalization-Version") != CANONICALIZATION_VERSION:
            errors.append(
                "Canonicalization-Version mismatch: "
                f"expected {CANONICALIZATION_VERSION}, "
                f"actual {metadata.get('Canonicalization-Version')!r}"
            )
        allowed_package_versions = (
            {PACKAGE_PROFILE_VERSION}
            if package_index_path.exists()
            else {None, PACKAGE_PROFILE_VERSION}
        )
        if metadata.get("Package-Profile-Version") not in allowed_package_versions:
            errors.append(
                "Package-Profile-Version mismatch: "
                f"expected {PACKAGE_PROFILE_VERSION}, "
                f"actual {metadata.get('Package-Profile-Version')!r}"
            )
        allowed_package_hashes = (
            {PACKAGE_PROFILE_HASH} if package_index_path.exists() else {None, PACKAGE_PROFILE_HASH}
        )
        if metadata.get("Package-Profile-Hash") not in allowed_package_hashes:
            errors.append(
                "Package-Profile-Hash mismatch: "
                f"expected {PACKAGE_PROFILE_HASH}, "
                f"actual {metadata.get('Package-Profile-Hash')!r}"
            )

    extra_required_tags = (PACKAGE_INDEX_NAME,) if package_index_path.exists() else ()
    tag_mismatched = _verify_tagmanifest(root, required_tag_files=extra_required_tags)
    return BagValidationResult(
        bag_root=root,
        data_root=data_root,
        metadata=metadata,
        manifest=manifest,
        actual=actual,
        actual_records=actual_records,
        missing=missing,
        extra=extra,
        mismatched=mismatched,
        tag_mismatched=tag_mismatched,
        errors=errors,
    )


def manifest_mismatch(actual: Mapping[str, str], expected: Mapping[str, str]) -> dict[str, Any]:
    """Return the intake manifest mismatch summary after shared canonicalization."""

    if _native is not None:
        try:
            return cast(
                dict[str, Any],
                json.loads(_native.manifest_mismatch_json(dict(actual), dict(expected))),
            )
        except ValueError as exc:
            raise ReceiveError(str(exc)) from exc

    actual_canonical = {
        canonicalize_manifest_path(path): digest.lower() for path, digest in actual.items()
    }
    expected_canonical = {
        canonicalize_manifest_path(path): digest.lower() for path, digest in expected.items()
    }
    missing = sorted(path for path in expected_canonical if path not in actual_canonical)
    extra = sorted(path for path in actual_canonical if path not in expected_canonical)
    mismatched = [
        {
            "path": path,
            "expected": expected_canonical[path],
            "actual": actual_canonical[path],
        }
        for path in sorted(actual_canonical.keys() & expected_canonical.keys())
        if actual_canonical[path] != expected_canonical[path]
    ]
    if not missing and not extra and not mismatched and expected_canonical:
        return {}
    if not expected_canonical:
        return {
            "reason": "manifest-has-no-sha256",
            "missing": [],
            "extra": sorted(actual_canonical),
            "mismatched": [],
        }
    return {"missing": missing, "extra": extra, "mismatched": mismatched}


def _receive_package_error(metadata: Mapping[str, str]) -> str | None:
    actual = metadata.get("Receive-Package")
    if actual in SUPPORTED_RECEIVE_PACKAGES:
        return None
    expected = ", ".join(sorted(SUPPORTED_RECEIVE_PACKAGES))
    return f"Receive-Package mismatch: expected {expected}, actual {actual!r}"


def canonicalize_filesystem_path(path: Path | str, root: Path | str) -> str:
    """Canonicalize a filesystem path relative to a root into a member name."""

    if _native is not None:
        try:
            return str(_native.canonicalize_filesystem_path(Path(path), Path(root)))
        except ValueError as exc:
            raise SourceScanError(str(exc)) from exc

    source = Path(path)
    base = Path(root)
    try:
        relative = source.relative_to(base)
    except ValueError as exc:
        raise SourceScanError(f"{source} is not under source root {base}") from exc
    return _canonicalize_parts(relative.parts, from_filesystem=True)


def canonicalize_manifest_path(raw: str) -> str:
    """Canonicalize a manifest path into the shared receive member-name form."""

    if _native is not None:
        try:
            return str(_native.canonicalize_manifest_path(raw))
        except ValueError as exc:
            raise ReceiveError(str(exc)) from exc

    value = raw
    while value.startswith("./"):
        value = value[2:]
    value = value.lstrip("/")
    if value.startswith(f"{DATA_DIR_NAME}/"):
        value = value[len(f"{DATA_DIR_NAME}/") :]
    pure = PurePosixPath(value)
    return _canonicalize_parts(pure.parts, from_filesystem=False)


def canonical_device_rel_path(value: str | None) -> str:
    """Return the canonical forward-slash card-relative wire path."""

    if _native is not None and hasattr(_native, "canonical_device_rel_path"):
        try:
            return str(_native.canonical_device_rel_path(value))
        except ValueError as exc:
            raise ReceiveError(str(exc)) from exc
    if value is None or value == "":
        return ""
    if len(value) > MAX_DEVICE_REL_PATH:
        raise ReceiveError("invalid source path")
    if "\\" in value:
        raise ReceiveError("invalid source path")
    if value.startswith("/"):
        raise ReceiveError("invalid source path")
    if _DEVICE_DRIVE_PREFIX.match(value):
        raise ReceiveError("invalid source path")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ReceiveError("invalid source path")
    canonical = posixpath.normpath(value)
    if canonical != value:
        raise ReceiveError("invalid source path")
    for part in parts[:-1]:
        if _is_package_boundary(part):
            raise ReceiveError(f"source path enters a package: {canonical}")
    return canonical


def derive_card_id(
    volume_uuid: str | None,
    source: str | None,
    mount_path: str | Path,
    label: str | None,
) -> str:
    """Return the shared opaque card id from real volume id or stable fallback."""

    if _native is not None and hasattr(_native, "derive_card_id"):
        return str(
            _native.derive_card_id(
                volume_uuid,
                str(source or ""),
                str(mount_path),
                str(label or ""),
            )
        )
    if volume_uuid:
        return f"volume:{volume_uuid}"
    seed = "|".join([str(source or ""), str(mount_path), str(label or "")])
    return f"volume:{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:24]}"


def _read_checksum_manifest(path: Path, *, payload_manifest: bool) -> dict[str, str]:
    records: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw_line:
            continue
        if len(raw_line) < 66:
            raise ReceiveError(f"invalid checksum line {line_number} in {path}")
        digest = raw_line[:64].lower()
        index = 64
        if raw_line[index] not in {" ", "\t"}:
            raise ReceiveError(f"invalid checksum separator at line {line_number} in {path}")
        while index < len(raw_line) and raw_line[index] in {" ", "\t"}:
            index += 1
        encoded_path = raw_line[index:]
        if not encoded_path:
            raise ReceiveError(f"missing checksum path at line {line_number} in {path}")
        if not _is_sha256_hex(digest):
            raise ReceiveError(f"invalid sha256 at line {line_number} in {path}")
        decoded_path = _decode_bagit_path(encoded_path)
        if payload_manifest:
            relpath = _canonicalize_payload_manifest_path(decoded_path)
            records[relpath] = digest
        else:
            records[PurePosixPath(decoded_path).as_posix()] = digest
    return records


def _canonicalize_payload_manifest_path(decoded_path: str) -> str:
    value = decoded_path
    while value.startswith("./"):
        value = value[2:]
    if PurePosixPath(value).is_absolute():
        raise ReceiveError(f"payload manifest path must be relative: {decoded_path!r}")
    if not value.startswith(f"{DATA_DIR_NAME}/"):
        raise ReceiveError(
            f"payload manifest path must start with {DATA_DIR_NAME}/: {decoded_path!r}"
        )
    return canonicalize_manifest_path(value)


def _verify_tagmanifest(
    root: Path,
    *,
    required_tag_files: Iterable[str] = (),
) -> list[dict[str, str | None]]:
    path = root / TAGMANIFEST_NAME
    mismatched: list[dict[str, str | None]] = []
    try:
        expected = _read_checksum_manifest(path, payload_manifest=False)
    except (OSError, ReceiveError, ValueError) as exc:
        return [{"path": TAGMANIFEST_NAME, "expected": "readable", "actual": str(exc)}]

    for relpath in (*_BAG_TAG_FILES, *tuple(required_tag_files)):
        if relpath not in expected:
            mismatched.append({"path": relpath, "expected": "listed", "actual": None})

    for relpath, expected_digest in sorted(expected.items()):
        target = root / relpath
        if not _is_safe_bag_relative_path(relpath):
            mismatched.append(
                {"path": relpath, "expected": expected_digest, "actual": "unsafe path"}
            )
            continue
        if not target.is_file():
            mismatched.append({"path": relpath, "expected": expected_digest, "actual": None})
            continue
        actual = sha256_file(target)
        if actual != expected_digest:
            mismatched.append({"path": relpath, "expected": expected_digest, "actual": actual})
    return mismatched


def _encode_bagit_path(path: str) -> str:
    return path.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def _decode_bagit_path(path: str) -> str:
    output: list[str] = []
    index = 0
    while index < len(path):
        char = path[index]
        if char != "%":
            output.append(char)
            index += 1
            continue
        token = path[index + 1 : index + 3]
        if len(token) != 2:
            raise ReceiveError(f"invalid BagIt percent escape in path: {path!r}")
        normalized = token.upper()
        if normalized == "25":
            output.append("%")
        elif normalized == "0D":
            output.append("\r")
        elif normalized == "0A":
            output.append("\n")
        else:
            raise ReceiveError(f"unsupported BagIt percent escape %{token} in path: {path!r}")
        index += 3
    return "".join(output)


def _bag_info_value(value: object) -> str:
    return str(value).replace("\r", " ").replace("\n", " ")


def _is_safe_bag_relative_path(relpath: str) -> bool:
    pure = PurePosixPath(relpath)
    return (
        bool(pure.parts)
        and not pure.is_absolute()
        and all(part not in {"", ".", ".."} for part in pure.parts)
    )


def slug_operator(operator: str) -> str:
    """Return a path-safe operator slug for receive intake ids."""

    if _native is not None:
        return str(_native.slug_operator(operator))

    normalized = unicodedata.normalize("NFKD", operator).encode("ascii", "ignore").decode()
    slug = re.sub(r"[^A-Za-z0-9]+", "-", normalized.lower()).strip("-")
    return slug or "operator"


def sha256_file(path: Path | str) -> str:
    """Return a file's SHA-256 digest without loading it into memory."""

    if _native is not None:
        try:
            return str(_native.sha256_file(Path(path)))
        except RuntimeError as exc:
            raise ReceiveError(str(exc)) from exc

    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(_COPY_BUFFER_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_payload_path(payload_root: Path | str, relpath: str) -> Path:
    """Return a payload destination path, rejecting traversal and symlinks."""

    if _native is not None:
        try:
            return Path(str(_native.safe_payload_path(Path(payload_root), relpath)))
        except ValueError as exc:
            raise ReceiveError(str(exc)) from exc

    root = Path(payload_root).resolve()
    pure = PurePosixPath(relpath)
    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        raise ReceiveError(f"unsafe payload relpath: {relpath!r}")
    current = root
    for part in pure.parts[:-1]:
        current = current / part
        if current.exists() and current.is_symlink():
            raise ReceiveError(f"payload directory is a symlink: {current}")
        if current.exists() and not current.is_dir():
            raise ReceiveError(f"payload path component is not a directory: {current}")
    final = root.joinpath(*pure.parts)
    if final.exists() and final.is_symlink():
        raise ReceiveError(f"payload destination is a symlink: {final}")
    try:
        final.resolve(strict=False).relative_to(root)
    except ValueError as exc:
        raise ReceiveError(f"payload destination escapes payload root: {relpath!r}") from exc
    return final


def _is_package_boundary(relpath: str) -> bool:
    path = PurePosixPath(relpath)
    name = path.name.casefold()
    whole = path.as_posix().casefold()
    return any(
        fnmatch.fnmatchcase(name, pattern.casefold())
        or fnmatch.fnmatchcase(whole, pattern.casefold())
        for pattern in PACKAGE_GLOBS
    )


def _package_index_payload(receipts: Iterable[FileReceipt]) -> dict[str, Any] | None:
    packages: list[dict[str, Any]] = []
    for receipt in receipts:
        if receipt.package_profile != PACKAGE_PROFILE_VERSION:
            continue
        if receipt.logical_relpath is None or receipt.stored_relpath is None:
            raise ReceiveError(f"package receipt missing logical/stored relpath: {receipt.relpath}")
        if not receipt.package_members:
            raise ReceiveError(f"package receipt missing inner index: {receipt.relpath}")
        packages.append(
            {
                "logical_member_path": receipt.logical_relpath,
                "stored_member_path": receipt.stored_relpath,
                "profile": PACKAGE_PROFILE_VERSION,
                "sha256": receipt.sha256_hex,
                "size_bytes": receipt.size_bytes,
                "members": list(receipt.package_members),
            }
        )
    if not packages:
        return None
    return build_package_index(packages)


def _annotate_package_records(
    records: tuple[FileReceipt, ...],
    package_index: Mapping[str, Any],
) -> tuple[FileReceipt, ...]:
    by_stored: dict[str, dict[str, Any]] = {}
    for raw_package in package_index.get("packages", []):
        package_entry = dict(raw_package)
        stored = canonicalize_manifest_path(str(package_entry["stored_member_path"]))
        if stored in by_stored:
            raise ReceiveError(f"{PACKAGE_INDEX_NAME} duplicates stored member {stored!r}")
        by_stored[stored] = package_entry

    annotated: list[FileReceipt] = []
    seen_stored: set[str] = set()
    for record in records:
        indexed_package = by_stored.get(record.relpath)
        if indexed_package is None:
            annotated.append(record)
            continue
        if str(indexed_package.get("sha256")) != record.sha256_hex:
            raise ReceiveError(
                f"{PACKAGE_INDEX_NAME} sha256 mismatch for {record.relpath}: "
                f"expected {record.sha256_hex}, actual {indexed_package.get('sha256')!r}"
            )
        if indexed_package.get("profile") != PACKAGE_PROFILE_VERSION:
            raise ReceiveError(
                f"{PACKAGE_INDEX_NAME} profile mismatch for {record.relpath}: "
                f"{indexed_package.get('profile')!r}"
            )
        annotated.append(
            FileReceipt(
                source_path=record.source_path,
                relpath=record.relpath,
                destination_path=record.destination_path,
                sha256_hex=record.sha256_hex,
                size_bytes=record.size_bytes,
                st_dev=record.st_dev,
                st_ino=record.st_ino,
                copied=record.copied,
                logical_relpath=canonicalize_manifest_path(
                    str(indexed_package["logical_member_path"])
                ),
                stored_relpath=record.relpath,
                package_profile=PACKAGE_PROFILE_VERSION,
                package_index=PACKAGE_INDEX_NAME,
            )
        )
        seen_stored.add(record.relpath)

    missing = sorted(set(by_stored) - seen_stored)
    if missing:
        raise ReceiveError(f"{PACKAGE_INDEX_NAME} references missing payloads: {missing}")
    return tuple(annotated)


def _scan_source(source_root: Path) -> tuple[tuple[_SourceEntry, ...], tuple[RejectedEntry, ...]]:
    if _is_package_boundary(source_root.name):
        return (
            (
                _SourceEntry(
                    source_root,
                    f"{source_root.name}.tar",
                    entry_type="package",
                    logical_relpath=source_root.name,
                ),
            ),
            (),
        )
    regulars: list[_SourceEntry] = []
    rejected: list[RejectedEntry] = []
    for root_raw, dirs, files in os.walk(source_root, topdown=True, followlinks=False):
        root = Path(root_raw)
        for dirname in list(dirs):
            path = root / dirname
            relpath = canonicalize_filesystem_path(path, source_root)
            if path.is_symlink():
                rejected.append(RejectedEntry(relpath, path, "symlink-directory"))
                dirs.remove(dirname)
                continue
            if _is_package_boundary(relpath):
                regulars.append(
                    _SourceEntry(
                        path,
                        f"{relpath}.tar",
                        entry_type="package",
                        logical_relpath=relpath,
                    )
                )
                dirs.remove(dirname)
        for filename in files:
            path = root / filename
            relpath = canonicalize_filesystem_path(path, source_root)
            try:
                mode = path.lstat().st_mode
            except FileNotFoundError as exc:
                raise SourceScanError(f"source entry disappeared during scan: {path}") from exc
            if stat.S_ISLNK(mode):
                rejected.append(RejectedEntry(relpath, path, "symlink"))
            elif stat.S_ISREG(mode):
                regulars.append(_SourceEntry(path, relpath))
            else:
                rejected.append(RejectedEntry(relpath, path, _special_file_reason(mode)))
    return tuple(sorted(regulars, key=lambda item: item.relpath)), tuple(rejected)


def _payload_unit_from_entry(entry: _SourceEntry) -> PayloadUnit:
    if entry.entry_type == "package":
        plan_size, mtime_ns = _package_plan_metadata(entry.source_path)
        return PayloadUnit(
            source_path=entry.source_path,
            relpath=entry.relpath,
            entry_type=entry.entry_type,
            logical_relpath=entry.logical_relpath,
            hint_size=0,
            plan_size=plan_size,
            mtime_ns=mtime_ns,
        )
    snapshot = _stat_snapshot(entry.source_path)
    return PayloadUnit(
        source_path=entry.source_path,
        relpath=entry.relpath,
        entry_type=entry.entry_type,
        logical_relpath=entry.logical_relpath,
        hint_size=snapshot.size,
        plan_size=snapshot.size,
        mtime_ns=snapshot.mtime_ns,
    )


def _stream_file_unit(unit: PayloadUnit, *, chunk_bytes: int) -> Iterator[bytes]:
    before = _stat_snapshot(unit.source_path)
    with unit.source_path.open("rb") as handle:
        yield from iter(lambda: handle.read(chunk_bytes), b"")
    after = _stat_snapshot(unit.source_path)
    _raise_if_mutated(unit.source_path, before, after)


def _stream_package_unit(unit: PayloadUnit, *, chunk_bytes: int) -> Iterator[bytes]:
    logical_relpath = unit.logical_relpath
    if logical_relpath is None:
        raise ReceiveError(f"package unit missing logical relpath: {unit.source_path}")
    before_tree = _package_tree_snapshot(unit.source_path)
    output: queue.Queue[bytes | BaseException | None] = queue.Queue(maxsize=16)
    records: list[dict[str, Any]] = []

    def produce() -> None:
        try:
            members = _package_members(unit.source_path, logical_relpath=logical_relpath)
            writer = _QueueTarWriter(output, chunk_bytes=chunk_bytes)
            with tarfile.open(
                fileobj=cast(Any, writer),
                mode="w",
                format=tarfile.PAX_FORMAT,
            ) as tar:
                for member in members:
                    info = _tar_info_for_package_member(member)
                    if member.type_name == "file":
                        before = _stat_snapshot(member.source_path)
                        with member.source_path.open("rb") as raw_in:
                            hashing_in = _HashingReader(raw_in)
                            tar.addfile(info, hashing_in)
                        after = _stat_snapshot(member.source_path)
                        _raise_if_mutated(member.source_path, before, after)
                        records.append(
                            {
                                "member": member.member_name,
                                "type": "file",
                                "length": member.size,
                                "sha256": hashing_in.hexdigest(),
                                "data_offset": getattr(info, "offset_data", None),
                            }
                        )
                    else:
                        tar.addfile(info)
                        record: dict[str, Any] = {
                            "member": member.member_name,
                            "type": member.type_name,
                            "length": 0,
                            "sha256": None,
                            "data_offset": None,
                        }
                        if member.linkname is not None:
                            record["linkname"] = member.linkname
                        records.append(record)
            writer.flush()
            after_tree = _package_tree_snapshot(unit.source_path)
            if before_tree != after_tree:
                raise SourceMutationError(f"package changed during receive: {unit.source_path}")
            unit._package_members_cache = tuple(sorted(records, key=lambda item: item["member"]))
            output.put(None)
        except BaseException as exc:  # pragma: no cover - exercised through consumer raise
            output.put(exc)

    thread = threading.Thread(target=produce, daemon=True)
    thread.start()
    try:
        while True:
            item = output.get()
            if item is None:
                break
            if isinstance(item, BaseException):
                raise item
            yield item
    finally:
        thread.join(timeout=5)


def _package_plan_metadata(package_root: Path) -> tuple[int, int]:
    snapshot = _package_tree_snapshot(package_root)
    total_size = sum(item[2] for item in snapshot if item[1] == "file")
    max_mtime = max((item[3] for item in snapshot), default=0)
    return total_size, max_mtime


def _package_tree_snapshot(package_root: Path) -> tuple[tuple[str, str, int, int], ...]:
    entries: list[tuple[str, str, int, int]] = []
    try:
        root_stat = package_root.lstat()
    except FileNotFoundError as exc:
        raise SourceScanError(f"package disappeared during receive: {package_root}") from exc
    if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
        raise ReceiveError(f"package boundary is not a directory: {package_root}")
    entries.append(("", "directory", 0, root_stat.st_mtime_ns))
    for root_raw, dirs, files in os.walk(package_root, topdown=True, followlinks=False):
        root = Path(root_raw)
        dirs.sort(key=lambda name: canonicalize_filesystem_path(root / name, package_root))
        for dirname in list(dirs):
            path = root / dirname
            relpath = canonicalize_filesystem_path(path, package_root)
            stat_result = path.lstat()
            if stat.S_ISLNK(stat_result.st_mode):
                entries.append((relpath, "symlink", 0, stat_result.st_mtime_ns))
                dirs.remove(dirname)
            elif stat.S_ISDIR(stat_result.st_mode):
                entries.append((relpath, "directory", 0, stat_result.st_mtime_ns))
            else:
                raise ReceiveError(
                    f"package contains unsupported {_special_file_reason(stat_result.st_mode)}: {relpath}"
                )
        for filename in sorted(
            files,
            key=lambda name: canonicalize_filesystem_path(root / name, package_root),
        ):
            path = root / filename
            relpath = canonicalize_filesystem_path(path, package_root)
            stat_result = path.lstat()
            if stat.S_ISREG(stat_result.st_mode):
                entries.append((relpath, "file", stat_result.st_size, stat_result.st_mtime_ns))
            elif stat.S_ISLNK(stat_result.st_mode):
                entries.append((relpath, "symlink", 0, stat_result.st_mtime_ns))
            else:
                raise ReceiveError(
                    f"package contains unsupported {_special_file_reason(stat_result.st_mode)}: {relpath}"
                )
    return tuple(sorted(entries))


def _check_collisions(entries: Iterable[_SourceEntry]) -> None:
    seen: dict[str, _SourceEntry] = {}
    for entry in entries:
        key = entry.relpath.casefold()
        prior = seen.get(key)
        if prior is not None and prior.source_path != entry.source_path:
            raise CollisionError(
                "canonical receive path collision: "
                f"{prior.source_path} and {entry.source_path} -> {entry.relpath!r}"
            )
        seen[key] = entry


def _copy_or_verify_entries(
    entries: Iterable[_SourceEntry],
    *,
    payload_root: Path,
    observer: AtomicWriteObserver | None,
    events: list[str],
) -> tuple[FileReceipt, ...]:
    receipts: list[FileReceipt] = []
    for entry in entries:
        copy_started_ns = time.monotonic_ns()
        if entry.entry_type == "package":
            receipt = _copy_or_verify_package_entry(
                entry,
                payload_root=payload_root,
                observer=observer,
                events=events,
            )
        else:
            receipt = _copy_or_verify_file_entry(
                entry,
                payload_root=payload_root,
                observer=observer,
                events=events,
            )
        events.append(
            "copy "
            + json.dumps(
                {
                    "relpath": receipt.relpath,
                    "bytes": receipt.size_bytes,
                    "copy_wall_ns": time.monotonic_ns() - copy_started_ns,
                },
                separators=(",", ":"),
            )
        )
        receipts.append(receipt)
    return tuple(receipts)


def _copy_or_verify_file_entry(
    entry: _SourceEntry,
    *,
    payload_root: Path,
    observer: AtomicWriteObserver | None,
    events: list[str],
) -> FileReceipt:
    destination = safe_payload_path(payload_root, entry.relpath)
    source_digest, snapshot = _hash_source_with_stat_guard(entry.source_path)
    copied = True
    existing_mode = _existing_destination_mode(destination)
    if existing_mode is not None:
        if stat.S_ISLNK(existing_mode):
            raise ReceiveError(f"payload destination is a symlink: {destination}")
        if not stat.S_ISREG(existing_mode):
            raise ReceiveError(
                f"payload destination is unsupported "
                f"{_special_file_reason(existing_mode)}: {destination}"
            )
        destination_digest = sha256_file(destination)
        if destination_digest == source_digest:
            copied = False
            events.append(f"resume kept verified {entry.relpath}")
        else:
            events.append(f"resume replacing mismatched {entry.relpath}")
            _copy_source_to_destination(entry.source_path, destination, observer=observer)
            source_digest, snapshot = _hash_source_with_stat_guard(entry.source_path)
    else:
        digest_from_copy, snapshot = _copy_source_to_destination(
            entry.source_path,
            destination,
            observer=observer,
        )
        source_digest = digest_from_copy
    return FileReceipt(
        source_path=entry.source_path,
        relpath=entry.relpath,
        destination_path=destination,
        sha256_hex=source_digest,
        size_bytes=snapshot.size,
        st_dev=snapshot.device,
        st_ino=snapshot.inode,
        copied=copied,
    )


def _copy_or_verify_package_entry(
    entry: _SourceEntry,
    *,
    payload_root: Path,
    observer: AtomicWriteObserver | None,
    events: list[str],
) -> FileReceipt:
    logical_relpath = entry.logical_relpath
    if logical_relpath is None:
        raise ReceiveError(f"package entry missing logical relpath: {entry.source_path}")
    destination = safe_payload_path(payload_root, entry.relpath)
    source_stat = _stat_snapshot(entry.source_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    _fsync_dir(destination.parent)
    temp_path = _temp_path_for(destination)
    try:
        package = _build_package_tar(entry.source_path, temp_path, logical_relpath=logical_relpath)
        after_source_stat = _stat_snapshot(entry.source_path)
        _raise_if_mutated(entry.source_path, source_stat, after_source_stat)
        copied = True
        existing_mode = _existing_destination_mode(destination)
        if existing_mode is not None:
            if stat.S_ISLNK(existing_mode):
                raise ReceiveError(f"payload destination is a symlink: {destination}")
            if not stat.S_ISREG(existing_mode):
                raise ReceiveError(
                    f"payload destination is unsupported "
                    f"{_special_file_reason(existing_mode)}: {destination}"
                )
            destination_digest = sha256_file(destination)
            if destination_digest == package.digest:
                temp_path.unlink()
                copied = False
                events.append(f"resume kept verified package {logical_relpath}")
            else:
                events.append(f"resume replacing mismatched package {logical_relpath}")
        else:
            events.append(f"packaged {logical_relpath} -> {entry.relpath}")

        if copied:
            if observer is not None:
                observer.before_rename(temp_path, destination)
            temp_path.replace(destination)
            _fsync_dir(destination.parent)
        return FileReceipt(
            source_path=entry.source_path,
            relpath=entry.relpath,
            destination_path=destination,
            sha256_hex=package.digest,
            size_bytes=package.size_bytes,
            st_dev=after_source_stat.device,
            st_ino=after_source_stat.inode,
            copied=copied,
            logical_relpath=logical_relpath,
            stored_relpath=entry.relpath,
            package_profile=PACKAGE_PROFILE_VERSION,
            package_index=PACKAGE_INDEX_NAME,
            package_members=package.members,
        )
    except Exception:
        with suppress(FileNotFoundError):
            temp_path.unlink()
        raise


def _existing_destination_mode(path: Path) -> int | None:
    try:
        return path.lstat().st_mode
    except FileNotFoundError:
        return None


def _prune_stale_payload_files(
    payload_root: Path,
    *,
    desired_relpaths: set[str],
    events: list[str],
) -> None:
    for path in _payload_paths_deepest_first(payload_root):
        try:
            mode = path.lstat().st_mode
        except FileNotFoundError:
            continue
        if stat.S_ISDIR(mode):
            continue
        relpath = canonicalize_filesystem_path(path, payload_root)
        if relpath in desired_relpaths:
            continue
        path.unlink()
        events.append(f"resume pruned stale {relpath}")
    for path in _payload_paths_deepest_first(payload_root):
        try:
            mode = path.lstat().st_mode
        except FileNotFoundError:
            continue
        if not stat.S_ISDIR(mode):
            continue
        with suppress(OSError):
            path.rmdir()
    _fsync_dir(payload_root)


def _payload_paths_deepest_first(payload_root: Path) -> list[Path]:
    return sorted(
        payload_root.rglob("*"),
        key=lambda item: len(item.relative_to(payload_root).parts),
        reverse=True,
    )


def _copy_source_to_destination(
    source: Path,
    destination: Path,
    *,
    observer: AtomicWriteObserver | None,
) -> tuple[str, _StatSnapshot]:
    before = _stat_snapshot(source)
    digest = hashlib.sha256()
    destination.parent.mkdir(parents=True, exist_ok=True)
    _fsync_dir(destination.parent)
    temp_path = _temp_path_for(destination)
    try:
        with source.open("rb") as raw_in, temp_path.open("xb") as raw_out:
            for chunk in iter(lambda: raw_in.read(_COPY_BUFFER_BYTES), b""):
                digest.update(chunk)
                raw_out.write(chunk)
            raw_out.flush()
            os.fsync(raw_out.fileno())
        after = _stat_snapshot(source)
        _raise_if_mutated(source, before, after)
        if observer is not None:
            observer.before_rename(temp_path, destination)
        temp_path.replace(destination)
        _fsync_dir(destination.parent)
        return digest.hexdigest(), after
    except Exception:
        with suppress(FileNotFoundError):
            temp_path.unlink()
        raise


def _build_package_tar(
    package_root: Path,
    destination: Path,
    *,
    logical_relpath: str,
) -> _PackageTarResult:
    """Write a deterministic receive package tar and return its index evidence."""

    try:
        root_mode = package_root.lstat().st_mode
    except FileNotFoundError as exc:
        raise SourceScanError(f"package disappeared during receive: {package_root}") from exc
    if stat.S_ISLNK(root_mode) or not stat.S_ISDIR(root_mode):
        raise ReceiveError(f"package boundary is not a directory: {package_root}")

    members = _package_members(package_root, logical_relpath=logical_relpath)
    member_records: dict[str, dict[str, Any]] = {}
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("xb") as raw_out:
        hashing_out = _HashingWriter(raw_out)
        with tarfile.open(
            fileobj=cast(Any, hashing_out),
            mode="w",
            format=tarfile.PAX_FORMAT,
        ) as tar:
            for member in members:
                info = _tar_info_for_package_member(member)
                if member.type_name == "file":
                    before = _stat_snapshot(member.source_path)
                    with member.source_path.open("rb") as raw_in:
                        hashing_in = _HashingReader(raw_in)
                        tar.addfile(info, hashing_in)
                    after = _stat_snapshot(member.source_path)
                    _raise_if_mutated(member.source_path, before, after)
                    member_records[member.member_name] = {
                        "member": member.member_name,
                        "type": "file",
                        "length": member.size,
                        "sha256": hashing_in.hexdigest(),
                    }
                else:
                    tar.addfile(info)
                    record: dict[str, Any] = {
                        "member": member.member_name,
                        "type": member.type_name,
                        "length": 0,
                        "sha256": None,
                    }
                    if member.linkname is not None:
                        record["linkname"] = member.linkname
                    member_records[member.member_name] = record
        raw_out.flush()
        os.fsync(raw_out.fileno())
        digest = hashing_out.hexdigest()

    offsets = _package_tar_offsets(destination)
    for member_name, record in member_records.items():
        record["data_offset"] = offsets.get(member_name) if record["type"] == "file" else None
    return _PackageTarResult(
        digest=digest,
        size_bytes=destination.stat().st_size,
        members=tuple(member_records[name] for name in sorted(member_records)),
    )


def _package_members(package_root: Path, *, logical_relpath: str) -> tuple[_PackageMember, ...]:
    members = [
        _PackageMember(
            source_path=package_root,
            member_name=logical_relpath,
            mode=_PACKAGE_DIR_MODE,
            size=0,
            type_name="directory",
        )
    ]
    for root_raw, dirs, files in os.walk(package_root, topdown=True, followlinks=False):
        root = Path(root_raw)
        dirs.sort(key=lambda name: canonicalize_filesystem_path(root / name, package_root))
        for dirname in list(dirs):
            path = root / dirname
            member = _package_member_from_path(
                package_root,
                path,
                logical_relpath=logical_relpath,
            )
            if member.type_name == "symlink":
                dirs.remove(dirname)
            members.append(member)
        for filename in sorted(
            files,
            key=lambda name: canonicalize_filesystem_path(root / name, package_root),
        ):
            members.append(
                _package_member_from_path(
                    package_root,
                    root / filename,
                    logical_relpath=logical_relpath,
                )
            )
    return tuple(sorted(members, key=lambda item: item.member_name))


def _package_member_from_path(
    package_root: Path,
    path: Path,
    *,
    logical_relpath: str,
) -> _PackageMember:
    relpath = canonicalize_filesystem_path(path, package_root)
    member_name = PurePosixPath(logical_relpath, relpath).as_posix()
    try:
        stat_result = path.lstat()
    except FileNotFoundError as exc:
        raise SourceScanError(f"package entry disappeared during receive: {path}") from exc
    mode = stat_result.st_mode
    if stat.S_ISLNK(mode):
        return _PackageMember(
            source_path=path,
            member_name=member_name,
            mode=_PACKAGE_SYMLINK_MODE,
            size=0,
            type_name="symlink",
            linkname=os.readlink(path),
        )
    if stat.S_ISDIR(mode):
        return _PackageMember(
            source_path=path,
            member_name=member_name,
            mode=_PACKAGE_DIR_MODE,
            size=0,
            type_name="directory",
        )
    if stat.S_ISREG(mode):
        return _PackageMember(
            source_path=path,
            member_name=member_name,
            mode=_PACKAGE_FILE_MODE,
            size=stat_result.st_size,
            type_name="file",
        )
    raise ReceiveError(f"package contains unsupported {_special_file_reason(mode)}: {member_name}")


def _tar_info_for_package_member(member: _PackageMember) -> tarfile.TarInfo:
    info = tarfile.TarInfo(member.member_name)
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = _PACKAGE_MTIME
    info.mode = member.mode
    info.pax_headers = {}
    if member.type_name == "file":
        info.type = tarfile.REGTYPE
        info.size = member.size
    elif member.type_name == "directory":
        info.type = tarfile.DIRTYPE
        info.size = 0
    elif member.type_name == "symlink":
        info.type = tarfile.SYMTYPE
        info.linkname = member.linkname or ""
        info.size = 0
    else:
        raise ReceiveError(f"unsupported package member type: {member.type_name}")
    return info


def _package_tar_offsets(path: Path) -> dict[str, int | None]:
    offsets: dict[str, int | None] = {}
    with tarfile.open(path, mode="r:") as tar:
        for info in tar:
            offsets[info.name] = getattr(info, "offset_data", None)
    return offsets


class _HashingWriter:
    """Binary file wrapper that records the SHA-256 of bytes written through it."""

    def __init__(self, handle: BinaryIO) -> None:
        self._handle = handle
        self._digest = hashlib.sha256()

    def write(self, data: bytes) -> int:
        self._digest.update(data)
        return self._handle.write(data)

    def tell(self) -> int:
        return self._handle.tell()

    def flush(self) -> None:
        self._handle.flush()

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


class _QueueTarWriter:
    """Minimal binary writer that streams tar bytes through a queue."""

    def __init__(
        self, output: queue.Queue[bytes | BaseException | None], *, chunk_bytes: int
    ) -> None:
        self._output = output
        self._chunk_bytes = chunk_bytes
        self._position = 0
        self._pending = bytearray()

    def write(self, data: bytes) -> int:
        self._position += len(data)
        self._pending.extend(data)
        while len(self._pending) >= self._chunk_bytes:
            self._output.put(bytes(self._pending[: self._chunk_bytes]))
            del self._pending[: self._chunk_bytes]
        return len(data)

    def tell(self) -> int:
        return self._position

    def flush(self) -> None:
        if self._pending:
            self._output.put(bytes(self._pending))
            self._pending.clear()


class _HashingReader:
    """Binary file wrapper that records the SHA-256 of bytes read through it."""

    def __init__(self, handle: BinaryIO) -> None:
        self._handle = handle
        self._digest = hashlib.sha256()

    def read(self, size: int = -1) -> bytes:
        data = self._handle.read(size)
        self._digest.update(data)
        return data

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def _hash_source_with_stat_guard(source: Path) -> tuple[str, _StatSnapshot]:
    before = _stat_snapshot(source)
    digest = sha256_file(source)
    after = _stat_snapshot(source)
    _raise_if_mutated(source, before, after)
    return digest, after


def _fsync_payload_files(receipts: Iterable[FileReceipt]) -> None:
    for receipt in receipts:
        with receipt.destination_path.open("rb") as handle:
            os.fsync(handle.fileno())


def _destination_mismatches(bag_path: Path) -> tuple[VerifyMismatch, ...]:
    manifest = read_manifest_sha256(bag_path / MANIFEST_NAME)
    actual = {
        record.relpath: record.sha256_hex
        for record in hash_payload_tree(bag_path / DATA_DIR_NAME, reject_native_packages=True)
    }
    mismatch = manifest_mismatch(actual, manifest)
    mismatches: list[VerifyMismatch] = []
    for path in mismatch.get("missing", []):
        mismatches.append(
            VerifyMismatch(path=str(path), expected=manifest.get(str(path)), actual=None)
        )
    for path in mismatch.get("extra", []):
        mismatches.append(
            VerifyMismatch(path=str(path), expected=None, actual=actual.get(str(path)))
        )
    for item in mismatch.get("mismatched", []):
        mismatches.append(
            VerifyMismatch(
                path=str(item["path"]),
                expected=str(item["expected"]),
                actual=str(item["actual"]),
            )
        )
    return tuple(mismatches)


def _write_transfer_verify_sidecar(bag_path: Path) -> None:
    result = VerifyResult(
        bag_path=bag_path,
        sidecar_path=bag_path / VERIFY_SIDECAR_NAME,
        stage="transfer",
        checked_at=_utcnow().isoformat(),
    )
    _atomic_write_json(result.sidecar_path, _verify_sidecar_payload(result), observer=None)


def _verify_sidecar_is_pending(bag_path: Path) -> bool:
    sidecar = bag_path / VERIFY_SIDECAR_NAME
    try:
        payload = _read_json(sidecar)
    except FileNotFoundError:
        return True
    return payload.get("stage") in {None, "transfer", "failed"}


def _verify_sidecar_payload(result: VerifyResult) -> dict[str, Any]:
    return {
        "checked_at": result.checked_at,
        "mismatches": _mismatch_payload(result.mismatches),
        "stage": result.stage,
    }


def _mismatch_payload(mismatches: Iterable[VerifyMismatch]) -> list[dict[str, str | None]]:
    return [
        {"path": mismatch.path, "expected": mismatch.expected, "actual": mismatch.actual}
        for mismatch in mismatches
    ]


def _normalize_verify_mode(value: str) -> str:
    if value not in {"staged", "blocking"}:
        raise ReceiveError(f"verify must be 'staged' or 'blocking', got {value!r}")
    return value


def _stat_snapshot(path: Path) -> _StatSnapshot:
    st = path.stat()
    return _StatSnapshot(
        size=st.st_size,
        mtime_ns=st.st_mtime_ns,
        inode=getattr(st, "st_ino", None),
        device=getattr(st, "st_dev", None),
    )


_ORIGINAL_STAT_SNAPSHOT = _stat_snapshot


def _raise_if_mutated(path: Path, before: _StatSnapshot, after: _StatSnapshot) -> None:
    if before != after:
        raise SourceMutationError(f"source changed during receive: {path}")


def _is_sha256_hex(value: str) -> bool:
    if len(value) != 64:
        return False
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True


def _canonicalize_parts(parts: Iterable[str], *, from_filesystem: bool) -> str:
    output: list[str] = []
    for part in parts:
        if part in {"", "."}:
            continue
        if part == "..":
            raise ReceiveError("relative paths must not contain '..'")
        output.append(
            _canonical_component_from_fs(part)
            if from_filesystem
            else _canonical_component_from_manifest(part)
        )
    if not output:
        raise ReceiveError("relative path is empty")
    return PurePosixPath(*output).as_posix()


def _canonical_component_from_fs(component: str) -> str:
    return _canonical_component_from_bytes(os.fsencode(component))


def _canonical_component_from_manifest(component: str) -> str:
    try:
        raw = unescape_member_name(component)
    except MemberNameError as exc:
        raise ReceiveError(f"invalid escaped manifest member name {component!r}") from exc
    return _canonical_component_from_bytes(raw)


def _canonical_component_from_bytes(raw: bytes) -> str:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return escape_member_name(raw)
    normalized = unicodedata.normalize("NFC", text)
    return escape_member_name(normalized.encode("utf-8"))


def _receiving_payload(
    *,
    intake_id: str,
    source: Path,
    landing: Path,
    source_kind: str,
    operator: str,
    source_ref: str | None,
    artifactclass: str,
    label: str | None,
    started_at: dt.datetime,
) -> dict[str, Any]:
    return {
        "intake_id": intake_id,
        "source": str(source),
        "landing": str(landing),
        "source_kind": source_kind,
        "operator": operator,
        "source_ref": source_ref,
        "artifactclass": artifactclass,
        "label": label,
        "started_at": started_at.isoformat(),
        "receive_version": RECEIVE_VERSION,
        "canonicalization_version": CANONICALIZATION_VERSION,
    }


def _mint_intake_id(operator: str, now: dt.datetime, landing_root: Path) -> str:
    prefix = f"{now:%Y%m%d}-{slug_operator(operator)}"
    for _ in range(100):
        candidate = f"{prefix}-{uuid.uuid4().hex}"
        if not (landing_root / candidate).exists():
            return candidate
    raise ReceiveError("could not mint a unique intake id")


def _validate_source_root(source: Path) -> None:
    if not source.exists():
        raise FileNotFoundError(source)
    if not source.is_dir():
        raise ReceiveError(f"receive source must be a directory: {source}")
    if _is_inside_existing_payload(source):
        raise ReceiveError(f"receive source is inside an existing intake payload: {source}")


def _validate_landing_relationship(source: Path, landing: Path) -> None:
    try:
        landing.relative_to(source)
    except ValueError:
        pass
    else:
        raise ReceiveError(f"landing root {landing} must not be inside source {source}")


def _is_inside_existing_payload(path: Path) -> bool:
    candidates = (path, *path.parents)
    for candidate in candidates:
        if candidate.name not in {DATA_DIR_NAME, "payload"}:
            continue
        parent = candidate.parent
        if (parent / "intake.json").exists() or (parent / ".receiving.json").exists():
            return True
    return False


def _special_file_reason(mode: int) -> str:
    if stat.S_ISFIFO(mode):
        return "fifo"
    if stat.S_ISCHR(mode):
        return "character-device"
    if stat.S_ISBLK(mode):
        return "block-device"
    if stat.S_ISSOCK(mode):
        return "socket"
    return "non-regular"


def _atomic_write_json(
    path: Path,
    payload: Mapping[str, Any],
    *,
    observer: AtomicWriteObserver | None = None,
) -> None:
    text = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    _atomic_write_text(path, text, observer=observer)


def _atomic_write_text(
    path: Path,
    text: str,
    *,
    observer: AtomicWriteObserver | None = None,
) -> None:
    _atomic_write_bytes(path, text.encode("utf-8"), observer=observer)


def _atomic_write_bytes(
    path: Path,
    data: bytes,
    *,
    observer: AtomicWriteObserver | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _fsync_dir(path.parent)
    temp_path = _temp_path_for(path)
    try:
        with temp_path.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if observer is not None:
            observer.before_rename(temp_path, path)
        temp_path.replace(path)
        _fsync_dir(path.parent)
    except Exception:
        with suppress(FileNotFoundError):
            temp_path.unlink()
        raise


def _write_receive_log(
    path: Path,
    events: Iterable[str],
    *,
    observer: AtomicWriteObserver | None = None,
) -> None:
    lines = [f"{_utcnow().isoformat()} {event}" for event in events]
    _atomic_write_text(path, "\n".join(lines) + "\n", observer=observer)


def _append_receive_log_event(path: Path, event: str) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        text = ""
    text += f"{_utcnow().isoformat()} {event}\n"
    _atomic_write_text(path, text, observer=None)


def _temp_path_for(path: Path) -> Path:
    return path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")


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


def _read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ReceiveError(f"{path} JSON root is not an object")
    return data


def _read_json_or_none(path: Path) -> dict[str, Any] | None:
    try:
        return _read_json(path)
    except (OSError, json.JSONDecodeError, ReceiveError):
        return None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _field(item: Any, name: str) -> Any:
    if isinstance(item, Mapping):
        return item[name]
    return getattr(item, name)


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
