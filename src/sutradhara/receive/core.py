"""Filesystem receive core for Sutradhara's source-agnostic intake front door.

This module is shared by edge-side `sutra receive` and server-side `intake scan`.
It keeps the contract-critical parts in one dependency-light place: canonical
member paths, ASC-MHL manifests, safe payload paths, atomic writes, resumable
landing directories, source quiescence checks, and server release markers.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import shutil
import stat
import time
import unicodedata
import uuid
from collections.abc import Callable, Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any
from xml.etree import ElementTree as ET

from sutradhara.member_name import MemberNameError, escape_member_name, unescape_member_name

RECEIVE_VERSION = "receive-v1"
CANONICALIZATION_VERSION = "receive-path-v1"
_COPY_BUFFER_BYTES = 1024 * 1024
_DEFAULT_ORPHAN_AGE = dt.timedelta(hours=24)


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
    """One regular file copied or verified in an intake payload."""

    source_path: Path
    relpath: str
    destination_path: Path
    sha256_hex: str
    size_bytes: int
    st_dev: int | None
    st_ino: int | None
    copied: bool

    @property
    def sha256_bytes(self) -> bytes:
        """Return the file digest as raw SHA-256 bytes."""

        return bytes.fromhex(self.sha256_hex)


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
    sentinel_path: Path
    file_count: int
    total_bytes: int
    skipped_count: int
    manifest_sha256: str


@dataclass(frozen=True)
class OrphanSweepResult:
    """Summary of stale `.receiving.json` directories removed from landing."""

    removed: tuple[Path, ...]


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


@dataclass(frozen=True)
class _StatSnapshot:
    size: int
    mtime_ns: int
    inode: int | None
    device: int | None


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
    atomic_observer: AtomicWriteObserver | None = None,
    after_copy_hook: Callable[[Path, tuple[FileReceipt, ...]], None] | None = None,
) -> ReceiveResult:
    """Receive one source tree into a contract-compliant landing intake.

    Bare receives always mint a new intake id. Resume is explicit and must name a
    prior sentinel-less intake whose `.receiving.json` records the original
    source and parameters. Sources are treated as read-only and must be quiescent:
    each regular file is stat-guarded before and after the read.
    """

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

    payload_root = intake_dir / "payload"
    try:
        regulars, rejected = _scan_source(source_root)
        _check_collisions(regulars)
        payload_root.mkdir(parents=True, exist_ok=True)
        _fsync_dir(payload_root)
        receipts = _copy_or_verify_entries(
            regulars,
            payload_root=payload_root,
            observer=atomic_observer,
            events=events,
        )
        if after_copy_hook is not None:
            after_copy_hook(payload_root, receipts)
        _verify_destination_files(receipts)
        entries = {receipt.relpath: receipt.sha256_hex for receipt in receipts}
        total_bytes = sum(receipt.size_bytes for receipt in receipts)
        for item in rejected:
            events.append(f"skipped {item.relpath}: {item.reason}")
        events.append(
            f"verified {len(receipts)} file(s), {total_bytes} byte(s), {len(rejected)} skipped"
        )
        _write_receive_log(intake_dir / "receive.log", events, observer=atomic_observer)

        manifest_path = intake_dir / "manifest.mhl"
        _atomic_write_text(
            manifest_path,
            manifest_text(entries),
            observer=atomic_observer,
        )
        manifest_digest = sha256_file(manifest_path)
        sentinel = {
            "intake_id": intake_id,
            "operator": operator,
            "source_kind": source_kind,
            "source_ref": source_ref,
            "artifactclass": artifactclass,
            "label": label,
            "created_at": started_at.isoformat(),
            "receive_version": RECEIVE_VERSION,
            "canonicalization_version": CANONICALIZATION_VERSION,
            "manifest_sha256": manifest_digest,
            "file_count": len(receipts),
            "total_bytes": total_bytes,
            "skipped_count": len(rejected),
        }
        _atomic_write_json(intake_dir / "intake.json", sentinel, observer=atomic_observer)
        _fsync_dir(intake_dir)
        with suppress(FileNotFoundError):
            (intake_dir / ".receiving.json").unlink()
        _fsync_dir(intake_dir)
        return ReceiveResult(
            intake_id=intake_id,
            intake_dir=intake_dir,
            manifest_path=manifest_path,
            sentinel_path=intake_dir / "intake.json",
            file_count=len(receipts),
            total_bytes=total_bytes,
            skipped_count=len(rejected),
            manifest_sha256=manifest_digest,
        )
    except ReceiveError as exc:
        events.append(f"failed: {exc}")
        if intake_dir.exists():
            _write_receive_log(intake_dir / "receive.log", events, observer=atomic_observer)
        raise


def sweep_orphans(
    landing: Path | str,
    *,
    older_than: dt.timedelta = _DEFAULT_ORPHAN_AGE,
    now: dt.datetime | None = None,
) -> OrphanSweepResult:
    """Remove stale sentinel-less receive directories from a landing root."""

    landing_root = Path(landing)
    current = now or _utcnow()
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
        verified = root / "intake.verified.json"
        if verified.exists():
            return ConfirmationResult(
                release_ok=True,
                status="verified",
                marker_path=verified,
                detail=_read_json(verified),
            )
        quarantined = root / "intake.quarantined.json"
        if quarantined.exists():
            return ConfirmationResult(
                release_ok=False,
                status="quarantined",
                marker_path=quarantined,
                detail=_read_json(quarantined),
            )
        if time.monotonic() >= deadline:
            return ConfirmationResult(
                release_ok=False,
                status="timeout",
                marker_path=None,
                detail=None,
            )
        time.sleep(poll_interval_seconds)


def hash_payload_tree(payload_root: Path | str) -> list[FileReceipt]:
    """Hash a payload tree using the same canonical relpaths as receive."""

    root = Path(payload_root)
    receipts: list[FileReceipt] = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relpath = canonicalize_filesystem_path(path, root)
        stat_result = path.stat()
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


def read_manifest_sha256(path: str | Path) -> dict[str, str]:
    """Read JSON or ASC-MHL and return canonical POSIX relpath -> SHA-256 hex."""

    manifest = Path(path)
    if manifest.suffix.lower() == ".json":
        return _read_json_manifest(manifest)
    try:
        return _read_xml_manifest(manifest)
    except ET.ParseError as exc:
        raise ValueError(f"manifest {manifest} is not valid XML") from exc


def write_mhl_manifest(path: str | Path, entries: Mapping[str, str]) -> str:
    """Write an ASC-MHL XML manifest and return its SHA-256 digest."""

    destination = Path(path)
    _atomic_write_text(destination, manifest_text(entries))
    return sha256_file(destination)


def manifest_text(entries: Mapping[str, str]) -> str:
    """Return deterministic ASC-MHL XML text for canonical relpath digests."""

    root = ET.Element("hashlist")
    for relpath in sorted(entries):
        digest = entries[relpath].lower()
        item = ET.SubElement(root, "hash")
        file_el = ET.SubElement(item, "file")
        file_el.text = f"payload/{canonicalize_manifest_path(relpath)}"
        sha_el = ET.SubElement(item, "sha256")
        sha_el.text = digest
    _indent_xml(root)
    return ET.tostring(root, encoding="unicode") + "\n"


def manifest_mismatch(actual: Mapping[str, str], expected: Mapping[str, str]) -> dict[str, Any]:
    """Return the intake manifest mismatch summary after shared canonicalization."""

    actual_canonical = {
        canonicalize_manifest_path(path): digest.lower() for path, digest in actual.items()
    }
    expected_canonical = {
        canonicalize_manifest_path(path): digest.lower() for path, digest in expected.items()
    }
    missing = sorted(path for path in actual_canonical if path not in expected_canonical)
    extra = sorted(path for path in expected_canonical if path not in actual_canonical)
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
            "missing": sorted(actual_canonical),
            "extra": [],
            "mismatched": [],
        }
    return {"missing": missing, "extra": extra, "mismatched": mismatched}


def canonicalize_filesystem_path(path: Path | str, root: Path | str) -> str:
    """Canonicalize a filesystem path relative to a root into a member name."""

    source = Path(path)
    base = Path(root)
    try:
        relative = source.relative_to(base)
    except ValueError as exc:
        raise SourceScanError(f"{source} is not under source root {base}") from exc
    return _canonicalize_parts(relative.parts, from_filesystem=True)


def canonicalize_manifest_path(raw: str) -> str:
    """Canonicalize a manifest path into the shared receive member-name form."""

    value = raw.strip()
    while value.startswith("./"):
        value = value[2:]
    value = value.lstrip("/")
    if value.startswith("payload/"):
        value = value[len("payload/") :]
    pure = PurePosixPath(value)
    return _canonicalize_parts(pure.parts, from_filesystem=False)


def slug_operator(operator: str) -> str:
    """Return a path-safe operator slug for receive intake ids."""

    normalized = unicodedata.normalize("NFKD", operator).encode("ascii", "ignore").decode()
    slug = re.sub(r"[^A-Za-z0-9]+", "-", normalized.lower()).strip("-")
    return slug or "operator"


def sha256_file(path: Path | str) -> str:
    """Return a file's SHA-256 digest without loading it into memory."""

    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(_COPY_BUFFER_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_payload_path(payload_root: Path | str, relpath: str) -> Path:
    """Return a payload destination path, rejecting traversal and symlinks."""

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


def _scan_source(source_root: Path) -> tuple[tuple[_SourceEntry, ...], tuple[RejectedEntry, ...]]:
    regulars: list[_SourceEntry] = []
    rejected: list[RejectedEntry] = []
    for root_raw, dirs, files in os.walk(source_root, topdown=True, followlinks=False):
        root = Path(root_raw)
        for dirname in list(dirs):
            path = root / dirname
            if path.is_symlink():
                relpath = canonicalize_filesystem_path(path, source_root)
                rejected.append(RejectedEntry(relpath, path, "symlink-directory"))
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
        destination = safe_payload_path(payload_root, entry.relpath)
        source_digest, snapshot = _hash_source_with_stat_guard(entry.source_path)
        copied = True
        if destination.exists():
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
        receipts.append(
            FileReceipt(
                source_path=entry.source_path,
                relpath=entry.relpath,
                destination_path=destination,
                sha256_hex=source_digest,
                size_bytes=snapshot.size,
                st_dev=snapshot.device,
                st_ino=snapshot.inode,
                copied=copied,
            )
        )
    return tuple(receipts)


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


def _hash_source_with_stat_guard(source: Path) -> tuple[str, _StatSnapshot]:
    before = _stat_snapshot(source)
    digest = sha256_file(source)
    after = _stat_snapshot(source)
    _raise_if_mutated(source, before, after)
    return digest, after


def _verify_destination_files(receipts: Iterable[FileReceipt]) -> None:
    # V1 deliberately pays a second local read before exposing the sentinel so
    # first-contact corruption is caught before the server sees the intake.
    mismatches = []
    for receipt in receipts:
        actual = sha256_file(receipt.destination_path)
        if actual != receipt.sha256_hex:
            mismatches.append(
                {
                    "path": receipt.relpath,
                    "expected": receipt.sha256_hex,
                    "actual": actual,
                }
            )
    if mismatches:
        raise DestinationVerificationError(f"destination verification failed: {mismatches}")


def _stat_snapshot(path: Path) -> _StatSnapshot:
    st = path.stat()
    return _StatSnapshot(
        size=st.st_size,
        mtime_ns=st.st_mtime_ns,
        inode=getattr(st, "st_ino", None),
        device=getattr(st, "st_dev", None),
    )


def _raise_if_mutated(path: Path, before: _StatSnapshot, after: _StatSnapshot) -> None:
    if before != after:
        raise SourceMutationError(f"source changed during receive: {path}")


def _read_json_manifest(path: Path) -> dict[str, str]:
    data = _read_json(path)
    records: dict[str, str] = {}
    if isinstance(data.get("files"), dict):
        for relpath, digest in data["files"].items():
            _add_manifest_record(records, str(relpath), str(digest))
    elif isinstance(data.get("files"), list):
        for entry in data["files"]:
            if not isinstance(entry, dict):
                continue
            relpath = entry.get("path") or entry.get("file") or entry.get("relative_path")
            digest = entry.get("sha256") or entry.get("sha256_hex")
            if relpath is not None and digest is not None:
                _add_manifest_record(records, str(relpath), str(digest))
    else:
        for relpath, digest in data.items():
            if isinstance(digest, str):
                _add_manifest_record(records, str(relpath), digest)
    return records


def _read_xml_manifest(path: Path) -> dict[str, str]:
    root = ET.parse(path).getroot()
    records: dict[str, str] = {}
    for element in root.iter():
        children = list(element)
        if not children:
            continue
        paths = [
            canonicalize_manifest_path(child.text or "")
            for child in children
            if _is_path_tag(child)
        ]
        shas = [_manifest_sha_from_element(child) for child in children]
        sha_values = [sha for sha in shas if sha is not None]
        if paths and sha_values:
            _add_manifest_record(records, paths[0], sha_values[0])
    return records


def _is_path_tag(element: ET.Element[str]) -> bool:
    name = _local_name(element.tag)
    return name in {"file", "filename", "path", "relativepath", "relative_path"}


def _manifest_sha_from_element(element: ET.Element[str]) -> str | None:
    name = _local_name(element.tag)
    text = (element.text or "").strip()
    attr_text = " ".join(str(v).lower() for v in element.attrib.values())
    if name in {"sha256", "sha-256"} and _is_sha256_hex(text):
        return text.lower()
    if name == "hash" and "sha256" in attr_text and _is_sha256_hex(text):
        return text.lower()
    for key in ("sha256", "sha-256"):
        value = element.attrib.get(key)
        if value and _is_sha256_hex(value):
            return value.lower()
    if "sha256" in attr_text:
        for value in element.attrib.values():
            if _is_sha256_hex(str(value)):
                return str(value).lower()
    return None


def _add_manifest_record(records: dict[str, str], raw_path: str, digest: str) -> None:
    try:
        relpath = canonicalize_manifest_path(raw_path)
    except ReceiveError:
        return
    if not relpath or not _is_sha256_hex(digest):
        return
    records[relpath] = digest.lower()


def _is_sha256_hex(value: str) -> bool:
    if len(value) != 64:
        return False
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True


def _local_name(tag: str) -> str:
    if "}" in tag:
        tag = tag.rsplit("}", 1)[1]
    return tag.lower().replace("-", "").replace("_", "")


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
        if candidate.name != "payload":
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


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _indent_xml(element: ET.Element[str], level: int = 0) -> None:
    indent = "\n" + level * "  "
    if len(element):
        if not element.text or not element.text.strip():
            element.text = indent + "  "
        for child in element:
            _indent_xml(child, level + 1)
        if not child.tail or not child.tail.strip():
            child.tail = indent
    if level and (not element.tail or not element.tail.strip()):
        element.tail = indent
