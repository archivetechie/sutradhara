"""Per-disk hdcache layout and verified I/O primitives.

The hdcache disk store owns only files below ``hdcache/v1`` on an enrolled
mount. It writes through mkstemp-unique files in ``tmp/``, hashes bytes while
streaming them, and verifies identity with a HMAC-signed disk sentinel. The
default HMAC secret path is ``/var/lib/replica/hdcache-disk-hmac.key``; callers
may inject bytes directly in tests or from service configuration.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import hmac
import json
import os
import queue
import re
import stat
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, TimeoutError
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Protocol

LAYOUT_VERSION = "v1"
HD_CACHE_DIR = "hdcache"
TMP_DIR = "tmp"
SENTINEL_NAME = "hdcache-disk.json"
RAW_REPRESENTATION = "raw-bytes"
AEAD_REPRESENTATION = "rao-aead-v1"
DEFAULT_HMAC_KEY_PATH = Path("/var/lib/replica/hdcache-disk-hmac.key")
BUFFER_SIZE = 1024 * 1024
KEY_EPOCH_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


class StoreError(RuntimeError):
    """Raised when a cache-disk store operation fails safely."""


class StoreReadTimeout(StoreError):
    """Raised when a deadline-covered cache disk operation does not finish."""


class StoreContentMismatch(StoreError):
    """Raised when bytes read from or written to cache fail content verification."""


@dataclass(frozen=True)
class EntryWriteResult:
    """Result of a verified write into the hdcache layout."""

    path: Path
    relpath: str
    size_bytes: int
    stream_digest: bytes
    stored_digest: bytes | None


@dataclass(frozen=True)
class EntryReadResult:
    """Result of a verified read from the hdcache layout."""

    path: Path
    size_bytes: int
    stream_digest: bytes
    data: bytes | None = None


@dataclass(frozen=True)
class EnumeratedEntry:
    """Entry discovered by walking hdcache filenames."""

    content_sha256: bytes
    representation: str
    key_epoch: str | None
    path: Path
    relpath: str
    size_bytes: int


@dataclass(frozen=True)
class RejectedEntryFile:
    """Cache-layout file rejected by rebuild parsing and left on disk."""

    path: Path
    relpath: str
    reason: str
    content_sha256: bytes | None = None


@dataclass(frozen=True)
class ExpectedDiskIdentity:
    """Expected physical identity for one enrolled cache disk."""

    disk_id: str
    serial: str
    fs_uuid: str
    wwn: str | None = None


@dataclass(frozen=True)
class ObservedBlockIdentity:
    """Observed block-device identity under a mounted cache disk."""

    mounted: bool
    serial: str | None = None
    fs_uuid: str | None = None
    wwn: str | None = None
    source: str | None = None


@dataclass(frozen=True)
class DiskIdentityResult:
    """Typed result for destructive-mode disk identity checks."""

    ok: bool
    status: str
    detail: str
    observed: ObservedBlockIdentity | None = None


class DiskIdentityProbe(Protocol):
    """Probe physical mount identity without creating cache paths."""

    def observe(self, mount: Path) -> ObservedBlockIdentity:
        """Return observed identity for ``mount``."""


class LocalDiskIdentityProbe:
    """Best-effort Linux identity probe using findmnt and lsblk."""

    def observe(self, mount: Path) -> ObservedBlockIdentity:
        findmnt = _run_json(
            [
                "findmnt",
                "--target",
                str(mount),
                "--json",
                "--output",
                "TARGET,SOURCE",
            ]
        )
        filesystems = findmnt.get("filesystems") if isinstance(findmnt, dict) else None
        if not filesystems:
            return ObservedBlockIdentity(mounted=False)
        filesystem = filesystems[0]
        source = str(filesystem.get("source") or "")
        if not source:
            return ObservedBlockIdentity(mounted=True)
        lsblk = _run_json(["lsblk", "--json", "--output", "NAME,PATH,SERIAL,WWN,UUID"])
        block = _find_lsblk_node(lsblk.get("blockdevices", []), source)
        if not isinstance(block, dict):
            return ObservedBlockIdentity(mounted=True, source=source)
        return ObservedBlockIdentity(
            mounted=True,
            source=source,
            serial=_nonempty(block.get("serial")),
            wwn=_nonempty(block.get("wwn")),
            fs_uuid=_nonempty(block.get("uuid")),
        )


@dataclass(frozen=True)
class _DiskReaderRequest:
    future: Future[Any]
    operation: Callable[[], Any]


@dataclass(frozen=True)
class _DiskActorSubmission:
    future: Future[Any]
    generation: int


class _DiskReaderActor:
    """One logical per-disk actor for all deadline-covered disk I/O.

    A timed-out worker cannot be killed safely, so the actor abandons that
    worker generation. Until a recovery probe succeeds, normal reads are
    rejected before they can queue behind the abandoned generation.
    """

    def __init__(self, disk_id: str) -> None:
        self._disk_id = disk_id
        self._lock = threading.Lock()
        self._state = "normal"
        self._generation = 0
        self._queue: queue.Queue[_DiskReaderRequest] = queue.Queue()
        self._start_worker_locked()

    def submit(
        self,
        operation: Callable[[], Any],
        *,
        recovery_probe: bool = False,
    ) -> _DiskActorSubmission:
        future: Future[Any] = Future()
        with self._lock:
            if self._state != "normal" and not recovery_probe:
                future.set_exception(
                    StoreReadTimeout("cache disk reader is awaiting recovery probe")
                )
                return _DiskActorSubmission(future=future, generation=self._generation)
            if recovery_probe and self._state == "abandoned":
                self._queue = queue.Queue()
                self._generation += 1
                self._state = "recovering"
                self._start_worker_locked()
            generation = self._generation
            self._queue.put(_DiskReaderRequest(future=future, operation=operation))
            return _DiskActorSubmission(future=future, generation=generation)

    def mark_abandoned(self, generation: int) -> None:
        with self._lock:
            if generation == self._generation:
                self._state = "abandoned"

    def mark_recovered(self, generation: int) -> None:
        with self._lock:
            if generation == self._generation:
                self._state = "normal"

    def _start_worker_locked(self) -> None:
        thread = threading.Thread(
            target=self._run,
            args=(self._queue,),
            name=f"hdcache-reader-{_thread_token(self._disk_id)}",
            daemon=True,
        )
        thread.start()

    def _run(self, requests: queue.Queue[_DiskReaderRequest]) -> None:
        while True:
            request = requests.get()
            if not request.future.set_running_or_notify_cancel():
                continue
            try:
                result = request.operation()
            except Exception as exc:
                request.future.set_exception(exc)
            else:
                request.future.set_result(result)


_DISK_READERS: dict[str, _DiskReaderActor] = {}
_DISK_READERS_LOCK = threading.Lock()


def read_hmac_secret(path: Path = DEFAULT_HMAC_KEY_PATH) -> bytes:
    """Read the server-held disk-sentinel HMAC secret."""

    secret = path.read_bytes()
    if not secret:
        raise StoreError(f"empty hdcache HMAC key: {path}")
    return secret


def initialize_layout(mount: Path) -> None:
    """Create the non-destructive hdcache layout under an enrolled mount."""

    entries_root(mount).mkdir(parents=True, exist_ok=True)
    tmp_root(mount).mkdir(parents=True, exist_ok=True)
    _fsync_dir(entries_root(mount))
    _fsync_dir(layout_root(mount))


def write_disk_sentinel(
    mount: Path,
    expected: ExpectedDiskIdentity,
    *,
    hmac_secret: bytes,
    enrolled_at: dt.datetime | None = None,
) -> Path:
    """Write the signed disk sentinel after provisioning has mounted the disk."""

    initialize_layout(mount)
    enrolled = enrolled_at or dt.datetime.now(dt.UTC)
    payload: dict[str, str] = {
        "disk_id": expected.disk_id,
        "serial": expected.serial,
        "fs_uuid": expected.fs_uuid,
        "layout": LAYOUT_VERSION,
        "enrolled_at": enrolled.isoformat(),
    }
    payload["hmac"] = _sentinel_hmac(payload, hmac_secret)
    path = mount / SENTINEL_NAME
    _atomic_write_json(path, payload)
    return path


def verify_disk_identity(
    mount: Path,
    expected: ExpectedDiskIdentity,
    *,
    hmac_secret: bytes,
    probe: DiskIdentityProbe | None = None,
) -> DiskIdentityResult:
    """Verify mount, block identity, filesystem UUID, and sentinel HMAC."""

    final_probe = probe or LocalDiskIdentityProbe()
    observed = final_probe.observe(mount)
    if not observed.mounted:
        return DiskIdentityResult(False, "not_mounted", "expected disk is not mounted", observed)
    if observed.serial is None:
        return DiskIdentityResult(
            False,
            "identity_unavailable",
            "block serial is unavailable",
            observed,
        )
    if observed.serial != expected.serial:
        return DiskIdentityResult(False, "wrong_serial", "block serial mismatch", observed)
    if observed.fs_uuid is None:
        return DiskIdentityResult(
            False,
            "identity_unavailable",
            "filesystem UUID is unavailable",
            observed,
        )
    if observed.fs_uuid != expected.fs_uuid:
        return DiskIdentityResult(False, "wrong_fs_uuid", "filesystem UUID mismatch", observed)
    if expected.wwn:
        if observed.wwn is None:
            return DiskIdentityResult(
                False,
                "identity_unavailable",
                "block WWN is unavailable",
                observed,
            )
        if observed.wwn != expected.wwn:
            return DiskIdentityResult(False, "wrong_wwn", "block WWN mismatch", observed)

    sentinel_path = mount / SENTINEL_NAME
    if not sentinel_path.exists():
        return DiskIdentityResult(False, "missing_sentinel", "disk sentinel is missing", observed)
    if not _is_regular_file(sentinel_path):
        return DiskIdentityResult(
            False,
            "bad_sentinel",
            "disk sentinel is not a regular file",
            observed,
        )
    try:
        sentinel = json.loads(sentinel_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return DiskIdentityResult(False, "bad_sentinel", f"disk sentinel is unreadable: {exc}", observed)
    if not isinstance(sentinel, dict):
        return DiskIdentityResult(False, "bad_sentinel", "disk sentinel is not an object", observed)
    hmac_value = sentinel.get("hmac")
    if not isinstance(hmac_value, str):
        return DiskIdentityResult(False, "bad_hmac", "disk sentinel has no HMAC", observed)
    unsigned = {key: value for key, value in sentinel.items() if key != "hmac"}
    expected_hmac = _sentinel_hmac(unsigned, hmac_secret)
    if not hmac.compare_digest(hmac_value, expected_hmac):
        return DiskIdentityResult(False, "bad_hmac", "disk sentinel HMAC mismatch", observed)
    for key, value in (
        ("disk_id", expected.disk_id),
        ("serial", expected.serial),
        ("fs_uuid", expected.fs_uuid),
        ("layout", LAYOUT_VERSION),
    ):
        if sentinel.get(key) != value:
            return DiskIdentityResult(False, "sentinel_mismatch", f"sentinel {key} mismatch", observed)
    return DiskIdentityResult(True, "ok", "disk identity verified", observed)


def verify_disk_identity_with_deadline(
    mount: Path,
    expected: ExpectedDiskIdentity,
    *,
    hmac_secret: bytes,
    disk_id: str,
    probe: DiskIdentityProbe | None = None,
    deadline_monotonic: float,
) -> DiskIdentityResult:
    """Verify disk identity through the per-disk actor with a bounded deadline."""

    return _run_disk_actor_operation(
        disk_id,
        lambda: verify_disk_identity(
            mount,
            expected,
            hmac_secret=hmac_secret,
            probe=probe,
        ),
        deadline_monotonic=deadline_monotonic,
        timeout_message="cache disk identity check deadline exceeded",
    )


def probe_disk_liveness_with_deadline(
    mount: Path,
    expected: ExpectedDiskIdentity,
    *,
    hmac_secret: bytes,
    disk_id: str,
    probe: DiskIdentityProbe | None = None,
    deadline_monotonic: float,
) -> DiskIdentityResult:
    """Run a statfs + sentinel identity probe through the disk's bounded reader."""

    def operation() -> DiskIdentityResult:
        os.statvfs(mount)
        return verify_disk_identity(
            mount,
            expected,
            hmac_secret=hmac_secret,
            probe=probe,
        )

    return _run_disk_actor_operation(
        disk_id,
        operation,
        deadline_monotonic=deadline_monotonic,
        timeout_message="cache disk liveness probe deadline exceeded",
        recovery_probe=True,
        recovery_succeeded=lambda result: result.ok,
    )


def write_entry(
    mount: Path,
    content_sha256: bytes,
    source: BinaryIO | Iterable[bytes] | bytes,
    *,
    representation: str = RAW_REPRESENTATION,
    key_epoch: str | None = None,
    expected_stream_sha256: bytes | None = None,
    before_rename: Any | None = None,
) -> EntryWriteResult:
    """Atomically write one cache entry while hashing the stored stream."""

    final_path = entry_path(
        mount,
        content_sha256,
        representation=representation,
        key_epoch=key_epoch,
    )
    tmp_dir = tmp_root(mount)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    expected = _expected_stream_digest(
        content_sha256,
        representation,
        expected_stream_sha256,
    )
    fd, tmp_name = tempfile.mkstemp(prefix=".entry.", suffix=".tmp", dir=tmp_dir)
    tmp_path = Path(tmp_name)
    digest = hashlib.sha256()
    size = 0
    try:
        with os.fdopen(fd, "wb") as handle:
            for chunk in _iter_chunks(source):
                digest.update(chunk)
                handle.write(chunk)
                size += len(chunk)
            handle.flush()
            os.fsync(handle.fileno())
        stream_digest = digest.digest()
        if stream_digest != expected:
            raise StoreContentMismatch(
                f"stream digest mismatch: expected {expected.hex()}, actual {stream_digest.hex()}"
            )
        if before_rename is not None:
            before_rename(tmp_path, final_path)
        os.replace(tmp_path, final_path)
        _fsync_dir(final_path.parent)
        _fsync_dir(tmp_dir)
        stored_digest = stream_digest if representation == AEAD_REPRESENTATION else None
        return EntryWriteResult(
            path=final_path,
            relpath=str(final_path.relative_to(layout_root(mount))),
            size_bytes=size,
            stream_digest=stream_digest,
            stored_digest=stored_digest,
        )
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            tmp_path.unlink()
        raise


def read_entry_verified(
    mount: Path,
    content_sha256: bytes,
    *,
    representation: str = RAW_REPRESENTATION,
    key_epoch: str | None = None,
    expected_stream_sha256: bytes | None = None,
    output: BinaryIO | None = None,
    deadline_monotonic: float | None = None,
    disk_id: str | None = None,
) -> EntryReadResult:
    """Read one entry while verifying the stored stream digest."""

    path = entry_path(
        mount,
        content_sha256,
        representation=representation,
        key_epoch=key_epoch,
    )
    expected = _expected_stream_digest(
        content_sha256,
        representation,
        expected_stream_sha256,
    )
    if deadline_monotonic is not None:
        return _read_entry_verified_with_deadline(
            path,
            expected,
            output=output,
            deadline_monotonic=deadline_monotonic,
            disk_id=disk_id or os.fspath(mount),
        )
    return _read_entry_verified_direct(path, expected, output=output)


def _read_entry_verified_direct(
    path: Path,
    expected: bytes,
    *,
    output: BinaryIO | None,
) -> EntryReadResult:
    _require_regular_file(path, "cache entry")
    digest = hashlib.sha256()
    size = 0
    chunks: list[bytes] | None = [] if output is None else None
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(BUFFER_SIZE), b""):
            digest.update(chunk)
            size += len(chunk)
            if output is not None:
                output.write(chunk)
            else:
                assert chunks is not None
                chunks.append(chunk)
    stream_digest = digest.digest()
    if stream_digest != expected:
        raise StoreContentMismatch(
            f"stored stream digest mismatch: expected {expected.hex()}, actual {stream_digest.hex()}"
        )
    return EntryReadResult(
        path=path,
        size_bytes=size,
        stream_digest=stream_digest,
        data=b"".join(chunks) if chunks is not None else None,
    )


def _read_entry_verified_with_deadline(
    path: Path,
    expected: bytes,
    *,
    output: BinaryIO | None,
    deadline_monotonic: float,
    disk_id: str,
) -> EntryReadResult:
    return _run_disk_actor_operation(
        disk_id,
        lambda: _read_entry_verified_direct(path, expected, output=output),
        deadline_monotonic=deadline_monotonic,
        timeout_message="cache read deadline exceeded",
    )


def delete_entry(
    mount: Path,
    content_sha256: bytes,
    *,
    representation: str = RAW_REPRESENTATION,
    key_epoch: str | None = None,
) -> bool:
    """Delete one entry, confined by construction to ``hdcache/v1``."""

    path = entry_path(
        mount,
        content_sha256,
        representation=representation,
        key_epoch=key_epoch,
    )
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    _fsync_dir(path.parent)
    return True


def enumerate_entries(mount: Path) -> list[EnumeratedEntry]:
    """Enumerate valid hdcache entry filenames under the v1 layout."""

    root = entries_root(mount)
    if not root.exists():
        return []
    entries: list[EnumeratedEntry] = []
    for prefix_dir in sorted(root.iterdir()):
        if not prefix_dir.is_dir() or prefix_dir.name == TMP_DIR:
            continue
        if len(prefix_dir.name) != 2:
            continue
        for path in sorted(prefix_dir.iterdir()):
            if not _is_regular_file(path):
                continue
            parsed = _parse_entry_filename(path.name)
            if parsed is None:
                continue
            content_sha256, representation, key_epoch = parsed
            if content_sha256.hex()[:2] != prefix_dir.name:
                continue
            entries.append(
                EnumeratedEntry(
                    content_sha256=content_sha256,
                    representation=representation,
                    key_epoch=key_epoch,
                    path=path,
                    relpath=str(path.relative_to(layout_root(mount))),
                    size_bytes=path.stat().st_size,
                )
            )
    return entries


def enumerate_rejected_entry_files(mount: Path) -> list[RejectedEntryFile]:
    """Enumerate malformed hdcache entry files that rebuild must report."""

    root = entries_root(mount)
    if not root.exists():
        return []
    rejected: list[RejectedEntryFile] = []
    for prefix_dir in sorted(root.iterdir()):
        if not prefix_dir.is_dir() or prefix_dir.name == TMP_DIR:
            continue
        prefix_valid = len(prefix_dir.name) == 2
        for path in sorted(prefix_dir.iterdir()):
            if not _is_regular_file(path):
                continue
            relpath = str(path.relative_to(layout_root(mount)))
            if not prefix_valid:
                rejected.append(RejectedEntryFile(path, relpath, "malformed-prefix"))
                continue
            parsed = _parse_entry_filename(path.name)
            if parsed is None:
                rejected.append(RejectedEntryFile(path, relpath, "malformed-name"))
                continue
            content_sha256, _representation, _key_epoch = parsed
            if content_sha256.hex()[:2] != prefix_dir.name:
                rejected.append(
                    RejectedEntryFile(
                        path,
                        relpath,
                        "prefix-mismatch",
                        content_sha256=content_sha256,
                    )
                )
    return rejected


def entry_path(
    mount: Path,
    content_sha256: bytes,
    *,
    representation: str = RAW_REPRESENTATION,
    key_epoch: str | None = None,
) -> Path:
    """Return the canonical path for one entry under ``mount``."""

    if len(content_sha256) != 32:
        raise StoreError("content_sha256 must be 32 bytes")
    filename = _entry_filename(
        content_sha256,
        representation=representation,
        key_epoch=key_epoch,
    )
    prefix = content_sha256.hex()[:2]
    return entries_root(mount) / prefix / filename


def layout_root(mount: Path) -> Path:
    return mount / HD_CACHE_DIR / LAYOUT_VERSION


def entries_root(mount: Path) -> Path:
    return layout_root(mount)


def tmp_root(mount: Path) -> Path:
    return layout_root(mount) / TMP_DIR


def _run_disk_actor_operation(
    disk_id: str,
    operation: Callable[[], Any],
    *,
    deadline_monotonic: float,
    timeout_message: str,
    recovery_probe: bool = False,
    recovery_succeeded: Callable[[Any], bool] | None = None,
) -> Any:
    remaining = deadline_monotonic - time.monotonic()
    if remaining <= 0:
        raise StoreReadTimeout(timeout_message)
    actor = _reader_for_disk(disk_id)
    submission = actor.submit(operation, recovery_probe=recovery_probe)
    try:
        result = submission.future.result(timeout=remaining)
    except TimeoutError as exc:
        submission.future.cancel()
        actor.mark_abandoned(submission.generation)
        raise StoreReadTimeout(timeout_message) from exc
    if recovery_probe and (recovery_succeeded is None or recovery_succeeded(result)):
        actor.mark_recovered(submission.generation)
    return result


def _reader_for_disk(disk_id: str) -> _DiskReaderActor:
    with _DISK_READERS_LOCK:
        actor = _DISK_READERS.get(disk_id)
        if actor is None:
            actor = _DiskReaderActor(disk_id)
            _DISK_READERS[disk_id] = actor
        return actor


def _thread_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")
    return (token or "unknown")[:48]


def _entry_filename(
    content_sha256: bytes,
    *,
    representation: str,
    key_epoch: str | None,
) -> str:
    sha = content_sha256.hex()
    if representation == RAW_REPRESENTATION:
        if key_epoch is not None:
            raise StoreError("raw cache entries must not specify key_epoch")
        return sha
    if representation == AEAD_REPRESENTATION:
        if not key_epoch:
            raise StoreError("AEAD cache entries require key_epoch")
        _validate_key_epoch(key_epoch)
        return f"{sha}.{AEAD_REPRESENTATION}.{key_epoch}"
    raise StoreError(f"unsupported cache representation: {representation}")


def _parse_entry_filename(name: str) -> tuple[bytes, str, str | None] | None:
    parts = name.split(".")
    if len(parts[0]) != 64:
        return None
    try:
        content_sha256 = bytes.fromhex(parts[0])
    except ValueError:
        return None
    if len(parts) == 1:
        return content_sha256, RAW_REPRESENTATION, None
    if len(parts) == 3 and parts[1] == AEAD_REPRESENTATION and _is_valid_key_epoch(parts[2]):
        return content_sha256, AEAD_REPRESENTATION, parts[2]
    return None


def _expected_stream_digest(
    content_sha256: bytes,
    representation: str,
    expected_stream_sha256: bytes | None,
) -> bytes:
    if expected_stream_sha256 is not None:
        _validate_digest(expected_stream_sha256, "expected_stream_sha256")
        return expected_stream_sha256
    return _default_expected_digest(content_sha256, representation)


def _default_expected_digest(content_sha256: bytes, representation: str) -> bytes:
    if representation == RAW_REPRESENTATION:
        return content_sha256
    raise StoreError("expected_stream_sha256 is required for sealed cache entries")


def _validate_digest(value: bytes, label: str) -> None:
    if len(value) != 32:
        raise StoreError(f"{label} must be 32 bytes")


def _validate_key_epoch(key_epoch: str) -> None:
    if not _is_valid_key_epoch(key_epoch):
        raise StoreError("key_epoch must be 1-128 ASCII letters, digits, '_' or '-'")


def _is_valid_key_epoch(key_epoch: str) -> bool:
    return KEY_EPOCH_PATTERN.fullmatch(key_epoch) is not None


def _iter_chunks(source: BinaryIO | Iterable[bytes] | bytes) -> Iterator[bytes]:
    if isinstance(source, bytes):
        yield source
        return
    read = getattr(source, "read", None)
    if callable(read):
        while True:
            chunk = read(BUFFER_SIZE)
            if not chunk:
                return
            yield bytes(chunk)
    else:
        for chunk in source:
            if chunk:
                yield bytes(chunk)


def _sentinel_hmac(payload: dict[str, Any], secret: bytes) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hmac.new(secret, canonical, hashlib.sha256).hexdigest()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    data = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        _fsync_dir(path.parent)
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            tmp_path.unlink()
        raise


def _is_regular_file(path: Path) -> bool:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return False
    return stat.S_ISREG(mode)


def _require_regular_file(path: Path, label: str) -> None:
    if not _is_regular_file(path):
        raise StoreError(f"{label} is not a regular file: {path}")


def _fsync_dir(path: Path) -> None:
    with contextlib.suppress(OSError):
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _run_json(args: list[str]) -> dict[str, Any]:
    try:
        output = subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL)
    except (OSError, subprocess.CalledProcessError):
        return {}
    try:
        payload = json.loads(output)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _find_lsblk_node(nodes: object, source: str) -> dict[str, Any] | None:
    if not isinstance(nodes, list):
        return None
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if node.get("path") == source or node.get("name") == source:
            return node
        found = _find_lsblk_node(node.get("children"), source)
        if found is not None:
            return found
    return None


def _nonempty(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None
