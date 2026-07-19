"""Rsync/SSH-backed object storage for encrypted temporary ingest copies.

This adapter presents a LAN file server as a Sutradhara storage backend without
mounting it locally. The catalog stores only per-object identity in locators;
host/root are resolved from the current Backend row when this adapter is built.
"""

from __future__ import annotations

import hashlib
import posixpath
import shlex
import subprocess
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from subprocess import CompletedProcess
from typing import BinaryIO, Protocol

from sutradhara.backend.port import (
    BackendError,
    BackendLocator,
    BackendNotFoundError,
    BackendUnavailableError,
    ByteRange,
    CopyRecord,
    StreamKind,
    VerifyResult,
)
from sutradhara.catalog.types import ContentHash, content_hash

_ABSENT_SENTINEL = 42
_SSH_UNAVAILABLE = 255
_DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
_DEFAULT_COMMAND_TIMEOUT_SECONDS = 300.0


class RemoteTransport(Protocol):
    def put(self, local: Path, relpath: str) -> None: ...

    def get(self, relpath: str, local: Path) -> None: ...

    def sha256(self, relpath: str) -> str | None: ...

    def size(self, relpath: str) -> int | None: ...

    def remove(self, relpath: str) -> None: ...

    def list_files(self) -> Iterator[str]: ...


class CommandRunner(Protocol):
    def __call__(
        self,
        argv: Sequence[str],
        *,
        timeout: float,
        shell: bool,
    ) -> CompletedProcess[str]: ...


class RsyncSshTransport:
    """Subprocess transport that copies objects with rsync and probes via ssh."""

    def __init__(
        self,
        host: str,
        root: str,
        user: str | None = None,
        identity_file: str | None = None,
        ssh_options: Sequence[str] | None = None,
        *,
        runner: CommandRunner | None = None,
        connect_timeout_seconds: float = _DEFAULT_CONNECT_TIMEOUT_SECONDS,
        command_timeout_seconds: float = _DEFAULT_COMMAND_TIMEOUT_SECONDS,
    ) -> None:
        if not host:
            raise ValueError("RsyncSshTransport requires a host")
        if not root:
            raise ValueError("RsyncSshTransport requires a root")
        self._host = host
        self._root = root.rstrip("/") or "/"
        self._user = user
        self._identity_file = identity_file
        self._ssh_options = list(ssh_options or [])
        self._runner = runner or _default_runner
        self._connect_timeout_seconds = connect_timeout_seconds
        self._command_timeout_seconds = command_timeout_seconds

    def put(self, local: Path, relpath: str) -> None:
        final_path = self._remote_path(relpath)
        parent = posixpath.dirname(final_path) or self._root
        partial_path = f"{final_path}.partial"
        self._run_checked_ssh(
            f"mkdir -p {shlex.quote(parent)}",
            operation=f"mkdir for {relpath!r}",
        )
        self._run_checked(
            [
                "rsync",
                "-a",
                "--partial",
                "--protect-args",
                "-e",
                shlex.join(self._ssh_base_args()),
                str(local),
                self._remote_spec(partial_path),
            ],
            operation=f"rsync put {relpath!r}",
        )
        self._run_checked_ssh(
            f"mv -f {shlex.quote(partial_path)} {shlex.quote(final_path)}",
            operation=f"rename {relpath!r}",
        )

    def get(self, relpath: str, local: Path) -> None:
        if self.size(relpath) is None:
            raise BackendNotFoundError(f"ssh_disk object not found: {relpath}")
        result = self._run(
            [
                "rsync",
                "-a",
                "--protect-args",
                "-e",
                shlex.join(self._ssh_base_args()),
                self._remote_spec(self._remote_path(relpath)),
                str(local),
            ]
        )
        self._check_result(result, operation=f"rsync get {relpath!r}")

    def sha256(self, relpath: str) -> str | None:
        remote_path = self._remote_path(relpath)
        result = self._run_ssh(
            f"test -e {shlex.quote(remote_path)} || exit {_ABSENT_SENTINEL}; "
            f"sha256sum {shlex.quote(remote_path)}"
        )
        if result.returncode == _ABSENT_SENTINEL:
            return None
        self._check_result(result, operation=f"sha256 {relpath!r}")
        fields = result.stdout.strip().split()
        if not fields:
            raise BackendError(f"sha256 {relpath!r} returned no digest")
        return fields[0]

    def size(self, relpath: str) -> int | None:
        remote_path = self._remote_path(relpath)
        result = self._run_ssh(
            f"test -e {shlex.quote(remote_path)} || exit {_ABSENT_SENTINEL}; "
            f"stat -c %s {shlex.quote(remote_path)}"
        )
        if result.returncode == _ABSENT_SENTINEL:
            return None
        self._check_result(result, operation=f"stat {relpath!r}")
        try:
            size = int(result.stdout.strip())
        except ValueError as exc:
            raise BackendError(
                f"stat {relpath!r} returned invalid size: {result.stdout!r}"
            ) from exc
        if size < 0:
            raise BackendError(f"stat {relpath!r} returned negative size: {size}")
        return size

    def remove(self, relpath: str) -> None:
        self._run_checked_ssh(
            f"rm -f {shlex.quote(self._remote_path(relpath))}",
            operation=f"remove {relpath!r}",
        )

    def list_files(self) -> Iterator[str]:
        partial_pattern = shlex.quote("*.partial")
        printf_format = shlex.quote("%P\\n")
        result = self._run_ssh(
            "find "
            f"{shlex.quote(self._root)} -type f ! -name {partial_pattern} "
            f"-printf {printf_format}"
        )
        self._check_result(result, operation="list ssh_disk files")
        return iter(result.stdout.splitlines())

    def _run_checked_ssh(self, remote_command: str, *, operation: str) -> None:
        self._check_result(self._run_ssh(remote_command), operation=operation)

    def _run_ssh(self, remote_command: str) -> CompletedProcess[str]:
        return self._run([*self._ssh_base_args(), self._target(), remote_command])

    def _run_checked(self, argv: Sequence[str], *, operation: str) -> None:
        self._check_result(self._run(argv), operation=operation)

    def _run(self, argv: Sequence[str]) -> CompletedProcess[str]:
        try:
            return self._runner(
                list(argv),
                timeout=self._command_timeout_seconds,
                shell=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise BackendUnavailableError(f"command timed out: {shlex.join(argv)}") from exc
        except FileNotFoundError as exc:
            raise BackendUnavailableError(f"command not found: {argv[0]}") from exc

    def _check_result(self, result: CompletedProcess[str], *, operation: str) -> None:
        if result.returncode == 0:
            return
        detail = (result.stderr or result.stdout or "").strip()
        if result.returncode == _SSH_UNAVAILABLE:
            raise BackendUnavailableError(
                f"{operation} failed: backend unavailable (exit 255)"
                + (f": {detail}" if detail else "")
            )
        raise BackendError(
            f"{operation} failed with exit {result.returncode}" + (f": {detail}" if detail else "")
        )

    def _ssh_base_args(self) -> list[str]:
        args = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={int(self._connect_timeout_seconds)}",
        ]
        if self._identity_file:
            args.extend(["-i", self._identity_file])
        args.extend(self._ssh_options)
        return args

    def _target(self) -> str:
        if self._user:
            return f"{self._user}@{self._host}"
        return self._host

    def _remote_spec(self, remote_path: str) -> str:
        return f"{self._target()}:{remote_path}"

    def _remote_path(self, relpath: str) -> str:
        if self._root == "/":
            return f"/{relpath}"
        return f"{self._root}/{relpath}"


class SshDiskBackend:
    """StorageBackend implementation for a validated relative-key SSH object store."""

    def __init__(
        self,
        name: str,
        *,
        host: str,
        root: str,
        user: str | None = None,
        identity_file: str | None = None,
        ssh_options: Sequence[str] | None = None,
        transport: RemoteTransport | None = None,
    ) -> None:
        if not name:
            raise ValueError("SshDiskBackend requires a name")
        self._name = name
        self._transport = transport or RsyncSshTransport(
            host,
            root,
            user=user,
            identity_file=identity_file,
            ssh_options=ssh_options,
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def stream_kind(self) -> StreamKind:
        """SSH disk reads rsync the whole object to scratch before delivery."""

        return StreamKind.scratch_stream

    def enumerate(self) -> Iterator[CopyRecord]:
        for relpath in self._transport.list_files():
            try:
                key = _validate_key(relpath)
            except ValueError:
                continue
            digest_hex = self._transport.sha256(key)
            size = self._transport.size(key)
            if digest_hex is None or size is None:
                continue
            if isinstance(size, bool) or not isinstance(size, int) or size < 0:
                continue
            try:
                digest = _content_hash_from_hex(digest_hex)
            except ValueError:
                continue
            yield CopyRecord(
                logical_id=digest,
                native_locator={"key": key, "sha256": digest.hex(), "size_bytes": size},
                integrity_hash=digest,
                size_bytes=size,
            )

    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        source_path = Path(source)
        clean_pool = pool.strip("/")
        key = f"{clean_pool}/{source_path.name}" if clean_pool else source_path.name
        return self.write_object(source_path, key=key, pool=pool)

    def write_object(self, source: Path | str, *, key: str, pool: str | None = None) -> CopyRecord:
        source_path = Path(source)
        final_key = _validate_key(key)
        digest = content_hash(_sha256_file(source_path))
        size = source_path.stat().st_size
        self._transport.put(source_path, final_key)
        return CopyRecord(
            logical_id=digest,
            native_locator={"key": final_key, "sha256": digest.hex(), "size_bytes": size},
            integrity_hash=digest,
            size_bytes=size,
            metadata=({"pool": pool} if pool else {}),
        )

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        key = _locator_key(locator)
        with tempfile.TemporaryDirectory(prefix="sutradhara-ssh-disk-") as raw:
            tmp = Path(raw) / "object"
            try:
                self._transport.get(key, tmp)
            except (FileNotFoundError, BackendNotFoundError) as exc:
                raise BackendNotFoundError(f"ssh_disk object not found: {key}") from exc
            if byte_range.is_whole_object:
                return tmp.read_bytes()
            with tmp.open("rb") as fh:
                fh.seek(byte_range.start)
                return fh.read(byte_range.length)

    @contextmanager
    def open_materialized_range_chunks(
        self,
        locator: BackendLocator,
        byte_range: ByteRange,
        *,
        chunk_bytes: int,
    ) -> Iterator[Iterator[bytes]]:
        """Rsync wholly to scratch, then keep that scratch alive while chunking."""

        if chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be greater than zero")
        key = _locator_key(locator)
        with tempfile.TemporaryDirectory(prefix="sutradhara-ssh-disk-") as raw:
            tmp = Path(raw) / "object"
            self._download(key, tmp)
            size = tmp.stat().st_size
            end = size if byte_range.is_whole_object else byte_range.end
            if end > size:
                raise ValueError(f"byte range end {end} exceeds object size {size}")
            with tmp.open("rb") as source:
                source.seek(byte_range.start)
                yield _read_file_chunks(
                    source,
                    remaining=end - byte_range.start,
                    chunk_bytes=chunk_bytes,
                )

    def verify(self, locator: BackendLocator) -> VerifyResult:
        key = _locator_key(locator)
        digest_value: object = self._transport.sha256(key)
        if digest_value is None:
            return VerifyResult(ok=False, measured=False, detail="absent")
        if not isinstance(digest_value, str):
            return VerifyResult(ok=False, measured=False, detail="invalid hash")
        digest_hex = digest_value
        expected_hex = locator.get("sha256")
        if not isinstance(expected_hex, str):
            return VerifyResult(ok=False, measured=False, detail="invalid hash")
        try:
            actual = _content_hash_from_hex(digest_hex)
            expected = _content_hash_from_hex(expected_hex)
        except ValueError:
            return VerifyResult(ok=False, measured=False, detail="invalid hash")
        if actual == expected:
            return VerifyResult(ok=True, measured=True, actual_hash=actual)
        return VerifyResult(
            ok=False,
            measured=True,
            actual_hash=actual,
            detail=f"expected {expected.hex()[:12]}..., got {actual.hex()[:12]}...",
        )

    def delete_object(self, locator: BackendLocator) -> bool:
        key = _locator_key(locator)
        existed = self._transport.sha256(key) is not None
        self._transport.remove(key)
        return existed

    def _download(self, key: str, destination: Path) -> None:
        try:
            self._transport.get(key, destination)
        except (FileNotFoundError, BackendNotFoundError) as exc:
            raise BackendNotFoundError(f"ssh_disk object not found: {key}") from exc


def _default_runner(
    argv: Sequence[str],
    *,
    timeout: float,
    shell: bool,
) -> CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        check=False,
        shell=shell,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _locator_key(locator: BackendLocator) -> str:
    key = locator.get("key")
    if not isinstance(key, str):
        raise BackendNotFoundError(f"ssh_disk locator must contain key; got {locator!r}")
    return _validate_key(key)


def _validate_key(key: str) -> str:
    if not isinstance(key, str) or not key:
        raise ValueError("ssh_disk key must be a non-empty string")
    if key.startswith("/") or "\\" in key:
        raise ValueError(f"unsafe ssh_disk key: {key!r}")
    if any(ord(char) < 32 or ord(char) == 127 for char in key):
        raise ValueError(f"unsafe ssh_disk key: {key!r}")
    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"unsafe ssh_disk key: {key!r}")
    normalized = posixpath.normpath(key)
    if posixpath.isabs(normalized) or normalized == ".." or normalized.startswith("../"):
        raise ValueError(f"unsafe ssh_disk key: {key!r}")
    return key


def _content_hash_from_hex(value: str) -> ContentHash:
    return content_hash(bytes.fromhex(value))


def _sha256_file(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()


def _read_file_chunks(
    source: BinaryIO,
    *,
    remaining: int,
    chunk_bytes: int,
) -> Iterator[bytes]:
    """Read a bounded range from the materialized SSH scratch file."""

    while remaining:
        chunk = source.read(min(chunk_bytes, remaining))
        if not chunk:
            raise BackendError("ssh_disk scratch object ended before the requested range")
        remaining -= len(chunk)
        yield chunk
