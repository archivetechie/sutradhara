"""Confinement and directory listing helpers for relayed card browse.

The helper is the authoritative process that can touch local mount paths. This
module keeps the shared browse/receive path guard in one place so server-sent
relative paths are resolved under the mounted card before any filesystem read.
"""

from __future__ import annotations

import fnmatch
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from sutra_agent._proto import device_pb2
from sutra_agent.mounts import MountedCard
from sutradhara_receive import (
    PACKAGE_GLOBS,
    ReceiveError,
    canonical_device_rel_path as _receive_canonical_device_rel_path,
)

MAX_DIRECTORY_FOLDERS = 5000
MAX_DIRECTORY_FILES = 500


class DevicePathError(ValueError):
    """Base class for display-safe helper path failures."""

    status: int
    message: str

    def __init__(self, message: str, *, detail: str = "") -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail

    def reason(self) -> str:
        """Return a relative-only reason suitable for ``CommandAck.reason``."""

        if self.detail:
            return f"{self.message}: {self.detail}"
        return self.message


class DevicePathConfinementError(DevicePathError):
    """Raised when a requested path is not confined to the card root."""

    status = device_pb2.DIR_STATUS_CONFINEMENT_VIOLATION


class DevicePathNotFoundError(DevicePathError):
    """Raised when a confined relative path does not exist."""

    status = device_pb2.DIR_STATUS_NOT_FOUND


class DevicePathNotDirectoryError(DevicePathError):
    """Raised when a confined relative path exists but is not a directory."""

    status = device_pb2.DIR_STATUS_NOT_A_DIRECTORY


class DevicePathPermissionError(DevicePathError):
    """Raised when a confined path cannot be read because of local permissions."""

    status = device_pb2.DIR_STATUS_PERMISSION_DENIED


class DevicePathCardUnavailableError(DevicePathError):
    """Raised when the advertised card mount is no longer present."""

    status = device_pb2.DIR_STATUS_CARD_UNAVAILABLE


class DevicePathIoError(DevicePathError):
    """Raised when an unexpected local filesystem error occurs."""

    status = device_pb2.DIR_STATUS_IO_ERROR


@dataclass(frozen=True)
class ConfinedPath:
    """Resolved card path plus its canonical relative request string."""

    path: Path
    rel_path: str


@dataclass(frozen=True)
class _DirectoryEntry:
    name: str
    is_dir: bool
    size_bytes: int
    is_package: bool


def resolve_directory(mount_path: Path, rel_path: str | None) -> ConfinedPath:
    """Resolve ``rel_path`` as an existing directory under ``mount_path``."""

    confined = resolve_card_path(mount_path, rel_path)
    try:
        mode = confined.path.stat().st_mode
    except FileNotFoundError as exc:
        raise DevicePathNotFoundError("source path not found", detail=confined.rel_path) from exc
    except PermissionError as exc:
        raise DevicePathPermissionError("permission denied", detail=confined.rel_path) from exc
    except OSError as exc:
        raise DevicePathIoError("source path cannot be read", detail=confined.rel_path) from exc
    if not stat.S_ISDIR(mode):
        raise DevicePathNotDirectoryError("source path is not a directory", detail=confined.rel_path)
    return confined


def resolve_card_path(mount_path: Path, rel_path: str | None) -> ConfinedPath:
    """Resolve ``rel_path`` under ``mount_path`` without allowing escape."""

    canonical = canonical_device_rel_path(rel_path)
    mount = Path(mount_path)
    try:
        root = mount.resolve(strict=True)
    except FileNotFoundError as exc:
        raise DevicePathCardUnavailableError("card not mounted") from exc
    except PermissionError as exc:
        raise DevicePathPermissionError("permission denied") from exc
    except OSError as exc:
        raise DevicePathIoError("card mount cannot be read") from exc

    candidate = root if canonical == "" else root.joinpath(*canonical.split("/"))
    try:
        resolved = candidate.resolve(strict=True)
    except FileNotFoundError as exc:
        raise DevicePathNotFoundError("source path not found", detail=canonical) from exc
    except PermissionError as exc:
        raise DevicePathPermissionError("permission denied", detail=canonical) from exc
    except OSError as exc:
        raise DevicePathIoError("source path cannot be read", detail=canonical) from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise DevicePathConfinementError("source path is outside the card", detail=canonical) from exc
    return ConfinedPath(path=resolved, rel_path=canonical)


def canonical_device_rel_path(value: str | None) -> str:
    """Return the canonical helper-relative path, with ``None``/``""`` as root."""

    try:
        return _receive_canonical_device_rel_path(value)
    except ReceiveError as exc:
        raise DevicePathConfinementError(str(exc)) from exc


def directory_listing_message(
    card: MountedCard | None,
    request: device_pb2.ListDirectory,
) -> device_pb2.DeviceMessage:
    """Return a ``DirectoryListing`` reply for one requested card path."""

    if card is None or card.status != "available":
        return _listing_error(
            request.request_id,
            device_pb2.DIR_STATUS_CARD_UNAVAILABLE,
            _safe_request_detail(request.rel_path),
        )
    try:
        confined = resolve_directory(card.mount_path, request.rel_path)
        entries, truncated = _directory_entries(confined.path)
    except DevicePathError as exc:
        return _listing_error(request.request_id, exc.status, exc.detail)
    return device_pb2.DeviceMessage(
        directory_listing=device_pb2.DirectoryListing(
            request_id=request.request_id,
            entries=[
                device_pb2.DirectoryEntry(
                    name=entry.name,
                    is_dir=entry.is_dir,
                    size_bytes=entry.size_bytes,
                    is_package=entry.is_package,
                )
                for entry in entries
            ],
            truncated=truncated,
            status=device_pb2.DIR_STATUS_OK,
        )
    )


def is_package_name(name: str) -> bool:
    """Return true when one path segment is an opaque macOS-style package."""

    folded = PurePosixPath(name).name.casefold()
    return any(fnmatch.fnmatchcase(folded, pattern.casefold()) for pattern in PACKAGE_GLOBS)


def _directory_entries(path: Path) -> tuple[tuple[_DirectoryEntry, ...], bool]:
    folders: list[_DirectoryEntry] = []
    files: list[_DirectoryEntry] = []
    try:
        children = list(path.iterdir())
    except PermissionError as exc:
        raise DevicePathPermissionError("permission denied") from exc
    except OSError as exc:
        raise DevicePathIoError("directory cannot be read") from exc
    for child in children:
        try:
            stat_result = child.lstat()
            mode = stat_result.st_mode
        except FileNotFoundError:
            continue
        except PermissionError as exc:
            raise DevicePathPermissionError("permission denied", detail=child.name) from exc
        except OSError as exc:
            raise DevicePathIoError("directory entry cannot be read", detail=child.name) from exc
        if stat.S_ISLNK(mode):
            continue
        if stat.S_ISDIR(mode):
            folders.append(
                _DirectoryEntry(
                    name=child.name,
                    is_dir=True,
                    size_bytes=0,
                    is_package=is_package_name(child.name),
                )
            )
        elif stat.S_ISREG(mode):
            files.append(
                _DirectoryEntry(
                    name=child.name,
                    is_dir=False,
                    size_bytes=stat_result.st_size,
                    is_package=False,
                )
            )
    folders.sort(key=lambda item: item.name.casefold())
    files.sort(key=lambda item: item.name.casefold())
    truncated = False
    if len(folders) > MAX_DIRECTORY_FOLDERS:
        folders = folders[:MAX_DIRECTORY_FOLDERS]
        files = []
        truncated = True
    elif len(files) > MAX_DIRECTORY_FILES:
        files = files[:MAX_DIRECTORY_FILES]
        truncated = True
    return tuple([*folders, *files]), truncated


def _listing_error(request_id: str, status: int, detail: str) -> device_pb2.DeviceMessage:
    return device_pb2.DeviceMessage(
        directory_listing=device_pb2.DirectoryListing(
            request_id=request_id,
            status=status,
            detail=detail,
        )
    )


def _safe_request_detail(rel_path: str) -> str:
    try:
        return canonical_device_rel_path(rel_path)
    except DevicePathError:
        return ""
