"""Local mount discovery for the operator-console helper.

The control daemon reports only opaque card identifiers to the Sutradhara server.
Local mount paths stay inside this process and are used solely to start the
streaming receive once the server commands a card by its opaque id.
"""

from __future__ import annotations

import ctypes
import importlib
import os
import platform
import plistlib
import shutil
import subprocess
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from sutradhara_receive import derive_card_id

CARD_KIND_CARD = "card"
CARD_KIND_DRIVE = "drive"
CARD_KIND_OTHER = "other"
CARD_KINDS = {CARD_KIND_CARD, CARD_KIND_DRIVE, CARD_KIND_OTHER}

DRIVE_UNKNOWN = 0
DRIVE_NO_ROOT_DIR = 1
DRIVE_REMOVABLE = 2
DRIVE_FIXED = 3
DRIVE_REMOTE = 4
DRIVE_CDROM = 5
DRIVE_RAMDISK = 6


@dataclass(frozen=True)
class MountedCard:
    """A locally mounted source that can be advertised to the server relay."""

    card_id: str
    label: str
    kind: str
    size_bytes: int
    status: str
    mount_path: Path

    def __post_init__(self) -> None:
        if self.kind not in CARD_KINDS:
            raise ValueError(f"unknown card kind: {self.kind!r}")
        if not self.card_id:
            raise ValueError("card_id must be non-empty")


@dataclass(frozen=True)
class MountInfo:
    """Raw mount metadata before it is converted into an outbound card view."""

    mount_path: Path
    label: str
    source: str | None = None
    volume_uuid: str | None = None
    removable: bool = False
    size_bytes: int | None = None


class CardSource(Protocol):
    """Callable source of the current local card snapshot."""

    def __call__(self) -> list[MountedCard]:
        """Return the current mounted-card list."""


def current_cards() -> list[MountedCard]:
    """Return local card/drive mounts visible to the helper."""

    return [card_from_mount(info) for info in current_mounts()]


def current_mounts() -> list[MountInfo]:
    """Enumerate likely removable/media mounts on the current platform."""

    system = platform.system()
    if system == "Darwin":
        return _darwin_mounts()
    if system == "Windows":
        return _windows_mounts()
    return _posix_mounts()


def card_from_mount(info: MountInfo) -> MountedCard:
    """Convert local mount metadata into an opaque server-facing card record."""

    mount_path = info.mount_path.expanduser()
    size = info.size_bytes
    if size is None:
        try:
            size = shutil.disk_usage(mount_path).total
        except OSError:
            size = 0
    kind = CARD_KIND_CARD if info.removable else CARD_KIND_DRIVE
    return MountedCard(
        card_id=derive_card_id(info.volume_uuid, info.source, mount_path, info.label),
        label=info.label or mount_path.name,
        kind=kind,
        size_bytes=int(size),
        status="available" if mount_path.exists() else "missing",
        mount_path=mount_path,
    )


def cards_signature(cards: list[MountedCard]) -> tuple[tuple[str, str, str, int, str], ...]:
    """Return the outbound fields used to detect card snapshot changes."""

    return tuple(
        sorted(
            (
                card.card_id,
                card.label,
                card.kind,
                card.size_bytes,
                card.status,
            )
            for card in cards
        )
    )


class PollingMountWatcher:
    """Small polling watcher used on every platform, including macOS fallback."""

    def __init__(
        self,
        source: CardSource = current_cards,
        *,
        interval_seconds: float = 2.0,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self.source = source
        self.interval_seconds = interval_seconds

    def run(
        self,
        callback: Callable[[list[MountedCard]], None],
        *,
        stop: threading.Event,
        emit_initial: bool = True,
    ) -> None:
        """Poll mounts and call ``callback(cards)`` whenever the snapshot changes."""

        last: tuple[tuple[str, str, str, int, str], ...] | None = None
        while not stop.is_set():
            cards = self.source()
            signature = cards_signature(cards)
            if last is None:
                if emit_initial:
                    callback(cards)
                last = signature
            elif signature != last:
                callback(cards)
                last = signature
            if stop.wait(self.interval_seconds):
                return


class FSEventsMountWatcher:
    """macOS mount watcher backed by the optional Python ``fsevents`` binding.

    The package is intentionally optional: production macOS installs can provide
    the native watcher, while tests and non-macOS deployments keep the dependency
    surface small and use ``PollingMountWatcher``.
    """

    def __init__(
        self,
        source: CardSource = current_cards,
        *,
        fallback_interval_seconds: float = 2.0,
    ) -> None:
        self.source = source
        self.fallback_interval_seconds = fallback_interval_seconds

    def run(
        self,
        callback: Callable[[list[MountedCard]], None],
        *,
        stop: threading.Event,
        emit_initial: bool = True,
    ) -> None:
        """Watch ``/Volumes`` with FSEvents when available, otherwise poll."""

        try:
            fsevents = importlib.import_module("fsevents")
            observer_cls = fsevents.Observer
            stream_cls = fsevents.Stream
        except (ImportError, AttributeError):
            PollingMountWatcher(
                self.source,
                interval_seconds=self.fallback_interval_seconds,
            ).run(callback, stop=stop, emit_initial=emit_initial)
            return

        last: tuple[tuple[str, str, str, int, str], ...] | None = None

        def emit_if_changed() -> None:
            nonlocal last
            cards = self.source()
            signature = cards_signature(cards)
            if last is None or signature != last:
                callback(cards)
                last = signature

        def on_event(_event: object) -> None:
            emit_if_changed()

        if emit_initial:
            emit_if_changed()
        else:
            last = cards_signature(self.source())

        try:
            observer = observer_cls()
            stream = stream_cls(on_event, "/Volumes", file_events=True)
            observer.schedule(stream)
            observer.start()
        except Exception:
            PollingMountWatcher(
                self.source,
                interval_seconds=self.fallback_interval_seconds,
            ).run(callback, stop=stop, emit_initial=False)
            return
        try:
            while not stop.wait(self.fallback_interval_seconds):
                emit_if_changed()
        finally:
            observer.stop()
            observer.join()


def default_mount_watcher(source: CardSource = current_cards) -> PollingMountWatcher | FSEventsMountWatcher:
    """Return the platform-preferred mount watcher."""

    if platform.system() == "Darwin":
        return FSEventsMountWatcher(source)
    return PollingMountWatcher(source)


def _darwin_mounts() -> list[MountInfo]:
    volumes = Path("/Volumes")
    if not volumes.is_dir():
        return []
    mounts: list[MountInfo] = []
    for path in sorted(volumes.iterdir(), key=lambda item: item.name):
        if not path.is_dir() or path.name == "Macintosh HD":
            continue
        info = _darwin_diskutil_info(path)
        mounts.append(
            MountInfo(
                mount_path=path,
                label=path.name,
                source=info.get("DeviceIdentifier"),
                volume_uuid=info.get("VolumeUUID"),
                removable=bool(info.get("RemovableMedia") or info.get("Ejectable")),
            )
        )
    return mounts


def _darwin_diskutil_info(path: Path) -> dict[str, object]:
    try:
        result = subprocess.run(
            ["diskutil", "info", "-plist", str(path)],
            check=True,
            capture_output=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return {}
    try:
        payload = plistlib.loads(result.stdout)
    except (plistlib.InvalidFileException, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _windows_mounts() -> list[MountInfo]:
    """Enumerate Windows removable drives and non-system fixed volumes."""

    system_drive = _normalize_win_drive_id(os.environ.get("SYSTEMDRIVE") or os.environ.get("SYSTEMROOT"))
    mounts: list[MountInfo] = []
    for drive in _win_logical_drive_letters():
        drive_id = _normalize_win_drive_id(drive)
        if drive_id is None:
            continue
        drive_type = _win_drive_type(drive_id)
        if drive_type == DRIVE_REMOVABLE:
            removable = True
        elif drive_type == DRIVE_FIXED and drive_id != system_drive:
            removable = False
        else:
            continue
        volume = _win_volume_info(drive_id)
        if volume is None:
            continue
        label, serial = volume
        mounts.append(
            MountInfo(
                mount_path=Path(f"{drive_id}\\"),
                label=label or drive_id,
                source=drive_id,
                volume_uuid=_win_serial_uuid(serial),
                removable=removable,
            )
        )
    return mounts


def _win_logical_drive_letters() -> list[str]:
    """Return present Windows drive ids such as ``C:`` from ``GetLogicalDrives``."""

    mask = int(_win_kernel32().GetLogicalDrives())
    return [f"{chr(ord('A') + index)}:" for index in range(26) if mask & (1 << index)]


def _win_drive_type(letter: str) -> int:
    """Return the Win32 drive type for one drive id."""

    return int(_win_kernel32().GetDriveTypeW(_win_root(letter)))


def _win_volume_info(letter: str) -> tuple[str, int] | None:
    """Return ``(label, serial)`` for one Windows volume, or ``None`` when unavailable."""

    volume_name = ctypes.create_unicode_buffer(261)
    file_system = ctypes.create_unicode_buffer(261)
    serial = ctypes.c_ulong()
    max_component = ctypes.c_ulong()
    flags = ctypes.c_ulong()
    ok = _win_kernel32().GetVolumeInformationW(
        _win_root(letter),
        volume_name,
        len(volume_name),
        ctypes.byref(serial),
        ctypes.byref(max_component),
        ctypes.byref(flags),
        file_system,
        len(file_system),
    )
    if not ok:
        return None
    return volume_name.value, int(serial.value)


def _win_kernel32() -> object:
    try:
        return ctypes.windll.kernel32
    except AttributeError as exc:
        raise RuntimeError("Windows mount enumeration requires ctypes.windll.kernel32") from exc


def _normalize_win_drive_id(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip().replace("/", "\\")
    if len(text) >= 2 and text[1] == ":":
        return f"{text[0].upper()}:"
    if text.startswith("\\") and len(text) >= 4 and text[2] == ":":
        return f"{text[1].upper()}:"
    return None


def _win_root(letter: str) -> str:
    drive_id = _normalize_win_drive_id(letter)
    if drive_id is None:
        raise ValueError(f"invalid Windows drive id: {letter!r}")
    return f"{drive_id}\\"


def _win_serial_uuid(serial: int) -> str:
    value = int(serial) & 0xFFFFFFFF
    return f"{(value >> 16) & 0xFFFF:04X}-{value & 0xFFFF:04X}"


def _posix_mounts() -> list[MountInfo]:
    mount_file = Path("/proc/mounts")
    if not mount_file.is_file():
        return []
    mounts: list[MountInfo] = []
    for line in mount_file.read_text(encoding="utf-8", errors="replace").splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        source, mountpoint = parts[0], Path(parts[1].replace("\\040", " "))
        if not _is_likely_operator_mount(mountpoint):
            continue
        mounts.append(
            MountInfo(
                mount_path=mountpoint,
                label=mountpoint.name,
                source=source,
                volume_uuid=_blkid_uuid(source),
                removable=_is_removable_mountpoint(mountpoint),
            )
        )
    return mounts


def _is_likely_operator_mount(path: Path) -> bool:
    parts = path.parts
    return (
        len(parts) >= 2
        and parts[0] == "/"
        and (
            parts[1] in {"media", "mnt", "Volumes"}
            or (len(parts) >= 3 and parts[1] == "run" and parts[2] == "media")
        )
    )


def _is_removable_mountpoint(path: Path) -> bool:
    parts = path.parts
    return (len(parts) >= 2 and parts[1] == "media") or (
        len(parts) >= 3 and parts[1] == "run" and parts[2] == "media"
    )


def _blkid_uuid(source: str) -> str | None:
    if not source.startswith("/dev/"):
        return None
    try:
        result = subprocess.run(
            ["blkid", "-s", "UUID", "-o", "value", source],
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    uuid = result.stdout.strip()
    return uuid or None
