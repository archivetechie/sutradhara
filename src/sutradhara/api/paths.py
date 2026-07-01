"""Shared path validation for device-relayed browse and receive requests.

The operator console sends helper paths as forward-slash relative strings. This
module performs the server-side syntactic check before a request is relayed to a
device; the helper remains authoritative for filesystem confinement.
"""

from __future__ import annotations

import posixpath
import re

MAX_DEVICE_REL_PATH = 1024
_DRIVE_PREFIX = re.compile(r"^[A-Za-z]:")


class DevicePathError(ValueError):
    """Raised when a device-relative path is syntactically unsafe."""


def canonical_device_rel_path(value: str | None) -> str:
    """Return the canonical device-relative path, with ``None``/``""`` as root."""

    if value is None or value == "":
        return ""
    if len(value) > MAX_DEVICE_REL_PATH:
        raise DevicePathError("path is too long")
    if "\\" in value:
        raise DevicePathError("path must use forward slashes")
    if value.startswith("/"):
        raise DevicePathError("path must be relative")
    if _DRIVE_PREFIX.match(value):
        raise DevicePathError("path must not use a drive prefix")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise DevicePathError("path must be normalized and relative")
    canonical = posixpath.normpath(value)
    if canonical != value:
        raise DevicePathError("path must be normalized")
    return canonical
