"""Shared path validation for device-relayed browse and receive requests.

The operator console sends helper paths as forward-slash relative strings. This
module performs the server-side syntactic check before a request is relayed to a
device; the helper remains authoritative for filesystem confinement.
"""

from __future__ import annotations

from sutradhara_receive import (
    ReceiveError,
    canonical_device_rel_path as _receive_canonical_device_rel_path,
)


class DevicePathError(ValueError):
    """Raised when a device-relative path is syntactically unsafe."""


def canonical_device_rel_path(value: str | None) -> str:
    """Return the canonical device-relative path, with ``None``/``""`` as root."""

    try:
        return _receive_canonical_device_rel_path(value)
    except ReceiveError as exc:
        raise DevicePathError(str(exc)) from exc
