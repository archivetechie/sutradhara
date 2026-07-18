"""Canonical factual component strings used by job handlers.

The component vocabulary is intentionally open. These helpers only centralize
the spellings shared by checksum, transcode, tape, and destination handlers.
"""

from __future__ import annotations

import socket
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from sutradhara.catalog.models import Copy
from sutradhara.jobs.registry import JobContext


def touch_asset(ctx: JobContext, digest: bytes | str) -> str:
    """Record one SHA-256 logical-asset identity and return its component."""

    text = digest.hex() if isinstance(digest, bytes) else digest
    suffix = text if text.startswith("sha256:") else f"sha256:{text}"
    component = f"asset:{suffix}"
    ctx.touch(component)
    return component


def touch_tool(ctx: JobContext, name: str, version: str) -> str:
    """Record the exact tool/version pair used by a handler."""

    component = f"tool:{name}@{version}"
    ctx.touch(component)
    return component


def touch_tape_locator(
    ctx: JobContext,
    locator: Mapping[str, Any],
    *,
    library: bytes | str | None = None,
) -> None:
    """Record tape and optional drive identities already present in a locator."""

    tape = _first_identity(locator, "media_id", "tape_barcode", "barcode", "tape_uuid")
    if tape is not None:
        ctx.touch(f"tape:{tape}")
    drive = _first_identity(locator, "drive", "drive_id", "drive_element_address")
    if drive is None:
        return
    if library is None:
        ctx.touch(f"drive:{drive}")
    else:
        parent = library.hex() if isinstance(library, bytes) else library
        ctx.touch(f"drive:{drive}", parent=f"library:{parent}")


def touch_copy_tape(ctx: JobContext, copy: Copy) -> None:
    """Record tape identities from a catalog copy and its backend configuration."""

    config = copy.backend.config or {}
    library = config.get("library_uuid")
    touch_tape_locator(
        ctx,
        copy.native_locator,
        library=library if isinstance(library, (bytes, str)) else None,
    )


def touch_destination(ctx: JobContext, destination: Path | str) -> str:
    """Record a local restore destination with the executing host identity."""

    component = f"dest:{socket.gethostname()}:{Path(destination)}"
    ctx.touch(component)
    return component


def _first_identity(locator: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = locator.get(key)
        if isinstance(value, bytes):
            return value.hex()
        if isinstance(value, str) and value:
            return value
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value)
    return None
