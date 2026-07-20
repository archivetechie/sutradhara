"""Canonical factual component strings used by job handlers.

The component vocabulary is intentionally open. These helpers only centralize
the spellings shared by checksum, transcode, tape, and destination handlers.
"""

from __future__ import annotations

import socket
from pathlib import Path

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


def touch_copy_tape(ctx: JobContext, copy: Copy) -> None:
    """Record the materialized tape identity under its configured VTL library."""

    if copy.media_family not in {"tape", "d2tape"}:
        return
    config = copy.backend.config or {}
    library = config.get("library_uuid") or config.get("library") or copy.backend.name
    parent = library.hex() if isinstance(library, bytes) else str(library)
    ctx.touch(f"tape:{copy.media_id}", parent=f"library:{parent}")


def touch_destination(ctx: JobContext, destination: Path | str) -> str:
    """Record a local restore destination with the executing host identity."""

    component = f"dest:{socket.gethostname()}:{Path(destination)}"
    ctx.touch(component)
    return component
