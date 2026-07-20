"""Canonical materialized media identity for catalog copy registration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


class CopyMediaIdentityError(ValueError):
    """A native locator cannot supply the identity required by its family."""


@dataclass(frozen=True)
class CopyMediaIdentity:
    """The canonical media identifier and its durability family."""

    media_id: str
    media_family: str


def _copy_media_id(
    implementation_family: str,
    native_locator: Mapping[str, Any],
    backend_id: int,
) -> CopyMediaIdentity:
    """Derive the only allowed values for ``copy.media_id`` and family.

    This pure function is called by copy registration and by the Wave 1 data
    backfill. Readers consume the materialized columns and never call it.
    """

    if implementation_family == "tape":
        native_id = native_locator.get("tape_uuid")
        required = "tape_uuid"
    elif implementation_family == "d2tape":
        native_id = native_locator.get("volume_uuid") or native_locator.get("barcode")
        required = "volume_uuid/barcode"
    elif implementation_family in {"disk", "cloud", "memory"}:
        native_id = f"backend:{backend_id}"
        required = "backend id"
    else:
        raise CopyMediaIdentityError(
            f"unsupported implementation_family={implementation_family!r}"
        )
    if not isinstance(native_id, str) or not native_id:
        raise CopyMediaIdentityError(
            f"{implementation_family} locator is missing {required}"
        )
    return CopyMediaIdentity(
        media_id=f"{implementation_family}:{native_id}",
        media_family=implementation_family,
    )
