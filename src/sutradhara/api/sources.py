"""Opaque source and landing catalogs for the operator receive API."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_SOURCE_ROOT = Path("/replica/sources")
DEFAULT_LANDING_ROOT = Path("/replica/landing")


class CatalogError(ValueError):
    """A browser-supplied catalog id is unknown or violates confinement."""


@dataclass(frozen=True)
class SourceEntry:
    """One allowlisted receive source exposed to the UI by opaque id."""

    source_id: str
    label: str
    kind: str
    path: Path


@dataclass(frozen=True)
class LandingEntry:
    """One allowlisted landing root exposed to the UI by opaque id."""

    landing_id: str
    label: str
    path: Path


def source_root() -> Path:
    return Path(os.environ.get("SUTRA_RECEIVE_SOURCE_ROOT", str(DEFAULT_SOURCE_ROOT)))


def landing_root() -> Path:
    return Path(os.environ.get("SUTRA_RECEIVE_LANDING_ROOT", str(DEFAULT_LANDING_ROOT)))


def list_sources(root: Path | None = None) -> list[SourceEntry]:
    """Return directory children below the source root as opaque source ids."""

    base = _canonical_root(root or source_root())
    if not base.is_dir():
        return []
    entries: list[SourceEntry] = []
    for child in sorted(base.iterdir(), key=lambda item: item.name):
        if not child.is_dir() or child.name.startswith("."):
            continue
        try:
            path = _confined_child(base, child.name)
        except CatalogError:
            continue
        entries.append(
            SourceEntry(
                source_id=child.name,
                label=_label(child.name),
                kind=_source_kind(child.name),
                path=path,
            )
        )
    return entries


def list_landings(root: Path | None = None) -> list[LandingEntry]:
    """Return configured landing directories as opaque landing ids."""

    base = _canonical_root(root or landing_root())
    if not base.is_dir():
        return []
    child_dirs = [
        child
        for child in sorted(base.iterdir(), key=lambda item: item.name)
        if child.is_dir() and not child.name.startswith(".")
    ]
    if not child_dirs:
        return [LandingEntry(landing_id="default", label="Default landing", path=base)]

    entries: list[LandingEntry] = []
    for child in child_dirs:
        try:
            path = _confined_child(base, child.name)
        except CatalogError:
            continue
        entries.append(LandingEntry(landing_id=child.name, label=_label(child.name), path=path))
    return entries


def resolve_source(source_id: str, root: Path | None = None) -> SourceEntry:
    """Resolve one opaque source id to a confined path below the source root."""

    base = _canonical_root(root or source_root())
    path = _confined_child(base, source_id)
    if not path.is_dir():
        raise CatalogError(f"unknown sourceId {source_id!r}")
    return SourceEntry(
        source_id=source_id,
        label=_label(source_id),
        kind=_source_kind(source_id),
        path=path,
    )


def resolve_landing(landing_id: str, root: Path | None = None) -> LandingEntry:
    """Resolve one opaque landing id to a confined landing directory."""

    base = _canonical_root(root or landing_root())
    if landing_id == "default":
        if _has_landing_children(base):
            raise CatalogError("landingId 'default' is not valid when explicit landings exist")
        return LandingEntry(landing_id="default", label="Default landing", path=base)
    path = _confined_child(base, landing_id)
    if not path.is_dir():
        raise CatalogError(f"unknown landingId {landing_id!r}")
    return LandingEntry(landing_id=landing_id, label=_label(landing_id), path=path)


def _canonical_root(root: Path) -> Path:
    return root.expanduser().resolve()


def _confined_child(root: Path, opaque_id: str) -> Path:
    _validate_opaque_id(opaque_id)
    candidate = (root / opaque_id).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise CatalogError(f"id {opaque_id!r} escapes catalog root") from exc
    return candidate


def _validate_opaque_id(opaque_id: str) -> None:
    if not opaque_id or opaque_id in {".", ".."}:
        raise CatalogError("catalog id must be non-empty and opaque")
    if opaque_id.startswith("."):
        raise CatalogError("catalog id must not be a dotfile")
    if "/" in opaque_id or "\\" in opaque_id:
        raise CatalogError("catalog id must not contain path separators")
    if Path(opaque_id).is_absolute():
        raise CatalogError("catalog id must not be a path")


def _has_landing_children(root: Path) -> bool:
    return any(child.is_dir() and not child.name.startswith(".") for child in root.iterdir())


def _label(opaque_id: str) -> str:
    return opaque_id.replace("-", " ").replace("_", " ").title()


def _source_kind(opaque_id: str) -> str:
    lowered = opaque_id.lower()
    if "upload" in lowered:
        return "upload"
    if "card" in lowered:
        return "card"
    if "drive" in lowered or "disk" in lowered:
        return "drive"
    return "handoff"
