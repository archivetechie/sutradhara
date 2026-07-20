"""Domain fact-recording API for catalog-producing job handlers.

The caller owns the transaction: these helpers may flush to allocate IDs and
surface uniqueness violations, but they never commit or roll back. Each helper
is idempotent on its natural fact key, so recording the same fact again updates
the existing occurrence when appropriate and never creates a duplicate row.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.copies import add_bundle_copy, add_copy
from sutradhara.catalog.models import AssetDerivation, IngestItem, LogicalAsset
from sutradhara.catalog.types import AssetValidity, MediaKind
from sutradhara.rem_archive_cli import sha256_file

record_copy = add_copy
record_bundle_copy = add_bundle_copy


def record_derivation(
    session: Session,
    *,
    source_item: IngestItem,
    output_path: Path,
    relpath: str,
    kind: str,
    artifactclass: str,
    media_kind: MediaKind,
    generated_by: str,
) -> IngestItem:
    """Record a derived ingest occurrence and its provenance edge."""

    try:
        stat = output_path.stat()
    except OSError as exc:
        raise ValueError(f"derived output missing or inaccessible: {output_path}") from exc
    if not output_path.is_file() or stat.st_size <= 0:
        raise ValueError(f"derived output missing or empty: {output_path}")

    digest = sha256_file(output_path)
    asset = session.get(LogicalAsset, digest)
    if asset is None:
        asset = LogicalAsset(
            content_sha256=digest,
            size_bytes=stat.st_size,
            media_kind=media_kind,
            media_info={"derived_from_item_id": source_item.id, "kind": kind},
            validity=AssetValidity.UNVALIDATED,
        )
        session.add(asset)

    item = session.scalars(
        select(IngestItem).where(
            IngestItem.intake_id == source_item.intake_id,
            IngestItem.as_received_path == relpath,
        )
    ).one_or_none()
    metadata = {
        "generated_by": generated_by,
        "source_item_id": source_item.id,
        "kind": kind,
    }
    if item is None:
        item = IngestItem(
            intake_id=source_item.intake_id,
            logical_asset_hash=digest,
            as_received_path=relpath,
            virtual_path=relpath,
            st_dev=getattr(stat, "st_dev", None),
            st_ino=getattr(stat, "st_ino", None),
            size_bytes=stat.st_size,
            artifactclass=artifactclass,
            source_path=str(output_path),
            item_metadata=metadata,
        )
        session.add(item)
        session.flush()
    else:
        item.logical_asset_hash = digest
        item.st_dev = getattr(stat, "st_dev", None)
        item.st_ino = getattr(stat, "st_ino", None)
        item.size_bytes = stat.st_size
        item.artifactclass = artifactclass
        item.source_path = str(output_path)
        item.item_metadata = {**(item.item_metadata or {}), **metadata}

    edge = session.scalars(
        select(AssetDerivation).where(
            AssetDerivation.derived_item_id == item.id,
            AssetDerivation.source_item_id == source_item.id,
            AssetDerivation.kind == kind,
        )
    ).one_or_none()
    if edge is None:
        session.add(
            AssetDerivation(
                derived_item_id=item.id,
                source_item_id=source_item.id,
                kind=kind,
            )
        )
    session.flush()
    return item


def record_index(
    session: Session,
    *,
    item: IngestItem,
    index_kind: str,
    sidecar_path: Path,
) -> None:
    """Record a sidecar index pointer for an ingest item without creating a Copy.

    ``index_kind`` is part of the domain fact contract. The sidecar path is a
    promoted typed fact and is never duplicated into occurrence metadata.
    """

    if not index_kind:
        raise ValueError("index_kind must be non-empty")
    item.pfr_sidecar_path = str(sidecar_path)


def record_validity(
    session: Session,
    *,
    asset: LogicalAsset,
    validity: AssetValidity,
    note: str | None = None,
) -> None:
    """Record the latest decode/parse validity fact for an asset.

    Passing ``note=None`` intentionally clears any previous validity note.
    """

    asset.validity = validity
    asset.validity_note = note


__all__ = [
    "record_bundle_copy",
    "record_copy",
    "record_derivation",
    "record_index",
    "record_validity",
]
