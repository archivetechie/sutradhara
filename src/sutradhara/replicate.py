"""Harness-facing fan-out wrapper for Sutradhara replication."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session

from sutradhara.catalog.models import Copy
from sutradhara.keys import KeyRegistry
from sutradhara.replication import (
    BackendMap,
    ReplicationStatus,
    WritableBackendMap,
    replicate_asset,
    replication_status,
)
from sutradhara.replication import (
    self_heal as replication_self_heal,
)
from sutradhara.sealing.rem_object import RemObjectCliOpener, RemObjectCliSealer


def fan_out(
    session: Session,
    asset_hash: bytes,
    source_path: Path | str,
    content_type: str,
    *,
    backends: WritableBackendMap,
) -> list[Copy]:
    """Replicate one asset through catalog pool membership policy."""
    if content_type not in {"o-archive", "n-archive"}:
        return replicate_asset(
            session,
            asset_hash,
            source_path,
            content_type,
            backends=backends,
        )

    registry = KeyRegistry()
    epoch = registry.create_epoch()
    return replicate_asset(
        session,
        asset_hash,
        source_path,
        content_type,
        backends=backends,
        sealer=RemObjectCliSealer(registry),
        key_epoch=epoch.key_id,
    )


def status(
    session: Session,
    asset_hash: bytes,
    content_type: str,
    *,
    backends: BackendMap,
) -> ReplicationStatus:
    """Report replication status through catalog pool membership policy."""
    return replication_status(session, asset_hash, content_type, backends)


def self_heal(
    session: Session,
    asset_hash: bytes,
    content_type: str,
    *,
    backends: WritableBackendMap,
    repair_id: str,
) -> list[Copy]:
    """Rebuild copies under one caller-owned repair execution identity."""
    if not repair_id:
        raise ValueError("repair_id must be non-empty")
    if content_type not in {"o-archive", "n-archive"}:
        return replication_self_heal(
            session,
            asset_hash,
            content_type,
            backends=backends,
            execution_id=repair_id,
        )

    registry = KeyRegistry()
    epoch = registry.create_epoch()
    return replication_self_heal(
        session,
        asset_hash,
        content_type,
        backends=backends,
        opener=RemObjectCliOpener(registry),
        sealer=RemObjectCliSealer(registry),
        key_epoch=epoch.key_id,
        execution_id=repair_id,
    )
