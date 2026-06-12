"""Harness-facing fan-out wrapper for Sutradhara replication.

The scenario harness imports this module as a stable seam. It keeps the default
replication behavior raw-byte compatible for existing content types and applies
the Scenario O representation/key policy only for `o-archive`.
"""

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
from sutradhara.sealing.rao import RaoCliOpener, RaoCliSealer
from sutradhara.sealing.policy import DEFAULT_POLICY, n_archive_policy, o_archive_policy


def fan_out(
    session: Session,
    asset_hash: bytes,
    source_path: Path | str,
    content_type: str,
    *,
    backends: WritableBackendMap,
) -> list[Copy]:
    """Replicate one asset through the harness-facing representation policy seam."""
    if content_type not in {"o-archive", "n-archive"}:
        return replicate_asset(
            session,
            asset_hash,
            source_path,
            content_type,
            backends=backends,
            policy=DEFAULT_POLICY,
        )

    registry = KeyRegistry()
    epoch = registry.create_epoch()
    policy = n_archive_policy() if content_type == "n-archive" else o_archive_policy()
    return replicate_asset(
        session,
        asset_hash,
        source_path,
        content_type,
        backends=backends,
        sealer=RaoCliSealer(registry),
        policy=policy,
        key_epoch=epoch.key_id,
    )


def status(
    session: Session,
    asset_hash: bytes,
    content_type: str,
    *,
    backends: BackendMap,
) -> ReplicationStatus:
    """Report replication status through the harness-facing policy seam."""
    if content_type == "o-archive":
        policy = o_archive_policy()
    elif content_type == "n-archive":
        policy = n_archive_policy()
    else:
        policy = DEFAULT_POLICY
    return replication_status(
        session,
        asset_hash,
        content_type,
        backends,
        policy=policy,
    )


def self_heal(
    session: Session,
    asset_hash: bytes,
    content_type: str,
    *,
    backends: WritableBackendMap,
) -> list[Copy]:
    """Rebuild missing copies through the harness-facing self-heal seam."""
    if content_type not in {"o-archive", "n-archive"}:
        return replication_self_heal(
            session,
            asset_hash,
            content_type,
            backends=backends,
            policy=DEFAULT_POLICY,
        )

    registry = KeyRegistry()
    epoch = registry.create_epoch()
    policy = n_archive_policy() if content_type == "n-archive" else o_archive_policy()
    return replication_self_heal(
        session,
        asset_hash,
        content_type,
        backends=backends,
        opener=RaoCliOpener(registry),
        sealer=RaoCliSealer(registry),
        policy=policy,
        key_epoch=epoch.key_id,
    )
