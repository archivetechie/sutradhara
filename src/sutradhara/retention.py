"""Retention gate for deleting temporary intake bytes after durable archive proof.

This module is the only Sutradhara path that deletes cloud-temp or landing
bytes. It keeps the gate pure and read-only, then performs deletion in
external-delete-before-DB order so retries are idempotent when a process fails
between the object deletion and the caller's transaction commit.
"""

from __future__ import annotations

import datetime as dt
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.backend import factory
from sutradhara.backend.port import BackendLocator, ByteRange, StorageBackend, VerifyResult
from sutradhara.catalog.models import (
    Arrangement,
    ArtifactClassPool,
    Backend,
    Copy,
    IngestItem,
    Intake,
    OffsiteConfirmation,
    Pool,
    RetentionEvent,
    Submission,
)
from sutradhara.catalog.types import (
    ArrangementStatus,
    IntakeStatus,
    RetentionState,
    SubmissionStatus,
)
from sutradhara.durability import AssetTarget, durable_placements
from sutradhara.intake import media_kind_for_path
from sutradhara.jobs.models import ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import CONDITION_BLOCKED, CONDITION_SATISFIED
from sutradhara.jobs.reconcilers.derivation import DOMAIN as DERIVATION_DOMAIN
from sutradhara.jobs.reconcilers.derivation import make_target_key
from sutradhara.jobs.reconcilers.profiles import entries_for
from sutradhara.replication import PoolTarget, _copy_media_id, target_pools
from sutradhara.restore import _fsync_directory

DEFAULT_STAGING_GRACE_DAYS = 30
CLOUD_BLOB_PREFIX = "cloud-blob:"
TERMINAL_DERIVATION_CONDITIONS = {CONDITION_SATISFIED, CONDITION_BLOCKED}


class RetentionError(RuntimeError):
    """Base class for retention gate and deletion failures."""


@dataclass(frozen=True)
class PoolGateStatus:
    """Durability decision for one asset and one recipe pool."""

    pool_id: str
    offsite_gate: bool
    satisfied: bool
    copy_id: int | None
    media_id: str | None
    reason: str | None = None


@dataclass(frozen=True)
class AssetGateStatus:
    """Durability decision for one ingest item."""

    ingest_item_id: int
    logical_asset_hash: str
    artifactclass: str
    releasable: bool
    pools: tuple[PoolGateStatus, ...]


@dataclass(frozen=True)
class IntakeGateStatus:
    """Full retention gate decision for one intake."""

    intake_id: str
    retention_state: str
    releasable: bool
    holds: tuple[str, ...]
    assets: tuple[AssetGateStatus, ...]
    released_at: dt.datetime | None
    staging_deleted_at: dt.datetime | None
    grace_deadline: dt.datetime | None = None


@dataclass(frozen=True)
class RetentionRunResult:
    """Result of one retention-run attempt for an intake."""

    intake_id: str
    released: bool
    deleted_copy_ids: tuple[int, ...]
    reason: str


@dataclass(frozen=True)
class StagingSweepResult:
    """Result of one staging sweep attempt for an intake."""

    intake_id: str
    purged: bool
    deleted_path: str | None
    reason: str


class _PolicyBackend(StorageBackend):
    """Read-only placeholder used to reuse target_pools for policy expansion."""

    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def enumerate(self) -> Any:
        return iter(())

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        raise NotImplementedError

    def verify(self, locator: BackendLocator) -> VerifyResult:
        raise NotImplementedError


def confirm_offsite(
    session: Session,
    *,
    media_id: str,
    confirmed_by: str,
    shipment_id: str | None = None,
    confirmed_at: dt.datetime | None = None,
) -> tuple[OffsiteConfirmation, bool]:
    """Record that one media id has arrived offsite, idempotently."""
    if not media_id:
        raise ValueError("media_id must be non-empty")
    if not confirmed_by:
        raise ValueError("confirmed_by must be non-empty")
    existing = session.get(OffsiteConfirmation, media_id)
    if existing is not None:
        return existing, False
    row = OffsiteConfirmation(
        media_id=media_id,
        confirmed_by=confirmed_by,
        shipment_id=shipment_id,
        confirmed_at=confirmed_at or _utcnow(),
    )
    session.add(row)
    session.flush()
    return row, True


def releasable(session: Session, intake: Intake | str) -> bool:
    """Return True iff the intake passes every retention gate without mutation."""
    return retention_status(session, intake).releasable


def retention_status(
    session: Session,
    intake: Intake | str,
    *,
    grace_days: int = DEFAULT_STAGING_GRACE_DAYS,
) -> IntakeGateStatus:
    """Return the read-only retention gate truth for one intake."""
    row = _get_intake(session, intake)
    items = tuple(_intake_items(session, row))
    eligibility_holds = _intake_eligibility_holds(row, items)
    holds = [
        *eligibility_holds,
        *_arrangement_holds(session, row),
        *_prepared_profile_holds(session, row),
    ]
    assets = (
        tuple(_asset_gate_status(session, item) for item in items) if not eligibility_holds else ()
    )
    assets_releasable = bool(assets) and all(asset.releasable for asset in assets)
    state_allows_release = row.retention_state == RetentionState.HELD
    is_releasable = state_allows_release and not holds and assets_releasable
    deadline = (
        _as_utc(row.released_at) + dt.timedelta(days=grace_days)
        if row.released_at is not None
        else None
    )
    return IntakeGateStatus(
        intake_id=row.intake_id,
        retention_state=str(row.retention_state),
        releasable=is_releasable,
        holds=tuple(holds),
        assets=assets,
        released_at=row.released_at,
        staging_deleted_at=row.staging_deleted_at,
        grace_deadline=deadline,
    )


def run_retention(
    session: Session,
    intake: Intake | str,
    *,
    actor: str,
) -> RetentionRunResult:
    """Release one held intake by deleting cloud-temp bytes after the gate passes."""
    if not actor:
        raise ValueError("actor must be non-empty")
    row = _get_intake(session, intake)
    if row.retention_state != RetentionState.HELD:
        return RetentionRunResult(row.intake_id, False, (), f"state={row.retention_state}")
    if not releasable(session, row):
        return RetentionRunResult(row.intake_id, False, (), "held")

    now = _utcnow()
    cloud_copies = _cloud_blob_copies(session, row.intake_id)
    deleted_ids: list[int] = []
    for copy in cloud_copies:
        _delete_copy_object(copy)
        copy.deleted_at = now
        deleted_ids.append(copy.id)

    row.retention_state = RetentionState.RELEASED
    row.released_at = now
    session.add(
        RetentionEvent(
            intake_id=row.intake_id,
            action="cloud_blob_deleted",
            actor=actor,
            at=now,
            detail={
                "bundle_id": _cloud_bundle_id(row.intake_id),
                "copy_ids": deleted_ids,
                "missing_copy": not deleted_ids,
            },
        )
    )
    session.add(
        RetentionEvent(
            intake_id=row.intake_id,
            action="released",
            actor=actor,
            at=now,
            detail={"copy_ids": deleted_ids},
        )
    )
    session.flush()
    return RetentionRunResult(row.intake_id, True, tuple(deleted_ids), "released")


def _intake_eligibility_holds(intake: Intake, items: tuple[IngestItem, ...]) -> list[str]:
    holds: list[str] = []
    if intake.status != IntakeStatus.REGISTERED:
        holds.append(f"intake-status:{intake.status}")
    if not items:
        holds.append("intake-empty")
    return holds


def sweep_staging(
    session: Session,
    intake: Intake | str,
    *,
    actor: str,
    grace_days: int = DEFAULT_STAGING_GRACE_DAYS,
) -> StagingSweepResult:
    """Delete one released intake's landing bytes after the grace period."""
    if not actor:
        raise ValueError("actor must be non-empty")
    row = _get_intake(session, intake)
    if row.retention_state == RetentionState.PURGED:
        return StagingSweepResult(row.intake_id, False, None, "already-purged")
    if row.retention_state != RetentionState.RELEASED or row.released_at is None:
        return StagingSweepResult(row.intake_id, False, None, f"state={row.retention_state}")

    now = _utcnow()
    deadline = _as_utc(row.released_at) + dt.timedelta(days=grace_days)
    if deadline >= now:
        return StagingSweepResult(row.intake_id, False, None, "grace-active")

    staging_root = _staging_root_for_intake(row)
    deleted_path = None if staging_root is None else str(staging_root)
    if staging_root is not None:
        _delete_path_idempotent(staging_root)

    row.retention_state = RetentionState.PURGED
    row.staging_deleted_at = now
    session.add(
        RetentionEvent(
            intake_id=row.intake_id,
            action="staging_deleted",
            actor=actor,
            at=now,
            detail={"path": deleted_path, "grace_days": grace_days},
        )
    )
    session.flush()
    return StagingSweepResult(row.intake_id, True, deleted_path, "purged")


def _arrangement_holds(session: Session, intake: Intake) -> list[str]:
    holds: list[str] = []
    arrangements = list(
        session.scalars(
            select(Arrangement)
            .where(Arrangement.intake_id == intake.intake_id)
            .order_by(Arrangement.id)
        )
    )
    for arrangement in arrangements:
        if arrangement.status == ArrangementStatus.ABANDONED:
            continue
        if arrangement.status == ArrangementStatus.SUBMITTED:
            if not arrangement.submission_id:
                holds.append(f"arrangement:{arrangement.id}:submitted-missing-submission")
                continue
            submission = session.get(Submission, arrangement.submission_id)
            if submission is not None and submission.status == SubmissionStatus.ARCHIVED:
                continue
            holds.append(f"arrangement:{arrangement.id}:submitted-not-archived")
            continue
        holds.append(f"arrangement:{arrangement.id}:{arrangement.status}")
    return holds


def _prepared_profile_holds(session: Session, intake: Intake) -> list[str]:
    if not intake.requested_profile:
        return []
    holds: list[str] = []
    for item in _intake_items(session, intake):
        media_kind = media_kind_for_path(item.as_received_path)
        for entry in entries_for(item.artifactclass, intake.requested_profile, media_kind):
            target_key = make_target_key(item.id, entry.job_kind)
            condition = session.scalars(
                select(ReconciliationCondition).where(
                    ReconciliationCondition.domain == DERIVATION_DOMAIN,
                    ReconciliationCondition.target_key == target_key,
                )
            ).one_or_none()
            if condition is None:
                holds.append(f"derivation:{target_key}:missing-condition")
            elif condition.condition not in TERMINAL_DERIVATION_CONDITIONS:
                holds.append(f"derivation:{target_key}:{condition.condition}")
    return holds


def _asset_gate_status(session: Session, item: IngestItem) -> AssetGateStatus:
    pool_statuses: list[PoolGateStatus] = []
    for target in _policy_targets(session, item.artifactclass):
        pool_statuses.append(_pool_gate_status(session, item.logical_asset_hash, target))
    return AssetGateStatus(
        ingest_item_id=item.id,
        logical_asset_hash=item.logical_asset_hash.hex(),
        artifactclass=item.artifactclass,
        releasable=all(status.satisfied for status in pool_statuses),
        pools=tuple(pool_statuses),
    )


def _pool_gate_status(
    session: Session,
    asset_hash: bytes,
    target: PoolTarget,
) -> PoolGateStatus:
    candidates = _qualifying_copies_for_pool(
        session,
        asset_hash,
        target.pool_id,
        target.artifactclass,
    )
    if not candidates:
        return PoolGateStatus(
            pool_id=target.pool_id,
            offsite_gate=target.offsite_gate,
            satisfied=False,
            copy_id=None,
            media_id=None,
            reason="no-verified-copy",
        )
    if not target.offsite_gate:
        copy = candidates[0]
        return PoolGateStatus(
            pool_id=target.pool_id,
            offsite_gate=False,
            satisfied=True,
            copy_id=copy.id,
            media_id=_copy_media_id(copy),
        )

    confirmed = _confirmed_media_ids(session)
    missing_media = False
    for copy in candidates:
        media_id = _copy_media_id(copy)
        if not media_id:
            missing_media = True
            continue
        if media_id in confirmed:
            return PoolGateStatus(
                pool_id=target.pool_id,
                offsite_gate=True,
                satisfied=True,
                copy_id=copy.id,
                media_id=media_id,
            )
    return PoolGateStatus(
        pool_id=target.pool_id,
        offsite_gate=True,
        satisfied=False,
        copy_id=candidates[0].id,
        media_id=_copy_media_id(candidates[0]),
        reason="missing-media-id" if missing_media else "offsite-unconfirmed",
    )


def _qualifying_copies_for_pool(
    session: Session,
    asset_hash: bytes,
    pool_id: str,
    artifactclass: str,
) -> list[Copy]:
    return [
        copy
        for copy in durable_placements(
            session,
            AssetTarget(asset_hash, artifactclass),
            require_verified=True,
            artifactclass=artifactclass,
            pool_id=pool_id,
        )
    ]


def _policy_targets(session: Session, artifactclass: str) -> list[PoolTarget]:
    backend_rows: dict[int, StorageBackend] = {}
    for backend_id, backend_name in session.execute(
        select(Backend.id, Backend.name)
        .join(Pool, Pool.backend_id == Backend.id)
        .join(ArtifactClassPool, ArtifactClassPool.pool_id == Pool.id)
        .where(
            ArtifactClassPool.artifactclass == artifactclass,
            ArtifactClassPool.active.is_(True),
        )
    ):
        backend_rows[int(backend_id)] = _PolicyBackend(str(backend_name))
    return [target for _, target in target_pools(session, artifactclass, backend_rows)]


def _cloud_blob_copies(session: Session, intake_id: str) -> list[Copy]:
    return list(
        session.scalars(
            select(Copy)
            .where(
                Copy.bundle_id == _cloud_bundle_id(intake_id),
                Copy.deleted_at.is_(None),
            )
            .order_by(Copy.id)
        )
    )


def _cloud_bundle_id(intake_id: str) -> str:
    return f"{CLOUD_BLOB_PREFIX}{intake_id}"


def _delete_copy_object(copy: Copy) -> None:
    backend = factory.backend_from_row(copy.backend)
    delete_object = getattr(backend, "delete_object", None)
    if not callable(delete_object):
        raise RetentionError(
            f"backend {copy.backend.name!r} for copy id={copy.id} does not support delete_object"
        )
    delete_object(copy.native_locator)


def _confirmed_media_ids(session: Session) -> set[str]:
    return set(session.scalars(select(OffsiteConfirmation.media_id)))


def _intake_items(session: Session, intake: Intake) -> list[IngestItem]:
    return list(
        session.scalars(
            select(IngestItem)
            .where(IngestItem.intake_id == intake.intake_id)
            .order_by(IngestItem.id)
        )
    )


def _get_intake(session: Session, intake: Intake | str) -> Intake:
    if isinstance(intake, Intake):
        return intake
    row = session.get(Intake, intake)
    if row is None:
        raise RetentionError(f"intake {intake!r} does not exist")
    return row


def _staging_root_for_intake(intake: Intake) -> Path | None:
    if intake.manifest_path:
        return Path(intake.manifest_path).resolve().parent
    source_paths = [
        Path(value).resolve()
        for item in intake.items
        if isinstance((value := item.item_metadata.get("source_path")), str) and value
    ]
    if not source_paths:
        return None
    parent_texts = [str(path.parent) for path in source_paths]
    return Path(os.path.commonpath(parent_texts)).resolve()


def _delete_path_idempotent(path: Path) -> None:
    resolved = path.resolve()
    if _unsafe_delete_root(resolved):
        raise RetentionError(f"refusing to delete unsafe staging path: {resolved}")
    parent = resolved.parent
    if resolved.is_dir():
        shutil.rmtree(resolved)
        _fsync_if_exists(parent)
        return
    try:
        resolved.unlink()
    except FileNotFoundError:
        return
    _fsync_if_exists(parent)


def _unsafe_delete_root(path: Path) -> bool:
    return path.parent == path or len(path.parts) < 3


def _fsync_if_exists(path: Path) -> None:
    if path.is_dir():
        _fsync_directory(path)


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _as_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)
