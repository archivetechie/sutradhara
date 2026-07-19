"""Authoritative retention gates and the only temporary-byte deletion funnel.

Receipts are audit evidence only.  Every decision below is re-derived from
authoritative catalog columns; remote witnesses are collected before SQLite
write reservations and are reused only for the immediately following act pass.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import shutil
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.backend import factory
from sutradhara.backend.port import (
    BackendLocator,
    ByteRange,
    RetentionWitnessBackend,
    StorageBackend,
    VerifyResult,
    WitnessResult,
)
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
from sutradhara.replication import (
    PoolTarget,
    ReplicationError,
    ReplicationPolicyMissing,
    _copy_media_id,
    target_pools,
)
from sutradhara.restore import _fsync_directory

DEFAULT_STAGING_GRACE_DAYS = 30
DEFAULT_BATCH_LIMIT = 25
RECENT_FLIP_WINDOW = dt.timedelta(hours=24)
WITNESS_MAX_AGE = dt.timedelta(seconds=5)
CLOUD_BLOB_PREFIX = "cloud-blob:"
POLICY_FINGERPRINT_VERSION = "v1"
TOMBSTONE_BASENAME_VERSION = "v1"
TERMINAL_DERIVATION_CONDITIONS = {CONDITION_SATISFIED, CONDITION_BLOCKED}

_EVENT_DETAIL_KEYS: dict[str, frozenset[str]] = {
    "released": frozenset({"copy_ids"}),
    "cloud_blob_deleted": frozenset({"bundle_id", "copy_ids", "outcome", "copy_outcomes"}),
    "staging_deleted": frozenset({"path"}),
    "release_attempted": frozenset({"policy_fingerprint"}),
    "purge_attempted": frozenset({"path", "grace_days"}),
    "staging_tombstoned": frozenset({"source_path", "tombstone_path"}),
    "staging_purge_held": frozenset({"reasons"}),
    "batch_invoked": frozenset({"action", "limit", "candidate_count", "dry_run", "refused"}),
    "batch_refused": frozenset(
        {"action", "limit", "candidate_count", "dry_run", "refused", "reason"}
    ),
    "grace_overridden": frozenset({"grace_days", "default_grace_days"}),
    "abandoned": frozenset({"reason", "previous_state"}),
    "correction_recorded": frozenset({"kind", "reason"}),
    "offsite_confirmed": frozenset({"shipment_id", "confirmed_by"}),
}

PurgeStatusValue = Literal[
    "not-applicable",
    "grace-active",
    "eligible",
    "tombstone-gc-pending",
    "abandoned",
]


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
class PurgeStatus:
    """API/CLI projection of the stage-2 disposition."""

    status: str
    assessed_at: dt.datetime


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
    grace_deadline: dt.datetime | None
    purge_status: PurgeStatus


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


@dataclass(frozen=True)
class BatchCandidate:
    """Read-only tripwire evidence for one intake."""

    intake_id: str
    release_condition: str
    evidence: tuple[str, ...]
    recent_flip: bool


@dataclass(frozen=True)
class BatchHold:
    """Authoritative reasons one pre-pass intake cannot act."""

    intake_id: str
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class RetentionBatchResult:
    """One logged batch invocation and its action results."""

    operation_id: str
    action: str
    limit: int
    candidate_count: int
    refused: bool
    dry_run: bool
    all_held: bool
    candidates: tuple[BatchCandidate, ...]
    holds: tuple[BatchHold, ...]
    results: tuple[RetentionRunResult | StagingSweepResult, ...]
    reason: str

    @property
    def exit_code(self) -> int:
        return 2 if self.refused or self.all_held else 0


@dataclass(frozen=True)
class _WitnessEvidence:
    eligible: bool
    result: WitnessResult | None
    observed_at: dt.datetime


@dataclass(frozen=True)
class _WitnessRequest:
    """Detached witness input prepared without making a remote call."""

    copy_id: int
    eligible: bool
    adapter: RetentionWitnessBackend | None
    locator: BackendLocator
    expected_hash: bytes
    error: str | None = None


WitnessAnswers = dict[int, _WitnessEvidence]


@dataclass(frozen=True)
class _GateAssessment:
    status: IntakeGateStatus
    witnesses: WitnessAnswers


class _PolicyBackend(StorageBackend):
    """Read-only placeholder used to reuse ``target_pools`` policy expansion."""

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
    """Confirm known media offsite and append the matching media receipt."""
    if not media_id:
        raise ValueError("media_id must be non-empty")
    if not confirmed_by:
        raise ValueError("confirmed_by must be non-empty")
    if not _known_media_id(session, media_id):
        raise RetentionError(f"media id {media_id!r} matches no known copy")
    existing = session.get(OffsiteConfirmation, media_id)
    if existing is not None and existing.revoked_at is None:
        return existing, False
    now = confirmed_at or _utcnow()
    if existing is None:
        row = OffsiteConfirmation(
            media_id=media_id,
            confirmed_by=confirmed_by,
            shipment_id=shipment_id,
            confirmed_at=now,
        )
        session.add(row)
    else:
        row = existing
        row.confirmed_by = confirmed_by
        row.shipment_id = shipment_id
        row.confirmed_at = now
        row.revoked_at = None
        row.revoked_by = None
    _add_event(
        session,
        subject_type="media",
        subject_id=media_id,
        action="offsite_confirmed",
        operation_id=f"offsite-confirm:{media_id}:{uuid.uuid4()}",
        actor=confirmed_by,
        at=now,
        detail={"shipment_id": shipment_id, "confirmed_by": confirmed_by},
    )
    session.flush()
    return row, True


def revoke_offsite(
    session: Session,
    *,
    media_id: str,
    actor: str,
    reason: str,
) -> tuple[OffsiteConfirmation, bool]:
    """Attribute and persist an offsite-confirmation correction."""
    if not actor or not reason:
        raise ValueError("actor and reason must be non-empty")
    row = session.get(OffsiteConfirmation, media_id)
    if row is None:
        raise RetentionError(f"offsite confirmation {media_id!r} does not exist")
    if row.revoked_at is not None:
        return row, False
    confirmation_receipt = session.scalars(
        select(RetentionEvent)
        .where(
            RetentionEvent.subject_type == "media",
            RetentionEvent.subject_id == media_id,
            RetentionEvent.action == "offsite_confirmed",
        )
        .order_by(RetentionEvent.event_id.desc())
        .limit(1)
    ).first()
    if confirmation_receipt is None:
        raise RetentionError(f"offsite confirmation {media_id!r} has no receipt to supersede")
    now = _utcnow()
    row.revoked_at = now
    row.revoked_by = actor
    _add_event(
        session,
        subject_type="media",
        subject_id=media_id,
        action="correction_recorded",
        operation_id=f"offsite-revoke:{media_id}:{uuid.uuid4()}",
        actor=actor,
        at=now,
        detail={"kind": "offsite-revocation", "reason": reason},
        supersedes_source="retention_event",
        supersedes_event_id=confirmation_receipt.event_id,
    )
    session.flush()
    return row, True


def abandon_retention(
    session: Session,
    intake: Intake | str,
    *,
    actor: str,
    reason: str,
) -> bool:
    """Move HELD/RELEASED to terminal ABANDONED without deleting bytes."""
    if not actor or not reason:
        raise ValueError("actor and reason must be non-empty")
    row = _get_intake(session, intake)
    if row.retention_state == RetentionState.ABANDONED:
        return False
    if row.retention_state not in (RetentionState.HELD, RetentionState.RELEASED):
        raise RetentionError(f"cannot abandon intake in state={row.retention_state}")
    previous_state = str(row.retention_state)
    now = _utcnow()
    row.retention_state = RetentionState.ABANDONED
    _add_event(
        session,
        intake_id=row.intake_id,
        subject_type="intake",
        subject_id=row.intake_id,
        action="abandoned",
        operation_id=f"abandon:{row.intake_id}:{uuid.uuid4()}",
        actor=actor,
        at=now,
        detail={"reason": reason, "previous_state": previous_state},
    )
    session.flush()
    return True


def releasable(session: Session, intake: Intake | str) -> bool:
    """Return True iff the intake passes every stage-1 gate without mutation."""
    return retention_status(session, intake).releasable


def retention_status(
    session: Session,
    intake: Intake | str,
    *,
    grace_days: int = DEFAULT_STAGING_GRACE_DAYS,
) -> IntakeGateStatus:
    """Report gate truth; witness failures become hold reasons, never errors."""
    return _assess_intake(session, intake, grace_days=grace_days).status


def purge_status(
    session: Session,
    intake: Intake | str,
    *,
    grace_days: int = DEFAULT_STAGING_GRACE_DAYS,
) -> PurgeStatus:
    """Return the reasoned stage-2 projection without unnecessary witnesses."""
    row = _get_intake(session, intake)
    now = _utcnow()
    if row.retention_state == RetentionState.TOMBSTONED:
        return PurgeStatus("tombstone-gc-pending", now)
    if row.retention_state == RetentionState.ABANDONED:
        return PurgeStatus("abandoned", now)
    if row.retention_state != RetentionState.RELEASED:
        return PurgeStatus("not-applicable", now)
    return _assess_intake(session, row, grace_days=grace_days).status.purge_status


def run_retention(
    session: Session,
    intake: Intake | str,
    *,
    actor: str,
    _witness_answers: WitnessAnswers | None = None,
) -> RetentionRunResult:
    """Release one intake using attempt-commit/delete/outcome-commit ordering."""
    if not actor:
        raise ValueError("actor must be non-empty")
    row = _get_intake(session, intake)
    intake_id = row.intake_id
    if row.retention_state != RetentionState.HELD:
        return RetentionRunResult(intake_id, False, (), f"state={row.retention_state}")

    items = tuple(_intake_items(session, row))
    witnesses = _fresh_witness_answers(session, items, _witness_answers)
    precheck = _local_gate_status(session, row, witnesses, require_held=True)
    if not precheck.releasable:
        return RetentionRunResult(intake_id, False, (), _hold_reason(precheck))

    operation_id = f"release:{intake_id}"
    session.commit()
    row = _require_intake(session, intake_id)
    items = tuple(_intake_items(session, row))
    witnesses = _fresh_witness_answers(session, items, witnesses)
    _begin_immediate(session)
    row = _require_intake(session, intake_id)
    reserved = _local_gate_status(session, row, witnesses, require_held=True)
    if not reserved.releasable:
        session.rollback()
        return RetentionRunResult(intake_id, False, (), _hold_reason(reserved))
    fingerprint = _policy_fingerprint(session, tuple(_intake_items(session, row)))
    _add_once_event(
        session,
        intake_id=intake_id,
        action="release_attempted",
        operation_id=operation_id,
        actor=actor,
        detail={"policy_fingerprint": fingerprint},
    )
    cloud_copies = tuple(_cloud_blob_copies(session, intake_id))
    session.commit()

    outcomes: list[dict[str, object]] = []
    for copy in cloud_copies:
        outcome = _delete_copy_object(copy)
        outcomes.append({"copy_id": copy.id, "outcome": outcome})
    aggregate_outcome = (
        "already-absent"
        if not outcomes or all(item["outcome"] == "already-absent" for item in outcomes)
        else "deleted"
    )

    _begin_immediate(session)
    row = _require_intake(session, intake_id)
    if row.retention_state != RetentionState.HELD:
        session.rollback()
        return RetentionRunResult(intake_id, False, (), f"state={row.retention_state}")
    now = _utcnow()
    copy_ids = tuple(copy.id for copy in cloud_copies)
    for copy_id in copy_ids:
        copy = session.get(Copy, copy_id)
        if copy is not None and copy.deleted_at is None:
            copy.deleted_at = now
    row.retention_state = RetentionState.RELEASED
    row.released_at = now
    row.release_policy_fingerprint = fingerprint
    _add_once_event(
        session,
        intake_id=intake_id,
        action="cloud_blob_deleted",
        operation_id=operation_id,
        actor=actor,
        at=now,
        detail={
            "bundle_id": _cloud_bundle_id(intake_id),
            "copy_ids": list(copy_ids),
            "outcome": aggregate_outcome,
            "copy_outcomes": outcomes,
        },
    )
    _add_once_event(
        session,
        intake_id=intake_id,
        action="released",
        operation_id=operation_id,
        actor=actor,
        at=now,
        detail={"copy_ids": list(copy_ids)},
    )
    session.commit()
    return RetentionRunResult(intake_id, True, copy_ids, "released")


def sweep_staging(
    session: Session,
    intake: Intake | str,
    *,
    actor: str,
    grace_days: int = DEFAULT_STAGING_GRACE_DAYS,
    break_glass: bool = False,
    _witness_answers: WitnessAnswers | None = None,
    _grace_override_evented: bool = False,
) -> StagingSweepResult:
    """Re-gate, atomically tombstone, then idempotently garbage-collect staging."""
    if not actor:
        raise ValueError("actor must be non-empty")
    row = _get_intake(session, intake)
    intake_id = row.intake_id
    if row.retention_state == RetentionState.PURGED:
        return StagingSweepResult(intake_id, False, None, "already-purged")
    if row.retention_state == RetentionState.ABANDONED:
        return StagingSweepResult(intake_id, False, None, "abandoned")
    if grace_days < DEFAULT_STAGING_GRACE_DAYS and not break_glass:
        return StagingSweepResult(intake_id, False, None, "grace-below-floor")
    if grace_days < DEFAULT_STAGING_GRACE_DAYS and not _grace_override_evented:
        _record_grace_override(session, actor=actor, grace_days=grace_days)
        session.commit()

    if row.retention_state == RetentionState.TOMBSTONED:
        return _resume_tombstone_gc(session, row, actor=actor)
    if row.retention_state != RetentionState.RELEASED or row.released_at is None:
        return StagingSweepResult(intake_id, False, None, f"state={row.retention_state}")

    items = tuple(_intake_items(session, row))
    witnesses = _fresh_witness_answers(session, items, _witness_answers)
    precheck_holds = _purge_holds(
        session,
        row,
        witnesses,
        grace_days=grace_days,
        now=_utcnow(),
    )
    try:
        staging_root, tombstone_path = _preflight_staging_paths(row)
    except RetentionError as exc:
        precheck_holds.append(f"staging-path:{exc}")
        staging_root = tombstone_path = None
    if precheck_holds:
        _record_purge_hold(session, row, actor=actor, reasons=precheck_holds)
        session.commit()
        return StagingSweepResult(intake_id, False, None, precheck_holds[0])
    assert staging_root is not None
    assert tombstone_path is not None

    operation_id = f"purge:{intake_id}"
    session.commit()
    row = _require_intake(session, intake_id)
    items = tuple(_intake_items(session, row))
    witnesses = _fresh_witness_answers(session, items, witnesses)
    _begin_immediate(session)
    row = _require_intake(session, intake_id)
    reservation_holds = _purge_holds(
        session,
        row,
        witnesses,
        grace_days=grace_days,
        now=_utcnow(),
    )
    if reservation_holds:
        _record_purge_hold(session, row, actor=actor, reasons=reservation_holds)
        session.commit()
        return StagingSweepResult(intake_id, False, None, reservation_holds[0])
    _add_once_event(
        session,
        intake_id=intake_id,
        action="purge_attempted",
        operation_id=operation_id,
        actor=actor,
        detail={"path": str(staging_root), "grace_days": grace_days},
    )
    session.commit()

    # Transaction B owns the point-of-no-return decision.  Check witness ages
    # only after its local re-gate and path hashing, immediately before rename.
    # A stale observation rolls B back; the one allowed remote refresh executes
    # with no transaction open, then a new B re-derives every local predicate.
    refreshed_after_stale = False
    while True:
        _begin_immediate(session)
        row = _require_intake(session, intake_id)
        commitment_holds = _purge_holds(
            session,
            row,
            witnesses,
            grace_days=grace_days,
            now=_utcnow(),
        )
        if commitment_holds:
            _record_purge_hold(session, row, actor=actor, reasons=commitment_holds)
            session.commit()
            return StagingSweepResult(intake_id, False, None, commitment_holds[0])
        staging_root, tombstone_path = _preflight_staging_paths(row)
        if _witness_answers_stale(witnesses):
            requests = (
                None
                if refreshed_after_stale
                else _prepare_witness_requests(
                    session,
                    tuple(_intake_items(session, row)),
                )
            )
            session.rollback()
            if refreshed_after_stale:
                row = _require_intake(session, intake_id)
                reasons = ["rem-unconfirmed"]
                _record_purge_hold(session, row, actor=actor, reasons=reasons)
                session.commit()
                return StagingSweepResult(intake_id, False, None, reasons[0])
            assert requests is not None
            if session.in_transaction():
                raise RuntimeError("witness refresh must run outside a transaction")
            witnesses = _execute_witness_requests(requests)
            refreshed_after_stale = True
            continue

        _atomic_tombstone(staging_root, tombstone_path, row.intake_id)
        now = _utcnow()
        row.retention_state = RetentionState.TOMBSTONED
        row.staging_tombstoned_at = now
        row.staging_tombstone_path = str(tombstone_path)
        _add_once_event(
            session,
            intake_id=intake_id,
            action="staging_tombstoned",
            operation_id=operation_id,
            actor=actor,
            at=now,
            detail={"source_path": str(staging_root), "tombstone_path": str(tombstone_path)},
        )
        session.commit()
        return _resume_tombstone_gc(session, row, actor=actor)


def run_retention_batch(
    session: Session,
    *,
    actor: str,
    intake_id: str | None = None,
    batch_limit: int | None = None,
    dry_run: bool = False,
) -> RetentionBatchResult:
    """Logged stage-1 batch funnel with a read-only pre-pass and tripwire."""
    if not actor:
        raise ValueError("actor must be non-empty")
    limit = _batch_limit(batch_limit)
    rows = _candidate_intakes(session, intake_id, (RetentionState.HELD,))
    assessments = [(_assess_intake(session, row), row.intake_id) for row in rows]
    candidates = tuple(
        _batch_candidate(
            session,
            assessment.status,
            release_condition=_release_condition(session, assessment.status),
        )
        for assessment, _ in assessments
        if assessment.status.releasable
    )
    holds = tuple(
        BatchHold(
            candidate_id, assessment.status.holds or (f"state={assessment.status.retention_state}",)
        )
        for assessment, candidate_id in assessments
        if not assessment.status.releasable
    )
    operation_id = f"batch:release:{uuid.uuid4()}"
    refused = len(candidates) > limit
    _record_batch_invocation(
        session,
        operation_id=operation_id,
        actor=actor,
        action="release",
        limit=limit,
        candidate_count=len(candidates),
        dry_run=dry_run,
        refused=refused,
    )
    session.commit()
    results: list[RetentionRunResult] = []
    if not refused and not dry_run:
        by_id = {candidate.intake_id for candidate in candidates}
        for assessment, candidate_id in assessments:
            if candidate_id in by_id:
                results.append(
                    run_retention(
                        session,
                        candidate_id,
                        actor=actor,
                        _witness_answers=assessment.witnesses,
                    )
                )
    all_held = (
        not candidates if dry_run or refused else not any(result.released for result in results)
    )
    reason = (
        "batch-limit-exceeded"
        if refused
        else "all-held"
        if all_held
        else "dry-run"
        if dry_run
        else "completed"
    )
    return RetentionBatchResult(
        operation_id=operation_id,
        action="release",
        limit=limit,
        candidate_count=len(candidates),
        refused=refused,
        dry_run=dry_run,
        all_held=all_held,
        candidates=candidates,
        holds=holds,
        results=tuple(results),
        reason=reason,
    )


def sweep_staging_batch(
    session: Session,
    *,
    actor: str,
    intake_id: str | None = None,
    batch_limit: int | None = None,
    dry_run: bool = False,
    grace_days: int = DEFAULT_STAGING_GRACE_DAYS,
    break_glass: bool = False,
) -> RetentionBatchResult:
    """Logged stage-2 batch funnel with grace floor and candidate brake."""
    if not actor:
        raise ValueError("actor must be non-empty")
    limit = _batch_limit(batch_limit)
    operation_id = f"batch:purge:{uuid.uuid4()}"
    if grace_days < DEFAULT_STAGING_GRACE_DAYS and not break_glass:
        _record_batch_invocation(
            session,
            operation_id=operation_id,
            actor=actor,
            action="purge",
            limit=limit,
            candidate_count=0,
            dry_run=dry_run,
            refused=True,
            refusal_reason="grace-below-floor",
        )
        session.commit()
        return RetentionBatchResult(
            operation_id,
            "purge",
            limit,
            0,
            True,
            dry_run,
            False,
            (),
            (),
            (),
            "grace-below-floor",
        )
    if grace_days < DEFAULT_STAGING_GRACE_DAYS:
        _record_grace_override(
            session,
            actor=actor,
            grace_days=grace_days,
            operation_id=operation_id,
        )

    rows = _candidate_intakes(
        session,
        intake_id,
        (RetentionState.RELEASED, RetentionState.TOMBSTONED),
    )
    assessments = [
        (_assess_intake(session, row, grace_days=grace_days), row.intake_id) for row in rows
    ]
    candidates = tuple(
        _batch_candidate(
            session,
            assessment.status,
            release_condition=(
                "purge:tombstone-gc-pending"
                if assessment.status.purge_status.status == "tombstone-gc-pending"
                else _release_condition(session, assessment.status)
            ),
        )
        for assessment, _ in assessments
        if assessment.status.purge_status.status in ("eligible", "tombstone-gc-pending")
    )
    holds = tuple(
        BatchHold(candidate_id, (assessment.status.purge_status.status,))
        for assessment, candidate_id in assessments
        if assessment.status.purge_status.status not in ("eligible", "tombstone-gc-pending")
    )
    refused = len(candidates) > limit
    _record_batch_invocation(
        session,
        operation_id=operation_id,
        actor=actor,
        action="purge",
        limit=limit,
        candidate_count=len(candidates),
        dry_run=dry_run,
        refused=refused,
    )
    session.commit()
    results: list[StagingSweepResult] = []
    if not refused and not dry_run:
        by_id = {candidate.intake_id for candidate in candidates}
        for assessment, candidate_id in assessments:
            if candidate_id in by_id:
                results.append(
                    sweep_staging(
                        session,
                        candidate_id,
                        actor=actor,
                        grace_days=grace_days,
                        break_glass=break_glass,
                        _witness_answers=assessment.witnesses,
                        _grace_override_evented=True,
                    )
                )
    all_held = (
        not candidates if dry_run or refused else not any(result.purged for result in results)
    )
    reason = (
        "batch-limit-exceeded"
        if refused
        else "all-held"
        if all_held
        else "dry-run"
        if dry_run
        else "completed"
    )
    return RetentionBatchResult(
        operation_id=operation_id,
        action="purge",
        limit=limit,
        candidate_count=len(candidates),
        refused=refused,
        dry_run=dry_run,
        all_held=all_held,
        candidates=candidates,
        holds=holds,
        results=tuple(results),
        reason=reason,
    )


def _assess_intake(
    session: Session,
    intake: Intake | str,
    *,
    grace_days: int = DEFAULT_STAGING_GRACE_DAYS,
) -> _GateAssessment:
    row = _get_intake(session, intake)
    items = tuple(_intake_items(session, row))
    witnesses = (
        {}
        if row.retention_state
        in (RetentionState.TOMBSTONED, RetentionState.ABANDONED, RetentionState.PURGED)
        else _collect_witness_answers(session, items)
    )
    status = _local_gate_status(session, row, witnesses, require_held=True, grace_days=grace_days)
    return _GateAssessment(status, witnesses)


def _local_gate_status(
    session: Session,
    row: Intake,
    witnesses: WitnessAnswers,
    *,
    require_held: bool,
    grace_days: int = DEFAULT_STAGING_GRACE_DAYS,
) -> IntakeGateStatus:
    items = tuple(_intake_items(session, row))
    eligibility_holds = _intake_eligibility_holds(row, items)
    base_holds = [
        *eligibility_holds,
        *_arrangement_holds(session, row),
        *_prepared_profile_holds(session, row),
    ]
    assets = (
        tuple(_asset_gate_status(session, item, witnesses=witnesses) for item in items)
        if not eligibility_holds
        else ()
    )
    asset_holds = _asset_holds(assets)
    holds = tuple([*base_holds, *asset_holds])
    assets_releasable = bool(assets) and all(asset.releasable for asset in assets)
    state_allows = not require_held or row.retention_state == RetentionState.HELD
    is_releasable = state_allows and not base_holds and assets_releasable
    now = _utcnow()
    deadline = (
        _as_utc(row.released_at) + dt.timedelta(days=grace_days)
        if row.released_at is not None
        else None
    )
    purge = _purge_projection(session, row, holds, deadline=deadline, now=now, items=items)
    return IntakeGateStatus(
        intake_id=row.intake_id,
        retention_state=str(row.retention_state),
        releasable=is_releasable,
        holds=holds,
        assets=assets,
        released_at=row.released_at,
        staging_deleted_at=row.staging_deleted_at,
        grace_deadline=deadline,
        purge_status=PurgeStatus(purge, now),
    )


def _purge_projection(
    session: Session,
    row: Intake,
    holds: tuple[str, ...],
    *,
    deadline: dt.datetime | None,
    now: dt.datetime,
    items: tuple[IngestItem, ...],
) -> str:
    if row.retention_state == RetentionState.TOMBSTONED:
        return "tombstone-gc-pending"
    if row.retention_state == RetentionState.ABANDONED:
        return "abandoned"
    if row.retention_state != RetentionState.RELEASED:
        return "not-applicable"
    if deadline is None or deadline >= now:
        return "grace-active"
    if row.release_policy_fingerprint is None:
        return "blocked:missing-policy-fingerprint"
    if _policy_fingerprint(session, items) != row.release_policy_fingerprint:
        return "blocked:policy-changed"
    if holds:
        return f"blocked:{holds[0]}"
    return "eligible"


def _purge_holds(
    session: Session,
    row: Intake,
    witnesses: WitnessAnswers,
    *,
    grace_days: int,
    now: dt.datetime,
) -> list[str]:
    if row.retention_state != RetentionState.RELEASED or row.released_at is None:
        return [f"state={row.retention_state}"]
    status = _local_gate_status(
        session,
        row,
        witnesses,
        require_held=False,
        grace_days=grace_days,
    )
    holds = list(status.holds)
    deadline = _as_utc(row.released_at) + dt.timedelta(days=grace_days)
    if deadline >= now:
        holds.append("grace-active")
    items = tuple(_intake_items(session, row))
    if row.release_policy_fingerprint is None:
        holds.append("missing-policy-fingerprint")
    elif _policy_fingerprint(session, items) != row.release_policy_fingerprint:
        holds.append("policy-changed")
    return holds


def _intake_eligibility_holds(intake: Intake, items: tuple[IngestItem, ...]) -> list[str]:
    holds: list[str] = []
    if intake.status != IntakeStatus.REGISTERED:
        holds.append(f"intake-status:{intake.status}")
    if not items:
        holds.append("intake-empty")
    return holds


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


def _asset_gate_status(
    session: Session,
    item: IngestItem,
    *,
    witnesses: WitnessAnswers | None = None,
) -> AssetGateStatus:
    """Existing per-item gate, extended with cached independent witnesses."""
    try:
        targets = _policy_targets(session, item.artifactclass)
    except (ReplicationError, ValueError) as exc:
        reason = _policy_expansion_hold(exc)
        return AssetGateStatus(
            ingest_item_id=item.id,
            logical_asset_hash=item.logical_asset_hash.hex(),
            artifactclass=item.artifactclass,
            releasable=False,
            pools=(
                PoolGateStatus(
                    pool_id="<policy>",
                    offsite_gate=False,
                    satisfied=False,
                    copy_id=None,
                    media_id=None,
                    reason=reason,
                ),
            ),
        )
    pool_statuses = tuple(
        _pool_gate_status(
            session,
            item.logical_asset_hash,
            target,
            witnesses=witnesses or {},
        )
        for target in targets
    )
    return AssetGateStatus(
        ingest_item_id=item.id,
        logical_asset_hash=item.logical_asset_hash.hex(),
        artifactclass=item.artifactclass,
        releasable=bool(pool_statuses) and all(status.satisfied for status in pool_statuses),
        pools=pool_statuses,
    )


def _pool_gate_status(
    session: Session,
    asset_hash: bytes,
    target: PoolTarget,
    *,
    witnesses: WitnessAnswers,
) -> PoolGateStatus:
    pool = session.get(Pool, target.pool_id)
    if pool is None:
        return PoolGateStatus(
            target.pool_id, target.offsite_gate, False, None, None, "pool-missing"
        )
    candidates = [
        copy
        for copy in _qualifying_copies_for_pool(
            session,
            asset_hash,
            target.pool_id,
            target.artifactclass,
        )
        if copy.pool_id == pool.id and copy.backend_id == pool.backend_id
    ]
    if not candidates:
        return PoolGateStatus(
            target.pool_id,
            target.offsite_gate,
            False,
            None,
            None,
            "no-verified-copy",
        )
    confirmed = _confirmed_media_ids(session) if target.offsite_gate else set()
    saw_missing_media = False
    saw_witness_hold = False
    for copy in candidates:
        witness = witnesses.get(copy.id)
        if witness is None:
            try:
                witness_eligible = factory.backend_declares_retention_witness(copy.backend)
            except Exception:
                witness_eligible = True
            if witness_eligible:
                saw_witness_hold = True
                continue
        elif witness.eligible and (witness.result is None or not witness.result.confirmed):
            saw_witness_hold = True
            continue
        media_id = _copy_media_id(copy)
        if target.offsite_gate:
            if not media_id:
                saw_missing_media = True
                continue
            if media_id not in confirmed:
                continue
        return PoolGateStatus(
            target.pool_id,
            target.offsite_gate,
            True,
            copy.id,
            media_id,
        )
    reason = (
        "rem-unconfirmed"
        if saw_witness_hold
        else "missing-media-id"
        if saw_missing_media
        else "offsite-unconfirmed"
    )
    return PoolGateStatus(
        target.pool_id,
        target.offsite_gate,
        False,
        candidates[0].id,
        _copy_media_id(candidates[0]),
        reason,
    )


def _qualifying_copies_for_pool(
    session: Session,
    asset_hash: bytes,
    pool_id: str,
    artifactclass: str,
) -> list[Copy]:
    return list(
        durable_placements(
            session,
            AssetTarget(asset_hash, artifactclass),
            require_verified=True,
            artifactclass=artifactclass,
            pool_id=pool_id,
        )
    )


def _collect_witness_answers(
    session: Session,
    items: tuple[IngestItem, ...],
) -> WitnessAnswers:
    """Perform bounded adapter witness calls before any write reservation."""

    return _execute_witness_requests(_prepare_witness_requests(session, items))


def _prepare_witness_requests(
    session: Session,
    items: tuple[IngestItem, ...],
) -> tuple[_WitnessRequest, ...]:
    """Resolve witness adapters and copy inputs without contacting a backend."""

    requests: list[_WitnessRequest] = []
    seen: set[int] = set()
    for item in items:
        try:
            targets = _policy_targets(session, item.artifactclass)
        except (ReplicationError, ValueError):
            continue
        for target in targets:
            for copy in _qualifying_copies_for_pool(
                session,
                item.logical_asset_hash,
                target.pool_id,
                target.artifactclass,
            ):
                if copy.id in seen:
                    continue
                seen.add(copy.id)
                try:
                    eligible = factory.backend_declares_retention_witness(copy.backend)
                except Exception as exc:
                    requests.append(
                        _WitnessRequest(
                            copy_id=copy.id,
                            eligible=True,
                            adapter=None,
                            locator=dict(copy.native_locator),
                            expected_hash=bytes(copy.integrity_hash),
                            error=f"witness capability unavailable: {exc}",
                        )
                    )
                    continue
                if not eligible:
                    requests.append(
                        _WitnessRequest(
                            copy_id=copy.id,
                            eligible=False,
                            adapter=None,
                            locator=dict(copy.native_locator),
                            expected_hash=bytes(copy.integrity_hash),
                        )
                    )
                    continue
                try:
                    adapter = factory.backend_from_row(copy.backend)
                except Exception as exc:
                    requests.append(
                        _WitnessRequest(
                            copy_id=copy.id,
                            eligible=True,
                            adapter=None,
                            locator=dict(copy.native_locator),
                            expected_hash=bytes(copy.integrity_hash),
                            error=f"witness unavailable: {exc}",
                        )
                    )
                    continue
                if not isinstance(adapter, RetentionWitnessBackend):
                    requests.append(
                        _WitnessRequest(
                            copy_id=copy.id,
                            eligible=True,
                            adapter=None,
                            locator=dict(copy.native_locator),
                            expected_hash=bytes(copy.integrity_hash),
                            error="adapter declares but does not implement witness capability",
                        )
                    )
                    continue
                requests.append(
                    _WitnessRequest(
                        copy_id=copy.id,
                        eligible=True,
                        adapter=adapter,
                        locator=dict(copy.native_locator),
                        expected_hash=bytes(copy.integrity_hash),
                    )
                )
    return tuple(requests)


def _execute_witness_requests(
    requests: tuple[_WitnessRequest, ...],
) -> WitnessAnswers:
    """Execute already-detached witness calls without consulting the database."""

    answers: WitnessAnswers = {}
    for request in requests:
        if not request.eligible:
            answers[request.copy_id] = _WitnessEvidence(
                eligible=False,
                result=None,
                observed_at=_utcnow(),
            )
            continue
        if request.error is not None:
            answers[request.copy_id] = _WitnessEvidence(
                eligible=True,
                result=WitnessResult(False, request.error),
                observed_at=_utcnow(),
            )
            continue
        assert request.adapter is not None
        try:
            result = request.adapter.witness_copy(
                request.locator,
                expected_hash=request.expected_hash,
            )
        except Exception as exc:
            result = WitnessResult(False, f"witness unavailable: {exc}")
        answers[request.copy_id] = _WitnessEvidence(
            eligible=True,
            result=result,
            observed_at=_utcnow(),
        )
    return answers


def _fresh_witness_answers(
    session: Session,
    items: tuple[IngestItem, ...],
    answers: WitnessAnswers | None,
) -> WitnessAnswers:
    """Reuse witness evidence only while every observation remains fresh."""

    if answers is None:
        return _collect_witness_answers(session, items)
    if _witness_answers_stale(answers):
        return _collect_witness_answers(session, items)
    return answers


def _witness_answers_stale(answers: WitnessAnswers) -> bool:
    """Return whether any release-authorizing remote observation has expired."""

    now = _utcnow()
    return any(
        answer.eligible and now - _as_utc(answer.observed_at) > WITNESS_MAX_AGE
        for answer in answers.values()
    )


def _policy_expansion_hold(exc: Exception) -> str:
    """Turn policy expansion failures into stable fail-closed gate evidence."""

    if isinstance(exc, ReplicationPolicyMissing):
        return "policy-missing"
    return f"policy-invalid:{type(exc).__name__}:{exc}"


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
    return [
        target
        for _, target in target_pools(
            session,
            artifactclass,
            backend_rows,
            write_eligible_only=False,
        )
    ]


def _policy_fingerprint(session: Session, items: tuple[IngestItem, ...]) -> str:
    artifactclasses = sorted({item.artifactclass for item in items})
    snapshot: list[dict[str, object]] = []
    for artifactclass in artifactclasses:
        pools: list[dict[str, object]] = []
        for membership, pool, backend in session.execute(
            select(ArtifactClassPool, Pool, Backend)
            .join(Pool, Pool.id == ArtifactClassPool.pool_id)
            .join(Backend, Backend.id == Pool.backend_id)
            .where(
                ArtifactClassPool.artifactclass == artifactclass,
                ArtifactClassPool.active.is_(True),
            )
            .order_by(Pool.id)
        ):
            del membership
            pools.append(
                {
                    "pool_id": pool.id,
                    "backend_id": backend.id,
                    "backend_name": backend.name,
                    "backend_kind": str(backend.kind),
                    "offsite_gate": pool.offsite_gate,
                    "witness_eligible": factory.backend_declares_retention_witness(backend),
                }
            )
        snapshot.append({"artifactclass": artifactclass, "pools": pools})
    canonical = json.dumps(
        {"version": 1, "artifactclasses": snapshot},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return f"{POLICY_FINGERPRINT_VERSION}:{hashlib.sha256(canonical).hexdigest()}"


def _preflight_staging_paths(intake: Intake) -> tuple[Path, Path]:
    staging_root = _staging_root_for_intake(intake)
    if staging_root is None:
        raise RetentionError("staging root is not recorded")
    tombstone_root = _tombstone_root()
    _assert_no_symlink_components(tombstone_root, allow_missing_leaf=True)
    tombstone_root.mkdir(parents=True, exist_ok=True)
    _assert_no_symlink_components(tombstone_root)
    digest = hashlib.sha256(intake.intake_id.encode("utf-8")).hexdigest()
    tombstone_path = tombstone_root / f"{TOMBSTONE_BASENAME_VERSION}-{digest}"
    if tombstone_path.exists() and not staging_root.exists():
        _validate_tombstone_path(tombstone_path, intake.intake_id)
        return staging_root, tombstone_path
    _validate_staging_root(staging_root, intake)
    if staging_root.stat().st_dev != tombstone_root.stat().st_dev:
        raise RetentionError("tombstone root is not on the staging filesystem")
    return staging_root, tombstone_path


def _validate_staging_root(root: Path, intake: Intake) -> None:
    lexical = root.absolute()
    _assert_no_symlink_components(lexical)
    resolved = lexical.resolve(strict=True)
    if _unsafe_delete_root(resolved):
        raise RetentionError(f"unsafe staging root: {resolved}")
    if not any(_is_relative_to(resolved, allowed) for allowed in _landing_roots()):
        raise RetentionError(f"staging root is outside the configured landing roots: {resolved}")
    _validate_sentinel(resolved, intake.intake_id)
    if intake.manifest_path is not None:
        manifest = Path(intake.manifest_path)
        _assert_no_symlink_components(manifest)
        manifest_resolved = manifest.resolve(strict=True)
        if not _is_relative_to(manifest_resolved, resolved):
            raise RetentionError("manifest escapes the staging root")
        if not manifest_resolved.is_file():
            raise RetentionError("manifest is not a regular file")
        if intake.manifest_digest is None:
            raise RetentionError("manifest digest is missing")
        if _sha256_file(manifest_resolved) != intake.manifest_digest:
            raise RetentionError("manifest digest mismatch")


def _validate_sentinel(root: Path, intake_id: str) -> None:
    sentinel = root / "intake.json"
    _assert_no_symlink_components(sentinel)
    try:
        if not sentinel.is_file():
            raise RetentionError("intake sentinel is not a regular file")
        payload = json.loads(sentinel.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError) as exc:
        raise RetentionError(f"invalid intake sentinel: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("intake_id") != intake_id:
        raise RetentionError("intake sentinel identity mismatch")


def _atomic_tombstone(source: Path, destination: Path, intake_id: str) -> None:
    if destination.exists():
        _validate_sentinel(destination, intake_id)
        if source.exists():
            raise RetentionError("both staging and tombstone paths exist")
        return
    if not source.exists():
        raise RetentionError("staging path disappeared before tombstone rename")
    os.replace(source, destination)
    _fsync_if_exists(destination.parent)
    _fsync_if_exists(source.parent)
    _validate_sentinel(destination, intake_id)


def _resume_tombstone_gc(
    session: Session,
    row: Intake,
    *,
    actor: str,
) -> StagingSweepResult:
    intake_id = row.intake_id
    if row.staging_tombstone_path is None:
        raise RetentionError("tombstoned intake is missing its tombstone path")
    tombstone_path = Path(row.staging_tombstone_path)
    _validate_tombstone_path(tombstone_path, intake_id)
    deleted_path = str(tombstone_path)
    _delete_path_idempotent(tombstone_path)
    operation_id = f"purge:{intake_id}"
    _begin_immediate_after_read(session)
    current = _require_intake(session, intake_id)
    if current.retention_state == RetentionState.PURGED:
        session.rollback()
        return StagingSweepResult(intake_id, False, deleted_path, "already-purged")
    if current.retention_state != RetentionState.TOMBSTONED:
        session.rollback()
        return StagingSweepResult(
            intake_id, False, deleted_path, f"state={current.retention_state}"
        )
    now = _utcnow()
    current.retention_state = RetentionState.PURGED
    current.staging_deleted_at = now
    _add_once_event(
        session,
        intake_id=intake_id,
        action="staging_deleted",
        operation_id=operation_id,
        actor=actor,
        at=now,
        detail={"path": deleted_path},
    )
    session.commit()
    return StagingSweepResult(intake_id, True, deleted_path, "purged")


def _validate_tombstone_path(path: Path, intake_id: str) -> None:
    expected_root = _tombstone_root().resolve(strict=True)
    lexical = path.absolute()
    _assert_no_symlink_components(lexical, allow_missing_leaf=True)
    resolved = lexical.resolve(strict=False)
    if resolved.parent != expected_root:
        raise RetentionError("recorded tombstone path is outside the configured root")
    expected = f"{TOMBSTONE_BASENAME_VERSION}-{hashlib.sha256(intake_id.encode()).hexdigest()}"
    if resolved.name != expected:
        raise RetentionError("recorded tombstone basename does not match the intake")
    if resolved.exists():
        _validate_sentinel(resolved, intake_id)


def _landing_roots() -> tuple[Path, ...]:
    raw = os.environ.get("SUTRADHARA_RETENTION_LANDING_ROOTS")
    values = [part for part in (raw or "").split(os.pathsep) if part]
    if not values:
        values = [os.environ.get("SUTRA_RECEIVE_LANDING_ROOT", "/replica/landing")]
    roots: list[Path] = []
    for value in values:
        path = Path(value).absolute()
        _assert_no_symlink_components(path, allow_missing_leaf=False)
        roots.append(path.resolve(strict=True))
    return tuple(roots)


def _tombstone_root() -> Path:
    configured = os.environ.get("SUTRADHARA_RETENTION_TOMBSTONE_ROOT")
    if configured:
        return Path(configured).absolute()
    [first, *_] = _landing_roots()
    return first / ".retention-tombstones"


def _assert_no_symlink_components(path: Path, *, allow_missing_leaf: bool = False) -> None:
    current = Path(path.anchor)
    parts = path.parts[1:] if path.is_absolute() else path.parts
    for index, part in enumerate(parts):
        current /= part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            if allow_missing_leaf and index == len(parts) - 1:
                return
            if allow_missing_leaf:
                continue
            raise RetentionError(f"path component does not exist: {current}") from None
        if stat.S_ISLNK(mode):
            raise RetentionError(f"symlink path component rejected: {current}")


def _staging_root_for_intake(intake: Intake) -> Path | None:
    if intake.manifest_path:
        return Path(intake.manifest_path).absolute().parent
    source_paths = [
        Path(value).absolute()
        for item in intake.items
        if isinstance((value := item.item_metadata.get("source_path")), str) and value
    ]
    if not source_paths:
        return None
    return Path(os.path.commonpath([str(path.parent) for path in source_paths])).absolute()


def _delete_path_idempotent(path: Path) -> None:
    resolved = path.resolve(strict=False)
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


def _cloud_blob_copies(session: Session, intake_id: str) -> list[Copy]:
    return list(
        session.scalars(
            select(Copy)
            .where(Copy.bundle_id == _cloud_bundle_id(intake_id), Copy.deleted_at.is_(None))
            .order_by(Copy.id)
        )
    )


def _cloud_bundle_id(intake_id: str) -> str:
    return f"{CLOUD_BLOB_PREFIX}{intake_id}"


def _delete_copy_object(copy: Copy) -> str:
    backend = factory.backend_from_row(copy.backend)
    delete_object = getattr(backend, "delete_object", None)
    if not callable(delete_object):
        raise RetentionError(
            f"backend {copy.backend.name!r} for copy id={copy.id} does not support delete_object"
        )
    deleted = delete_object(copy.native_locator)
    return "already-absent" if deleted is False else "deleted"


def _confirmed_media_ids(session: Session) -> set[str]:
    return set(
        session.scalars(
            select(OffsiteConfirmation.media_id).where(OffsiteConfirmation.revoked_at.is_(None))
        )
    )


def _known_media_id(session: Session, media_id: str) -> bool:
    return any(
        _copy_media_id(copy) == media_id
        for copy in session.scalars(select(Copy).where(Copy.deleted_at.is_(None)))
    )


def _asset_holds(assets: tuple[AssetGateStatus, ...]) -> list[str]:
    return [
        f"asset:{asset.ingest_item_id}:pool:{pool.pool_id}:{pool.reason}"
        for asset in assets
        for pool in asset.pools
        if not pool.satisfied
    ]


def _hold_reason(status: IntakeGateStatus) -> str:
    if status.holds:
        return status.holds[0]
    return f"state={status.retention_state}"


def _batch_candidate(
    session: Session,
    status: IntakeGateStatus,
    *,
    release_condition: str,
) -> BatchCandidate:
    """Capture one candidate under the explicit condition that releases it."""

    selected = [pool for asset in status.assets for pool in asset.pools if pool.satisfied]
    evidence = tuple(
        f"asset:{asset.ingest_item_id}:pool:{pool.pool_id}:copy:{pool.copy_id}"
        for asset in status.assets
        for pool in asset.pools
        if pool.satisfied
    )
    cutoff = _utcnow() - RECENT_FLIP_WINDOW
    recent = any(
        copy is not None and _as_utc(copy.health_changed_at) >= cutoff
        for pool in selected
        if pool.copy_id is not None
        for copy in (session.get(Copy, pool.copy_id),)
    )
    return BatchCandidate(status.intake_id, release_condition, evidence, recent)


def _release_condition(session: Session, status: IntakeGateStatus) -> str:
    """Name the latest concrete evidence item that completed the gate."""

    evidence: list[tuple[dt.datetime, str]] = []
    for asset in status.assets:
        for pool in asset.pools:
            if not pool.satisfied or pool.copy_id is None:
                continue
            copy = session.get(Copy, pool.copy_id)
            if copy is None or copy.last_measured_at is None:
                continue
            evidence.append((_as_utc(copy.last_measured_at), f"verified:{pool.pool_id}"))
            if not pool.offsite_gate or pool.media_id is None:
                continue
            confirmation = session.get(OffsiteConfirmation, pool.media_id)
            if confirmation is not None and confirmation.revoked_at is None:
                evidence.append(
                    (
                        _as_utc(confirmation.confirmed_at),
                        f"offsite-confirmed:{pool.media_id}",
                    )
                )
    if not evidence:
        raise RetentionError(
            f"releasable intake {status.intake_id!r} has no release-enabling evidence"
        )
    return max(evidence, key=lambda item: (item[0], item[1]))[1]


def _record_batch_invocation(
    session: Session,
    *,
    operation_id: str,
    actor: str,
    action: str,
    limit: int,
    candidate_count: int,
    dry_run: bool,
    refused: bool,
    refusal_reason: str = "batch-limit-exceeded",
) -> None:
    detail = {
        "action": action,
        "limit": limit,
        "candidate_count": candidate_count,
        "dry_run": dry_run,
        "refused": refused,
    }
    _add_event(
        session,
        subject_type="batch",
        subject_id=operation_id,
        action="batch_invoked",
        operation_id=operation_id,
        actor=actor,
        detail=detail,
    )
    if refused:
        _add_event(
            session,
            subject_type="batch",
            subject_id=operation_id,
            action="batch_refused",
            operation_id=operation_id,
            actor=actor,
            detail={**detail, "reason": refusal_reason},
        )


def _record_grace_override(
    session: Session,
    *,
    actor: str,
    grace_days: int,
    operation_id: str | None = None,
) -> None:
    correlation = operation_id or f"grace-override:{uuid.uuid4()}"
    _add_event(
        session,
        subject_type="batch",
        subject_id=correlation,
        action="grace_overridden",
        operation_id=correlation,
        actor=actor,
        detail={"grace_days": grace_days, "default_grace_days": DEFAULT_STAGING_GRACE_DAYS},
    )


def _record_purge_hold(
    session: Session,
    row: Intake,
    *,
    actor: str,
    reasons: list[str],
) -> None:
    _add_event(
        session,
        intake_id=row.intake_id,
        subject_type="intake",
        subject_id=row.intake_id,
        action="staging_purge_held",
        operation_id=f"purge:{row.intake_id}",
        actor=actor,
        detail={"reasons": reasons},
    )


def _add_once_event(
    session: Session,
    *,
    intake_id: str,
    action: str,
    operation_id: str,
    actor: str,
    detail: dict[str, object],
    at: dt.datetime | None = None,
) -> RetentionEvent:
    existing = session.scalars(
        select(RetentionEvent).where(
            RetentionEvent.action == action,
            RetentionEvent.operation_id == operation_id,
        )
    ).one_or_none()
    if existing is not None:
        return existing
    return _add_event(
        session,
        intake_id=intake_id,
        subject_type="intake",
        subject_id=intake_id,
        action=action,
        operation_id=operation_id,
        actor=actor,
        at=at,
        detail=detail,
    )


def _add_event(
    session: Session,
    *,
    subject_type: str,
    subject_id: str,
    action: str,
    operation_id: str,
    actor: str,
    detail: dict[str, object],
    intake_id: str | None = None,
    at: dt.datetime | None = None,
    supersedes_source: str | None = None,
    supersedes_event_id: int | None = None,
) -> RetentionEvent:
    _validate_event_detail(action, detail)
    row = RetentionEvent(
        intake_id=intake_id,
        subject_type=subject_type,
        subject_id=subject_id,
        action=action,
        operation_id=operation_id,
        actor=actor,
        at=at or _utcnow(),
        detail=detail,
        supersedes_source=supersedes_source,
        supersedes_event_id=supersedes_event_id,
    )
    session.add(row)
    return row


def _validate_event_detail(action: str, detail: dict[str, object]) -> None:
    """Enforce the canonical payload shape for every audit action."""
    expected = _EVENT_DETAIL_KEYS.get(action)
    if expected is None:
        raise ValueError(f"unknown retention event action: {action}")
    actual = frozenset(detail)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        raise ValueError(f"invalid {action} detail keys: missing={missing!r} unknown={unknown!r}")
    if action == "cloud_blob_deleted" and detail["outcome"] not in {
        "deleted",
        "already-absent",
    }:
        raise ValueError("cloud_blob_deleted outcome must be deleted or already-absent")


def _batch_limit(value: int | None) -> int:
    limit = DEFAULT_BATCH_LIMIT if value is None else value
    if limit <= 0:
        raise ValueError("batch_limit must be greater than zero")
    return limit


def _candidate_intakes(
    session: Session,
    intake_id: str | None,
    states: tuple[RetentionState, ...],
) -> list[Intake]:
    query = select(Intake).order_by(Intake.intake_id)
    if intake_id is not None:
        query = query.where(Intake.intake_id == intake_id)
    else:
        query = query.where(Intake.retention_state.in_(states))
    return list(session.scalars(query))


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
    return _require_intake(session, intake)


def _require_intake(session: Session, intake_id: str) -> Intake:
    row = session.get(Intake, intake_id)
    if row is None:
        raise RetentionError(f"intake {intake_id!r} does not exist")
    return row


def _begin_immediate(session: Session) -> None:
    if session.in_transaction():
        session.commit()
    connection = session.connection()
    if connection.dialect.name == "sqlite":
        connection.exec_driver_sql("BEGIN IMMEDIATE")


def _begin_immediate_after_read(session: Session) -> None:
    _begin_immediate(session)


def _unsafe_delete_root(path: Path) -> bool:
    return path.parent == path or len(path.parts) < 3


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return path != root


def _fsync_if_exists(path: Path) -> None:
    if path.is_dir():
        _fsync_directory(path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _as_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)
