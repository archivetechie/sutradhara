"""Shared intake archive predicates and the phase-1c preservation audit.

The operator API, compatibility ``archived`` boolean, and rollout audit must
use exactly the same archive-evidence definition.  Keeping the SQL expressions
here prevents the legacy ANY predicate and the ALL-semantics read model from
drifting apart during the phase-1c rollout.
"""

from __future__ import annotations

import datetime as dt
import os
from collections.abc import Mapping
from typing import Any

from sqlalchemy import case, func, or_, select
from sqlalchemy.orm import Session, aliased

from sutradhara.catalog.models import (
    Bundle,
    BundleMember,
    IngestItem,
    Intake,
    Submission,
    SubmissionMember,
)
from sutradhara.catalog.types import RetentionState, SubmissionStatus

ARCHIVED_ALL_SEMANTICS_ENV = "SUTRADHARA_ARCHIVED_ALL_SEMANTICS"
ARCHIVE_AUDIT_SCHEMA = "sutradhara.archive-predicate-audit/v1"
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})
_RETENTION_PASSED = (RetentionState.RELEASED.value, RetentionState.PURGED.value)


def archived_all_semantics_enabled(environ: Mapping[str, str] | None = None) -> bool:
    """Return the rollout-gate value, defaulting safely to legacy ANY semantics."""

    source = os.environ if environ is None else environ
    raw = source.get(ARCHIVED_ALL_SEMANTICS_ENV)
    if raw is None or not raw.strip():
        return False
    normalized = raw.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    accepted = ", ".join(sorted(_TRUE_VALUES | _FALSE_VALUES))
    raise ValueError(f"{ARCHIVED_ALL_SEMANTICS_ENV} must be one of: {accepted}")


def intake_archive_evidence_exists() -> Any:
    """Return legacy ANY-era archive evidence correlated to ``Intake``."""

    sealed_bundle_evidence = (
        select(1)
        .select_from(IngestItem)
        .join(BundleMember, BundleMember.logical_asset_hash == IngestItem.logical_asset_hash)
        .join(Bundle, Bundle.id == BundleMember.bundle_id)
        .where(
            IngestItem.intake_id == Intake.intake_id,
            Bundle.status == "sealed",
        )
        .exists()
    )
    archived_submission_evidence = (
        select(1)
        .select_from(IngestItem)
        .join(SubmissionMember, SubmissionMember.ingest_item_id == IngestItem.id)
        .join(Submission, Submission.id == SubmissionMember.submission_id)
        .where(
            IngestItem.intake_id == Intake.intake_id,
            Submission.status == SubmissionStatus.ARCHIVED.value,
        )
        .exists()
    )
    return or_(sealed_bundle_evidence, archived_submission_evidence)


def hash_archive_evidence_exists(item: Any = IngestItem) -> Any:
    """Return archive evidence for the distinct content hash on ``item``."""

    archived_submission_item = aliased(IngestItem)
    sealed_for_hash = (
        select(1)
        .select_from(BundleMember)
        .join(Bundle, Bundle.id == BundleMember.bundle_id)
        .where(
            BundleMember.logical_asset_hash == item.logical_asset_hash,
            Bundle.status == "sealed",
        )
        .exists()
    )
    archived_submission_for_hash = (
        select(1)
        .select_from(SubmissionMember)
        .join(Submission, Submission.id == SubmissionMember.submission_id)
        .join(
            archived_submission_item,
            archived_submission_item.id == SubmissionMember.ingest_item_id,
        )
        .where(
            archived_submission_item.logical_asset_hash == item.logical_asset_hash,
            Submission.status == SubmissionStatus.ARCHIVED.value,
        )
        .exists()
    )
    return or_(sealed_for_hash, archived_submission_for_hash)


def intake_archive_state_expr() -> Any:
    """Return the ALL-semantics state using indexed correlated anti-joins."""

    hash_archived = hash_archive_evidence_exists()
    any_relevant = (
        select(1).select_from(IngestItem).where(IngestItem.intake_id == Intake.intake_id).exists()
    )
    any_archived = (
        select(1)
        .select_from(IngestItem)
        .where(IngestItem.intake_id == Intake.intake_id, hash_archived)
        .exists()
    )
    any_missing = intake_missing_hash_exists(hash_archived=hash_archived)
    return case(
        (~any_relevant, "none"),
        (~any_missing, "complete"),
        (any_archived, "partial"),
        else_="none",
    )


def intake_missing_hash_exists(*, hash_archived: Any | None = None) -> Any:
    """Return whether an intake has at least one hash lacking archive evidence."""

    evidence = hash_archive_evidence_exists() if hash_archived is None else hash_archived
    return (
        select(1)
        .select_from(IngestItem)
        .where(IngestItem.intake_id == Intake.intake_id, ~evidence)
        .exists()
    )


def legacy_archived_expr(*, all_semantics: bool) -> Any:
    """Return the gated SQL predicate behind the legacy ``archived`` boolean."""

    if all_semantics:
        any_relevant = (
            select(1)
            .select_from(IngestItem)
            .where(IngestItem.intake_id == Intake.intake_id)
            .exists()
        )
        any_missing = intake_missing_hash_exists()
        return any_relevant & ~any_missing
    return intake_archive_evidence_exists()


def build_archive_predicate_audit(
    session: Session,
    *,
    generated_at: dt.datetime | None = None,
) -> dict[str, object]:
    """Report retention-passed intakes that the phase-1c flip makes unarchived.

    The audit is deliberately read-only.  Every affected intake is marked for
    the normal archive pipeline; the report provides no grandfathering escape
    hatch because a missing hash may represent a preservation gap.
    """

    retention_filter = Intake.retention_state.in_(_RETENTION_PASSED)
    audited_intakes = int(
        session.scalar(select(func.count()).select_from(Intake).where(retention_filter)) or 0
    )
    candidates = list(
        session.scalars(
            select(Intake)
            .where(
                retention_filter,
                intake_archive_evidence_exists(),
                intake_missing_hash_exists(),
            )
            .order_by(Intake.intake_id)
        )
    )
    candidate_ids = [intake.intake_id for intake in candidates]
    missing_by_intake: dict[str, list[dict[str, object]]] = {
        intake_id: [] for intake_id in candidate_ids
    }
    if candidate_ids:
        missing_rows = session.execute(
            select(
                IngestItem.intake_id,
                IngestItem.logical_asset_hash,
                IngestItem.artifactclass,
                func.count(IngestItem.id).label("occurrence_count"),
            )
            .where(
                IngestItem.intake_id.in_(candidate_ids),
                ~hash_archive_evidence_exists(),
            )
            .group_by(
                IngestItem.intake_id,
                IngestItem.logical_asset_hash,
                IngestItem.artifactclass,
            )
            .order_by(
                IngestItem.intake_id,
                IngestItem.logical_asset_hash,
                IngestItem.artifactclass,
            )
        )
        for intake_id, digest, artifactclass, occurrence_count in missing_rows:
            missing_by_intake[str(intake_id)].append(
                {
                    "content_sha256": digest.hex(),
                    "artifactclass": str(artifactclass),
                    "occurrence_count": int(occurrence_count),
                }
            )

    affected: list[dict[str, Any]] = []
    for intake in candidates:
        missing_assets = missing_by_intake[intake.intake_id]
        affected.append(
            {
                "intake_id": intake.intake_id,
                "retention_state": str(intake.retention_state),
                "released_at": _optional_iso(intake.released_at),
                "staging_deleted_at": _optional_iso(intake.staging_deleted_at),
                "archive_state": "partial",
                "legacy_archived": True,
                "flipped_archived": False,
                "repair_action": "normal_archive_pipeline",
                "missing_assets": missing_assets,
            }
        )

    now = generated_at or dt.datetime.now(dt.UTC)
    distinct_missing = {
        asset["content_sha256"] for row in affected for asset in row["missing_assets"]
    }
    return {
        "schema": ARCHIVE_AUDIT_SCHEMA,
        "generated_at": _iso(now),
        "rollout_gate": {
            "environment_variable": ARCHIVED_ALL_SEMANTICS_ENV,
            "enabled": archived_all_semantics_enabled(),
        },
        "summary": {
            "audited_intakes": audited_intakes,
            "affected_intakes": len(affected),
            "missing_distinct_assets": len(distinct_missing),
            "gate_safe": not affected,
        },
        "affected_intakes": affected,
    }


def _optional_iso(value: dt.datetime | None) -> str | None:
    return None if value is None else _iso(value)


def _iso(value: dt.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")
