"""Shared intake archive predicates and the preservation-gap audit.

There is exactly one definition of archive evidence and every reader uses it:
**a logical asset is archived when it sits in a SEALED bundle whose verified
copies meet that member's own class ``min_copies``.**

Two things that used to count no longer do, deliberately (design-bundle-groups
§4, "derived ARCHIVED"):

- **A stored ``Submission.status == archived`` flag is not evidence.** The flag
  is now a projection of this predicate, so reading it back would be circular
  and — worse — a submission that reached ``accumulated`` would have released
  its sources while its material was still sitting in an OPEN bundle.
- **A sealed bundle alone is not evidence.** Sealed with one unverified copy is
  not durability; the member's class declares how many verified copies it takes.

The phase-1c rollout environment gate that once selected between ANY and ALL
semantics, and the legacy ANY predicate behind it, are **deleted** — not
defaulted off: pre-production, no runtime flags, ``git revert`` is the backout.
ALL semantics are the only semantics, which strictly tightens source-erasure
eligibility. (The variable's name is deliberately not written anywhere in the
tree; a test greps for it, so a reintroduction fails loudly.)

Cost, accepted out loud: the evidence predicate carries a correlated COUNT over
``copy`` per candidate bundle. Erasure decisions are rare and the count runs on
indexed columns (``copy.bundle_id``); the alternative — a maintained rollup
column — is a second source of truth for exactly the fact that must not drift.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session, aliased

from sutradhara.api.console import iso_utc
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    Bundle,
    BundleMember,
    Copy,
    IngestItem,
    Intake,
    SubmissionMember,
)
from sutradhara.catalog.types import CopyHealth, RetentionState

ARCHIVE_AUDIT_SCHEMA = "sutradhara.archive-predicate-audit/v2"
_RETENTION_PASSED = (RetentionState.RELEASED.value, RetentionState.PURGED.value)


def verified_bundle_copy_count(bundle_id: Any) -> Any:
    """Return the scalar count of verified, live copies of one bundle.

    "Verified" carries the single meaning ``durability._measurement_filter``
    gives it everywhere else: healthy, not deleted, and measured to its own
    integrity hash. A sealed bundle whose copies were never measured is not
    archive evidence.
    """

    return (
        select(func.count(Copy.id))
        .where(
            Copy.bundle_id == bundle_id,
            Copy.health == CopyHealth.OK,
            Copy.deleted_at.is_(None),
            Copy.last_measured_digest.is_not(None),
            Copy.last_measured_digest == Copy.integrity_hash,
        )
        .scalar_subquery()
    )


def member_archive_evidence(member: Any = BundleMember) -> Any:
    """Return the sealed-and-sufficiently-replicated condition for one member.

    ``member`` is ``BundleMember`` or an alias of it; the caller supplies the
    correlation. A member whose class has no applied policy row has no declared
    ``min_copies`` and therefore no floor it can be shown to meet — the join is
    an inner join on purpose, so such a member is never evidence.
    """

    policy = aliased(ArtifactClassPolicyRecord)
    bundle = aliased(Bundle)
    return (
        select(1)
        .select_from(bundle)
        .join(policy, policy.artifactclass == member.artifactclass)
        .where(
            bundle.id == member.bundle_id,
            bundle.status == "sealed",
            verified_bundle_copy_count(bundle.id) >= policy.min_copies,
        )
        .exists()
    )


def hash_archive_evidence_exists() -> Any:
    """Return archive evidence for the current ``IngestItem`` content hash."""

    member = aliased(BundleMember)
    return (
        select(1)
        .select_from(member)
        .where(
            member.logical_asset_hash == IngestItem.logical_asset_hash,
            member_archive_evidence(member),
        )
        .exists()
    )


def intake_hash_archive_evidence_exists(*, hash_archived: Any | None = None) -> Any:
    """Return whether an intake has any hash with archive evidence."""

    evidence = hash_archive_evidence_exists() if hash_archived is None else hash_archived
    return (
        select(1)
        .select_from(IngestItem)
        .where(IngestItem.intake_id == Intake.intake_id, evidence)
        .exists()
    )


def intake_archive_state_expr() -> Any:
    """Return the archive state using indexed correlated anti-joins."""

    hash_archived = hash_archive_evidence_exists()
    any_relevant = (
        select(1).select_from(IngestItem).where(IngestItem.intake_id == Intake.intake_id).exists()
    )
    any_archived = intake_hash_archive_evidence_exists(hash_archived=hash_archived)
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


def intake_archived_expr() -> Any:
    """Return the compatibility ``archived`` boolean: ALL relevant hashes archived."""

    any_relevant = (
        select(1).select_from(IngestItem).where(IngestItem.intake_id == Intake.intake_id).exists()
    )
    return any_relevant & ~intake_missing_hash_exists()


def submission_is_archived(session: Session, submission_id: str) -> bool:
    """Return whether every member of one submission is archive evidence.

    The strong predicate the deletion path gets. It is member-grain and does
    not care which bundle each member landed in: a submission split across a
    seal boundary is archived when *both* bundles are sealed and sufficiently
    replicated, and not before.
    """

    member = aliased(BundleMember)
    total = session.scalar(
        select(func.count(SubmissionMember.id)).where(
            SubmissionMember.submission_id == submission_id
        )
    )
    if not total:
        return False
    archived = session.scalar(
        select(func.count(SubmissionMember.id))
        .where(
            SubmissionMember.submission_id == submission_id,
            select(1)
            .select_from(member)
            .where(
                member.logical_asset_hash == SubmissionMember.sha256,
                member_archive_evidence(member),
            )
            .exists(),
        )
    )
    return archived == total


def build_archive_predicate_audit(
    session: Session,
    *,
    generated_at: dt.datetime | None = None,
) -> dict[str, object]:
    """Report every retention-passed intake whose hash archive state is partial.

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
                intake_hash_archive_evidence_exists(),
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
        "generated_at": iso_utc(now),
        "summary": {
            "audited_intakes": audited_intakes,
            "affected_intakes": len(affected),
            "missing_distinct_assets": len(distinct_missing),
            "clean": not affected,
        },
        "affected_intakes": affected,
    }


def _optional_iso(value: dt.datetime | None) -> str | None:
    return None if value is None else iso_utc(value)


__all__ = [
    "ARCHIVE_AUDIT_SCHEMA",
    "build_archive_predicate_audit",
    "hash_archive_evidence_exists",
    "intake_archive_state_expr",
    "intake_archived_expr",
    "intake_hash_archive_evidence_exists",
    "intake_missing_hash_exists",
    "member_archive_evidence",
    "submission_is_archived",
    "verified_bundle_copy_count",
]
