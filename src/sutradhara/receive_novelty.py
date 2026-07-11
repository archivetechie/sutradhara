"""Shared projections for receive content novelty.

The registration path owns authoritative server-hash dispositions. This module
keeps their REST summary and the pre-transfer path/size estimate consistent.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import ArtifactClassPolicyRecord, IngestItem, Intake
from sutradhara.catalog.types import IngestDisposition, IntakeStatus
from sutradhara.durability import AssetTarget, placement_status
from sutradhara.replication import ReplicationPolicyMissing


@dataclass(frozen=True)
class ListingEntry:
    """One current card file used only by the path/size heuristic."""

    path: str
    size_bytes: int


def novelty_summary(session: Session, intake_id: str) -> dict[str, int]:
    """Return the normative immutable disposition counts for one intake."""

    return novelty_summaries(session, [intake_id])[intake_id]


def novelty_summaries(
    session: Session, intake_ids: list[str]
) -> dict[str, dict[str, int]]:
    """Batch disposition counts without adding an intake-list N+1 query."""

    counts_by_intake: dict[str, Counter[str]] = {
        intake_id: Counter() for intake_id in intake_ids
    }
    if intake_ids:
        for intake_id, disposition in session.execute(
            select(IngestItem.intake_id, IngestItem.disposition).where(
                IngestItem.intake_id.in_(intake_ids)
            )
        ):
            counts_by_intake[str(intake_id)][str(disposition)] += 1
    return {
        intake_id: _novelty_counts(count)
        for intake_id, count in counts_by_intake.items()
    }


def _novelty_counts(counts: Counter[str]) -> dict[str, int]:
    """Shape one disposition counter as the phase-2 wire object."""

    return {
        "total": sum(counts.values()),
        "new": counts[IngestDisposition.NEW.value],
        "known_durable": counts[IngestDisposition.KNOWN_DURABLE.value],
        "known_under_durable": counts[IngestDisposition.KNOWN_UNDER_DURABLE.value],
        "reverified": counts[IngestDisposition.REVERIFIED.value],
        "legacy_unknown": counts[IngestDisposition.LEGACY_UNKNOWN.value],
    }


def asset_policy_qualified_durable(session: Session, item: IngestItem) -> bool:
    """Return whether an item's asset currently satisfies its durability policy."""

    if session.get(ArtifactClassPolicyRecord, item.artifactclass) is None:
        return False
    try:
        status = placement_status(
            session,
            AssetTarget(item.logical_asset_hash, item.artifactclass),
            require_verified=True,
        )
    except ReplicationPolicyMissing:
        return False
    return status["complete"] and bool(status["want"])


def work_suppression_safe(session: Session, item: IngestItem) -> bool:
    """Re-check I2 live durability before suppressing work for an occurrence."""

    return (
        item.disposition == IngestDisposition.KNOWN_DURABLE
        and asset_policy_qualified_durable(session, item)
    )


def estimate_listing_novelty(
    session: Session,
    *,
    card_identity: str,
    requester: str,
    listing: list[ListingEntry],
    listing_complete: bool,
) -> dict[str, object]:
    """Compare a current listing with the newest verified receive of this card."""

    prior = session.scalars(
        select(Intake)
        .where(
            Intake.card_id == card_identity,
            Intake.status == IntakeStatus.REGISTERED,
        )
        .order_by(Intake.registered_at.desc(), Intake.created_at.desc(), Intake.intake_id.desc())
        .limit(1)
    ).one_or_none()
    prior_entries: set[tuple[str, int]] = set()
    if prior is not None:
        prior_entries = {
            (item.as_received_path, item.size_bytes)
            for item in session.scalars(
                select(IngestItem).where(IngestItem.intake_id == prior.intake_id)
            )
            if asset_policy_qualified_durable(session, item)
        }
    match_prior = sum(
        (entry.path, entry.size_bytes) in prior_entries for entry in listing
    )
    likely_new = len(listing) - match_prior
    visible = prior is not None and prior.operator == requester
    estimate: dict[str, object] = {
        "listing_files": len(listing),
        "match_prior": match_prior,
        "likely_new": likely_new,
        "all_known_estimate": (
            prior is not None and listing_complete and likely_new == 0
        ),
        "visible": visible,
    }
    if visible and prior is not None:
        estimate["prior_intake_id"] = prior.intake_id
    return estimate
