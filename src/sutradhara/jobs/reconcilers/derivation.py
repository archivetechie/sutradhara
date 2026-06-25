"""Derivation-domain reconciler.

This domain turns a recorded intake prepare profile into desired derivative
facts. It reuses the generic reconciler spine: observations read catalog facts,
and reconciliation enqueues ordinary transcode/index jobs for missing facts.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from sutradhara.catalog.models import AssetDerivation, IngestItem, Intake
from sutradhara.catalog.types import IntakeStatus
from sutradhara.intake import media_kind_for_path
from sutradhara.jobs.config import derivation_cache_root
from sutradhara.jobs.engine import submit
from sutradhara.jobs.reconcilers.conditions import OBSERVED_MISSING, OBSERVED_PRESENT
from sutradhara.jobs.reconcilers.profiles import DerivationEntry, entries_for, entry_for_job
from sutradhara.jobs.reconcilers.registry import Reconciler, TargetObservation, register_reconciler

DOMAIN = "derivation"
TARGET_PREFIX = "derivation"


def make_target_key(source_item_id: int, job_kind: str) -> str:
    """Return the opaque derivation target key for one source item/job kind."""

    if not job_kind:
        raise ValueError("derivation target job_kind must be non-empty")
    return f"{TARGET_PREFIX}:{source_item_id}:{job_kind}"


def parse_target_key(target_key: str) -> tuple[int, str]:
    """Parse ``derivation:{source_item_id}:{job_kind}`` target keys."""

    prefix, sep, rest = target_key.partition(":")
    if prefix != TARGET_PREFIX or not sep:
        raise ValueError(f"derivation target key must start with 'derivation:'; got {target_key!r}")
    item_id_raw, sep, job_kind = rest.partition(":")
    if not sep or not item_id_raw or not job_kind:
        raise ValueError(
            "derivation target key must be derivation:<source_item_id>:<job_kind>; "
            f"got {target_key!r}"
        )
    try:
        item_id = int(item_id_raw)
    except ValueError as exc:
        raise ValueError(f"derivation target key has invalid item id {item_id_raw!r}") from exc
    return item_id, job_kind


def enumerate_targets(
    session: Session,
    cursor: int | None,
    batch: int,
) -> list[TargetObservation]:
    """Enumerate desired derivative targets from a bounded live-ingest batch."""

    observations: list[TargetObservation] = []
    for item in _live_prepared_items(session, cursor=cursor, batch=batch):
        for entry in _matching_entries(item):
            observations.append(_observe_entry(session, item, entry))
    return observations


def observe(session: Session, target_key: str) -> TargetObservation:
    """Observe one concrete ``derivation:{item}:{job}`` target."""

    source_item_id, job_kind = parse_target_key(target_key)
    item = session.get(IngestItem, source_item_id, options=(joinedload(IngestItem.intake),))
    if item is None or item.intake is None:
        return TargetObservation(
            target_key=target_key,
            desired=False,
            observed_state=OBSERVED_MISSING,
        )
    entry = _matching_entry_for_job(item, job_kind)
    if entry is None:
        return TargetObservation(
            target_key=target_key,
            desired=False,
            observed_state=OBSERVED_MISSING,
        )
    return _observe_entry(session, item, entry)


def reconcile_target(session: Session, target_key: str) -> None:
    """Enqueue one derivation job for a missing profile entry."""

    source_item_id, job_kind = parse_target_key(target_key)
    item = session.get(IngestItem, source_item_id, options=(joinedload(IngestItem.intake),))
    if item is None:
        raise ValueError(f"no IngestItem with id={source_item_id}; cannot reconcile {target_key!r}")
    entry = _matching_entry_for_job(item, job_kind)
    if entry is None:
        raise ValueError(f"no matching derivation profile entry for {target_key!r}")

    params = {
        "ingest_item_id": source_item_id,
        "output_class": entry.output_class,
        "cache_root": str(derivation_cache_root()),
        **entry.params,
    }
    submit(
        session,
        entry.job_kind,
        params,
        required_resources=[dict(resource) for resource in entry.resources],
        recon_domain=DOMAIN,
        recon_target_key=target_key,
        dedupe_key=f"{DOMAIN}:{target_key}",
    )


def _live_prepared_items(
    session: Session,
    *,
    cursor: int | None,
    batch: int,
) -> list[IngestItem]:
    query = (
        select(IngestItem)
        .join(Intake, IngestItem.intake_id == Intake.intake_id)
        .options(joinedload(IngestItem.intake))
        .where(
            Intake.status == IntakeStatus.REGISTERED,
            Intake.requested_profile.is_not(None),
        )
        .order_by(IngestItem.id)
        .limit(batch)
    )
    if cursor is not None:
        query = query.where(IngestItem.id > cursor)
    return list(session.scalars(query))


def _matching_entries(item: IngestItem) -> tuple[DerivationEntry, ...]:
    intake = item.intake
    if intake is None or intake.status != IntakeStatus.REGISTERED:
        return ()
    media_kind = media_kind_for_path(item.as_received_path)
    return entries_for(item.artifactclass, intake.requested_profile, media_kind)


def _matching_entry_for_job(item: IngestItem, job_kind: str) -> DerivationEntry | None:
    intake = item.intake
    if intake is None or intake.status != IntakeStatus.REGISTERED:
        return None
    media_kind = media_kind_for_path(item.as_received_path)
    return entry_for_job(item.artifactclass, intake.requested_profile, media_kind, job_kind)


def _observe_entry(session: Session, item: IngestItem, entry: DerivationEntry) -> TargetObservation:
    return TargetObservation(
        target_key=make_target_key(item.id, entry.job_kind),
        desired=True,
        observed_state=(
            OBSERVED_PRESENT if _entry_facts_present(session, item, entry) else OBSERVED_MISSING
        ),
    )


def _entry_facts_present(session: Session, item: IngestItem, entry: DerivationEntry) -> bool:
    derivation_kinds = {fact.kind for fact in entry.produces if fact.fact_type == "derivation"}
    if derivation_kinds and not _source_has_derivations(session, item.id, derivation_kinds):
        return False

    index_kinds = {fact.kind for fact in entry.produces if fact.fact_type == "index"}
    if index_kinds:
        if index_kinds != {"pfr-index-v1"}:
            raise ValueError(
                "only pfr-index-v1 sidecar observation is supported before typed sidecars; "
                f"got {sorted(index_kinds)!r}"
            )
        if not _has_pfr_sidecar(item):
            return False
    return True


def _source_has_derivations(session: Session, item_id: int, kinds: set[str]) -> bool:
    rows = session.scalars(
        select(AssetDerivation.kind).where(
            AssetDerivation.source_item_id == item_id,
            AssetDerivation.kind.in_(kinds),
        )
    ).all()
    return kinds.issubset(set(rows))


def _has_pfr_sidecar(item: IngestItem) -> bool:
    path = item.item_metadata.get("pfr_sidecar_path") if item.item_metadata else None
    return isinstance(path, str) and Path(path).exists()


register_reconciler(DOMAIN)(
    Reconciler(
        enumerate_targets=enumerate_targets,
        observe=observe,
        reconcile_target=reconcile_target,
    )
)
