"""The policy-apply report: what coalescing did, and what it does not imply.

Bundle groups are derived, never declared (design-bundle-groups §2), so an
operator who applies a policy has no document to read back that tells them
which classes now share a crate. This module is that read-back. It is computed
from catalog rows only, after ``apply_artifactclass_policy`` has refreshed its
projections, and it shows per group:

* the fingerprint and the canonical basis (the exact text the fingerprint hashes),
* the member classes and the effective (min-age / max-target) thresholds,
* the pool set with representations,
* **near-miss groups** — cohorts inside the group that declare different
  thresholds. Thresholds are deliberately *not* part of identity; had they
  been, each cohort would have been its own starved group. Naming them keeps
  that split visible rather than invisible.
* the **differing excluded fields** (``restore_preference``, staging and
  hdcache config) — so an operator sees what coalescing does *not* imply,
* the count of bundles carrying ``backfilled`` vs ``derived`` basis,
* the clamp-activation warning, the no-floor-declared warning per pool, and
  the open accumulators that predate the group's current membership.

The report is a value, not a side effect: callers render it, log it, or assert
on it. Thresholds come from :func:`sutradhara.bundle_group.group_thresholds` —
the same computation an accumulator freezes at open — so the report can never
describe arithmetic the runtime does not perform.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.bundle_group import (
    BASIS_SOURCE_BACKFILLED,
    BASIS_SOURCE_DERIVED,
    GroupThresholds,
    canonical_basis_json,
    compute_bundle_group,
    group_thresholds,
)
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    Bundle,
    Pool,
)

# Fields deliberately excluded from group identity that stay per-class forever
# (§2/§5). When members of one group disagree on one of these, the report says
# so — coalescing placement does not coalesce read-side or staging behaviour.
EXCLUDED_FIELDS = ("restore_preference", "staging_config", "hdcache_config")

WARNING_CLAMP_ACTIVE = "generation-floor-clamp"
WARNING_NO_FLOOR_DECLARED = "no-floor-declared"
WARNING_NEAR_MISS = "near-miss-thresholds"
WARNING_MEMBERSHIP_CHANGED = "membership-changed"
WARNING_STALE_PROJECTION = "stale-projection"


@dataclass(frozen=True)
class PoolFact:
    """One basis pool as the report shows it."""

    pool_id: str
    representation: str | None
    min_object_bytes: int | None

    def to_json(self) -> dict[str, Any]:
        return {
            "pool": self.pool_id,
            "representation": self.representation,
            "min_object_bytes": self.min_object_bytes,
        }


@dataclass(frozen=True)
class NearMissCohort:
    """Classes inside one group that declare the same thresholds as each other.

    Two or more cohorts in a group means "identical pools, differing
    thresholds": the group the design chose *not* to split.
    """

    target_bytes: int
    max_age_seconds: int
    artifactclasses: tuple[str, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "target_bytes": self.target_bytes,
            "max_age_seconds": self.max_age_seconds,
            "artifactclasses": list(self.artifactclasses),
        }


@dataclass(frozen=True)
class GroupWarning:
    """One operator-actionable note about a group."""

    kind: str
    message: str

    def to_json(self) -> dict[str, Any]:
        return {"kind": self.kind, "message": self.message}


@dataclass(frozen=True)
class GroupReport:
    """One bundle group, as policy-apply reports it."""

    fingerprint: str
    canonical_basis: tuple[dict[str, Any], ...]
    canonical_basis_json: str
    member_classes: tuple[str, ...]
    pools: tuple[PoolFact, ...]
    effective_target_bytes: int
    effective_max_age_seconds: int
    declared_target_bytes: int
    near_miss_cohorts: tuple[NearMissCohort, ...]
    differing_excluded_fields: dict[str, dict[str, Any]]
    basis_source_counts: dict[str, int]
    open_bundles_predating_change: tuple[str, ...]
    warnings: tuple[GroupWarning, ...] = field(default=())

    @property
    def has_near_miss(self) -> bool:
        """True when the group's classes declare more than one threshold pair."""
        return len(self.near_miss_cohorts) > 1

    def warning_kinds(self) -> tuple[str, ...]:
        return tuple(warning.kind for warning in self.warnings)

    def to_json(self) -> dict[str, Any]:
        return {
            "fingerprint": self.fingerprint,
            "canonical_basis": [dict(entry) for entry in self.canonical_basis],
            "canonical_basis_json": self.canonical_basis_json,
            "member_classes": list(self.member_classes),
            "pools": [pool.to_json() for pool in self.pools],
            "effective": {
                "target_bytes": self.effective_target_bytes,
                "max_age_seconds": self.effective_max_age_seconds,
                "declared_target_bytes": self.declared_target_bytes,
            },
            "near_miss_cohorts": [cohort.to_json() for cohort in self.near_miss_cohorts],
            "differing_excluded_fields": self.differing_excluded_fields,
            "basis_source_counts": self.basis_source_counts,
            "open_bundles_predating_change": list(self.open_bundles_predating_change),
            "warnings": [warning.to_json() for warning in self.warnings],
        }


@dataclass(frozen=True)
class PolicyApplyReport:
    """Every derived bundle group on the estate, after an apply."""

    groups: tuple[GroupReport, ...]
    applied_artifactclass: str | None = None

    def group_of(self, artifactclass: str) -> GroupReport | None:
        """Return the group a class belongs to, or ``None`` if it has no policy."""
        for group in self.groups:
            if artifactclass in group.member_classes:
                return group
        return None

    def group(self, fingerprint: str) -> GroupReport | None:
        for group in self.groups:
            if group.fingerprint == fingerprint:
                return group
        return None

    def to_json(self) -> dict[str, Any]:
        return {
            "applied_artifactclass": self.applied_artifactclass,
            "groups": [group.to_json() for group in self.groups],
        }


def build_policy_apply_report(
    session: Session,
    *,
    applied_artifactclass: str | None = None,
) -> PolicyApplyReport:
    """Build the policy-apply report from catalog rows only.

    Groups are keyed by each class's **live-derived** fingerprint rather than
    its stored projection, so a group whose projection writer was missed still
    reports under the group it actually belongs to — and the discrepancy is
    reported as a warning instead of silently splitting the group in two.
    """
    records = list(session.scalars(select(ArtifactClassPolicyRecord)))
    live: dict[str, tuple[str, list[dict[str, Any]]]] = {}
    by_fingerprint: dict[str, list[ArtifactClassPolicyRecord]] = {}
    for record in records:
        fingerprint, basis = compute_bundle_group(session, record.artifactclass)
        live[record.artifactclass] = (fingerprint, basis)
        by_fingerprint.setdefault(fingerprint, []).append(record)

    bundles_by_group = _bundles_by_group(session)
    pool_rows = {pool.id: pool for pool in session.scalars(select(Pool))}

    groups = [
        _group_report(
            session,
            fingerprint=fingerprint,
            members=sorted(by_fingerprint[fingerprint], key=lambda row: row.artifactclass),
            basis=live[by_fingerprint[fingerprint][0].artifactclass][1],
            pool_rows=pool_rows,
            bundles=bundles_by_group.get(fingerprint, []),
        )
        for fingerprint in sorted(by_fingerprint)
    ]
    return PolicyApplyReport(
        groups=tuple(groups),
        applied_artifactclass=applied_artifactclass,
    )


def _bundles_by_group(session: Session) -> dict[str, list[Bundle]]:
    grouped: dict[str, list[Bundle]] = {}
    for bundle in session.scalars(select(Bundle)):
        grouped.setdefault(bundle.bundle_group, []).append(bundle)
    return grouped


def _group_report(
    session: Session,
    *,
    fingerprint: str,
    members: list[ArtifactClassPolicyRecord],
    basis: list[dict[str, Any]],
    pool_rows: dict[str, Pool],
    bundles: list[Bundle],
) -> GroupReport:
    representative = members[0]
    thresholds = group_thresholds(
        session,
        artifactclass=representative.artifactclass,
        policy=representative,
        fingerprint=fingerprint,
        basis=basis,
    )
    member_classes = tuple(record.artifactclass for record in members)
    pools = tuple(
        PoolFact(
            pool_id=str(entry["pool"]),
            representation=entry.get("representation"),
            min_object_bytes=(
                pool_rows[str(entry["pool"])].min_object_bytes
                if str(entry["pool"]) in pool_rows
                else None
            ),
        )
        for entry in basis
    )
    cohorts = _near_miss_cohorts(members)
    predating = _open_bundles_predating_change(bundles, thresholds)
    return GroupReport(
        fingerprint=fingerprint,
        canonical_basis=tuple(dict(entry) for entry in basis),
        canonical_basis_json=canonical_basis_json(basis),
        member_classes=member_classes,
        pools=pools,
        effective_target_bytes=thresholds.target_bytes,
        effective_max_age_seconds=thresholds.max_age_seconds,
        declared_target_bytes=thresholds.declared_target_bytes,
        near_miss_cohorts=cohorts,
        differing_excluded_fields=_differing_excluded_fields(members),
        basis_source_counts=_basis_source_counts(bundles),
        open_bundles_predating_change=predating,
        warnings=_warnings(
            fingerprint=fingerprint,
            members=members,
            thresholds=thresholds,
            cohorts=cohorts,
            predating=predating,
        ),
    )


def _near_miss_cohorts(members: list[ArtifactClassPolicyRecord]) -> tuple[NearMissCohort, ...]:
    """Partition a group's classes by declared thresholds.

    One cohort = the classes agree. More than one = "identical pools,
    differing thresholds": the near-miss split the design declined to make.
    """
    cohorts: dict[tuple[int, int], list[str]] = {}
    for record in members:
        cohorts.setdefault(
            (record.target_bytes, record.max_age_seconds), []
        ).append(record.artifactclass)
    return tuple(
        NearMissCohort(
            target_bytes=target_bytes,
            max_age_seconds=max_age_seconds,
            artifactclasses=tuple(sorted(classes)),
        )
        for (target_bytes, max_age_seconds), classes in sorted(cohorts.items())
    )


def _differing_excluded_fields(
    members: list[ArtifactClassPolicyRecord],
) -> dict[str, dict[str, Any]]:
    """Report the identity-excluded fields whose values differ across a group.

    A field on which every member agrees is not reported — the point is to
    show what coalescing does *not* imply, and agreement implies nothing.
    """
    differing: dict[str, dict[str, Any]] = {}
    for name in EXCLUDED_FIELDS:
        values = {record.artifactclass: getattr(record, name) for record in members}
        # Compare canonically: these are JSON columns, so key order in the
        # stored dict must not read as a difference.
        rendered = {
            artifactclass: json.dumps(value, sort_keys=True, default=str)
            for artifactclass, value in values.items()
        }
        if len(set(rendered.values())) > 1:
            differing[name] = values
    return differing


def _basis_source_counts(bundles: list[Bundle]) -> dict[str, int]:
    """Count this group's bundles by basis provenance (derived vs backfilled).

    Backfilled bases are the migration's honest guess and are excluded from
    agreement checks (§7.2); the count is how an operator sees how much of the
    estate is still guessed.
    """
    counts = {BASIS_SOURCE_DERIVED: 0, BASIS_SOURCE_BACKFILLED: 0}
    for bundle in bundles:
        source = str((bundle.group_basis or {}).get("basis_source") or "")
        if source in counts:
            counts[source] += 1
    return counts


def _open_bundles_predating_change(
    bundles: list[Bundle],
    thresholds: GroupThresholds,
) -> tuple[str, ...]:
    """Open accumulators whose frozen witness disagrees with the group today.

    A class that joins (or retunes) a group while a bundle is open is honoured
    **from the next bundle** — the open bundle's thresholds are never changed
    (§2). This is how the apply report says so out loud. Backfilled bases are
    skipped: they are a marked guess, excluded from agreement checks (§7.2).
    """
    predating: list[str] = []
    for bundle in bundles:
        if bundle.status != "open":
            continue
        document = bundle.group_basis or {}
        if document.get("basis_source") == BASIS_SOURCE_BACKFILLED:
            continue
        frozen = document.get("effective") or {}
        if (
            frozen.get("target_bytes") != thresholds.target_bytes
            or frozen.get("max_age_seconds") != thresholds.max_age_seconds
        ):
            predating.append(bundle.id)
    return tuple(sorted(predating))


def _warnings(
    *,
    fingerprint: str,
    members: list[ArtifactClassPolicyRecord],
    thresholds: GroupThresholds,
    cohorts: tuple[NearMissCohort, ...],
    predating: tuple[str, ...],
) -> tuple[GroupWarning, ...]:
    warnings: list[GroupWarning] = []
    if thresholds.clamp_active:
        pools = ", ".join(
            f"{pool_id} (floor {thresholds.pool_floors[pool_id]})"
            for pool_id in thresholds.clamping_pools
        )
        warnings.append(
            GroupWarning(
                WARNING_CLAMP_ACTIVE,
                f"declared target {thresholds.declared_target_bytes} sits below a member "
                f"pool floor; clamped up to {thresholds.target_bytes} by {pools}",
            )
        )
    for pool_id in thresholds.floorless_pools:
        warnings.append(
            GroupWarning(
                WARNING_NO_FLOOR_DECLARED,
                f"pool {pool_id} declares no min_object_bytes; no efficiency floor is "
                "enforced for this group (NULL is never an implicit zero)",
            )
        )
    if len(cohorts) > 1:
        detail = "; ".join(
            f"{', '.join(cohort.artifactclasses)} -> target={cohort.target_bytes} "
            f"max_age={cohort.max_age_seconds}"
            for cohort in cohorts
        )
        warnings.append(
            GroupWarning(
                WARNING_NEAR_MISS,
                f"identical pools, differing thresholds ({detail}); the group takes "
                f"max target {thresholds.declared_target_bytes} and min age "
                f"{thresholds.max_age_seconds}",
            )
        )
    if predating:
        warnings.append(
            GroupWarning(
                WARNING_MEMBERSHIP_CHANGED,
                f"open accumulator(s) {', '.join(predating)} were opened under different "
                "effective thresholds; the change is honoured from the next bundle",
            )
        )
    stale = sorted(
        record.artifactclass for record in members if record.bundle_group != fingerprint
    )
    if stale:
        warnings.append(
            GroupWarning(
                WARNING_STALE_PROJECTION,
                f"class(es) {', '.join(stale)} carry a bundle_group projection that "
                "disagrees with their live-derived fingerprint; a fingerprint-input "
                "writer was missed",
            )
        )
    return tuple(warnings)


def render_policy_apply_report(report: PolicyApplyReport) -> str:
    """Render the report as plain text for an operator surface."""
    lines: list[str] = []
    if report.applied_artifactclass is not None:
        lines.append(f"bundle groups after applying {report.applied_artifactclass!r}:")
    else:
        lines.append("bundle groups:")
    for group in report.groups:
        lines.append("")
        lines.append(f"  group {group.fingerprint[:16]}…  ({group.fingerprint})")
        lines.append(f"    basis          {group.canonical_basis_json}")
        lines.append(f"    classes        {', '.join(group.member_classes)}")
        pools = ", ".join(
            f"{pool.pool_id}[{pool.representation or '-'}]"
            + ("" if pool.min_object_bytes is None else f" floor={pool.min_object_bytes}")
            for pool in group.pools
        )
        lines.append(f"    pools          {pools or '(none)'}")
        lines.append(
            f"    effective      target_bytes={group.effective_target_bytes} "
            f"max_age_seconds={group.effective_max_age_seconds} "
            f"(declared target {group.declared_target_bytes})"
        )
        if group.has_near_miss:
            for cohort in group.near_miss_cohorts:
                lines.append(
                    f"    near-miss      {', '.join(cohort.artifactclasses)}: "
                    f"target={cohort.target_bytes} max_age={cohort.max_age_seconds}"
                )
        for name, values in sorted(group.differing_excluded_fields.items()):
            lines.append(f"    differs        {name}:")
            for artifactclass, value in sorted(values.items()):
                lines.append(f"                     {artifactclass} = {value!r}")
        counts = group.basis_source_counts
        lines.append(
            f"    bundles        derived={counts.get(BASIS_SOURCE_DERIVED, 0)} "
            f"backfilled={counts.get(BASIS_SOURCE_BACKFILLED, 0)}"
        )
        for warning in group.warnings:
            lines.append(f"    warning        [{warning.kind}] {warning.message}")
    return "\n".join(lines)
