"""Enqueue-batch conformance scanning and accumulator routing.

Scanning left the build (design-bundle-groups §4): ``--map`` conflicts with
``--rules``, so the member-grain scan contract lands at enqueue-batch time.
The scan unit is one ``rem archive build --scan-only --rules`` per
(artifactclass, source tree root) — never per single file. Rules match paths
relative to the scan root; a single-file scan sees only the basename, which
silently disarms path-scoped exclusion rules (the round-4 inversion) and
would cost thousands of rem spawns per photo submission.

Verdicts recorded here:

- Exclusion clusters arrive as prefix clusters with no digests, and a
  never-ingested file has no ``LogicalAsset`` row to key to — cluster
  exclusion rows record with ``logical_asset_hash`` NULL, keyed by prefix
  plus the (class, root) scan identity. The per-asset hash column stays
  reserved for exclusions of ingested material.
- ``blob`` verdicts are recorded the same way and route the subtree to the
  cloud-blob funnel path (the subtree is withheld from the group
  accumulator); rule-driven blob-wrapping stays out of scope for group
  flushes. Routing here means exactly that — record the verdict, keep the
  subtree out of the accumulator, and name the funnel in the row and the
  event. It does not mint a blob object: today's ``cloud-blob`` job is
  whole-intake grain (``jobs/handlers/cloud_blob.py`` takes an intake id and
  a payload root, not a prefix), so a subtree-grain funnel job is its own
  piece of machinery and its own prompt.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.archive_bundle import record_exclusion
from sutradhara.archive_fanout import (
    ConformanceScan,
    _normalized_rem_scan_report,
    _scan_from_json,
)
from sutradhara.artifactclass_policy import get_artifactclass_policy
from sutradhara.catalog.models import ArtifactClassPolicyRecord, ExclusionRecord, Intake
from sutradhara.catalog.types import IntakeStatus
from sutradhara.rem_archive_cli import run_rem_archive_scan
from sutradhara.staging import stage_and_enqueue_artifact
from sutradhara.structured_logs import emit_structured_event

BLOB_FUNNEL = "cloud-blob"
_EXCLUDE_REASON = "exclude-rule"
_BLOB_REASON = "blob-rule"


class ArchiveEnqueueError(Exception):
    """Base class for enqueue-batch failures."""


class BatchScanHeld(ArchiveEnqueueError):
    """A compliant-expect batch scan found deviations; nothing was enqueued."""

    def __init__(self, summary: dict[str, Any]) -> None:
        super().__init__("enqueue batch held for conformance review")
        self.summary = summary


@dataclass(frozen=True)
class EnqueueItem:
    """One candidate member of an enqueue batch."""

    logical_asset_hash: bytes
    source_path: Path
    member_path: str  # relative to the scan root, POSIX separators


@dataclass(frozen=True)
class EnqueuedMember:
    """One member the batch actually enqueued."""

    member_path: str  # final recorded name (post naming-ladder)
    bundle_id: str  # the bundle the member landed in (accumulator or funnel)


@dataclass(frozen=True)
class BatchEnqueueResult:
    """Outcome of one (artifactclass, scan root) enqueue batch."""

    artifactclass: str
    scan_root: Path
    enqueued: tuple[EnqueuedMember, ...]
    excluded_prefixes: tuple[str, ...]
    blob_prefixes: tuple[str, ...]


def scan_enqueue_batch(
    session: Session,
    *,
    artifactclass: str,
    policy: ArtifactClassPolicyRecord,
    scan_root: Path | str,
    items: list[EnqueueItem],
    staging_root: Path | str,
    rem_bin: str | Path | None = None,
) -> BatchEnqueueResult:
    """Scan one (class, source tree root) once, then enqueue the batch.

    The scan runs over the TREE ROOT — never over individual files — so
    path-scoped rules see the same relative paths they were written against.
    """
    root = Path(scan_root)
    if not root.is_dir():
        raise ArchiveEnqueueError(f"scan root is not a directory: {root}")
    if not items:
        raise ArchiveEnqueueError(
            f"enqueue batch for {artifactclass!r} under {root} has no items"
        )
    report = run_rem_archive_scan(
        inputs=[root],
        ruleset=policy.ruleset or None,
        rem_bin=rem_bin,
        failure_label="rem archive scan",
    )
    scan = _scan_from_json(_normalized_rem_scan_report(report))
    if policy.expect == "compliant" and scan.has_deviations:
        # Refuse before anything lands. Holding the group accumulator would
        # gate other classes' durability on this class's review latency —
        # exactly what the hold-split exists to avoid.
        raise BatchScanHeld(scan.to_summary())

    excluded_prefixes = _record_verdict_clusters(
        session,
        scan=scan,
        artifactclass=artifactclass,
        ruleset_name=policy.ruleset or None,
        scan_root=root,
        items=items,
        reasons=_exclusion_clusters(scan),
        funnel=None,
    )
    blob_prefixes = _record_verdict_clusters(
        session,
        scan=scan,
        artifactclass=artifactclass,
        ruleset_name=policy.ruleset or None,
        scan_root=root,
        items=items,
        reasons=_blob_clusters(scan),
        funnel=BLOB_FUNNEL,
    )
    skip_prefixes = excluded_prefixes + blob_prefixes

    enqueued: list[EnqueuedMember] = []
    for item in items:
        if any(_prefix_covers(prefix, item.member_path) for prefix in skip_prefixes):
            continue
        staged = stage_and_enqueue_artifact(
            session,
            artifactclass=artifactclass,
            policy=policy,
            source_path=item.source_path,
            staging_root=staging_root,
            member_path=item.member_path,
        )
        if staged.logical_sha256 != item.logical_asset_hash:
            raise ArchiveEnqueueError(
                f"source {item.source_path} hashes to {staged.logical_sha256.hex()}, "
                f"expected {item.logical_asset_hash.hex()}"
            )
        if staged.bundle_id is None:  # pragma: no cover - enqueue always routes
            raise ArchiveEnqueueError(
                f"staging did not report a bundle for {item.member_path!r}"
            )
        enqueued.append(
            EnqueuedMember(
                member_path=staged.stored_member_path,
                bundle_id=staged.bundle_id,
            )
        )
    return BatchEnqueueResult(
        artifactclass=artifactclass,
        scan_root=root,
        enqueued=tuple(enqueued),
        excluded_prefixes=excluded_prefixes,
        blob_prefixes=blob_prefixes,
    )


def enqueue_intake_batch(
    session: Session,
    *,
    intake_id: str,
    staging_root: Path | str | None = None,
    rem_bin: str | Path | None = None,
) -> list[BatchEnqueueResult]:
    """Enqueue a registered intake's items, one scan per (class, tree root).

    The batch anchors on ``(IngestItem.artifactclass,
    Intake.manifest_path.parent/"data")`` — the same root shape the
    submission path derives (``archive_submission._source_root``).
    """
    intake = session.get(Intake, intake_id)
    if intake is None:
        raise ArchiveEnqueueError(f"no intake {intake_id!r}")
    if intake.status != IntakeStatus.REGISTERED:
        raise ArchiveEnqueueError(
            f"intake {intake_id!r} is {intake.status!r}; only registered intakes enqueue"
        )
    if not intake.manifest_path:
        raise ArchiveEnqueueError(
            f"intake {intake_id!r} has no manifest_path; cannot derive the scan root"
        )
    scan_root = (Path(intake.manifest_path).parent / "data").resolve()
    if not scan_root.is_dir():
        raise ArchiveEnqueueError(f"intake {intake_id!r} scan root is missing: {scan_root}")
    resolved_staging = (
        Path(staging_root)
        if staging_root is not None
        else scan_root.parent / ".sutradhara-stage"
    )

    items_by_class: dict[str, list[EnqueueItem]] = {}
    for item in sorted(intake.items, key=lambda entry: entry.as_received_path):
        metadata = item.item_metadata or {}
        stored = metadata.get("stored_member_path")
        member_path = stored if isinstance(stored, str) and stored else item.as_received_path
        items_by_class.setdefault(item.artifactclass, []).append(
            EnqueueItem(
                logical_asset_hash=item.logical_asset_hash,
                source_path=scan_root / member_path,
                member_path=member_path,
            )
        )
    if not items_by_class:
        raise ArchiveEnqueueError(f"intake {intake_id!r} has no ingest items to enqueue")

    results: list[BatchEnqueueResult] = []
    for artifactclass in sorted(items_by_class):
        policy = get_artifactclass_policy(session, artifactclass)
        results.append(
            scan_enqueue_batch(
                session,
                artifactclass=artifactclass,
                policy=policy,
                scan_root=scan_root,
                items=items_by_class[artifactclass],
                staging_root=resolved_staging,
                rem_bin=rem_bin,
            )
        )
    return results


def _exclusion_clusters(scan: ConformanceScan) -> list[tuple[str, str, int, int, tuple[str, ...]]]:
    clusters: dict[str, tuple[str, str, int, int, tuple[str, ...]]] = {}
    for cluster in scan.exclusions:
        clusters[cluster.prefix] = (
            cluster.prefix,
            cluster.reason or _EXCLUDE_REASON,
            cluster.count,
            cluster.bytes_total,
            cluster.samples,
        )
    for cluster in scan.clusters:
        if cluster.reason == _EXCLUDE_REASON and cluster.prefix not in clusters:
            clusters[cluster.prefix] = (
                cluster.prefix,
                cluster.reason,
                cluster.count,
                cluster.bytes_total,
                cluster.samples,
            )
    return [clusters[prefix] for prefix in sorted(clusters)]


def _blob_clusters(scan: ConformanceScan) -> list[tuple[str, str, int, int, tuple[str, ...]]]:
    return [
        (cluster.prefix, cluster.reason, cluster.count, cluster.bytes_total, cluster.samples)
        for cluster in sorted(scan.clusters, key=lambda cluster: cluster.prefix)
        if cluster.reason == _BLOB_REASON
    ]


def _record_verdict_clusters(
    session: Session,
    *,
    scan: ConformanceScan,
    artifactclass: str,
    ruleset_name: str | None,
    scan_root: Path,
    items: list[EnqueueItem],
    reasons: list[tuple[str, str, int, int, tuple[str, ...]]],
    funnel: str | None,
) -> tuple[str, ...]:
    """Record clusters that cover at least one batch item; return their prefixes."""
    recorded: list[str] = []
    for prefix, reason, count, bytes_total, samples in reasons:
        if not any(_prefix_covers(prefix, item.member_path) for item in items):
            continue
        recorded.append(prefix)
        detail: dict[str, Any] = {
            # The scan identity that keys this cluster row, with the prefix.
            "scan_root": str(scan_root),
            "artifactclass": artifactclass,
            "prefix": prefix,
            "samples": list(samples),
        }
        if funnel is not None:
            # Names the destination path, not a job that was created — see the
            # module docstring on why blob routing stops at the record here.
            detail["routed_to"] = funnel
        if _cluster_already_recorded(
            session,
            artifactclass=artifactclass,
            prefix=prefix,
            reason=reason,
            scan_root=scan_root,
        ):
            continue
        record_exclusion(
            session,
            artifactclass=artifactclass,
            reason=reason,
            # Prefix clusters carry no digests and never-ingested files have
            # no LogicalAsset row: logical_asset_hash stays NULL by design.
            logical_asset_hash=None,
            path=prefix,
            count=count,
            bytes_total=bytes_total,
            ruleset_name=ruleset_name,
            detail=detail,
        )
        if funnel is not None:
            emit_structured_event(
                "enqueue_blob_routed",
                artifactclass=artifactclass,
                scan_root=str(scan_root),
                prefix=prefix,
                funnel=funnel,
            )
    return tuple(recorded)


def _cluster_already_recorded(
    session: Session,
    *,
    artifactclass: str,
    prefix: str,
    reason: str,
    scan_root: Path,
) -> bool:
    rows = session.scalars(
        select(ExclusionRecord).where(
            ExclusionRecord.artifactclass == artifactclass,
            ExclusionRecord.path == prefix,
            ExclusionRecord.reason == reason,
            ExclusionRecord.logical_asset_hash.is_(None),
        )
    )
    return any(
        (row.detail or {}).get("scan_root") == str(scan_root) for row in rows
    )


def _prefix_covers(prefix: str, member_path: str) -> bool:
    if not prefix:
        return False
    if member_path == prefix:
        return True
    if prefix.endswith("/"):
        return member_path.startswith(prefix)
    return member_path.startswith(prefix + "/")
