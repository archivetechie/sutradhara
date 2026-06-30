"""Explicit intake lifecycle for landing-root acceptance.

P1.1 splits the old combined landing-root pass into three verbs:
`inspect_intake` validates without writing rows, `register_intake` admits
catalog truth and ensures the cloud-temp stopgap, and `prepare_intake` records a
profile for the derivation reconciler. Callers own transactions.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import uuid
from contextlib import suppress
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import (
    Copy,
    IngestItem,
    Intake,
    LogicalAsset,
)
from sutradhara.catalog.types import (
    AssetValidity,
    IntakeSourceKind,
    IntakeStatus,
    MediaKind,
    RetentionState,
)
from sutradhara.jobs.engine import submit
from sutradhara.jobs.models import LIVE_JOB_STATUS_VALUES, Job
from sutradhara.jobs.reconcilers.profiles import known_profile_names
from sutradhara_receive import (
    BAG_INFO_NAME,
    DATA_DIR_NAME,
    MANIFEST_NAME,
    FileReceipt,
    ReceiveError,
    hash_payload_tree,
    read_bag_info,
    validate_bag,
)

VIDEO_EXTENSIONS = {
    ".3gp",
    ".avi",
    ".m2ts",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".mts",
    ".mxf",
    ".r3d",
    ".wmv",
}
AUDIO_EXTENSIONS = {".aac", ".aif", ".aiff", ".flac", ".m4a", ".mp3", ".wav"}
IMAGE_EXTENSIONS = {
    ".arw",
    ".cr2",
    ".dng",
    ".heic",
    ".jpeg",
    ".jpg",
    ".nef",
    ".png",
    ".tif",
    ".tiff",
}
DOCUMENT_EXTENSIONS = {".csv", ".doc", ".docx", ".json", ".pdf", ".txt", ".xml"}

PayloadRecord = FileReceipt


@dataclass(frozen=True)
class IntakeMarker:
    """Terminal marker payload to publish after the caller commits catalog state."""

    path: Path
    payload: dict[str, Any]


class IntakeDiscrepancyError(ValueError):
    """Raised when an already-registered intake no longer matches its catalog truth."""

    def __init__(
        self,
        message: str,
        *,
        marker: IntakeMarker,
        intake_id: str,
        path: Path,
        reason: str,
        details: dict[str, Any],
    ) -> None:
        super().__init__(message)
        self.marker = marker
        self.intake_id = intake_id
        self.path = path
        self.reason = reason
        self.details = details


@dataclass(frozen=True)
class InspectReport:
    """Read-only validation report for a completed intake directory."""

    intake_id: str
    path: Path
    status: str
    item_count: int = 0
    reason: str | None = None
    manifest_path: Path | None = None
    manifest_digest: str | None = None
    artifactclass: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IntakeRegisterOutcome:
    """Result of explicit catalog acceptance for one intake."""

    intake_id: str
    path: Path
    status: str
    item_count: int = 0
    jobs_submitted: int = 0
    reason: str | None = None
    manifest_path: Path | None = None
    manifest_digest: str | None = None
    artifactclass: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    marker: IntakeMarker | None = None


@dataclass(frozen=True)
class PrepareOutcome:
    """Result of recording and ensuring derivative work for a prepare profile."""

    intake_id: str
    status: str
    profile: str
    jobs_submitted: int = 0
    reason: str | None = None


@dataclass(frozen=True)
class _IntakeContext:
    root: Path
    sentinel: dict[str, Any]
    is_bag: bool
    payload_root: Path
    manifest_path: Path | None
    metadata: dict[str, Any]
    intake_id: str


@dataclass(frozen=True)
class _ValidatedPayload:
    intake_id: str
    metadata: dict[str, Any]
    records: list[PayloadRecord]
    valid: bool
    complete: bool
    manifest_digest: str | None
    details: dict[str, Any]


def inspect_landing_root(
    session: Session,
    landing_root: str | Path,
) -> list[InspectReport]:
    """Inspect every completed intake below a landing root without DB writes."""

    return [inspect_intake(session, path) for path in _iter_intake_dirs(landing_root)]


def inspect_intake(session: Session, intake_dir: str | Path) -> InspectReport:
    """Validate one intake directory and report readiness without mutating catalog rows."""

    ctx = _read_intake_context(intake_dir)
    validated = _validate_payload(ctx)
    existing = session.get(Intake, validated.intake_id)
    artifactclass = _optional_str(validated.metadata.get("artifactclass"))
    if existing is not None and existing.status == IntakeStatus.REGISTERED:
        status = "already-registered"
        reason = status
        item_count = len(existing.items)
    elif existing is not None and existing.status == IntakeStatus.QUARANTINED:
        status = "quarantined"
        reason = status
        item_count = 0
    elif validated.valid:
        status = "ready"
        reason = None
        item_count = len(validated.records)
    elif not validated.complete:
        status = "incomplete"
        reason = "bag-incomplete"
        item_count = 0
    else:
        status = "invalid"
        reason = "bag-invalid"
        item_count = 0
    return InspectReport(
        intake_id=validated.intake_id,
        path=ctx.root,
        status=status,
        item_count=item_count,
        reason=reason,
        manifest_path=ctx.manifest_path,
        manifest_digest=validated.manifest_digest,
        artifactclass=artifactclass,
        details=validated.details,
    )


def register_landing_root(
    session: Session,
    landing_root: str | Path,
    *,
    artifactclass: str | None = None,
    cache_root: str | Path | None = None,
    cloud_backend_name: str = "cloud-temp",
    cloud_pool_id: str = "cloud-temp",
) -> list[IntakeRegisterOutcome]:
    """Register every completed intake below a landing root."""

    root = Path(landing_root).resolve()
    final_cache_root = Path(cache_root).resolve() if cache_root else root / ".sutradhara-cache"
    return [
        register_intake(
            session,
            path,
            artifactclass=artifactclass,
            cache_root=final_cache_root,
            cloud_backend_name=cloud_backend_name,
            cloud_pool_id=cloud_pool_id,
        )
        for path in _iter_intake_dirs(root)
    ]


def register_intake(
    session: Session,
    intake_dir: str | Path,
    *,
    artifactclass: str | None = None,
    cache_root: str | Path | None = None,
    cloud_backend_name: str = "cloud-temp",
    cloud_pool_id: str = "cloud-temp",
) -> IntakeRegisterOutcome:
    """Validate and explicitly admit one intake into the catalog."""

    ctx = _read_intake_context(intake_dir)
    validated = _validate_payload(ctx)
    resolved_class = _resolve_artifactclass(ctx, validated.metadata, artifactclass)
    metadata = {**validated.metadata, "artifactclass": resolved_class}
    existing = session.get(Intake, validated.intake_id)
    final_cache_root = (
        Path(cache_root).resolve() if cache_root else ctx.root.parent / ".sutradhara-cache"
    )

    if existing is not None and existing.status == IntakeStatus.REGISTERED:
        return _handle_registered_intake(
            session,
            ctx,
            validated,
            existing,
            resolved_class=resolved_class,
            cache_root=final_cache_root,
            cloud_backend_name=cloud_backend_name,
            cloud_pool_id=cloud_pool_id,
        )

    if not validated.valid:
        reason = "bag-incomplete" if not validated.complete else "bag-invalid"
        intake = _upsert_intake(
            session,
            intake_id=validated.intake_id,
            metadata=metadata,
            manifest_path=ctx.manifest_path,
            status=IntakeStatus.QUARANTINED,
        )
        intake.manifest_digest = validated.manifest_digest
        session.flush()
        marker = _quarantine_receipt_marker(
            ctx.root,
            intake,
            validated.details,
            reason=reason,
        )
        return IntakeRegisterOutcome(
            intake_id=validated.intake_id,
            path=ctx.root,
            status=IntakeStatus.QUARANTINED.value,
            reason=reason,
            manifest_path=ctx.manifest_path,
            manifest_digest=validated.manifest_digest,
            artifactclass=resolved_class,
            details=validated.details,
            marker=marker,
        )

    intake = _upsert_intake(
        session,
        intake_id=validated.intake_id,
        metadata=metadata,
        manifest_path=ctx.manifest_path,
        status=IntakeStatus.VERIFYING,
    )
    for record in validated.records:
        _register_payload_record(session, intake, ctx.payload_root, record)
    intake.manifest_digest = validated.manifest_digest
    intake.status = IntakeStatus.REGISTERED
    intake.registered_at = _utcnow()
    intake.quarantined_at = None
    intake.updated_at = intake.registered_at
    session.flush()

    submitted = _enqueue_missing_cloud_job(
        session,
        intake,
        payload_root=ctx.payload_root,
        cache_root=final_cache_root,
        cloud_backend_name=cloud_backend_name,
        cloud_pool_id=cloud_pool_id,
    )
    marker = _verified_receipt_marker(
        ctx.root,
        intake,
        item_count=len(validated.records),
        jobs_submitted=submitted,
    )
    return IntakeRegisterOutcome(
        intake_id=validated.intake_id,
        path=ctx.root,
        status=IntakeStatus.REGISTERED.value,
        item_count=len(validated.records),
        jobs_submitted=submitted,
        manifest_path=ctx.manifest_path,
        manifest_digest=validated.manifest_digest,
        artifactclass=resolved_class,
        marker=marker,
    )


def prepare_intake(
    session: Session,
    intake_id: str,
    *,
    profile: str,
) -> PrepareOutcome:
    """Record a prepare profile for the derivation reconciler."""

    profile_names = known_profile_names()
    if profile not in profile_names:
        allowed = ", ".join(sorted(profile_names))
        raise ValueError(f"unknown prepare profile {profile!r}; expected one of: {allowed}")
    intake = session.get(Intake, intake_id)
    if intake is None:
        raise ValueError(f"intake {intake_id!r} is not registered; register first")
    if intake.status != IntakeStatus.REGISTERED:
        raise ValueError(f"intake {intake_id!r} is {intake.status}; prepare requires registered")
    if intake.retention_state in {RetentionState.RELEASED, RetentionState.PURGED}:
        raise ValueError(
            f"intake {intake_id!r} is {intake.retention_state}; "
            "use virtual arrangements for post-archive organizing"
        )
    previous = intake.requested_profile
    intake.requested_profile = profile
    intake.updated_at = _utcnow()
    session.flush()
    reason = "already-prepared" if previous == profile else "profile-recorded"
    return PrepareOutcome(
        intake_id=intake.intake_id,
        status=IntakeStatus.REGISTERED.value,
        profile=profile,
        jobs_submitted=0,
        reason=reason,
    )


def accept_landing_root(
    session: Session,
    landing_root: str | Path,
    *,
    artifactclass: str | None = None,
    prepare_profile: str | None = None,
    cache_root: str | Path | None = None,
    cloud_backend_name: str = "cloud-temp",
    cloud_pool_id: str = "cloud-temp",
) -> list[IntakeRegisterOutcome]:
    """Register every completed intake and optionally prepare registered ones."""

    root = Path(landing_root).resolve()
    final_cache_root = Path(cache_root).resolve() if cache_root else root / ".sutradhara-cache"
    outcomes: list[IntakeRegisterOutcome] = []
    for path in _iter_intake_dirs(root):
        outcome = accept_intake(
            session,
            path,
            artifactclass=artifactclass,
            prepare_profile=prepare_profile,
            cache_root=final_cache_root,
            cloud_backend_name=cloud_backend_name,
            cloud_pool_id=cloud_pool_id,
        )
        outcomes.append(outcome)
    return outcomes


def accept_intake(
    session: Session,
    intake_dir: str | Path,
    *,
    artifactclass: str | None = None,
    prepare_profile: str | None = None,
    cache_root: str | Path | None = None,
    cloud_backend_name: str = "cloud-temp",
    cloud_pool_id: str = "cloud-temp",
) -> IntakeRegisterOutcome:
    """Register one intake and optionally prepare it in the caller's transaction."""

    outcome = register_intake(
        session,
        intake_dir,
        artifactclass=artifactclass,
        cache_root=cache_root,
        cloud_backend_name=cloud_backend_name,
        cloud_pool_id=cloud_pool_id,
    )
    if prepare_profile is None or outcome.status != IntakeStatus.REGISTERED.value:
        return outcome
    prepared = prepare_intake(
        session,
        outcome.intake_id,
        profile=prepare_profile,
    )
    jobs_submitted = outcome.jobs_submitted + prepared.jobs_submitted
    marker = outcome.marker
    intake = session.get(Intake, outcome.intake_id)
    if intake is not None and outcome.status == IntakeStatus.REGISTERED.value:
        marker = _verified_receipt_marker(
            outcome.path,
            intake,
            item_count=outcome.item_count,
            jobs_submitted=jobs_submitted,
        )
    return replace(outcome, jobs_submitted=jobs_submitted, marker=marker)


def _handle_registered_intake(
    session: Session,
    ctx: _IntakeContext,
    validated: _ValidatedPayload,
    existing: Intake,
    *,
    resolved_class: str,
    cache_root: Path,
    cloud_backend_name: str,
    cloud_pool_id: str,
) -> IntakeRegisterOutcome:
    if not validated.valid:
        reason = "registered-intake-invalid"
        details = {
            "reason": reason,
            "validation": validated.details,
            "expected": {
                "manifest_digest": existing.manifest_digest,
                "artifactclass": existing.artifactclass,
            },
            "actual": {
                "manifest_digest": validated.manifest_digest,
                "artifactclass": resolved_class,
            },
        }
        marker = _discrepancy_receipt_marker(ctx.root, existing, details)
        raise IntakeDiscrepancyError(
            f"registered intake {existing.intake_id!r} no longer validates",
            marker=marker,
            intake_id=existing.intake_id,
            path=ctx.root,
            reason=reason,
            details=details,
        )

    same_fingerprint = (
        existing.manifest_digest == validated.manifest_digest
        and existing.artifactclass == resolved_class
    )
    if not same_fingerprint:
        reason = "fingerprint-mismatch"
        details = {
            "reason": reason,
            "expected": {
                "manifest_digest": existing.manifest_digest,
                "artifactclass": existing.artifactclass,
            },
            "actual": {
                "manifest_digest": validated.manifest_digest,
                "artifactclass": resolved_class,
            },
        }
        marker = _discrepancy_receipt_marker(ctx.root, existing, details)
        raise IntakeDiscrepancyError(
            f"registered intake {existing.intake_id!r} fingerprint changed",
            marker=marker,
            intake_id=existing.intake_id,
            path=ctx.root,
            reason=reason,
            details=details,
        )

    submitted = _enqueue_missing_cloud_job(
        session,
        existing,
        payload_root=ctx.payload_root,
        cache_root=cache_root,
        cloud_backend_name=cloud_backend_name,
        cloud_pool_id=cloud_pool_id,
    )
    marker = _verified_receipt_marker(
        ctx.root,
        existing,
        item_count=len(existing.items),
        jobs_submitted=submitted,
    )
    return IntakeRegisterOutcome(
        intake_id=existing.intake_id,
        path=ctx.root,
        status=IntakeStatus.REGISTERED.value,
        item_count=len(existing.items),
        jobs_submitted=submitted,
        reason="already-registered",
        manifest_path=Path(existing.manifest_path) if existing.manifest_path else ctx.manifest_path,
        manifest_digest=existing.manifest_digest,
        artifactclass=existing.artifactclass,
        marker=marker,
    )


def _enqueue_missing_cloud_job(
    session: Session,
    intake: Intake,
    *,
    payload_root: Path,
    cache_root: Path,
    cloud_backend_name: str,
    cloud_pool_id: str,
) -> int:
    if _cloud_bundle_has_copy(session, intake.intake_id):
        return 0
    return int(
        _submit_once(
            session,
            "cloud-blob",
            {
                "intake_id": intake.intake_id,
                "intake_root": str(payload_root.parent.resolve()),
                "payload_root": str(payload_root.resolve()),
                "cache_root": str(cache_root.resolve()),
                "backend_name": cloud_backend_name,
                "pool_id": cloud_pool_id,
            },
            dedupe_key=f"cloud-blob:{intake.intake_id}",
            resources=[{"pool": "io", "count": 1}],
        )
    )


def _submit_once(
    session: Session,
    kind: str,
    params: dict[str, Any],
    *,
    dedupe_key: str,
    resources: list[dict[str, Any]],
) -> bool:
    existing = session.scalars(
        select(Job)
        .where(Job.dedupe_key == dedupe_key, Job.status.in_(LIVE_JOB_STATUS_VALUES))
        .limit(1)
    ).one_or_none()
    if existing is not None:
        return False
    job = submit(
        session,
        kind,
        params,
        required_resources=resources,
        dedupe_key=dedupe_key,
    )
    return job is not existing


def _cloud_bundle_has_copy(session: Session, intake_id: str) -> bool:
    bundle_id = f"cloud-blob:{intake_id}"
    return (
        session.scalars(select(Copy).where(Copy.bundle_id == bundle_id).limit(1)).one_or_none()
        is not None
    )


def _iter_intake_dirs(landing_root: str | Path) -> list[Path]:
    root = Path(landing_root).resolve()
    if not root.exists():
        raise FileNotFoundError(root)
    if (root / "intake.json").exists():
        return [root]
    return sorted(
        path for path in root.iterdir() if path.is_dir() and (path / "intake.json").exists()
    )


def _read_intake_context(intake_dir: str | Path) -> _IntakeContext:
    root = Path(intake_dir).resolve()
    sentinel_path = root / "intake.json"
    if not sentinel_path.exists():
        raise FileNotFoundError(sentinel_path)

    sentinel = _read_json(sentinel_path)
    is_bag = _is_bag_intake(root)
    if is_bag:
        payload_root = root / DATA_DIR_NAME
        manifest_path = root / MANIFEST_NAME
        metadata = _intake_metadata_from_bag(root, sentinel)
    else:
        payload_root = root / "payload"
        manifest_path = None
        metadata = _intake_metadata_from_mapping(sentinel, root)
        if not payload_root.is_dir():
            raise FileNotFoundError(payload_root)
    intake_id = str(metadata.get("intake_id") or root.name)
    return _IntakeContext(
        root=root,
        sentinel=sentinel,
        is_bag=is_bag,
        payload_root=payload_root,
        manifest_path=manifest_path,
        metadata=metadata,
        intake_id=intake_id,
    )


def _validate_payload(ctx: _IntakeContext) -> _ValidatedPayload:
    if ctx.is_bag:
        validation = validate_bag(ctx.root)
        metadata = _intake_metadata_from_labels(validation.metadata, ctx.sentinel, ctx.root)
        digest = (
            _sha256_file(ctx.manifest_path)
            if ctx.manifest_path and ctx.manifest_path.exists()
            else None
        )
        return _ValidatedPayload(
            intake_id=str(metadata.get("intake_id") or ctx.intake_id),
            metadata=metadata,
            records=list(validation.actual_records) if validation.valid else [],
            valid=validation.valid,
            complete=validation.complete,
            manifest_digest=digest,
            details=validation.details(),
        )
    records = _hash_payload(ctx.payload_root)
    return _ValidatedPayload(
        intake_id=ctx.intake_id,
        metadata=ctx.metadata,
        records=records,
        valid=True,
        complete=True,
        manifest_digest=_legacy_manifest_digest(records),
        details={},
    )


def _resolve_artifactclass(
    ctx: _IntakeContext,
    metadata: dict[str, Any],
    requested_artifactclass: str | None,
) -> str:
    cli_class = _optional_str(requested_artifactclass)
    bag_class = _optional_str(metadata.get("artifactclass")) if ctx.is_bag else None
    if bag_class and cli_class and bag_class != cli_class:
        raise ValueError(
            f"artifactclass mismatch: bag has {bag_class!r}, register was given {cli_class!r}"
        )
    resolved = bag_class or cli_class
    if resolved is None:
        raise ValueError("artifactclass is required for intake registration")
    return resolved


def _is_bag_intake(root: Path) -> bool:
    return any((root / name).exists() for name in ("bagit.txt", BAG_INFO_NAME, MANIFEST_NAME))


def _intake_metadata_from_bag(root: Path, sentinel: dict[str, Any]) -> dict[str, Any]:
    try:
        labels = read_bag_info(root / BAG_INFO_NAME)
    except (OSError, ReceiveError, ValueError):
        labels = {}
    return _intake_metadata_from_labels(labels, sentinel, root)


def _intake_metadata_from_labels(
    labels: dict[str, str],
    sentinel: dict[str, Any],
    root: Path,
) -> dict[str, Any]:
    return {
        "intake_id": labels.get("Intake-Id") or sentinel.get("intake_id") or root.name,
        "operator": labels.get("Operator") or sentinel.get("operator"),
        "source_kind": labels.get("Source-Kind") or sentinel.get("source_kind"),
        "source_ref": labels.get("Source-Ref") or sentinel.get("source_ref"),
        "artifactclass": labels.get("Artifactclass") or sentinel.get("artifactclass"),
        "label": labels.get("Label") or sentinel.get("label"),
    }


def _intake_metadata_from_mapping(source: dict[str, Any], root: Path) -> dict[str, Any]:
    return {
        "intake_id": source.get("intake_id") or root.name,
        "operator": source.get("operator"),
        "source_kind": source.get("source_kind"),
        "source_ref": source.get("source_ref"),
        "artifactclass": source.get("artifactclass"),
        "label": source.get("label"),
    }


def _upsert_intake(
    session: Session,
    *,
    intake_id: str,
    metadata: dict[str, Any],
    manifest_path: Path | None,
    status: IntakeStatus,
) -> Intake:
    source_kind = _source_kind(str(metadata.get("source_kind") or IntakeSourceKind.OTHER.value))
    artifactclass = _optional_str(metadata.get("artifactclass"))
    if artifactclass is None:
        raise ValueError("artifactclass is required for intake registration")
    now = _utcnow()
    intake = session.get(Intake, intake_id)
    if intake is None:
        intake = Intake(
            intake_id=intake_id,
            operator=str(metadata.get("operator") or os.environ.get("USER") or "unknown"),
            source_kind=source_kind,
            source_ref=_optional_str(metadata.get("source_ref")),
            artifactclass=artifactclass,
            label=_optional_str(metadata.get("label")),
            manifest_path=str(manifest_path) if manifest_path else None,
            status=status,
            created_at=now,
            updated_at=now,
        )
        session.add(intake)
    else:
        intake.operator = str(metadata.get("operator") or intake.operator)
        intake.source_kind = source_kind
        intake.source_ref = _optional_str(metadata.get("source_ref"))
        intake.artifactclass = artifactclass
        intake.label = _optional_str(metadata.get("label"))
        intake.manifest_path = str(manifest_path) if manifest_path else None
        intake.status = status
        intake.updated_at = now
    if status == IntakeStatus.QUARANTINED:
        intake.quarantined_at = now
        intake.registered_at = None
    return intake


def _register_payload_record(
    session: Session,
    intake: Intake,
    payload_root: Path,
    record: PayloadRecord,
) -> IngestItem:
    as_received_path = record.as_received_relpath
    stored_member_path = record.stored_relpath or record.relpath
    asset = session.get(LogicalAsset, record.sha256_bytes)
    if asset is None:
        asset = LogicalAsset(
            content_sha256=record.sha256_bytes,
            size_bytes=record.size_bytes,
            media_kind=media_kind_for_path(as_received_path),
            media_info={"path": as_received_path, "stored_member_path": stored_member_path},
            validity=AssetValidity.UNVALIDATED,
        )
        session.add(asset)
    elif asset.media_kind is None:
        asset.media_kind = media_kind_for_path(as_received_path)

    item = session.scalars(
        select(IngestItem).where(
            IngestItem.intake_id == intake.intake_id,
            IngestItem.as_received_path == as_received_path,
        )
    ).one_or_none()
    metadata = {
        "source_path": str(record.source_path),
        "payload_root": str(payload_root),
        "sha256": record.sha256_hex,
        "stored_member_path": stored_member_path,
    }
    if record.logical_relpath is not None:
        metadata["logical_member_path"] = record.logical_relpath
    if record.package_profile is not None:
        metadata["package_profile"] = record.package_profile
    if record.package_index is not None:
        metadata["package_index_path"] = str(payload_root.parent / record.package_index)
    if item is None:
        item = IngestItem(
            intake=intake,
            logical_asset=asset,
            as_received_path=as_received_path,
            virtual_path=as_received_path,
            st_dev=record.st_dev,
            st_ino=record.st_ino,
            size_bytes=record.size_bytes,
            artifactclass=intake.artifactclass,
            item_metadata=metadata,
        )
        session.add(item)
    else:
        item.logical_asset = asset
        item.virtual_path = item.virtual_path or as_received_path
        item.st_dev = record.st_dev
        item.st_ino = record.st_ino
        item.size_bytes = record.size_bytes
        item.artifactclass = intake.artifactclass
        item.item_metadata = {**(item.item_metadata or {}), **metadata}
    return item


def _hash_payload(payload_root: Path) -> list[PayloadRecord]:
    return hash_payload_tree(payload_root)


def _legacy_manifest_digest(records: list[PayloadRecord]) -> str:
    payload = [
        (record.relpath, record.sha256_hex) for record in sorted(records, key=lambda r: r.relpath)
    ]
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def media_kind_for_path(path: str) -> MediaKind:
    """Classify an intake occurrence by its as-received path suffix."""

    suffix = Path(path).suffix.lower()
    if suffix in VIDEO_EXTENSIONS:
        return MediaKind.VIDEO
    if suffix in AUDIO_EXTENSIONS:
        return MediaKind.AUDIO
    if suffix in IMAGE_EXTENSIONS:
        return MediaKind.IMAGE
    if suffix in DOCUMENT_EXTENSIONS:
        return MediaKind.DOCUMENT
    return MediaKind.OTHER


_media_kind_for_path = media_kind_for_path


def _is_video_path(path: str) -> bool:
    return Path(path).suffix.lower() in VIDEO_EXTENSIONS


def _source_kind(raw: str) -> IntakeSourceKind:
    try:
        return IntakeSourceKind(raw)
    except ValueError:
        return IntakeSourceKind.OTHER


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return data


def publish_intake_marker(marker: IntakeMarker | None, *, observer: Any | None = None) -> None:
    """Atomically publish a terminal intake marker after the caller commits."""

    if marker is None:
        return
    _atomic_write_json(marker.path, marker.payload, observer=observer)


def publish_intake_markers(outcomes: list[IntakeRegisterOutcome]) -> None:
    """Publish terminal markers from registration outcomes after a batch commit."""

    for outcome in outcomes:
        publish_intake_marker(outcome.marker)


def _verified_receipt_marker(
    intake_dir: Path,
    intake: Intake,
    *,
    item_count: int,
    jobs_submitted: int,
) -> IntakeMarker:
    payload = {
        "intake_id": intake.intake_id,
        "status": IntakeStatus.REGISTERED.value,
        "registered_at": intake.registered_at.isoformat() if intake.registered_at else None,
        "artifactclass": intake.artifactclass,
        "source_kind": intake.source_kind,
        "manifest_path": intake.manifest_path,
        "manifest_digest": intake.manifest_digest,
        "requested_profile": intake.requested_profile,
        "item_count": item_count,
        "jobs_submitted": jobs_submitted,
        "release_signal": intake.source_kind == IntakeSourceKind.CARD,
    }
    return IntakeMarker(path=intake_dir / "intake.verified.json", payload=payload)


def _quarantine_receipt_marker(
    intake_dir: Path,
    intake: Intake,
    mismatch: dict[str, Any],
    *,
    reason: str = "manifest-mismatch",
) -> IntakeMarker:
    payload = {
        "intake_id": intake.intake_id,
        "status": IntakeStatus.QUARANTINED.value,
        "quarantined_at": intake.quarantined_at.isoformat() if intake.quarantined_at else None,
        "manifest_path": intake.manifest_path,
        "manifest_digest": intake.manifest_digest,
        "reason": reason,
        "details": mismatch,
    }
    return IntakeMarker(path=intake_dir / "intake.quarantined.json", payload=payload)


def _discrepancy_receipt_marker(
    intake_dir: Path,
    intake: Intake,
    details: dict[str, Any],
) -> IntakeMarker:
    payload = {
        "intake_id": intake.intake_id,
        "status": "discrepancy",
        "registered_at": intake.registered_at.isoformat() if intake.registered_at else None,
        "reason": "registered-intake-discrepancy",
        "details": details,
    }
    return IntakeMarker(path=intake_dir / "intake.discrepancy.json", payload=payload)


def _atomic_write_json(path: Path, payload: dict[str, Any], *, observer: Any | None = None) -> None:
    text = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    data = text.encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    _fsync_dir(path.parent)
    temp_path = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temp_path.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if observer is not None:
            observer.before_rename(temp_path, path)
        temp_path.replace(path)
        _fsync_dir(path.parent)
    except Exception:
        with suppress(FileNotFoundError):
            temp_path.unlink()
        raise


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
