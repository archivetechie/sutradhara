"""Landing-root intake scanner for Phase R.

The scanner is level-triggered over completed intake directories. A completed
front-door receive is a BagIt bag with an `intake.json` sentinel and a `data/`
subtree. Baseline legacy intakes without BagIt tags still register from
`payload/` with no manifest.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import (
    AssetDerivation,
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
)
from sutradhara.jobs.engine import submit
from sutradhara.jobs.models import LIVE_JOB_STATUS_VALUES, Job
from sutradhara.receive import (
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


@dataclass
class IntakeScanOutcome:
    intake_id: str
    path: Path
    status: str
    item_count: int = 0
    jobs_submitted: int = 0
    reason: str | None = None
    manifest_path: Path | None = None
    details: dict[str, Any] = field(default_factory=dict)


def scan_landing_root(
    session: Session,
    landing_root: str | Path,
    *,
    enqueue_jobs: bool = True,
    cache_root: str | Path | None = None,
    proxy_artifactclass: str = "proxy",
    cloud_backend_name: str = "cloud-temp",
    cloud_pool_id: str = "cloud-temp",
) -> list[IntakeScanOutcome]:
    """Scan completed intakes below `landing_root`.

    Caller owns the transaction. This function is idempotent against registered
    intakes and live jobs; terminal job history is not treated as durable
    desired state.
    """

    root = Path(landing_root).resolve()
    outcomes: list[IntakeScanOutcome] = []
    if not root.exists():
        raise FileNotFoundError(root)

    final_cache_root = Path(cache_root).resolve() if cache_root else root / ".sutradhara-cache"
    for intake_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        sentinel = intake_dir / "intake.json"
        if not sentinel.exists():
            continue
        outcomes.append(
            scan_intake(
                session,
                intake_dir,
                enqueue_jobs=enqueue_jobs,
                cache_root=final_cache_root,
                proxy_artifactclass=proxy_artifactclass,
                cloud_backend_name=cloud_backend_name,
                cloud_pool_id=cloud_pool_id,
            )
        )
    return outcomes


def scan_intake(
    session: Session,
    intake_dir: str | Path,
    *,
    enqueue_jobs: bool = True,
    cache_root: str | Path,
    proxy_artifactclass: str = "proxy",
    cloud_backend_name: str = "cloud-temp",
    cloud_pool_id: str = "cloud-temp",
) -> IntakeScanOutcome:
    """Verify and register a single completed intake directory."""

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
    if not is_bag and not payload_root.is_dir():
        raise FileNotFoundError(payload_root)

    intake_id = str(metadata.get("intake_id") or root.name)
    existing = session.get(Intake, intake_id)
    if existing is not None and existing.status == IntakeStatus.QUARANTINED:
        return IntakeScanOutcome(
            intake_id=intake_id,
            path=root,
            status=IntakeStatus.QUARANTINED.value,
            reason="already-quarantined",
            manifest_path=Path(existing.manifest_path) if existing.manifest_path else manifest_path,
        )

    if existing is not None and existing.status == IntakeStatus.REGISTERED:
        submitted = (
            _enqueue_missing_jobs(
                session,
                existing,
                payload_root=payload_root,
                cache_root=Path(cache_root),
                proxy_artifactclass=proxy_artifactclass,
                cloud_backend_name=cloud_backend_name,
                cloud_pool_id=cloud_pool_id,
            )
            if enqueue_jobs
            else 0
        )
        _write_verified_receipt(
            root,
            existing,
            item_count=len(existing.items),
            jobs_submitted=submitted,
        )
        return IntakeScanOutcome(
            intake_id=intake_id,
            path=root,
            status=IntakeStatus.REGISTERED.value,
            item_count=len(existing.items),
            jobs_submitted=submitted,
            reason="already-registered",
            manifest_path=Path(existing.manifest_path) if existing.manifest_path else manifest_path,
        )

    if is_bag:
        validation = validate_bag(root)
        metadata = _intake_metadata_from_labels(validation.metadata, sentinel, root)
        intake_id = str(metadata.get("intake_id") or intake_id)
        if not validation.valid:
            reason = "bag-incomplete" if not validation.complete else "bag-invalid"
            intake = _upsert_intake(
                session,
                intake_id=intake_id,
                metadata=metadata,
                manifest_path=manifest_path,
                status=IntakeStatus.QUARANTINED,
            )
            details = validation.details()
            _write_quarantine_receipt(root, intake, details, reason=reason)
            return IntakeScanOutcome(
                intake_id=intake_id,
                path=root,
                status=IntakeStatus.QUARANTINED.value,
                reason=reason,
                manifest_path=manifest_path,
                details=details,
            )
        records = list(validation.actual_records)
    else:
        records = _hash_payload(payload_root)

    intake = _upsert_intake(
        session,
        intake_id=intake_id,
        metadata=metadata,
        manifest_path=manifest_path,
        status=IntakeStatus.VERIFYING,
    )
    for record in records:
        _register_payload_record(session, intake, payload_root, record)
    intake.status = IntakeStatus.REGISTERED
    intake.registered_at = _utcnow()
    intake.quarantined_at = None
    intake.updated_at = intake.registered_at
    session.flush()

    submitted = (
        _enqueue_missing_jobs(
            session,
            intake,
            payload_root=payload_root,
            cache_root=Path(cache_root),
            proxy_artifactclass=proxy_artifactclass,
            cloud_backend_name=cloud_backend_name,
            cloud_pool_id=cloud_pool_id,
        )
        if enqueue_jobs
        else 0
    )
    _write_verified_receipt(root, intake, item_count=len(records), jobs_submitted=submitted)
    return IntakeScanOutcome(
        intake_id=intake_id,
        path=root,
        status=IntakeStatus.REGISTERED.value,
        item_count=len(records),
        jobs_submitted=submitted,
        manifest_path=manifest_path,
    )


def _enqueue_missing_jobs(
    session: Session,
    intake: Intake,
    *,
    payload_root: Path,
    cache_root: Path,
    proxy_artifactclass: str,
    cloud_backend_name: str,
    cloud_pool_id: str,
) -> int:
    submitted = 0
    cache_root = cache_root.resolve()
    for item in sorted(intake.items, key=lambda r: r.as_received_path):
        if _is_video_path(item.as_received_path):
            if not _source_has_derivations(session, item.id, {"mezz", "preview"}) and _submit_once(
                session,
                "transcode",
                {
                    "ingest_item_id": item.id,
                    "cache_root": str(cache_root),
                    "proxy_artifactclass": proxy_artifactclass,
                },
                dedupe_key=f"transcode:{item.id}",
                resources=[{"pool": "cpu", "count": 8}],
            ):
                submitted += 1
            if not _has_pfr_sidecar(item) and _submit_once(
                session,
                "pfr-index",
                {
                    "ingest_item_id": item.id,
                    "cache_root": str(cache_root),
                },
                dedupe_key=f"pfr-index:{item.id}",
                resources=[{"pool": "io", "count": 1}, {"pool": "cpu", "count": 1}],
            ):
                submitted += 1
    cloud_needed = not _cloud_bundle_has_copy(session, intake.intake_id)
    if cloud_needed and _submit_once(
        session,
        "cloud-blob",
        {
            "intake_id": intake.intake_id,
            "intake_root": str(payload_root.parent.resolve()),
            "payload_root": str(payload_root.resolve()),
            "cache_root": str(cache_root),
            "backend_name": cloud_backend_name,
            "pool_id": cloud_pool_id,
        },
        dedupe_key=f"cloud-blob:{intake.intake_id}",
        resources=[{"pool": "io", "count": 1}],
    ):
        submitted += 1
    return submitted


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


def _cloud_bundle_has_copy(session: Session, intake_id: str) -> bool:
    bundle_id = f"cloud-blob:{intake_id}"
    return (
        session.scalars(select(Copy).where(Copy.bundle_id == bundle_id).limit(1)).one_or_none()
        is not None
    )


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
    now = _utcnow()
    intake = session.get(Intake, intake_id)
    if intake is None:
        intake = Intake(
            intake_id=intake_id,
            operator=str(metadata.get("operator") or os.environ.get("USER") or "unknown"),
            source_kind=source_kind,
            source_ref=_optional_str(metadata.get("source_ref")),
            artifactclass=str(metadata.get("artifactclass") or "default"),
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
        intake.artifactclass = str(metadata.get("artifactclass") or intake.artifactclass)
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
    asset = session.get(LogicalAsset, record.sha256_bytes)
    if asset is None:
        asset = LogicalAsset(
            content_sha256=record.sha256_bytes,
            size_bytes=record.size_bytes,
            media_kind=_media_kind_for_path(record.relpath),
            media_info={"path": record.relpath},
            validity=AssetValidity.UNVALIDATED,
        )
        session.add(asset)
    elif asset.media_kind is None:
        asset.media_kind = _media_kind_for_path(record.relpath)

    item = session.scalars(
        select(IngestItem).where(
            IngestItem.intake_id == intake.intake_id,
            IngestItem.as_received_path == record.relpath,
        )
    ).one_or_none()
    metadata = {
        "source_path": str(record.source_path),
        "payload_root": str(payload_root),
        "sha256": record.sha256_hex,
    }
    if item is None:
        item = IngestItem(
            intake=intake,
            logical_asset=asset,
            as_received_path=record.relpath,
            virtual_path=record.relpath,
            st_dev=record.st_dev,
            st_ino=record.st_ino,
            size_bytes=record.size_bytes,
            artifactclass=intake.artifactclass,
            item_metadata=metadata,
        )
        session.add(item)
    else:
        item.logical_asset = asset
        item.virtual_path = item.virtual_path or record.relpath
        item.st_dev = record.st_dev
        item.st_ino = record.st_ino
        item.size_bytes = record.size_bytes
        item.artifactclass = intake.artifactclass
        item.item_metadata = {**(item.item_metadata or {}), **metadata}
    return item


def _hash_payload(payload_root: Path) -> list[PayloadRecord]:
    return hash_payload_tree(payload_root)


def _media_kind_for_path(path: str) -> MediaKind:
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


def _write_verified_receipt(
    intake_dir: Path,
    intake: Intake,
    *,
    item_count: int,
    jobs_submitted: int,
) -> None:
    payload = {
        "intake_id": intake.intake_id,
        "status": IntakeStatus.REGISTERED.value,
        "registered_at": intake.registered_at.isoformat() if intake.registered_at else None,
        "artifactclass": intake.artifactclass,
        "source_kind": intake.source_kind,
        "manifest_path": intake.manifest_path,
        "item_count": item_count,
        "jobs_submitted": jobs_submitted,
        "release_signal": intake.source_kind == IntakeSourceKind.CARD,
    }
    _write_json(intake_dir / "intake.verified.json", payload)


def _write_quarantine_receipt(
    intake_dir: Path,
    intake: Intake,
    mismatch: dict[str, Any],
    *,
    reason: str = "manifest-mismatch",
) -> None:
    payload = {
        "intake_id": intake.intake_id,
        "status": IntakeStatus.QUARANTINED.value,
        "quarantined_at": intake.quarantined_at.isoformat() if intake.quarantined_at else None,
        "manifest_path": intake.manifest_path,
        "reason": reason,
        "details": mismatch,
    }
    _write_json(intake_dir / "intake.quarantined.json", payload)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
