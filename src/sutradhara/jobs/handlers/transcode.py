"""`transcode` job: derive mezzanine and preview proxies for one video item."""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from sqlalchemy import select

from sutradhara.catalog.models import AssetDerivation, IngestItem, LogicalAsset
from sutradhara.catalog.types import AssetValidity, MediaKind
from sutradhara.jobs.registry import JobContext, JobResult, register_handler


@register_handler("transcode")
def handle_transcode(ctx: JobContext) -> JobResult:
    params = ctx.job.params
    item_id = params.get("ingest_item_id")
    if not isinstance(item_id, int):
        raise ValueError("transcode job requires params.ingest_item_id (int)")
    item = ctx.session.get(IngestItem, item_id)
    if item is None:
        raise ValueError(f"no IngestItem with id={item_id}")

    source_path = _source_path(item)
    source_asset = ctx.session.get(LogicalAsset, item.logical_asset_hash)
    if source_asset is None:
        raise ValueError(f"no LogicalAsset for ingest_item id={item_id}")
    if not source_path.exists() or not source_path.is_file():
        return JobResult(
            ok=False,
            detail=f"read error: source path is unavailable: {source_path}",
            step_state={"transcode": {"kind": "read_error", "path": str(source_path)}},
        )
    try:
        source_path.open("rb").close()
    except OSError as exc:
        return JobResult(
            ok=False,
            detail=f"read error: {exc}",
            step_state={"transcode": {"kind": "read_error", "path": str(source_path)}},
        )

    cache_root = Path(str(params.get("cache_root") or ".sutradhara-cache")).resolve()
    output_dir = cache_root / "intakes" / item.intake_id / "derivatives" / str(item.id)
    output_dir.mkdir(parents=True, exist_ok=True)
    mezz_path = output_dir / "mezz.mp4"
    preview_path = output_dir / "preview.mp4"

    if os.environ.get("SUTRADHARA_FAKE_TRANSCODE") == "1":
        result = _fake_transcode(source_path, mezz_path, preview_path)
    else:
        result = _run_ffmpeg(
            source_path,
            mezz_path,
            preview_path,
            threads=_granted_cpu_threads(ctx),
        )

    if result["kind"] == "decode_error":
        source_asset.validity = AssetValidity.SUSPECT
        source_asset.validity_note = str(result["detail"])
        return JobResult(
            ok=True,
            detail=str(result["detail"]),
            step_state={"transcode": result},
        )
    if result["kind"] == "no_proxy":
        return JobResult(
            ok=True,
            detail=str(result["detail"]),
            step_state={"transcode": result},
        )
    if result["kind"] != "ok":
        return JobResult(
            ok=False,
            detail=str(result["detail"]),
            step_state={"transcode": result},
        )

    proxy_artifactclass = str(params.get("proxy_artifactclass") or item.artifactclass)
    mezz = _register_derived_item(
        ctx,
        source_item=item,
        output_path=mezz_path,
        relpath=f"derived/{item.id}/mezz.mp4",
        kind="mezz",
        artifactclass=proxy_artifactclass,
        media_kind=MediaKind.VIDEO,
    )
    preview = _register_derived_item(
        ctx,
        source_item=item,
        output_path=preview_path,
        relpath=f"derived/{item.id}/preview.mp4",
        kind="preview",
        artifactclass=proxy_artifactclass,
        media_kind=MediaKind.VIDEO,
    )
    source_asset.validity = AssetValidity.OK
    source_asset.validity_note = "transcode completed"
    return JobResult(
        ok=True,
        detail="transcode outputs registered",
        step_state={
            "transcode": {
                "kind": "ok",
                "threads": _granted_cpu_threads(ctx),
                "mezz_item_id": mezz.id,
                "preview_item_id": preview.id,
                "mezz_path": str(mezz_path),
                "preview_path": str(preview_path),
            }
        },
    )


def _source_path(item: IngestItem) -> Path:
    raw = (item.item_metadata or {}).get("source_path")
    if not isinstance(raw, str) or not raw:
        raise ValueError(f"ingest_item id={item.id} has no metadata.source_path")
    return Path(raw)


def _fake_transcode(source: Path, mezz: Path, preview: Path) -> dict[str, Any]:
    marker = _read_prefix(source, 64)
    if marker.startswith(b"DECODE_FAIL"):
        return {
            "kind": "decode_error",
            "detail": "decode error via fake transcode marker",
        }
    source_digest = _sha256_file(source).hex()
    mezz.write_bytes(f"fake mezzanine for {source_digest}\n".encode())
    preview.write_bytes(f"fake preview for {source_digest}\n".encode())
    return {"kind": "ok", "detail": "fake transcode completed"}


def _run_ffmpeg(source: Path, mezz: Path, preview: Path, *, threads: int) -> dict[str, Any]:
    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg is None:
        return {
            "kind": "no_proxy",
            "reason": "ffmpeg-unavailable",
            "detail": "ffmpeg is unavailable; no proxy produced",
        }
    cmd = [
        ffmpeg,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-threads",
        str(threads),
        "-i",
        str(source),
        "-map",
        "0",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "20",
        "-c:a",
        "aac",
        str(mezz),
        "-map",
        "0:v:0",
        "-vf",
        "scale=-2:360",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "28",
        "-an",
        str(preview),
    ]
    try:
        completed = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=60 * 60,
        )
    except OSError as exc:
        return {
            "kind": "no_proxy",
            "reason": "ffmpeg-exec-error",
            "detail": f"ffmpeg could not be started: {exc}",
        }
    except subprocess.TimeoutExpired:
        return {
            "kind": "no_proxy",
            "reason": "ffmpeg-timeout",
            "detail": "ffmpeg timed out; no proxy produced",
        }
    if completed.returncode == 0 and mezz.exists() and preview.exists():
        return {"kind": "ok", "detail": "ffmpeg completed"}
    stderr = completed.stderr or ""
    if _stderr_is_decode_error(stderr):
        return {
            "kind": "decode_error",
            "detail": f"decode error via ffmpeg: {stderr.strip() or completed.returncode}",
        }
    if completed.returncode in {137, -9} or "Cannot allocate memory" in stderr:
        return {
            "kind": "no_proxy",
            "reason": "ffmpeg-resource-failure",
            "detail": "ffmpeg resource failure; no proxy produced",
        }
    return {
        "kind": "no_proxy",
        "reason": "ffmpeg-operational-failure",
        "detail": f"ffmpeg failed without decode classification: {stderr.strip()}",
    }


def _stderr_is_decode_error(stderr: str) -> bool:
    lowered = stderr.lower()
    needles = (
        "invalid data found",
        "moov atom not found",
        "error while decoding",
        "corrupt",
        "could not find codec parameters",
    )
    return any(needle in lowered for needle in needles)


def _register_derived_item(
    ctx: JobContext,
    *,
    source_item: IngestItem,
    output_path: Path,
    relpath: str,
    kind: str,
    artifactclass: str,
    media_kind: MediaKind,
) -> IngestItem:
    if not output_path.exists() or output_path.stat().st_size <= 0:
        raise ValueError(f"transcode output missing or empty: {output_path}")
    digest = _sha256_file(output_path)
    asset = ctx.session.get(LogicalAsset, digest)
    if asset is None:
        asset = LogicalAsset(
            content_sha256=digest,
            size_bytes=output_path.stat().st_size,
            media_kind=media_kind,
            media_info={"derived_from_item_id": source_item.id, "kind": kind},
            validity=AssetValidity.UNVALIDATED,
        )
        ctx.session.add(asset)

    item = ctx.session.scalars(
        select(IngestItem).where(
            IngestItem.intake_id == source_item.intake_id,
            IngestItem.as_received_path == relpath,
        )
    ).one_or_none()
    stat = output_path.stat()
    metadata = {
        "source_path": str(output_path),
        "generated_by": "transcode",
        "source_item_id": source_item.id,
        "kind": kind,
    }
    if item is None:
        item = IngestItem(
            intake_id=source_item.intake_id,
            logical_asset_hash=digest,
            as_received_path=relpath,
            virtual_path=relpath,
            st_dev=getattr(stat, "st_dev", None),
            st_ino=getattr(stat, "st_ino", None),
            size_bytes=stat.st_size,
            artifactclass=artifactclass,
            item_metadata=metadata,
        )
        ctx.session.add(item)
        ctx.session.flush()
    else:
        item.logical_asset_hash = digest
        item.st_dev = getattr(stat, "st_dev", None)
        item.st_ino = getattr(stat, "st_ino", None)
        item.size_bytes = stat.st_size
        item.artifactclass = artifactclass
        item.item_metadata = {**(item.item_metadata or {}), **metadata}

    edge = ctx.session.scalars(
        select(AssetDerivation).where(
            AssetDerivation.derived_item_id == item.id,
            AssetDerivation.source_item_id == source_item.id,
            AssetDerivation.kind == kind,
        )
    ).one_or_none()
    if edge is None:
        ctx.session.add(
            AssetDerivation(
                derived_item_id=item.id,
                source_item_id=source_item.id,
                kind=kind,
            )
        )
    return item


def _granted_cpu_threads(ctx: JobContext) -> int:
    raw = ctx.granted_leases.get("cpu")
    if raw is None:
        for resource in ctx.job.required_resources or []:
            if resource.get("pool") == "cpu":
                raw = resource.get("count")
                break
    try:
        return max(1, int(raw or 1))
    except (TypeError, ValueError):
        return 1


def _read_prefix(path: Path, size: int) -> bytes:
    with path.open("rb") as fh:
        return fh.read(size)


def _sha256_file(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()
