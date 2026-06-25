"""`transcode` job: derive mezzanine and preview proxies for one video item."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from sutradhara.catalog.facts import record_derivation, record_validity
from sutradhara.catalog.models import IngestItem, LogicalAsset
from sutradhara.catalog.types import AssetValidity, MediaKind
from sutradhara.jobs.reconcilers.conditions import CONDITION_BLOCKED
from sutradhara.jobs.registry import ConditionProjection, JobContext, JobResult, register_handler
from sutradhara.rem_archive_cli import sha256_file


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

    output_class = params.get("output_class")
    if not isinstance(output_class, str) or not output_class:
        raise ValueError("transcode job requires params.output_class (non-empty str)")

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
        record_validity(
            ctx.session,
            asset=source_asset,
            validity=AssetValidity.SUSPECT,
            note=str(result["detail"]),
        )
        return JobResult(
            ok=True,
            detail=str(result["detail"]),
            step_state={"transcode": result},
            condition=ConditionProjection(
                condition=CONDITION_BLOCKED,
                reason="unsupported-source",
                message=str(result["detail"]),
                blocked_tool=("ffmpeg", _tool_version("ffmpeg")),
            ),
        )
    if result["kind"] == "no_proxy":
        return JobResult(
            ok=False,
            detail=str(result["detail"]),
            step_state={"transcode": result},
        )
    if result["kind"] != "ok":
        return JobResult(
            ok=False,
            detail=str(result["detail"]),
            step_state={"transcode": result},
        )

    mezz = record_derivation(
        ctx.session,
        source_item=item,
        output_path=mezz_path,
        relpath=f"derived/{item.id}/mezz.mp4",
        kind="mezz",
        artifactclass=output_class,
        media_kind=MediaKind.VIDEO,
        generated_by="transcode",
    )
    preview = record_derivation(
        ctx.session,
        source_item=item,
        output_path=preview_path,
        relpath=f"derived/{item.id}/preview.mp4",
        kind="preview",
        artifactclass=output_class,
        media_kind=MediaKind.VIDEO,
        generated_by="transcode",
    )
    record_validity(
        ctx.session,
        asset=source_asset,
        validity=AssetValidity.OK,
        note="transcode completed",
    )
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
    source_digest = sha256_file(source).hex()
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


def _tool_version(tool: str) -> str:
    path = shutil.which(tool)
    if path is None:
        return "unknown"
    try:
        completed = subprocess.run(
            [path, "-version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "unknown"
    lines = (completed.stdout or completed.stderr or "").splitlines()
    return lines[0][:128] if lines else "unknown"


def _read_prefix(path: Path, size: int) -> bytes:
    with path.open("rb") as fh:
        return fh.read(size)
