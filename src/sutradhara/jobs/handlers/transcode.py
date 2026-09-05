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
from sutradhara.jobs.components import touch_asset, touch_tool
from sutradhara.jobs.reconcilers.conditions import CONDITION_BLOCKED
from sutradhara.jobs.registry import ConditionProjection, JobContext, JobResult, register_handler
from sutradhara.jobs.tool_versions import current_tool_version
from sutradhara.rem_archive_cli import sha256_file
from sutradhara.resource_control import cpu_lease_from_job, resource_role_for_job, run_managed


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
    touch_asset(ctx, source_asset.content_sha256)
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
        tool_version = "1"
        touch_tool(ctx, "fake-transcode", tool_version)
        result = _fake_transcode(source_path, mezz_path, preview_path)
    else:
        tool_version = current_tool_version("ffmpeg")
        touch_tool(ctx, "ffmpeg", tool_version)
        threads = _granted_cpu_threads(ctx)
        result = _run_ffmpeg(
            source_path,
            mezz_path,
            preview_path,
            threads=threads,
            role=resource_role_for_job(ctx.job.kind, ctx.job.params),
        )
    _observe_transcode_result(ctx, result, tool_version=tool_version)

    if result["kind"] == "stderr_pattern":
        # SUSPECT is a validity statement about the asset; a capability match
        # says nothing adverse about the asset, and marking it would deny
        # restore/hdcache admission for intact bytes (diff gate on a5f6fe5).
        if result["bucket"] == "damage":
            record_validity(
                ctx.session,
                asset=source_asset,
                validity=AssetValidity.SUSPECT,
                note=str(result["detail"]),
            )
        blocked_tool = ("ffmpeg", tool_version) if result["bucket"] == "capability" else None
        return JobResult(
            ok=True,
            detail=str(result["detail"]),
            step_state={"transcode": result},
            condition=ConditionProjection(
                condition=CONDITION_BLOCKED,
                reason=f"stderr-pattern:{result['bucket']}",
                message=str(result["detail"]),
                blocked_tool=blocked_tool,
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
    ctx.observe(
        {
            "mezz_sha256": mezz.logical_asset_hash.hex(),
            "preview_sha256": preview.logical_asset_hash.hex(),
        }
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
        result = _stderr_pattern_result(
            "invalid data found via fake transcode marker",
            origin="fake-transcode-marker",
        )
        if result is None:
            raise RuntimeError("decode-failure marker did not produce a classification")
        return result
    source_digest = sha256_file(source).hex()
    mezz.write_bytes(f"fake mezzanine for {source_digest}\n".encode())
    preview.write_bytes(f"fake preview for {source_digest}\n".encode())
    return {"kind": "ok", "detail": "fake transcode completed"}


def _run_ffmpeg(
    source: Path,
    mezz: Path,
    preview: Path,
    *,
    threads: int,
    role: str,
) -> dict[str, Any]:
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
        completed = run_managed(
            cmd,
            role=role,
            cpu_lease=threads,
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
        return {"kind": "ok", "detail": "ffmpeg completed", "exit_status": completed.returncode}
    stderr = completed.stderr or ""
    stderr_pattern_result = _stderr_pattern_result(stderr)
    if stderr_pattern_result is not None:
        stderr_pattern_result["exit_status"] = completed.returncode
        return stderr_pattern_result
    if completed.returncode in {137, -9} or "Cannot allocate memory" in stderr:
        return {
            "kind": "no_proxy",
            "reason": "ffmpeg-resource-failure",
            "detail": "ffmpeg resource failure; no proxy produced",
            "exit_status": completed.returncode,
            "stderr_excerpt": stderr,
        }
    return {
        "kind": "no_proxy",
        "reason": "ffmpeg-operational-failure",
        "detail": f"ffmpeg failed without decode classification: {stderr.strip()}",
        "exit_status": completed.returncode,
        "stderr_excerpt": stderr,
    }


_DAMAGE_STDERR_PATTERNS = (
    "invalid data found",
    "moov atom not found",
    "error while decoding",
    "corrupt",
    "truncated",
)
_CAPABILITY_STDERR_PATTERNS = (
    "unknown decoder",
    "codec not currently supported",
    "could not find codec parameters",
)


def _classify_ffmpeg_stderr(stderr: str) -> tuple[str, str] | None:
    """Return the parking bucket and canonical pattern matched in ffmpeg stderr."""

    lowered = stderr.lower()
    for bucket, patterns in (
        ("damage", _DAMAGE_STDERR_PATTERNS),
        ("capability", _CAPABILITY_STDERR_PATTERNS),
    ):
        for pattern in patterns:
            if pattern in lowered:
                return bucket, pattern
    return None


def _stderr_pattern_result(stderr: str, *, origin: str = "ffmpeg-stderr") -> dict[str, Any] | None:
    """Build the factual job record for one classified stderr value."""

    classification = _classify_ffmpeg_stderr(stderr)
    if classification is None:
        return None
    bucket, matched_pattern = classification
    return {
        "kind": "stderr_pattern",
        "bucket": bucket,
        "matched_pattern": matched_pattern,
        "origin": origin,
        # Verbatim, unmodified — the record is the fact (design Part A).
        "stderr_excerpt": stderr,
        "detail": f"{origin} matched {matched_pattern!r}: {stderr.strip()}",
    }


def _granted_cpu_threads(ctx: JobContext) -> int:
    return cpu_lease_from_job(ctx.granted_leases, ctx.job.required_resources) or 1


def _observe_transcode_result(
    ctx: JobContext,
    result: dict[str, Any],
    *,
    tool_version: str,
) -> None:
    """Append only verbatim tool output and returned scalar values."""

    facts: dict[str, Any] = {"tool_version": tool_version}
    for key in ("exit_status", "stderr_excerpt", "matched_pattern", "origin"):
        if key in result:
            facts[key] = result[key]
    if len(facts) > 1:
        ctx.observe(facts)


def _read_prefix(path: Path, size: int) -> bytes:
    with path.open("rb") as fh:
        return fh.read(size)
