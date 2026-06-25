"""`pfr-index` job: produce a sidecar index for a video master."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from sutradhara.catalog.facts import record_index, record_validity
from sutradhara.catalog.models import IngestItem, LogicalAsset
from sutradhara.catalog.types import AssetValidity
from sutradhara.jobs.registry import JobContext, JobResult, register_handler


@register_handler("pfr-index")
def handle_pfr_index(ctx: JobContext) -> JobResult:
    params = ctx.job.params
    item_id = params.get("ingest_item_id")
    if not isinstance(item_id, int):
        raise ValueError("pfr-index job requires params.ingest_item_id (int)")
    item = ctx.session.get(IngestItem, item_id)
    if item is None:
        raise ValueError(f"no IngestItem with id={item_id}")
    source = _source_path(item)
    if not source.exists() or not source.is_file():
        return JobResult(
            ok=False,
            detail=f"read error: source path is unavailable: {source}",
            step_state={"pfr_index": {"kind": "read_error", "path": str(source)}},
        )

    cache_root = Path(str(params.get("cache_root") or ".sutradhara-cache")).resolve()
    sidecar_dir = cache_root / "intakes" / item.intake_id / "pfr"
    sidecar_dir.mkdir(parents=True, exist_ok=True)
    sidecar_path = sidecar_dir / f"{item.id}.pfr.json"

    if os.environ.get("SUTRADHARA_FAKE_FFPROBE") == "1":
        probe = {"mode": "fake", "path": str(source), "size_bytes": source.stat().st_size}
    else:
        probe_result = _run_ffprobe(source)
        if probe_result["kind"] == "container_parse_error":
            asset = ctx.session.get(LogicalAsset, item.logical_asset_hash)
            if asset is not None:
                record_validity(
                    ctx.session,
                    asset=asset,
                    validity=AssetValidity.SUSPECT,
                    note=str(probe_result["detail"]),
                )
            return JobResult(
                ok=True,
                detail=str(probe_result["detail"]),
                step_state={"pfr_index": probe_result},
            )
        if probe_result["kind"] == "no_index":
            return JobResult(
                ok=True,
                detail=str(probe_result["detail"]),
                step_state={"pfr_index": probe_result},
            )
        probe = probe_result["probe"]

    sidecar = {
        "kind": "pfr-index-v1",
        "ingest_item_id": item.id,
        "logical_asset_hash": item.logical_asset_hash.hex(),
        "source_path": str(source),
        "probe": probe,
    }
    sidecar_path.write_text(json.dumps(sidecar, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    record_index(
        ctx.session,
        item=item,
        index_kind="pfr-index-v1",
        sidecar_path=sidecar_path,
    )
    return JobResult(
        ok=True,
        detail="pfr sidecar written",
        step_state={
            "pfr_index": {
                "kind": "ok",
                "sidecar_path": str(sidecar_path),
            }
        },
    )


def _run_ffprobe(source: Path) -> dict[str, Any]:
    ffprobe = shutil.which("ffprobe")
    if ffprobe is None:
        return {
            "kind": "no_index",
            "reason": "ffprobe-unavailable",
            "detail": "ffprobe is unavailable; no PFR sidecar produced",
        }
    cmd = [
        ffprobe,
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        str(source),
    ]
    try:
        completed = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=10 * 60,
        )
    except OSError as exc:
        return {
            "kind": "no_index",
            "reason": "ffprobe-exec-error",
            "detail": f"ffprobe could not be started: {exc}",
        }
    except subprocess.TimeoutExpired:
        return {
            "kind": "no_index",
            "reason": "ffprobe-timeout",
            "detail": "ffprobe timed out; no PFR sidecar produced",
        }
    if completed.returncode != 0:
        return {
            "kind": "container_parse_error",
            "detail": f"ffprobe container parse error: {completed.stderr.strip()}",
        }
    try:
        probe = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError as exc:
        return {
            "kind": "container_parse_error",
            "detail": f"ffprobe returned invalid JSON: {exc}",
        }
    return {"kind": "ok", "probe": probe}


def _source_path(item: IngestItem) -> Path:
    raw = (item.item_metadata or {}).get("source_path")
    if not isinstance(raw, str) or not raw:
        raise ValueError(f"ingest_item id={item.id} has no metadata.source_path")
    return Path(raw)
