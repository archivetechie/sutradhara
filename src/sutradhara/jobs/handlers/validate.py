"""`validate` job: decode/parse validation for a logical asset.

Validation is a content-level condition signal. The bytes are still archived;
decode-invalid content is flagged on ``LogicalAsset.validity`` and normal restore
is gated until an operator requests force restore.
"""

from __future__ import annotations

import json
from pathlib import Path

from sutradhara.catalog.facts import record_validity
from sutradhara.catalog.models import LogicalAsset
from sutradhara.catalog.types import AssetValidity, is_content_hash
from sutradhara.jobs.registry import JobContext, JobResult, register_handler


@register_handler("validate")
def handle_validate(ctx: JobContext) -> JobResult:
    params = ctx.job.params
    raw_hash = params.get("asset_hash")
    if not isinstance(raw_hash, str):
        raise ValueError("validate job requires params.asset_hash hex string")
    try:
        asset_hash = bytes.fromhex(raw_hash)
    except ValueError as exc:
        raise ValueError("validate params.asset_hash must be hex") from exc
    if not is_content_hash(asset_hash):
        raise ValueError("validate params.asset_hash must be a SHA-256 hash")

    path_raw = params.get("path")
    if not isinstance(path_raw, str) or not path_raw:
        raise ValueError("validate job requires params.path")
    validator = params.get("validator", "utf-8")
    if validator not in {"utf-8", "json"}:
        raise ValueError("validate params.validator must be 'utf-8' or 'json'")

    asset = ctx.session.get(LogicalAsset, asset_hash)
    if asset is None:
        raise ValueError(f"no LogicalAsset with content hash {raw_hash}")

    path = Path(path_raw)
    try:
        data = path.read_bytes()
    except OSError as exc:
        return JobResult(
            ok=False,
            detail=f"read error: {exc}",
            step_state={"validate": {"kind": "read_error", "path": str(path)}},
        )

    try:
        text = data.decode("utf-8")
        if validator == "json":
            json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        note = f"decode error via {validator}: {exc}"
        record_validity(ctx.session, asset=asset, validity=AssetValidity.SUSPECT, note=note)
        return JobResult(
            ok=True,
            detail=note,
            step_state={
                "validate": {
                    "kind": "decode_error",
                    "validator": validator,
                    "validity": AssetValidity.SUSPECT.value,
                }
            },
        )

    record_validity(
        ctx.session, asset=asset, validity=AssetValidity.OK, note=f"validated via {validator}"
    )
    return JobResult(
        ok=True,
        detail="validated ok",
        step_state={
            "validate": {
                "kind": "ok",
                "validator": validator,
                "validity": AssetValidity.OK.value,
            }
        },
    )
