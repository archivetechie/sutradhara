"""`pfr-index` job: produce a real pfr_core sidecar for a video master."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

from pfr_core import PFRSidecar, make_fallback_sidecar
from pfr_core.failure import ReasonId, ScrapeFailure
from pfr_core.source import LocalFile
from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.facts import record_index, record_validity
from sutradhara.catalog.models import IngestItem, LogicalAsset
from sutradhara.catalog.types import AssetValidity
from sutradhara.jobs.models import ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import CONDITION_BACKOFF, CONDITION_BLOCKED
from sutradhara.jobs.registry import ConditionProjection, JobContext, JobResult, register_handler
from sutradhara.pfr import (
    PFR_INDEX_KIND,
    PFR_RECIPE_METADATA_KEY,
    atomic_write_sidecar,
    enforce_blob_lru,
    pfr_core_version,
    scrape_path_isolated_120,
    sidecar_blobs_complete,
    sidecar_with_catalog_identity,
)
from sutradhara.resource_control import cpu_lease_from_job, resource_role_for_job

_RETRYABLE_REASON_IDS = frozenset(
    {
        ReasonId.SOURCE_CHANGED,
        ReasonId.BUDGET_EXCEEDED,
        ReasonId.BUDGET_EXHAUSTED,
        ReasonId.EXCEPTION,
    }
)
_FALLBACK_REASON_IDS = frozenset(
    {
        ReasonId.CAP_EXCEEDED_FALLBACK,
        ReasonId.OP_ATOM_UNSUPPORTED,
        ReasonId.PLUGIN_MISSING,
        ReasonId.FALLBACK,
    }
)
_PARSE_DETERMINATION_REASON_IDS = frozenset({ReasonId.INDEX_UNAVAILABLE})
_LOUD_STOP_REASON_IDS = frozenset(
    {
        ReasonId.BUNDLE_NOT_ADDRESSABLE,
        ReasonId.REWRAP_NOT_DEPLOYED,
        ReasonId.UNSUPPORTED_TIME_BASIS,
        ReasonId.RIP_MISMATCH,
        ReasonId.GOP_REWRAP_UNSUPPORTED,
        ReasonId.SIDECAR_SOURCE_MISMATCH,
    }
)


@register_handler("pfr-index")
def handle_pfr_index(ctx: JobContext) -> JobResult:
    """Scrape an ingest source into a pfr_core sidecar and record its pointer."""

    params = ctx.job.params
    item_id = params.get("ingest_item_id")
    if not isinstance(item_id, int):
        raise ValueError("pfr-index job requires params.ingest_item_id (int)")
    item = ctx.session.get(IngestItem, item_id)
    if item is None:
        raise ValueError(f"no IngestItem with id={item_id}")
    source = _source_path(item)
    if not source.exists() or not source.is_file():
        detail = f"read error: source path is unavailable: {source}"
        return JobResult(
            ok=False,
            detail=detail,
            step_state={"pfr_index": {"kind": "read_error", "path": str(source)}},
            condition=ConditionProjection(
                condition=CONDITION_BACKOFF,
                reason="pfr_core:source_open:read_error",
                message=detail,
                auto_block=False,
            ),
        )

    cache_root = Path(str(params.get("cache_root") or ".sutradhara-cache")).resolve()
    sidecar_dir = cache_root / "intakes" / item.intake_id / "pfr"
    blob_dir = sidecar_dir / "blobs"
    sidecar_dir.mkdir(parents=True, exist_ok=True)
    blob_dir.mkdir(parents=True, exist_ok=True)
    sidecar_path = sidecar_dir / f"{item.id}.pfr.json"

    role = resource_role_for_job(ctx.job.kind, ctx.job.params)
    cpu_lease = cpu_lease_from_job(ctx.granted_leases, ctx.job.required_resources)
    inputs = _index_inputs(ctx.session, item_id)
    temporary_path: Path | None = None
    try:
        with ctx.outside_transaction() as active:
            scraped = scrape_path_isolated_120(
                source, blob_dir=blob_dir, role=role, cpu_lease=cpu_lease
            )
            sidecar = None
            if isinstance(scraped, ScrapeFailure):
                _assert_reason_matrix_closed()
                if scraped.reason_id in _FALLBACK_REASON_IDS:
                    fallback = make_fallback_sidecar(LocalFile(source), scraped)
                    if isinstance(fallback, ScrapeFailure):
                        raise RuntimeError(
                            "pfr_core fallback sidecar creation failed: "
                            f"{fallback.reason_id.value}: {fallback.message}"
                        )
                    sidecar = sidecar_with_catalog_identity(fallback, item)
            else:
                sidecar = sidecar_with_catalog_identity(scraped, item)
            if sidecar is not None:
                fd, name = tempfile.mkstemp(prefix=f".{item_id}.", suffix=".tmp", dir=sidecar_dir)
                temporary_path = Path(name)
                os.close(fd)
                atomic_write_sidecar(sidecar, temporary_path)
                enforce_blob_lru(blob_dir, protect_sidecar=sidecar)
                if not sidecar_blobs_complete(sidecar, blob_dir=blob_dir):
                    raise RuntimeError(
                        "pfr blob cache trim removed blobs required by the published sidecar"
                    )

        if active and _index_inputs(ctx.session, item_id) != inputs:
            return JobResult(
                ok=False, detail="superseded: pfr-index inputs changed", superseded=True
            )
        if sidecar is None:
            if not isinstance(scraped, ScrapeFailure):
                raise RuntimeError("pfr-index missing sidecar requires a ScrapeFailure")
            return _failure_result(ctx, item, failure=scraped)
        if temporary_path is None:
            raise RuntimeError("pfr-index sidecar requires a temporary path before publishing")
        _publish_sidecar(
            ctx, item, sidecar=sidecar, sidecar_path=sidecar_path, temporary_path=temporary_path
        )
        state = {
            "kind": "fallback" if sidecar.grammar_id == "fallback" else "ok",
            "sidecar_path": str(sidecar_path),
            "grammar_id": sidecar.grammar_id,
            "recipe_version": sidecar.recipe_version,
        }
        if isinstance(scraped, ScrapeFailure):
            return JobResult(
                ok=True,
                detail=f"pfr fallback sidecar written ({scraped.reason_id.value})",
                step_state={
                    "pfr_index": {**state, "kind": "fallback", "failure": scraped.to_dict()}
                },
            )
        return JobResult(
            ok=True,
            detail=f"pfr sidecar written ({sidecar.grammar_id})",
            step_state={"pfr_index": state},
        )
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _failure_result(
    ctx: JobContext,
    item: IngestItem,
    *,
    failure: ScrapeFailure,
) -> JobResult:
    if _is_parse_determination(failure) and _previous_same_failure(ctx, failure):
        _mark_suspect(ctx, item, failure)
        detail = _failure_detail(failure)
        return JobResult(
            ok=True,
            detail=detail,
            step_state={"pfr_index": {"kind": "blocked", "failure": failure.to_dict()}},
            condition=ConditionProjection(
                condition=CONDITION_BLOCKED,
                reason="unsupported-source",
                message=detail,
                blocked_tool=("pfr_core", pfr_core_version()),
            ),
        )

    if _is_retryable_failure(failure) or _is_parse_determination(failure):
        detail = _failure_detail(failure)
        return JobResult(
            ok=False,
            detail=detail,
            step_state={"pfr_index": {"kind": "retryable", "failure": failure.to_dict()}},
            condition=ConditionProjection(
                condition=CONDITION_BACKOFF,
                reason=_condition_reason(failure),
                message=detail,
                auto_block=False,
            ),
        )

    raise RuntimeError(f"unmapped pfr_core ReasonId {failure.reason_id.value}: {failure.to_json()}")


def _is_retryable_failure(failure: ScrapeFailure) -> bool:
    if failure.reason_id in _PARSE_DETERMINATION_REASON_IDS:
        return False
    return failure.reason_id in _RETRYABLE_REASON_IDS and not _is_parse_determination(failure)


def _is_parse_determination(failure: ScrapeFailure) -> bool:
    if failure.plugin != "mxf" or failure.stage != "scrape":
        return False
    if failure.reason_id in _PARSE_DETERMINATION_REASON_IDS:
        return True
    return bool(
        failure.reason_id == ReasonId.EXCEPTION and failure.exception_class == "MXFParseError"
    )


def _previous_same_failure(ctx: JobContext, failure: ScrapeFailure) -> bool:
    if ctx.job.recon_domain is None or ctx.job.recon_target_key is None:
        return False
    row = ctx.session.scalars(
        select(ReconciliationCondition).where(
            ReconciliationCondition.domain == ctx.job.recon_domain,
            ReconciliationCondition.target_key == ctx.job.recon_target_key,
        )
    ).one_or_none()
    if row is None or row.attempt_count < 1:
        return False
    return row.reason == _condition_reason(failure)


def _mark_suspect(ctx: JobContext, item: IngestItem, failure: ScrapeFailure) -> None:
    asset = ctx.session.get(LogicalAsset, item.logical_asset_hash)
    if asset is None:
        return
    record_validity(
        ctx.session,
        asset=asset,
        validity=AssetValidity.SUSPECT,
        note=_failure_detail(failure),
    )


def _condition_reason(failure: ScrapeFailure) -> str:
    exc = failure.exception_class or ""
    return f"pfr_core:{failure.stage}:{failure.reason_id.value}:{exc}"


def _failure_detail(failure: ScrapeFailure) -> str:
    message = failure.message or failure.reason_id.value
    return f"pfr_core {failure.plugin}/{failure.stage} {failure.reason_id.value}: {message}"


def _publish_sidecar(
    ctx: JobContext,
    item: IngestItem,
    *,
    sidecar_path: Path,
    temporary_path: Path,
    sidecar: PFRSidecar,
) -> None:
    record_index(
        ctx.session,
        item=item,
        index_kind=PFR_INDEX_KIND,
        sidecar_path=sidecar_path,
    )
    item.item_metadata = {
        **(item.item_metadata or {}),
        PFR_RECIPE_METADATA_KEY: sidecar.recipe_version,
    }
    ctx.session.flush()
    os.replace(temporary_path, sidecar_path)
    directory_fd = os.open(sidecar_path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _index_inputs(session: Session, item_id: int) -> tuple[Any, ...] | None:
    """Snapshot the item and source validity without refreshing deleted objects."""
    row = session.execute(
        select(
            IngestItem.logical_asset_hash,
            IngestItem.item_metadata,
            LogicalAsset.validity,
            LogicalAsset.validity_note,
        )
        .outerjoin(LogicalAsset, LogicalAsset.content_sha256 == IngestItem.logical_asset_hash)
        .where(IngestItem.id == item_id)
    ).one_or_none()
    if row is None:
        return None
    return (
        row.logical_asset_hash,
        (row.item_metadata or {}).get("source_path"),
        row.validity,
        row.validity_note,
    )


def _assert_reason_matrix_closed() -> None:
    covered = (
        _RETRYABLE_REASON_IDS
        | _FALLBACK_REASON_IDS
        | _PARSE_DETERMINATION_REASON_IDS
        | _LOUD_STOP_REASON_IDS
    )
    missing = set(ReasonId) - covered
    if missing:
        raise RuntimeError(
            "pfr-index failure matrix does not classify ReasonId(s): "
            + ", ".join(sorted(reason.value for reason in missing))
        )


def _source_path(item: IngestItem) -> Path:
    raw = (item.item_metadata or {}).get("source_path")
    if not isinstance(raw, str) or not raw:
        raise ValueError(f"ingest_item id={item.id} has no metadata.source_path")
    return Path(raw)
