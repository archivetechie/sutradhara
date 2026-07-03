"""Job handler for hdcache fills.

The handler is intentionally thin: hdcache fill policy lives in
``sutradhara.hdcache.fill`` so the post-flush hook, CLI, and reconciler all use
the same dedupe, live-cap, source fallback, and policy-conformance rules.
"""

from __future__ import annotations

from sutradhara.hdcache.fill import (
    HdcacheFillBlocked,
    HdcacheFillError,
    fill_config_from_env,
    fill_target,
    fill_target_from_params,
)
from sutradhara.hdcache.repopulate import RepopulationError, execute_repopulation_batch
from sutradhara.jobs.reconcilers.conditions import CONDITION_BACKOFF, CONDITION_BLOCKED
from sutradhara.jobs.registry import ConditionProjection, JobContext, JobResult, register_handler


@register_handler("hdcache_fill")
def handle_hdcache_fill(ctx: JobContext) -> JobResult:
    """Fill one hdcache entry from landing or archive restore fallback."""

    try:
        if ctx.job.params.get("repopulate_batch") is True:
            results = execute_repopulation_batch(ctx.session, ctx.job.params)
            return JobResult(
                ok=True,
                detail=f"hdcache repopulation batch filled {len(results)} entries",
                step_state={
                    "hdcache_fill": {
                        "kind": "repopulation-batch",
                        "batch_id": ctx.job.params.get("batch_id"),
                        "count": len(results),
                        "items": [
                            {
                                "content_sha256": result.content_sha256.hex(),
                                "disk_id": result.disk_id,
                                "bytes": result.size_bytes,
                                "source": result.source,
                            }
                            for result in results
                        ],
                    }
                },
            )
        target = fill_target_from_params(ctx.session, ctx.job.params)
        result = fill_target(ctx.session, target, config=fill_config_from_env())
    except HdcacheFillBlocked as exc:
        return JobResult(
            ok=False,
            detail=exc.detail,
            step_state={"hdcache_fill": {"kind": exc.reason, "detail": exc.detail}},
            condition=ConditionProjection(
                condition=CONDITION_BLOCKED,
                reason=exc.reason,
                message=exc.detail,
            ),
        )
    except (HdcacheFillError, RepopulationError, OSError, RuntimeError, ValueError) as exc:
        detail = f"{type(exc).__name__}: {exc}"
        return JobResult(
            ok=False,
            detail=detail,
            step_state={"hdcache_fill": {"kind": "failed", "detail": detail}},
            condition=ConditionProjection(
                condition=CONDITION_BACKOFF,
                reason="fill-failed",
                message=detail,
            ),
        )

    return JobResult(
        ok=True,
        detail=(
            f"hdcache fill {target.sha_hex} {result.representation} "
            f"on {result.disk_id}/{result.relpath}"
        ),
        step_state={
            "hdcache_fill": {
                "kind": "already-present" if result.already_present else "filled",
                "content_sha256": target.sha_hex,
                "disk_id": result.disk_id,
                "relpath": result.relpath,
                "bytes": result.size_bytes,
                "representation": result.representation,
                "key_epoch": result.key_epoch,
                "stored_digest": None
                if result.stored_digest is None
                else result.stored_digest.hex(),
                "source": result.source,
            }
        },
    )
