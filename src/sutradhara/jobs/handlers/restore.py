"""`restore` job: execute one gated hdcache restore request item.

M4 closes the operator restore bypass: worker restore jobs no longer accept raw
``copy_id`` / ``dest_path`` parameters. Admission through the hdcache manager
creates a ``restore_request_item`` after privacy, validity, and destination
gates; this handler only validates that gated row and asks the manager to serve
it from cache or tape fallback.
"""

from __future__ import annotations

from sutradhara.hdcache.manager import (
    ITEM_DONE,
    ITEM_FAILED,
    RestoreAdmissionInvalid,
    restore_config_from_env,
    serve_restore_item,
    validate_restore_item_admission,
)
from sutradhara.hdcache.models import RestoreRequestItem
from sutradhara.jobs.registry import JobContext, JobResult, register_handler


@register_handler("restore")
def handle_restore(ctx: JobContext) -> JobResult:
    params = ctx.job.params
    if "copy_id" in params or "dest_path" in params:
        return _failure(
            "bad-params",
            "restore job rejects raw copy_id/dest_path params; use restore_request_item_id",
        )
    item_id = params.get("restore_request_item_id")
    if not isinstance(item_id, int):
        return _failure("bad-params", "restore job requires params.restore_request_item_id (int)")
    item = ctx.session.get(RestoreRequestItem, item_id)
    if item is None:
        return _failure(
            "unknown-request-item",
            f"no RestoreRequestItem with id={item_id}; nothing to restore",
        )
    if item.state != "queued":
        return _failure(
            "not-queued",
            f"restore request item id={item_id} is state={item.state!r}",
        )
    try:
        validate_restore_item_admission(item)
    except RestoreAdmissionInvalid as exc:
        item.state = ITEM_FAILED
        item.detail = str(exc)
        return _failure("not-admitted", str(exc))
    try:
        result = serve_restore_item(
            ctx.session,
            item,
            gates_already_admitted=True,
            config=restore_config_from_env(),
        )
    except Exception as exc:
        return _failure("restore-failed", f"{type(exc).__name__}: {exc}")

    if item.state != ITEM_DONE:
        return _failure("restore-failed", item.detail or f"item ended in state={item.state!r}")
    return JobResult(
        ok=True,
        detail=f"restored request item id={item_id} to {result.output_path}",
        step_state={
            "restore": {
                "kind": "ok",
                "restore_request_item_id": item_id,
                "path": str(result.output_path),
                "sha256": item.content_sha256.hex(),
                "bytes": result.size_bytes,
                "source": result.source,
            }
        },
    )


def _failure(reason: str, detail: str) -> JobResult:
    return JobResult(
        ok=False,
        detail=detail,
        step_state={"restore": {"kind": reason, "detail": detail}},
    )
