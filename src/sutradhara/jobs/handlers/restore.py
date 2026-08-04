"""`restore` job: execute one gated hdcache restore request item.

M4 closes the operator restore bypass: worker restore jobs no longer accept raw
``copy_id`` / ``dest_path`` parameters. Admission through the hdcache manager
creates a ``restore_request_item`` after privacy, validity, and destination
gates; this handler only validates that gated row and asks the manager to serve
it from cache or tape fallback.
"""

from __future__ import annotations

from sutradhara.catalog.models import Copy
from sutradhara.hdcache.manager import (
    ITEM_DONE,
    RestoreAdmissionInvalid,
    RestoreManagerError,
    destination_for_request_item,
    restore_config_from_env,
    serve_restore_item,
)
from sutradhara.hdcache.models import RestoreRequestItem
from sutradhara.hdcache.read_ordering import (
    note_restore_item_outcome,
    restore_release_allowed,
)
from sutradhara.jobs.components import touch_asset, touch_copy_tape, touch_destination
from sutradhara.jobs.registry import (
    JobContext,
    JobResult,
    register_dispatch_gate,
    register_handler,
)

# The dispatcher enforces read ordering at claim time: a volume's restore
# jobs are released per the persisted per-volume list; items in no list
# dispatch exactly as today (design-restore-read-ordering §4).
register_dispatch_gate("restore", restore_release_allowed)


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
    touch_asset(ctx, item.content_sha256)
    config = restore_config_from_env()
    try:
        destination = destination_for_request_item(config, item.request.destination_id, item)
    except RestoreManagerError:
        destination = None
    if destination is not None:
        touch_destination(ctx, destination)
    try:
        result = serve_restore_item(
            ctx.session,
            item,
            gates_already_admitted=True,
            config=config,
        )
    except RestoreAdmissionInvalid as exc:
        return _failure("not-admitted", str(exc))
    except Exception as exc:
        return _failure("restore-failed", f"{type(exc).__name__}: {exc}")

    # Read-ordering runtime hooks: the single post-mount re-plan and the
    # read-failure re-plan observe every served item here. Never raises.
    note_restore_item_outcome(
        ctx.session,
        item,
        served_copy_id=result.copy_id,
        config=config,
    )

    if item.state != ITEM_DONE:
        return _failure("restore-failed", item.detail or f"item ended in state={item.state!r}")
    if result.copy_id is not None:
        source_copy = ctx.session.get(Copy, result.copy_id)
        if source_copy is not None:
            touch_copy_tape(ctx, source_copy)
    touch_destination(ctx, result.output_path)
    ctx.observe(
        {
            "restore_request_item_id": item_id,
            "path": str(result.output_path),
            "sha256": item.content_sha256.hex(),
            "bytes": result.size_bytes,
            "source": result.source,
        }
    )
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
