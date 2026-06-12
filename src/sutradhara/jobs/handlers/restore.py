"""`restore` job: read a logical asset's bytes back from one copy.

Params:
    copy_id (int) - the catalog `copy.id` to read from.

Status (day-1):
    EXECUTION IS NOT IMPLEMENTED. Dispatching a restore job records *intent*
    (a PENDING row created by `sutradhara.jobs.dispatch.dispatch_restore`);
    actually reading bytes back needs the backend read path
    (`backend.read_range`, whole-object day-1) plus an output/destination
    interface, neither of which exists yet.

    The handler is registered anyway - on purpose. An unregistered kind crashes
    `run_one` with a generic HandlerNotRegistered; a registered handler that
    raises `NotImplementedError` lets the engine record a clean, intentional
    FAILED job whose `last_error` states the roadmap. It must NOT fake success:
    a restore that produced no bytes is not SUCCEEDED.

    Partial / byte-range restore is deferred (spec roadmap item 10). Choosing
    WHICH copy to restore from is a separate policy layer; this handler is told
    the copy via `params.copy_id`.
"""

from __future__ import annotations

from sutradhara.jobs.registry import JobContext, JobResult, register_handler


@register_handler("restore")
def handle_restore(ctx: JobContext) -> JobResult:
    params = ctx.job.params
    raise NotImplementedError(
        "restore execution is not implemented yet: reading bytes back from a "
        "backend requires the backend read path (backend.read_range, whole-object) "
        "plus an output/destination interface, which do not exist. dispatch_restore "
        "records restore intent only (day-1). "
        f"requested: copy_id={params.get('copy_id')!r}"
    )
