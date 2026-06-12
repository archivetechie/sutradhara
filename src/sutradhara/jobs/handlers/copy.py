"""`copy` job: realize a logical asset onto a target backend.

Params:
    asset_hash (str)      — hex SHA-256 of the LogicalAsset to copy.
    target_backend (str)  — `Backend.name` to write the copy to.

Status (day-1):
    EXECUTION IS NOT IMPLEMENTED. Dispatching a copy job records *intent*
    (a PENDING row created by `sutradhara.jobs.dispatch.dispatch_write_to_tape`);
    actually moving bytes onto a backend needs a write-capable backend port,
    which does not exist yet. In the steering harness the byte-write is a
    separate seam (`rem.tape.write_object`) owned by the Remanence project,
    and recording the resulting Copy is another seam (`catalog.add_copy`).

    The handler is registered anyway — on purpose. An unregistered kind
    crashes `run_one` with a generic HandlerNotRegistered; a registered
    handler that raises `NotImplementedError` lets the engine record a
    clean, intentional FAILED job whose `last_error` states the roadmap.
    It must NOT fake success: a job that moved no bytes is not SUCCEEDED.

When the write path lands, this handler will: load the asset, resolve the
target Backend via the factory, write the bytes through the write-capable
port, then record the Copy row (collapsing today's A.5/A.6 harness seams).
"""

from __future__ import annotations

from sutradhara.jobs.registry import JobContext, JobResult, register_handler


@register_handler("copy")
def handle_copy(ctx: JobContext) -> JobResult:
    params = ctx.job.params
    raise NotImplementedError(
        "copy execution is not implemented yet: writing bytes to a backend "
        "requires a write-capable backend port (Remanence Layer 5 / the "
        "rem.tape.write_object seam), which does not exist. dispatch_write_to_tape "
        "records copy intent only (day-1). "
        f"requested: asset_hash={params.get('asset_hash')!r} "
        f"target_backend={params.get('target_backend')!r}"
    )
