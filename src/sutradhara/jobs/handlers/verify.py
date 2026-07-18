"""`verify` job: re-check a single copy's integrity.

Params:
    copy_id (int)  — the catalog `copy.id` to verify.

Behavior:
    1. Load the Copy row + its Backend row.
    2. Instantiate the backend via the factory.
    3. Call `backend.verify(copy.native_locator)`.
    4. Update `copy.health` (OK / SUSPECT / CORRUPT) and `copy.last_verified_at`.
    5. Return JobResult(ok=True) regardless of integrity outcome — the
       JOB succeeded; the verify ANSWER is captured in catalog state and
       in `step_state` for inspection.

The handler is idempotent: re-running the same job re-verifies and
updates timestamps. Step_state captures the most recent answer.
"""

from __future__ import annotations

import datetime as dt
from typing import cast

# Imported as a module (not via `from ... import backend_from_row`) so tests
# can monkeypatch `factory.backend_from_row` and have the override take
# effect here.
from sutradhara.backend import factory
from sutradhara.catalog.models import Copy
from sutradhara.catalog.types import CopyHealth
from sutradhara.jobs.components import touch_asset, touch_copy_tape
from sutradhara.jobs.registry import JobContext, JobResult, register_handler


@register_handler("verify")
def handle_verify(ctx: JobContext) -> JobResult:
    params = ctx.job.params
    raw_copy_id = params.get("copy_id")
    if not isinstance(raw_copy_id, int):
        raise ValueError(f"verify job requires params.copy_id (int); got {raw_copy_id!r}")

    copy = ctx.session.get(Copy, raw_copy_id)
    if copy is None:
        raise ValueError(f"no copy with id={raw_copy_id}")
    if copy.deleted_at is not None:
        raise ValueError(f"copy id={raw_copy_id} has been tombstoned by retention")

    if copy.logical_asset_hash is not None:
        touch_asset(ctx, copy.logical_asset_hash)
    ctx.touch(f"backend:{copy.backend.name}")
    touch_copy_tape(ctx, copy)
    backend = factory.backend_from_row(copy.backend)
    result = backend.verify(copy.native_locator)
    ctx.observe(
        {
            "verify_ok": result.ok,
            "actual_hash": (cast(bytes, result.actual_hash).hex() if result.actual_hash else None),
            "detail": result.detail,
        }
    )

    copy.last_verified_at = dt.datetime.now(dt.UTC)
    if result.ok:
        # If this copy was previously suspect/missing/corrupt and now
        # verifies clean, restore it to OK.
        copy.health = CopyHealth.OK
    else:
        # Distinguish corruption (hash known and wrong) from suspect
        # (hash unknown or hash matched but other signal off). Day-1:
        # any non-ok verify result is SUSPECT.
        copy.health = CopyHealth.SUSPECT

    return JobResult(
        ok=True,
        detail=("verified ok" if result.ok else f"integrity mismatch: {result.detail}"),
        step_state={
            "verify_result": {
                "ok": result.ok,
                "actual_hash": (
                    cast(bytes, result.actual_hash).hex() if result.actual_hash else None
                ),
                "detail": result.detail,
            },
            "copy_health_after": copy.health.value,
        },
    )
