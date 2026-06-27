"""`restore` job: read verified whole-asset bytes back from one copy.

The handler is the imperative P2.1 restore mechanism: it restores the caller's
chosen asset-scoped ``copy_id`` to a validated absolute ``dest_path``. Copy
selection, path resolution, member extraction, and corruption response remain
outside this job; this handler only reads, opens, verifies, and atomically
places one whole object.
"""

from __future__ import annotations

from sutradhara.backend.factory import backend_from_row
from sutradhara.backend.port import BackendError, StorageBackend
from sutradhara.catalog.models import Copy
from sutradhara.catalog.types import CopyHealth
from sutradhara.jobs.registry import JobContext, JobResult, register_handler
from sutradhara.keys import KeyRegistry
from sutradhara.restore import (
    RestoreError,
    atomic_write_verified_file,
    restore_copy,
    validate_restore_destination,
)
from sutradhara.sealing.rao import RaoCliOpener


def resolve_restore_backend(copy: Copy) -> StorageBackend:
    """Return the backend instance used to read this restore copy."""
    return backend_from_row(copy.backend)


@register_handler("restore")
def handle_restore(ctx: JobContext) -> JobResult:
    params = ctx.job.params
    copy_id = params.get("copy_id")
    if not isinstance(copy_id, int):
        return _failure("bad-params", "restore job requires params.copy_id (int)")
    try:
        destination = validate_restore_destination(params.get("dest_path"))
    except ValueError as exc:
        return _failure("bad-destination", str(exc))

    copy = ctx.session.get(Copy, copy_id)
    if copy is None:
        return _failure("unknown-copy", f"no Copy with id={copy_id}; nothing to restore")
    if copy.health == CopyHealth.MISSING:
        return _failure(
            "missing-copy",
            f"copy id={copy_id} has health=missing; there are no bytes to restore",
        )
    if copy.deleted_at is not None:
        return _failure(
            "deleted-copy",
            f"copy id={copy_id} has been tombstoned by retention; there are no bytes to restore",
        )
    if copy.bundle_id is not None or copy.logical_asset_hash is None:
        return _failure("bundle-unsupported", "bundle restore is not supported by P2.1")

    try:
        backend = resolve_restore_backend(copy)
        opener = RaoCliOpener(KeyRegistry())
        with restore_copy(ctx.session, copy, backend=backend, opener=opener) as result:
            atomic_write_verified_file(result.path, destination)
    except RestoreError as exc:
        return _failure("restore-failed", str(exc))
    except (BackendError, OSError, RuntimeError, ValueError, KeyError) as exc:
        return _failure("restore-failed", f"{type(exc).__name__}: {exc}")

    return JobResult(
        ok=True,
        detail=f"restored copy id={copy_id} to {destination}",
        step_state={
            "restore": {
                "kind": "ok",
                "copy_id": copy_id,
                "path": str(destination),
                "sha256": result.sha256.hex(),
                "bytes": result.size_bytes,
                "representation": result.representation.value,
            }
        },
    )


def _failure(reason: str, detail: str) -> JobResult:
    return JobResult(
        ok=False,
        detail=detail,
        step_state={"restore": {"kind": reason, "detail": detail}},
    )
