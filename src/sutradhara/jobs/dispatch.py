"""High-level job dispatch helpers.

These wrap `engine.submit` with the catalog validation a caller shouldn't
have to repeat: the asset must exist, and the target backend must be a
registered `Backend` row. Dispatch is *pure dispatch* — it creates a
PENDING job and returns a handle. It does NOT run the job; execution is
the engine's concern (`run_one`/`run_pending`), and keeping the boundary
clean is what lets a real async scheduler slot in later.

The caller owns the transaction (`session.commit()`), matching the
`engine.submit` contract.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import Backend, LogicalAsset
from sutradhara.catalog.types import BackendKind, is_content_hash
from sutradhara.hdcache.manager import RestoreAdmissionInvalid, validate_restore_item_admission
from sutradhara.hdcache.models import RestoreRequestItem
from sutradhara.jobs.engine import submit
from sutradhara.jobs.models import Job


class DispatchError(Exception):
    """Base for dispatch-time validation failures."""


class AssetNotInCatalog(DispatchError):
    """The asset_id does not name a registered LogicalAsset."""


class NoEligibleBackend(DispatchError):
    """No registered Backend can satisfy the requested copy target."""


class AmbiguousBackend(DispatchError):
    """More than one Backend can satisfy the request."""


class UnknownRestoreRequestItem(DispatchError):
    """The restore_request_item_id does not name a gated restore item."""


class RestoreRequestItemNotRunnable(DispatchError):
    """The restore request item is not queued for worker execution."""


def dispatch_write_to_tape(
    session: Session,
    asset_id: bytes,
    *,
    target_backend: str | None = None,
) -> dict[str, Any]:
    """Dispatch a `copy` job that targets a tape backend.

    Validates that `asset_id` names a known asset and resolves a single
    `rem_tape` backend, then submits a PENDING `copy` job pointing at it.

    Returns a handle: `{"job_id", "kind", "params", "target_backend"}`.

    Raises:
        ValueError          — asset_id is not a 32-byte content hash.
        AssetNotInCatalog   — no LogicalAsset with that hash.
        NoEligibleBackend   — no requested/eligible rem_tape Backend is registered.
        AmbiguousBackend    — multiple rem_tape Backend rows exist and no target was named.
    """
    if not is_content_hash(asset_id):
        raise ValueError(
            f"asset_id must be a 32-byte SHA-256 content hash; "
            f"got {len(asset_id) if isinstance(asset_id, bytes) else type(asset_id)!r}"
        )

    if session.get(LogicalAsset, asset_id) is None:
        raise AssetNotInCatalog(
            f"no LogicalAsset with content hash {asset_id.hex()}; "
            "register the asset before dispatching a copy"
        )

    backend = _resolve_tape_backend(session, target_backend)

    job: Job = submit(
        session,
        "copy",
        {"asset_hash": asset_id.hex(), "target_backend": backend.name},
    )
    return {
        "job_id": job.id,
        "kind": job.kind,
        "params": job.params,
        "target_backend": backend.name,
    }


def dispatch_restore(
    session: Session,
    restore_request_item_id: int,
) -> dict[str, Any]:
    """Dispatch a gated operator restore job for one request item.

    The request item is created by the hdcache restore admission path after
    privacy, validity, and destination gates run. Raw copy ids and destination
    paths are deliberately not accepted here.

    Returns a handle: `{"job_id", "kind", "params", "restore_request_item_id"}`.

    Raises:
        UnknownRestoreRequestItem       — no restore request item with that id.
        RestoreRequestItemNotRunnable   — item is denied/done/failed already.
    """
    item = session.get(RestoreRequestItem, restore_request_item_id)
    if item is None:
        raise UnknownRestoreRequestItem(
            f"no RestoreRequestItem with id={restore_request_item_id}; nothing to restore"
        )
    if item.state != "queued":
        raise RestoreRequestItemNotRunnable(
            f"restore request item id={restore_request_item_id} is state={item.state!r}"
        )
    try:
        validate_restore_item_admission(item)
    except RestoreAdmissionInvalid as exc:
        raise RestoreRequestItemNotRunnable(str(exc)) from exc

    job: Job = submit(
        session,
        "restore",
        {"restore_request_item_id": restore_request_item_id},
    )
    return {
        "job_id": job.id,
        "kind": job.kind,
        "params": job.params,
        "restore_request_item_id": restore_request_item_id,
    }


def _resolve_tape_backend(session: Session, target_backend: str | None) -> Backend:
    if target_backend is not None:
        backend = session.scalars(
            select(Backend).where(Backend.name == target_backend)
        ).one_or_none()
        if backend is None:
            raise NoEligibleBackend(
                f"target backend {target_backend!r} is not registered; "
                "register it before dispatching a copy"
            )
        if backend.kind != BackendKind.REM_TAPE:
            raise NoEligibleBackend(
                f"target backend {target_backend!r} has kind={backend.kind!r}; "
                "copy-to-tape dispatch requires kind='rem_tape'"
            )
        return backend

    candidates = list(
        session.scalars(
            select(Backend).where(Backend.kind == BackendKind.REM_TAPE).order_by(Backend.name)
        )
    )
    if not candidates:
        raise NoEligibleBackend(
            "no rem_tape backend registered; "
            "run `sutra backends add <name> --kind rem_tape ...` first"
        )
    if len(candidates) > 1:
        names = ", ".join(repr(backend.name) for backend in candidates)
        raise AmbiguousBackend(
            f"multiple rem_tape backends registered: {names}; pass target_backend explicitly"
        )
    return candidates[0]
