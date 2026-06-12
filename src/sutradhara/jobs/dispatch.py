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

from sutradhara.catalog.models import Backend, Copy, LogicalAsset
from sutradhara.catalog.types import BackendKind, CopyHealth, is_content_hash
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


class UnknownCopy(DispatchError):
    """The copy_id does not name a registered Copy."""


class CopyNotRestorable(DispatchError):
    """The copy cannot be restored from (e.g. health == MISSING)."""


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


def dispatch_restore(session: Session, copy_id: int) -> dict[str, Any]:
    """Dispatch a read-only `restore` job for one existing copy.

    Pure dispatch: validate the copy, submit a PENDING `restore` job, and return
    a handle. The caller (a future copy-selection policy layer) chooses which
    `copy_id` to restore; this helper is the mechanism, not the policy.

    Returns a handle: `{"job_id", "kind", "params", "copy_id", "source_backend"}`.

    Raises:
        UnknownCopy        — no Copy with that id.
        CopyNotRestorable  — the copy's health is MISSING (no bytes to read).
    """
    copy = session.get(Copy, copy_id)
    if copy is None:
        raise UnknownCopy(f"no Copy with id={copy_id}; nothing to restore")

    if copy.health == CopyHealth.MISSING:
        raise CopyNotRestorable(
            f"copy id={copy_id} on backend {copy.backend.name!r} has health=missing; "
            "there are no bytes to restore from it"
        )

    job: Job = submit(session, "restore", {"copy_id": copy_id})
    return {
        "job_id": job.id,
        "kind": job.kind,
        "params": job.params,
        "copy_id": copy_id,
        "source_backend": copy.backend.name,
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
            select(Backend)
            .where(Backend.kind == BackendKind.REM_TAPE)
            .order_by(Backend.name)
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
            f"multiple rem_tape backends registered: {names}; "
            "pass target_backend explicitly"
        )
    return candidates[0]
