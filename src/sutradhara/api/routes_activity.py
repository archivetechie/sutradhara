"""Activity monitoring endpoint for the operator-console API."""

from __future__ import annotations

from functools import partial

import anyio
from fastapi import APIRouter, HTTPException, Request

from sutradhara.api.activity import MAX_ACTIVITY_DAYS, MIN_ACTIVITY_DAYS, read_activity
from sutradhara.api.identity import Identity, parse_identity

router = APIRouter()


@router.get("/api/activity")
async def get_activity(request: Request, days: int = 7) -> dict[str, object]:
    """Return the cross-operator receive activity read model."""

    _require_view(parse_identity(request.headers))
    if days < MIN_ACTIVITY_DAYS or days > MAX_ACTIVITY_DAYS:
        _raise(400, "bad_request", f"days must be between {MIN_ACTIVITY_DAYS} and {MAX_ACTIVITY_DAYS}")
    return await anyio.to_thread.run_sync(
        partial(
            read_activity,
            request.app.state.engine,
            days=days,
            now=getattr(request.app.state, "activity_now", None),
        )
    )


def _require_view(identity: Identity) -> Identity:
    if not identity.has_capability("can_view"):
        _raise(403, "forbidden", "operator has no sutradhara role")
    return identity


def _raise(status_code: int, error: str, detail: str) -> None:
    raise HTTPException(status_code=status_code, detail={"error": error, "detail": detail})
