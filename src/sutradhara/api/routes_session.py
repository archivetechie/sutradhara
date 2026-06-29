"""Session endpoint for the operator console API."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from sutradhara.api.identity import parse_identity

router = APIRouter()


@router.get("/api/session")
def get_session(request: Request) -> dict[str, object]:
    """Return the current operator identity without leaking raw groups."""

    identity = parse_identity(request.headers)
    if identity.role is None:
        raise HTTPException(
            status_code=403,
            detail={"error": "forbidden", "detail": "operator has no sutradhara role"},
        )
    return {
        "operatorUsername": identity.operator_username,
        "displayName": identity.display_name,
        "role": identity.role,
        "capabilities": list(identity.capabilities),
    }
