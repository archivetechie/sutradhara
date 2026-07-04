"""Session endpoint for the operator console API."""

from __future__ import annotations

from fastapi import APIRouter, Request

from sutradhara.api.identity import parse_identity

router = APIRouter()


@router.get("/api/session")
def get_session(request: Request) -> dict[str, object]:
    """Return the current operator identity without leaking raw groups."""

    identity = parse_identity(request.headers)
    return {
        "operatorUsername": identity.operator_username,
        "displayName": identity.display_name,
        "role": identity.role,
        "capabilities": list(identity.capabilities),
    }
