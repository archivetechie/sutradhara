"""Authentik header parsing and role/capability mapping for the HTTP API."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

VIEWER_GROUP = "sutradhara-viewer"
OPERATOR_GROUP = "sutradhara-operator"
ADMIN_GROUP = "sutradhara-admin"

ROLE_CAPABILITIES: dict[str, tuple[str, ...]] = {
    "viewer": ("can_view",),
    "operator": ("can_view", "can_receive"),
    "admin": ("can_view", "can_receive", "can_admin"),
}

_ROLE_GROUPS: tuple[tuple[str, str], ...] = (
    ("admin", ADMIN_GROUP),
    ("operator", OPERATOR_GROUP),
    ("viewer", VIEWER_GROUP),
)


@dataclass(frozen=True)
class Identity:
    """Operator identity derived from trusted Caddy/Authentik request headers."""

    operator_username: str
    display_name: str
    email: str | None
    groups: tuple[str, ...]
    role: str | None
    capabilities: tuple[str, ...]

    def has_capability(self, capability: str) -> bool:
        return capability in self.capabilities


def parse_identity(headers: Mapping[str, str] | Sequence[tuple[str, str]]) -> Identity:
    """Parse Authentik headers using exact pipe-delimited group matching.

    Missing or empty groups deliberately produce no role/capabilities; callers
    must fail closed for API access. Raw groups are kept inside the server only.
    """

    normalized = _normalize_headers(headers)
    username = _nonempty(normalized.get("x-authentik-username")) or "unknown"
    display_name = _nonempty(normalized.get("x-authentik-name")) or username
    email = _nonempty(normalized.get("x-authentik-email"))
    groups = _parse_groups(normalized.get("x-authentik-groups"))
    role = _highest_role(groups)
    capabilities = ROLE_CAPABILITIES.get(role, ())
    return Identity(
        operator_username=username,
        display_name=display_name,
        email=email,
        groups=groups,
        role=role,
        capabilities=capabilities,
    )


def _normalize_headers(headers: Mapping[str, str] | Sequence[tuple[str, str]]) -> dict[str, str]:
    if isinstance(headers, Mapping):
        return {key.lower(): value for key, value in headers.items()}
    return {key.lower(): value for key, value in headers}


def _parse_groups(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    return tuple(group.strip() for group in raw.split("|") if group.strip())


def _highest_role(groups: tuple[str, ...]) -> str | None:
    group_set = set(groups)
    for role, group in _ROLE_GROUPS:
        if group in group_set:
            return role
    return None


def _nonempty(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None
