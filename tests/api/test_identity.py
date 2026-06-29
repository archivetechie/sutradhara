"""Identity parsing tests for the operator HTTP API."""

from __future__ import annotations

from sutradhara.api.identity import parse_identity


def test_parse_identity_operator_capabilities() -> None:
    identity = parse_identity(
        {
            "X-Authentik-Username": "owner",
            "X-Authentik-Name": "Ada Operator",
            "X-Authentik-Groups": "sutradhara-operator",
            "X-Authentik-Email": "owner@example.test",
        }
    )

    assert identity.operator_username == "owner"
    assert identity.display_name == "Ada Operator"
    assert identity.role == "operator"
    assert identity.capabilities == ("can_view", "can_receive")


def test_parse_identity_chooses_highest_exact_group() -> None:
    identity = parse_identity(
        {
            "X-Authentik-Username": "owner",
            "X-Authentik-Groups": "sutradhara-viewer|sutradhara-operator|sutradhara-admin",
        }
    )

    assert identity.role == "admin"
    assert identity.capabilities == ("can_view", "can_receive", "can_admin")


def test_parse_identity_never_uses_substring_group_match() -> None:
    identity = parse_identity(
        {
            "X-Authentik-Username": "owner",
            "X-Authentik-Groups": "sutradhara-admin-extra",
        }
    )

    assert identity.role is None
    assert identity.capabilities == ()


def test_parse_identity_empty_groups_fail_closed() -> None:
    identity = parse_identity({"X-Authentik-Username": "owner", "X-Authentik-Groups": ""})

    assert identity.role is None
    assert identity.capabilities == ()
