"""Identity parsing tests for the operator HTTP API."""

from __future__ import annotations

import pytest

from sutradhara.api.identity import parse_identity


@pytest.mark.parametrize(
    ("group", "role", "capabilities"),
    [
        ("sutradhara-ingest", "ingest", ("can_view", "can_receive")),
        ("sutradhara-restore", "restore", ("can_view", "can_restore")),
        ("sutradhara-oversight", "oversight", ("can_view",)),
        ("sutradhara-admin", "admin", ("can_view", "can_admin")),
        ("sutradhara-restore-p2", None, ("can_restore_p2",)),
        ("sutradhara-restore-p3", None, ("can_restore_p2", "can_restore_p3")),
        ("sutradhara-operator", "ingest", ("can_view", "can_receive")),
        ("sutradhara-viewer", "oversight", ("can_view",)),
    ],
)
def test_parse_identity_single_group_capabilities(
    group: str,
    role: str | None,
    capabilities: tuple[str, ...],
) -> None:
    identity = parse_identity(
        {
            "X-Authentik-Username": "owner",
            "X-Authentik-Name": "Ada Operator",
            "X-Authentik-Groups": group,
            "X-Authentik-Email": "owner@example.test",
        }
    )

    assert identity.operator_username == "owner"
    assert identity.display_name == "Ada Operator"
    assert identity.role == role
    assert identity.capabilities == capabilities


@pytest.mark.parametrize(
    ("groups", "role", "capabilities"),
    [
        (
            "sutradhara-ingest|sutradhara-restore",
            "restore",
            ("can_view", "can_receive", "can_restore"),
        ),
        (
            "sutradhara-admin|sutradhara-ingest",
            "admin",
            ("can_view", "can_receive", "can_admin"),
        ),
        (
            "sutradhara-restore|sutradhara-restore-p3",
            "restore",
            ("can_view", "can_restore", "can_restore_p2", "can_restore_p3"),
        ),
        (
            "sutradhara-restore|sutradhara-restore-p2",
            "restore",
            ("can_view", "can_restore", "can_restore_p2"),
        ),
    ],
)
def test_parse_identity_unions_capabilities_across_groups(
    groups: str,
    role: str,
    capabilities: tuple[str, ...],
) -> None:
    identity = parse_identity(
        {
            "X-Authentik-Username": "owner",
            "X-Authentik-Groups": groups,
        }
    )

    assert identity.role == role
    assert identity.capabilities == capabilities


def test_parse_identity_display_role_uses_precedence_not_gates() -> None:
    identity = parse_identity(
        {
            "X-Authentik-Username": "owner",
            "X-Authentik-Groups": (
                "sutradhara-oversight|sutradhara-ingest|"
                "sutradhara-restore|sutradhara-admin"
            ),
        }
    )

    assert identity.role == "admin"
    assert identity.capabilities == (
        "can_view",
        "can_receive",
        "can_restore",
        "can_admin",
    )


def test_parse_identity_old_aliases_resolve_to_new_group_capabilities() -> None:
    ingest = parse_identity({"X-Authentik-Groups": "sutradhara-ingest"})
    operator = parse_identity({"X-Authentik-Groups": "sutradhara-operator"})
    ingest_p3 = parse_identity(
        {"X-Authentik-Groups": "sutradhara-ingest|sutradhara-restore-p3"}
    )
    operator_p3 = parse_identity(
        {"X-Authentik-Groups": "sutradhara-operator|sutradhara-restore-p3"}
    )
    oversight = parse_identity({"X-Authentik-Groups": "sutradhara-oversight"})
    viewer = parse_identity({"X-Authentik-Groups": "sutradhara-viewer"})

    assert operator.role == ingest.role == "ingest"
    assert operator.capabilities == ingest.capabilities == ("can_view", "can_receive")
    assert operator_p3.role == ingest_p3.role == "ingest"
    assert operator_p3.capabilities == ingest_p3.capabilities == (
        "can_view",
        "can_receive",
        "can_restore_p2",
        "can_restore_p3",
    )
    assert viewer.role == oversight.role == "oversight"
    assert viewer.capabilities == oversight.capabilities == ("can_view",)


def test_parse_identity_admin_does_not_imply_receive_or_restore() -> None:
    identity = parse_identity({"X-Authentik-Groups": "sutradhara-admin"})

    assert identity.capabilities == ("can_view", "can_admin")
    assert not identity.has_capability("can_receive")
    assert not identity.has_capability("can_restore")


@pytest.mark.parametrize(
    ("group", "capabilities"),
    [
        ("sutradhara-restore-p2", ("can_restore_p2",)),
        ("sutradhara-restore-p3", ("can_restore_p2", "can_restore_p3")),
    ],
)
def test_parse_identity_privacy_caps_do_not_grant_view_or_restore(
    group: str,
    capabilities: tuple[str, ...],
) -> None:
    identity = parse_identity({"X-Authentik-Groups": group})

    assert identity.role is None
    assert identity.capabilities == capabilities
    assert not identity.has_capability("can_view")
    assert not identity.has_capability("can_restore")


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
