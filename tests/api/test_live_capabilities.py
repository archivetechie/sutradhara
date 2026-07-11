"""Live operator-capability registry and fail-closed resolver tests."""

from __future__ import annotations

import datetime as dt

from sutradhara.api.live_capabilities import (
    LiveCapabilityResolver,
    replace_operator_capabilities,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope


def test_live_capability_resolver_reads_current_registry_and_revocation() -> None:
    engine = make_engine("sqlite:///:memory:")
    create_all(engine)
    with session_scope(engine) as session:
        replace_operator_capabilities(
            session, operator="ada", capabilities=frozenset({"can_restore"})
        )
    resolver = LiveCapabilityResolver.from_database(engine)

    assert resolver.has_capability("ada", "can_restore")
    with session_scope(engine) as session:
        replace_operator_capabilities(session, operator="ada", capabilities=frozenset())
    assert not resolver.has_capability("ada", "can_restore")


def test_live_capability_resolver_denies_stale_synchronized_snapshot() -> None:
    engine = make_engine("sqlite:///:memory:")
    create_all(engine)
    with session_scope(engine) as session:
        replace_operator_capabilities(
            session,
            operator="ada",
            capabilities=frozenset({"can_restore"}),
            valid_for=dt.timedelta(seconds=1),
            now=dt.datetime(2020, 1, 1, tzinfo=dt.UTC),
        )

    assert not LiveCapabilityResolver.from_database(engine).has_capability("ada", "can_restore")


def test_live_capability_resolver_fails_closed_when_source_unreachable() -> None:
    def unavailable(_operator: str) -> frozenset[str]:
        raise ConnectionError("identity source unavailable")

    resolver = LiveCapabilityResolver(unavailable)

    assert resolver.capabilities_for("ada") == frozenset()
    assert not resolver.has_capability("ada", "can_restore")
