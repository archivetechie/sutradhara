"""Durable store tests for streaming gRPC intake."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, inspect

from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.grpc import store


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_grpc_store_round_trip_state_digest_and_owner(engine: Engine) -> None:
    with session_scope(engine) as session:
        row = store.insert_intake(
            session,
            intake_id="intake-1",
            operator="owner",
            device_id="mac-1",
            idempotency_key="key",
            source_plan_digest="a" * 64,
            artifactclass="video-master",
            source_kind="card",
            source_ref="card-a",
            label="Card A",
            landing_root="/landing",
        )
        assert row.state == "streaming"

    with session_scope(engine) as session:
        assert store.compare_and_set_state(
            session,
            "intake-1",
            expect="streaming",
            update="committing",
        )
        assert not store.compare_and_set_state(
            session,
            "intake-1",
            expect="streaming",
            update="committed",
        )
        store.set_committed_digest(session, "intake-1", "b" * 64)

    with session_scope(engine) as session:
        row = store.get_intake(session, "intake-1")
        assert row is not None
        assert row.operator == "owner"
        assert row.device_id == "mac-1"
        assert row.state == "committed"
        assert row.manifest_digest == "b" * 64


def test_device_enrollment_token_revoke_and_schema(engine: Engine) -> None:
    tables = set(inspect(engine).get_table_names())
    assert {"grpc_intake", "grpc_device_enrollment", "grpc_enroll_token"} <= tables

    with session_scope(engine) as session:
        token = store.issue_enroll_token(
            session,
            operator="owner",
            device_id="mac-1",
            ttl=dt.timedelta(seconds=1),
        )
        grant = store.consume_enroll_token(session, token, device_id="mac-1")
        assert grant.operator == "owner"
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="owner",
        )

    with session_scope(engine) as session:
        identity = store.resolve_device(
            session,
            device_id="mac-1",
            cert_fingerprint="aa" * 32,
        )
        assert identity.operator == "owner"
        assert store.revoke_device(session, "mac-1") == 1

    with session_scope(engine) as session, pytest.raises(PermissionError):
        store.resolve_device(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
        )


def test_expired_or_reused_enrollment_token_is_refused(engine: Engine) -> None:
    with session_scope(engine) as session:
        expired = store.issue_enroll_token(
            session,
            operator="owner",
            device_id="mac-1",
            ttl=dt.timedelta(seconds=-1),
        )
        used = store.issue_enroll_token(session, operator="owner", device_id="mac-1")
        wrong_device = store.issue_enroll_token(session, operator="owner", device_id="mac-1")
        store.consume_enroll_token(session, used, device_id="mac-1")

    with session_scope(engine) as session:
        with pytest.raises(ValueError, match="expired"):
            store.consume_enroll_token(session, expired)
        with pytest.raises(ValueError, match="already used"):
            store.consume_enroll_token(session, used)
        with pytest.raises(ValueError, match="common name"):
            store.consume_enroll_token(session, wrong_device, device_id="mac-2")
