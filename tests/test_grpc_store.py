"""Durable store tests for streaming gRPC intake."""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, inspect, select

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
            operator="ada",
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
        assert row.operator == "ada"
        assert row.device_id == "mac-1"
        assert row.state == "committed"
        assert row.manifest_digest == "b" * 64


def test_device_enrollment_token_revoke_and_schema(engine: Engine) -> None:
    tables = set(inspect(engine).get_table_names())
    assert {"grpc_intake", "grpc_device_enrollment", "grpc_enroll_token"} <= tables

    with session_scope(engine) as session:
        token = store.issue_enroll_token(
            session,
            operator="ada",
            device_id="mac-1",
            ttl=dt.timedelta(seconds=1),
        )
        grant = store.consume_enroll_token(session, token, device_id="mac-1")
        assert grant.operator == "ada"
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )

    with session_scope(engine) as session:
        identity = store.resolve_device(
            session,
            device_id="mac-1",
            cert_fingerprint="aa" * 32,
        )
        assert identity.operator == "ada"
        assert store.revoke_device(session, "mac-1") == 1

    with session_scope(engine) as session, pytest.raises(PermissionError):
        store.resolve_device(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
        )


def test_device_reenrollment_requires_rotation_proof(engine: Engine) -> None:
    with session_scope(engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
        with pytest.raises(store.DeviceRotationProofError):
            store.record_device_enrollment(
                session,
                device_id="mac-1",
                cert_fingerprint="BB" * 32,
                operator="ada",
            )

    with session_scope(engine) as session:
        assert store.operator_for_device(session, "mac-1") == "ada"
        assert (
            store.resolve_device(
                session,
                device_id="mac-1",
                cert_fingerprint="AA" * 32,
            ).operator
            == "ada"
        )
        with pytest.raises(PermissionError):
            store.resolve_device(session, device_id="mac-1", cert_fingerprint="BB" * 32)
        assert _active_enrollment_count(session, "mac-1") == 1


def test_device_reenrollment_allows_old_key_proof(engine: Engine, caplog) -> None:
    with session_scope(engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
        with caplog.at_level(logging.INFO, logger=store.__name__):
            store.record_device_enrollment(
                session,
                device_id="mac-1",
                cert_fingerprint="BB" * 32,
                operator="ada",
                rotation_authority="self",
                rotation_fingerprint="AA" * 32,
            )
    assert "device certificate rotated" in caplog.text

    with session_scope(engine) as session:
        with pytest.raises(PermissionError):
            store.resolve_device(session, device_id="mac-1", cert_fingerprint="AA" * 32)
        identity = store.resolve_device(
            session,
            device_id="mac-1",
            cert_fingerprint="BB" * 32,
        )
        assert identity.operator == "ada"
        assert _active_enrollment_count(session, "mac-1") == 1


def test_device_reenrollment_allows_admin_rotation(engine: Engine) -> None:
    with session_scope(engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="BB" * 32,
            operator="ada",
            rotation_authority="admin",
        )

    with session_scope(engine) as session:
        with pytest.raises(PermissionError):
            store.resolve_device(session, device_id="mac-1", cert_fingerprint="AA" * 32)
        assert (
            store.resolve_device(
                session,
                device_id="mac-1",
                cert_fingerprint="BB" * 32,
            ).operator
            == "ada"
        )


def test_device_reenrollment_same_fingerprint_is_idempotent(engine: Engine) -> None:
    with session_scope(engine) as session:
        first = store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
        second = store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )
        assert first.id == second.id

    with session_scope(engine) as session:
        assert _active_enrollment_count(session, "mac-1") == 1
        identity = store.resolve_device(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
        )
        assert identity.operator == "ada"


def test_device_reenrollment_refuses_different_operator_without_mutation(engine: Engine) -> None:
    with session_scope(engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="ada",
        )

    with session_scope(engine) as session, pytest.raises(store.DeviceOwnershipError):
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="CC" * 32,
            operator="other",
        )

    with session_scope(engine) as session:
        assert _active_enrollment_count(session, "mac-1") == 1
        assert (
            store.resolve_device(
                session,
                device_id="mac-1",
                cert_fingerprint="AA" * 32,
            ).operator
            == "ada"
        )
        with pytest.raises(PermissionError):
            store.resolve_device(session, device_id="mac-1", cert_fingerprint="CC" * 32)


def test_expired_or_reused_enrollment_token_is_refused(engine: Engine) -> None:
    with session_scope(engine) as session:
        # Distinct devices: minting supersedes prior unredeemed tokens for the
        # SAME operator/device (contract-enroll-bundle 2026-07-04), so same-device
        # tokens would invalidate each other before the assertions below.
        expired = store.issue_enroll_token(
            session,
            operator="ada",
            device_id="mac-expired",
            ttl=dt.timedelta(seconds=-1),
        )
        used = store.issue_enroll_token(session, operator="ada", device_id="mac-used")
        wrong_device = store.issue_enroll_token(session, operator="ada", device_id="mac-wrong")
        store.consume_enroll_token(session, used, device_id="mac-used")

    with session_scope(engine) as session:
        with pytest.raises(ValueError, match="expired"):
            store.consume_enroll_token(session, expired)
        with pytest.raises(ValueError, match="already used"):
            store.consume_enroll_token(session, used)
        with pytest.raises(ValueError, match="common name"):
            store.consume_enroll_token(session, wrong_device, device_id="mac-2")


def _active_enrollment_count(session, device_id: str) -> int:
    return len(
        list(
            session.scalars(
                select(store.GrpcDeviceEnrollment).where(
                    store.GrpcDeviceEnrollment.device_id == device_id,
                    store.GrpcDeviceEnrollment.revoked.is_(False),
                )
            )
        )
    )
