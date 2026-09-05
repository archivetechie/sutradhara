"""Hermetic tests for receive-dedup phase 1a intent, history, and lease behavior."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import Engine, select
from starlette.requests import Request

from sutradhara._proto import device_pb2
from sutradhara.api import routes_devices
from sutradhara.api import store as api_store
from sutradhara.api.receive_history import latest_card_history
from sutradhara.api.routes_devices import (
    DeviceReceiveRequest,
    _device_payloads_with_history,
    post_device_receive,
)
from sutradhara.catalog.models import Intake
from sutradhara.catalog.session import session_scope
from sutradhara.catalog.types import IntakeSourceKind, IntakeStatus
from sutradhara.grpc import status as grpc_status
from sutradhara.grpc import store as grpc_store
from sutradhara.grpc.registry import Card, CommandAck, ConnectedDeviceRegistry
from sutradhara.grpc.store import DeviceIdentity
from tests.api.conftest import make_api_app, post_headers


def test_identity_match_authorizes_and_other_body_conflicts(api_engine: Engine) -> None:
    _add_catalog_intake(api_engine, intake_id="prior", card_id="card-1")
    key = str(uuid4())

    authorized = _begin(api_engine, key=key, request_hash="base")
    replay = _begin(api_engine, key=key, request_hash="base")
    conflict = _begin(api_engine, key=key, request_hash="changed")
    authorized_replay = _begin(
        api_engine,
        key=key,
        request_hash="base",
        acknowledge_duplicate=True,
    )

    assert authorized.state == "authorized"
    assert replay.state == authorized_replay.state == "in_progress"
    assert conflict.state == "conflict"
    with session_scope(api_engine) as session:
        intent = session.scalars(
            select(api_store.IdempotencyRecord).where(
                api_store.IdempotencyRecord.idempotency_key == key
            )
        ).one()
        assert intent.status == "authorized"
        assert intent.duplicate_acknowledged is False
        assert intent.warned_at is None
        assert intent.authorized_at is not None
        assert intent.lease_source_id == api_store.card_lease_source_id("card-1")
    assert api_store.duplicate_telemetry_counts(api_engine) == {
        "warned_then_never_acknowledged": 0,
        "warned_then_acknowledged": 0,
    }


def test_card_lease_excludes_concurrent_identity_and_reconciles_on_restart(
    api_engine: Engine,
) -> None:
    first = _begin(api_engine, key="key-a", request_hash="a")
    second = _begin(api_engine, key="key-b", request_hash="b", operator="other")

    assert first.state == "authorized"
    assert second.state == "busy"

    lease_id = api_store.card_lease_source_id("card-1")
    with session_scope(api_engine) as session:
        claim = session.get(api_store.SourceClaim, lease_id)
        assert claim is not None
        session.delete(claim)

    result = api_store.reconcile_device_receive_leases(api_engine)

    assert result == {"rebuilt": 1, "expired": 0, "orphaned": 0}
    with session_scope(api_engine) as session:
        claim = session.get(api_store.SourceClaim, lease_id)
        assert claim is not None
        assert claim.idempotency_key == "key-a"


def test_history_projection_prefers_most_recent_failed_over_verified(
    api_engine: Engine,
) -> None:
    old = dt.datetime(2026, 7, 1, tzinfo=dt.UTC)
    _add_catalog_intake(
        api_engine,
        intake_id="verified-old",
        card_id="card-1",
        created_at=old,
    )
    with session_scope(api_engine) as session:
        session.add(
            api_store.IdempotencyRecord(
                operator_username="ada",
                endpoint=api_store.DEVICE_RECEIVE_ENDPOINT,
                idempotency_key="failed-key",
                request_hash="failed-body",
                status="failed",
                intake_id="failed-new",
                device_id="mac-2",
                card_identity="card-1",
                card_label="Card One",
                created_at=old + dt.timedelta(days=1),
                updated_at=old + dt.timedelta(days=1),
                last_heartbeat=old + dt.timedelta(days=1),
                started_at=old + dt.timedelta(days=1),
                terminal_at=old + dt.timedelta(days=1),
            )
        )

    with session_scope(api_engine) as session:
        match = latest_card_history(
            session,
            card_identity="card-1",
            requester="ada",
        )

    assert match is not None
    assert match.intake_id == "failed-new"
    assert match.state == "failed"
    assert match.visible is True


def test_foreign_identity_history_does_not_block(api_engine: Engine) -> None:
    _add_catalog_intake(api_engine, intake_id="foreign-prior", card_id="card-1")

    decision = _begin(
        api_engine,
        key="foreign-key",
        request_hash="foreign",
        operator="other",
    )

    assert decision.state == "authorized"


def test_file_receipt_renewal_honors_floor_timer(api_engine: Engine) -> None:
    assert _begin(api_engine, key="renew-key", request_hash="renew").state == "authorized"
    with session_scope(api_engine) as session:
        linked = api_store.claim_start_intake(
            session,
            operator_username="ada",
            device_id="device-ada",
            idempotency_key="renew-key",
            intake_id="renew-intake",
        )
        assert linked.state == "claimed"
        intent = session.scalars(
            select(api_store.IdempotencyRecord).where(
                api_store.IdempotencyRecord.idempotency_key == "renew-key"
            )
        ).one()
        old = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=1)
        intent.last_heartbeat = old
        assert intent.lease_source_id is not None
        claim = session.get(api_store.SourceClaim, intent.lease_source_id)
        assert claim is not None
        claim.last_heartbeat = old

    assert (
        api_store.renew_device_intake_lease(
            api_engine,
            intake_id="renew-intake",
            floor=dt.timedelta(seconds=5),
        )
        == "renewed"
    )
    assert (
        api_store.renew_device_intake_lease(
            api_engine,
            intake_id="renew-intake",
            floor=dt.timedelta(seconds=5),
        )
        == "throttled"
    )


def test_stale_same_key_terminalizes_and_fresh_key_rechecks_history(api_engine: Engine) -> None:
    """A stale key fails durably; only a fresh key reruns duplicate history."""

    assert _begin(api_engine, key="stale-replay", request_hash="same").state == "authorized"
    with session_scope(api_engine) as session:
        linked = api_store.claim_start_intake(
            session,
            operator_username="ada",
            device_id="device-ada",
            idempotency_key="stale-replay",
            intake_id="stalled-intake",
        )
        assert linked.state == "claimed"
    with session_scope(api_engine) as session:
        intent = session.scalars(
            select(api_store.IdempotencyRecord).where(
                api_store.IdempotencyRecord.idempotency_key == "stale-replay"
            )
        ).one()
        stale = dt.datetime.now(dt.UTC) - dt.timedelta(hours=1)
        intent.last_heartbeat = stale
        assert intent.lease_source_id is not None
        claim = session.get(api_store.SourceClaim, intent.lease_source_id)
        assert claim is not None
        claim.last_heartbeat = stale

    changed = _begin(api_engine, key="stale-replay", request_hash="changed")
    terminal = _begin(api_engine, key="stale-replay", request_hash="same")
    terminal_replay = api_store.peek_device_receive_intent(
        api_engine,
        operator_username="ada",
        idempotency_key="stale-replay",
        request_hash="same",
        acknowledge_duplicate=False,
    )
    fresh = _begin(api_engine, key="fresh-retry", request_hash="fresh")

    assert changed.state == "conflict"
    assert terminal.state == "terminal"
    assert terminal.terminal_state == "failed"
    assert terminal_replay is not None
    assert terminal_replay.state == "terminal"
    assert terminal_replay.terminal_state == "failed"
    assert fresh.state == "authorized"
    with session_scope(api_engine) as session:
        intent = session.scalars(
            select(api_store.IdempotencyRecord).where(
                api_store.IdempotencyRecord.idempotency_key == "stale-replay"
            )
        ).one()
        assert intent.status == "failed"
        assert intent.intake_id == "stalled-intake"
        assert intent.response_json is None
        assert intent.started_at is not None
        assert intent.terminal_at is not None
        assert intent.lease_source_id is not None
        claim = session.get(api_store.SourceClaim, intent.lease_source_id)
        assert claim is not None
        assert claim.idempotency_key == "fresh-retry"


def test_terminal_history_receipts_are_memoized_for_device_polls(
    api_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated card-history polls parse a terminal receipt ledger only once."""

    with session_scope(api_engine) as session:
        grpc_store.insert_intake(
            session,
            intake_id="historical-grpc",
            operator="ada",
            device_id="mac-old",
            idempotency_key="historical-key",
            source_plan_digest="a" * 64,
            artifactclass="s-masters",
            source_kind="card",
            source_ref="DCIM",
            label="Card One",
            landing_root=str(tmp_path),
        )
        assert grpc_store.set_card_id(
            session,
            intake_id="historical-grpc",
            operator="ada",
            device_id="mac-old",
            card_id="card-1",
        )
        grpc_store.set_committed_digest(session, "historical-grpc", "b" * 64)
    ledger = tmp_path / "historical-grpc" / "receive-receipts.jsonl"
    ledger.parent.mkdir()
    ledger.write_text(
        json.dumps({"relpath": "clip.mov", "server_sha256": "c" * 64, "bytes": 7}) + "\n",
        encoding="utf-8",
    )
    registry = _online_registry(api_engine, enroll=False)
    original_read_text = Path.read_text
    reads = 0

    def counted_read_text(path: Path, *args, **kwargs):
        nonlocal reads
        if path == ledger:
            reads += 1
        return original_read_text(path, *args, **kwargs)

    grpc_status._cached_terminal_receipt_summary.cache_clear()
    monkeypatch.setattr(Path, "read_text", counted_read_text)

    first = _device_payloads_with_history(api_engine, "ada", registry.devices_for("ada"))
    second = _device_payloads_with_history(api_engine, "ada", registry.devices_for("ada"))

    assert first == second
    assert first[0]["cards"][0]["receivedBefore"]["state"] == "verifying"
    assert reads == 1


def test_terminal_receipt_summary_retries_failed_read_and_stays_bounded(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    """A missing terminal ledger is retried and cached without retaining relpaths."""

    with session_scope(api_engine) as session:
        row = grpc_store.insert_intake(
            session,
            intake_id="late-ledger",
            operator="ada",
            device_id="mac-1",
            idempotency_key="late-key",
            source_plan_digest="a" * 64,
            artifactclass="s-masters",
            source_kind="card",
            source_ref=None,
            label=None,
            landing_root=str(tmp_path),
        )
        grpc_store.set_committed_digest(session, row.intake_id, "b" * 64)
        session.flush()
        session.expunge(row)
    grpc_status._cached_terminal_receipt_summary.cache_clear()

    assert grpc_status.intake_receipt_summary(row) is None
    ledger = tmp_path / "late-ledger" / "receive-receipts.jsonl"
    ledger.parent.mkdir()
    ledger.write_text(
        json.dumps({"relpath": "clip.mov", "server_sha256": "c" * 64, "bytes": 9}) + "\n",
        encoding="utf-8",
    )

    summary = grpc_status.intake_receipt_summary(row)
    assert summary == grpc_status.IntakeReceiptSummary(bytes_total=9, file_count=1)
    assert not hasattr(summary, "relpaths")


def test_reconcile_terminalizes_inactive_orphaned_grpc_intake(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    """A stale streaming row without a live intent stops projecting as verifying."""

    old = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
    with session_scope(api_engine) as session:
        grpc_store.insert_intake(
            session,
            intake_id="orphan-stream",
            operator="ada",
            device_id="mac-old",
            idempotency_key="orphan-key",
            source_plan_digest="a" * 64,
            artifactclass="s-masters",
            source_kind="card",
            source_ref=None,
            label="Orphan",
            landing_root=str(tmp_path),
        )
        assert grpc_store.set_card_id(
            session,
            intake_id="orphan-stream",
            operator="ada",
            device_id="mac-old",
            card_id="orphan-card",
        )
        row = grpc_store.get_intake(session, "orphan-stream")
        assert row is not None
        row.created_at = old
        row.updated_at = old

    with session_scope(api_engine) as session:
        before = latest_card_history(session, card_identity="orphan-card", requester="ada")
    result = api_store.reconcile_device_receive_leases(
        api_engine,
        ttl=dt.timedelta(minutes=30),
    )
    with session_scope(api_engine) as session:
        row = grpc_store.get_intake(session, "orphan-stream")
        after = latest_card_history(session, card_identity="orphan-card", requester="ada")
        assert row is not None
        assert row.state == "aborted"

    assert before is not None
    assert before.state == "verifying"
    assert after is not None
    assert after.state == "failed"
    assert result == {"rebuilt": 0, "expired": 0, "orphaned": 1}


def test_device_receive_identity_match_proceeds_without_acknowledgement(
    api_engine: Engine,
    tmp_path: Path,
    caplog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _add_catalog_intake(api_engine, intake_id="prior", card_id="card-1")
    registry = _online_registry(api_engine, capabilities=["browse"])
    original_listing = registry.request_directory_listing

    def listing_with_new_file(**kwargs):
        pending = original_listing(**kwargs)
        pending.future.set_result(
            device_pb2.DirectoryListing(
                request_id=pending.request_id,
                status=device_pb2.DIR_STATUS_OK,
                entries=[
                    device_pb2.DirectoryEntry(
                        name="new.mov",
                        is_dir=False,
                        size_bytes=12,
                    )
                ],
            )
        )
        return pending

    registry.request_directory_listing = listing_with_new_file  # type: ignore[method-assign]
    original = registry.send_start_receive

    def auto_ack(**kwargs):
        pending = original(**kwargs)
        with session_scope(api_engine) as session:
            linked = api_store.claim_start_intake(
                session,
                operator_username="ada",
                device_id="mac-1",
                idempotency_key=kwargs["idempotency_key"],
                intake_id="new-intake",
            )
            assert linked.state == "claimed"
            grpc_store.insert_intake(
                session,
                intake_id="new-intake",
                operator="ada",
                device_id="mac-1",
                idempotency_key=kwargs["idempotency_key"],
                source_plan_digest="a" * 64,
                artifactclass=kwargs["artifactclass"],
                source_kind="card",
                source_ref=kwargs["source_ref"],
                label=kwargs["label"],
                landing_root=str(tmp_path),
            )
        pending.future.set_result(
            CommandAck(
                command_id=pending.command_id,
                accepted=True,
                reason=None,
                intake_id="new-intake",
            )
        )
        return pending

    registry.send_start_receive = auto_ack  # type: ignore[method-assign]
    app = make_api_app(api_engine)
    app.state.registry = registry
    key = str(uuid4())
    payload = {
        "card_id": "card-1",
        "artifactclass": "s-masters",
        "idempotencyKey": key,
    }

    async def direct_run_sync(func, *args, **_kwargs):
        return func(*args)

    monkeypatch.setattr(routes_devices.anyio.to_thread, "run_sync", direct_run_sync)
    request = _request(app)

    with caplog.at_level(logging.INFO, logger="sutradhara.api.store"):
        accepted = asyncio.run(
            post_device_receive(
                "mac-1",
                request,
                DeviceReceiveRequest.model_validate(payload),
            )
        )
        replay = asyncio.run(
            post_device_receive(
                "mac-1",
                request,
                DeviceReceiveRequest.model_validate(payload),
            )
        )
        acknowledged_replay = asyncio.run(
            post_device_receive(
                "mac-1",
                request,
                DeviceReceiveRequest.model_validate({**payload, "acknowledge_duplicate": True}),
            )
        )

    assert accepted == {"intakeId": "new-intake", "status": "streaming"}
    assert replay == accepted
    assert acknowledged_replay == accepted
    assert api_store.DUPLICATE_WARNED_EVENT not in caplog.text


def test_device_receive_returns_nothing_new_409_for_all_known_listing(
    api_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only the content estimate, not identity alone, creates the blocking 409."""

    _add_catalog_intake(api_engine, intake_id="prior", card_id="card-1")
    registry = _online_registry(api_engine, capabilities=["browse"])
    original = registry.request_directory_listing

    def empty_listing(**kwargs):
        pending = original(**kwargs)
        pending.future.set_result(
            device_pb2.DirectoryListing(
                request_id=pending.request_id,
                status=device_pb2.DIR_STATUS_OK,
            )
        )
        return pending

    registry.request_directory_listing = empty_listing  # type: ignore[method-assign]
    app = make_api_app(api_engine)
    app.state.registry = registry

    async def direct_run_sync(func, *args, **_kwargs):
        return func(*args)

    monkeypatch.setattr(routes_devices.anyio.to_thread, "run_sync", direct_run_sync)
    response = asyncio.run(
        post_device_receive(
            "mac-1",
            _request(app),
            DeviceReceiveRequest.model_validate(
                {
                    "card_id": "card-1",
                    "artifactclass": "s-masters",
                    "idempotencyKey": str(uuid4()),
                }
            ),
        )
    )

    assert response.status_code == 409
    payload = json.loads(response.body)
    assert payload["error"] == "nothing_new"
    assert payload["retryable"] is True
    assert payload["estimate"]["all_known_estimate"] is True


def test_preview_treats_package_as_opaque_listing_item(
    api_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _online_registry(api_engine, capabilities=["browse"])
    original = registry.request_directory_listing
    calls = 0

    def package_listing(**kwargs):
        nonlocal calls
        calls += 1
        pending = original(**kwargs)
        pending.future.set_result(
            device_pb2.DirectoryListing(
                request_id=pending.request_id,
                status=device_pb2.DIR_STATUS_OK,
                entries=[
                    device_pb2.DirectoryEntry(
                        name="A001.fcpbundle",
                        is_dir=True,
                        is_package=True,
                        size_bytes=42,
                    )
                ],
            )
        )
        return pending

    registry.request_directory_listing = package_listing  # type: ignore[method-assign]
    app = make_api_app(api_engine)
    app.state.registry = registry

    async def direct_run_sync(func, *args, **_kwargs):
        return func(*args)

    monkeypatch.setattr(routes_devices.anyio.to_thread, "run_sync", direct_run_sync)
    listing, complete = asyncio.run(
        routes_devices._current_card_listing(
            _request(app),
            operator="ada",
            device_id="mac-1",
            card_id="card-1",
            source_ref="",
        )
    )

    assert calls == 1
    assert listing == [("A001.fcpbundle", 42)]
    assert complete is True


def test_devices_received_before_uses_same_projection(api_engine: Engine) -> None:
    _add_catalog_intake(api_engine, intake_id="prior", card_id="card-1")
    registry = _online_registry(api_engine, enroll=False)

    payload = _device_payloads_with_history(api_engine, "ada", registry.devices_for("ada"))

    badge = payload[0]["cards"][0]["receivedBefore"]
    assert badge["state"] == "verified"
    assert badge["visible"] is True
    assert badge["receivedAt"] is not None


def test_card_snapshot_identity_and_label_are_bounded() -> None:
    with pytest.raises(ValueError, match="card_id"):
        Card(card_id="bad/card", label="Card", kind="card", size_bytes=1, status="available")
    with pytest.raises(ValueError, match="at most 512"):
        Card(
            card_id="card-1",
            label="x" * 513,
            kind="card",
            size_bytes=1,
            status="available",
        )


def _begin(
    engine: Engine,
    *,
    key: str,
    request_hash: str,
    acknowledge_duplicate: bool = False,
    operator: str = "ada",
):
    return api_store.begin_device_receive_intent(
        engine,
        operator_username=operator,
        device_id=f"device-{operator}",
        card_identity="card-1",
        card_label="Card One",
        idempotency_key=key,
        request_hash=request_hash,
        acknowledge_duplicate=acknowledge_duplicate,
    )


def _add_catalog_intake(
    engine: Engine,
    *,
    intake_id: str,
    card_id: str,
    created_at: dt.datetime | None = None,
) -> None:
    timestamp = created_at or dt.datetime.now(dt.UTC)
    with session_scope(engine) as session:
        session.add(
            Intake(
                intake_id=intake_id,
                operator="ada",
                source_kind=IntakeSourceKind.CARD,
                source_ref="DCIM",
                card_id=card_id,
                device_id="mac-old",
                artifactclass="s-masters",
                label="Card One",
                status=IntakeStatus.REGISTERED,
                created_at=timestamp,
                updated_at=timestamp,
                registered_at=timestamp,
            )
        )


def _online_registry(
    engine: Engine,
    *,
    enroll: bool = True,
    capabilities: list[str] | None = None,
) -> ConnectedDeviceRegistry:
    registry = ConnectedDeviceRegistry()
    stream = registry.register(
        DeviceIdentity(operator="ada", device_id="mac-1", fingerprint="AA" * 32)
    )
    stream.update_cards(
        [Card(card_id="card-1", label="Card One", kind="card", size_bytes=10, status="available")],
        capabilities=capabilities,
    )
    if enroll:
        with session_scope(engine) as session:
            grpc_store.record_device_enrollment(
                session,
                device_id="mac-1",
                cert_fingerprint="AA" * 32,
                operator="ada",
            )
    return registry


def _request(app) -> Request:
    headers = [
        (key.lower().encode("ascii"), value.encode("ascii"))
        for key, value in post_headers("operator").items()
    ]
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/devices/mac-1/receive",
            "headers": headers,
            "app": app,
            "scheme": "http",
            "query_string": b"",
            "server": ("testserver", 80),
            "client": ("testclient", 1),
            "root_path": "",
        }
    )
