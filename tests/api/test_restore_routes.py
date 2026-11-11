"""HTTP contract tests for hdcache restore-console routes."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, event, func, select
from starlette.requests import Request

from sutradhara.api import routes_restore
from sutradhara.catalog.models import LogicalAsset
from sutradhara.catalog.session import session_scope
from sutradhara.grpc.store import GrpcDeviceDestinationGrant, GrpcLogicalDevice
from sutradhara.hdcache.alarms import (
    ALARM_DOMAIN,
    restore_event_alarm_sink,
    walker_event_alarm_sink,
)
from sutradhara.hdcache.manager import (
    InvalidRestoreDestination,
    RestoreConfig,
    RestoreDestination,
    RestoreEvent,
)
from sutradhara.hdcache.models import (
    RestoreItemAttempt,
    RestoreItemGiveup,
    RestoreRequest,
    RestoreRequestItem,
    RestoreServerInstance,
)
from sutradhara.hdcache.walker import HdcacheWalkerEvent
from sutradhara.jobs.models import Job, ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import CONDITION_BLOCKED, CONDITION_OPEN
from tests.api.conftest import auth_headers, make_api_app, post_headers
from tests.bundle_group_helpers import bundle_kwargs


def test_restore_destinations_contract_shape(api_engine: Engine, tmp_path: Path) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    app = make_api_app(api_engine)
    app.state.restore_config = RestoreConfig(
        destinations={
            "media-server": RestoreDestination(
                id="media-server",
                root=root,
                label="Media server restore",
                writable=True,
            )
        }
    )
    client = TestClient(app)

    response = client.get("/api/ui/restore-destinations", headers=auth_headers("viewer"))

    assert response.status_code == 200
    assert response.json() == {
        "destinations": [{"id": "media-server", "label": "Media server restore", "writable": True}]
    }


def test_restore_post_requires_can_restore(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    digest = hashlib.sha256(b"allowed").hexdigest()
    _seed_asset(api_engine, bytes.fromhex(digest), privacy="none")
    app = make_api_app(api_engine)
    app.state.restore_config = RestoreConfig(
        destinations={
            "media-server": RestoreDestination(
                id="media-server",
                root=root,
                label="Media server restore",
                writable=True,
            )
        }
    )
    client = TestClient(app)
    payload = {
        "destination_id": "media-server",
        "items": [{"content_sha256": digest, "artifactclass": "s-masters"}],
    }

    can_view_only = client.post(
        "/api/ui/restores",
        headers=post_headers("viewer"),
        json=payload,
    )
    can_restore = client.post(
        "/api/ui/restores",
        headers=post_headers("restore"),
        json=payload,
    )

    assert can_view_only.status_code == 403
    assert can_view_only.json()["detail"]["error"] == "forbidden"
    assert can_restore.status_code == 201
    assert can_restore.json()["request_id"]


def test_agent_restore_admission_binds_receiver_and_does_not_submit_local_job(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    digest = hashlib.sha256(b"agent restore").hexdigest()
    _seed_asset(api_engine, bytes.fromhex(digest), privacy="none")
    with session_scope(api_engine) as session:
        session.add(GrpcLogicalDevice(device_id="restore-1", scopes=["restore"]))
        session.flush()
        session.add(
            GrpcDeviceDestinationGrant(
                device_id="restore-1",
                destination_id="media-server",
                dest_root="/srv/restore",
            )
        )
    app = make_api_app(api_engine)
    app.state.restore_config = RestoreConfig(
        destinations={
            "media-server": RestoreDestination(
                id="media-server",
                root=root,
                label="Media server restore",
                writable=True,
            )
        }
    )
    headers = post_headers("restore")
    request = Request(
        {
            "type": "http",
            "app": app,
            "headers": [
                (key.lower().encode("latin-1"), value.encode("latin-1"))
                for key, value in headers.items()
            ],
        }
    )
    response = routes_restore.post_restore(
        request,
        {
            "destination_id": "media-server",
            "delivery_mode": "agent",
            "receiver_device_id": "restore-1",
            "items": [
                {
                    "content_sha256": digest,
                    "artifactclass": "s-masters",
                    "final_rel_path": "project/clip.mov",
                }
            ],
        },
    )

    assert response.status_code == 201
    response_body = json.loads(response.body)
    with session_scope(api_engine) as session:
        row = session.get(RestoreRequest, response_body["request_id"])
        assert row is not None
        assert row.delivery_mode == "agent"
        assert row.receiver_device_id == "restore-1"
        assert row.items[0].final_rel_path == "project/clip.mov"
        assert list(session.scalars(select(Job))) == []
    assert list(root.iterdir()) == []


def test_restore_post_mixed_cart_and_request_status_shapes(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    allowed = hashlib.sha256(b"allowed").hexdigest()
    denied = hashlib.sha256(b"denied").hexdigest()
    _seed_asset(api_engine, bytes.fromhex(allowed), privacy="none")
    _seed_asset(api_engine, bytes.fromhex(denied), privacy="p3")
    app = make_api_app(api_engine)
    app.state.restore_config = RestoreConfig(
        destinations={
            "media-server": RestoreDestination(
                id="media-server",
                root=root,
                label="Media server restore",
                writable=True,
            )
        }
    )
    client = TestClient(app)

    response = client.post(
        "/api/ui/restores",
        headers=post_headers("restore"),
        json={
            "destination_id": "media-server",
            "items": [
                {"content_sha256": allowed, "artifactclass": "s-masters"},
                {"content_sha256": denied, "artifactclass": "private"},
            ],
            "force": False,
            "force_rejected": False,
        },
    )

    assert response.status_code == 201
    request_id = response.json()["request_id"]
    detail = client.get(f"/api/ui/restore-requests/{request_id}", headers=auth_headers("viewer"))
    assert detail.status_code == 200
    body = detail.json()
    assert set(body) == {
        "id",
        "identity",
        "created_at",
        "destination_id",
        "delivery_mode",
        "receiver_device_id",
        "state",
        "bytes_total",
        "bytes_restored",
        "items",
    }
    assert body["id"] == request_id
    assert body["identity"] == "ada"
    assert body["destination_id"] == "media-server"
    assert body["delivery_mode"] == "server_local"
    assert body["receiver_device_id"] is None
    assert body["state"] == "pending"
    assert body["bytes_total"] == 2
    assert body["bytes_restored"] == 0
    assert body["items"] == [
        {
            "content_sha256": allowed,
            "artifactclass": "s-masters",
            "final_rel_path": None,
            "state": "queued",
            "detail": None,
            "denial_kind": None,
            "size_bytes": 1,
            "bytes_restored": 0,
            "source": None,
            "updated_at": body["items"][0]["updated_at"],
            "checkpoint": None,
            "retry": None,
        },
        {
            "content_sha256": denied,
            "artifactclass": "private",
            "final_rel_path": None,
            "state": "denied",
            "detail": "requires sutradhara-restore-p3",
            "denial_kind": "capability",
            "size_bytes": 1,
            "bytes_restored": 0,
            "source": None,
            "updated_at": body["items"][1]["updated_at"],
            "checkpoint": None,
            "retry": None,
        },
    ]
    listed = client.get(
        "/api/ui/restore-requests?state=pending&limit=5", headers=auth_headers("viewer")
    )
    assert listed.status_code == 200
    assert listed.json()["requests"][0]["id"] == request_id
    with api_engine.connect() as conn:
        jobs = conn.execute(select(Job.kind, Job.params, Job.required_resources)).all()
    assert jobs == [("restore", {"restore_request_item_id": 1}, [{"pool": "io", "count": 1}])]


def test_restore_post_unknown_destination_and_malformed_payload(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    digest = hashlib.sha256(b"allowed").hexdigest()
    _seed_asset(api_engine, bytes.fromhex(digest), privacy="none")
    app = make_api_app(api_engine)
    app.state.restore_config = RestoreConfig(
        destinations={
            "media-server": RestoreDestination(
                id="media-server",
                root=root,
                label="Media server restore",
                writable=True,
            )
        }
    )
    client = TestClient(app)

    unknown = client.post(
        "/api/ui/restores",
        headers=post_headers("restore"),
        json={
            "destination_id": "unknown",
            "items": [{"content_sha256": digest, "artifactclass": "s-masters"}],
        },
    )
    malformed = client.post(
        "/api/ui/restores",
        headers=post_headers("restore"),
        json={"destination_id": "media-server", "items": []},
    )

    assert unknown.status_code == 404
    assert unknown.json()["detail"]["error"] == "unknown_destination"
    assert malformed.status_code == 400
    assert malformed.json()["detail"]["error"] == "bad_request"


def test_restore_post_idempotency_replays_same_request_and_rejects_different_body(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    digest = hashlib.sha256(b"allowed").hexdigest()
    other = hashlib.sha256(b"other").hexdigest()
    _seed_asset(api_engine, bytes.fromhex(digest), privacy="none")
    _seed_asset(api_engine, bytes.fromhex(other), privacy="none")
    app = make_api_app(api_engine)
    app.state.restore_config = RestoreConfig(
        destinations={
            "media-server": RestoreDestination(
                id="media-server",
                root=root,
                label="Media server restore",
                writable=True,
            )
        }
    )
    client = TestClient(app)
    payload = {
        "idempotency_key": "restore-key-1",
        "destination_id": "media-server",
        "items": [{"content_sha256": digest, "artifactclass": "s-masters"}],
    }

    first = client.post("/api/ui/restores", headers=post_headers("restore"), json=payload)
    replay = client.post("/api/ui/restores", headers=post_headers("restore"), json=payload)
    conflict = client.post(
        "/api/ui/restores",
        headers=post_headers("restore"),
        json={
            **payload,
            "items": [{"content_sha256": other, "artifactclass": "s-masters"}],
        },
    )

    assert first.status_code == 201
    assert replay.status_code == 200
    assert replay.json()["request_id"] == first.json()["request_id"]
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["error"] == "restore_request_invalid"
    with api_engine.connect() as conn:
        assert len(conn.execute(select(Job.id)).all()) == 1
        assert len(conn.execute(select(RestoreRequest.id)).all()) == 1


def test_restore_post_manager_error_returns_sanitized_4xx(
    api_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    digest = hashlib.sha256(b"allowed").hexdigest()
    app = make_api_app(api_engine)
    app.state.restore_config = RestoreConfig(
        destinations={
            "media-server": RestoreDestination(
                id="media-server",
                root=root,
                label="Media server restore",
                writable=True,
            )
        }
    )

    def fake_admit_restore_request(*_args: object, **_kwargs: object) -> RestoreRequest:
        raise InvalidRestoreDestination(
            "restore destination escapes configured root: /var/lib/replica/private/export"
        )

    monkeypatch.setattr(routes_restore, "admit_restore_request", fake_admit_restore_request)
    client = TestClient(app)

    response = client.post(
        "/api/ui/restores",
        headers=post_headers("restore"),
        json={
            "destination_id": "media-server",
            "items": [{"content_sha256": digest, "artifactclass": "s-masters"}],
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "invalid_restore_destination"
    assert response.json()["detail"]["detail"] == "restore destination is invalid"
    assert "/var/lib/replica" not in response.json()["detail"]["detail"]


def test_restore_get_sanitizes_persisted_item_detail(api_engine: Engine) -> None:
    digest = hashlib.sha256(b"failed").digest()
    _seed_asset(api_engine, digest, privacy="none")
    request_id = "req-path-detail"
    with api_engine.begin() as conn:
        conn.execute(
            RestoreRequest.__table__.insert(),
            {
                "id": request_id,
                "identity": "ada",
                "destination_id": "media-server",
                "state": "completed_with_errors",
                "created_at": dt.datetime(2026, 7, 3, tzinfo=dt.UTC),
            },
        )
        conn.execute(
            RestoreRequestItem.__table__.insert(),
            {
                "request_id": request_id,
                "content_sha256": digest,
                "artifactclass": "s-masters",
                "state": "failed",
                "detail": "restore failed at /var/lib/replica/private/export.mov",
                "updated_at": dt.datetime(2026, 7, 3, tzinfo=dt.UTC),
            },
        )
    client = TestClient(make_api_app(api_engine))

    response = client.get(f"/api/ui/restore-requests/{request_id}", headers=auth_headers("viewer"))

    assert response.status_code == 200
    detail = response.json()["items"][0]["detail"]
    assert detail == "restore failed at <path>"
    assert "/var/lib/replica" not in detail


def test_event_alarm_sinks_project_conditions_visible_in_reconciliation(
    api_engine: Engine,
) -> None:
    restore_sink = restore_event_alarm_sink(engine=api_engine)
    walker_sink = walker_event_alarm_sink(engine=api_engine)
    restore_sink(
        RestoreEvent(
            code="privacy-unmapped",
            severity="alarm",
            detail="privacy level p4 unmapped",
        )
    )
    restore_sink(
        RestoreEvent(
            code="cache-fallback:read-deadline",
            severity="warning",
            detail="cache read deadline exceeded",
        )
    )
    restore_sink(
        RestoreEvent(
            code="disk-circuit-open",
            severity="alarm",
            detail="cache disk d001 exceeded failure threshold",
        )
    )
    walker_sink(
        HdcacheWalkerEvent(
            code="walker-tripwire-halt",
            severity="alarm",
            disk_id="d001",
            detail="101 unknown files over threshold",
        )
    )

    with api_engine.connect() as conn:
        rows = {
            target_key: reason
            for target_key, reason in conn.execute(
                select(ReconciliationCondition.target_key, ReconciliationCondition.reason).where(
                    ReconciliationCondition.domain == ALARM_DOMAIN,
                    ReconciliationCondition.condition == CONDITION_OPEN,
                )
            )
        }
    assert rows == {
        "disk-unreachable:restore": "disk-unreachable",
        "fallback-reason:read-deadline": "fallback-reason-spike",
        "unmapped-privacy-level": "unmapped-privacy-level",
        "walker-tripwire:d001": "walker-tripwire",
    }

    client = TestClient(make_api_app(api_engine))
    response = client.get("/api/ui/reconciliation", headers=auth_headers("viewer"))

    assert response.status_code == 200
    visible = {
        row["target_key"]: row["reason"]
        for row in response.json()["conditions"]
        if row["domain"] == ALARM_DOMAIN
    }
    assert rows.items() <= visible.items()


def test_restore_post_unmapped_privacy_projects_alarm_via_app_config(
    api_engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    digest = hashlib.sha256(b"private").hexdigest()
    _seed_asset(api_engine, bytes.fromhex(digest), privacy="p4")
    app = make_api_app(api_engine)
    app.state.restore_config = RestoreConfig(
        destinations={
            "media-server": RestoreDestination(
                id="media-server",
                root=root,
                label="Media server restore",
                writable=True,
            )
        },
        privacy_capability_map={},
    )
    client = TestClient(app)

    created = client.post(
        "/api/ui/restores",
        headers=post_headers("restore"),
        json={
            "destination_id": "media-server",
            "items": [{"content_sha256": digest, "artifactclass": "private"}],
        },
    )
    response = client.get("/api/ui/reconciliation", headers=auth_headers("viewer"))

    assert created.status_code == 201
    detail = client.get(
        f"/api/ui/restore-requests/{created.json()['request_id']}",
        headers=auth_headers("viewer"),
    )
    assert detail.json()["items"][0]["denial_kind"] == "privacy_unmapped"
    assert response.status_code == 200
    conditions = {row["target_key"]: row for row in response.json()["conditions"]}
    assert conditions["unmapped-privacy-level"]["reason"] == "unmapped-privacy-level"


def test_reconciliation_endpoint_shapes_message_by_admin_and_carries_blocked_fields(
    api_engine: Engine,
) -> None:
    with api_engine.begin() as conn:
        conn.execute(
            ReconciliationCondition.__table__.insert(),
            {
                "domain": ALARM_DOMAIN,
                "target_key": "unmapped-privacy-level",
                "observed_state": "missing",
                "condition": CONDITION_OPEN,
                "reason": "unmapped-privacy-level",
                "message": "privacy level p4 unmapped",
                "attempt_count": 0,
                "updated_at": dt.datetime(2026, 7, 3, tzinfo=dt.UTC),
            },
        )
        conn.execute(
            ReconciliationCondition.__table__.insert(),
            {
                "domain": "bundle_copy",
                "target_key": "bundle:bundle-1",
                "observed_state": "missing",
                "condition": CONDITION_BLOCKED,
                "reason": "not-implemented",
                "message": "bundle repair requires /var/lib/replica/private/tool",
                "attempt_count": 2,
                "blocked_tool_name": "rem-debug",
                "blocked_tool_version": "1.2.3",
                "updated_at": dt.datetime(2026, 7, 3, tzinfo=dt.UTC),
            },
        )
    client = TestClient(make_api_app(api_engine))

    viewer = client.get("/api/ui/reconciliation", headers=auth_headers("viewer"))
    admin = client.get("/api/ui/reconciliation", headers=auth_headers("admin"))

    assert viewer.status_code == 200
    viewer_conditions = {row["target_key"]: row for row in viewer.json()["conditions"]}
    alarm = viewer_conditions["unmapped-privacy-level"]
    assert "message" not in alarm
    assert alarm == {
        "domain": ALARM_DOMAIN,
        "target_key": "unmapped-privacy-level",
        "condition": "open",
        "reason": "unmapped-privacy-level",
        "cause": "A privacy level is not mapped to a restore capability",
        "blocked_tool_name": None,
        "blocked_tool_version": None,
        "attempt_count": 0,
        "owner": "archive operator",
        "updated_at": alarm["updated_at"],
    }
    blocked = viewer_conditions["bundle:bundle-1"]
    assert "message" not in blocked
    assert blocked["cause"] == "rem-debug is blocking reconciliation"
    assert blocked["blocked_tool_name"] == "rem-debug"
    assert blocked["blocked_tool_version"] == "1.2.3"
    assert blocked["attempt_count"] == 2

    assert admin.status_code == 200
    admin_conditions = {row["target_key"]: row for row in admin.json()["conditions"]}
    assert admin_conditions["unmapped-privacy-level"]["message"] == "privacy level p4 unmapped"
    assert admin_conditions["bundle:bundle-1"]["message"] == "bundle repair requires <path>"


def _seed_asset(engine: Engine, digest: bytes, *, privacy: str) -> None:
    from sutradhara.catalog.models import (
        ArtifactClassPolicyRecord,
        Bundle,
        BundleMember,
        LogicalAsset,
    )
    from sutradhara.catalog.session import session_scope

    artifactclass = "private" if privacy != "none" else "s-masters"
    with session_scope(engine) as session:
        session.merge(LogicalAsset(content_sha256=digest, size_bytes=1))
        session.merge(
            ArtifactClassPolicyRecord(
                artifactclass=artifactclass,
                ruleset="test.rules",
                expect="messy",
                target_bytes=1024,
                max_age_seconds=3600,
                restore_preference=[],
                staging_config={},
                hdcache_config={"enabled": True, "privacy_level": privacy},
            )
        )
        bundle_id = f"bundle-{artifactclass}-{digest.hex()[:12]}"
        if session.get(Bundle, bundle_id) is None:
            session.add(
                Bundle(
                    id=bundle_id,
                    **bundle_kwargs(seed=artifactclass),
                    status="sealed",
                    target_bytes=1024,
                    max_age_seconds=3600,
                )
            )
            session.add(
                BundleMember(
                    bundle_id=bundle_id,
                    logical_asset_hash=digest,
                    artifactclass=artifactclass,
                    member_path=f"{digest.hex()}.mov",
                    size_bytes=1,
                    file_sha256=digest,
                )
            )


def _seed_retry_page(engine: Engine) -> dt.datetime:
    """A mixed page with histories that distinguish each of D8's retry fields."""

    cases = {
        "counted": ("queued", [("error", "source_integrity"), ("error", "source_unavailable")]),
        "interrupted": ("queued", [("cancelled", None), ("lost", None)]),
        "delivered": ("sent", [("job_end", None)]),
        "done": ("done", [("job_end", None)]),
        "running": ("streaming", [("error", "server_error"), (None, None)]),
        "given-up": ("failed", [("error", "source_integrity")] * 3),
        "empty": ("queued", []),
        "replay": ("done", [("job_end", None)]),
        "local": ("queued", []),
    }
    with session_scope(engine) as session:
        now = session.scalar(select(func.now()))
        assert now is not None
        session.add(GrpcLogicalDevice(device_id="retry-receiver", scopes=["restore"]))
        owner = RestoreServerInstance(
            host="retry-test",
            pid=1,
            code_version="test",
            started_at=now,
            heartbeat_at=now,
        )
        session.add(owner)
        session.flush()
        for name, (state, history) in cases.items():
            digest = hashlib.sha256(name.encode()).digest()
            session.add(LogicalAsset(content_sha256=digest, size_bytes=1))
            session.flush()
            request = RestoreRequest(
                id=f"retry-{name}",
                identity="ada",
                destination_id="media-server",
                delivery_mode="server_local" if name == "local" else "agent",
                receiver_device_id=None if name == "local" else "retry-receiver",
                state="completed" if state == "done" else "active",
            )
            item = RestoreRequestItem(
                request=request,
                content_sha256=digest,
                artifactclass="s-masters",
                final_rel_path=f"{name}.bin",
                state=state,
                size_bytes=1,
            )
            session.add(item)
            session.flush()
            for number, (kind, cause) in enumerate(history, 1):
                row = RestoreItemAttempt(
                    restore_request_item_id=item.id,
                    attempt_no=number,
                    receiver_device_id="retry-receiver",
                    server_instance_id=owner.id,
                    opened_from_state="done" if name == "replay" else "queued",
                    opened_at=now,
                    end_kind=kind,
                    cause=cause,
                    ended_at=now if kind is not None and kind != "lost" else None,
                    closed_at=now if kind == "lost" else None,
                    message="/srv/private/archive.bin" if cause is not None else None,
                    code_version="test",
                )
                session.add(row)
                session.flush()
                if name == "given-up" and number == 3:
                    session.add(
                        RestoreItemGiveup(
                            restore_request_item_id=item.id,
                            decisive_attempt_id=row.id,
                            rule="max_failures",
                            counted=3,
                            threshold=3,
                            decided_at=now,
                            code_version="test",
                        )
                    )
                    item.detail = "gave up after 3 failed tries: the archive copies failed their integrity check: /srv/private/archive.bin"
    return now.replace(tzinfo=dt.UTC)


def _retry_policy_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in {
        "BASE_SECONDS": "90",
        "MAX_SECONDS": "900",
        "MAX_FAILURES": "3",
        "MAX_INTERRUPTIONS": "4",
        "WAIT_SECONDS": "45",
    }.items():
        monkeypatch.setenv(f"SUTRADHARA_RESTORE_RETRY_{name}", value)


def test_r6_retry_page_and_detail_share_one_summary_query(
    api_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    _retry_policy_env(monkeypatch)
    now = _seed_retry_page(api_engine)
    client = TestClient(make_api_app(api_engine))
    summaries: list[str] = []

    def count_summary(
        _conn: Any, _cursor: Any, statement: str, _parameters: Any, _context: Any, _many: bool
    ) -> None:
        if statement.lstrip().startswith("WITH classified AS"):
            summaries.append(statement)

    event.listen(api_engine, "before_cursor_execute", count_summary)
    try:
        response = client.get("/api/ui/restore-requests", headers=auth_headers("viewer"))
        assert response.status_code == 200
        assert len(summaries) == 1
        rows = {
            row["id"].removeprefix("retry-"): row["items"][0] for row in response.json()["requests"]
        }
        assert set(rows) == {
            "counted",
            "interrupted",
            "delivered",
            "done",
            "running",
            "given-up",
            "empty",
            "replay",
            "local",
        }
        assert rows["local"]["retry"] is None
        empty = {
            "attempts": 0,
            "counted_failures": 0,
            "max_failures": 3,
            "interrupted_run": 0,
            "max_interruptions": 4,
            "deliveries": 0,
            "running": False,
            "next_offer_not_before": None,
            "awaiting_confirmation": False,
            "last_cause": None,
            "last_error": None,
        }
        expected = {
            "empty": empty,
            "counted": {
                **empty,
                "attempts": 2,
                "counted_failures": 2,
                "next_offer_not_before": (now + dt.timedelta(seconds=180)).isoformat(),
                "last_cause": "source_unavailable",
                "last_error": "no readable source copy: <path>",
            },
            "interrupted": {
                **empty,
                "attempts": 2,
                "interrupted_run": 2,
                "next_offer_not_before": (now + dt.timedelta(seconds=90)).isoformat(),
                "last_error": "the transfer was interrupted",
            },
            "delivered": {**empty, "attempts": 1, "deliveries": 1, "awaiting_confirmation": True},
            "done": {**empty, "attempts": 1, "deliveries": 1},
            "replay": {**empty, "attempts": 1},
            "running": {
                **empty,
                "attempts": 2,
                "counted_failures": 1,
                "running": True,
                "last_cause": "server_error",
                "last_error": "server error: <path>",
            },
            "given-up": {
                **empty,
                "attempts": 3,
                "counted_failures": 3,
                "last_cause": "source_integrity",
                "last_error": "the archive copies failed their integrity check: <path>",
            },
        }
        for name, retry in expected.items():
            assert rows[name]["retry"] == retry, name
            summaries.clear()
            detail = client.get(
                f"/api/ui/restore-requests/retry-{name}", headers=auth_headers("viewer")
            )
            assert detail.status_code == 200
            assert detail.json()["items"][0]["retry"] == retry
            assert len(summaries) == 1
        assert (
            rows["given-up"]["detail"]
            == "gave up after 3 failed tries: the archive copies failed their integrity check: <path>"
        )
    finally:
        event.remove(api_engine, "before_cursor_execute", count_summary)


def test_r6_done_delivery_is_not_awaiting_confirmation(
    api_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    _retry_policy_env(monkeypatch)
    _seed_retry_page(api_engine)
    client = TestClient(make_api_app(api_engine))
    response = client.get("/api/ui/restore-requests/retry-done", headers=auth_headers("viewer"))
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["state"] == "done"
    assert item["retry"]["deliveries"] == 1
    assert item["retry"]["awaiting_confirmation"] is False
