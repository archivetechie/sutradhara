"""HTTP contract tests for hdcache restore-console routes."""

from __future__ import annotations

import datetime as dt
import hashlib
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import Engine, select

from sutradhara.hdcache.alarms import ALARM_DOMAIN
from sutradhara.hdcache.manager import RestoreConfig, RestoreDestination
from sutradhara.hdcache.models import RestoreRequest
from sutradhara.jobs.models import Job, ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import CONDITION_OPEN
from tests.api.conftest import auth_headers, make_api_app, post_headers


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
        "destinations": [
            {"id": "media-server", "label": "Media server restore", "writable": True}
        ]
    }


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
        headers=post_headers("viewer"),
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
    assert set(body) == {"id", "identity", "created_at", "destination_id", "state", "items"}
    assert body["id"] == request_id
    assert body["identity"] == "owner"
    assert body["destination_id"] == "media-server"
    assert body["state"] == "pending"
    assert body["items"] == [
        {
            "content_sha256": allowed,
            "artifactclass": "s-masters",
            "state": "queued",
            "detail": None,
            "updated_at": body["items"][0]["updated_at"],
        },
        {
            "content_sha256": denied,
            "artifactclass": "private",
            "state": "denied",
            "detail": "requires sutradhara-restore-p3",
            "updated_at": body["items"][1]["updated_at"],
        },
    ]
    listed = client.get("/api/ui/restore-requests?state=pending&limit=5", headers=auth_headers("viewer"))
    assert listed.status_code == 200
    assert listed.json()["requests"][0]["id"] == request_id
    with api_engine.connect() as conn:
        jobs = conn.execute(select(Job.kind, Job.params)).all()
    assert jobs == [("restore", {"restore_request_item_id": 1})]


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
        headers=post_headers("viewer"),
        json={
            "destination_id": "unknown",
            "items": [{"content_sha256": digest, "artifactclass": "s-masters"}],
        },
    )
    malformed = client.post(
        "/api/ui/restores",
        headers=post_headers("viewer"),
        json={"destination_id": "media-server", "items": []},
    )

    assert unknown.status_code == 404
    assert unknown.json()["error"] == "unknown_destination"
    assert malformed.status_code == 400
    assert malformed.json()["error"] == "bad_request"


def test_reconciliation_endpoint_includes_hdcache_alarm_owner(
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
    client = TestClient(make_api_app(api_engine))

    response = client.get("/api/ui/reconciliation", headers=auth_headers("viewer"))

    assert response.status_code == 200
    assert response.json()["conditions"] == [
        {
            "domain": ALARM_DOMAIN,
            "target_key": "unmapped-privacy-level",
            "condition": "open",
            "reason": "unmapped-privacy-level",
            "message": "privacy level p4 unmapped",
            "owner": "archive operator",
            "updated_at": response.json()["conditions"][0]["updated_at"],
        }
    ]


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
                    artifactclass=artifactclass,
                    status="sealed",
                    target_bytes=1024,
                    max_age_seconds=3600,
                )
            )
            session.add(
                BundleMember(
                    bundle_id=bundle_id,
                    logical_asset_hash=digest,
                    member_path=f"{digest.hex()}.mov",
                    size_bytes=1,
                    file_sha256=digest,
                )
            )
