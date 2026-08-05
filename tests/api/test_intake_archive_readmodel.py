"""HTTP contract tests for intake and archive/catalog console read models."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, func, select

from sutradhara.api.routes_intake_archive import MAX_LIMIT, _intake_payload
from sutradhara.archive_predicate import intake_archive_state_expr
from sutradhara.catalog.models import (
    Arrangement,
    AssetDerivation,
    AssetLocator,
    Backend,
    Bundle,
    BundleMember,
    Copy,
    IngestItem,
    Intake,
    LogicalAsset,
    Pool,
    Submission,
    SubmissionMember,
)
from sutradhara.catalog.session import locator_key, session_scope
from sutradhara.catalog.types import (
    ArrangementStatus,
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
    IntakeSourceKind,
    IntakeStatus,
    MediaKind,
    RetentionState,
    SubmissionStatus,
)
from tests.api.conftest import auth_headers, make_api_app
from tests.bundle_group_helpers import bundle_kwargs

INTAKE_KEYS = {
    "intake_id",
    "operator",
    "source_kind",
    "artifactclass",
    "label",
    "status",
    "retention_state",
    "purge_status",
    "created_at",
    "updated_at",
    "registered_at",
    "quarantined_at",
    "item_count",
    "bytes_total",
    "archived",
    "archive_state",
    "archiveSemantics",
    "source_release_safe",
    "novelty",
}
INTAKE_DETAIL_KEYS = INTAKE_KEYS | {"items", "derivations"}
INGEST_ITEM_KEYS = {
    "content_sha256",
    "virtual_path",
    "size_bytes",
    "artifactclass",
    "disposition",
}
DERIVATION_KEYS = {"kind", "source_sha256", "derived_sha256"}
BUNDLE_KEYS = {
    "id",
    "artifactclass",
    "status",
    "member_count",
    "total_bytes",
    "copy_count",
    "opened_at",
    "sealed_at",
}
SUBMISSION_KEYS = {
    "id",
    "arrangement_id",
    "artifactclass",
    "member_count",
    "status",
    "submitted_by",
    "submitted_at",
    "archived_at",
}
ASSET_DETAIL_KEYS = {
    "content_sha256",
    "artifactclass",
    "size_bytes",
    "originating_intake_id",
    "copies",
}
COPY_KEYS = {
    "backend_name",
    "backend_kind",
    "tier",
    "pool_id",
    "health",
    "source",
    "representation",
    "last_checked_at",
}
CATALOG_KEYS = {
    "content_sha256",
    "artifactclass",
    "media_kind",
    "size_bytes",
    "copy_count",
    "health_rollup",
    "last_checked_at",
}


def test_intake_contract_detail_uses_virtual_paths_and_cross_intake_derivations(
    api_engine: Engine,
) -> None:
    base = dt.datetime(2026, 7, 4, 8, 0, tzinfo=dt.UTC)
    source_hash = _digest("source")
    derived_hash = _digest("derived")
    with session_scope(api_engine) as session:
        _add_asset(session, source_hash, size=10)
        _add_asset(session, derived_hash, size=20)
        _add_intake(session, "intake-a", artifactclass="s-masters", created_at=base)
        _add_intake(
            session,
            "intake-b",
            artifactclass="s-proxy",
            created_at=base + dt.timedelta(minutes=1),
        )
        source_item = _add_item(
            session,
            intake_id="intake-a",
            digest=source_hash,
            artifactclass="s-masters",
            virtual_path="event/source.mov",
            as_received_path="/mnt/card/private/source.mov",
            created_at=base,
        )
        derived_item = _add_item(
            session,
            intake_id="intake-b",
            digest=derived_hash,
            artifactclass="s-proxy",
            virtual_path="proxy/source.mp4",
            as_received_path="/var/lib/replica/cache/source.mp4",
            created_at=base + dt.timedelta(minutes=1),
        )
        session.add(
            AssetDerivation(
                source_item_id=source_item.id,
                derived_item_id=derived_item.id,
                kind="transcode",
                created_at=base + dt.timedelta(minutes=2),
            )
        )
    client = TestClient(make_api_app(api_engine))

    list_response = client.get(
        "/api/ui/intakes?status=registered&limit=10", headers=auth_headers("viewer")
    )
    detail_response = client.get("/api/ui/intakes/intake-a", headers=auth_headers("viewer"))

    assert list_response.status_code == 200
    list_body = list_response.json()
    assert set(list_body) == {"total", "truncated", "intakes"}
    assert list_body["total"] == 2
    assert list_body["truncated"] is False
    assert [row["intake_id"] for row in list_body["intakes"]] == ["intake-b", "intake-a"]
    assert all(set(row) == INTAKE_KEYS for row in list_body["intakes"])

    assert detail_response.status_code == 200
    body = detail_response.json()
    assert set(body) == INTAKE_DETAIL_KEYS
    assert body["intake_id"] == "intake-a"
    assert body["item_count"] == 1
    assert body["bytes_total"] == 10
    assert body["novelty"] == {
        "total": 1,
        "new": 0,
        "known_durable": 0,
        "known_under_durable": 0,
        "reverified": 0,
        "legacy_unknown": 1,
    }
    assert body["items"] == [
        {
            "content_sha256": source_hash.hex(),
            "virtual_path": "event/source.mov",
            "size_bytes": 10,
            "artifactclass": "s-masters",
            "disposition": "legacy_unknown",
        }
    ]
    assert set(body["items"][0]) == INGEST_ITEM_KEYS
    assert body["derivations"] == [
        {
            "kind": "transcode",
            "source_sha256": source_hash.hex(),
            "derived_sha256": derived_hash.hex(),
        }
    ]
    assert set(body["derivations"][0]) == DERIVATION_KEYS
    payload_text = json.dumps(body)
    assert "as_received_path" not in payload_text
    assert "/mnt/card" not in payload_text
    assert "/var/lib/replica" not in payload_text


def test_intakes_stage_filters_registered_archive_evidence(api_engine: Engine) -> None:
    base = dt.datetime(2026, 7, 4, 8, 0, tzinfo=dt.UTC)
    bundle_hash = _digest("stage-bundle-evidence")
    submission_hash = _digest("stage-submission-evidence")
    unarchived_hash = _digest("stage-no-evidence")
    with session_scope(api_engine) as session:
        _add_asset(session, bundle_hash, size=10)
        _add_asset(session, submission_hash, size=20)
        _add_asset(session, unarchived_hash, size=30)

        _add_intake(
            session,
            "registered-bundle",
            artifactclass="s-masters",
            created_at=base + dt.timedelta(minutes=1),
        )
        _add_item(
            session,
            intake_id="registered-bundle",
            digest=bundle_hash,
            artifactclass="s-masters",
            virtual_path="bundle/clip.mov",
            as_received_path="/card/bundle/clip.mov",
            created_at=base + dt.timedelta(minutes=1),
        )
        _add_intake(
            session,
            "registered-submission",
            artifactclass="s-masters",
            created_at=base + dt.timedelta(minutes=2),
        )
        submission_item = _add_item(
            session,
            intake_id="registered-submission",
            digest=submission_hash,
            artifactclass="s-masters",
            virtual_path="submitted/clip.mov",
            as_received_path="/card/submitted/clip.mov",
            created_at=base + dt.timedelta(minutes=2),
        )
        _add_archived_submission_member(
            session,
            item=submission_item,
            submission_id="stage-submission",
            artifactclass="s-masters",
            created_at=base + dt.timedelta(minutes=3),
        )
        _add_intake(
            session,
            "registered-unarchived",
            artifactclass="s-masters",
            created_at=base + dt.timedelta(minutes=4),
        )
        _add_item(
            session,
            intake_id="registered-unarchived",
            digest=unarchived_hash,
            artifactclass="s-masters",
            virtual_path="plain/clip.mov",
            as_received_path="/card/plain/clip.mov",
            created_at=base + dt.timedelta(minutes=4),
        )
        _add_intake(
            session,
            "quarantined-stage",
            artifactclass="s-masters",
            created_at=base + dt.timedelta(minutes=5),
            status=IntakeStatus.QUARANTINED,
        )
        _add_intake(
            session,
            "verifying-with-evidence",
            artifactclass="s-masters",
            created_at=base + dt.timedelta(minutes=6),
            status=IntakeStatus.VERIFYING,
        )
        _add_item(
            session,
            intake_id="verifying-with-evidence",
            digest=bundle_hash,
            artifactclass="s-masters",
            virtual_path="verifying/clip.mov",
            as_received_path="/card/verifying/clip.mov",
            created_at=base + dt.timedelta(minutes=6),
        )
        bundle = _add_bundle(
            session,
            "stage-sealed-bundle",
            artifactclass="s-masters",
            status="sealed",
            total_bytes=10,
            member_count=1,
            opened_at=base + dt.timedelta(minutes=7),
            sealed_at=base + dt.timedelta(minutes=8),
        )
        _add_bundle_member(session, bundle, bundle_hash, size=10)
    client = TestClient(make_api_app(api_engine))

    archived = client.get("/api/ui/intakes?stage=archived&limit=10", headers=auth_headers("viewer"))
    unarchived = client.get(
        "/api/ui/intakes?stage=registered_unarchived&limit=10",
        headers=auth_headers("viewer"),
    )
    quarantined = client.get(
        "/api/ui/intakes?status=quarantined&limit=10",
        headers=auth_headers("viewer"),
    )
    verifying = client.get(
        "/api/ui/intakes?status=verifying&limit=10",
        headers=auth_headers("viewer"),
    )
    contradictory = client.get(
        "/api/ui/intakes?status=quarantined&stage=archived&limit=10",
        headers=auth_headers("viewer"),
    )
    bad_stage = client.get("/api/ui/intakes?stage=done", headers=auth_headers("viewer"))

    assert archived.status_code == 200
    archived_body = archived.json()
    assert archived_body["total"] == 2
    assert archived_body["truncated"] is False
    assert [row["intake_id"] for row in archived_body["intakes"]] == [
        "registered-submission",
        "registered-bundle",
    ]
    assert all(row["status"] == "registered" for row in archived_body["intakes"])

    assert unarchived.status_code == 200
    assert unarchived.json()["total"] == 1
    assert unarchived.json()["truncated"] is False
    assert [row["intake_id"] for row in unarchived.json()["intakes"]] == ["registered-unarchived"]

    assert quarantined.status_code == 200
    assert quarantined.json()["total"] == 1
    assert quarantined.json()["intakes"][0]["intake_id"] == "quarantined-stage"
    assert verifying.status_code == 200
    assert verifying.json()["total"] == 1
    assert verifying.json()["intakes"][0]["intake_id"] == "verifying-with-evidence"
    assert contradictory.status_code == 200
    assert contradictory.json() == {"total": 0, "truncated": False, "intakes": []}
    assert bad_stage.status_code == 400
    assert bad_stage.json()["detail"]["error"] == "bad_request"


def test_intake_archive_state_none_partial_complete_and_empty(api_engine: Engine) -> None:
    base = dt.datetime(2026, 7, 4, 8, 0, tzinfo=dt.UTC)
    sealed_hash = _digest("archive-state-sealed")
    missing_hash = _digest("archive-state-missing")
    with session_scope(api_engine) as session:
        _add_asset(session, sealed_hash, size=10)
        _add_asset(session, missing_hash, size=20)
        for intake_id in ("none", "partial", "complete", "empty"):
            _add_intake(session, intake_id, artifactclass="s-masters", created_at=base)
        _add_item(
            session,
            intake_id="none",
            digest=missing_hash,
            artifactclass="s-masters",
            virtual_path="none.mov",
            as_received_path="/card/none.mov",
            created_at=base,
        )
        for intake_id in ("partial", "complete"):
            _add_item(
                session,
                intake_id=intake_id,
                digest=sealed_hash,
                artifactclass="s-masters",
                virtual_path=f"{intake_id}/sealed.mov",
                as_received_path=f"/card/{intake_id}/sealed.mov",
                created_at=base,
            )
        _add_item(
            session,
            intake_id="partial",
            digest=missing_hash,
            artifactclass="s-masters",
            virtual_path="partial/missing.mov",
            as_received_path="/card/partial/missing.mov",
            created_at=base,
        )
        bundle = _add_bundle(
            session,
            "archive-state-bundle",
            artifactclass="s-masters",
            status="sealed",
            total_bytes=10,
            member_count=1,
            opened_at=base,
            sealed_at=base,
        )
        _add_bundle_member(session, bundle, sealed_hash, size=10)

    with session_scope(api_engine) as session:
        rows = session.execute(
            select(Intake, intake_archive_state_expr().label("archive_state")).where(
                Intake.intake_id.in_(("none", "partial", "complete", "empty"))
            )
        )
        by_id = {
            intake.intake_id: _intake_payload(
                session,
                intake,
                item_count=len(intake.items),
                bytes_total=sum(item.size_bytes for item in intake.items),
                archive_state=str(archive_state),
                archived=str(archive_state) in {"partial", "complete"},
            )
            for intake, archive_state in rows
        }

    assert by_id["none"]["archive_state"] == "none"
    assert by_id["partial"]["archive_state"] == "partial"
    assert by_id["complete"]["archive_state"] == "complete"
    assert by_id["empty"]["archive_state"] == "none"
    assert by_id["none"]["archived"] is False
    assert by_id["partial"]["archived"] is True
    assert by_id["complete"]["archived"] is True
    assert by_id["empty"]["archived"] is False
    assert all(row["archiveSemantics"] == 2 for row in by_id.values())


def test_archived_rollout_gate_flips_payload_and_stage_filter(
    api_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = dt.datetime(2026, 7, 4, 8, 0, tzinfo=dt.UTC)
    archived_hash = _digest("gate-archived")
    missing_hash = _digest("gate-missing")
    with session_scope(api_engine) as session:
        _add_asset(session, archived_hash, size=10)
        _add_asset(session, missing_hash, size=20)
        _add_intake(session, "gate-partial", artifactclass="s-masters", created_at=base)
        for digest, name in ((archived_hash, "archived"), (missing_hash, "missing")):
            _add_item(
                session,
                intake_id="gate-partial",
                digest=digest,
                artifactclass="s-masters",
                virtual_path=f"{name}.mov",
                as_received_path=f"/card/{name}.mov",
                created_at=base,
            )
        bundle = _add_bundle(
            session,
            "gate-bundle",
            artifactclass="s-masters",
            status="sealed",
            total_bytes=10,
            member_count=1,
            opened_at=base,
            sealed_at=base,
        )
        _add_bundle_member(session, bundle, archived_hash, size=10)
    legacy_client = TestClient(make_api_app(api_engine))

    legacy = legacy_client.get("/api/ui/intakes/gate-partial", headers=auth_headers("viewer"))
    legacy_stage = legacy_client.get(
        "/api/ui/intakes?stage=archived", headers=auth_headers("viewer")
    )

    assert legacy.json()["archive_state"] == "partial"
    assert legacy.json()["archived"] is True
    assert [row["intake_id"] for row in legacy_stage.json()["intakes"]] == ["gate-partial"]

    monkeypatch.setenv("SUTRADHARA_ARCHIVED_ALL_SEMANTICS", "true")
    still_legacy = legacy_client.get(
        "/api/ui/intakes/gate-partial", headers=auth_headers("viewer")
    )
    flipped_client = TestClient(make_api_app(api_engine))
    flipped = flipped_client.get("/api/ui/intakes/gate-partial", headers=auth_headers("viewer"))
    flipped_archived = flipped_client.get(
        "/api/ui/intakes?stage=archived", headers=auth_headers("viewer")
    )
    flipped_unarchived = flipped_client.get(
        "/api/ui/intakes?stage=registered_unarchived", headers=auth_headers("viewer")
    )

    assert still_legacy.json()["archived"] is True
    assert flipped.json()["archive_state"] == "partial"
    assert flipped.json()["archived"] is False
    assert flipped_archived.json()["intakes"] == []
    assert [row["intake_id"] for row in flipped_unarchived.json()["intakes"]] == ["gate-partial"]


def test_gate_off_cross_intake_submission_evidence_keeps_chip_and_filter_aligned(
    api_engine: Engine,
) -> None:
    base = dt.datetime(2026, 7, 4, 8, 0, tzinfo=dt.UTC)
    shared_hash = _digest("cross-intake-archived")
    missing_hash = _digest("cross-intake-missing")
    with session_scope(api_engine) as session:
        _add_asset(session, shared_hash, size=10)
        _add_asset(session, missing_hash, size=20)
        _add_intake(session, "victim", artifactclass="s-masters", created_at=base)
        _add_intake(session, "donor", artifactclass="s-masters", created_at=base)
        _add_item(
            session,
            intake_id="victim",
            digest=shared_hash,
            artifactclass="s-masters",
            virtual_path="victim/shared.mov",
            as_received_path="/card/victim/shared.mov",
            created_at=base,
        )
        _add_item(
            session,
            intake_id="victim",
            digest=missing_hash,
            artifactclass="s-masters",
            virtual_path="victim/missing.mov",
            as_received_path="/card/victim/missing.mov",
            created_at=base,
        )
        donor_item = _add_item(
            session,
            intake_id="donor",
            digest=shared_hash,
            artifactclass="s-masters",
            virtual_path="donor/shared.mov",
            as_received_path="/card/donor/shared.mov",
            created_at=base,
        )
        _add_archived_submission_member(
            session,
            item=donor_item,
            submission_id="cross-intake-submission",
            artifactclass="s-masters",
            created_at=base,
        )

    client = TestClient(make_api_app(api_engine))
    victim = client.get("/api/ui/intakes/victim", headers=auth_headers("viewer"))
    unarchived = client.get(
        "/api/ui/intakes?stage=registered_unarchived", headers=auth_headers("viewer")
    )
    archived = client.get("/api/ui/intakes?stage=archived", headers=auth_headers("viewer"))

    assert victim.status_code == 200
    assert victim.json()["archive_state"] == "partial"
    assert victim.json()["archived"] is False
    assert [row["intake_id"] for row in unarchived.json()["intakes"]] == ["victim"]
    assert all(row["archived"] is False for row in unarchived.json()["intakes"])
    assert [row["intake_id"] for row in archived.json()["intakes"]] == ["donor"]
    assert all(row["archived"] is True for row in archived.json()["intakes"])


def test_invalid_archive_gate_fails_during_app_creation(
    api_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUTRADHARA_ARCHIVED_ALL_SEMANTICS", "enabled")

    with pytest.raises(
        RuntimeError,
        match="invalid configuration: SUTRADHARA_ARCHIVED_ALL_SEMANTICS",
    ):
        make_api_app(api_engine)


def test_archive_bundle_and_submission_contracts_and_status_vocabularies(
    api_engine: Engine,
) -> None:
    base = dt.datetime(2026, 7, 4, 8, 0, tzinfo=dt.UTC)
    digest = _digest("bundle-member")
    with session_scope(api_engine) as session:
        _add_asset(session, digest, size=123)
        backend, pool = _add_backend_pool(session, "rem-main", BackendKind.REM_TAPE, "main-pool")
        bundle = _add_bundle(
            session,
            "bundle-1",
            artifactclass="s-masters",
            status="sealed",
            total_bytes=123,
            member_count=1,
            opened_at=base,
            sealed_at=base + dt.timedelta(minutes=5),
        )
        _add_bundle_member(session, bundle, digest, size=123)
        _add_bundle_copy(
            session,
            bundle,
            backend,
            pool,
            locator={"media_id": "VOL001", "object_id": "bundle-1"},
        )
        _add_submission_fixture(
            session,
            intake_id="submission-intake",
            submission_id="sub-1",
            artifactclass="s-masters",
            status=SubmissionStatus.PENDING_ARCHIVE,
            created_at=base,
        )
    client = TestClient(make_api_app(api_engine))

    bundles = client.get(
        "/api/ui/archive/bundles?artifactclass=s-masters&status=sealed&limit=10",
        headers=auth_headers("viewer"),
    )
    submissions = client.get(
        "/api/ui/archive/submissions?status=pending_archive&limit=10",
        headers=auth_headers("viewer"),
    )
    bad_bundle_status = client.get(
        "/api/ui/archive/bundles?status=deleted",
        headers=auth_headers("viewer"),
    )
    bad_submission_status = client.get(
        "/api/ui/archive/submissions?status=flushing",
        headers=auth_headers("viewer"),
    )

    assert bundles.status_code == 200
    bundle_body = bundles.json()
    assert set(bundle_body) == {"total", "truncated", "bundles"}
    assert bundle_body["total"] == 1
    assert bundle_body["truncated"] is False
    assert set(bundle_body["bundles"][0]) == BUNDLE_KEYS
    assert bundle_body["bundles"][0]["status"] == "sealed"
    assert bundle_body["bundles"][0]["copy_count"] == 1

    assert submissions.status_code == 200
    submission_body = submissions.json()
    assert set(submission_body) == {"total", "truncated", "submissions"}
    assert submission_body["total"] == 1
    assert submission_body["truncated"] is False
    assert set(submission_body["submissions"][0]) == SUBMISSION_KEYS
    assert submission_body["submissions"][0]["status"] == "pending_archive"

    assert bad_bundle_status.status_code == 400
    assert bad_bundle_status.json()["detail"]["error"] == "bad_request"
    assert bad_submission_status.status_code == 400
    assert bad_submission_status.json()["detail"]["error"] == "bad_request"


def test_archive_asset_uses_asset_locator_origin_rule_and_locator_shaping(
    api_engine: Engine,
) -> None:
    base = dt.datetime(2026, 7, 4, 8, 0, tzinfo=dt.UTC)
    digest = _digest("locator-only-asset")
    with session_scope(api_engine) as session:
        _add_asset(session, digest, size=456)
        _add_intake(session, "origin-old", artifactclass="s-masters", created_at=base)
        _add_item(
            session,
            intake_id="origin-old",
            digest=digest,
            artifactclass="s-masters",
            virtual_path="old/clip.mov",
            as_received_path="/card/a/clip.mov",
            created_at=base,
        )
        _add_intake(
            session,
            "origin-new",
            artifactclass="s-proxy",
            created_at=base + dt.timedelta(hours=1),
        )
        _add_item(
            session,
            intake_id="origin-new",
            digest=digest,
            artifactclass="s-proxy",
            virtual_path="new/clip.mp4",
            as_received_path="/card/b/clip.mp4",
            created_at=base + dt.timedelta(hours=1),
        )
        backend, pool = _add_backend_pool(session, "rem-main", BackendKind.REM_TAPE, "main-pool")
        bundle = _add_bundle(
            session,
            "bundle-locator-only",
            artifactclass="s-masters",
            status="sealed",
            total_bytes=456,
            member_count=1,
            opened_at=base,
        )
        _add_bundle_member(session, bundle, digest, size=456)
        copy = _add_bundle_copy(
            session,
            bundle,
            backend,
            pool,
            locator={"media_id": "VOL001", "object_path": "/var/lib/replica/private/bundle.rao"},
            health=CopyHealth.SUSPECT,
            last_checked_at=base + dt.timedelta(hours=2),
        )
        session.add(
            AssetLocator(
                logical_asset_hash=digest,
                pool_id=pool.id,
                copy_id=copy.id,
                bundle_id=bundle.id,
                native_locator={"first_chunk_lba": 7, "debug_path": "/srv/archive/member.mov"},
                member_path="clip.mov",
                representation="RAO_PLAIN",
                created_at=base + dt.timedelta(hours=2),
            )
        )
        direct_copy_count = session.scalar(
            select(func.count()).select_from(Copy).where(Copy.logical_asset_hash == digest)
        )
        assert direct_copy_count == 0
    client = TestClient(make_api_app(api_engine))

    admin_response = client.get(
        f"/api/ui/archive/assets/{digest.hex()}",
        headers=auth_headers("admin"),
    )
    viewer_response = client.get(
        f"/api/ui/archive/assets/{digest.hex()}",
        headers=auth_headers("viewer"),
    )

    assert admin_response.status_code == 200
    admin_body = admin_response.json()
    assert set(admin_body) == ASSET_DETAIL_KEYS
    assert admin_body["originating_intake_id"] == "origin-new"
    assert admin_body["artifactclass"] == "s-proxy"
    assert len(admin_body["copies"]) == 1
    admin_copy = admin_body["copies"][0]
    assert set(admin_copy) == COPY_KEYS | {"locator_summary"}
    assert admin_copy["backend_kind"] == "rem_tape"
    assert admin_copy["tier"] == "self_describing"
    assert admin_copy["health"] == "suspect"
    assert admin_copy["representation"] == "RAO_PLAIN"
    assert admin_copy["locator_summary"] == "media VOL001"
    admin_text = json.dumps(admin_body)
    assert "/var/lib" not in admin_text
    assert "/srv/archive" not in admin_text

    assert viewer_response.status_code == 200
    viewer_body = viewer_response.json()
    viewer_copy = viewer_body["copies"][0]
    assert set(viewer_copy) == COPY_KEYS
    assert "locator_summary" not in viewer_copy
    assert viewer_copy["backend_kind"] == "rem_tape"
    assert viewer_copy["tier"] == "self_describing"
    assert viewer_copy["health"] == "suspect"


def test_catalog_assets_are_asset_class_pairs_with_offset_paging(api_engine: Engine) -> None:
    base = dt.datetime(2026, 7, 4, 8, 0, tzinfo=dt.UTC)
    digest = _digest("multi-class-asset")
    with session_scope(api_engine) as session:
        _add_asset(session, digest, size=99, media_kind=MediaKind.VIDEO)
        _add_intake(session, "catalog-intake", artifactclass="s-masters", created_at=base)
        _add_item(
            session,
            intake_id="catalog-intake",
            digest=digest,
            artifactclass="s-masters",
            virtual_path="masters/clip.mov",
            as_received_path="/card/clip.mov",
            created_at=base + dt.timedelta(minutes=1),
        )
        backend, pool = _add_backend_pool(session, "disk-main", BackendKind.SSH_DISK, "disk-pool")
        bundle = _add_bundle(
            session,
            "proxy-bundle",
            artifactclass="s-proxy",
            status="sealed",
            total_bytes=99,
            member_count=1,
            opened_at=base + dt.timedelta(minutes=2),
        )
        _add_bundle_member(
            session,
            bundle,
            digest,
            size=99,
            member_path="proxies/clip.mp4",
            added_at=base + dt.timedelta(minutes=2),
        )
        copy = _add_bundle_copy(
            session,
            bundle,
            backend,
            pool,
            locator={"key": "/srv/disk/private/proxy-bundle"},
            health=CopyHealth.OK,
            last_checked_at=base + dt.timedelta(minutes=3),
        )
        session.add(
            AssetLocator(
                logical_asset_hash=digest,
                pool_id=pool.id,
                copy_id=copy.id,
                bundle_id=bundle.id,
                native_locator={"opaque_key": "/srv/disk/private/proxies/clip.mp4"},
                member_path="proxies/clip.mp4",
                representation="RAW",
                created_at=base + dt.timedelta(minutes=3),
            )
        )
    client = TestClient(make_api_app(api_engine))

    full = client.get(
        f"/api/ui/catalog/assets?q={digest.hex()[:12]}&limit=10",
        headers=auth_headers("viewer"),
    )
    first = client.get(
        f"/api/ui/catalog/assets?q={digest.hex()[:12]}&limit=1&offset=0",
        headers=auth_headers("viewer"),
    )
    second = client.get(
        f"/api/ui/catalog/assets?q={digest.hex()[:12]}&limit=1&offset=1",
        headers=auth_headers("viewer"),
    )
    filtered = client.get(
        f"/api/ui/catalog/assets?q={digest.hex()[:12]}&artifactclass=s-proxy&limit=10",
        headers=auth_headers("viewer"),
    )

    assert full.status_code == 200
    full_body = full.json()
    assert set(full_body) == {"total", "truncated", "assets"}
    assert full_body["total"] == 2
    assert full_body["truncated"] is False
    assert {row["artifactclass"] for row in full_body["assets"]} == {"s-masters", "s-proxy"}
    assert all(row["content_sha256"] == digest.hex() for row in full_body["assets"])
    assert all(set(row) == CATALOG_KEYS for row in full_body["assets"])
    assert all(row["copy_count"] == 1 for row in full_body["assets"])
    assert all(row["health_rollup"] == "ok" for row in full_body["assets"])
    assert all(row["media_kind"] == "video" for row in full_body["assets"])

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["total"] == 2
    assert first.json()["truncated"] is True
    assert second.json()["truncated"] is False
    assert first.json()["assets"][0] != second.json()["assets"][0]
    assert [first.json()["assets"][0], second.json()["assets"][0]] == full_body["assets"]

    assert filtered.status_code == 200
    assert filtered.json()["total"] == 1
    assert filtered.json()["assets"][0]["artifactclass"] == "s-proxy"


def test_p4_read_models_require_can_view(api_engine: Engine) -> None:
    client = TestClient(make_api_app(api_engine))
    digest = "00" * 32

    for path in (
        "/api/ui/intakes",
        "/api/ui/intakes?stage=archived",
        "/api/ui/intakes/any",
        "/api/ui/archive/bundles",
        "/api/ui/archive/submissions",
        f"/api/ui/archive/assets/{digest}",
        "/api/ui/catalog/assets",
    ):
        response = client.get(path, headers=auth_headers("restore-p2"))
        assert response.status_code == 403
        assert response.json() == {
            "detail": {"error": "forbidden", "detail": "operator has no sutradhara role"}
        }


def test_intakes_cap_limit_with_total_and_truncated(api_engine: Engine) -> None:
    base = dt.datetime(2026, 7, 4, 8, 0, tzinfo=dt.UTC)
    with session_scope(api_engine) as session:
        for index in range(MAX_LIMIT + 5):
            _add_intake(
                session,
                f"bulk-{index:03d}",
                artifactclass="s-masters",
                created_at=base + dt.timedelta(seconds=index),
            )
    client = TestClient(make_api_app(api_engine))

    response = client.get("/api/ui/intakes?limit=999", headers=auth_headers("viewer"))

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == MAX_LIMIT + 5
    assert body["truncated"] is True
    assert len(body["intakes"]) == MAX_LIMIT
    assert body["intakes"][0]["intake_id"] == f"bulk-{MAX_LIMIT + 4:03d}"
    assert body["intakes"][-1]["intake_id"] == "bulk-005"


def _digest(label: str) -> bytes:
    return hashlib.sha256(label.encode("utf-8")).digest()


def _add_asset(
    session: Any,
    digest: bytes,
    *,
    size: int,
    media_kind: MediaKind | None = None,
) -> LogicalAsset:
    asset = LogicalAsset(content_sha256=digest, size_bytes=size, media_kind=media_kind)
    session.add(asset)
    session.flush([asset])
    return asset


def _add_intake(
    session: Any,
    intake_id: str,
    *,
    artifactclass: str,
    created_at: dt.datetime,
    status: IntakeStatus = IntakeStatus.REGISTERED,
) -> Intake:
    intake = Intake(
        intake_id=intake_id,
        operator="ada",
        source_kind=IntakeSourceKind.CARD,
        source_ref="card-1",
        artifactclass=artifactclass,
        label=f"Label {intake_id}",
        status=status,
        retention_state=RetentionState.HELD,
        created_at=created_at,
        updated_at=created_at,
        registered_at=created_at if status == IntakeStatus.REGISTERED else None,
    )
    session.add(intake)
    session.flush([intake])
    return intake


def _add_item(
    session: Any,
    *,
    intake_id: str,
    digest: bytes,
    artifactclass: str,
    virtual_path: str,
    as_received_path: str,
    created_at: dt.datetime,
) -> IngestItem:
    item = IngestItem(
        intake_id=intake_id,
        logical_asset_hash=digest,
        as_received_path=as_received_path,
        virtual_path=virtual_path,
        size_bytes=session.get(LogicalAsset, digest).size_bytes,
        artifactclass=artifactclass,
        item_metadata={"source_path": as_received_path},
        created_at=created_at,
    )
    session.add(item)
    session.flush([item])
    return item


def _add_backend_pool(
    session: Any,
    backend_name: str,
    kind: BackendKind,
    pool_id: str,
) -> tuple[Backend, Pool]:
    backend = Backend(
        name=backend_name,
        kind=kind,
        tier=BackendTier.SELF_DESCRIBING,
    )
    session.add(backend)
    session.flush([backend])
    pool = Pool(
        id=pool_id,
        backend_id=backend.id,
        representation="RAO_PLAIN",
        location="test",
        tier="archive",
    )
    session.add(pool)
    session.flush([pool])
    return backend, pool


def _add_bundle(
    session: Any,
    bundle_id: str,
    *,
    artifactclass: str,
    status: str,
    total_bytes: int,
    member_count: int,
    opened_at: dt.datetime,
    sealed_at: dt.datetime | None = None,
) -> Bundle:
    bundle = Bundle(
        id=bundle_id,
        **bundle_kwargs(seed=artifactclass),
        status=status,
        total_bytes=total_bytes,
        member_count=member_count,
        opened_at=opened_at,
        sealed_at=sealed_at,
    )
    session.add(bundle)
    session.flush([bundle])
    # The class moved to member grain; remember it for _add_bundle_member.
    bundle._test_artifactclass = artifactclass
    return bundle


def _add_bundle_member(
    session: Any,
    bundle: Bundle,
    digest: bytes,
    *,
    size: int,
    member_path: str = "clip.mov",
    added_at: dt.datetime | None = None,
) -> BundleMember:
    member = BundleMember(
        bundle_id=bundle.id,
        logical_asset_hash=digest,
        artifactclass=getattr(bundle, "_test_artifactclass", "s-masters"),
        member_path=member_path,
        source_path="/private/source/clip.mov",
        size_bytes=size,
        file_sha256=digest,
        source_metadata={"source_path": "/private/source/clip.mov"},
        added_at=added_at or bundle.opened_at,
    )
    session.add(member)
    session.flush([member])
    return member


def _add_bundle_copy(
    session: Any,
    bundle: Bundle,
    backend: Backend,
    pool: Pool,
    *,
    locator: dict[str, Any],
    health: CopyHealth = CopyHealth.OK,
    last_checked_at: dt.datetime | None = None,
) -> Copy:
    copy = Copy(
        bundle_id=bundle.id,
        backend_id=backend.id,
        pool_id=pool.id,
        native_locator=locator,
        native_locator_key=locator_key(locator),
        storage_metadata={},
        integrity_hash=_digest(f"copy-{bundle.id}-{pool.id}"),
        health=health,
        source=CopySource.INGEST,
        last_checked_at=last_checked_at,
    )
    session.add(copy)
    session.flush([copy])
    return copy


def _add_archived_submission_member(
    session: Any,
    *,
    item: IngestItem,
    submission_id: str,
    artifactclass: str,
    created_at: dt.datetime,
) -> Submission:
    arrangement = Arrangement(
        label=f"Arrangement {submission_id}",
        intake_id=item.intake_id,
        artifactclass=artifactclass,
        status=ArrangementStatus.SUBMITTED,
        created_at=created_at,
        updated_at=created_at,
        submitted_at=created_at,
    )
    session.add(arrangement)
    session.flush([arrangement])
    submission = Submission(
        id=submission_id,
        arrangement_id=arrangement.id,
        artifactclass=artifactclass,
        source_map_path=f"/var/lib/replica/submissions/{submission_id}/source-map.tsv",
        manifest_digest="b" * 64,
        member_count=1,
        status=SubmissionStatus.ARCHIVED,
        submitted_by="ada",
        submitted_at=created_at,
        archived_at=created_at,
    )
    session.add(submission)
    session.flush([submission])
    session.add(
        SubmissionMember(
            submission_id=submission_id,
            ingest_item_id=item.id,
            archive_path=item.virtual_path,
            source_path=str(item.item_metadata["source_path"]),
            sha256=item.logical_asset_hash,
            size_bytes=item.size_bytes,
            ord=0,
        )
    )
    arrangement.submission_id = submission_id
    session.flush()
    return submission


def _add_submission_fixture(
    session: Any,
    *,
    intake_id: str,
    submission_id: str,
    artifactclass: str,
    status: SubmissionStatus,
    created_at: dt.datetime,
) -> Submission:
    _add_intake(session, intake_id, artifactclass=artifactclass, created_at=created_at)
    arrangement = Arrangement(
        label="submission arrangement",
        intake_id=intake_id,
        artifactclass=artifactclass,
        status=ArrangementStatus.SUBMITTED,
        created_at=created_at,
        updated_at=created_at,
        submitted_at=created_at,
    )
    session.add(arrangement)
    session.flush([arrangement])
    submission = Submission(
        id=submission_id,
        arrangement_id=arrangement.id,
        artifactclass=artifactclass,
        source_map_path="/var/lib/replica/submissions/sub-1/source-map.tsv",
        manifest_digest="a" * 64,
        member_count=1,
        status=status,
        submitted_by="ada",
        submitted_at=created_at,
        archived_at=None if status == SubmissionStatus.PENDING_ARCHIVE else created_at,
    )
    session.add(submission)
    session.flush([submission])
    return submission
