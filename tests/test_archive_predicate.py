"""Phase-1c archive predicate, audit, and pilot-scale query-plan tests."""

from __future__ import annotations

import datetime as dt
import hashlib

import pytest
from sqlalchemy import event, select, text

from sutradhara.archive_predicate import (
    archived_all_semantics_enabled,
    build_archive_predicate_audit,
    intake_archive_state_expr,
    legacy_archived_expr,
)
from sutradhara.catalog.models import (
    Arrangement,
    ArtifactClass,
    Bundle,
    BundleMember,
    IngestItem,
    Intake,
    LogicalAsset,
    Submission,
    SubmissionMember,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import (
    ArrangementStatus,
    IntakeSourceKind,
    IntakeStatus,
    RetentionState,
    SubmissionStatus,
)


def test_rollout_gate_defaults_off_and_rejects_ambiguous_values() -> None:
    assert archived_all_semantics_enabled({}) is False
    assert archived_all_semantics_enabled({"SUTRADHARA_ARCHIVED_ALL_SEMANTICS": "yes"}) is True
    assert archived_all_semantics_enabled({"SUTRADHARA_ARCHIVED_ALL_SEMANTICS": "OFF"}) is False
    with pytest.raises(ValueError, match="SUTRADHARA_ARCHIVED_ALL_SEMANTICS"):
        archived_all_semantics_enabled({"SUTRADHARA_ARCHIVED_ALL_SEMANTICS": "enabled"})


def test_audit_reports_partial_retention_passed_intakes_without_mutating(tmp_path) -> None:
    engine = make_engine(f"sqlite:///{tmp_path / 'audit.db'}")
    create_all(engine)
    archived_hash = _digest("archived")
    missing_hash = _digest("missing")
    now = dt.datetime(2026, 7, 11, 9, 0, tzinfo=dt.UTC)
    with session_scope(engine) as session:
        session.add(ArtifactClass(name="s-masters"))
        for digest in (archived_hash, missing_hash):
            session.add(LogicalAsset(content_sha256=digest, size_bytes=10))
        intake = Intake(
            intake_id="released-partial",
            operator="ada",
            source_kind=IntakeSourceKind.CARD,
            artifactclass="s-masters",
            status=IntakeStatus.REGISTERED,
            retention_state=RetentionState.RELEASED,
            released_at=now,
            created_at=now,
            updated_at=now,
            registered_at=now,
        )
        session.add(intake)
        session.flush()
        for index, digest in enumerate((archived_hash, missing_hash)):
            session.add(
                IngestItem(
                    intake_id=intake.intake_id,
                    logical_asset_hash=digest,
                    as_received_path=f"clip-{index}.mov",
                    virtual_path=f"clip-{index}.mov",
                    size_bytes=10,
                    artifactclass="s-masters",
                    item_metadata={},
                    created_at=now,
                )
            )
        bundle = Bundle(
            id="sealed",
            artifactclass="s-masters",
            status="sealed",
            total_bytes=10,
            member_count=1,
            target_bytes=10,
            max_age_seconds=60,
            opened_at=now,
            sealed_at=now,
        )
        session.add(bundle)
        session.flush()
        session.add(
            BundleMember(
                bundle_id=bundle.id,
                logical_asset_hash=archived_hash,
                member_path="archived.mov",
                size_bytes=10,
                file_sha256=archived_hash,
                added_at=now,
            )
        )

    generated_at = dt.datetime(2026, 7, 11, 10, 0, tzinfo=dt.UTC)
    with session_scope(engine) as session:
        state, legacy_archived, flipped_archived = session.execute(
            select(
                intake_archive_state_expr(),
                legacy_archived_expr(all_semantics=False),
                legacy_archived_expr(all_semantics=True),
            ).where(Intake.intake_id == "released-partial")
        ).one()
        report = build_archive_predicate_audit(session, generated_at=generated_at)
        unchanged = session.get(Intake, "released-partial")
        assert unchanged is not None
        assert unchanged.retention_state == RetentionState.RELEASED

    assert state == "partial"
    assert legacy_archived is True
    assert flipped_archived is False
    assert report["schema"] == "sutradhara.archive-predicate-audit/v1"
    assert report["generated_at"] == "2026-07-11T10:00:00+00:00"
    assert report["summary"] == {
        "audited_intakes": 1,
        "affected_intakes": 1,
        "missing_distinct_assets": 1,
        "gate_safe": False,
    }
    affected = report["affected_intakes"]
    assert isinstance(affected, list)
    assert affected == [
        {
            "intake_id": "released-partial",
            "retention_state": "released",
            "released_at": "2026-07-11T09:00:00+00:00",
            "staging_deleted_at": None,
            "archive_state": "partial",
            "legacy_archived": True,
            "flipped_archived": False,
            "repair_action": "normal_archive_pipeline",
            "missing_assets": [
                {
                    "content_sha256": missing_hash.hex(),
                    "artifactclass": "s-masters",
                    "occurrence_count": 1,
                }
            ],
        }
    ]


def test_audit_catches_released_partial_from_cross_intake_submission_evidence(tmp_path) -> None:
    engine = make_engine(f"sqlite:///{tmp_path / 'cross-intake-audit.db'}")
    create_all(engine)
    shared_hash = _digest("cross-intake-shared")
    missing_hash = _digest("cross-intake-missing")
    now = dt.datetime(2026, 7, 11, 9, 0, tzinfo=dt.UTC)
    with session_scope(engine) as session:
        session.add(ArtifactClass(name="s-masters"))
        for digest in (shared_hash, missing_hash):
            session.add(LogicalAsset(content_sha256=digest, size_bytes=10))
        victim = Intake(
            intake_id="victim",
            operator="ada",
            source_kind=IntakeSourceKind.CARD,
            artifactclass="s-masters",
            status=IntakeStatus.REGISTERED,
            retention_state=RetentionState.RELEASED,
            released_at=now,
            created_at=now,
            updated_at=now,
            registered_at=now,
        )
        donor = Intake(
            intake_id="donor",
            operator="ada",
            source_kind=IntakeSourceKind.CARD,
            artifactclass="s-masters",
            status=IntakeStatus.REGISTERED,
            retention_state=RetentionState.HELD,
            created_at=now,
            updated_at=now,
            registered_at=now,
        )
        session.add_all((victim, donor))
        session.flush()
        victim_shared = IngestItem(
            intake_id="victim",
            logical_asset_hash=shared_hash,
            as_received_path="victim/shared.mov",
            virtual_path="victim/shared.mov",
            size_bytes=10,
            artifactclass="s-masters",
            item_metadata={},
            created_at=now,
        )
        victim_missing = IngestItem(
            intake_id="victim",
            logical_asset_hash=missing_hash,
            as_received_path="victim/missing.mov",
            virtual_path="victim/missing.mov",
            size_bytes=10,
            artifactclass="s-masters",
            item_metadata={},
            created_at=now,
        )
        donor_shared = IngestItem(
            intake_id="donor",
            logical_asset_hash=shared_hash,
            as_received_path="donor/shared.mov",
            virtual_path="donor/shared.mov",
            size_bytes=10,
            artifactclass="s-masters",
            item_metadata={},
            created_at=now,
        )
        session.add_all((victim_shared, victim_missing, donor_shared))
        session.flush()
        arrangement = Arrangement(
            label="Donor arrangement",
            intake_id="donor",
            artifactclass="s-masters",
            status=ArrangementStatus.SUBMITTED,
            created_at=now,
            updated_at=now,
            submitted_at=now,
        )
        session.add(arrangement)
        session.flush()
        submission = Submission(
            id="donor-submission",
            arrangement_id=arrangement.id,
            artifactclass="s-masters",
            source_map_path="/tmp/donor-source-map.tsv",
            manifest_digest="d" * 64,
            member_count=1,
            status=SubmissionStatus.ARCHIVED,
            submitted_by="ada",
            submitted_at=now,
            archived_at=now,
        )
        session.add(submission)
        session.flush()
        session.add(
            SubmissionMember(
                submission_id=submission.id,
                ingest_item_id=donor_shared.id,
                archive_path="donor/shared.mov",
                source_path="donor/shared.mov",
                sha256=shared_hash,
                size_bytes=10,
                ord=0,
            )
        )
        arrangement.submission_id = submission.id

    with session_scope(engine) as session:
        report = build_archive_predicate_audit(session, generated_at=now)

    assert report["summary"] == {
        "audited_intakes": 1,
        "affected_intakes": 1,
        "missing_distinct_assets": 1,
        "gate_safe": False,
    }
    affected = report["affected_intakes"]
    assert isinstance(affected, list)
    assert affected[0]["intake_id"] == "victim"
    assert affected[0]["archive_state"] == "partial"
    assert affected[0]["legacy_archived"] is False
    assert affected[0]["missing_assets"] == [
        {
            "content_sha256": missing_hash.hex(),
            "artifactclass": "s-masters",
            "occurrence_count": 1,
        }
    ]


def test_all_predicate_uses_indexes_in_one_query_at_pilot_scale(tmp_path) -> None:
    """Exercise 400 intakes and 100,000 memberships without per-intake SQL."""

    engine = make_engine(f"sqlite:///{tmp_path / 'pilot.db'}")
    create_all(engine)
    archived_hash = _digest("pilot-archived")
    missing_hash = _digest("pilot-missing")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "INSERT INTO artifactclass (name, created_at) VALUES "
            "('s-masters', CURRENT_TIMESTAMP)"
        )
        connection.exec_driver_sql(
            "INSERT INTO logical_asset "
            "(content_sha256, size_bytes, first_seen_at, validity) VALUES "
            "(?, 1, CURRENT_TIMESTAMP, 'ok'), (?, 1, CURRENT_TIMESTAMP, 'ok')",
            (archived_hash, missing_hash),
        )
        connection.exec_driver_sql(
            "WITH RECURSIVE seq(n) AS (VALUES(0) UNION ALL SELECT n + 1 FROM seq WHERE n < 399) "
            "INSERT INTO intake "
            "(intake_id, operator, source_kind, artifactclass, status, created_at, updated_at, "
            "registered_at, retention_state) "
            "SELECT printf('pilot-%03d', n), 'pilot', 'card', 's-masters', 'registered', "
            "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'held' FROM seq"
        )
        connection.exec_driver_sql(
            "WITH RECURSIVE seq(n) AS (VALUES(0) UNION ALL SELECT n + 1 FROM seq WHERE n < 99999) "
            "INSERT INTO ingest_item "
            "(intake_id, logical_asset_hash, as_received_path, virtual_path, size_bytes, "
            "artifactclass, metadata, created_at) "
            "SELECT printf('pilot-%03d', n / 250), CASE WHEN n % 2 = 0 THEN ? ELSE ? END, "
            "printf('source/%06d', n), printf('virtual/%06d', n), 1, 's-masters', '{}', "
            "CURRENT_TIMESTAMP FROM seq",
            (archived_hash, missing_hash),
        )
        connection.exec_driver_sql(
            "INSERT INTO bundle "
            "(id, artifactclass, status, total_bytes, member_count, target_bytes, "
            "max_age_seconds, opened_at, sealed_at) VALUES "
            "('pilot-sealed', 's-masters', 'sealed', 1, 1, 1, 60, CURRENT_TIMESTAMP, "
            "CURRENT_TIMESTAMP)"
        )
        connection.exec_driver_sql(
            "INSERT INTO bundle_member "
            "(bundle_id, logical_asset_hash, member_path, size_bytes, file_sha256, added_at) "
            "VALUES ('pilot-sealed', ?, 'archived', 1, ?, CURRENT_TIMESTAMP)",
            (archived_hash, archived_hash),
        )

    query = select(Intake.intake_id, intake_archive_state_expr().label("archive_state")).order_by(
        Intake.intake_id
    )
    statement_count = 0

    def count_statement(*_args: object) -> None:
        nonlocal statement_count
        statement_count += 1

    event.listen(engine, "before_cursor_execute", count_statement)
    try:
        with session_scope(engine) as session:
            plan = session.execute(
                text(
                    "EXPLAIN QUERY PLAN "
                    + str(query.compile(engine, compile_kwargs={"literal_binds": True}))
                )
            ).all()
            statement_count = 0
            rows = list(session.execute(query))
    finally:
        event.remove(engine, "before_cursor_execute", count_statement)

    plan_text = " ".join(str(row[-1]) for row in plan)
    assert "ix_ingest_item_intake_hash" in plan_text
    assert "ix_bundle_member_logical_asset_hash" in plan_text
    assert "SCAN ingest_item" not in plan_text
    assert statement_count == 1
    assert len(rows) == 400
    assert {state for _intake_id, state in rows} == {"partial"}


def _digest(value: str) -> bytes:
    return hashlib.sha256(value.encode()).digest()
