"""The derived archive predicate, the audit, and its pilot-scale query plan.

There is one definition of archive evidence: a **sealed** bundle carrying the
member, whose **verified** copies meet that member's own class ``min_copies``.
Each test below names the wrong answer it guards against, and three of them
guard the deliberate tightening this arc introduced — a stored submission flag,
a sealed-but-unmeasured bundle, and a sealed-but-under-replicated bundle all
used to count as archived and no longer do.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import subprocess
from pathlib import Path

from sqlalchemy import event, select, text
from sqlalchemy.orm import Session

from sutradhara.archive_predicate import (
    build_archive_predicate_audit,
    intake_archive_state_expr,
    intake_archived_expr,
    submission_is_archived,
)
from sutradhara.catalog.models import (
    Arrangement,
    ArtifactClassPolicyRecord,
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
from sutradhara.catalog.session import create_all, locator_key, make_engine, session_scope
from sutradhara.catalog.types import (
    ArrangementStatus,
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
    IntakeSourceKind,
    IntakeStatus,
    RetentionState,
    SubmissionStatus,
)
from tests.bundle_group_helpers import bundle_kwargs

REPO_ROOT = Path(__file__).resolve().parents[1]


def _digest(value: str) -> bytes:
    return hashlib.sha256(value.encode()).digest()


def _policy(session: Session, artifactclass: str, *, min_copies: int = 1) -> None:
    record = session.get(ArtifactClassPolicyRecord, artifactclass)
    if record is None:
        record = ArtifactClassPolicyRecord(
            artifactclass=artifactclass,
            ruleset=f"{artifactclass}.rules",
            expect="messy",
            target_bytes=1024,
            max_age_seconds=3600,
            restore_preference=[],
            min_impl_families=1,
        )
        session.add(record)
    record.min_copies = min_copies
    session.flush()


def _verified_copies(
    session: Session,
    bundle: Bundle,
    *,
    count: int = 1,
    measured: bool = True,
    health: CopyHealth = CopyHealth.OK,
) -> None:
    """Attach copies to a bundle; ``measured`` controls whether they are verified."""
    for index in range(count):
        backend = Backend(
            name=f"backend-{bundle.id}-{index}",
            kind=BackendKind.MEMORY,
            tier=BackendTier.CATALOG_AUTHORITATIVE,
        )
        session.add(backend)
        session.flush()
        pool = Pool(id=f"pool-{bundle.id}-{index}", backend_id=backend.id, representation="raw")
        session.add(pool)
        session.flush()
        locator = {"object_id": f"{bundle.id}-{index}"}
        copy = Copy(
            bundle_id=bundle.id,
            backend_id=backend.id,
            pool_id=pool.id,
            native_locator=locator,
            native_locator_key=locator_key(locator),
            storage_metadata={},
            integrity_hash=_digest(f"copy-{bundle.id}-{index}"),
            health=health,
            source=CopySource.INGEST,
        )
        if measured:
            copy.last_measured_digest = copy.integrity_hash
            copy.last_measured_at = dt.datetime.now(dt.UTC)
            copy.last_checked_at = copy.last_measured_at
        session.add(copy)
    session.flush()


def _sealed_bundle(
    session: Session,
    bundle_id: str,
    *,
    artifactclass: str,
    digest: bytes,
    now: dt.datetime,
    status: str = "sealed",
) -> Bundle:
    bundle = Bundle(
        id=bundle_id,
        **bundle_kwargs(seed=bundle_id),
        status=status,
        total_bytes=10,
        member_count=1,
        target_bytes=10,
        max_age_seconds=60,
        opened_at=now,
        sealed_at=now if status == "sealed" else None,
    )
    session.add(bundle)
    session.flush()
    session.add(
        BundleMember(
            bundle_id=bundle.id,
            logical_asset_hash=digest,
            artifactclass=artifactclass,
            member_path=f"{bundle_id}.mov",
            size_bytes=10,
            file_sha256=digest,
            added_at=now,
        )
    )
    session.flush()
    return bundle


def _intake_with_items(
    session: Session,
    intake_id: str,
    *,
    digests: tuple[bytes, ...],
    now: dt.datetime,
    retention_state: RetentionState = RetentionState.RELEASED,
) -> Intake:
    intake = Intake(
        intake_id=intake_id,
        operator="ada",
        source_kind=IntakeSourceKind.CARD,
        artifactclass="s-masters",
        status=IntakeStatus.REGISTERED,
        retention_state=retention_state,
        released_at=now if retention_state == RetentionState.RELEASED else None,
        created_at=now,
        updated_at=now,
        registered_at=now,
    )
    session.add(intake)
    session.flush()
    for index, digest in enumerate(digests):
        session.add(
            IngestItem(
                intake_id=intake_id,
                logical_asset_hash=digest,
                as_received_path=f"{intake_id}/clip-{index}.mov",
                virtual_path=f"{intake_id}/clip-{index}.mov",
                size_bytes=10,
                artifactclass="s-masters",
                item_metadata={},
                created_at=now,
            )
        )
    session.flush()
    return intake


# --------------------------------------------------------------------------
# What counts as evidence
# --------------------------------------------------------------------------


def test_open_bundle_membership_is_not_archive_evidence(tmp_path: Path) -> None:
    """Guards the source-erasure inversion this arc exists to close.

    Material sitting in an OPEN accumulator has not been written anywhere. If
    membership alone counted, a submission would go ``accumulated`` and its
    card would become releasable while the only copy of the footage was still
    the card. Only ``sealed`` counts.
    """
    engine = make_engine(f"sqlite:///{tmp_path / 'open.db'}")
    create_all(engine)
    now = dt.datetime(2026, 8, 5, 9, 0, tzinfo=dt.UTC)
    digest = _digest("open-member")
    with session_scope(engine) as session:
        _policy(session, "s-masters")
        session.add(LogicalAsset(content_sha256=digest, size_bytes=10))
        _intake_with_items(session, "open-intake", digests=(digest,), now=now)
        bundle = _sealed_bundle(
            session,
            "open-bundle",
            artifactclass="s-masters",
            digest=digest,
            now=now,
            status="open",
        )
        _verified_copies(session, bundle)

        state = session.scalar(
            select(intake_archive_state_expr()).where(Intake.intake_id == "open-intake")
        )
        archived = session.scalar(
            select(intake_archived_expr()).where(Intake.intake_id == "open-intake")
        )
    assert state == "none"
    assert archived is False
    engine.dispose()


def test_sealed_bundle_with_unmeasured_copies_is_not_archive_evidence(tmp_path: Path) -> None:
    """Guards: "sealed" read as "durable".

    A copy that was written but never read back is an unverified claim. The
    predicate carries ``durability``'s single meaning of verified — measured,
    and measured to its own integrity hash.
    """
    engine = make_engine(f"sqlite:///{tmp_path / 'unmeasured.db'}")
    create_all(engine)
    now = dt.datetime(2026, 8, 5, 9, 0, tzinfo=dt.UTC)
    digest = _digest("unmeasured")
    with session_scope(engine) as session:
        _policy(session, "s-masters")
        session.add(LogicalAsset(content_sha256=digest, size_bytes=10))
        _intake_with_items(session, "unmeasured-intake", digests=(digest,), now=now)
        bundle = _sealed_bundle(
            session, "unmeasured-bundle", artifactclass="s-masters", digest=digest, now=now
        )
        _verified_copies(session, bundle, measured=False)
        assert (
            session.scalar(
                select(intake_archive_state_expr()).where(Intake.intake_id == "unmeasured-intake")
            )
            == "none"
        )
        # Measuring the same copy flips it.
        for copy in session.scalars(select(Copy)):
            copy.last_measured_digest = copy.integrity_hash
            copy.last_measured_at = now
        session.flush()
        assert (
            session.scalar(
                select(intake_archive_state_expr()).where(Intake.intake_id == "unmeasured-intake")
            )
            == "complete"
        )
    engine.dispose()


def test_evidence_uses_the_member_own_class_min_copies(tmp_path: Path) -> None:
    """Guards: a global copy floor, or a co-resident class's floor.

    A group bundle holds several classes. Each member's durability floor is its
    own class's ``min_copies``: the same sealed bundle can be evidence for a
    two-copy class and not yet evidence for a three-copy one.
    """
    engine = make_engine(f"sqlite:///{tmp_path / 'floors.db'}")
    create_all(engine)
    now = dt.datetime(2026, 8, 5, 9, 0, tzinfo=dt.UTC)
    lax = _digest("lax-class-member")
    strict = _digest("strict-class-member")
    with session_scope(engine) as session:
        _policy(session, "lax", min_copies=2)
        _policy(session, "strict", min_copies=3)
        for digest in (lax, strict):
            session.add(LogicalAsset(content_sha256=digest, size_bytes=10))
        session.flush()
        bundle = _sealed_bundle(session, "mixed-bundle", artifactclass="lax", digest=lax, now=now)
        session.add(
            BundleMember(
                bundle_id=bundle.id,
                logical_asset_hash=strict,
                artifactclass="strict",
                member_path="strict.mov",
                size_bytes=10,
                file_sha256=strict,
                added_at=now,
            )
        )
        session.flush()
        _verified_copies(session, bundle, count=2)

        lax_intake = _intake_with_items(session, "lax-intake", digests=(lax,), now=now)
        strict_intake = _intake_with_items(session, "strict-intake", digests=(strict,), now=now)
        # Both intakes declare s-masters items; re-point them at the classes
        # under test so the member's own class governs.
        for item in session.scalars(select(IngestItem)):
            item.artifactclass = "lax" if item.intake_id == lax_intake.intake_id else "strict"
        session.flush()

        states = dict(session.execute(select(Intake.intake_id, intake_archive_state_expr())).all())
    assert states[lax_intake.intake_id] == "complete"
    assert states[strict_intake.intake_id] == "none"
    engine.dispose()


def test_stored_submission_archived_flag_is_not_archive_evidence(tmp_path: Path) -> None:
    """Guards: the circular read this arc deletes.

    ``Submission.status`` is a *projection* of the predicate now. Reading it
    back as evidence would make an ``archived`` flag written by any path — an
    old row, a manual fix — sufficient to release the sources it describes.
    """
    engine = make_engine(f"sqlite:///{tmp_path / 'flag.db'}")
    create_all(engine)
    now = dt.datetime(2026, 8, 5, 9, 0, tzinfo=dt.UTC)
    digest = _digest("flagged")
    with session_scope(engine) as session:
        _policy(session, "s-masters")
        session.add(LogicalAsset(content_sha256=digest, size_bytes=10))
        intake = _intake_with_items(session, "flagged-intake", digests=(digest,), now=now)
        item = session.scalars(
            select(IngestItem).where(IngestItem.intake_id == intake.intake_id)
        ).one()
        arrangement = Arrangement(
            label="flagged",
            intake_id=intake.intake_id,
            artifactclass="s-masters",
            status=ArrangementStatus.SUBMITTED,
            created_at=now,
            updated_at=now,
            submitted_at=now,
        )
        session.add(arrangement)
        session.flush()
        submission = Submission(
            id="flagged-submission",
            arrangement_id=arrangement.id,
            artifactclass="s-masters",
            source_map_path="/tmp/flagged.tsv",
            manifest_digest="f" * 64,
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
                ingest_item_id=item.id,
                archive_path="flagged.mov",
                source_path="/data/flagged.mov",
                sha256=digest,
                size_bytes=10,
                ord=0,
            )
        )
        session.flush()

        assert (
            session.scalar(
                select(intake_archive_state_expr()).where(Intake.intake_id == "flagged-intake")
            )
            == "none"
        )
        # And the strong per-submission predicate agrees with the read model.
        assert submission_is_archived(session, submission.id) is False
    engine.dispose()


def test_submission_is_archived_only_when_every_member_is_evidence(tmp_path: Path) -> None:
    """A submission split across a seal boundary is archived only when BOTH
    bundles are sealed and sufficiently replicated.

    Guards: reporting a split submission archived on the strength of its first
    sealed bundle — which would release the sources of the members still
    sitting in the second, open one.
    """
    engine = make_engine(f"sqlite:///{tmp_path / 'split.db'}")
    create_all(engine)
    now = dt.datetime(2026, 8, 5, 9, 0, tzinfo=dt.UTC)
    first = _digest("split-first")
    second = _digest("split-second")
    with session_scope(engine) as session:
        _policy(session, "s-masters")
        for digest in (first, second):
            session.add(LogicalAsset(content_sha256=digest, size_bytes=10))
        intake = _intake_with_items(session, "split-intake", digests=(first, second), now=now)
        items = list(
            session.scalars(select(IngestItem).where(IngestItem.intake_id == intake.intake_id))
        )
        arrangement = Arrangement(
            label="split",
            intake_id=intake.intake_id,
            artifactclass="s-masters",
            status=ArrangementStatus.SUBMITTED,
            created_at=now,
            updated_at=now,
            submitted_at=now,
        )
        session.add(arrangement)
        session.flush()
        submission = Submission(
            id="split-submission",
            arrangement_id=arrangement.id,
            artifactclass="s-masters",
            source_map_path="/tmp/split.tsv",
            manifest_digest="a" * 64,
            member_count=2,
            status=SubmissionStatus.ACCUMULATED,
            submitted_by="ada",
            submitted_at=now,
        )
        session.add(submission)
        session.flush()
        for ordinal, (item, digest) in enumerate(zip(items, (first, second), strict=True)):
            session.add(
                SubmissionMember(
                    submission_id=submission.id,
                    ingest_item_id=item.id,
                    archive_path=f"split-{ordinal}.mov",
                    source_path=f"/data/split-{ordinal}.mov",
                    sha256=digest,
                    size_bytes=10,
                    ord=ordinal,
                )
            )
        session.flush()

        sealed = _sealed_bundle(
            session, "split-a", artifactclass="s-masters", digest=first, now=now
        )
        _verified_copies(session, sealed)
        still_open = _sealed_bundle(
            session,
            "split-b",
            artifactclass="s-masters",
            digest=second,
            now=now,
            status="open",
        )
        assert submission_is_archived(session, submission.id) is False

        still_open.status = "sealed"
        still_open.sealed_at = now
        _verified_copies(session, still_open)
        assert submission_is_archived(session, submission.id) is True
    engine.dispose()


def test_submission_with_no_members_is_never_archived(tmp_path: Path) -> None:
    """Guards: vacuous truth. "every member is evidence" over an empty member
    set is trivially true, which would report an empty submission archived."""
    engine = make_engine(f"sqlite:///{tmp_path / 'empty.db'}")
    create_all(engine)
    with session_scope(engine) as session:
        assert submission_is_archived(session, "no-such-submission") is False
    engine.dispose()


def test_env_gate_and_legacy_any_semantics_have_no_writer_or_reader(tmp_path: Path) -> None:
    """Grep-level assertion for the deleted rollout gate and ANY predicate.

    Guards: the gate coming back as a default-off environment read, or the ANY
    predicate surviving as a helper nothing calls today and something calls
    tomorrow. Both are deleted, not disabled.
    """
    for needle in (
        "SUTRADHARA_" + "ARCHIVED_ALL_SEMANTICS",
        "archived_all_semantics_enabled",
        "legacy_archived_expr",
        "intake_archive_evidence_exists",
        "all_semantics",
    ):
        hits = subprocess.run(
            ["git", "grep", "-l", needle, "--", "src", "docs"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert hits.stdout.strip() == "", f"{needle} still present in: {hits.stdout}"


# --------------------------------------------------------------------------
# The audit
# --------------------------------------------------------------------------


def test_audit_reports_partial_retention_passed_intakes_without_mutating(
    tmp_path: Path,
) -> None:
    engine = make_engine(f"sqlite:///{tmp_path / 'audit.db'}")
    create_all(engine)
    archived_hash = _digest("archived")
    missing_hash = _digest("missing")
    now = dt.datetime(2026, 7, 11, 9, 0, tzinfo=dt.UTC)
    with session_scope(engine) as session:
        _policy(session, "s-masters")
        for digest in (archived_hash, missing_hash):
            session.add(LogicalAsset(content_sha256=digest, size_bytes=10))
        _intake_with_items(
            session, "released-partial", digests=(archived_hash, missing_hash), now=now
        )
        bundle = _sealed_bundle(
            session, "sealed", artifactclass="s-masters", digest=archived_hash, now=now
        )
        _verified_copies(session, bundle)

    generated_at = dt.datetime(2026, 7, 11, 10, 0, tzinfo=dt.UTC)
    with session_scope(engine) as session:
        state, archived = session.execute(
            select(intake_archive_state_expr(), intake_archived_expr()).where(
                Intake.intake_id == "released-partial"
            )
        ).one()
        report = build_archive_predicate_audit(session, generated_at=generated_at)
        unchanged = session.get(Intake, "released-partial")
        assert unchanged is not None
        assert unchanged.retention_state == RetentionState.RELEASED

    assert state == "partial"
    assert archived is False
    assert report["schema"] == "sutradhara.archive-predicate-audit/v2"
    assert report["generated_at"] == "2026-07-11T10:00:00+00:00"
    assert report["summary"] == {
        "audited_intakes": 1,
        "affected_intakes": 1,
        "missing_distinct_assets": 1,
        "clean": False,
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
    engine.dispose()


def test_audit_catches_a_partial_intake_sharing_a_hash_with_a_sealed_bundle(
    tmp_path: Path,
) -> None:
    """Cross-intake evidence: the shared hash is archived through another
    intake's bundle, the intake's own second hash is not, and the audit reports
    the gap rather than the aggregate."""
    engine = make_engine(f"sqlite:///{tmp_path / 'cross-intake-audit.db'}")
    create_all(engine)
    shared_hash = _digest("cross-intake-shared")
    missing_hash = _digest("cross-intake-missing")
    now = dt.datetime(2026, 7, 11, 9, 0, tzinfo=dt.UTC)
    with session_scope(engine) as session:
        _policy(session, "s-masters")
        for digest in (shared_hash, missing_hash):
            session.add(LogicalAsset(content_sha256=digest, size_bytes=10))
        _intake_with_items(session, "victim", digests=(shared_hash, missing_hash), now=now)
        _intake_with_items(
            session,
            "donor",
            digests=(shared_hash,),
            now=now,
            retention_state=RetentionState.HELD,
        )
        bundle = _sealed_bundle(
            session, "donor-bundle", artifactclass="s-masters", digest=shared_hash, now=now
        )
        _verified_copies(session, bundle)

    with session_scope(engine) as session:
        report = build_archive_predicate_audit(session, generated_at=now)

    assert report["summary"] == {
        "audited_intakes": 1,
        "affected_intakes": 1,
        "missing_distinct_assets": 1,
        "clean": False,
    }
    affected = report["affected_intakes"]
    assert isinstance(affected, list)
    assert affected[0]["intake_id"] == "victim"
    assert affected[0]["archive_state"] == "partial"
    assert affected[0]["missing_assets"] == [
        {
            "content_sha256": missing_hash.hex(),
            "artifactclass": "s-masters",
            "occurrence_count": 1,
        }
    ]
    engine.dispose()


# --------------------------------------------------------------------------
# Query cost
# --------------------------------------------------------------------------


def test_all_predicate_uses_indexes_in_one_query_at_pilot_scale(tmp_path: Path) -> None:
    """Exercise 400 intakes and 100,000 memberships without per-intake SQL.

    The predicate gained a correlated COUNT over ``copy`` (the design's
    accepted cost). This test pins that the added count did not cost the plan
    its indexes and did not turn one statement into N.
    """

    engine = make_engine(f"sqlite:///{tmp_path / 'pilot.db'}")
    create_all(engine)
    archived_hash = _digest("pilot-archived")
    missing_hash = _digest("pilot-missing")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "INSERT INTO logical_asset "
            "(content_sha256, size_bytes, first_seen_at, validity) VALUES "
            "(?, 1, CURRENT_TIMESTAMP, 'ok'), (?, 1, CURRENT_TIMESTAMP, 'ok')",
            (archived_hash, missing_hash),
        )
        connection.exec_driver_sql(
            "INSERT INTO artifactclass_policy "
            "(artifactclass, ruleset, expect, target_bytes, max_age_seconds, "
            "restore_preference, min_copies, min_impl_families, staging_config, "
            "hdcache_config, updated_at) VALUES "
            "('s-masters', 'r.v1', 'messy', 1024, 60, '[]', 1, 1, '{}', '{}', "
            "CURRENT_TIMESTAMP)"
        )
        connection.exec_driver_sql(
            "INSERT INTO backend (name, kind, implementation_family, tier, added_at) "
            "VALUES ('pilot-backend', 'memory', 'memory', 'catalog_authoritative', "
            "CURRENT_TIMESTAMP)"
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
        pilot_fields = bundle_kwargs(seed="s-masters")
        connection.exec_driver_sql(
            "INSERT INTO bundle "
            "(id, bundle_group, group_basis, status, total_bytes, member_count, "
            "target_bytes, max_age_seconds, opened_at, sealed_at) VALUES "
            "('pilot-sealed', ?, ?, 'sealed', 1, 1, 1, 60, CURRENT_TIMESTAMP, "
            "CURRENT_TIMESTAMP)",
            (pilot_fields["bundle_group"], json.dumps(pilot_fields["group_basis"])),
        )
        connection.exec_driver_sql(
            "INSERT INTO bundle_member "
            "(bundle_id, logical_asset_hash, artifactclass, member_path, size_bytes, "
            "file_sha256, added_at) "
            "VALUES ('pilot-sealed', ?, 's-masters', 'archived', 1, ?, CURRENT_TIMESTAMP)",
            (archived_hash, archived_hash),
        )
        connection.exec_driver_sql(
            "INSERT INTO copy "
            "(bundle_id, backend_id, native_locator, native_locator_key, storage_metadata, "
            "integrity_hash, integrity_hash_provenance, health, health_changed_at, "
            "last_measured_digest, last_measured_at, first_observed_at, source) VALUES "
            "('pilot-sealed', 1, '{}', 'pilot-locator', '{}', ?, 'locally_computed', 'ok', "
            "CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'ingest')",
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
    # The correlated copy count must use the bundle_id index, not a table scan.
    assert "SCAN copy" not in plan_text
    assert statement_count == 1
    assert len(rows) == 400
    assert {state for _intake_id, state in rows} == {"partial"}
    engine.dispose()
