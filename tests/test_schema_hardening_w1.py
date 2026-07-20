"""Behavioral gates for Wave 1 history, registry, and clock semantics."""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, func, select
from sqlalchemy.exc import IntegrityError

from sutradhara.catalog.copies import add_copy
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    AssetReviewEvent,
    Backend,
    Copy,
    Intake,
    LogicalAsset,
    OffsiteConfirmation,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import (
    BackendKind,
    BackendTier,
    CopySource,
    IntakeSourceKind,
    IntakeStatus,
    RetentionState,
)
from sutradhara.jobs.attempts import record_attempt
from sutradhara.jobs.models import Job, JobAttempt, JobStatus
from sutradhara.jobs.reconcilers import conditions
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BLOCKED,
    CONDITION_OPEN,
    OBSERVED_MISSING,
    record_condition,
    record_observation,
    reopen_condition,
)
from sutradhara.virtual_arrangement import reject_asset, unreject_asset


@pytest.fixture
def engine() -> Iterator[Engine]:
    """Return a fresh foreign-key-enforcing catalog."""

    value = make_engine("sqlite:///:memory:")
    create_all(value)
    yield value
    value.dispose()


def test_unknown_artifactclass_is_rejected_until_policy_administration_registers_it(
    engine: Engine,
) -> None:
    """Every artifactclass-bearing write resolves through the policy registry."""

    with session_scope(engine) as session:
        session.add(_intake("unknown", "typo", IntakeStatus.VERIFYING))
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            session.flush()
        session.rollback()

    with session_scope(engine) as session:
        session.add(
            ArtifactClassPolicyRecord(
                artifactclass="masters",
                ruleset="masters",
                expect="messy",
                target_bytes=1,
                max_age_seconds=60,
                restore_preference=[],
                min_copies=1,
                min_impl_families=1,
                staging_config={},
                hdcache_config={},
            )
        )
        session.flush()
        session.add(_intake("known", "masters", IntakeStatus.REGISTERED))
        session.flush()


def test_attempt_snapshot_survives_terminal_job_pruning(engine: Engine) -> None:
    """A pruned queue row cannot erase the attempt's subject or parameters."""

    now = dt.datetime(2026, 7, 20, 1, 0, tzinfo=dt.UTC)
    with session_scope(engine) as session:
        job = Job(
            kind="verify",
            params={"copy_id": 17},
            status=JobStatus.SUCCEEDED,
            attempts=2,
            recon_domain="copy",
            recon_target_key="asset:17",
            started_at=now,
            finished_at=now + dt.timedelta(seconds=1),
        )
        session.add(job)
        session.flush()
        job_id = job.id
        attempt = record_attempt(session, job)
        attempt_id = attempt.id

        session.delete(job)
        session.flush()
        session.expire_all()
        retained = session.get(JobAttempt, attempt_id)
        assert retained is not None
        assert retained.job_id is None
        assert retained.subject_job_id == job_id
        assert retained.subject_domain == "copy"
        assert retained.subject_key == "asset:17"
        assert retained.params_snapshot == {"copy_id": 17}


def test_condition_clocks_and_reopen_attribution_have_distinct_writers(
    engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Observations cannot move the condition clock or rewrite reopen attribution."""

    moments = iter(dt.datetime(2026, 7, 20, hour, tzinfo=dt.UTC) for hour in range(1, 6))
    monkeypatch.setattr(conditions, "_utcnow", lambda: next(moments))

    with session_scope(engine) as session:
        row = record_observation(
            session,
            domain="copy",
            target_key="asset:clock",
            desired=True,
            observed_state=OBSERVED_MISSING,
        )
        first_observed = row.observed_at
        first_changed = row.condition_changed_at
        assert row.condition == CONDITION_OPEN

        record_observation(
            session,
            domain="copy",
            target_key="asset:clock",
            desired=True,
            observed_state=OBSERVED_MISSING,
        )
        assert row.observed_at > first_observed
        assert row.condition_changed_at == first_changed

        record_condition(
            session,
            domain="copy",
            target_key="asset:clock",
            condition=CONDITION_BLOCKED,
            reason="operator-required",
        )
        blocked_at = row.condition_changed_at
        assert blocked_at > first_changed

        reopen_condition(session, row, actor="operator", note="media mounted")
        reopened_at = row.reopened_at
        assert row.reopened_by == "operator"
        assert reopened_at is not None
        assert row.condition_changed_at == reopened_at
        assert row.message == "media mounted"

        record_observation(
            session,
            domain="copy",
            target_key="asset:clock",
            desired=True,
            observed_state=OBSERVED_MISSING,
        )
        assert row.reopened_by == "operator"
        assert row.reopened_at == reopened_at
        assert row.condition_changed_at == reopened_at


def test_media_identity_query_battery_uses_typed_columns_for_volume_and_offsite(
    engine: Engine,
) -> None:
    """A4 and F2 are direct relational queries with no locator JSON archaeology."""

    tape_a = "a" * 32
    tape_b = "b" * 32
    with session_scope(engine) as session:
        backend = Backend(
            name="query-battery-tape",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
            config={"library": "mainlib"},
        )
        assets = [
            LogicalAsset(content_sha256=hashlib.sha256(body).digest(), size_bytes=len(body))
            for body in (b"a1", b"a2", b"b1")
        ]
        session.add_all([backend, *assets])
        session.flush()
        for ordinal, (asset, tape_uuid) in enumerate(
            zip(assets, (tape_a, tape_a, tape_b), strict=True),
            start=1,
        ):
            add_copy(
                session,
                logical_asset_hash=asset.content_sha256,
                backend_id=backend.id,
                native_locator={"tape_uuid": tape_uuid, "object_id": f"object-{ordinal}"},
                integrity_hash=asset.content_sha256,
                source=CopySource.INGEST,
            )
        session.add(
            OffsiteConfirmation(
                media_id=f"tape:{tape_a}",
                confirmed_by="query-battery",
            )
        )
        session.flush()

        volume_rows = list(
            session.execute(
                select(Copy.media_family, Copy.media_id, func.count(Copy.id))
                .group_by(Copy.media_family, Copy.media_id)
                .order_by(Copy.media_id)
            )
        )
        offsite_rows = list(
            session.execute(
                select(Copy.media_id, OffsiteConfirmation.confirmed_by)
                .outerjoin(
                    OffsiteConfirmation,
                    OffsiteConfirmation.media_id == Copy.media_id,
                )
                .group_by(Copy.media_id, OffsiteConfirmation.confirmed_by)
                .order_by(Copy.media_id)
            )
        )

    assert volume_rows == [
        ("tape", f"tape:{tape_a}", 2),
        ("tape", f"tape:{tape_b}", 1),
    ]
    assert offsite_rows == [
        (f"tape:{tape_a}", "query-battery"),
        (f"tape:{tape_b}", None),
    ]


def test_unreject_preserves_append_only_decision_history_and_restricts_asset_delete(
    engine: Engine,
) -> None:
    """Projection clearing appends an event and the audit history blocks deletion."""

    digest = hashlib.sha256(b"reviewed asset").digest()
    with session_scope(engine) as session:
        asset = LogicalAsset(content_sha256=digest, size_bytes=14)
        session.add(asset)
        session.flush()
        reject_asset(session, digest, actor="reviewer", reason="bad take")
        unreject_asset(session, digest, actor="reviewer", reason="approved on appeal")

        events = list(
            session.scalars(
                select(AssetReviewEvent)
                .where(AssetReviewEvent.logical_asset_hash == digest)
                .order_by(AssetReviewEvent.id)
            )
        )
        assert [(event.action, event.reason) for event in events] == [
            ("reject", "bad take"),
            ("unreject", "approved on appeal"),
        ]
        assert asset.rejected_at is None
        assert asset.rejected_by is None

        session.delete(asset)
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            session.flush()
        session.rollback()


def _intake(intake_id: str, artifactclass: str, status: IntakeStatus) -> Intake:
    """Build one intake whose retention state satisfies the ordering CHECK."""

    return Intake(
        intake_id=intake_id,
        operator="operator",
        source_kind=IntakeSourceKind.CARD,
        artifactclass=artifactclass,
        status=status,
        retention_state=(
            RetentionState.HELD
            if status == IntakeStatus.REGISTERED
            else RetentionState.NOT_APPLICABLE
        ),
        registered_at=(
            dt.datetime(2026, 7, 20, tzinfo=dt.UTC) if status == IntakeStatus.REGISTERED else None
        ),
    )
