"""Tests for the P0.3 copy reconciler.

The production ``copy`` handler remains a stub. Tests temporarily replace the
registered handler only where a successful or transient-failing copy attempt is
needed to exercise the reconciler spine end to end.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Callable, Iterator
from contextlib import contextmanager

import pytest
from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

from sutradhara.catalog.copies import add_copy
from sutradhara.catalog.models import (
    ArtifactClassPool,
    Backend,
    IngestItem,
    Intake,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource
from sutradhara.jobs import handlers as _handlers  # noqa: F401 -- register production copy stub
from sutradhara.jobs.engine import run_one
from sutradhara.jobs.models import Job, ReconciliationCondition
from sutradhara.jobs.reconcilers import copy as copy_reconciler
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    CONDITION_BLOCKED,
    CONDITION_OPEN,
    CONDITION_SATISFIED,
    DEFAULT_BACKOFF_GIVE_UP_ATTEMPTS,
)
from sutradhara.jobs.reconcilers.spine import discover, process, reconcile
from sutradhara.jobs.registry import JobContext, JobResult
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_missing_copy_is_enqueued_from_reconcile_and_deduped(engine: Engine) -> None:
    asset_hash = _add_registered_asset_with_pool(engine, b"asset-a", "class-a", "pool-a")

    with session_scope(engine) as session:
        discovered, processed = reconcile(session, "copy")

        assert discovered == 1
        assert processed == 1
        condition = _condition(session, copy_reconciler.make_target_key(asset_hash, "pool-a"))
        assert condition.condition == CONDITION_OPEN
        assert condition.observed_state == "missing"
        jobs = list(session.scalars(select(Job).where(Job.kind == "copy")))
        assert len(jobs) == 1
        assert jobs[0].recon_domain == "copy"
        assert jobs[0].recon_target_key == condition.target_key
        assert jobs[0].dedupe_key == f"copy:{condition.target_key}"
        assert jobs[0].params["pool_id"] == "pool-a"

        process(session, "copy")
        assert session.scalar(select(func.count()).select_from(Job)) == 1


def test_success_becomes_satisfied_only_after_observation(engine: Engine) -> None:
    asset_hash = _add_registered_asset_with_pool(engine, b"asset-b", "class-a", "pool-a")
    target_key = copy_reconciler.make_target_key(asset_hash, "pool-a")

    with _copy_handler(_record_healthy_copy), session_scope(engine) as session:
        reconcile(session, "copy")
        job = session.scalars(select(Job).where(Job.kind == "copy")).one()
        result = run_one(session, job.id)
        assert result.ok

        condition = _condition(session, target_key)
        assert condition.condition == CONDITION_OPEN
        assert condition.last_success_at is not None

        process(session, "copy")

        condition = _condition(session, target_key)
        assert condition.condition == CONDITION_SATISFIED
        assert condition.observed_state == "present"


def test_ok_true_without_copy_never_auto_satisfies(engine: Engine) -> None:
    asset_hash = _add_registered_asset_with_pool(engine, b"asset-c", "class-a", "pool-a")
    target_key = copy_reconciler.make_target_key(asset_hash, "pool-a")

    with (
        _copy_handler(lambda _ctx: JobResult(ok=True, detail="did nothing")),
        session_scope(engine) as session,
    ):
        reconcile(session, "copy")
        first_job = session.scalars(select(Job).where(Job.kind == "copy")).one()
        run_one(session, first_job.id)

        condition = _condition(session, target_key)
        assert condition.condition == CONDITION_OPEN
        assert condition.last_success_at is not None

        process(session, "copy")
        condition = _condition(session, target_key)
        assert condition.condition == CONDITION_OPEN
        assert session.scalar(select(func.count()).select_from(Job)) == 2


def test_failure_backoff_accumulates_until_blocked(engine: Engine) -> None:
    asset_hash = _add_registered_asset_with_pool(engine, b"asset-d", "class-a", "pool-a")
    target_key = copy_reconciler.make_target_key(asset_hash, "pool-a")

    with (
        _copy_handler(lambda _ctx: JobResult(ok=False, detail="drive busy")),
        session_scope(engine) as session,
    ):
        reconcile(session, "copy")

        for expected_count in range(1, DEFAULT_BACKOFF_GIVE_UP_ATTEMPTS + 1):
            job = session.scalars(
                select(Job).where(Job.kind == "copy").order_by(Job.id.desc()).limit(1)
            ).one()
            run_one(session, job.id)
            condition = _condition(session, target_key)
            assert condition.attempt_count == expected_count

            if expected_count < DEFAULT_BACKOFF_GIVE_UP_ATTEMPTS:
                assert condition.condition == CONDITION_BACKOFF
                assert condition.next_eligible_at is not None
                condition.next_eligible_at = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1)
                process(session, "copy")
            else:
                assert condition.condition == CONDITION_BLOCKED
                process(session, "copy")
                assert session.scalar(select(func.count()).select_from(Job)) == expected_count


def test_shared_asset_uses_union_of_all_live_classes(engine: Engine) -> None:
    asset_hash = _add_registered_asset(engine, b"asset-e", "class-a")
    _add_membership(engine, asset_hash, "class-b", intake_id="intake-class-b")
    with session_scope(engine) as session:
        _add_pool(session, "pool-a", "class-a")
        _add_pool(session, "pool-b", "class-b")

    key_a = copy_reconciler.make_target_key(asset_hash, "pool-a")
    key_b = copy_reconciler.make_target_key(asset_hash, "pool-b")

    with session_scope(engine) as session:
        discover(session, "copy")
        assert _condition(session, key_a).condition == CONDITION_OPEN
        assert _condition(session, key_b).condition == CONDITION_OPEN

        membership = session.scalars(
            select(ArtifactClassPool).where(ArtifactClassPool.artifactclass == "class-a")
        ).one()
        membership.active = False
        session.flush()

        obs_a = copy_reconciler.observe(session, key_a)
        obs_b = copy_reconciler.observe(session, key_b)
        assert not obs_a.desired
        assert obs_b.desired

        process(session, "copy")
        assert _condition(session, key_a).condition == CONDITION_SATISFIED
        assert _condition(session, key_b).condition == CONDITION_OPEN


def test_policy_shrink_to_zero_closes_existing_due_row(engine: Engine) -> None:
    asset_hash = _add_registered_asset_with_pool(engine, b"asset-f", "class-a", "pool-a")
    target_key = copy_reconciler.make_target_key(asset_hash, "pool-a")

    with session_scope(engine) as session:
        discover(session, "copy")
        condition = _condition(session, target_key)
        condition.condition = CONDITION_BACKOFF
        condition.attempt_count = 2
        condition.next_eligible_at = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1)
        session.scalars(
            select(ArtifactClassPool).where(ArtifactClassPool.artifactclass == "class-a")
        ).one().active = False
        session.flush()

        process(session, "copy")

        condition = _condition(session, target_key)
        assert condition.condition == CONDITION_SATISFIED
        assert condition.attempt_count == 0
        assert session.scalar(select(func.count()).select_from(Job)) == 0


def test_missing_policy_on_discovery_logs_diagnostic_and_skips(
    engine: Engine,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _add_registered_asset(engine, b"asset-g", "class-without-pools")

    with session_scope(engine) as session, caplog.at_level("WARNING"):
        assert discover(session, "copy") == 0
        assert session.scalar(select(func.count()).select_from(ReconciliationCondition)) == 0

    assert "class class-without-pools registered but no active pools" in caplog.text


def test_production_stub_blocks_without_hammering(engine: Engine) -> None:
    asset_hash = _add_registered_asset_with_pool(engine, b"asset-h", "class-a", "pool-a")
    target_key = copy_reconciler.make_target_key(asset_hash, "pool-a")

    with session_scope(engine) as session:
        reconcile(session, "copy")
        job = session.scalars(select(Job).where(Job.kind == "copy")).one()
        result = run_one(session, job.id)
        assert not result.ok

        condition = _condition(session, target_key)
        assert condition.condition == CONDITION_BLOCKED
        assert condition.reason == "not-implemented"

        process(session, "copy")
        assert session.scalar(select(func.count()).select_from(Job)) == 1


def _add_registered_asset_with_pool(
    engine: Engine,
    data: bytes,
    artifactclass: str,
    pool_id: str,
) -> bytes:
    asset_hash = _add_registered_asset(engine, data, artifactclass)
    with session_scope(engine) as session:
        _add_pool(session, pool_id, artifactclass)
    return asset_hash


def _add_registered_asset(engine: Engine, data: bytes, artifactclass: str) -> bytes:
    asset_hash = hashlib.sha256(data).digest()
    with session_scope(engine) as session:
        session.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(data)))
        _add_membership_rows(
            session,
            asset_hash,
            artifactclass,
            intake_id=f"intake-{artifactclass}-{asset_hash.hex()[:8]}",
        )
    return asset_hash


def _add_membership(
    engine: Engine,
    asset_hash: bytes,
    artifactclass: str,
    *,
    intake_id: str,
) -> None:
    with session_scope(engine) as session:
        _add_membership_rows(session, asset_hash, artifactclass, intake_id=intake_id)


def _add_membership_rows(
    session: Session,
    asset_hash: bytes,
    artifactclass: str,
    *,
    intake_id: str,
) -> None:
    session.add(
        Intake(
            intake_id=intake_id,
            operator="tester",
            source_kind="upload",
            artifactclass=artifactclass,
            status="registered",
            registered_at=dt.datetime.now(dt.UTC),
        )
    )
    session.add(
        IngestItem(
            intake_id=intake_id,
            logical_asset_hash=asset_hash,
            as_received_path=f"{artifactclass}/{asset_hash.hex()}",
            virtual_path=f"{artifactclass}/{asset_hash.hex()}",
            size_bytes=1,
            artifactclass=artifactclass,
        )
    )


def _add_pool(session: Session, pool_id: str, artifactclass: str) -> None:
    backend_name = f"backend-{pool_id}"
    backend = Backend(
        name=backend_name,
        kind=BackendKind.REM_TAPE,
        tier=BackendTier.SELF_DESCRIBING,
        config={"daemon_endpoint": f"unix:/{backend_name}.sock"},
    )
    session.add(backend)
    session.flush()
    session.add(
        Pool(
            id=pool_id,
            backend_id=backend.id,
            representation=Representation.RAW_BYTES.value,
        )
    )
    session.add(ArtifactClassPool(artifactclass=artifactclass, pool_id=pool_id))


def _condition(session: Session, target_key: str) -> ReconciliationCondition:
    return session.scalars(
        select(ReconciliationCondition).where(
            ReconciliationCondition.domain == "copy",
            ReconciliationCondition.target_key == target_key,
        )
    ).one()


def _record_healthy_copy(ctx: JobContext) -> JobResult:
    params = ctx.job.params
    asset_hash = bytes.fromhex(params["asset_hash"])
    backend = ctx.session.scalars(
        select(Backend).where(Backend.name == params["target_backend"])
    ).one()
    add_copy(
        ctx.session,
        logical_asset_hash=asset_hash,
        backend_id=backend.id,
        pool_id=params["pool_id"],
        native_locator={"pool_id": params["pool_id"], "asset": params["asset_hash"]},
        integrity_hash=asset_hash,
        source=CopySource.INGEST,
        health=CopyHealth.OK,
        storage_metadata={"representation": Representation.RAW_BYTES.value},
    )
    return JobResult(ok=True, detail="copied")


@contextmanager
def _copy_handler(handler: Callable[[JobContext], JobResult]) -> Iterator[None]:
    from sutradhara.jobs import registry as _r

    original = _r._HANDLERS["copy"]
    _r._HANDLERS["copy"] = handler
    try:
        yield
    finally:
        _r._HANDLERS["copy"] = original
