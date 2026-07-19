"""Retention gate tests for temporary-byte deletion safety."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

import sutradhara.retention as retention_module
from sutradhara.arrangement import ArrangementError, create_from_intake
from sutradhara.backend.port import ByteRange, VerifyResult, WitnessResult
from sutradhara.catalog.models import (
    Arrangement,
    ArtifactClassPool,
    AssetLocator,
    Backend,
    Bundle,
    Copy,
    IngestItem,
    Intake,
    LogicalAsset,
    Pool,
    RetentionEvent,
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
)
from sutradhara.intake import prepare_intake
from sutradhara.jobs.models import ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BLOCKED,
    CONDITION_OPEN,
    CONDITION_SATISFIED,
    OBSERVED_MISSING,
    OBSERVED_PRESENT,
)
from sutradhara.jobs.reconcilers.derivation import DOMAIN as DERIVATION_DOMAIN
from sutradhara.jobs.reconcilers.derivation import make_target_key
from sutradhara.replication import select_restore_source
from sutradhara.retention import (
    confirm_offsite,
    releasable,
    run_retention,
    sweep_staging,
)
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture(autouse=True)
def retention_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUTRADHARA_RETENTION_LANDING_ROOTS", str(tmp_path))
    monkeypatch.setenv(
        "SUTRADHARA_RETENTION_TOMBSTONE_ROOT",
        str(tmp_path / ".retention-tombstones"),
    )
    monkeypatch.setattr(
        "sutradhara.backend.remanence.RemanenceBackend.witness_copy",
        lambda _self, _locator, *, expected_hash: WitnessResult(True),
    )


def _copy_by_id(session: Session, copy_id: int) -> Copy:
    copy = session.get(Copy, copy_id)
    assert copy is not None
    return copy


class _DeleteBackend:
    def __init__(self) -> None:
        self.name = "test-delete"
        self.objects: set[str] = set()
        self.deleted: list[dict[str, Any]] = []

    def add(self, key: str) -> dict[str, str]:
        self.objects.add(key)
        return {"key": key}

    def enumerate(self) -> Iterator[Any]:
        return iter(())

    def read_range(self, locator: dict[str, Any], byte_range: ByteRange) -> bytes:
        raise NotImplementedError

    def verify(self, locator: dict[str, Any]) -> VerifyResult:
        raise NotImplementedError

    def witness_copy(
        self,
        locator: dict[str, Any],
        *,
        expected_hash: bytes,
    ) -> WitnessResult:
        return WitnessResult(True)

    def delete_object(self, locator: dict[str, Any]) -> bool:
        key = str(locator["key"])
        existed = key in self.objects
        self.objects.discard(key)
        self.deleted.append(dict(locator))
        return existed


def test_gate_truth_table_offsite_and_proxy_only(engine: Engine, tmp_path: Path) -> None:
    with session_scope(engine) as session:
        offsite = _add_pool(
            session,
            artifactclass="s-masters",
            pool_id="offsite-pool",
            offsite_gate=True,
            kind=BackendKind.REM_TAPE,
        )
        item = _add_intake_with_item(session, tmp_path, "intake-a", artifactclass="s-masters")
        copy = _add_asset_copy(
            session,
            item,
            backend_id=offsite.backend_id,
            pool_id=offsite.id,
            tape_uuid="tape-a",
            verified=False,
        )

        assert not releasable(session, "intake-a")

        copy.last_checked_at = _now()
        assert not releasable(session, "intake-a")

        confirm_offsite(session, media_id="tape:tape-a", confirmed_by="ops")
        assert not releasable(session, "intake-a")
        copy.last_measured_digest = copy.integrity_hash
        copy.last_measured_at = _now()
        assert releasable(session, "intake-a")

        proxy_pool = _add_pool(
            session,
            artifactclass="s-proxy",
            pool_id="proxy-pool",
            offsite_gate=False,
            kind=BackendKind.MEMORY,
        )
        proxy_item = _add_intake_with_item(
            session,
            tmp_path,
            "intake-proxy",
            artifactclass="s-proxy",
            relpath="proxy.mp4",
            data=b"proxy payload",
        )
        _add_asset_copy(
            session,
            proxy_item,
            backend_id=proxy_pool.backend_id,
            pool_id=proxy_pool.id,
            native_locator={"object": "proxy-copy"},
            verified=True,
        )
        assert releasable(session, "intake-proxy")


def test_bundle_asset_locator_copy_counts_for_durability(
    engine: Engine,
    tmp_path: Path,
) -> None:
    with session_scope(engine) as session:
        pool = _add_pool(
            session,
            artifactclass="s-masters",
            pool_id="offsite-pool",
            offsite_gate=True,
            kind=BackendKind.REM_TAPE,
        )
        item = _add_intake_with_item(session, tmp_path, "intake-b", artifactclass="s-masters")
        bundle = Bundle(id="submission-sub-a", artifactclass="s-masters", status="sealed")
        session.add(bundle)
        session.flush()
        locator = {"tape_uuid": "tape-b", "object_id": "bundle-copy"}
        copy = Copy(
            bundle_id=bundle.id,
            backend_id=pool.backend_id,
            pool_id=pool.id,
            native_locator=locator,
            native_locator_key=locator_key(locator),
            storage_metadata={"representation": Representation.RAO_PLAIN_V1.value},
            integrity_hash=item.logical_asset_hash,
            health=CopyHealth.OK,
            last_checked_at=_now(),
            last_measured_digest=item.logical_asset_hash,
            last_measured_at=_now(),
            source=CopySource.INGEST,
        )
        session.add(copy)
        session.flush()
        session.add(
            AssetLocator(
                logical_asset_hash=item.logical_asset_hash,
                pool_id=pool.id,
                copy_id=copy.id,
                bundle_id=bundle.id,
                native_locator={"first_chunk_lba": 1, "size_bytes": item.size_bytes},
                member_path=item.as_received_path,
                representation=Representation.RAO_PLAIN_V1.value,
            )
        )
        confirm_offsite(session, media_id="tape:tape-b", confirmed_by="ops")

        assert releasable(session, "intake-b")


def test_bundle_locator_pool_mismatch_does_not_satisfy_retention_pool(
    engine: Engine,
    tmp_path: Path,
) -> None:
    with session_scope(engine) as session:
        offsite = _add_pool(
            session,
            artifactclass="s-masters",
            pool_id="offsite-pool",
            offsite_gate=True,
            kind=BackendKind.REM_TAPE,
        )
        wrong_pool = _add_pool(
            session,
            artifactclass="other-class",
            pool_id="wrong-pool",
            kind=BackendKind.REM_TAPE,
        )
        item = _add_intake_with_item(session, tmp_path, "intake-mismatch", artifactclass="s-masters")
        bundle = Bundle(id="submission-mismatch", artifactclass="s-masters", status="sealed")
        session.add(bundle)
        session.flush()
        locator = {"tape_uuid": "tape-mismatch", "object_id": "bundle-copy"}
        copy = Copy(
            bundle_id=bundle.id,
            backend_id=offsite.backend_id,
            pool_id=offsite.id,
            native_locator=locator,
            native_locator_key=locator_key(locator),
            storage_metadata={"representation": Representation.RAO_PLAIN_V1.value},
            integrity_hash=item.logical_asset_hash,
            health=CopyHealth.OK,
            last_checked_at=_now(),
            last_measured_digest=item.logical_asset_hash,
            last_measured_at=_now(),
            source=CopySource.INGEST,
        )
        session.add(copy)
        session.flush()
        session.add(
            AssetLocator(
                logical_asset_hash=item.logical_asset_hash,
                pool_id=wrong_pool.id,
                copy_id=copy.id,
                bundle_id=bundle.id,
                native_locator={"first_chunk_lba": 1, "size_bytes": item.size_bytes},
                member_path=item.as_received_path,
                representation=Representation.RAO_PLAIN_V1.value,
            )
        )
        confirm_offsite(session, media_id="tape:tape-mismatch", confirmed_by="ops")

        status = retention_module.retention_status(session, "intake-mismatch")

    assert not status.releasable
    [asset] = status.assets
    [pool_status] = asset.pools
    assert pool_status.pool_id == "offsite-pool"
    assert pool_status.satisfied is False
    assert pool_status.copy_id is None
    assert pool_status.reason == "no-verified-copy"


def test_landing_holds_arrangement_and_prepared_profile_fail_closed(
    engine: Engine,
    tmp_path: Path,
) -> None:
    with session_scope(engine) as session:
        pool = _add_pool(session, artifactclass="s-masters", pool_id="pool-a")
        item = _add_intake_with_item(session, tmp_path, "intake-c", artifactclass="s-masters")
        _add_asset_copy(
            session,
            item,
            backend_id=pool.backend_id,
            pool_id=pool.id,
            verified=True,
        )

        session.add(
            Arrangement(
                label="draft",
                intake_id="intake-c",
                artifactclass="s-masters",
                status=ArrangementStatus.DRAFT,
            )
        )
        session.flush()
        assert not releasable(session, "intake-c")

        arrangement = session.scalars(select(Arrangement)).one()
        arrangement.status = ArrangementStatus.SUBMITTED
        arrangement.submission_id = None
        assert not releasable(session, "intake-c")

        arrangement.status = ArrangementStatus.ABANDONED
        intake = session.get(Intake, "intake-c")
        assert intake is not None
        intake.requested_profile = "hd-review"
        assert not releasable(session, "intake-c")

        transcode_key = make_target_key(item.id, "transcode")
        pfr_key = make_target_key(item.id, "pfr-index")
        _add_condition(session, transcode_key, CONDITION_OPEN)
        _add_condition(session, pfr_key, CONDITION_SATISFIED)
        assert not releasable(session, "intake-c")

        session.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.target_key == transcode_key
            )
        ).one().condition = CONDITION_SATISFIED
        session.scalars(
            select(ReconciliationCondition).where(ReconciliationCondition.target_key == pfr_key)
        ).one().condition = CONDITION_BLOCKED
        assert releasable(session, "intake-c")


def test_per_pool_existential_and_tombstone_are_global(
    engine: Engine,
    tmp_path: Path,
) -> None:
    with session_scope(engine) as session:
        pool = _add_pool(session, artifactclass="s-masters", pool_id="pool-a")
        item = _add_intake_with_item(session, tmp_path, "intake-d", artifactclass="s-masters")
        stale = _add_asset_copy(
            session,
            item,
            backend_id=pool.backend_id,
            pool_id=pool.id,
            native_locator={"object": "stale"},
            verified=False,
        )
        stale.deleted_at = _now()
        good = _add_asset_copy(
            session,
            item,
            backend_id=pool.backend_id,
            pool_id=pool.id,
            native_locator={"object": "good"},
            verified=True,
        )

        assert releasable(session, "intake-d")
        assert select_restore_source(session, item.logical_asset_hash) == good

        good.deleted_at = _now()
        assert not releasable(session, "intake-d")
        assert select_restore_source(session, item.logical_asset_hash) is None


def test_run_retention_deletes_cloud_after_gate_and_is_idempotent(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_cloud = _DeleteBackend()

    def _backend_from_row(_row: Backend) -> _DeleteBackend:
        return fake_cloud

    monkeypatch.setattr("sutradhara.retention.factory.backend_from_row", _backend_from_row)

    with session_scope(engine) as session:
        pool = _add_pool(
            session,
            artifactclass="s-masters",
            pool_id="offsite-pool",
            offsite_gate=True,
            kind=BackendKind.REM_TAPE,
        )
        item = _add_intake_with_item(session, tmp_path, "intake-e", artifactclass="s-masters")
        _add_asset_copy(
            session,
            item,
            backend_id=pool.backend_id,
            pool_id=pool.id,
            tape_uuid="tape-e",
            verified=True,
        )
        cloud_copy = _add_cloud_copy(session, "intake-e", fake_cloud.add("cloud-e"))

        no_op = run_retention(session, "intake-e", actor="ops")
        assert not no_op.released
        assert fake_cloud.deleted == []
        assert _copy_by_id(session, cloud_copy.id).deleted_at is None

        confirm_offsite(session, media_id="tape:tape-e", confirmed_by="ops")
        released = run_retention(session, "intake-e", actor="ops")
        assert released.released
        assert released.deleted_copy_ids == (cloud_copy.id,)
        assert fake_cloud.deleted == [{"key": "cloud-e"}]
        assert fake_cloud.objects == set()
        assert _copy_by_id(session, cloud_copy.id).deleted_at is not None
        intake = session.get(Intake, "intake-e")
        assert intake is not None
        assert intake.retention_state == RetentionState.RELEASED
        assert intake.released_at is not None
        assert Path(str(intake.manifest_path)).parent.exists()
        assert session.scalar(select(func.count()).select_from(RetentionEvent)) == 4

        again = run_retention(session, "intake-e", actor="ops")
        assert not again.released
        assert len(fake_cloud.deleted) == 1


def test_retention_fails_closed_for_non_registered_and_empty_intakes(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_cloud = _DeleteBackend()
    monkeypatch.setattr("sutradhara.retention.factory.backend_from_row", lambda _row: fake_cloud)

    with session_scope(engine) as session:
        quarantined = _add_empty_intake(
            session,
            tmp_path,
            "intake-quarantined",
            status=IntakeStatus.QUARANTINED,
        )
        cloud_copy = _add_cloud_copy(
            session, quarantined.intake_id, fake_cloud.add("cloud-quarantined")
        )

        quarantined_status = retention_module.retention_status(session, quarantined)
        assert not quarantined_status.releasable
        assert "intake-status:quarantined" in quarantined_status.holds
        assert "intake-empty" in quarantined_status.holds

        quarantined_result = run_retention(session, quarantined, actor="ops")
        assert not quarantined_result.released
        assert fake_cloud.deleted == []
        assert _copy_by_id(session, cloud_copy.id).deleted_at is None

        empty_registered = _add_empty_intake(
            session,
            tmp_path,
            "intake-empty-registered",
            status=IntakeStatus.REGISTERED,
        )
        empty_status = retention_module.retention_status(session, empty_registered)
        assert not empty_status.releasable
        assert empty_status.holds == ("intake-empty",)
        assert empty_status.assets == ()
        assert not run_retention(session, empty_registered, actor="ops").released


def test_release_freezes_new_work_and_sweep_staging_after_grace(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_cloud = _DeleteBackend()
    monkeypatch.setattr("sutradhara.retention.factory.backend_from_row", lambda _row: fake_cloud)

    with session_scope(engine) as session:
        pool = _add_pool(session, artifactclass="s-masters", pool_id="pool-a")
        item = _add_intake_with_item(session, tmp_path, "intake-f", artifactclass="s-masters")
        item.logical_asset.rejected_at = _now()
        _add_asset_copy(
            session,
            item,
            backend_id=pool.backend_id,
            pool_id=pool.id,
            verified=True,
        )
        _add_cloud_copy(session, "intake-f", fake_cloud.add("cloud-f"))
        released = run_retention(session, "intake-f", actor="ops")
        assert released.released

        with pytest.raises(ArrangementError, match="virtual arrangements"):
            create_from_intake(session, "intake-f", label="late")
        with pytest.raises(ValueError, match="virtual arrangements"):
            prepare_intake(session, "intake-f", profile="hd-review")

        intake = session.get(Intake, "intake-f")
        assert intake is not None
        intake_root = Path(str(intake.manifest_path)).parent
        early = sweep_staging(session, "intake-f", actor="ops", grace_days=30)
        assert not early.purged
        assert intake_root.exists()

        intake.released_at = _now() - dt.timedelta(days=31)
        purged = sweep_staging(session, "intake-f", actor="ops", grace_days=30)
        assert purged.purged
        assert not intake_root.exists()
        assert intake.retention_state == RetentionState.PURGED
        assert intake.staging_deleted_at is not None

        again = sweep_staging(session, "intake-f", actor="ops", grace_days=30)
        assert not again.purged
        assert session.scalar(select(func.count()).select_from(RetentionEvent)) == 7


def test_release_attempt_survives_crash_and_outcomes_deduplicate(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crash after external deletion leaves one durable attempt for retry."""

    fake_cloud = _DeleteBackend()
    monkeypatch.setattr(retention_module.factory, "backend_from_row", lambda _row: fake_cloud)
    original_delete = retention_module._delete_copy_object

    with session_scope(engine) as session:
        pool = _add_pool(session, artifactclass="s-masters", pool_id="attempt-pool")
        item = _add_intake_with_item(
            session,
            tmp_path,
            "attempt-crash",
            artifactclass="s-masters",
        )
        _add_asset_copy(
            session,
            item,
            backend_id=pool.backend_id,
            pool_id=pool.id,
            verified=True,
        )
        _add_cloud_copy(session, "attempt-crash", fake_cloud.add("attempt-cloud"))

        def crash_after_delete(copy: Copy) -> str:
            original_delete(copy)
            raise RuntimeError("simulated crash after external delete")

        monkeypatch.setattr(retention_module, "_delete_copy_object", crash_after_delete)
        with pytest.raises(RuntimeError, match="simulated crash"):
            run_retention(session, "attempt-crash", actor="ops")

    with session_scope(engine) as session:
        intake = session.get(Intake, "attempt-crash")
        assert intake is not None
        assert intake.retention_state == RetentionState.HELD
        actions = list(
            session.scalars(
                select(RetentionEvent.action)
                .where(RetentionEvent.intake_id == "attempt-crash")
                .order_by(RetentionEvent.event_id)
            )
        )
        assert actions == ["release_attempted"]

    monkeypatch.setattr(retention_module, "_delete_copy_object", original_delete)
    with session_scope(engine) as session:
        result = run_retention(session, "attempt-crash", actor="ops")
        assert result.released
        events = list(
            session.scalars(
                select(RetentionEvent)
                .where(RetentionEvent.intake_id == "attempt-crash")
                .order_by(RetentionEvent.event_id)
            )
        )
        assert [event.action for event in events] == [
            "release_attempted",
            "cloud_blob_deleted",
            "released",
        ]
        assert {event.operation_id for event in events} == {"release:attempt-crash"}
        cloud_event = next(event for event in events if event.action == "cloud_blob_deleted")
        assert cloud_event.detail["outcome"] == "already-absent"


def test_tombstone_commit_survives_crash_before_garbage_collection(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TOMBSTONED commits while bytes remain, and retry alone records deletion."""

    original_resume = retention_module._resume_tombstone_gc
    with session_scope(engine) as session:
        intake_root = _seed_released_intake_for_purge(
            session,
            tmp_path,
            "tombstone-gc-crash",
        )

        def crash_before_gc(
            crash_session: Session,
            row: Intake,
            *,
            actor: str,
        ) -> Any:
            del actor
            assert row.retention_state == RetentionState.TOMBSTONED
            assert row.staging_tombstone_path is not None
            assert Path(row.staging_tombstone_path).exists()
            actions = list(
                crash_session.scalars(
                    select(RetentionEvent.action).where(
                        RetentionEvent.intake_id == row.intake_id
                    )
                )
            )
            assert "staging_tombstoned" in actions
            assert "staging_deleted" not in actions
            raise RuntimeError("simulated crash before tombstone GC")

        monkeypatch.setattr(retention_module, "_resume_tombstone_gc", crash_before_gc)
        with pytest.raises(RuntimeError, match="before tombstone GC"):
            sweep_staging(session, "tombstone-gc-crash", actor="ops")
        assert not intake_root.exists()

    with session_scope(engine) as session:
        intake = session.get(Intake, "tombstone-gc-crash")
        assert intake is not None
        assert intake.retention_state == RetentionState.TOMBSTONED
        assert intake.staging_tombstone_path is not None
        assert Path(intake.staging_tombstone_path).exists()

    monkeypatch.setattr(retention_module, "_resume_tombstone_gc", original_resume)
    with session_scope(engine) as session:
        result = sweep_staging(session, "tombstone-gc-crash", actor="ops")
        assert result.purged
        intake = session.get(Intake, "tombstone-gc-crash")
        assert intake is not None
        assert intake.retention_state == RetentionState.PURGED
        assert intake.staging_tombstone_path is not None
        assert not Path(intake.staging_tombstone_path).exists()
        actions = list(
            session.scalars(
                select(RetentionEvent.action)
                .where(RetentionEvent.intake_id == intake.intake_id)
                .order_by(RetentionEvent.event_id)
            )
        )
        assert actions.count("purge_attempted") == 1
        assert actions.count("staging_tombstoned") == 1
        assert actions.count("staging_deleted") == 1


def test_rename_marker_recovers_when_crash_precedes_tombstoned_commit(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rename with a rolled-back state transition is recognized on retry."""

    original_atomic = retention_module._atomic_tombstone
    intake_root = tmp_path / "rename-marker-crash"

    def crash_during_sweep() -> None:
        with session_scope(engine) as session:
            _seed_released_intake_for_purge(
                session,
                tmp_path,
                "rename-marker-crash",
            )

            def crash_after_rename(source: Path, destination: Path, intake_id: str) -> None:
                original_atomic(source, destination, intake_id)
                raise RuntimeError("simulated crash after tombstone rename")

            monkeypatch.setattr(retention_module, "_atomic_tombstone", crash_after_rename)
            sweep_staging(session, "rename-marker-crash", actor="ops")

    with pytest.raises(RuntimeError, match="after tombstone rename"):
        crash_during_sweep()

    assert not intake_root.exists()
    with session_scope(engine) as session:
        intake = session.get(Intake, "rename-marker-crash")
        assert intake is not None
        assert intake.retention_state == RetentionState.RELEASED
        assert intake.staging_tombstoned_at is None
        assert intake.staging_tombstone_path is None
        assert list(
            session.scalars(
                select(RetentionEvent.action).where(
                    RetentionEvent.intake_id == intake.intake_id,
                    RetentionEvent.action == "purge_attempted",
                )
            )
        ) == ["purge_attempted"]

    monkeypatch.setattr(retention_module, "_atomic_tombstone", original_atomic)
    with session_scope(engine) as session:
        result = sweep_staging(session, "rename-marker-crash", actor="ops")
        assert result.purged
        intake = session.get(Intake, "rename-marker-crash")
        assert intake is not None
        assert intake.retention_state == RetentionState.PURGED
        actions = list(
            session.scalars(
                select(RetentionEvent.action).where(
                    RetentionEvent.intake_id == intake.intake_id
                )
            )
        )
        assert actions.count("purge_attempted") == 1
        assert actions.count("staging_tombstoned") == 1
        assert actions.count("staging_deleted") == 1


def _seed_released_intake_for_purge(
    session: Session,
    tmp_path: Path,
    intake_id: str,
) -> Path:
    pool = _add_pool(
        session,
        artifactclass="s-masters",
        pool_id=f"{intake_id}-pool",
    )
    item = _add_intake_with_item(
        session,
        tmp_path,
        intake_id,
        artifactclass="s-masters",
    )
    _add_asset_copy(
        session,
        item,
        backend_id=pool.backend_id,
        pool_id=pool.id,
        verified=True,
    )
    assert run_retention(session, intake_id, actor="ops").released
    intake = session.get(Intake, intake_id)
    assert intake is not None
    intake.released_at = _now() - dt.timedelta(days=31)
    assert intake.manifest_path is not None
    return Path(intake.manifest_path).parent


def _add_pool(
    session: Session,
    *,
    artifactclass: str,
    pool_id: str,
    offsite_gate: bool = False,
    kind: BackendKind = BackendKind.MEMORY,
) -> Pool:
    backend = Backend(
        name=f"backend-{pool_id}",
        kind=kind,
        tier=BackendTier.SELF_DESCRIBING,
        config={"daemon_endpoint": "unix:/fake.sock"} if kind == BackendKind.REM_TAPE else {},
    )
    session.add(backend)
    session.flush()
    pool = Pool(
        id=pool_id,
        backend_id=backend.id,
        representation=Representation.RAW_BYTES.value,
        offsite_gate=offsite_gate,
    )
    session.add(pool)
    session.add(ArtifactClassPool(artifactclass=artifactclass, pool_id=pool_id))
    session.flush()
    return pool


def _add_intake_with_item(
    session: Session,
    tmp_path: Path,
    intake_id: str,
    *,
    artifactclass: str,
    relpath: str = "clip.mov",
    data: bytes = b"payload",
) -> IngestItem:
    intake_root = tmp_path / intake_id
    source = intake_root / "data" / relpath
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(data)
    manifest = intake_root / "manifest-sha256.txt"
    manifest_bytes = b"manifest\n"
    manifest.write_bytes(manifest_bytes)
    (intake_root / "intake.json").write_text(
        json.dumps({"intake_id": intake_id}),
        encoding="utf-8",
    )
    digest = hashlib.sha256(data).digest()
    intake = Intake(
        intake_id=intake_id,
        operator="tester",
        source_kind=IntakeSourceKind.CARD,
        source_ref="card",
        artifactclass=artifactclass,
        manifest_path=str(manifest),
        manifest_digest=hashlib.sha256(manifest_bytes).hexdigest(),
        status=IntakeStatus.REGISTERED,
        registered_at=_now(),
        retention_state=RetentionState.HELD,
    )
    asset = LogicalAsset(content_sha256=digest, size_bytes=len(data))
    session.add_all([intake, asset])
    session.flush()
    item = IngestItem(
        intake_id=intake_id,
        logical_asset_hash=digest,
        as_received_path=relpath,
        virtual_path=relpath,
        size_bytes=len(data),
        artifactclass=artifactclass,
        item_metadata={"source_path": str(source)},
    )
    session.add(item)
    session.flush()
    return item


def _add_empty_intake(
    session: Session,
    tmp_path: Path,
    intake_id: str,
    *,
    status: IntakeStatus,
    artifactclass: str = "s-masters",
) -> Intake:
    intake_root = tmp_path / intake_id
    intake_root.mkdir(parents=True, exist_ok=True)
    manifest = intake_root / "manifest-sha256.txt"
    manifest_bytes = b"manifest\n"
    manifest.write_bytes(manifest_bytes)
    (intake_root / "intake.json").write_text(
        json.dumps({"intake_id": intake_id}),
        encoding="utf-8",
    )
    intake = Intake(
        intake_id=intake_id,
        operator="tester",
        source_kind=IntakeSourceKind.CARD,
        source_ref="card",
        artifactclass=artifactclass,
        manifest_path=str(manifest),
        manifest_digest=hashlib.sha256(manifest_bytes).hexdigest(),
        status=status,
        registered_at=_now() if status == IntakeStatus.REGISTERED else None,
        quarantined_at=_now() if status == IntakeStatus.QUARANTINED else None,
        retention_state=RetentionState.HELD,
    )
    session.add(intake)
    session.flush()
    return intake


def _add_asset_copy(
    session: Session,
    item: IngestItem,
    *,
    backend_id: int,
    pool_id: str,
    native_locator: dict[str, Any] | None = None,
    tape_uuid: str | None = None,
    verified: bool,
) -> Copy:
    locator = dict(native_locator or {"object": f"copy-{item.id}-{pool_id}"})
    if tape_uuid is not None:
        locator["tape_uuid"] = tape_uuid
    copy = Copy(
        logical_asset_hash=item.logical_asset_hash,
        backend_id=backend_id,
        pool_id=pool_id,
        native_locator=locator,
        native_locator_key=locator_key(locator),
        storage_metadata={"representation": Representation.RAW_BYTES.value},
        integrity_hash=item.logical_asset_hash,
        health=CopyHealth.OK,
        last_checked_at=_now() if verified else None,
        last_measured_digest=item.logical_asset_hash if verified else None,
        last_measured_at=_now() if verified else None,
        source=CopySource.INGEST,
    )
    session.add(copy)
    session.flush()
    return copy


def _add_cloud_copy(session: Session, intake_id: str, locator: dict[str, str]) -> Copy:
    backend = Backend(
        name="cloud-temp",
        kind=BackendKind.MEMORY,
        tier=BackendTier.CATALOG_AUTHORITATIVE,
        config={},
    )
    session.add(backend)
    session.flush()
    pool = Pool(
        id=f"cloud-temp-{intake_id}",
        backend_id=backend.id,
        representation=Representation.RAO_AEAD_V1.value,
        location="cloud-temp",
    )
    bundle = Bundle(
        id=f"cloud-blob:{intake_id}",
        artifactclass="cloud-temp",
        status="sealed",
    )
    session.add_all([pool, bundle])
    session.flush()
    copy = Copy(
        bundle_id=bundle.id,
        backend_id=backend.id,
        pool_id=pool.id,
        native_locator=locator,
        native_locator_key=locator_key(locator),
        storage_metadata={"representation": Representation.RAO_AEAD_V1.value},
        integrity_hash=b"0" * 32,
        health=CopyHealth.OK,
        last_checked_at=_now(),
        source=CopySource.INGEST,
    )
    session.add(copy)
    session.flush()
    return copy


def _add_condition(session: Session, target_key: str, condition: str) -> None:
    session.add(
        ReconciliationCondition(
            domain=DERIVATION_DOMAIN,
            target_key=target_key,
            observed_state=(
                OBSERVED_PRESENT if condition == CONDITION_SATISFIED else OBSERVED_MISSING
            ),
            condition=condition,
        )
    )
    session.flush()


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
