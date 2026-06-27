"""Retention gate tests for temporary-byte deletion safety."""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

import sutradhara.retention as retention_module
from sutradhara.arrangement import ArrangementError, create_from_intake
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


class _DeleteBackend:
    def __init__(self) -> None:
        self.objects: set[str] = set()
        self.deleted: list[dict[str, Any]] = []

    def add(self, key: str) -> dict[str, str]:
        self.objects.add(key)
        return {"key": key}

    def delete_object(self, locator: dict[str, Any]) -> None:
        key = str(locator["key"])
        self.objects.discard(key)
        self.deleted.append(dict(locator))


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

        copy.last_verified_at = _now()
        assert not releasable(session, "intake-a")

        confirm_offsite(session, media_id="tape:tape-a", confirmed_by="ops")
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
            last_verified_at=_now(),
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

    def _backend_from_row(row: Backend) -> _DeleteBackend:
        assert row.name == "cloud-temp"
        return fake_cloud

    monkeypatch.setattr(retention_module.factory, "backend_from_row", _backend_from_row)

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
        assert session.get(Copy, cloud_copy.id).deleted_at is None

        confirm_offsite(session, media_id="tape:tape-e", confirmed_by="ops")
        released = run_retention(session, "intake-e", actor="ops")
        assert released.released
        assert released.deleted_copy_ids == (cloud_copy.id,)
        assert fake_cloud.deleted == [{"key": "cloud-e"}]
        assert fake_cloud.objects == set()
        assert session.get(Copy, cloud_copy.id).deleted_at is not None
        intake = session.get(Intake, "intake-e")
        assert intake is not None
        assert intake.retention_state == RetentionState.RELEASED
        assert intake.released_at is not None
        assert Path(str(intake.manifest_path)).parent.exists()
        assert session.scalar(select(func.count()).select_from(RetentionEvent)) == 2

        again = run_retention(session, "intake-e", actor="ops")
        assert not again.released
        assert len(fake_cloud.deleted) == 1


def test_release_freezes_new_work_and_sweep_staging_after_grace(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_cloud = _DeleteBackend()
    monkeypatch.setattr(retention_module.factory, "backend_from_row", lambda _row: fake_cloud)

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
        assert session.scalar(select(func.count()).select_from(RetentionEvent)) == 3


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
    manifest.write_text("manifest\n", encoding="utf-8")
    digest = hashlib.sha256(data).digest()
    intake = Intake(
        intake_id=intake_id,
        operator="tester",
        source_kind=IntakeSourceKind.CARD,
        source_ref="card",
        artifactclass=artifactclass,
        manifest_path=str(manifest),
        manifest_digest="manifest",
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
        last_verified_at=_now() if verified else None,
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
        last_verified_at=_now(),
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
