"""Durability predicate tests for mixed asset and bundle copy grains."""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from sutradhara.archive_restore import restore_asset
from sutradhara.artifactclass_policy import ArtifactClassPolicyRecord
from sutradhara.backend.port import BackendLocator, ByteRange, VerifyResult
from sutradhara.catalog.copies import add_bundle_copy, add_copy
from sutradhara.catalog.models import (
    ArtifactClassPool,
    AssetLocator,
    Backend,
    Bundle,
    Copy,
    IngestItem,
    Intake,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import (
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
    IntakeSourceKind,
    IntakeStatus,
    RetentionState,
)
from sutradhara.durability import (
    AssetTarget,
    BundleTarget,
    bundle_replication_status,
    direct_copies,
    durable_placements,
    placement_status,
)
from sutradhara.jobs.models import Job, JobStatus
from sutradhara.jobs.registry import CHECKPOINT_BATCH_STATE_KEY
from sutradhara.replication import replication_status
from sutradhara.sealing.port import Representation
from tests.bundle_group_helpers import bundle_kwargs


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


class _NoReadBackend:
    def __init__(self, name: str) -> None:
        self.name = name

    def enumerate(self) -> Any:
        return iter(())

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        raise NotImplementedError

    def verify(self, locator: BackendLocator) -> VerifyResult:
        raise NotImplementedError


class _StaticExtractor:
    def __init__(self, payload_by_copy_id: dict[int, bytes], *, forbidden: set[int] | None = None):
        self.payload_by_copy_id = payload_by_copy_id
        self.forbidden = forbidden or set()
        self.calls: list[int] = []

    def extract_to_path(
        self,
        *,
        locator: AssetLocator,
        copy: Copy,
        backend: _NoReadBackend,
        destination: Path,
    ) -> None:
        self.calls.append(copy.id)
        if copy.id in self.forbidden:
            raise AssertionError(f"restore should not select copy id={copy.id}")
        destination.write_bytes(self.payload_by_copy_id[copy.id])


def test_durable_placements_axes_and_direct_copies(engine: Engine) -> None:
    payload = b"shared logical asset"
    asset_hash = _digest(payload)
    with session_scope(engine) as session:
        pool = _add_pool(session, "pool-a", artifactclass="masters")
        session.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(payload)))
        session.add_all(
            [
                Bundle(id="masters-1", **bundle_kwargs(seed="masters"), status="sealed"),
                Bundle(id="masters-2", **bundle_kwargs(seed="masters"), status="sealed"),
                Bundle(id="proxies-1", **bundle_kwargs(seed="proxies"), status="sealed"),
            ]
        )
        session.flush()
        asset_copy, _ = add_copy(
            session,
            logical_asset_hash=asset_hash,
            backend_id=pool.backend_id,
            pool_id=pool.id,
            native_locator={"object": "asset-copy"},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
            storage_metadata=_metadata(),
        )
        verified_bundle = _add_bundle_copy_with_locator(
            session,
            bundle_id="masters-1",
            asset_hash=asset_hash,
            pool=pool,
            locator_id="masters-verified",
            verified=True,
        )
        _add_locator(
            session,
            copy=verified_bundle,
            asset_hash=asset_hash,
            pool_id=pool.id,
            bundle_id="masters-1",
            member_path="duplicate-member.bin",
        )
        unverified_bundle = _add_bundle_copy_with_locator(
            session,
            bundle_id="masters-2",
            asset_hash=asset_hash,
            pool=pool,
            locator_id="masters-unverified",
            verified=False,
        )
        wrong_class_bundle = _add_bundle_copy_with_locator(
            session,
            bundle_id="proxies-1",
            asset_hash=asset_hash,
            pool=pool,
            locator_id="proxies-copy",
            verified=True,
        )

        accounting = durable_placements(
            session,
            AssetTarget(asset_hash, "masters"),
            require_verified=False,
            artifactclass="masters",
        )
        verified_accounting = durable_placements(
            session,
            AssetTarget(asset_hash, "masters"),
            require_verified=True,
            artifactclass="masters",
        )
        direct = direct_copies(session, asset_hash)
        bundle_only = durable_placements(
            session,
            BundleTarget("masters-1"),
            require_verified=False,
            artifactclass=None,
        )

    accounting_ids = [copy.id for copy in accounting]
    assert accounting_ids == [asset_copy.id, verified_bundle.id, unverified_bundle.id]
    assert accounting_ids.count(verified_bundle.id) == 1
    assert wrong_class_bundle.id not in accounting_ids
    assert [copy.id for copy in verified_accounting] == [verified_bundle.id]
    assert [copy.id for copy in direct] == [asset_copy.id]
    assert [copy.id for copy in bundle_only] == [verified_bundle.id]


def test_placement_status_flags_duplicate_pool_without_raw_counting(engine: Engine) -> None:
    asset_hash = _digest(b"duplicate target")
    with session_scope(engine) as session:
        pool_a = _add_pool(session, "pool-a", artifactclass="masters", sort_order=0)
        _add_pool(session, "pool-b", artifactclass="masters", sort_order=1)
        session.add(LogicalAsset(content_sha256=asset_hash, size_bytes=16))
        session.flush()
        for locator_id in ("copy-a1", "copy-a2"):
            add_copy(
                session,
                logical_asset_hash=asset_hash,
                backend_id=pool_a.backend_id,
                pool_id=pool_a.id,
                native_locator={"object": locator_id},
                integrity_hash=asset_hash,
                source=CopySource.INGEST,
                health=CopyHealth.OK,
                storage_metadata=_metadata(),
            )

        status = placement_status(session, AssetTarget(asset_hash, "masters"))

    assert status["complete"] is False
    assert len(status["have"]) == 1
    [entry] = list(status["have"])
    assert entry.pool_id == "pool-a"
    assert entry.backend_name == "backend-pool-a"
    assert entry.representation == Representation.RAW_BYTES.value
    assert entry.have is True
    assert entry.duplicate_count == 2
    assert entry.is_duplicate is True
    [missing] = list(status["missing"])
    assert missing.pool_id == "pool-b"
    assert missing.have is False
    assert missing.duplicate_count == 0


def test_open_batch_tracking_does_not_contribute_to_durability_floor(
    engine: Engine,
) -> None:
    asset_hash = _digest(b"written but not checkpointed")
    with session_scope(engine) as session:
        pool = _add_pool(session, "pool-a", artifactclass="masters")
        session.add(LogicalAsset(content_sha256=asset_hash, size_bytes=26))
        session.add(
            Job(
                kind="copy",
                params={"asset_hash": asset_hash.hex()},
                status=JobStatus.RUNNING,
                step_state={
                    CHECKPOINT_BATCH_STATE_KEY: {
                        "batch-a": {
                            "objects": [
                                {
                                    "caller_object_id": asset_hash.hex(),
                                    "provisional_ordinal": 1,
                                    "source": "/staging/object.rao",
                                    "restart_offset": 0,
                                }
                            ]
                        }
                    }
                },
            )
        )
        session.flush()

        accounting = durable_placements(
            session,
            AssetTarget(asset_hash, "masters"),
            require_verified=False,
            artifactclass="masters",
        )
        status = placement_status(session, AssetTarget(asset_hash, "masters"))

    assert accounting == []
    assert status["complete"] is False
    assert status["have"] == set()
    assert {target.pool_id for target in status["missing"]} == {pool.id}


def test_bundle_replication_status_reports_complete_and_missing(engine: Engine) -> None:
    with session_scope(engine) as session:
        pool_a = _add_pool(session, "pool-a", artifactclass="masters", sort_order=0)
        pool_b = _add_pool(session, "pool-b", artifactclass="masters", sort_order=1)
        session.add(Bundle(id="bundle-1", **bundle_kwargs(seed="masters"), status="sealed"))
        session.flush()
        copy_a, _ = add_bundle_copy(
            session,
            bundle_id="bundle-1",
            backend_id=pool_a.backend_id,
            pool_id=pool_a.id,
            native_locator={"object": "bundle-copy-a"},
            integrity_hash=_digest(b"bundle-copy-a"),
            source=CopySource.INGEST,
            health=CopyHealth.OK,
            storage_metadata=_metadata(),
        )
        _qualify_copy(copy_a)

        missing = bundle_replication_status(session, "bundle-1")
        copy_b, _ = add_bundle_copy(
            session,
            bundle_id="bundle-1",
            backend_id=pool_b.backend_id,
            pool_id=pool_b.id,
            native_locator={"object": "bundle-copy-b"},
            integrity_hash=_digest(b"bundle-copy-b"),
            source=CopySource.INGEST,
            health=CopyHealth.OK,
            storage_metadata=_metadata(),
        )
        _qualify_copy(copy_b)
        complete = bundle_replication_status(session, "bundle-1")

    assert missing["complete"] is False
    assert {entry.pool_id for entry in missing["missing"]} == {"pool-b"}
    assert complete["complete"] is True
    assert complete["missing"] == set()
    assert {entry.pool_id for entry in complete["have"]} == {"pool-a", "pool-b"}


def test_replication_status_ignores_bundle_only_asset_copy(engine: Engine) -> None:
    payload = b"bundle-contained asset"
    asset_hash = _digest(payload)
    with session_scope(engine) as session:
        pool = _add_pool(session, "pool-a", artifactclass="masters")
        session.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(payload)))
        session.add(Bundle(id="bundle-asset-only", **bundle_kwargs(seed="masters"), status="sealed"))
        session.flush()
        bundle_copy = _add_bundle_copy_with_locator(
            session,
            bundle_id="bundle-asset-only",
            asset_hash=asset_hash,
            pool=pool,
            locator_id="bundle-copy",
            verified=True,
        )

        asset_status = replication_status(
            session,
            asset_hash,
            "masters",
            {pool.backend_id: _NoReadBackend("backend-pool-a")},
        )
        bundle_status = bundle_replication_status(session, "bundle-asset-only")
        placements = durable_placements(
            session,
            AssetTarget(asset_hash, "masters"),
            require_verified=False,
            artifactclass="masters",
        )

    assert asset_status["complete"] is False
    assert asset_status["have"] == set()
    assert {target.pool_id for target in asset_status["missing"]} == {"pool-a"}
    assert bundle_status["complete"] is True
    assert {target.pool_id for target in bundle_status["have"]} == {"pool-a"}
    assert [copy.id for copy in placements] == [bundle_copy.id]


def test_restore_asset_filters_cross_class_bundle_locators(engine: Engine, tmp_path: Path) -> None:
    payload = b"masters bytes"
    asset_hash = _digest(payload)
    with session_scope(engine) as session:
        wrong_pool = _add_pool(session, "wrong-pool", artifactclass="masters", sort_order=0)
        right_pool = _add_pool(session, "right-pool", artifactclass="masters", sort_order=1)
        session.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(payload)))
        session.add_all(
            [
                Bundle(id="proxy-bundle", **bundle_kwargs(seed="proxies"), status="sealed"),
                Bundle(id="master-bundle", **bundle_kwargs(seed="masters"), status="sealed"),
            ]
        )
        _add_policy_record(session, "masters", ["wrong-pool", "right-pool"])
        session.flush()
        wrong_copy = _add_bundle_copy_with_locator(
            session,
            bundle_id="proxy-bundle",
            asset_hash=asset_hash,
            pool=wrong_pool,
            locator_id="proxy-copy",
            verified=True,
        )
        right_copy = _add_bundle_copy_with_locator(
            session,
            bundle_id="master-bundle",
            asset_hash=asset_hash,
            pool=right_pool,
            locator_id="master-copy",
            verified=True,
        )
        extractor = _StaticExtractor(
            {right_copy.id: payload, wrong_copy.id: b"wrong class bytes"},
            forbidden={wrong_copy.id},
        )
        restored = restore_asset(
            session,
            asset_hash=asset_hash,
            artifactclass="masters",
            destination=tmp_path / "restore.bin",
            backends={
                wrong_pool.backend_id: _NoReadBackend("wrong"),
                right_pool.backend_id: _NoReadBackend("right"),
            },
            extractor=extractor,
        )

    assert restored.copy_id == right_copy.id
    assert extractor.calls == [right_copy.id]
    assert restored.output_path.read_bytes() == payload


def test_restore_asset_admits_null_bundle_locator_via_ingest_membership(
    engine: Engine,
    tmp_path: Path,
) -> None:
    payload = b"legacy locator bytes"
    asset_hash = _digest(payload)
    with session_scope(engine) as session:
        pool = _add_pool(session, "legacy-pool", artifactclass="masters")
        _add_registered_item(session, asset_hash, len(payload), artifactclass="masters")
        _add_policy_record(session, "masters", ["legacy-pool"])
        asset_copy, _ = add_copy(
            session,
            logical_asset_hash=asset_hash,
            backend_id=pool.backend_id,
            pool_id=pool.id,
            native_locator={"object": "legacy-copy"},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
            storage_metadata=_metadata(),
        )
        _add_locator(
            session,
            copy=asset_copy,
            asset_hash=asset_hash,
            pool_id=pool.id,
            bundle_id=None,
            member_path="legacy.bin",
        )
        extractor = _StaticExtractor({asset_copy.id: payload})
        restored = restore_asset(
            session,
            asset_hash=asset_hash,
            artifactclass="masters",
            destination=tmp_path / "legacy.bin",
            backends={pool.backend_id: _NoReadBackend("legacy")},
            extractor=extractor,
        )

    assert restored.copy_id == asset_copy.id
    assert extractor.calls == [asset_copy.id]
    assert restored.output_path.read_bytes() == payload


def _digest(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _metadata() -> dict[str, object]:
    return {"representation": Representation.RAW_BYTES.value}


def _add_pool(
    session: Session,
    pool_id: str,
    *,
    artifactclass: str,
    sort_order: int = 0,
) -> Pool:
    backend = Backend(
        name=f"backend-{pool_id}",
        kind=BackendKind.MEMORY,
        tier=BackendTier.CATALOG_AUTHORITATIVE,
    )
    session.add(backend)
    session.flush()
    pool = Pool(
        id=pool_id,
        backend_id=backend.id,
        representation=Representation.RAW_BYTES.value,
    )
    session.add(pool)
    session.add(
        ArtifactClassPool(
            artifactclass=artifactclass,
            pool_id=pool_id,
            sort_order=sort_order,
        )
    )
    session.flush()
    return pool


def _add_bundle_copy_with_locator(
    session: Session,
    *,
    bundle_id: str,
    asset_hash: bytes,
    pool: Pool,
    locator_id: str,
    verified: bool,
) -> Copy:
    copy, _ = add_bundle_copy(
        session,
        bundle_id=bundle_id,
        backend_id=pool.backend_id,
        pool_id=pool.id,
        native_locator={"object": locator_id},
        integrity_hash=_digest(locator_id.encode("utf-8")),
        source=CopySource.INGEST,
        health=CopyHealth.OK,
        storage_metadata=_metadata(),
    )
    if verified:
        _qualify_copy(copy)
    _add_locator(
        session,
        copy=copy,
        asset_hash=asset_hash,
        pool_id=pool.id,
        bundle_id=bundle_id,
        member_path=f"{locator_id}.bin",
    )
    return copy


def _qualify_copy(copy: Copy) -> None:
    copy.last_checked_at = _now()
    copy.last_measured_digest = copy.integrity_hash
    copy.last_measured_at = _now()


def _add_locator(
    session: Session,
    *,
    copy: Copy,
    asset_hash: bytes,
    pool_id: str,
    bundle_id: str | None,
    member_path: str,
) -> None:
    session.add(
        AssetLocator(
            logical_asset_hash=asset_hash,
            pool_id=pool_id,
            copy_id=copy.id,
            bundle_id=bundle_id,
            native_locator={"size_bytes": 1, "member_path": member_path},
            member_path=member_path,
            representation=Representation.RAW_BYTES.value,
        )
    )
    session.flush()


def _add_registered_item(
    session: Session,
    asset_hash: bytes,
    size_bytes: int,
    *,
    artifactclass: str,
) -> None:
    session.add(LogicalAsset(content_sha256=asset_hash, size_bytes=size_bytes))
    session.add(
        Intake(
            intake_id=f"intake-{artifactclass}",
            operator="tester",
            source_kind=IntakeSourceKind.CARD,
            source_ref="card",
            artifactclass=artifactclass,
            status=IntakeStatus.REGISTERED,
            registered_at=_now(),
            retention_state=RetentionState.HELD,
        )
    )
    session.flush()
    session.add(
        IngestItem(
            intake_id=f"intake-{artifactclass}",
            logical_asset_hash=asset_hash,
            as_received_path="legacy.bin",
            virtual_path="legacy.bin",
            size_bytes=size_bytes,
            artifactclass=artifactclass,
            item_metadata={},
        )
    )
    session.flush()


def _add_policy_record(
    session: Session,
    artifactclass: str,
    restore_preference: list[str],
) -> None:
    session.add(
        ArtifactClassPolicyRecord(
            artifactclass=artifactclass,
            ruleset="test.rules",
            expect="messy",
            target_bytes=1024,
            max_age_seconds=3600,
            restore_preference=restore_preference,
            staging_config={"appledouble": {"action": "off"}, "compression": {"codec": "off"}},
            hdcache_config={"enabled": False, "privacy_level": "none"},
        )
    )
    session.flush()
