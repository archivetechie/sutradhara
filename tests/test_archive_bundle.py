"""Tests for archive bundle catalog helpers."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator

from sqlalchemy import Engine, select

import pytest

from sutradhara.archive_bundle import (
    UnknownBundlePool,
    add_bundle_member,
    get_or_create_open_bundle,
    record_asset_locator,
    record_blob_root,
    record_exclusion,
)
from sutradhara.catalog.models import (
    AssetLocator,
    Backend,
    BlobRoot,
    BundleMember,
    ExclusionRecord,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def _hash(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def test_bundle_helpers_record_members_locators_roots_and_exclusions(
    engine: Engine,
) -> None:
    asset_hash = _hash(b"member")
    file_hash = _hash(b"file")
    root_hash = _hash(b"root")

    with session_scope(engine) as s:
        backend = Backend(
            name="rem",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(backend)
        s.flush()
        s.add(
            Pool(
                id="archive-pool",
                backend_id=backend.id,
                representation=Representation.RAO_PLAIN_V1.value,
            )
        )
        s.add(LogicalAsset(content_sha256=asset_hash, size_bytes=6))
        s.flush()

        bundle, created = get_or_create_open_bundle(
            s,
            artifactclass="o-archive",
            representation=Representation.RAO_PLAIN_V1.value,
            bundle_id="bundle-test",
        )
        same_bundle, second_created = get_or_create_open_bundle(
            s,
            artifactclass="o-archive",
            representation=Representation.RAO_PLAIN_V1.value,
        )
        member, member_created = add_bundle_member(
            s,
            bundle=bundle,
            logical_asset_hash=asset_hash,
            member_path="member.bin",
            size_bytes=6,
            file_sha256=file_hash,
        )
        locator = record_asset_locator(
            s,
            logical_asset_hash=asset_hash,
            pool_id="archive-pool",
            native_locator={"pool_id": "archive-pool", "object_id": "bundle-test"},
            representation=Representation.RAO_PLAIN_V1.value,
            bundle_id=bundle.id,
        )
        root = record_blob_root(
            s,
            logical_asset_hash=asset_hash,
            algorithm="sha256-tree-v1",
            root_hash=root_hash,
        )
        exclusion = record_exclusion(
            s,
            artifactclass="o-archive",
            reason="unsupported-entry",
            path="skip.tmp",
            detail={"kind": "socket"},
        )

        assert created is True
        assert second_created is False
        assert same_bundle.id == "bundle-test"
        assert member_created is True
        assert member.bundle_id == "bundle-test"
        assert bundle.total_bytes == 6
        assert locator.bundle_id == "bundle-test"
        assert root.root_hash == root_hash
        assert exclusion.reason == "unsupported-entry"

        assert len(list(s.scalars(select(BundleMember)))) == 1
        assert len(list(s.scalars(select(AssetLocator)))) == 1
        assert len(list(s.scalars(select(BlobRoot)))) == 1
        assert len(list(s.scalars(select(ExclusionRecord)))) == 1


def test_record_asset_locator_rejects_unknown_pool(engine: Engine) -> None:
    asset_hash = _hash(b"member")
    with session_scope(engine) as s:
        s.add(LogicalAsset(content_sha256=asset_hash, size_bytes=6))
        s.flush()
        with pytest.raises(UnknownBundlePool, match="no Pool"):
            record_asset_locator(
                s,
                logical_asset_hash=asset_hash,
                pool_id="missing",
                native_locator={},
                representation=Representation.RAW_BYTES.value,
            )
