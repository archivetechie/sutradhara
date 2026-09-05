"""Tests for archive bundle catalog helpers."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, select

from sutradhara.archive_bundle import (
    UnknownBundlePool,
    add_bundle_member,
    enqueue_artifact,
    get_or_create_open_bundle,
    record_asset_locator,
    record_blob_root,
    record_exclusion,
)
from sutradhara.catalog.copies import add_bundle_copy
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    ArtifactClassPool,
    AssetLocator,
    Backend,
    BlobRoot,
    BundleMember,
    ExclusionRecord,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopySource
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

    with session_scope(engine) as s:
        policy = ArtifactClassPolicyRecord(
            artifactclass="o-archive",
            ruleset="rao.o.v1",
            expect="messy",
            target_bytes=1024,
            max_age_seconds=3600,
            restore_preference=["archive-pool"],
        )
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
        s.add(ArtifactClassPool(artifactclass="o-archive", pool_id="archive-pool", active=True))
        s.add(LogicalAsset(content_sha256=asset_hash, size_bytes=6))
        s.add(policy)
        s.flush()

        bundle, created = get_or_create_open_bundle(
            s,
            artifactclass="o-archive",
            policy=policy,
            bundle_id="bundle-test",
        )
        same_bundle, second_created = get_or_create_open_bundle(
            s,
            artifactclass="o-archive",
            policy=policy,
        )
        member, member_created = add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="o-archive",
            logical_asset_hash=asset_hash,
            member_path="member.bin",
            size_bytes=6,
            file_sha256=file_hash,
        )
        copy, copy_created = add_bundle_copy(
            s,
            bundle_id=bundle.id,
            backend_id=backend.id,
            pool_id="archive-pool",
            native_locator={"pool_id": "archive-pool", "object_id": "bundle-test"},
            integrity_hash=_hash(b"stored-bundle"),
            source=CopySource.INGEST,
            storage_metadata={"representation": Representation.RAO_PLAIN_V1.value},
        )
        locator = record_asset_locator(
            s,
            logical_asset_hash=asset_hash,
            pool_id="archive-pool",
            native_locator={
                "pool_id": "archive-pool",
                "object_id": "bundle-test",
                "member_path": "member.bin",
            },
            representation=Representation.RAO_PLAIN_V1.value,
            copy_id=copy.id,
            bundle_id=bundle.id,
        )
        root = record_blob_root(
            s,
            bundle_id=bundle.id,
            copy_id=copy.id,
            pool_id="archive-pool",
            root_path="member.bin",
            native_locator={"tree": "sha256-tree-v1"},
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
        assert copy_created is True
        assert member.bundle_id == "bundle-test"
        assert bundle.total_bytes == 6
        assert locator.bundle_id == "bundle-test"
        assert root.root_path == "member.bin"
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
                copy_id=0,
                bundle_id="bundle-missing",
            )


def test_enqueue_artifact_escapes_default_member_path_from_raw_filename(
    engine: Engine,
    tmp_path: Path,
) -> None:
    raw_name = b"legacy-\xff\\name.bin"
    raw_path = os.fsencode(tmp_path) + b"/" + raw_name
    fd = os.open(raw_path, os.O_CREAT | os.O_WRONLY, 0o644)
    try:
        os.write(fd, b"payload")
    finally:
        os.close(fd)
    source = Path(os.fsdecode(raw_path))
    asset_hash = _hash(b"payload")

    with session_scope(engine) as s:
        policy = ArtifactClassPolicyRecord(
            artifactclass="o-archive",
            ruleset="rao.o.v1",
            expect="messy",
            target_bytes=1024,
            max_age_seconds=3600,
            restore_preference=[],
        )
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
        s.add(ArtifactClassPool(artifactclass="o-archive", pool_id="archive-pool", active=True))
        s.add_all([policy, LogicalAsset(content_sha256=asset_hash, size_bytes=7)])
        s.flush()

        _, member, _ = enqueue_artifact(
            s,
            artifactclass="o-archive",
            policy=policy,
            logical_asset_hash=asset_hash,
            source_path=source,
            bundle_id="bundle-escape",
        )

        assert member.member_path == r"legacy-\xff\\name.bin"
