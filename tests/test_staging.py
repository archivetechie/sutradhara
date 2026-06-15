"""Tests for archive staging transforms."""

from __future__ import annotations

import os
import struct
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, select

from sutradhara.artifactclass_policy import (
    AppleDoubleStagingPolicy,
    StagingPolicy,
)
from sutradhara.catalog.models import ArtifactClassPolicyRecord, BundleMember, StagingTransform
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.staging import StagingHeld, stage_and_enqueue_artifact


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_appledouble_merge_records_transform_and_consumes_sidecar(
    engine: Engine,
    tmp_path: Path,
) -> None:
    _require_xattrs(tmp_path)
    source = tmp_path / "photo.tif"
    source.write_bytes(b"image-data")
    resource = b"resource-fork"
    finder_info = b"F" * 32
    source.with_name("._photo.tif").write_bytes(_appledouble(resource, finder_info))

    with session_scope(engine) as s:
        policy = ArtifactClassPolicyRecord(
            artifactclass="photo",
            ruleset="rao.photo.v1",
            expect="messy",
            target_bytes=1024,
            max_age_seconds=60,
            restore_preference=[],
            staging_config=StagingPolicy(
                appledouble=AppleDoubleStagingPolicy(action="merge-to-xattrs")
            ).to_json(),
        )
        s.add(policy)
        s.flush()

        staged = stage_and_enqueue_artifact(
            s,
            artifactclass="photo",
            policy=policy,
            source_path=source,
            staging_root=tmp_path / "stage",
            bundle_id="bundle-photo",
        )

        members = list(s.scalars(select(BundleMember)))
        transforms = list(s.scalars(select(StagingTransform)))

    assert staged.staged_path.read_bytes() == b"image-data"
    assert not staged.staged_path.with_name("._photo.tif").exists()
    assert os.getxattr(staged.staged_path, "user.com.apple.ResourceFork") == resource
    assert os.getxattr(staged.staged_path, "user.com.apple.FinderInfo") == finder_info
    assert [member.member_path for member in members] == ["photo.tif"]
    assert [transform.kind for transform in transforms] == ["appledouble-merge-v1"]
    assert transforms[0].reversible is False
    assert transforms[0].result["merged"] is True


def test_malformed_appledouble_holds_open_bundle(
    engine: Engine,
    tmp_path: Path,
) -> None:
    _require_xattrs(tmp_path)
    source = tmp_path / "photo.tif"
    source.write_bytes(b"image-data")
    source.with_name("._photo.tif").write_bytes(b"not-appledouble")

    with session_scope(engine) as s:
        policy = ArtifactClassPolicyRecord(
            artifactclass="photo",
            ruleset="rao.photo.v1",
            expect="messy",
            target_bytes=1024,
            max_age_seconds=60,
            restore_preference=[],
            staging_config=StagingPolicy(
                appledouble=AppleDoubleStagingPolicy(action="merge-to-xattrs")
            ).to_json(),
        )
        s.add(policy)
        s.flush()

        with pytest.raises(StagingHeld):
            stage_and_enqueue_artifact(
                s,
                artifactclass="photo",
                policy=policy,
                source_path=source,
                staging_root=tmp_path / "stage",
                bundle_id="bundle-held",
            )


def _require_xattrs(tmp_path: Path) -> None:
    probe = tmp_path / "probe"
    probe.write_bytes(b"")
    try:
        os.setxattr(probe, "user.sutradhara-test", b"ok")
    except OSError as exc:
        pytest.skip(f"filesystem does not support user xattrs: {exc}")


def _appledouble(resource: bytes, finder_info: bytes) -> bytes:
    header_len = 26 + 2 * 12
    resource_offset = header_len
    finder_offset = resource_offset + len(resource)
    header = struct.pack(">II16sH", 0x00051607, 0x00020000, b"\0" * 16, 2)
    entries = b"".join(
        [
            struct.pack(">III", 2, resource_offset, len(resource)),
            struct.pack(">III", 9, finder_offset, len(finder_info)),
        ]
    )
    return header + entries + resource + finder_info
