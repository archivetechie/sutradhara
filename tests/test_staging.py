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
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    ArtifactClassPool,
    Backend,
    BundleMember,
    Pool,
    StagingTransform,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier
from sutradhara.staging import StagingHeld, stage_and_enqueue_artifact


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def _add_placement(s, artifactclass: str) -> None:
    backend = Backend(
        name=f"rem-{artifactclass}",
        kind=BackendKind.REM_TAPE,
        tier=BackendTier.SELF_DESCRIBING,
    )
    s.add(backend)
    s.flush()
    pool_id = f"pool-{artifactclass}"
    if s.get(Pool, pool_id) is None:
        s.add(Pool(id=pool_id, backend_id=backend.id, representation="rao-plain-v1"))
    s.add(ArtifactClassPool(artifactclass=artifactclass, pool_id=pool_id, active=True))
    s.flush()


def test_appledouble_merge_records_transform_and_consumes_sidecar(
    engine: Engine,
    tmp_path: Path,
) -> None:
    _require_xattrs(tmp_path)
    source = tmp_path / "photo.tif"
    source.write_bytes(b"image-data")
    resource = b"resource-fork"
    finder_info = b"F" * 32
    tags = b"bplist-tag-data"
    source.with_name("._photo.tif").write_bytes(
        _appledouble(
            resource,
            finder_info,
            attrs={"com.apple.metadata:_kMDItemUserTags": tags},
        )
    )

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
        _add_placement(s, "photo")

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
    assert os.getxattr(staged.staged_path, "user.com.apple.metadata:_kMDItemUserTags") == tags
    assert [member.member_path for member in members] == ["photo.tif"]
    assert [transform.kind for transform in transforms] == ["appledouble-merge-v1"]
    assert transforms[0].reversible is False
    assert transforms[0].result["merged"] is True
    assert transforms[0].result["consumed_sidecar"] is True
    assert "user.com.apple.metadata:_kMDItemUserTags" in transforms[0].result["xattrs"]


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
        _add_placement(s, "photo")

        with pytest.raises(StagingHeld):
            stage_and_enqueue_artifact(
                s,
                artifactclass="photo",
                policy=policy,
                source_path=source,
                staging_root=tmp_path / "stage",
                bundle_id="bundle-held",
            )


def test_appledouble_sidecar_source_holds_instead_of_enqueuing(
    engine: Engine,
    tmp_path: Path,
) -> None:
    source = tmp_path / "photo.tif"
    source.write_bytes(b"image-data")
    sidecar = tmp_path / "._photo.tif"
    sidecar.write_bytes(b"metadata")

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
        _add_placement(s, "photo")

        with pytest.raises(StagingHeld) as raised:
            stage_and_enqueue_artifact(
                s,
                artifactclass="photo",
                policy=policy,
                source_path=sidecar,
                staging_root=tmp_path / "stage",
                bundle_id="bundle-photo",
            )
        members = list(s.scalars(select(BundleMember)))

    cluster = raised.value.summary["clusters"][0]
    assert cluster["reason"] == "appledouble-sidecar-consumed"
    assert members == []


def _require_xattrs(tmp_path: Path) -> None:
    probe = tmp_path / "probe"
    probe.write_bytes(b"")
    try:
        os.setxattr(probe, "user.sutradhara-test", b"ok")
    except OSError as exc:
        pytest.skip(f"filesystem does not support user xattrs: {exc}")


def _appledouble(
    resource: bytes,
    finder_info: bytes,
    *,
    attrs: dict[str, bytes] | None = None,
) -> bytes:
    assert len(finder_info) == 32
    header_len = 26 + 2 * 12
    resource_offset = header_len
    finder_offset = resource_offset + len(resource)
    finder_payload = finder_info + _appledouble_attr_blob(finder_offset + len(finder_info), attrs)
    header = struct.pack(">II16sH", 0x00051607, 0x00020000, b"\0" * 16, 2)
    entries = b"".join(
        [
            struct.pack(">III", 2, resource_offset, len(resource)),
            struct.pack(">III", 9, finder_offset, len(finder_payload)),
        ]
    )
    return header + entries + resource + finder_payload


def _appledouble_attr_blob(finder_info_end: int, attrs: dict[str, bytes] | None) -> bytes:
    if not attrs:
        return b""
    pad = b"\0\0"
    attr_start = finder_info_end + len(pad)
    header_size = 36
    entries = bytearray()
    data = bytearray()
    names_and_values = [(name.encode("utf-8") + b"\0", value) for name, value in attrs.items()]
    data_start = (
        attr_start + header_size + sum(_align4(11 + len(name)) for name, _value in names_and_values)
    )
    for name, value in names_and_values:
        value_offset = data_start + len(data)
        entries.extend(struct.pack(">IIHB", value_offset, len(value), 0, len(name)))
        entries.extend(name)
        entries.extend(b"\0" * (_align4(11 + len(name)) - 11 - len(name)))
        data.extend(value)
    header = struct.pack(
        ">IIIII3IHH",
        0x41545452,
        0,
        header_size + len(entries) + len(data),
        data_start,
        len(data),
        0,
        0,
        0,
        0,
        len(names_and_values),
    )
    return pad + header + bytes(entries) + bytes(data)


def _align4(value: int) -> int:
    return (value + 3) & ~3
