"""Enqueue-batch scanning: per-(class, tree root) scans, verdicts, routing.

Every test names the failure it guards. The batch scan closes the P1 gate
condition C4: scanning under ONE representative class's ruleset — or against
a single file's basename — is a preservation-correctness inversion, and both
shapes are pinned here as regressions.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, select

from sutradhara.archive_enqueue import (
    ArchiveEnqueueError,
    BatchScanHeld,
    EnqueueItem,
    enqueue_intake_batch,
    scan_enqueue_batch,
)
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    ArtifactClassPool,
    Backend,
    Bundle,
    BundleMember,
    ExclusionRecord,
    IngestItem,
    Intake,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, IntakeSourceKind, IntakeStatus
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def _digest(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _install_pool(s) -> None:
    backend = Backend(
        name="rem",
        kind=BackendKind.REM_TAPE,
        tier=BackendTier.SELF_DESCRIBING,
    )
    s.add(backend)
    s.flush()
    s.add(
        Pool(
            id="shared-pool",
            backend_id=backend.id,
            representation=Representation.RAO_PLAIN_V1.value,
        )
    )
    s.flush()


def _install_class(
    s,
    artifactclass: str,
    *,
    ruleset: str,
    expect: str = "messy",
    target_bytes: int = 1 << 30,
) -> ArtifactClassPolicyRecord:
    policy = ArtifactClassPolicyRecord(
        artifactclass=artifactclass,
        ruleset=ruleset,
        expect=expect,
        target_bytes=target_bytes,
        max_age_seconds=3600,
        restore_preference=["shared-pool"],
    )
    s.add(policy)
    s.add(ArtifactClassPool(artifactclass=artifactclass, pool_id="shared-pool", active=True))
    s.flush()
    return policy


def _write_tree(root: Path, files: dict[str, bytes]) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for rel, data in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        paths[rel] = path
    return paths


class _ScanBoundary:
    """Mocked ``run_rem_archive_scan`` process boundary."""

    def __init__(self, reports: dict[str | None, dict[str, Any]] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.reports = reports or {}

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        ruleset = kwargs.get("ruleset")
        key = None if ruleset is None else str(ruleset)
        return self.reports.get(key, {"clusters": [], "exclusions": []})


def test_batch_scans_the_tree_root_never_single_files(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guards the round-4 inversion: rules match paths relative to the scan
    root, so a per-file scan sees only basenames and path-scoped exclusion
    rules silently stop firing. The batch must hand rem the TREE ROOT, once."""
    root = tmp_path / "data"
    files = {
        "day-1/a.bin": b"alpha",
        "day-1/b.bin": b"beta",
        "day-2/c.bin": b"gamma",
    }
    paths = _write_tree(root, files)
    scan = _ScanBoundary()
    monkeypatch.setattr("sutradhara.archive_enqueue.run_rem_archive_scan", scan)

    with session_scope(engine) as s:
        _install_pool(s)
        policy = _install_class(s, "photos", ruleset="rules-photos.toml")
        items = [
            EnqueueItem(
                logical_asset_hash=_digest(data),
                source_path=paths[rel],
                member_path=rel,
            )
            for rel, data in files.items()
        ]
        result = scan_enqueue_batch(
            s,
            artifactclass="photos",
            policy=policy,
            scan_root=root,
            items=items,
            staging_root=tmp_path / "stage",
        )
        assert len(result.enqueued) == 3

    # One scan for the whole batch, over the root — not one per file.
    assert len(scan.calls) == 1
    assert scan.calls[0]["inputs"] == [root]
    assert str(scan.calls[0]["ruleset"]) == "rules-photos.toml"


def test_each_class_scans_under_its_own_ruleset(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guards the C4 hazard: a multi-class batch scanned under ONE
    representative class's ruleset. Each (class, root) pair gets its own
    scan invocation with its own ruleset."""
    root = tmp_path / "data"
    paths = _write_tree(root, {"a.jpg": b"photo-bytes", "a.wav": b"audio-bytes"})
    scan = _ScanBoundary()
    monkeypatch.setattr("sutradhara.archive_enqueue.run_rem_archive_scan", scan)

    with session_scope(engine) as s:
        _install_pool(s)
        photo_policy = _install_class(s, "photos", ruleset="rules-photos.toml")
        audio_policy = _install_class(s, "audio", ruleset="rules-audio.toml")
        for policy, rel, data in (
            (photo_policy, "a.jpg", b"photo-bytes"),
            (audio_policy, "a.wav", b"audio-bytes"),
        ):
            scan_enqueue_batch(
                s,
                artifactclass=policy.artifactclass,
                policy=policy,
                scan_root=root,
                items=[
                    EnqueueItem(
                        logical_asset_hash=_digest(data),
                        source_path=paths[rel],
                        member_path=rel,
                    )
                ],
                staging_root=tmp_path / "stage",
            )

    invocations = {str(call["ruleset"]) for call in scan.calls}
    assert invocations == {"rules-photos.toml", "rules-audio.toml"}
    assert all(call["inputs"] == [root] for call in scan.calls)


def test_path_scoped_exclusion_rule_fires_for_batch_scanned_tree(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The round-4 inversion case, pinned: a path-scoped exclusion cluster
    ("tmp/") reported for the scanned tree keeps its members OUT of the
    accumulator, exactly as the pre-change flush-time scan excluded them."""
    root = tmp_path / "data"
    files = {
        "tmp/scratch.bin": b"scratch",
        "keep/a.bin": b"keeper",
    }
    paths = _write_tree(root, files)
    scan = _ScanBoundary(
        reports={
            "rules-photos.toml": {
                "clusters": [],
                "exclusions": [
                    {
                        "prefix": "tmp/",
                        "reason": "exclude-rule",
                        "count": 1,
                        "bytes_total": 7,
                        "samples": ["tmp/scratch.bin"],
                    }
                ],
            }
        }
    )
    monkeypatch.setattr("sutradhara.archive_enqueue.run_rem_archive_scan", scan)

    with session_scope(engine) as s:
        _install_pool(s)
        policy = _install_class(s, "photos", ruleset="rules-photos.toml")
        items = [
            EnqueueItem(
                logical_asset_hash=_digest(data),
                source_path=paths[rel],
                member_path=rel,
            )
            for rel, data in files.items()
        ]
        result = scan_enqueue_batch(
            s,
            artifactclass="photos",
            policy=policy,
            scan_root=root,
            items=items,
            staging_root=tmp_path / "stage",
        )
        assert [member.member_path for member in result.enqueued] == ["keep/a.bin"]
        assert result.excluded_prefixes == ("tmp/",)
        member_paths = set(s.scalars(select(BundleMember.member_path)))
        assert member_paths == {"keep/a.bin"}


def test_cluster_exclusion_rows_carry_null_hash_prefix_and_scan_identity(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cluster exclusions arrive as prefix clusters with no digests and a
    never-ingested file has no LogicalAsset row: the recorded row must carry
    a NULL hash, the prefix, and the (class, root) scan identity — and a
    re-run of the same batch must not duplicate it."""
    root = tmp_path / "data"
    files = {"tmp/junk.bin": b"junk", "keep/a.bin": b"keeper"}
    paths = _write_tree(root, files)
    scan = _ScanBoundary(
        reports={
            "rules-photos.toml": {
                "clusters": [],
                "exclusions": [
                    {
                        "prefix": "tmp/",
                        "reason": "exclude-rule",
                        "count": 1,
                        "bytes_total": 4,
                        "samples": ["tmp/junk.bin"],
                    }
                ],
            }
        }
    )
    monkeypatch.setattr("sutradhara.archive_enqueue.run_rem_archive_scan", scan)

    with session_scope(engine) as s:
        _install_pool(s)
        policy = _install_class(s, "photos", ruleset="rules-photos.toml")
        items = [
            EnqueueItem(
                logical_asset_hash=_digest(data),
                source_path=paths[rel],
                member_path=rel,
            )
            for rel, data in files.items()
        ]
        for _ in range(2):  # second run must be idempotent on the cluster row
            scan_enqueue_batch(
                s,
                artifactclass="photos",
                policy=policy,
                scan_root=root,
                items=items,
                staging_root=tmp_path / "stage",
            )
        rows = list(s.scalars(select(ExclusionRecord)))
        assert len(rows) == 1
        [row] = rows
        assert row.logical_asset_hash is None
        assert row.path == "tmp/"
        assert row.artifactclass == "photos"
        assert row.reason == "exclude-rule"
        assert row.ruleset_name == "rules-photos.toml"
        assert row.detail is not None
        assert row.detail["scan_root"] == str(root)
        assert row.detail["prefix"] == "tmp/"


def test_blob_verdict_recorded_and_routed_off_the_accumulator(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A blob verdict must be durably recorded and its subtree withheld from
    the group accumulator (routed to the funnel path); silently bundling a
    blob-verdict subtree would re-open rule-driven blob-wrapping for group
    flushes, which is out of scope by design."""
    root = tmp_path / "data"
    files = {"proj.fcpbundle/inner.bin": b"opaque", "keep/a.bin": b"keeper"}
    paths = _write_tree(root, files)
    scan = _ScanBoundary(
        reports={
            "rules-photos.toml": {
                "clusters": [
                    {
                        "prefix": "proj.fcpbundle/",
                        "reason": "blob-rule",
                        "count": 1,
                        "bytes_total": 6,
                        "samples": ["proj.fcpbundle/inner.bin"],
                    }
                ],
                "exclusions": [],
            }
        }
    )
    monkeypatch.setattr("sutradhara.archive_enqueue.run_rem_archive_scan", scan)
    events: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        "sutradhara.archive_enqueue.emit_structured_event",
        lambda name, **fields: events.append((name, fields)),
    )

    with session_scope(engine) as s:
        _install_pool(s)
        policy = _install_class(s, "photos", ruleset="rules-photos.toml")
        items = [
            EnqueueItem(
                logical_asset_hash=_digest(data),
                source_path=paths[rel],
                member_path=rel,
            )
            for rel, data in files.items()
        ]
        result = scan_enqueue_batch(
            s,
            artifactclass="photos",
            policy=policy,
            scan_root=root,
            items=items,
            staging_root=tmp_path / "stage",
        )
        assert [member.member_path for member in result.enqueued] == ["keep/a.bin"]
        assert result.blob_prefixes == ("proj.fcpbundle/",)
        [row] = list(s.scalars(select(ExclusionRecord)))
        assert row.reason == "blob-rule"
        assert row.logical_asset_hash is None
        assert row.detail is not None
        assert row.detail["routed_to"] == "cloud-blob"
    assert [name for name, _ in events] == ["enqueue_blob_routed"]


def test_compliant_expect_batch_refuses_on_deviations_touching_nothing(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Migrated flush-time hold gate: a compliant-expect class whose scan
    reports deviations must refuse the batch BEFORE anything lands — holding
    the shared group accumulator would gate other classes' durability on one
    class's review latency."""
    root = tmp_path / "data"
    paths = _write_tree(root, {"a.bin": b"alpha"})
    scan = _ScanBoundary(
        reports={
            "rules-photos.toml": {
                "clusters": [
                    {
                        "prefix": "a.bin",
                        "reason": "unsupported-entry",
                        "count": 1,
                        "bytes_total": 5,
                        "samples": ["a.bin"],
                    }
                ],
                "exclusions": [],
            }
        }
    )
    monkeypatch.setattr("sutradhara.archive_enqueue.run_rem_archive_scan", scan)

    with session_scope(engine) as s:
        _install_pool(s)
        policy = _install_class(s, "photos", ruleset="rules-photos.toml", expect="compliant")
        with pytest.raises(BatchScanHeld) as held:
            scan_enqueue_batch(
                s,
                artifactclass="photos",
                policy=policy,
                scan_root=root,
                items=[
                    EnqueueItem(
                        logical_asset_hash=_digest(b"alpha"),
                        source_path=paths["a.bin"],
                        member_path="a.bin",
                    )
                ],
                staging_root=tmp_path / "stage",
            )
        assert held.value.summary["clusters"][0]["reason"] == "unsupported-entry"
        assert list(s.scalars(select(Bundle))) == []
        assert list(s.scalars(select(BundleMember))) == []
        assert list(s.scalars(select(ExclusionRecord))) == []


def test_intake_batch_enqueue_over_fixture_tree(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The intake side had no batch enqueue at all (the CLI was
    file-at-a-time). The new batch anchors on (IngestItem.artifactclass,
    Intake.manifest_path.parent/'data'), scans once per class over that
    root, and preserves tree-relative member paths."""
    intake_root = tmp_path / "intake-1"
    data_root = intake_root / "data"
    files = {
        "day-1/a.jpg": b"photo-a",
        "day-1/b.jpg": b"photo-b",
        "audio/a.wav": b"audio-a",
    }
    _write_tree(data_root, files)
    (intake_root / "manifest-sha256.txt").write_text("stub\n")
    scan = _ScanBoundary()
    monkeypatch.setattr("sutradhara.archive_enqueue.run_rem_archive_scan", scan)

    with session_scope(engine) as s:
        _install_pool(s)
        _install_class(s, "photos", ruleset="rules-photos.toml")
        _install_class(s, "audio", ruleset="rules-audio.toml")
        intake = Intake(
            intake_id="intake-1",
            operator="op",
            source_kind=IntakeSourceKind.CARD,
            artifactclass="photos",
            manifest_path=str(intake_root / "manifest-sha256.txt"),
            status=IntakeStatus.REGISTERED,
        )
        s.add(intake)
        for data in files.values():
            digest = _digest(data)
            if s.get(LogicalAsset, digest) is None:
                s.add(LogicalAsset(content_sha256=digest, size_bytes=len(data)))
        s.flush()
        for rel, data in files.items():
            s.add(
                IngestItem(
                    intake_id="intake-1",
                    logical_asset_hash=_digest(data),
                    as_received_path=rel,
                    virtual_path=rel,
                    size_bytes=len(data),
                    artifactclass="audio" if rel.startswith("audio/") else "photos",
                )
            )
        s.flush()

        results = enqueue_intake_batch(s, intake_id="intake-1")

        assert [result.artifactclass for result in results] == ["audio", "photos"]
        by_class = {result.artifactclass: result for result in results}
        assert {m.member_path for m in by_class["photos"].enqueued} == {
            "day-1/a.jpg",
            "day-1/b.jpg",
        }
        assert {m.member_path for m in by_class["audio"].enqueued} == {"audio/a.wav"}
        # Members of one class share one accumulator bundle.
        photo_bundles = {m.bundle_id for m in by_class["photos"].enqueued}
        assert len(photo_bundles) == 1
        members = list(s.scalars(select(BundleMember)))
        assert {m.member_path for m in members} == set(files)

    # One scan per (class, root): two classes, three files, two invocations,
    # each over the intake data root.
    assert len(scan.calls) == 2
    assert {str(call["ruleset"]) for call in scan.calls} == {
        "rules-photos.toml",
        "rules-audio.toml",
    }
    assert all(call["inputs"] == [data_root.resolve()] for call in scan.calls)


def test_intake_batch_requires_registered_intake_with_manifest(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """The scan-root anchor is the manifest's data dir; an unregistered or
    manifest-less intake must refuse loudly instead of guessing a root."""
    with session_scope(engine) as s:
        s.add(
            Intake(
                intake_id="intake-x",
                operator="op",
                source_kind=IntakeSourceKind.CARD,
                artifactclass="photos",
                manifest_path=None,
                status=IntakeStatus.REGISTERED,
            )
        )
        s.flush()
        with pytest.raises(ArchiveEnqueueError, match="manifest_path"):
            enqueue_intake_batch(s, intake_id="intake-x")
        with pytest.raises(ArchiveEnqueueError, match="no intake"):
            enqueue_intake_batch(s, intake_id="missing")
