"""Phase R ingest job handler tests."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, func, select

import sutradhara.jobs.handlers  # noqa: F401 - register built-in handlers
from sutradhara.backend.port import CopyRecord
from sutradhara.catalog.facts import (
    record_copy,
    record_derivation,
    record_index,
    record_validity,
)
from sutradhara.catalog.models import (
    AssetDerivation,
    Backend,
    Bundle,
    Copy,
    IngestItem,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, make_engine, make_session_factory, session_scope
from sutradhara.catalog.types import (
    AssetValidity,
    BackendKind,
    BackendTier,
    CopySource,
    MediaKind,
    content_hash,
)
from sutradhara.intake import scan_landing_root
from sutradhara.jobs.engine import run_one, submit
from sutradhara.jobs.models import Job, JobStatus


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_dispatch_runs_proxies_pfr_and_cloud_copy(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUTRADHARA_FAKE_TRANSCODE", "1")
    monkeypatch.setenv("SUTRADHARA_FAKE_FFPROBE", "1")
    monkeypatch.setenv("SUTRADHARA_FAKE_CLOUD_BLOB", "1")
    fake_backend = _FakeObjectBackend("cloud-temp")
    monkeypatch.setattr("sutradhara.backend.factory.backend_from_row", lambda row: fake_backend)

    landing = tmp_path / "landing"
    _write_intake(landing, "card-100", {"clip.mov": b"valid video payload"})

    with session_scope(engine) as session:
        _add_cloud_backend(session)
        scan_landing_root(session, landing, cache_root=tmp_path / "cache")
        jobs = list(session.scalars(select(Job).order_by(Job.kind)))
        assert [job.kind for job in jobs] == ["cloud-blob", "pfr-index", "transcode"]
        for job in jobs:
            leases = {"cpu": 8} if job.kind == "transcode" else {"io": 1}
            result = run_one(session, job.id, granted_leases=leases)
            assert result.ok

    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 3
        assert session.scalar(select(func.count()).select_from(AssetDerivation)) == 2
        kinds = set(session.scalars(select(AssetDerivation.kind)))
        assert kinds == {"mezz", "preview"}
        source_item = session.scalars(
            select(IngestItem).where(IngestItem.as_received_path == "clip.mov")
        ).one()
        assert Path(source_item.item_metadata["pfr_sidecar_path"]).exists()
        source_asset = session.get(LogicalAsset, source_item.logical_asset_hash)
        assert source_asset is not None
        assert source_asset.validity == AssetValidity.OK
        bundle = session.get(Bundle, "cloud-blob:card-100")
        assert bundle is not None
        assert bundle.status == "sealed"
        copy = session.scalars(select(Copy).where(Copy.bundle_id == bundle.id)).one()
        assert copy.native_locator["key"] == "intakes/card-100.rao"
        assert copy.storage_metadata["representation"] == "rao-aead-v1"
        assert fake_backend.objects["intakes/card-100.rao"]


def test_transcode_derivation_facts_are_idempotent(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUTRADHARA_FAKE_TRANSCODE", "1")
    landing = tmp_path / "landing"
    _write_intake(landing, "card-110", {"clip.mov": b"valid video payload"})

    with session_scope(engine) as session:
        scan_landing_root(session, landing, enqueue_jobs=False, cache_root=tmp_path / "cache")
        item_id = session.scalars(select(IngestItem.id)).one()

    first_ids = _run_transcode_job(engine, item_id, tmp_path)
    second_ids = _run_transcode_job(engine, item_id, tmp_path)

    assert second_ids == first_ids
    with session_scope(engine) as session:
        derived = list(
            session.scalars(
                select(IngestItem)
                .where(IngestItem.as_received_path.like("derived/%"))
                .order_by(IngestItem.as_received_path)
            )
        )
        assert [item.id for item in derived] == [first_ids["mezz"], first_ids["preview"]]
        assert {item.item_metadata["kind"] for item in derived} == {"mezz", "preview"}
        assert {item.artifactclass for item in derived} == {"proxy"}
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 3
        assert session.scalar(select(func.count()).select_from(AssetDerivation)) == 2
        assert session.scalar(select(func.count()).select_from(LogicalAsset)) == 3
        edges = list(session.scalars(select(AssetDerivation).order_by(AssetDerivation.kind)))
        assert [edge.kind for edge in edges] == ["mezz", "preview"]


def test_pfr_index_fact_is_idempotent_and_preserves_metadata(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUTRADHARA_FAKE_FFPROBE", "1")
    landing = tmp_path / "landing"
    _write_intake(landing, "card-111", {"clip.mov": b"valid video payload"})

    with session_scope(engine) as session:
        scan_landing_root(session, landing, enqueue_jobs=False, cache_root=tmp_path / "cache")
        item = session.scalars(select(IngestItem)).one()
        item.item_metadata = {**(item.item_metadata or {}), "operator_note": "keep me"}
        item_id = item.id

    first_path = _run_pfr_index_job(engine, item_id, tmp_path)
    second_path = _run_pfr_index_job(engine, item_id, tmp_path)

    assert second_path == first_path
    with session_scope(engine) as session:
        fetched_item = session.get(IngestItem, item_id)
        assert fetched_item is not None
        assert fetched_item.item_metadata["pfr_sidecar_path"] == first_path
        assert fetched_item.item_metadata["operator_note"] == "keep me"
        assert Path(first_path).exists()


def test_transcode_decode_error_marks_master_suspect(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUTRADHARA_FAKE_TRANSCODE", "1")
    landing = tmp_path / "landing"
    _write_intake(landing, "card-101", {"clip.mov": b"DECODE_FAIL damaged"})

    with session_scope(engine) as session:
        scan_landing_root(session, landing, enqueue_jobs=False, cache_root=tmp_path / "cache")
        item = session.scalars(select(IngestItem)).one()
        job = submit(
            session,
            "transcode",
            {
                "ingest_item_id": item.id,
                "cache_root": str(tmp_path / "cache"),
                "proxy_artifactclass": "proxy",
            },
            required_resources=[{"pool": "cpu", "count": 8}],
        )
        result = run_one(session, job.id, granted_leases={"cpu": 8})
        assert result.ok

    with session_scope(engine) as session:
        item = session.scalars(select(IngestItem)).one()
        asset = session.get(LogicalAsset, item.logical_asset_hash)
        assert asset is not None
        assert asset.validity == AssetValidity.SUSPECT
        assert session.scalar(select(func.count()).select_from(AssetDerivation)) == 0
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 1
        job_row = session.get(Job, job.id)
        assert job_row is not None
        assert job_row.status == JobStatus.SUCCEEDED


def test_transcode_read_error_fails_without_suspect(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUTRADHARA_FAKE_TRANSCODE", "1")
    landing = tmp_path / "landing"
    source = _write_intake(landing, "card-102", {"clip.mov": b"valid video payload"})

    with session_scope(engine) as session:
        scan_landing_root(session, landing, enqueue_jobs=False, cache_root=tmp_path / "cache")
        item = session.scalars(select(IngestItem)).one()
        source.unlink()
        job = submit(
            session,
            "transcode",
            {
                "ingest_item_id": item.id,
                "cache_root": str(tmp_path / "cache"),
                "proxy_artifactclass": "proxy",
            },
        )
        result = run_one(session, job.id)
        assert not result.ok

    with session_scope(engine) as session:
        item = session.scalars(select(IngestItem)).one()
        asset = session.get(LogicalAsset, item.logical_asset_hash)
        assert asset is not None
        assert asset.validity == AssetValidity.UNVALIDATED
        job_row = session.get(Job, job.id)
        assert job_row is not None
        assert job_row.status == JobStatus.FAILED


def test_fact_recording_api_does_not_commit(engine: Engine, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    _write_intake(landing, "card-112", {"clip.mov": b"valid video payload"})
    derived_path = tmp_path / "derived.mp4"
    derived_path.write_bytes(b"derived payload")

    with session_scope(engine) as session:
        _add_cloud_backend(session)
        scan_landing_root(session, landing, enqueue_jobs=False, cache_root=tmp_path / "cache")
        item = session.scalars(select(IngestItem)).one()
        item_id = item.id
        asset_hash = item.logical_asset_hash
        backend_id = session.scalars(select(Backend.id).where(Backend.name == "cloud-temp")).one()

    factory = make_session_factory(engine)
    session = factory()
    try:
        tx_item = session.get(IngestItem, item_id)
        assert tx_item is not None
        asset = session.get(LogicalAsset, asset_hash)
        assert asset is not None
        derived = record_derivation(
            session,
            source_item=tx_item,
            output_path=derived_path,
            relpath="derived/rollback/test.mp4",
            kind="rollback-test",
            artifactclass="proxy",
            media_kind=MediaKind.VIDEO,
            generated_by="test",
        )
        record_index(
            session,
            item=tx_item,
            index_kind="pfr-index-v1",
            sidecar_path=tmp_path / "sidecar.pfr.json",
        )
        record_validity(
            session,
            asset=asset,
            validity=AssetValidity.SUSPECT,
            note="rollback check",
        )
        record_copy(
            session,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator={"bucket": "test-bucket", "key": "rollback"},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
        )
        assert derived.id is not None
        session.rollback()
    finally:
        session.close()

    with session_scope(engine) as session:
        persisted_item = session.get(IngestItem, item_id)
        assert persisted_item is not None
        assert "pfr_sidecar_path" not in persisted_item.item_metadata
        asset = session.get(LogicalAsset, asset_hash)
        assert asset is not None
        assert asset.validity == AssetValidity.UNVALIDATED
        assert session.scalar(select(func.count()).select_from(AssetDerivation)) == 0
        assert session.scalar(select(func.count()).select_from(Copy)) == 0
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 1
        assert session.scalar(select(func.count()).select_from(LogicalAsset)) == 1


def _run_transcode_job(engine: Engine, item_id: int, tmp_path: Path) -> dict[str, int]:
    with session_scope(engine) as session:
        job = submit(
            session,
            "transcode",
            {
                "ingest_item_id": item_id,
                "cache_root": str(tmp_path / "cache"),
                "proxy_artifactclass": "proxy",
            },
            required_resources=[{"pool": "cpu", "count": 8}],
        )
        result = run_one(session, job.id, granted_leases={"cpu": 8})
        assert result.ok
        state = result.step_state["transcode"]
        return {"mezz": int(state["mezz_item_id"]), "preview": int(state["preview_item_id"])}


def _run_pfr_index_job(engine: Engine, item_id: int, tmp_path: Path) -> str:
    with session_scope(engine) as session:
        job = submit(
            session,
            "pfr-index",
            {
                "ingest_item_id": item_id,
                "cache_root": str(tmp_path / "cache"),
            },
        )
        result = run_one(session, job.id)
        assert result.ok
        return str(result.step_state["pfr_index"]["sidecar_path"])


def _write_intake(landing: Path, intake_id: str, files: dict[str, bytes]) -> Path:
    root = landing / intake_id
    payload = root / "payload"
    payload.mkdir(parents=True)
    first_path: Path | None = None
    for relpath, content in files.items():
        path = payload / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        first_path = first_path or path
    (root / "intake.json").write_text(
        json.dumps(
            {
                "intake_id": intake_id,
                "operator": "tester",
                "source_kind": "card",
                "artifactclass": "video-master",
            }
        ),
        encoding="utf-8",
    )
    assert first_path is not None
    return first_path


def _add_cloud_backend(session: Any) -> None:
    backend = Backend(
        name="cloud-temp",
        kind=BackendKind.S3,
        tier=BackendTier.CATALOG_AUTHORITATIVE,
        config={"bucket": "test-bucket", "prefix": ""},
    )
    session.add(backend)
    session.flush()
    session.add(
        Pool(
            id="cloud-temp",
            backend_id=backend.id,
            representation="rao-aead-v1",
            location="s3://test-bucket",
            tier="cloud",
        )
    )


class _FakeObjectBackend:
    def __init__(self, name: str) -> None:
        self.name = name
        self.objects: dict[str, bytes] = {}

    def write_object(self, source: Path | str, *, key: str, pool: str | None = None) -> CopyRecord:
        data = Path(source).read_bytes()
        digest = content_hash(hashlib.sha256(data).digest())
        self.objects[key] = data
        return CopyRecord(
            logical_id=digest,
            native_locator={"bucket": "test-bucket", "key": key, "sha256": digest.hex()},
            integrity_hash=digest,
            size_bytes=len(data),
            metadata={"sha256": digest.hex(), "pool": pool or ""},
        )
