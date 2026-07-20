"""Unit coverage for PFR sidecar handling, RAO cuts, and failure projection."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from collections.abc import Iterator
from concurrent import futures
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import grpc
import pytest
from click.testing import CliRunner
from pfr_core import PFRSidecar
from pfr_core.cut import CutRefusal
from pfr_core.failure import ReasonId, ScrapeFailure
from pfr_core.schema import BlobRef, CapabilitySnapshot
from pfr_core.source import SourceChanged
from sqlalchemy import Engine, select

import sutradhara.jobs.handlers  # noqa: F401 -- register built-in handlers
from sutradhara._proto import layer5_pb2, layer5_pb2_grpc
from sutradhara.backend.port import (
    BackendSessionInvalidatedError,
    BackendTransientError,
    ByteRange,
)
from sutradhara.backend.remanence import RemanenceBackend
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    ArtifactClassPool,
    AssetLocator,
    Backend,
    Bundle,
    BundleMember,
    Copy,
    CopySource,
    IngestItem,
    Intake,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, locator_key, make_engine, session_scope
from sutradhara.catalog.types import (
    AssetValidity,
    BackendKind,
    BackendTier,
    CopyHealth,
    IntakeSourceKind,
    IntakeStatus,
    MediaKind,
)
from sutradhara.cli.main import cli
from sutradhara.jobs.engine import run_one, submit
from sutradhara.jobs.models import Job, ReconciliationCondition
from sutradhara.jobs.reconcilers import derivation as derivation_reconciler
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    CONDITION_BLOCKED,
    OBSERVED_MISSING,
    record_observation,
)
from sutradhara.jobs.registry import JobContext
from sutradhara.pfr import (
    RaoObject,
    atomic_write_sidecar,
    cut_pfr_asset,
    enforce_blob_lru,
    sidecar_blobs_complete,
)
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import RAO_CHUNK_SIZE


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


class _ReadSession(layer5_pb2_grpc.ReadSessionServiceServicer):
    def __init__(
        self,
        data: bytes,
        *,
        read_error: grpc.StatusCode | None = None,
    ) -> None:
        self.data = data
        self.read_error = read_error
        self.session_id = b"pfr-read-session"
        self.tape_uuid = bytes.fromhex("b8f6123456784e90aabbccddeeff0011")
        self.open_count = 0
        self.close_count = 0
        self.read_requests: list[layer5_pb2.ReadObjectRangeRequest] = []

    def OpenReadSession(
        self,
        request: layer5_pb2.OpenReadSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.ReadSession:
        self.open_count += 1
        return layer5_pb2.ReadSession(session_id=self.session_id, tape_uuid=self.tape_uuid)

    def ReadObjectRange(
        self,
        request: layer5_pb2.ReadObjectRangeRequest,
        context: grpc.ServicerContext,
    ) -> Iterator[layer5_pb2.BytesChunk]:
        self.read_requests.append(request)
        if self.read_error is not None:
            context.abort(self.read_error, "read failed")
            raise AssertionError("unreachable after context.abort")
        payload = (
            self.data
            if request.start_byte == 0 and request.end_byte == 0
            else self.data[request.start_byte : request.end_byte]
        )
        yield layer5_pb2.BytesChunk(data=payload, is_last=True)

    def CloseReadSession(
        self,
        request: layer5_pb2.CloseReadSessionRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.ReadSession:
        self.close_count += 1
        return layer5_pb2.ReadSession(session_id=self.session_id, tape_uuid=self.tape_uuid)


class _Catalog(layer5_pb2_grpc.CatalogServicer):
    def __init__(self, *, object_id: bytes, member_size: int) -> None:
        self.object_id = object_id
        self.member_size = member_size

    def GetFile(
        self,
        request: layer5_pb2.GetFileRequest,
        context: grpc.ServicerContext,
    ) -> layer5_pb2.FileRecord:
        if request.object_id != self.object_id:
            context.abort(grpc.StatusCode.NOT_FOUND, "object not found")
            raise AssertionError("unreachable after context.abort")
        return layer5_pb2.FileRecord(
            object_id=request.object_id,
            path=request.path,
            size_bytes=self.member_size,
        )


@contextmanager
def _serve_remanence(
    read: _ReadSession,
    catalog: _Catalog,
) -> Iterator[str]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    layer5_pb2_grpc.add_ReadSessionServiceServicer_to_server(read, server)  # type: ignore[no-untyped-call]
    layer5_pb2_grpc.add_CatalogServicer_to_server(catalog, server)  # type: ignore[no-untyped-call]
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        yield f"127.0.0.1:{port}"
    finally:
        server.stop(grace=None)


class _CutResult:
    def __init__(self, payload: dict[str, Any] | None = None) -> None:
        self._payload = payload or {"ok": True}

    def to_dict(self) -> dict[str, Any]:
        return dict(self._payload)


def test_cut_pfr_asset_reuses_one_read_session_and_member_relative_ranges(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    member = b"0123456789"
    first_chunk_lba = 2
    base = first_chunk_lba * RAO_CHUNK_SIZE
    object_bytes = b"x" * base + member + b"tail"
    read = _ReadSession(object_bytes)
    object_id = bytes.fromhex("1cd8ebd3d70a4998a02ab868b8aafbf3")
    catalog = _Catalog(object_id=object_id, member_size=len(member))

    with _serve_remanence(read, catalog) as endpoint:
        backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
        asset_hash = hashlib.sha256(member).digest()
        sidecar_path = _write_sidecar(
            tmp_path,
            _sidecar(
                measured_facts={"logical_size": len(member)},
                source_identity={"kind": "fixture", "size_bytes": len(member)},
            ),
        )
        with session_scope(engine) as session:
            _add_catalog_rows(
                session,
                asset_hash=asset_hash,
                member_size=len(member),
                sidecar_path=sidecar_path,
                representation=Representation.RAO_PLAIN_V1,
                object_id=object_id,
                first_chunk_lba=first_chunk_lba,
            )

        def fake_cut(
            sidecar: PFRSidecar,
            source: RaoObject,
            *,
            t_in: float,
            t_out: float,
            out_path: Path,
            blob_dir: Path,
        ) -> _CutResult:
            assert source.size() == len(member)
            assert source.read(0, 3) == member[0:3]
            assert source.read(3, 4) == member[3:7]
            assert source.read(-2, 2) == member[-2:]
            assert source.read(len(member), 1) == b""
            out_path.write_bytes(b"clip")
            return _CutResult({"source_size": source.size()})

        monkeypatch.setattr("sutradhara.pfr.cut_from_sidecar", fake_cut)
        output = tmp_path / "clip.mxf"
        with session_scope(engine) as session:
            result = cut_pfr_asset(
                session,
                asset_hash=asset_hash,
                artifactclass="video-master",
                destination=output,
                backends={1: backend},
                t_in=1.0,
                t_out=2.0,
            )

    assert result.rung == 1
    assert output.read_bytes() == b"clip"
    assert read.open_count == 1
    assert read.close_count == 1
    requested = [(req.start_byte, req.end_byte) for req in read.read_requests]
    assert requested == [
        (base, base + 3),
        (base + 3, base + 7),
        (base + len(member) - 2, base + len(member)),
    ]


def test_cut_refusal_records_reason_and_does_not_reopen_ranged_session(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    member = b"0123456789"
    object_id = bytes.fromhex("1cd8ebd3d70a4998a02ab868b8aafbf3")
    read = _ReadSession(member)
    catalog = _Catalog(object_id=object_id, member_size=len(member))

    with _serve_remanence(read, catalog) as endpoint:
        backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
        asset_hash = hashlib.sha256(member).digest()
        sidecar_path = _write_sidecar(
            tmp_path,
            _sidecar(
                measured_facts={"logical_size": len(member)},
                source_identity={"kind": "fixture", "size_bytes": len(member)},
            ),
        )
        with session_scope(engine) as session:
            _add_catalog_rows(
                session,
                asset_hash=asset_hash,
                member_size=len(member),
                sidecar_path=sidecar_path,
                representation=Representation.RAO_PLAIN_V1,
                object_id=object_id,
                first_chunk_lba=0,
            )

        def refuse_cut(*args: object, **kwargs: object) -> _CutResult:
            raise CutRefusal(
                _failure(
                    ReasonId.SIDECAR_SOURCE_MISMATCH,
                    plugin="mxf",
                    stage="sidecar_source_check",
                )
            )

        monkeypatch.setattr("sutradhara.pfr.cut_from_sidecar", refuse_cut)
        monkeypatch.setattr(
            "sutradhara.pfr._restore_member_whole",
            lambda **kwargs: Path(kwargs["output_path"]).write_bytes(b"whole"),
        )
        with session_scope(engine) as session:
            result = cut_pfr_asset(
                session,
                asset_hash=asset_hash,
                artifactclass="video-master",
                destination=tmp_path / "clip.mxf",
                backends={1: backend},
                t_in=1.0,
                t_out=2.0,
            )

    assert result.rung == 2
    assert result.attempts[0].rung == 1
    assert result.attempts[0].reason == ReasonId.SIDECAR_SOURCE_MISMATCH.value
    assert read.open_count == 1


def test_rao_object_uses_shared_member_base_and_translates_session_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_member_base(locator: dict[str, Any]) -> int:
        calls.append(dict(locator))
        return 1234

    class Reader:
        def __init__(self) -> None:
            self.ranges: list[ByteRange] = []

        def read_range(self, byte_range: ByteRange) -> bytes:
            self.ranges.append(byte_range)
            return b"abc"

    copy, locator = _copy_and_locator(member_size=10, first_chunk_lba=7)
    monkeypatch.setattr("sutradhara.pfr.member_byte_base", fake_member_base)
    reader = Reader()
    source = RaoObject(reader=reader, copy=copy, locator=locator)

    assert source.read(2, 3) == b"abc"
    assert calls == [dict(locator.native_locator)]
    assert (reader.ranges[0].start, reader.ranges[0].end) == (1236, 1239)

    class InvalidatingReader:
        def read_range(self, byte_range: ByteRange) -> bytes:
            raise BackendSessionInvalidatedError("lost session")

    with pytest.raises(SourceChanged):
        RaoObject(reader=InvalidatingReader(), copy=copy, locator=locator).read(0, 1)


def test_remanence_read_range_maps_grpc_codes_to_typed_errors() -> None:
    for code, expected in [
        (grpc.StatusCode.UNAVAILABLE, BackendTransientError),
        (grpc.StatusCode.DEADLINE_EXCEEDED, BackendTransientError),
        (grpc.StatusCode.DATA_LOSS, BackendTransientError),
        (grpc.StatusCode.FAILED_PRECONDITION, BackendSessionInvalidatedError),
        (grpc.StatusCode.ABORTED, BackendSessionInvalidatedError),
    ]:
        read = _ReadSession(b"x", read_error=code)
        object_id = bytes.fromhex("1cd8ebd3d70a4998a02ab868b8aafbf3")
        catalog = _Catalog(object_id=object_id, member_size=1)
        with _serve_remanence(read, catalog) as endpoint:
            backend = RemanenceBackend.from_grpc("primary-tape", endpoint)
            with (
                backend.open_read_session(_rem_locator(object_id)) as session,
                pytest.raises(expected),
            ):
                session.read_range(ByteRange(0, 1))


def test_pfr_retryables_never_auto_promote_but_parse_determination_blocks_after_restart(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "clip.mxf"
    source.write_bytes(b"not a real mxf")
    with session_scope(engine) as session:
        item = _add_ingest_item(session, tmp_path, source=source)
        retry_target = derivation_reconciler.make_target_key(item.id, "pfr-index")
        record_observation(
            session,
            domain=derivation_reconciler.DOMAIN,
            target_key=retry_target,
            desired=True,
            observed_state=OBSERVED_MISSING,
        )

    retry_failure = _failure(ReasonId.BUDGET_EXCEEDED, plugin="registry", stage="scrape")
    monkeypatch.setattr(
        "sutradhara.jobs.handlers.pfr_index.scrape_path_isolated_120",
        lambda *args, **kwargs: retry_failure,
    )
    for _ in range(3):
        with session_scope(engine) as session:
            item = session.scalars(select(IngestItem)).one()
            job = submit(
                session,
                "pfr-index",
                {"ingest_item_id": item.id, "cache_root": str(tmp_path / "cache")},
                recon_domain=derivation_reconciler.DOMAIN,
                recon_target_key=retry_target,
            )
            result = run_one(session, job.id, granted_leases={"io": 1, "cpu": 1})
            assert not result.ok

    with session_scope(engine) as session:
        row = _condition(session, retry_target)
        assert row.condition == CONDITION_BACKOFF
        assert row.attempt_count == 3

    parse_failure = _failure(ReasonId.INDEX_UNAVAILABLE, plugin="mxf", stage="scrape")
    monkeypatch.setattr(
        "sutradhara.jobs.handlers.pfr_index.scrape_path_isolated_120",
        lambda *args, **kwargs: parse_failure,
    )
    with session_scope(engine) as session:
        item = session.scalars(select(IngestItem)).one()
        parse_target = derivation_reconciler.make_target_key(item.id, "pfr-index-parse")
        record_observation(
            session,
            domain=derivation_reconciler.DOMAIN,
            target_key=parse_target,
            desired=True,
            observed_state=OBSERVED_MISSING,
        )
        job = submit(
            session,
            "pfr-index",
            {"ingest_item_id": item.id, "cache_root": str(tmp_path / "cache")},
            recon_domain=derivation_reconciler.DOMAIN,
            recon_target_key=parse_target,
        )
        first = run_one(session, job.id, granted_leases={"io": 1, "cpu": 1})
        assert not first.ok

    with session_scope(engine) as restarted_session:
        item = restarted_session.scalars(select(IngestItem)).one()
        job = submit(
            restarted_session,
            "pfr-index",
            {"ingest_item_id": item.id, "cache_root": str(tmp_path / "cache")},
            recon_domain=derivation_reconciler.DOMAIN,
            recon_target_key=parse_target,
        )
        second = run_one(restarted_session, job.id, granted_leases={"io": 1, "cpu": 1})
        assert second.ok

    with session_scope(engine) as session:
        row = _condition(session, parse_target)
        assert row.condition == CONDITION_BLOCKED
        assert row.reason == "unsupported-source"
        assert row.blocked_tool_name == "pfr_core"
        item = session.scalars(select(IngestItem)).one()
        asset = session.get(LogicalAsset, item.logical_asset_hash)
        assert asset is not None
        assert asset.validity == AssetValidity.SUSPECT


def test_pfr_handler_reason_matrix_covers_every_reason(
    engine: Engine,
    tmp_path: Path,
) -> None:
    source = tmp_path / "clip.mxf"
    source.write_bytes(b"bytes")
    with session_scope(engine) as session:
        item = _add_ingest_item(session, tmp_path, source=source)
        target_key = derivation_reconciler.make_target_key(item.id, "pfr-index")
        record_observation(
            session,
            domain=derivation_reconciler.DOMAIN,
            target_key=target_key,
            desired=True,
            observed_state=OBSERVED_MISSING,
        )
        job = submit(
            session,
            "pfr-index",
            {"ingest_item_id": item.id, "cache_root": str(tmp_path / "cache")},
            recon_domain=derivation_reconciler.DOMAIN,
            recon_target_key=target_key,
        )
        ctx = JobContext(session=session, job=job, granted_leases={"io": 1, "cpu": 1})
        import sutradhara.jobs.handlers.pfr_index as handler

        retryable = {
            ReasonId.SOURCE_CHANGED,
            ReasonId.BUDGET_EXCEEDED,
            ReasonId.BUDGET_EXHAUSTED,
            ReasonId.EXCEPTION,
            ReasonId.INDEX_UNAVAILABLE,
        }
        fallback = {
            ReasonId.CAP_EXCEEDED_FALLBACK,
            ReasonId.OP_ATOM_UNSUPPORTED,
            ReasonId.PLUGIN_MISSING,
            ReasonId.FALLBACK,
        }
        loud = set(ReasonId) - retryable - fallback

        for reason in sorted(retryable, key=lambda item: item.value):
            result = handler._failure_result(
                ctx,
                item,
                source=source,
                sidecar_path=tmp_path / "cache" / f"{reason.value}.json",
                blob_dir=tmp_path / "cache" / "blobs",
                failure=_failure(
                    reason,
                    plugin="mxf" if reason is ReasonId.INDEX_UNAVAILABLE else "registry",
                    stage="scrape",
                ),
            )
            assert not result.ok
            assert result.condition is not None
            assert result.condition.condition == CONDITION_BACKOFF
            assert result.condition.auto_block is False

        parse_exception = handler._failure_result(
            ctx,
            item,
            source=source,
            sidecar_path=tmp_path / "cache" / "mxf-parse-exception.json",
            blob_dir=tmp_path / "cache" / "blobs",
            failure=_failure(ReasonId.EXCEPTION, plugin="mxf", stage="scrape"),
        )
        assert not parse_exception.ok
        assert parse_exception.condition is not None
        assert parse_exception.condition.condition == CONDITION_BACKOFF
        assert parse_exception.condition.auto_block is False

        for reason in sorted(fallback, key=lambda item: item.value):
            result = handler._failure_result(
                ctx,
                item,
                source=source,
                sidecar_path=tmp_path / "cache" / f"{reason.value}.json",
                blob_dir=tmp_path / "cache" / "blobs",
                failure=_failure(reason),
            )
            assert result.ok
            assert result.step_state["pfr_index"]["kind"] == "fallback"

        for reason in sorted(loud, key=lambda item: item.value):
            with pytest.raises(RuntimeError, match="unmapped pfr_core ReasonId"):
                handler._failure_result(
                    ctx,
                    item,
                    source=source,
                    sidecar_path=tmp_path / "cache" / f"{reason.value}.json",
                    blob_dir=tmp_path / "cache" / "blobs",
                    failure=_failure(reason),
                )


def test_atomic_sidecar_write_keeps_old_payload_when_replace_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "sidecar.json"
    old = _sidecar(recipe_version="old")
    atomic_write_sidecar(old, path)
    original = path.read_text(encoding="utf-8")

    def fail_replace(src: str, dst: Path) -> None:
        raise OSError("crash before rename")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError, match="crash before rename"):
        atomic_write_sidecar(_sidecar(recipe_version="new"), path)

    assert path.read_text(encoding="utf-8") == original
    assert not list(tmp_path.glob("*.tmp"))


def test_blob_lru_tracks_access_and_protects_current_sidecar(
    tmp_path: Path,
) -> None:
    blob_dir = tmp_path / "blobs"
    protected_bytes = b"protected"
    evict_bytes = b"evict-me"
    protected = _write_blob(blob_dir, protected_bytes)
    evictable = _write_blob(blob_dir, evict_bytes)
    old = (dt.datetime.now(dt.UTC) - dt.timedelta(days=2)).timestamp()
    os.utime(protected, (old, old))
    os.utime(evictable, (old - 10, old - 10))
    sidecar = _sidecar(
        blobs=(
            BlobRef(
                content_address=f"sha256:{hashlib.sha256(protected_bytes).hexdigest()}",
                sha256=hashlib.sha256(protected_bytes).hexdigest(),
                size=len(protected_bytes),
            ),
        )
    )

    before = protected.stat().st_atime_ns
    assert sidecar_blobs_complete(sidecar, blob_dir=blob_dir)
    assert protected.stat().st_atime_ns >= before

    enforce_blob_lru(
        blob_dir,
        max_bytes=len(protected_bytes),
        protect_sidecar=sidecar,
    )
    assert protected.exists()
    assert not evictable.exists()
    assert sidecar_blobs_complete(sidecar, blob_dir=blob_dir)


def test_cut_regenerates_missing_blobs_and_protects_them_from_trim(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    member = b"0123456789"
    asset_hash = hashlib.sha256(member).digest()
    blob_payload = b"regenerated-blob"
    blob_sha = hashlib.sha256(blob_payload).hexdigest()
    blob = BlobRef(content_address=f"sha256:{blob_sha}", sha256=blob_sha, size=len(blob_payload))
    sidecar_path = _write_sidecar(
        tmp_path,
        _sidecar(
            measured_facts={"logical_size": len(member)},
            source_identity={"kind": "fixture", "size_bytes": len(member)},
            blobs=(blob,),
        ),
    )
    with session_scope(engine) as session:
        _add_catalog_rows(
            session,
            asset_hash=asset_hash,
            member_size=len(member),
            sidecar_path=sidecar_path,
            representation=Representation.RAO_PLAIN_V1,
            object_id=bytes.fromhex("1cd8ebd3d70a4998a02ab868b8aafbf3"),
            first_chunk_lba=0,
        )

    class Backend:
        has_live_catalog = False

        def open_read_session(self, locator: dict[str, Any]) -> _Session:
            return _Session(member)

    class _Session:
        def __init__(self, data: bytes) -> None:
            self.data = data

        def __enter__(self) -> _Session:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read_range(self, byte_range: ByteRange) -> bytes:
            return self.data[byte_range.start : byte_range.end]

    class Registry:
        def scrape_source(self, source: RaoObject, *, blob_dir: Path) -> PFRSidecar:
            _write_blob(blob_dir, blob_payload)
            return _sidecar(blobs=(blob,))

    monkeypatch.setattr("sutradhara.pfr.default_registry", lambda *, blob_dir=None: Registry())
    monkeypatch.setattr("sutradhara.pfr.pfr_blob_cache_bytes", lambda: len(blob_payload))

    def fake_cut(
        sidecar: PFRSidecar,
        source: RaoObject,
        *,
        t_in: float,
        t_out: float,
        out_path: Path,
        blob_dir: Path,
    ) -> _CutResult:
        assert sidecar_blobs_complete(sidecar, blob_dir=blob_dir)
        out_path.write_bytes(source.read(0, 4))
        return _CutResult()

    monkeypatch.setattr("sutradhara.pfr.cut_from_sidecar", fake_cut)
    output = tmp_path / "clip.mxf"
    with session_scope(engine) as session:
        result = cut_pfr_asset(
            session,
            asset_hash=asset_hash,
            artifactclass="video-master",
            destination=output,
            backends={1: Backend()},
            t_in=0,
            t_out=1,
            cache_root=tmp_path / "cache",
        )

    assert result.rung == 1
    assert output.read_bytes() == member[:4]
    assert (sidecar_path.parent / "blobs" / blob_sha[:2] / blob_sha).exists()


def test_reindex_force_submits_deduped_jobs_with_recon_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "pfr.db"
    monkeypatch.setenv("SUTRADHARA_DB_URL", f"sqlite:///{db_path}")
    engine = make_engine()
    create_all(engine)
    source = tmp_path / "clip.mxf"
    source.write_bytes(b"clip")
    sidecar_path = _write_sidecar(tmp_path, _sidecar(recipe_version="old"))
    with session_scope(engine) as session:
        item = _add_ingest_item(session, tmp_path, source=source, sidecar_path=sidecar_path)
        item_id = item.id

    monkeypatch.setattr("sutradhara.cli.pfr.current_pfr_recipe_version", lambda: "r2")
    runner = CliRunner()
    first = runner.invoke(cli, ["pfr", "reindex", "--all", "--json"])
    second = runner.invoke(cli, ["pfr", "reindex", "--all", "--json"])
    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    assert json.loads(first.output)["count"] == 1
    assert json.loads(second.output)["jobs"] == json.loads(first.output)["jobs"]

    with session_scope(engine) as session:
        jobs = list(session.scalars(select(Job).where(Job.kind == "pfr-index")))
        assert len(jobs) == 1
        job = jobs[0]
        target_key = derivation_reconciler.make_target_key(item_id, "pfr-index")
        assert job.dedupe_key == f"pfr-reindex:{item_id}:r2"
        assert job.recon_domain == derivation_reconciler.DOMAIN
        assert job.recon_target_key == target_key
        row = _condition(session, target_key)
        assert row.condition == "open"


def _condition(session: Any, target_key: str) -> ReconciliationCondition:
    return session.scalars(
        select(ReconciliationCondition).where(
            ReconciliationCondition.domain == derivation_reconciler.DOMAIN,
            ReconciliationCondition.target_key == target_key,
        )
    ).one()


def _failure(
    reason: ReasonId,
    *,
    plugin: str = "registry",
    stage: str = "scrape",
) -> ScrapeFailure:
    return ScrapeFailure(
        plugin=plugin,
        stage=stage,
        reason_id=reason,
        exception_class="MXFParseError"
        if reason is ReasonId.EXCEPTION and plugin == "mxf"
        else None,
        source_identity={"kind": "test"},
        message=f"{reason.value} message",
    )


def _sidecar(
    *,
    recipe_version: str = "r1",
    measured_facts: dict[str, Any] | None = None,
    source_identity: dict[str, Any] | None = None,
    blobs: tuple[BlobRef, ...] = (),
) -> PFRSidecar:
    return PFRSidecar(
        grammar_id="mxf",
        schema_version="1",
        plugin_version="test",
        recipe_version=recipe_version,
        measured_facts=measured_facts or {"logical_size": 1},
        source_identity=source_identity or {"kind": "fixture", "size_bytes": 1},
        capability_snapshot=CapabilitySnapshot(
            achieved_granularity="edit_unit",
            tc_source="file_relative",
            rewrap_tool="test",
        ),
        blobs=blobs,
    )


def _write_sidecar(tmp_path: Path, sidecar: PFRSidecar) -> Path:
    sidecar_dir = tmp_path / "cache" / "intakes" / "card-pfr" / "pfr"
    sidecar_dir.mkdir(parents=True, exist_ok=True)
    path = sidecar_dir / "1.pfr.json"
    path.write_text(json.dumps(sidecar.to_dict()), encoding="utf-8")
    return path


def _write_blob(blob_dir: Path, payload: bytes) -> Path:
    digest = hashlib.sha256(payload).hexdigest()
    path = blob_dir / digest[:2] / digest
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def _add_ingest_item(
    session: Any,
    tmp_path: Path,
    *,
    source: Path,
    sidecar_path: Path | None = None,
) -> IngestItem:
    digest = hashlib.sha256(source.read_bytes()).digest()
    session.add(
        ArtifactClassPolicyRecord(
            artifactclass="video-master",
            ruleset="test",
            expect="compliant",
            target_bytes=1,
            max_age_seconds=1,
            restore_preference=[],
            min_copies=1,
            min_impl_families=1,
            staging_config={},
            hdcache_config={},
        )
    )
    session.flush()
    session.add(
        LogicalAsset(
            content_sha256=digest,
            size_bytes=source.stat().st_size,
            media_kind=MediaKind.VIDEO,
            validity=AssetValidity.UNVALIDATED,
        )
    )
    session.add(
        Intake(
            intake_id="card-pfr",
            operator="op",
            source_kind=IntakeSourceKind.CARD,
            artifactclass="video-master",
            status=IntakeStatus.REGISTERED,
            registered_at=dt.datetime.now(dt.UTC),
        )
    )
    item = IngestItem(
        intake_id="card-pfr",
        logical_asset_hash=digest,
        as_received_path=source.name,
        virtual_path=source.name,
        size_bytes=source.stat().st_size,
        artifactclass="video-master",
        source_path=str(source),
        pfr_sidecar_path=str(sidecar_path) if sidecar_path is not None else None,
        item_metadata={},
    )
    session.add(item)
    session.flush()
    return item


def _add_catalog_rows(
    session: Any,
    *,
    asset_hash: bytes,
    member_size: int,
    sidecar_path: Path,
    representation: Representation,
    object_id: bytes,
    first_chunk_lba: int,
) -> None:
    session.add(
        ArtifactClassPolicyRecord(
            artifactclass="video-master",
            ruleset="test",
            expect="compliant",
            target_bytes=1,
            max_age_seconds=1,
            restore_preference=["primary-pool"],
            min_copies=1,
            min_impl_families=1,
            staging_config={},
            hdcache_config={},
        )
    )
    session.flush()
    session.add(
        LogicalAsset(
            content_sha256=asset_hash,
            size_bytes=member_size,
            media_kind=MediaKind.VIDEO,
            validity=AssetValidity.UNVALIDATED,
        )
    )
    session.add(
        Intake(
            intake_id="card-pfr",
            operator="op",
            source_kind=IntakeSourceKind.CARD,
            artifactclass="video-master",
            status=IntakeStatus.REGISTERED,
            registered_at=dt.datetime.now(dt.UTC),
        )
    )
    session.add(
        IngestItem(
            intake_id="card-pfr",
            logical_asset_hash=asset_hash,
            as_received_path="clip.mxf",
            virtual_path="clip.mxf",
            size_bytes=member_size,
            artifactclass="video-master",
            pfr_sidecar_path=str(sidecar_path),
            item_metadata={},
        )
    )
    backend = Backend(
        id=1,
        name="primary-tape",
        kind=BackendKind.REM_TAPE,
        tier=BackendTier.SELF_DESCRIBING,
        config={},
    )
    session.add(backend)
    session.add(
        Pool(
            id="primary-pool",
            backend_id=1,
            representation=representation.value,
            location="test",
        )
    )
    session.add(
        ArtifactClassPool(
            artifactclass="video-master",
            pool_id="primary-pool",
            active=True,
            sort_order=0,
        )
    )
    bundle = Bundle(
        id="pfr-bundle",
        artifactclass="video-master",
        status="sealed",
    )
    session.add(bundle)
    session.flush()
    session.add(
        BundleMember(
            bundle_id=bundle.id,
            logical_asset_hash=asset_hash,
            member_path="clip.mxf",
            size_bytes=member_size,
            file_sha256=asset_hash,
        )
    )
    native = _rem_locator(object_id)
    copy = Copy(
        bundle_id=bundle.id,
        backend_id=1,
        pool_id="primary-pool",
        native_locator=native,
        native_locator_key=locator_key(native),
        storage_metadata={"stored_size_bytes": member_size},
        integrity_hash=asset_hash,
        health=CopyHealth.OK,
        source=CopySource.INGEST,
    )
    session.add(copy)
    session.flush()
    session.add(
        AssetLocator(
            logical_asset_hash=asset_hash,
            pool_id="primary-pool",
            copy_id=copy.id,
            bundle_id=bundle.id,
            native_locator={
                "first_chunk_lba": first_chunk_lba,
                "size_bytes": member_size,
            },
            member_path="clip.mxf",
            representation=representation.value,
        )
    )
    session.flush()


def _copy_and_locator(
    *,
    member_size: int,
    first_chunk_lba: int,
) -> tuple[Copy, AssetLocator]:
    digest = hashlib.sha256(b"x").digest()
    copy = Copy(
        id=1,
        logical_asset_hash=digest,
        backend_id=1,
        pool_id="primary-pool",
        native_locator=_rem_locator(bytes.fromhex("1cd8ebd3d70a4998a02ab868b8aafbf3")),
        native_locator_key="{}",
        media_id="tape:b8f6123456784e90aabbccddeeff0011",
        media_family="tape",
        storage_metadata={},
        integrity_hash=digest,
        health=CopyHealth.OK,
        source=CopySource.INGEST,
    )
    locator = AssetLocator(
        id=1,
        logical_asset_hash=digest,
        pool_id="primary-pool",
        copy_id=1,
        native_locator={
            "first_chunk_lba": first_chunk_lba,
            "size_bytes": member_size,
        },
        member_path="clip.mxf",
        representation=Representation.RAO_PLAIN_V1.value,
    )
    return copy, locator


def _rem_locator(object_id: bytes) -> dict[str, Any]:
    return {
        "tape_uuid": "b8f6123456784e90aabbccddeeff0011",
        "tape_file_number": 1,
        "first_body_lba": 0,
        "object_id": object_id.hex(),
        "caller_object_id": "obj-1",
        "content_sha256": "0" * 64,
        "pool_id": "primary-pool",
        "body_format": "rem-tar-v1",
    }
