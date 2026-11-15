"""The sweeper, claim discipline, and the reaper. Every test names its defect.

``bundle_due`` had no production caller before this arc: the size arm fired
only incidentally during an enqueue and **the age arm ran nowhere at all**.
Several tests here are therefore the first execution of code that has been in
the tree, untested at a real call site, since the accumulator was written.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import socket
import sqlite3
import subprocess
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, event, select
from sqlalchemy.orm import Session

from sutradhara.archive_bundle import (
    BundleClaimLost,
    BundleStateError,
    add_bundle_member,
    bundle_due,
    claim_bundle_for_flush,
    close_bundle,
    enqueue_artifact,
    get_or_create_open_bundle,
)
from sutradhara.archive_fanout import (
    LocalArchiveBuilder,
    flush_bundle,
)
from sutradhara.archive_sweeper import (
    VOID_STATUS,
    claim_is_live,
    drain_candidates,
    due_bundles,
    live_group_fingerprints,
    reap_stuck_flushing,
    sweep_bundles,
    void_seal_orphans,
)
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    BundlingPolicy,
    DurabilityPolicy,
    PlacementPolicy,
    apply_artifactclass_policy,
    get_artifactclass_policy,
)
from sutradhara.backend.port import (
    BackendLocator,
    ByteRange,
    CopyRecord,
    VerifyObserver,
    VerifyResult,
)
from sutradhara.catalog.models import Backend, Bundle, Copy, LogicalAsset, Pool
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, content_hash
from sutradhara.jobs.attempts import default_worker_id
from sutradhara.jobs.models import ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import CONDITION_BLOCKED
from sutradhara.jobs.worker_lock import (
    exclusive_process_lock,
    held_process_lock_identity,
    process_lockfile_for,
)
from sutradhara.replication import PoolObject
from sutradhara.sealing.port import Representation


@pytest.mark.parametrize("route", ["worker", "cli-flush", "cli-sweep", "direct"])
def test_rba_window_releases_writer_and_runs_final_fast_path(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog, route: str
) -> None:
    """T1/T2: real funnels lend the writer and refresh only after the final commit."""
    from click.testing import CliRunner

    from sutradhara.cli.archive import bundle_flush, bundle_sweep
    from sutradhara.jobs.config import WorkerConfig
    from sutradhara.jobs.engine import submit
    from sutradhara.jobs.reconcilers import bundle_copy
    from sutradhara.jobs.worker import JobWorker

    with session_scope(engine) as session:
        _install_class(session, "windows", pools=("window-pool",), target_bytes=100)
        for name in ("first.bin", "second.bin"):
            bundle = _enqueue(
                session,
                artifactclass="windows",
                source=_source(tmp_path, name, size=60),
                member_path=name,
            )
        bundle_id = bundle.id
        backend_id = session.scalar(select(Backend.id))

    trace = []
    io_calls = []
    refreshes = []

    def trace_statement(connection, cursor, statement, parameters, context, executemany):
        trace.append((connection, statement))

    event.listen(engine, "before_cursor_execute", trace_statement)

    def external(operation):
        assert refreshes == []
        connection, statement = trace[-1]
        assert statement == "COMMIT", trace[-8:]
        assert not connection.connection.dbapi_connection.in_transaction
        before = len(trace)
        with sqlite3.connect(engine.url.database, timeout=0) as other:
            other.execute("BEGIN IMMEDIATE")
            other.execute(
                "INSERT INTO backend (name, kind, tier, config, implementation_family, added_at) "
                "SELECT ?, kind, tier, config, implementation_family, added_at "
                "FROM backend WHERE id = ?",
                (f"other-{operation}-{len(io_calls)}", backend_id),
            )
        assert len(trace) == before, "attempt connection executed SQL during media work"
        io_calls.append((operation, connection, before))

    class WindowBackend(_WriteBackend):
        def write_object_to_pool(self, *args, **kwargs):
            external("write")
            return super().write_object_to_pool(*args, **kwargs)

        def verify(self, *args, **kwargs):
            external("readback")
            return super().verify(*args, **kwargs)

    class WindowBuilder(LocalArchiveBuilder):
        def build(self, **kwargs):
            external("build")
            assert kwargs["bundle"].id == bundle_id
            return super().build(**kwargs)

    backend = WindowBackend()
    builder = WindowBuilder()
    real_refresh = bundle_copy.refresh_condition

    def refresh(session, target_key):
        assert target_key == bundle_id
        assert session.get(Bundle, bundle_id).status == "sealed"
        with sqlite3.connect(engine.url.database, timeout=0) as other:
            other.execute("BEGIN IMMEDIATE")
        row = real_refresh(session, target_key)
        refreshes.append((row.condition, row.reason, row.observed_state))
        return row

    monkeypatch.setattr(bundle_copy, "refresh_condition", refresh)
    monkeypatch.setattr("sutradhara.backend.factory.backend_from_row", lambda row: backend)
    monkeypatch.setattr("sutradhara.cli.archive.backend_from_row", lambda row: backend)
    monkeypatch.setattr(
        "sutradhara.jobs.handlers.bundle_sweep.make_archive_builder", lambda **kw: builder
    )
    monkeypatch.setattr("sutradhara.cli.archive.RemArchiveBuilder", lambda *args: builder)
    monkeypatch.setattr("sutradhara.cli.archive.make_engine", lambda: engine)
    if route == "worker":
        with session_scope(engine) as session:
            submit(session, "bundle-sweep")
        outcomes = JobWorker(engine, config=WorkerConfig(executor_workers=1)).drain()
        assert len(outcomes) == 1
        assert outcomes[0].result.ok, outcomes[0].result.detail
    elif route.startswith("cli-"):
        command = bundle_flush if route == "cli-flush" else bundle_sweep
        result = CliRunner().invoke(command, [bundle_id] if route == "cli-flush" else [])
        assert result.exit_code == 0, (result.output, result.exception)
    else:
        with session_scope(engine) as session:
            assert session.scalar(select(Bundle.id)) == bundle_id
            result = flush_bundle(
                session, bundle_id=bundle_id, backends={backend_id: backend}, builder=builder
            )
            assert not result.partial
            assert refreshes == []

    assert [operation for operation, _, _ in io_calls] == ["build", "write", "readback"]
    assert backend.writes == ["window-pool"]
    assert len(refreshes) == 1
    assert "bundle_copy_fast_path_failed" not in caplog.text
    connection = io_calls[0][1]
    statements = [sql for conn, sql in trace if conn is connection]
    assert statements[0] == ("BEGIN" if route == "worker" else "BEGIN IMMEDIATE") or (
        route == "direct" and statements[0].startswith("SELECT")
    )
    assert "BEGIN IMMEDIATE" in statements
    assert any(sql.startswith("SAVEPOINT") for sql in statements)
    for index, statement in enumerate(statements):
        if statement == "COMMIT":
            assert statements[index - 1].startswith("RELEASE SAVEPOINT")
            assert statements[index + 1] == "BEGIN IMMEDIATE"
            assert statements[index + 2].startswith("SAVEPOINT")
    for _, conn, boundary in io_calls:
        assert trace[boundary] == (conn, "BEGIN IMMEDIATE")
    with session_scope(engine) as session:
        row = session.scalar(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == "bundle_copy",
                ReconciliationCondition.target_key == bundle_id,
            )
        )
        assert (row.condition, row.reason, row.observed_state) == refreshes[0]
        assert len(list(session.scalars(select(Backend)))) == 4


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    # A file-backed database, not :memory:, because the reaper resolves the
    # worker lockfile from the engine URL and the two must agree.
    eng = make_engine(f"sqlite:///{tmp_path / 'sweeper.db'}")
    create_all(eng)
    yield eng
    eng.dispose()


def _digest(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


class _WriteBackend:
    """In-memory writable backend with a scriptable write failure.

    ``fail_pools`` fails the *write* (transient: nothing reaches media).
    ``bad_verify_pools`` lets the write succeed and fails the readback verify —
    the non-transient post-write failure, where bytes are on media and the
    check refuses them.
    """

    reissue_after_absent = True
    holds_names = False

    def lookup_pool_object(self, pool, object_name, *, candidate_digests):
        return None

    def __init__(
        self,
        name: str = "rem",
        *,
        fail_pools: tuple[str, ...] = (),
        bad_verify_pools: tuple[str, ...] = (),
    ) -> None:
        self._name = name
        self.fail_pools = set(fail_pools)
        self.bad_verify_pools = set(bad_verify_pools)
        self.objects: dict[str, bytes] = {}
        self.writes: list[str] = []
        self._counter = 0

    @property
    def name(self) -> str:
        return self._name

    def write_object_to_pool(
        self,
        source: Path | str,
        pool: str,
        *,
        caller_object_id: str | None = None,
        content_sha256: bytes | None = None,
    ) -> CopyRecord:
        if pool in self.fail_pools:
            raise OSError(f"configured write failure for {pool}")
        data = Path(source).read_bytes()
        digest = content_hash(hashlib.sha256(data).digest())
        self._counter += 1
        object_id = f"{self._name}-{self._counter}"
        self.objects[object_id] = data
        self.writes.append(pool)
        return CopyRecord(
            logical_id=digest,
            native_locator={
                "pool_id": pool,
                "object_id": object_id,
                "content_sha256": digest.hex(),
            },
            integrity_hash=digest,
            size_bytes=len(data),
        )

    def enumerate(self) -> Iterator[CopyRecord]:
        return iter(())

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        data = self.objects[str(locator["object_id"])]
        if byte_range.is_whole_object:
            return data
        return data[byte_range.start : byte_range.end]

    @contextmanager
    def open_range_chunks(
        self,
        locator: BackendLocator,
        byte_range: ByteRange,
        *,
        chunk_bytes: int,
    ) -> Iterator[Iterator[bytes]]:
        data = self.objects[str(locator["object_id"])]
        end = len(data) if byte_range.is_whole_object else byte_range.end

        def chunks() -> Iterator[bytes]:
            for cursor in range(byte_range.start, end, chunk_bytes):
                yield data[cursor : min(cursor + chunk_bytes, end)]

        yield chunks()

    def verify(
        self, locator: BackendLocator, *, observe: VerifyObserver | None = None
    ) -> VerifyResult:
        if str(locator["pool_id"]) in self.bad_verify_pools:
            return VerifyResult(ok=False, measured=False, detail="configured verify refusal")
        data = self.read_range(locator, ByteRange(0, 0))
        if observe is not None:
            observe(0, data)
        actual = content_hash(hashlib.sha256(data).digest())
        expected = content_hash(bytes.fromhex(str(locator["content_sha256"])))
        return VerifyResult(ok=actual == expected, measured=True, actual_hash=actual)


def _install_class(
    session: Session,
    artifactclass: str,
    *,
    pools: tuple[str, ...],
    target_bytes: int = 1_000_000,
    max_age_seconds: int = 3600,
) -> None:
    for pool_id in pools:
        if session.get(Pool, pool_id) is not None:
            continue
        backend = session.scalars(select(Backend).where(Backend.name == pool_id)).first()
        if backend is None:
            backend = Backend(
                name=pool_id,
                kind=BackendKind.MEMORY,
                tier=BackendTier.CATALOG_AUTHORITATIVE,
            )
            session.add(backend)
            session.flush()
        session.add(
            Pool(
                id=pool_id,
                backend_id=backend.id,
                representation=Representation.RAW_BYTES.value,
            )
        )
    session.flush()
    apply_artifactclass_policy(
        session,
        artifactclass,
        ArtifactClassPolicy(
            ruleset=f"{artifactclass}.rules",
            placements=tuple(PlacementPolicy(pool_id) for pool_id in pools),
            bundling=BundlingPolicy(
                target_gb=target_bytes / 1_000_000_000,
                max_age_seconds=max_age_seconds,
            ),
            restore_preference=pools,
            expect="messy",
            durability=DurabilityPolicy(min_copies=1, min_impl_families=1),
        ),
    )


def _backends(session: Session, backend: _WriteBackend) -> dict[int, Any]:
    return {row.id: backend for row in session.scalars(select(Backend))}


def _enqueue(
    session: Session,
    *,
    artifactclass: str,
    source: Path,
    member_path: str,
) -> Bundle:
    data = source.read_bytes()
    asset_hash = _digest(data)
    if session.get(LogicalAsset, asset_hash) is None:
        session.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(data)))
        session.flush()
    bundle, _, _ = enqueue_artifact(
        session,
        artifactclass=artifactclass,
        policy=get_artifactclass_policy(session, artifactclass),
        logical_asset_hash=asset_hash,
        source_path=source,
        member_path=member_path,
    )
    return bundle


def _dead_pid() -> int:
    """A pid that is certainly not running: a child we started and reaped.

    Not a made-up number: low pids are kernel threads (pid 2 is ``kthreadd``),
    so ``os.kill(2, 0)`` succeeds and the claim reads as live.
    """
    proc = subprocess.Popen(["true"])
    proc.wait()
    return proc.pid


def _source(tmp_path: Path, name: str, size: int = 64) -> Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(name.encode().ljust(size, b"."))
    return path


class _RbaCountingBuilder(LocalArchiveBuilder):
    """Count actual target builds and retain their complete directory paths."""

    def __init__(self):
        self.directories = []

    def build(self, **kwargs):
        self.directories.append(kwargs["work_dir"])
        return super().build(**kwargs)


@pytest.mark.parametrize("state", ["unresolved", "suspect", "missing-present", "conflict"])
def test_rba_repair_resolves_without_source_or_build(engine, tmp_path, monkeypatch, state):
    """T24/T25/T30/T30b/T30c: resolution/rereads finish before source extraction."""
    from sutradhara.backend.port import BackendUnavailableError
    from sutradhara.catalog.models import VerifyReceipt
    from sutradhara.jobs.handlers import bundle_repair

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend("conflict" if state == "conflict" else "ok")
    if state == "unresolved":
        backend.read_error = BackendUnavailableError("temporary read-back outage")
    with session_scope(engine) as session:
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        copy = session.scalar(select(Copy).where(Copy.pool_id == "p-b"))
        if state in {"suspect", "missing-present"}:
            copy.health = CopyHealth.SUSPECT if state == "suspect" else CopyHealth.MISSING
        receipts_before = len(list(session.scalars(select(VerifyReceipt))))
    backend.read_error = None
    builder = _RbaCountingBuilder()
    source_reads = []
    real_extract = bundle_repair._extract_member_sources

    def extract(**kwargs):
        source_reads.append(kwargs["source_copy_id"])
        return real_extract(**kwargs)

    monkeypatch.setattr(
        "sutradhara.jobs.handlers.bundle_repair._extract_member_sources",
        extract,
    )
    result = _rba_repair(engine, bundle_id, backend, monkeypatch, builder=builder)
    assert source_reads == []
    assert builder.directories == []
    assert backend.calls == {"p-a": 1, "p-b": 1}
    with session_scope(engine) as session:
        if state == "conflict":
            assert not result.ok
            assert result.condition.reason == "copy-identity-conflict"
            row = session.scalar(
                select(ReconciliationCondition).where(
                    ReconciliationCondition.target_key == bundle_id
                )
            )
            assert row.condition == "blocked"
            assert row.reason == "copy-identity-conflict"
        else:
            assert result.ok, result.detail
            copy = session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one()
            assert copy.health == CopyHealth.OK
            receipts = list(session.scalars(select(VerifyReceipt).order_by(VerifyReceipt.event_id)))
            assert len(receipts) == receipts_before + 1
            assert receipts[-1].source == "fanout"
            assert receipts[-1].execution_id.startswith(
                f"{bundle_id}:p-b:{copy.native_locator_key}:job"
            )


@pytest.mark.parametrize("lock_retry", [False, True])
def test_rba_repair_build_directories_and_lock_retry(engine, tmp_path, monkeypatch, lock_retry):
    """T26/T27/M8: same-representation targets build separately and write once per run."""
    from sqlalchemy.exc import OperationalError

    from sutradhara import archive_fanout
    from sutradhara.bundle_copy_ledger import identity_attempts

    bundle_id = _rba_seed(engine, tmp_path, pools=("p-a", "p-b", "p-c"))
    backend = _RbaBackend()
    with session_scope(engine) as session:
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        for copy in session.scalars(select(Copy).where(Copy.pool_id.in_(("p-b", "p-c")))):
            copy.health = CopyHealth.MISSING
    backend.names = {key: value for key, value in backend.names.items() if key[0] == "p-a"}
    delays = []
    monkeypatch.setattr("sutradhara.jobs.worker.time.sleep", delays.append)
    real_record = archive_fanout._record_copy_result
    injected = []

    def record(session, **kwargs):
        if lock_retry and kwargs["target"].pool_id == "p-b" and not injected:
            injected.append(True)
            raise OperationalError(
                "repair recording lock", {}, sqlite3.OperationalError("database is locked")
            )
        return real_record(session, **kwargs)

    monkeypatch.setattr(archive_fanout, "_record_copy_result", record)
    builder = _RbaCountingBuilder()
    result = _rba_repair(engine, bundle_id, backend, monkeypatch, builder=builder)
    assert result.ok, result.detail
    assert len(delays) == int(lock_retry)
    assert len(builder.directories) == 2
    assert len(set(builder.directories)) == 2
    assert backend.calls == {"p-a": 1, "p-b": 2, "p-c": 2}
    with session_scope(engine) as session:
        for pool in ("p-b", "p-c"):
            assert (
                len(identity_attempts(session, bundle_id=bundle_id, pool_id=pool, generation=0))
                == 2
            )
            assert (
                session.scalar(
                    select(Copy.id).where(Copy.pool_id == pool, Copy.health == CopyHealth.OK)
                )
                is not None
            )


def test_rba_repair_held_corrupt_name_requires_generation(engine, tmp_path, monkeypatch):
    """T28: a corrupt object whose name is held cannot be rewritten automatically."""
    from sutradhara.backend.port import BackendStoredCorruptError

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend("holds")
    with session_scope(engine) as session:
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one().health = CopyHealth.CORRUPT
    backend.read_error = BackendStoredCorruptError("name still holds corrupt data")
    builder = _RbaCountingBuilder()
    result = _rba_repair(engine, bundle_id, backend, monkeypatch, builder=builder)
    assert not result.ok
    assert result.condition.reason == "copy-rewrite-needs-generation"
    assert "p-b" in result.condition.message
    assert builder.directories == []
    assert backend.calls == {"p-a": 1, "p-b": 1}


def _rba_assert_repair_overwrite(engine, tmp_path, monkeypatch, *, pending_landed):
    """Exercise identical-key repair with or without an unresolved duplicate attempt."""
    from sutradhara.backend.port import BackendStoredCorruptError
    from sutradhara.bundle_copy_ledger import identity_attempts, record_attempt
    from sutradhara.catalog.models import AssetLocator, BlobRoot
    from tests.test_bundle_repair import _OutputsBuilder

    class OverwriteBackend(_RbaBackend):
        def write_object_to_pool(self, source, pool, **kwargs):
            previous = self.names.get((pool, Path(source).name))
            record = super().write_object_to_pool(source, pool, **kwargs)
            if previous is not None:
                assert record.integrity_hash == previous.copy_record.integrity_hash
                record = replace(record, native_locator=previous.copy_record.native_locator)
                self.objects[str(record.native_locator["object_id"])] = Path(source).read_bytes()
                self.names[(pool, Path(source).name)] = PoolObject(
                    record, bytes(record.integrity_hash)
                )
                self.read_error = None
            return record

    class Builder(_OutputsBuilder):
        def __init__(self):
            self.directories = []

        def build(self, **kwargs):
            self.directories.append(kwargs["work_dir"])
            return super().build(**kwargs)

    bundle_id = _rba_seed(engine, tmp_path)
    backend, builder = OverwriteBackend(), Builder()
    with session_scope(engine) as session:
        flush_bundle(
            session, bundle_id=bundle_id, backends=_backends(session, backend), builder=builder
        )
        copy = session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one()
        copy.health = CopyHealth.SUSPECT if pending_landed else CopyHealth.CORRUPT
        copy_id = copy.id
        if pending_landed:
            attempt, outcome = identity_attempts(
                session, bundle_id=bundle_id, pool_id="p-b", generation=0
            )[0]
            assert outcome == "landed"
            record_attempt(
                session,
                bundle_id=bundle_id,
                pool_id="p-b",
                backend_id=attempt.backend_id,
                generation=0,
                object_name=attempt.object_name,
                digest=attempt.digest,
                size_bytes=attempt.size_bytes,
                member_layout=attempt.member_layout,
                recipient_epochs=attempt.recipient_epochs,
            )
        locator_ids = list(
            session.scalars(select(AssetLocator.id).where(AssetLocator.copy_id == copy_id))
        )
        root_ids = list(session.scalars(select(BlobRoot.id).where(BlobRoot.copy_id == copy_id)))
    builder.directories.clear()
    backend.read_error = BackendStoredCorruptError("corrupt before overwrite")
    result = _rba_repair(engine, bundle_id, backend, monkeypatch, builder=builder)
    assert result.ok, result.detail
    assert len(builder.directories) == 1
    assert backend.calls == {"p-a": 1, "p-b": 2}
    with session_scope(engine) as session:
        copy = session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one()
        assert copy.id == copy_id
        assert copy.health == CopyHealth.OK
        assert copy.last_measured_digest == copy.integrity_hash
        assert (
            list(session.scalars(select(AssetLocator.id).where(AssetLocator.copy_id == copy_id)))
            == locator_ids
        )
        assert (
            list(session.scalars(select(BlobRoot.id).where(BlobRoot.copy_id == copy_id)))
            == root_ids
        )
        if pending_landed:
            assert [
                outcome
                for _, outcome in identity_attempts(
                    session, bundle_id=bundle_id, pool_id="p-b", generation=0
                )
            ] == ["landed", "not-landed", "landed"]


def test_rba_repair_overwrite_reuses_copy_and_locators(engine, tmp_path, monkeypatch):
    """T28b/M11/M16: an identical overwrite reuses its copy, locators, roots and evidence."""
    _rba_assert_repair_overwrite(engine, tmp_path, monkeypatch, pending_landed=False)


def test_rba_repair_landed_suspect_match_overwrites_once(engine, tmp_path, monkeypatch):
    """A step-1 reread records separately from the successful overwrite's receipt."""
    _rba_assert_repair_overwrite(engine, tmp_path, monkeypatch, pending_landed=True)


def test_rba_repair_legacy_copy_clears_not_landed_backoff(engine, tmp_path, monkeypatch):
    """A recovered pre-ledger copy heals placement despite an unrelated failed attempt."""
    from sutradhara import archive_fanout
    from sutradhara.bundle_copy_ledger import identity_attempts, record_attempt, record_resolution
    from sutradhara.jobs.commit_point import commit_point_for

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend()

    def legacy_writer(session, *, bundle, target, artifact, backend, **kwargs):
        # Seed the old catalogue shape: a copy and its evidence, without any
        # ledger attempts. The normal writer is restored before repair.
        identifier = bundle.id
        with commit_point_for(session).window():
            record = backend.write_object_to_pool(
                artifact.artifact_path, target.pool_id, content_sha256=artifact.stored_digest
            )
        return archive_fanout._record_copy_result(
            session,
            bundle_id=identifier,
            target=target,
            record=record,
            artifact=artifact,
            result=VerifyResult(
                ok=True, measured=True, actual_hash=content_hash(artifact.stored_digest)
            ),
            error=None,
            run="legacy",
        )

    with monkeypatch.context() as patch:
        patch.setattr(archive_fanout, "write_bundle_copy_for_pool", legacy_writer)
        with session_scope(engine) as session:
            flush_bundle(
                session,
                bundle_id=bundle_id,
                backends=_backends(session, backend),
                builder=LocalArchiveBuilder(),
            )
    with session_scope(engine) as session:
        assert identity_attempts(session, bundle_id=bundle_id, pool_id="p-b", generation=0) == []
        copy = session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one()
        copy.health = CopyHealth.MISSING
        copy_id = copy.id
        artifact = archive_fanout._artifact_from_copy(session, copy, tmp_path)
        name = next(name for pool, name in backend.names if pool == "p-b")
        failed = record_attempt(
            session,
            bundle_id=bundle_id,
            pool_id="p-b",
            backend_id=copy.backend_id,
            generation=0,
            object_name=name,
            digest=b"z" * 32,
            size_bytes=1,
            member_layout=archive_fanout._artifact_layout(artifact),
        )
        record_resolution(session, attempt_id=failed.id, outcome="not-landed", learned_by="lookup")
    builder = _RbaCountingBuilder()
    result = _rba_repair(engine, bundle_id, backend, monkeypatch, builder=builder)
    assert result.ok, result.detail
    assert result.condition is None
    assert builder.directories == []
    assert backend.calls == {"p-a": 1, "p-b": 1}
    with session_scope(engine) as session:
        assert session.get(Copy, copy_id).health == CopyHealth.OK
        assert [
            outcome
            for _, outcome in identity_attempts(
                session, bundle_id=bundle_id, pool_id="p-b", generation=0
            )
        ] == ["not-landed"]


def test_rba_repair_absent_copy_then_leaves_old_missing_row(engine, tmp_path, monkeypatch):
    """T29/T29b: a lost copy is rebuilt; the next repair ignores its old MISSING row."""
    from sutradhara.bundle_copy_ledger import identity_attempts

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend()
    with session_scope(engine) as session:
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        old = session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one()
        old.health = CopyHealth.MISSING
        old_id = old.id
    old_key = next(key for key in backend.names if key[0] == "p-b")
    del backend.names[old_key]
    builder = _RbaCountingBuilder()
    result = _rba_repair(engine, bundle_id, backend, monkeypatch, builder=builder)
    assert result.ok, result.detail
    assert len(builder.directories) == 1
    assert backend.calls == {"p-a": 1, "p-b": 2}
    assert old_key in backend.names
    with session_scope(engine) as session:
        assert session.get(Copy, old_id).health == CopyHealth.MISSING
        attempts = identity_attempts(session, bundle_id=bundle_id, pool_id="p-b", generation=0)
        assert [outcome for _, outcome in attempts] == ["landed", "landed"]
        assert attempts[0][0].object_name == attempts[1][0].object_name
    before = len(backend.lookup_calls)
    result = _rba_repair(engine, bundle_id, backend, monkeypatch, builder=builder)
    assert result.ok, result.detail
    assert len(backend.lookup_calls) == before
    assert len(builder.directories) == 1
    with session_scope(engine) as session:
        assert session.get(Copy, old_id).health == CopyHealth.MISSING


def test_rba_repair_fenced_absence_never_reads_source(engine, tmp_path, monkeypatch):
    """T30c: a mixed repair may look up a fenced pool but never write it."""
    bundle_id = _rba_seed(engine, tmp_path, pools=("p-a", "p-b", "p-c"))
    backend = _RbaBackend()
    with session_scope(engine) as session:
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one().health = CopyHealth.MISSING
        session.scalars(select(Copy).where(Copy.pool_id == "p-c")).one().health = CopyHealth.SUSPECT
        session.get(Pool, "p-b").accepts_writes = False
    backend.names = {key: value for key, value in backend.names.items() if key[0] != "p-b"}
    monkeypatch.setattr(
        "sutradhara.jobs.handlers.bundle_repair._extract_member_sources",
        lambda **kw: pytest.fail("no writable target needs a source"),
    )
    builder = _RbaCountingBuilder()
    result = _rba_repair(engine, bundle_id, backend, monkeypatch, builder=builder)
    assert result.ok, result.detail
    assert result.condition.reason == "fenced-missing"
    assert any(pool == "p-b" for pool, _, _ in backend.lookup_calls)
    assert builder.directories == []
    assert backend.calls == {"p-a": 1, "p-b": 1, "p-c": 1}


class _RbaBackend(_WriteBackend):
    """New recovery-test fake: committed names and write attempts are observable."""

    def __init__(self, mode="ok", *, failed_pool="p-b"):
        super().__init__()
        self.mode = mode
        self.failed_pool = failed_pool
        self.calls = Counter()
        self.lookup_calls = []
        self.names = {}
        self.reissue_after_absent = mode != "d2-absent"
        self.holds_names = mode == "holds"
        self.read_error = None
        self.lookup_override = None

    def write_object_to_pool(self, source, pool, **kwargs):
        from sutradhara.backend.port import BackendObjectExistsError, BackendWriteInDoubtError

        self.calls[pool] += 1
        if pool == self.failed_pool and self.mode in {"absent", "d2-absent"}:
            raise OSError("injected write did not land")
        if pool == self.failed_pool and self.mode == "stale" and self.calls[pool] == 2:
            raise BackendObjectExistsError("earlier write became visible")
        record = super().write_object_to_pool(source, pool, **kwargs)
        found = PoolObject(record, bytes(record.integrity_hash))
        self.names[(pool, Path(source).name)] = found
        if pool == self.failed_pool:
            if self.mode == "conflict":
                self.names[(pool, Path(source).name)] = replace(found, content_hash=b"x" * 32)
                raise OSError("name holds another object")
            if self.mode in {"raised-landed", "stale"}:
                raise OSError("reply lost after commit")
            if self.mode == "exists":
                raise BackendObjectExistsError("object exists")
            if self.mode == "in-doubt":
                raise BackendWriteInDoubtError("daemon still owns the write")
        return record

    def lookup_pool_object(self, pool, object_name, *, candidate_digests):
        self.lookup_calls.append((pool, object_name, tuple(candidate_digests)))
        if self.lookup_override is not None:
            return self.lookup_override(pool, object_name, candidate_digests)
        if pool == self.failed_pool and self.mode == "stale" and self.calls[pool] == 1:
            return None
        return self.names.get((pool, object_name))

    def verify(self, locator, **kwargs):
        if locator["pool_id"] == self.failed_pool and self.read_error is not None:
            raise self.read_error
        return super().verify(locator, **kwargs)


def _rba_seed(engine, tmp_path, *, pools=("p-a", "p-b")):
    """Create a due bundle with two members and independent pool destinations."""
    with session_scope(engine) as session:
        _install_class(session, "recovery", pools=pools, target_bytes=100)
        for name in ("rba-first.bin", "rba-second.bin"):
            bundle = _enqueue(
                session,
                artifactclass="recovery",
                source=_source(tmp_path, name, size=60),
                member_path=name,
            )
        return bundle.id


def _rba_repair(engine, bundle_id, backend, monkeypatch, *, builder=None):
    """Submit through the real reconciler and drain its repair through JobWorker."""
    from sutradhara.jobs.config import WorkerConfig
    from sutradhara.jobs.handlers import bundle_repair
    from sutradhara.jobs.reconcilers import bundle_copy
    from sutradhara.jobs.worker import JobWorker

    monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda row: backend)
    monkeypatch.setattr(
        bundle_repair, "make_archive_builder", lambda **kw: builder or LocalArchiveBuilder()
    )
    with session_scope(engine) as session:
        bundle_copy.reconcile_target(session, bundle_id)
    outcomes = JobWorker(engine, config=WorkerConfig(executor_workers=1)).drain()
    assert len(outcomes) == 1
    return outcomes[0].result


@pytest.mark.parametrize(
    ("mode", "reason", "outcome"),
    [
        ("raised-landed", None, "landed"),
        ("exists", None, "landed"),
        ("absent", "transient-backend-failure", "not-landed"),
        ("conflict", "copy-identity-conflict", "conflict"),
        ("in-doubt", "write-outcome-unknown", None),
        ("d2-absent", "write-outcome-unknown", None),
        ("unavailable", "write-unresolved", None),
        ("corrupt", "post-write-pool-failure", "landed"),
    ],
)
def test_rba_write_resolution_lanes(engine, tmp_path, monkeypatch, mode, reason, outcome):
    """T12-T19: write refusal, pool truth and read-back choose durable outcomes."""
    from sutradhara.backend.port import BackendStoredCorruptError, BackendUnavailableError
    from sutradhara.bundle_copy_ledger import identity_attempts
    from sutradhara.catalog.models import BundleCopyWriteResolution

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend(mode)
    if mode == "unavailable":
        backend.read_error = BackendUnavailableError("read-back is offline")
    elif mode == "corrupt":
        backend.read_error = BackendStoredCorruptError("stored object is corrupt")
    with session_scope(engine) as session:
        result = flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        assert result.condition_reason == reason
        assert result.partial is (reason is not None)
        attempts = identity_attempts(session, bundle_id=bundle_id, pool_id="p-b", generation=0)
        assert len(attempts) == 1
        attempt, current = attempts[0]
        assert current == outcome
        resolutions = list(
            session.scalars(
                select(BundleCopyWriteResolution).where(
                    BundleCopyWriteResolution.attempt_id == attempt.id
                )
            )
        )
        copies = list(
            session.scalars(select(Copy).where(Copy.bundle_id == bundle_id, Copy.pool_id == "p-b"))
        )
        if outcome == "landed":
            assert len(copies) == 1
            assert copies[0].health == (CopyHealth.CORRUPT if mode == "corrupt" else CopyHealth.OK)
            assert resolutions[0].learned_by == (
                "write-response" if mode == "corrupt" else "lookup"
            )
        else:
            assert copies == []
        if mode == "conflict":
            assert resolutions[0].observed_hash == b"x" * 32
        if reason is not None:
            condition = session.scalar(
                select(ReconciliationCondition).where(
                    ReconciliationCondition.target_key == bundle_id
                )
            )
            assert condition.condition == (
                "blocked" if mode in {"conflict", "d2-absent", "corrupt"} else "backoff"
            )
            assert "p-b" in condition.message
    assert backend.calls == {"p-a": 1, "p-b": 1}
    if mode in {"in-doubt", "unavailable", "corrupt"}:
        assert backend.lookup_calls == []
    if mode in {"absent", "unavailable"}:
        backend.mode = "ok"
        backend.read_error = None
        repaired = _rba_repair(engine, bundle_id, backend, monkeypatch)
        assert repaired.ok, repaired.detail
        assert backend.calls["p-b"] == (2 if mode == "absent" else 1)
        with session_scope(engine) as session:
            attempts = identity_attempts(session, bundle_id=bundle_id, pool_id="p-b", generation=0)
            assert [value for _, value in attempts] == (
                ["not-landed", "landed"] if mode == "absent" else ["landed"]
            )


def test_rba_stale_absence_adopts_earlier_attempt(engine, tmp_path, monkeypatch):
    """T13b/M3: later ALREADY_EXISTS can overturn an earlier NOT_FOUND."""
    from sutradhara.bundle_copy_ledger import identity_attempts
    from sutradhara.catalog.models import BundleCopyWriteResolution

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend("stale")
    with session_scope(engine) as session:
        result = flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        assert result.condition_reason == "transient-backend-failure"
    result = _rba_repair(engine, bundle_id, backend, monkeypatch)
    assert result.ok, result.detail
    assert backend.calls == {"p-a": 1, "p-b": 2}
    with session_scope(engine) as session:
        attempts = identity_attempts(session, bundle_id=bundle_id, pool_id="p-b", generation=0)
        assert [value for _, value in attempts] == ["landed", "not-landed"]
        facts = list(
            session.scalars(
                select(BundleCopyWriteResolution)
                .where(BundleCopyWriteResolution.attempt_id == attempts[0][0].id)
                .order_by(BundleCopyWriteResolution.id)
            )
        )
        assert [(fact.outcome, fact.learned_by) for fact in facts] == [
            ("not-landed", "lookup"),
            ("landed", "lookup"),
        ]
        assert len(list(session.scalars(select(Copy).where(Copy.pool_id == "p-b")))) == 1


def test_rba_killed_write_resumes_by_lookup(engine, tmp_path, monkeypatch):
    """T11: orphan recovery adopts a committed write before any copy was recorded."""
    from sutradhara import archive_fanout
    from sutradhara.bundle_copy_ledger import identity_attempts
    from sutradhara.catalog.models import BundleCopyWriteResolution
    from sutradhara.jobs.config import WorkerConfig
    from sutradhara.jobs.engine import submit
    from sutradhara.jobs.models import Job
    from sutradhara.jobs.worker import JobWorker

    class Killed(BaseException):
        pass

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend()
    monkeypatch.setattr("sutradhara.backend.factory.backend_from_row", lambda row: backend)
    monkeypatch.setattr(
        "sutradhara.jobs.handlers.bundle_sweep.make_archive_builder",
        lambda **kw: LocalArchiveBuilder(),
    )
    real_record = archive_fanout._record_copy_result

    def kill(*args, **kwargs):
        raise Killed()

    monkeypatch.setattr(archive_fanout, "_record_copy_result", kill)
    with session_scope(engine) as session:
        job_id = submit(session, "bundle-sweep").id
    config = WorkerConfig(executor_workers=1)
    with pytest.raises(Killed):
        JobWorker(engine, config=config).drain()
    with session_scope(engine) as session:
        assert session.get(Bundle, bundle_id).status == "flushing"
        assert [
            outcome
            for _, outcome in identity_attempts(
                session, bundle_id=bundle_id, pool_id="p-a", generation=0
            )
        ] == [None]
        assert list(session.scalars(select(Copy))) == []
        assert session.get(Job, job_id).attempts == 1
    monkeypatch.setattr(archive_fanout, "_record_copy_result", real_record)
    outcomes = JobWorker(engine, config=config).drain()
    assert outcomes[0].result.ok, outcomes[0].result.detail
    assert backend.calls == {"p-a": 1, "p-b": 1}
    with session_scope(engine) as session:
        assert session.get(Bundle, bundle_id).status == "sealed"
        assert session.get(Job, job_id).attempts == 2
        attempt, outcome = identity_attempts(
            session, bundle_id=bundle_id, pool_id="p-a", generation=0
        )[0]
        assert outcome == "landed"
        resolution = session.scalar(
            select(BundleCopyWriteResolution).where(
                BundleCopyWriteResolution.attempt_id == attempt.id
            )
        )
        assert resolution.learned_by == "lookup"


@pytest.mark.parametrize("bad_read", [False, True, None])
@pytest.mark.parametrize("holds_names", [False, True])
def test_rba_landed_match_rereads_its_own_copy(engine, tmp_path, bad_read, holds_names):
    """C5: a shared digest prefers landed history and reads that copy's locator."""
    from sutradhara.backend.port import BackendUnavailableError
    from sutradhara.bundle_copy_ledger import identity_attempts, record_attempt, resolve_identity
    from sutradhara.replication import bundle_group_targets

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend()
    with session_scope(engine) as session:
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        original, outcome = identity_attempts(
            session, bundle_id=bundle_id, pool_id="p-b", generation=0
        )[0]
        assert outcome == "landed"
        copy_id = session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one().id
        record_attempt(
            session,
            bundle_id=bundle_id,
            pool_id="p-b",
            backend_id=original.backend_id,
            generation=0,
            object_name=original.object_name,
            digest=original.digest,
            size_bytes=original.size_bytes,
            member_layout=original.member_layout,
            recipient_epochs=original.recipient_epochs,
        )
    backend.holds_names = holds_names
    if bad_read:
        backend.bad_verify_pools.add("p-b")
    elif bad_read is None:
        backend.read_error = BackendUnavailableError("landed copy temporarily offline")

    def lookup(pool, object_name, candidate_digests):
        found = backend.names[(pool, object_name)]
        # This locator is deliberately not the recorded copy's locator. The
        # digest match must select the landed copy, never create a second row.
        return PoolObject(
            replace(found.copy_record, native_locator={"object_id": "unreadable-lookup"}),
            found.content_hash,
        )

    backend.lookup_override = lookup
    with session_scope(engine) as session:
        bundle = session.get(Bundle, bundle_id)
        target = next(
            target
            for _, target in bundle_group_targets(session, bundle, _backends(session, backend))
            if target.pool_id == "p-b"
        )
        work_dir = tmp_path / "landed-reread"
        work_dir.mkdir()
        copy, problem, rebuild = resolve_identity(
            session,
            bundle_id=bundle_id,
            target=target,
            backend=backend,
            builder=LocalArchiveBuilder(),
            work_dir=work_dir,
        )
        assert copy.id == copy_id
        assert rebuild is (bad_read is True and not holds_names)
        assert (None if problem is None else problem.reason) == (
            "transient-backend-failure"
            if bad_read is None
            else "copy-rewrite-needs-generation"
            if bad_read and holds_names
            else None
        )
        assert [
            outcome
            for _, outcome in identity_attempts(
                session, bundle_id=bundle_id, pool_id="p-b", generation=0
            )
        ] == ["landed", "not-landed"]
        assert len(list(session.scalars(select(Copy).where(Copy.pool_id == "p-b")))) == 1
    assert backend.calls == {"p-a": 1, "p-b": 1}


def test_rba_two_resolvers_reread_current_outcomes(engine, tmp_path):
    """T17b/M15: a resolver finishing second respects the first resolver's facts."""
    from sutradhara.backend.port import BackendUnavailableError
    from sutradhara.bundle_copy_ledger import identity_attempts, resolve_identity
    from sutradhara.catalog.models import BundleCopyWriteResolution
    from sutradhara.replication import bundle_group_targets

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend()
    backend.read_error = BackendUnavailableError("initial read-back unavailable")
    with session_scope(engine) as session:
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
    backend.read_error = None
    first = []

    def lookup(pool, object_name, candidate_digests):
        backend.lookup_override = None
        with session_scope(engine) as session:
            bundle = session.get(Bundle, bundle_id)
            target = next(
                target
                for _, target in bundle_group_targets(session, bundle, _backends(session, backend))
                if target.pool_id == "p-b"
            )
            work_dir = tmp_path / "first-resolver"
            work_dir.mkdir()
            copy, problem, _ = resolve_identity(
                session,
                bundle_id=bundle_id,
                target=target,
                backend=backend,
                builder=LocalArchiveBuilder(),
                work_dir=work_dir,
            )
            assert problem is None
            first.append(copy.id)
        return backend.names[(pool, object_name)]

    backend.lookup_override = lookup
    with session_scope(engine) as session:
        bundle = session.get(Bundle, bundle_id)
        target = next(
            target
            for _, target in bundle_group_targets(session, bundle, _backends(session, backend))
            if target.pool_id == "p-b"
        )
        work_dir = tmp_path / "second-resolver"
        work_dir.mkdir()
        copy, problem, _ = resolve_identity(
            session,
            bundle_id=bundle_id,
            target=target,
            backend=backend,
            builder=LocalArchiveBuilder(),
            work_dir=work_dir,
        )
        assert problem is None
        assert copy.id == first[0]
        attempt, outcome = identity_attempts(
            session, bundle_id=bundle_id, pool_id="p-b", generation=0
        )[0]
        assert outcome == "landed"
        assert (
            len(
                list(
                    session.scalars(
                        select(BundleCopyWriteResolution).where(
                            BundleCopyWriteResolution.attempt_id == attempt.id
                        )
                    )
                )
            )
            == 1
        )
    assert backend.calls == {"p-a": 1, "p-b": 1}


def test_rba_resume_keeps_prior_identity_conflict(engine, tmp_path):
    """T19b/M13: historical conflict owns the final projection after other adoption."""
    from sqlalchemy import delete

    from sutradhara.backend.port import BackendWriteInDoubtError

    class ConflictAndPending(_RbaBackend):
        def write_object_to_pool(self, source, pool, **kwargs):
            record = super().write_object_to_pool(source, pool, **kwargs)
            if pool == "p-b":
                raise BackendWriteInDoubtError("write still pending")
            return record

    bundle_id = _rba_seed(engine, tmp_path)
    backend = ConflictAndPending("conflict", failed_pool="p-a")
    with session_scope(engine) as session:
        result = flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        assert result.condition_reason == "copy-identity-conflict"
        # Reproduce the durable pre-seal state without relying on a prior
        # condition row to carry the conflict into this resumed projection.
        bundle = session.get(Bundle, bundle_id)
        bundle.status = "flushing"
        bundle.claimed_by = None
        bundle.claim_job_id = None
        bundle.claim_job_attempt = None
    with session_scope(engine) as session:
        session.execute(
            delete(ReconciliationCondition).where(ReconciliationCondition.target_key == bundle_id)
        )
    with session_scope(engine) as session:
        result = flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        assert result.partial
        assert result.condition_reason == "copy-identity-conflict"
        assert "p-a" in result.condition_message
        condition = session.scalar(
            select(ReconciliationCondition).where(ReconciliationCondition.target_key == bundle_id)
        )
        assert condition.condition == "blocked"
        assert session.get(Bundle, bundle_id).status == "sealed"
        assert len(list(session.scalars(select(Copy).where(Copy.pool_id == "p-b")))) == 1
    assert backend.calls == {"p-a": 1, "p-b": 1}


def test_rba_sweep_lock_retry_resumes_own_claim(engine, tmp_path, monkeypatch):
    """T20/M4/M14: a lock after the write window resumes the same claim once."""
    from sqlalchemy.exc import OperationalError

    from sutradhara import archive_fanout
    from sutradhara.jobs.config import WorkerConfig
    from sutradhara.jobs.engine import submit
    from sutradhara.jobs.worker import JobWorker

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend()
    monkeypatch.setattr("sutradhara.backend.factory.backend_from_row", lambda row: backend)
    monkeypatch.setattr(
        "sutradhara.jobs.handlers.bundle_sweep.make_archive_builder",
        lambda **kw: LocalArchiveBuilder(),
    )
    real_record = archive_fanout._record_copy_result
    claims, alarms, delays = [], [], []
    monkeypatch.setattr("sutradhara.jobs.worker.time.sleep", delays.append)
    monkeypatch.setattr(
        "sutradhara.archive_sweeper.emit_structured_event", lambda name, **kw: alarms.append(name)
    )

    def record(session, **kwargs):
        bundle = session.get(Bundle, bundle_id)
        claims.append((bundle.claimed_by, bundle.claim_job_id, bundle.claim_job_attempt))
        if len(claims) == 1:
            assert claims[0][1:] == (job_id, 1)
            raise OperationalError(
                "injected recording lock", {}, sqlite3.OperationalError("database is locked")
            )
        return real_record(session, **kwargs)

    monkeypatch.setattr(archive_fanout, "_record_copy_result", record)
    with session_scope(engine) as session:
        job_id = submit(session, "bundle-sweep").id
    result = JobWorker(engine, config=WorkerConfig(executor_workers=1)).drain()[0].result
    assert result.ok, result.detail
    assert len(delays) == 1
    assert len(claims) == 3
    assert all(claim == claims[0] for claim in claims)
    assert "bundle_flush_claim_live" not in alarms
    assert backend.calls == {"p-a": 1, "p-b": 1}
    with session_scope(engine) as session:
        assert session.get(Bundle, bundle_id).status == "sealed"
        assert len(list(session.scalars(select(Copy)))) == 2


def test_rba_sweep_superseded_stops_before_later_bundle(engine, tmp_path, monkeypatch):
    """T20b: a superseded attempt never turns into a per-bundle failure."""
    from sutradhara.jobs.engine import AttemptSuperseded

    bundle_id = _rba_seed(engine, tmp_path)
    calls = []

    def superseded(session, **kwargs):
        calls.append(kwargs["bundle_id"])
        raise AttemptSuperseded("superseded proof")

    monkeypatch.setattr("sutradhara.archive_sweeper.flush_bundle", superseded)
    monkeypatch.setattr(
        "sutradhara.archive_sweeper.due_bundles",
        lambda session, **kw: [session.get(Bundle, bundle_id), Bundle(id="zz-later")],
    )
    with (
        pytest.raises(AttemptSuperseded, match="superseded proof"),
        session_scope(engine) as session,
    ):
        sweep_bundles(
            session, backends=_backends(session, _RbaBackend()), builder=LocalArchiveBuilder()
        )
    assert calls == [bundle_id]


@pytest.mark.parametrize("claim", ["dead-empty", "dead-written", "released", "live", "stale-job"])
def test_rba_reaper_and_flushing_only_backends(engine, tmp_path, monkeypatch, claim):
    """T21/T22/M5: the job reaps/resumes flushing-only pools and respects liveness."""
    from sutradhara.jobs.config import WorkerConfig
    from sutradhara.jobs.engine import submit
    from sutradhara.jobs.handlers.bundle_sweep import sweep_backends
    from sutradhara.jobs.models import JobStatus
    from sutradhara.jobs.worker import JobWorker

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend("in-doubt")
    if claim in {"dead-written", "released"}:
        with session_scope(engine) as session:
            flush_bundle(
                session,
                bundle_id=bundle_id,
                backends=_backends(session, backend),
                builder=LocalArchiveBuilder(),
            )
    with session_scope(engine) as session:
        bundle = session.get(Bundle, bundle_id)
        bundle.status = "flushing"
        bundle.claimed_by = (
            None
            if claim == "released"
            else default_worker_id()
            if claim in {"live", "stale-job"}
            else "dead-host:123"
        )
        if claim == "stale-job":
            old = submit(session, "bundle-sweep")
            old.status = JobStatus.FAILED
            bundle.claim_job_id, bundle.claim_job_attempt = old.id, 1
        assert not list(session.scalars(select(Bundle).where(Bundle.status == "open")))
    backend.mode = "ok"
    monkeypatch.setattr("sutradhara.backend.factory.backend_from_row", lambda row: backend)
    monkeypatch.setattr(
        "sutradhara.jobs.handlers.bundle_sweep.make_archive_builder",
        lambda **kw: LocalArchiveBuilder(),
    )
    with session_scope(engine) as session:
        assert len(sweep_backends(session)) == 2
        submit(session, "bundle-sweep")
    events = []
    monkeypatch.setattr(
        "sutradhara.archive_sweeper.emit_structured_event",
        lambda name, **kw: events.append((name, kw)),
    )
    result = JobWorker(engine, config=WorkerConfig(executor_workers=1)).drain()[0].result
    assert result.ok, result.detail
    with session_scope(engine) as session:
        bundle = session.get(Bundle, bundle_id)
        if claim == "live":
            assert bundle.status == "flushing"
            assert bundle.claimed_by == default_worker_id()
            assert any(name == "bundle_flush_claim_live" for name, _ in events)
            assert not backend.calls
        else:
            assert bundle.status == "sealed"
            assert backend.calls == {"p-a": 1, "p-b": 1}
            if claim != "released":
                assert any(name == "bundle_flush_claim_reaped" for name, _ in events)


@pytest.mark.parametrize("with_attempt", [False, True])
def test_rba_flush_owns_failure_cleanup(engine, tmp_path, monkeypatch, with_attempt):
    """T23/M6: the direct caller's rollback cannot strand a committed claim."""
    from sutradhara import archive_fanout

    class Killed(BaseException):
        pass

    class BrokenBuilder(LocalArchiveBuilder):
        def build(self, **kwargs):
            raise RuntimeError("builder infrastructure failed")

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend()
    if with_attempt:
        real_record = archive_fanout._record_copy_result

        def kill(*args, **kwargs):
            raise Killed()

        monkeypatch.setattr(archive_fanout, "_record_copy_result", kill)
        with pytest.raises(Killed), session_scope(engine) as session:
            flush_bundle(
                session,
                bundle_id=bundle_id,
                backends=_backends(session, backend),
                builder=LocalArchiveBuilder(),
            )
        monkeypatch.setattr(archive_fanout, "_record_copy_result", real_record)
        with session_scope(engine) as session:
            session.get(Bundle, bundle_id).claimed_by = "dead-host:123"
    with (
        pytest.raises(RuntimeError, match="builder infrastructure failed"),
        session_scope(engine) as session,
    ):
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=BrokenBuilder(),
        )
    with session_scope(engine) as session:
        bundle = session.get(Bundle, bundle_id)
        assert bundle.status == ("flushing" if with_attempt else "open")
        assert (bundle.claimed_by, bundle.claim_job_id, bundle.claim_job_attempt) == (
            None,
            None,
            None,
        )
        if not with_attempt:
            assert bundle.archive_id is None
            assert bundle.flushed_at is None
            assert bundle.scan_summary is None
        result = sweep_bundles(
            session, backends=_backends(session, backend), builder=LocalArchiveBuilder()
        )
        assert result.flushed == (bundle_id,)
    assert backend.calls == {"p-a": 1, "p-b": 1}


def test_rba_unclaim_keeps_new_accumulator_distinct(engine, tmp_path):
    """T23b/M7: a new accumulator can open during the failed build window."""
    bundle_id = _rba_seed(engine, tmp_path)
    opened = []

    class BrokenBuilder(LocalArchiveBuilder):
        def build(self, **kwargs):
            with session_scope(engine) as session:
                opened.append(
                    _enqueue(
                        session,
                        artifactclass="recovery",
                        source=_source(tmp_path, "next.bin"),
                        member_path="next.bin",
                    ).id
                )
            raise RuntimeError("failed after another accumulator opened")

    with pytest.raises(RuntimeError, match="another accumulator"), session_scope(engine) as session:
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, _RbaBackend()),
            builder=BrokenBuilder(),
        )
    with session_scope(engine) as session:
        bundle = session.get(Bundle, bundle_id)
        assert bundle.status == "open"
        assert bundle.claimed_by is None
        assert bundle.archive_id is not None
        assert bundle.flushed_at is not None
        assert bundle.scan_summary is None
        assert opened[0] != bundle_id
        assert session.get(Bundle, opened[0]).archive_id is None
        assert [candidate.id for candidate in due_bundles(session)] == [bundle_id]


def test_rba_sweep_entry_window_preserves_previous_bundle(engine, tmp_path, monkeypatch):
    """T23d/M17: pre-window failure of b cannot roll back a's final recording."""
    from sutradhara import archive_fanout
    from sutradhara.bundle_copy_ledger import identity_attempts
    from tests.bundle_group_helpers import bundle_kwargs_for_class

    with session_scope(engine) as session:
        for label in ("a", "b", "c"):
            _install_class(session, label, pools=(f"p-{label}",), target_bytes=100)
            session.add(
                Bundle(
                    id=f"bundle-{label}",
                    status="open",
                    target_bytes=100,
                    **bundle_kwargs_for_class(session, label, target_bytes=100),
                )
            )
            session.flush()
            for index in range(2):
                _enqueue(
                    session,
                    artifactclass=label,
                    source=_source(tmp_path, f"{label}-{index}.bin", size=60),
                    member_path=f"{index}.bin",
                )
        claim_bundle_for_flush(session, session.get(Bundle, "bundle-a"), worker_id="dead-host:123")
    validate = archive_fanout.validate_submission_member_identity

    def refuse_middle(session, members):
        if members[0].bundle_id == "bundle-b":
            raise archive_fanout.ArchiveFanoutError("injected member identity refusal")
        return validate(session, members)

    monkeypatch.setattr(archive_fanout, "validate_submission_member_identity", refuse_middle)
    backend = _RbaBackend()
    with session_scope(engine) as session:
        result = sweep_bundles(
            session, backends=_backends(session, backend), builder=LocalArchiveBuilder()
        )
        assert result.reaped == ("bundle-a",)
        assert result.flushed == ("bundle-a", "bundle-c")
        assert [identifier for identifier, _ in result.failed] == ["bundle-b"]
    with session_scope(engine) as session:
        for label in ("a", "c"):
            assert session.get(Bundle, f"bundle-{label}").status == "sealed"
            assert [
                outcome
                for _, outcome in identity_attempts(
                    session, bundle_id=f"bundle-{label}", pool_id=f"p-{label}", generation=0
                )
            ] == ["landed"]
        middle = session.get(Bundle, "bundle-b")
        assert middle.status == "open"
        assert middle.claimed_by is None
        assert middle.archive_id is None
        assert middle.flushed_at is None
        assert {copy.bundle_id for copy in session.scalars(select(Copy))} == {
            "bundle-a",
            "bundle-c",
        }


# --------------------------------------------------------------------------
# The due scan
# --------------------------------------------------------------------------


def test_sweeper_flushes_an_accumulator_that_is_due_by_size(engine: Engine, tmp_path: Path) -> None:
    """Guards: the size arm firing only as an incidental side effect of an
    enqueue. Nothing appends after the accumulator crosses its target, so
    without a sweeper the bundle sits open until the next member arrives —
    which for a finished shoot is never."""
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=100)
        _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif", size=80),
            member_path="a.tif",
        )
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "b.tif", size=80),
            member_path="b.tif",
        )
        bundle_id = bundle.id
        assert bundle.status == "open"
        assert bundle.total_bytes >= bundle.target_bytes

        result = sweep_bundles(
            session,
            backends=_backends(session, _WriteBackend()),
            builder=LocalArchiveBuilder(),
        )
        assert result.flushed == (bundle_id,)
        assert session.get(Bundle, bundle_id).status == "sealed"


def test_sweeper_flushes_an_accumulator_that_is_due_only_by_age(
    engine: Engine, tmp_path: Path
) -> None:
    """The age arm's first execution at a real call site.

    Guards: a trickle class holding one open accumulator forever. The bundle
    is nowhere near its byte target — only its ``max_age_seconds`` has passed —
    and before the sweeper existed nothing in the tree would ever have looked.

    The sweep runs against an EXPIRED identity map on purpose. SQLite does not
    store the offset behind ``DateTime(timezone=True)``, so ``opened_at`` comes
    back naive only on a genuine re-read; a bundle still cached from the append
    hands ``_as_utc`` the aware value it was constructed with, and the
    naive-minus-aware subtraction that ``_as_utc`` exists to prevent never
    happens. That is the shape the production sweeper always sees.
    """
    with session_scope(engine) as session:
        _install_class(session, "audio", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="audio",
            source=_source(tmp_path, "take.wav"),
            member_path="take.wav",
        )
        bundle_id = bundle.id
        assert bundle.total_bytes < bundle.target_bytes
        assert bundle_due(bundle) is False
        overdue = bundle.opened_at + dt.timedelta(seconds=bundle.max_age_seconds + 1)

        session.expire_all()
        reread = session.get(Bundle, bundle_id)
        assert reread.opened_at.tzinfo is None, "the read-back must be naive to test _as_utc"

        assert bundle_due(reread, now=overdue) is True
        result = sweep_bundles(
            session,
            backends=_backends(session, _WriteBackend()),
            builder=LocalArchiveBuilder(),
            now=overdue,
        )
        assert result.flushed == (bundle_id,)
        assert session.get(Bundle, bundle_id).status == "sealed"


def test_due_scan_covers_open_funnel_bundles_not_only_accumulators(
    engine: Engine, tmp_path: Path
) -> None:
    """P1 gate condition C3.

    Guards: a due-scan filtered on ``archive_id IS NULL``. An include-alone
    member is minted into an immediately-due funnel bundle that carries
    ``archive_id`` from creation and has no flusher of its own — a sweeper
    that only looked at accumulators would strand every oversized member
    forever, which is exactly the material that most needs to reach tape.
    """
    with session_scope(engine) as session:
        _install_class(session, "video", pools=("p-main",), target_bytes=100)
        funnel = _enqueue(
            session,
            artifactclass="video",
            source=_source(tmp_path, "big.mov", size=500),
            member_path="big.mov",
        )
        funnel_id = funnel.id
        # Include-alone routing: a funnel, not the group accumulator.
        assert funnel.archive_id is not None
        assert funnel.status == "open"

        assert [bundle.id for bundle in due_bundles(session)] == [funnel_id]
        result = sweep_bundles(
            session,
            backends=_backends(session, _WriteBackend()),
            builder=LocalArchiveBuilder(),
        )
        assert result.flushed == (funnel_id,)
        assert session.get(Bundle, funnel_id).status == "sealed"


# --------------------------------------------------------------------------
# The agreement check: drain, no-drain, re-adopt, void
# --------------------------------------------------------------------------


def test_full_group_move_drains_the_orphaned_accumulator(engine: Engine, tmp_path: Path) -> None:
    """Guards: an accumulator whose only class moved to a new pool set sitting
    open until its age arm happens to fire. Nothing will ever be appended to
    it again, so the drain rule seals it now."""
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id
        assert bundle_due(bundle) is False

        # The whole group moves: photos now places on a different pool.
        _install_class(session, "photos", pools=("p-other",), target_bytes=10_000_000)
        assert [candidate.id for candidate in drain_candidates(session)] == [bundle_id]

        result = sweep_bundles(
            session,
            backends=_backends(session, _WriteBackend()),
            builder=LocalArchiveBuilder(),
        )
        assert result.drained == (bundle_id,)
        assert result.flushed == (bundle_id,)
        assert session.get(Bundle, bundle_id).status == "sealed"


def test_partial_group_move_does_not_drain_the_accumulator(engine: Engine, tmp_path: Path) -> None:
    """Guards: reading "a class left the group" as "the group is dead".

    Two classes share a group. One leaves. The fingerprint is still the other
    class's live fingerprint, so the accumulator is not orphaned — draining it
    here would seal the staying class's material short for a policy edit that
    had nothing to do with it.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        _install_class(session, "audio", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id

        _install_class(session, "photos", pools=("p-other",), target_bytes=10_000_000)
        assert drain_candidates(session) == []

        result = sweep_bundles(
            session,
            backends=_backends(session, _WriteBackend()),
            builder=LocalArchiveBuilder(),
        )
        assert result.drained == ()
        assert result.flushed == ()
        assert session.get(Bundle, bundle_id).status == "open"

        # And the departed class's next member routes to its NEW group, while
        # the old accumulator carries on for the class that stayed.
        moved = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "b.tif"),
            member_path="b.tif",
        )
        assert moved.id != bundle_id
        stayed = _enqueue(
            session,
            artifactclass="audio",
            source=_source(tmp_path, "take.wav"),
            member_path="take.wav",
        )
        assert stayed.id == bundle_id


def test_a_reverted_policy_re_adopts_the_still_open_accumulator(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: sealing on the way out and opening a second accumulator on the
    way back. A revert restores the fingerprint, the still-open accumulator is
    adopted again, and no short object was ever minted by the round trip."""
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id
        original_group = bundle.bundle_group

        _install_class(session, "photos", pools=("p-other",), target_bytes=10_000_000)
        assert original_group not in live_group_fingerprints(session)

        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        assert original_group in live_group_fingerprints(session)
        assert drain_candidates(session) == []

        readopted = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "b.tif"),
            member_path="b.tif",
        )
        assert readopted.id == bundle_id
        assert readopted.member_count == 2


def test_empty_orphan_accumulator_is_void_sealed_not_left_forever(
    engine: Engine,
) -> None:
    """Guards: an empty orphan lingering.

    ``bundle_due`` refuses empty bundles by design, so an accumulator that was
    opened and never filled — and whose group no live policy derives any more —
    can never reach the flush path. It is sealed ``void`` directly instead.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle, _ = get_or_create_open_bundle(
            session,
            artifactclass="photos",
            policy=get_artifactclass_policy(session, "photos"),
        )
        bundle_id = bundle.id
        assert bundle.member_count == 0
        assert bundle_due(bundle, now=dt.datetime.now(dt.UTC) + dt.timedelta(days=365)) is False

        # Still live: not an orphan, so not voided.
        assert void_seal_orphans(session) == []

        _install_class(session, "photos", pools=("p-other",), target_bytes=10_000_000)
        assert void_seal_orphans(session) == [bundle_id]
        assert session.get(Bundle, bundle_id).status == VOID_STATUS


def test_empty_open_funnel_is_not_void_sealed(engine: Engine) -> None:
    """Guards: voiding a cloud-blob funnel that is waiting for its wrap.

    An empty *funnel* is a destination a job is about to fill; voiding it would
    delete that destination out from under the job. Only accumulators
    (``archive_id IS NULL``) are void candidates.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle, _ = get_or_create_open_bundle(
            session,
            artifactclass="photos",
            policy=get_artifactclass_policy(session, "photos"),
        )
        bundle.archive_id = f"archive-{bundle.id}"
        session.flush()
        _install_class(session, "photos", pools=("p-other",), target_bytes=10_000_000)
        assert void_seal_orphans(session) == []
        assert session.get(Bundle, bundle.id).status == "open"


# --------------------------------------------------------------------------
# Claim discipline
# --------------------------------------------------------------------------


def test_claim_is_taken_before_the_member_snapshot(engine: Engine, tmp_path: Path) -> None:
    """Guards: the round-3 defect — a claim taken *after* the member load.

    An appender that lands a member between the snapshot and the seal would
    have its member recorded in a sealed bundle that was built without it:
    catalog says archived, media does not have it. With the claim first, the
    bundle is ``flushing`` before any member is read and ``add_bundle_member``'s
    open-status check refuses the append outright.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        token = claim_bundle_for_flush(session, bundle)
        assert token == default_worker_id()
        assert bundle.status == "flushing"
        assert bundle.claimed_by == token

        # The appender cannot slip a member into the sealing bundle: a direct
        # add is refused outright...
        with pytest.raises(BundleStateError, match="is not open"):
            add_bundle_member(
                session,
                bundle=bundle,
                artifactclass="photos",
                logical_asset_hash=bundle.members[0].logical_asset_hash,
                member_path="late.tif",
                size_bytes=1,
                file_sha256=bundle.members[0].file_sha256,
            )
        # ...and the ordinary enqueue path routes to a fresh accumulator
        # instead, leaving the claimed member snapshot exactly as it was.
        late = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "late.tif"),
            member_path="late.tif",
        )
        assert late.id != bundle.id
        assert session.get(Bundle, bundle.id).member_count == 1


def test_second_claim_on_a_claimed_bundle_loses(engine: Engine, tmp_path: Path) -> None:
    """Guards: two flushers building the same member set into two objects.

    SQLite is the default dialect, where ``FOR UPDATE`` compiles to nothing, so
    the guard has to be rowcount on a conditional UPDATE, not a row lock.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        claim_bundle_for_flush(session, bundle)
        with pytest.raises(BundleClaimLost, match="not 'open'"):
            claim_bundle_for_flush(session, bundle, worker_id="otherhost:999")


def test_pre_write_failure_rolls_back_to_open_and_is_due_again(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: a pre-physical-write failure leaving the bundle stuck ``flushing``.

    Everything up to the first ``write_object_to_pool`` is one transaction, so
    the rollback IS the un-claim: the bundle comes back ``open``, un-claimed,
    and visible to ``bundle_due`` again. Nothing was written, so nothing is
    orphaned.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id
        source = Path(bundle.members[0].source_path)

    # A source that vanished between enqueue and flush fails the build. The
    # exception type is the builder's (OSError here); what this test pins is
    # that ANY pre-physical-write failure un-claims via the rollback.
    source.unlink()
    backend = _WriteBackend()
    with pytest.raises(OSError, match=r"a\.tif"), session_scope(engine) as session:
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )

    assert backend.writes == []
    with session_scope(engine) as session:
        bundle = session.get(Bundle, bundle_id)
        assert bundle.status == "open"
        assert bundle.claimed_by is None
        assert [candidate.id for candidate in due_bundles(session)] == []
        assert bundle_due(bundle, force=True) is True


def test_post_write_failure_seals_partial_with_the_earlier_copy_recorded(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: rolling back after a successful physical write.

    A tape append is unreclaimable and costs a bootstrap row, so once bytes
    are on media the flush seals partial rather than un-writing them. The
    earlier target's copy row must survive the later target's failure.
    """
    with session_scope(engine) as session:
        _install_class(
            session,
            "photos",
            pools=("p-aaa", "p-zzz"),
            target_bytes=10_000_000,
        )
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id

    backend = _WriteBackend(fail_pools=("p-zzz",))
    with session_scope(engine) as session:
        result = flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        assert result.partial is True
        assert result.failed_pools == ("p-zzz",)

    assert backend.writes == ["p-aaa"]
    with session_scope(engine) as session:
        bundle = session.get(Bundle, bundle_id)
        assert bundle.status == "sealed"
        copies = list(session.scalars(select(Copy).where(Copy.bundle_id == bundle_id)))
        assert [copy.pool_id for copy in copies] == ["p-aaa"]


def test_a_failing_bundle_in_a_sweep_is_un_claimed_and_the_pass_carries_on(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: the sweep committing a failed flush's partial state.

    ``sweep_bundles`` records a per-bundle failure and continues, and the
    caller (``session_scope``) then COMMITS. Without a per-bundle savepoint
    that commit keeps the failed flush's claim — ``status='flushing'`` plus
    ``claimed_by``, the ``archive_id`` mint and the ``scan_summary`` — and
    under the job worker the bundle is then unrecoverable: ``claimed_by`` names
    the worker's own live pid, so the reaper reads the claim as live forever
    and ``bundle_due`` never sees a ``flushing`` bundle. The material silently
    never reaches media.

    Two groups, one healthy and one whose source vanished after enqueue: the
    healthy one must seal, the broken one must come back ``open`` and
    un-claimed, and ``SweepResult.failed`` must name it.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=100)
        _install_class(session, "audio", pools=("p-other",), target_bytes=100)
        # Two members each, under the target individually: real accumulators,
        # so `archive_id` is minted by the flush and must not outlive it.
        for name in ("a.tif", "b.tif"):
            healthy = _enqueue(
                session,
                artifactclass="photos",
                source=_source(tmp_path, name, size=60),
                member_path=name,
            )
        for name in ("take.wav", "take2.wav"):
            broken = _enqueue(
                session,
                artifactclass="audio",
                source=_source(tmp_path, name, size=60),
                member_path=name,
            )
        healthy_id, broken_id = healthy.id, broken.id
        assert healthy_id != broken_id
        assert broken.archive_id is None
        vanishing = Path(broken.members[0].source_path)

    vanishing.unlink()
    backend = _WriteBackend()
    with session_scope(engine) as session:
        result = sweep_bundles(
            session,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        assert result.flushed == (healthy_id,)
        assert [bundle_id for bundle_id, _ in result.failed] == [broken_id]

    # After the commit: this is where a missing savepoint shows.
    assert backend.writes == ["p-main"]
    with session_scope(engine) as session:
        assert session.get(Bundle, healthy_id).status == "sealed"
        stranded = session.get(Bundle, broken_id)
        assert stranded.status == "open"
        assert stranded.claimed_by is None
        assert stranded.archive_id is None
        assert stranded.scan_summary is None
        # Visible to the next pass again, which is the whole point.
        assert [candidate.id for candidate in due_bundles(session)] == [broken_id]


def test_sweeper_records_missing_seal_key_and_flushes_other_bundle(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T2: one sealed bundle without a key cannot prevent an independent flush."""
    events: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        "sutradhara.archive_sweeper.emit_structured_event",
        lambda name, **fields: events.append((name, fields)),
    )
    with session_scope(engine) as session:
        _install_class(session, "sealed", pools=("sealed-pool",), target_bytes=100)
        pool = session.get(Pool, "sealed-pool")
        assert pool is not None
        pool.representation = Representation.REM_ENCRYPT_V1.value
        session.flush()
        # Enqueue after changing the representation so the frozen basis is sealed.
        _install_class(session, "plain", pools=("plain-pool",), target_bytes=100)
        for index in range(2):
            sealed = _enqueue(
                session,
                artifactclass="sealed",
                source=_source(tmp_path, f"sealed-{index}", size=60),
                member_path=f"sealed-{index}",
            )
            plain = _enqueue(
                session,
                artifactclass="plain",
                source=_source(tmp_path, f"plain-{index}", size=60),
                member_path=f"plain-{index}",
            )
        sealed_id, plain_id = sealed.id, plain.id
    backend = _WriteBackend()
    with session_scope(engine) as session:
        result = sweep_bundles(
            session, backends=_backends(session, backend), builder=LocalArchiveBuilder()
        )
        assert result.flushed == (plain_id,)
        assert [bundle_id for bundle_id, _ in result.failed] == [sealed_id]
        assert "key-epoch-missing" in result.failed[0][1]
        assert "sealed-pool" in result.failed[0][1]
        bundle = session.get(Bundle, sealed_id)
        assert bundle is not None
        assert bundle.status == "open"
        assert bundle.claimed_by is None
        assert bundle.archive_id is None
    assert backend.writes == ["plain-pool"]
    failures = [fields for name, fields in events if name == "bundle_sweep_flush_failed"]
    assert len(failures) == 1
    assert failures[0]["reason"] == "SealKeyUnavailable"


def test_post_write_verify_failure_seals_partial_through_the_sweep(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: a NON-transient post-write failure unwinding real copies.

    Only ``TransientPoolFanoutError`` used to be caught in the write loop, so a
    hard post-write refusal — bytes on media, readback verify says no —
    propagated out of ``flush_bundle``. Under the per-bundle savepoint above,
    that propagation would roll the sibling target's committed copy back too:
    the design's "post-write failures seal partial rather than roll back" would
    hold for retryable failures only, and a later re-flush would append a
    second unreclaimable object for the copy that was silently discarded.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-aaa", "p-zzz"), target_bytes=100)
        for name in ("a.tif", "b.tif"):
            bundle = _enqueue(
                session,
                artifactclass="photos",
                source=_source(tmp_path, name, size=60),
                member_path=name,
            )
        bundle_id = bundle.id

    backend = _WriteBackend(bad_verify_pools=("p-zzz",))
    with session_scope(engine) as session:
        result = sweep_bundles(
            session,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        # The pass reports it as flushed, not failed: the bundle sealed.
        assert result.flushed == (bundle_id,)
        assert result.failed == []

    assert backend.writes == ["p-aaa", "p-zzz"]
    with session_scope(engine) as session:
        assert session.get(Bundle, bundle_id).status == "sealed"
        copies = {
            copy.pool_id: copy.health
            for copy in session.scalars(select(Copy).where(Copy.bundle_id == bundle_id))
        }
        # The good placement survives...
        assert copies["p-aaa"] == CopyHealth.OK
        # ...and the refused one is recorded SUSPECT rather than erased: its
        # bytes are on media and a discarded row would let a repair append a
        # second object to the same pool.
        assert copies["p-zzz"] == CopyHealth.SUSPECT
        # And it alarms: blocked, not backed off — an automatic retry would
        # append rather than replace.
        condition = session.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == "bundle_copy",
                ReconciliationCondition.target_key == bundle_id,
            )
        ).one()
        assert condition.condition == CONDITION_BLOCKED


def test_hold_written_during_a_failing_enqueue_survives_the_rollback(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guards: the CLI defect round 4 found — a held bundle discarded by the
    surrounding rollback.

    ``stage_and_enqueue_artifact`` writes ``held`` on the bundle and then
    re-raises. A caller that raises out of its session scope rolls that write
    back with everything else, so the operator is told to review a bundle that
    is not held. The enqueue-intake CLI now carries the summary out of the
    scope and raises after the commit.
    """
    from click.testing import CliRunner

    from sutradhara.cli.main import cli
    from sutradhara.staging import StagingHeld

    db = tmp_path / "hold.db"
    engine = make_engine(f"sqlite:///{db}")
    create_all(engine)
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle, _ = get_or_create_open_bundle(
            session,
            artifactclass="photos",
            policy=get_artifactclass_policy(session, "photos"),
        )
        bundle_id = bundle.id

    def _held(*_args: Any, **_kwargs: Any) -> Any:
        from sutradhara.archive_bundle import hold_bundle

        with session_scope(engine) as session:
            hold_bundle(session, session.get(Bundle, bundle_id), summary={"why": "test"})
        raise StagingHeld({"why": "test"})

    monkeypatch.setattr("sutradhara.cli.archive.enqueue_intake_batch", _held)
    result = CliRunner().invoke(
        cli,
        ["archive", "bundle", "enqueue-intake", "intake-x"],
        env={"SUTRADHARA_DB_URL": f"sqlite:///{db}"},
    )
    assert result.exit_code == 1
    assert "why" in result.output
    with session_scope(engine) as session:
        assert session.get(Bundle, bundle_id).status == "held"
    engine.dispose()


# --------------------------------------------------------------------------
# The reaper
# --------------------------------------------------------------------------


def test_stuck_flushing_bundle_with_a_dead_claimer_is_reaped(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: a bundle stuck ``flushing`` forever after its flusher died.

    Nothing else can claim it, ``bundle_due`` ignores it (it is not ``open``),
    and the group's material never reaches media. The reaper returns it to
    ``open`` with an alarm — keeping ``archive_id``, so it is non-adoptable and
    cannot collide with the fresh accumulator that opened meanwhile.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id
        # A claimer on this host whose pid is long gone.
        claim_bundle_for_flush(session, bundle, worker_id=f"{socket.gethostname()}:{_dead_pid()}")
        bundle.archive_id = f"archive-{bundle_id}"
        session.flush()

        # A fresh accumulator opened while the first one was flushing.
        fresh = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "b.tif"),
            member_path="b.tif",
        )
        assert fresh.id != bundle_id

        assert reap_stuck_flushing(session) == [bundle_id]
        reaped = session.get(Bundle, bundle_id)
        assert reaped.status == "open"
        assert reaped.claimed_by is None
        # Non-adoptable: it keeps archive_id, so it does not collide with the
        # fresh accumulator on the one-open-accumulator partial unique index.
        assert reaped.archive_id is not None
        assert bundle_due(reaped, force=True) is True


def test_stuck_flushing_bundle_with_a_live_claimer_is_not_reaped(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: reaping a flush that is still running — the worst outcome of
    all, because the returning flusher and the new one both build, and the
    tape pays for both. Liveness is checked against the worker-lock holder."""
    lockfile = process_lockfile_for(engine, namespace="worker")
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id
        token = f"{socket.gethostname()}:{os.getpid()}"
        claim_bundle_for_flush(session, bundle, worker_id=token)

        with exclusive_process_lock(lockfile, purpose="worker"):
            assert claim_is_live(session, token) is True
            assert reap_stuck_flushing(session) == []
        assert session.get(Bundle, bundle_id).status == "flushing"


def test_the_reaper_emits_an_event_for_every_claim_it_leaves_alone(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guards: silence on the skip path.

    "Live claimer, left alone" and "claim this check can never retire" look
    identical from outside, and only the second is a defect — but a reaper that
    says nothing when it skips gives an operator no way to tell them apart, and
    no way to notice a bundle that has been skipped on every pass for a week.
    That silence is what made the committed-claim strand invisible.
    """
    events: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        "sutradhara.archive_sweeper.emit_structured_event",
        lambda name, **fields: events.append((name, fields)),
    )
    lockfile = process_lockfile_for(engine, namespace="worker")
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        token = f"{socket.gethostname()}:{os.getpid()}"
        claim_bundle_for_flush(session, bundle, worker_id=token)

        with exclusive_process_lock(lockfile, purpose="worker"):
            assert reap_stuck_flushing(session) == []
    skipped = [fields for name, fields in events if name == "bundle_flush_claim_live"]
    assert [fields["bundle_id"] for fields in skipped] == [bundle.id]
    assert skipped[0]["claimed_by"] == token


def test_claim_on_a_foreign_host_without_the_worker_lock_is_not_live(
    engine: Engine,
) -> None:
    """Guards: a same-pid coincidence on another host reading as live. The
    process probe only applies to claims from this host; a foreign-host claim
    is live only while it holds the worker lock."""
    with session_scope(engine) as session:
        assert claim_is_live(session, f"not-this-host:{os.getpid()}") is False
        assert claim_is_live(session, None) is False


def test_a_foreground_flush_on_this_host_is_live_without_the_worker_lock(
    engine: Engine,
) -> None:
    """Guards: reaping an operator's `sutra archive bundle flush` mid-write.

    A foreground CLI flush claims under its own ``hostname:pid`` and never
    holds the worker lock, so the lock check alone would call it dead. The pid
    probe is the second conjunct that saves it — and it was unreached by the
    existing tests, which held the worker lock and short-circuited before it.
    """
    lockfile = process_lockfile_for(engine, namespace="worker")
    with session_scope(engine) as session:
        assert held_process_lock_identity(lockfile) is None
        assert claim_is_live(session, f"{socket.gethostname()}:{os.getpid()}") is True
        # Same host, dead pid: not live, which is what makes the probe useful.
        assert claim_is_live(session, f"{socket.gethostname()}:{_dead_pid()}") is False
        # Malformed tokens are not live either.
        assert claim_is_live(session, f"{socket.gethostname()}:not-a-pid") is False
        assert claim_is_live(session, f"{socket.gethostname()}:0") is False


def test_a_reaped_drain_flush_is_still_reachable_without_an_age_arm(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: a reaped bundle that no rule can reach any more.

    The reaper keeps ``archive_id`` on purpose — the bundle must stay
    non-adoptable so it cannot collide with the accumulator that opened while
    it sat ``flushing``. But that same ``archive_id`` disqualifies it from the
    drain rule, and a bundle the DRAIN rule flushed is short by construction:
    under its byte target, and with ``max_age_seconds`` at its default 0 there
    is no age arm either. ``bundle_due`` therefore says False forever, nothing
    else looks, and nothing alarms. The material silently never reaches media.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id

        # The whole group moves away: the drain rule is what flushes this one.
        _install_class(session, "photos", pools=("p-other",), target_bytes=10_000_000)
        assert [candidate.id for candidate in drain_candidates(session)] == [bundle_id]

        # A drain flush that died after claiming: `flush_bundle` mints
        # `archive_id` right after the claim.
        claim_bundle_for_flush(session, bundle, worker_id=f"{socket.gethostname()}:{_dead_pid()}")
        bundle.archive_id = f"archive-{bundle_id}"
        session.flush()
        assert reap_stuck_flushing(session) == [bundle_id]

        reaped = session.get(Bundle, bundle_id)
        assert reaped.status == "open"
        assert reaped.archive_id is not None
        # An accumulator always carries a positive age arm (zero thresholds are
        # refused at open), so this one would eventually flush late — the drain
        # rule exists precisely to say that waiting buys nothing. A funnel does
        # not even get that: `jobs/handlers/cloud_blob.py` mints its funnels
        # with `max_age_seconds=0`, and one member quarantined out of a failing
        # flush leaves it under its own target. Set both here, which is the
        # state a reaped cloud-blob funnel is genuinely in.
        reaped.max_age_seconds = 0
        session.flush()
        assert reaped.total_bytes < reaped.target_bytes
        # Neither arm of `bundle_due` can ever fire for it...
        assert bundle_due(reaped) is False
        assert bundle_due(reaped, now=reaped.opened_at + dt.timedelta(days=3650)) is False
        # ...and the drain rule refuses it, because it is no longer adoptable.
        assert drain_candidates(session) == []

        # The sweep reaches it anyway: it was already judged flush-worthy when
        # it was claimed, and it can never grow.
        assert [candidate.id for candidate in due_bundles(session)] == [bundle_id]
        result = sweep_bundles(
            session,
            backends=_backends(session, _WriteBackend()),
            builder=LocalArchiveBuilder(),
            reap=False,
        )
        assert result.flushed == (bundle_id,)
        assert session.get(Bundle, bundle_id).status == "sealed"


def test_reaped_then_returning_flusher_fails_the_close_cas_loudly(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: the returning flusher sealing a member set that is not on media.

    While it was presumed dead the bundle went back to ``open`` and may have
    been re-claimed and rebuilt with a different member set. Sealing on the
    stale claim would record *this* flusher's member list against whatever is
    actually on media. The CAS on ``status='flushing' AND claimed_by=:token``
    refuses instead.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        token = claim_bundle_for_flush(
            session, bundle, worker_id=f"{socket.gethostname()}:{_dead_pid()}"
        )
        assert reap_stuck_flushing(session) == [bundle.id]

        with pytest.raises(BundleClaimLost, match="lost its flush claim"):
            close_bundle(session, bundle, claim_token=token)
        assert session.get(Bundle, bundle.id).status == "open"


class _Fr1OverwriteBackend(_RbaBackend):
    """Model S3 replacing a named object while preserving its catalogue locator."""

    def write_object_to_pool(self, source, pool, **kwargs):
        previous = self.names.get((pool, Path(source).name))
        record = super().write_object_to_pool(source, pool, **kwargs)
        if previous is not None:
            assert record.integrity_hash == previous.copy_record.integrity_hash
            record = replace(record, native_locator=previous.copy_record.native_locator)
            self.objects[str(record.native_locator["object_id"])] = Path(source).read_bytes()
            self.names[(pool, Path(source).name)] = PoolObject(record, bytes(record.integrity_hash))
            self.read_error = None
        return record


def _fr1_pending_duplicate(session, bundle_id, pool_id="p-b"):
    """Leave an unresolved duplicate beside the identity's original landed attempt."""
    from sutradhara.bundle_copy_ledger import identity_attempts, record_attempt

    original, outcome = identity_attempts(
        session, bundle_id=bundle_id, pool_id=pool_id, generation=0
    )[0]
    assert outcome == "landed"
    return record_attempt(
        session,
        bundle_id=bundle_id,
        pool_id=pool_id,
        backend_id=original.backend_id,
        generation=0,
        object_name=original.object_name,
        digest=original.digest,
        size_bytes=original.size_bytes,
        member_layout=original.member_layout,
        recipient_epochs=original.recipient_epochs,
    ).id


def _fr1_cacheable_bundle(engine, tmp_path):
    """Seed one cacheable member and its observation without scheduling a fill."""
    from sutradhara.artifactclass_policy import HdcachePolicy
    from sutradhara.hdcache.fill import JOB_KIND
    from sutradhara.jobs.models import Job
    from sutradhara.jobs.reconcilers.conditions import record_observation

    with session_scope(engine) as session:
        _install_class(session, "cached", pools=("p-a", "p-b"), target_bytes=100)
        apply_artifactclass_policy(
            session,
            "cached",
            ArtifactClassPolicy(
                ruleset="cached.rules",
                placements=(PlacementPolicy("p-a"), PlacementPolicy("p-b")),
                bundling=BundlingPolicy(target_gb=100 / 1_000_000_000, max_age_seconds=3600),
                restore_preference=("p-a", "p-b"),
                expect="messy",
                hdcache=HdcachePolicy(enabled=True, privacy_level="none"),
                durability=DurabilityPolicy(min_copies=1, min_impl_families=1),
            ),
        )
        source = _source(tmp_path, "cached.bin", size=120)
        bundle_id = _enqueue(
            session, artifactclass="cached", source=source, member_path="cached.bin"
        ).id
        digest = _digest(source.read_bytes()).hex()
        record_observation(
            session,
            domain="hdcache",
            target_key=digest,
            desired=True,
            observed_state="missing",
        )
        assert not list(session.scalars(select(Job).where(Job.kind == JOB_KIND)))
    return bundle_id, digest


def test_fr1_flush_direct_enqueues_hdcache_fill(engine, tmp_path, monkeypatch):
    """F1/M18: fresh-session callers enqueue fills after explicitly reloading members."""
    from sutradhara.hdcache.fill import JOB_KIND
    from sutradhara.jobs.models import Job

    monkeypatch.setenv("SUTRADHARA_HDCACHE_SCRATCH_ROOT", str(tmp_path))
    bundle_id, digest = _fr1_cacheable_bundle(engine, tmp_path)
    backend = _RbaBackend()
    with session_scope(engine) as session:
        # The harness calls flush directly without loading the Bundle first.
        result = flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        assert not result.partial
        fills = list(session.scalars(select(Job).where(Job.kind == JOB_KIND)))
        assert any(
            job.kind == JOB_KIND
            and job.params["content_sha256"] == digest
            and job.params["bundle_key"] == bundle_id
            for job in fills
        ), "fresh-session flush did not enqueue the member's hdcache_fill job"
    assert backend.calls == {"p-a": 1, "p-b": 1}


def test_fr1_flush_worker_enqueues_hdcache_fill(engine, tmp_path, monkeypatch):
    """F1 production-route coverage; exempt from red proof because it passes at base."""
    from sutradhara.hdcache.fill import JOB_KIND
    from sutradhara.jobs.config import WorkerConfig
    from sutradhara.jobs.engine import submit
    from sutradhara.jobs.models import Job
    from sutradhara.jobs.worker import JobWorker

    monkeypatch.setenv("SUTRADHARA_HDCACHE_SCRATCH_ROOT", str(tmp_path))
    bundle_id, digest = _fr1_cacheable_bundle(engine, tmp_path)
    backend = _RbaBackend()
    with session_scope(engine) as session:
        submit(session, "bundle-sweep")
    monkeypatch.setattr("sutradhara.backend.factory.backend_from_row", lambda row: backend)
    monkeypatch.setattr(
        "sutradhara.jobs.handlers.bundle_sweep.make_archive_builder",
        lambda **kw: LocalArchiveBuilder(),
    )
    JobWorker(engine, config=WorkerConfig(capacities={"io": 1}, executor_workers=1)).drain()
    assert backend.calls == {"p-a": 1, "p-b": 1}
    with session_scope(engine) as session:
        fills = list(session.scalars(select(Job).where(Job.kind == JOB_KIND)))
        assert any(
            job.kind == JOB_KIND
            and job.params["content_sha256"] == digest
            and job.params["bundle_key"] == bundle_id
            for job in fills
        ), ("flush did not enqueue the member's hdcache-fill job", [job.params for job in fills])


@pytest.mark.parametrize("fenced", [False, True], ids=["writable", "fenced"])
def test_fr1_repair_landed_ok_reread_records_corruption(engine, tmp_path, monkeypatch, fenced):
    """F2(a,b)/M20: an OK landed copy's failed reread records before any rewrite."""
    from sutradhara.backend.port import BackendStoredCorruptError
    from sutradhara.catalog.models import VerifyReceipt
    from tests.test_bundle_repair import _OutputsBuilder

    bundle_id = _rba_seed(engine, tmp_path, pools=("p-a", "p-b", "p-c"))
    backend, builder = _Fr1OverwriteBackend(), _OutputsBuilder()
    with session_scope(engine) as session:
        flush_bundle(
            session, bundle_id=bundle_id, backends=_backends(session, backend), builder=builder
        )
        copy = session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one()
        assert copy.health == CopyHealth.OK
        assert copy.last_measured_digest == copy.integrity_hash
        copy_id = copy.id
        newest_id = _fr1_pending_duplicate(session, bundle_id)
        session.scalars(select(Copy).where(Copy.pool_id == "p-c")).one().health = CopyHealth.SUSPECT
        session.get(Pool, "p-b").accepts_writes = not fenced
        previous_receipts = {r.event_id for r in session.scalars(select(VerifyReceipt))}
    backend.read_error = BackendStoredCorruptError("p-b stored object is corrupt")
    result = _rba_repair(engine, bundle_id, backend, monkeypatch, builder=builder)
    with session_scope(engine) as session:
        copy = session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one()
        assert copy.id == copy_id
        assert copy.health == (CopyHealth.CORRUPT if fenced else CopyHealth.OK)
        receipts = [
            r
            for r in session.scalars(
                select(VerifyReceipt)
                .where(VerifyReceipt.copy_id == copy_id)
                .order_by(VerifyReceipt.event_id)
            )
            if r.event_id not in previous_receipts
        ]
        assert len(receipts) == (1 if fenced else 2)
        assert receipts[0].source == "fanout"
        assert not receipts[0].backend_ok
        prefix = f"{bundle_id}:p-b:{copy.native_locator_key}:"
        assert receipts[0].execution_id.startswith(prefix)
        run = receipts[0].execution_id[len(prefix) :].removesuffix(f":reread:landed:{newest_id}")
        assert receipts[0].execution_id == f"{prefix}{run}:reread:landed:{newest_id}"
        if not fenced:
            assert receipts[1].execution_id == prefix + run
            assert receipts[1].backend_ok
    assert backend.calls == {"p-a": 1, "p-b": 1 if fenced else 2, "p-c": 1}
    assert result.ok, result.detail
    assert (None if result.condition is None else result.condition.reason) == (
        "fenced-missing" if fenced else None
    )


@pytest.mark.parametrize("lock_retry", [False, True], ids=["ordinary", "lock-retry"])
def test_fr1_repair_step2_reread_and_overwrite(engine, tmp_path, monkeypatch, lock_retry):
    """F2(c,e)/M19: reread state and rewrite have distinct, replay-safe receipts."""
    from sqlalchemy.exc import OperationalError

    from sutradhara import archive_fanout
    from sutradhara.backend.port import BackendStoredCorruptError
    from sutradhara.bundle_copy_ledger import identity_attempts
    from sutradhara.catalog.models import VerifyReceipt
    from tests.test_bundle_repair import _OutputsBuilder

    bundle_id = _rba_seed(engine, tmp_path)
    backend, builder = _Fr1OverwriteBackend(), _OutputsBuilder()
    with session_scope(engine) as session:
        flush_bundle(
            session, bundle_id=bundle_id, backends=_backends(session, backend), builder=builder
        )
        copy = session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one()
        copy.health = CopyHealth.SUSPECT
        copy_id = copy.id
        old_attempt_id = identity_attempts(
            session, bundle_id=bundle_id, pool_id="p-b", generation=0
        )[-1][0].id
        previous_receipts = {r.event_id for r in session.scalars(select(VerifyReceipt))}
    backend.read_error = BackendStoredCorruptError("corrupt before overwrite")
    delays, injected = [], []
    monkeypatch.setattr("sutradhara.jobs.worker.time.sleep", delays.append)
    real_record = archive_fanout._record_copy_result

    def record(session, **kwargs):
        if lock_retry and kwargs["target"].pool_id == "p-b" and not injected:
            injected.append(True)
            raise OperationalError(
                "repair recording lock", {}, sqlite3.OperationalError("database is locked")
            )
        return real_record(session, **kwargs)

    monkeypatch.setattr(archive_fanout, "_record_copy_result", record)
    result = _rba_repair(engine, bundle_id, backend, monkeypatch, builder=builder)
    assert result.ok, result.detail
    assert "p-b" in result.detail
    assert len(delays) == int(lock_retry)
    assert backend.calls == {"p-a": 1, "p-b": 2}
    with session_scope(engine) as session:
        copy = session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one()
        assert copy.id == copy_id
        assert copy.health == CopyHealth.OK
        assert copy.last_measured_digest == copy.integrity_hash
        receipts = [
            r
            for r in session.scalars(
                select(VerifyReceipt)
                .where(VerifyReceipt.copy_id == copy_id)
                .order_by(VerifyReceipt.event_id)
            )
            if r.event_id not in previous_receipts
        ]
        prefix = f"{bundle_id}:p-b:{copy.native_locator_key}:"
        assert receipts[0].execution_id.startswith(prefix)
        assert receipts[0].execution_id.endswith(f":reread:step2:{old_attempt_id}")
        run = (
            receipts[0].execution_id[len(prefix) :].removesuffix(f":reread:step2:{old_attempt_id}")
        )
        assert receipts[0].execution_id == f"{prefix}{run}:reread:step2:{old_attempt_id}"
        assert not receipts[0].backend_ok
        assert receipts[1].execution_id == f"{prefix}{run}:reread:copy:{old_attempt_id}"
        assert not receipts[1].backend_ok
        if lock_retry:
            newest = identity_attempts(session, bundle_id=bundle_id, pool_id="p-b", generation=0)[
                -1
            ][0].id
            assert newest != old_attempt_id
            assert len(receipts) == 3
            assert receipts[2].execution_id == f"{prefix}{run}:reread:landed:{newest}"
        else:
            assert len(receipts) == 3
            assert receipts[2].execution_id == prefix + run
        assert receipts[2].backend_ok


def test_fr2_repair_step2_and_lookup_have_distinct_receipts(engine, tmp_path, monkeypatch):
    """G2/M28: a good lookup read heals the corruption seen earlier in this repair."""
    from sutradhara.backend.port import BackendStoredCorruptError
    from sutradhara.bundle_copy_ledger import identity_attempts
    from sutradhara.catalog.models import VerifyReceipt
    from tests.test_bundle_repair import _OutputsBuilder

    class OnceCorruptBackend(_Fr1OverwriteBackend):
        """Consume the injected fault so step 2b observes the same stored bytes well."""

        def verify(self, locator, **kwargs):
            try:
                return super().verify(locator, **kwargs)
            finally:
                if locator["pool_id"] == "p-b":
                    self.read_error = None

    bundle_id = _rba_seed(engine, tmp_path)
    backend, builder = OnceCorruptBackend(), _OutputsBuilder()
    with session_scope(engine) as session:
        flush_bundle(
            session, bundle_id=bundle_id, backends=_backends(session, backend), builder=builder
        )
        copy = session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one()
        copy.health = CopyHealth.SUSPECT
        copy_id = copy.id
        newest_id = identity_attempts(session, bundle_id=bundle_id, pool_id="p-b", generation=0)[
            -1
        ][0].id
        previous_receipts = {r.event_id for r in session.scalars(select(VerifyReceipt))}
    backend.read_error = BackendStoredCorruptError("corrupt only on step 2's read")
    result = _rba_repair(engine, bundle_id, backend, monkeypatch, builder=builder)
    assert result.ok, result.detail
    assert result.condition is None
    assert backend.calls == {"p-a": 1, "p-b": 1}
    with session_scope(engine) as session:
        copy = session.get(Copy, copy_id)
        assert copy.health == CopyHealth.OK, "step 2b's good read reused step 2's corrupt receipt"
        assert copy.last_measured_digest == copy.integrity_hash
        receipts = [
            r
            for r in session.scalars(
                select(VerifyReceipt)
                .where(VerifyReceipt.copy_id == copy_id)
                .order_by(VerifyReceipt.event_id)
            )
            if r.event_id not in previous_receipts
        ]
        assert len(receipts) == 2
        prefix = f"{bundle_id}:p-b:{copy.native_locator_key}:"
        assert receipts[0].execution_id.startswith(prefix + "job")
        run = receipts[0].execution_id[len(prefix) :].removesuffix(f":reread:step2:{newest_id}")
        assert receipts[0].execution_id == f"{prefix}{run}:reread:step2:{newest_id}"
        assert receipts[1].execution_id == f"{prefix}{run}:reread:copy:{newest_id}"
        assert not receipts[0].backend_ok
        assert receipts[1].backend_ok


@pytest.mark.parametrize("health", [CopyHealth.SUSPECT, CopyHealth.CORRUPT])
def test_fr2_repair_step1_unexpected_read_is_not_repeated(engine, tmp_path, monkeypatch, health):
    """G3/M29: a landed reread's unexpected error stays write-unresolved in later steps."""

    class CountingBackend(_RbaBackend):
        """Count verification attempts, including the one whose error must be contained."""

        def __init__(self):
            super().__init__()
            self.reads = Counter()

        def verify(self, locator, **kwargs):
            self.reads[locator["pool_id"]] += 1
            return super().verify(locator, **kwargs)

    bundle_id = _rba_seed(engine, tmp_path)
    backend = CountingBackend()
    with session_scope(engine) as session:
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        copy = session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one()
        copy.health = health
        copy_id = copy.id
        _fr1_pending_duplicate(session, bundle_id)
    backend.reads.clear()
    backend.read_error = ValueError("unexpected landed read error")
    result = _rba_repair(engine, bundle_id, backend, monkeypatch)
    assert result.condition is not None, result.detail
    assert result.condition.condition == "backoff"
    assert result.condition.reason == "write-unresolved"
    assert "p-b" in result.condition.message
    assert backend.reads == {"p-b": 1}
    assert backend.calls == {"p-a": 1, "p-b": 1}
    with session_scope(engine) as session:
        assert session.get(Copy, copy_id).health == health
        condition = session.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == "bundle_copy",
                ReconciliationCondition.target_key == bundle_id,
            )
        ).one()
        assert condition.condition == "backoff"
        assert condition.reason == "write-unresolved"
        assert "p-b" in condition.message


def _fr1_released_landed_bundle(engine, tmp_path, backend):
    """Seed a flush resume with an OK landed copy and unresolved duplicate."""
    bundle_id = _rba_seed(engine, tmp_path)
    with session_scope(engine) as session:
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        newest = _fr1_pending_duplicate(session, bundle_id)
        bundle = session.get(Bundle, bundle_id)
        bundle.status = "flushing"
        bundle.claimed_by = None
        return bundle_id, newest


@pytest.mark.parametrize("failure", ["corrupt", "unexpected"])
def test_fr1_resume_failed_landed_reread(engine, tmp_path, monkeypatch, failure):
    """F2(d),F5/M21,M24: resume maps rebuild permission and unexpected reread errors."""
    from sutradhara.backend.port import BackendStoredCorruptError
    from sutradhara.catalog.models import VerifyReceipt

    backend = _RbaBackend()
    bundle_id, newest = _fr1_released_landed_bundle(engine, tmp_path, backend)
    backend.read_error = (
        BackendStoredCorruptError("landed bytes corrupt")
        if failure == "corrupt"
        else ValueError("unexpected landed read error")
    )
    events = []
    monkeypatch.setattr(
        "sutradhara.archive_fanout.emit_structured_event",
        lambda name, **kwargs: events.append((name, kwargs)),
    )
    with session_scope(engine) as session:
        result = flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        assert result.partial, "resume ignored the landed copy's failed reread"
        assert result.condition_reason == (
            "post-write-pool-failure" if failure == "corrupt" else "write-unresolved"
        )
        assert result.failed_pools == ("p-b",)
        assert "p-b" in result.condition_message
        assert session.get(Bundle, bundle_id).status == "sealed"
        condition = session.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == "bundle_copy",
                ReconciliationCondition.target_key == bundle_id,
            )
        ).one()
        assert condition.condition == ("blocked" if failure == "corrupt" else "backoff")
        copy = session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one()
        assert result.copy_ids.count(copy.id) == 1
        receipts = list(
            session.scalars(select(VerifyReceipt).where(VerifyReceipt.copy_id == copy.id))
        )
        assert len(receipts) == (2 if failure == "corrupt" else 1)
        if failure == "corrupt":
            assert copy.health == CopyHealth.CORRUPT
            assert receipts[-1].execution_id.endswith(f":reread:landed:{newest}")
            assert any(
                name == "bundle_copy_post_write_failure" and "p-b" in data["pools"]
                for name, data in events
            )
        else:
            assert copy.health == CopyHealth.OK
    assert backend.calls == {"p-a": 1, "p-b": 1}


def test_fr1_resume_counts_committed_landed_failure(engine, tmp_path, monkeypatch):
    """F3/M22: a killed flush keeps the earlier pool's committed failed readback."""
    from sutradhara import archive_fanout
    from sutradhara.jobs.config import WorkerConfig
    from sutradhara.jobs.engine import submit
    from sutradhara.jobs.worker import JobWorker

    class Killed(BaseException):
        pass

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend()
    backend.bad_verify_pools.add("p-a")
    monkeypatch.setattr("sutradhara.backend.factory.backend_from_row", lambda row: backend)
    monkeypatch.setattr(
        "sutradhara.jobs.handlers.bundle_sweep.make_archive_builder",
        lambda **kw: LocalArchiveBuilder(),
    )
    real_record = archive_fanout._record_copy_result
    events, resumed = [], []

    def record(session, **kwargs):
        if kwargs["target"].pool_id == "p-b":
            raise Killed()
        return real_record(session, **kwargs)

    monkeypatch.setattr(archive_fanout, "_record_copy_result", record)
    monkeypatch.setattr(
        archive_fanout,
        "emit_structured_event",
        lambda name, **kwargs: events.append((name, kwargs)),
    )
    with session_scope(engine) as session:
        submit(session, "bundle-sweep")
    config = WorkerConfig(executor_workers=1)
    with pytest.raises(Killed):
        JobWorker(engine, config=config).drain()
    with session_scope(engine) as session:
        copy = session.scalars(select(Copy).where(Copy.pool_id == "p-a")).one()
        assert copy.health == CopyHealth.SUSPECT
        copy_id = copy.id
    monkeypatch.setattr(archive_fanout, "_record_copy_result", real_record)
    real_flush = flush_bundle

    def flush(session, **kwargs):
        result = real_flush(session, **kwargs)
        resumed.append(result)
        return result

    monkeypatch.setattr("sutradhara.archive_sweeper.flush_bundle", flush)
    outcomes = JobWorker(engine, config=config).drain()
    assert outcomes[0].result.ok, outcomes[0].result.detail
    assert len(resumed) == 1
    assert resumed[0].partial, "resume discarded the earlier landed copy's failed readback"
    assert resumed[0].condition_reason == "post-write-pool-failure"
    assert "p-a" in resumed[0].condition_message
    assert copy_id in resumed[0].copy_ids
    assert backend.calls == {"p-a": 1, "p-b": 1}
    assert any(
        name == "bundle_copy_post_write_failure" and "p-a" in data["pools"] for name, data in events
    )
    with session_scope(engine) as session:
        assert session.get(Bundle, bundle_id).status == "sealed"
        condition = session.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == "bundle_copy",
                ReconciliationCondition.target_key == bundle_id,
            )
        ).one()
        assert condition.condition == "blocked"
        assert condition.reason == "post-write-pool-failure"


def test_fr2_resume_preserves_not_landed_failure(engine, tmp_path, monkeypatch):
    """G1/M27: a fresh worker keeps the failed pool committed by the next write's window."""
    from sutradhara import archive_fanout
    from sutradhara.bundle_copy_ledger import identity_attempts
    from sutradhara.catalog.models import BundleCopyWriteResolution
    from sutradhara.jobs.config import WorkerConfig
    from sutradhara.jobs.engine import submit
    from sutradhara.jobs.worker import JobWorker

    class Killed(BaseException):
        pass

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend("absent", failed_pool="p-a")
    monkeypatch.setattr("sutradhara.backend.factory.backend_from_row", lambda row: backend)
    monkeypatch.setattr(
        "sutradhara.jobs.handlers.bundle_sweep.make_archive_builder",
        lambda **kw: LocalArchiveBuilder(),
    )
    real_record = archive_fanout._record_copy_result

    def record(session, **kwargs):
        if kwargs["target"].pool_id == "p-b":
            raise Killed()
        return real_record(session, **kwargs)

    monkeypatch.setattr(archive_fanout, "_record_copy_result", record)
    with session_scope(engine) as session:
        submit(session, "bundle-sweep")
    config = WorkerConfig(executor_workers=1)
    with pytest.raises(Killed):
        JobWorker(engine, config=config).drain()
    with session_scope(engine) as session:
        assert session.get(Bundle, bundle_id).status == "flushing"
        assert [
            outcome
            for _, outcome in identity_attempts(
                session, bundle_id=bundle_id, pool_id="p-a", generation=0
            )
        ] == ["not-landed"]
        assert [
            outcome
            for _, outcome in identity_attempts(
                session, bundle_id=bundle_id, pool_id="p-b", generation=0
            )
        ] == [None]
        assert list(session.scalars(select(Copy))) == []
    monkeypatch.setattr(archive_fanout, "_record_copy_result", real_record)
    resumed = []

    def flush(session, **kwargs):
        result = flush_bundle(session, **kwargs)
        resumed.append(result)
        return result

    monkeypatch.setattr("sutradhara.archive_sweeper.flush_bundle", flush)
    outcomes = JobWorker(engine, config=config).drain()
    assert len(outcomes) == 1
    assert outcomes[0].result.ok, outcomes[0].result.detail
    assert len(resumed) == 1
    assert resumed[0].partial, "resume cleared the all-not-landed pool's failure"
    assert resumed[0].condition_reason == "transient-backend-failure"
    assert resumed[0].failed_pools == ("p-a",)
    assert "p-a" in resumed[0].condition_message
    assert backend.calls == {"p-a": 1, "p-b": 1}
    with session_scope(engine) as session:
        assert session.get(Bundle, bundle_id).status == "sealed"
        copy = session.scalars(select(Copy).where(Copy.bundle_id == bundle_id)).one()
        assert copy.pool_id == "p-b"
        assert resumed[0].copy_ids == (copy.id,)
        attempt, outcome = identity_attempts(
            session, bundle_id=bundle_id, pool_id="p-b", generation=0
        )[0]
        assert outcome == "landed"
        resolution = session.scalars(
            select(BundleCopyWriteResolution).where(
                BundleCopyWriteResolution.attempt_id == attempt.id
            )
        ).one()
        assert resolution.learned_by == "lookup"
        condition = session.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == "bundle_copy",
                ReconciliationCondition.target_key == bundle_id,
            )
        ).one()
        assert condition.condition == "backoff"
        assert condition.reason == "transient-backend-failure"
        assert "p-a" in condition.message


def test_fr1_resolution_integrity_error_escapes_flush(engine, tmp_path, monkeypatch):
    """F4(b)/M23: a failed write's resolution recording error is a flush failure."""
    from sqlalchemy.exc import IntegrityError

    from sutradhara import bundle_copy_ledger
    from sutradhara.catalog.models import BundleCopyWriteAttempt

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend("absent")
    real_resolution = bundle_copy_ledger.record_resolution

    def resolution(session, **kwargs):
        attempt = session.get(BundleCopyWriteAttempt, kwargs["attempt_id"])
        if attempt.pool_id == "p-b":
            raise IntegrityError("p-b resolution failed", {}, RuntimeError("injected constraint"))
        return real_resolution(session, **kwargs)

    monkeypatch.setattr(bundle_copy_ledger, "record_resolution", resolution)
    with session_scope(engine) as session:
        with pytest.raises(IntegrityError, match="p-b resolution failed"):
            flush_bundle(
                session,
                bundle_id=bundle_id,
                backends=_backends(session, backend),
                builder=LocalArchiveBuilder(),
            )
        bundle = session.get(Bundle, bundle_id)
        assert bundle.status == "flushing"
        assert bundle.claimed_by is None
    assert backend.calls == {"p-a": 1, "p-b": 1}


def test_fr1_null_locator_key_uses_attempt_name(engine, tmp_path, monkeypatch):
    """F8: repair's pre-rebuild lookup falls through a null locator key."""
    from sutradhara.bundle_copy_ledger import identity_attempts

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend()
    with session_scope(engine) as session:
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        copy = session.scalars(select(Copy).where(Copy.pool_id == "p-b")).one()
        copy.health = CopyHealth.MISSING
        assert "caller_object_id" not in copy.native_locator
        copy.native_locator = {**copy.native_locator, "key": None}
        expected_name = identity_attempts(
            session, bundle_id=bundle_id, pool_id="p-b", generation=0
        )[-1][0].object_name
    result = _rba_repair(engine, bundle_id, backend, monkeypatch)
    assert backend.lookup_calls == [("p-b", expected_name, backend.lookup_calls[0][2])]
    assert result.ok, result.detail
    assert backend.calls == {"p-a": 1, "p-b": 1}


def test_fr1_concurrent_landed_copy_ignores_lookup_receipt(engine, tmp_path):
    """F9/M26: a resolver must not record another locator's read onto a landed copy."""
    from sutradhara.backend.port import BackendUnavailableError
    from sutradhara.bundle_copy_ledger import resolve_identity
    from sutradhara.catalog.models import VerifyReceipt
    from sutradhara.replication import bundle_group_targets

    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend()
    backend.read_error = BackendUnavailableError("initial readback unavailable")
    with session_scope(engine) as session:
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
    backend.read_error = None
    first = []

    def lookup(pool, object_name, candidate_digests):
        backend.lookup_override = None
        with session_scope(engine) as session:
            target = next(
                target
                for _, target in bundle_group_targets(
                    session, session.get(Bundle, bundle_id), _backends(session, backend)
                )
                if target.pool_id == "p-b"
            )
            work_dir = tmp_path / "resolver-a"
            work_dir.mkdir()
            copy, problem, _ = resolve_identity(
                session,
                bundle_id=bundle_id,
                target=target,
                backend=backend,
                builder=LocalArchiveBuilder(),
                work_dir=work_dir,
            )
            assert problem is None
            first.append(copy.id)
        found = backend.names[(pool, object_name)]
        readable = "readable-second-object"
        backend.objects[readable] = backend.objects[
            str(found.copy_record.native_locator["object_id"])
        ]
        return replace(
            found,
            copy_record=replace(
                found.copy_record,
                native_locator={**found.copy_record.native_locator, "object_id": readable},
            ),
        )

    backend.lookup_override = lookup
    with session_scope(engine) as session:
        target = next(
            target
            for _, target in bundle_group_targets(
                session, session.get(Bundle, bundle_id), _backends(session, backend)
            )
            if target.pool_id == "p-b"
        )
        work_dir = tmp_path / "resolver-b"
        work_dir.mkdir()
        copy, problem, may_rebuild = resolve_identity(
            session,
            bundle_id=bundle_id,
            target=target,
            backend=backend,
            builder=LocalArchiveBuilder(),
            work_dir=work_dir,
        )
        assert problem is None
        assert not may_rebuild
        assert copy.id == first[0]
        receipts = list(
            session.scalars(select(VerifyReceipt).where(VerifyReceipt.copy_id == copy.id))
        )
        assert len(receipts) == 1, "resolver B recorded its lookup read on resolver A's copy"
        assert receipts[0].backend_ok
    assert backend.calls == {"p-a": 1, "p-b": 1}


def test_fr1_sweep_entry_window_precedes_reaper_update(engine, tmp_path):
    """F6/M25: an existing deferred read transaction gains the writer before reaping."""
    bundle_id = _rba_seed(engine, tmp_path)
    backend = _RbaBackend()
    with session_scope(engine) as session:
        bundle = session.get(Bundle, bundle_id)
        claim_bundle_for_flush(session, bundle, worker_id="dead-host:123")
        bundle.archive_id = f"archive-{bundle_id}"
    statements = []

    def trace(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", trace)
    try:
        with session_scope(engine) as session:
            session.connection().exec_driver_sql("BEGIN")
            assert session.scalar(select(Bundle.id)) == bundle_id
            result = sweep_bundles(
                session, backends=_backends(session, backend), builder=LocalArchiveBuilder()
            )
            assert result.reaped == (bundle_id,)
    finally:
        event.remove(engine, "before_cursor_execute", trace)
    first_update = next(i for i, sql in enumerate(statements) if sql.startswith("UPDATE"))
    assert statements.index("BEGIN IMMEDIATE") < first_update, statements[: first_update + 1]
