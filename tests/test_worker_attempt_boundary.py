"""ALB-1 proofs through worker attempts on file-backed SQLite databases.

The second writer uses a real independent connection, with no sockets or
external services. Caller ownership and CLI startup are the stated exceptions
to driving the worker attempt path.
"""

import datetime as dt
import hashlib
import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner
from sqlalchemy import event, select, text, update
from sqlalchemy.exc import IntegrityError

from sutradhara.backend.port import BackendStoredCorruptError, VerifyResult
from sutradhara.catalog.models import (
    AssetDerivation,
    Backend,
    Copy,
    IngestItem,
    Intake,
    LogicalAsset,
    VerifyReceipt,
)
from sutradhara.catalog.session import create_all, locator_key, make_engine, session_scope
from sutradhara.catalog.types import AssetValidity, BackendKind, BackendTier, CopyHealth, CopySource
from sutradhara.cli.worker import worker_cmd
from sutradhara.jobs import registry
from sutradhara.jobs.config import WorkerConfig
from sutradhara.jobs.engine import run_one, submit
from sutradhara.jobs.models import Job, JobAttempt, JobStatus, ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import record_observation
from sutradhara.jobs.registry import JobResult
from sutradhara.jobs.worker import JobWorker, _execute_job_once


def _backend(name):
    return Backend(name=name, kind=BackendKind.MEMORY, tier=BackendTier.SELF_DESCRIBING)


@pytest.fixture
def boundary_env(tmp_path, monkeypatch):
    path = tmp_path / "attempt.db"
    engine = make_engine(f"sqlite:///{path}", busy_timeout=0.2)
    create_all(engine)
    config = WorkerConfig(executor_workers=1)
    events = []

    def emit(name, **fields):
        events.append((name, fields))

    monkeypatch.setattr("sutradhara.jobs.engine.emit_structured_event", emit)
    monkeypatch.setattr("sutradhara.jobs.worker.emit_structured_event", emit)
    yield engine, path, config, events
    engine.dispose()


def _submit_handler(env, monkeypatch, handler):
    engine, _path, _config, _events = env
    monkeypatch.setitem(registry._HANDLERS, "_boundary", handler)
    with session_scope(engine) as session:
        return submit(session, "_boundary").id


def _hold_writer(path, threads):
    """Hold the real write lock for about a second, spanning boundary exit."""
    ready = threading.Event()
    errors = []

    def hold():
        try:
            with sqlite3.connect(path, timeout=0) as connection:
                connection.execute("BEGIN IMMEDIATE")
                ready.set()
                threading.Event().wait(1)
        except BaseException as exc:
            errors.append(exc)
            ready.set()

    thread = threading.Thread(target=hold)
    threads.append((thread, errors))
    thread.start()
    assert ready.wait(3)
    assert not errors


def _join_holders(threads):
    for thread, errors in threads:
        thread.join(timeout=3)
        assert not thread.is_alive()
        assert not errors


def test_boundary_waits_without_replaying_external_work(boundary_env, monkeypatch):
    """Proof 2: busy-timeout retries acquire the lock without replay."""
    engine, path, config, events = boundary_env
    calls = []
    threads = []

    def handler(ctx):
        with ctx.outside_transaction() as active:
            assert active
            calls.append(1)
            _hold_writer(path, threads)
        ctx.session.add(_backend("published"))
        return JobResult(ok=True)

    job_id = _submit_handler(boundary_env, monkeypatch, handler)
    started = time.monotonic()
    try:
        outcomes = JobWorker(engine, config=config).drain()
    finally:
        _join_holders(threads)
    assert time.monotonic() - started >= 0.9
    assert len(outcomes) == 1
    assert outcomes[0].result.ok
    assert calls == [1]
    waits = [fields for name, fields in events if name.endswith("write_lock_wait")]
    assert waits
    assert all(row["seconds_waited"] >= 0.19 for row in waits)
    assert all(row["job_id"] == job_id and row["job_kind"] == "_boundary" for row in waits)
    with session_scope(engine) as session:
        assert session.scalars(select(Backend)).one().name == "published"
        assert session.scalars(select(JobAttempt)).one().outcome == JobStatus.SUCCEEDED


@pytest.mark.parametrize("write", ["orm_pending", "orm_flushed", "core"])
def test_boundary_refuses_prior_writes(boundary_env, monkeypatch, write):
    """Proof 3: both ORM state and driver total_changes protect the boundary."""
    engine, _path, config, _events = boundary_env

    def handler(ctx):
        if write == "core":
            ctx.session.execute(update(Job).where(Job.id == ctx.job.id).values(priority=7))
        else:
            ctx.session.add(_backend("must-rollback"))
            if write == "orm_flushed":
                ctx.session.flush()
        with ctx.outside_transaction():
            pytest.fail("guard allowed external work after writes")
        return JobResult(ok=True)

    job_id = _submit_handler(boundary_env, monkeypatch, handler)
    outcome = JobWorker(engine, config=config).drain()[0]
    assert not outcome.result.ok
    with session_scope(engine) as session:
        assert list(session.scalars(select(Backend))) == []
        assert session.get(Job, job_id).priority == 0
        attempt = session.scalars(select(JobAttempt)).one()
        assert (
            "AttemptBoundaryError: attempt transaction boundary requires no prior writes"
            in attempt.error
        )


def test_job_reset_supersedes_attempt_without_audit_row(boundary_env, monkeypatch):
    """Proof 4: R4 preserves the changer's row and discards observations."""
    engine, path, config, events = boundary_env
    reset_row = []

    def handler(ctx):
        job_id = ctx.job.id
        with ctx.outside_transaction():
            with sqlite3.connect(path, timeout=0) as connection:
                connection.execute(
                    "UPDATE job SET status='pending', last_error='reset by other' WHERE id=?",
                    (job_id,),
                )
                reset_row.extend(
                    connection.execute("SELECT * FROM job WHERE id=?", (job_id,)).fetchone()
                )
            ctx.observe({"discarded": True})
        ctx.session.add(_backend("must-not-publish"))
        return JobResult(ok=True)

    job_id = _submit_handler(boundary_env, monkeypatch, handler)
    worker = JobWorker(engine, config=config)
    assert worker._claim_available(max_new=1) == [(job_id, {})]
    outcome = _execute_job_once(engine, job_id, {}, config)
    assert not outcome.result.ok
    assert outcome.result.superseded
    assert "changed from RUNNING" in outcome.result.detail
    with sqlite3.connect(path) as connection:
        assert (
            list(connection.execute("SELECT * FROM job WHERE id=?", (job_id,)).fetchone())
            == reset_row
        )
    with session_scope(engine) as session:
        assert list(session.scalars(select(Backend))) == []
        assert list(session.scalars(select(JobAttempt))) == []
    finished = [fields for name, fields in events if name.endswith("finished")]
    assert len(finished) == 1
    assert finished[0]["outcome"] == "superseded"
    assert finished[0]["job_status"] == JobStatus.PENDING


@pytest.mark.parametrize(
    "failure", ["exception", "handler_sql", "engine_sql", "raw_sql", "engine_add"]
)
def test_boundary_failure_waits_before_recording(boundary_env, monkeypatch, failure):
    """Proof 6: exceptional exits and SQL misuse reacquire before failure recording."""
    engine, path, config, events = boundary_env
    threads = []
    calls = []

    def handler(ctx):
        engine_session = ctx.session.info["attempt_boundary"].session
        with ctx.outside_transaction():
            calls.append(1)
            _hold_writer(path, threads)
            if failure == "exception":
                raise ValueError("external failure")
            if failure == "raw_sql":
                ctx.session.connection().exec_driver_sql("SELECT 1")
            if failure == "engine_add":
                engine_session.add(_backend("forbidden"))
            session = ctx.session if failure == "handler_sql" else engine_session
            session.execute(text("SELECT 1"))
        return JobResult(ok=True)

    _submit_handler(boundary_env, monkeypatch, handler)
    try:
        outcome = JobWorker(engine, config=config).drain()[0]
    finally:
        _join_holders(threads)
    assert not outcome.result.ok
    assert calls == [1]
    assert any(name.endswith("write_lock_wait") for name, _fields in events)
    with session_scope(engine) as session:
        attempt = session.scalars(select(JobAttempt)).one()
        assert attempt.outcome == JobStatus.FAILED
        expected = (
            "ValueError: external failure"
            if failure == "exception"
            else "AttemptBoundaryError: SQL used"
            if failure == "raw_sql"
            else "AttemptBoundaryError: session used"
        )
        assert expected in attempt.error
        assert list(session.scalars(select(Backend))) == []


def test_rollback_arm_waits_before_recording_failure(boundary_env, monkeypatch):
    """Proof 11: an inactive engine session re-takes the writer lock after rollback."""
    engine, path, config, events = boundary_env
    threads = []
    calls = []

    def handler(ctx):
        engine_session = ctx.session.info["attempt_boundary"].session
        with ctx.outside_transaction() as active:
            assert active
            calls.append(1)
        # Fail the outer session's flush, which rolls back the IMMEDIATE
        # transaction. Hold the freed writer slot before run_one recovers it.
        engine_session.add_all([_backend("duplicate"), _backend("duplicate")])
        try:
            engine_session.flush()
        except IntegrityError:
            assert not engine_session.is_active
            _hold_writer(path, threads)
            raise
        pytest.fail("duplicate backend names did not fail the flush")

    job_id = _submit_handler(boundary_env, monkeypatch, handler)
    worker = JobWorker(engine, config=config)
    assert worker._claim_available(max_new=1) == [(job_id, {})]
    started = time.monotonic()
    try:
        outcome = _execute_job_once(engine, job_id, {}, config)
    finally:
        _join_holders(threads)
    assert time.monotonic() - started >= 0.9
    assert not outcome.result.ok
    assert calls == [1]
    waits = [fields for name, fields in events if name.endswith("write_lock_wait")]
    assert waits
    assert all(row["job_id"] == job_id and row["job_kind"] == "_boundary" for row in waits)
    with session_scope(engine) as session:
        assert session.get(Job, job_id).status == JobStatus.FAILED
        assert session.get(Job, job_id).attempts == 1
        attempt = session.scalars(select(JobAttempt)).one()
        assert attempt.outcome == JobStatus.FAILED
        assert "IntegrityError" in attempt.error
        assert "UNIQUE constraint failed: backend.name" in attempt.error
        assert list(session.scalars(select(Backend))) == []


def test_non_idle_driver_waits_before_recording_failure(boundary_env, monkeypatch):
    """Proof 13: raw driver misuse is rolled back, then failed under the write lock."""
    engine, path, config, events = boundary_env
    threads = []
    calls = []

    def handler(ctx):
        dbapi = ctx.session.info["attempt_boundary"].connection.connection.dbapi_connection
        with ctx.outside_transaction() as active:
            assert active
            calls.append(1)
            # Bypass SQLAlchemy's SQL guard to reach the driver-idleness check.
            dbapi.execute("BEGIN")
            dbapi.execute("SELECT count(*) FROM backend").fetchone()
            assert dbapi.in_transaction
            _hold_writer(path, threads)
        ctx.session.add(_backend("must-not-publish"))
        return JobResult(ok=True)

    job_id = _submit_handler(boundary_env, monkeypatch, handler)
    worker = JobWorker(engine, config=config)
    assert worker._claim_available(max_new=1) == [(job_id, {})]
    started = time.monotonic()
    try:
        outcome = _execute_job_once(engine, job_id, {}, config)
    finally:
        _join_holders(threads)
    assert time.monotonic() - started >= 0.9
    assert not outcome.result.ok
    assert calls == [1]
    waits = [fields for name, fields in events if name.endswith("write_lock_wait")]
    assert waits
    assert all(row["job_id"] == job_id and row["job_kind"] == "_boundary" for row in waits)
    with session_scope(engine) as session:
        assert session.get(Job, job_id).status == JobStatus.FAILED
        assert session.get(Job, job_id).attempts == 1
        attempt = session.scalars(select(JobAttempt)).one()
        assert attempt.outcome == JobStatus.FAILED
        assert (
            "AttemptBoundaryError: attempt driver was not idle before re-opening" in attempt.error
        )
        assert list(session.scalars(select(Backend))) == []


def test_caller_owned_session_keeps_transaction(boundary_env, monkeypatch):
    """Proof 7: direct run_one never releases a caller's writes or compares."""
    engine, path, _config, _events = boundary_env

    def handler(ctx):
        ctx.session.add(_backend("caller-owned"))
        ctx.session.flush()
        with ctx.outside_transaction() as active:
            assert not active
            assert ctx.session.scalar(select(Job.status)) == JobStatus.RUNNING
            with sqlite3.connect(path, timeout=0) as connection:
                assert connection.execute("SELECT count(*) FROM backend").fetchone() == (0,)
        return JobResult(ok=True)

    job_id = _submit_handler(boundary_env, monkeypatch, handler)
    with session_scope(engine) as session:
        result = run_one(session, job_id)
        assert result.ok
        assert not result.superseded
        session.rollback()
    with session_scope(engine) as session:
        assert session.get(Job, job_id).status == JobStatus.PENDING
        assert list(session.scalars(select(Backend))) == []
        assert list(session.scalars(select(JobAttempt))) == []


def test_second_boundary_is_refused(boundary_env, monkeypatch):
    """The fold allows exactly one active boundary per attempt."""
    engine, _path, config, _events = boundary_env

    def handler(ctx):
        with ctx.outside_transaction():
            pass
        with ctx.outside_transaction():
            pytest.fail("second boundary entered")
        return JobResult(ok=True)

    _submit_handler(boundary_env, monkeypatch, handler)
    assert not JobWorker(engine, config=config).drain()[0].result.ok
    with session_scope(engine) as session:
        assert (
            "AttemptBoundaryError: attempt transaction boundary already used"
            in session.scalars(select(JobAttempt)).one().error
        )


@pytest.mark.parametrize("mode", ["memory", "wal"])
def test_worker_requires_wal_after_connect_hook(boundary_env, monkeypatch, mode):
    """Proof 9: in-memory SQLite cannot switch to WAL; a local file can."""
    engine, _path, _config, _events = boundary_env
    monkeypatch.setenv("SUTRADHARA_DB_URL", str(engine.url))
    if mode == "memory":
        # Keep the singleton lock file under tmp_path, but give the worker the
        # non-WAL engine whose own connect hook has attempted the WAL switch.
        memory_engine = make_engine("sqlite:///:memory:", busy_timeout=0.2)
        monkeypatch.setattr("sutradhara.cli.worker.make_engine", lambda *a, **kw: memory_engine)
    result = CliRunner().invoke(worker_cmd, ["--once"])
    if mode == "memory":
        assert result.exit_code == 1
        assert "requires SQLite WAL" in result.output
        assert "database reports memory" in result.output
    else:
        assert result.exit_code == 0, result.output
        assert "worker drained 0 job(s)" in result.output


def test_worker_pool_reserves_scheduler_connections(boundary_env, monkeypatch):
    """Proof 10: the configured pool holds all executors plus two connections."""
    engine, _path, _config, _events = boundary_env
    monkeypatch.setenv("SUTRADHARA_DB_URL", str(engine.url))
    config = WorkerConfig(executor_workers=7)
    monkeypatch.setattr(WorkerConfig, "defaults", classmethod(lambda cls: config))
    pools = []

    def inspect_drain(worker):
        pools.append(worker.engine.pool.size())
        connections = [worker.engine.connect() for _ in range(9)]
        try:
            assert worker.engine.pool.overflow() <= 0
        finally:
            for connection in connections:
                connection.close()
            worker.engine.dispose()
        return []

    monkeypatch.setattr(JobWorker, "drain", inspect_drain)
    result = CliRunner().invoke(worker_cmd, ["--once"])
    assert result.exit_code == 0, result.output
    assert pools
    assert pools[0] >= 9


def _late_job(env, tmp_path, monkeypatch, kind, external, *, reconciler=False, corrupt=False):
    """Seed real handler inputs and patch only their external operation."""
    if kind == "pfr-index":
        pytest.importorskip("pfr_core", exc_type=ImportError)
        from pfr_core import PFRSidecar
        from pfr_core.schema import CapabilitySnapshot

    engine, _path, _config, _events = env
    source = tmp_path / "source.mov"
    source.write_bytes(b'"valid source"')
    digest = hashlib.sha256(source.read_bytes()).digest()
    cache = tmp_path / "cache"
    with session_scope(engine) as session:
        asset = LogicalAsset(content_sha256=digest, size_bytes=source.stat().st_size)
        backend = _backend("unrelated")
        intake = Intake(intake_id="alb", operator="test", source_kind="card", artifactclass="video")
        session.add_all([asset, backend, intake])
        session.flush()
        item = IngestItem(
            intake_id=intake.intake_id,
            logical_asset_hash=digest,
            as_received_path="source.mov",
            virtual_path="source.mov",
            artifactclass="video",
            size_bytes=source.stat().st_size,
            item_metadata={"source_path": str(source), "kept": "metadata"},
        )
        locator = {"hash_hex": digest.hex()}
        copy = Copy(
            logical_asset_hash=digest,
            backend_id=backend.id,
            native_locator=locator,
            native_locator_key=locator_key(locator),
            integrity_hash=digest,
            source=CopySource.INGEST,
        )
        session.add_all([item, copy])
        session.flush()
        params = {
            "transcode": {
                "ingest_item_id": item.id,
                "output_class": "proxy",
                "cache_root": str(cache),
            },
            "pfr-index": {"ingest_item_id": item.id, "cache_root": str(cache)},
            "verify": {"copy_id": copy.id},
            "validate": {"asset_hash": digest.hex(), "path": str(source), "validator": "json"},
        }[kind]
        options = {}
        if reconciler:
            options = {"recon_domain": "derivation", "recon_target_key": f"pfr:{item.id}"}
            condition = record_observation(
                session,
                domain="derivation",
                target_key=options["recon_target_key"],
                desired=True,
                observed_state="missing",
            )
            # Past the ordinary give-up threshold: superseded must still BACKOFF.
            condition.attempt_count = 5
        job_id = submit(session, kind, params, **options).id
        item_id, copy_id = item.id, copy.id

    def transcode(_source, mezz, preview, **kwargs):
        external()
        mezz.write_bytes(b"mezz")
        preview.write_bytes(b"preview")
        return {"kind": "ok", "detail": "test transcode"}

    def scrape(*args, **kwargs):
        external()
        return PFRSidecar(
            grammar_id="mxf",
            schema_version="1",
            plugin_version="test",
            recipe_version="r1",
            measured_facts={"logical_size": source.stat().st_size},
            source_identity={"kind": "fixture", "size_bytes": source.stat().st_size},
            capability_snapshot=CapabilitySnapshot(
                achieved_granularity="edit_unit", tc_source="file_relative", rewrap_tool="test"
            ),
        )

    def verify(_locator):
        external()
        if corrupt:
            raise BackendStoredCorruptError("stored bytes corrupt")
        return VerifyResult(ok=True, measured=True, actual_hash=digest, detail="measured")

    read_bytes = Path.read_bytes

    def read(path):
        if path == source:
            external()
        return read_bytes(path)

    if kind == "transcode":
        monkeypatch.delenv("SUTRADHARA_FAKE_TRANSCODE", raising=False)
        monkeypatch.setattr(
            "sutradhara.jobs.handlers.transcode.current_tool_version", lambda tool: "test"
        )
        monkeypatch.setattr("sutradhara.jobs.handlers.transcode._run_ffmpeg", transcode)
    elif kind == "pfr-index":
        monkeypatch.setattr("sutradhara.jobs.handlers.pfr_index.scrape_path_isolated_120", scrape)
    elif kind == "verify":
        monkeypatch.setattr(
            "sutradhara.backend.factory.backend_from_row",
            lambda row: SimpleNamespace(verify=verify),
        )
    else:
        monkeypatch.setattr(Path, "read_bytes", read)
    return SimpleNamespace(
        job_id=job_id,
        item_id=item_id,
        copy_id=copy_id,
        digest=digest,
        source=source,
        sidecar=cache / "intakes" / "alb" / "pfr" / f"{item_id}.pfr.json",
    )


@pytest.mark.parametrize("kind", ["transcode", "pfr-index", "verify", "validate"])
def test_late_writers_allow_concurrent_commit_once(boundary_env, tmp_path, monkeypatch, kind):
    """Proof 1: each real handler leaves SQLite around its external operation."""
    engine, path, config, _events = boundary_env
    calls = []

    def external():
        calls.append(1)
        if len(calls) == 1:
            with sqlite3.connect(path, timeout=0) as other:
                other.execute("UPDATE backend SET name='concurrent' WHERE name='unrelated'")

    seeded = _late_job(boundary_env, tmp_path, monkeypatch, kind, external)
    outcomes = JobWorker(engine, config=config).drain()
    assert len(outcomes) == 1
    assert outcomes[0].result.ok, outcomes[0].result.detail
    assert calls == [1]
    with session_scope(engine) as session:
        assert session.get(Job, seeded.job_id).status == JobStatus.SUCCEEDED
        assert session.scalars(select(JobAttempt)).one().outcome == JobStatus.SUCCEEDED
        assert session.scalars(select(Backend)).one().name == "concurrent"
        if kind == "transcode":
            assert len(list(session.scalars(select(AssetDerivation)))) == 2
        elif kind == "pfr-index":
            assert seeded.sidecar.exists()
            assert session.get(IngestItem, seeded.item_id).item_metadata["pfr_sidecar_path"] == str(
                seeded.sidecar
            )
        elif kind == "verify":
            assert session.scalars(select(VerifyReceipt)).one().measured_digest == seeded.digest
        else:
            assert session.get(LogicalAsset, seeded.digest).validity == AssetValidity.OK


def test_fake_transcode_supplies_output_digests_without_resource_role(
    boundary_env, tmp_path, monkeypatch
):
    """Proof 12 and item 3: fake transcode supplies both hashes despite an invalid role."""
    from sutradhara.jobs.handlers import transcode

    engine, _path, config, _events = boundary_env
    seeded = _late_job(boundary_env, tmp_path, monkeypatch, "transcode", lambda: None)
    monkeypatch.setenv("SUTRADHARA_FAKE_TRANSCODE", "1")
    with session_scope(engine) as session:
        job = session.get(Job, seeded.job_id)
        job.params = {**job.params, "resource_role": "invalid-fake-role"}

    def no_hash_in_write_phase(*args, **kwargs):
        raise RuntimeError("record_derivation must use the digest computed outside the transaction")

    drivers = []
    hashes = {}
    sha256_file = transcode.sha256_file

    def remember_driver(connection, *args):
        drivers[:] = [connection.connection.dbapi_connection]

    def hash_outside_transaction(path):
        assert drivers
        assert not drivers[0].in_transaction
        digest = sha256_file(path)
        hashes[path] = digest
        return digest

    monkeypatch.setattr("sutradhara.catalog.facts.sha256_file", no_hash_in_write_phase)
    monkeypatch.setattr(transcode, "sha256_file", hash_outside_transaction)
    event.listen(engine, "before_cursor_execute", remember_driver)
    try:
        outcomes = JobWorker(engine, config=config).drain()
    finally:
        event.remove(engine, "before_cursor_execute", remember_driver)
    assert len(outcomes) == 1
    assert outcomes[0].result.ok, outcomes[0].result.detail
    with session_scope(engine) as session:
        job = session.get(Job, seeded.job_id)
        assert job.status == JobStatus.SUCCEEDED
        attempt = session.scalars(select(JobAttempt)).one()
        assert attempt.outcome == JobStatus.SUCCEEDED
        edges = list(session.scalars(select(AssetDerivation)))
        assert {edge.kind for edge in edges} == {"mezz", "preview"}
        assert len(edges) == 2
        for edge in edges:
            assert edge.source_item_id == seeded.item_id
            derived = session.get(IngestItem, edge.derived_item_id)
            output = Path(derived.item_metadata["source_path"])
            expected_bytes = (
                f"fake {'mezzanine' if edge.kind == 'mezz' else 'preview'} "
                f"for {seeded.digest.hex()}\n"
            ).encode()
            assert output.read_bytes() == expected_bytes
            assert derived.logical_asset_hash == hashlib.sha256(expected_bytes).digest()
            assert hashes[output] == derived.logical_asset_hash
            assert session.get(LogicalAsset, derived.logical_asset_hash) is not None
        assert len(hashes) == 3  # The fake reads its source as well as both outputs.


@pytest.mark.parametrize("corrupt", [False, True])
def test_verify_superseded_keeps_measurement_without_receipt(
    boundary_env, tmp_path, monkeypatch, corrupt
):
    """Proof 5: measured and stored-corrupt answers cannot overwrite newer health."""
    engine, path, config, events = boundary_env
    calls = []

    def external():
        calls.append(1)
        with sqlite3.connect(path, timeout=0) as other:
            other.execute("UPDATE copy SET health='missing' WHERE id=?", (seeded.copy_id,))

    seeded = _late_job(boundary_env, tmp_path, monkeypatch, "verify", external, corrupt=corrupt)
    started = dt.datetime.now(dt.UTC)
    outcome = JobWorker(engine, config=config).drain()[0]
    assert not outcome.result.ok
    assert calls == [1]
    with session_scope(engine) as session:
        copy = session.get(Copy, seeded.copy_id)
        assert copy.health == CopyHealth.MISSING
        assert copy.last_measured_digest is None
        assert copy.last_measured_at is None
        assert copy.last_checked_at is None
        assert list(session.scalars(select(VerifyReceipt))) == []
        job = session.get(Job, seeded.job_id)
        assert job.status == JobStatus.PENDING
        assert job.started_at is None
        assert job.finished_at is None
        assert job.not_before.replace(tzinfo=dt.UTC) >= started + dt.timedelta(seconds=30)
        assert job.attempts == 1
        attempt = session.scalars(select(JobAttempt)).one()
        assert attempt.outcome == JobStatus.FAILED
        assert attempt.error.startswith("superseded:")
        observation = next(row for row in attempt.detail["observations"] if "verify_ok" in row)
        assert observation["verify_ok"] is not corrupt
        assert observation["measured"] is not corrupt
        assert observation["actual_hash"] == (None if corrupt else seeded.digest.hex())
    assert outcome.result.superseded
    assert [fields["outcome"] for name, fields in events if name.endswith("finished")] == [
        "superseded"
    ]


def test_pfr_superseded_preserves_sidecar_and_backs_off(boundary_env, tmp_path, monkeypatch):
    """Proof 5: a new item identity discards the staged file without projection."""
    engine, path, config, _events = boundary_env
    changed_hash = hashlib.sha256(b"changed").digest()

    def external():
        with sqlite3.connect(path, timeout=0) as other:
            other.execute(
                "UPDATE ingest_item SET logical_asset_hash=? WHERE id=?",
                (changed_hash, seeded.item_id),
            )

    seeded = _late_job(boundary_env, tmp_path, monkeypatch, "pfr-index", external, reconciler=True)
    seeded.sidecar.parent.mkdir(parents=True)
    seeded.sidecar.write_bytes(b"previous catalog sidecar")
    with session_scope(engine) as session:
        session.add(LogicalAsset(content_sha256=changed_hash, size_bytes=7))
        item = session.get(IngestItem, seeded.item_id)
        item.item_metadata = {**item.item_metadata, "pfr_sidecar_path": str(seeded.sidecar)}

    def no_record(*args, **kwargs):
        pytest.fail("superseded pfr-index must not record_index")

    monkeypatch.setattr("sutradhara.jobs.handlers.pfr_index.record_index", no_record)
    outcome = JobWorker(engine, config=config).drain()[0]
    assert not outcome.result.ok
    assert outcome.result.superseded
    assert seeded.sidecar.read_bytes() == b"previous catalog sidecar"
    assert sorted(p.name for p in seeded.sidecar.parent.iterdir()) == [seeded.sidecar.name, "blobs"]
    with session_scope(engine) as session:
        condition = session.scalars(select(ReconciliationCondition)).one()
        assert condition.condition == "backoff"
        assert condition.reason == "superseded"
        assert condition.attempt_count == 6
        assert condition.next_eligible_at is not None
        assert session.get(IngestItem, seeded.item_id).logical_asset_hash == changed_hash
        assert "pfr_recipe_version" not in session.get(IngestItem, seeded.item_id).item_metadata


@pytest.mark.parametrize("fail", [False, True])
def test_early_writer_keeps_atomic_attempt(boundary_env, monkeypatch, fail):
    """Proof 8 regression guard: early writes and terminal status commit together."""
    engine, path, config, _events = boundary_env

    def handler(ctx):
        ctx.session.add(_backend("early"))
        ctx.session.commit()
        with sqlite3.connect(path, timeout=0) as other:
            assert other.execute("SELECT count(*) FROM backend").fetchone() == (0,)
            assert other.execute("SELECT status FROM job").fetchone() == ("running",)
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                other.execute("BEGIN IMMEDIATE")
        if fail:
            raise ValueError("after early write")
        return JobResult(ok=True)

    job_id = _submit_handler(boundary_env, monkeypatch, handler)
    outcome = JobWorker(engine, config=config).drain()[0]
    assert outcome.result.ok is not fail
    with session_scope(engine) as session:
        assert len(list(session.scalars(select(Backend)))) == (0 if fail else 1)
        expected = JobStatus.FAILED if fail else JobStatus.SUCCEEDED
        assert session.get(Job, job_id).status == expected
        assert session.scalars(select(JobAttempt)).one().outcome == expected


@pytest.mark.parametrize("kind", ["transcode", "pfr-index", "verify", "validate"])
def test_deleted_inputs_supersede_without_projection(boundary_env, tmp_path, monkeypatch, kind):
    """R5: a vanished row is a comparison difference, not an ORM load error."""
    engine, path, config, _events = boundary_env

    def external():
        with sqlite3.connect(path, timeout=0) as other:
            other.execute("PRAGMA foreign_keys=ON")
            if kind == "verify":
                other.execute("DELETE FROM copy WHERE id=?", (seeded.copy_id,))
            elif kind == "validate":
                other.execute("DELETE FROM logical_asset WHERE content_sha256=?", (seeded.digest,))
            else:
                other.execute("DELETE FROM ingest_item WHERE id=?", (seeded.item_id,))

    seeded = _late_job(boundary_env, tmp_path, monkeypatch, kind, external)
    outcome = JobWorker(engine, config=config).drain()[0]
    assert not outcome.result.ok, outcome.result.detail
    assert outcome.result.superseded, outcome.result.detail
    assert not seeded.sidecar.exists()
    with session_scope(engine) as session:
        assert session.get(Job, seeded.job_id).status == JobStatus.PENDING
        assert session.scalars(select(JobAttempt)).one().error.startswith("superseded:")
        assert list(session.scalars(select(VerifyReceipt))) == []
        assert list(session.scalars(select(AssetDerivation))) == []


def test_transcode_changed_target_is_not_overwritten(boundary_env, tmp_path, monkeypatch):
    """R5: record_derivation's intake/relpath key is compared before either write."""
    engine, _path, config, _events = boundary_env
    target_id = []

    def external():
        with session_scope(engine) as session:
            target = IngestItem(
                intake_id="alb",
                logical_asset_hash=seeded.digest,
                as_received_path=f"derived/{seeded.item_id}/preview.mp4",
                virtual_path="other",
                artifactclass="other",
                size_bytes=13,
            )
            session.add(target)
            session.flush()
            target_id.append(target.id)

    seeded = _late_job(boundary_env, tmp_path, monkeypatch, "transcode", external)
    outcome = JobWorker(engine, config=config).drain()[0]
    assert not outcome.result.ok
    assert outcome.result.superseded
    with session_scope(engine) as session:
        target = session.get(IngestItem, target_id[0])
        assert target.logical_asset_hash == seeded.digest
        assert target.artifactclass == "other"
        assert session.get(LogicalAsset, seeded.digest).validity == AssetValidity.UNVALIDATED
        assert list(session.scalars(select(AssetDerivation))) == []
