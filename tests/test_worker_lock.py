"""Worker singleton lock tests for the CLI process boundary."""

from __future__ import annotations

import datetime as dt
import os
import signal
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import Engine

import sutradhara.jobs.worker_lock as worker_lock_module
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.jobs.engine import claim_job_by_id, reset_orphaned_running_jobs, submit
from sutradhara.jobs.models import Job, JobStatus
from sutradhara.jobs.worker_lock import (
    exclusive_process_lock,
    held_process_lock_identity,
    worker_lock,
)


def test_worker_lock_denies_second_once_and_loop_process(tmp_path: Path) -> None:
    db_url, engine = _sqlite_db(tmp_path)
    try:
        env = _worker_env(db_url)
        with worker_lock(db_url):
            for args in (["worker", "--once"], ["worker"]):
                result = subprocess.run(
                    [_sutra_bin(), *args],
                    cwd=Path(__file__).resolve().parents[1],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=15,
                    check=False,
                )

                assert result.returncode != 0
                assert "holder pid" in result.stderr
                assert str(os.getpid()) in result.stderr
    finally:
        engine.dispose()


def test_worker_start_cannot_reset_orphans_without_lock(tmp_path: Path) -> None:
    db_url, engine = _sqlite_db(tmp_path)
    try:
        with session_scope(engine) as session:
            job = submit(session, "verify", {"copy_id": 1}, prerequisites=[999_999])
            job.status = JobStatus.RUNNING
            job.started_at = dt.datetime.now(dt.UTC)

        env = _worker_env(db_url)
        with worker_lock(db_url):
            locked = subprocess.run(
                [_sutra_bin(), "worker", "--once"],
                cwd=Path(__file__).resolve().parents[1],
                env=env,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
        assert locked.returncode != 0
        with session_scope(engine) as session:
            row = session.get(Job, job.id)
            assert row is not None
            assert row.status == JobStatus.RUNNING
            assert row.started_at is not None

        unlocked = subprocess.run(
            [_sutra_bin(), "worker", "--once"],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert unlocked.returncode == 0, unlocked.stderr + unlocked.stdout
        with session_scope(engine) as session:
            row = session.get(Job, job.id)
            assert row is not None
            assert row.status == JobStatus.PENDING
            assert row.started_at is None
    finally:
        engine.dispose()


def test_the_holder_probe_never_takes_the_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guards: a reader that answers "is it held?" by holding it.

    ``held_process_lock_identity`` used to take ``LOCK_EX`` when the lock was
    free, so a worker starting inside that window got ``WorkerAlreadyRunning``
    and exited — a spurious singleton conflict caused by the reaper merely
    looking. ``LOCK_SH`` would not have fixed it either: a shared lock still
    refuses the worker's exclusive one.
    """
    lockfile = tmp_path / "probe.worker.lock"

    def _no_flock(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("the holder probe must not touch flock()")

    # Free: the answer is "nobody", derived without acquiring anything.
    lockfile.write_text("stale-host:999999\npid=999999\npurpose=worker\n", encoding="utf-8")
    monkeypatch.setattr(worker_lock_module.fcntl, "flock", _no_flock)
    assert held_process_lock_identity(lockfile) is None
    monkeypatch.undo()

    # Held: the identity comes back, still without acquiring anything.
    with exclusive_process_lock(lockfile, purpose="worker"):
        monkeypatch.setattr(worker_lock_module.fcntl, "flock", _no_flock)
        assert held_process_lock_identity(lockfile) == f"{socket.gethostname()}:{os.getpid()}"
        monkeypatch.undo()

    # And a starting worker is not blocked by a concurrent probe: the probe
    # holds nothing, so the exclusive acquisition that follows it succeeds.
    assert held_process_lock_identity(lockfile) is None
    with exclusive_process_lock(lockfile, purpose="worker"):
        pass


def test_the_holder_probe_falls_back_when_proc_locks_is_unreadable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guards: reading "cannot tell" as "unheld" off a platform without /proc.

    For the reaper "unheld" means "reap", and reaping a live flush is the worst
    outcome available — so an unreadable ``/proc/locks`` falls back to the
    acquire-probe rather than answering None.
    """
    lockfile = tmp_path / "fallback.worker.lock"
    monkeypatch.setattr(worker_lock_module, "_PROC_LOCKS", tmp_path / "no-such-proc-locks")
    with exclusive_process_lock(lockfile, purpose="worker"):
        assert held_process_lock_identity(lockfile) == f"{socket.gethostname()}:{os.getpid()}"
    assert held_process_lock_identity(lockfile) is None


def _sqlite_db(tmp_path: Path) -> tuple[str, Engine]:
    db_url = f"sqlite:///{tmp_path / 'worker-lock.db'}"
    engine = make_engine(db_url)
    create_all(engine)
    return db_url, engine


def _worker_env(db_url: str) -> dict[str, str]:
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = db_url
    env["SUTRADHARA_RESOURCE_CONTROL"] = "0"
    return env


def _sutra_bin() -> str:
    """Hermetic CLI path: the venv's sutra next to the running interpreter (no uv resolve)."""
    import sys

    return str(Path(sys.executable).with_name("sutra"))


def test_worker_lock_records_requested_stop(tmp_path, monkeypatch):
    """GS T1: consume one stop record without changing any identity reader."""
    from sutradhara.jobs.worker_lock import HeldWorkerLock, WorkerAlreadyRunning

    url = f"sqlite:///{tmp_path / 'stop.db'}"
    pid = os.getpid()
    identity = f"{socket.gethostname()}:{pid}"
    expected = f"{identity}\npid={pid}\npurpose=worker\n"
    with worker_lock(url) as first:
        assert isinstance(first, HeldWorkerLock)
        assert not first.previous_holder_stop_requested
        first.record_stop_request()
        assert first.path.read_text() == expected + "stop-requested\n"
        assert held_process_lock_identity(first.path) == identity
        with monkeypatch.context() as patch:
            patch.setattr(worker_lock_module, "_PROC_LOCKS", tmp_path / "missing-proc-locks")
            assert held_process_lock_identity(first.path) == identity
        with pytest.raises(WorkerAlreadyRunning) as refused, worker_lock(url):
            pytest.fail("second holder acquired the worker lock")
        assert refused.value.holder_pid == pid
        assert f"pid={pid}" in first.path.read_text()
    with worker_lock(url) as second:
        assert second.previous_holder_stop_requested
        assert second.path.read_text() == expected
        first.record_stop_request()
        assert second.path.read_text() == expected
    with worker_lock(url) as third:
        assert not third.previous_holder_stop_requested

        def fail_write(*args):
            raise OSError("injected append failure")

        with monkeypatch.context() as patch:
            patch.setattr(worker_lock_module.os, "write", fail_write)
            third.record_stop_request()
        assert third.path.read_text() == expected


def _rba_hold_worker_lock(url, ready, release):
    """Hold the production lock in a distinct process until the test releases it."""
    with worker_lock(url):
        ready.set()
        if not release.wait(15):
            raise RuntimeError("test did not release worker lock")


@pytest.mark.parametrize("by_id", [False, True])
def test_jobs_run_refuses_worker_lock_before_claim(tmp_path, monkeypatch, by_id):
    """T3: both CliRunner routes refuse a separate process's lock immediately."""
    import multiprocessing
    import time

    from click.testing import CliRunner

    from sutradhara.cli.jobs import jobs_group

    url, engine = _sqlite_db(tmp_path)
    monkeypatch.setenv("SUTRADHARA_DB_URL", url)
    with session_scope(engine) as session:
        job_id = submit(session, "verify", {"copy_id": 999}).id
    context = multiprocessing.get_context("fork")
    ready, release = context.Event(), context.Event()
    holder = context.Process(target=_rba_hold_worker_lock, args=(url, ready, release))
    holder.start()
    try:
        assert ready.wait(5)
        started = time.monotonic()
        result = CliRunner().invoke(jobs_group, ["run", *(["--id", str(job_id)] if by_id else [])])
        assert result.exit_code == 2, result.output
        assert time.monotonic() - started < 3
        assert "worker already running" in result.output
        assert ".worker.lock" in result.output
        with session_scope(engine) as session:
            job = session.get(Job, job_id)
            assert job.status == JobStatus.PENDING
            assert job.attempts == 0
    finally:
        release.set()
        holder.join(5)
        if holder.is_alive():
            holder.terminate()
            holder.join(5)
        engine.dispose()
    assert holder.exitcode == 0


@pytest.mark.parametrize("by_id", [False, True])
def test_jobs_run_resets_orphan_under_begin_before_savepoint(tmp_path, monkeypatch, by_id):
    """T3: orphan reset precedes a fresh attempt under BEGIN IMMEDIATE and the lock."""
    from click.testing import CliRunner
    from sqlalchemy import event

    from sutradhara.cli.jobs import jobs_group
    from sutradhara.jobs import registry
    from sutradhara.jobs.registry import JobResult
    from sutradhara.jobs.worker_lock import WorkerAlreadyRunning

    url, engine = _sqlite_db(tmp_path)
    monkeypatch.setenv("SUTRADHARA_DB_URL", url)
    monkeypatch.setattr("sutradhara.cli.jobs.make_engine", lambda _url: engine)
    seen = []

    def handler(ctx):
        assert ctx.job.attempts == 4
        assert ctx.job.status == JobStatus.RUNNING
        assert "orphaned RUNNING" in ctx.job.step_state["engine_observations"][-1]["note"]
        with pytest.raises(WorkerAlreadyRunning), worker_lock(url):
            pytest.fail("CLI released the worker lock before handler execution")
        seen.append(ctx.job.id)
        return JobResult(ok=True)

    monkeypatch.setitem(registry._HANDLERS, "_rba_cli", handler)
    with session_scope(engine) as session:
        job = submit(session, "_rba_cli")
        job.status = JobStatus.RUNNING
        job.attempts = 3
        job_id = job.id
    statements = []

    def trace(conn, cursor, sql, parameters, context, executemany):
        statements.append(sql)

    event.listen(engine, "before_cursor_execute", trace)
    result = CliRunner().invoke(jobs_group, ["run", *(["--id", str(job_id)] if by_id else [])])
    assert result.exit_code == 0, result.output
    assert "ok" in result.output
    assert seen == [job_id]
    assert statements[0] == "BEGIN IMMEDIATE"
    savepoint = next(i for i, sql in enumerate(statements) if sql.startswith("SAVEPOINT"))
    assert any(sql.startswith("UPDATE job") for sql in statements[1:savepoint])
    assert any(sql.startswith("RELEASE SAVEPOINT") for sql in statements[savepoint:])
    with session_scope(engine) as session:
        job = session.get(Job, job_id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.attempts == 4
    engine.dispose()


@pytest.mark.parametrize("case", ["terminal-id", "missing-id", "drain"])
def test_jobs_run_commits_recovery_before_run_refusal(tmp_path, monkeypatch, case):
    """T10: errors in the run transaction cannot undo startup recovery."""
    from click.testing import CliRunner

    from sutradhara.cli.jobs import jobs_group

    url, engine = _sqlite_db(tmp_path)
    monkeypatch.setenv("SUTRADHARA_DB_URL", url)
    monkeypatch.setattr("sutradhara.cli.jobs.make_engine", lambda _url: engine)
    with session_scope(engine) as session:
        job = submit(session, "verify", {"copy_id": 1}, prerequisites=[999_999])
        if case != "missing-id":
            for _ in range(2):
                assert claim_job_by_id(session, job.id) is job
                reset_orphaned_running_jobs(session)
        assert claim_job_by_id(session, job.id) is job
        job_id = job.id
    args = (
        ["--limit", "0"]
        if case == "drain"
        else ["--id", "999" if case == "missing-id" else str(job_id)]
    )
    result = CliRunner().invoke(jobs_group, ["run", *args])
    if case == "drain":
        assert result.exit_code == 0, result.output
        assert "(no pending jobs)" in result.output
    else:
        assert result.exit_code == 2, result.output
        assert (
            "no job with id=999"
            if case == "missing-id"
            else f"job id={job_id} is in terminal status failed; cannot re-run"
        ) in result.output
    with session_scope(engine) as session:
        row = session.get(Job, job_id)
        if case == "missing-id":
            assert row.status == JobStatus.PENDING
            assert row.kills == 1
        else:
            assert row.status == JobStatus.FAILED
            assert row.kills == 3
            assert row.last_error.startswith("killed-repeatedly:")
    engine.dispose()


def test_requested_stop_drains_only_running_attempt(tmp_path, monkeypatch):
    """GS T6: finish A after a stop without admitting its newly eligible B."""
    import threading

    from sutradhara.jobs import registry
    from sutradhara.jobs.config import WorkerConfig
    from sutradhara.jobs.registry import JobResult
    from sutradhara.jobs.worker import JobWorker

    _url, engine = _sqlite_db(tmp_path)
    started, release = threading.Event(), threading.Event()
    outcomes, errors = [], []

    def block(ctx):
        started.set()
        if not release.wait(15):
            return JobResult(ok=False, detail="test did not release attempt")
        return JobResult(ok=True)

    monkeypatch.setitem(registry._HANDLERS, "_gs_thread", block)
    with session_scope(engine) as session:
        a = submit(session, "_gs_thread").id
        b = submit(session, "_gs_thread", prerequisites=[a]).id
    worker = JobWorker(engine, config=WorkerConfig(executor_workers=1))

    def drain():
        try:
            outcomes.extend(worker.drain())
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=drain, daemon=True)
    thread.start()
    try:
        assert started.wait(15), "A did not start"
        assert not worker.stop_requested
        worker.request_stop()
        assert worker.stop_requested
        release.set()
        thread.join(5)
        assert not thread.is_alive(), "drain did not return within 5 s"
        assert errors == []
        assert [outcome.job_id for outcome in outcomes] == [a]
        with session_scope(engine) as session:
            assert session.get(Job, a).status == JobStatus.SUCCEEDED
            assert session.get(Job, b).status == JobStatus.PENDING
            assert session.get(Job, b).attempts == 0
    finally:
        release.set()
        thread.join(15)
        engine.dispose()


@contextmanager
def _gs_child(tmp_path, url, *args, name="worker"):
    """Spawn only our helper and reap that exact child on every exit."""
    output_path = tmp_path / f"{name}.log"
    env = {**_worker_env(url), "PYTHONUNBUFFERED": "1"}
    with output_path.open("w") as output:
        child = subprocess.Popen(
            [sys.executable, "tests/graceful_stop_child.py", *args],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            stdout=output,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            yield child, output_path
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=15)


def _gs_wait_for(predicate, reason, *, timeout=15):
    """Bound marker/output waits without needing a second database writer."""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail(reason)
        time.sleep(0.02)


def _gs_wait_for_stop(lockfile):
    _gs_wait_for(
        lambda: lockfile.exists() and "stop-requested\n" in lockfile.read_text(),
        "no stop line within 5 s",
        timeout=5,
    )


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT], ids=["SIGTERM", "SIGINT"])
def test_first_signal_finishes_attempt_without_claiming_next(tmp_path, signum):
    """GS T7: both first signals preserve A and leave dependent B unclaimed."""
    url, engine = _sqlite_db(tmp_path)
    try:
        with session_scope(engine) as session:
            a = submit(session, "_gs_block", {"directory": str(tmp_path)}).id
            b = submit(session, "_gs_block", {"directory": str(tmp_path)}, prerequisites=[a]).id
        with _gs_child(tmp_path, url) as (child, output_path):
            _gs_wait_for(lambda: (tmp_path / f"started-{a}").exists(), "A did not start")
            child.send_signal(signum)
            time.sleep(1)
            assert child.poll() is None, "worker did not stay alive 1 s after first signal"
            _gs_wait_for_stop(tmp_path / "worker-lock.db.worker.lock")
            (tmp_path / "release").touch()
            assert child.wait(timeout=15) == 0, output_path.read_text()
            assert "worker stopped" in output_path.read_text()
        with session_scope(engine) as session:
            assert session.get(Job, a).status == JobStatus.SUCCEEDED
            assert session.get(Job, b).status == JobStatus.PENDING
            assert session.get(Job, b).attempts == 0
    finally:
        engine.dispose()


@pytest.mark.parametrize("stopped", [True, False], ids=["stopped", "killed"])
def test_cut_off_attempt_recovers_stop_or_kill(tmp_path, stopped):
    """GS T8: the stop record is durable even while the attempt owns SQLite."""
    import sqlite3

    url, engine = _sqlite_db(tmp_path)
    try:
        with session_scope(engine) as session:
            a = submit(session, "_gs_block", {"directory": str(tmp_path)}).id
        with _gs_child(tmp_path, url, name="first") as (child, _output_path):
            _gs_wait_for(lambda: (tmp_path / f"started-{a}").exists(), "A did not start")
            connection = sqlite3.connect(tmp_path / "worker-lock.db", timeout=0)
            try:
                with pytest.raises(sqlite3.OperationalError, match="database is locked"):
                    connection.execute("BEGIN IMMEDIATE")
            finally:
                connection.close()
            if stopped:
                child.send_signal(signal.SIGTERM)
                _gs_wait_for_stop(tmp_path / "worker-lock.db.worker.lock")
            child.kill()
            assert child.wait(timeout=15) == -signal.SIGKILL
        (tmp_path / "release").touch()
        with _gs_child(tmp_path, url, "--once", name="restart") as (child, output_path):
            assert child.wait(timeout=15) == 0, output_path.read_text()
        with session_scope(engine) as session:
            row = session.get(Job, a)
            assert row.status == JobStatus.SUCCEEDED
            if stopped:
                assert row.stops == 1
                assert row.kills == 0
            else:
                assert row.kills == 1
                assert row.stops == 0
            assert row.attempts == 2
    finally:
        engine.dispose()


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT], ids=["SIGTERM", "SIGINT"])
def test_second_signal_ends_worker_immediately(tmp_path, signum):
    """GS T9: a second signal ends the worker without waiting for A's release."""
    url, engine = _sqlite_db(tmp_path)
    try:
        with session_scope(engine) as session:
            a = submit(session, "_gs_block", {"directory": str(tmp_path)}).id
        with _gs_child(tmp_path, url) as (child, _output_path):
            _gs_wait_for(lambda: (tmp_path / f"started-{a}").exists(), "A did not start")
            child.send_signal(signal.SIGTERM)
            _gs_wait_for_stop(tmp_path / "worker-lock.db.worker.lock")
            child.send_signal(signum)
            assert child.wait(timeout=2) == -signum
    finally:
        engine.dispose()


@pytest.mark.parametrize("stopped", [False, True], ids=["after-kill", "after-stop"])
def test_startup_reports_requeued_and_ended_jobs_separately(tmp_path, stopped):
    """GS T10: recovered counts exclude ended jobs, and stops never end them."""
    url, engine = _sqlite_db(tmp_path)
    try:
        with session_scope(engine) as session:
            x = submit(session, "verify", {"copy_id": 1}, prerequisites=[999_999])
            y = submit(session, "verify", {"copy_id": 2}, prerequisites=[999_999])
            for _ in range(2):
                assert claim_job_by_id(session, x.id) is x
                reset_orphaned_running_jobs(session)
            assert x.kills == 2
            assert y.kills == 0
            assert claim_job_by_id(session, x.id) is x
            assert claim_job_by_id(session, y.id) is y
            x_id = x.id
        if stopped:
            with worker_lock(url) as held:
                held.record_stop_request()
        with _gs_child(tmp_path, url) as (child, output_path):
            _gs_wait_for(
                lambda: "orphaned running job(s)" in output_path.read_text(),
                "worker did not print its recovery line",
            )
            child.send_signal(signal.SIGTERM)
            returncode = child.wait(timeout=15)
            output = output_path.read_text()
            if stopped:
                assert (
                    "recovered 2 orphaned running job(s): 0 after a kill, 2 after a requested stop"
                    in output
                )
                assert "ended " not in output
            else:
                assert (
                    "recovered 1 orphaned running job(s): 1 after a kill, 0 after a requested stop"
                    in output
                )
                assert (
                    "ended 1 job(s) killed-repeatedly (found running at a third worker start): "
                    f"{x_id}" in output
                )
            assert returncode == 0, output
    finally:
        engine.dispose()


def test_worker_once_installs_and_restores_signal_handlers(tmp_path, monkeypatch):
    """GS T11: handlers cover the no-argument drain and are restored on exit."""
    from click.testing import CliRunner

    from sutradhara.cli.worker import worker_cmd
    from sutradhara.jobs.worker import JobWorker

    url, engine = _sqlite_db(tmp_path)
    monkeypatch.setenv("SUTRADHARA_DB_URL", url)
    monkeypatch.setenv("SUTRADHARA_RESOURCE_CONTROL", "0")
    monkeypatch.setattr("sutradhara.cli.worker.make_engine", lambda *args, **kwargs: engine)
    signals = (signal.SIGTERM, signal.SIGINT)
    before = {sig: signal.getsignal(sig) for sig in signals}
    during = {}

    def inspect_drain(worker):
        during.update({sig: signal.getsignal(sig) for sig in signals})
        return []

    monkeypatch.setattr(JobWorker, "drain", inspect_drain)
    try:
        result = CliRunner().invoke(worker_cmd, ["--once"])
        assert result.exit_code == 0, result.output
        assert all(during[sig] != before[sig] for sig in signals), "during drain"
        assert {sig: signal.getsignal(sig) for sig in signals} == before, "after drain"
    finally:
        for sig, handler in before.items():
            signal.signal(sig, handler)
        engine.dispose()


def test_worker_unit_sets_stop_scope_and_deadline():
    """GS T12: the shipped unit protects children until the provisional deadline."""
    import configparser

    unit = configparser.ConfigParser(interpolation=None)
    unit.read(Path(__file__).resolve().parents[1] / "systemd/sutradhara-worker.service")
    assert unit["Service"].get("KillMode") == "mixed"
    assert unit["Service"].get("TimeoutStopSec") == "90s"


def test_jobs_run_consumes_previous_holder_stop(tmp_path, monkeypatch):
    """GS T13: the synchronous runner forwards the same lock-file stop fact."""
    from click.testing import CliRunner

    from sutradhara.cli.jobs import jobs_group

    url, engine = _sqlite_db(tmp_path)
    monkeypatch.setenv("SUTRADHARA_DB_URL", url)
    monkeypatch.setattr("sutradhara.cli.jobs.make_engine", lambda _url: engine)
    try:
        with session_scope(engine) as session:
            job = submit(session, "verify", {"copy_id": 1}, prerequisites=[999_999])
            assert claim_job_by_id(session, job.id) is job
            job_id = job.id
        with worker_lock(url) as held:
            held.record_stop_request()
        result = CliRunner().invoke(jobs_group, ["run", "--limit", "0"])
        assert result.exit_code == 0, result.output
        with session_scope(engine) as session:
            row = session.get(Job, job_id)
            assert row.status == JobStatus.PENDING
            assert row.stops == 1
            assert row.kills == 0
    finally:
        engine.dispose()
