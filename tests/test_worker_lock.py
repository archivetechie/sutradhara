"""Worker singleton lock tests for the CLI process boundary."""

from __future__ import annotations

import datetime as dt
import os
import socket
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import Engine

import sutradhara.jobs.worker_lock as worker_lock_module
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.jobs.engine import submit
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
