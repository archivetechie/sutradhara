"""Worker singleton lock tests for the CLI process boundary."""

from __future__ import annotations

import datetime as dt
import os
import subprocess
from pathlib import Path

from sqlalchemy import Engine

from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.jobs.engine import submit
from sutradhara.jobs.models import Job, JobStatus
from sutradhara.jobs.worker_lock import worker_lock


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
