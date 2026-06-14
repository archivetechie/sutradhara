"""Schema wiring tests for runtime bootstrap and Alembic.

These tests run schema creation in a fresh Python process so they catch missing
model imports that can be hidden by pytest's already-imported modules.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path


def _tables(db_path: Path) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        return {
            row[0]
            for row in conn.execute(
                "select name from sqlite_master where type='table'"
            )
        }


def test_create_all_creates_job_table_without_prior_job_import(tmp_path: Path) -> None:
    db_path = tmp_path / "create_all.db"
    code = f"""
from sutradhara.catalog.session import create_all, make_engine

engine = make_engine("sqlite:///{db_path.as_posix()}")
create_all(engine)
engine.dispose()
"""
    subprocess.run([sys.executable, "-c", code], check=True)

    tables = _tables(db_path)
    assert "job" in tables
    assert "pool" in tables
    assert "artifactclass_pool" in tables
    assert "bundle" in tables
    assert "bundle_member" in tables
    assert "asset_locator" in tables
    assert "blob_root" in tables
    assert "exclusion_record" in tables
    assert "placement_tag_pin" not in tables


def test_alembic_upgrade_head_creates_job_table(tmp_path: Path) -> None:
    db_path = tmp_path / "alembic.db"
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = f"sqlite:///{db_path.as_posix()}"

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )

    tables = _tables(db_path)
    assert "job" in tables
    assert "pool" in tables
    assert "artifactclass_pool" in tables
    assert "bundle" in tables
    assert "bundle_member" in tables
    assert "asset_locator" in tables
    assert "blob_root" in tables
    assert "exclusion_record" in tables
    assert "placement_tag_pin" not in tables
