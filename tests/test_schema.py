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
        return {row[0] for row in conn.execute("select name from sqlite_master where type='table'")}


def _unique_index_columns(db_path: Path, table: str) -> set[tuple[str, ...]]:
    with sqlite3.connect(db_path) as conn:
        result: set[tuple[str, ...]] = set()
        for row in conn.execute(f"PRAGMA index_list({table})"):
            index_name = row[1]
            unique = bool(row[2])
            if not unique:
                continue
            columns = tuple(col[2] for col in conn.execute(f"PRAGMA index_info({index_name})"))
            result.add(columns)
        return result


def _table_sql(db_path: Path, table: str) -> str:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "select sql from sqlite_master where type='table' and name=?",
            (table,),
        ).fetchone()
    assert row is not None
    return str(row[0])


def _assert_archive_invariants(db_path: Path) -> None:
    assert (
        "copy_id",
        "logical_asset_hash",
        "member_path",
    ) in _unique_index_columns(db_path, "asset_locator")
    assert "ck_copy_asset_xor_bundle" in _table_sql(db_path, "copy")
    assert (
        "bundle_member_id",
        "step_order",
    ) in _unique_index_columns(db_path, "staging_transform")
    assert (
        "bundle_id",
        "stored_member_path",
        "step_order",
    ) in _unique_index_columns(db_path, "staging_transform")
    assert "staging_config" in _table_sql(db_path, "artifactclass_policy")


def _assert_worker_lease_invariants(db_path: Path) -> None:
    assert "not_before" in _table_sql(db_path, "job")
    assert "priority" in _table_sql(db_path, "job")
    assert "dedupe_key" in _table_sql(db_path, "job")
    assert ("dedupe_key",) in _unique_index_columns(db_path, "job")
    assert "validity" in _table_sql(db_path, "logical_asset")
    assert "validity_note" in _table_sql(db_path, "logical_asset")
    assert "ck_logical_asset_validity" in _table_sql(db_path, "logical_asset")


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
    assert "artifactclass_policy" in tables
    assert "bundle" in tables
    assert "bundle_member" in tables
    assert "asset_locator" in tables
    assert "blob_root" in tables
    assert "exclusion_record" in tables
    assert "review_decision" in tables
    assert "staging_transform" in tables
    assert "placement_tag_pin" not in tables
    _assert_archive_invariants(db_path)
    _assert_worker_lease_invariants(db_path)


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
    assert "artifactclass_policy" in tables
    assert "bundle" in tables
    assert "bundle_member" in tables
    assert "asset_locator" in tables
    assert "blob_root" in tables
    assert "exclusion_record" in tables
    assert "review_decision" in tables
    assert "staging_transform" in tables
    assert "placement_tag_pin" not in tables
    _assert_archive_invariants(db_path)
    _assert_worker_lease_invariants(db_path)


def test_alembic_archive_migration_round_trips(tmp_path: Path) -> None:
    db_path = tmp_path / "roundtrip.db"
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = f"sqlite:///{db_path.as_posix()}"

    for target in ("head", "base", "head"):
        subprocess.run(
            [
                sys.executable,
                "-m",
                "alembic",
                "upgrade" if target == "head" else "downgrade",
                target,
            ],
            cwd=repo_root,
            env=env,
            check=True,
        )

    tables = _tables(db_path)
    assert "asset_locator" in tables
    assert "copy" in tables
    _assert_archive_invariants(db_path)
    _assert_worker_lease_invariants(db_path)
