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


def _index_columns(db_path: Path, table: str) -> set[tuple[str, ...]]:
    with sqlite3.connect(db_path) as conn:
        result: set[tuple[str, ...]] = set()
        for row in conn.execute(f"PRAGMA index_list({table})"):
            index_name = row[1]
            columns = tuple(col[2] for col in conn.execute(f"PRAGMA index_info({index_name})"))
            result.add(columns)
        return result


def _index_sql(db_path: Path, index_name: str) -> str:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "select sql from sqlite_master where type='index' and name=?",
            (index_name,),
        ).fetchone()
    assert row is not None
    return str(row[0])


def _table_sql(db_path: Path, table: str) -> str:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "select sql from sqlite_master where type='table' and name=?",
            (table,),
        ).fetchone()
    assert row is not None
    return str(row[0])


def _assert_archive_invariants(db_path: Path) -> None:
    assert "member_path VARCHAR(2048)" in _table_sql(db_path, "bundle_member")
    assert "member_path VARCHAR(2048)" in _table_sql(db_path, "asset_locator")
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
    index_sql = _index_sql(db_path, "uq_job_dedupe_key_live")
    assert "UNIQUE INDEX" in index_sql
    assert "WHERE status IN ('pending', 'running', 'queued')" in index_sql
    assert "uq_job_dedupe_key" not in _table_sql(db_path, "job")
    assert "validity" in _table_sql(db_path, "logical_asset")
    assert "validity_note" in _table_sql(db_path, "logical_asset")
    assert "ck_logical_asset_validity" in _table_sql(db_path, "logical_asset")
    assert "job_attempt" in _tables(db_path)
    attempt_sql = _table_sql(db_path, "job_attempt")
    assert "job_kind" in attempt_sql
    assert "attempt_number" in attempt_sql
    assert "outcome" in attempt_sql
    assert "granted_leases" in attempt_sql
    assert "code_version" in attempt_sql
    assert "DEFAULT '{}'" in attempt_sql
    assert "ON DELETE SET NULL" in attempt_sql
    attempt_indexes = _index_columns(db_path, "job_attempt")
    assert ("job_id",) in attempt_indexes
    assert ("job_kind",) in attempt_indexes


def _assert_intake_invariants(db_path: Path) -> None:
    intake_sql = _table_sql(db_path, "intake")
    assert "ck_intake_source_kind" in intake_sql
    assert "ck_intake_status" in intake_sql
    assert "manifest_digest" in intake_sql
    assert "requested_profile" in intake_sql
    assert (
        "intake_id",
        "as_received_path",
    ) in _unique_index_columns(db_path, "ingest_item")
    assert (
        "derived_item_id",
        "source_item_id",
        "kind",
    ) in _unique_index_columns(db_path, "asset_derivation")
    assert "metadata" in _table_sql(db_path, "ingest_item")


def _assert_arrangement_invariants(db_path: Path) -> None:
    arrangement_sql = _table_sql(db_path, "arrangement")
    assert "ck_arrangement_status" in arrangement_sql
    assert "fk_arrangement_submission_id" in arrangement_sql
    assert "submission_id" in arrangement_sql
    member_index_sql = _index_sql(db_path, "uq_arrangement_member_path_active")
    assert "UNIQUE INDEX" in member_index_sql
    assert "arrangement_id" in member_index_sql
    assert "member_path" in member_index_sql
    assert "WHERE" in member_index_sql
    assert "excluded" in member_index_sql

    submission_sql = _table_sql(db_path, "submission")
    assert "ck_submission_status" in submission_sql
    assert "uq_submission_arrangement_id" in submission_sql
    assert ("arrangement_id",) in _unique_index_columns(db_path, "submission")
    assert (
        "submission_id",
        "archive_path",
    ) in _unique_index_columns(db_path, "submission_member")
    submission_member_indexes = _index_columns(db_path, "submission_member")
    assert ("source_path",) in submission_member_indexes
    assert ("submission_id",) in submission_member_indexes


def _assert_virtual_arrangement_invariants(db_path: Path) -> None:
    logical_asset_sql = _table_sql(db_path, "logical_asset")
    assert "rejected_at" in logical_asset_sql
    assert "rejected_by" in logical_asset_sql
    assert "rejection_reason" in logical_asset_sql
    assert "virtual_arrangement" in _tables(db_path)
    member_sql = _table_sql(db_path, "virtual_arrangement_member")
    assert "artifactclass" in member_sql
    assert "path VARCHAR(2048)" in member_sql
    assert (
        "va_id",
        "logical_asset_hash",
        "artifactclass",
    ) in _unique_index_columns(db_path, "virtual_arrangement_member")
    active_path_sql = _index_sql(db_path, "uq_virtual_arrangement_member_path_active")
    assert "UNIQUE INDEX" in active_path_sql
    assert "va_id" in active_path_sql
    assert "path" in active_path_sql
    assert "WHERE" in active_path_sql
    assert "excluded" in active_path_sql

    history_sql = _table_sql(db_path, "virtual_arrangement_history")
    assert "va_member_id" in history_sql
    assert "logical_asset_hash" in history_sql
    assert "artifactclass" in history_sql
    assert "ON DELETE SET NULL" in history_sql

    tag_sql = _table_sql(db_path, "asset_tag")
    assert "removed_at" in tag_sql
    tag_index_sql = _index_sql(db_path, "uq_asset_tag_active")
    assert "UNIQUE INDEX" in tag_index_sql
    assert "logical_asset_hash" in tag_index_sql
    assert "tag" in tag_index_sql
    assert "removed_at IS NULL" in tag_index_sql


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
    assert "intake" in tables
    assert "ingest_item" in tables
    assert "asset_derivation" in tables
    assert "arrangement" in tables
    assert "arrangement_member" in tables
    assert "submission" in tables
    assert "submission_member" in tables
    assert "virtual_arrangement" in tables
    assert "virtual_arrangement_member" in tables
    assert "virtual_arrangement_history" in tables
    assert "asset_tag" in tables
    assert "placement_tag_pin" not in tables
    _assert_archive_invariants(db_path)
    _assert_worker_lease_invariants(db_path)
    _assert_intake_invariants(db_path)
    _assert_arrangement_invariants(db_path)
    _assert_virtual_arrangement_invariants(db_path)


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
    assert "intake" in tables
    assert "ingest_item" in tables
    assert "asset_derivation" in tables
    assert "arrangement" in tables
    assert "arrangement_member" in tables
    assert "submission" in tables
    assert "submission_member" in tables
    assert "virtual_arrangement" in tables
    assert "virtual_arrangement_member" in tables
    assert "virtual_arrangement_history" in tables
    assert "asset_tag" in tables
    assert "placement_tag_pin" not in tables
    _assert_archive_invariants(db_path)
    _assert_worker_lease_invariants(db_path)
    _assert_intake_invariants(db_path)
    _assert_arrangement_invariants(db_path)
    _assert_virtual_arrangement_invariants(db_path)


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
    assert "intake" in tables
    assert "ingest_item" in tables
    assert "asset_derivation" in tables
    assert "arrangement" in tables
    assert "arrangement_member" in tables
    assert "submission" in tables
    assert "submission_member" in tables
    assert "virtual_arrangement" in tables
    assert "virtual_arrangement_member" in tables
    assert "virtual_arrangement_history" in tables
    assert "asset_tag" in tables
    _assert_archive_invariants(db_path)
    _assert_worker_lease_invariants(db_path)
    _assert_intake_invariants(db_path)
    _assert_arrangement_invariants(db_path)
    _assert_virtual_arrangement_invariants(db_path)
