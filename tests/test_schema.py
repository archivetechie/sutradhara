"""Schema wiring tests for runtime bootstrap and Alembic.

These tests run schema creation in a fresh Python process so they catch missing
model imports that can be hidden by pytest's already-imported modules.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest


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


def _trigger_sql(db_path: Path, trigger_name: str) -> str | None:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "select sql from sqlite_master where type='trigger' and name=?",
            (trigger_name,),
        ).fetchone()
    return None if row is None else str(row[0])


def _foreign_key_delete_actions(db_path: Path, table: str) -> dict[tuple[str, str], str]:
    with sqlite3.connect(db_path) as conn:
        return {
            (str(row[3]), str(row[2])): str(row[6])
            for row in conn.execute(f"PRAGMA foreign_key_list({table})")
        }


def _schema_signatures(db_path: Path) -> dict[str, dict[str, tuple[tuple[object, ...], ...]]]:
    """Return exhaustive SQLite column, foreign-key, and index signatures."""

    with sqlite3.connect(db_path) as conn:
        tables = sorted(
            name
            for (name,) in conn.execute(
                "select name from sqlite_master where type='table' and name not like 'sqlite_%'"
            )
            if name != "alembic_version"
        )
        result: dict[str, dict[str, tuple[tuple[object, ...], ...]]] = {}
        for table in tables:
            columns = tuple(
                sorted(
                    (str(row[1]), str(row[2]).upper(), bool(row[3]), int(row[5]))
                    for row in conn.execute(f"PRAGMA table_info({table})")
                )
            )
            foreign_key_rows = list(conn.execute(f"PRAGMA foreign_key_list({table})"))
            foreign_keys: dict[int, list[tuple[int, str, str]]] = {}
            foreign_key_meta: dict[int, tuple[str, str, str, str]] = {}
            for row in foreign_key_rows:
                key = int(row[0])
                foreign_keys.setdefault(key, []).append((int(row[1]), str(row[3]), str(row[4])))
                foreign_key_meta[key] = (str(row[2]), str(row[5]), str(row[6]), str(row[7]))
            foreign_key_signature = tuple(
                sorted(
                    (*foreign_key_meta[key], tuple(sorted(parts)))
                    for key, parts in foreign_keys.items()
                )
            )
            indexes = []
            for row in conn.execute(f"PRAGMA index_list({table})"):
                index_name = str(row[1])
                index_columns = tuple(
                    str(part[2]) for part in conn.execute(f"PRAGMA index_info({index_name})")
                )
                indexes.append((bool(row[2]), str(row[3]), bool(row[4]), index_columns))
            result[table] = {
                "columns": columns,
                "foreign_keys": foreign_key_signature,
                "indexes": tuple(sorted(indexes)),
            }
        return result


def _assert_archive_invariants(db_path: Path) -> None:
    backend_sql = _table_sql(db_path, "backend")
    pool_sql = _table_sql(db_path, "pool")
    policy_sql = _table_sql(db_path, "artifactclass_policy")
    assert "implementation_family" in backend_sql
    assert "implementation_family VARCHAR(64) NOT NULL" in backend_sql
    assert "accepts_writes" in pool_sql
    assert "retired" in pool_sql
    assert "media_generation" in pool_sql
    assert "min_copies" in policy_sql
    assert "min_impl_families" in policy_sql
    assert "member_path VARCHAR(2048)" in _table_sql(db_path, "bundle_member")
    assert "member_path VARCHAR(2048)" in _table_sql(db_path, "asset_locator")
    # The flush claim identity. Asserted in the shared invariant block so the
    # migration and create_all() are checked to agree: a column present only
    # in the model would leave every migrated deployment's claim CAS matching
    # nothing, and every flush would fail its close.
    assert "claimed_by VARCHAR(255)" in _table_sql(db_path, "bundle")
    assert (
        "copy_id",
        "logical_asset_hash",
        "member_path",
    ) in _unique_index_columns(db_path, "asset_locator")
    assert (
        "copy_id",
        "root_path",
    ) in _unique_index_columns(db_path, "blob_root")
    assert "ck_copy_asset_xor_bundle" in _table_sql(db_path, "copy")
    assert ("backend_id", "native_locator_key") in _unique_index_columns(db_path, "copy")
    assert _foreign_key_delete_actions(db_path, "asset_locator")[("pool_id", "pool")] == "RESTRICT"
    assert _foreign_key_delete_actions(db_path, "blob_root")[("pool_id", "pool")] == "RESTRICT"
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
    assert "hdcache_config" in _table_sql(db_path, "artifactclass_policy")


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
    assert "condition_component" in _tables(db_path)
    component_sql = _table_sql(db_path, "condition_component")
    assert "condition_id" in component_sql
    assert "component VARCHAR(2048) NOT NULL" in component_sql
    assert "ON DELETE CASCADE" in component_sql
    component_indexes = _index_columns(db_path, "condition_component")
    assert ("component",) in component_indexes
    assert ("condition_id", "component") in _unique_index_columns(db_path, "condition_component")


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
    assert ("intake_id", "logical_asset_hash") in _index_columns(db_path, "ingest_item")
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


def _assert_retention_invariants(db_path: Path) -> None:
    intake_sql = _table_sql(db_path, "intake")
    assert "retention_state" in intake_sql
    assert "DEFAULT 'held'" in intake_sql
    assert "released_at" in intake_sql
    assert "staging_deleted_at" in intake_sql
    assert "release_policy_fingerprint" in intake_sql
    assert "staging_tombstoned_at" in intake_sql
    assert "staging_tombstone_path" in intake_sql
    assert "ck_intake_retention_state" in intake_sql
    assert "ck_intake_tombstone_pair" in intake_sql
    assert "ck_intake_tombstoned_state" in intake_sql
    assert "ck_intake_purged_state" in intake_sql
    copy_sql = _table_sql(db_path, "copy")
    assert "deleted_at" in copy_sql
    assert "last_checked_at" in copy_sql
    assert "last_verified_at" not in copy_sql
    assert "last_measured_digest" in copy_sql
    assert "last_measured_at" in copy_sql
    assert "health_changed_at" in copy_sql
    assert "integrity_hash_provenance" in copy_sql
    assert "ck_copy_health" in copy_sql
    assert "ck_copy_measurement_pair" in copy_sql
    assert _trigger_sql(db_path, "trg_copy_health_changed_at") is not None
    assert "offsite_confirmation" in _tables(db_path)
    offsite_sql = _table_sql(db_path, "offsite_confirmation")
    assert "media_id" in offsite_sql
    assert "revoked_at" in offsite_sql
    assert "revoked_by" in offsite_sql
    assert "ck_offsite_confirmation_revocation_pair" in offsite_sql
    assert "retention_event" in _tables(db_path)
    event_sql = _table_sql(db_path, "retention_event")
    assert "ck_retention_event_action" in event_sql
    assert "ck_retention_event_subject" in event_sql
    assert "event_id" in event_sql
    assert "subject_type" in event_sql
    assert "subject_id" in event_sql
    assert "operation_id" in event_sql
    assert "operation_id VARCHAR(512) NOT NULL" in event_sql
    assert "detail" in event_sql
    assert ("intake_id",) in _index_columns(db_path, "retention_event")
    assert _foreign_key_delete_actions(db_path, "retention_event")[("intake_id", "intake")] == (
        "RESTRICT"
    )
    assert "verify_receipt" in _tables(db_path)
    receipt_sql = _table_sql(db_path, "verify_receipt")
    assert "ck_verify_receipt_source" in receipt_sql
    assert "ck_verify_receipt_scrub_unmeasured" in receipt_sql
    assert ("source", "execution_id", "copy_id") in _unique_index_columns(db_path, "verify_receipt")


def _assert_grpc_relay_invariants(db_path: Path) -> None:
    grpc_intake_sql = _table_sql(db_path, "grpc_intake")
    assert "card_id" in grpc_intake_sql
    token_sql = _table_sql(db_path, "grpc_enroll_token")
    assert "operator" in token_sql
    assert "device_id" in token_sql
    assert "rotation_authority" in token_sql
    assert "rotation_fingerprint" in token_sql


def _assert_hdcache_invariants(db_path: Path) -> None:
    tables = _tables(db_path)
    assert "cache_disk" in tables
    assert "cache_entry" in tables
    assert "restore_request" in tables
    assert "restore_request_item" in tables

    disk_sql = _table_sql(db_path, "cache_disk")
    assert "ck_cache_disk_state" in disk_sql
    assert "serial VARCHAR(256) NOT NULL" in disk_sql
    assert ("serial",) in _unique_index_columns(db_path, "cache_disk")
    assert ("state",) in _index_columns(db_path, "cache_disk")

    entry_sql = _table_sql(db_path, "cache_entry")
    assert "ck_cache_entry_state" in entry_sql
    assert "ck_cache_entry_representation" in entry_sql
    assert "content_sha256 BLOB" in entry_sql
    assert "FOREIGN KEY(content_sha256) REFERENCES logical_asset" in entry_sql
    assert "FOREIGN KEY(disk_id) REFERENCES cache_disk" in entry_sql
    assert "lost_origin_disk_id" in entry_sql
    assert "lost_drill_id" in entry_sql
    assert "lost_at" in entry_sql
    assert "refilled_at" in entry_sql
    entry_indexes = _index_columns(db_path, "cache_entry")
    assert ("bundle_key",) in entry_indexes
    assert ("group_key",) in entry_indexes
    assert ("disk_id",) in entry_indexes
    assert ("state",) in entry_indexes

    request_sql = _table_sql(db_path, "restore_request")
    assert "ck_restore_request_state" in request_sql
    request_indexes = _index_columns(db_path, "restore_request")
    assert ("created_at",) in request_indexes
    assert ("state",) in request_indexes

    item_sql = _table_sql(db_path, "restore_request_item")
    assert "ck_restore_request_item_state" in item_sql
    assert "fell_back_to_tape" in item_sql
    assert "admitted_by" in request_sql
    assert "admitted_at" in request_sql
    assert "admitted_capabilities" in request_sql
    assert "admitted_force_suspect" in item_sql
    assert "admitted_force_rejected" in item_sql
    assert "ON DELETE CASCADE" in item_sql
    item_indexes = _index_columns(db_path, "restore_request_item")
    assert ("request_id",) in item_indexes
    assert ("content_sha256",) in item_indexes
    assert ("state",) in item_indexes


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
    assert "offsite_confirmation" in tables
    assert "retention_event" in tables
    assert "cache_disk" in tables
    assert "cache_entry" in tables
    assert "restore_request" in tables
    assert "restore_request_item" in tables
    assert "placement_tag_pin" not in tables
    _assert_archive_invariants(db_path)
    _assert_worker_lease_invariants(db_path)
    _assert_intake_invariants(db_path)
    _assert_arrangement_invariants(db_path)
    _assert_virtual_arrangement_invariants(db_path)
    _assert_retention_invariants(db_path)
    _assert_grpc_relay_invariants(db_path)
    _assert_hdcache_invariants(db_path)


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
    assert "offsite_confirmation" in tables
    assert "retention_event" in tables
    assert "cache_disk" in tables
    assert "cache_entry" in tables
    assert "restore_request" in tables
    assert "restore_request_item" in tables
    assert "placement_tag_pin" not in tables
    _assert_archive_invariants(db_path)
    _assert_worker_lease_invariants(db_path)
    _assert_intake_invariants(db_path)
    _assert_arrangement_invariants(db_path)
    _assert_virtual_arrangement_invariants(db_path)
    _assert_retention_invariants(db_path)
    _assert_grpc_relay_invariants(db_path)
    _assert_hdcache_invariants(db_path)


def test_create_all_and_migration_head_have_schema_shape_parity(tmp_path: Path) -> None:
    """Every modeled table, column, foreign key, and index must also exist after Alembic."""

    create_all_path = tmp_path / "create-all-parity.db"
    migration_path = tmp_path / "migration-parity.db"
    repo_root = Path(__file__).resolve().parents[1]
    code = f"""
from sutradhara.catalog.session import create_all, make_engine

engine = make_engine("sqlite:///{create_all_path.as_posix()}")
create_all(engine)
engine.dispose()
"""
    subprocess.run([sys.executable, "-c", code], check=True)
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = f"sqlite:///{migration_path.as_posix()}"
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )

    assert _schema_signatures(migration_path) == _schema_signatures(create_all_path)


def test_deletion_evidence_migration_recreates_raw_sql_health_trigger(
    tmp_path: Path,
) -> None:
    """The copy-table batch rebuild must leave raw SQL health flips timestamped."""

    import datetime as dt
    import hashlib

    from sutradhara.catalog.models import Backend, Copy, LogicalAsset
    from sutradhara.catalog.session import locator_key, make_engine, session_scope
    from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource

    db_path = tmp_path / "deletion-evidence-trigger.db"
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = f"sqlite:///{db_path.as_posix()}"
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )

    digest = hashlib.sha256(b"raw-sql-health-trigger").digest()
    locator = {"object": "raw-sql-health-trigger"}
    engine = make_engine(env["SUTRADHARA_DB_URL"])
    try:
        with session_scope(engine) as session:
            backend = Backend(
                name="trigger-memory",
                kind=BackendKind.MEMORY,
                tier=BackendTier.SELF_DESCRIBING,
            )
            session.add_all([backend, LogicalAsset(content_sha256=digest, size_bytes=22)])
            session.flush()
            copy = Copy(
                logical_asset_hash=digest,
                backend_id=backend.id,
                native_locator=locator,
                native_locator_key=locator_key(locator),
                integrity_hash=digest,
                health=CopyHealth.OK,
                health_changed_at=dt.datetime(2000, 1, 1, tzinfo=dt.UTC),
                source=CopySource.INGEST,
            )
            session.add(copy)
            session.flush()
            copy_id = copy.id
    finally:
        engine.dispose()

    with sqlite3.connect(db_path) as conn:
        before = conn.execute(
            "SELECT health_changed_at FROM copy WHERE id=?", (copy_id,)
        ).fetchone()
        conn.execute("UPDATE copy SET health='suspect' WHERE id=?", (copy_id,))
        conn.commit()
        after = conn.execute(
            "SELECT health, health_changed_at FROM copy WHERE id=?", (copy_id,)
        ).fetchone()
    assert before is not None
    assert after is not None
    assert after[0] == "suspect"
    assert after[1] != before[0]
    assert _trigger_sql(db_path, "trg_copy_health_changed_at") is not None

    subprocess.run(
        [sys.executable, "-m", "alembic", "downgrade", "c8d2e4f6a1b3"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(copy)")}
    assert "last_verified_at" in columns
    assert "last_checked_at" not in columns
    assert _trigger_sql(db_path, "trg_copy_health_changed_at") is None


def test_retention_event_subject_constraint_rejects_raw_sql_mispairing(
    tmp_path: Path,
) -> None:
    """Every raw-SQL subject class enforces its intake-FK pairing contract."""

    db_path = tmp_path / "retention-event-subjects.db"
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = f"sqlite:///{db_path.as_posix()}"
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    now = "2026-07-19 00:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO intake "
            "(intake_id, operator, source_kind, artifactclass, status, created_at, "
            "updated_at, retention_state) VALUES "
            "('subject-intake', 'ops', 'card', 'masters', 'registered', ?, ?, 'held')",
            (now, now),
        )
        valid_rows = (
            (
                "subject-intake",
                "intake",
                "subject-intake",
                "staging_purge_held",
                "subject:intake",
            ),
            (None, "media", "tape:abc", "offsite_confirmed", "subject:media"),
            (None, "batch", "batch:1", "batch_invoked", "subject:batch"),
        )
        for intake_id, subject_type, subject_id, action, operation_id in valid_rows:
            conn.execute(
                "INSERT INTO retention_event "
                "(intake_id, subject_type, subject_id, action, operation_id, actor, at) "
                "VALUES (?, ?, ?, ?, ?, 'ops', ?)",
                (intake_id, subject_type, subject_id, action, operation_id, now),
            )
        conn.commit()

        invalid_rows = (
            (None, "intake", "subject-intake", "staging_purge_held", "bad:intake"),
            (
                "subject-intake",
                "media",
                "tape:abc",
                "offsite_confirmed",
                "bad:media",
            ),
            (
                "subject-intake",
                "batch",
                "batch:1",
                "batch_invoked",
                "bad:batch",
            ),
        )
        for row in invalid_rows:
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
                conn.execute(
                    "INSERT INTO retention_event "
                    "(intake_id, subject_type, subject_id, action, operation_id, actor, at) "
                    "VALUES (?, ?, ?, ?, ?, 'ops', ?)",
                    (*row, now),
                )
            conn.rollback()

        assert conn.execute("SELECT count(*) FROM retention_event").fetchone() == (3,)


def test_deletion_evidence_downgrade_transforms_new_intake_states(
    tmp_path: Path,
) -> None:
    """TOMBSTONED/ABANDONED rows survive a clean head/down/up cycle."""

    db_path = tmp_path / "deletion-evidence-state-roundtrip.db"
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = f"sqlite:///{db_path.as_posix()}"
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    now = "2026-07-19 00:00:00"
    with sqlite3.connect(db_path) as conn:
        base = (
            "INSERT INTO intake "
            "(intake_id, operator, source_kind, artifactclass, status, created_at, "
            "updated_at, retention_state, staging_tombstoned_at, staging_tombstone_path) "
            "VALUES (?, 'ops', 'card', 'masters', 'registered', ?, ?, ?, ?, ?)"
        )
        conn.execute(
            base,
            ("state-tombstoned", now, now, "tombstoned", now, "/tmp/tombstone"),
        )
        conn.execute(
            base,
            ("state-abandoned", now, now, "abandoned", None, None),
        )
        conn.commit()

    subprocess.run(
        [sys.executable, "-m", "alembic", "downgrade", "c8d2e4f6a1b3"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    with sqlite3.connect(db_path) as conn:
        states = dict(conn.execute("SELECT intake_id, retention_state FROM intake"))
        deleted_at = conn.execute(
            "SELECT staging_deleted_at FROM intake WHERE intake_id='state-tombstoned'"
        ).fetchone()
    assert states == {"state-tombstoned": "purged", "state-abandoned": "held"}
    assert deleted_at is not None
    assert deleted_at[0] is not None

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    with sqlite3.connect(db_path) as conn:
        assert dict(conn.execute("SELECT intake_id, retention_state FROM intake")) == states


def test_deletion_evidence_downgrade_exports_and_transforms_used_database(
    tmp_path: Path,
) -> None:
    """A fully used evidence schema exports incompatibilities and round-trips."""

    db_path = tmp_path / "deletion-evidence-used.db"
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = f"sqlite:///{db_path.as_posix()}"
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    now = "2026-07-19 12:00:00"
    released_at = "2026-06-01 08:30:00"
    intake_actions = (
        "release_attempted",
        "purge_attempted",
        "staging_tombstoned",
        "staging_purge_held",
        "abandoned",
        "correction_recorded",
    )
    batch_actions = ("batch_invoked", "batch_refused", "grace_overridden")
    digest = bytes.fromhex("ab" * 32)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO intake "
            "(intake_id, operator, source_kind, artifactclass, status, created_at, "
            "updated_at, retention_state, released_at, staging_tombstoned_at, "
            "staging_tombstone_path) VALUES "
            "('used-tombstoned', 'ops', 'card', 'masters', 'registered', ?, ?, "
            "'tombstoned', ?, ?, '/tmp/used-tombstone')",
            (now, now, released_at, now),
        )
        conn.execute(
            "INSERT INTO intake "
            "(intake_id, operator, source_kind, artifactclass, status, created_at, "
            "updated_at, retention_state, released_at) VALUES "
            "('used-abandoned', 'ops', 'card', 'masters', 'registered', ?, ?, "
            "'abandoned', ?)",
            (now, now, released_at),
        )
        for action in intake_actions:
            intake_id = "used-abandoned" if action == "abandoned" else "used-tombstoned"
            conn.execute(
                "INSERT INTO retention_event "
                "(intake_id, subject_type, subject_id, action, operation_id, actor, at) "
                "VALUES (?, 'intake', ?, ?, ?, 'ops', ?)",
                (intake_id, intake_id, action, f"used:{action}", now),
            )
        # A COMPLETED purge records staging_tombstoned and staging_deleted
        # under one operation id — the realistic pair whose downgrade mapping
        # would collide with the once-only unique index unless the pair
        # collapses to the single legacy row.
        conn.execute(
            "INSERT INTO retention_event "
            "(intake_id, subject_type, subject_id, action, operation_id, actor, at) "
            "VALUES ('used-tombstoned', 'intake', 'used-tombstoned', "
            "'staging_deleted', 'used:staging_tombstoned', 'ops', ?)",
            (now,),
        )
        for action in batch_actions:
            conn.execute(
                "INSERT INTO retention_event "
                "(subject_type, subject_id, action, operation_id, actor, at) "
                "VALUES ('batch', 'batch:used', ?, ?, 'ops', ?)",
                (action, f"used:{action}", now),
            )
        conn.execute(
            "INSERT INTO retention_event "
            "(subject_type, subject_id, action, operation_id, actor, at) VALUES "
            "('media', 'tape:used', 'offsite_confirmed', 'used:offsite_confirmed', "
            "'ops', ?)",
            (now,),
        )
        conn.execute(
            "INSERT INTO backend "
            "(id, name, kind, implementation_family, config, tier, added_at) VALUES "
            "(1, 'used-memory', 'memory', 'memory', '{}', 'self_describing', ?) ",
            (now,),
        )
        conn.execute(
            "INSERT INTO logical_asset "
            "(content_sha256, size_bytes, first_seen_at, validity) VALUES (?, 1, ?, "
            "'unvalidated')",
            (digest, now),
        )
        conn.execute(
            "INSERT INTO copy "
            "(id, logical_asset_hash, backend_id, native_locator, native_locator_key, "
            "integrity_hash, health, last_checked_at, first_observed_at, source, "
            "storage_metadata, last_measured_digest, last_measured_at) VALUES "
            "(1, ?, 1, '{\"object\":\"used\"}', 'used-object', ?, 'ok', ?, ?, "
            "'ingest', '{}', ?, ?)",
            (digest, digest, now, now, digest, now),
        )
        conn.execute(
            "INSERT INTO verify_receipt "
            "(copy_id, backend_id, expected_digest, measured_digest, backend_ok, source, "
            "execution_id, producer_process, actor, at) VALUES "
            "(1, 1, ?, ?, 1, 'restore', 'used-restore-request', 'used-host:1', 'ops', ?)",
            (digest, digest, now),
        )
        conn.commit()

    result = subprocess.run(
        [sys.executable, "-m", "alembic", "downgrade", "c8d2e4f6a1b3"],
        cwd=repo_root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    marker = "deletion-evidence downgrade export: "
    export_line = next(line for line in output.splitlines() if marker in line)
    sidecar = Path(export_line.split(marker, 1)[1].strip())
    assert sidecar.exists()
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    assert payload["format"] == "sutradhara-deletion-evidence-downgrade-v1"
    assert {row["action"] for row in payload["retention_events"]} == {
        *intake_actions,
        *batch_actions,
        "offsite_confirmed",
    }
    assert {row["subject_type"] for row in payload["retention_events"]} == {
        "intake",
        "batch",
        "media",
    }
    assert len(payload["verify_receipts"]) == 1
    [receipt] = payload["verify_receipts"]
    assert receipt["execution_id"] == "used-restore-request"
    assert receipt["expected_digest"] == {"encoding": "hex", "value": digest.hex()}

    with sqlite3.connect(db_path) as conn:
        assert list(
            conn.execute("SELECT intake_id, action, at FROM retention_event ORDER BY id")
        ) == [("used-tombstoned", "staging_deleted", now)]
        states = dict(conn.execute("SELECT intake_id, retention_state FROM intake"))
        timestamps = dict(conn.execute("SELECT intake_id, released_at FROM intake"))
        tombstoned_deleted_at = conn.execute(
            "SELECT staging_deleted_at FROM intake WHERE intake_id='used-tombstoned'"
        ).fetchone()
    assert states == {"used-tombstoned": "purged", "used-abandoned": "held"}
    assert timestamps == {
        "used-tombstoned": released_at,
        "used-abandoned": released_at,
    }
    assert tombstoned_deleted_at == (now,)

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    with sqlite3.connect(db_path) as conn:
        assert dict(conn.execute("SELECT intake_id, retention_state FROM intake")) == states
        assert conn.execute(
            "SELECT subject_type, subject_id, action FROM retention_event"
        ).fetchone() == ("intake", "used-tombstoned", "staging_deleted")


def test_retention_journal_downgrade_exports_and_removes_used_receipt_correction(
    tmp_path: Path,
) -> None:
    """A used verify-receipt correction survives downgrade in the sidecar only."""

    db_path = tmp_path / "retention-journal-used-correction.db"
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = f"sqlite:///{db_path.as_posix()}"
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    now = "2026-07-20 12:00:00"
    digest = bytes.fromhex("cd" * 32)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO backend "
            "(id, name, kind, implementation_family, config, tier, added_at) VALUES "
            "(1, 'correction-memory', 'memory', 'memory', '{}', 'self_describing', ?)",
            (now,),
        )
        conn.execute(
            "INSERT INTO logical_asset "
            "(content_sha256, size_bytes, first_seen_at, validity) VALUES (?, 1, ?, "
            "'unvalidated')",
            (digest, now),
        )
        conn.execute(
            "INSERT INTO copy "
            "(id, logical_asset_hash, backend_id, native_locator, native_locator_key, "
            "integrity_hash, health, first_observed_at, source, storage_metadata) VALUES "
            "(1, ?, 1, '{\"object\":\"correction\"}', 'correction-object', ?, 'ok', ?, "
            "'ingest', '{}')",
            (digest, digest, now),
        )
        conn.execute(
            "INSERT INTO verify_receipt "
            "(event_id, copy_id, backend_id, expected_digest, measured_digest, backend_ok, "
            "source, execution_id, producer_process, actor, at) VALUES "
            "(1, 1, 1, ?, ?, 1, 'verify-job', 'used-correction-verify', 'host:1', "
            "'ops', ?)",
            (digest, digest, now),
        )
        conn.execute(
            "INSERT INTO retention_event "
            "(subject_type, subject_id, action, operation_id, actor, at, detail, "
            "supersedes_source, supersedes_event_id) VALUES "
            "('receipt', 'verify_receipt:1', 'correction_recorded', "
            "'journal-correction:verify_receipt:1:used', 'ops', ?, "
            '\'{"kind":"receipt-supersession","reason":"wrong read"}\', '
            "'verify_receipt', 1)",
            (now,),
        )
        conn.commit()

    result = subprocess.run(
        [sys.executable, "-m", "alembic", "downgrade", "e1f2a3b4c5d6"],
        cwd=repo_root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    marker = "retention-journal downgrade export: "
    export_line = next(line for line in output.splitlines() if marker in line)
    sidecar = Path(export_line.split(marker, 1)[1].strip())
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    assert payload["format"] == "sutradhara-retention-journal-downgrade-v1"
    assert payload["retention_events"] == [
        {
            "action": "correction_recorded",
            "actor": "ops",
            "at": now,
            "detail": '{"kind":"receipt-supersession","reason":"wrong read"}',
            "event_id": 1,
            "intake_id": None,
            "operation_id": "journal-correction:verify_receipt:1:used",
            "subject_id": "verify_receipt:1",
            "subject_type": "receipt",
            "supersedes_event_id": 1,
            "supersedes_source": "verify_receipt",
        }
    ]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT count(*) FROM retention_event").fetchone() == (0,)
        assert conn.execute("SELECT count(*) FROM verify_receipt").fetchone() == (1,)

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT count(*) FROM retention_event").fetchone() == (0,)
        assert conn.execute("SELECT count(*) FROM verify_receipt").fetchone() == (1,)


def test_retention_journal_upgrade_backfills_confirmation_receipts_and_targets(
    tmp_path: Path,
) -> None:
    """Legacy active and revoked confirmations remain safely revocable after upgrade."""

    from sutradhara.catalog.session import make_engine, session_scope
    from sutradhara.retention import revoke_offsite

    db_path = tmp_path / "retention-journal-confirmation-backfill.db"
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = f"sqlite:///{db_path.as_posix()}"
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "e1f2a3b4c5d6"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    confirmed_at = "2026-07-19 09:00:00"
    revoked_at = "2026-07-19 10:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO offsite_confirmation "
            "(media_id, confirmed_at, confirmed_by, shipment_id) VALUES "
            "('legacy:active', ?, 'active-operator', 'shipment-active')",
            (confirmed_at,),
        )
        conn.execute(
            "INSERT INTO offsite_confirmation "
            "(media_id, confirmed_at, confirmed_by, shipment_id, revoked_at, revoked_by) "
            "VALUES ('legacy:revoked', ?, 'confirming-operator', 'shipment-revoked', ?, "
            "'revoking-operator')",
            (confirmed_at, revoked_at),
        )
        conn.execute(
            "INSERT INTO retention_event "
            "(subject_type, subject_id, action, operation_id, actor, at, detail) VALUES "
            "('media', 'legacy:revoked', 'correction_recorded', "
            "'offsite-revoke:legacy:revoked:prompt-1', 'revoking-operator', ?, "
            '\'{"kind":"offsite-revocation","reason":"wrong shipment"}\')',
            (revoked_at,),
        )
        conn.commit()

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )

    def assert_backfill() -> dict[str, int]:
        with sqlite3.connect(db_path) as conn:
            receipt_rows = list(
                conn.execute(
                    "SELECT event_id, subject_id, operation_id FROM retention_event "
                    "WHERE action='offsite_confirmed' ORDER BY subject_id"
                )
            )
            receipts = {subject_id: event_id for event_id, subject_id, _ in receipt_rows}
            corrections = {
                subject_id: (source, event_id)
                for subject_id, source, event_id in conn.execute(
                    "SELECT subject_id, supersedes_source, supersedes_event_id "
                    "FROM retention_event WHERE action='correction_recorded' "
                    "ORDER BY subject_id"
                )
            }
        assert receipt_rows == [
            (
                receipts["legacy:active"],
                "legacy:active",
                "migration:f2a3b4c5d6e7:offsite-confirm:legacy:active",
            ),
            (
                receipts["legacy:revoked"],
                "legacy:revoked",
                "migration:f2a3b4c5d6e7:offsite-confirm:legacy:revoked",
            ),
        ]
        assert corrections == {
            "legacy:active": ("retention_event", receipts["legacy:active"]),
            "legacy:revoked": ("retention_event", receipts["legacy:revoked"]),
        }
        return receipts

    with sqlite3.connect(db_path) as conn:
        revoked_receipt_id = conn.execute(
            "SELECT event_id FROM retention_event WHERE subject_id='legacy:revoked' "
            "AND action='offsite_confirmed'"
        ).fetchone()
        revoked_target = conn.execute(
            "SELECT supersedes_source, supersedes_event_id FROM retention_event "
            "WHERE subject_id='legacy:revoked' AND action='correction_recorded'"
        ).fetchone()
    assert revoked_receipt_id is not None
    assert revoked_target == ("retention_event", revoked_receipt_id[0])

    engine = make_engine(f"sqlite:///{db_path.as_posix()}")
    try:
        with session_scope(engine) as session:
            _row, changed = revoke_offsite(
                session,
                media_id="legacy:active",
                actor="new-revoker",
                reason="confirmation was mistaken",
            )
            assert changed
    finally:
        engine.dispose()
    initial_receipts = assert_backfill()

    subprocess.run(
        [sys.executable, "-m", "alembic", "downgrade", "e1f2a3b4c5d6"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    assert assert_backfill() == initial_receipts


def test_retention_event_payload_validation_is_per_action() -> None:
    from sutradhara.retention import _validate_event_detail

    _validate_event_detail("released", {"copy_ids": []})

    with pytest.raises(ValueError, match=r"missing=.*copy_ids"):
        _validate_event_detail("released", {})
    with pytest.raises(ValueError, match=r"unknown=.*typo"):
        _validate_event_detail("released", {"copy_ids": [], "typo": True})
    with pytest.raises(ValueError, match="outcome must be"):
        _validate_event_detail(
            "cloud_blob_deleted",
            {
                "bundle_id": "cloud-blob:intake-1",
                "copy_ids": [],
                "outcome": "maybe",
                "copy_outcomes": [],
            },
        )


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
    assert "offsite_confirmation" in tables
    assert "retention_event" in tables
    assert "cache_disk" in tables
    assert "cache_entry" in tables
    assert "restore_request" in tables
    assert "restore_request_item" in tables
    _assert_archive_invariants(db_path)
    _assert_worker_lease_invariants(db_path)
    _assert_intake_invariants(db_path)
    _assert_arrangement_invariants(db_path)
    _assert_virtual_arrangement_invariants(db_path)
    _assert_retention_invariants(db_path)
    _assert_grpc_relay_invariants(db_path)
    _assert_hdcache_invariants(db_path)


def test_restore_agent_foundation_migration_backfills_constraints_and_downgrades(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "restore-agent-backfill.db"
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = f"sqlite:///{db_path.as_posix()}"

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "e5f6a7b8c9d0"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    now = "2026-01-01 00:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO grpc_device_enrollment "
            "(device_id, cert_fingerprint, operator, revoked, created_at, revoked_at) "
            "VALUES ('existing-device', ?, 'ada', 0, ?, NULL)",
            ("AA:" * 31 + "AA", now),
        )
        conn.execute(
            "INSERT INTO grpc_enroll_token "
            "(token, created_at, expires_at, used_at, operator, device_id, "
            "rotation_authority, rotation_fingerprint) "
            "VALUES ('existing-token', ?, ?, NULL, 'ada', 'pending-device', NULL, NULL)",
            (now, "2027-01-01 00:00:00"),
        )
        conn.execute(
            "INSERT INTO restore_request "
            "(id, identity, created_at, destination_id, state, admitted_by, admitted_at, "
            "admitted_capabilities, idempotency_key, idempotency_body_hash) "
            "VALUES ('existing-restore', 'ada', ?, 'media-server', 'pending', NULL, NULL, "
            "NULL, NULL, NULL)",
            (now,),
        )
        conn.commit()

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        assert conn.execute(
            "SELECT scopes FROM grpc_logical_device WHERE device_id='existing-device'"
        ).fetchone() == ('["ingest"]',)
        assert conn.execute(
            "SELECT scopes FROM grpc_enroll_token WHERE token='existing-token'"
        ).fetchone() == ('["ingest"]',)
        assert conn.execute(
            "SELECT delivery_mode, receiver_device_id FROM restore_request "
            "WHERE id='existing-restore'"
        ).fetchone() == ("server_local", None)
        delivery_column = next(
            row
            for row in conn.execute("PRAGMA table_info(restore_request)")
            if row[1] == "delivery_mode"
        )
        assert delivery_column[4] == "'server_local'"
        tables = _tables(db_path)
        assert {
            "grpc_logical_device",
            "grpc_device_destination_grant",
            "restore_item_checkpoint",
            "restore_open_session",
            "operator_capability_sync",
            "operator_live_capability",
        }.issubset(tables)
        checkpoint_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='restore_item_checkpoint'"
        ).fetchone()[0]
        lease_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='restore_open_session'"
        ).fetchone()[0]
        assert "committed_index >= 0" in checkpoint_sql
        assert "revealed = false OR committed_index >= 1" in checkpoint_sql
        assert "length(manifest_sha256) = 32" in checkpoint_sql
        assert "generation >= 1" in lease_sql
        assert "length(manifest_sha256) = 32" in lease_sql
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO grpc_device_enrollment "
                "(device_id, cert_fingerprint, operator, revoked, created_at, revoked_at) "
                "VALUES ('orphan', ?, 'ada', 0, ?, NULL)",
                ("BB:" * 31 + "BB", now),
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO restore_item_checkpoint "
                "(restore_request_item_id, manifest_sha256, committed_index, revealed, updated_at) "
                "VALUES (999, ?, -1, 0, ?)",
                (bytes(32), now),
            )

    subprocess.run(
        [sys.executable, "-m", "alembic", "downgrade", "e5f6a7b8c9d0"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    with sqlite3.connect(db_path) as conn:
        assert "grpc_logical_device" not in _tables(db_path)
        assert "scopes" not in {
            row[1] for row in conn.execute("PRAGMA table_info(grpc_enroll_token)")
        }
        assert "delivery_mode" not in {
            row[1] for row in conn.execute("PRAGMA table_info(restore_request)")
        }


def test_receive_dedup_migration_preserves_dead_intent_heartbeat_and_downgrades(
    tmp_path: Path,
) -> None:
    """Phase 1a must not make an old streaming receive look newly alive."""

    db_path = tmp_path / "receive-dedup-backfill.db"
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = f"sqlite:///{db_path.as_posix()}"

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "c9a0d1e2f3b4"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    old_timestamp = "2025-01-02 03:04:05"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO grpc_intake "
            "(intake_id, operator, device_id, state, manifest_digest, idempotency_key, "
            "source_plan_digest, artifactclass, source_kind, source_ref, label, landing_root, "
            "created_at, updated_at, card_id) "
            "VALUES (?, 'ada', 'mac-1', 'streaming', NULL, 'dead-key', ?, 's-masters', "
            "'card', 'DCIM', 'Dead Card', '/tmp/landing', ?, ?, 'card-dead')",
            ("dead-intake", "a" * 64, old_timestamp, old_timestamp),
        )
        conn.execute(
            "INSERT INTO idempotency_record "
            "(operator_username, endpoint, idempotency_key, request_hash, status, intake_id, "
            "response_json, created_at, updated_at, last_heartbeat) "
            "VALUES ('ada', 'POST /api/devices/receive', 'dead-key', ?, 'in_progress', "
            "'dead-intake', NULL, ?, ?, ?)",
            ("b" * 64, old_timestamp, old_timestamp, old_timestamp),
        )
        conn.commit()

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "d4e5f6a7b8c9"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    with sqlite3.connect(db_path) as conn:
        migrated = conn.execute(
            "SELECT status, updated_at, last_heartbeat, card_identity "
            "FROM idempotency_record WHERE idempotency_key='dead-key'"
        ).fetchone()
    assert migrated == ("started", old_timestamp, old_timestamp, "card-dead")

    subprocess.run(
        [sys.executable, "-m", "alembic", "downgrade", "c9a0d1e2f3b4"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    with sqlite3.connect(db_path) as conn:
        downgraded = conn.execute(
            "SELECT status, updated_at, last_heartbeat "
            "FROM idempotency_record WHERE idempotency_key='dead-key'"
        ).fetchone()
        columns = {row[1] for row in conn.execute("PRAGMA table_info(idempotency_record)")}
    assert downgraded == ("in_progress", old_timestamp, old_timestamp)
    assert "card_identity" not in columns


def test_copygrain_m3_migration_backfills_and_preserves_constraints(tmp_path: Path) -> None:
    db_path = tmp_path / "m3-backfill.db"
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = f"sqlite:///{db_path.as_posix()}"

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "7c2d4e9f0a1b"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    with sqlite3.connect(db_path) as conn:
        for index, kind in enumerate(
            (
                "rem_tape",
                "d2_tape",
                "rem_disk",
                "plain_disk",
                "ssh_disk",
                "s3",
                "gcs",
                "azure_blob",
                "memory",
            ),
            start=1,
        ):
            conn.execute(
                "INSERT INTO backend (id, name, kind, config, tier, added_at) "
                "VALUES (?, ?, ?, NULL, 'self_describing', '2026-01-01 00:00:00')",
                (index, f"backend-{kind}", kind),
            )
        conn.execute(
            "INSERT INTO artifactclass_policy "
            "(artifactclass, ruleset, expect, target_bytes, max_age_seconds, "
            "restore_preference, staging_config, hdcache_config, policy_source, "
            "policy_sha256, updated_at) "
            "VALUES ('existing', 'rules', 'messy', 1, 60, '[]', '{}', '{}', NULL, NULL, "
            "'2026-01-01 00:00:00')"
        )
        conn.commit()

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )

    with sqlite3.connect(db_path) as conn:
        families = dict(conn.execute("SELECT kind, implementation_family FROM backend"))
        assert families == {
            "rem_tape": "tape",
            "d2_tape": "d2tape",
            "rem_disk": "disk",
            "plain_disk": "disk",
            "ssh_disk": "disk",
            "s3": "cloud",
            "gcs": "cloud",
            "azure_blob": "cloud",
            "memory": "memory",
        }
        assert conn.execute(
            "SELECT min_copies, min_impl_families FROM artifactclass_policy "
            "WHERE artifactclass='existing'"
        ).fetchone() == (3, 2)
    _assert_archive_invariants(db_path)


def test_retention_journal_backfill_two_cycle_history_targets_by_cycle(
    tmp_path: Path,
) -> None:
    """An early-cycle correction must never supersede a later re-confirmation.

    History under prompt 1: legacy confirmation (no receipt) -> correction E1
    (its revocation) -> re-confirmation receipt E2 -> correction E3.  The
    upgrade backfill must synthesize a leading receipt for the legacy
    confirmation and target E1 at it, while E3 targets E2 exactly.
    """

    db_path = tmp_path / "retention-journal-two-cycle.db"
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = f"sqlite:///{db_path.as_posix()}"
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "e1f2a3b4c5d6"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    cycle1_revoked_at = "2026-07-18 09:30:00"
    cycle2_confirmed_at = "2026-07-19 11:00:00"
    cycle2_revoked_at = "2026-07-19 12:00:00"
    with sqlite3.connect(db_path) as conn:
        # The confirmation row holds the CURRENT (cycle-2) state.
        conn.execute(
            "INSERT INTO offsite_confirmation "
            "(media_id, confirmed_at, confirmed_by, shipment_id, revoked_at, revoked_by) "
            "VALUES ('legacy:two-cycle', ?, 'cycle2-operator', 'shipment-2', ?, "
            "'cycle2-revoker')",
            (cycle2_confirmed_at, cycle2_revoked_at),
        )
        # E1: cycle-1 revocation correction (no receipt existed yet).
        conn.execute(
            "INSERT INTO retention_event "
            "(subject_type, subject_id, action, operation_id, actor, at, detail) VALUES "
            "('media', 'legacy:two-cycle', 'correction_recorded', "
            "'offsite-revoke:two-cycle:c1', 'cycle1-revoker', ?, "
            '\'{"kind":"offsite-revocation","reason":"wrong tape"}\')',
            (cycle1_revoked_at,),
        )
        # E2: cycle-2 re-confirmation receipt.
        conn.execute(
            "INSERT INTO retention_event "
            "(subject_type, subject_id, action, operation_id, actor, at, detail) VALUES "
            "('media', 'legacy:two-cycle', 'offsite_confirmed', "
            "'offsite-confirm:two-cycle:c2', 'cycle2-operator', ?, "
            '\'{"shipment_id":"shipment-2","confirmed_by":"cycle2-operator"}\')',
            (cycle2_confirmed_at,),
        )
        # E3: cycle-2 revocation correction.
        conn.execute(
            "INSERT INTO retention_event "
            "(subject_type, subject_id, action, operation_id, actor, at, detail) VALUES "
            "('media', 'legacy:two-cycle', 'correction_recorded', "
            "'offsite-revoke:two-cycle:c2', 'cycle2-revoker', ?, "
            '\'{"kind":"offsite-revocation","reason":"failed audit"}\')',
            (cycle2_revoked_at,),
        )
        conn.commit()

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        check=True,
    )
    with sqlite3.connect(db_path) as conn:
        receipts = dict(
            conn.execute(
                "SELECT operation_id, event_id FROM retention_event "
                "WHERE action='offsite_confirmed' AND subject_id='legacy:two-cycle'"
            )
        )
        corrections = dict(
            conn.execute(
                "SELECT operation_id, supersedes_event_id FROM retention_event "
                "WHERE action='correction_recorded' AND subject_id='legacy:two-cycle'"
            )
        )
    lead_op = "migration:f2a3b4c5d6e7:offsite-confirm:legacy:two-cycle:legacy-lead"
    assert set(receipts) == {lead_op, "offsite-confirm:two-cycle:c2"}
    # Each correction targets its OWN cycle's receipt, by correction identity.
    assert corrections["offsite-revoke:two-cycle:c1"] == receipts[lead_op]
    assert corrections["offsite-revoke:two-cycle:c2"] == (receipts["offsite-confirm:two-cycle:c2"])
