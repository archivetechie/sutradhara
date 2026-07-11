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


def _foreign_key_delete_actions(db_path: Path, table: str) -> dict[tuple[str, str], str]:
    with sqlite3.connect(db_path) as conn:
        return {
            (str(row[3]), str(row[2])): str(row[6])
            for row in conn.execute(f"PRAGMA foreign_key_list({table})")
        }


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
    assert "ck_intake_retention_state" in intake_sql
    assert "deleted_at" in _table_sql(db_path, "copy")
    assert "offsite_confirmation" in _tables(db_path)
    assert "media_id" in _table_sql(db_path, "offsite_confirmation")
    assert "retention_event" in _tables(db_path)
    event_sql = _table_sql(db_path, "retention_event")
    assert "ck_retention_event_action" in event_sql
    assert "detail" in event_sql
    assert ("intake_id",) in _index_columns(db_path, "retention_event")


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
