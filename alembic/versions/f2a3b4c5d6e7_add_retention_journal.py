"""Add retention-journal checkpoint and supersession targets.

Revision ID: f2a3b4c5d6e7
Revises: e1f2a3b4c5d6
Create Date: 2026-07-20
"""

from __future__ import annotations

import datetime as dt
import json
import os
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from alembic import op

revision: str = "f2a3b4c5d6e7"
down_revision: str | Sequence[str] | None = "e1f2a3b4c5d6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Install the emit-only retention-journal catalog state."""

    with op.batch_alter_table("retention_event") as batch:
        batch.drop_constraint("ck_retention_event_subject", type_="check")
        batch.add_column(sa.Column("supersedes_source", sa.String(32), nullable=True))
        batch.add_column(sa.Column("supersedes_event_id", sa.Integer(), nullable=True))
        batch.create_check_constraint(
            "ck_retention_event_subject",
            "(subject_type = 'intake' AND intake_id IS NOT NULL AND "
            "intake_id = subject_id AND action IN "
            "('released', 'cloud_blob_deleted', 'staging_deleted', 'release_attempted', "
            "'purge_attempted', 'staging_tombstoned', 'staging_purge_held', 'abandoned', "
            "'correction_recorded')) OR "
            "(subject_type = 'media' AND intake_id IS NULL AND action IN "
            "('offsite_confirmed', 'correction_recorded')) OR "
            "(subject_type = 'batch' AND intake_id IS NULL AND action IN "
            "('batch_invoked', 'batch_refused', 'grace_overridden')) OR "
            "(subject_type = 'receipt' AND intake_id IS NULL AND "
            "action = 'correction_recorded')",
        )
        batch.create_check_constraint(
            "ck_retention_event_supersession",
            "(supersedes_source IS NULL AND supersedes_event_id IS NULL) OR "
            "(supersedes_source IN ('verify_receipt', 'retention_event') AND "
            "supersedes_event_id IS NOT NULL AND action = 'correction_recorded')",
        )

    _backfill_offsite_confirmation_receipts_and_targets()

    op.create_table(
        "retention_journal_checkpoint",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("envelope_id", sa.String(128), nullable=False),
        sa.Column("hash_algorithm_id", sa.String(32), nullable=False),
        sa.Column("global_sequence", sa.Integer(), nullable=False),
        sa.Column("head_hash", sa.LargeBinary(32), nullable=False),
        sa.Column("verify_receipt_cursor", sa.Integer(), nullable=False),
        sa.Column("retention_event_cursor", sa.Integer(), nullable=False),
        sa.Column("published_filename", sa.String(512), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("id = 1", name="ck_retention_journal_checkpoint_singleton"),
        sa.CheckConstraint("global_sequence >= 0", name="ck_retention_journal_sequence"),
        sa.CheckConstraint(
            "verify_receipt_cursor >= 0 AND retention_event_cursor >= 0",
            name="ck_retention_journal_cursors",
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Export incompatible journal evidence, then remove prompt-2 state."""

    _export_incompatible_journal_evidence()
    op.execute("DELETE FROM retention_event WHERE subject_type = 'receipt'")
    op.drop_table("retention_journal_checkpoint")
    with op.batch_alter_table("retention_event") as batch:
        batch.drop_constraint("ck_retention_event_supersession", type_="check")
        batch.drop_constraint("ck_retention_event_subject", type_="check")
        batch.create_check_constraint(
            "ck_retention_event_subject",
            "(subject_type = 'intake' AND intake_id IS NOT NULL AND "
            "intake_id = subject_id AND action IN "
            "('released', 'cloud_blob_deleted', 'staging_deleted', 'release_attempted', "
            "'purge_attempted', 'staging_tombstoned', 'staging_purge_held', 'abandoned', "
            "'correction_recorded')) OR "
            "(subject_type = 'media' AND intake_id IS NULL AND action IN "
            "('offsite_confirmed', 'correction_recorded')) OR "
            "(subject_type = 'batch' AND intake_id IS NULL AND action IN "
            "('batch_invoked', 'batch_refused', 'grace_overridden'))",
        )
        batch.drop_column("supersedes_event_id")
        batch.drop_column("supersedes_source")


def _backfill_offsite_confirmation_receipts_and_targets() -> None:
    """Make legacy confirmations revocable through targeted append-only corrections."""

    bind = op.get_bind()
    confirmations = list(
        bind.execute(
            sa.text(
                "SELECT media_id, confirmed_at, confirmed_by, shipment_id "
                "FROM offsite_confirmation ORDER BY media_id"
            )
        ).mappings()
    )
    event_table = sa.table(
        "retention_event",
        sa.column("subject_type", sa.String()),
        sa.column("subject_id", sa.String()),
        sa.column("action", sa.String()),
        sa.column("operation_id", sa.String()),
        sa.column("actor", sa.String()),
        sa.column("at", sa.DateTime(timezone=True)),
        sa.column("detail", sa.JSON()),
    )
    for confirmation in confirmations:
        media_id = str(confirmation["media_id"])
        receipt_id = bind.execute(
            sa.text(
                "SELECT event_id FROM retention_event "
                "WHERE subject_type = 'media' AND subject_id = :media_id "
                "AND action = 'offsite_confirmed' ORDER BY event_id DESC LIMIT 1"
            ),
            {"media_id": media_id},
        ).scalar_one_or_none()
        if receipt_id is None:
            operation_id = f"migration:{revision}:offsite-confirm:{media_id}"
            bind.execute(
                event_table.insert().values(
                    subject_type="media",
                    subject_id=media_id,
                    action="offsite_confirmed",
                    operation_id=operation_id,
                    actor=confirmation["confirmed_by"],
                    at=_datetime_value(confirmation["confirmed_at"]),
                    detail={
                        "shipment_id": confirmation["shipment_id"],
                        "confirmed_by": confirmation["confirmed_by"],
                    },
                )
            )
            receipt_id = bind.execute(
                sa.text(
                    "SELECT event_id FROM retention_event "
                    "WHERE action = 'offsite_confirmed' AND operation_id = :operation_id"
                ),
                {"operation_id": operation_id},
            ).scalar_one()

        corrections = bind.execute(
            sa.text(
                "SELECT event_id FROM retention_event "
                "WHERE subject_type = 'media' AND subject_id = :media_id "
                "AND action = 'correction_recorded' AND supersedes_source IS NULL "
                "AND supersedes_event_id IS NULL ORDER BY event_id"
            ),
            {"media_id": media_id},
        ).scalars()
        for correction_id in list(corrections):
            preceding_receipt_id = bind.execute(
                sa.text(
                    "SELECT event_id FROM retention_event "
                    "WHERE subject_type = 'media' AND subject_id = :media_id "
                    "AND action = 'offsite_confirmed' AND event_id < :correction_id "
                    "ORDER BY event_id DESC LIMIT 1"
                ),
                {"media_id": media_id, "correction_id": correction_id},
            ).scalar_one_or_none()
            bind.execute(
                sa.text(
                    "UPDATE retention_event SET supersedes_source = 'retention_event', "
                    "supersedes_event_id = :receipt_id WHERE event_id = :correction_id"
                ),
                {
                    "receipt_id": (
                        preceding_receipt_id
                        if preceding_receipt_id is not None
                        else receipt_id
                    ),
                    "correction_id": correction_id,
                },
            )


def _export_incompatible_journal_evidence() -> Path | None:
    """Sidecar targeted rows before prompt-2-only identities are transformed away."""

    bind = op.get_bind()
    events = list(
        bind.execute(
            sa.text(
                "SELECT * FROM retention_event WHERE subject_type = 'receipt' "
                "OR supersedes_source IS NOT NULL OR supersedes_event_id IS NOT NULL "
                "ORDER BY event_id"
            )
        ).mappings()
    )
    if not events:
        return None

    sidecar = _downgrade_sidecar_path()
    temporary = sidecar.with_name(f".{sidecar.name}.tmp")
    payload = {
        "format": "sutradhara-retention-journal-downgrade-v1",
        "revision": revision,
        "exported_at": dt.datetime.now(dt.UTC).isoformat(),
        "retention_events": [_json_row(row) for row in events],
    }
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, sidecar)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    print(f"retention-journal downgrade export: {sidecar}", flush=True)
    return sidecar


def _downgrade_sidecar_path() -> Path:
    """Choose a unique sidecar beside the SQLite catalog when possible."""

    database = op.get_bind().engine.url.database
    base = (
        Path(database).expanduser().resolve()
        if database and database != ":memory:"
        else (Path.cwd() / "sutradhara.db").resolve()
    )
    return base.with_name(f"{base.name}.retention-journal-downgrade-{uuid.uuid4().hex}.json")


def _json_row(row: Any) -> dict[str, Any]:
    """Render one SQLAlchemy mapping with durable encodings."""

    return {str(key): _json_value(value) for key, value in row.items()}


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"encoding": "hex", "value": value.hex()}
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    return value


def _datetime_value(value: Any) -> dt.datetime:
    """Normalize raw SQLite timestamps for SQLAlchemy's typed insert."""

    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, str):
        return dt.datetime.fromisoformat(value)
    raise TypeError(f"unsupported confirmation timestamp: {value!r}")
