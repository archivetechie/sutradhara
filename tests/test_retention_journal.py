"""Hermetic retention-journal chain, recovery, shipping, and alarm tests."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from sqlalchemy import Engine, func, select

from sutradhara.backend.port import VerifyResult
from sutradhara.catalog.models import (
    Backend,
    Copy,
    LogicalAsset,
    OffsiteConfirmation,
    RetentionEvent,
    RetentionJournalCheckpoint,
)
from sutradhara.catalog.session import (
    create_all,
    locator_key,
    make_engine,
    make_session_factory,
    session_scope,
)
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource, content_hash
from sutradhara.cli.main import cli
from sutradhara.evidence_recorder import record_measured
from sutradhara.jobs.models import ReconciliationCondition
from sutradhara.jobs.worker_lock import process_lockfile_for
from sutradhara.retention import confirm_offsite
from sutradhara.retention_journal import (
    ALARM_DOMAIN,
    GENESIS_HASH,
    JournalError,
    JournalExportAlreadyRunning,
    LocalAppendOnlyDestination,
    SshDiskJournalDestination,
    check_journal,
    export_journal,
    journal_operational_status,
    record_journal_correction,
    refresh_staleness_alarm,
)


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    value = make_engine(f"sqlite:///{tmp_path / 'journal.db'}")
    create_all(value)
    yield value
    value.dispose()


def test_chain_sequence_cross_file_link_and_interleaved_source_resume(
    engine: Engine, tmp_path: Path
) -> None:
    journal_dir = tmp_path / "journal"
    dr_dir = tmp_path / "dr"
    destination = LocalAppendOnlyDestination(dr_dir)
    copy_id = _seed_copy(engine)
    for ordinal in (1, 2, 3):
        _append_verify_receipt(engine, copy_id, ordinal=ordinal, at=_time(1))
    _append_retention_event(engine, ordinal=1, at=_time(1))

    first = export_journal(
        engine,
        journal_dir=journal_dir,
        destination=destination,
        now=_time(2),
    )
    assert first.published
    assert first.entry_count == 4
    assert first.state.global_sequence == 4
    assert first.state.verify_receipt_cursor == 3
    assert first.state.retention_event_cursor == 1
    first_lines = _json_lines(first.segment)
    assert _entry_identities(first.segment) == [
        ("verify_receipt", 1),
        ("verify_receipt", 2),
        ("verify_receipt", 3),
        ("retention_event", 1),
    ]
    assert first_lines[0]["sequence"] == 1
    assert first_lines[0]["prev_hash"] == GENESIS_HASH
    assert first_lines[-1]["cursors"] == {
        "verify_receipt": 3,
        "retention_event": 1,
    }

    for ordinal in (4, 5):
        _append_verify_receipt(engine, copy_id, ordinal=ordinal, at=_time(3))
    for ordinal in (2, 3, 4):
        _append_retention_event(engine, ordinal=ordinal, at=_time(3))
    second = export_journal(
        engine,
        journal_dir=journal_dir,
        destination=destination,
        now=_time(4),
    )
    assert second.published
    assert second.entry_count == 5
    assert second.state.global_sequence == 9
    second_lines = _json_lines(second.segment)
    assert _entry_identities(second.segment) == [
        ("verify_receipt", 4),
        ("verify_receipt", 5),
        ("retention_event", 2),
        ("retention_event", 3),
        ("retention_event", 4),
    ]
    assert second_lines[0]["sequence"] == 5
    assert second_lines[0]["prev_hash"] == first.state.head_hash
    assert second.state.verify_receipt_cursor == 5
    assert second.state.retention_event_cursor == 4
    assert second_lines[-1]["cursors"] == {
        "verify_receipt": 5,
        "retention_event": 4,
    }
    assert [
        identity
        for segment in sorted(journal_dir.glob("????-??-??/*.jsonl"))
        for identity in _entry_identities(segment)
    ] == [
        ("verify_receipt", 1),
        ("verify_receipt", 2),
        ("verify_receipt", 3),
        ("retention_event", 1),
        ("verify_receipt", 4),
        ("verify_receipt", 5),
        ("retention_event", 2),
        ("retention_event", 3),
        ("retention_event", 4),
    ]

    checked = check_journal(engine, journal_dir=journal_dir, destination=destination)
    assert checked.ok, checked.issues + checked.projection_mismatches
    assert checked.file_count == 2
    assert checked.entry_count == 9
    assert checked.offbox_compared


def test_resume_uses_published_footer_after_crash_before_checkpoint(
    engine: Engine, tmp_path: Path
) -> None:
    journal_dir = tmp_path / "journal"
    destination = LocalAppendOnlyDestination(tmp_path / "dr")
    copy_id = _seed_copy(engine)
    for ordinal in (1, 2):
        _append_verify_receipt(engine, copy_id, ordinal=ordinal, at=_time(1))
    _append_retention_event(engine, ordinal=1, at=_time(1))

    def crash(point: str) -> None:
        assert point == "after_publish_before_checkpoint"
        raise RuntimeError("simulated checkpoint crash")

    with pytest.raises(RuntimeError, match="checkpoint crash"):
        export_journal(
            engine,
            journal_dir=journal_dir,
            destination=destination,
            now=_time(2),
            crash_hook=crash,
        )
    with session_scope(engine) as session:
        assert session.get(RetentionJournalCheckpoint, 1) is None
    [crash_segment] = journal_dir.glob("????-??-??/*.jsonl")
    assert _entry_identities(crash_segment) == [
        ("verify_receipt", 1),
        ("verify_receipt", 2),
        ("retention_event", 1),
    ]
    assert _json_lines(crash_segment)[-1]["cursors"] == {
        "verify_receipt": 2,
        "retention_event": 1,
    }

    _append_verify_receipt(engine, copy_id, ordinal=3, at=_time(3))
    for ordinal in (2, 3, 4):
        _append_retention_event(engine, ordinal=ordinal, at=_time(3))
    resumed = export_journal(
        engine,
        journal_dir=journal_dir,
        destination=destination,
        now=_time(4),
    )
    assert resumed.entry_count == 4
    assert resumed.state.global_sequence == 7
    assert resumed.state.verify_receipt_cursor == 3
    assert resumed.state.retention_event_cursor == 4
    assert _entry_identities(resumed.segment) == [
        ("verify_receipt", 3),
        ("retention_event", 2),
        ("retention_event", 3),
        ("retention_event", 4),
    ]
    assert len(list(journal_dir.glob("????-??-??/*.jsonl"))) == 2
    assert [
        identity
        for segment in sorted(journal_dir.glob("????-??-??/*.jsonl"))
        for identity in _entry_identities(segment)
    ] == [
        ("verify_receipt", 1),
        ("verify_receipt", 2),
        ("retention_event", 1),
        ("verify_receipt", 3),
        ("retention_event", 2),
        ("retention_event", 3),
        ("retention_event", 4),
    ]
    with session_scope(engine) as session:
        checkpoint = session.get(RetentionJournalCheckpoint, 1)
        assert checkpoint is not None
        assert checkpoint.global_sequence == 7
        assert checkpoint.verify_receipt_cursor == 3
        assert checkpoint.retention_event_cursor == 4
    assert check_journal(engine, journal_dir=journal_dir, destination=destination).ok


def test_checkpoint_never_authorizes_resume_after_published_file_loss(
    engine: Engine, tmp_path: Path
) -> None:
    journal_dir = tmp_path / "journal"
    destination = LocalAppendOnlyDestination(tmp_path / "dr")
    copy_id = _seed_copy(engine)
    _append_pair(engine, copy_id, ordinal=1, at=_time(1))
    exported = export_journal(
        engine,
        journal_dir=journal_dir,
        destination=destination,
        now=_time(2),
    )
    assert exported.segment is not None
    exported.segment.unlink()

    with pytest.raises(RuntimeError, match="checkpoint-ahead-of-files"):
        export_journal(
            engine,
            journal_dir=journal_dir,
            destination=destination,
            now=_time(3),
        )
    assert list(journal_dir.glob("????-??-??/*.jsonl")) == []


def test_concurrent_exporter_is_excluded_by_worker_pattern_flock(
    engine: Engine, tmp_path: Path
) -> None:
    lockfile = process_lockfile_for(engine, namespace="retention-journal-export")
    copy_id = _seed_copy(engine)
    _append_pair(engine, copy_id, ordinal=1, at=_time(1))
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "from pathlib import Path\n"
                "from sutradhara.jobs.worker_lock import exclusive_process_lock\n"
                "import sys\n"
                "with exclusive_process_lock(Path(sys.argv[1]), purpose='test holder'):\n"
                "    print('locked', flush=True)\n"
                "    sys.stdin.readline()\n"
            ),
            str(lockfile),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "locked"
        with pytest.raises(JournalExportAlreadyRunning, match="holder pid"):
            export_journal(engine, journal_dir=tmp_path / "different-configured-journal")
    finally:
        holder.communicate("\n", timeout=5)
    assert holder.returncode == 0


def test_tamper_and_offbox_head_damage_are_detected(engine: Engine, tmp_path: Path) -> None:
    journal_dir = tmp_path / "journal"
    dr_dir = tmp_path / "dr"
    destination = LocalAppendOnlyDestination(dr_dir)
    copy_id = _seed_copy(engine)
    _append_pair(engine, copy_id, ordinal=1, at=_time(1))
    exported = export_journal(
        engine,
        journal_dir=journal_dir,
        destination=destination,
        now=_time(2),
    )
    assert exported.segment is not None

    lines = _json_lines(exported.segment)
    lines[0]["payload"]["actor"] = "accidental-rewrite"
    exported.segment.write_text(
        "".join(json.dumps(line, sort_keys=True, separators=(",", ":")) + "\n" for line in lines)
    )
    broken = check_journal(engine, journal_dir=journal_dir, destination=destination)
    assert not broken.ok
    assert any("entry checksum mismatch" in issue for issue in broken.issues)

    anchor = next(dr_dir.glob("????-??-??/*.head.json"))
    anchor.write_text('{"wrong":"head"}\n')
    head_broken = check_journal(engine, journal_dir=journal_dir, destination=destination)
    assert any("offbox-head-mismatch" in issue for issue in head_broken.issues)


def test_projection_comparison_is_non_gating_but_fails_check(
    engine: Engine, tmp_path: Path
) -> None:
    journal_dir = tmp_path / "journal"
    destination = LocalAppendOnlyDestination(tmp_path / "dr")
    copy_id = _seed_copy(engine)
    _append_pair(engine, copy_id, ordinal=1, at=_time(1))
    export_journal(
        engine,
        journal_dir=journal_dir,
        destination=destination,
        now=_time(2),
    )
    with session_scope(engine) as session:
        copy = session.get(Copy, copy_id)
        assert copy is not None
        copy.last_measured_digest = hashlib.sha256(b"forged").digest()

    checked = check_journal(engine, journal_dir=journal_dir, destination=destination)
    assert not checked.ok
    assert checked.issues == ()
    assert checked.projection_mismatches == (
        "projection: copy 1 measured digest differs from receipt 1",
    )


def test_append_only_destination_rejects_different_existing_bytes(tmp_path: Path) -> None:
    destination = LocalAppendOnlyDestination(tmp_path / "dr")
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    assert destination.publish_file(first, "2026-07-20/segment")
    assert not destination.publish_file(first, "2026-07-20/segment")
    with pytest.raises(RuntimeError, match="append-only DR collision"):
        destination.publish_file(second, "2026-07-20/segment")


def test_ssh_journal_destination_exports_only_append_only_dated_objects(
    engine: Engine, tmp_path: Path
) -> None:
    """The production SSH destination must never overwrite or delete DR evidence."""

    transport = _SpyAppendOnlyTransport()
    destination = SshDiskJournalDestination(transport, prefix="audit/journal")  # type: ignore[arg-type]
    copy_id = _seed_copy(engine)
    _append_pair(engine, copy_id, ordinal=1, at=_time(1))

    exported = export_journal(
        engine,
        journal_dir=tmp_path / "journal",
        destination=destination,
        now=_time(2),
    )

    assert exported.shipping_error is None
    assert exported.shipped_segments == 1
    segment_key = (
        "audit/journal/2026-07-20/retention-journal-00000000000000000001-00000000000000000002.jsonl"
    )
    head_key = f"{segment_key}.head.json"
    assert set(transport.objects) == {segment_key, head_key}
    assert [call for call in transport.calls if call[0] in {"put", "put_if_absent"}] == [
        ("put_if_absent", segment_key),
        ("put_if_absent", head_key),
    ]

    assert exported.segment is not None
    assert not destination.publish_file(
        exported.segment, segment_key.removeprefix("audit/journal/")
    )
    head_bytes = transport.objects[head_key]
    assert not destination.publish_bytes(head_bytes, head_key.removeprefix("audit/journal/"))

    collision = tmp_path / "collision.jsonl"
    collision.write_bytes(b"different immutable bytes")
    with pytest.raises(JournalError, match="append-only DR collision"):
        destination.publish_file(collision, segment_key.removeprefix("audit/journal/"))
    assert not any(call[0] in {"put", "remove"} for call in transport.calls)


def test_staleness_projects_alarm_and_clears_after_export(engine: Engine, tmp_path: Path) -> None:
    copy_id = _seed_copy(engine)
    old = _time(1)
    _append_pair(engine, copy_id, ordinal=1, at=old)
    status = refresh_staleness_alarm(
        engine,
        journal_dir=tmp_path / "journal",
        threshold_seconds=60,
        now=old + dt.timedelta(seconds=61),
    )
    assert status.stale
    with session_scope(engine) as session:
        alarm = session.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == ALARM_DOMAIN,
                ReconciliationCondition.target_key == "export-stale",
            )
        ).one()
        assert alarm.condition == "open"

    export_journal(
        engine,
        journal_dir=tmp_path / "journal",
        destination=LocalAppendOnlyDestination(tmp_path / "dr"),
        now=old + dt.timedelta(seconds=62),
    )
    current = journal_operational_status(
        engine,
        journal_dir=tmp_path / "journal",
        threshold_seconds=60,
        now=old + dt.timedelta(seconds=63),
    )
    assert not current.stale
    assert current.pending_entries == 0
    with session_scope(engine) as session:
        alarm = session.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == ALARM_DOMAIN,
                ReconciliationCondition.target_key == "export-stale",
            )
        ).one()
        assert alarm.condition == "satisfied"


def test_offsite_confirmation_receipt_is_atomic_and_revocation_supersedes(
    engine: Engine,
) -> None:
    copy_id = _seed_copy(engine)
    media_id = f"memory:exempt:{copy_id}"
    factory = make_session_factory(engine)
    session = factory()
    try:
        confirm_offsite(
            session,
            media_id=media_id,
            confirmed_by="ops",
            shipment_id="shipment-1",
        )
        session.rollback()
    finally:
        session.close()
    with session_scope(engine) as session:
        assert session.get(OffsiteConfirmation, media_id) is None
        assert session.scalar(select(func.count()).select_from(RetentionEvent)) == 0

    with session_scope(engine) as session:
        row, created = confirm_offsite(
            session,
            media_id=media_id,
            confirmed_by="ops",
            shipment_id="shipment-1",
        )
        assert created
        assert row.media_id == media_id
    with session_scope(engine) as session:
        receipt = session.scalars(select(RetentionEvent)).one()
        assert receipt.action == "offsite_confirmed"
        correction = record_journal_correction(
            session,
            source="retention_event",
            event_id=receipt.event_id,
            actor="ops",
            reason="wrong shipment",
        )
        assert correction.supersedes_source == "retention_event"
        assert correction.supersedes_event_id == receipt.event_id


def test_journal_check_cli_alarms_nonzero_and_prints_runbook(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal_dir = tmp_path / "journal"
    destination = LocalAppendOnlyDestination(tmp_path / "dr")
    copy_id = _seed_copy(engine)
    _append_pair(engine, copy_id, ordinal=1, at=_time(1))
    exported = export_journal(
        engine,
        journal_dir=journal_dir,
        destination=destination,
        now=_time(2),
    )
    assert exported.segment is not None
    lines = _json_lines(exported.segment)
    lines[0]["payload"]["actor"] = "tampered"
    exported.segment.write_text(
        "".join(json.dumps(line, sort_keys=True, separators=(",", ":")) + "\n" for line in lines)
    )
    monkeypatch.setenv("SUTRADHARA_DB_URL", str(engine.url))
    monkeypatch.setenv("SUTRADHARA_RETENTION_JOURNAL_DIR", str(journal_dir))
    monkeypatch.setattr(
        "sutradhara.cli.retention.configured_dr_destination",
        lambda _engine: destination,
    )

    result = CliRunner().invoke(cli, ["retention", "journal", "check"])

    assert result.exit_code == 1
    assert "journal check: FAIL" in result.output
    assert "entry checksum mismatch" in result.output
    assert "offbox-head-mismatch" in result.output
    assert "RUNBOOK:" in result.output
    with session_scope(engine) as session:
        alarm = session.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == ALARM_DOMAIN,
                ReconciliationCondition.target_key == "check-failed",
            )
        ).one()
        assert alarm.condition == "open"


def _seed_copy(engine: Engine) -> int:
    digest = content_hash(hashlib.sha256(b"journal evidence bytes").digest())
    locator = {"hash_hex": digest.hex()}
    with session_scope(engine) as session:
        backend = Backend(
            name="journal-memory",
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
            source=CopySource.INGEST,
            health=CopyHealth.OK,
        )
        session.add(copy)
        session.flush()
        return copy.id


def _append_pair(engine: Engine, copy_id: int, *, ordinal: int, at: dt.datetime) -> None:
    _append_verify_receipt(engine, copy_id, ordinal=ordinal, at=at)
    _append_retention_event(engine, ordinal=ordinal, at=at)


def _append_verify_receipt(
    engine: Engine,
    copy_id: int,
    *,
    ordinal: int,
    at: dt.datetime,
) -> None:
    with session_scope(engine) as session:
        copy = session.get(Copy, copy_id)
        assert copy is not None
        record_measured(
            session,
            copy,
            VerifyResult(ok=True, measured=True, actual_hash=content_hash(copy.integrity_hash)),
            source="verify-job",
            execution_id=f"verify-{ordinal}",
            measured_at=at,
        )


def _append_retention_event(engine: Engine, *, ordinal: int, at: dt.datetime) -> None:
    with session_scope(engine) as session:
        session.add(
            RetentionEvent(
                subject_type="batch",
                subject_id=f"batch-{ordinal}",
                action="batch_invoked",
                operation_id=f"batch-{ordinal}",
                actor="ops",
                at=at,
                detail={
                    "action": "release",
                    "limit": 25,
                    "candidate_count": ordinal,
                    "dry_run": True,
                    "refused": False,
                },
            )
        )


class _SpyAppendOnlyTransport:
    """In-memory spy implementing the SSH transport surface used by journal DR."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.calls: list[tuple[str, str]] = []

    def put_if_absent(self, local: Path, relpath: str) -> bool:
        self.calls.append(("put_if_absent", relpath))
        if relpath in self.objects:
            return False
        self.objects[relpath] = local.read_bytes()
        return True

    def put(self, local: Path, relpath: str) -> None:
        del local
        self.calls.append(("put", relpath))
        raise AssertionError("journal DR must not use overwriting put")

    def get(self, relpath: str, local: Path) -> None:
        self.calls.append(("get", relpath))
        if relpath not in self.objects:
            raise FileNotFoundError(relpath)
        local.write_bytes(self.objects[relpath])

    def sha256(self, relpath: str) -> str | None:
        self.calls.append(("sha256", relpath))
        content = self.objects.get(relpath)
        return None if content is None else hashlib.sha256(content).hexdigest()

    def size(self, relpath: str) -> int | None:
        self.calls.append(("size", relpath))
        content = self.objects.get(relpath)
        return None if content is None else len(content)

    def remove(self, relpath: str) -> None:
        self.calls.append(("remove", relpath))
        raise AssertionError("journal DR must not delete final objects")


def _json_lines(path: Path | None) -> list[dict[str, Any]]:
    assert path is not None
    return [json.loads(line) for line in path.read_text().splitlines()]


def _entry_identities(path: Path | None) -> list[tuple[str, int]]:
    return [(line["source"], line["event_id"]) for line in _json_lines(path)[:-1]]


def _time(hour: int) -> dt.datetime:
    return dt.datetime(2026, 7, 20, hour, tzinfo=dt.UTC)
