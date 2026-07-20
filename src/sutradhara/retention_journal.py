"""Emit-only retention receipt export, append-only DR shipping, and verification.

The retention gate never imports this module.  Journal failures are projected as
operator alarms but cannot authorize or prevent deletion.  Published JSONL
segments, rather than the catalog checkpoint, are the authoritative chain.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import os
import re
import shutil
import tempfile
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path, PurePosixPath
from typing import Protocol

from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

from sutradhara.backend.ssh_disk import RsyncSshTransport
from sutradhara.catalog.models import (
    Backend,
    Copy,
    RetentionEvent,
    RetentionJournalCheckpoint,
    VerifyReceipt,
)
from sutradhara.catalog.session import session_scope
from sutradhara.catalog.types import BackendKind
from sutradhara.jobs.models import ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_OPEN,
    OBSERVED_MISSING,
    OBSERVED_PRESENT,
    record_observation,
)
from sutradhara.jobs.worker_lock import (
    ProcessLockHeld,
    exclusive_process_lock,
    process_lockfile_for,
)

ENVELOPE_ID = "sutradhara.retention-journal/v1"
HASH_ALGORITHM_ID = "sha256"
GENESIS_HASH = hashlib.sha256(b"sutradhara.retention-journal:genesis:v1").hexdigest()
SOURCE_RANK = {"verify_receipt": 0, "retention_event": 1}
DEFAULT_STALE_SECONDS = 2 * 60 * 60
ALARM_DOMAIN = "retention_journal_alarm"
RUNBOOK_TEXT = (
    "RUNBOOK: Retention remains non-gating. Compare local export files with dated DR "
    "copies and the off-box head; compare exported entries with verify_receipt and "
    "retention_event rows; use WAL point-in-time state to locate the first divergence. "
    "Never rewrite or repair a published file; re-export damaged evidence and "
    "cross-check the DR copies."
)

_SEGMENT_RE = re.compile(r"^retention-journal-(\d{20})-(\d{20})\.jsonl$")


class JournalError(RuntimeError):
    """Base error for journal export, shipping, and verification."""


class JournalExportAlreadyRunning(JournalError):
    """Raised when the singleton exporter lock is already held."""


class AppendOnlyDestination(Protocol):
    """Minimal off-box destination used by the single export path."""

    def publish_file(self, source: Path, key: str) -> bool: ...

    def publish_bytes(self, content: bytes, key: str) -> bool: ...

    def read_bytes(self, key: str) -> bytes | None: ...


@dataclass(frozen=True)
class JournalState:
    """Chain head and inclusive source cursors from a published footer."""

    global_sequence: int = 0
    head_hash: str = GENESIS_HASH
    verify_receipt_cursor: int = 0
    retention_event_cursor: int = 0
    published_filename: str | None = None
    published_at: dt.datetime | None = None


@dataclass(frozen=True)
class JournalExportResult:
    """Outcome of one serialized export and shipping attempt."""

    published: bool
    entry_count: int
    segment: Path | None
    state: JournalState
    shipped_segments: int
    shipping_error: str | None


@dataclass(frozen=True)
class JournalCheckResult:
    """Full local-chain, off-box-head, and projection consistency result."""

    ok: bool
    file_count: int
    entry_count: int
    state: JournalState
    offbox_compared: bool
    issues: tuple[str, ...]
    projection_mismatches: tuple[str, ...]


@dataclass(frozen=True)
class JournalOperationalStatus:
    """Sitrep-facing export lag projection."""

    state: JournalState
    pending_entries: int
    oldest_pending_at: dt.datetime | None
    stale_seconds: int
    threshold_seconds: int
    stale: bool


@dataclass(frozen=True)
class _SourceEntry:
    source: str
    event_id: int
    payload: dict[str, object]


class LocalAppendOnlyDestination:
    """Filesystem destination used by hermetic checks and local DR drills."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def publish_file(self, source: Path, key: str) -> bool:
        destination = self._path(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            _require_same_file(source, destination, key)
            return False
        temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
        try:
            shutil.copyfile(source, temporary)
            _fsync_file(temporary)
            try:
                os.link(temporary, destination)
            except FileExistsError:
                _require_same_file(source, destination, key)
                return False
            _fsync_directory(destination.parent)
            return True
        finally:
            temporary.unlink(missing_ok=True)

    def publish_bytes(self, content: bytes, key: str) -> bool:
        with tempfile.TemporaryDirectory(prefix="sutradhara-journal-head-") as raw:
            source = Path(raw) / "head.json"
            source.write_bytes(content)
            _fsync_file(source)
            return self.publish_file(source, key)

    def read_bytes(self, key: str) -> bytes | None:
        path = self._path(key)
        try:
            return path.read_bytes()
        except FileNotFoundError:
            return None

    def _path(self, key: str) -> Path:
        clean = _validated_destination_key(key)
        return self.root.joinpath(*PurePosixPath(clean).parts)


class SshDiskJournalDestination:
    """Append-only journal view over one explicitly configured ssh_disk backend."""

    def __init__(self, transport: RsyncSshTransport, *, prefix: str) -> None:
        self._transport = transport
        self._prefix = _validated_destination_key(prefix.strip("/")) if prefix else ""

    def publish_file(self, source: Path, key: str) -> bool:
        remote_key = self._key(key)
        created = self._transport.put_if_absent(source, remote_key)
        if created:
            return True
        remote_hash = self._transport.sha256(remote_key)
        if remote_hash != _hash_file(source):
            raise JournalError(
                f"append-only DR collision for {remote_key!r}; existing bytes differ"
            )
        return False

    def publish_bytes(self, content: bytes, key: str) -> bool:
        with tempfile.TemporaryDirectory(prefix="sutradhara-journal-head-") as raw:
            source = Path(raw) / "head.json"
            source.write_bytes(content)
            return self.publish_file(source, key)

    def read_bytes(self, key: str) -> bytes | None:
        remote_key = self._key(key)
        with tempfile.TemporaryDirectory(prefix="sutradhara-journal-read-") as raw:
            destination = Path(raw) / "object"
            try:
                self._transport.get(remote_key, destination)
            except Exception as exc:
                # RsyncSshTransport maps an absent object to BackendNotFoundError;
                # importing that type solely for this branch obscures real transport
                # failures, so confirm absence via size and re-raise otherwise.
                if self._transport.size(remote_key) is None:
                    return None
                raise JournalError(f"failed reading DR object {remote_key!r}: {exc}") from exc
            return destination.read_bytes()

    def _key(self, key: str) -> str:
        clean = _validated_destination_key(key)
        return f"{self._prefix}/{clean}" if self._prefix else clean


def export_journal(
    engine: Engine,
    *,
    journal_dir: Path | str | None = None,
    destination: AppendOnlyDestination | None = None,
    now: dt.datetime | None = None,
    crash_hook: Callable[[str], None] | None = None,
) -> JournalExportResult:
    """Publish all new receipt rows, checkpoint, then ship append-only DR copies."""

    export_root = _journal_dir(engine, journal_dir)
    lockfile = process_lockfile_for(engine, namespace="retention-journal-export")
    try:
        with exclusive_process_lock(lockfile, purpose="retention journal exporter"):
            return _export_locked(
                engine,
                export_root=export_root,
                destination=destination,
                now=_as_utc(now or dt.datetime.now(dt.UTC)),
                crash_hook=crash_hook,
            )
    except ProcessLockHeld as exc:
        raise JournalExportAlreadyRunning(str(exc)) from exc


def check_journal(
    engine: Engine,
    *,
    journal_dir: Path | str | None = None,
    destination: AppendOnlyDestination | None = None,
) -> JournalCheckResult:
    """Walk every published link and compare the latest DR head and DB projection."""

    export_root = _journal_dir(engine, journal_dir)
    issues: list[str] = []
    entries = 0
    state = JournalState()
    segments: list[tuple[Path, dict[str, object]]] = []
    try:
        segments = _published_segments(export_root)
    except JournalError as exc:
        issues.append(str(exc))

    if not segments and not issues:
        issues.append("no-published-files: retention journal has no authoritative footer")

    if segments:
        expected_sequence = 1
        expected_hash = GENESIS_HASH
        cursors = {"verify_receipt": 0, "retention_event": 0}
        for path, _summary_footer in segments:
            try:
                file_entries, expected_sequence, expected_hash, cursors, footer = _check_segment(
                    path,
                    expected_sequence=expected_sequence,
                    expected_hash=expected_hash,
                    cursors=cursors,
                )
            except JournalError as exc:
                issues.append(f"{path.name}: {exc}")
                break
            entries += file_entries
            state = _state_from_footer(footer, path)

    offbox_compared = False
    if segments:
        if destination is None:
            issues.append("offbox-head-unavailable: no ssh_disk DR target is configured")
        else:
            latest_path = segments[-1][0]
            anchor_key = _anchor_key(export_root, latest_path)
            anchored_state = _state_from_footer(segments[-1][1], latest_path)
            expected_anchor = _anchor_bytes(anchored_state)
            try:
                for path, _footer in segments:
                    dr_segment = destination.read_bytes(_segment_key(export_root, path))
                    if dr_segment != path.read_bytes():
                        issues.append(
                            "offbox-head-mismatch: local segment bytes differ from the "
                            f"append-only DR copy for {path.name}"
                        )
                actual_anchor = destination.read_bytes(anchor_key)
            except Exception as exc:
                issues.append(f"offbox-head-read-failed: {exc}")
            else:
                offbox_compared = True
                if actual_anchor != expected_anchor:
                    issues.append(
                        "offbox-head-mismatch: DR anchor is missing or differs from local head"
                    )

    published_state = _state_from_footer(segments[-1][1], segments[-1][0]) if segments else state
    checkpoint_issue = _checkpoint_ahead_issue(engine, published_state)
    if checkpoint_issue is not None:
        issues.append(checkpoint_issue)

    projection_mismatches = tuple(_projection_mismatches(engine))
    return JournalCheckResult(
        ok=not issues and not projection_mismatches,
        file_count=len(segments),
        entry_count=entries,
        state=state,
        offbox_compared=offbox_compared,
        issues=tuple(issues),
        projection_mismatches=projection_mismatches,
    )


def journal_operational_status(
    engine: Engine,
    *,
    journal_dir: Path | str | None = None,
    threshold_seconds: int | None = None,
    now: dt.datetime | None = None,
) -> JournalOperationalStatus:
    """Measure unexported receipt age for sitrep and staleness alarms."""

    threshold = _stale_threshold(threshold_seconds)
    current = _as_utc(now or dt.datetime.now(dt.UTC))
    segments = _published_segments(_journal_dir(engine, journal_dir))
    state = _state_from_footer(segments[-1][1], segments[-1][0]) if segments else JournalState()
    pending_times: list[dt.datetime] = []
    with Session(engine) as session:
        verify_count = int(
            session.scalar(
                select(func.count())
                .select_from(VerifyReceipt)
                .where(VerifyReceipt.event_id > state.verify_receipt_cursor)
            )
            or 0
        )
        retention_count = int(
            session.scalar(
                select(func.count())
                .select_from(RetentionEvent)
                .where(RetentionEvent.event_id > state.retention_event_cursor)
            )
            or 0
        )
        for value in (
            session.scalar(
                select(func.min(VerifyReceipt.recorded_at)).where(
                    VerifyReceipt.event_id > state.verify_receipt_cursor
                )
            ),
            session.scalar(
                select(func.min(RetentionEvent.occurred_at)).where(
                    RetentionEvent.event_id > state.retention_event_cursor
                )
            ),
        ):
            if isinstance(value, dt.datetime):
                pending_times.append(_as_utc(value))
    oldest = min(pending_times) if pending_times else None
    stale_seconds = max(0, int((current - oldest).total_seconds())) if oldest else 0
    return JournalOperationalStatus(
        state=state,
        pending_entries=verify_count + retention_count,
        oldest_pending_at=oldest,
        stale_seconds=stale_seconds,
        threshold_seconds=threshold,
        stale=oldest is not None and stale_seconds > threshold,
    )


def configured_dr_destination(engine: Engine) -> AppendOnlyDestination | None:
    """Build the explicitly named ssh_disk journal destination, if configured."""

    backend_name = os.environ.get("SUTRADHARA_RETENTION_JOURNAL_DR_BACKEND")
    if not backend_name:
        return None
    with Session(engine) as session:
        rows = list(session.scalars(select(Backend).where(Backend.name == backend_name)))
        if len(rows) != 1:
            raise JournalError(
                f"journal DR backend {backend_name!r} must name exactly one catalog backend"
            )
        row = rows[0]
        if row.kind != BackendKind.SSH_DISK:
            raise JournalError(
                f"journal DR backend {backend_name!r} must be kind=ssh_disk; got {row.kind}"
            )
        config = row.config or {}
    host = _required_config_string(config, "host", backend_name)
    root = _required_config_string(config, "root", backend_name)
    user = _optional_config_string(config, "user", backend_name)
    identity_file = _optional_config_string(config, "identity_file", backend_name)
    options = config.get("ssh_options", [])
    if not isinstance(options, list) or not all(isinstance(value, str) for value in options):
        raise JournalError(
            f"journal DR backend {backend_name!r} config.ssh_options must be strings"
        )
    prefix = os.environ.get("SUTRADHARA_RETENTION_JOURNAL_DR_PREFIX", "retention-journal")
    return SshDiskJournalDestination(
        RsyncSshTransport(
            host,
            root,
            user=user,
            identity_file=identity_file,
            ssh_options=options,
        ),
        prefix=prefix,
    )


def record_journal_correction(
    session: Session,
    *,
    source: str,
    event_id: int,
    actor: str,
    reason: str,
) -> RetentionEvent:
    """Append an attributed correction targeting one immutable receipt identity."""

    if source not in SOURCE_RANK:
        raise ValueError("source must be verify_receipt or retention_event")
    if event_id <= 0:
        raise ValueError("event_id must be greater than zero")
    if not actor or not reason:
        raise ValueError("actor and reason must be non-empty")
    if source == "retention_event":
        target = session.get(RetentionEvent, event_id)
        if target is None:
            raise JournalError(f"retention_event event_id={event_id} does not exist")
        subject_type = target.subject_type
        subject_id = target.subject_id
        intake_id = target.intake_id
    else:
        target_receipt = session.get(VerifyReceipt, event_id)
        if target_receipt is None:
            raise JournalError(f"verify_receipt event_id={event_id} does not exist")
        subject_type = "receipt"
        subject_id = f"verify_receipt:{event_id}"
        intake_id = None
    row = RetentionEvent(
        intake_id=intake_id,
        subject_type=subject_type,
        subject_id=subject_id,
        action="correction_recorded",
        operation_id=f"journal-correction:{source}:{event_id}:{uuid.uuid4()}",
        actor=actor,
        occurred_at=dt.datetime.now(dt.UTC),
        detail={"kind": "receipt-supersession", "reason": reason},
        supersedes_source=source,
        supersedes_event_id=event_id,
    )
    session.add(row)
    session.flush([row])
    return row


def project_journal_alarm(
    engine: Engine,
    *,
    target_key: str,
    active: bool,
    reason: str,
    message: str,
) -> ReconciliationCondition:
    """Publish or clear one non-gating journal condition on the gap board."""

    with session_scope(engine) as session:
        row = record_observation(
            session,
            domain=ALARM_DOMAIN,
            target_key=target_key,
            desired=active,
            observed_state=OBSERVED_MISSING if active else OBSERVED_PRESENT,
        )
        if active:
            row.condition = CONDITION_OPEN
            row.reason = reason
            row.message = message
            session.flush([row])
        return row


def refresh_staleness_alarm(
    engine: Engine,
    *,
    journal_dir: Path | str | None = None,
    threshold_seconds: int | None = None,
    now: dt.datetime | None = None,
) -> JournalOperationalStatus:
    """Evaluate and project the export-staleness condition."""

    status = journal_operational_status(
        engine,
        journal_dir=journal_dir,
        threshold_seconds=threshold_seconds,
        now=now,
    )
    project_journal_alarm(
        engine,
        target_key="export-stale",
        active=status.stale,
        reason="export-stale",
        message=(
            f"{status.pending_entries} journal entries pending; oldest is "
            f"{status.stale_seconds}s old (threshold {status.threshold_seconds}s)"
        ),
    )
    return status


def _export_locked(
    engine: Engine,
    *,
    export_root: Path,
    destination: AppendOnlyDestination | None,
    now: dt.datetime,
    crash_hook: Callable[[str], None] | None,
) -> JournalExportResult:
    export_root_was_missing = not export_root.exists()
    export_root.mkdir(parents=True, exist_ok=True)
    if export_root_was_missing:
        _fsync_directory(export_root.parent)
    segments = _published_segments(export_root)
    state = _state_from_footer(segments[-1][1], segments[-1][0]) if segments else JournalState()
    checkpoint_issue = _checkpoint_ahead_issue(engine, state)
    if checkpoint_issue is not None:
        raise JournalError(checkpoint_issue)
    _write_checkpoint(engine, state)
    source_entries = _new_source_entries(engine, state)
    published_path: Path | None = None
    if source_entries:
        published_path, state = _publish_segment(
            export_root,
            source_entries,
            previous=state,
            now=now,
        )
        segments.append((published_path, _read_footer(published_path)))
        if crash_hook is not None:
            crash_hook("after_publish_before_checkpoint")
        _write_checkpoint(engine, state)

    shipped = 0
    shipping_error: str | None = None
    if segments:
        if destination is None:
            shipping_error = "no ssh_disk DR target is configured"
        else:
            try:
                shipped = _ship_segments(export_root, segments, destination)
            except Exception as exc:
                shipping_error = str(exc)

    project_journal_alarm(
        engine,
        target_key="export-failed",
        active=False,
        reason="export-failed",
        message="retention journal export succeeded",
    )
    project_journal_alarm(
        engine,
        target_key="shipping-failed",
        active=shipping_error is not None,
        reason="shipping-failed",
        message=shipping_error or "retention journal DR shipping succeeded",
    )
    refresh_staleness_alarm(engine, journal_dir=export_root, now=now)
    return JournalExportResult(
        published=published_path is not None,
        entry_count=len(source_entries),
        segment=published_path,
        state=state,
        shipped_segments=shipped,
        shipping_error=shipping_error,
    )


def _ship_segments(
    export_root: Path,
    segments: list[tuple[Path, dict[str, object]]],
    destination: AppendOnlyDestination,
) -> int:
    """Resume shipping after the newest matching append-only head anchor."""

    first_unshipped = 0
    for index in range(len(segments) - 1, -1, -1):
        path, footer = segments[index]
        expected = _anchor_bytes(_state_from_footer(footer, path))
        actual = destination.read_bytes(_anchor_key(export_root, path))
        if actual == expected:
            first_unshipped = index + 1
            break
        if actual is not None:
            raise JournalError(
                f"append-only DR head collision for {_anchor_key(export_root, path)!r}"
            )
    shipped = 0
    for path, footer in segments[first_unshipped:]:
        destination.publish_file(path, _segment_key(export_root, path))
        destination.publish_bytes(
            _anchor_bytes(_state_from_footer(footer, path)),
            _anchor_key(export_root, path),
        )
        shipped += 1
    return shipped


def _new_source_entries(engine: Engine, state: JournalState) -> list[_SourceEntry]:
    entries: list[_SourceEntry] = []
    with Session(engine) as session:
        receipts = session.scalars(
            select(VerifyReceipt)
            .where(VerifyReceipt.event_id > state.verify_receipt_cursor)
            .order_by(VerifyReceipt.event_id)
        )
        entries.extend(
            _SourceEntry("verify_receipt", row.event_id, _verify_receipt_payload(row))
            for row in receipts
        )
        events = session.scalars(
            select(RetentionEvent)
            .where(RetentionEvent.event_id > state.retention_event_cursor)
            .order_by(RetentionEvent.event_id)
        )
        entries.extend(
            _SourceEntry("retention_event", row.event_id, _retention_event_payload(row))
            for row in events
        )
    return entries


def _publish_segment(
    export_root: Path,
    entries: list[_SourceEntry],
    *,
    previous: JournalState,
    now: dt.datetime,
) -> tuple[Path, JournalState]:
    start_sequence = previous.global_sequence + 1
    end_sequence = previous.global_sequence + len(entries)
    dated_dir = export_root / now.date().isoformat()
    dated_dir_was_missing = not dated_dir.exists()
    dated_dir.mkdir(parents=True, exist_ok=True)
    if dated_dir_was_missing:
        _fsync_directory(export_root)
    final = dated_dir / f"retention-journal-{start_sequence:020d}-{end_sequence:020d}.jsonl"
    if final.exists():
        raise JournalError(f"refusing to replace published journal segment {final}")
    temporary = dated_dir / f".{final.name}.{uuid.uuid4().hex}.tmp"
    head_hash = previous.head_hash
    cursors = {
        "verify_receipt": previous.verify_receipt_cursor,
        "retention_event": previous.retention_event_cursor,
    }
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            for offset, entry in enumerate(entries, start=1):
                sequence = previous.global_sequence + offset
                checksum = _hash_json(entry.payload)
                link = {
                    "envelope_id": ENVELOPE_ID,
                    "hash_algorithm_id": HASH_ALGORITHM_ID,
                    "sequence": sequence,
                    "source": entry.source,
                    "event_id": entry.event_id,
                    "checksum": checksum,
                    "prev_hash": head_hash,
                }
                entry_hash = _hash_json(link)
                line = {
                    "kind": "entry",
                    **link,
                    "payload": entry.payload,
                    "entry_hash": entry_hash,
                }
                stream.write(_canonical_json(line) + "\n")
                head_hash = entry_hash
                cursors[entry.source] = entry.event_id
            footer_base: dict[str, object] = {
                "kind": "footer",
                "footer_version": 1,
                "envelope_id": ENVELOPE_ID,
                "hash_algorithm_id": HASH_ALGORITHM_ID,
                "segment_start_sequence": start_sequence,
                "entry_count": len(entries),
                "global_sequence": end_sequence,
                "head_hash": head_hash,
                "cursors": cursors,
                "published_at": _isoformat(now),
            }
            footer = {**footer_base, "footer_checksum": _hash_json(footer_base)}
            stream.write(_canonical_json(footer) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.rename(temporary, final)
        _fsync_directory(dated_dir)
    finally:
        temporary.unlink(missing_ok=True)
    return final, _state_from_footer(footer, final)


def _published_segments(root: Path) -> list[tuple[Path, dict[str, object]]]:
    if not root.exists():
        return []
    segments: list[tuple[Path, dict[str, object]]] = []
    for path in root.glob("????-??-??/retention-journal-*.jsonl"):
        match = _SEGMENT_RE.fullmatch(path.name)
        if match is None:
            continue
        footer = _read_footer(path)
        start = _integer(footer.get("segment_start_sequence"), "segment_start_sequence")
        end = _integer(footer.get("global_sequence"), "global_sequence")
        if start != int(match.group(1)) or end != int(match.group(2)):
            raise JournalError(f"segment filename/footer mismatch: {path}")
        segments.append((path, footer))
    segments.sort(key=lambda item: _integer(item[1]["segment_start_sequence"], "sequence"))
    for previous, current in pairwise(segments):
        previous_end = _integer(previous[1]["global_sequence"], "global_sequence")
        current_start = _integer(current[1]["segment_start_sequence"], "segment_start_sequence")
        if current_start != previous_end + 1:
            raise JournalError(
                f"published segment range discontinuity: {previous[0].name} -> {current[0].name}"
            )
    return segments


def _read_footer(path: Path) -> dict[str, object]:
    try:
        with path.open(encoding="utf-8") as stream:
            last = ""
            for line in stream:
                if line.strip():
                    last = line
    except OSError as exc:
        raise JournalError(f"cannot read published journal segment {path}: {exc}") from exc
    if not last:
        raise JournalError(f"published journal segment is empty: {path}")
    try:
        value = json.loads(last)
    except json.JSONDecodeError as exc:
        raise JournalError(f"published journal footer is invalid JSON: {path}: {exc}") from exc
    if not isinstance(value, dict) or value.get("kind") != "footer":
        raise JournalError(f"published journal footer is missing: {path}")
    footer = {str(key): item for key, item in value.items()}
    _validate_footer(footer)
    return footer


def _validate_footer(footer: dict[str, object]) -> None:
    if footer.get("footer_version") != 1:
        raise JournalError(f"unsupported footer version {footer.get('footer_version')!r}")
    if footer.get("envelope_id") != ENVELOPE_ID:
        raise JournalError(f"unsupported envelope id {footer.get('envelope_id')!r}")
    if footer.get("hash_algorithm_id") != HASH_ALGORITHM_ID:
        raise JournalError(f"unsupported hash algorithm {footer.get('hash_algorithm_id')!r}")
    checksum = footer.get("footer_checksum")
    if not isinstance(checksum, str):
        raise JournalError("footer checksum is missing")
    without_checksum = {key: value for key, value in footer.items() if key != "footer_checksum"}
    if not _constant_time_equal(checksum, _hash_json(without_checksum)):
        raise JournalError("footer checksum mismatch")
    _integer(footer.get("global_sequence"), "global_sequence")
    _integer(footer.get("segment_start_sequence"), "segment_start_sequence")
    _integer(footer.get("entry_count"), "entry_count")
    head = footer.get("head_hash")
    if not isinstance(head, str) or not _is_sha256(head):
        raise JournalError("footer head_hash is not a SHA-256 digest")
    cursors = footer.get("cursors")
    if not isinstance(cursors, dict) or set(cursors) != set(SOURCE_RANK):
        raise JournalError("footer cursors must name both source tables exactly")
    for source in SOURCE_RANK:
        _integer(cursors[source], f"cursor {source}")
    if _integer(footer.get("entry_count"), "entry_count") == 0:
        raise JournalError("published segment footer cannot have zero entries")


def _check_segment(
    path: Path,
    *,
    expected_sequence: int,
    expected_hash: str,
    cursors: dict[str, int],
) -> tuple[int, int, str, dict[str, int], dict[str, object]]:
    entry_count = 0
    segment_start = expected_sequence
    previous_order: tuple[int, int] | None = None
    footer: dict[str, object] | None = None
    try:
        stream = path.open(encoding="utf-8")
    except OSError as exc:
        raise JournalError(f"cannot read segment: {exc}") from exc
    with stream:
        for line_number, raw in enumerate(stream, start=1):
            try:
                value = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise JournalError(f"line {line_number}: invalid JSON: {exc}") from exc
            if not isinstance(value, dict):
                raise JournalError(f"line {line_number}: envelope must be an object")
            if value.get("kind") == "footer":
                if footer is not None:
                    raise JournalError(f"line {line_number}: duplicate footer")
                footer = {str(key): item for key, item in value.items()}
                continue
            if footer is not None:
                raise JournalError(f"line {line_number}: entry appears after footer")
            if value.get("kind") != "entry":
                raise JournalError(f"line {line_number}: unknown envelope kind")
            if value.get("envelope_id") != ENVELOPE_ID:
                raise JournalError(f"line {line_number}: envelope id mismatch")
            if value.get("hash_algorithm_id") != HASH_ALGORITHM_ID:
                raise JournalError(f"line {line_number}: hash algorithm mismatch")
            sequence = _integer(value.get("sequence"), f"line {line_number} sequence")
            if sequence != expected_sequence:
                raise JournalError(
                    f"line {line_number}: sequence discontinuity, expected "
                    f"{expected_sequence}, got {sequence}"
                )
            source = value.get("source")
            if not isinstance(source, str) or source not in SOURCE_RANK:
                raise JournalError(f"line {line_number}: unknown source {source!r}")
            event_id = _integer(value.get("event_id"), f"line {line_number} event_id")
            order = (SOURCE_RANK[source], event_id)
            if previous_order is not None and order <= previous_order:
                raise JournalError(f"line {line_number}: source-rank/event-id order violation")
            previous_order = order
            if event_id <= cursors[source]:
                raise JournalError(f"line {line_number}: non-increasing {source} cursor")
            payload = value.get("payload")
            if not isinstance(payload, dict):
                raise JournalError(f"line {line_number}: payload is not an object")
            if payload.get("event_id") != event_id:
                raise JournalError(f"line {line_number}: payload event_id differs from envelope")
            checksum = value.get("checksum")
            if not isinstance(checksum, str) or not _constant_time_equal(
                checksum, _hash_json(payload)
            ):
                raise JournalError(f"line {line_number}: entry checksum mismatch")
            if value.get("prev_hash") != expected_hash:
                raise JournalError(f"line {line_number}: previous-hash link mismatch")
            link = {
                "envelope_id": value.get("envelope_id"),
                "hash_algorithm_id": value.get("hash_algorithm_id"),
                "sequence": sequence,
                "source": source,
                "event_id": event_id,
                "checksum": checksum,
                "prev_hash": expected_hash,
            }
            entry_hash = value.get("entry_hash")
            if not isinstance(entry_hash, str) or not _constant_time_equal(
                entry_hash, _hash_json(link)
            ):
                raise JournalError(f"line {line_number}: entry hash mismatch")
            expected_hash = entry_hash
            expected_sequence += 1
            cursors[source] = event_id
            entry_count += 1
    if footer is None:
        raise JournalError("footer is missing")
    _validate_footer(footer)
    footer_cursors = footer["cursors"]
    assert isinstance(footer_cursors, dict)
    expected_footer = {
        "segment_start_sequence": segment_start,
        "entry_count": entry_count,
        "global_sequence": expected_sequence - 1,
        "head_hash": expected_hash,
        "verify_receipt_cursor": cursors["verify_receipt"],
        "retention_event_cursor": cursors["retention_event"],
    }
    actual_footer = {
        "segment_start_sequence": footer.get("segment_start_sequence"),
        "entry_count": footer.get("entry_count"),
        "global_sequence": footer.get("global_sequence"),
        "head_hash": footer.get("head_hash"),
        "verify_receipt_cursor": footer_cursors.get("verify_receipt"),
        "retention_event_cursor": footer_cursors.get("retention_event"),
    }
    if actual_footer != expected_footer:
        raise JournalError(
            f"footer does not match walked segment: expected {expected_footer!r}, "
            f"got {actual_footer!r}"
        )
    return entry_count, expected_sequence, expected_hash, dict(cursors), footer


def _write_checkpoint(engine: Engine, state: JournalState) -> None:
    if state.published_filename is None or state.published_at is None:
        return
    with session_scope(engine) as session:
        row = session.get(RetentionJournalCheckpoint, 1)
        if row is None:
            row = RetentionJournalCheckpoint(
                id=1,
                envelope_id=ENVELOPE_ID,
                hash_algorithm_id=HASH_ALGORITHM_ID,
                global_sequence=state.global_sequence,
                head_hash=bytes.fromhex(state.head_hash),
                verify_receipt_cursor=state.verify_receipt_cursor,
                retention_event_cursor=state.retention_event_cursor,
                published_filename=state.published_filename,
                published_at=state.published_at,
            )
            session.add(row)
        else:
            row.envelope_id = ENVELOPE_ID
            row.hash_algorithm_id = HASH_ALGORITHM_ID
            row.global_sequence = state.global_sequence
            row.head_hash = bytes.fromhex(state.head_hash)
            row.verify_receipt_cursor = state.verify_receipt_cursor
            row.retention_event_cursor = state.retention_event_cursor
            row.published_filename = state.published_filename
            row.published_at = state.published_at


def _checkpoint_ahead_issue(engine: Engine, state: JournalState) -> str | None:
    """Detect local published-file loss without using the checkpoint to resume."""

    with Session(engine) as session:
        row = session.get(RetentionJournalCheckpoint, 1)
        if row is None or row.global_sequence <= state.global_sequence:
            return None
        return (
            "checkpoint-ahead-of-files: catalog sequence "
            f"{row.global_sequence} exceeds authoritative published sequence "
            f"{state.global_sequence}"
        )


def _state_from_footer(footer: dict[str, object], path: Path) -> JournalState:
    _validate_footer(footer)
    cursors = footer["cursors"]
    assert isinstance(cursors, dict)
    published_at = footer.get("published_at")
    if not isinstance(published_at, str):
        raise JournalError("footer published_at is missing")
    try:
        parsed_at = _as_utc(dt.datetime.fromisoformat(published_at))
    except ValueError as exc:
        raise JournalError(f"footer published_at is invalid: {published_at!r}") from exc
    head = footer["head_hash"]
    assert isinstance(head, str)
    return JournalState(
        global_sequence=_integer(footer["global_sequence"], "global_sequence"),
        head_hash=head,
        verify_receipt_cursor=_integer(cursors["verify_receipt"], "verify_receipt cursor"),
        retention_event_cursor=_integer(cursors["retention_event"], "retention_event cursor"),
        published_filename=str(path),
        published_at=parsed_at,
    )


def _verify_receipt_payload(row: VerifyReceipt) -> dict[str, object]:
    return {
        "event_id": row.event_id,
        "copy_id": row.copy_id,
        "backend_id": row.backend_id,
        "expected_digest": row.expected_digest.hex(),
        "measured_digest": None if row.measured_digest is None else row.measured_digest.hex(),
        "backend_ok": row.backend_ok,
        "failure_kind": row.failure_kind,
        "failure_detail": row.failure_detail,
        "source": row.source,
        "execution_id": row.execution_id,
        "producer_process": row.producer_process,
        "actor": row.actor,
        "at": _isoformat(row.recorded_at),
    }


def _retention_event_payload(row: RetentionEvent) -> dict[str, object]:
    return {
        "event_id": row.event_id,
        "intake_id": row.intake_id,
        "subject_type": row.subject_type,
        "subject_id": row.subject_id,
        "action": row.action,
        "operation_id": row.operation_id,
        "actor": row.actor,
        "at": _isoformat(row.occurred_at),
        "detail": _json_value(row.detail),
        "supersedes_source": row.supersedes_source,
        "supersedes_event_id": row.supersedes_event_id,
    }


def _projection_mismatches(engine: Engine) -> Iterable[str]:
    with Session(engine) as session:
        latest_ids = (
            select(VerifyReceipt.copy_id, func.max(VerifyReceipt.event_id).label("event_id"))
            .group_by(VerifyReceipt.copy_id)
            .subquery()
        )
        receipts = list(
            session.scalars(
                select(VerifyReceipt).join(
                    latest_ids,
                    (VerifyReceipt.copy_id == latest_ids.c.copy_id)
                    & (VerifyReceipt.event_id == latest_ids.c.event_id),
                )
            )
        )
        seen: set[int] = set()
        for receipt in receipts:
            seen.add(receipt.copy_id)
            copy = session.get(Copy, receipt.copy_id)
            if copy is None:
                yield f"projection: receipt {receipt.event_id} references missing copy {receipt.copy_id}"
                continue
            if copy.backend_id != receipt.backend_id:
                yield f"projection: copy {copy.id} backend differs from receipt {receipt.event_id}"
            if copy.integrity_hash != receipt.expected_digest:
                yield f"projection: copy {copy.id} expected digest differs from receipt {receipt.event_id}"
            if copy.last_measured_digest != receipt.measured_digest:
                yield f"projection: copy {copy.id} measured digest differs from receipt {receipt.event_id}"
            expected_at = receipt.recorded_at if receipt.measured_digest is not None else None
            if not _same_datetime(copy.last_measured_at, expected_at):
                yield f"projection: copy {copy.id} measured time differs from receipt {receipt.event_id}"
        unreceipted = session.scalars(
            select(Copy).where(
                Copy.id.not_in(seen),
                (Copy.last_measured_digest.is_not(None) | Copy.last_measured_at.is_not(None)),
            )
        )
        for copy in unreceipted:
            yield f"projection: copy {copy.id} has measured evidence without a receipt"


def _anchor_bytes(state: JournalState) -> bytes:
    value = {
        "kind": "offbox-head",
        "anchor_version": 1,
        "envelope_id": ENVELOPE_ID,
        "hash_algorithm_id": HASH_ALGORITHM_ID,
        "global_sequence": state.global_sequence,
        "head_hash": state.head_hash,
        "cursors": {
            "verify_receipt": state.verify_receipt_cursor,
            "retention_event": state.retention_event_cursor,
        },
        "published_filename": (
            Path(state.published_filename).name if state.published_filename is not None else None
        ),
        "published_at": None if state.published_at is None else _isoformat(state.published_at),
    }
    return (_canonical_json(value) + "\n").encode()


def _segment_key(root: Path, path: Path) -> str:
    return PurePosixPath(*path.relative_to(root).parts).as_posix()


def _anchor_key(root: Path, path: Path) -> str:
    return _segment_key(root, path) + ".head.json"


def _journal_dir(engine: Engine, configured: Path | str | None) -> Path:
    if configured is not None:
        return Path(configured).expanduser().resolve(strict=False)
    raw = os.environ.get("SUTRADHARA_RETENTION_JOURNAL_DIR")
    if raw:
        return Path(raw).expanduser().resolve(strict=False)
    database = engine.url.database
    if engine.url.drivername.startswith("sqlite") and database not in {None, "", ":memory:"}:
        assert isinstance(database, str)
        db_path = Path(database).expanduser()
        if not db_path.is_absolute():
            db_path = Path.cwd() / db_path
        return db_path.resolve(strict=False).with_name("retention-journal")
    state_root = os.environ.get("SUTRADHARA_STATE_DIR") or os.environ.get("XDG_STATE_HOME")
    base = Path(state_root).expanduser() if state_root else Path.home() / ".local" / "state"
    return (base / "sutradhara" / "retention-journal").resolve(strict=False)


def _stale_threshold(value: int | None) -> int:
    if value is None:
        raw = os.environ.get("SUTRADHARA_RETENTION_JOURNAL_STALE_SECONDS")
        try:
            value = DEFAULT_STALE_SECONDS if raw is None else int(raw)
        except ValueError as exc:
            raise JournalError(
                "SUTRADHARA_RETENTION_JOURNAL_STALE_SECONDS must be an integer"
            ) from exc
    if value <= 0:
        raise JournalError("journal staleness threshold must be greater than zero")
    return value


def _required_config_string(config: dict[str, object], key: str, backend: str) -> str:
    value = config.get(key)
    if not isinstance(value, str) or not value:
        raise JournalError(f"journal DR backend {backend!r} needs config.{key}")
    return value


def _optional_config_string(config: dict[str, object], key: str, backend: str) -> str | None:
    value = config.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise JournalError(f"journal DR backend {backend!r} config.{key} must be a string")
    return value


def _validated_destination_key(key: str) -> str:
    if not key or key.startswith("/") or "\\" in key:
        raise JournalError(f"unsafe journal destination key: {key!r}")
    if any(ord(char) < 32 or ord(char) == 127 for char in key):
        raise JournalError(f"unsafe journal destination key: {key!r}")
    if any(part in {"", ".", ".."} for part in key.split("/")):
        raise JournalError(f"unsafe journal destination key: {key!r}")
    path = PurePosixPath(key)
    return path.as_posix()


def _require_same_file(source: Path, existing: Path, key: str) -> None:
    if source.stat().st_size == existing.stat().st_size and _hash_file(source) == _hash_file(
        existing
    ):
        return
    raise JournalError(f"append-only DR collision for {key!r}; existing bytes differ")


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hash_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode()).hexdigest()


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _json_value(value: object) -> object:
    if isinstance(value, dt.datetime):
        return _isoformat(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise JournalError(f"{field} must be a non-negative integer")
    return value


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _constant_time_equal(left: str, right: str) -> bool:
    return hmac.compare_digest(left, right)


def _same_datetime(left: dt.datetime | None, right: dt.datetime | None) -> bool:
    if left is None or right is None:
        return left is right
    return _as_utc(left) == _as_utc(right)


def _as_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _isoformat(value: dt.datetime) -> str:
    return _as_utc(value).isoformat().replace("+00:00", "Z")


def _fsync_file(path: Path) -> None:
    with path.open("rb") as stream:
        os.fsync(stream.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
