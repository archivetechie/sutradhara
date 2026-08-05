"""Arrangement workspace and source-map submission primitives.

P2.3a arranges registered master ``IngestItem`` occurrences into a pre-archive
namespace, then freezes that namespace into a durable source-map plus
DB-queryable submission rows. Callers own database transactions; these helpers
flush to allocate IDs and surface invariants but never commit or roll back.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import posixpath
import tempfile
import unicodedata
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from sqlalchemy import exists, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from sutradhara.catalog.models import (
    Arrangement,
    ArrangementMember,
    AssetDerivation,
    IngestItem,
    Intake,
    Submission,
    SubmissionMember,
)
from sutradhara.catalog.types import (
    ArrangementStatus,
    IntakeStatus,
    RetentionState,
    SubmissionStatus,
)
from sutradhara.receive_novelty import work_suppression_safe
from sutradhara.restore import _fsync_directory, atomic_write_verified_file, sha256_file

DEFAULT_SUBMISSION_ROOT = Path("/replica/submissions")
SOURCE_MAP_NAME = "source-map.tsv"
SUBMISSION_JSON_NAME = "submission.json"
MANIFEST_NAME = "manifest-sha256.txt"
SOURCE_MAP_COLUMNS = ("archive_path", "source_path", "sha256", "size", "ingest_item_id")
MUTABLE_ARRANGEMENT_STATUSES = {
    ArrangementStatus.DRAFT,
    ArrangementStatus.PENDING_DERIVATIVES,
    ArrangementStatus.READY,
}


class ArrangementError(ValueError):
    """Base class for arrangement-domain validation failures."""


class ArrangementFrozen(ArrangementError):
    """The requested mutation targets a submitted or abandoned arrangement."""


class ArrangementSubmitRace(ArrangementError):
    """Another transaction submitted the arrangement first."""


@dataclass(frozen=True)
class SourceMapEntry:
    """One frozen source-map row ready for TSV export and DB mirroring."""

    archive_path: str
    source_path: str
    sha256: bytes
    size_bytes: int
    # Widened for member grain (§5): a bundle group mixes arrangement-origin
    # members, which always carry an ingest item, with intake-origin members,
    # which do not. Absent is absent — never the literal string "None", which
    # a bare str() would have written into a signed, hashed source map.
    ingest_item_id: int | None


@dataclass(frozen=True)
class ArrangementSummary:
    """Operator-facing summary of an arrangement workspace."""

    id: int
    label: str
    intake_id: str
    artifactclass: str
    status: str
    submission_id: str | None
    cloned_from_arrangement_id: int | None
    member_count: int
    active_member_count: int


@dataclass(frozen=True)
class SubmissionResult:
    """Result of freezing an arrangement into source-map files and rows."""

    submission_id: str
    arrangement_id: int
    source_map_path: Path
    manifest_digest: str
    member_count: int


def create_from_intake(session: Session, intake_id: str, *, label: str) -> Arrangement:
    """Create a draft arrangement containing one member per live master in an intake."""

    if not label:
        raise ArrangementError("arrangement label must be non-empty")
    intake = session.get(Intake, intake_id)
    if intake is None:
        raise ArrangementError(f"intake {intake_id!r} does not exist")
    if intake.status != IntakeStatus.REGISTERED:
        raise ArrangementError(
            f"intake {intake_id!r} is {intake.status}; arrangement requires registered"
        )
    _assert_intake_accepts_landing_work(intake)

    arrangement = Arrangement(
        label=label,
        intake_id=intake.intake_id,
        artifactclass=intake.artifactclass,
        status=ArrangementStatus.DRAFT,
    )
    session.add(arrangement)
    session.flush()

    for item in _live_master_items_for_intake(session, intake.intake_id):
        arrangement.members.append(
            ArrangementMember(
                ingest_item_id=item.id,
                member_path=canonical_member_path(item.as_received_path),
                excluded=False,
            )
        )
    session.flush()
    return arrangement


def create_from_arrangement(session: Session, arrangement_id: int, *, label: str) -> Arrangement:
    """Clone non-excluded members from an existing arrangement into a new draft."""

    if not label:
        raise ArrangementError("arrangement label must be non-empty")
    source = _get_arrangement(session, arrangement_id)
    source_intake = session.get(Intake, source.intake_id)
    if source_intake is not None:
        _assert_intake_accepts_landing_work(source_intake)
    clone = Arrangement(
        label=label,
        intake_id=source.intake_id,
        artifactclass=source.artifactclass,
        status=ArrangementStatus.DRAFT,
        cloned_from_arrangement_id=source.id,
    )
    session.add(clone)
    session.flush()

    for member in sorted(source.members, key=lambda row: row.member_path):
        if member.excluded:
            continue
        clone.members.append(
            ArrangementMember(
                ingest_item_id=member.ingest_item_id,
                member_path=member.member_path,
                excluded=False,
            )
        )
    session.flush()
    return clone


def move_member(
    session: Session, arrangement_id: int, from_path: str, to_path: str
) -> ArrangementMember:
    """Move one active arrangement member to a new archive path."""

    arrangement = _get_mutable_arrangement(session, arrangement_id)
    source_path = canonical_member_path(from_path)
    target_path = canonical_member_path(to_path)
    member = _one_active_member_by_path(arrangement, source_path)
    if any(
        row.id != member.id and not row.excluded and row.member_path == target_path
        for row in arrangement.members
    ):
        raise ArrangementError(f"duplicate active archive path {target_path!r}")
    member.member_path = target_path
    member.updated_at = _utcnow()
    arrangement.updated_at = member.updated_at
    try:
        session.flush()
    except IntegrityError as exc:
        raise ArrangementError(f"duplicate active archive path {target_path!r}") from exc
    return member


def exclude_member(session: Session, arrangement_id: int, member_path: str) -> ArrangementMember:
    """Mark one active arrangement member excluded from submit output."""

    arrangement = _get_mutable_arrangement(session, arrangement_id)
    archive_path = canonical_member_path(member_path)
    member = _one_active_member_by_path(arrangement, archive_path)
    member.excluded = True
    member.updated_at = _utcnow()
    arrangement.updated_at = member.updated_at
    session.flush()
    return member


def include_member(session: Session, arrangement_id: int, member_path: str) -> ArrangementMember:
    """Re-show one excluded arrangement member in submit output."""

    arrangement = _get_mutable_arrangement(session, arrangement_id)
    archive_path = canonical_member_path(member_path)
    member = _one_excluded_member_by_path(arrangement, archive_path)
    if any(
        row.id != member.id and not row.excluded and row.member_path == archive_path
        for row in arrangement.members
    ):
        raise ArrangementError(f"duplicate active archive path {archive_path!r}")
    member.excluded = False
    member.updated_at = _utcnow()
    arrangement.updated_at = member.updated_at
    try:
        session.flush()
    except IntegrityError as exc:
        raise ArrangementError(f"duplicate active archive path {archive_path!r}") from exc
    return member


def list_arrangements(session: Session) -> list[ArrangementSummary]:
    """Return compact summaries for every arrangement."""

    rows = list(
        session.scalars(
            select(Arrangement).options(selectinload(Arrangement.members)).order_by(Arrangement.id)
        )
    )
    return [summarize_arrangement(row) for row in rows]


def show_arrangement(session: Session, arrangement_id: int) -> Arrangement:
    """Load one arrangement with members for inspection."""

    return _get_arrangement(session, arrangement_id)


def summarize_arrangement(arrangement: Arrangement) -> ArrangementSummary:
    """Return a stable summary object for a loaded arrangement."""

    return ArrangementSummary(
        id=arrangement.id,
        label=arrangement.label,
        intake_id=arrangement.intake_id,
        artifactclass=arrangement.artifactclass,
        status=str(arrangement.status),
        submission_id=arrangement.submission_id,
        cloned_from_arrangement_id=arrangement.cloned_from_arrangement_id,
        member_count=len(arrangement.members),
        active_member_count=sum(1 for member in arrangement.members if not member.excluded),
    )


def submit_arrangement(
    session: Session,
    arrangement_id: int,
    *,
    submitted_by: str,
    submission_root: Path = DEFAULT_SUBMISSION_ROOT,
    submission_id: str | None = None,
) -> SubmissionResult:
    """Freeze an arrangement into source-map files and immutable submission rows."""

    if not submitted_by:
        raise ArrangementError("submitted_by must be non-empty")
    arrangement = _lock_arrangement_for_submit(session, arrangement_id)
    if arrangement.status == ArrangementStatus.SUBMITTED:
        raise ArrangementFrozen(f"arrangement {arrangement_id} is already submitted")
    if arrangement.status not in MUTABLE_ARRANGEMENT_STATUSES:
        raise ArrangementFrozen(
            f"arrangement {arrangement_id} is {arrangement.status}; cannot submit"
        )

    entries = _build_source_map_entries(session, arrangement)
    if not entries:
        raise ArrangementError("cannot submit an arrangement with no active members")

    final_submission_id = submission_id or str(uuid.uuid4())
    submitted_at = _utcnow()
    submission_dir = submission_root.resolve() / final_submission_id
    source_map_path = submission_dir / SOURCE_MAP_NAME
    source_map_bytes = render_source_map(entries).encode("utf-8")
    manifest_digest = hashlib.sha256(source_map_bytes).hexdigest()
    submission_json_bytes = _render_submission_json(
        submission_id=final_submission_id,
        arrangement=arrangement,
        source_map_path=source_map_path,
        manifest_digest=manifest_digest,
        member_count=len(entries),
        submitted_by=submitted_by,
        submitted_at=submitted_at,
    )
    manifest_bytes = f"{manifest_digest}  {SOURCE_MAP_NAME}\n".encode()

    _write_submission_files(
        submission_dir,
        {
            SOURCE_MAP_NAME: source_map_bytes,
            SUBMISSION_JSON_NAME: submission_json_bytes,
            MANIFEST_NAME: manifest_bytes,
        },
    )

    result = cast(
        CursorResult[Any],
        session.execute(
            update(Arrangement)
            .where(
                Arrangement.id == arrangement.id,
                Arrangement.status != ArrangementStatus.SUBMITTED,
            )
            .values(
                status=ArrangementStatus.SUBMITTED,
                submitted_at=submitted_at,
                updated_at=submitted_at,
            )
        ),
    )
    if result.rowcount != 1:
        raise ArrangementSubmitRace(f"arrangement {arrangement.id} was submitted concurrently")
    arrangement.status = ArrangementStatus.SUBMITTED
    arrangement.submitted_at = submitted_at
    arrangement.updated_at = submitted_at

    submission = Submission(
        id=final_submission_id,
        arrangement_id=arrangement.id,
        artifactclass=arrangement.artifactclass,
        source_map_path=str(source_map_path),
        manifest_digest=manifest_digest,
        member_count=len(entries),
        status=SubmissionStatus.PENDING_ARCHIVE,
        archived_at=None,
        submitted_by=submitted_by,
        submitted_at=submitted_at,
    )
    session.add(submission)
    session.flush()
    for ordinal, entry in enumerate(entries):
        session.add(
            SubmissionMember(
                submission_id=final_submission_id,
                ingest_item_id=entry.ingest_item_id,
                archive_path=entry.archive_path,
                source_path=entry.source_path,
                sha256=entry.sha256,
                size_bytes=entry.size_bytes,
                ord=ordinal,
            )
        )

    arrangement.submission_id = final_submission_id
    session.flush()

    return SubmissionResult(
        submission_id=final_submission_id,
        arrangement_id=arrangement.id,
        source_map_path=source_map_path,
        manifest_digest=manifest_digest,
        member_count=len(entries),
    )


def render_source_map(entries: list[SourceMapEntry]) -> str:
    """Render source-map rows as deterministic UTF-8 TSV."""

    lines = ["\t".join(SOURCE_MAP_COLUMNS)]
    for entry in entries:
        lines.append(
            "\t".join(
                (
                    entry.archive_path,
                    entry.source_path,
                    entry.sha256.hex(),
                    str(entry.size_bytes),
                    "" if entry.ingest_item_id is None else str(entry.ingest_item_id),
                )
            )
        )
    return "\n".join(lines) + "\n"


def canonical_member_path(value: str) -> str:
    """Validate and return a normalized relative POSIX archive path."""

    if not isinstance(value, str) or not value:
        raise ArrangementError("member_path must be a non-empty string")
    _reject_control_chars(value, "member_path")
    if "\\" in value:
        raise ArrangementError("member_path must use forward slashes")
    if unicodedata.normalize("NFC", value) != value:
        raise ArrangementError("member_path must be NFC-normalized")
    if value.startswith("/"):
        raise ArrangementError("member_path must be relative")
    normalized = posixpath.normpath(value)
    if normalized in {"", "."}:
        raise ArrangementError("member_path must name a file")
    if normalized != value:
        raise ArrangementError("member_path must be normalized")
    parts = normalized.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ArrangementError("member_path must not contain empty, '.', or '..' segments")
    return normalized


def _build_source_map_entries(session: Session, arrangement: Arrangement) -> list[SourceMapEntry]:
    entries: list[SourceMapEntry] = []
    seen_paths: set[str] = set()
    for member in sorted(
        (row for row in arrangement.members if not row.excluded),
        key=lambda row: row.member_path,
    ):
        archive_path = canonical_member_path(member.member_path)
        if archive_path in seen_paths:
            raise ArrangementError(f"duplicate active archive path {archive_path!r}")
        seen_paths.add(archive_path)
        item = session.get(IngestItem, member.ingest_item_id)
        if item is None:
            raise ArrangementError(
                f"arrangement member {member.id} references a missing ingest item"
            )
        _validate_live_master(session, item)
        if item.artifactclass != arrangement.artifactclass:
            raise ArrangementError(
                f"member {member.id} artifactclass {item.artifactclass!r} "
                f"does not match arrangement artifactclass {arrangement.artifactclass!r}"
            )
        source_path = _source_path_for_item(item)
        _reject_control_chars(source_path, "source_path")
        path = Path(source_path)
        if not path.is_file():
            raise ArrangementError(
                f"source_path for item {item.id} is missing or not a file: {path}"
            )
        digest = sha256_file(path)
        if digest != item.logical_asset_hash:
            raise ArrangementError(
                f"source_path for item {item.id} hashes to {digest.hex()}, "
                f"expected {item.logical_asset_hash.hex()}"
            )
        try:
            size_bytes = path.stat().st_size
        except OSError as exc:
            raise ArrangementError(
                f"source_path for item {item.id} is inaccessible: {path}"
            ) from exc
        if size_bytes != item.size_bytes:
            raise ArrangementError(
                f"source_path for item {item.id} has size {size_bytes}, expected {item.size_bytes}"
            )
        entries.append(
            SourceMapEntry(
                archive_path=archive_path,
                source_path=source_path,
                sha256=item.logical_asset_hash,
                size_bytes=item.size_bytes,
                ingest_item_id=item.id,
            )
        )
    return entries


def _render_submission_json(
    *,
    submission_id: str,
    arrangement: Arrangement,
    source_map_path: Path,
    manifest_digest: str,
    member_count: int,
    submitted_by: str,
    submitted_at: dt.datetime,
) -> bytes:
    payload = {
        "id": submission_id,
        "arrangement_id": arrangement.id,
        "artifactclass": arrangement.artifactclass,
        "source_map_path": str(source_map_path),
        "manifest_digest": manifest_digest,
        "member_count": member_count,
        "status": SubmissionStatus.PENDING_ARCHIVE.value,
        "submitted_by": submitted_by,
        "submitted_at": submitted_at.isoformat(),
    }
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _write_submission_files(submission_dir: Path, files: dict[str, bytes]) -> None:
    try:
        submission_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise ArrangementError(f"submission directory already exists: {submission_dir}") from exc
    _fsync_directory(submission_dir.parent)
    with tempfile.TemporaryDirectory(prefix="sutradhara-submission-") as temp_dir_raw:
        temp_dir = Path(temp_dir_raw)
        for name, payload in files.items():
            source = temp_dir / name
            source.write_bytes(payload)
            atomic_write_verified_file(source, submission_dir / name)


def _lock_arrangement_for_submit(session: Session, arrangement_id: int) -> Arrangement:
    arrangement = session.scalars(
        select(Arrangement)
        .options(selectinload(Arrangement.members))
        .where(Arrangement.id == arrangement_id)
        .with_for_update()
    ).one_or_none()
    if arrangement is None:
        raise ArrangementError(f"arrangement {arrangement_id} does not exist")
    return arrangement


def _get_arrangement(session: Session, arrangement_id: int) -> Arrangement:
    arrangement = session.scalars(
        select(Arrangement)
        .options(selectinload(Arrangement.members))
        .where(Arrangement.id == arrangement_id)
    ).one_or_none()
    if arrangement is None:
        raise ArrangementError(f"arrangement {arrangement_id} does not exist")
    return arrangement


def _get_mutable_arrangement(session: Session, arrangement_id: int) -> Arrangement:
    arrangement = _get_arrangement(session, arrangement_id)
    if arrangement.status not in MUTABLE_ARRANGEMENT_STATUSES:
        raise ArrangementFrozen(
            f"arrangement {arrangement_id} is {arrangement.status}; only draft arrangements mutate"
        )
    return arrangement


def _one_active_member_by_path(arrangement: Arrangement, member_path: str) -> ArrangementMember:
    matches = [
        member
        for member in arrangement.members
        if member.member_path == member_path and not member.excluded
    ]
    if not matches:
        raise ArrangementError(f"arrangement {arrangement.id} has no active member {member_path!r}")
    if len(matches) > 1:
        raise ArrangementError(
            f"arrangement {arrangement.id} has ambiguous active member {member_path!r}"
        )
    return matches[0]


def _one_excluded_member_by_path(arrangement: Arrangement, member_path: str) -> ArrangementMember:
    matches = [
        member
        for member in arrangement.members
        if member.member_path == member_path and member.excluded
    ]
    if not matches:
        raise ArrangementError(f"arrangement {arrangement.id} has no excluded member {member_path!r}")
    if len(matches) > 1:
        raise ArrangementError(
            f"arrangement {arrangement.id} has ambiguous excluded member {member_path!r}"
        )
    return matches[0]


def _live_master_items_for_intake(session: Session, intake_id: str) -> list[IngestItem]:
    derived_exists = exists().where(AssetDerivation.derived_item_id == IngestItem.id)
    candidates = list(
        session.scalars(
            select(IngestItem)
            .join(Intake, Intake.intake_id == IngestItem.intake_id)
            .where(
                IngestItem.intake_id == intake_id,
                Intake.status == IntakeStatus.REGISTERED,
                ~derived_exists,
            )
            .order_by(IngestItem.as_received_path)
        )
    )
    return [item for item in candidates if not work_suppression_safe(session, item)]


def _validate_live_master(session: Session, item: IngestItem) -> None:
    intake = session.get(Intake, item.intake_id)
    if intake is None or intake.status != IntakeStatus.REGISTERED:
        raise ArrangementError(f"item {item.id} is not in a registered intake")
    _assert_intake_accepts_landing_work(intake)
    derived_edge = session.scalars(
        select(AssetDerivation.id).where(AssetDerivation.derived_item_id == item.id).limit(1)
    ).one_or_none()
    if derived_edge is not None:
        raise ArrangementError(f"item {item.id} is a derived item, not a master")


def _source_path_for_item(item: IngestItem) -> str:
    value = item.item_metadata.get("source_path") if item.item_metadata else None
    if not isinstance(value, str) or not value:
        raise ArrangementError(f"item {item.id} has no item_metadata['source_path']")
    return value


def _assert_intake_accepts_landing_work(intake: Intake) -> None:
    if intake.retention_state in {RetentionState.RELEASED, RetentionState.PURGED}:
        raise ArrangementError(
            f"intake {intake.intake_id!r} is {intake.retention_state}; "
            "use virtual arrangements for post-archive organizing"
        )


def _reject_control_chars(value: str, label: str) -> None:
    for char in value:
        codepoint = ord(char)
        if codepoint < 32 or 127 <= codepoint <= 159:
            raise ArrangementError(f"{label} contains a control character")


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


__all__ = [
    "DEFAULT_SUBMISSION_ROOT",
    "ArrangementError",
    "ArrangementFrozen",
    "ArrangementSubmitRace",
    "ArrangementSummary",
    "SourceMapEntry",
    "SubmissionResult",
    "canonical_member_path",
    "create_from_arrangement",
    "create_from_intake",
    "exclude_member",
    "include_member",
    "list_arrangements",
    "move_member",
    "render_source_map",
    "show_arrangement",
    "submit_arrangement",
]
