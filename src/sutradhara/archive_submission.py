"""Archive frozen arrangement submissions through the bundle fan-out path.

P2.5 turns a P2.3a submission source-map into a deterministic open Bundle, then
reuses ``flush_bundle`` for policy fan-out. The function owns source-root
proofs, pre-write source re-verification, and rem map-report identity checks;
the caller owns the SQL transaction and commit.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.archive_bundle import add_bundle_member
from sutradhara.archive_fanout import ArchiveBuilder, BuildArtifact, RemArchiveBuilder, flush_bundle
from sutradhara.artifactclass_policy import get_artifactclass_policy
from sutradhara.bundle_group import (
    BASIS_SOURCE_DERIVED,
    compute_bundle_group,
    group_basis_document,
)
from sutradhara.catalog.models import Bundle, Copy, Submission, SubmissionMember
from sutradhara.catalog.types import SubmissionStatus
from sutradhara.rem_archive_cli import sha256_file
from sutradhara.replication import PoolTarget, WritableStorageBackend
from sutradhara.sealing.port import Representation


class ArchiveSubmissionError(ValueError):
    """The submitted source-map cannot be archived safely."""


@dataclass(frozen=True)
class ArchiveSubmissionResult:
    """Summary of a submission archive attempt."""

    submission_id: str
    bundle_id: str
    copy_ids: tuple[int, ...]
    archived: bool
    noop: bool = False


def archive_submission(
    session: Session,
    submission_id: str,
    *,
    backends: dict[int, WritableStorageBackend],
    builder: ArchiveBuilder | None = None,
    key_epoch: str | None = None,
    tape_capacity_bytes: int | None = None,
) -> ArchiveSubmissionResult:
    """Archive one pending submission without committing the caller's session."""

    submission = session.get(Submission, submission_id)
    if submission is None:
        raise ArchiveSubmissionError(f"no submission {submission_id!r}")
    bundle_id = _bundle_id(submission.id)
    if submission.status == SubmissionStatus.ARCHIVED:
        copy_ids = tuple(
            session.scalars(select(Copy.id).where(Copy.bundle_id == bundle_id).order_by(Copy.id))
        )
        return ArchiveSubmissionResult(
            submission_id=submission.id,
            bundle_id=bundle_id,
            copy_ids=copy_ids,
            archived=True,
            noop=True,
        )
    if submission.status != SubmissionStatus.PENDING_ARCHIVE:
        raise ArchiveSubmissionError(
            f"submission {submission.id!r} is {submission.status!r}; expected pending_archive"
        )
    if session.get(Bundle, bundle_id) is not None:
        raise ArchiveSubmissionError(
            f"pending submission {submission.id!r} already has bundle {bundle_id!r}"
        )

    source_map_path = _verified_source_map_path(submission)
    source_root = _source_root(submission)
    members = list(
        session.scalars(
            select(SubmissionMember)
            .where(SubmissionMember.submission_id == submission.id)
            .order_by(SubmissionMember.ord)
        )
    )
    if not members:
        raise ArchiveSubmissionError(f"submission {submission.id!r} has no members")
    _verify_sources(source_root, members)
    bundle = _project_bundle(session, submission, members, bundle_id=bundle_id)
    expected = {member.archive_path: member for member in members}

    result = flush_bundle(
        session,
        bundle_id=bundle.id,
        backends=backends,
        builder=builder or RemArchiveBuilder(),
        key_epoch=key_epoch,
        tape_capacity_bytes=tape_capacity_bytes,
        map_path=source_map_path,
        source_root=source_root,
        map_sha256=submission.manifest_digest,
        artifact_validator=lambda target, artifact: _validate_artifact_members(
            target,
            artifact,
            expected,
        ),
    )
    submission.status = SubmissionStatus.ARCHIVED
    submission.archived_at = dt.datetime.now(dt.UTC)
    session.flush()
    return ArchiveSubmissionResult(
        submission_id=submission.id,
        bundle_id=result.bundle_id,
        copy_ids=result.copy_ids,
        archived=True,
    )


def _bundle_id(submission_id: str) -> str:
    return f"submission-{submission_id}"


def _verified_source_map_path(submission: Submission) -> Path:
    path = Path(submission.source_map_path)
    if not path.is_file():
        raise ArchiveSubmissionError(f"submission source-map is missing: {path}")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != submission.manifest_digest:
        raise ArchiveSubmissionError(
            f"submission source-map digest drifted: {digest} != {submission.manifest_digest}"
        )
    return path


def _source_root(submission: Submission) -> Path:
    arrangement = submission.arrangement
    intake = None if arrangement is None else arrangement.intake
    manifest_path = None if intake is None else intake.manifest_path
    if not manifest_path:
        raise ArchiveSubmissionError(
            f"submission {submission.id!r} intake has no manifest_path; cannot derive source root"
        )
    root = (Path(manifest_path).parent / "data").resolve()
    if not root.is_dir():
        raise ArchiveSubmissionError(f"submission source root is missing: {root}")
    return root


def _verify_sources(source_root: Path, members: list[SubmissionMember]) -> None:
    for member in members:
        source = Path(member.source_path)
        try:
            resolved = source.resolve(strict=True)
        except OSError as exc:
            raise ArchiveSubmissionError(
                f"source_path for submission member {member.id} is unavailable: {source}"
            ) from exc
        if not _is_under(resolved, source_root):
            raise ArchiveSubmissionError(
                f"source_path for submission member {member.id} escapes source root: {resolved}"
            )
        if not resolved.is_file():
            raise ArchiveSubmissionError(
                f"source_path for submission member {member.id} is not a file: {resolved}"
            )
        digest = sha256_file(resolved)
        if digest != member.sha256:
            raise ArchiveSubmissionError(
                f"source_path for submission member {member.id} hashes to {digest.hex()}, "
                f"expected {member.sha256.hex()}"
            )
        size = resolved.stat().st_size
        if size != member.size_bytes:
            raise ArchiveSubmissionError(
                f"source_path for submission member {member.id} has size {size}, "
                f"expected {member.size_bytes}"
            )


def _project_bundle(
    session: Session,
    submission: Submission,
    members: list[SubmissionMember],
    *,
    bundle_id: str,
) -> Bundle:
    policy = get_artifactclass_policy(session, submission.artifactclass)
    fingerprint, basis = compute_bundle_group(session, submission.artifactclass)
    bundle = Bundle(
        id=bundle_id,
        bundle_group=fingerprint,
        group_basis=group_basis_document(
            basis,
            basis_source=BASIS_SOURCE_DERIVED,
            target_bytes=policy.target_bytes,
            max_age_seconds=policy.max_age_seconds,
        ),
        status="open",
        target_bytes=policy.target_bytes,
        max_age_seconds=policy.max_age_seconds,
        # Funnel-style mint: the per-submission bundle is non-accumulating and
        # never adoptable, and must not collide with the group accumulator on
        # the one-open-accumulator partial index. flush_bundle keeps a
        # pre-assigned archive_id as-is. (The per-submission build itself is
        # retired by the P3 submission-convergence rework.)
        archive_id=f"archive-{bundle_id}",
        opened_at=dt.datetime.now(dt.UTC),
    )
    session.add(bundle)
    session.flush()
    for member in members:
        add_bundle_member(
            session,
            bundle=bundle,
            artifactclass=submission.artifactclass,
            logical_asset_hash=member.sha256,
            member_path=member.archive_path,
            source_path=None,
            size_bytes=member.size_bytes,
            file_sha256=member.sha256,
            source_metadata={
                "source_path_bytes_hex": os.fsencode(Path(member.source_path)).hex(),
            },
        )
    return bundle


def _validate_artifact_members(
    target: PoolTarget,
    artifact: BuildArtifact,
    expected: dict[str, SubmissionMember],
) -> None:
    representation = Representation(target.representation)
    if representation not in {Representation.RAO_PLAIN_V1, Representation.RAO_AEAD_V1}:
        return
    seen: set[str] = set()
    for built in artifact.members:
        member = expected.get(built.member_path)
        if member is None:
            raise ArchiveSubmissionError(
                f"archive build returned unexpected member {built.member_path!r}"
            )
        seen.add(built.member_path)
        if built.logical_asset_hash != member.sha256:
            raise ArchiveSubmissionError(
                f"archive build member {built.member_path!r} has wrong logical hash"
            )
        if built.file_sha256 != member.sha256:
            raise ArchiveSubmissionError(
                f"archive build member {built.member_path!r} has sha256 "
                f"{built.file_sha256.hex()}, expected {member.sha256.hex()}"
            )
        if built.size_bytes != member.size_bytes:
            raise ArchiveSubmissionError(
                f"archive build member {built.member_path!r} has size "
                f"{built.size_bytes}, expected {member.size_bytes}"
            )
        if member.ingest_item_id is None:
            raise ArchiveSubmissionError(
                f"submission member {member.id} has no ingest_item_id to cross-check"
            )
        if built.ingest_item_id != str(member.ingest_item_id):
            raise ArchiveSubmissionError(
                f"archive build member {built.member_path!r} echoed ingest_item_id "
                f"{built.ingest_item_id!r}, expected {member.ingest_item_id}"
            )
    missing = sorted(set(expected) - seen)
    if missing:
        raise ArchiveSubmissionError(f"archive build omitted members: {missing!r}")


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True
