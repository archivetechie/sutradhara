"""Converge frozen arrangement submissions onto the group accumulator.

The per-submission bundle is retired (design-bundle-groups §4). A submission no
longer mints ``bundle-{submission-id}`` and no longer drives its own build:
it appends its members to the open accumulator for its class's bundle group,
exactly as an intake enqueue does, and the sweeper flushes that accumulator
when ``bundle_due`` says so. Two submissions and an intake batch that share a
storage placement therefore converge into one object.

What that changes, stated:

- **The submit-time map stops being the build instruction.** It stays what it
  truly is — the arrangement's frozen integrity artifact — and its digest is
  still verified here. The build instruction is the flush-time catalog map
  ``flush_bundle`` renders from the bundle's own member rows.
- **Member integrity is verified at append**, against the submission manifest,
  as ``_verify_sources`` already did. The widened append-to-flush window is
  guarded on the far side by ``validate_submission_member_identity`` (pre-write,
  every representation) and by rem's writer hashing the streamed payload
  against the map's digest column.
- **Submission status is derived.** ``accumulated`` is written at append.
  ``archived`` is true only when every member sits in a sealed bundle whose
  copies satisfy the member's class ``min_copies`` in verified state —
  ``sutradhara.archive_predicate``. Material in an open bundle is not archive
  evidence and releases no source hold.
- **The result shape is a mapping, not a pair.** A seal boundary may split a
  submission across bundles, which the flat ``bundle_id``/``copy_ids`` pair
  cannot express: ``bundle_ids`` is ordered by the bundles' ``opened_at`` and
  ``copies_by_bundle`` maps each to its copy ids. Re-entrant calls return the
  same shape.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.archive_bundle import (
    enqueue_artifact,
    submission_link_metadata,
    submission_links,
)
from sutradhara.archive_predicate import submission_is_archived
from sutradhara.artifactclass_policy import get_artifactclass_policy
from sutradhara.catalog.models import Bundle, BundleMember, Copy, Submission, SubmissionMember
from sutradhara.catalog.types import SubmissionStatus
from sutradhara.rem_archive_cli import sha256_file


class ArchiveSubmissionError(ValueError):
    """The submitted source-map cannot be accumulated safely."""


@dataclass(frozen=True)
class ArchiveSubmissionResult:
    """Where one submission's members currently live.

    ``bundle_ids`` is ordered by the bundles' ``opened_at`` (ties broken by id),
    and ``copies_by_bundle`` carries each bundle's recorded copy ids — empty
    for a bundle that is still open. ``archived`` is the derived predicate, not
    a stored flag.
    """

    submission_id: str
    bundle_ids: tuple[str, ...]
    copies_by_bundle: dict[str, tuple[int, ...]]
    archived: bool
    noop: bool = False

    @property
    def copy_ids(self) -> tuple[int, ...]:
        """Every copy id across the submission's bundles, in bundle order."""
        return tuple(
            copy_id
            for bundle_id in self.bundle_ids
            for copy_id in self.copies_by_bundle.get(bundle_id, ())
        )


def archive_submission(session: Session, submission_id: str) -> ArchiveSubmissionResult:
    """Append one pending submission to its group accumulator.

    Re-entrancy is a **member-presence check**, not a bundle-id probe: the
    per-submission bundle id no longer exists, and a crash-retry after a
    partial append must land the remaining members without duplicating the
    ones already recorded. Every member already present is skipped by the
    naming ladder's idempotency rung; the result shape is identical on every
    re-entrant call.
    """

    submission = session.get(Submission, submission_id)
    if submission is None:
        raise ArchiveSubmissionError(f"no submission {submission_id!r}")
    if submission.status not in {
        SubmissionStatus.PENDING_ARCHIVE,
        SubmissionStatus.ACCUMULATED,
        SubmissionStatus.ARCHIVED,
    }:
        raise ArchiveSubmissionError(
            f"submission {submission.id!r} is {submission.status!r}; "
            "expected pending_archive, accumulated, or archived"
        )

    members = list(
        session.scalars(
            select(SubmissionMember)
            .where(SubmissionMember.submission_id == submission.id)
            .order_by(SubmissionMember.ord)
        )
    )
    if not members:
        raise ArchiveSubmissionError(f"submission {submission.id!r} has no members")

    already = submission_bundle_members(session, submission)
    if len(already) == len(members):
        # Every member is already accumulated: a pure re-entry. Nothing is
        # appended and nothing is re-hashed.
        _refresh_submission_status(session, submission)
        return _result(session, submission, noop=True)

    _verified_source_map_path(submission)
    source_root = _source_root(submission)
    _verify_sources(source_root, members)
    policy = get_artifactclass_policy(session, submission.artifactclass)
    for member in members:
        if member.id in already:
            continue
        # One funnel: the same enqueue helper the intake path uses, so
        # include-alone routing, the one-open-accumulator index, and the
        # canonical naming ladder all behave identically for both producers.
        # The digest and size are handed in because _verify_sources just
        # computed them — re-hashing every byte a second time at append is
        # the whole submission's bytes for nothing.
        enqueue_artifact(
            session,
            artifactclass=submission.artifactclass,
            policy=policy,
            logical_asset_hash=member.sha256,
            source_path=Path(member.source_path),
            member_path=member.archive_path,
            # Submission names are validated upstream by the arrangement's
            # canonical_member_path, which rejects what the escaper would
            # emit, so the naming helper leaves them as validated (§5).
            member_path_is_escaped=True,
            size_bytes=member.size_bytes,
            file_sha256=member.sha256,
            # The recorded linkage design §4 names: SubmissionMember ->
            # bundle_member -> bundle. It survives the naming ladder, which an
            # archive_path join does not, and it MERGES on an idempotent hit —
            # a member whose content a co-resident already enqueued lands on
            # the existing row, and the linkage has to reach that row or this
            # submission can never recognise its own members again.
            source_metadata=submission_link_metadata(submission.id, member.id),
        )
    _refresh_submission_status(session, submission)
    return _result(session, submission)


def submission_bundle_members(
    session: Session,
    submission: Submission,
) -> dict[int, BundleMember]:
    """Map ``SubmissionMember.id`` to the bundle member row that carries it.

    Indexed on ``bundle_member.logical_asset_hash`` (the submission's own
    digests bound the scan), then filtered on the recorded linkage. Reading the
    linkage rather than re-deriving a name is the §3 rule: catalog names are
    authoritative, and a disambiguated member's recorded name is not
    reproducible from the input set alone.

    The digest set comes from a query, not from ``submission.members``: that
    collection is loaded once per instance, so a caller that added members in
    the same session would silently narrow the scan to the members it happened
    to have loaded first.
    """
    digests = set(
        session.scalars(
            select(SubmissionMember.sha256).where(
                SubmissionMember.submission_id == submission.id
            )
        )
    )
    if not digests:
        return {}
    found: dict[int, BundleMember] = {}
    for row in session.scalars(
        select(BundleMember).where(BundleMember.logical_asset_hash.in_(digests))
    ):
        for submission_id, member_id in submission_links(row.source_metadata):
            if submission_id == submission.id:
                found[member_id] = row
    return found


def submission_bundles(session: Session, submission: Submission) -> list[Bundle]:
    """Return the submission's bundles ordered by ``opened_at``, then id."""
    bundle_ids = {
        row.bundle_id for row in submission_bundle_members(session, submission).values()
    }
    if not bundle_ids:
        return []
    return list(
        session.scalars(
            select(Bundle).where(Bundle.id.in_(bundle_ids)).order_by(Bundle.opened_at, Bundle.id)
        )
    )


def _refresh_submission_status(session: Session, submission: Submission) -> None:
    """Move the submission to its derived status.

    ``accumulated`` at append; ``archived`` only when the derived predicate
    says every member is on sealed, sufficiently-replicated media. The status
    is a projection of that predicate and never an independent claim — the
    erasure path reads the predicate itself.
    """
    if submission_is_archived(session, submission.id):
        if submission.status != SubmissionStatus.ARCHIVED:
            submission.status = SubmissionStatus.ARCHIVED
            submission.archived_at = dt.datetime.now(dt.UTC)
    elif submission.status != SubmissionStatus.ACCUMULATED:
        submission.status = SubmissionStatus.ACCUMULATED
        submission.archived_at = None
    session.flush()


def _result(
    session: Session,
    submission: Submission,
    *,
    noop: bool = False,
) -> ArchiveSubmissionResult:
    bundles = submission_bundles(session, submission)
    bundle_ids = tuple(bundle.id for bundle in bundles)
    copies: dict[str, tuple[int, ...]] = {}
    for bundle in bundles:
        copies[bundle.id] = tuple(
            session.scalars(
                select(Copy.id).where(Copy.bundle_id == bundle.id).order_by(Copy.id)
            )
        )
    return ArchiveSubmissionResult(
        submission_id=submission.id,
        bundle_ids=bundle_ids,
        copies_by_bundle=copies,
        archived=submission.status == SubmissionStatus.ARCHIVED,
        noop=noop,
    )


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


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True
