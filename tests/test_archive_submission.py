"""Archive frozen arrangement submissions from their source-map rows."""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

from sutradhara.archive_bundle import (
    enqueue_artifact,
    submission_link_metadata,
    submission_links,
)
from sutradhara.archive_fanout import (
    ArchiveFanoutError,
    BuildArtifact,
    BuiltMember,
    MemberInput,
    flush_bundle,
)
from sutradhara.archive_restore import (
    RestoreNameError,
    resolve_member_asset_hash,
    restore_asset,
)
from sutradhara.archive_submission import ArchiveSubmissionError, archive_submission
from sutradhara.arrangement import create_from_intake, move_member, submit_arrangement
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    BundlingPolicy,
    DurabilityPolicy,
    PlacementPolicy,
    apply_artifactclass_policy,
    get_artifactclass_policy,
)
from sutradhara.backend.port import BackendLocator, ByteRange, CopyRecord, VerifyResult
from sutradhara.catalog.models import (
    AssetLocator,
    Backend,
    Bundle,
    BundleMember,
    Copy,
    IngestItem,
    Intake,
    LogicalAsset,
    Pool,
    Submission,
    SubmissionMember,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import (
    BackendKind,
    BackendTier,
    IntakeSourceKind,
    IntakeStatus,
    SubmissionStatus,
    content_hash,
)
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import RAO_CHUNK_SIZE

ARCHIVE_EPOCH = "archive-" + "a" * 32
RECOVERY_EPOCH = "recovery-" + "b" * 32


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


@dataclass(frozen=True)
class _Setup:
    submission_id: str
    rem_backend_id: int
    d2_backend_id: int
    source_paths: dict[str, Path]
    asset_hashes: dict[str, bytes]


class _WriteBackend:
    def __init__(self, name: str, *, fail_on_write: int | None = None) -> None:
        self._name = name
        self.fail_on_write = fail_on_write
        self._counter = 0
        self.objects: dict[str, bytes] = {}
        self.writes: list[str] = []

    @property
    def name(self) -> str:
        return self._name

    def write_object_to_pool(self, source: Path | str, pool: str, *, caller_object_id: str | None = None) -> CopyRecord:
        self._counter += 1
        if self.fail_on_write == self._counter:
            raise RuntimeError(f"configured write failure for {pool}")
        data = Path(source).read_bytes()
        digest = content_hash(hashlib.sha256(data).digest())
        object_id = f"{self._name}-{self._counter}"
        self.objects[object_id] = data
        self.writes.append(pool)
        return CopyRecord(
            logical_id=digest,
            native_locator={
                "object_id": object_id,
                "content_sha256": digest.hex(),
            },
            integrity_hash=digest,
            size_bytes=len(data),
        )

    def enumerate(self) -> Iterator[CopyRecord]:
        return iter(())

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        data = self.objects[str(locator["object_id"])]
        if byte_range.is_whole_object:
            return data
        return data[byte_range.start : byte_range.end]

    def verify(self, locator: BackendLocator) -> VerifyResult:
        data = self.read_range(locator, ByteRange(0, 0))
        actual = content_hash(hashlib.sha256(data).digest())
        expected = content_hash(bytes.fromhex(str(locator["content_sha256"])))
        return VerifyResult(ok=actual == expected, measured=True, actual_hash=actual)


class _MapArchiveBuilder:
    def __init__(self, *, bad_ingest_path: str | None = None) -> None:
        self.bad_ingest_path = bad_ingest_path
        self.calls: list[tuple[Representation, Path | None, Path | None, str | None]] = []

    # No `scan` method by design: scanning left the ArchiveBuilder boundary
    # entirely (design §4 — it lives at enqueue-batch grain now), so a stub
    # that raises on a call nothing can make guards nothing.

    def build(
        self,
        *,
        bundle: Bundle,
        members: Sequence[MemberInput],
        representation: Representation,
        ruleset: str,
        key_epoch: str | None,
        work_dir: Path,
        map_path: Path | None = None,
        source_root: Path | None = None,
        map_sha256: str | None = None,
    ) -> BuildArtifact:
        assert map_path is not None
        assert source_root is not None
        assert map_sha256 is not None
        self.calls.append((representation, map_path, source_root, map_sha256))
        by_path = _source_map_ingest_ids(map_path)
        ext = ".aead" if representation is Representation.RAO_AEAD_V1 else ".rao"
        artifact_path = work_dir / f"{bundle.id}-{representation.value}{ext}"
        size = 0
        payloads: list[tuple[MemberInput, bytes, int]] = []
        for index, member in enumerate(members):
            data = member.source_path.read_bytes()
            first_lba = index + 1
            size = max(size, first_lba * RAO_CHUNK_SIZE + len(data))
            payloads.append((member, data, first_lba))
        object_bytes = bytearray(b"\0" * size)
        built: list[BuiltMember] = []
        for member, data, first_lba in payloads:
            start = first_lba * RAO_CHUNK_SIZE
            object_bytes[start : start + len(data)] = data
            ingest_item_id = by_path[member.member_path]
            if member.member_path == self.bad_ingest_path:
                ingest_item_id = f"wrong-{ingest_item_id}"
            built.append(
                BuiltMember(
                    logical_asset_hash=member.logical_asset_hash,
                    member_path=member.member_path,
                    size_bytes=member.size_bytes,
                    file_sha256=member.file_sha256,
                    native_locator={
                        "member_path": member.member_path,
                        "first_chunk_lba": first_lba,
                        "size_bytes": member.size_bytes,
                    },
                    ingest_item_id=ingest_item_id,
                )
            )
        artifact_path.write_bytes(object_bytes)
        return BuildArtifact(
            artifact_path=artifact_path,
            stored_digest=hashlib.sha256(object_bytes).digest(),
            members=tuple(built),
            manifest_path=None,
            recipient_epochs=(
                (key_epoch or ARCHIVE_EPOCH, RECOVERY_EPOCH)
                if representation is Representation.RAO_AEAD_V1
                else ()
            ),
        )

    def verify_member_copy(
        self,
        *,
        backend: _WriteBackend,
        copy_locator: dict[str, Any],
        member: BuiltMember,
        representation: Representation,
        storage_metadata: Mapping[str, Any],
        work_dir: Path,
    ) -> bytes:
        start = int(member.native_locator["first_chunk_lba"]) * RAO_CHUNK_SIZE
        return backend.read_range(copy_locator, ByteRange(start, start + member.size_bytes))


def _flush_all(
    engine: Engine,
    setup: _Setup,
    *,
    builder: Any = None,
) -> tuple[_WriteBackend, _WriteBackend]:
    """Seal every open bundle, the way the sweeper does. Returns the backends."""
    rem_backend = _WriteBackend("rem")
    d2_backend = _WriteBackend("d2")
    with session_scope(engine) as session:
        for bundle in session.scalars(select(Bundle).where(Bundle.status == "open")):
            flush_bundle(
                session,
                bundle_id=bundle.id,
                backends={
                    setup.rem_backend_id: rem_backend,
                    setup.d2_backend_id: d2_backend,
                },
                builder=builder or _MapArchiveBuilder(),
                key_epoch=ARCHIVE_EPOCH,
            )
    return rem_backend, d2_backend


def test_two_submissions_and_intake_members_converge_into_one_object(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """The point of the arc, at the submission seam.

    Guards: the retired ``bundle-{submission-id}`` contract coming back. Each
    submission used to mint and immediately flush its own object, so three
    small deliveries with identical storage placement became three small
    objects — three bootstrap rows, three short seals. They now share one
    accumulator and seal as one.
    """
    first = _create_submission(engine, tmp_path, intake_id="intake-a", submission_id="submit-a")
    second = _create_submission(
        engine,
        tmp_path,
        intake_id="intake-b",
        submission_id="submit-b",
        files={"clip-c.mov": b"gamma-body", "clip-d.mov": b"delta-body"},
    )

    with session_scope(engine) as session:
        first_result = archive_submission(session, first.submission_id)
        second_result = archive_submission(session, second.submission_id)
        # An intake enqueue for the same class lands in the same accumulator.
        extra = tmp_path / "intake-extra.tif"
        extra.write_bytes(b"extra-body")
        extra_hash = hashlib.sha256(b"extra-body").digest()
        session.add(LogicalAsset(content_sha256=extra_hash, size_bytes=len(b"extra-body")))
        session.flush()
        accumulator, _, _ = enqueue_artifact(
            session,
            artifactclass="s-masters",
            policy=get_artifactclass_policy(session, "s-masters"),
            logical_asset_hash=extra_hash,
            source_path=extra,
            member_path="intake-extra.tif",
        )

        assert first_result.bundle_ids == second_result.bundle_ids == (accumulator.id,)
        assert accumulator.status == "open"
        assert accumulator.member_count == 5
        # Nothing is on media yet, so neither submission is archived.
        assert first_result.archived is False
        assert second_result.archived is False
        assert session.get(Submission, first.submission_id).status == (
            SubmissionStatus.ACCUMULATED
        )

    rem_backend, d2_backend = _flush_all(engine, first)
    with session_scope(engine) as session:
        bundles = list(session.scalars(select(Bundle)))
        assert len(bundles) == 1
        assert bundles[0].status == "sealed"
        assert bundles[0].member_count == 5
    # One object per pool, not one per submission.
    assert rem_backend.writes == ["offsite-pool", "working-pool"]
    assert d2_backend.writes == ["d2-shelf-pool"]


def test_a_co_resident_enqueue_does_not_swallow_the_submission_linkage(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """Guards: the linkage dropped on an idempotent member hit.

    The intake path enqueues one of the submission's files first — same class,
    same archive path, same content hash. The submission's append then lands on
    that existing row, and ``add_bundle_member`` used to return it untouched,
    silently discarding ``submission_links``. The member was then invisible to
    ``submission_bundle_members``: ``archive_submission`` could never reach its
    noop branch, so it re-hashed every byte of the submission on every call,
    the member's bundle was missing from the reported result, and the pre-write
    identity gate skipped that member as a co-resident.

    The second call must therefore noop **without touching the sources at
    all**, which is what deleting them before it proves.
    """
    setup = _create_submission(engine, tmp_path)
    with session_scope(engine) as session:
        members = list(
            session.scalars(
                select(SubmissionMember)
                .where(SubmissionMember.submission_id == setup.submission_id)
                .order_by(SubmissionMember.ord)
            )
        )
        co_resident = members[0]
        enqueue_artifact(
            session,
            artifactclass="s-masters",
            policy=get_artifactclass_policy(session, "s-masters"),
            logical_asset_hash=co_resident.sha256,
            source_path=Path(co_resident.source_path),
            member_path=co_resident.archive_path,
            member_path_is_escaped=True,
            size_bytes=co_resident.size_bytes,
            file_sha256=co_resident.sha256,
        )
        assert session.scalar(select(func.count()).select_from(BundleMember)) == 1

        result = archive_submission(session, setup.submission_id)
        assert session.scalar(select(func.count()).select_from(BundleMember)) == len(members)
        assert len(result.bundle_ids) == 1

        # The co-resident row carries the linkage, merged rather than replaced.
        row = session.scalars(
            select(BundleMember).where(
                BundleMember.logical_asset_hash == co_resident.sha256
            )
        ).one()
        assert submission_links(row.source_metadata) == [
            (setup.submission_id, co_resident.id)
        ]

        for member in members:
            Path(member.source_path).unlink()
        again = archive_submission(session, setup.submission_id)
        assert again.noop is True
        assert again.bundle_ids == result.bundle_ids
        assert session.scalar(select(func.count()).select_from(BundleMember)) == len(members)


def test_two_submissions_of_identical_content_both_keep_their_linkage(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """Guards: last-writer-wins on a single-valued linkage.

    Duplicate content across two deliveries is a legitimate workflow, and both
    submissions' members converge on the same bundle member row. A scalar
    ``submission_id`` would let the second append steal the first's linkage —
    and then each call would take it back from the other, so neither submission
    ever reaches noop and each re-hashes its whole delivery every time.
    """
    same = {"clip-a.mov": b"alpha-body", "clip-b.mov": b"beta-body"}
    first = _create_submission(
        engine, tmp_path / "one", intake_id="intake-a", submission_id="submit-a", files=same
    )
    second = _create_submission(
        engine, tmp_path / "two", intake_id="intake-b", submission_id="submit-b", files=same
    )

    with session_scope(engine) as session:
        first_result = archive_submission(session, first.submission_id)
        second_result = archive_submission(session, second.submission_id)
        # One row per (class, path, content), shared by both submissions.
        assert session.scalar(select(func.count()).select_from(BundleMember)) == 2
        assert first_result.bundle_ids == second_result.bundle_ids
        assert len(first_result.bundle_ids) == 1

        for row in session.scalars(select(BundleMember)):
            assert {link[0] for link in submission_links(row.source_metadata)} == {
                first.submission_id,
                second.submission_id,
            }
        # Neither submission has to re-derive anything on a second call.
        assert archive_submission(session, first.submission_id).noop is True
        assert archive_submission(session, second.submission_id).noop is True


def test_result_shape_carries_bundle_ids_by_opened_at_and_copies_per_bundle(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """A submission split across a seal boundary.

    Guards: the flat ``bundle_id``/``copy_ids`` pair, which cannot express a
    split submission — it would name one of the two bundles and silently drop
    the other's copies from the caller's view. The result is
    ``bundle_ids`` ordered by ``opened_at`` plus a per-bundle copy mapping,
    identical on a re-entrant call.
    """
    setup = _create_submission(engine, tmp_path)
    with session_scope(engine) as session:
        members = list(
            session.scalars(
                select(SubmissionMember)
                .where(SubmissionMember.submission_id == setup.submission_id)
                .order_by(SubmissionMember.ord)
            )
        )
        # Append the first member, then seal the accumulator under it, so the
        # second member has to open a new one.
        first_only = members[0]
        remaining = members[1:]
        for member in remaining:
            session.delete(member)
        session.flush()
        archive_submission(session, setup.submission_id)
        first_bundle_id = session.scalars(
            select(Bundle.id).where(Bundle.status == "open")
        ).one()

    _flush_all(engine, setup)

    with session_scope(engine) as session:
        submission = session.get(Submission, setup.submission_id)
        for ordinal, member in enumerate(remaining, start=1):
            session.add(
                SubmissionMember(
                    submission_id=submission.id,
                    ingest_item_id=member.ingest_item_id,
                    archive_path=member.archive_path,
                    source_path=member.source_path,
                    sha256=member.sha256,
                    size_bytes=member.size_bytes,
                    ord=ordinal,
                )
            )
        session.flush()
        result = archive_submission(session, submission.id)
        second_bundle_id = next(
            bundle_id for bundle_id in result.bundle_ids if bundle_id != first_bundle_id
        )
        assert result.bundle_ids == (first_bundle_id, second_bundle_id)
        # Ordered by opened_at: the sealed bundle first.
        assert list(result.copies_by_bundle[first_bundle_id]) != []
        assert result.copies_by_bundle[second_bundle_id] == ()
        assert result.archived is False

        # Re-entrant call: same shape, no second append, no duplicate members.
        replay = archive_submission(session, submission.id)
        assert replay.noop is True
        assert replay.bundle_ids == result.bundle_ids
        assert replay.copies_by_bundle == result.copies_by_bundle
        assert (
            session.scalar(
                select(func.count())
                .select_from(BundleMember)
                .where(BundleMember.bundle_id == second_bundle_id)
            )
            == 1
        )
    assert first_only is members[0]


def test_crash_retry_after_a_partial_append_is_a_no_op_for_landed_members(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """Guards: a retry duplicating the members that already landed.

    Re-entrancy is a member-presence check over ``SubmissionMember``, not a
    bundle-id probe — the per-submission bundle id no longer exists, so a
    crash-retry has nothing to look up and must reason from the members
    themselves.
    """
    setup = _create_submission(engine, tmp_path)
    with session_scope(engine) as session:
        members = list(
            session.scalars(
                select(SubmissionMember)
                .where(SubmissionMember.submission_id == setup.submission_id)
                .order_by(SubmissionMember.ord)
            )
        )
        # Simulate a crash after the first member appended: append it by hand
        # through the same funnel, with the same recorded linkage.
        enqueue_artifact(
            session,
            artifactclass="s-masters",
            policy=get_artifactclass_policy(session, "s-masters"),
            logical_asset_hash=members[0].sha256,
            source_path=Path(members[0].source_path),
            member_path=members[0].archive_path,
            member_path_is_escaped=True,
            size_bytes=members[0].size_bytes,
            file_sha256=members[0].sha256,
            source_metadata=submission_link_metadata(setup.submission_id, members[0].id),
        )
        member_count_before = session.scalar(select(func.count()).select_from(BundleMember))
        assert member_count_before == 1

        result = archive_submission(session, setup.submission_id)
        assert session.scalar(select(func.count()).select_from(BundleMember)) == len(members)
        assert len(result.bundle_ids) == 1

        # A second retry adds nothing at all.
        again = archive_submission(session, setup.submission_id)
        assert again.noop is True
        assert session.scalar(select(func.count()).select_from(BundleMember)) == len(members)


def test_identity_mismatch_is_caught_before_any_physical_write(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """P4 gate condition C3, closed.

    The old per-submission validator early-returned for representations
    outside the RAO family, and basis-ordered fan-out sorts the D2 shelf pool
    first — so an identity mismatch was caught only *after* the shelf write and
    left a media-only orphan. The identity gate is now catalog-grain and runs
    once, before any build and before any physical write, for every
    representation. Both backends must be untouched.
    """
    setup = _create_submission(engine, tmp_path)
    with session_scope(engine) as session:
        archive_submission(session, setup.submission_id)
        # The submission row and the catalog member now disagree: exactly the
        # class of defect the pre-write gate exists to catch.
        member = session.scalars(
            select(SubmissionMember)
            .where(SubmissionMember.submission_id == setup.submission_id)
            .order_by(SubmissionMember.ord)
        ).first()
        member.size_bytes = member.size_bytes + 1
        session.flush()
        bundle_id = session.scalars(select(Bundle.id).where(Bundle.status == "open")).one()

    builder = _MapArchiveBuilder()
    rem_backend = _WriteBackend("rem")
    d2_backend = _WriteBackend("d2")
    with (
        pytest.raises(ArchiveFanoutError, match="submission member"),
        session_scope(engine) as session,
    ):
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends={setup.rem_backend_id: rem_backend, setup.d2_backend_id: d2_backend},
            builder=builder,
            key_epoch=ARCHIVE_EPOCH,
        )

    # Nothing was built and nothing reached ANY backend — no media-only orphan.
    assert builder.calls == []
    assert rem_backend.writes == []
    assert d2_backend.writes == []
    with session_scope(engine) as session:
        assert list(session.scalars(select(Copy))) == []
        assert list(session.scalars(select(AssetLocator))) == []
        # And the un-claim: the bundle is open and flushable again once fixed.
        assert session.get(Bundle, bundle_id).status == "open"
        assert session.get(Bundle, bundle_id).claimed_by is None


def test_builder_that_echoes_a_wrong_ingest_item_id_is_caught_before_its_write(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """Guards: a builder mis-associating a member with someone else's lineage.

    Every target is built and validated before the first
    ``write_object_to_pool``, so a defect the builder introduces on the second
    representation still costs no media.
    """
    setup = _create_submission(
        engine,
        tmp_path,
        moves={"clip-a.mov": "arranged/day-1/clip-a.mov"},
    )
    with session_scope(engine) as session:
        archive_submission(session, setup.submission_id)
        bundle_id = session.scalars(select(Bundle.id).where(Bundle.status == "open")).one()

    builder = _MapArchiveBuilder(bad_ingest_path="arranged/day-1/clip-a.mov")
    rem_backend = _WriteBackend("rem")
    d2_backend = _WriteBackend("d2")
    with (
        pytest.raises(ArchiveFanoutError, match="echoed ingest_item_id"),
        session_scope(engine) as session,
    ):
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends={setup.rem_backend_id: rem_backend, setup.d2_backend_id: d2_backend},
            builder=builder,
            key_epoch=ARCHIVE_EPOCH,
        )

    assert builder.calls
    assert rem_backend.writes == []
    # The D2 shelf pool sorts first in basis order and used to be written
    # before the RAO-family mismatch surfaced. It is not written now.
    assert d2_backend.writes == []
    with session_scope(engine) as session:
        assert list(session.scalars(select(Copy))) == []
        assert list(session.scalars(select(AssetLocator))) == []


def test_accumulated_material_restores_and_reports_archived_once_verified(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """The whole path: append, seal, restore an arranged member by name, and
    only then report the submission archived.

    Guards: reporting archived at append (material in an open bundle is not
    archive evidence) and losing member-name resolution across the retired
    per-submission bundle.
    """
    setup = _create_submission(
        engine,
        tmp_path,
        moves={"clip-a.mov": "arranged/day-1/clip-a.mov"},
    )
    with session_scope(engine) as session:
        result = archive_submission(session, setup.submission_id)
        assert result.archived is False
        assert result.copy_ids == ()

    rem_backend, d2_backend = _flush_all(engine, setup)

    with session_scope(engine) as session:
        result = archive_submission(session, setup.submission_id)
        assert result.noop is True
        assert result.archived is True
        assert session.get(Submission, setup.submission_id).status == SubmissionStatus.ARCHIVED
        assert len(result.copy_ids) == 3

        resolved = resolve_member_asset_hash(
            session,
            artifactclass="s-masters",
            member_name="arranged/day-1/clip-a.mov",
        )
        assert resolved == setup.asset_hashes["arranged/day-1/clip-a.mov"]
        restored = tmp_path / "restored" / "clip-a.mov"
        restore_asset(
            session,
            asset_hash=resolved,
            artifactclass="s-masters",
            destination=restored,
            backends={setup.rem_backend_id: rem_backend, setup.d2_backend_id: d2_backend},
        )
        assert restored.read_bytes() == b"alpha-body"

    # A real member name that was never arranged still misses cleanly.
    with session_scope(engine) as session, pytest.raises(RestoreNameError, match="no catalog"):
        resolve_member_asset_hash(
            session,
            artifactclass="s-masters",
            member_name="arranged/day-1/does-not-exist.mov",
        )


def test_source_drift_fails_before_any_append(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """Guards: appending a member whose bytes no longer match the frozen
    manifest. The digest check runs at append, which is now the only place
    the submission touches the source at all."""
    setup = _create_submission(engine, tmp_path)
    setup.source_paths["clip-a.mov"].write_bytes(b"ALPHA-body")

    with pytest.raises(ArchiveSubmissionError, match="hashes to"), session_scope(engine) as session:
        archive_submission(session, setup.submission_id)

    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(BundleMember)) == 0
        assert session.get(Submission, setup.submission_id).status == (
            SubmissionStatus.PENDING_ARCHIVE
        )


def test_source_root_escape_fails_before_any_append(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_submission(engine, tmp_path)
    outside = tmp_path / "outside.mov"
    outside.write_bytes(b"alpha-body")
    with session_scope(engine) as session:
        member = session.scalar(
            select(SubmissionMember).where(SubmissionMember.submission_id == setup.submission_id)
        )
        member.source_path = str(outside)

    with (
        pytest.raises(ArchiveSubmissionError, match="escapes source root"),
        session_scope(engine) as session,
    ):
        archive_submission(session, setup.submission_id)

    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(BundleMember)) == 0


def test_archive_refuses_tampered_source_map(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """The frozen submit-time map is no longer the build instruction, but it is
    still the arrangement's integrity artifact and its digest still gates the
    append (``_verified_source_map_path``)."""
    setup = _create_submission(
        engine,
        tmp_path,
        moves={"clip-a.mov": "arranged/day-1/clip-a.mov"},
    )
    with session_scope(engine) as session:
        source_map = Path(session.get(Submission, setup.submission_id).source_map_path)
    source_map.write_bytes(source_map.read_bytes() + b"# tampered\n")

    with (
        pytest.raises(ArchiveSubmissionError, match="digest drifted"),
        session_scope(engine) as session,
    ):
        archive_submission(session, setup.submission_id)

    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(BundleMember)) == 0
        assert session.get(Submission, setup.submission_id).status == (
            SubmissionStatus.PENDING_ARCHIVE
        )


def test_archive_refuses_missing_source_map(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_submission(engine, tmp_path)
    with session_scope(engine) as session:
        Path(session.get(Submission, setup.submission_id).source_map_path).unlink()

    with (
        pytest.raises(ArchiveSubmissionError, match="source-map is missing"),
        session_scope(engine) as session,
    ):
        archive_submission(session, setup.submission_id)

    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(BundleMember)) == 0


def test_long_paths_accumulate_and_archive_through_the_group_bundle(
    engine: Engine,
    tmp_path: Path,
) -> None:
    long_archive_path = "/".join(["arranged", *[f"segment{i:03d}" for i in range(95)], "clip.mov"])
    assert 1024 < len(long_archive_path) < 2048
    setup = _create_submission(
        engine,
        tmp_path,
        moves={"clip-a.mov": long_archive_path},
        long_source_for="clip-a.mov",
    )
    assert len(str(setup.source_paths["clip-a.mov"])) > 2048

    with session_scope(engine) as session:
        archive_submission(session, setup.submission_id)
    _flush_all(engine, setup)

    with session_scope(engine) as session:
        result = archive_submission(session, setup.submission_id)
        assert result.archived is True
        assert (
            session.scalar(
                select(func.count())
                .select_from(AssetLocator)
                .where(AssetLocator.member_path == long_archive_path)
            )
            == 3
        )


def _install_policy(session: Session) -> tuple[int, int]:
    existing_rem = session.scalars(select(Backend).where(Backend.name == "rem")).first()
    if existing_rem is not None:
        d2 = session.scalars(select(Backend).where(Backend.name == "d2")).one()
        return existing_rem.id, d2.id
    rem = Backend(name="rem", kind=BackendKind.REM_TAPE, tier=BackendTier.SELF_DESCRIBING)
    d2 = Backend(name="d2", kind=BackendKind.D2_TAPE, tier=BackendTier.SELF_DESCRIBING)
    session.add_all([rem, d2])
    session.flush()
    session.add_all(
        [
            Pool(
                id="working-pool",
                backend_id=rem.id,
                representation=Representation.RAO_PLAIN_V1.value,
            ),
            Pool(
                id="offsite-pool",
                backend_id=rem.id,
                representation=Representation.RAO_AEAD_V1.value,
            ),
            Pool(
                id="d2-shelf-pool",
                backend_id=d2.id,
                representation=Representation.D2TAR_RAW.value,
            ),
        ]
    )
    session.flush()
    apply_artifactclass_policy(
        session,
        "s-masters",
        ArtifactClassPolicy(
            ruleset="rao.s.v1",
            placements=(
                PlacementPolicy("working-pool", role="primary"),
                PlacementPolicy("offsite-pool", role="offsite"),
                PlacementPolicy("d2-shelf-pool", role="shelf"),
            ),
            bundling=BundlingPolicy(target_gb=1, max_age_seconds=60),
            restore_preference=("working-pool", "offsite-pool", "d2-shelf-pool"),
            expect="compliant",
            durability=DurabilityPolicy(min_copies=3, min_impl_families=2),
        ),
    )
    return rem.id, d2.id


def _create_submission(
    engine: Engine,
    tmp_path: Path,
    *,
    moves: dict[str, str] | None = None,
    long_source_for: str | None = None,
    intake_id: str = "intake-a",
    submission_id: str = "submit-a",
    files: dict[str, bytes] | None = None,
) -> _Setup:
    with session_scope(engine) as session:
        rem_id, d2_id = _install_policy(session)
        items = _registered_intake(
            session,
            tmp_path,
            intake_id,
            files
            or {
                "clip-a.mov": b"alpha-body",
                "clip-b.mov": b"beta-body",
            },
            long_source_for=long_source_for,
        )
        arrangement = create_from_intake(session, intake_id, label="archive")
        for from_path, to_path in (moves or {}).items():
            move_member(session, arrangement.id, from_path, to_path)
        result = submit_arrangement(
            session,
            arrangement.id,
            submitted_by="tester",
            submission_root=tmp_path / "submissions",
            submission_id=submission_id,
        )
        source_paths = {
            relpath: Path(str(item.item_metadata["source_path"])) for relpath, item in items.items()
        }
        asset_hashes = {
            member.archive_path: member.sha256
            for member in session.scalars(
                select(SubmissionMember).where(
                    SubmissionMember.submission_id == result.submission_id
                )
            )
        }
        return _Setup(
            submission_id=result.submission_id,
            rem_backend_id=rem_id,
            d2_backend_id=d2_id,
            source_paths=source_paths,
            asset_hashes=asset_hashes,
        )


def _registered_intake(
    session: Session,
    tmp_path: Path,
    intake_id: str,
    files: dict[str, bytes],
    *,
    long_source_for: str | None = None,
) -> dict[str, IngestItem]:
    intake_root = tmp_path / intake_id
    session.add(
        Intake(
            intake_id=intake_id,
            operator="tester",
            source_kind=IntakeSourceKind.CARD,
            source_ref="card-a",
            artifactclass="s-masters",
            label=intake_id,
            manifest_path=str(intake_root / "manifest-sha256.txt"),
            manifest_digest="manifest",
            status=IntakeStatus.REGISTERED,
            registered_at=dt.datetime.now(dt.UTC),
        )
    )
    session.flush()
    return {
        relpath: _add_item(
            session,
            intake_root,
            relpath,
            data,
            long_source=relpath == long_source_for,
        )
        for relpath, data in files.items()
    }


def _add_item(
    session: Session,
    intake_root: Path,
    relpath: str,
    data: bytes,
    *,
    long_source: bool,
) -> IngestItem:
    if long_source:
        source = _long_source_path(intake_root / "data")
    else:
        source = intake_root / "data" / relpath
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(data)
    digest = hashlib.sha256(data).digest()
    # Idempotent: two intakes may legitimately deliver the same bytes, which is
    # exactly the duplicate-content case one test below exercises.
    if session.get(LogicalAsset, digest) is None:
        session.add(LogicalAsset(content_sha256=digest, size_bytes=len(data)))
        session.flush()
    item = IngestItem(
        intake_id=intake_root.name,
        logical_asset_hash=digest,
        as_received_path=relpath,
        virtual_path=relpath,
        st_dev=source.stat().st_dev,
        st_ino=source.stat().st_ino,
        size_bytes=len(data),
        artifactclass="s-masters",
        item_metadata={"source_path": str(source)},
    )
    session.add(item)
    session.flush()
    return item


def _long_source_path(root: Path) -> Path:
    current = root
    index = 0
    while len(str(current / "clip.mov")) <= 2050:
        current = current / f"segment-{index:03d}"
        index += 1
    return current / "clip.mov"


def _source_map_ingest_ids(map_path: Path) -> dict[str, str]:
    lines = map_path.read_text(encoding="utf-8").splitlines()
    result: dict[str, str] = {}
    for line in lines[1:]:
        archive_path, _source_path, _sha256, _size, ingest_item_id = line.split("\t")
        result[archive_path] = ingest_item_id
    return result
