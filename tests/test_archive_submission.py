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

from sutradhara.archive_fanout import BuildArtifact, BuiltMember, ConformanceScan, MemberInput
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
)
from sutradhara.backend.port import BackendLocator, ByteRange, CopyRecord, VerifyResult
from sutradhara.catalog.models import (
    AssetLocator,
    Backend,
    Bundle,
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

    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        self._counter += 1
        if self.fail_on_write == self._counter:
            raise RuntimeError(f"configured write failure for {pool}")
        data = Path(source).read_bytes()
        digest = content_hash(hashlib.sha256(data).digest())
        object_id = f"{self._name}-{self._counter}"
        media_locator = (
            {"volume_uuid": object_id}
            if self._name == "d2"
            else {"tape_uuid": object_id}
        )
        self.objects[object_id] = data
        self.writes.append(pool)
        return CopyRecord(
            logical_id=digest,
            native_locator={
                "object_id": object_id,
                "content_sha256": digest.hex(),
                **media_locator,
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
        self.scans = 0

    def scan(
        self,
        *,
        bundle: Bundle,
        members: Sequence[MemberInput],
        ruleset: str,
    ) -> ConformanceScan:
        self.scans += 1
        raise AssertionError("map-mode submission archive must not call scan")

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


def test_archive_submission_fans_out_and_restores_arranged_member(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_submission(
        engine,
        tmp_path,
        moves={"clip-a.mov": "arranged/day-1/clip-a.mov"},
    )
    rem_backend = _WriteBackend("rem")
    d2_backend = _WriteBackend("d2")
    builder = _MapArchiveBuilder()

    with session_scope(engine) as session:
        result = archive_submission(
            session,
            setup.submission_id,
            backends={setup.rem_backend_id: rem_backend, setup.d2_backend_id: d2_backend},
            builder=builder,
            key_epoch=ARCHIVE_EPOCH,
        )

        assert result.archived is True
        assert result.noop is False
        assert result.bundle_id == f"submission-{setup.submission_id}"
        submission = session.get(Submission, setup.submission_id)
        assert submission is not None
        assert submission.status == SubmissionStatus.ARCHIVED
        bundle = session.get(Bundle, result.bundle_id)
        assert bundle is not None
        assert bundle.status == "sealed"
        copies = list(session.scalars(select(Copy).where(Copy.bundle_id == result.bundle_id)))
        assert copies
        assert all(copy.last_checked_at is not None for copy in copies)
        assert bundle.scan_summary == {
            "mode": "map",
            "source_map_path": str(Path(submission.source_map_path)),
        }

        locators = list(
            session.scalars(select(AssetLocator).where(AssetLocator.bundle_id == result.bundle_id))
        )
        assert len(locators) == 6
        by_pool = {
            locator.pool_id: locator
            for locator in locators
            if locator.member_path.endswith("clip-a.mov")
        }
        assert by_pool["working-pool"].native_locator["first_chunk_lba"] == 1
        assert "first_chunk_lba" in by_pool["offsite-pool"].native_locator
        assert "block_range" in by_pool["d2-shelf-pool"].native_locator

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

    assert builder.scans == 0
    assert [call[0] for call in builder.calls] == [
        Representation.RAO_PLAIN_V1,
        Representation.RAO_AEAD_V1,
    ]
    assert rem_backend.writes == ["working-pool", "offsite-pool"]
    assert d2_backend.writes == ["d2-shelf-pool"]


def test_archive_submission_noop_replay_writes_nothing(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_submission(engine, tmp_path)
    rem_backend = _WriteBackend("rem")
    d2_backend = _WriteBackend("d2")
    with session_scope(engine) as session:
        archive_submission(
            session,
            setup.submission_id,
            backends={setup.rem_backend_id: rem_backend, setup.d2_backend_id: d2_backend},
            builder=_MapArchiveBuilder(),
            key_epoch=ARCHIVE_EPOCH,
        )

    with session_scope(engine) as session:
        copy_count = session.scalar(select(func.count()).select_from(Copy))
        locator_count = session.scalar(select(func.count()).select_from(AssetLocator))
        result = archive_submission(
            session,
            setup.submission_id,
            backends={},
            builder=_MapArchiveBuilder(),
            key_epoch=ARCHIVE_EPOCH,
        )
        assert result.noop is True
        assert session.scalar(select(func.count()).select_from(Copy)) == copy_count
        assert session.scalar(select(func.count()).select_from(AssetLocator)) == locator_count


def test_partial_failure_rolls_back_catalog_and_retry_rearchives(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_submission(engine, tmp_path)
    rem_backend = _WriteBackend("rem", fail_on_write=2)
    d2_backend = _WriteBackend("d2")

    with (
        pytest.raises(RuntimeError, match="configured write failure"),
        session_scope(engine) as session,
    ):
        archive_submission(
            session,
            setup.submission_id,
            backends={setup.rem_backend_id: rem_backend, setup.d2_backend_id: d2_backend},
            builder=_MapArchiveBuilder(),
            key_epoch=ARCHIVE_EPOCH,
        )

    assert len(rem_backend.objects) == 1
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(Bundle)) == 0
        assert session.scalar(select(func.count()).select_from(Copy)) == 0
        assert session.scalar(select(func.count()).select_from(AssetLocator)) == 0
        submission = session.get(Submission, setup.submission_id)
        assert submission is not None
        assert submission.status == SubmissionStatus.PENDING_ARCHIVE

    rem_backend.fail_on_write = None
    with session_scope(engine) as session:
        result = archive_submission(
            session,
            setup.submission_id,
            backends={setup.rem_backend_id: rem_backend, setup.d2_backend_id: d2_backend},
            builder=_MapArchiveBuilder(),
            key_epoch=ARCHIVE_EPOCH,
        )
        assert result.archived is True
        assert session.scalar(select(func.count()).select_from(Bundle)) == 1


def test_source_drift_fails_before_build_or_write(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_submission(engine, tmp_path)
    source = setup.source_paths["clip-a.mov"]
    source.write_bytes(b"ALPHA-body")
    builder = _MapArchiveBuilder()
    rem_backend = _WriteBackend("rem")

    with pytest.raises(ArchiveSubmissionError, match="hashes to"), session_scope(engine) as session:
        archive_submission(
            session,
            setup.submission_id,
            backends={setup.rem_backend_id: rem_backend},
            builder=builder,
            key_epoch=ARCHIVE_EPOCH,
        )

    assert builder.calls == []
    assert rem_backend.writes == []


def test_source_root_escape_fails_before_build_or_write(
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
        assert member is not None
        member.source_path = str(outside)

    builder = _MapArchiveBuilder()
    rem_backend = _WriteBackend("rem")
    with (
        pytest.raises(ArchiveSubmissionError, match="escapes source root"),
        session_scope(engine) as session,
    ):
        archive_submission(
            session,
            setup.submission_id,
            backends={setup.rem_backend_id: rem_backend},
            builder=builder,
            key_epoch=ARCHIVE_EPOCH,
        )

    assert builder.calls == []
    assert rem_backend.writes == []


def test_archive_refuses_tampered_source_map(
    engine: Engine,
    tmp_path: Path,
) -> None:
    # Per-member source drift is covered elsewhere; this guards the frozen
    # source-map *receipt* itself (archive_submission._verified_source_map_path).
    setup = _create_submission(
        engine,
        tmp_path,
        moves={"clip-a.mov": "arranged/day-1/clip-a.mov"},
    )
    with session_scope(engine) as session:
        submission = session.get(Submission, setup.submission_id)
        assert submission is not None
        source_map = Path(submission.source_map_path)
    # Tamper the frozen receipt after submit, before archive.
    source_map.write_bytes(source_map.read_bytes() + b"# tampered\n")

    builder = _MapArchiveBuilder()
    rem_backend = _WriteBackend("rem")
    d2_backend = _WriteBackend("d2")
    with (
        pytest.raises(ArchiveSubmissionError, match="digest drifted"),
        session_scope(engine) as session,
    ):
        archive_submission(
            session,
            setup.submission_id,
            backends={setup.rem_backend_id: rem_backend, setup.d2_backend_id: d2_backend},
            builder=builder,
            key_epoch=ARCHIVE_EPOCH,
        )

    assert builder.calls == []
    assert rem_backend.writes == []
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(Bundle)) == 0
        assert session.scalar(select(func.count()).select_from(Copy)) == 0
        assert session.scalar(select(func.count()).select_from(AssetLocator)) == 0
        submission = session.get(Submission, setup.submission_id)
        assert submission is not None
        assert submission.status == SubmissionStatus.PENDING_ARCHIVE


def test_archive_refuses_missing_source_map(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_submission(engine, tmp_path)
    with session_scope(engine) as session:
        submission = session.get(Submission, setup.submission_id)
        assert submission is not None
        source_map = Path(submission.source_map_path)
    source_map.unlink()

    builder = _MapArchiveBuilder()
    rem_backend = _WriteBackend("rem")
    with (
        pytest.raises(ArchiveSubmissionError, match="source-map is missing"),
        session_scope(engine) as session,
    ):
        archive_submission(
            session,
            setup.submission_id,
            backends={setup.rem_backend_id: rem_backend},
            builder=builder,
            key_epoch=ARCHIVE_EPOCH,
        )

    assert builder.calls == []
    assert rem_backend.writes == []
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(Bundle)) == 0
        submission = session.get(Submission, setup.submission_id)
        assert submission is not None
        assert submission.status == SubmissionStatus.PENDING_ARCHIVE


def test_resolve_member_rejects_valid_but_absent_name(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_submission(
        engine,
        tmp_path,
        moves={"clip-a.mov": "arranged/day-1/clip-a.mov"},
    )
    with session_scope(engine) as session:
        archive_submission(
            session,
            setup.submission_id,
            backends={
                setup.rem_backend_id: _WriteBackend("rem"),
                setup.d2_backend_id: _WriteBackend("d2"),
            },
            builder=_MapArchiveBuilder(),
            key_epoch=ARCHIVE_EPOCH,
        )
        # The arranged member resolves to its asset hash...
        assert (
            resolve_member_asset_hash(
                session,
                artifactclass="s-masters",
                member_name="arranged/day-1/clip-a.mov",
            )
            == setup.asset_hashes["arranged/day-1/clip-a.mov"]
        )
        # ...but a well-formed name that was never arranged misses cleanly (not a crash).
        with pytest.raises(RestoreNameError, match="no catalog member"):
            resolve_member_asset_hash(
                session,
                artifactclass="s-masters",
                member_name="arranged/day-1/does-not-exist.mov",
            )
        # A real member name under the wrong artifactclass also misses (no cross-class leak).
        with pytest.raises(RestoreNameError, match="no catalog member"):
            resolve_member_asset_hash(
                session,
                artifactclass="s-proxy",
                member_name="arranged/day-1/clip-a.mov",
            )


def test_identity_mismatch_fails_before_backend_write(
    engine: Engine,
    tmp_path: Path,
) -> None:
    setup = _create_submission(
        engine,
        tmp_path,
        moves={"clip-a.mov": "arranged/day-1/clip-a.mov"},
    )
    builder = _MapArchiveBuilder(bad_ingest_path="arranged/day-1/clip-a.mov")
    rem_backend = _WriteBackend("rem")
    d2_backend = _WriteBackend("d2")

    with (
        pytest.raises(ArchiveSubmissionError, match="ingest_item_id"),
        session_scope(engine) as session,
    ):
        archive_submission(
            session,
            setup.submission_id,
            backends={setup.rem_backend_id: rem_backend, setup.d2_backend_id: d2_backend},
            builder=builder,
            key_epoch=ARCHIVE_EPOCH,
        )

    assert builder.calls
    assert rem_backend.writes == []
    assert d2_backend.writes == []


def test_long_paths_archive_through_widened_member_path_and_source_metadata(
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
        result = archive_submission(
            session,
            setup.submission_id,
            backends={
                setup.rem_backend_id: _WriteBackend("rem"),
                setup.d2_backend_id: _WriteBackend("d2"),
            },
            builder=_MapArchiveBuilder(),
            key_epoch=ARCHIVE_EPOCH,
        )
        assert result.archived is True
        bundle_member = session.scalar(select(Bundle).where(Bundle.id == result.bundle_id))
        assert bundle_member is not None
        assert (
            session.scalar(
                select(func.count())
                .select_from(AssetLocator)
                .where(AssetLocator.member_path == long_archive_path)
            )
            == 3
        )


def _install_policy(session: Session) -> tuple[int, int]:
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
) -> _Setup:
    with session_scope(engine) as session:
        rem_id, d2_id = _install_policy(session)
        items = _registered_intake(
            session,
            tmp_path,
            "intake-a",
            {
                "clip-a.mov": b"alpha-body",
                "clip-b.mov": b"beta-body",
            },
            long_source_for=long_source_for,
        )
        arrangement = create_from_intake(session, "intake-a", label="archive")
        for from_path, to_path in (moves or {}).items():
            move_member(session, arrangement.id, from_path, to_path)
        result = submit_arrangement(
            session,
            arrangement.id,
            submitted_by="tester",
            submission_root=tmp_path / "submissions",
            submission_id="submit-a",
        )
        source_paths = {
            relpath: Path(str(item.source_path)) for relpath, item in items.items()
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
        source_path=str(source),
        item_metadata={},
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
