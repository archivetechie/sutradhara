from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import sutradhara.arrangement as arrangement_module
from sutradhara.arrangement import (
    ArrangementError,
    ArrangementFrozen,
    ArrangementSubmitRace,
    create_from_arrangement,
    create_from_intake,
    exclude_member,
    include_member,
    move_member,
    show_arrangement,
    submit_arrangement,
)
from sutradhara.catalog.models import (
    Arrangement,
    AssetDerivation,
    IngestItem,
    Intake,
    LogicalAsset,
    Submission,
    SubmissionMember,
)
from sutradhara.catalog.session import create_all, make_engine, make_session_factory, session_scope
from sutradhara.catalog.types import (
    ArrangementStatus,
    IntakeSourceKind,
    IntakeStatus,
    SubmissionStatus,
)


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    try:
        yield eng
    finally:
        eng.dispose()


def test_create_from_intake_selects_masters_only(engine: Engine, tmp_path: Path) -> None:
    with session_scope(engine) as session:
        masters = _registered_intake(session, tmp_path, "intake-a", ["DCIM/A.mov", "DCIM/B.mov"])
        derived = _add_item(session, tmp_path, "intake-a", "proxy/A.mp4", b"proxy")
        session.add(
            AssetDerivation(
                source_item_id=masters[0].id,
                derived_item_id=derived.id,
                kind="preview",
            )
        )

        arrangement = create_from_intake(session, "intake-a", label="cut")

        assert arrangement.status == ArrangementStatus.DRAFT
        assert [member.ingest_item_id for member in arrangement.members] == [
            masters[0].id,
            masters[1].id,
        ]
        assert [member.member_path for member in arrangement.members] == [
            "DCIM/A.mov",
            "DCIM/B.mov",
        ]

        intake = session.get(Intake, "intake-a")
        assert intake is not None
        intake.status = IntakeStatus.VERIFYING
        with pytest.raises(ArrangementError, match="requires registered"):
            create_from_intake(session, "intake-a", label="bad")


def test_move_touches_only_arrangement(engine: Engine, tmp_path: Path) -> None:
    with session_scope(engine) as session:
        item = _registered_intake(session, tmp_path, "intake-b", ["DCIM/A.mov"])[0]
        original = (item.as_received_path, item.virtual_path, item.item_metadata["source_path"])
        arrangement = create_from_intake(session, "intake-b", label="moves")

        move_member(session, arrangement.id, "DCIM/A.mov", "satsang/day-1/A.mov")

        assert (
            item.as_received_path,
            item.virtual_path,
            item.item_metadata["source_path"],
        ) == original
        assert Path(str(item.item_metadata["source_path"])).exists()
        assert arrangement.members[0].member_path == "satsang/day-1/A.mov"


def test_exclude_frees_archive_path_and_submit_omits_excluded(
    engine: Engine,
    tmp_path: Path,
) -> None:
    submission_root = tmp_path / "submissions"
    with session_scope(engine) as session:
        items = _registered_intake(session, tmp_path, "intake-c", ["foo.mov", "bar.mov"])
        arrangement = create_from_intake(session, "intake-c", label="exclude")

        with pytest.raises(ArrangementError, match="duplicate"):
            move_member(session, arrangement.id, "bar.mov", "foo.mov")
        exclude_member(session, arrangement.id, "foo.mov")
        move_member(session, arrangement.id, "bar.mov", "foo.mov")
        result = submit_arrangement(
            session,
            arrangement.id,
            submitted_by="tester",
            submission_root=submission_root,
            submission_id="exclude-submit",
        )

        rows = result.source_map_path.read_text(encoding="utf-8").splitlines()
        assert rows[0] == "archive_path\tsource_path\tsha256\tsize\tingest_item_id"
        assert len(rows) == 2
        submitted = rows[1].split("\t")
        assert submitted[0] == "foo.mov"
        assert submitted[4] == str(items[1].id)


def test_include_member_restores_excluded_member(engine: Engine, tmp_path: Path) -> None:
    with session_scope(engine) as session:
        _registered_intake(session, tmp_path, "intake-include", ["foo.mov"])
        arrangement = create_from_intake(session, "intake-include", label="include")

        exclude_member(session, arrangement.id, "foo.mov")
        restored = include_member(session, arrangement.id, "foo.mov")

        assert restored.excluded is False
        assert arrangement.members[0].excluded is False


def test_include_member_rejects_active_path_collision(engine: Engine, tmp_path: Path) -> None:
    with session_scope(engine) as session:
        _registered_intake(session, tmp_path, "intake-include-collide", ["foo.mov", "bar.mov"])
        arrangement = create_from_intake(session, "intake-include-collide", label="include")

        exclude_member(session, arrangement.id, "foo.mov")
        move_member(session, arrangement.id, "bar.mov", "foo.mov")
        with pytest.raises(ArrangementError, match="duplicate"):
            include_member(session, arrangement.id, "foo.mov")


def test_submit_emits_source_map_manifest_and_queryable_rows(
    engine: Engine,
    tmp_path: Path,
) -> None:
    submission_root = tmp_path / "submissions"
    with session_scope(engine) as session:
        items = _registered_intake(session, tmp_path, "intake-d", ["z.mov", "a.mov"])
        arrangement = create_from_intake(session, "intake-d", label="submit")

        result = submit_arrangement(
            session,
            arrangement.id,
            submitted_by="tester",
            submission_root=submission_root,
            submission_id="submit-ok",
        )

        source_map = result.source_map_path.read_text(encoding="utf-8")
        digest = hashlib.sha256(source_map.encode("utf-8")).hexdigest()
        assert result.manifest_digest == digest
        assert (submission_root / "submit-ok" / "manifest-sha256.txt").read_text(
            encoding="utf-8"
        ) == f"{digest}  source-map.tsv\n"
        assert (submission_root / "submit-ok" / "submission.json").exists()

        lines = source_map.splitlines()
        assert [line.split("\t")[0] for line in lines[1:]] == ["a.mov", "z.mov"]

        submission = session.get(Submission, "submit-ok")
        assert submission is not None
        assert submission.arrangement_id == arrangement.id
        assert submission.manifest_digest == digest
        assert submission.member_count == 2
        assert submission.status == SubmissionStatus.PENDING_ARCHIVE
        mirror = list(
            session.scalars(
                select(SubmissionMember)
                .where(SubmissionMember.submission_id == "submit-ok")
                .order_by(SubmissionMember.ord)
            )
        )
        assert [row.archive_path for row in mirror] == ["a.mov", "z.mov"]
        assert [row.sha256 for row in mirror] == [
            items[1].logical_asset_hash,
            items[0].logical_asset_hash,
        ]
        assert [row.source_path for row in mirror] == [
            items[1].item_metadata["source_path"],
            items[0].item_metadata["source_path"],
        ]


def test_submit_validation_fails_closed_before_writing_files(
    engine: Engine,
    tmp_path: Path,
) -> None:
    submission_root = tmp_path / "submissions"
    with session_scope(engine) as session:
        _registered_intake(session, tmp_path, "intake-e", ["bad.mov"])
        arrangement = create_from_intake(session, "intake-e", label="invalid")
        arrangement_id = arrangement.id

    with pytest.raises(ArrangementError, match="control character"):
        _submit_with_bad_member_path(engine, arrangement_id, submission_root)

    assert not (submission_root / "bad-submit").exists()
    with session_scope(engine) as session:
        assert session.get(Submission, "bad-submit") is None
        loaded = session.get(Arrangement, arrangement_id)
        assert loaded is not None
        assert loaded.status == ArrangementStatus.DRAFT


@pytest.mark.parametrize(
    ("bad_path", "match"),
    [
        ("../bad.mov", "must not contain"),
        ("/absolute.mov", "relative"),
        ("day//clip.mov", "normalized"),
        ("cafe\u0301.mov", "NFC-normalized"),
    ],
)
def test_submit_rejects_invalid_member_path_variants_before_writing_files(
    engine: Engine,
    tmp_path: Path,
    bad_path: str,
    match: str,
) -> None:
    submission_root = tmp_path / "submissions"
    with session_scope(engine) as session:
        _registered_intake(session, tmp_path, f"intake-invalid-{hash(bad_path)}", ["clip.mov"])
        arrangement = create_from_intake(
            session,
            f"intake-invalid-{hash(bad_path)}",
            label="invalid-path",
        )
        arrangement_id = arrangement.id

    with pytest.raises(ArrangementError, match=match):
        _submit_with_bad_member_path_value(
            engine,
            arrangement_id,
            bad_path,
            submission_root,
            submission_id=f"bad-path-{abs(hash(bad_path))}",
        )

    assert not any(submission_root.glob("bad-path-*"))
    with session_scope(engine) as session:
        assert session.scalar(select(func.count()).select_from(Submission)) == 0
        loaded = session.get(Arrangement, arrangement_id)
        assert loaded is not None
        assert loaded.status == ArrangementStatus.DRAFT


def test_submit_rejects_changed_source_bytes_without_db_row(
    engine: Engine,
    tmp_path: Path,
) -> None:
    submission_root = tmp_path / "submissions"
    with session_scope(engine) as session:
        item = _registered_intake(session, tmp_path, "intake-f", ["clip.mov"])[0]
        arrangement = create_from_intake(session, "intake-f", label="changed")
        arrangement_id = arrangement.id
        Path(str(item.item_metadata["source_path"])).write_bytes(b"changed")

    with pytest.raises(ArrangementError, match="hashes to"):
        _submit_changed_source(engine, arrangement_id, submission_root)

    assert not (submission_root / "changed-submit").exists()
    with session_scope(engine) as session:
        assert session.get(Submission, "changed-submit") is None


def test_submit_rejects_missing_source_without_db_row(
    engine: Engine,
    tmp_path: Path,
) -> None:
    submission_root = tmp_path / "submissions"
    with session_scope(engine) as session:
        item = _registered_intake(session, tmp_path, "intake-missing", ["clip.mov"])[0]
        arrangement = create_from_intake(session, "intake-missing", label="missing")
        arrangement_id = arrangement.id
        Path(str(item.item_metadata["source_path"])).unlink()

    submit_missing = "missing-submit"
    with pytest.raises(ArrangementError, match="missing or not a file"):
        _submit_changed_source(
            engine, arrangement_id, submission_root, submission_id=submit_missing
        )

    assert not (submission_root / "missing-submit").exists()
    with session_scope(engine) as session:
        assert session.get(Submission, "missing-submit") is None


def test_submit_is_terminal_and_revision_is_clone(
    engine: Engine,
    tmp_path: Path,
) -> None:
    submission_root = tmp_path / "submissions"
    with session_scope(engine) as session:
        _registered_intake(session, tmp_path, "intake-g", ["clip.mov"])
        arrangement = create_from_intake(session, "intake-g", label="first")
        first = submit_arrangement(
            session,
            arrangement.id,
            submitted_by="tester",
            submission_root=submission_root,
            submission_id="first-submit",
        )
        arrangement_id = arrangement.id
        first_bytes = first.source_map_path.read_bytes()

    with pytest.raises(ArrangementFrozen):
        _move_submitted_arrangement(engine, arrangement_id)
    with pytest.raises(ArrangementFrozen):
        _include_submitted_arrangement(engine, arrangement_id)
    with pytest.raises(ArrangementFrozen):
        _resubmit_arrangement(engine, arrangement_id, submission_root)

    with session_scope(engine) as session:
        clone = create_from_arrangement(session, arrangement_id, label="second")
        move_member(session, clone.id, "clip.mov", "new.mov")
        second = submit_arrangement(
            session,
            clone.id,
            submitted_by="tester",
            submission_root=submission_root,
            submission_id="second-submit",
        )

        assert first.source_map_path.read_bytes() == first_bytes
        assert second.source_map_path != first.source_map_path
        assert session.scalar(select(func.count()).select_from(Submission)) == 2


def test_file_first_submit_leaves_only_orphan_files_on_rollback(
    engine: Engine,
    tmp_path: Path,
) -> None:
    submission_root = tmp_path / "submissions"
    with session_scope(engine) as session:
        _registered_intake(session, tmp_path, "intake-h", ["clip.mov"])
        arrangement = create_from_intake(session, "intake-h", label="rollback")
        arrangement_id = arrangement.id

    factory = make_session_factory(engine)
    session = factory()
    try:
        result = submit_arrangement(
            session,
            arrangement_id,
            submitted_by="tester",
            submission_root=submission_root,
            submission_id="rolled-back",
        )
        assert result.source_map_path.exists()
        session.rollback()
    finally:
        session.close()

    assert (submission_root / "rolled-back" / "source-map.tsv").exists()
    with session_scope(engine) as session:
        assert session.get(Submission, "rolled-back") is None
        loaded = session.get(Arrangement, arrangement_id)
        assert loaded is not None
        assert loaded.status == ArrangementStatus.DRAFT


def test_one_submission_per_arrangement_is_db_enforced(
    engine: Engine,
    tmp_path: Path,
) -> None:
    submission_root = tmp_path / "submissions"
    with session_scope(engine) as session:
        _registered_intake(session, tmp_path, "intake-i", ["clip.mov"])
        arrangement = create_from_intake(session, "intake-i", label="unique")
        submit_arrangement(
            session,
            arrangement.id,
            submitted_by="tester",
            submission_root=submission_root,
            submission_id="unique-submit",
        )
        arrangement_id = arrangement.id

    with pytest.raises(IntegrityError):
        _insert_duplicate_submission(engine, arrangement_id)


def test_interleaved_concurrent_submit_hits_status_guard_before_insert(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = make_engine(f"sqlite:///{tmp_path / 'race.db'}")
    create_all(engine)
    submission_root = tmp_path / "submissions"
    try:
        with session_scope(engine) as session:
            _registered_intake(session, tmp_path, "intake-race", ["clip.mov"])
            arrangement = create_from_intake(session, "intake-race", label="race")
            arrangement_id = arrangement.id

        original_write_submission_files = arrangement_module._write_submission_files
        interleaved = False

        def write_and_interleave(submission_dir: Path, files: dict[str, bytes]) -> None:
            nonlocal interleaved
            original_write_submission_files(submission_dir, files)
            if interleaved:
                return
            interleaved = True
            with session_scope(engine) as winner:
                submit_arrangement(
                    winner,
                    arrangement_id,
                    submitted_by="winner",
                    submission_root=submission_root,
                    submission_id="race-winner",
                )

        monkeypatch.setattr(arrangement_module, "_write_submission_files", write_and_interleave)

        with session_scope(engine) as loser, pytest.raises(ArrangementSubmitRace):
            submit_arrangement(
                loser,
                arrangement_id,
                submitted_by="loser",
                submission_root=submission_root,
                submission_id="race-loser",
            )

        assert (submission_root / "race-loser" / "source-map.tsv").exists()
        with session_scope(engine) as session:
            assert session.get(Submission, "race-winner") is not None
            assert session.get(Submission, "race-loser") is None
            assert session.scalar(select(func.count()).select_from(Submission)) == 1
    finally:
        engine.dispose()


def test_submit_refuses_existing_submission_directory_without_overwrite(
    engine: Engine,
    tmp_path: Path,
) -> None:
    submission_root = tmp_path / "submissions"
    with session_scope(engine) as session:
        _registered_intake(session, tmp_path, "intake-j", ["clip.mov"])
        first_arrangement = create_from_intake(session, "intake-j", label="first")
        first = submit_arrangement(
            session,
            first_arrangement.id,
            submitted_by="tester",
            submission_root=submission_root,
            submission_id="stable-submit",
        )
        first_bytes = first.source_map_path.read_bytes()
        clone = create_from_arrangement(session, first_arrangement.id, label="duplicate-id")
        clone_id = clone.id

    with pytest.raises(ArrangementError, match="submission directory already exists"):
        _submit_duplicate_submission_directory(engine, clone_id, submission_root)

    assert (submission_root / "stable-submit" / "source-map.tsv").read_bytes() == first_bytes
    with session_scope(engine) as session:
        assert session.get(Submission, "stable-submit") is not None
        assert session.scalar(select(func.count()).select_from(Submission)) == 1


def _registered_intake(
    session: Session,
    tmp_path: Path,
    intake_id: str,
    relpaths: list[str],
) -> list[IngestItem]:
    session.add(
        Intake(
            intake_id=intake_id,
            operator="tester",
            source_kind=IntakeSourceKind.CARD,
            source_ref="card-a",
            artifactclass="s-masters",
            label=intake_id,
            manifest_path=str(tmp_path / intake_id / "manifest-sha256.txt"),
            manifest_digest="manifest",
            status=IntakeStatus.REGISTERED,
            registered_at=dt.datetime.now(dt.UTC),
        )
    )
    session.flush()
    return [
        _add_item(session, tmp_path, intake_id, relpath, f"{intake_id}:{relpath}".encode())
        for relpath in relpaths
    ]


def _add_item(
    session: Session,
    tmp_path: Path,
    intake_id: str,
    relpath: str,
    data: bytes,
) -> IngestItem:
    source = tmp_path / intake_id / "data" / relpath
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(data)
    digest = hashlib.sha256(data).digest()
    asset = LogicalAsset(content_sha256=digest, size_bytes=len(data))
    session.add(asset)
    session.flush()
    item = IngestItem(
        intake_id=intake_id,
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


def _submit_with_bad_member_path(
    engine: Engine,
    arrangement_id: int,
    submission_root: Path,
) -> None:
    with session_scope(engine) as session:
        arrangement = show_arrangement(session, arrangement_id)
        arrangement.members[0].member_path = "bad\tpath.mov"
        submit_arrangement(
            session,
            arrangement_id,
            submitted_by="tester",
            submission_root=submission_root,
            submission_id="bad-submit",
        )


def _submit_changed_source(
    engine: Engine,
    arrangement_id: int,
    submission_root: Path,
    submission_id: str = "changed-submit",
) -> None:
    with session_scope(engine) as session:
        submit_arrangement(
            session,
            arrangement_id,
            submitted_by="tester",
            submission_root=submission_root,
            submission_id=submission_id,
        )


def _move_submitted_arrangement(engine: Engine, arrangement_id: int) -> None:
    with session_scope(engine) as session:
        move_member(session, arrangement_id, "clip.mov", "new.mov")


def _include_submitted_arrangement(engine: Engine, arrangement_id: int) -> None:
    with session_scope(engine) as session:
        include_member(session, arrangement_id, "clip.mov")


def _resubmit_arrangement(
    engine: Engine,
    arrangement_id: int,
    submission_root: Path,
) -> None:
    with session_scope(engine) as session:
        submit_arrangement(
            session,
            arrangement_id,
            submitted_by="tester",
            submission_root=submission_root,
            submission_id="second-on-same",
        )


def _insert_duplicate_submission(engine: Engine, arrangement_id: int) -> None:
    with session_scope(engine) as session:
        session.add(
            Submission(
                id="duplicate-submit",
                arrangement_id=arrangement_id,
                artifactclass="s-masters",
                source_map_path="/tmp/source-map.tsv",
                manifest_digest="0" * 64,
                member_count=0,
                status=SubmissionStatus.PENDING_ARCHIVE,
                submitted_by="tester",
                submitted_at=dt.datetime.now(dt.UTC),
            )
        )
        session.flush()


def _submit_duplicate_submission_directory(
    engine: Engine,
    arrangement_id: int,
    submission_root: Path,
) -> None:
    with session_scope(engine) as session:
        submit_arrangement(
            session,
            arrangement_id,
            submitted_by="tester",
            submission_root=submission_root,
            submission_id="stable-submit",
        )


def _submit_with_bad_member_path_value(
    engine: Engine,
    arrangement_id: int,
    member_path: str,
    submission_root: Path,
    *,
    submission_id: str,
) -> None:
    with session_scope(engine) as session:
        arrangement = show_arrangement(session, arrangement_id)
        arrangement.members[0].member_path = member_path
        submit_arrangement(
            session,
            arrangement_id,
            submitted_by="tester",
            submission_root=submission_root,
            submission_id=submission_id,
        )
