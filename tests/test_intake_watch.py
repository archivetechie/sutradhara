"""`sutra intake watch` core tests."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, select

from sutradhara.catalog.models import Intake
from sutradhara.catalog.session import create_all, make_engine, make_session_factory, session_scope
from sutradhara.catalog.types import IntakeStatus
from sutradhara.intake import register_intake
from sutradhara.intake_watch import process_landing_once
from sutradhara_receive import receive_source


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_watch_once_registers_completed_receive(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    result = _receive_fixture(tmp_path, landing, "source")

    events = process_landing_once(
        landing,
        engine=engine,
        settle_seconds=0,
        stable_polls=1,
        validation_attempts=1,
        cache_root=tmp_path / "cache",
        use_lock=False,
    )

    assert [event.event for event in events] == ["intake-registered"]
    assert (result.intake_dir / "intake.verified.json").is_file()
    with session_scope(engine) as session:
        intake = session.get(Intake, result.intake_id)
        assert intake is not None
        assert intake.status == IntakeStatus.REGISTERED


def test_watch_skips_active_receive_even_with_stale_sentinel(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    result = _receive_fixture(tmp_path, landing, "source")
    (result.intake_dir / ".receiving.json").write_text("{}", encoding="utf-8")

    events = process_landing_once(
        landing,
        engine=engine,
        settle_seconds=0,
        stable_polls=1,
        cache_root=tmp_path / "cache",
        use_lock=False,
    )

    assert events[0].event == "intake-skipped"
    assert events[0].reason == "active-receive"
    assert not (result.intake_dir / "intake.verified.json").exists()
    with session_scope(engine) as session:
        assert session.get(Intake, result.intake_id) is None


def test_watch_commit_failure_leaves_no_terminal_marker(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    result = _receive_fixture(tmp_path, landing, "source")
    factory = make_session_factory(engine)

    def failing_factory() -> Any:
        return _CommitFailingSession(factory())

    events = process_landing_once(
        landing,
        session_factory=failing_factory,
        settle_seconds=0,
        stable_polls=1,
        cache_root=tmp_path / "cache",
        use_lock=False,
    )

    assert events[0].event == "intake-error"
    assert events[0].reason == "RuntimeError"
    assert not (result.intake_dir / "intake.verified.json").exists()
    assert not (result.intake_dir / "intake.quarantined.json").exists()
    with session_scope(engine) as session:
        assert session.get(Intake, result.intake_id) is None


def test_watch_transient_extra_payload_file_keeps_candidate_unstable(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    result = _receive_fixture(tmp_path, landing, "source")
    extra = result.intake_dir / "data" / "extra.mov"
    extra.write_bytes(b"transient")

    def remove_extra(_seconds: float) -> None:
        extra.unlink()

    events = process_landing_once(
        landing,
        engine=engine,
        settle_seconds=0,
        stable_polls=2,
        validation_attempts=2,
        cache_root=tmp_path / "cache",
        sleep=remove_extra,
        use_lock=False,
    )

    assert events[0].event == "intake-skipped"
    assert events[0].reason == "not-stable"
    assert not (result.intake_dir / "intake.quarantined.json").exists()
    with session_scope(engine) as session:
        assert session.get(Intake, result.intake_id) is None


def test_watch_discrepancy_does_not_block_next_candidate(
    engine: Engine,
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    first = _receive_fixture(tmp_path, landing, "a-source", payload=b"first")
    second = _receive_fixture(tmp_path, landing, "b-source", payload=b"second")
    with session_scope(engine) as session:
        register_intake(session, first.intake_dir, cache_root=tmp_path / "cache")
    payload_path = first.intake_dir / "data" / "clip.mov"
    payload_path.write_bytes(b"tampered")
    digest = hashlib.sha256(b"first").hexdigest()
    assert digest in (first.intake_dir / "manifest-sha256.txt").read_text(encoding="utf-8")

    events = process_landing_once(
        landing,
        engine=engine,
        settle_seconds=0,
        stable_polls=1,
        validation_attempts=1,
        cache_root=tmp_path / "cache",
        use_lock=False,
    )

    assert {event.event for event in events} == {"intake-discrepancy", "intake-registered"}
    assert (first.intake_dir / "intake.discrepancy.json").is_file()
    assert (second.intake_dir / "intake.verified.json").is_file()


class _CommitFailingSession:
    def __init__(self, session: Any) -> None:
        self._session = session

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)

    def commit(self) -> None:
        raise RuntimeError("simulated commit failure")

    def rollback(self) -> None:
        self._session.rollback()

    def close(self) -> None:
        self._session.close()


def _receive_fixture(
    tmp_path: Path,
    landing: Path,
    source_name: str,
    *,
    payload: bytes = b"video",
) -> Any:
    source = tmp_path / source_name
    source.mkdir()
    (source / "clip.mov").write_bytes(payload)
    return receive_source(
        source,
        landing=landing,
        source_kind="card",
        operator="op",
        artifactclass="video-master",
    )
