"""Tests for the P1.2 derivation reconciler domain."""

from __future__ import annotations

import datetime as dt
import json
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

import sutradhara.jobs.handlers  # noqa: F401 -- register built-in handlers
from sutradhara.catalog.facts import record_derivation
from sutradhara.catalog.models import (
    ArtifactClassPool,
    AssetDerivation,
    Backend,
    IngestItem,
    Pool,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, MediaKind
from sutradhara.intake import prepare_intake, register_intake
from sutradhara.jobs.config import override_derivation_cache_root
from sutradhara.jobs.engine import run_one, submit
from sutradhara.jobs.handlers.transcode import handle_transcode
from sutradhara.jobs.models import Job, ReconciliationCondition
from sutradhara.jobs.reconcilers import copy as _copy_reconciler  # noqa: F401
from sutradhara.jobs.reconcilers import derivation as derivation_reconciler
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    CONDITION_BLOCKED,
    CONDITION_OPEN,
    CONDITION_SATISFIED,
    DEFAULT_BACKOFF_GIVE_UP_ATTEMPTS,
)
from sutradhara.jobs.reconcilers.profiles import (
    DerivationEntry,
    FactSpec,
    validate_profiles,
)
from sutradhara.jobs.reconcilers.spine import process, reconcile
from sutradhara.jobs.registry import JobContext
from sutradhara.jobs.tool_versions import register_tool_version, unregister_tool_version
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_proxy_lands_as_output_class_and_copy_policy_uses_proxy_class(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUTRADHARA_FAKE_TRANSCODE", "1")
    item_id = _register_prepared_video(
        engine,
        tmp_path,
        "card-proxy",
        profile="proxy-review",
    )

    with session_scope(engine) as session:
        _add_pool(session, "master-pool", "s-masters")
        _add_pool(session, "proxy-pool", "s-proxy")
        reconcile(session, "derivation")
        job = session.scalars(select(Job).where(Job.kind == "transcode")).one()
        assert job.params["output_class"] == "s-proxy"
        assert job.required_resources == [{"pool": "cpu", "count": 8}]
        result = run_one(session, job.id, granted_leases={"cpu": 8})
        assert result.ok
        process(session, "derivation")

    with session_scope(engine) as session:
        derived_items = list(
            session.scalars(
                select(IngestItem)
                .where(IngestItem.as_received_path.like("derived/%"))
                .order_by(IngestItem.as_received_path)
            )
        )
        assert {item.item_metadata["kind"] for item in derived_items} == {"mezz", "preview"}
        assert {item.artifactclass for item in derived_items} == {"s-proxy"}

        reconcile(session, "copy")
        derived_hashes = {item.logical_asset_hash.hex() for item in derived_items}
        copy_jobs = list(session.scalars(select(Job).where(Job.kind == "copy")))
        derived_copy_jobs = [job for job in copy_jobs if job.params["asset_hash"] in derived_hashes]
        assert {job.params["pool_id"] for job in derived_copy_jobs} == {"proxy-pool"}
        assert all(job.params["pool_id"] != "master-pool" for job in derived_copy_jobs)

    with pytest.raises(ValueError, match="output_class"):
        _run_transcode_without_output_class(engine, item_id, tmp_path)


def test_transcode_target_observes_both_facts_and_reenqueues_missing_fact(
    engine: Engine,
    tmp_path: Path,
) -> None:
    item_id = _register_prepared_video(
        engine,
        tmp_path,
        "card-idempotent",
        profile="proxy-review",
    )
    with session_scope(engine) as session:
        item = session.get(IngestItem, item_id)
        assert item is not None
        _record_derivative(session, item, tmp_path, "mezz")
        _record_derivative(session, item, tmp_path, "preview")
        reconcile(session, "derivation")
        target_key = derivation_reconciler.make_target_key(item_id, "transcode")
        assert _condition(session, target_key).condition == CONDITION_SATISFIED
        assert (
            session.scalar(select(func.count()).select_from(Job).where(Job.kind == "transcode"))
            == 0
        )

        edge = session.scalars(
            select(AssetDerivation).where(AssetDerivation.kind == "preview")
        ).one()
        session.delete(edge)
        session.flush()

        reconcile(session, "derivation")
        condition = _condition(session, target_key)
        assert condition.condition == CONDITION_OPEN
        jobs = list(session.scalars(select(Job).where(Job.kind == "transcode")))
        assert len(jobs) == 1


def test_partial_transcode_output_is_one_missing_target_one_live_job(
    engine: Engine,
    tmp_path: Path,
) -> None:
    item_id = _register_prepared_video(
        engine,
        tmp_path,
        "card-partial",
        profile="proxy-review",
    )
    with session_scope(engine) as session:
        item = session.get(IngestItem, item_id)
        assert item is not None
        _record_derivative(session, item, tmp_path, "mezz")

        reconcile(session, "derivation")
        target_key = derivation_reconciler.make_target_key(item_id, "transcode")
        assert _condition(session, target_key).condition == CONDITION_OPEN
        assert (
            session.scalar(select(func.count()).select_from(Job).where(Job.kind == "transcode"))
            == 1
        )

        process(session, "derivation")
        assert (
            session.scalar(select(func.count()).select_from(Job).where(Job.kind == "transcode"))
            == 1
        )


def test_non_video_sibling_gets_no_derivation_target(engine: Engine, tmp_path: Path) -> None:
    root = _write_intake(
        tmp_path / "landing",
        "card-mixed",
        {"clip.mov": b"video bytes", "notes.txt": b"notes"},
    )
    with session_scope(engine) as session:
        register_intake(session, root, artifactclass="s-masters", cache_root=tmp_path / "cache")
        prepare_intake(session, "card-mixed", profile="hd-review")
        reconcile(session, "derivation")

        clip = session.scalars(
            select(IngestItem).where(IngestItem.as_received_path == "clip.mov")
        ).one()
        notes = session.scalars(
            select(IngestItem).where(IngestItem.as_received_path == "notes.txt")
        ).one()
        derivation_jobs = list(
            session.scalars(select(Job).where(Job.kind.in_(("transcode", "pfr-index"))))
        )
        assert {job.params["ingest_item_id"] for job in derivation_jobs} == {clip.id}
        assert notes.id not in {job.params["ingest_item_id"] for job in derivation_jobs}
        assert len(derivation_jobs) == 2


def test_pfr_sidecar_observation_has_no_derived_item_or_copy(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUTRADHARA_FAKE_FFPROBE", "1")
    item_id = _register_prepared_video(engine, tmp_path, "card-pfr", profile="hd-review")

    with session_scope(engine) as session:
        reconcile(session, "derivation")
        pfr_job = session.scalars(select(Job).where(Job.kind == "pfr-index")).one()
        assert pfr_job.required_resources == [
            {"pool": "io", "count": 1},
            {"pool": "cpu", "count": 1},
        ]
        result = run_one(session, pfr_job.id, granted_leases={"io": 1, "cpu": 1})
        assert result.ok
        process(session, "derivation")

        item = session.get(IngestItem, item_id)
        assert item is not None
        assert Path(item.item_metadata["pfr_sidecar_path"]).exists()
        target_key = derivation_reconciler.make_target_key(item_id, "pfr-index")
        assert _condition(session, target_key).condition == CONDITION_SATISFIED
        assert session.scalar(select(func.count()).select_from(AssetDerivation)) == 0
        assert session.scalar(select(func.count()).select_from(IngestItem)) == 1

        reconcile(session, "copy")
        copy_jobs = list(session.scalars(select(Job).where(Job.kind == "copy")))
        assert copy_jobs == []


def test_profile_clear_closes_due_derivation_condition_without_new_enqueue(
    engine: Engine,
    tmp_path: Path,
) -> None:
    item_id = _register_prepared_video(
        engine,
        tmp_path,
        "card-clear",
        profile="proxy-review",
    )
    target_key = derivation_reconciler.make_target_key(item_id, "transcode")

    with session_scope(engine) as session:
        reconcile(session, "derivation")
        assert _condition(session, target_key).condition == CONDITION_OPEN
        job_count = session.scalar(select(func.count()).select_from(Job))

        item = session.get(IngestItem, item_id)
        assert item is not None
        intake = item.intake
        intake.requested_profile = None
        process(session, "derivation")

        assert _condition(session, target_key).condition == CONDITION_SATISFIED
        assert session.scalar(select(func.count()).select_from(Job)) == job_count


@pytest.mark.parametrize(
    ("fixture_name", "bucket", "matched_pattern", "blocked_on_ffmpeg"),
    [
        ("ffmpeg_damage.stderr", "damage", "moov atom not found", False),
        ("ffmpeg_capability.stderr", "capability", "unknown decoder", True),
    ],
)
def test_ffmpeg_stderr_bucket_parking_records_factual_match(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fixture_name: str,
    bucket: str,
    matched_pattern: str,
    blocked_on_ffmpeg: bool,
) -> None:
    stderr = _ffmpeg_stderr_fixture(fixture_name)
    previous_provider = register_tool_version("ffmpeg", lambda: "fixture-ffmpeg-1")
    monkeypatch.setattr("sutradhara.jobs.handlers.transcode.shutil.which", lambda _tool: "/ffmpeg")
    monkeypatch.setattr(
        "sutradhara.jobs.handlers.transcode.run_managed",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 1, stdout="", stderr=stderr),
    )
    try:
        item_id = _register_prepared_video(
            engine,
            tmp_path,
            f"card-{bucket}",
            profile="proxy-review",
        )
        target_key = derivation_reconciler.make_target_key(item_id, "transcode")

        with (
            override_derivation_cache_root(tmp_path / "cache"),
            session_scope(engine) as session,
        ):
            reconcile(session, "derivation")
            job = _transcode_job_for_item(session, item_id)
            result = run_one(session, job.id, granted_leases={"cpu": 8})

            assert result.ok
            persisted_job = session.get(Job, job.id)
            assert persisted_job is not None
            state = persisted_job.step_state["transcode"]
            assert state["bucket"] == bucket
            assert state["matched_pattern"] == matched_pattern
            assert state["stderr_excerpt"] == stderr.strip()

            condition = _condition(session, target_key)
            assert condition.condition == CONDITION_BLOCKED
            assert condition.reason == f"stderr-pattern:{bucket}"
            assert condition.message == state["detail"]
            message = condition.message
            assert message is not None
            assert matched_pattern in message
            assert stderr.strip() in message
            assert condition.blocked_tool_name == ("ffmpeg" if blocked_on_ffmpeg else None)
            assert condition.blocked_tool_version == (
                "fixture-ffmpeg-1" if blocked_on_ffmpeg else None
            )
    finally:
        if previous_provider is None:
            unregister_tool_version("ffmpeg")
        else:
            register_tool_version("ffmpeg", previous_provider)


def test_ffmpeg_version_bump_reopens_capability_but_not_damage(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stderr_by_intake = {
        "card-damage-reopen": _ffmpeg_stderr_fixture("ffmpeg_damage.stderr"),
        "card-capability-reopen": _ffmpeg_stderr_fixture("ffmpeg_capability.stderr"),
    }
    version = ["fixture-ffmpeg-1"]
    previous_provider = register_tool_version("ffmpeg", lambda: version[0])
    monkeypatch.setattr("sutradhara.jobs.handlers.transcode.shutil.which", lambda _tool: "/ffmpeg")

    def fail_for_source(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        source = command[command.index("-i") + 1]
        stderr = next(text for intake, text in stderr_by_intake.items() if intake in source)
        return subprocess.CompletedProcess(command, 1, stdout="", stderr=stderr)

    monkeypatch.setattr("sutradhara.jobs.handlers.transcode.run_managed", fail_for_source)
    try:
        damage_id = _register_prepared_video(
            engine,
            tmp_path,
            "card-damage-reopen",
            profile="proxy-review",
        )
        capability_id = _register_prepared_video(
            engine,
            tmp_path,
            "card-capability-reopen",
            profile="proxy-review",
        )
        damage_key = derivation_reconciler.make_target_key(damage_id, "transcode")
        capability_key = derivation_reconciler.make_target_key(capability_id, "transcode")

        with (
            override_derivation_cache_root(tmp_path / "cache"),
            session_scope(engine) as session,
        ):
            reconcile(session, "derivation")
            run_one(
                session,
                _transcode_job_for_item(session, damage_id).id,
                granted_leases={"cpu": 8},
            )
            run_one(
                session,
                _transcode_job_for_item(session, capability_id).id,
                granted_leases={"cpu": 8},
            )
            assert _condition(session, damage_key).blocked_tool_name is None
            assert _condition(session, capability_key).blocked_tool_name == "ffmpeg"

            version[0] = "fixture-ffmpeg-2"
            reconcile(session, "derivation")

            assert _condition(session, capability_key).condition == CONDITION_OPEN
            assert _condition(session, capability_key).blocked_tool_name is None
            assert _condition(session, damage_key).condition == CONDITION_BLOCKED
            assert _condition(session, damage_key).reason == "stderr-pattern:damage"
    finally:
        if previous_provider is None:
            unregister_tool_version("ffmpeg")
        else:
            register_tool_version("ffmpeg", previous_provider)


def test_damage_pattern_blocks_but_missing_toolchain_backs_off_then_gives_up(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUTRADHARA_FAKE_TRANSCODE", "1")
    damaged_id = _register_prepared_video(
        engine,
        tmp_path,
        "card-damaged",
        profile="proxy-review",
        payload=b"DECODE_FAIL damaged",
    )
    damaged_key = derivation_reconciler.make_target_key(damaged_id, "transcode")

    with (
        override_derivation_cache_root(tmp_path / "cache"),
        session_scope(engine) as session,
    ):
        reconcile(session, "derivation")
        job = session.scalars(select(Job).where(Job.kind == "transcode")).one()
        result = run_one(session, job.id, granted_leases={"cpu": 8})
        assert result.ok
        condition = _condition(session, damaged_key)
        assert condition.condition == CONDITION_BLOCKED
        assert condition.reason == "stderr-pattern:damage"
        assert condition.blocked_tool_name is None

    monkeypatch.delenv("SUTRADHARA_FAKE_TRANSCODE", raising=False)
    monkeypatch.setattr("sutradhara.jobs.handlers.transcode.shutil.which", lambda _tool: None)
    missing_tool_id = _register_prepared_video(
        engine,
        tmp_path,
        "card-missing-tool",
        profile="proxy-review",
    )
    missing_tool_key = derivation_reconciler.make_target_key(missing_tool_id, "transcode")

    with (
        override_derivation_cache_root(tmp_path / "cache"),
        session_scope(engine) as session,
    ):
        reconcile(session, "derivation")
        job = session.scalars(
            select(Job)
            .where(
                Job.kind == "transcode",
                Job.params["ingest_item_id"].as_integer() == missing_tool_id,
            )
            .order_by(Job.id.desc())
            .limit(1)
        ).one()
        result = run_one(session, job.id, granted_leases={"cpu": 8})
        assert not result.ok

        condition = _condition(session, missing_tool_key)
        assert condition.condition == CONDITION_BACKOFF
        assert condition.attempt_count == 1

        for expected_attempt in range(2, DEFAULT_BACKOFF_GIVE_UP_ATTEMPTS + 1):
            condition.next_eligible_at = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1)
            process(session, "derivation")
            job = session.scalars(
                select(Job)
                .where(
                    Job.kind == "transcode",
                    Job.params["ingest_item_id"].as_integer() == missing_tool_id,
                )
                .order_by(Job.id.desc())
                .limit(1)
            ).one()
            run_one(session, job.id, granted_leases={"cpu": 8})
            condition = _condition(session, missing_tool_key)
            assert condition.attempt_count == expected_attempt

        assert condition.condition == CONDITION_BLOCKED


def test_cache_root_comes_from_env_or_reconcile_override(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_item_id = _register_prepared_video(
        engine,
        tmp_path,
        "card-cache-env",
        profile="proxy-review",
    )
    monkeypatch.setenv("SUTRADHARA_CACHE_ROOT", str(tmp_path / "env-cache"))

    with session_scope(engine) as session:
        reconcile(session, "derivation")
        job = _transcode_job_for_item(session, env_item_id)
        assert job.params["cache_root"] == str((tmp_path / "env-cache").resolve())

    override_item_id = _register_prepared_video(
        engine,
        tmp_path,
        "card-cache-override",
        profile="proxy-review",
    )
    with (
        override_derivation_cache_root(tmp_path / "override-cache"),
        session_scope(engine) as session,
    ):
        reconcile(session, "derivation")
        job = _transcode_job_for_item(session, override_item_id)
        assert job.params["cache_root"] == str((tmp_path / "override-cache").resolve())


def test_duplicate_job_kind_profile_is_rejected() -> None:
    entry = DerivationEntry(
        job_kind="transcode",
        input_media_kind=MediaKind.VIDEO,
        produces=(FactSpec(kind="mezz", fact_type="derivation"),),
        output_class="s-proxy",
    )
    with pytest.raises(ValueError, match="duplicate derivation job_kind"):
        validate_profiles({("s-masters", "bad"): (entry, entry)})


def _register_prepared_video(
    engine: Engine,
    tmp_path: Path,
    intake_id: str,
    *,
    profile: str,
    payload: bytes = b"valid video payload",
) -> int:
    root = _write_intake(tmp_path / "landing", intake_id, {"clip.mov": payload})
    with session_scope(engine) as session:
        register_intake(session, root, artifactclass="s-masters", cache_root=tmp_path / "cache")
        prepare_intake(session, intake_id, profile=profile)
        return session.scalars(
            select(IngestItem.id).where(
                IngestItem.intake_id == intake_id,
                IngestItem.as_received_path == "clip.mov",
            )
        ).one()


def _ffmpeg_stderr_fixture(name: str) -> str:
    return (Path(__file__).parent / "fixtures" / name).read_text(encoding="utf-8")


def _write_intake(landing: Path, intake_id: str, files: dict[str, bytes]) -> Path:
    root = landing / intake_id
    payload = root / "payload"
    payload.mkdir(parents=True)
    for relpath, content in files.items():
        path = payload / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    (root / "intake.json").write_text(
        json.dumps(
            {
                "intake_id": intake_id,
                "operator": "tester",
                "source_kind": "card",
                "artifactclass": "s-masters",
            }
        ),
        encoding="utf-8",
    )
    return root


def _record_derivative(
    session: Session,
    source_item: IngestItem,
    tmp_path: Path,
    kind: str,
) -> IngestItem:
    path = tmp_path / f"{source_item.intake_id}-{kind}.mp4"
    path.write_bytes(f"{source_item.id}:{kind}".encode())
    return record_derivation(
        session,
        source_item=source_item,
        output_path=path,
        relpath=f"derived/{source_item.id}/{kind}.mp4",
        kind=kind,
        artifactclass="s-proxy",
        media_kind=MediaKind.VIDEO,
        generated_by="test",
    )


def _add_pool(session: Session, pool_id: str, artifactclass: str) -> None:
    backend = Backend(
        name=f"backend-{pool_id}",
        kind=BackendKind.REM_TAPE,
        tier=BackendTier.SELF_DESCRIBING,
        config={"daemon_endpoint": f"unix:/{pool_id}.sock"},
    )
    session.add(backend)
    session.flush()
    session.add(
        Pool(id=pool_id, backend_id=backend.id, representation=Representation.RAW_BYTES.value)
    )
    session.add(ArtifactClassPool(artifactclass=artifactclass, pool_id=pool_id))


def _condition(session: Session, target_key: str) -> ReconciliationCondition:
    return session.scalars(
        select(ReconciliationCondition).where(
            ReconciliationCondition.domain == "derivation",
            ReconciliationCondition.target_key == target_key,
        )
    ).one()


def _transcode_job_for_item(session: Session, item_id: int) -> Job:
    return session.scalars(
        select(Job)
        .where(Job.kind == "transcode", Job.params["ingest_item_id"].as_integer() == item_id)
        .order_by(Job.id.desc())
        .limit(1)
    ).one()


def _run_transcode_without_output_class(engine: Engine, item_id: int, tmp_path: Path) -> None:
    with session_scope(engine) as session:
        job = submit(
            session,
            "transcode",
            {"ingest_item_id": item_id, "cache_root": str(tmp_path / "cache")},
        )
        handle_transcode(JobContext(session=session, job=job, granted_leases={"cpu": 8}))
