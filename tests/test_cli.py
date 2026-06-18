"""End-to-end CLI tests.

Exercises the full vertical slice: `db init` → `backends add` →
`backends list` → `scrub --backend` → `list assets`. This is the
demo of the rebuildable-index principle that the day-1 slice is
designed to prove.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from sutradhara.artifactclass_policy import AppleDoubleStagingPolicy, StagingPolicy
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    Bundle,
    LogicalAsset,
    ReviewDecision,
)
from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.catalog.types import AssetValidity
from sutradhara.cli.main import cli
from sutradhara.jobs.engine import submit
from sutradhara.jobs.models import Job, JobStatus

FIXTURE = Path(__file__).parent / "fixtures" / "remanence_objects.json"


@pytest.fixture
def cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Isolated per-test DB + CWD (monkeypatch handles cleanup)."""
    db_path = tmp_path / "sutradhara.db"
    monkeypatch.setenv("SUTRADHARA_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.chdir(tmp_path)
    return {"db": str(db_path)}


def _run(args: list[str], expect_exit: int = 0) -> Any:
    runner = CliRunner()
    result = runner.invoke(cli, args)
    if result.exit_code != expect_exit:
        msg = (
            f"CLI args {args!r} exited {result.exit_code}, expected {expect_exit}\n"
            f"output:\n{result.output}"
        )
        if result.exception:
            import traceback

            msg += "\n" + "".join(
                traceback.format_exception(
                    type(result.exception),
                    result.exception,
                    result.exception.__traceback__,
                )
            )
        pytest.fail(msg)
    return result


def test_version(cli_env: dict[str, str]) -> None:
    result = _run(["--version"])
    assert "0.0.1" in result.output


def test_help_lists_subcommands(cli_env: dict[str, str]) -> None:
    result = _run(["--help"])
    for cmd in (
        "db",
        "backends",
        "list",
        "scrub",
        "intake",
        "admin",
        "archive",
        "review",
        "receive",
        "worker",
    ):
        assert cmd in result.output


def test_db_init_creates_schema(cli_env: dict[str, str]) -> None:
    result = _run(["db", "init"])
    assert "OK" in result.output
    assert os.path.exists(cli_env["db"])


def test_worker_once_cli_drains_validate_job(
    cli_env: dict[str, str],
    tmp_path: Path,
) -> None:
    _run(["db", "init"])
    source = tmp_path / "payload.txt"
    source.write_text("valid text", encoding="utf-8")
    payload = source.read_bytes()
    digest = hashlib.sha256(payload).digest()

    engine = make_engine()
    with session_scope(engine) as session:
        session.add(LogicalAsset(content_sha256=digest, size_bytes=len(payload)))
        job = submit(
            session,
            "validate",
            {
                "asset_hash": digest.hex(),
                "path": str(source),
                "validator": "utf-8",
            },
        )
        job_id = job.id

    result = _run(["worker", "--once", "--pools", "cpu=2"])

    assert "worker drained 1 job(s)" in result.output
    with session_scope(engine) as session:
        row = session.get(Job, job_id)
        asset = session.get(LogicalAsset, digest)
        assert row is not None
        assert row.status == JobStatus.SUCCEEDED
        assert asset is not None
        assert asset.validity == AssetValidity.OK


def test_backends_list_empty(cli_env: dict[str, str]) -> None:
    _run(["db", "init"])
    result = _run(["backends", "list"])
    assert "(no backends registered)" in result.output


def test_backends_add_and_list(cli_env: dict[str, str]) -> None:
    _run(["db", "init"])
    _run(
        [
            "backends",
            "add",
            "rem-primary",
            "--kind",
            "rem_tape",
            "--tier",
            "self_describing",
            "--fixture",
            str(FIXTURE),
        ]
    )
    result = _run(["backends", "list"])
    assert "rem-primary" in result.output
    assert "rem_tape" in result.output

    # JSON mode round-trips.
    result_json = _run(["backends", "list", "--json"])
    payload = json.loads(result_json.output.strip())
    assert payload["name"] == "rem-primary"
    assert payload["kind"] == "rem_tape"
    assert payload["config"] == {"fixture_path": str(FIXTURE)}


def test_admin_reset_clears_catalog(cli_env: dict[str, str]) -> None:
    _run(["db", "init"])
    _run(
        [
            "backends",
            "add",
            "rem-primary",
            "--kind",
            "rem_tape",
            "--tier",
            "self_describing",
            "--fixture",
            str(FIXTURE),
        ]
    )

    result = _run(["admin", "reset", "--i-mean-it"])

    assert "OK" in result.output
    listed = _run(["backends", "list"])
    assert "(no backends registered)" in listed.output


def test_admin_reset_requires_confirmation(cli_env: dict[str, str]) -> None:
    _run(["admin", "reset"], expect_exit=2)


def test_top_level_review_shows_and_records_held_bundle(
    cli_env: dict[str, str],
) -> None:
    _run(["db", "init"])
    engine = make_engine()
    with session_scope(engine) as session:
        session.add(
            Bundle(
                id="bundle-held",
                artifactclass="o-archive",
                status="held",
                review_summary={"clusters": [{"prefix": "tmp/", "count": 2}]},
            )
        )
        session.add(
            Bundle(
                id="bundle-open",
                artifactclass="o-archive",
                status="open",
            )
        )

    shown = _run(["review", "bundle-held"])
    assert '"prefix": "tmp/"' in shown.output

    missing_actor = _run(
        ["review", "bundle-held", "--action", "exclude", "--why", "temporary files"],
        expect_exit=1,
    )
    assert "--who is required" in missing_actor.output

    not_held = _run(
        [
            "review",
            "bundle-open",
            "--action",
            "exclude",
            "--why",
            "temporary files",
            "--who",
            "operator",
        ],
        expect_exit=1,
    )
    assert "only held bundles can be reviewed" in not_held.output

    result = _run(
        [
            "review",
            "bundle-held",
            "--action",
            "exclude",
            "--scope",
            "just-this-ingest",
            "--subtree",
            "tmp/",
            "--why",
            "temporary files",
            "--who",
            "operator",
        ]
    )
    assert "recorded review decision" in result.output
    with session_scope(engine) as session:
        [decision] = session.query(ReviewDecision).all()
        assert decision.bundle_id == "bundle-held"
        assert decision.action == "exclude"
        assert decision.reason == "temporary files"
        assert decision.reviewer == "operator"


def test_archive_bundle_enqueue_persists_held_bundle_after_staging_failure(
    cli_env: dict[str, str],
    tmp_path: Path,
) -> None:
    _run(["db", "init"])
    source = tmp_path / "photo.tif"
    source.write_bytes(b"image-data")
    source.with_name("._photo.tif").write_bytes(b"not-appledouble")
    engine = make_engine()
    with session_scope(engine) as session:
        session.add(
            ArtifactClassPolicyRecord(
                artifactclass="photo",
                ruleset="rao.photo.v1",
                expect="messy",
                target_bytes=1024,
                max_age_seconds=60,
                restore_preference=[],
                staging_config=StagingPolicy(
                    appledouble=AppleDoubleStagingPolicy(action="merge-to-xattrs")
                ).to_json(),
            )
        )

    result = _run(
        [
            "archive",
            "bundle",
            "enqueue",
            "photo",
            hashlib.sha256(b"image-data").hexdigest(),
            str(source),
        ],
        expect_exit=1,
    )

    assert "appledouble-merge-failed" in result.output
    with session_scope(engine) as session:
        [bundle] = session.query(Bundle).all()
        assert bundle.artifactclass == "photo"
        assert bundle.status == "held"
        assert bundle.review_summary is not None
        assert bundle.review_summary["clusters"][0]["reason"] == "appledouble-merge-failed"


def test_admin_doctor_reports_rem_and_key_registry(
    cli_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rem = tmp_path / "rem"
    rem.write_text("#!/bin/sh\nexit 0\n")
    rem.chmod(rem.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("REM_BIN", str(rem))
    monkeypatch.setenv("SUTRADHARA_KEY_REGISTRY_DIR", str(tmp_path / "keys"))

    result = _run(["admin", "doctor"])

    assert f"rem: OK - using {rem}" in result.output
    assert "key-registry: OK" in result.output


def test_admin_doctor_strict_fails_on_missing_rem(
    cli_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REM_BIN", str(tmp_path / "missing-rem"))
    monkeypatch.setenv("SUTRADHARA_KEY_REGISTRY_DIR", str(tmp_path / "keys"))

    result = _run(["admin", "doctor", "--strict"], expect_exit=1)

    assert "rem: WARN" in result.output
    assert "one or more diagnostics" in result.output


def test_scrub_against_empty_catalog_populates_everything(cli_env: dict[str, str]) -> None:
    """The load-bearing demo: scrub fills an empty catalog from a backend."""
    _run(["db", "init"])
    _run(
        [
            "backends",
            "add",
            "rem-primary",
            "--kind",
            "rem_tape",
            "--tier",
            "self_describing",
            "--fixture",
            str(FIXTURE),
        ]
    )

    result = _run(["scrub", "--backend", "rem-primary"])
    # Fixture has 3 unique assets, 4 copies (one asset has 2 copies).
    assert "assets added:      3" in result.output
    assert "copies added:      4" in result.output
    assert "copies updated:    0" in result.output
    assert "copies missing:    0" in result.output

    # List shows them.
    listed = _run(["list", "assets"])
    assert listed.output.count("rem-primary") == 3
    # First two assets have 1 copy; third has 2.
    assert "  1  " in listed.output  # at least one row with copy count 1
    assert "  2  " in listed.output  # at least one row with copy count 2


def test_second_scrub_is_idempotent(cli_env: dict[str, str]) -> None:
    """Re-scrubbing the same backend updates timestamps, adds nothing."""
    _run(["db", "init"])
    _run(
        [
            "backends",
            "add",
            "rem-primary",
            "--kind",
            "rem_tape",
            "--fixture",
            str(FIXTURE),
        ]
    )
    _run(["scrub", "--backend", "rem-primary"])  # first scrub

    result = _run(["scrub", "--backend", "rem-primary"])  # second scrub
    assert "assets added:      0" in result.output
    assert "copies added:      0" in result.output
    assert "copies updated:    4" in result.output
    assert "copies missing:    0" in result.output


def test_dedup_across_two_backends_into_one_asset(cli_env: dict[str, str], tmp_path: Path) -> None:
    """Same content on two backends → one logical asset, two copies.

    This is the spec's load-bearing dedup claim (spec-v0.1.md §2
    principle 3, §4.1: same hash = same row).
    """
    # Build a second fixture with the same first-asset content but a
    # different tape_uuid / object_id (so the copy isn't a duplicate-locator).
    import base64
    import hashlib

    content = b"hello world"  # matches the first object in the main fixture
    h = hashlib.sha256(content).hexdigest()
    second_fixture = tmp_path / "second.json"
    second_fixture.write_text(
        json.dumps(
            [
                {
                    "object_id": "00000000-0000-0000-0000-000000000099",
                    "caller_object_id": "asset-on-second-backend",
                    "content_sha256": h,
                    "logical_size_bytes": len(content),
                    "body_format": "rem-tar-v1",
                    "caller_metadata": {"campaign": "second"},
                    "content_b64": base64.b64encode(content).decode(),
                    "copies": [
                        {
                            "tape_uuid": "d" * 32,
                            "tape_file_number": 1,
                            "first_body_lba": 0,
                            "health": "ok",
                        }
                    ],
                }
            ]
        )
    )

    _run(["db", "init"])
    _run(["backends", "add", "primary", "--kind", "rem_tape", "--fixture", str(FIXTURE)])
    _run(["backends", "add", "mirror", "--kind", "rem_tape", "--fixture", str(second_fixture)])

    _run(["scrub", "--backend", "primary"])
    second = _run(["scrub", "--backend", "mirror"])

    # The mirror's only asset is byte-identical to one already in catalog.
    # → 0 assets added (deduplicated), 1 copy added.
    assert "assets added:      0" in second.output
    assert "copies added:      1" in second.output

    # JSON listing confirms one asset now has copies across two backends.
    listed = _run(["list", "assets", "--json"])
    rows = [json.loads(line) for line in listed.output.strip().split("\n")]
    [shared] = [r for r in rows if r["content_sha256"] == h]
    assert shared["copies_by_backend"] == {"primary": 1, "mirror": 1}


def test_scrub_marks_missing_when_backend_loses_an_object(
    cli_env: dict[str, str], tmp_path: Path
) -> None:
    """A copy that was previously present and is no longer enumerated must
    be marked MISSING, not deleted."""
    import base64
    import hashlib

    # Initial fixture: two objects.
    full = tmp_path / "full.json"
    objects = []
    for i, content in enumerate([b"first", b"second"]):
        objects.append(
            {
                "object_id": f"00000000-0000-0000-0000-{i:012x}",
                "caller_object_id": f"a{i}",
                "content_sha256": hashlib.sha256(content).hexdigest(),
                "logical_size_bytes": len(content),
                "content_b64": base64.b64encode(content).decode(),
                "copies": [
                    {
                        "tape_uuid": f"{i:032x}",
                        "tape_file_number": 1,
                        "first_body_lba": 0,
                        "health": "ok",
                    }
                ],
            }
        )
    full.write_text(json.dumps(objects))
    _run(["db", "init"])
    _run(["backends", "add", "tape1", "--kind", "rem_tape", "--fixture", str(full)])
    _run(["scrub", "--backend", "tape1"])

    # Now point the same backend at a smaller fixture missing the second object.
    smaller = tmp_path / "smaller.json"
    smaller.write_text(json.dumps(objects[:1]))

    # Re-register at the new fixture (in production this would be an
    # operator's `backends update` or moved underlying storage; we simulate
    # by re-creating the backend via SQL).
    from sqlalchemy import select

    from sutradhara.catalog.models import Backend
    from sutradhara.catalog.session import make_engine, session_scope

    engine = make_engine()
    with session_scope(engine) as s:
        b = s.scalars(select(Backend).where(Backend.name == "tape1")).one()
        b.config = {"fixture_path": str(smaller)}

    result = _run(["scrub", "--backend", "tape1"])
    assert "copies updated:    1" in result.output
    assert "copies missing:    1" in result.output

    listed = _run(["list", "assets", "--json"])
    rows = [json.loads(line) for line in listed.output.strip().split("\n")]
    missing_hash = hashlib.sha256(b"second").hexdigest()
    [missing_asset] = [r for r in rows if r["content_sha256"] == missing_hash]
    assert missing_asset["copies_by_backend"] == {}


def test_scrub_unknown_backend_exits_nonzero(cli_env: dict[str, str]) -> None:
    _run(["db", "init"])
    result = _run(["scrub", "--backend", "no-such-backend"], expect_exit=2)
    assert "no backend named" in result.output


def test_backend_without_required_config_errors(cli_env: dict[str, str]) -> None:
    """rem_tape backend needs either live daemon config or an explicit dev fixture."""
    _run(["db", "init"])
    _run(["backends", "add", "broken", "--kind", "rem_tape"])
    result = _run(["scrub", "--backend", "broken"], expect_exit=2)
    assert "needs config.fixture_path" in result.output
    assert "config.daemon_endpoint" in result.output
