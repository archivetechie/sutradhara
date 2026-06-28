"""Edge-agent receive workflow tests.

These tests prove the first `sutra-agent` layer stays outside the receive
contract: it delegates byte movement to `sutradhara-receive`, records local
operator state, and only marks source release safe after server intake writes the
verified marker.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine

from sutra_agent.cli import main as agent_main
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import IntakeStatus
from sutradhara.intake import register_landing_root
from sutradhara_receive import ReceiveError, read_bag_info, receive_source, validate_bag


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_agent_receive_records_pending_ledger(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    config, ledger = _init_config(tmp_path, landing, capsys)

    exit_code = agent_main(
        ["receive", "--config", str(config), "--fake-source", str(source), "--json"]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert captured.err == ""
    assert payload["confirmation_status"] == "pending"
    assert payload["release_ok"] is False
    assert payload["ledger_path"] == str(ledger)
    assert validate_bag(Path(payload["intake_dir"])).valid is True
    assert "Receive-Package" in read_bag_info(Path(payload["intake_dir"]) / "bag-info.txt")

    ledger_payload = json.loads(ledger.read_text(encoding="utf-8"))
    assert ledger_payload["schema"] == "sutra-agent-ledger-v1"
    assert [run["intake_id"] for run in ledger_payload["runs"]] == [payload["intake_id"]]
    assert ledger_payload["runs"][0]["confirmation"]["status"] == "pending"


def test_agent_status_sees_server_verified_marker(
    engine: Engine,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    config, _ledger = _init_config(tmp_path, landing, capsys)
    receive_payload = _agent_receive_json(config, source, capsys)

    with session_scope(engine) as session:
        outcomes = register_landing_root(session, landing, cache_root=tmp_path / "cache")

    assert outcomes[0].status == IntakeStatus.REGISTERED.value
    assert (Path(receive_payload["intake_dir"]) / "intake.verified.json").is_file()

    exit_code = agent_main(
        ["status", "--config", str(config), receive_payload["intake_id"], "--json"]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    record = payload["runs"][0]
    assert exit_code == 0
    assert record["intake_id"] == receive_payload["intake_id"]
    assert record["confirmation"]["status"] == "verified"
    assert record["confirmation"]["release_ok"] is True


def test_agent_status_sees_server_quarantine_marker(
    engine: Engine,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"video")
    config, _ledger = _init_config(tmp_path, landing, capsys)
    receive_payload = _agent_receive_json(config, source, capsys)
    manifest = Path(receive_payload["intake_dir"]) / "manifest-sha256.txt"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            hashlib.sha256(b"video").hexdigest(), "0" * 64
        ),
        encoding="utf-8",
    )

    with session_scope(engine) as session:
        outcomes = register_landing_root(session, landing)

    assert outcomes[0].status == IntakeStatus.QUARANTINED.value
    intake_dir = Path(receive_payload["intake_dir"])
    assert (intake_dir / "intake.quarantined.json").is_file()
    (intake_dir / "intake.verified.json").write_text(
        '{"unexpected": "conflicting marker"}',
        encoding="utf-8",
    )

    exit_code = agent_main(
        ["status", "--config", str(config), receive_payload["intake_id"], "--json"]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    record = payload["runs"][0]
    assert exit_code == 0
    assert record["confirmation"]["status"] == "quarantined"
    assert record["confirmation"]["release_ok"] is False


def test_agent_status_for_specific_missing_ledger_is_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = tmp_path / "agent-config.json"
    missing_ledger = tmp_path / "missing-ledger.json"
    exit_code = agent_main(
        [
            "config",
            "init",
            "--config",
            str(config),
            "--landing",
            str(tmp_path / "landing"),
            "--operator",
            "Op",
            "--ledger",
            str(missing_ledger),
        ]
    )
    capsys.readouterr()
    assert exit_code == 0

    exit_code = agent_main(["status", "--config", str(config), "unknown-intake"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "intake is not tracked in agent ledger: unknown-intake" in captured.err


def test_agent_receive_resumes_explicit_partial_intake(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "source"
    landing = tmp_path / "landing"
    source.mkdir()
    (source / "a.mov").write_bytes(b"a")
    (source / "b.mov").write_bytes(b"b")
    config, _ledger = _init_config(tmp_path, landing, capsys)

    def crash_after_copy(_payload: Path, _receipts: tuple[Any, ...]) -> None:
        raise ReceiveError("simulated crash")

    with pytest.raises(ReceiveError, match="simulated crash"):
        receive_source(
            source,
            landing=landing,
            source_kind="card",
            operator="Op",
            after_copy_hook=crash_after_copy,
        )
    intake_id = next(path.name for path in landing.iterdir() if path.is_dir())

    exit_code = agent_main(["receive", "--config", str(config), "--resume", intake_id, "--json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["intake_id"] == intake_id
    assert payload["resume_of"] == intake_id
    assert validate_bag(Path(payload["intake_dir"])).valid is True


def test_agent_receive_sweeps_stale_partial_intakes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    landing = tmp_path / "landing"
    stale = landing / "stale"
    fresh = landing / "fresh"
    for path in (stale, fresh):
        path.mkdir(parents=True)
        (path / ".receiving.json").write_text("{}", encoding="utf-8")
    old = dt.datetime.now().timestamp() - 48 * 3600
    os.utime(stale / ".receiving.json", (old, old))
    config, _ledger = _init_config(tmp_path, landing, capsys)

    exit_code = agent_main(
        [
            "receive",
            "sweep",
            "--config",
            str(config),
            "--older-than-hours",
            "24",
            "--json",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload == {"removed": [str(stale)]}
    assert not stale.exists()
    assert fresh.exists()


def _init_config(
    tmp_path: Path,
    landing: Path,
    capsys: pytest.CaptureFixture[str],
) -> tuple[Path, Path]:
    config = tmp_path / "agent-config.json"
    ledger = tmp_path / "agent-ledger.json"
    exit_code = agent_main(
        [
            "config",
            "init",
            "--config",
            str(config),
            "--landing",
            str(landing),
            "--operator",
            "Op",
            "--ledger",
            str(ledger),
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.err == ""
    assert json.loads(captured.out)["config_path"] == str(config)
    return config, ledger


def _agent_receive_json(
    config: Path,
    source: Path,
    capsys: pytest.CaptureFixture[str],
) -> dict[str, Any]:
    exit_code = agent_main(
        ["receive", "--config", str(config), "--fake-source", str(source), "--json"]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert isinstance(payload, dict)
    return payload
