"""Tests for the shared Remanence archive-build CLI adapter."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from sutradhara.rem_archive_cli import (
    resolve_rem_bin,
    run_rem_archive_build,
    run_rem_archive_scan,
)


def test_rem_bin_resolver_uses_env_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rem = _write_executable(tmp_path / "custom-rem")
    monkeypatch.setenv("REM_BIN", str(rem))
    monkeypatch.setenv("PATH", "")

    assert resolve_rem_bin() == str(rem)


def test_rem_bin_resolver_uses_path_lookup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rem = _write_executable(tmp_path / "rem")
    monkeypatch.delenv("REM_BIN", raising=False)
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))

    assert resolve_rem_bin() == str(rem)


def test_rem_bin_resolver_uses_home_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    home = tmp_path / "home"
    rem = _write_executable(home / "remanence" / "target" / "release" / "rem")
    monkeypatch.delenv("REM_BIN", raising=False)
    monkeypatch.setenv("PATH", "")
    monkeypatch.setenv("HOME", str(home))

    assert resolve_rem_bin() == str(rem)


def test_rem_bin_resolver_missing_binary_is_actionable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("REM_BIN", raising=False)
    monkeypatch.setenv("PATH", "")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))

    with pytest.raises(FileNotFoundError, match="Set REM_BIN"):
        resolve_rem_bin()


def test_run_rem_archive_build_uses_current_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rem = _write_executable(tmp_path / "rem")
    rules = tmp_path / "rules.rem"
    rules.write_text("blob **/\n", encoding="utf-8")
    key_file = tmp_path / "root.key"
    key_file.write_bytes(b"k" * 32)
    inputs = [tmp_path / "intake"]
    inputs[0].mkdir()
    output = tmp_path / "out.rao"
    manifest = tmp_path / "manifest.json"
    captured: dict[str, list[str]] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["cmd"] = cmd
        assert kwargs == {"capture_output": True, "text": True, "check": False}
        output.write_bytes(b"rao bytes")
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=json.dumps({"files": [], "stored_digest": "unused"}) + "\n",
            stderr="",
        )

    monkeypatch.setattr("sutradhara.rem_archive_cli.subprocess.run", fake_run)

    result = run_rem_archive_build(
        inputs=inputs,
        ruleset=rules,
        output_path=output,
        manifest_path=manifest,
        rem_bin=rem,
        encrypt=True,
        key_id="a" * 32,
        key_file=key_file,
        failure_label="test rem build",
    )

    cmd = captured["cmd"]
    assert cmd[:3] == [str(rem), "archive", "build"]
    assert "--out" in cmd
    assert "--output" not in cmd
    assert "--inputs" in cmd
    assert cmd[cmd.index("--inputs") + 1 :] == [str(inputs[0])]
    assert "--key-file" in cmd
    assert cmd[cmd.index("--key-file") + 1] == str(key_file)
    assert "--key-id" in cmd
    assert cmd[cmd.index("--key-id") + 1] == "a" * 32
    assert "--key-epoch" not in cmd
    assert result.stored_digest == hashlib.sha256(b"rao bytes").digest()


def test_run_rem_archive_build_uses_map_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rem = _write_executable(tmp_path / "rem")
    source_map = tmp_path / "source-map.tsv"
    source_root = tmp_path / "source"
    source_root.mkdir()
    source_map.write_text(
        "archive_path\tsource_path\tsha256\tsize\tingest_item_id\n",
        encoding="utf-8",
    )
    output = tmp_path / "out.rao"
    captured: dict[str, list[str]] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["cmd"] = cmd
        output.write_bytes(b"map rao bytes")
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=json.dumps({"files": [], "stored_digest": "unused"}) + "\n",
            stderr="",
        )

    monkeypatch.setattr("sutradhara.rem_archive_cli.subprocess.run", fake_run)

    run_rem_archive_build(
        map_path=source_map,
        source_root=source_root,
        map_sha256="a" * 64,
        output_path=output,
        rem_bin=rem,
    )

    cmd = captured["cmd"]
    assert "--map" in cmd
    assert cmd[cmd.index("--map") + 1] == str(source_map)
    assert "--source-root" in cmd
    assert cmd[cmd.index("--source-root") + 1] == str(source_root)
    assert "--map-sha256" in cmd
    assert cmd[cmd.index("--map-sha256") + 1] == "a" * 64
    assert "--inputs" not in cmd
    assert "--rules" not in cmd


def test_run_rem_archive_build_rejects_map_inputs_mix(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="cannot be combined"):
        run_rem_archive_build(
            inputs=[tmp_path / "input"],
            map_path=tmp_path / "source-map.tsv",
            source_root=tmp_path,
            output_path=tmp_path / "out.rao",
        )


def test_run_rem_archive_build_failure_includes_command_and_stderr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rem = _write_executable(tmp_path / "rem")
    output = tmp_path / "out.rao"

    def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(cmd, 2, stdout="", stderr="bad flag")

    monkeypatch.setattr("sutradhara.rem_archive_cli.subprocess.run", fake_run)

    with pytest.raises(RuntimeError, match="bad flag"):
        run_rem_archive_build(
            inputs=[tmp_path],
            ruleset=None,
            output_path=output,
            rem_bin=rem,
            failure_label="test rem build",
        )


def test_run_rem_archive_scan_uses_current_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rem = _write_executable(tmp_path / "rem")
    rules = tmp_path / "rules.rem"
    rules.write_text("blob **/\n", encoding="utf-8")
    source = tmp_path / "intake"
    source.mkdir()
    captured: dict[str, list[str]] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["cmd"] = cmd
        assert kwargs == {"capture_output": True, "text": True, "check": False}
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=json.dumps({"scan": {"clusters": []}}) + "\n",
            stderr="",
        )

    monkeypatch.setattr("sutradhara.rem_archive_cli.subprocess.run", fake_run)

    assert run_rem_archive_scan(inputs=[source], ruleset=rules, rem_bin=rem) == {
        "scan": {"clusters": []}
    }

    cmd = captured["cmd"]
    assert cmd[:4] == [str(rem), "archive", "build", "--scan-only"]
    assert "--scan-out" not in cmd
    assert "--inputs" in cmd
    assert cmd[cmd.index("--inputs") + 1 :] == [str(source)]


def _write_executable(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    path.chmod(0o755)
    return path
