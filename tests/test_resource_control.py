"""Tests for cgroup-backed subprocess resource control."""

from __future__ import annotations

import logging
import signal
import subprocess
import sys
from typing import Any, cast

import pytest

from sutradhara import resource_control as rc


@pytest.fixture(autouse=True)
def _clean_capability_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUTRADHARA_RESOURCE_CONTROL", raising=False)
    monkeypatch.delenv("SUTRADHARA_RESOURCE_CONTROL_REQUIRE", raising=False)
    monkeypatch.delenv("SUTRADHARA_RESOURCE_CONTROL_SYSTEMD", raising=False)
    rc.clear_capability_cache()


def test_run_managed_builds_systemd_scope_argv_for_profiles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        rc,
        "capability",
        lambda: rc.ResourceCapability(
            mode="systemd",
            manager="user",
            properties=frozenset({"CPUWeight", "IOWeight"}),
        ),
    )
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((cmd, dict(kwargs)))
        return subprocess.CompletedProcess(cmd, 0, stdout="child-out", stderr="child-err")

    monkeypatch.setattr("sutradhara.resource_control.subprocess.run", fake_run)

    expected = {
        "high": ("CPUWeight=1000", "IOWeight=1000", ["ionice", "-c", "2", "-n", "4"]),
        "medium": ("CPUWeight=100", "IOWeight=100", ["ionice", "-c", "2", "-n", "4"]),
        "low": ("CPUWeight=25", "IOWeight=10", ["ionice", "-c", "3"]),
    }
    for role, (cpu_weight, io_weight, ionice_prefix) in expected.items():
        result = rc.run_managed(
            ["/bin/true"],
            role=role,
            cpu_lease=8,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.stdout == "child-out"
        cmd, kwargs = calls[-1]
        assert "--pipe" not in cmd
        assert cmd[:6] == [
            "systemd-run",
            "--user",
            "--scope",
            "--collect",
            "--quiet",
            "--no-ask-password",
        ]
        assert any(part.startswith("--unit=sutradhara-rc-") for part in cmd)
        properties = [cmd[index + 1] for index, value in enumerate(cmd) if value == "-p"]
        assert cpu_weight in properties
        assert io_weight in properties
        child = cmd[cmd.index("--") + 1 :]
        assert child[: len(ionice_prefix)] == ionice_prefix
        assert "nice" in child
        nice_index = child.index("nice")
        assert int(child[nice_index + 2]) >= 0
        if role == "low":
            assert child[:3] == ["ionice", "-c", "3"]
            assert child[3:6] == ["nice", "-n", "19"]
        assert child[-1] == "/bin/true"
        assert kwargs["capture_output"] is True
        assert kwargs["text"] is True
        assert kwargs["check"] is False


def test_child_output_is_preserved_on_systemd_and_degraded_paths(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(
        rc,
        "capability",
        lambda: rc.ResourceCapability(
            mode="systemd",
            manager="user",
            properties=frozenset({"CPUWeight"}),
        ),
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(cmd, 7, stdout="stdout\n", stderr="stderr\n")

    monkeypatch.setattr("sutradhara.resource_control.subprocess.run", fake_run)
    result = rc.run_managed(
        ["/bin/false"],
        role="medium",
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 7
    assert result.stdout == "stdout\n"
    assert result.stderr == "stderr\n"

    monkeypatch.setitem(
        rc.RESOURCE_PROFILES,
        "plain",
        rc.ResourceProfile(cpu_weight=100, io_weight=100, nice=0, ionice=None),
    )
    monkeypatch.setattr(
        rc,
        "capability",
        lambda: rc.ResourceCapability(
            mode="degraded",
            manager="user",
            properties=frozenset(),
            reason="forced test fallback",
        ),
    )
    caplog.set_level(logging.ERROR, logger="sutradhara.resource_control")
    result = rc.run_managed(
        [
            sys.executable,
            "-c",
            "import sys; print('plain-out'); print('plain-err', file=sys.stderr); sys.exit(3)",
        ],
        role="plain",
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 3
    assert result.stdout == "plain-out\n"
    assert result.stderr == "plain-err\n"
    assert "resource enforcement degraded" in caplog.text


def test_degraded_mode_logs_error_once_and_require_raises(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setitem(
        rc.RESOURCE_PROFILES,
        "plain",
        rc.ResourceProfile(cpu_weight=100, io_weight=100, nice=0, ionice=None),
    )
    monkeypatch.setattr(
        rc,
        "capability",
        lambda: rc.ResourceCapability(
            mode="degraded",
            manager="user",
            properties=frozenset(),
            reason="forced test fallback",
        ),
    )

    caplog.set_level(logging.ERROR, logger="sutradhara.resource_control")
    for _index in range(2):
        result = rc.run_managed(
            [sys.executable, "-c", "print('ran')"],
            role="plain",
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0

    degraded_logs = [
        record for record in caplog.records if "resource enforcement degraded" in record.message
    ]
    assert len(degraded_logs) == 1
    assert degraded_logs[0].levelno == logging.ERROR

    rc.clear_capability_cache()
    monkeypatch.setenv("SUTRADHARA_RESOURCE_CONTROL_REQUIRE", "1")
    with pytest.raises(rc.ResourceControlUnavailable, match="forced test fallback"):
        rc.run_managed(
            [sys.executable, "-c", "print('ran')"],
            role="plain",
            capture_output=True,
            text=True,
            check=False,
        )


def test_capability_drops_rejected_quota_and_child_still_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(
        rc.RESOURCE_PROFILES,
        "quota",
        rc.ResourceProfile(cpu_weight=200, io_weight=100, nice=0, ionice=None, cpu_quota_pct=25),
    )
    monkeypatch.setattr("sutradhara.resource_control.shutil.which", lambda name: f"/usr/bin/{name}")
    launch_commands: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if cmd[-1] == "true":
            properties = {cmd[index + 1] for index, value in enumerate(cmd) if value == "-p"}
            if any(prop.startswith("CPUQuota=") for prop in properties):
                return subprocess.CompletedProcess(
                    cmd,
                    1,
                    stdout="",
                    stderr="Failed to set unit properties: CPUQuota",
                )
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        launch_commands.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="ran\n", stderr="")

    monkeypatch.setattr("sutradhara.resource_control.subprocess.run", fake_run)
    rc.clear_capability_cache()

    cap = rc.capability()
    assert cap.mode == "systemd"
    assert cap.properties == frozenset({"CPUWeight", "IOWeight"})

    result = rc.run_managed(
        ["/bin/echo", "ran"],
        role="quota",
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.stdout == "ran\n"
    properties = [
        launch_commands[0][index + 1]
        for index, value in enumerate(launch_commands[0])
        if value == "-p"
    ]
    assert "CPUWeight=200" in properties
    assert "IOWeight=100" in properties
    assert not any(prop.startswith("CPUQuota=") for prop in properties)


def test_child_nonzero_is_not_retried_but_setup_failure_degrades(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        rc,
        "capability",
        lambda: rc.ResourceCapability(
            mode="systemd",
            manager="user",
            properties=frozenset({"CPUWeight"}),
        ),
    )
    calls: list[list[str]] = []

    def child_failure(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="ffmpeg failed\n")

    monkeypatch.setattr("sutradhara.resource_control.subprocess.run", child_failure)
    result = rc.run_managed(
        ["/bin/false"],
        role="medium",
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert result.stderr == "ffmpeg failed\n"
    assert len(calls) == 1

    monkeypatch.setitem(
        rc.RESOURCE_PROFILES,
        "plain",
        rc.ResourceProfile(cpu_weight=100, io_weight=100, nice=0, ionice=None),
    )
    setup_calls: list[list[str]] = []

    def setup_failure(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        setup_calls.append(cmd)
        return subprocess.CompletedProcess(
            cmd,
            1,
            stdout="",
            stderr="Failed to start transient scope: denied\n",
        )

    monkeypatch.setattr("sutradhara.resource_control.subprocess.run", setup_failure)
    result = rc.run_managed(
        [sys.executable, "-c", "print('fallback-ran')"],
        role="plain",
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert result.stdout == "fallback-ran\n"
    assert len(setup_calls) == 1


def test_systemd_timeout_stops_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        rc,
        "capability",
        lambda: rc.ResourceCapability(
            mode="systemd",
            manager="user",
            properties=frozenset({"CPUWeight"}),
        ),
    )
    stop_commands: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "systemd-run":
            raise subprocess.TimeoutExpired(cmd, float(kwargs.get("timeout") or 0.01))
        stop_commands.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr("sutradhara.resource_control.subprocess.run", fake_run)

    with pytest.raises(subprocess.TimeoutExpired):
        rc.run_managed(["/bin/sleep", "60"], role="medium", timeout=0.01)

    assert len(stop_commands) == 1
    assert stop_commands[0][:3] == ["systemctl", "--user", "--no-ask-password"]
    assert stop_commands[0][3] == "stop"
    assert stop_commands[0][4].startswith("sutradhara-rc-medium-")


def test_degraded_timeout_kills_process_group(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(
        rc.RESOURCE_PROFILES,
        "plain",
        rc.ResourceProfile(cpu_weight=100, io_weight=100, nice=0, ionice=None),
    )
    monkeypatch.setattr(
        rc,
        "capability",
        lambda: rc.ResourceCapability(
            mode="degraded",
            manager="user",
            properties=frozenset(),
            reason="forced test fallback",
        ),
    )
    popen_kwargs: dict[str, Any] = {}

    class FakeProcess:
        pid = 4321
        returncode = None

        def __init__(self) -> None:
            self.calls = 0

        def communicate(
            self,
            input: Any = None,
            timeout: float | None = None,
        ) -> tuple[str, str]:
            self.calls += 1
            if self.calls == 1:
                raise subprocess.TimeoutExpired(
                    ["child"],
                    timeout or 0.01,
                    output="partial",
                    stderr="err",
                )
            self.returncode = -9
            return ("partial", "err")

    def fake_popen(args: list[str], **kwargs: Any) -> FakeProcess:
        popen_kwargs.update(kwargs)
        return FakeProcess()

    killpg_calls: list[tuple[int, int]] = []
    monkeypatch.setattr("sutradhara.resource_control.subprocess.Popen", fake_popen)
    monkeypatch.setattr(
        "sutradhara.resource_control.os.killpg",
        lambda pid, sig: killpg_calls.append((pid, sig)),
    )

    with pytest.raises(subprocess.TimeoutExpired) as raised:
        rc.run_managed(["child"], role="plain", timeout=0.01, capture_output=True, text=True)

    assert popen_kwargs["start_new_session"] is True
    assert killpg_calls == [(4321, signal.SIGKILL)]
    assert raised.value.output == "partial"
    assert cast(Any, raised.value.stderr) == "err"


def test_role_resolution_uses_kind_and_explicit_override_not_priority() -> None:
    assert rc.resource_role_for_job("restore") == "high"
    assert rc.resource_role_for_job("transcode") == "medium"
    assert rc.resource_role_for_job("verify") == "low"
    assert rc.resource_role_for_job("transcode", {"resource_role": "low"}) == "low"
    assert rc.resource_role_for_job("transcode", {"priority": -999}) == "medium"
    with pytest.raises(ValueError, match="unknown resource_role"):
        rc.resource_role_for_job("transcode", {"resource_role": 7})
