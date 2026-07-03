"""Tool-version providers used by job handlers and reconciler reopen logic.

Handlers record the external tool version that classified a blocked condition.
The reconciler can later compare the stored version with the current provider
value and reopen blocked work after an operator upgrades the tool.
"""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Callable

from sutradhara.resource_control import run_managed

ToolVersionProvider = Callable[[], str]

_PROVIDERS: dict[str, ToolVersionProvider] = {}


def register_tool_version(tool: str, provider: ToolVersionProvider) -> ToolVersionProvider | None:
    """Register a version provider and return the previous provider, if any."""

    previous = _PROVIDERS.get(tool)
    _PROVIDERS[tool] = provider
    return previous


def unregister_tool_version(tool: str) -> None:
    """Remove a version provider; intended for tests."""

    _PROVIDERS.pop(tool, None)


def current_tool_version(tool: str) -> str:
    """Return the current version string for ``tool``, or ``"unknown"``."""

    provider = _PROVIDERS.get(tool)
    if provider is None:
        return _command_tool_version(tool)
    return provider()


def _command_tool_version(tool: str) -> str:
    path = shutil.which(tool)
    if path is None:
        return "unknown"
    try:
        completed = run_managed(
            [path, "-version"],
            role="medium",
            cpu_lease=1,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "unknown"
    lines = (completed.stdout or completed.stderr or "").splitlines()
    return lines[0][:128] if lines else "unknown"


register_tool_version("ffmpeg", lambda: _command_tool_version("ffmpeg"))
register_tool_version("ffprobe", lambda: _command_tool_version("ffprobe"))
