"""Regression coverage for external-tool and Python-distribution version probes."""

from __future__ import annotations

from sutradhara.jobs import tool_versions


def test_pfr_tools_use_distribution_version_provider(monkeypatch) -> None:
    seen: list[str] = []

    def fake_distribution_version(distribution: str) -> str:
        seen.append(distribution)
        return "1.2.3"

    monkeypatch.setattr(tool_versions, "_distribution_version", fake_distribution_version)

    assert tool_versions.current_tool_version("format-anatomy") == "1.2.3"
    assert tool_versions.current_tool_version("pfr_core") == "1.2.3"
    assert seen == ["format-anatomy", "format-anatomy"]
