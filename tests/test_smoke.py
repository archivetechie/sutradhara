"""Smoke tests — confirm scaffold is wired correctly."""

from __future__ import annotations

from click.testing import CliRunner

from sutradhara import __version__
from sutradhara.cli.main import cli


def test_version_is_set() -> None:
    assert __version__ == "0.0.1"


def test_cli_runs() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert "0.0.1" in result.output


def test_cli_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Sutradhara" in result.output
