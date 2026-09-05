"""Regression coverage for optional integrations in a standalone installation."""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

from sutradhara import optional


def test_pfr_compatibility_constants_precede_handler_registration() -> None:
    """A stale enum must fail before the real handler can enter the registry."""

    source = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "sutradhara"
        / "jobs"
        / "handlers"
        / "pfr_index.py"
    ).read_text()
    tree = ast.parse(source)
    compatibility_names = {
        "_RETRYABLE_REASON_IDS",
        "_FALLBACK_REASON_IDS",
        "_PARSE_DETERMINATION_REASON_IDS",
        "_LOUD_STOP_REASON_IDS",
    }
    assignments = {
        node.targets[0].id: node.lineno
        for node in tree.body
        if isinstance(node, ast.Assign)
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id in compatibility_names
    }
    handler_line = next(
        node.lineno
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "handle_pfr_index"
    )
    assert assignments.keys() == compatibility_names
    assert max(assignments.values()) < handler_line


def test_cli_import_does_not_require_pfr_core() -> None:
    """The base CLI import graph must not eagerly load the optional PFR package."""

    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "Blocker = type('Blocker', (), {'find_spec': lambda self, name, path=None, "
                "target=None: (_ for _ in ()).throw(ImportError(name)) "
                "if name == 'pfr_core' else None}); "
                "sys.meta_path.insert(0, Blocker()); "
                "import sutradhara.cli.main; "
                "raise SystemExit(int('pfr_core' in sys.modules))"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_require_pfr_core_has_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicitly requested PFR operation explains the missing integration."""

    monkeypatch.setattr(optional, "find_spec", lambda _name: None)
    with pytest.raises(optional.OptionalDependencyError, match="optional format-anatomy"):
        optional.require_pfr_core()


def test_missing_pfr_handler_records_blocked_without_failed_job() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "Blocker = type('Blocker', (), {'find_spec': lambda self, name, path=None, "
                "target=None: (_ for _ in ()).throw(ImportError(name)) "
                "if name == 'pfr_core' else None}); "
                "sys.meta_path.insert(0, Blocker()); "
                "import sutradhara.jobs.handlers; "
                "from sutradhara.jobs import tool_versions; "
                "tool_versions._distribution_version = lambda _name: 'unknown'; "
                "from sutradhara.jobs.registry import get_handler; "
                "result = get_handler('pfr-index')(None); "
                "assert result.ok; "
                "assert result.condition.condition == 'blocked'; "
                "assert result.condition.blocked_tool == ('format-anatomy', 'missing')"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_incompatible_pfr_installation_falls_back_without_breaking_cli(tmp_path: Path) -> None:
    """A discoverable package with a stale API must degrade like a missing integration."""

    (tmp_path / "pfr_core.py").write_text("# deliberately incomplete contract stub\n")
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sutradhara.cli.main; "
                "import sutradhara.jobs.handlers; "
                "from types import SimpleNamespace; "
                "from sutradhara.jobs.reconcilers.derivation import _has_pfr_sidecar; "
                "from sutradhara.jobs.registry import get_handler; "
                "result = get_handler('pfr-index')(None); "
                "assert result.ok; "
                "assert result.condition.condition == 'blocked'; "
                "assert not _has_pfr_sidecar(SimpleNamespace("
                "item_metadata={'pfr_sidecar_path': '/tmp/stale.pfr'}))"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
        env={"PATH": "", "PYTHONPATH": str(tmp_path)},
    )
    assert completed.returncode == 0, completed.stderr
