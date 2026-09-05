"""Repository-level source policies that must survive future refactors."""

from __future__ import annotations

import ast
from pathlib import Path


def test_production_source_has_no_optimizable_assert_statements() -> None:
    """Runtime invariants must not disappear when Python runs with ``-O``."""

    root = Path(__file__).resolve().parents[1] / "src" / "sutradhara"
    violations: list[str] = []
    for path in sorted(root.rglob("*.py")):
        if "_proto" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        violations.extend(
            f"{path.relative_to(root.parent.parent)}:{node.lineno}"
            for node in ast.walk(tree)
            if isinstance(node, ast.Assert)
        )
    assert violations == []
