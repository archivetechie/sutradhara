"""Regression tests for the receive-core Rust migration fixture corpus."""

from __future__ import annotations

import difflib
import importlib.util
import json
from collections.abc import Mapping
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
EXTRACTOR_PATH = REPO_ROOT / "packages/sutradhara-receive/scripts/extract_fixtures.py"
FIXTURE_ROOT = REPO_ROOT / "packages/sutradhara-receive/fixtures"
CORPUS_KEYS = [
    "public_api",
    "strings",
    "writer_outputs",
    "receive_bags",
    "validate_mismatch",
    "cli_matrix",
    "verify_sidecars",
]


@pytest.fixture(scope="module")
def fixture_extractor() -> ModuleType:
    spec = importlib.util.spec_from_file_location("receive_fixture_extractor", EXTRACTOR_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def regenerated_corpus(
    fixture_extractor: ModuleType,
    tmp_path_factory: pytest.TempPathFactory,
) -> Mapping[str, Any]:
    return fixture_extractor.build_corpus(tmp_path_factory.mktemp("receive-core-corpus"))


@pytest.fixture(scope="module")
def committed_corpus(fixture_extractor: ModuleType) -> Mapping[str, Any]:
    return fixture_extractor.load_committed_corpus(FIXTURE_ROOT)


@pytest.mark.parametrize("corpus_key", CORPUS_KEYS)
def test_receive_core_fixture_matches_current_python_contract(
    corpus_key: str,
    committed_corpus: Mapping[str, Any],
    regenerated_corpus: Mapping[str, Any],
) -> None:
    assert corpus_key in committed_corpus
    assert corpus_key in regenerated_corpus
    _assert_json_equal(committed_corpus[corpus_key], regenerated_corpus[corpus_key])


def test_receive_bag_fixture_cases_are_complete_and_valid(
    committed_corpus: Mapping[str, Any],
) -> None:
    for case in committed_corpus["receive_bags"]["cases"]:
        assert case["validation"]["complete"] is True
        assert case["validation"]["valid"] is True


def _assert_json_equal(left: Any, right: Any) -> None:
    left_json = json.dumps(left, indent=2, sort_keys=True).splitlines()
    right_json = json.dumps(right, indent=2, sort_keys=True).splitlines()
    if left_json == right_json:
        return
    diff = difflib.unified_diff(
        left_json,
        right_json,
        fromfile="committed",
        tofile="regenerated",
        lineterm="",
    )
    pytest.fail("fixture drift:\n" + "\n".join(diff))
