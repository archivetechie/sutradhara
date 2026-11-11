"""Validate restore retry policy when constructing an API app, without HTTP clients."""

from __future__ import annotations

import pytest
from sqlalchemy import Engine

from sutradhara.api.app import create_app


def test_malformed_restore_retry_policy_prevents_app_construction(
    api_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Invalid retry configuration fails before any restore route can be served."""

    monkeypatch.setenv("SUTRADHARA_RESTORE_RETRY_MAX_FAILURES", "x")
    with pytest.raises(ValueError, match="invalid literal"):
        create_app(api_engine, ensure_schema=False)


def test_restore_retry_policy_is_stored_on_app(
    api_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each app keeps the validated retry policy loaded during its construction."""

    monkeypatch.setenv("SUTRADHARA_RESTORE_RETRY_MAX_FAILURES", "3")
    app = create_app(api_engine, ensure_schema=False)
    assert app.state.restore_retry_policy.max_failures == 3
