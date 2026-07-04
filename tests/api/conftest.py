"""Shared fixtures for Sutradhara operator API tests."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine

from sutradhara.api.app import create_app
from sutradhara.catalog.models import ArtifactClassPolicyRecord
from sutradhara.catalog.session import create_all, make_engine, session_scope


@pytest.fixture
def api_engine(tmp_path: Path) -> Iterator[Engine]:
    engine = make_engine(f"sqlite:///{tmp_path / 'api.db'}")
    create_all(engine)
    seed_artifactclass(engine, "s-masters")
    yield engine
    engine.dispose()


def make_api_app(engine: Engine):
    app = create_app(engine, ensure_schema=False)
    app.state.idempotency_wait_seconds = 2.0
    app.state.heartbeat_interval = dt.timedelta(milliseconds=50)
    return app


def seed_artifactclass(engine: Engine, artifactclass: str) -> None:
    with session_scope(engine) as session:
        session.merge(
            ArtifactClassPolicyRecord(
                artifactclass=artifactclass,
                ruleset="test.rules.v1",
                expect="messy",
                target_bytes=1024,
                max_age_seconds=3600,
                restore_preference=[],
                staging_config={},
            )
        )


def auth_headers(role: str = "operator") -> dict[str, str]:
    return {
        "X-Authentik-Username": "owner",
        "X-Authentik-Name": "Ada Operator",
        "X-Authentik-Email": "owner@example.test",
        "X-Authentik-Groups": _group_header_value(role),
    }


def post_headers(role: str = "operator") -> dict[str, str]:
    return {
        **auth_headers(role),
        "Origin": "http://testserver",
        "Host": "testserver",
        "Content-Type": "application/json",
    }


def _group_header_value(role: str) -> str:
    if role.startswith("sutradhara-") or "|" in role:
        return role
    if role == "operator":
        return "sutradhara-ingest"
    if role == "viewer":
        return "sutradhara-oversight"
    return f"sutradhara-{role}"
