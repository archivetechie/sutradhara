"""Shared fixtures for Sutradhara operator API tests."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine

from sutradhara.api.app import create_app
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    BundlingPolicy,
    DurabilityPolicy,
    PlacementPolicy,
    apply_artifactclass_policy,
)
from sutradhara.catalog.models import Backend, Pool
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier
from sutradhara.sealing.port import Representation

_POLICY_BACKEND_NAME = "api-fixture-policy-backend"
_POLICY_POOL_ID = "api-fixture-policy-pool"


@pytest.fixture
def api_engine(tmp_path: Path) -> Iterator[Engine]:
    engine = make_engine(f"sqlite:///{tmp_path / 'api.db'}")
    create_all(engine)
    administer_artifactclass(engine, "s-masters")
    yield engine
    engine.dispose()


@pytest.fixture(autouse=True)
def clear_agent_bundle_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUTRA_AGENT_BUNDLE_CONFIG", raising=False)


def make_api_app(engine: Engine):
    app = create_app(engine, ensure_schema=False)
    app.state.idempotency_wait_seconds = 2.0
    app.state.heartbeat_interval = dt.timedelta(milliseconds=50)
    return app


def administer_artifactclass(engine: Engine, artifactclass: str) -> None:
    """Administer an API-test artifactclass through the production policy path."""

    with session_scope(engine) as session:
        pool = session.get(Pool, _POLICY_POOL_ID)
        if pool is None:
            backend = Backend(
                name=_POLICY_BACKEND_NAME,
                kind=BackendKind.MEMORY,
                tier=BackendTier.SELF_DESCRIBING,
            )
            session.add(backend)
            session.flush([backend])
            pool = Pool(
                id=_POLICY_POOL_ID,
                backend_id=backend.id,
                representation=Representation.RAW_BYTES.value,
                location="test",
                storage_class="archive",
            )
            session.add(pool)
            session.flush([pool])
        apply_artifactclass_policy(
            session,
            artifactclass,
            ArtifactClassPolicy(
                ruleset="test.rules.v1",
                placements=(PlacementPolicy(_POLICY_POOL_ID, role="test"),),
                bundling=BundlingPolicy(target_gb=1 / 1024**2, max_age_seconds=3600),
                restore_preference=(_POLICY_POOL_ID,),
                expect="messy",
                durability=DurabilityPolicy(min_copies=1, min_impl_families=1),
            ),
        )


def auth_headers(role: str = "operator") -> dict[str, str]:
    return {
        "X-Authentik-Username": "ada",
        "X-Authentik-Name": "Ada Operator",
        "X-Authentik-Email": "ada@example.test",
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
