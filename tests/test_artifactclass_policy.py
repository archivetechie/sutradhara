"""Tests for strict artifactclass policy TOML documents."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, select

from sutradhara.artifactclass_policy import (
    ArtifactClassPolicyError,
    ArtifactClassPolicyWarning,
    UnknownPolicyPool,
    apply_artifactclass_policy,
    get_artifactclass_policy,
    parse_artifactclass_policy,
)
from sutradhara.catalog.models import ArtifactClassPolicyRecord, ArtifactClassPool, Backend, Pool
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def _policy_text() -> str:
    return """
ruleset = "rao.o.v1"
expect = "messy"

[[placements]]
pool = "o-copy-1-pool"
role = "plain"

[[placements]]
pool = "o-copy-2-pool"
role = "encrypted"

[bundling]
target_gb = 32
max_age = "48h"

[restore]
preference = ["o-copy-1-pool", "o-copy-2-pool"]
"""


def _staging_policy_text() -> str:
    return (
        _policy_text()
        + """
[staging.appledouble]
action = "merge-to-xattrs"
tool = "sutradhara-parser"
on_error = "hold"
record = true

[staging.compression]
codec = "zstd"
level = 10
globs = ["**/*.img"]
min_bytes = 1024
"""
    )


def _hdcache_policy_text() -> str:
    return (
        _policy_text()
        + """
[hdcache]
enabled = true
privacy_level = "p3"
"""
    )


def _durability_override(min_copies: int = 2, min_impl_families: int = 1) -> str:
    return f"""
[durability]
min_copies = {min_copies}
min_impl_families = {min_impl_families}
"""


def test_parse_artifactclass_policy_accepts_strict_document() -> None:
    policy = parse_artifactclass_policy(_policy_text())

    assert policy.ruleset == "rao.o.v1"
    assert policy.expect == "messy"
    assert [placement.pool for placement in policy.placements] == [
        "o-copy-1-pool",
        "o-copy-2-pool",
    ]
    assert policy.bundling.target_gb == 32
    assert policy.bundling.max_age_seconds == 48 * 3600
    assert policy.restore_preference == ("o-copy-1-pool", "o-copy-2-pool")
    assert policy.staging.appledouble.action == "off"
    assert policy.staging.compression.codec == "off"
    assert policy.hdcache.enabled is False
    assert policy.hdcache.privacy_level == "none"
    assert policy.durability.min_copies == 3
    assert policy.durability.min_impl_families == 2


def test_parse_artifactclass_policy_accepts_strict_staging_config() -> None:
    policy = parse_artifactclass_policy(_staging_policy_text())

    assert policy.staging.appledouble.action == "merge-to-xattrs"
    assert policy.staging.appledouble.tool == "sutradhara-parser"
    assert policy.staging.appledouble.on_error == "hold"
    assert policy.staging.compression.codec == "zstd"
    assert policy.staging.compression.level == 10
    assert policy.staging.compression.globs == ("**/*.img",)
    assert policy.staging.compression.min_bytes == 1024


def test_parse_artifactclass_policy_accepts_strict_hdcache_config() -> None:
    policy = parse_artifactclass_policy(_hdcache_policy_text())

    assert policy.hdcache.enabled is True
    assert policy.hdcache.privacy_level == "p3"


def test_parse_artifactclass_policy_accepts_durability_override() -> None:
    policy = parse_artifactclass_policy(_policy_text() + _durability_override())

    assert policy.durability.min_copies == 2
    assert policy.durability.min_impl_families == 1


def test_parse_artifactclass_policy_rejects_unknown_keys() -> None:
    text = _policy_text() + "\nextra = true\n"

    with pytest.raises(ArtifactClassPolicyError, match="unknown key"):
        parse_artifactclass_policy(text)


def test_parse_artifactclass_policy_rejects_unknown_staging_keys() -> None:
    text = _policy_text() + '\n[staging.compression]\ncodec = "zstd"\nlevel = 3\nsuffix = ".zst"\n'

    with pytest.raises(ArtifactClassPolicyError, match="unknown key"):
        parse_artifactclass_policy(text)


def test_parse_artifactclass_policy_rejects_unknown_hdcache_keys() -> None:
    text = _policy_text() + "\n[hdcache]\nenabled = true\nsurprise = true\n"

    with pytest.raises(ArtifactClassPolicyError, match="unknown key"):
        parse_artifactclass_policy(text)


def test_parse_artifactclass_policy_rejects_unknown_durability_keys() -> None:
    text = _policy_text() + "\n[durability]\nmin_copies = 2\nmin_impl_families = 1\nmedia = true\n"

    with pytest.raises(ArtifactClassPolicyError, match="unknown key"):
        parse_artifactclass_policy(text)


def test_parse_artifactclass_policy_rejects_bad_durability_floor() -> None:
    text = _policy_text() + "\n[durability]\nmin_copies = 0\nmin_impl_families = 1\n"

    with pytest.raises(ArtifactClassPolicyError, match="min_copies"):
        parse_artifactclass_policy(text)


def test_parse_artifactclass_policy_rejects_bad_hdcache_privacy() -> None:
    text = _policy_text() + '\n[hdcache]\nprivacy_level = "secret"\n'

    with pytest.raises(ArtifactClassPolicyError, match="privacy_level"):
        parse_artifactclass_policy(text)


def test_parse_artifactclass_policy_rejects_restore_dispatch_policy_block() -> None:
    text = _policy_text() + '\n[restore_dispatch]\nforeign_format = "bru-v1"\n'

    with pytest.raises(ArtifactClassPolicyError, match="unknown key"):
        parse_artifactclass_policy(text)


def test_parse_artifactclass_policy_requires_zstd_level() -> None:
    text = _policy_text() + '\n[staging.compression]\ncodec = "zstd"\n'

    with pytest.raises(ArtifactClassPolicyError, match="level"):
        parse_artifactclass_policy(text)


def test_parse_artifactclass_policy_requires_recorded_appledouble_merge() -> None:
    text = (
        _policy_text()
        + """
[staging.appledouble]
action = "merge-to-xattrs"
record = false
"""
    )

    with pytest.raises(ArtifactClassPolicyError, match="record"):
        parse_artifactclass_policy(text)


def test_parse_artifactclass_policy_rejects_bad_expect() -> None:
    text = _policy_text().replace('expect = "messy"', 'expect = "hopeful"')

    with pytest.raises(ArtifactClassPolicyError, match="expect"):
        parse_artifactclass_policy(text)


def test_parse_artifactclass_policy_rejects_duplicate_pools() -> None:
    text = _policy_text().replace("o-copy-2-pool", "o-copy-1-pool", 1)

    with pytest.raises(ArtifactClassPolicyError, match="duplicate pool"):
        parse_artifactclass_policy(text)


def test_apply_artifactclass_policy_upserts_memberships(engine: Engine) -> None:
    policy = parse_artifactclass_policy(
        _staging_policy_text() + '\n[hdcache]\nenabled = true\n' + _durability_override()
    )
    with session_scope(engine) as s:
        backend = Backend(
            name="rem",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(backend)
        s.flush()
        s.add_all(
            [
                Pool(
                    id="o-copy-1-pool",
                    backend_id=backend.id,
                    representation=Representation.RAO_PLAIN_V1.value,
                ),
                Pool(
                    id="o-copy-2-pool",
                    backend_id=backend.id,
                    representation=Representation.RAO_AEAD_V1.value,
                ),
                Pool(
                    id="stale-pool",
                    backend_id=backend.id,
                    representation=Representation.RAW_BYTES.value,
                ),
                ArtifactClassPool(
                    artifactclass="o-archive",
                    pool_id="stale-pool",
                    active=True,
                ),
            ]
        )
        s.flush()

        with pytest.warns(ArtifactClassPolicyWarning, match="AppleDouble merge"):
            apply_artifactclass_policy(s, "o-archive", policy)

        memberships = list(
            s.scalars(
                select(ArtifactClassPool)
                .where(ArtifactClassPool.artifactclass == "o-archive")
                .order_by(ArtifactClassPool.sort_order, ArtifactClassPool.pool_id)
            )
        )
        assert [(m.pool_id, m.active, m.role) for m in memberships] == [
            ("o-copy-1-pool", True, "plain"),
            ("stale-pool", False, None),
            ("o-copy-2-pool", True, "encrypted"),
        ]
        record = get_artifactclass_policy(s, "o-archive")
        assert isinstance(record, ArtifactClassPolicyRecord)
        assert record.ruleset == "rao.o.v1"
        assert record.expect == "messy"
        assert record.target_bytes == 32 * 1024**3
        assert record.max_age_seconds == 48 * 3600
        assert record.restore_preference == ["o-copy-1-pool", "o-copy-2-pool"]
        assert record.min_copies == 2
        assert record.min_impl_families == 1
        assert record.staging_config == policy.staging.to_json()
        assert record.hdcache_config == policy.hdcache.to_json()


def test_apply_artifactclass_policy_rejects_unmapped_private_hdcache_level(
    engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = parse_artifactclass_policy(_hdcache_policy_text() + _durability_override())
    monkeypatch.setenv("SUTRADHARA_HDCACHE_PRIVACY_CAPABILITIES", "{}")
    with session_scope(engine) as s:
        backend = Backend(
            name="rem",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(backend)
        s.flush()
        s.add_all(
            [
                Pool(
                    id="o-copy-1-pool",
                    backend_id=backend.id,
                    representation=Representation.RAO_PLAIN_V1.value,
                ),
                Pool(
                    id="o-copy-2-pool",
                    backend_id=backend.id,
                    representation=Representation.RAO_AEAD_V1.value,
                ),
            ]
        )

        with pytest.raises(ArtifactClassPolicyError, match="no configured restore capability"):
            apply_artifactclass_policy(s, "o-archive", policy)


def test_apply_artifactclass_policy_rejects_unknown_pool(engine: Engine) -> None:
    policy = parse_artifactclass_policy(_policy_text())
    with session_scope(engine) as s, pytest.raises(UnknownPolicyPool, match="unknown"):
        apply_artifactclass_policy(s, "o-archive", policy)


def test_apply_artifactclass_policy_rejects_unknown_restore_preference_pool(
    engine: Engine,
) -> None:
    text = _policy_text().replace(
        'preference = ["o-copy-1-pool", "o-copy-2-pool"]',
        'preference = ["o-copy-1-pool", "missing-restore-pool"]',
    )
    policy = parse_artifactclass_policy(text)
    with session_scope(engine) as s:
        backend = Backend(
            name="rem",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(backend)
        s.flush()
        s.add_all(
            [
                Pool(
                    id="o-copy-1-pool",
                    backend_id=backend.id,
                    representation=Representation.RAO_PLAIN_V1.value,
                ),
                Pool(
                    id="o-copy-2-pool",
                    backend_id=backend.id,
                    representation=Representation.RAO_AEAD_V1.value,
                ),
            ]
        )

        with pytest.raises(ArtifactClassPolicyError, match=r"restore\.preference"):
            apply_artifactclass_policy(s, "o-archive", policy)


def test_apply_artifactclass_policy_rejects_same_family_floor(engine: Engine) -> None:
    policy = parse_artifactclass_policy(
        _policy_text().replace(
            'preference = ["o-copy-1-pool", "o-copy-2-pool"]',
            'preference = ["o-copy-1-pool", "o-copy-2-pool", "o-copy-3-pool"]',
        )
        + """
[[placements]]
pool = "o-copy-3-pool"
"""
    )
    with session_scope(engine) as s:
        backend = Backend(
            name="rem",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(backend)
        s.flush()
        s.add_all(
            [
                Pool(
                    id=f"o-copy-{index}-pool",
                    backend_id=backend.id,
                    representation=Representation.RAO_PLAIN_V1.value,
                )
                for index in (1, 2, 3)
            ]
        )

        with pytest.raises(ArtifactClassPolicyError, match="min_impl_families"):
            apply_artifactclass_policy(s, "o-archive", policy)


def test_apply_artifactclass_policy_warns_for_write_fenced_restore_pool(
    engine: Engine,
) -> None:
    policy = parse_artifactclass_policy(_policy_text() + _durability_override(1, 1))
    with session_scope(engine) as s:
        backend = Backend(
            name="rem",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(backend)
        s.flush()
        s.add_all(
            [
                Pool(
                    id="o-copy-1-pool",
                    backend_id=backend.id,
                    representation=Representation.RAO_PLAIN_V1.value,
                ),
                Pool(
                    id="o-copy-2-pool",
                    backend_id=backend.id,
                    representation=Representation.RAO_AEAD_V1.value,
                    accepts_writes=False,
                ),
            ]
        )

        with pytest.warns(ArtifactClassPolicyWarning, match="write-fenced"):
            apply_artifactclass_policy(s, "o-archive", policy)
