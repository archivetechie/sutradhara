"""Bundle-group fingerprint, thresholds, naming ladder, races, include-alone.

Failure paths outweigh happy paths: every test here names the defect it
guards against (design-bundle-groups v0.8, prompt bg-p1).
"""

from __future__ import annotations

import hashlib
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, select
from sqlalchemy.exc import IntegrityError

import sutradhara.archive_bundle as archive_bundle_module
from sutradhara.archive_bundle import (
    BundleStateError,
    MemberNamingError,
    add_bundle_member,
    enqueue_artifact,
    extract_member_tag,
    get_or_create_open_bundle,
    tag_member_path,
)
from sutradhara.bundle_group import (
    BundleGroupError,
    EmptyBundleGroupError,
    canonical_basis_json,
    compute_bundle_group,
    effective_group_thresholds,
    fingerprint_basis,
)
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    ArtifactClassPool,
    Backend,
    Bundle,
    BundleMember,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier
from sutradhara.pools import set_pool_representation
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def _hash(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _add_backend_pools(s, pool_specs: list[tuple[str, str]]) -> None:
    backend = Backend(
        name="rem",
        kind=BackendKind.REM_TAPE,
        tier=BackendTier.SELF_DESCRIBING,
    )
    s.add(backend)
    s.flush()
    for pool_id, representation in pool_specs:
        s.add(Pool(id=pool_id, backend_id=backend.id, representation=representation))
    s.flush()


def _add_class(
    s,
    artifactclass: str,
    pool_ids: list[str],
    *,
    target_bytes: int = 1024,
    max_age_seconds: int = 3600,
    write_projection: bool = True,
) -> ArtifactClassPolicyRecord:
    policy = ArtifactClassPolicyRecord(
        artifactclass=artifactclass,
        ruleset="rao.v1",
        expect="messy",
        target_bytes=target_bytes,
        max_age_seconds=max_age_seconds,
        restore_preference=pool_ids or ["none"],
    )
    s.add(policy)
    for pool_id in pool_ids:
        s.add(ArtifactClassPool(artifactclass=artifactclass, pool_id=pool_id, active=True))
    s.flush()
    if write_projection:
        fingerprint, _ = compute_bundle_group(s, artifactclass)
        policy.bundle_group = fingerprint
        s.flush()
    return policy


def _add_asset(s, data: bytes) -> bytes:
    digest = _hash(data)
    if s.get(LogicalAsset, digest) is None:
        s.add(LogicalAsset(content_sha256=digest, size_bytes=len(data)))
        s.flush()
    return digest


def _source(tmp_path: Path, name: str, data: bytes) -> Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


# --- fingerprint -----------------------------------------------------------


def test_fingerprint_ignores_accepts_writes_flip(engine: Engine) -> None:
    """A maintenance fence must never re-partition groups."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        _add_class(s, "photo", ["pool-a"])
        before, _ = compute_bundle_group(s, "photo")
        pool = s.get(Pool, "pool-a")
        pool.accepts_writes = False
        s.flush()
        after, _ = compute_bundle_group(s, "photo")
        assert before == after


def test_fingerprint_changes_on_representation_change(engine: Engine) -> None:
    """Representation is a fingerprint input; a change must re-partition."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        _add_class(s, "photo", ["pool-a"])
        before, _ = compute_bundle_group(s, "photo")
        set_pool_representation(s, "pool-a", Representation.RAO_AEAD_V1)
        after, _ = compute_bundle_group(s, "photo")
        assert before != after


def test_set_pool_representation_recomputes_projection(engine: Engine) -> None:
    """The out-of-apply representation writer must maintain the projection."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"])
        stale = policy.bundle_group
        set_pool_representation(s, "pool-a", Representation.RAO_AEAD_V1)
        assert policy.bundle_group != stale
        live, _ = compute_bundle_group(s, "photo")
        assert policy.bundle_group == live


def test_fingerprint_sort_stability(engine: Engine) -> None:
    """Membership insertion order must not change identity."""
    with session_scope(engine) as s:
        _add_backend_pools(
            s, [("pool-a", "rao-plain-v1"), ("pool-b", "rao-aead-v1")]
        )
        _add_class(s, "one", ["pool-a", "pool-b"])
        _add_class(s, "two", ["pool-b", "pool-a"])
        fp_one, basis_one = compute_bundle_group(s, "one")
        fp_two, basis_two = compute_bundle_group(s, "two")
        assert fp_one == fp_two
        assert basis_one == basis_two


def _migration_module():
    """Import the bundle-groups schema migration by file path."""
    import importlib.util

    path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "b9c8d7e6f5a4_bundle_groups_schema.py"
    )
    spec = importlib.util.spec_from_file_location("bundle_groups_schema_migration", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Golden fingerprint for a fixed mixed-case ASCII basis (F9). Pinned before the
# canonical sort moved from SQL ORDER BY to Python sorted(): byte order for
# ASCII pool ids ('A' < 'Z' < 'a') under SQLite BINARY collation and Python
# codepoint sort must agree, so this constant must never change.
_F9_GOLDEN_BASIS = [
    {"pool": "A-Pool", "representation": "rao-plain-v1"},
    {"pool": "Z-pool", "representation": "d2tar-raw"},
    {"pool": "a-pool", "representation": "rao-aead-v1"},
]
_F9_GOLDEN_FINGERPRINT = "f0286b4b680bf2f8a19fb2d2c9071853486fb35bc60c43d4f70e68ca4d8a4ca1"


def test_f9_fingerprint_parity_library_vs_migration(engine: Engine) -> None:
    """F9: the canonical order must be collation-independent and identical
    across the library and the migration backfill — for ASCII pool ids the
    fingerprint must equal the pre-change pinned golden value."""
    migration = _migration_module()
    with session_scope(engine) as s:
        backend = Backend(
            name="rem", kind=BackendKind.REM_TAPE, tier=BackendTier.SELF_DESCRIBING
        )
        s.add(backend)
        s.flush()
        # Mixed-case ids seeded in non-canonical insertion order: exposes any
        # collation- or insertion-order-dependent sort.
        s.add(Pool(id="a-pool", backend_id=backend.id, representation="rao-aead-v1"))
        s.add(Pool(id="Z-pool", backend_id=backend.id, representation="d2tar-raw"))
        s.add(Pool(id="A-Pool", backend_id=backend.id, representation="rao-plain-v1"))
        s.flush()
        _add_class(s, "mixed-case", ["a-pool", "Z-pool", "A-Pool"])
        fingerprint, basis = compute_bundle_group(s, "mixed-case")

        assert basis == _F9_GOLDEN_BASIS
        assert fingerprint == _F9_GOLDEN_FINGERPRINT

        migration_bases = migration._class_bases(s.connection())
        assert migration_bases["mixed-case"] == _F9_GOLDEN_BASIS
        assert migration._fingerprint(migration_bases["mixed-case"]) == fingerprint


def test_inactive_membership_leaves_fingerprint(engine: Engine) -> None:
    """Only active memberships are identity."""
    with session_scope(engine) as s:
        _add_backend_pools(
            s, [("pool-a", "rao-plain-v1"), ("pool-b", "rao-aead-v1")]
        )
        _add_class(s, "photo", ["pool-a"])
        before, _ = compute_bundle_group(s, "photo")
        s.add(
            ArtifactClassPool(
                artifactclass="photo", pool_id="pool-b", active=False
            )
        )
        s.flush()
        after, _ = compute_bundle_group(s, "photo")
        assert before == after


def test_null_values_canonicalise_as_absent_keys() -> None:
    """A NULL never enters the canonical serialization as an explicit null."""
    with_null = [{"pool": "p1", "representation": None}]
    without_key = [{"pool": "p1"}]
    assert canonical_basis_json(with_null) == canonical_basis_json(without_key)
    assert fingerprint_basis(with_null) == fingerprint_basis(without_key)
    assert "null" not in canonical_basis_json(with_null)


# --- thresholds ------------------------------------------------------------


def test_thresholds_min_age_max_target_over_declared_set(engine: Engine) -> None:
    """min over ages (a promise), max over targets (a goal) — never min-of-targets."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        _add_class(s, "audio", ["pool-a"], target_bytes=1 * 1024, max_age_seconds=7200)
        _add_class(s, "photo", ["pool-a"], target_bytes=20 * 1024, max_age_seconds=3600)
        policy = _add_class(
            s, "video", ["pool-a"], target_bytes=5 * 1024, max_age_seconds=86400
        )
        fingerprint, basis = compute_bundle_group(s, "video")
        target, age = effective_group_thresholds(
            s,
            artifactclass="video",
            policy=policy,
            fingerprint=fingerprint,
            basis=basis,
        )
        assert target == 20 * 1024
        assert age == 3600


def test_threshold_clamp_activates_on_declared_floor(engine: Engine) -> None:
    """The strictest member pool's declared floor clamps the target up."""
    with session_scope(engine) as s:
        _add_backend_pools(
            s, [("pool-a", "rao-plain-v1"), ("pool-b", "rao-aead-v1")]
        )
        s.get(Pool, "pool-a").min_object_bytes = 50 * 1024
        s.get(Pool, "pool-b").min_object_bytes = 80 * 1024
        s.flush()
        policy = _add_class(s, "photo", ["pool-a", "pool-b"], target_bytes=1024)
        fingerprint, basis = compute_bundle_group(s, "photo")
        target, _ = effective_group_thresholds(
            s,
            artifactclass="photo",
            policy=policy,
            fingerprint=fingerprint,
            basis=basis,
        )
        assert target == 80 * 1024


def test_threshold_clamp_inactive_without_declared_floor(engine: Engine) -> None:
    """NULL min_object_bytes = no floor declared — never an implicit zero."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"], target_bytes=1024)
        fingerprint, basis = compute_bundle_group(s, "photo")
        target, _ = effective_group_thresholds(
            s,
            artifactclass="photo",
            policy=policy,
            fingerprint=fingerprint,
            basis=basis,
        )
        assert target == 1024


def test_empty_declared_set_errors_at_open(engine: Engine) -> None:
    """A class with no active placements never opens a never-sealing accumulator."""
    with session_scope(engine) as s:
        policy = _add_class(s, "orphan", [])
        with pytest.raises(EmptyBundleGroupError):
            get_or_create_open_bundle(s, artifactclass="orphan", policy=policy)


def test_zero_thresholds_never_written(engine: Engine) -> None:
    """A defective policy row (zero threshold) must fail loudly at open."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"], target_bytes=1024)
        policy.max_age_seconds = 0
        s.flush()
        with pytest.raises(BundleGroupError):
            get_or_create_open_bundle(s, artifactclass="photo", policy=policy)


def test_opener_union_covers_stale_projection(engine: Engine) -> None:
    """The declared-set query includes the opener even when its stored
    projection points at an old fingerprint (a missed writer)."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        _add_class(s, "photo", ["pool-a"], target_bytes=10 * 1024, max_age_seconds=3600)
        opener = _add_class(
            s,
            "video",
            ["pool-a"],
            target_bytes=99 * 1024,
            max_age_seconds=60,
            write_projection=False,
        )
        opener.bundle_group = "stale-fingerprint-from-an-old-pool-set"
        s.flush()
        fingerprint, basis = compute_bundle_group(s, "video")
        target, age = effective_group_thresholds(
            s,
            artifactclass="video",
            policy=opener,
            fingerprint=fingerprint,
            basis=basis,
        )
        # The opener's own values participate despite the stale projection.
        assert target == 99 * 1024
        assert age == 60


def test_open_freezes_thresholds_and_witness(engine: Engine) -> None:
    """Thresholds are frozen on the open bundle and equal the witness."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"], target_bytes=4096, max_age_seconds=600)
        bundle, created = get_or_create_open_bundle(s, artifactclass="photo", policy=policy)
        assert created
        assert bundle.group_basis["basis_source"] == "derived"
        assert bundle.group_basis["effective"] == {
            "target_bytes": 4096,
            "max_age_seconds": 600,
        }
        # A stricter class joining the group is honoured from the NEXT bundle.
        _add_class(s, "audio", ["pool-a"], target_bytes=8192, max_age_seconds=60)
        again, created_again = get_or_create_open_bundle(
            s, artifactclass="photo", policy=policy
        )
        assert not created_again
        assert again.id == bundle.id
        assert again.target_bytes == 4096
        assert again.max_age_seconds == 600


# --- accumulator keying and races ------------------------------------------


def test_same_pool_set_classes_share_accumulator(engine: Engine) -> None:
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        photo = _add_class(s, "photo", ["pool-a"])
        audio = _add_class(s, "audio", ["pool-a"])
        b1, _ = get_or_create_open_bundle(s, artifactclass="photo", policy=photo)
        b2, created = get_or_create_open_bundle(s, artifactclass="audio", policy=audio)
        assert b1.id == b2.id
        assert not created


def test_different_pool_sets_get_own_accumulators(engine: Engine) -> None:
    """Confidentiality needs no mechanism: a different pool set = own crate."""
    with session_scope(engine) as s:
        _add_backend_pools(
            s, [("pool-a", "rao-plain-v1"), ("pool-priv", "rao-aead-v1")]
        )
        public = _add_class(s, "public", ["pool-a"])
        confidential = _add_class(s, "confidential", ["pool-priv"])
        b1, _ = get_or_create_open_bundle(s, artifactclass="public", policy=public)
        b2, _ = get_or_create_open_bundle(
            s, artifactclass="confidential", policy=confidential
        )
        assert b1.id != b2.id
        assert b1.bundle_group != b2.bundle_group


def test_accumulator_race_loser_adopts_winner(
    engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Forced IntegrityError on the accumulator index: the loser adopts and
    a raw IntegrityError never escapes."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"])
        winner, _ = get_or_create_open_bundle(s, artifactclass="photo", policy=policy)
        s.flush()

        real_find = archive_bundle_module._find_open_accumulator
        calls = {"n": 0}

        def racing_find(session, fingerprint):
            calls["n"] += 1
            if calls["n"] == 1:
                return None  # stale read: the check misses the winner
            return real_find(session, fingerprint)

        monkeypatch.setattr(
            archive_bundle_module, "_find_open_accumulator", racing_find
        )
        adopted, created = get_or_create_open_bundle(
            s, artifactclass="photo", policy=policy
        )
        assert not created
        assert adopted.id == winner.id
        assert calls["n"] == 2


def test_accumulator_race_translates_foreign_integrity_error(
    engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An IntegrityError with no adoptable winner surfaces as a domain error,
    never raw."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"])
        _winner, _ = get_or_create_open_bundle(s, artifactclass="photo", policy=policy)
        s.flush()
        monkeypatch.setattr(
            archive_bundle_module, "_find_open_accumulator", lambda *a, **k: None
        )
        with pytest.raises(BundleStateError):
            get_or_create_open_bundle(s, artifactclass="photo", policy=policy)


def test_member_race_loser_reruns_ladder(
    engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Forced IntegrityError on (bundle_id, member_path): the loser re-runs
    the ladder against the now-visible winner and lands on a tagged name."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"])
        bundle, _ = get_or_create_open_bundle(s, artifactclass="photo", policy=policy)
        winner_hash = _add_asset(s, b"winner")
        loser_hash = _add_asset(s, b"loser")
        add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo",
            logical_asset_hash=winner_hash,
            member_path="IMG_0001.JPG",
            size_bytes=6,
            file_sha256=winner_hash,
        )

        real_resolve = archive_bundle_module._resolve_member_name
        calls = {"n": 0}

        def racing_resolve(session, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                # Stale read: the ladder misses the winner and picks the
                # requested name, so the insert hits the unique surface.
                return kwargs["requested"], None, None
            return real_resolve(session, **kwargs)

        monkeypatch.setattr(
            archive_bundle_module, "_resolve_member_name", racing_resolve
        )
        member, created = add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo",
            logical_asset_hash=loser_hash,
            member_path="IMG_0001.JPG",
            size_bytes=5,
            file_sha256=loser_hash,
        )
        assert created
        assert member.member_path != "IMG_0001.JPG"
        assert member.member_path.startswith("IMG_0001.")
        assert calls["n"] == 2


def test_atomic_counters_accumulate(engine: Engine) -> None:
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"])
        bundle, _ = get_or_create_open_bundle(s, artifactclass="photo", policy=policy)
        for index in range(3):
            digest = _add_asset(s, f"data-{index}".encode())
            add_bundle_member(
                s,
                bundle=bundle,
                artifactclass="photo",
                logical_asset_hash=digest,
                member_path=f"file-{index}.bin",
                size_bytes=6,
                file_sha256=digest,
            )
        assert bundle.member_count == 3
        assert bundle.total_bytes == 18


# --- naming ladder ---------------------------------------------------------


def test_cross_class_same_path_disambiguates_with_own_hash(engine: Engine) -> None:
    """Two cameras' IMG_0001.JPG across classes is routine intake, not an
    outage: the later member gets its own content-hash tag."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        photo = _add_class(s, "photo", ["pool-a"])
        _add_class(s, "audio", ["pool-a"])
        bundle, _ = get_or_create_open_bundle(s, artifactclass="photo", policy=photo)
        first = _add_asset(s, b"camera-one")
        second = _add_asset(s, b"camera-two")
        m1, _ = add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo",
            logical_asset_hash=first,
            member_path="IMG_0001.JPG",
            size_bytes=10,
            file_sha256=first,
        )
        m2, created = add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="audio",
            logical_asset_hash=second,
            member_path="IMG_0001.JPG",
            size_bytes=10,
            file_sha256=second,
        )
        assert created
        assert m1.member_path == "IMG_0001.JPG"
        assert m2.member_path == f"IMG_0001.{second.hex()[:10]}.JPG"


def test_same_class_same_path_different_hash_disambiguates(engine: Engine) -> None:
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"])
        bundle, _ = get_or_create_open_bundle(s, artifactclass="photo", policy=policy)
        first = _add_asset(s, b"one")
        second = _add_asset(s, b"two")
        add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo",
            logical_asset_hash=first,
            member_path="IMG_0001.JPG",
            size_bytes=3,
            file_sha256=first,
        )
        member, created = add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo",
            logical_asset_hash=second,
            member_path="IMG_0001.JPG",
            size_bytes=3,
            file_sha256=second,
        )
        assert created
        assert member.member_path == f"IMG_0001.{second.hex()[:10]}.JPG"


def test_idempotent_noop_same_class_same_hash(engine: Engine) -> None:
    """The no-op requires class AND hash agreement — never path alone."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"])
        bundle, _ = get_or_create_open_bundle(s, artifactclass="photo", policy=policy)
        digest = _add_asset(s, b"same")
        first, created_first = add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo",
            logical_asset_hash=digest,
            member_path="a.bin",
            size_bytes=4,
            file_sha256=digest,
        )
        second, created_second = add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo",
            logical_asset_hash=digest,
            member_path="a.bin",
            size_bytes=4,
            file_sha256=digest,
        )
        assert created_first
        assert not created_second
        assert first.id == second.id
        assert bundle.member_count == 1


def test_collision_at_tagged_name_climbs_ladder(engine: Engine) -> None:
    """A literal name occupying a tag-syntax rung is just a name; the ladder
    climbs past it, re-checking idempotency per rung."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"])
        bundle, _ = get_or_create_open_bundle(s, artifactclass="photo", policy=policy)
        blocker = _add_asset(s, b"blocker")
        newcomer = _add_asset(s, b"newcomer")
        squatter = _add_asset(s, b"squatter")
        add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo",
            logical_asset_hash=blocker,
            member_path="IMG_0001.JPG",
            size_bytes=7,
            file_sha256=blocker,
        )
        # A literal member whose name equals the newcomer's rung-1 candidate.
        rung_one = f"IMG_0001.{newcomer.hex()[:10]}.JPG"
        add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo",
            logical_asset_hash=squatter,
            member_path=rung_one,
            size_bytes=8,
            file_sha256=squatter,
        )
        member, created = add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo",
            logical_asset_hash=newcomer,
            member_path="IMG_0001.JPG",
            size_bytes=8,
            file_sha256=newcomer,
        )
        assert created
        assert member.member_path == f"IMG_0001.{newcomer.hex()[:20]}.JPG"


def test_crash_retry_lands_on_own_row_at_tagged_rung(engine: Engine) -> None:
    """Re-running the exact insert after a crash must land on the previously
    tagged row, not mint a second name."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"])
        bundle, _ = get_or_create_open_bundle(s, artifactclass="photo", policy=policy)
        first = _add_asset(s, b"first")
        second = _add_asset(s, b"second")
        add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo",
            logical_asset_hash=first,
            member_path="IMG_0001.JPG",
            size_bytes=5,
            file_sha256=first,
        )
        tagged, created = add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo",
            logical_asset_hash=second,
            member_path="IMG_0001.JPG",
            size_bytes=6,
            file_sha256=second,
        )
        assert created
        retried, retried_created = add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo",
            logical_asset_hash=second,
            member_path="IMG_0001.JPG",
            size_bytes=6,
            file_sha256=second,
        )
        assert not retried_created
        assert retried.id == tagged.id
        assert bundle.member_count == 2


def test_arrival_order_independence_both_orders(engine: Engine) -> None:
    """The tag derives from the member's own content hash, never arrival
    order: whichever member arrives second carries its OWN hash prefix."""
    data_a, data_b = b"content-a", b"content-b"
    for order in ((data_a, data_b), (data_b, data_a)):
        eng = make_engine("sqlite:///:memory:")
        create_all(eng)
        with session_scope(eng) as s:
            _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
            policy = _add_class(s, "photo", ["pool-a"])
            bundle, _ = get_or_create_open_bundle(
                s, artifactclass="photo", policy=policy
            )
            for data in order:
                digest = _add_asset(s, data)
                add_bundle_member(
                    s,
                    bundle=bundle,
                    artifactclass="photo",
                    logical_asset_hash=digest,
                    member_path="IMG_0001.JPG",
                    size_bytes=len(data),
                    file_sha256=digest,
                )
            second_digest = _hash(order[1])
            tagged = s.scalars(
                select(BundleMember).where(
                    BundleMember.member_path != "IMG_0001.JPG"
                )
            ).one()
            assert tagged.member_path == f"IMG_0001.{second_digest.hex()[:10]}.JPG"
            assert tagged.logical_asset_hash == second_digest
        eng.dispose()


def test_ladder_exhaustion_is_a_hash_collision_assert(engine: Engine) -> None:
    """Same-key different-hash within a class is impossible by construction
    after the ladder; exhausting it must raise the assert, not insert."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"])
        bundle, _ = get_or_create_open_bundle(s, artifactclass="photo", policy=policy)
        newcomer = _add_asset(s, b"newcomer")
        # Occupy every rung of the newcomer's ladder with foreign rows.
        ladder = archive_bundle_module._name_ladder(
            "IMG_0001.JPG", newcomer, "photo"
        )
        for index, candidate in enumerate(ladder):
            squatter = _add_asset(s, f"squatter-{index}".encode())
            s.add(
                BundleMember(
                    bundle_id=bundle.id,
                    logical_asset_hash=squatter,
                    artifactclass="photo",
                    member_path=candidate,
                    size_bytes=1,
                    file_sha256=squatter,
                )
            )
        s.flush()
        with pytest.raises(MemberNamingError):
            add_bundle_member(
                s,
                bundle=bundle,
                artifactclass="photo",
                logical_asset_hash=newcomer,
                member_path="IMG_0001.JPG",
                size_bytes=8,
                file_sha256=newcomer,
            )


def test_f5_slug_collision_classes_get_distinct_terminal_rungs(engine: Engine) -> None:
    """F5: ``photo.raw`` and ``photo-raw`` share a slug; the terminal rung is
    seeded with a hash of the raw class name, so the same content under the
    same requested name in both classes lands on two distinct rows instead of
    exhausting the ladder."""
    assert archive_bundle_module._class_slug("photo.raw") == archive_bundle_module._class_slug(
        "photo-raw"
    )
    content = b"same bytes in both classes"
    digest = _hash(content)
    ladder_a = archive_bundle_module._name_ladder("IMG_0001.JPG", digest, "photo.raw")
    ladder_b = archive_bundle_module._name_ladder("IMG_0001.JPG", digest, "photo-raw")
    # Same content, same name: every hash-prefix rung collides across the two
    # classes — only the seeded terminal rung separates them.
    assert ladder_a[:-1] == ladder_b[:-1]
    assert ladder_a[-1] != ladder_b[-1]

    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy_a = _add_class(s, "photo.raw", ["pool-a"])
        _add_class(s, "photo-raw", ["pool-a"])
        asset = _add_asset(s, content)
        bundle, _ = get_or_create_open_bundle(s, artifactclass="photo.raw", policy=policy_a)
        member_a, created_a = add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo.raw",
            logical_asset_hash=asset,
            member_path="IMG_0001.JPG",
            size_bytes=len(content),
            file_sha256=asset,
        )
        # Occupy every shared rung so the second class is forced to the
        # terminal rung — the pre-F5 defect point.
        for index, candidate in enumerate(ladder_a[1:-1]):
            squatter = _add_asset(s, f"f5-squatter-{index}".encode())
            s.add(
                BundleMember(
                    bundle_id=bundle.id,
                    logical_asset_hash=squatter,
                    artifactclass="photo.raw",
                    member_path=candidate,
                    size_bytes=1,
                    file_sha256=squatter,
                )
            )
        s.flush()
        member_b, created_b = add_bundle_member(
            s,
            bundle=bundle,
            artifactclass="photo-raw",
            logical_asset_hash=asset,
            member_path="IMG_0001.JPG",
            size_bytes=len(content),
            file_sha256=asset,
        )
        assert created_a
        assert created_b
        assert member_a.id != member_b.id
        assert member_b.member_path == ladder_b[-1]
        assert member_a.member_path != member_b.member_path


def test_tag_helpers_roundtrip() -> None:
    tagged = tag_member_path("dir/name.ext.zst", "abc123")
    assert tagged == "dir/name.abc123.ext.zst"
    assert extract_member_tag("dir/name.ext.zst", tagged) == "abc123"
    assert extract_member_tag("a.bin", "a.bin") is None
    # Tagging commutes with suffix appends (the staging chain linkage).
    assert tag_member_path("name.ext", "t") + ".zst" == tag_member_path(
        "name.ext.zst", "t"
    )
    assert tag_member_path("noext", "t") == "noext.t"
    assert extract_member_tag("noext", "noext.t") == "t"


# --- include-alone ---------------------------------------------------------


def test_include_alone_routes_oversized_member_to_funnel(
    engine: Engine, tmp_path: Path
) -> None:
    """Oversized member with a non-empty accumulator: fresh non-adoptable
    bundle, accumulator untouched, no partial-index violation."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"], target_bytes=100)
        small = _source(tmp_path, "small.bin", b"x" * 10)
        big = _source(tmp_path, "big.bin", b"y" * 200)
        small_hash = _add_asset(s, b"x" * 10)
        big_hash = _add_asset(s, b"y" * 200)

        accumulator, _, _ = enqueue_artifact(
            s,
            artifactclass="photo",
            policy=policy,
            logical_asset_hash=small_hash,
            source_path=small,
        )
        funnel, _member, created = enqueue_artifact(
            s,
            artifactclass="photo",
            policy=policy,
            logical_asset_hash=big_hash,
            source_path=big,
        )
        assert created
        assert funnel.id != accumulator.id
        assert funnel.archive_id is not None  # non-adoptable by construction
        assert funnel.bundle_group == accumulator.bundle_group
        assert accumulator.member_count == 1
        assert accumulator.total_bytes == 10
        # Immediately due for flush; the accumulator is not.
        from sutradhara.archive_bundle import bundle_due

        assert bundle_due(funnel)
        assert not bundle_due(accumulator)
        # A follow-up small member still adopts the untouched accumulator.
        third_hash = _add_asset(s, b"z" * 5)
        third = _source(tmp_path, "third.bin", b"z" * 5)
        again, _, _ = enqueue_artifact(
            s,
            artifactclass="photo",
            policy=policy,
            logical_asset_hash=third_hash,
            source_path=third,
        )
        assert again.id == accumulator.id


def test_include_alone_retry_lands_on_open_funnel(
    engine: Engine, tmp_path: Path
) -> None:
    """A crash-retry of the same oversized enqueue must not mint a second
    funnel while the first is still open."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"], target_bytes=100)
        big = _source(tmp_path, "big.bin", b"y" * 200)
        big_hash = _add_asset(s, b"y" * 200)
        funnel, _, created = enqueue_artifact(
            s,
            artifactclass="photo",
            policy=policy,
            logical_asset_hash=big_hash,
            source_path=big,
        )
        assert created
        again, _member, created_again = enqueue_artifact(
            s,
            artifactclass="photo",
            policy=policy,
            logical_asset_hash=big_hash,
            source_path=big,
        )
        assert not created_again
        assert again.id == funnel.id


def test_f8_include_alone_mint_savepoint_guard(engine: Engine, tmp_path: Path) -> None:
    """F8: an IntegrityError on the funnel mint (explicit bundle id colliding
    with a crash-orphaned funnel that has no member row yet) must resolve
    through the savepoint guard — adopt the open funnel, add the member, and
    never let a raw IntegrityError escape."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"], target_bytes=100)
        big = _source(tmp_path, "big.bin", b"y" * 200)
        big_hash = _add_asset(s, b"y" * 200)
        # A crash between funnel insert and member insert: the funnel row
        # exists, the member row does not — so the pre-mint idempotency
        # select misses and the mint collides on the bundle id.
        _fingerprint, basis = compute_bundle_group(s, "photo")
        from tests.bundle_group_helpers import bundle_kwargs

        s.add(
            Bundle(
                id="funnel-fixed",
                status="open",
                archive_id="archive-funnel-fixed",
                **bundle_kwargs(basis=basis, target_bytes=100, max_age_seconds=3600),
            )
        )
        s.flush()
        bundle, member, created = enqueue_artifact(
            s,
            artifactclass="photo",
            policy=policy,
            logical_asset_hash=big_hash,
            source_path=big,
            bundle_id="funnel-fixed",
        )
        assert created
        assert bundle.id == "funnel-fixed"
        assert member.logical_asset_hash == big_hash
        assert bundle.member_count == 1


def test_f8_include_alone_mint_foreign_collision_is_domain_error(
    engine: Engine, tmp_path: Path
) -> None:
    """F8: a mint collision that is NOT an adoptable open funnel (the id is
    occupied by a sealed bundle) translates to the retryable domain error,
    never a raw IntegrityError."""
    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"], target_bytes=100)
        big = _source(tmp_path, "big.bin", b"y" * 200)
        big_hash = _add_asset(s, b"y" * 200)
        from tests.bundle_group_helpers import bundle_kwargs

        s.add(
            Bundle(
                id="occupied",
                status="sealed",
                **bundle_kwargs(seed="foreign", target_bytes=1, max_age_seconds=1),
            )
        )
        s.flush()
        with pytest.raises(BundleStateError):
            enqueue_artifact(
                s,
                artifactclass="photo",
                policy=policy,
                logical_asset_hash=big_hash,
                source_path=big,
                bundle_id="occupied",
            )


# --- partial unique index ---------------------------------------------------


def test_partial_index_blocks_second_open_accumulator(engine: Engine) -> None:
    """The one-open-accumulator invariant is a database fact, not app logic."""
    from tests.bundle_group_helpers import bundle_kwargs

    with session_scope(engine) as s:
        fields = bundle_kwargs(seed="pool-a", target_bytes=1, max_age_seconds=1)
        s.add(Bundle(id="open-1", status="open", **fields))
        s.flush()
        s.add(Bundle(id="open-2", status="open", **fields))
        with pytest.raises(IntegrityError):
            s.flush()
        s.rollback()


def test_partial_index_ignores_funnels_and_sealed(engine: Engine) -> None:
    from tests.bundle_group_helpers import bundle_kwargs

    with session_scope(engine) as s:
        fields = bundle_kwargs(seed="pool-a", target_bytes=1, max_age_seconds=1)
        s.add(Bundle(id="open-1", status="open", **fields))
        s.add(Bundle(id="funnel", status="open", archive_id="archive-funnel", **fields))
        s.add(Bundle(id="sealed", status="sealed", **fields))
        s.flush()


# --- staging re-key ---------------------------------------------------------


def test_staging_rekey_tags_transformed_member_chain(
    engine: Engine, tmp_path: Path
) -> None:
    """A zstd-transformed member that collides gets tagged; the final
    transform equality and the (bundle_id, stored_member_path, step_order)
    uniqueness both hold, and the restore join still resolves the recorded
    name."""
    from sutradhara.archive_restore import resolve_member_asset_hash
    from sutradhara.catalog.models import StagingTransform
    from sutradhara.staging import stage_and_enqueue_artifact

    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"], target_bytes=1 << 30)
        policy.staging_config = {
            "appledouble": {
                "action": "off",
                "tool": "sutradhara-parser",
                "on_error": "hold",
                "record": True,
            },
            "compression": {"codec": "zstd", "level": 3, "globs": []},
        }
        s.flush()

        first_dir = tmp_path / "card-one"
        second_dir = tmp_path / "card-two"
        first = _source(first_dir, "IMG_0001.JPG", b"camera-one-bytes" * 10)
        second = _source(second_dir, "IMG_0001.JPG", b"camera-two-bytes" * 10)

        staged_first = stage_and_enqueue_artifact(
            s,
            artifactclass="photo",
            policy=policy,
            source_path=first,
            staging_root=tmp_path / "stage-one",
        )
        staged_second = stage_and_enqueue_artifact(
            s,
            artifactclass="photo",
            policy=policy,
            source_path=second,
            staging_root=tmp_path / "stage-two",
        )

        assert staged_first.stored_member_path == "IMG_0001.JPG.zst"
        # The second member's recorded name carries its own hash tag,
        # inserted at the first-dot stem so it commutes with the .zst suffix.
        second_member = s.scalars(
            select(BundleMember).where(
                BundleMember.logical_asset_hash == staged_second.logical_sha256
            )
        ).one()
        assert second_member.member_path != "IMG_0001.JPG.zst"
        assert second_member.member_path.startswith("IMG_0001.")
        assert second_member.member_path.endswith(".JPG.zst")
        assert staged_second.stored_member_path == second_member.member_path
        assert (second_member.source_metadata or {})["stored_member_path"] == (
            second_member.member_path
        )
        # Untagged logical receipt name is shared; stored names distinguish.
        assert (second_member.source_metadata or {})["logical_path"] == "IMG_0001.JPG"

        transforms = list(
            s.scalars(
                select(StagingTransform).order_by(
                    StagingTransform.bundle_member_id, StagingTransform.step_order
                )
            )
        )
        # Final-step equality holds for both chains under tagging.
        for transform in transforms:
            member = s.get(BundleMember, transform.bundle_member_id)
            assert transform.stored_member_path == member.member_path
            assert transform.stored_sha256 == member.file_sha256
        # The (bundle_id, stored_member_path, step_order) surface is distinct.
        keys = {
            (t.bundle_id, t.stored_member_path, t.step_order) for t in transforms
        }
        assert len(keys) == len(transforms)

        # Restore join resolves each recorded (tagged) name to its own asset.
        resolved = resolve_member_asset_hash(
            s,
            artifactclass="photo",
            member_name=second_member.member_path,
        )
        assert resolved == staged_second.logical_sha256


def test_staging_rekey_crash_retry_is_idempotent(
    engine: Engine, tmp_path: Path
) -> None:
    """Re-running the tagged member's stage-and-enqueue lands on its own row
    and does not duplicate transform records."""
    from sutradhara.catalog.models import StagingTransform
    from sutradhara.staging import stage_and_enqueue_artifact

    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _add_class(s, "photo", ["pool-a"], target_bytes=1 << 30)
        policy.staging_config = {
            "appledouble": {
                "action": "off",
                "tool": "sutradhara-parser",
                "on_error": "hold",
                "record": True,
            },
            "compression": {"codec": "zstd", "level": 3, "globs": []},
        }
        s.flush()
        first = _source(tmp_path / "one", "IMG_0001.JPG", b"one" * 20)
        second = _source(tmp_path / "two", "IMG_0001.JPG", b"two" * 20)
        stage_and_enqueue_artifact(
            s,
            artifactclass="photo",
            policy=policy,
            source_path=first,
            staging_root=tmp_path / "stage-one",
        )
        staged = stage_and_enqueue_artifact(
            s,
            artifactclass="photo",
            policy=policy,
            source_path=second,
            staging_root=tmp_path / "stage-two",
        )
        retried = stage_and_enqueue_artifact(
            s,
            artifactclass="photo",
            policy=policy,
            source_path=second,
            staging_root=tmp_path / "stage-two",
        )
        assert retried.stored_member_path == staged.stored_member_path
        members = list(s.scalars(select(BundleMember)))
        assert len(members) == 2
        transforms = list(s.scalars(select(StagingTransform)))
        assert len(transforms) == 2  # one zstd step per member, no duplicates


# --- restore by the name the receipt prints ---------------------------------


def _staging_class(
    s,
    artifactclass: str,
    pool_ids: list[str],
    *,
    appledouble: str = "off",
    compression: bool = True,
) -> ArtifactClassPolicyRecord:
    """A class in the shared group, with a chosen staging chain length."""
    policy = _add_class(s, artifactclass, pool_ids, target_bytes=1 << 30)
    policy.staging_config = {
        "appledouble": {
            "action": appledouble,
            "tool": "sutradhara-parser",
            "on_error": "hold",
            "record": True,
        },
        "compression": (
            {"codec": "zstd", "level": 3, "globs": []} if compression else {"codec": "off"}
        ),
    }
    s.flush()
    return policy


def _receipt_member_name(member: BundleMember) -> str:
    """The `member_name` the customer manifest prints for this member.

    Mirrors ``archive_fanout._customer_manifest_members`` — the receipt reads
    the logical name and falls back to the stored name when staging recorded
    none.
    """
    logical = (member.source_metadata or {}).get("logical_path")
    return logical if isinstance(logical, str) and logical else member.member_path


def test_tagged_transformed_member_restores_by_its_receipt_name(
    engine: Engine, tmp_path: Path
) -> None:
    """A tagged member's receipt name resolves, and the chain stops lying.

    Guards the iron finding: two classes in one group both zstd-staging
    ``images/disk.img``. The second is recorded as
    ``images/disk.<tag>.img.zst``, and the re-key used to rewrite the chain's
    ``original_member_path`` onto the tagged name too — so
    ``resolve_member_asset_hash`` raised ``RestoreNameError`` for the very
    name the second member's own customer manifest prints, while the
    co-resident first member resolved fine. Two receipts, one ``member_name``,
    only one of them restorable.
    """
    from sutradhara.archive_restore import resolve_member_asset_hash
    from sutradhara.catalog.models import StagingTransform
    from sutradhara.staging import stage_and_enqueue_artifact

    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        first_policy = _staging_class(s, "disk.one", ["pool-a"])
        second_policy = _staging_class(s, "disk.two", ["pool-a"])

        first = _source(tmp_path / "one" / "images", "disk.img", b"one-bytes" * 20)
        second = _source(tmp_path / "two" / "images", "disk.img", b"two-bytes" * 20)
        staged_first = stage_and_enqueue_artifact(
            s,
            artifactclass="disk.one",
            policy=first_policy,
            source_path=first,
            staging_root=tmp_path / "stage-one",
            member_path="images/disk.img",
        )
        staged_second = stage_and_enqueue_artifact(
            s,
            artifactclass="disk.two",
            policy=second_policy,
            source_path=second,
            staging_root=tmp_path / "stage-two",
            member_path="images/disk.img",
        )
        # Both landed in the one group accumulator, and the ladder tagged the
        # second — the collision this whole case needs.
        assert staged_first.bundle_id == staged_second.bundle_id
        assert staged_first.stored_member_path == "images/disk.img.zst"
        assert staged_second.stored_member_path != "images/disk.img.zst"

        members = {
            member.artifactclass: member
            for member in s.scalars(select(BundleMember))
        }
        # Both receipts print the same logical name (design §5), so both must
        # restore by it.
        assert _receipt_member_name(members["disk.one"]) == "images/disk.img"
        assert _receipt_member_name(members["disk.two"]) == "images/disk.img"
        for artifactclass, staged in (
            ("disk.one", staged_first),
            ("disk.two", staged_second),
        ):
            assert (
                resolve_member_asset_hash(
                    s,
                    artifactclass=artifactclass,
                    member_name=_receipt_member_name(members[artifactclass]),
                )
                == staged.logical_sha256
            )

        # The chain no longer records a pre-transform name no file ever had:
        # step 0's original is the source's own logical name, while every
        # stored name carries the tag.
        tagged = s.scalars(
            select(StagingTransform).where(StagingTransform.artifactclass == "disk.two")
        ).one()
        assert tagged.step_order == 0
        assert tagged.original_member_path == "images/disk.img"
        assert tagged.stored_member_path == members["disk.two"].member_path


def test_tagged_untransformed_member_restores_by_its_receipt_name(
    engine: Engine, tmp_path: Path
) -> None:
    """The same defect with no transform chain to fall back on.

    A class that stages nothing (compression off, AppleDouble off) records no
    ``staging_transform`` rows at all, so a member the ladder tagged had *no*
    catalog row holding its logical name that ``resolve_member_asset_hash``
    read — the receipt printed ``images/disk.img`` and the restore path
    refused it. Untagging the transform chain cannot reach this case; only
    resolving through the logical name the receipt itself reads does.
    """
    from sutradhara.archive_restore import resolve_member_asset_hash
    from sutradhara.catalog.models import StagingTransform
    from sutradhara.staging import stage_and_enqueue_artifact

    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        first_policy = _staging_class(s, "disk.one", ["pool-a"], compression=False)
        second_policy = _staging_class(s, "disk.two", ["pool-a"], compression=False)

        first = _source(tmp_path / "one" / "images", "disk.img", b"one-bytes" * 20)
        second = _source(tmp_path / "two" / "images", "disk.img", b"two-bytes" * 20)
        stage_and_enqueue_artifact(
            s,
            artifactclass="disk.one",
            policy=first_policy,
            source_path=first,
            staging_root=tmp_path / "stage-one",
            member_path="images/disk.img",
        )
        staged_second = stage_and_enqueue_artifact(
            s,
            artifactclass="disk.two",
            policy=second_policy,
            source_path=second,
            staging_root=tmp_path / "stage-two",
            member_path="images/disk.img",
        )

        assert list(s.scalars(select(StagingTransform))) == []
        second_member = s.scalars(
            select(BundleMember).where(BundleMember.artifactclass == "disk.two")
        ).one()
        assert second_member.member_path != "images/disk.img"
        assert _receipt_member_name(second_member) == "images/disk.img"
        assert (
            resolve_member_asset_hash(
                s, artifactclass="disk.two", member_name="images/disk.img"
            )
            == staged_second.logical_sha256
        )


def test_rekey_tags_every_intermediate_stored_path(
    engine: Engine, tmp_path: Path
) -> None:
    """Tagging only the *final* stored name breaks a two-step chain.

    Guards the alternative fix for the finding above. Two classes staging the
    same logical path through AppleDouble-merge **then** zstd each record two
    transform rows; their ``step_order = 0`` rows both store the untagged
    intermediate name, which collides on
    ``uq_staging_transform_bundle_stored_step`` and lets a raw
    ``IntegrityError`` escape ``record_staging_transform`` — the one thing §3
    says never happens. Every stored name carries the tag; only step 0's
    ``original_member_path``, which is the source's own logical name, does not.
    """
    from sutradhara.catalog.models import StagingTransform
    from sutradhara.staging import stage_and_enqueue_artifact

    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        first_policy = _staging_class(
            s, "disk.one", ["pool-a"], appledouble="merge-to-xattrs"
        )
        second_policy = _staging_class(
            s, "disk.two", ["pool-a"], appledouble="merge-to-xattrs"
        )
        first = _source(tmp_path / "one" / "images", "disk.img", b"one-bytes" * 20)
        second = _source(tmp_path / "two" / "images", "disk.img", b"two-bytes" * 20)
        stage_and_enqueue_artifact(
            s,
            artifactclass="disk.one",
            policy=first_policy,
            source_path=first,
            staging_root=tmp_path / "stage-one",
            member_path="images/disk.img",
        )
        stage_and_enqueue_artifact(
            s,
            artifactclass="disk.two",
            policy=second_policy,
            source_path=second,
            staging_root=tmp_path / "stage-two",
            member_path="images/disk.img",
        )

        rows = list(
            s.scalars(
                select(StagingTransform).order_by(
                    StagingTransform.artifactclass, StagingTransform.step_order
                )
            )
        )
        assert [(row.artifactclass, row.step_order) for row in rows] == [
            ("disk.one", 0),
            ("disk.one", 1),
            ("disk.two", 0),
            ("disk.two", 1),
        ]
        keys = {(row.bundle_id, row.stored_member_path, row.step_order) for row in rows}
        assert len(keys) == len(rows)

        by_class: dict[str, list[StagingTransform]] = {}
        for row in rows:
            by_class.setdefault(row.artifactclass, []).append(row)
        tagged_steps = by_class["disk.two"]
        # The intermediate stored name is tagged (this is what the unique
        # surface needs), and the chain still links step 1 to step 0.
        assert tagged_steps[0].stored_member_path != "images/disk.img"
        assert tagged_steps[1].original_member_path == tagged_steps[0].stored_member_path
        # Step 0's original is untagged in both chains: it is the logical name.
        assert by_class["disk.one"][0].original_member_path == "images/disk.img"
        assert tagged_steps[0].original_member_path == "images/disk.img"


def test_stored_member_name_wins_over_a_co_resident_logical_name(
    engine: Engine, tmp_path: Path
) -> None:
    """Adding the logical tier must not cost the untagged member its own name.

    Inside one class the first arrival keeps ``images/disk.img`` and its
    neighbour is tagged, so that one string is both a *stored* name (the
    first member's) and a *logical* name (the second's). Resolving both tiers
    together would make it ambiguous and leave the untagged member
    unrestorable by the only name it has — a fix that broke the case it was
    meant to widen. The stored name is authoritative and wins; the tagged
    member is reached by the ``stored_member_name`` its receipt prints.
    """
    from sutradhara.archive_restore import resolve_member_asset_hash
    from sutradhara.staging import stage_and_enqueue_artifact

    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        policy = _staging_class(s, "disk.one", ["pool-a"], compression=False)
        first = _source(tmp_path / "one" / "images", "disk.img", b"one-bytes" * 20)
        second = _source(tmp_path / "two" / "images", "disk.img", b"two-bytes" * 20)
        staged_first = stage_and_enqueue_artifact(
            s,
            artifactclass="disk.one",
            policy=policy,
            source_path=first,
            staging_root=tmp_path / "stage-one",
            member_path="images/disk.img",
        )
        staged_second = stage_and_enqueue_artifact(
            s,
            artifactclass="disk.one",
            policy=policy,
            source_path=second,
            staging_root=tmp_path / "stage-two",
            member_path="images/disk.img",
        )
        assert staged_first.stored_member_path == "images/disk.img"
        assert staged_second.stored_member_path != "images/disk.img"

        assert (
            resolve_member_asset_hash(
                s, artifactclass="disk.one", member_name="images/disk.img"
            )
            == staged_first.logical_sha256
        )
        assert (
            resolve_member_asset_hash(
                s,
                artifactclass="disk.one",
                member_name=staged_second.stored_member_path,
            )
            == staged_second.logical_sha256
        )


def test_logical_name_shared_by_two_tagged_members_is_ambiguous(
    engine: Engine, tmp_path: Path
) -> None:
    """A logical name that maps to two assets must fail loudly and usefully.

    Two bundles, each holding a co-resident ``other`` member that took
    ``images/disk.img`` first, so **both** of this class's members are tagged
    and the logical name matches no stored name of its own. Silently
    returning either would restore the wrong bytes under a correctly-quoted
    receipt name. The error names the two ``stored_member_name`` values that
    tell them apart, and each of those still resolves.
    """
    from sutradhara.archive_restore import RestoreNameError, resolve_member_asset_hash
    from sutradhara.staging import stage_and_enqueue_artifact

    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        other = _staging_class(s, "disk.other", ["pool-a"], compression=False)
        mine = _staging_class(s, "disk.mine", ["pool-a"], compression=False)

        tagged: list[bytes] = []
        for index in range(2):
            occupant = _source(
                tmp_path / f"occupant-{index}" / "images",
                "disk.img",
                f"occupant-{index}".encode() * 20,
            )
            stage_and_enqueue_artifact(
                s,
                artifactclass="disk.other",
                policy=other,
                source_path=occupant,
                staging_root=tmp_path / f"stage-occupant-{index}",
                member_path="images/disk.img",
            )
            source = _source(
                tmp_path / f"mine-{index}" / "images",
                "disk.img",
                f"mine-{index}".encode() * 20,
            )
            staged = stage_and_enqueue_artifact(
                s,
                artifactclass="disk.mine",
                policy=mine,
                source_path=source,
                staging_root=tmp_path / f"stage-mine-{index}",
                member_path="images/disk.img",
            )
            assert staged.stored_member_path != "images/disk.img"
            tagged.append(staged.logical_sha256)
            # Seal the accumulator so the next pair opens a fresh one.
            bundle = s.get(Bundle, staged.bundle_id)
            bundle.status = "sealed"
            s.flush()

        paths = sorted(
            member.member_path
            for member in s.scalars(
                select(BundleMember).where(BundleMember.artifactclass == "disk.mine")
            )
        )
        assert len(paths) == 2
        with pytest.raises(RestoreNameError) as excinfo:
            resolve_member_asset_hash(
                s, artifactclass="disk.mine", member_name="images/disk.img"
            )
        message = str(excinfo.value)
        assert "ambiguous" in message
        for path in paths:
            assert path in message
        # Each stored name still resolves to its own asset.
        resolved = {
            resolve_member_asset_hash(s, artifactclass="disk.mine", member_name=path)
            for path in paths
        }
        assert resolved == set(tagged)


def test_ambiguity_hint_never_recommends_the_name_that_just_failed(
    engine: Engine, tmp_path: Path
) -> None:
    """The hint must not answer "which name?" with the name that raised.

    An ordinary history reaches this: asset X is the first arrival in bundle
    one and keeps ``images/disk.img``; re-staged into bundle two it finds a
    co-resident holding that name and is tagged; asset Y is then the first
    arrival in bundle three and keeps ``images/disk.img`` too. The request
    matches two members in the *stored* tier, so the name that just raised is
    itself one of the stored names the hint collects — it used to be echoed
    back as the remedy ("… instead: images/disk.<tag>.img, images/disk.img").

    Excluding it is not enough on its own: what remains is X's tagged name,
    which leaves Y with no name of its own, so no name partitions the two and
    the hint must fall through to the hashes rather than half-answer.
    """
    from sutradhara.archive_restore import RestoreNameError, resolve_member_asset_hash
    from sutradhara.staging import stage_and_enqueue_artifact

    def seal(bundle_id: str) -> None:
        s.get(Bundle, bundle_id).status = "sealed"
        s.flush()

    with session_scope(engine) as s:
        _add_backend_pools(s, [("pool-a", "rao-plain-v1")])
        other = _staging_class(s, "disk.other", ["pool-a"], compression=False)
        mine = _staging_class(s, "disk.mine", ["pool-a"], compression=False)

        x_source = _source(tmp_path / "x" / "images", "disk.img", b"asset-x" * 20)
        y_source = _source(tmp_path / "y" / "images", "disk.img", b"asset-y" * 20)
        occupant = _source(tmp_path / "occupant" / "images", "disk.img", b"occupant" * 20)

        # Bundle one: X is the first arrival and keeps the name.
        first = stage_and_enqueue_artifact(
            s,
            artifactclass="disk.mine",
            policy=mine,
            source_path=x_source,
            staging_root=tmp_path / "stage-x-1",
            member_path="images/disk.img",
        )
        assert first.stored_member_path == "images/disk.img"
        seal(first.bundle_id)

        # Bundle two: a co-resident takes the name first, so X is tagged here.
        stage_and_enqueue_artifact(
            s,
            artifactclass="disk.other",
            policy=other,
            source_path=occupant,
            staging_root=tmp_path / "stage-occupant",
            member_path="images/disk.img",
        )
        again = stage_and_enqueue_artifact(
            s,
            artifactclass="disk.mine",
            policy=mine,
            source_path=x_source,
            staging_root=tmp_path / "stage-x-2",
            member_path="images/disk.img",
        )
        assert again.logical_sha256 == first.logical_sha256
        tagged_x = again.stored_member_path
        assert tagged_x != "images/disk.img"
        seal(again.bundle_id)

        # Bundle three: Y is the first arrival and keeps the name too.
        third = stage_and_enqueue_artifact(
            s,
            artifactclass="disk.mine",
            policy=mine,
            source_path=y_source,
            staging_root=tmp_path / "stage-y",
            member_path="images/disk.img",
        )
        assert third.stored_member_path == "images/disk.img"
        assert third.logical_sha256 != first.logical_sha256

        with pytest.raises(RestoreNameError) as excinfo:
            resolve_member_asset_hash(
                s, artifactclass="disk.mine", member_name="images/disk.img"
            )
        message = str(excinfo.value)
        assert "ambiguous" in message
        assert "Restore by one of the stored member names" not in message
        # X's tagged name is a real stored name, but recommending it alone
        # would leave Y unnameable, so no name is recommended at all.
        assert tagged_x not in message
        assert "restore by asset hash" in message
        for digest in (first.logical_sha256, third.logical_sha256):
            assert digest.hex() in message


# --- migration (§7 order) ---------------------------------------------------


def _alembic(db_path: Path, revision: str) -> subprocess.CompletedProcess[bytes]:
    import os
    import sys

    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["SUTRADHARA_DB_URL"] = f"sqlite:///{db_path.as_posix()}"
    return subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", revision],
        cwd=repo_root,
        env=env,
        capture_output=True,
    )


def _seed_pre_migration_estate(db_path: Path, *, duplicate_open: bool) -> None:
    """Two classes sharing one pool set, each with an open per-class
    accumulator (pre-migration schema) — the duplicate-fingerprint estate."""
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO backend (id, name, kind, implementation_family, tier, added_at) "
            "VALUES (1, 'rem', 'rem_tape', 'tape', 'self_describing', '2026-01-01')"
        )
        conn.execute(
            "INSERT INTO pool (id, backend_id, representation, location, offsite_gate, "
            "tier, accepts_writes, retired, created_at) "
            "VALUES ('pool-a', 1, 'rao-plain-v1', '', 0, '', 1, 0, '2026-01-01')"
        )
        for artifactclass in ("photo", "audio"):
            conn.execute(
                "INSERT INTO artifactclass_pool "
                "(artifactclass, pool_id, active, sort_order, created_at) "
                f"VALUES ('{artifactclass}', 'pool-a', 1, 0, '2026-01-01')"
            )
            conn.execute(
                "INSERT INTO artifactclass_policy "
                "(artifactclass, ruleset, expect, target_bytes, max_age_seconds, "
                "restore_preference, min_copies, min_impl_families, staging_config, "
                "hdcache_config, updated_at) "
                f"VALUES ('{artifactclass}', 'rules', 'messy', 1024, 60, '[]', 3, 2, "
                "'{}', '{}', '2026-01-01')"
            )
        conn.execute(
            "INSERT INTO logical_asset (content_sha256, size_bytes, first_seen_at, validity) "
            "VALUES (X'11', 3, '2026-01-01', 'unvalidated')"
        )
        open_bundles = [("bundle-photo", "photo")]
        if duplicate_open:
            open_bundles.append(("bundle-audio", "audio"))
        else:
            # The drained estate seals the second class's accumulator.
            conn.execute(
                "INSERT INTO bundle (id, artifactclass, status, total_bytes, "
                "member_count, target_bytes, max_age_seconds, ruleset, expect, opened_at) "
                "VALUES ('bundle-audio', 'audio', 'sealed', 3, 1, 1024, 60, "
                "'rules', 'messy', '2026-01-01')"
            )
        for bundle_id, artifactclass in open_bundles:
            conn.execute(
                "INSERT INTO bundle (id, artifactclass, status, total_bytes, "
                "member_count, target_bytes, max_age_seconds, ruleset, expect, opened_at) "
                f"VALUES ('{bundle_id}', '{artifactclass}', 'open', 3, 1, 1024, 60, "
                "'rules', 'messy', '2026-01-01')"
            )
        conn.execute(
            "INSERT INTO bundle_member (bundle_id, logical_asset_hash, member_path, "
            "size_bytes, file_sha256, added_at) "
            "VALUES ('bundle-photo', X'11', 'a.bin', 3, X'11', '2026-01-01')"
        )
        conn.execute(
            "INSERT INTO bundle_member (bundle_id, logical_asset_hash, member_path, "
            "size_bytes, file_sha256, added_at) "
            "VALUES ('bundle-audio', X'11', 'b.bin', 3, X'11', '2026-01-01')"
        )
        conn.commit()


def test_migration_aborts_on_duplicate_open_accumulators(tmp_path: Path) -> None:
    """Two open per-class accumulators sharing a fingerprint must abort the
    migration — it never merges member sets or fakes a seal."""
    db_path = tmp_path / "dup.db"
    result = _alembic(db_path, "a4b5c6d7e8f9")
    assert result.returncode == 0, result.stderr.decode()
    _seed_pre_migration_estate(db_path, duplicate_open=True)
    result = _alembic(db_path, "head")
    assert result.returncode != 0
    assert b"share a" in result.stderr or b"share a" in result.stdout


def test_migration_backfills_drained_estate(tmp_path: Path) -> None:
    """A drained estate migrates: member classes backfilled, fingerprints
    with basis_source backfilled, projections written, columns dropped."""
    import json as json_module
    import sqlite3

    db_path = tmp_path / "drained.db"
    result = _alembic(db_path, "a4b5c6d7e8f9")
    assert result.returncode == 0, result.stderr.decode()
    _seed_pre_migration_estate(db_path, duplicate_open=False)
    result = _alembic(db_path, "head")
    assert result.returncode == 0, result.stderr.decode() + result.stdout.decode()

    with sqlite3.connect(db_path) as conn:
        member_classes = dict(
            conn.execute("SELECT bundle_id, artifactclass FROM bundle_member")
        )
        assert member_classes == {
            "bundle-photo": "photo",
            "bundle-audio": "audio",
        }
        rows = conn.execute(
            "SELECT id, bundle_group, group_basis FROM bundle ORDER BY id"
        ).fetchall()
        assert len(rows) == 2
        groups = {row[0]: row[1] for row in rows}
        # Same pool set -> same fingerprint for both bundles.
        assert groups["bundle-photo"] == groups["bundle-audio"]
        for _bundle_id, _group, basis_raw in rows:
            document = json_module.loads(basis_raw)
            assert document["basis_source"] == "backfilled"
            assert document["basis"] == [
                {"pool": "pool-a", "representation": "rao-plain-v1"}
            ]
            assert document["effective"]["target_bytes"] == 1024
        projections = dict(
            conn.execute("SELECT artifactclass, bundle_group FROM artifactclass_policy")
        )
        assert projections["photo"] == groups["bundle-photo"]
        assert projections["audio"] == groups["bundle-photo"]
        bundle_cols = {row[1] for row in conn.execute("PRAGMA table_info(bundle)")}
        assert "artifactclass" not in bundle_cols
        assert "ruleset" not in bundle_cols
        assert "expect" not in bundle_cols
        index_sql = conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE name='uq_bundle_open_accumulator_per_group'"
        ).fetchone()[0]
        assert "WHERE status = 'open' AND archive_id IS NULL" in index_sql
        submission_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='submission' AND type='table'"
        ).fetchone()[0]
        assert "accumulated" in submission_sql
        pool_cols = {row[1] for row in conn.execute("PRAGMA table_info(pool)")}
        assert "min_object_bytes" in pool_cols
