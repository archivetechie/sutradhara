"""Member-grain rewrites of the dropped ``bundle.artifactclass`` column (§5).

The column carried a lie once bundles could hold more than one class: it made
every consumer that read it — restore ordering, hdcache privacy, durability
filters, operator surfaces — answer a member-grain question with a
bundle-grain answer. These tests pin what each rewritten join must now do, and
each names the specific wrong answer it guards against.
"""

from __future__ import annotations

import ast
import datetime as dt
import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from sutradhara.archive_fanout import (
    BuildArtifact,
    BuiltExclusion,
    _customer_manifest_members,
    _record_build_exclusions,
)
from sutradhara.archive_restore import (
    RestoreNameError,
    _choose_bundle_restore_group,
    resolve_member_asset_hash,
)
from sutradhara.arrangement import SourceMapEntry, render_source_map
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    BundlingPolicy,
    DurabilityPolicy,
    HdcachePolicy,
    PlacementPolicy,
    apply_artifactclass_policy,
)
from sutradhara.bundle_group import (
    BASIS_SOURCE_BACKFILLED,
    compute_bundle_group,
    group_basis_document,
)
from sutradhara.bundle_group_report import (
    DEGENERATE_TARGET_FLOOR_BYTES,
    WARNING_CLAMP_ACTIVE,
    WARNING_MEMBERSHIP_CHANGED,
    WARNING_NEAR_MISS,
    WARNING_NO_FLOOR_DECLARED,
    WARNING_ORPHAN_GROUP,
    WARNING_STALE_PROJECTION,
    WARNING_TARGET_BELOW_FLOOR,
    build_policy_apply_report,
    render_policy_apply_report,
)
from sutradhara.catalog.copies import add_bundle_copy
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    AssetLocator,
    Backend,
    Bundle,
    BundleMember,
    ExclusionRecord,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource
from sutradhara.durability import AssetTarget, BundleTarget, durable_placements
from sutradhara.hdcache.fill import desired_target_for_asset, effective_privacy_level
from sutradhara.pools import set_pool_representation
from sutradhara.replication import select_source_candidates
from sutradhara.sealing.port import Representation
from sutradhara.virtual_arrangement import _healthy_archived_artifactclasses
from tests.bundle_group_helpers import bundle_kwargs_for_class

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def _digest(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _add_backend(session: Session, name: str) -> Backend:
    backend = Backend(
        name=name,
        kind=BackendKind.MEMORY,
        tier=BackendTier.CATALOG_AUTHORITATIVE,
    )
    session.add(backend)
    session.flush()
    return backend


def _add_pool(
    session: Session,
    pool_id: str,
    backend: Backend,
    *,
    min_object_bytes: int | None = None,
) -> Pool:
    pool = Pool(
        id=pool_id,
        backend_id=backend.id,
        representation=Representation.RAW_BYTES.value,
        min_object_bytes=min_object_bytes,
    )
    session.add(pool)
    session.flush()
    return pool


def _apply(
    session: Session,
    artifactclass: str,
    *,
    pools: tuple[str, ...],
    restore_preference: tuple[str, ...] | None = None,
    target_gb: float = 1.0,
    max_age_seconds: int = 3600,
    hdcache: HdcachePolicy = HdcachePolicy(),
) -> Any:
    return apply_artifactclass_policy(
        session,
        artifactclass,
        ArtifactClassPolicy(
            ruleset=f"{artifactclass}.rules",
            placements=tuple(PlacementPolicy(pool_id) for pool_id in pools),
            bundling=BundlingPolicy(target_gb=target_gb, max_age_seconds=max_age_seconds),
            restore_preference=restore_preference or pools,
            expect="messy",
            hdcache=hdcache,
            durability=DurabilityPolicy(min_copies=len(pools), min_impl_families=1),
        ),
    )


def _add_member(
    session: Session,
    *,
    bundle_id: str,
    artifactclass: str,
    asset_hash: bytes,
    member_path: str,
) -> BundleMember:
    if session.get(LogicalAsset, asset_hash) is None:
        session.add(LogicalAsset(content_sha256=asset_hash, size_bytes=64))
        session.flush()
    member = BundleMember(
        bundle_id=bundle_id,
        logical_asset_hash=asset_hash,
        artifactclass=artifactclass,
        member_path=member_path,
        size_bytes=64,
        file_sha256=asset_hash,
    )
    session.add(member)
    session.flush()
    return member


def _place_bundle_in_pool(
    session: Session,
    *,
    bundle_id: str,
    pool: Pool,
    members: list[BundleMember],
) -> None:
    """Write one healthy, verified bundle copy in a pool, with member locators."""
    copy, _ = add_bundle_copy(
        session,
        bundle_id=bundle_id,
        backend_id=pool.backend_id,
        pool_id=pool.id,
        native_locator={"object": f"{bundle_id}@{pool.id}"},
        integrity_hash=_digest(f"{bundle_id}@{pool.id}".encode()),
        source=CopySource.INGEST,
        health=CopyHealth.OK,
        storage_metadata={"representation": Representation.RAW_BYTES.value},
    )
    now = dt.datetime.now(dt.UTC)
    copy.last_checked_at = now
    copy.last_measured_digest = copy.integrity_hash
    copy.last_measured_at = now
    for member in members:
        session.add(
            AssetLocator(
                logical_asset_hash=member.logical_asset_hash,
                pool_id=pool.id,
                copy_id=copy.id,
                bundle_id=bundle_id,
                native_locator={"size_bytes": 64, "member_path": member.member_path},
                member_path=member.member_path,
                representation=Representation.RAW_BYTES.value,
            )
        )
    session.flush()


# --------------------------------------------------------------------------
# Restore selection: replication.py::_user_restore_candidates (§5)
# --------------------------------------------------------------------------


def test_mixed_bundle_restore_honours_each_member_own_restore_preference(
    engine: Engine,
) -> None:
    """Guards: a member restoring through a co-resident class's preference.

    Two classes coalesce into one group (identical pool sets) but declare
    *opposing* ``restore_preference`` orders. A member-initiated restore must
    read its own class's order; the whole-bundle operator restore has no
    single class to speak for it and must use ``group_basis`` order.
    """
    hash_a = _digest(b"member of class alpha")
    hash_b = _digest(b"member of class beta")

    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        pool_one = _add_pool(session, "pool-1", backend)
        pool_two = _add_pool(session, "pool-2", backend)
        # Same pool set -> same fingerprint -> one group. Opposing read orders:
        # restore_preference is explicitly excluded from group identity (§2).
        _apply(
            session,
            "cls-alpha",
            pools=("pool-1", "pool-2"),
            restore_preference=("pool-2", "pool-1"),
        )
        _apply(
            session, "cls-beta", pools=("pool-1", "pool-2"), restore_preference=("pool-1", "pool-2")
        )
        alpha_group, _ = compute_bundle_group(session, "cls-alpha")
        beta_group, _ = compute_bundle_group(session, "cls-beta")
        assert alpha_group == beta_group, "the two classes must coalesce for this test to bite"

        bundle = Bundle(
            id="mixed-1",
            **bundle_kwargs_for_class(session, "cls-alpha"),
            status="sealed",
        )
        session.add(bundle)
        session.flush()
        member_a = _add_member(
            session,
            bundle_id="mixed-1",
            artifactclass="cls-alpha",
            asset_hash=hash_a,
            member_path="alpha/one.bin",
        )
        member_b = _add_member(
            session,
            bundle_id="mixed-1",
            artifactclass="cls-beta",
            asset_hash=hash_b,
            member_path="beta/one.bin",
        )
        for pool in (pool_one, pool_two):
            _place_bundle_in_pool(
                session,
                bundle_id="mixed-1",
                pool=pool,
                members=[member_a, member_b],
            )

        alpha_order = [
            copy.pool_id
            for copy in select_source_candidates(
                session, AssetTarget(hash_a, "cls-alpha"), purpose="user_restore"
            )
        ]
        beta_order = [
            copy.pool_id
            for copy in select_source_candidates(
                session, AssetTarget(hash_b, "cls-beta"), purpose="user_restore"
            )
        ]
        bundle_order = [
            copy.pool_id
            for copy in select_source_candidates(
                session, BundleTarget("mixed-1"), purpose="user_restore"
            )
        ]

        assert alpha_order == ["pool-2", "pool-1"]
        assert beta_order == ["pool-1", "pool-2"]
        # Basis order is canonical pool-id order, which contradicts cls-alpha's
        # preference — so this cannot be passing by accident.
        assert bundle_order == ["pool-1", "pool-2"]
        assert bundle_order != alpha_order


def test_bundle_restore_group_selection_ranks_by_the_member_class_preference(
    engine: Engine,
) -> None:
    """``_choose_bundle_restore_group`` ranks pools by the *asking* class."""
    asset_hash = _digest(b"grouped restore member")

    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        pool_one = _add_pool(session, "pool-1", backend)
        pool_two = _add_pool(session, "pool-2", backend)
        _apply(
            session,
            "cls-alpha",
            pools=("pool-1", "pool-2"),
            restore_preference=("pool-2", "pool-1"),
        )
        _apply(
            session, "cls-beta", pools=("pool-1", "pool-2"), restore_preference=("pool-1", "pool-2")
        )
        bundle = Bundle(
            id="mixed-2",
            **bundle_kwargs_for_class(session, "cls-alpha"),
            status="sealed",
        )
        session.add(bundle)
        session.flush()
        member_alpha = _add_member(
            session,
            bundle_id="mixed-2",
            artifactclass="cls-alpha",
            asset_hash=asset_hash,
            member_path="alpha/shared.bin",
        )
        member_beta = _add_member(
            session,
            bundle_id="mixed-2",
            artifactclass="cls-beta",
            asset_hash=asset_hash,
            member_path="beta/shared.bin",
        )
        for pool in (pool_one, pool_two):
            _place_bundle_in_pool(
                session,
                bundle_id="mixed-2",
                pool=pool,
                members=[member_alpha, member_beta],
            )
        backends = {backend.id: object()}

        alpha_choice = _choose_bundle_restore_group(
            session, [asset_hash], "cls-alpha", backends=backends
        )
        beta_choice = _choose_bundle_restore_group(
            session, [asset_hash], "cls-beta", backends=backends
        )

    assert alpha_choice is not None
    assert beta_choice is not None
    assert alpha_choice[0] == "pool-2"
    assert beta_choice[0] == "pool-1"


# --------------------------------------------------------------------------
# hdcache privacy: per-member, unaffected by co-residence (§5, round-1 O14)
# --------------------------------------------------------------------------


def test_hdcache_privacy_does_not_escalate_under_co_residence(engine: Engine) -> None:
    """Guards: a confidential co-resident dragging an open member up to p2.

    hdcache privacy is a second confidentiality axis and is per-member. Once
    two classes share a crate, an open member sitting next to a p2 member must
    keep privacy ``none`` — the join is member class + asset hash, never the
    bundle's occupancy. The reverse also holds: the p2 member never falls to
    the open member's level.
    """
    open_hash = _digest(b"public daily rushes")
    private_hash = _digest(b"confidential interview")

    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        pool = _add_pool(session, "pool-1", backend)
        _apply(
            session,
            "cls-open",
            pools=("pool-1",),
            hdcache=HdcachePolicy(enabled=True, privacy_level="none"),
        )
        _apply(
            session,
            "cls-private",
            pools=("pool-1",),
            hdcache=HdcachePolicy(enabled=True, privacy_level="p2"),
        )
        bundle = Bundle(
            id="co-resident",
            **bundle_kwargs_for_class(session, "cls-open"),
            status="sealed",
        )
        session.add(bundle)
        session.flush()
        open_member = _add_member(
            session,
            bundle_id="co-resident",
            artifactclass="cls-open",
            asset_hash=open_hash,
            member_path="open/rushes.mov",
        )
        private_member = _add_member(
            session,
            bundle_id="co-resident",
            artifactclass="cls-private",
            asset_hash=private_hash,
            member_path="private/interview.mov",
        )
        _place_bundle_in_pool(
            session,
            bundle_id="co-resident",
            pool=pool,
            members=[open_member, private_member],
        )

        assert effective_privacy_level(session, open_hash) == "none"
        assert effective_privacy_level(session, private_hash) == "p2"

        open_target = desired_target_for_asset(session, open_hash)
        private_target = desired_target_for_asset(session, private_hash)

    assert open_target is not None
    assert private_target is not None
    assert open_target.artifactclass == "cls-open"
    assert private_target.artifactclass == "cls-private"
    # The group key stays per-member too: co-residence must not merge two
    # classes' cache cohorts into one.
    assert open_target.group_key != private_target.group_key


def test_hdcache_privacy_escalates_only_through_the_asset_own_classes(
    engine: Engine,
) -> None:
    """The strictest-of-containing-classes rule applies to the *asset*.

    The Sony split puts one content hash under two classes; that content is as
    private as the strictest class that actually holds it. Escalation through
    genuine shared membership is correct — escalation through mere bundle
    co-residence (the test above) is not.
    """
    shared_hash = _digest(b"split card content")

    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        pool = _add_pool(session, "pool-1", backend)
        _apply(
            session,
            "cls-open",
            pools=("pool-1",),
            hdcache=HdcachePolicy(enabled=True, privacy_level="none"),
        )
        _apply(
            session,
            "cls-private",
            pools=("pool-1",),
            hdcache=HdcachePolicy(enabled=True, privacy_level="p2"),
        )
        bundle = Bundle(
            id="shared-content",
            **bundle_kwargs_for_class(session, "cls-open"),
            status="sealed",
        )
        session.add(bundle)
        session.flush()
        members = [
            _add_member(
                session,
                bundle_id="shared-content",
                artifactclass="cls-open",
                asset_hash=shared_hash,
                member_path="event-1/clip.mov",
            ),
            _add_member(
                session,
                bundle_id="shared-content",
                artifactclass="cls-private",
                asset_hash=shared_hash,
                member_path="event-2/clip.mov",
            ),
        ]
        _place_bundle_in_pool(
            session,
            bundle_id="shared-content",
            pool=pool,
            members=members,
        )

        assert effective_privacy_level(session, shared_hash) == "p2"


# --------------------------------------------------------------------------
# Sony split: duplicate content resolves distinctly through every join (§5)
# --------------------------------------------------------------------------


def test_sony_split_duplicate_content_resolves_by_hash_and_class(engine: Engine) -> None:
    """Guards: co-residence answering "is this asset in this class?" with yes.

    The Sony two-event card split is the one legitimate duplicate-content
    case: one content hash, two events, two classes, two member paths, all in
    one group bundle. Every rewritten join must key on (asset hash, class) —
    never on the bundle's co-residents and never on the path alone. The
    discriminating case is ``solo_hash``: it belongs to ``cls-two`` only, and
    the pre-rewrite bundle-level filter would have reported it archived under
    ``cls-one`` purely because ``cls-one`` has members in the same bundle.
    """
    shared_hash = _digest(b"the duplicated clip")
    solo_hash = _digest(b"a clip that belongs to one class only")

    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        pool = _add_pool(session, "pool-1", backend)
        _apply(session, "cls-one", pools=("pool-1",))
        _apply(session, "cls-two", pools=("pool-1",))
        bundle = Bundle(
            id="sony-split",
            **bundle_kwargs_for_class(session, "cls-one"),
            status="sealed",
        )
        session.add(bundle)
        session.flush()
        members = [
            _add_member(
                session,
                bundle_id="sony-split",
                artifactclass="cls-one",
                asset_hash=shared_hash,
                member_path="event-1/clip.mov",
            ),
            _add_member(
                session,
                bundle_id="sony-split",
                artifactclass="cls-two",
                asset_hash=shared_hash,
                member_path="event-2/clip.mov",
            ),
            _add_member(
                session,
                bundle_id="sony-split",
                artifactclass="cls-two",
                asset_hash=solo_hash,
                member_path="event-2/solo.mov",
            ),
        ]
        _place_bundle_in_pool(session, bundle_id="sony-split", pool=pool, members=members)

        # durability.py::_locator_artifactclass_filter — the shared hash is
        # placed under both classes; the solo hash under exactly one.
        assert durable_placements(
            session,
            AssetTarget(shared_hash, "cls-one"),
            require_verified=False,
            artifactclass="cls-one",
        )
        assert durable_placements(
            session,
            AssetTarget(shared_hash, "cls-two"),
            require_verified=False,
            artifactclass="cls-two",
        )
        assert durable_placements(
            session,
            AssetTarget(solo_hash, "cls-two"),
            require_verified=False,
            artifactclass="cls-two",
        )
        assert (
            durable_placements(
                session,
                AssetTarget(solo_hash, "cls-one"),
                require_verified=False,
                artifactclass="cls-one",
            )
            == []
        )

        # archive_restore.py::resolve_member_asset_hash — path *and* class; the
        # duplicate resolves to the same content from either side, and a path
        # never leaks across the class boundary.
        assert (
            resolve_member_asset_hash(
                session, artifactclass="cls-one", member_name="event-1/clip.mov"
            )
            == shared_hash
        )
        assert (
            resolve_member_asset_hash(
                session, artifactclass="cls-two", member_name="event-2/clip.mov"
            )
            == shared_hash
        )
        with pytest.raises(RestoreNameError, match="no catalog member"):
            resolve_member_asset_hash(
                session, artifactclass="cls-one", member_name="event-2/solo.mov"
            )

        # archive_restore.py::_choose_bundle_restore_group — the solo hash has
        # no cls-one member row, so no locator qualifies for cls-one.
        backends = {backend.id: object()}
        assert (
            _choose_bundle_restore_group(session, [solo_hash], "cls-one", backends=backends) is None
        )
        assert (
            _choose_bundle_restore_group(session, [solo_hash], "cls-two", backends=backends)
            is not None
        )

        # virtual_arrangement.py::_healthy_archived_artifactclasses — class
        # health reads the asset's own member rows.
        assert _healthy_archived_artifactclasses(session, shared_hash) == [
            "cls-one",
            "cls-two",
        ]
        assert _healthy_archived_artifactclasses(session, solo_hash) == ["cls-two"]


# --------------------------------------------------------------------------
# The policy-apply report (§2)
# --------------------------------------------------------------------------


def _report_estate(session: Session) -> None:
    """Two coalesced classes, one near-miss class, one floorless pool.

    ``pool-floor`` declares a min-object floor well above every class's
    declared target, so the clamp activates; ``pool-open`` declares none.
    ``rep-a``/``rep-b`` agree on thresholds; ``rep-c`` shares their pool set
    (so it coalesces) but tunes both thresholds (so it is a near-miss).
    """
    backend = _add_backend(session, "mem")
    _add_pool(session, "pool-floor", backend, min_object_bytes=64 * 1024**3)
    _add_pool(session, "pool-open", backend)
    _apply(
        session,
        "rep-a",
        pools=("pool-floor", "pool-open"),
        target_gb=1.0,
        max_age_seconds=3600,
    )
    _apply(
        session,
        "rep-b",
        pools=("pool-floor", "pool-open"),
        target_gb=1.0,
        max_age_seconds=3600,
        hdcache=HdcachePolicy(enabled=True, privacy_level="p2"),
    )
    _apply(
        session,
        "rep-c",
        pools=("pool-floor", "pool-open"),
        restore_preference=("pool-open", "pool-floor"),
        target_gb=2.0,
        max_age_seconds=600,
    )


def test_policy_apply_report_golden(engine: Engine) -> None:
    """Every named §2 section, on an estate built to exercise all of them."""
    with session_scope(engine) as session:
        _report_estate(session)
        fingerprint, _basis = compute_bundle_group(session, "rep-a")
        report = build_policy_apply_report(session, applied_artifactclass="rep-c")
        rendered = render_policy_apply_report(report)
        payload = report.to_json()

    assert payload == {
        "applied_artifactclass": "rep-c",
        "groups": [
            {
                "fingerprint": fingerprint,
                "canonical_basis": [
                    {"pool": "pool-floor", "representation": "raw-bytes"},
                    {"pool": "pool-open", "representation": "raw-bytes"},
                ],
                "canonical_basis_json": (
                    '[{"pool":"pool-floor","representation":"raw-bytes"},'
                    '{"pool":"pool-open","representation":"raw-bytes"}]'
                ),
                "member_classes": ["rep-a", "rep-b", "rep-c"],
                "pools": [
                    {
                        "pool": "pool-floor",
                        "representation": "raw-bytes",
                        "min_object_bytes": 64 * 1024**3,
                    },
                    {
                        "pool": "pool-open",
                        "representation": "raw-bytes",
                        "min_object_bytes": None,
                    },
                ],
                "effective": {
                    # max-of-targets (2 GiB), clamped up to the strictest
                    # declared pool floor; min-of-ages (600s).
                    "target_bytes": 64 * 1024**3,
                    "max_age_seconds": 600,
                    "declared_target_bytes": 2 * 1024**3,
                },
                "near_miss_cohorts": [
                    {
                        "target_bytes": 1024**3,
                        "max_age_seconds": 3600,
                        "artifactclasses": ["rep-a", "rep-b"],
                    },
                    {
                        "target_bytes": 2 * 1024**3,
                        "max_age_seconds": 600,
                        "artifactclasses": ["rep-c"],
                    },
                ],
                "differing_excluded_fields": {
                    "restore_preference": {
                        "rep-a": ["pool-floor", "pool-open"],
                        "rep-b": ["pool-floor", "pool-open"],
                        "rep-c": ["pool-open", "pool-floor"],
                    },
                    "hdcache_config": {
                        "rep-a": {"enabled": False, "privacy_level": "none"},
                        "rep-b": {"enabled": True, "privacy_level": "p2"},
                        "rep-c": {"enabled": False, "privacy_level": "none"},
                    },
                },
                "basis_source_counts": {"derived": 0, "backfilled": 0},
                "open_bundles_predating_change": [],
                "warnings": [
                    {
                        "kind": WARNING_CLAMP_ACTIVE,
                        "message": (
                            "declared target 2147483648 sits below a member pool floor; "
                            "clamped up to 68719476736 by pool-floor (floor 68719476736)"
                        ),
                    },
                    {
                        "kind": WARNING_NO_FLOOR_DECLARED,
                        "message": (
                            "pool pool-open declares no min_object_bytes; no efficiency "
                            "floor is enforced for this group (NULL is never an implicit "
                            "zero)"
                        ),
                    },
                    {
                        "kind": WARNING_NEAR_MISS,
                        "message": (
                            "identical pools, differing thresholds (rep-a, rep-b -> "
                            "target=1073741824 max_age=3600; rep-c -> target=2147483648 "
                            "max_age=600); the group takes max target 2147483648 and min "
                            "age 600"
                        ),
                    },
                ],
            }
        ],
        # Every class on this estate still derives its own group's fingerprint.
        "orphan_groups": [],
    }

    # The rendered operator view names every section the design promises.
    for needle in (
        fingerprint,
        "basis",
        "classes        rep-a, rep-b, rep-c",
        "pool-floor[raw-bytes] floor=68719476736",
        "pool-open[raw-bytes]",
        "effective      target_bytes=68719476736 max_age_seconds=600",
        "near-miss",
        "differs        restore_preference:",
        "bundles        derived=0 backfilled=0",
        f"[{WARNING_CLAMP_ACTIVE}]",
        f"[{WARNING_NO_FLOOR_DECLARED}]",
        f"[{WARNING_NEAR_MISS}]",
    ):
        assert needle in rendered, needle


def test_report_warns_when_the_effective_target_makes_include_alone_the_rule(
    engine: Engine,
) -> None:
    """The silent degeneration the iron run found, now said out loud.

    ``enqueue_artifact`` routes any member at or above the group's effective
    ``target_bytes`` into a non-adoptable funnel bundle of its own. A group
    whose effective target sits at or below ordinary member sizes therefore
    seals one object per member — the tape-row waste bundling exists to
    prevent — and the report used to say nothing at all: the clamp is inactive
    (no pool declares a floor) and ``no-floor-declared`` only reports that the
    guard is *off*, never that it being off has already produced a harmful
    number. The observed shape: a ``target_gb=0.000000001`` policy computes
    ``target_bytes = 1``, and three 1.5 KB members became three sealed objects.
    """
    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        _add_pool(session, "pool-open", backend)
        report = _apply(
            session,
            "degenerate",
            pools=("pool-open",),
            target_gb=0.000000001,
            max_age_seconds=3600,
        )
        group = report.group_of("degenerate")
        assert group is not None
        assert group.effective_target_bytes == 1
        assert WARNING_TARGET_BELOW_FLOOR in group.warning_kinds()
        message = next(
            warning.message
            for warning in group.warnings
            if warning.kind == WARNING_TARGET_BELOW_FLOOR
        )
        assert str(DEGENERATE_TARGET_FLOOR_BYTES) in message
        assert "no member pool declares a min_object_bytes" in message
        assert "include-alone" in message
        # It reaches the operator's actual surface, not just the JSON.
        assert f"[{WARNING_TARGET_BELOW_FLOOR}]" in render_policy_apply_report(report)


def test_report_warns_when_the_declared_pool_floor_is_itself_degenerate(
    engine: Engine,
) -> None:
    """A floor comparison against the pool's own declaration cannot fire.

    Guards two wrong shapes at once. Keying the warning on the *declared*
    ``min_object_bytes`` is vacuous — the clamp lifts the effective target to
    at least the strictest declared floor, so ``effective < declared`` is
    unreachable. Gating it on "no floor declared" is worse: it stays silent
    exactly when an operator declared a floor so low it degenerates anyway.
    Here ``pool-tiny`` declares 4 KiB, the clamp fires, and the group still
    seals one object per member.
    """
    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        _add_pool(session, "pool-tiny", backend, min_object_bytes=4096)
        report = _apply(
            session,
            "tiny-floor",
            pools=("pool-tiny",),
            target_gb=0.000000001,
            max_age_seconds=3600,
        )
        group = report.group_of("tiny-floor")
        assert group is not None
        assert group.effective_target_bytes == 4096
        kinds = group.warning_kinds()
        assert WARNING_TARGET_BELOW_FLOOR in kinds
        # The pool declares a floor, so the floorless warning must not fire —
        # the degenerate-target warning is the only one that can catch this.
        assert WARNING_NO_FLOOR_DECLARED not in kinds
        message = next(
            warning.message
            for warning in group.warnings
            if warning.kind == WARNING_TARGET_BELOW_FLOOR
        )
        assert "pool-tiny = 4096" in message


def test_degenerate_target_warning_is_a_warning_and_not_a_gate(
    engine: Engine,
) -> None:
    """Refusing the policy would break legitimate small-object pools.

    The apply must succeed, the group must open on the thresholds it declared,
    and a small member must still route include-alone exactly as before. The
    report is a value the operator reads; nothing on the write path consults
    the floor — the constant does not even live in ``bundle_group``.
    """
    from sutradhara.archive_bundle import enqueue_artifact
    from sutradhara.bundle_group import effective_group_thresholds

    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        _add_pool(session, "pool-open", backend)
        report = _apply(
            session,
            "small-objects",
            pools=("pool-open",),
            target_gb=0.000000001,
            max_age_seconds=3600,
        )
        assert WARNING_TARGET_BELOW_FLOOR in report.group_of("small-objects").warning_kinds()

        record = session.scalars(
            select(ArtifactClassPolicyRecord).where(
                ArtifactClassPolicyRecord.artifactclass == "small-objects"
            )
        ).one()
        fingerprint, basis = compute_bundle_group(session, "small-objects")
        assert effective_group_thresholds(
            session,
            artifactclass="small-objects",
            policy=record,
            fingerprint=fingerprint,
            basis=basis,
        ) == (1, 3600)

        source = Path(REPO_ROOT / "pyproject.toml")
        payload = source.read_bytes()
        asset_hash = hashlib.sha256(payload).digest()
        session.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(payload)))
        session.flush()
        bundle, member, created = enqueue_artifact(
            session,
            artifactclass="small-objects",
            policy=record,
            logical_asset_hash=asset_hash,
            source_path=source,
        )
        assert created
        # Include-alone: its own non-adoptable funnel, untouched by the warning.
        assert bundle.archive_id is not None
        assert bundle.member_count == 1
        assert member.bundle_id == bundle.id


def test_report_is_silent_when_the_effective_target_clears_the_object_floor(
    engine: Engine,
) -> None:
    """A tuned estate must not be nagged.

    The golden estate's 64 GiB clamped target and this 2 GiB unclamped one are
    both healthy; a warning that fired here would be noise on every apply and
    would train operators to ignore the one that matters.
    """
    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        _add_pool(session, "pool-open", backend)
        report = _apply(
            session, "healthy", pools=("pool-open",), target_gb=2.0, max_age_seconds=3600
        )
        group = report.group_of("healthy")
        assert group is not None
        assert group.effective_target_bytes == 2 * 1024**3
        assert WARNING_TARGET_BELOW_FLOOR not in group.warning_kinds()
        # The floorless pool is still reported — the two warnings are distinct
        # claims: "the guard is off" versus "it being off already hurt".
        assert WARNING_NO_FLOOR_DECLARED in group.warning_kinds()


def test_policy_apply_report_names_open_bundles_predating_a_membership_change(
    engine: Engine,
) -> None:
    """A class that joins or retunes a group is honoured from the next bundle.

    The open accumulator keeps the thresholds frozen at its open, so the apply
    report has to say out loud that the estate the operator just described is
    not the estate the open crate is running under.
    """
    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        _add_pool(session, "pool-1", backend)
        _apply(session, "solo", pools=("pool-1",), target_gb=1.0, max_age_seconds=3600)
        bundle = Bundle(
            id="open-accumulator",
            **bundle_kwargs_for_class(session, "solo", target_bytes=1024**3, max_age_seconds=3600),
            status="open",
            target_bytes=1024**3,
            max_age_seconds=3600,
        )
        session.add(bundle)
        session.flush()

        settled = build_policy_apply_report(session)
        assert settled.groups[0].open_bundles_predating_change == ()
        assert WARNING_MEMBERSHIP_CHANGED not in settled.groups[0].warning_kinds()
        assert settled.groups[0].basis_source_counts == {"derived": 1, "backfilled": 0}

        # A second class joins the group with a tighter latency ceiling. The
        # group's effective max_age drops; the open bundle keeps its own.
        report = _apply(session, "joiner", pools=("pool-1",), target_gb=1.0, max_age_seconds=600)

        group = report.group_of("solo")
        assert group is not None
        assert group.member_classes == ("joiner", "solo")
        assert group.effective_max_age_seconds == 600
        assert group.open_bundles_predating_change == ("open-accumulator",)
        assert WARNING_MEMBERSHIP_CHANGED in group.warning_kinds()
        assert bundle.max_age_seconds == 3600, "an open bundle's thresholds never change"


def test_policy_apply_report_names_a_group_no_live_class_derives_any_more(
    engine: Engine,
) -> None:
    """Guards: an orphaned accumulator vanishing from the report in silence.

    ``set_pool_representation`` mutates a fingerprint input outside policy
    apply and recomputes the projection (§2, writer set) — so flipping a pool's
    representation under an open accumulator moves every class off the
    fingerprint the accumulator carries. Nothing moves the *bundle*: it keeps
    the old fingerprint and no live class derives it any more.

    Total membership loss is the strictest form of "membership changed since
    the last apply". Grouping the report by policy rows alone would drop the
    orphan entirely — the operator would see only the new fingerprint and be
    told nothing about the crate left behind, which can never be routed into
    again.
    """
    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        _add_pool(session, "pool-1", backend, min_object_bytes=2 * 1024**3)
        _apply(session, "solo", pools=("pool-1",), target_gb=1.0, max_age_seconds=3600)
        stranded_fingerprint, _basis = compute_bundle_group(session, "solo")
        session.add(
            Bundle(
                id="stranded-accumulator",
                **bundle_kwargs_for_class(
                    session, "solo", target_bytes=2 * 1024**3, max_age_seconds=3600
                ),
                status="open",
                target_bytes=2 * 1024**3,
                max_age_seconds=3600,
            )
        )
        session.flush()

        set_pool_representation(session, "pool-1", Representation.RAO_PLAIN_V1)
        report = build_policy_apply_report(session)
        rendered = render_policy_apply_report(report)

        # The class moved to a new fingerprint; the bundle did not follow it.
        live_fingerprint, _ = compute_bundle_group(session, "solo")
        assert live_fingerprint != stranded_fingerprint
        assert [group.fingerprint for group in report.groups] == [live_fingerprint]
        assert report.group(stranded_fingerprint) is None

        orphan = report.orphan_group(stranded_fingerprint)
        assert orphan is not None, "the orphaned group must not be dropped from the report"
        assert orphan.bundle_ids == ("stranded-accumulator",)
        assert orphan.open_bundle_ids == ("stranded-accumulator",)
        assert orphan.basis_source_counts == {"derived": 1, "backfilled": 0}
        # The basis is read back from the bundle's own frozen witness — the
        # placement it was opened under, which is no longer derivable.
        assert orphan.canonical_basis == ({"pool": "pool-1", "representation": "raw-bytes"},)
        assert [(pool.pool_id, pool.min_object_bytes) for pool in orphan.pools] == [
            ("pool-1", 2 * 1024**3)
        ]
        assert orphan.warning_kinds() == (WARNING_ORPHAN_GROUP,)
        assert "no live artifactclass derives this fingerprint" in orphan.warnings[0].message
        assert "stranded-accumulator" in orphan.warnings[0].message

    # And the operator's text view says so, not just the JSON.
    assert f"orphan group {stranded_fingerprint[:16]}" in rendered
    assert "no live class derives this fingerprint" in rendered
    assert "open           stranded-accumulator" in rendered
    assert f"[{WARNING_ORPHAN_GROUP}]" in rendered


def test_policy_apply_report_counts_a_backfilled_basis_and_excludes_it_from_drift(
    engine: Engine,
) -> None:
    """A backfilled basis is a marked guess: counted, never used as evidence.

    ``basis_source_counts`` is how an operator sees how much of the estate is
    still the migration's guess rather than a derived basis, so the backfilled
    arm has to be shown counting something. The same bundle also pins the other
    half of §7.2: a backfilled witness is excluded from the agreement check, so
    it never raises a membership-changed warning however far its frozen
    thresholds have drifted from the group's.
    """
    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        _add_pool(session, "pool-1", backend)
        _apply(session, "solo", pools=("pool-1",), target_gb=1.0, max_age_seconds=3600)
        fingerprint, basis = compute_bundle_group(session, "solo")
        session.add(
            Bundle(
                id="migrated-accumulator",
                bundle_group=fingerprint,
                group_basis=group_basis_document(
                    basis,
                    basis_source=BASIS_SOURCE_BACKFILLED,
                    # Deliberately nothing like the group's effective values.
                    target_bytes=7,
                    max_age_seconds=11,
                ),
                status="open",
                target_bytes=7,
                max_age_seconds=11,
            )
        )
        session.flush()

        group = build_policy_apply_report(session).group(fingerprint)

    assert group is not None
    assert group.basis_source_counts == {"derived": 0, "backfilled": 1}
    assert group.open_bundles_predating_change == ()
    assert WARNING_MEMBERSHIP_CHANGED not in group.warning_kinds()


def test_policy_apply_report_states_the_class_set_divergence_of_a_stale_projection(
    engine: Engine,
) -> None:
    """Guards: effective thresholds counting a class the group never lists.

    Member classes and near-miss cohorts are live-derived from each class's
    current placements. The threshold arithmetic is not: it comes from
    ``group_thresholds``, whose declared set is the **stored** ``bundle_group``
    projection — deliberately, because that is the set an accumulator actually
    uses at open, so the report can never describe arithmetic the runtime does
    not perform.

    A missed projection writer pulls the two apart in both directions at once,
    and here it is the numbers that lie by omission: ``alpha``'s group takes
    ``beta``'s 4 GiB target and 600 s ceiling while listing only ``alpha``, and
    no cohort mentions ``beta`` at all. The report states the divergence rather
    than leaving a reader to assume one class set produced both.
    """
    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        _add_pool(session, "pool-1", backend)
        _add_pool(session, "pool-2", backend)
        _apply(session, "alpha", pools=("pool-1",), target_gb=1.0, max_age_seconds=3600)
        _apply(session, "beta", pools=("pool-1",), target_gb=4.0, max_age_seconds=600)
        shared_fingerprint, _basis = compute_bundle_group(session, "alpha")

        # beta is re-placed onto its own pool, then its projection write is
        # undone — the "a fingerprint-input writer was missed" state.
        _apply(session, "beta", pools=("pool-2",), target_gb=4.0, max_age_seconds=600)
        beta_record = session.get(ArtifactClassPolicyRecord, "beta")
        assert beta_record is not None
        beta_record.bundle_group = shared_fingerprint
        session.flush()

        report = build_policy_apply_report(session)
        rendered = render_policy_apply_report(report)

    alpha_group = report.group(shared_fingerprint)
    assert alpha_group is not None
    assert alpha_group.member_classes == ("alpha",)
    assert [cohort.artifactclasses for cohort in alpha_group.near_miss_cohorts] == [("alpha",)]
    # beta's declaration is still counted into alpha's effective thresholds...
    assert alpha_group.declared_target_bytes == 4 * 1024**3
    assert alpha_group.effective_max_age_seconds == 600
    # ...so the warning names it as counted-but-not-a-member.
    counted = next(
        warning for warning in alpha_group.warnings if warning.kind == WARNING_STALE_PROJECTION
    )
    assert "beta" in counted.message
    assert "still counted in its thresholds" in counted.message
    assert "live-derived" in counted.message

    # And from beta's own live group, the same row reads as a stale projection.
    beta_group = report.group_of("beta")
    assert beta_group is not None
    assert beta_group.fingerprint != shared_fingerprint
    assert WARNING_STALE_PROJECTION in beta_group.warning_kinds()

    assert rendered.count(f"[{WARNING_STALE_PROJECTION}]") == 2


# --------------------------------------------------------------------------
# Build exclusions: class, ruleset and hash all come from the member (§5)
# --------------------------------------------------------------------------


def test_build_exclusions_source_class_and_ruleset_from_the_member(
    engine: Engine,
) -> None:
    """Guards: an exclusion filed under a co-resident class's ruleset.

    An exclusion whose path matches a member row exactly is a per-asset
    exclusion: it must record that member's class, that class's ruleset, and
    that member's logical hash — so the record joins back through members by
    hash. A cluster exclusion with no matching member has no recoverable
    producing class in a multi-class bundle and is recorded classless rather
    than guessed onto whichever class sorted first.
    """
    one_hash = _digest(b"member of class one")
    two_hash = _digest(b"member of class two")

    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        _add_pool(session, "pool-1", backend)
        _apply(session, "cls-one", pools=("pool-1",))
        _apply(session, "cls-two", pools=("pool-1",))
        bundle = Bundle(
            id="excluding",
            **bundle_kwargs_for_class(session, "cls-one"),
            status="open",
        )
        session.add(bundle)
        session.flush()
        _add_member(
            session,
            bundle_id="excluding",
            artifactclass="cls-one",
            asset_hash=one_hash,
            member_path="one/kept.mov",
        )
        _add_member(
            session,
            bundle_id="excluding",
            artifactclass="cls-two",
            asset_hash=two_hash,
            member_path="two/kept.mov",
        )
        session.refresh(bundle)

        _record_build_exclusions(
            session,
            bundle=bundle,
            artifact=BuildArtifact(
                artifact_path=Path("/dev/null"),
                stored_digest=_digest(b"artifact"),
                members=(),
                exclusions=(
                    BuiltExclusion(path="two/kept.mov", reason="deviation"),
                    BuiltExclusion(path="tmp/", reason="unsupported-entry", count=4),
                ),
            ),
        )
        records = {record.path: record for record in session.scalars(select(ExclusionRecord))}

        member_sourced = records["two/kept.mov"]
        assert member_sourced.artifactclass == "cls-two"
        assert member_sourced.ruleset_name == "cls-two.rules"
        assert member_sourced.logical_asset_hash == two_hash

        cluster = records["tmp/"]
        assert cluster.artifactclass == ""
        assert cluster.ruleset_name is None
        assert cluster.logical_asset_hash is None


# --------------------------------------------------------------------------
# The customer manifest is a member-grain receipt (§5)
# --------------------------------------------------------------------------


def test_customer_manifest_carries_per_member_class_and_distinguishes_by_stored_name(
    engine: Engine,
) -> None:
    """Two co-resident same-named members share ``member_name`` by design.

    The receipt is customer-facing, so the logical ``member_name`` stays
    untagged — which means the two rows repeat it. They are distinguished by
    ``stored_member_name``, which carries the disambiguation tag and matches
    the on-media layout, and by their own ``artifactclass``. Stated here so the
    repetition is never read as a duplicated member.
    """
    hash_one = _digest(b"event one clip")
    hash_two = _digest(b"event two clip")

    with session_scope(engine) as session:
        backend = _add_backend(session, "mem")
        _add_pool(session, "pool-1", backend)
        _apply(session, "cls-one", pools=("pool-1",))
        _apply(session, "cls-two", pools=("pool-1",))
        bundle = Bundle(
            id="receipt",
            **bundle_kwargs_for_class(session, "cls-one"),
            status="sealed",
        )
        session.add(bundle)
        session.flush()
        first = _add_member(
            session,
            bundle_id="receipt",
            artifactclass="cls-one",
            asset_hash=hash_one,
            member_path="CLIP.MOV",
        )
        first.source_metadata = {"logical_path": "CLIP.MOV"}
        # The ladder tagged the second member's stored name; its logical name
        # is unchanged.
        second = _add_member(
            session,
            bundle_id="receipt",
            artifactclass="cls-two",
            asset_hash=hash_two,
            member_path=f"CLIP.{hash_two.hex()[:10]}.MOV",
        )
        second.source_metadata = {"logical_path": "CLIP.MOV"}
        session.flush()
        session.refresh(bundle)

        entries = _customer_manifest_members(bundle)

    assert [entry["member_name"] for entry in entries] == ["CLIP.MOV", "CLIP.MOV"]
    assert len({entry["stored_member_name"] for entry in entries}) == 2
    by_class = {entry["artifactclass"]: entry for entry in entries}
    assert set(by_class) == {"cls-one", "cls-two"}
    assert by_class["cls-one"]["stored_member_name"] == "CLIP.MOV"
    assert by_class["cls-two"]["stored_member_name"].startswith("CLIP.")
    assert by_class["cls-two"]["stored_member_name"].endswith(".MOV")


# --------------------------------------------------------------------------
# The source-map renderer: absent is absent, never the string "None" (§5)
# --------------------------------------------------------------------------


def test_source_map_renders_an_absent_ingest_item_id_as_empty() -> None:
    """Guards: the literal string ``None`` reaching a hashed source map.

    A bundle group mixes arrangement-origin members (which always carry an
    ingest item) with intake-origin members (which do not). The source map is
    hashed into the submission manifest, so a stringified ``None`` would be a
    durable, signed lie about provenance rather than a display glitch.
    """
    rendered = render_source_map(
        [
            SourceMapEntry(
                archive_path="day-1/from-arrangement.mov",
                source_path="/src/a.mov",
                sha256=_digest(b"a"),
                size_bytes=11,
                ingest_item_id=7,
            ),
            SourceMapEntry(
                archive_path="day-1/from-intake.mov",
                source_path="/src/b.mov",
                sha256=_digest(b"b"),
                size_bytes=22,
                ingest_item_id=None,
            ),
        ]
    )

    rows = rendered.splitlines()
    assert rows[0].split("\t")[-1] == "ingest_item_id"
    assert rows[1].split("\t")[-1] == "7"
    assert rows[2].split("\t")[-1] == ""
    assert "None" not in rendered


# --------------------------------------------------------------------------
# The column is gone, and so are the stopgaps that stood in for it
# --------------------------------------------------------------------------


def _python_sources(*roots: str) -> list[Path]:
    return sorted(
        path
        for root in roots
        for path in (REPO_ROOT / root).rglob("*.py")
        if "__pycache__" not in path.parts
    )


_BUNDLE_BINDINGS = {"bundle", "Bundle"}


def _is_a_bundle_binding(base: ast.expr) -> bool:
    """True when ``base`` names a bundle — bare, or at the end of a chain.

    Matching the terminal name rather than only a bare ``ast.Name`` is what
    makes ``self.bundle.artifactclass`` and ``ctx.bundle.artifactclass``
    offenders too; the name-only form let an attribute chain slip through. The
    token match stays exact so that ``bundle_member.artifactclass`` — the
    correct member-grain read — is not swept up with it.
    """
    if isinstance(base, ast.Name):
        return base.id in _BUNDLE_BINDINGS
    if isinstance(base, ast.Attribute):
        return base.attr in _BUNDLE_BINDINGS
    return False


def test_no_source_reads_the_dropped_bundle_artifactclass_column() -> None:
    """No reader of ``bundle.artifactclass`` survives anywhere in ``src/``.

    Parsed rather than grepped: the column is discussed in prose in more than
    one docstring, and a text scan would either trip on the prose or be
    loosened until it stopped catching real attribute reads.
    """
    offenders: list[str] = []
    for path in _python_sources("src"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Attribute) or node.attr != "artifactclass":
                continue
            if _is_a_bundle_binding(node.value):
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}")
    assert offenders == []

    # The matcher itself, pinned: a scan that only caught bare names would pass
    # this file trivially while a chained read went unnoticed.
    def _base_of(source: str) -> ast.expr:
        node = ast.parse(source, mode="eval").body
        assert isinstance(node, ast.Attribute)
        return node.value

    assert _is_a_bundle_binding(_base_of("bundle.artifactclass"))
    assert _is_a_bundle_binding(_base_of("self.bundle.artifactclass"))
    assert not _is_a_bundle_binding(_base_of("bundle_member.artifactclass"))
    # And the ORM no longer offers it to read.
    assert not hasattr(Bundle, "artifactclass")


def test_no_bg_p4_stopgap_markers_remain() -> None:
    """Every BG-P4 stopgap comment P1/P3 left has been replaced, not deferred."""
    # The needle is assembled at runtime, and named without its comment prefix
    # in the docstring above, so this file never matches its own assertion.
    marker = "# BG-" + "P4"
    offenders = [
        f"{path.relative_to(REPO_ROOT)}"
        for path in _python_sources("src", "tests", "alembic")
        if marker in path.read_text(encoding="utf-8")
    ]
    assert offenders == []
