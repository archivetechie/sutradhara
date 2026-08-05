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
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from sutradhara.archive_restore import (
    RestoreNameError,
    _choose_bundle_restore_group,
    resolve_member_asset_hash,
)
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    BundlingPolicy,
    DurabilityPolicy,
    HdcachePolicy,
    PlacementPolicy,
    apply_artifactclass_policy,
)
from sutradhara.bundle_group import compute_bundle_group
from sutradhara.bundle_group_report import (
    WARNING_CLAMP_ACTIVE,
    WARNING_MEMBERSHIP_CHANGED,
    WARNING_NEAR_MISS,
    WARNING_NO_FLOOR_DECLARED,
    build_policy_apply_report,
    render_policy_apply_report,
)
from sutradhara.catalog.copies import add_bundle_copy
from sutradhara.catalog.models import (
    AssetLocator,
    Backend,
    Bundle,
    BundleMember,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource
from sutradhara.durability import AssetTarget, BundleTarget, durable_placements
from sutradhara.hdcache.fill import desired_target_for_asset, effective_privacy_level
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
        _apply(session, "cls-alpha", pools=("pool-1", "pool-2"),
               restore_preference=("pool-2", "pool-1"))
        _apply(session, "cls-beta", pools=("pool-1", "pool-2"),
               restore_preference=("pool-1", "pool-2"))
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
        _apply(session, "cls-alpha", pools=("pool-1", "pool-2"),
               restore_preference=("pool-2", "pool-1"))
        _apply(session, "cls-beta", pools=("pool-1", "pool-2"),
               restore_preference=("pool-1", "pool-2"))
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
        _place_bundle_in_pool(
            session, bundle_id="sony-split", pool=pool, members=members
        )

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
            _choose_bundle_restore_group(session, [solo_hash], "cls-one", backends=backends)
            is None
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
            **bundle_kwargs_for_class(
                session, "solo", target_bytes=1024**3, max_age_seconds=3600
            ),
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
        report = _apply(
            session, "joiner", pools=("pool-1",), target_gb=1.0, max_age_seconds=600
        )

        group = report.group_of("solo")
        assert group is not None
        assert group.member_classes == ("joiner", "solo")
        assert group.effective_max_age_seconds == 600
        assert group.open_bundles_predating_change == ("open-accumulator",)
        assert WARNING_MEMBERSHIP_CHANGED in group.warning_kinds()
        assert bundle.max_age_seconds == 3600, "an open bundle's thresholds never change"


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
            base = node.value
            if isinstance(base, ast.Name) and base.id in {"bundle", "Bundle"}:
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}")
    assert offenders == []
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
