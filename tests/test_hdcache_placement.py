"""Placement-property tests for the hdcache disk tier."""

from __future__ import annotations

import hashlib
import inspect
from collections import Counter
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pytest
from sqlalchemy import Engine, event, select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import ArtifactClass, Backend, Copy, LogicalAsset, Pool
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.hdcache.models import CacheDisk, CacheEntry
from sutradhara.hdcache.placement import (
    DEFAULT_SPREAD_MIN_BYTES,
    DefaultDiskPlacementPolicy,
    DiskState,
    PlacementConfig,
    PlacementContext,
    PlacementError,
    build_placement_context,
    choose_placement,
    disk_states_from_catalog,
)


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    eng = make_engine(f"sqlite:///{tmp_path / 'hdcache-placement.db'}")
    create_all(eng)
    with session_scope(eng) as session:
        session.add(ArtifactClass(name="s-masters"))
    yield eng
    eng.dispose()


def test_build_placement_context_uses_cache_entry_grouping_and_two_selects(engine: Engine) -> None:
    current = _digest("current")
    ignored_lost = _digest("lost")
    with session_scope(engine) as session:
        _seed_disks(session, 4)
        _record_entry(
            session,
            _digest("bundle-a"),
            disk_id="d001",
            bundle_key="bundle-1",
            group_key="event-1",
        )
        _record_entry(
            session,
            _digest("bundle-b"),
            disk_id="d002",
            bundle_key="bundle-1",
            group_key="event-1",
        )
        _record_entry(session, _digest("event-only"), disk_id="d002", group_key="event-1")
        _record_entry(session, ignored_lost, disk_id="d003", group_key="event-1", state="lost")
        _record_entry(
            session,
            current,
            disk_id="d004",
            bundle_key="bundle-1",
            group_key="event-1",
        )

        statements: list[str] = []

        def _collect_select(
            _conn: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: object,
        ) -> None:
            if statement.lstrip().upper().startswith("SELECT"):
                statements.append(statement)

        event.listen(engine, "before_cursor_execute", _collect_select)
        try:
            ctx = build_placement_context(
                session,
                content_sha256=current,
                size_bytes=1024,
                artifactclass="s-masters",
                bundle_key="bundle-1",
                group_key="event-1",
            )
        finally:
            event.remove(engine, "before_cursor_execute", _collect_select)

    assert len(statements) == 2
    assert all("cache_entry" in statement for statement in statements)
    assert ctx.sibling_disks == frozenset({"d001", "d002"})
    assert dict(ctx.group_disk_counts) == {"d001": 1, "d002": 2}


def test_large_bundle_members_spread_when_candidates_are_sufficient(engine: Engine) -> None:
    policy = DefaultDiskPlacementPolicy(PlacementConfig(reserve_fraction=0))
    bundle_key = "bundle-large"
    group_key = "event-large"
    chosen: list[str] = []
    with session_scope(engine) as session:
        _seed_disks(session, 5, capacity_bytes=20 * DEFAULT_SPREAD_MIN_BYTES)
        for index in range(5):
            digest = _digest(f"large-{index}")
            disk_id = choose_placement(
                session,
                content_sha256=digest,
                size_bytes=DEFAULT_SPREAD_MIN_BYTES,
                artifactclass="s-masters",
                bundle_key=bundle_key,
                group_key=group_key,
                policy=policy,
            )
            chosen.append(disk_id)
            _record_entry(
                session,
                digest,
                disk_id=disk_id,
                size_bytes=DEFAULT_SPREAD_MIN_BYTES,
                bundle_key=bundle_key,
                group_key=group_key,
                state="filling",
            )
            session.flush()

    assert len(set(chosen)) == 5


def test_small_files_colocate_same_bundle_before_same_event(engine: Engine) -> None:
    policy = DefaultDiskPlacementPolicy(PlacementConfig(reserve_fraction=0))
    with session_scope(engine) as session:
        _seed_disks(session, 4)
        _record_entry(
            session,
            _digest("same-bundle-large"),
            disk_id="d001",
            size_bytes=DEFAULT_SPREAD_MIN_BYTES,
            bundle_key="bundle-1",
            group_key="event-1",
        )
        for index in range(3):
            _record_entry(
                session,
                _digest(f"other-bundle-small-{index}"),
                disk_id="d003",
                bundle_key="bundle-other",
                group_key="event-1",
            )

        first_small = choose_placement(
            session,
            content_sha256=_digest("same-bundle-small-1"),
            size_bytes=10 * 1024 * 1024,
            artifactclass="s-masters",
            bundle_key="bundle-1",
            group_key="event-1",
            policy=policy,
        )
        assert first_small == "d001"
        _record_entry(
            session,
            _digest("same-bundle-small-1"),
            disk_id=first_small,
            bundle_key="bundle-1",
            group_key="event-1",
        )
        session.flush()

        second_small = choose_placement(
            session,
            content_sha256=_digest("same-bundle-small-2"),
            size_bytes=10 * 1024 * 1024,
            artifactclass="s-masters",
            bundle_key="bundle-1",
            group_key="event-1",
            policy=policy,
        )
        assert second_small == first_small

        event_only = choose_placement(
            session,
            content_sha256=_digest("event-only-small"),
            size_bytes=10 * 1024 * 1024,
            artifactclass="s-masters",
            bundle_key=None,
            group_key="event-1",
            policy=policy,
        )
        assert event_only == "d003"


def test_balance_is_bounded_under_uniform_load() -> None:
    policy = DefaultDiskPlacementPolicy(PlacementConfig(spread_min_bytes=10**9, reserve_fraction=0))
    states = [
        DiskState(
            disk_id=f"d{index:03d}",
            state="active",
            capacity_bytes=1_000_000,
            filled_bytes=0,
        )
        for index in range(1, 9)
    ]
    counts: Counter[str] = Counter()
    for index in range(96):
        ctx = _ctx(_digest(f"uniform-{index}"), size_bytes=1, bundle_key=None, group_key=None)
        disk_id = policy.choose(states, ctx)
        counts[disk_id] += 1
        states = [
            replace(state, filled_bytes=state.filled_bytes + 1)
            if state.disk_id == disk_id
            else state
            for state in states
        ]

    assert max(counts.values()) - min(counts.values()) <= 1


def test_determinism_is_independent_of_candidate_order() -> None:
    policy = DefaultDiskPlacementPolicy(PlacementConfig(reserve_fraction=0))
    states = [
        DiskState("d001", "active", 10_000, 100),
        DiskState("d002", "active", 10_000, 100),
        DiskState("d003", "active", 10_000, 100),
    ]
    ctx = _ctx(_digest("deterministic"), size_bytes=1000, bundle_key="b", group_key="g")

    selected = policy.choose(states, ctx)

    assert selected == policy.choose(states, ctx)
    assert selected == policy.choose(list(reversed(states)), ctx)


def test_in_flight_accounting_prevents_concurrent_overshoot() -> None:
    policy = DefaultDiskPlacementPolicy(PlacementConfig(reserve_fraction=0))
    states = [
        DiskState(f"d{index:03d}", "active", capacity_bytes=600, filled_bytes=0)
        for index in range(1, 5)
    ]
    counts: Counter[str] = Counter()
    for index in range(24):
        ctx = _ctx(_digest(f"concurrent-{index}"), size_bytes=100)
        disk_id = policy.choose(states, ctx)
        counts[disk_id] += 1
        states = [
            replace(state, filling_bytes=state.filling_bytes + 100)
            if state.disk_id == disk_id
            else state
            for state in states
        ]

    assert counts == {"d001": 6, "d002": 6, "d003": 6, "d004": 6}
    assert all(state.committed_bytes == state.capacity_bytes for state in states)
    with pytest.raises(PlacementError):
        policy.choose(states, _ctx(_digest("one-too-many"), size_bytes=100))


def test_large_placement_degrades_without_failing_when_candidates_are_fewer_than_members() -> None:
    policy = DefaultDiskPlacementPolicy(PlacementConfig(reserve_fraction=0))
    states = [
        DiskState("d001", "active", 10 * DEFAULT_SPREAD_MIN_BYTES, 0),
        DiskState("d002", "active", 10 * DEFAULT_SPREAD_MIN_BYTES, 0),
    ]
    chosen: list[str] = []
    sibling_disks: frozenset[str] = frozenset()
    for index in range(5):
        ctx = _ctx(
            _digest(f"degrade-{index}"),
            size_bytes=DEFAULT_SPREAD_MIN_BYTES,
            bundle_key="bundle",
            group_key="event",
            sibling_disks=sibling_disks,
        )
        disk_id = policy.choose(states, ctx)
        chosen.append(disk_id)
        sibling_disks = frozenset({*sibling_disks, disk_id})
        states = [
            replace(state, filled_bytes=state.filled_bytes + 1)
            if state.disk_id == disk_id
            else state
            for state in states
        ]

    assert set(chosen) == {"d001", "d002"}
    assert len(chosen) == 5


def test_near_reserve_uses_only_viable_candidates() -> None:
    policy = DefaultDiskPlacementPolicy(
        PlacementConfig(
            reserve_largest_expected_file_bytes=15,
            reserve_tmp_headroom_bytes=5,
            reserve_fraction=0,
        )
    )
    states = [
        DiskState("d001", "active", capacity_bytes=1000, filled_bytes=971),
        DiskState("d002", "active", capacity_bytes=1000, filled_bytes=970),
        DiskState("d003", "retiring", capacity_bytes=1000, filled_bytes=0),
    ]

    assert policy.choose(states, _ctx(_digest("near-reserve"), size_bytes=10)) == "d002"


def test_capacity_over_reserve_disk_is_not_viable_even_with_free_bytes() -> None:
    policy = DefaultDiskPlacementPolicy(PlacementConfig(reserve_fraction=0))
    states = [
        DiskState(
            "d001",
            "active",
            capacity_bytes=1000,
            filled_bytes=0,
            capacity_state="over_reserve",
        ),
        DiskState("d002", "active", capacity_bytes=1000, filled_bytes=900),
    ]

    assert policy.choose(states, _ctx(_digest("over-reserve"), size_bytes=10)) == "d002"


def test_enclosure_spread_is_optional_and_off_by_default() -> None:
    states = [
        DiskState("d001", "active", 4 * DEFAULT_SPREAD_MIN_BYTES, 0, enclosure="shelf-a"),
        DiskState("d002", "active", 4 * DEFAULT_SPREAD_MIN_BYTES, 0, enclosure="shelf-a"),
        DiskState(
            "d003",
            "active",
            4 * DEFAULT_SPREAD_MIN_BYTES,
            2 * DEFAULT_SPREAD_MIN_BYTES,
            enclosure="shelf-b",
        ),
    ]
    ctx = _ctx(
        _digest("enclosure"),
        size_bytes=DEFAULT_SPREAD_MIN_BYTES,
        bundle_key="bundle",
        group_key="event",
        sibling_disks=frozenset({"d001"}),
    )

    assert (
        DefaultDiskPlacementPolicy(PlacementConfig(reserve_fraction=0)).choose(states, ctx)
        == "d002"
    )
    assert (
        DefaultDiskPlacementPolicy(
            PlacementConfig(reserve_fraction=0, enclosure_spread=True)
        ).choose(states, ctx)
        == "d003"
    )


def test_disk_states_from_catalog_trusts_cache_disk_filled_bytes(engine: Engine) -> None:
    with session_scope(engine) as session:
        _seed_disks(session, 2, capacity_bytes=1000, filled_bytes=100)
        _record_entry(
            session,
            _digest("present"),
            disk_id="d001",
            size_bytes=100,
            state="present",
        )
        _record_entry(
            session,
            _digest("filling"),
            disk_id="d001",
            size_bytes=200,
            state="filling",
        )
        states = {state.disk_id: state for state in disk_states_from_catalog(session)}

    assert states["d001"].filled_bytes == 100
    assert states["d001"].filling_bytes == 0
    assert states["d001"].free_bytes == 900
    assert states["d002"].free_bytes == 900


def test_placement_inv1_import_boundary_and_no_archival_rows(engine: Engine) -> None:
    import sutradhara.hdcache.placement as placement

    source = inspect.getsource(placement)
    assert "sutradhara.backend" not in source
    assert "sutradhara.replication" not in source
    assert "sutradhara.archive" not in source
    assert "ArtifactClassPool" not in source
    assert "Pool" not in source
    assert "Copy" not in source

    with session_scope(engine) as session:
        _seed_disks(session, 2)
        choose_placement(
            session,
            content_sha256=_digest("inv1"),
            size_bytes=1024,
            artifactclass="s-masters",
            bundle_key=None,
            group_key=None,
            policy=DefaultDiskPlacementPolicy(PlacementConfig(reserve_fraction=0)),
        )
        assert list(session.scalars(select(Backend))) == []
        assert list(session.scalars(select(Pool))) == []
        assert list(session.scalars(select(Copy))) == []


def _seed_disks(
    session: Session,
    count: int,
    *,
    capacity_bytes: int = 10_000_000_000,
    filled_bytes: int = 0,
) -> None:
    for index in range(1, count + 1):
        disk_id = f"d{index:03d}"
        session.add(
            CacheDisk(
                disk_id=disk_id,
                serial=f"SER{index:03d}",
                fs_uuid=f"fs-{index:03d}",
                mount=f"/srv/hdcache/{disk_id}",
                state="active",
                capacity_bytes=capacity_bytes,
                filled_bytes=filled_bytes,
                enclosure=f"shelf-{(index - 1) // 12}",
                slot=f"{index:02d}",
            )
        )


def _record_entry(
    session: Session,
    digest: bytes,
    *,
    disk_id: str,
    size_bytes: int = 1,
    state: str = "present",
    bundle_key: str | None = None,
    group_key: str | None = None,
) -> None:
    session.add(LogicalAsset(content_sha256=digest, size_bytes=size_bytes))
    session.add(
        CacheEntry(
            content_sha256=digest,
            artifactclass="s-masters",
            bundle_key=bundle_key,
            group_key=group_key,
            disk_id=disk_id,
            relpath=f"{digest.hex()[:2]}/{digest.hex()}",
            size_bytes=size_bytes,
            state=state,
            representation="raw-bytes",
            trusted=True,
        )
    )


def _ctx(
    digest: bytes,
    *,
    size_bytes: int,
    bundle_key: str | None = None,
    sibling_disks: frozenset[str] = frozenset(),
    group_key: str | None = None,
    group_disk_counts: dict[str, int] | None = None,
) -> PlacementContext:
    return PlacementContext(
        content_sha256=digest,
        size_bytes=size_bytes,
        artifactclass="s-masters",
        bundle_key=bundle_key,
        sibling_disks=sibling_disks,
        group_key=group_key,
        group_disk_counts=group_disk_counts or {},
    )


def _digest(label: str) -> bytes:
    return hashlib.sha256(label.encode("utf-8")).digest()
