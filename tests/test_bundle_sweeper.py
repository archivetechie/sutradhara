"""The sweeper, claim discipline, and the reaper. Every test names its defect.

``bundle_due`` had no production caller before this arc: the size arm fired
only incidentally during an enqueue and **the age arm ran nowhere at all**.
Several tests here are therefore the first execution of code that has been in
the tree, untested at a real call site, since the accumulator was written.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import socket
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from sutradhara.archive_bundle import (
    BundleClaimLost,
    BundleStateError,
    add_bundle_member,
    bundle_due,
    claim_bundle_for_flush,
    close_bundle,
    enqueue_artifact,
    get_or_create_open_bundle,
)
from sutradhara.archive_fanout import (
    LocalArchiveBuilder,
    flush_bundle,
)
from sutradhara.archive_sweeper import (
    VOID_STATUS,
    claim_is_live,
    drain_candidates,
    due_bundles,
    flush_if_due,
    live_group_fingerprints,
    reap_stuck_flushing,
    sweep_bundles,
    void_seal_orphans,
)
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    BundlingPolicy,
    DurabilityPolicy,
    PlacementPolicy,
    apply_artifactclass_policy,
    get_artifactclass_policy,
)
from sutradhara.backend.port import BackendLocator, ByteRange, CopyRecord, VerifyResult
from sutradhara.catalog.models import Backend, Bundle, Copy, LogicalAsset, Pool
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, content_hash
from sutradhara.jobs.attempts import default_worker_id
from sutradhara.jobs.models import ReconciliationCondition
from sutradhara.jobs.reconcilers.conditions import CONDITION_BLOCKED
from sutradhara.jobs.worker_lock import exclusive_process_lock, process_lockfile_for
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    # A file-backed database, not :memory:, because the reaper resolves the
    # worker lockfile from the engine URL and the two must agree.
    eng = make_engine(f"sqlite:///{tmp_path / 'sweeper.db'}")
    create_all(eng)
    yield eng
    eng.dispose()


def _digest(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


class _WriteBackend:
    """In-memory writable backend with a scriptable write failure.

    ``fail_pools`` fails the *write* (transient: nothing reaches media).
    ``bad_verify_pools`` lets the write succeed and fails the readback verify —
    the non-transient post-write failure, where bytes are on media and the
    check refuses them.
    """

    def __init__(
        self,
        name: str = "rem",
        *,
        fail_pools: tuple[str, ...] = (),
        bad_verify_pools: tuple[str, ...] = (),
    ) -> None:
        self._name = name
        self.fail_pools = set(fail_pools)
        self.bad_verify_pools = set(bad_verify_pools)
        self.objects: dict[str, bytes] = {}
        self.writes: list[str] = []
        self._counter = 0

    @property
    def name(self) -> str:
        return self._name

    def write_object_to_pool(
        self,
        source: Path | str,
        pool: str,
        *,
        caller_object_id: str | None = None,
    ) -> CopyRecord:
        if pool in self.fail_pools:
            raise OSError(f"configured write failure for {pool}")
        data = Path(source).read_bytes()
        digest = content_hash(hashlib.sha256(data).digest())
        self._counter += 1
        object_id = f"{self._name}-{self._counter}"
        self.objects[object_id] = data
        self.writes.append(pool)
        return CopyRecord(
            logical_id=digest,
            native_locator={
                "pool_id": pool,
                "object_id": object_id,
                "content_sha256": digest.hex(),
            },
            integrity_hash=digest,
            size_bytes=len(data),
        )

    def enumerate(self) -> Iterator[CopyRecord]:
        return iter(())

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        data = self.objects[str(locator["object_id"])]
        if byte_range.is_whole_object:
            return data
        return data[byte_range.start : byte_range.end]

    @contextmanager
    def open_range_chunks(
        self,
        locator: BackendLocator,
        byte_range: ByteRange,
        *,
        chunk_bytes: int,
    ) -> Iterator[Iterator[bytes]]:
        data = self.objects[str(locator["object_id"])]
        end = len(data) if byte_range.is_whole_object else byte_range.end

        def chunks() -> Iterator[bytes]:
            for cursor in range(byte_range.start, end, chunk_bytes):
                yield data[cursor : min(cursor + chunk_bytes, end)]

        yield chunks()

    def verify(self, locator: BackendLocator) -> VerifyResult:
        if str(locator["pool_id"]) in self.bad_verify_pools:
            return VerifyResult(ok=False, measured=False, detail="configured verify refusal")
        data = self.read_range(locator, ByteRange(0, 0))
        actual = content_hash(hashlib.sha256(data).digest())
        expected = content_hash(bytes.fromhex(str(locator["content_sha256"])))
        return VerifyResult(ok=actual == expected, measured=True, actual_hash=actual)


def _install_class(
    session: Session,
    artifactclass: str,
    *,
    pools: tuple[str, ...],
    target_bytes: int = 1_000_000,
    max_age_seconds: int = 3600,
) -> None:
    for pool_id in pools:
        if session.get(Pool, pool_id) is not None:
            continue
        backend = session.scalars(select(Backend).where(Backend.name == pool_id)).first()
        if backend is None:
            backend = Backend(
                name=pool_id,
                kind=BackendKind.MEMORY,
                tier=BackendTier.CATALOG_AUTHORITATIVE,
            )
            session.add(backend)
            session.flush()
        session.add(
            Pool(
                id=pool_id,
                backend_id=backend.id,
                representation=Representation.RAW_BYTES.value,
            )
        )
    session.flush()
    apply_artifactclass_policy(
        session,
        artifactclass,
        ArtifactClassPolicy(
            ruleset=f"{artifactclass}.rules",
            placements=tuple(PlacementPolicy(pool_id) for pool_id in pools),
            bundling=BundlingPolicy(
                target_gb=target_bytes / 1_000_000_000,
                max_age_seconds=max_age_seconds,
            ),
            restore_preference=pools,
            expect="messy",
            durability=DurabilityPolicy(min_copies=1, min_impl_families=1),
        ),
    )


def _backends(session: Session, backend: _WriteBackend) -> dict[int, Any]:
    return {row.id: backend for row in session.scalars(select(Backend))}


def _enqueue(
    session: Session,
    *,
    artifactclass: str,
    source: Path,
    member_path: str,
) -> Bundle:
    data = source.read_bytes()
    asset_hash = _digest(data)
    if session.get(LogicalAsset, asset_hash) is None:
        session.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(data)))
        session.flush()
    bundle, _, _ = enqueue_artifact(
        session,
        artifactclass=artifactclass,
        policy=get_artifactclass_policy(session, artifactclass),
        logical_asset_hash=asset_hash,
        source_path=source,
        member_path=member_path,
    )
    return bundle


def _dead_pid() -> int:
    """A pid that is certainly not running: a child we started and reaped.

    Not a made-up number: low pids are kernel threads (pid 2 is ``kthreadd``),
    so ``os.kill(2, 0)`` succeeds and the claim reads as live.
    """
    proc = subprocess.Popen(["true"])
    proc.wait()
    return proc.pid


def _source(tmp_path: Path, name: str, size: int = 64) -> Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(name.encode().ljust(size, b"."))
    return path


# --------------------------------------------------------------------------
# The due scan
# --------------------------------------------------------------------------


def test_sweeper_flushes_an_accumulator_that_is_due_by_size(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: the size arm firing only as an incidental side effect of an
    enqueue. Nothing appends after the accumulator crosses its target, so
    without a sweeper the bundle sits open until the next member arrives —
    which for a finished shoot is never."""
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=100)
        _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif", size=80),
            member_path="a.tif",
        )
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "b.tif", size=80),
            member_path="b.tif",
        )
        bundle_id = bundle.id
        assert bundle.status == "open"
        assert bundle.total_bytes >= bundle.target_bytes

        result = sweep_bundles(
            session,
            backends=_backends(session, _WriteBackend()),
            builder=LocalArchiveBuilder(),
        )
        assert result.flushed == (bundle_id,)
        assert session.get(Bundle, bundle_id).status == "sealed"


def test_sweeper_flushes_an_accumulator_that_is_due_only_by_age(
    engine: Engine, tmp_path: Path
) -> None:
    """The age arm's first execution at a real call site.

    Guards: a trickle class holding one open accumulator forever. The bundle
    is nowhere near its byte target — only its ``max_age_seconds`` has passed —
    and before the sweeper existed nothing in the tree would ever have looked.

    The sweep runs against an EXPIRED identity map on purpose. SQLite does not
    store the offset behind ``DateTime(timezone=True)``, so ``opened_at`` comes
    back naive only on a genuine re-read; a bundle still cached from the append
    hands ``_as_utc`` the aware value it was constructed with, and the
    naive-minus-aware subtraction that ``_as_utc`` exists to prevent never
    happens. That is the shape the production sweeper always sees.
    """
    with session_scope(engine) as session:
        _install_class(session, "audio", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="audio",
            source=_source(tmp_path, "take.wav"),
            member_path="take.wav",
        )
        bundle_id = bundle.id
        assert bundle.total_bytes < bundle.target_bytes
        assert bundle_due(bundle) is False
        overdue = bundle.opened_at + dt.timedelta(seconds=bundle.max_age_seconds + 1)

        session.expire_all()
        reread = session.get(Bundle, bundle_id)
        assert reread.opened_at.tzinfo is None, "the read-back must be naive to test _as_utc"

        assert bundle_due(reread, now=overdue) is True
        result = sweep_bundles(
            session,
            backends=_backends(session, _WriteBackend()),
            builder=LocalArchiveBuilder(),
            now=overdue,
        )
        assert result.flushed == (bundle_id,)
        assert session.get(Bundle, bundle_id).status == "sealed"


def test_due_scan_covers_open_funnel_bundles_not_only_accumulators(
    engine: Engine, tmp_path: Path
) -> None:
    """P1 gate condition C3.

    Guards: a due-scan filtered on ``archive_id IS NULL``. An include-alone
    member is minted into an immediately-due funnel bundle that carries
    ``archive_id`` from creation and has no flusher of its own — a sweeper
    that only looked at accumulators would strand every oversized member
    forever, which is exactly the material that most needs to reach tape.
    """
    with session_scope(engine) as session:
        _install_class(session, "video", pools=("p-main",), target_bytes=100)
        funnel = _enqueue(
            session,
            artifactclass="video",
            source=_source(tmp_path, "big.mov", size=500),
            member_path="big.mov",
        )
        funnel_id = funnel.id
        # Include-alone routing: a funnel, not the group accumulator.
        assert funnel.archive_id is not None
        assert funnel.status == "open"

        assert [bundle.id for bundle in due_bundles(session)] == [funnel_id]
        result = sweep_bundles(
            session,
            backends=_backends(session, _WriteBackend()),
            builder=LocalArchiveBuilder(),
        )
        assert result.flushed == (funnel_id,)
        assert session.get(Bundle, funnel_id).status == "sealed"


# --------------------------------------------------------------------------
# The agreement check: drain, no-drain, re-adopt, void
# --------------------------------------------------------------------------


def test_full_group_move_drains_the_orphaned_accumulator(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: an accumulator whose only class moved to a new pool set sitting
    open until its age arm happens to fire. Nothing will ever be appended to
    it again, so the drain rule seals it now."""
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id
        assert bundle_due(bundle) is False

        # The whole group moves: photos now places on a different pool.
        _install_class(session, "photos", pools=("p-other",), target_bytes=10_000_000)
        assert [candidate.id for candidate in drain_candidates(session)] == [bundle_id]

        result = sweep_bundles(
            session,
            backends=_backends(session, _WriteBackend()),
            builder=LocalArchiveBuilder(),
        )
        assert result.drained == (bundle_id,)
        assert result.flushed == (bundle_id,)
        assert session.get(Bundle, bundle_id).status == "sealed"


def test_partial_group_move_does_not_drain_the_accumulator(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: reading "a class left the group" as "the group is dead".

    Two classes share a group. One leaves. The fingerprint is still the other
    class's live fingerprint, so the accumulator is not orphaned — draining it
    here would seal the staying class's material short for a policy edit that
    had nothing to do with it.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        _install_class(session, "audio", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id

        _install_class(session, "photos", pools=("p-other",), target_bytes=10_000_000)
        assert drain_candidates(session) == []

        result = sweep_bundles(
            session,
            backends=_backends(session, _WriteBackend()),
            builder=LocalArchiveBuilder(),
        )
        assert result.drained == ()
        assert result.flushed == ()
        assert session.get(Bundle, bundle_id).status == "open"

        # And the departed class's next member routes to its NEW group, while
        # the old accumulator carries on for the class that stayed.
        moved = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "b.tif"),
            member_path="b.tif",
        )
        assert moved.id != bundle_id
        stayed = _enqueue(
            session,
            artifactclass="audio",
            source=_source(tmp_path, "take.wav"),
            member_path="take.wav",
        )
        assert stayed.id == bundle_id


def test_a_reverted_policy_re_adopts_the_still_open_accumulator(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: sealing on the way out and opening a second accumulator on the
    way back. A revert restores the fingerprint, the still-open accumulator is
    adopted again, and no short object was ever minted by the round trip."""
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id
        original_group = bundle.bundle_group

        _install_class(session, "photos", pools=("p-other",), target_bytes=10_000_000)
        assert original_group not in live_group_fingerprints(session)

        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        assert original_group in live_group_fingerprints(session)
        assert drain_candidates(session) == []

        readopted = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "b.tif"),
            member_path="b.tif",
        )
        assert readopted.id == bundle_id
        assert readopted.member_count == 2


def test_empty_orphan_accumulator_is_void_sealed_not_left_forever(
    engine: Engine,
) -> None:
    """Guards: an empty orphan lingering.

    ``bundle_due`` refuses empty bundles by design, so an accumulator that was
    opened and never filled — and whose group no live policy derives any more —
    can never reach the flush path. It is sealed ``void`` directly instead.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle, _ = get_or_create_open_bundle(
            session,
            artifactclass="photos",
            policy=get_artifactclass_policy(session, "photos"),
        )
        bundle_id = bundle.id
        assert bundle.member_count == 0
        assert bundle_due(bundle, now=dt.datetime.now(dt.UTC) + dt.timedelta(days=365)) is False

        # Still live: not an orphan, so not voided.
        assert void_seal_orphans(session) == []

        _install_class(session, "photos", pools=("p-other",), target_bytes=10_000_000)
        assert void_seal_orphans(session) == [bundle_id]
        assert session.get(Bundle, bundle_id).status == VOID_STATUS


def test_empty_open_funnel_is_not_void_sealed(engine: Engine) -> None:
    """Guards: voiding a cloud-blob funnel that is waiting for its wrap.

    An empty *funnel* is a destination a job is about to fill; voiding it would
    delete that destination out from under the job. Only accumulators
    (``archive_id IS NULL``) are void candidates.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle, _ = get_or_create_open_bundle(
            session,
            artifactclass="photos",
            policy=get_artifactclass_policy(session, "photos"),
        )
        bundle.archive_id = f"archive-{bundle.id}"
        session.flush()
        _install_class(session, "photos", pools=("p-other",), target_bytes=10_000_000)
        assert void_seal_orphans(session) == []
        assert session.get(Bundle, bundle.id).status == "open"


# --------------------------------------------------------------------------
# Claim discipline
# --------------------------------------------------------------------------


def test_claim_is_taken_before_the_member_snapshot(engine: Engine, tmp_path: Path) -> None:
    """Guards: the round-3 defect — a claim taken *after* the member load.

    An appender that lands a member between the snapshot and the seal would
    have its member recorded in a sealed bundle that was built without it:
    catalog says archived, media does not have it. With the claim first, the
    bundle is ``flushing`` before any member is read and ``add_bundle_member``'s
    open-status check refuses the append outright.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        token = claim_bundle_for_flush(session, bundle)
        assert token == default_worker_id()
        assert bundle.status == "flushing"
        assert bundle.claimed_by == token

        # The appender cannot slip a member into the sealing bundle: a direct
        # add is refused outright...
        with pytest.raises(BundleStateError, match="is not open"):
            add_bundle_member(
                session,
                bundle=bundle,
                artifactclass="photos",
                logical_asset_hash=bundle.members[0].logical_asset_hash,
                member_path="late.tif",
                size_bytes=1,
                file_sha256=bundle.members[0].file_sha256,
            )
        # ...and the ordinary enqueue path routes to a fresh accumulator
        # instead, leaving the claimed member snapshot exactly as it was.
        late = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "late.tif"),
            member_path="late.tif",
        )
        assert late.id != bundle.id
        assert session.get(Bundle, bundle.id).member_count == 1


def test_second_claim_on_a_claimed_bundle_loses(engine: Engine, tmp_path: Path) -> None:
    """Guards: two flushers building the same member set into two objects.

    SQLite is the default dialect, where ``FOR UPDATE`` compiles to nothing, so
    the guard has to be rowcount on a conditional UPDATE, not a row lock.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        claim_bundle_for_flush(session, bundle)
        with pytest.raises(BundleClaimLost, match="not 'open'"):
            claim_bundle_for_flush(session, bundle, worker_id="otherhost:999")


def test_pre_write_failure_rolls_back_to_open_and_is_due_again(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: a pre-physical-write failure leaving the bundle stuck ``flushing``.

    Everything up to the first ``write_object_to_pool`` is one transaction, so
    the rollback IS the un-claim: the bundle comes back ``open``, un-claimed,
    and visible to ``bundle_due`` again. Nothing was written, so nothing is
    orphaned.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id
        source = Path(bundle.members[0].source_path)

    # A source that vanished between enqueue and flush fails the build. The
    # exception type is the builder's (OSError here); what this test pins is
    # that ANY pre-physical-write failure un-claims via the rollback.
    source.unlink()
    backend = _WriteBackend()
    with pytest.raises(OSError, match=r"a\.tif"), session_scope(engine) as session:
        flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )

    assert backend.writes == []
    with session_scope(engine) as session:
        bundle = session.get(Bundle, bundle_id)
        assert bundle.status == "open"
        assert bundle.claimed_by is None
        assert [candidate.id for candidate in due_bundles(session)] == []
        assert bundle_due(bundle, force=True) is True


def test_post_write_failure_seals_partial_with_the_earlier_copy_recorded(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: rolling back after a successful physical write.

    A tape append is unreclaimable and costs a bootstrap row, so once bytes
    are on media the flush seals partial rather than un-writing them. The
    earlier target's copy row must survive the later target's failure.
    """
    with session_scope(engine) as session:
        _install_class(
            session,
            "photos",
            pools=("p-aaa", "p-zzz"),
            target_bytes=10_000_000,
        )
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id

    backend = _WriteBackend(fail_pools=("p-zzz",))
    with session_scope(engine) as session:
        result = flush_bundle(
            session,
            bundle_id=bundle_id,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        assert result.partial is True
        assert result.failed_pools == ("p-zzz",)

    assert backend.writes == ["p-aaa"]
    with session_scope(engine) as session:
        bundle = session.get(Bundle, bundle_id)
        assert bundle.status == "sealed"
        copies = list(session.scalars(select(Copy).where(Copy.bundle_id == bundle_id)))
        assert [copy.pool_id for copy in copies] == ["p-aaa"]


def test_a_failing_bundle_in_a_sweep_is_un_claimed_and_the_pass_carries_on(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: the sweep committing a failed flush's partial state.

    ``sweep_bundles`` records a per-bundle failure and continues, and the
    caller (``session_scope``) then COMMITS. Without a per-bundle savepoint
    that commit keeps the failed flush's claim — ``status='flushing'`` plus
    ``claimed_by``, the ``archive_id`` mint and the ``scan_summary`` — and
    under the job worker the bundle is then unrecoverable: ``claimed_by`` names
    the worker's own live pid, so the reaper reads the claim as live forever
    and ``bundle_due`` never sees a ``flushing`` bundle. The material silently
    never reaches media.

    Two groups, one healthy and one whose source vanished after enqueue: the
    healthy one must seal, the broken one must come back ``open`` and
    un-claimed, and ``SweepResult.failed`` must name it.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=100)
        _install_class(session, "audio", pools=("p-other",), target_bytes=100)
        # Two members each, under the target individually: real accumulators,
        # so `archive_id` is minted by the flush and must not outlive it.
        for name in ("a.tif", "b.tif"):
            healthy = _enqueue(
                session,
                artifactclass="photos",
                source=_source(tmp_path, name, size=60),
                member_path=name,
            )
        for name in ("take.wav", "take2.wav"):
            broken = _enqueue(
                session,
                artifactclass="audio",
                source=_source(tmp_path, name, size=60),
                member_path=name,
            )
        healthy_id, broken_id = healthy.id, broken.id
        assert healthy_id != broken_id
        assert broken.archive_id is None
        vanishing = Path(broken.members[0].source_path)

    vanishing.unlink()
    backend = _WriteBackend()
    with session_scope(engine) as session:
        result = sweep_bundles(
            session,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        assert result.flushed == (healthy_id,)
        assert [bundle_id for bundle_id, _ in result.failed] == [broken_id]

    # After the commit: this is where a missing savepoint shows.
    assert backend.writes == ["p-main"]
    with session_scope(engine) as session:
        assert session.get(Bundle, healthy_id).status == "sealed"
        stranded = session.get(Bundle, broken_id)
        assert stranded.status == "open"
        assert stranded.claimed_by is None
        assert stranded.archive_id is None
        assert stranded.scan_summary is None
        # Visible to the next pass again, which is the whole point.
        assert [candidate.id for candidate in due_bundles(session)] == [broken_id]


def test_post_write_verify_failure_seals_partial_through_the_sweep(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: a NON-transient post-write failure unwinding real copies.

    Only ``TransientPoolFanoutError`` used to be caught in the write loop, so a
    hard post-write refusal — bytes on media, readback verify says no —
    propagated out of ``flush_bundle``. Under the per-bundle savepoint above,
    that propagation would roll the sibling target's committed copy back too:
    the design's "post-write failures seal partial rather than roll back" would
    hold for retryable failures only, and a later re-flush would append a
    second unreclaimable object for the copy that was silently discarded.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-aaa", "p-zzz"), target_bytes=100)
        for name in ("a.tif", "b.tif"):
            bundle = _enqueue(
                session,
                artifactclass="photos",
                source=_source(tmp_path, name, size=60),
                member_path=name,
            )
        bundle_id = bundle.id

    backend = _WriteBackend(bad_verify_pools=("p-zzz",))
    with session_scope(engine) as session:
        result = sweep_bundles(
            session,
            backends=_backends(session, backend),
            builder=LocalArchiveBuilder(),
        )
        # The pass reports it as flushed, not failed: the bundle sealed.
        assert result.flushed == (bundle_id,)
        assert result.failed == []

    assert backend.writes == ["p-aaa", "p-zzz"]
    with session_scope(engine) as session:
        assert session.get(Bundle, bundle_id).status == "sealed"
        copies = {
            copy.pool_id: copy.health
            for copy in session.scalars(select(Copy).where(Copy.bundle_id == bundle_id))
        }
        # The good placement survives...
        assert copies["p-aaa"] == CopyHealth.OK
        # ...and the refused one is recorded SUSPECT rather than erased: its
        # bytes are on media and a discarded row would let a repair append a
        # second object to the same pool.
        assert copies["p-zzz"] == CopyHealth.SUSPECT
        # And it alarms: blocked, not backed off — an automatic retry would
        # append rather than replace.
        condition = session.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == "bundle_copy",
                ReconciliationCondition.target_key == bundle_id,
            )
        ).one()
        assert condition.condition == CONDITION_BLOCKED
        assert condition.reason == "post-write-pool-failure"


def test_hold_written_during_a_failing_enqueue_survives_the_rollback(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guards: the CLI defect round 4 found — a held bundle discarded by the
    surrounding rollback.

    ``stage_and_enqueue_artifact`` writes ``held`` on the bundle and then
    re-raises. A caller that raises out of its session scope rolls that write
    back with everything else, so the operator is told to review a bundle that
    is not held. The enqueue-intake CLI now carries the summary out of the
    scope and raises after the commit.
    """
    from click.testing import CliRunner

    from sutradhara.cli.main import cli
    from sutradhara.staging import StagingHeld

    db = tmp_path / "hold.db"
    engine = make_engine(f"sqlite:///{db}")
    create_all(engine)
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle, _ = get_or_create_open_bundle(
            session,
            artifactclass="photos",
            policy=get_artifactclass_policy(session, "photos"),
        )
        bundle_id = bundle.id

    def _held(*_args: Any, **_kwargs: Any) -> Any:
        from sutradhara.archive_bundle import hold_bundle

        with session_scope(engine) as session:
            hold_bundle(session, session.get(Bundle, bundle_id), summary={"why": "test"})
        raise StagingHeld({"why": "test"})

    monkeypatch.setattr("sutradhara.cli.archive.enqueue_intake_batch", _held)
    result = CliRunner().invoke(
        cli,
        ["archive", "bundle", "enqueue-intake", "intake-x"],
        env={"SUTRADHARA_DB_URL": f"sqlite:///{db}"},
    )
    assert result.exit_code == 1
    assert "why" in result.output
    with session_scope(engine) as session:
        assert session.get(Bundle, bundle_id).status == "held"
    engine.dispose()


# --------------------------------------------------------------------------
# The reaper
# --------------------------------------------------------------------------


def test_stuck_flushing_bundle_with_a_dead_claimer_is_reaped(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: a bundle stuck ``flushing`` forever after its flusher died.

    Nothing else can claim it, ``bundle_due`` ignores it (it is not ``open``),
    and the group's material never reaches media. The reaper returns it to
    ``open`` with an alarm — keeping ``archive_id``, so it is non-adoptable and
    cannot collide with the fresh accumulator that opened meanwhile.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id
        # A claimer on this host whose pid is long gone.
        claim_bundle_for_flush(session, bundle, worker_id=f"{socket.gethostname()}:{_dead_pid()}")
        bundle.archive_id = f"archive-{bundle_id}"
        session.flush()

        # A fresh accumulator opened while the first one was flushing.
        fresh = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "b.tif"),
            member_path="b.tif",
        )
        assert fresh.id != bundle_id

        assert reap_stuck_flushing(session) == [bundle_id]
        reaped = session.get(Bundle, bundle_id)
        assert reaped.status == "open"
        assert reaped.claimed_by is None
        # Non-adoptable: it keeps archive_id, so it does not collide with the
        # fresh accumulator on the one-open-accumulator partial unique index.
        assert reaped.archive_id is not None
        assert bundle_due(reaped, force=True) is True


def test_stuck_flushing_bundle_with_a_live_claimer_is_not_reaped(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: reaping a flush that is still running — the worst outcome of
    all, because the returning flusher and the new one both build, and the
    tape pays for both. Liveness is checked against the worker-lock holder."""
    lockfile = process_lockfile_for(engine, namespace="worker")
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id
        token = f"{socket.gethostname()}:{os.getpid()}"
        claim_bundle_for_flush(session, bundle, worker_id=token)

        with exclusive_process_lock(lockfile, purpose="worker"):
            assert claim_is_live(session, token) is True
            assert reap_stuck_flushing(session) == []
        assert session.get(Bundle, bundle_id).status == "flushing"


def test_claim_on_a_foreign_host_without_the_worker_lock_is_not_live(
    engine: Engine,
) -> None:
    """Guards: a same-pid coincidence on another host reading as live. The
    process probe only applies to claims from this host; a foreign-host claim
    is live only while it holds the worker lock."""
    with session_scope(engine) as session:
        assert claim_is_live(session, f"not-this-host:{os.getpid()}") is False
        assert claim_is_live(session, None) is False


def test_a_reaped_drain_flush_is_still_reachable_without_an_age_arm(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: a reaped bundle that no rule can reach any more.

    The reaper keeps ``archive_id`` on purpose — the bundle must stay
    non-adoptable so it cannot collide with the accumulator that opened while
    it sat ``flushing``. But that same ``archive_id`` disqualifies it from the
    drain rule, and a bundle the DRAIN rule flushed is short by construction:
    under its byte target, and with ``max_age_seconds`` at its default 0 there
    is no age arm either. ``bundle_due`` therefore says False forever, nothing
    else looks, and nothing alarms. The material silently never reaches media.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        bundle_id = bundle.id

        # The whole group moves away: the drain rule is what flushes this one.
        _install_class(session, "photos", pools=("p-other",), target_bytes=10_000_000)
        assert [candidate.id for candidate in drain_candidates(session)] == [bundle_id]

        # A drain flush that died after claiming: `flush_bundle` mints
        # `archive_id` right after the claim.
        claim_bundle_for_flush(
            session, bundle, worker_id=f"{socket.gethostname()}:{_dead_pid()}"
        )
        bundle.archive_id = f"archive-{bundle_id}"
        session.flush()
        assert reap_stuck_flushing(session) == [bundle_id]

        reaped = session.get(Bundle, bundle_id)
        assert reaped.status == "open"
        assert reaped.archive_id is not None
        # An accumulator always carries a positive age arm (zero thresholds are
        # refused at open), so this one would eventually flush late — the drain
        # rule exists precisely to say that waiting buys nothing. A funnel does
        # not even get that: `jobs/handlers/cloud_blob.py` mints its funnels
        # with `max_age_seconds=0`, and one member quarantined out of a failing
        # flush leaves it under its own target. Set both here, which is the
        # state a reaped cloud-blob funnel is genuinely in.
        reaped.max_age_seconds = 0
        session.flush()
        assert reaped.total_bytes < reaped.target_bytes
        # Neither arm of `bundle_due` can ever fire for it...
        assert bundle_due(reaped) is False
        assert bundle_due(reaped, now=reaped.opened_at + dt.timedelta(days=3650)) is False
        # ...and the drain rule refuses it, because it is no longer adoptable.
        assert drain_candidates(session) == []

        # The sweep reaches it anyway: it was already judged flush-worthy when
        # it was claimed, and it can never grow.
        assert [candidate.id for candidate in due_bundles(session)] == [bundle_id]
        result = sweep_bundles(
            session,
            backends=_backends(session, _WriteBackend()),
            builder=LocalArchiveBuilder(),
            reap=False,
        )
        assert result.flushed == (bundle_id,)
        assert session.get(Bundle, bundle_id).status == "sealed"


def test_reaped_then_returning_flusher_fails_the_close_cas_loudly(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: the returning flusher sealing a member set that is not on media.

    While it was presumed dead the bundle went back to ``open`` and may have
    been re-claimed and rebuilt with a different member set. Sealing on the
    stale claim would record *this* flusher's member list against whatever is
    actually on media. The CAS on ``status='flushing' AND claimed_by=:token``
    refuses instead.
    """
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=10_000_000)
        bundle = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif"),
            member_path="a.tif",
        )
        token = claim_bundle_for_flush(session, bundle, worker_id=f"{socket.gethostname()}:{_dead_pid()}")
        assert reap_stuck_flushing(session) == [bundle.id]

        with pytest.raises(BundleClaimLost, match="lost its flush claim"):
            close_bundle(session, bundle, claim_token=token)
        assert session.get(Bundle, bundle.id).status == "open"


# --------------------------------------------------------------------------
# The post-append check
# --------------------------------------------------------------------------


def test_post_append_check_flushes_the_member_that_crossed_the_target(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: latency. The periodic pass guarantees the seal eventually; the
    post-append check takes the case the appender can already see."""
    with session_scope(engine) as session:
        _install_class(session, "photos", pools=("p-main",), target_bytes=100)
        first = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "a.tif", size=40),
            member_path="a.tif",
        )
        backend = _WriteBackend()
        assert (
            flush_if_due(
                session,
                first,
                backends=_backends(session, backend),
                builder=LocalArchiveBuilder(),
            )
            is False
        )
        second = _enqueue(
            session,
            artifactclass="photos",
            source=_source(tmp_path, "b.tif", size=80),
            member_path="b.tif",
        )
        assert second.id == first.id
        assert (
            flush_if_due(
                session,
                second,
                backends=_backends(session, backend),
                builder=LocalArchiveBuilder(),
            )
            is True
        )
        assert session.get(Bundle, first.id).status == "sealed"
