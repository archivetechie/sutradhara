"""The bundle sweeper: due flushes, the agreement check, and the claim reaper.

``bundle_due`` had no production caller before this module — the size arm fired
only because a fat member happened to push the accumulator over its target
during an enqueue, and **the age arm ran nowhere at all**. A trickle class
could therefore hold an open accumulator forever. The sweeper is the missing
caller, and it runs three things in one pass:

1. **The reaper.** A ``flushing`` bundle whose claimer is gone goes back to
   ``open`` with an alarm. Liveness is a real mechanism, not a timeout: the
   claim carries the flusher's ``hostname:pid`` and the reaper checks it
   against the worker-lock holder (and, on this host, against the process
   itself, so an operator's foreground CLI flush is never reaped out from
   under itself).
2. **The agreement check.** An accumulator drains only when **no live class's
   policy derives its fingerprint**. A partial edit — class A leaves the group,
   class B stays — is *not* a drain: the fingerprint is still B's live
   fingerprint, A's next member simply routes to its new group, and the
   accumulator carries on. A policy that reverts re-adopts a still-open
   accumulator, which is fine and intended. An **empty** orphan is sealed
   ``void`` directly, because ``bundle_due`` refuses empty bundles by design
   and would otherwise strand it forever.
3. **The due scan.** Every open bundle that ``bundle_due`` says is due gets
   flushed through the map route.

The due scan covers **open funnel bundles too** (``archive_id IS NOT NULL``),
not just accumulators. Include-alone bundles are minted immediately-due funnels
with no flusher of their own; a sweeper that scanned only accumulators would
strand every oversized member forever. That is P1 gate condition C3 and it is
the reason ``_open_bundles`` filters on nothing but ``status``.

**Deferred, stated:** §4 also names a post-append check — flush the bundle the
appender can already see is due, as a latency shortcut. It is not here. The
enqueue seam (``archive_enqueue.scan_enqueue_batch``) is reached by the CLI and
by the intake-archive API route, and neither has, or should have, a writable
backend set and an archive builder to hand: wiring it there would make an HTTP
request write to tape synchronously, inside the enqueue's own transaction,
where a flush failure would roll the whole enqueue batch back. The periodic
pass is what guarantees the seal; the shortcut only ever bought latency, and it
belongs behind a job enqueue rather than inside the appender's transaction.
"""

from __future__ import annotations

import datetime as dt
import os
import socket
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from sutradhara.archive_bundle import bundle_due
from sutradhara.archive_fanout import ArchiveBuilder, flush_bundle
from sutradhara.bundle_group import compute_group_basis, fingerprint_basis
from sutradhara.catalog.models import ArtifactClassPolicyRecord, Bundle
from sutradhara.jobs.worker_lock import held_process_lock_identity, process_lockfile_for
from sutradhara.replication import WritableStorageBackend
from sutradhara.structured_logs import emit_structured_event

VOID_STATUS = "void"


@dataclass
class SweepResult:
    """What one sweeper pass did, in the order it did it."""

    reaped: tuple[str, ...] = ()
    voided: tuple[str, ...] = ()
    drained: tuple[str, ...] = ()
    flushed: tuple[str, ...] = ()
    failed: list[tuple[str, str]] = field(default_factory=list)

    @property
    def acted(self) -> int:
        return len(self.reaped) + len(self.voided) + len(self.flushed)


def live_group_fingerprints(session: Session) -> set[str]:
    """Return every fingerprint some live class's policy derives right now.

    Derived from ``artifactclass_pool`` + ``pool`` rather than read off the
    ``artifactclass_policy.bundle_group`` projection: this pass is the
    projection's drift detector (§3), so trusting the projection here would
    make the check circular. Drift is reported, never silently repaired — a
    projection writer that stopped writing is a defect the operator should see.
    """
    live: set[str] = set()
    for record in session.scalars(select(ArtifactClassPolicyRecord)):
        derived = fingerprint_basis(compute_group_basis(session, record.artifactclass))
        live.add(derived)
        if record.bundle_group != derived:
            emit_structured_event(
                "bundle_group_projection_drift",
                artifactclass=record.artifactclass,
                projected=record.bundle_group,
                derived=derived,
            )
    return live


def _open_bundles(session: Session) -> list[Bundle]:
    # No archive_id filter, deliberately: C3 (P1). Funnel bundles
    # (include-alone, cloud-blob) sit ``open`` with archive_id set and have no
    # flusher of their own.
    return list(
        session.scalars(
            select(Bundle).where(Bundle.status == "open").order_by(Bundle.opened_at, Bundle.id)
        )
    )


def void_seal_orphans(
    session: Session,
    *,
    live: set[str] | None = None,
    now: dt.datetime | None = None,
) -> list[str]:
    """Seal empty orphan accumulators with the terminal ``void`` status.

    Only accumulators (``archive_id IS NULL``). An empty *funnel* is a bundle
    waiting for its content — a cloud-blob funnel before its wrap lands — and
    voiding it would delete the destination out from under the wrap job.
    """
    fingerprints = live_group_fingerprints(session) if live is None else live
    stamp = now or dt.datetime.now(dt.UTC)
    voided: list[str] = []
    for bundle in _open_bundles(session):
        if bundle.archive_id is not None or bundle.member_count != 0:
            continue
        if bundle.bundle_group in fingerprints:
            continue
        result = session.execute(
            update(Bundle)
            .where(Bundle.id == bundle.id, Bundle.status == "open")
            .values(status=VOID_STATUS, sealed_at=stamp)
        )
        if result.rowcount != 1:  # pragma: no cover - lost to a concurrent claim
            continue
        session.expire(bundle, ["status", "sealed_at"])
        emit_structured_event(
            "bundle_void_sealed",
            bundle_id=bundle.id,
            bundle_group=bundle.bundle_group,
            reason="no-live-class-derives-fingerprint",
        )
        voided.append(bundle.id)
    return voided


def drain_candidates(session: Session, *, live: set[str] | None = None) -> list[Bundle]:
    """Return non-empty accumulators no live class's policy derives any more.

    These seal on the drain rule instead of waiting out their age arm: nothing
    will ever be appended to them again, so waiting buys nothing.
    """
    fingerprints = live_group_fingerprints(session) if live is None else live
    return [
        bundle
        for bundle in _open_bundles(session)
        if bundle.archive_id is None
        and bundle.member_count > 0
        and bundle.bundle_group not in fingerprints
    ]


def was_reaped(bundle: Bundle) -> bool:
    """Return whether the reaper is what put this bundle back to ``open``.

    ``flushed_at`` is written by ``claim_bundle_for_flush`` and by nothing else,
    and the reaper is the only thing that clears ``claimed_by`` while leaving
    the bundle ``open``. So an open bundle with a flush stamp and no claimer was
    claimed once and returned — and it kept ``archive_id``, which makes it
    non-adoptable: no member can ever be appended to it again.
    """
    return (
        bundle.status == "open"
        and bundle.archive_id is not None
        and bundle.flushed_at is not None
        and bundle.claimed_by is None
        and bundle.member_count > 0
    )


def due_bundles(session: Session, *, now: dt.datetime | None = None) -> list[Bundle]:
    """Return every open bundle that should flush — funnels and reaped ones included.

    A reaped bundle is due *because it was reaped*, without consulting
    ``bundle_due`` again. It was already judged flush-worthy when it was
    claimed, and it cannot grow: the reaper deliberately keeps ``archive_id``
    so the bundle stays non-adoptable and cannot collide with the fresh
    accumulator that opened while it sat ``flushing``. Re-asking ``bundle_due``
    therefore asks the wrong question, and answers it wrongly in the ordinary
    case: a short accumulator sealed by the DRAIN rule is under its byte target
    by construction, and ``max_age_seconds`` defaults to 0 (no age arm), so
    ``bundle_due`` returns False forever. Nothing else would ever look at it and
    nothing would alarm — the material would simply never reach media.
    """
    return [
        bundle
        for bundle in _open_bundles(session)
        if was_reaped(bundle) or bundle_due(bundle, now=now)
    ]


def claim_is_live(session: Session, token: str | None) -> bool:
    """Return whether a bundle's flush claim still has a process behind it.

    Two conjunct checks, both conservative (an unsure answer means "live", so
    the reaper leaves the bundle alone and a human sees a stuck flush rather
    than a duplicated one):

    - the worker-lock holder identity, which is the design's named mechanism
      and covers the single-node worker; and
    - a same-host pid probe, which the worker lock alone does not cover: a
      foreground ``sutra archive bundle flush`` claims under its **own**
      ``hostname:pid`` and never holds the worker lock, so the lock check by
      itself would reap a live operator flush mid-write.
    """
    if not token:
        # No claim identity at all: nothing to be alive. (Only reachable for
        # rows that entered `flushing` before the claim column existed.)
        return False
    lockfile = process_lockfile_for(session.get_bind(), namespace="worker")
    if held_process_lock_identity(lockfile) == token:
        return True
    host, _, raw_pid = token.rpartition(":")
    if host != socket.gethostname():
        return False
    try:
        pid = int(raw_pid)
    except ValueError:
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Alive, owned by someone else.
        return True
    return True


def reap_stuck_flushing(session: Session) -> list[str]:
    """Return stuck ``flushing`` bundles to ``open``, with an alarm each.

    The reaped bundle keeps its ``archive_id``. That is load-bearing, not
    laziness: while it sat ``flushing`` a fresh accumulator may have opened
    under the same fingerprint, and re-opening this one as an *adoptable*
    accumulator (``archive_id IS NULL``) would collide with the
    one-open-accumulator partial unique index. Kept non-adoptable it stops
    taking new members, stays visible to ``bundle_due``, and flushes on its
    own — which is exactly the wanted outcome.

    Its old claim token is cleared, so a flusher that comes back from the dead
    fails the ``close_bundle`` compare-and-set loudly instead of sealing a
    member set that may no longer be the one it built.
    """
    reaped: list[str] = []
    stuck = list(
        session.scalars(select(Bundle).where(Bundle.status == "flushing").order_by(Bundle.id))
    )
    for bundle in stuck:
        token = bundle.claimed_by
        if claim_is_live(session, token):
            # Emitted, not passed over in silence: a claim that reads as live
            # is indistinguishable from a claim that IS live, and the two have
            # opposite meanings. A flush in progress produces one of these per
            # pass and stops; a bundle stuck behind a claim the liveness check
            # cannot retire produces one every pass, forever, which is the only
            # signal that says so.
            emit_structured_event(
                "bundle_flush_claim_live",
                bundle_id=bundle.id,
                bundle_group=bundle.bundle_group,
                claimed_by=token,
                flushed_at=None if bundle.flushed_at is None else bundle.flushed_at.isoformat(),
            )
            continue
        result = session.execute(
            update(Bundle)
            .where(Bundle.id == bundle.id, Bundle.status == "flushing")
            .values(status="open", claimed_by=None)
        )
        if result.rowcount != 1:  # pragma: no cover - lost to a concurrent seal
            continue
        session.expire(bundle, ["status", "claimed_by"])
        emit_structured_event(
            "bundle_flush_claim_reaped",
            bundle_id=bundle.id,
            bundle_group=bundle.bundle_group,
            claimed_by=token,
            flushed_at=None if bundle.flushed_at is None else bundle.flushed_at.isoformat(),
        )
        reaped.append(bundle.id)
    return reaped


def sweep_bundles(
    session: Session,
    *,
    backends: Mapping[int, WritableStorageBackend],
    builder: ArchiveBuilder,
    key_epoch: str | None = None,
    deliverables_dir: Path | str | None = None,
    now: dt.datetime | None = None,
    reap: bool = True,
) -> SweepResult:
    """Run one full sweeper pass: reap, void-seal, drain, then flush what is due.

    Order matters. Reaping first returns stuck bundles to ``open`` so the same
    pass can re-flush them. Void-sealing before the due scan keeps empty
    orphans out of a scan that would refuse them anyway. Draining marks the
    orphaned accumulators that must seal now rather than wait out an age arm
    nothing will ever restart.

    A flush that fails does not abort the pass — one bad bundle must not stop
    every other group from sealing — but the failure is recorded and returned,
    and **it rolls back to its own savepoint**. Without that savepoint the pass
    would carry the failed flush's partial state to the caller's commit: the
    claim (``status='flushing'`` plus ``claimed_by``), the ``archive_id`` mint,
    the ``scan_summary`` and any quarantine rows the attempt created. Under the
    job worker that committed claim is unrecoverable — ``claimed_by`` names the
    worker's own still-running pid, so the reaper's liveness check says "live"
    forever, ``_open_bundles`` never sees a ``flushing`` bundle again, and the
    material silently never reaches media. The savepoint rollback IS the
    un-claim, which is the design's own rule (§4) applied to the batch caller.
    """
    result = SweepResult()
    if reap:
        result.reaped = tuple(reap_stuck_flushing(session))
    live = live_group_fingerprints(session)
    result.voided = tuple(void_seal_orphans(session, live=live, now=now))

    drained = {bundle.id: bundle for bundle in drain_candidates(session, live=live)}
    due = {bundle.id: bundle for bundle in due_bundles(session, now=now)}
    result.drained = tuple(sorted(drained))
    flushed: list[str] = []
    for bundle_id in sorted({**drained, **due}):
        try:
            # One savepoint per bundle: a failed flush discards its own partial
            # catalog state (claim included) and the pass carries on. Nested
            # inside it, `_fan_out_targets` keeps its per-target savepoints, so
            # a post-write failure still seals partial rather than unwinding to
            # here — the two levels encode the pre-write/post-write boundary.
            with session.begin_nested():
                flush_bundle(
                    session,
                    bundle_id=bundle_id,
                    backends=backends,
                    builder=builder,
                    key_epoch=key_epoch,
                    deliverables_dir=deliverables_dir,
                )
        except Exception as exc:
            # Deliberately broad: the sweep is a batch over independent
            # bundles, and one group's bad member, missing source, or
            # unreachable backend must not stop every other group from
            # sealing. Nothing is swallowed and nothing partial survives — the
            # savepoint above has already rolled this bundle back to ``open``
            # and un-claimed, and the failure is recorded in the result,
            # emitted as a structured event, and exits the CLI non-zero.
            result.failed.append((bundle_id, str(exc)))
            emit_structured_event(
                "bundle_sweep_flush_failed",
                bundle_id=bundle_id,
                reason=type(exc).__name__,
                message=str(exc)[:500],
            )
            continue
        flushed.append(bundle_id)
    result.flushed = tuple(flushed)
    return result
