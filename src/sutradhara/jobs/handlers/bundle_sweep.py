"""``bundle-sweep`` job: the periodic caller ``bundle_due`` never had.

One job kind runs the whole pass from ``sutradhara.archive_sweeper``: reap
stuck flush claims, void-seal empty orphan accumulators, drain accumulators no
live policy derives any more, and flush everything ``bundle_due`` says is due —
open funnel bundles included (P1 gate condition C3).

The handler is a thin wrapper on purpose. It resolves backends from the bundles
it is actually about to flush, and the sweep logic itself stays testable
without the jobs machinery.
"""

from __future__ import annotations

from typing import cast

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.archive_fanout import ArchiveFanoutError
from sutradhara.archive_sweeper import SweepResult, sweep_bundles
from sutradhara.backend import factory
from sutradhara.bundle_group import basis_pool_ids
from sutradhara.catalog.models import Backend, Bundle, Pool
from sutradhara.jobs.handlers.bundle_repair import make_archive_builder
from sutradhara.jobs.registry import JobContext, JobResult, register_handler
from sutradhara.replication import WritableStorageBackend


@register_handler("bundle-sweep")
def handle_bundle_sweep(ctx: JobContext) -> JobResult:
    params = ctx.job.params
    key_epoch = params.get("key_epoch")
    if key_epoch is not None and not isinstance(key_epoch, str):
        raise ValueError(f"bundle-sweep params.key_epoch must be a string; got {key_epoch!r}")
    rem_bin = params.get("rem_bin")
    if rem_bin is not None and not isinstance(rem_bin, str):
        raise ValueError(f"bundle-sweep params.rem_bin must be a string; got {rem_bin!r}")

    backends = sweep_backends(ctx.session)
    for name in sorted({backend.name for backend in backends.values()}):
        ctx.touch(f"backend:{name}")
    result = sweep_bundles(
        ctx.session,
        backends=backends,
        builder=make_archive_builder(rem_bin=rem_bin),
        key_epoch=key_epoch,
    )
    ctx.observe(
        {
            "reaped": list(result.reaped),
            "voided": list(result.voided),
            "drained": list(result.drained),
            "flushed": list(result.flushed),
            "failed": [bundle_id for bundle_id, _ in result.failed],
        }
    )
    for bundle_id in result.flushed:
        ctx.touch(f"bundle:{bundle_id}")
    # A flush failure is a fact about one bundle, not a broken sweeper: the
    # pass still sealed everything else, so the job stays ok=True and the
    # detail names what did not seal.
    return JobResult(ok=True, detail=_detail(result))


def sweep_backends(session: Session) -> dict[int, WritableStorageBackend]:
    """Backends for every pool named by an open bundle's frozen ``group_basis``.

    Basis pools, not live class placements: a bundle fans out to the placement
    it was opened against (§2/§5), and a sweeper that resolved backends from
    today's policy would hand the flush a backend set that does not match the
    basis it is about to build for.
    """
    pool_ids: set[str] = set()
    for bundle in session.scalars(select(Bundle).where(Bundle.status == "open")):
        pool_ids.update(basis_pool_ids(bundle.group_basis))
    if not pool_ids:
        return {}
    rows = list(
        session.scalars(
            select(Backend)
            .join(Pool, Pool.backend_id == Backend.id)
            .where(Pool.id.in_(pool_ids))
            .order_by(Backend.id)
        ).unique()
    )
    resolved: dict[int, WritableStorageBackend] = {}
    for row in rows:
        backend = factory.backend_from_row(row)
        if not hasattr(backend, "write_object_to_pool"):
            raise ArchiveFanoutError(
                f"backend {row.name!r} does not implement write_object_to_pool"
            )
        resolved[row.id] = cast(WritableStorageBackend, backend)
    return resolved


def _detail(result: SweepResult) -> str:
    parts = [
        f"reaped={len(result.reaped)}",
        f"voided={len(result.voided)}",
        f"flushed={len(result.flushed)}",
    ]
    if result.failed:
        failures = "; ".join(f"{bundle_id}: {reason}" for bundle_id, reason in result.failed)
        parts.append(f"failed={len(result.failed)} ({failures})")
    return "bundle sweep: " + " ".join(parts)
