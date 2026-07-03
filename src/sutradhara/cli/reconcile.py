"""`sutra reconcile` — run one desired-state reconciliation pass."""

from __future__ import annotations

import sys
from pathlib import Path

import click

from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.jobs.config import override_derivation_cache_root
from sutradhara.jobs.reconcilers import copy as _copy_reconciler  # noqa: F401 -- register copy
from sutradhara.jobs.reconcilers import (
    derivation as _derivation_reconciler,  # noqa: F401 -- register derivation
)
from sutradhara.jobs.reconcilers import hdcache as _hdcache_reconciler  # noqa: F401
from sutradhara.jobs.reconcilers.registry import ReconcilerNotRegistered
from sutradhara.jobs.reconcilers.spine import reconcile


@click.command("reconcile")
@click.argument("domain")
@click.option("--batch", type=int, default=1000, show_default=True, help="Discover batch size.")
@click.option("--cursor", type=int, default=None, help="Ingest-item id cursor for discovery.")
@click.option("--limit", type=int, default=100, show_default=True, help="Process work limit.")
@click.option(
    "--cache-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="Override derivation cache root for this reconcile run.",
)
def reconcile_cmd(
    domain: str,
    batch: int,
    cursor: int | None,
    limit: int,
    cache_root: Path | None,
) -> None:
    """Run one bounded reconcile cycle for DOMAIN."""

    engine = make_engine()
    with override_derivation_cache_root(cache_root), session_scope(engine) as session:
        try:
            discovered, processed = reconcile(
                session,
                domain,
                batch=batch,
                cursor=cursor,
                limit=limit,
            )
        except ReconcilerNotRegistered as exc:
            click.echo(f"error: {exc}", err=True)
            sys.exit(2)

    click.echo(
        f"reconcile domain={domain!r} complete: "
        f"observed {discovered} target(s), processed {processed} condition(s)"
    )
