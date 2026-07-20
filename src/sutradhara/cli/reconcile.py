"""`sutra reconcile` — run one desired-state reconciliation pass."""

from __future__ import annotations

import getpass
import sys
from pathlib import Path
from typing import Any

import click
from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.jobs.config import override_derivation_cache_root
from sutradhara.jobs.models import ConditionComponent, ReconciliationCondition
from sutradhara.jobs.reconcilers import bundle_copy as _bundle_copy_reconciler  # noqa: F401
from sutradhara.jobs.reconcilers import copy as _copy_reconciler  # noqa: F401 -- register copy
from sutradhara.jobs.reconcilers import (
    derivation as _derivation_reconciler,  # noqa: F401 -- register derivation
)
from sutradhara.jobs.reconcilers import hdcache as _hdcache_reconciler  # noqa: F401
from sutradhara.jobs.reconcilers import log_pipeline as _log_pipeline_reconciler  # noqa: F401
from sutradhara.jobs.reconcilers import restore_open as _restore_open_reconciler  # noqa: F401
from sutradhara.jobs.reconcilers.conditions import CONDITION_BLOCKED, reopen_condition
from sutradhara.jobs.reconcilers.registry import ReconcilerNotRegistered
from sutradhara.jobs.reconcilers.spine import reconcile


@click.command("reconcile")
@click.argument("domain")
@click.argument("component", required=False)
@click.option("--batch", type=int, default=1000, show_default=True, help="Discover batch size.")
@click.option("--cursor", type=int, default=None, help="Ingest-item id cursor for discovery.")
@click.option("--limit", type=int, default=100, show_default=True, help="Process work limit.")
@click.option(
    "--cache-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="Override derivation cache root for this reconcile run.",
)
@click.option("--list-blocked", is_flag=True, help="List blocked conditions for DOMAIN.")
@click.option("--reopen-blocked", is_flag=True, help="Reopen blocked conditions for DOMAIN.")
@click.option("--reason", "reason_filter", default=None, help="Filter --reopen-blocked by reason.")
@click.option("--note", default=None, help="Operator note for record-fix.")
def reconcile_cmd(
    domain: str,
    component: str | None,
    batch: int,
    cursor: int | None,
    limit: int,
    cache_root: Path | None,
    list_blocked: bool,
    reopen_blocked: bool,
    reason_filter: str | None,
    note: str | None,
) -> None:
    """Run one bounded reconcile cycle, or record-fix COMPONENT."""

    if domain == "record-fix":
        if component is None:
            raise click.ClickException("record-fix requires COMPONENT")
        if note is None or not note.strip():
            raise click.ClickException("record-fix requires --note TEXT")
        if list_blocked or reopen_blocked or reason_filter is not None or cache_root is not None:
            raise click.ClickException(
                "record-fix does not accept domain reconcile filters or --cache-root"
            )
        if limit < 1 or limit > 1000:
            raise click.ClickException("record-fix --limit must be between 1 and 1000")
        engine = make_engine()
        with session_scope(engine) as session:
            count = _record_fix(session, component, note=note.strip(), limit=limit)
        click.echo(f"reopened {count} blocked condition(s) for component {component}")
        return

    if component is not None:
        raise click.ClickException("unexpected COMPONENT; use 'reconcile record-fix COMPONENT'")
    if note is not None:
        raise click.ClickException("--note may only be used with record-fix")

    if list_blocked and reopen_blocked:
        raise click.ClickException("--list-blocked and --reopen-blocked are mutually exclusive")
    if reason_filter is not None and not reopen_blocked:
        raise click.ClickException("--reason may only be used with --reopen-blocked")

    engine = make_engine()
    with override_derivation_cache_root(cache_root), session_scope(engine) as session:
        if list_blocked:
            _list_blocked(session, domain)
            return
        if reopen_blocked:
            count = _reopen_blocked(session, domain, reason_filter=reason_filter)
            click.echo(f"reopened {count} blocked condition(s)")
            return
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


def _blocked_query(domain: str) -> Any:
    return (
        select(ReconciliationCondition)
        .where(
            ReconciliationCondition.domain == domain,
            ReconciliationCondition.condition == CONDITION_BLOCKED,
        )
        .order_by(ReconciliationCondition.condition_changed_at, ReconciliationCondition.id)
    )


def _list_blocked(session: Session, domain: str) -> None:
    for row in session.scalars(_blocked_query(domain)):
        click.echo(
            " ".join(
                [
                    f"target_key={row.target_key}",
                    f"reason={row.reason or ''}",
                    f"blocked_tool_name={row.blocked_tool_name or ''}",
                    f"blocked_tool_version={row.blocked_tool_version or ''}",
                    f"since={row.condition_changed_at.isoformat()}",
                ]
            )
        )


def _reopen_blocked(session: Session, domain: str, *, reason_filter: str | None) -> int:
    rows = list(session.scalars(_blocked_query(domain)))
    count = 0
    actor = getpass.getuser()
    for row in rows:
        if reason_filter is not None and row.reason != reason_filter:
            continue
        reopen_condition(
            session,
            row,
            actor=actor,
            note="operator reopen",
        )
        count += 1
    return count


def _record_fix(session: Session, component: str, *, note: str, limit: int) -> int:
    """Reopen one bounded batch of blocked rows with an exact component match."""

    rows = list(
        session.scalars(
            select(ReconciliationCondition)
            .join(
                ConditionComponent,
                ConditionComponent.condition_id == ReconciliationCondition.id,
            )
            .where(
                ReconciliationCondition.condition == CONDITION_BLOCKED,
                ConditionComponent.component == component,
            )
            .order_by(ReconciliationCondition.condition_changed_at, ReconciliationCondition.id)
            .limit(limit)
        )
    )
    actor = getpass.getuser()
    for row in rows:
        reopen_condition(session, row, actor=actor, note=note)
    return len(rows)
