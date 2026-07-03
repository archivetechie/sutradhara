"""`sutra jobs` — submit, list, run, show jobs."""

from __future__ import annotations

import datetime as dt
import json
import sys
from typing import Any

import click
from sqlalchemy import select

from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.jobs import handlers as _handlers  # noqa: F401 -- register built-ins
from sutradhara.jobs.config import parse_pool_overrides
from sutradhara.jobs.engine import run_one, run_pending, submit
from sutradhara.jobs.models import Job, JobStatus
from sutradhara.jobs.registry import (
    HandlerNotRegistered,
    JobResult,
    registered_kinds,
)


@click.group("jobs")
def jobs_group() -> None:
    """Submit, run, and inspect jobs."""


@jobs_group.command("submit")
@click.argument("kind")
@click.option(
    "--param",
    "-p",
    multiple=True,
    help="key=value (repeatable). Values are JSON-decoded if possible, else strings.",
)
@click.option(
    "--resource",
    multiple=True,
    help="Required counted resource, pool=count. Repeatable.",
)
@click.option("--prereq", "prerequisites", multiple=True, type=int, help="Prerequisite job id.")
@click.option("--not-before", default=None, help="ISO-8601 UTC timestamp before dispatch.")
@click.option("--priority", type=int, default=0, show_default=True, help="Lower runs earlier.")
@click.option("--dedupe-key", default=None, help="Idempotency key for submit retries.")
def jobs_submit(
    kind: str,
    param: tuple[str, ...],
    resource: tuple[str, ...],
    prerequisites: tuple[int, ...],
    not_before: str | None,
    priority: int,
    dedupe_key: str | None,
) -> None:
    """Submit a new job of KIND with --param key=value pairs."""
    if kind == "restore":
        click.echo(
            "error: restore jobs must be created from gated restore requests",
            err=True,
        )
        sys.exit(2)
    if kind not in registered_kinds():
        click.echo(
            f"error: no handler registered for kind {kind!r}; known: {sorted(registered_kinds())}",
            err=True,
        )
        sys.exit(2)

    params = _parse_params(param)
    required_resources = _parse_resources(resource)
    not_before_dt = _parse_not_before(not_before)
    engine = make_engine()
    with session_scope(engine) as s:
        job = submit(
            s,
            kind,
            params,
            required_resources=required_resources,
            prerequisites=list(prerequisites),
            not_before=not_before_dt,
            priority=priority,
            dedupe_key=dedupe_key,
        )
        click.echo(f"submitted job id={job.id} kind={job.kind!r} status={job.status}")


@jobs_group.command("list")
@click.option(
    "--status",
    "status_filter",
    type=click.Choice([s.value for s in JobStatus]),
    default=None,
    help="Filter by status.",
)
@click.option(
    "--limit",
    type=int,
    default=50,
    show_default=True,
    help="Maximum rows (0 = unlimited).",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit JSON (one record per line) instead of a table.",
)
def jobs_list(status_filter: str | None, limit: int, as_json: bool) -> None:
    """List jobs."""
    engine = make_engine()
    with session_scope(engine) as s:
        q = select(Job).order_by(Job.created_at.desc(), Job.id.desc())
        if status_filter:
            q = q.where(Job.status == JobStatus(status_filter))
        if limit:
            q = q.limit(limit)
        rows = list(s.scalars(q))

        if not rows:
            click.echo("(no jobs)")
            return

        if as_json:
            for r in rows:
                click.echo(json.dumps(_job_to_dict(r)))
            return

        click.echo(f"{'ID':>5}  {'KIND'.ljust(12)}  {'STATUS'.ljust(10)}  {'ATT':>3}  CREATED")
        click.echo("-----  ------------  ----------  ---  ----------------------------")
        for r in rows:
            click.echo(
                f"{r.id:>5}  {r.kind.ljust(12)}  {r.status.ljust(10)}  "
                f"{r.attempts:>3}  {r.created_at.isoformat(timespec='seconds')}"
            )


@jobs_group.command("show")
@click.argument("job_id", type=int)
def jobs_show(job_id: int) -> None:
    """Print full detail for one job."""
    engine = make_engine()
    with session_scope(engine) as s:
        job = s.get(Job, job_id)
        if job is None:
            click.echo(f"error: no job with id={job_id}", err=True)
            sys.exit(2)
        click.echo(json.dumps(_job_to_dict(job), indent=2, default=str))


@jobs_group.command("run")
@click.option(
    "--id",
    "job_id",
    type=int,
    default=None,
    help="Run one specific job by id.",
)
@click.option(
    "--limit",
    type=int,
    default=1,
    show_default=True,
    help="Run up to N pending jobs (0 = drain queue). Ignored if --id is set.",
)
def jobs_run(job_id: int | None, limit: int) -> None:
    """Run pending jobs synchronously (or one specific job by --id)."""
    engine = make_engine()
    with session_scope(engine) as s:
        try:
            if job_id is not None:
                result = run_one(s, job_id)
                _emit_result(job_id, result)
                return
            results = run_pending(s, limit=limit)
        except (HandlerNotRegistered, ValueError) as e:
            click.echo(f"error: {e}", err=True)
            sys.exit(2)

    if not results:
        click.echo("(no pending jobs)")
        return

    for jid, result in results:
        _emit_result(jid, result)


# --- helpers -------------------------------------------------------------


def _emit_result(job_id: int, result: JobResult) -> None:
    ok_str = "ok" if result.ok else "FAILED"
    click.echo(f"job id={job_id}: {ok_str} — {result.detail}")


def _job_to_dict(job: Job) -> dict[str, Any]:
    return {
        "id": job.id,
        "kind": job.kind,
        "status": job.status,
        "params": job.params,
        "required_resources": job.required_resources,
        "prerequisites": job.prerequisites,
        "not_before": job.not_before.isoformat(),
        "priority": job.priority,
        "dedupe_key": job.dedupe_key,
        "step_state": job.step_state,
        "attempts": job.attempts,
        "last_error": job.last_error,
        "created_at": job.created_at.isoformat(),
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
    }


def _parse_params(pairs: tuple[str, ...]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for p in pairs:
        if "=" not in p:
            raise click.UsageError(f"--param {p!r} must be key=value")
        k, _, v = p.partition("=")
        try:
            out[k] = json.loads(v)
        except json.JSONDecodeError:
            out[k] = v
    return out


def _parse_resources(items: tuple[str, ...]) -> list[dict[str, Any]]:
    try:
        resources = parse_pool_overrides(items)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc
    return [{"pool": pool, "count": count} for pool, count in resources.items()]


def _parse_not_before(raw: str | None) -> dt.datetime | None:
    if raw is None:
        return None
    try:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise click.UsageError("--not-before must be an ISO-8601 datetime") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed
