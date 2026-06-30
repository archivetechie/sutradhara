"""`sutra intake` and `sutra prepare` commands."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click

from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.catalog.types import IntakeStatus
from sutradhara.intake import (
    IntakeDiscrepancyError,
    accept_intake,
    inspect_intake,
    inspect_landing_root,
    prepare_intake,
    publish_intake_marker,
    register_intake,
)
from sutradhara.intake_watch import WatchEvent, process_landing_once, watch_landing


@click.group("intake")
def intake_group() -> None:
    """Inspect and register landing intakes."""


@intake_group.command("inspect")
@click.argument("path", type=click.Path(path_type=Path, exists=True, file_okay=False))
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON report.")
def intake_inspect(path: Path, as_json: bool) -> None:
    """Validate an intake directory or landing root without catalog writes."""

    engine = make_engine()
    with session_scope(engine) as session:
        reports = (
            [inspect_intake(session, path)]
            if (path / "intake.json").exists()
            else inspect_landing_root(session, path)
        )
    payload = [_report_dict(row) for row in reports]
    if as_json:
        click.echo(json.dumps(payload[0] if len(payload) == 1 else payload, indent=2, default=str))
    else:
        for row in payload:
            reason = f" reason={row['reason']}" if row["reason"] else ""
            click.echo(f"{row['intake_id']}: {row['status']} items={row['item_count']}{reason}")
    if any(row["status"] in {"incomplete", "invalid"} for row in payload):
        sys.exit(1)


@intake_group.command("register")
@click.argument("intake_id")
@click.option(
    "--landing-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="Landing root containing INTAKE_ID. If omitted, INTAKE_ID may be a path.",
)
@click.option("--artifactclass", default=None, help="Required for legacy/non-bag intakes.")
@click.option(
    "--cache-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="Directory for register-time cloud-temp work.",
)
@click.option(
    "--cloud-backend",
    default="cloud-temp",
    show_default=True,
    help="Backend name for intake cloud blob jobs.",
)
@click.option(
    "--cloud-pool",
    default="cloud-temp",
    show_default=True,
    help="Pool id for intake cloud blob jobs.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def intake_register(
    intake_id: str,
    landing_root: Path | None,
    artifactclass: str | None,
    cache_root: Path | None,
    cloud_backend: str,
    cloud_pool: str,
    as_json: bool,
) -> None:
    """Explicitly accept one completed intake into the catalog."""

    intake_dir = _resolve_intake_dir(intake_id, landing_root)
    engine = make_engine()
    try:
        with session_scope(engine) as session:
            outcome = register_intake(
                session,
                intake_dir,
                artifactclass=artifactclass,
                cache_root=cache_root,
                cloud_backend_name=cloud_backend,
                cloud_pool_id=cloud_pool,
            )
    except IntakeDiscrepancyError as exc:
        publish_intake_marker(exc.marker)
        click.echo(f"error: {exc}", err=True)
        sys.exit(2)
    except (FileNotFoundError, ValueError) as exc:
        click.echo(f"error: {exc}", err=True)
        sys.exit(2)
    publish_intake_marker(outcome.marker)
    _emit_outcome(outcome, as_json=as_json)
    if outcome.status == IntakeStatus.QUARANTINED.value:
        sys.exit(1)


@intake_group.command("accept")
@click.argument("intake_id")
@click.option(
    "--landing-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="Landing root containing INTAKE_ID. If omitted, INTAKE_ID may be a path.",
)
@click.option("--artifactclass", default=None, help="Required for legacy/non-bag intakes.")
@click.option("--prepare", "prepare_profile", default=None, help="Prepare profile to record.")
@click.option(
    "--cache-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="Directory for register cloud-temp work.",
)
@click.option(
    "--cloud-backend",
    default="cloud-temp",
    show_default=True,
    help="Backend name for intake cloud blob jobs.",
)
@click.option(
    "--cloud-pool",
    default="cloud-temp",
    show_default=True,
    help="Pool id for intake cloud blob jobs.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def intake_accept(
    intake_id: str,
    landing_root: Path | None,
    artifactclass: str | None,
    prepare_profile: str | None,
    cache_root: Path | None,
    cloud_backend: str,
    cloud_pool: str,
    as_json: bool,
) -> None:
    """Register one intake and optionally record a prepare profile."""

    intake_dir = _resolve_intake_dir(intake_id, landing_root)
    engine = make_engine()
    try:
        with session_scope(engine) as session:
            outcome = accept_intake(
                session,
                intake_dir,
                artifactclass=artifactclass,
                prepare_profile=prepare_profile,
                cache_root=cache_root,
                cloud_backend_name=cloud_backend,
                cloud_pool_id=cloud_pool,
            )
    except IntakeDiscrepancyError as exc:
        publish_intake_marker(exc.marker)
        click.echo(f"error: {exc}", err=True)
        sys.exit(2)
    except (FileNotFoundError, ValueError) as exc:
        click.echo(f"error: {exc}", err=True)
        sys.exit(2)
    publish_intake_marker(outcome.marker)
    _emit_outcome(outcome, as_json=as_json)
    if outcome.status == IntakeStatus.QUARANTINED.value:
        sys.exit(1)


@intake_group.command("watch")
@click.option(
    "--landing-root",
    "--landing",
    "landing_root",
    type=click.Path(path_type=Path, file_okay=False),
    required=True,
    help="Landing root to scan for completed intakes.",
)
@click.option("--once", is_flag=True, default=False, help="Scan/process once and exit.")
@click.option("--interval", "interval_seconds", type=float, default=5.0, show_default=True)
@click.option("--settle-seconds", type=float, default=2.0, show_default=True)
@click.option("--stable-polls", type=int, default=2, show_default=True)
@click.option("--validation-attempts", type=int, default=2, show_default=True)
@click.option("--artifactclass", default=None, help="Required only for legacy non-BagIt intakes.")
@click.option("--prepare", "prepare_profile", default=None, help="Prepare profile to record.")
@click.option(
    "--cache-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="Directory for lock and register-time cloud-temp work.",
)
@click.option(
    "--cloud-backend",
    default="cloud-temp",
    show_default=True,
    help="Backend name for intake cloud blob jobs.",
)
@click.option(
    "--cloud-pool",
    default="cloud-temp",
    show_default=True,
    help="Pool id for intake cloud blob jobs.",
)
@click.option("--json-lines", is_flag=True, default=False, help="Emit one JSON object per event.")
def intake_watch(
    landing_root: Path,
    once: bool,
    interval_seconds: float,
    settle_seconds: float,
    stable_polls: int,
    validation_attempts: int,
    artifactclass: str | None,
    prepare_profile: str | None,
    cache_root: Path | None,
    cloud_backend: str,
    cloud_pool: str,
    json_lines: bool,
) -> None:
    """Poll a landing root and register completed intakes."""

    if stable_polls < 1:
        raise click.BadParameter("must be >= 1", param_hint="--stable-polls")
    if validation_attempts < 1:
        raise click.BadParameter("must be >= 1", param_hint="--validation-attempts")
    engine = make_engine()
    common = {
        "engine": engine,
        "interval_seconds": interval_seconds,
        "settle_seconds": settle_seconds,
        "stable_polls": stable_polls,
        "validation_attempts": validation_attempts,
        "artifactclass": artifactclass,
        "prepare_profile": prepare_profile,
        "cache_root": cache_root,
        "cloud_backend_name": cloud_backend,
        "cloud_pool_id": cloud_pool,
    }
    if once:
        events = process_landing_once(landing_root, **common)
        for event in events:
            _emit_watch_event(event, json_lines=json_lines)
        if any(event.is_bad_once_outcome for event in events):
            sys.exit(1)
        return

    def emit(event: WatchEvent) -> None:
        _emit_watch_event(event, json_lines=json_lines)

    watch_landing(landing_root, on_event=emit, **common)


@click.command("prepare")
@click.argument("intake_id")
@click.option("--profile", required=True, help="Prepare profile to record.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def prepare_cmd(
    intake_id: str,
    profile: str,
    as_json: bool,
) -> None:
    """Record a prepare profile for the derivation reconciler."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            outcome = prepare_intake(
                session,
                intake_id,
                profile=profile,
            )
    except ValueError as exc:
        click.echo(f"error: {exc}", err=True)
        sys.exit(2)
    payload = {
        "intake_id": outcome.intake_id,
        "status": outcome.status,
        "profile": outcome.profile,
        "jobs_submitted": outcome.jobs_submitted,
        "reason": outcome.reason,
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
    else:
        reason = f" reason={outcome.reason}" if outcome.reason else ""
        click.echo(
            f"{outcome.intake_id}: prepared profile={outcome.profile} "
            f"jobs={outcome.jobs_submitted}{reason}"
        )


def _resolve_intake_dir(intake_id: str, landing_root: Path | None) -> Path:
    if landing_root is not None:
        return landing_root / intake_id
    candidate = Path(intake_id)
    if candidate.is_dir():
        return candidate
    return Path.cwd() / intake_id


def _emit_outcome(row: Any, *, as_json: bool) -> None:
    payload = _outcome_dict(row)
    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
        return
    reason = f" reason={payload['reason']}" if payload["reason"] else ""
    click.echo(
        f"{payload['intake_id']}: {payload['status']} "
        f"items={payload['item_count']} jobs={payload['jobs_submitted']}{reason}"
    )


def _emit_watch_event(event: WatchEvent, *, json_lines: bool) -> None:
    payload = event.payload()
    if json_lines:
        click.echo(json.dumps(payload, default=str, sort_keys=True))
        return
    name = event.path.name if event.path is not None else "-"
    if event.event == "watch-start":
        click.echo(
            f"watch-start landing={event.path} "
            f"interval={event.details.get('interval') if event.details else None} "
            f"settle={event.details.get('settle_seconds') if event.details else None}"
        )
        return
    if event.event == "watch-stop":
        click.echo("watch-stop")
        return
    if event.event == "intake-skipped":
        click.echo(f"{name}: skipped reason={event.reason}")
        return
    if event.event == "intake-validation-retry":
        click.echo(f"{name}: validation-retry reason={event.reason}")
        return
    if event.event == "intake-error":
        click.echo(f"{name}: error reason={event.reason}")
        return
    if event.event == "intake-discrepancy":
        click.echo(f"{name}: discrepancy reason={event.reason}")
        return
    if event.event == "intake-quarantined":
        click.echo(f"{name}: quarantined reason={event.reason}")
        return
    if event.event == "intake-already-registered":
        click.echo(
            f"{name}: already-registered items={event.item_count} jobs={event.jobs_submitted}"
        )
        return
    if event.event == "intake-registered":
        click.echo(f"{name}: registered items={event.item_count} jobs={event.jobs_submitted}")
        return
    click.echo(f"{name}: {event.event} reason={event.reason}")


def _report_dict(row: Any) -> dict[str, Any]:
    return {
        "intake_id": row.intake_id,
        "path": str(row.path),
        "status": row.status,
        "item_count": row.item_count,
        "reason": row.reason,
        "manifest_path": str(row.manifest_path) if row.manifest_path else None,
        "manifest_digest": row.manifest_digest,
        "artifactclass": row.artifactclass,
        "details": row.details,
        "marker_path": str(row.marker.path) if getattr(row, "marker", None) else None,
    }


def _outcome_dict(row: Any) -> dict[str, Any]:
    return {
        "intake_id": row.intake_id,
        "path": str(row.path),
        "status": row.status,
        "item_count": row.item_count,
        "jobs_submitted": row.jobs_submitted,
        "reason": row.reason,
        "manifest_path": str(row.manifest_path) if row.manifest_path else None,
        "manifest_digest": row.manifest_digest,
        "artifactclass": row.artifactclass,
        "details": row.details,
    }
