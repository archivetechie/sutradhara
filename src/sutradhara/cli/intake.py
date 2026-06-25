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
    accept_intake,
    inspect_intake,
    inspect_landing_root,
    prepare_intake,
    register_intake,
)


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
    except (FileNotFoundError, ValueError) as exc:
        click.echo(f"error: {exc}", err=True)
        sys.exit(2)
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
    help="Directory for register/prepare work.",
)
@click.option(
    "--proxy-artifactclass",
    default="proxy",
    show_default=True,
    help="Artifactclass assigned to transcode outputs.",
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
    proxy_artifactclass: str,
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
                proxy_artifactclass=proxy_artifactclass,
                cloud_backend_name=cloud_backend,
                cloud_pool_id=cloud_pool,
            )
    except (FileNotFoundError, ValueError) as exc:
        click.echo(f"error: {exc}", err=True)
        sys.exit(2)
    _emit_outcome(outcome, as_json=as_json)
    if outcome.status == IntakeStatus.QUARANTINED.value:
        sys.exit(1)


@click.command("prepare")
@click.argument("intake_id")
@click.option("--profile", required=True, help="Prepare profile to record.")
@click.option(
    "--cache-root",
    type=click.Path(path_type=Path, file_okay=False),
    required=True,
    help="Directory for generated derivatives and sidecars.",
)
@click.option(
    "--proxy-artifactclass",
    default="proxy",
    show_default=True,
    help="Artifactclass assigned to transcode outputs.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def prepare_cmd(
    intake_id: str,
    profile: str,
    cache_root: Path,
    proxy_artifactclass: str,
    as_json: bool,
) -> None:
    """Record a prepare profile and enqueue missing derivative work."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            outcome = prepare_intake(
                session,
                intake_id,
                profile=profile,
                cache_root=cache_root,
                proxy_artifactclass=proxy_artifactclass,
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
