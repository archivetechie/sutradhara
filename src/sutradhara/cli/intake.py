"""`sutra intake` commands."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click

from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.intake import scan_landing_root


@click.group("intake")
def intake_group() -> None:
    """Verify and register landing intakes."""


@intake_group.command("scan")
@click.argument("landing_root", type=click.Path(path_type=Path, exists=True, file_okay=False))
@click.option(
    "--cache-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="Directory for generated derivatives and sidecars.",
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
@click.option(
    "--no-enqueue",
    is_flag=True,
    default=False,
    help="Register intakes without enqueueing derived work.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def intake_scan(
    landing_root: Path,
    cache_root: Path | None,
    proxy_artifactclass: str,
    cloud_backend: str,
    cloud_pool: str,
    no_enqueue: bool,
    as_json: bool,
) -> None:
    """Scan completed intake directories below LANDING_ROOT."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            outcomes = scan_landing_root(
                session,
                landing_root,
                enqueue_jobs=not no_enqueue,
                cache_root=cache_root,
                proxy_artifactclass=proxy_artifactclass,
                cloud_backend_name=cloud_backend,
                cloud_pool_id=cloud_pool,
            )
    except (FileNotFoundError, ValueError) as exc:
        click.echo(f"error: {exc}", err=True)
        sys.exit(2)

    payload = [_outcome_dict(row) for row in outcomes]
    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
        return
    if not outcomes:
        click.echo("(no completed intakes)")
        return
    for row in payload:
        reason = f" reason={row['reason']}" if row["reason"] else ""
        click.echo(
            f"{row['intake_id']}: {row['status']} "
            f"items={row['item_count']} jobs={row['jobs_submitted']}{reason}"
        )


def _outcome_dict(row: Any) -> dict[str, Any]:
    return {
        "intake_id": row.intake_id,
        "path": str(row.path),
        "status": row.status,
        "item_count": row.item_count,
        "jobs_submitted": row.jobs_submitted,
        "reason": row.reason,
        "manifest_path": str(row.manifest_path) if row.manifest_path else None,
        "details": row.details,
    }
