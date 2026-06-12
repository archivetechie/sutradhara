"""`sutra scrub` — reconcile a backend against the catalog."""

from __future__ import annotations

import sys

import click
from sqlalchemy import select

from sutradhara.backend.factory import (
    BackendNotConfigured,
    UnsupportedBackendKind,
    backend_from_row,
)
from sutradhara.catalog.models import Backend
from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.scrub import scrub_backend


@click.command("scrub")
@click.option(
    "--backend",
    "backend_name",
    required=True,
    help="Name of a registered backend.",
)
def scrub_cmd(backend_name: str) -> None:
    """Re-enumerate a backend and reconcile against the catalog.

    The load-bearing demo of the rebuildable-index principle
    (docs/spec-v0.1.md §2 principle 1 + §7).
    """
    engine = make_engine()
    with session_scope(engine) as s:
        row = s.scalars(
            select(Backend).where(Backend.name == backend_name)
        ).one_or_none()
        if row is None:
            click.echo(f"error: no backend named {backend_name!r}", err=True)
            sys.exit(2)

        try:
            backend = backend_from_row(row)
        except (BackendNotConfigured, UnsupportedBackendKind) as e:
            click.echo(f"error: cannot construct backend: {e}", err=True)
            sys.exit(2)

        report = scrub_backend(s, row, backend)

    click.echo(f"scrub of backend {backend_name!r} complete:")
    click.echo(f"  assets added:      {report.assets_added}")
    click.echo(f"  copies added:      {report.copies_added}")
    click.echo(f"  copies updated:    {report.copies_updated}")
    click.echo(f"  copies missing:    {report.copies_marked_missing}")
    if report.integrity_warnings:
        click.echo(f"  integrity warnings: {len(report.integrity_warnings)}")
        for w in report.integrity_warnings:
            click.echo(f"    - {w}", err=True)
