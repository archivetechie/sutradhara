"""`sutra list assets` — query the catalog."""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

import click
from sqlalchemy import select

from sutradhara.catalog.models import LogicalAsset
from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.catalog.types import CopyHealth


@click.group("list")
def list_group() -> None:
    """Query the catalog."""


@list_group.command("assets")
@click.option(
    "--limit",
    type=int,
    default=50,
    show_default=True,
    help="Maximum rows to print (0 = unlimited).",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit JSON (one record per line) instead of a table.",
)
def list_assets(limit: int, as_json: bool) -> None:
    """List logical assets in the catalog."""
    engine = make_engine()
    with session_scope(engine) as s:
        q = select(LogicalAsset).order_by(LogicalAsset.first_seen_at)
        if limit:
            q = q.limit(limit)
        rows = list(s.scalars(q))

        if not rows:
            click.echo("(catalog is empty)")
            return

        if as_json:
            for r in rows:
                available_copies = [c for c in r.copies if c.health != CopyHealth.MISSING]
                copies_by_backend = Counter(c.backend.name for c in available_copies)
                payload: dict[str, Any] = {
                    "content_sha256": r.content_sha256.hex(),
                    "size_bytes": r.size_bytes,
                    "first_seen_at": r.first_seen_at.isoformat(),
                    "human_label": r.human_label,
                    "media_kind": r.media_kind,
                    "copies_by_backend": dict(copies_by_backend),
                }
                click.echo(json.dumps(payload))
            return

        click.echo(
            f"{'HASH'.ljust(16)}  {'SIZE'.rjust(12)}  {'COPIES'.rjust(6)}  BACKENDS"
        )
        click.echo(f"{'-' * 16}  {'-' * 12}  {'-' * 6}  --------")
        for r in rows:
            available_copies = [c for c in r.copies if c.health != CopyHealth.MISSING]
            backends = sorted({c.backend.name for c in available_copies})
            click.echo(
                f"{r.content_sha256.hex()[:16]}  "
                f"{r.size_bytes:>12}  "
                f"{len(available_copies):>6}  "
                f"{', '.join(backends) if backends else '(none)'}"
            )
