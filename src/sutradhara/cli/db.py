"""`sutra db` — schema management commands."""

from __future__ import annotations

import click

from sutradhara.catalog.session import create_all, database_url, make_engine


@click.group("db")
def db_group() -> None:
    """Schema management (dev convenience; production uses alembic)."""


@db_group.command("init")
@click.option(
    "--echo",
    is_flag=True,
    default=False,
    help="Echo SQL during DDL.",
)
def db_init(echo: bool) -> None:
    """Create all tables on the configured DB.

    Convenience for development. Production deployments should use
    `alembic upgrade head` instead so the change history is tracked.
    """
    url = database_url()
    click.echo(f"Initializing catalog schema at {url}")
    engine = make_engine(echo=echo)
    create_all(engine)
    click.echo("OK.")
