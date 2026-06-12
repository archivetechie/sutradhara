"""Sutradhara CLI entry point."""

from __future__ import annotations

import click

from sutradhara import __version__
from sutradhara.cli.admin import admin_group
from sutradhara.cli.assets import list_group
from sutradhara.cli.backends import backends_group
from sutradhara.cli.db import db_group
from sutradhara.cli.jobs import jobs_group
from sutradhara.cli.scrub import scrub_cmd


@click.group()
@click.version_option(__version__, prog_name="sutra")
def cli() -> None:
    """Sutradhara — orchestrator above Remanence."""


cli.add_command(db_group)
cli.add_command(backends_group)
cli.add_command(list_group)
cli.add_command(scrub_cmd)
cli.add_command(jobs_group)
cli.add_command(admin_group)


if __name__ == "__main__":
    cli()
