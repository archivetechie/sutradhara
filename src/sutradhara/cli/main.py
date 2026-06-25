"""Sutradhara CLI entry point."""

from __future__ import annotations

import click

from sutradhara import __version__
from sutradhara.cli.admin import admin_group
from sutradhara.cli.archive import archive_group, review_cmd
from sutradhara.cli.assets import list_group
from sutradhara.cli.backends import backends_group
from sutradhara.cli.db import db_group
from sutradhara.cli.intake import intake_group
from sutradhara.cli.jobs import jobs_group
from sutradhara.cli.receive import receive_group
from sutradhara.cli.reconcile import reconcile_cmd
from sutradhara.cli.scrub import scrub_cmd
from sutradhara.cli.worker import worker_cmd


@click.group()
@click.version_option(__version__, prog_name="sutra")
def cli() -> None:
    """Sutradhara — orchestrator above Remanence."""


cli.add_command(db_group)
cli.add_command(backends_group)
cli.add_command(list_group)
cli.add_command(scrub_cmd)
cli.add_command(intake_group)
cli.add_command(jobs_group)
cli.add_command(receive_group)
cli.add_command(reconcile_cmd)
cli.add_command(worker_cmd)
cli.add_command(admin_group)
cli.add_command(archive_group)
cli.add_command(review_cmd)


if __name__ == "__main__":
    cli()
