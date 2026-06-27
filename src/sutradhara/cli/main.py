"""Sutradhara CLI entry point."""

from __future__ import annotations

import click

from sutradhara import __version__
from sutradhara.cli.admin import admin_group
from sutradhara.cli.archive import archive_group, review_cmd
from sutradhara.cli.arrangement import arrangement_group
from sutradhara.cli.assets import list_group
from sutradhara.cli.backends import backends_group
from sutradhara.cli.db import db_group
from sutradhara.cli.intake import intake_group, prepare_cmd
from sutradhara.cli.jobs import jobs_group
from sutradhara.cli.receive import receive_group
from sutradhara.cli.reconcile import reconcile_cmd
from sutradhara.cli.scrub import scrub_cmd
from sutradhara.cli.virtual import reject_cmd, tag_group, unreject_cmd, virtual_group
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
cli.add_command(prepare_cmd)
cli.add_command(jobs_group)
cli.add_command(receive_group)
cli.add_command(reconcile_cmd)
cli.add_command(worker_cmd)
cli.add_command(admin_group)
cli.add_command(archive_group)
cli.add_command(review_cmd)
cli.add_command(arrangement_group)
cli.add_command(virtual_group)
cli.add_command(reject_cmd)
cli.add_command(unreject_cmd)
cli.add_command(tag_group)


if __name__ == "__main__":
    cli()
