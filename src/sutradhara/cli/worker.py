"""`sutra worker` command for the lease-bounded job worker."""

from __future__ import annotations

import click

from sutradhara.catalog.session import make_engine
from sutradhara.jobs.config import WorkerConfig, parse_pool_overrides
from sutradhara.jobs.worker import JobWorker


@click.command("worker")
@click.option("--once", is_flag=True, help="Drain currently eligible jobs and exit.")
@click.option(
    "--pools",
    multiple=True,
    help="Override counted pool capacity, e.g. --pools cpu=8 --pools io=2.",
)
def worker_cmd(once: bool, pools: tuple[str, ...]) -> None:
    """Run the single-node lease-aware job worker."""
    try:
        config = WorkerConfig.defaults().with_pool_overrides(parse_pool_overrides(pools))
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    worker = JobWorker(make_engine(), config=config)
    if once:
        outcomes = worker.drain()
        click.echo(f"worker drained {len(outcomes)} job(s)")
        return
    recovered = worker.recover_orphans()
    click.echo("worker starting; press Ctrl-C to stop")
    if recovered:
        click.echo(f"recovered {recovered} orphaned running job(s)")
    try:
        while True:
            outcomes = worker.drain(recover_orphans=False)
            if not outcomes:
                import time

                time.sleep(1)
    except KeyboardInterrupt:
        click.echo("worker stopped")
