"""`sutra worker` command for the lease-bounded job worker."""

from __future__ import annotations

import click

from sutradhara.catalog.session import database_url, make_engine
from sutradhara.jobs.config import WorkerConfig, parse_pool_overrides
from sutradhara.jobs.worker import JobWorker
from sutradhara.jobs.worker_lock import WorkerAlreadyRunning, worker_lock
from sutradhara.resource_control import capability
from sutradhara.structured_logs import configure_structured_stdout_logging


@click.command("worker")
@click.option("--once", is_flag=True, help="Drain currently eligible jobs and exit.")
@click.option(
    "--pools",
    multiple=True,
    help="Override counted pool capacity, e.g. --pools cpu=8 --pools io=2.",
)
def worker_cmd(once: bool, pools: tuple[str, ...]) -> None:
    """Run the single-node lease-aware job worker."""
    configure_structured_stdout_logging()
    try:
        config = WorkerConfig.defaults().with_pool_overrides(parse_pool_overrides(pools))
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    try:
        url = database_url()
        with worker_lock(url):
            _echo_resource_control()
            worker = JobWorker(make_engine(url), config=config)
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
    except WorkerAlreadyRunning as exc:
        pid = "unknown" if exc.holder_pid is None else str(exc.holder_pid)
        raise click.ClickException(
            f"worker already running for this database; holder pid={pid}"
        ) from exc


def _echo_resource_control() -> None:
    cap = capability()
    if cap.mode == "systemd":
        click.echo(f"resource-control: systemd ({cap.manager})")
        return
    click.echo(f"resource-control: DEGRADED - {cap.reason or 'systemd scope unavailable'}")
