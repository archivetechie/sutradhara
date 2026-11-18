"""Run the real worker with a test handler that holds SQLite's writer slot.

The parent releases the attempt through a file, so stop handling must work
without any database write by the parent or the worker's signal handler.
"""

import sys
import time
from pathlib import Path

from sutradhara.cli.worker import worker_cmd
from sutradhara.jobs.registry import JobContext, JobResult, register_handler


@register_handler("_gs_block")
def block_until_released(ctx: JobContext) -> JobResult:
    """Flush a changed JSON value before announcing that the writer is held."""
    directory = Path(ctx.job.params["directory"])
    ctx.job.step_state = {**ctx.job.step_state, "gs_attempt": ctx.job.attempts}
    ctx.session.flush()
    (directory / f"started-{ctx.job.id}").touch()
    deadline = time.monotonic() + 30
    while not (directory / "release").exists():
        if time.monotonic() >= deadline:
            return JobResult(ok=False, detail="test did not release the attempt within 30 s")
        time.sleep(0.02)
    return JobResult(ok=True)


if __name__ == "__main__":
    worker_cmd.main(sys.argv[1:])
