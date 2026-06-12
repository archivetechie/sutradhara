"""Job engine — submit, run, track work.

Day-1 vertical slice (spec §12.3 step 1):
  - Job table + minimal in-process queue
  - Handler registry (register_handler decorator, dispatch by kind)
  - First handler: `verify` (delegates to backend.verify())

What's intentionally not yet here (later slices, spec §6 of the
project spec):
  - Resource pools (tape_drive, gpu, cpu) — required_resources is
    in the model for forward-compatibility, but the scheduler
    ignores it.
  - DAG / prerequisites — same: column exists, scheduler doesn't read.
  - Worker fleets via `sutra worker --pools` — only a synchronous
    in-process runner today.
  - Retry with backoff — `attempts` counter exists, but no auto-retry.
  - Cancellation — column value defined, no cancel API yet.
"""
