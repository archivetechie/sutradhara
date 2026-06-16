# Codex prompt — sutradhara job worker + resource-lease scheduler

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`.**
> **Source of truth:** `docs/design-worker-lease-scheduler.md` (read it first — it
> carries the rationale and the d2/d3 lessons). Read `CLAUDE.md` + `AGENTS.md` too.
> This builds the **execution core**; Phase R's `transcode`/`pfr-index`/`cloud-blob`
> handlers (separate prompts) are its first consumers and land after it.

## What exists — build on it, do not rebuild
`src/sutradhara/jobs/`: `models.py` (`Job` with `kind`/`params`/`status`/`step_state`/
`attempts`/`required_resources`/`prerequisites`/timestamps; `JobStatus`
PENDING/RUNNING/SUCCEEDED/FAILED + reserved QUEUED/CANCELLED), `engine.py`
(`submit`/`run_one`/`claim_pending`/`run_pending` — **sequential, non-atomic
claim**), `registry.py` (`register_handler`/`get_handler`, `JobContext`,
`JobResult`), `handlers/` (`verify` live; `copy`/`restore` stubs), `cli/jobs.py`.
`required_resources` + `prerequisites` are **stored but never read** today — this
prompt makes them enforced. SQLite via `catalog/session.py` (WAL). The spec design
is `docs/spec-v0.1.md §6.3–6.6`.

## The work
Implement the single-node lease worker exactly as the design specifies. In order,
test each step, commit per step.

1. **Schema additions** (use the repo's alembic/migration convention):
   - `Job.not_before` (tz datetime, default = created_at) for retry backoff +
     scheduling delay.
   - `Job.priority` (small int, default 0) for the work-conserving/aging scan.
   - A uniqueness mechanism for **enqueue idempotency**: a nullable
     `Job.dedupe_key` (str, unique-when-present) so `submit` can be made
     idempotent on a caller-supplied key (e.g. `transcode:<ingest_item_id>`).
   - `LogicalAsset.validity` (`ok` | `suspect` | `unvalidated`, default per existing
     rows = `unvalidated` or `ok` — pick and document) + a `validity_note` (text,
     nullable) for the condition/diagnostic. (Content-level property — on the
     content-addressed asset, not per-occurrence.)

2. **Resource-lease scheduler** (`jobs/leases.py` or in `engine.py`):
   - Counted pools from config: `cpu` (default `os.cpu_count()`), `io` (hard count
     cap, default 2–3), `tape_drive` (capacity from the tape layer / config — define
     the pool now even with no tape jobs yet), `gpu` (default 0).
   - In-memory live tally of leased units per pool (authoritative — single worker).
   - `fits(required_resources)` and reserve/release. A job is dispatchable iff every
     `{pool,count}` fits the remaining budget.

3. **Atomic claim** (`engine.py`): replace `claim_pending` with a claim that selects
   **and** flips `PENDING→RUNNING` in one statement (SQLite: guarded
   `UPDATE … WHERE id=(SELECT … LIMIT 1) AND status='pending'`), respecting
   `not_before`, **prerequisites all SUCCEEDED**, and lease-fit. Keep the SQL shaped
   so the later Postgres `FOR UPDATE SKIP LOCKED` swap touches only this function.

4. **Worker** (`jobs/worker.py` + `sutra worker` CLI in `cli/`):
   - One **claimer thread**: scan eligible PENDING jobs in (priority, created_at)
     order; **work-conserving** — skip a job whose leases don't fit and try later
     ones that do — with **aging** (a job that has waited ≥ N scans blocks smaller
     jobs from jumping it, so a big `cpu:8` job can't starve). Claim atomically,
     reserve leases, submit the handler to a `ThreadPoolExecutor` (sized ≥ max
     admissible concurrency; the lease budget is the real bound).
   - On completion (success / `ok=False` / exception): release leases; apply retry
     (below); flush so the next claim sees the transition; wake the claimer.
   - `sutra worker [--once] [--pools cpu=…,io=…]` runs the loop (the harness uses
     `--once`/a drain mode; production runs it as a daemon). Keep a `run_pending`-
     style synchronous drain for tests.

5. **Prerequisite (DAG) enforcement**: a job is eligible only when every id in
   `prerequisites` is SUCCEEDED. (This is the "wait for all N" primitive; no
   rolled-up status anywhere.)

6. **Retry/backoff + crash recovery**:
   - On failure, if `attempts < max_attempts` (per-kind default in config, global
     fallback) re-enqueue PENDING with `not_before = now + backoff(attempts)`; else
     terminal FAILED. (`attempts` already increments in `run_one`.)
   - On worker **startup**, reset any `RUNNING` job to PENDING (orphaned — single
     worker); in-memory leases are naturally gone.

7. **Idempotent `submit`**: honour `dedupe_key` — if a non-terminal job with the same
   key exists, return it instead of inserting a duplicate.

8. **`JobContext` carries the granted leases** so a handler knows its budget (e.g.
   the future `transcode` handler reads its `cpu` count to pass ffmpeg `-threads`).

9. **`validate` reference handler** (`handlers/validate.py`, `@register_handler
   ("validate")`): a thin, generic decode/parse validation that sets
   `LogicalAsset.validity` (and writes the diagnostic). It is the template the R
   `transcode` handler follows for its decode-error path, and it covers non-proxied
   types. Distinguish **read error (unreadable file)** from **decode error
   (invalid content → `suspect`)** — only the latter sets `suspect`.

10. **Restore-gate**: in the restore path (`archive_restore.py` / `cli` restore),
    **refuse a normal restore of a `suspect` asset** (clear message) unless an
    explicit `--force`/expert flag is given; surface the validity + note. Keep it a
    single check so all restore entry points share it.

## Tests (DoD gate — paste output)
- Lease enforcement: 3 jobs declaring `cpu:8` on a `cpu:24` pool run **3 at once**;
  a 4th waits; with `cpu:16` only 2 run. An `io:1`-cap holds io-jobs to the cap.
- Work-conserving + aging: a small job runs ahead of a non-fitting big one; the big
  one is not starved past the aging threshold.
- Atomic claim: a claimed job flips to RUNNING; (simulate) a second claim does not
  re-grab it.
- Prerequisites: a job with an unmet prereq is **not** run; becomes eligible when all
  prereqs SUCCEEDED.
- Retry/backoff: a failing job re-enqueues up to `max_attempts` honouring
  `not_before`, then FAILED.
- Crash recovery: a `RUNNING` job at startup resets to PENDING.
- Idempotent submit: same `dedupe_key` twice → one job.
- Validation: `validate` sets `validity=suspect` on a decode-invalid fixture and
  leaves `ok` on a clean one; an unreadable file errors as a read error, **not**
  `suspect`.
- Restore-gate: normal restore of a `suspect` asset is refused; `--force` allows it.
- Existing job/verify/restore tests + the whole suite stay green.
- `pytest`, `ruff`/format, type-check — paste.

## Constraints
- Don't break existing handlers (`verify` works; `copy`/`restore` stay as they are
  except the restore-gate). No change to the archive bundling/fan-out APIs.
- One config source for pools/limits (no drift). SQLite stays; shape the claim SQL
  for a clean Postgres `SKIP LOCKED` swap later.
- DoD per `AGENTS.md`; commit per step; never leave the tree dirty; update the
  `docs/INDEX.md` row. **This doc and the design live in this repo's `docs/`.**

## DoD
- `sutra worker` runs the lease-bounded concurrent loop; leases/prereqs/retry/
  recovery/validity-gate all enforced and tested (paste). Report what's covered and
  any limit (e.g. SQLite single-writer) carried forward.
