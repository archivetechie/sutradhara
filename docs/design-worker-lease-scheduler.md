# Design — sutradhara job worker + resource-lease scheduler (single-node)

> Status: **design, for review** (the owner + Claude, 2026-06-16). Brainstormed off a
> three-generation comparison (d2 custom / d3 Prefect / sutradhara) — see memory
> `job-framework-lineage-lessons`. Decision taken: **single-node lease worker now**
> (one worker process, resource-lease-bounded pool, stay on SQLite). This doc is
> the design; a codex prompt follows after review. Per-repo: lives in
> sutradhara/docs because it is engine work.

## 1. Goal & non-goals
**Goal:** make the existing `jobs/` framework run jobs **concurrently and bounded**
on one box — so an I/O-light `pfr-index` job and a CPU-heavy `transcode` job run at
the same time without ever oversubscribing CPU/IO/drives. Turn the dead
`required_resources` / `prerequisites` columns into enforced behaviour, and add a
real worker.

**Non-goals (deliberately not now):** multi-node worker fleet; Postgres; a separate
control plane (server/UI/message broker); GPU scheduling; per-tenant fairness. The
lease model + handler contract are designed so the multi-node move later
(Postgres + `SELECT … FOR UPDATE SKIP LOCKED`) changes the *claim*, not the
handlers or the resource model.

## 2. Starting point (what's already true)
`jobs/` has: a `Job` table with `kind`/`params`/`status`/`step_state`/`attempts`/
`required_resources`/`prerequisites`, a `register_handler(kind)` registry, and a
**sequential** `run_pending` loop. `claim_pending` is **not atomic**, leases and
prerequisites are **stored but never read**, there is **no worker daemon and no
parallelism**. Spec `spec-v0.1.md §6.3–6.6` already designs exactly the scheduler
below; this builds it.

## 3. Resource-lease model (the one gate)
Resources are **counted pools** with a capacity; a job declares what it needs; the
scheduler dispatches a job only when **every** requested lease fits the remaining
budget, reserves on dispatch, releases on terminal.

- Pools (config-driven capacities): `cpu` (default = box logical cores),
  `io` (concurrent heavy sequential reads, e.g. 2–3), `tape_drive` (= physical
  drives), `gpu` (0/omitted for now).
- Job declaration: `required_resources: [{pool, count}]` — e.g.
  `transcode → [{cpu, 8}]`, `pfr-index → [{io, 1},{cpu, 1}]`,
  `cloud-blob → [{io, 1}]`.
- Invariant: `Σ leased[pool] ≤ capacity[pool]` at all times. "3 transcodes × 8
  threads on a 24-core box, plus N small pfr-index jobs" becomes an invariant, not
  a hope — and ffmpeg `-threads` is pinned to the leased `cpu` count so the
  subprocess honours exactly what it reserved (this is the fix for d2's uncapped-
  ffmpeg fire). Lease accounting is in-memory in the single worker process
  (authoritative because there is exactly one worker).

## 4. Worker + scheduler
**One worker process = one claimer thread + a bounded execution pool.**
- A single **claimer/scheduler thread** owns dispatch. Loop: scan PENDING jobs in
  order; for the first whose **prerequisites are all SUCCEEDED** *and* whose
  `required_resources` fit the remaining budget, **atomically claim** it
  (`UPDATE job SET status='running', started_at=…, attempts=attempts+1 WHERE
  id=? AND status='pending'`), reserve its leases, and submit its handler to the
  execution pool. Repeat until nothing else fits; then wait on a condition
  (job-finished or new-submit) and rescan.
- The **execution pool** is a `ThreadPoolExecutor` — correct for our work because
  the heavy jobs are **subprocesses** (ffmpeg / rem CLI) and Python releases the
  GIL while waiting on them; the pool just needs to be sized ≥ the max jobs the
  budget can admit. The *real* concurrency bound is the lease budget, not the pool
  size.
- On handler completion (success, `ok=False`, or exception) the worker **releases
  the leases** and applies the retry policy (§6). Because a single thread claims,
  there is no in-process claim race; the atomic `UPDATE` is belt-and-suspenders and
  forward-compatible with multi-worker Postgres later.
- **Prerequisites (DAG)** are now enforced at claim time (a job with an unmet
  prereq is skipped, not run). This is the fix for d2's "re-read-from-DB-before-save"
  race strategy — claim is atomic and prereqs are checked, not raced.

## 5. Handler contract
- `JobContext` gains the **granted leases** (so a handler knows its CPU budget).
  The `transcode` handler passes the leased `cpu` count to ffmpeg `-threads`
  (and x265 pools, etc.) — no hardcoded thread counts, no auto-threading
  oversubscription.
- Handlers stay plain `handle_x(ctx) -> JobResult`, registered by `kind`.
  `ok=True` means "the job machinery worked" (not "the check passed"), as today.
- Idempotency: handlers use `step_state` to skip already-done steps so a retried/
  resumed job doesn't redo finished work.

## 6. Retry/backoff & crash recovery
- **Retry:** on failure, if `attempts < max_attempts` (per-kind default, global
  fallback), re-enqueue to PENDING with a backoff delay; else terminal FAILED.
  Needs a `not_before`/`available_at` timestamp the claimer honours (small schema
  add). **[judgment call — see §11].**
- **Crash recovery:** on worker startup, any job left `RUNNING` is orphaned (single
  worker ⇒ nobody else owns it) → reset to PENDING; in-memory leases vanish with
  the process, so budget is reclaimed automatically. This is the fix for d2's
  "write/restore holding a tape indefinitely" stuck-job class.

## 7. Config
One config source (no drift — d3's lesson): pool capacities (`cpu`, `io`,
`tape_drive`, `gpu`), per-kind default `required_resources`, per-kind
`max_attempts`/backoff, execution-pool size. Capacities default from the box
(`cpu = os.cpu_count()`), overridable. Drive capacity comes from the tape layer.

## 8. Storage substrate
**SQLite now** (WAL). Correct because there is exactly one worker process = one
writer; no cross-process claim contention. The migration trigger to **Postgres +
`SELECT … FOR UPDATE SKIP LOCKED`** is "we need workers on more than one process/
box" — at which point only `claim` changes; the lease model, handlers, and config
are unchanged.

## 9. First consumers & sequencing
Phase R's compute jobs are the first consumers and the reason to build this now:
- `transcode` — `[{cpu, 8}]`, CPU-bound (one decode → mezz + preview).
- `pfr-index` — `[{io, 1},{cpu, 1}]`, **I/O-bound, parse-only (ffprobe, no decode)**;
  extracts header/footer/index → sidecar for high-bitrate PFR (see memory
  `pfr-pre-ingest-high-bitrate`). Reads the **original**, sibling of `transcode`.
- `cloud-blob` — `[{io, 1}]`, builds the one rao-aead intake object → S3.

**Sequencing:** this worker/lease piece lands **before** R's compute handlers
(they declare resources and run under it). R's *intake-scan + register* half is
synchronous and does **not** depend on the worker, so it can proceed in parallel;
only the job handlers wait on this. The R prompts will be updated to add
`pfr-index` to the job set and to have intake-scan enqueue all three kinds.

## 10. Lessons encoded (why this shape)
- **Resource leases as the single gate** — fixes d2's gating smeared across
  pool-size + per-task queues + hardcoded numbers, and its uncapped-ffmpeg fire.
- **Atomic claim + enforced prereqs** — fixes d2's years of races from
  re-read-before-save.
- **ThreadPool over subprocesses, one event loop of control** — avoids d3's
  `ConcurrentTaskRunner` "event loop is closed" failure.
- **No separate control plane; DB is the queue and the state authority** — avoids
  d3's server + shared Postgres + worker-fleet + compat-shim weight and its two
  disagreeing state machines.

## 11. Open / judgment calls for review
1. **Backoff schema:** add a `not_before` column for retry delay (clean), vs.
   immediate capped retry (no schema change, cruder). Lean: add the column.
2. **Head-of-line blocking:** strict FIFO (a big `cpu:8` job blocks the queue until
   it fits) vs. **work-conserving** (let a later small job that fits run first) with
   a simple aging guard against starving the big job. Lean: work-conserving + aging,
   but it's the one place this can get subtle — flag for your call.
3. **`io` pool semantics:** a hard concurrent-count cap (simple) vs. bandwidth-aware
   (overkill now). Lean: hard count cap.
4. **Scope of `tape_drive` now:** model it in the lease vocabulary now even though
   tape-write jobs arrive in a later phase (so the archive fan-out can adopt it
   without a model change), vs. add it when needed. Lean: define the pool now, wire
   real tape jobs later.
