# Codex prompt — job-engine safety-rails (sutradhara)

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`
> (single repo — no Shared-contract section).** Read `CLAUDE.md` + `AGENTS.md` first.
> **Source of the decisions: `~/system/docs/report-fable-review-hard-threads-2026-07-03.md`,
> Thread 2 ("job concurrency layer / reconciler spine"), findings 1/3/4/5/6 and the
> "Safety-rails codex prompt" bullet under *Actions*.** Companion design:
> `docs/design-reconciler-spine.md` §4.1/§5.5 (condition axes + the version-gated reopen).
>
> **What this is.** Six surgical safety-rails on the *already-built* lease worker +
> reconciler spine. The semantics (atomic guarded claim, two-axis conditions, NOT-EXISTS
> live-job gate) are correct and reviewed; this prompt closes the operational gaps that
> betray them — a live single-worker duplicate-execution hazard, an hdcache I/O fire that
> bypasses the one gate, silent resource-control degradation, a `blocked` black hole, and
> backoff/dedupe/never-fit rough edges. **No redesign. No new tables.** Every existing
> pytest stays green.

## What already exists — BUILD ON IT, do not rebuild
- **Lease worker** (`src/sutradhara/jobs/worker.py`): `JobWorker.drain` (`:54-82`) recovers
  orphans then loops `_claim_available` → `ThreadPoolExecutor.submit(_execute_job)`;
  `_claim_available` (`:84-112`) claims every lease-fitting candidate with **no bound tied
  to executor free slots**; `_mark_never_fits` (`:115-118`) sets a candidate `FAILED` with
  no attempt/condition row.
- **Engine** (`src/sutradhara/jobs/engine.py`): `submit` (`:46-91`) does a live-dedupe
  `SELECT` then insert (`:63-74`); `run_one` (`:94-186`) is the single terminal-path writer;
  `reset_orphaned_running_jobs` (`:289-299`) flips **all** RUNNING → PENDING;
  `apply_retry_policy` (`:302-320`) backs off with no jitter/clamp.
- **Config** (`src/sutradhara/jobs/config.py`): `RetryPolicy.delay_seconds` (`:33-37`) is a
  bare exponential; `WorkerConfig.defaults` (`:51-61`) sets `io` capacity = 2,
  `executor_workers = max(cpu, io, 1)`.
- **Leases** (`src/sutradhara/jobs/leases.py`): `LeaseManager.can_ever_fit` (`:63-64`),
  `fits`/`reserve` (`:59-72`). A job with **no** declared resources reserves `{}` and always
  fits — the claim-all hazard.
- **Conditions** (`src/sutradhara/jobs/reconcilers/conditions.py`): `record_condition`
  (`:92-155`) → give-up to `blocked` at `DEFAULT_BACKOFF_GIVE_UP_ATTEMPTS` (`:130-134`);
  `_default_backoff_due` (`:211-213`) is a bare exponential; `record_observation` holds
  `blocked` untouched (`HELD_CONDITIONS`, `:79-81`); only `present → satisfied`
  (`_mark_satisfied`, `:171-185`) exits `blocked`. `blocked_tool_name/version` columns
  (`models.py:168-169`) are **written, never read**.
- **Spine** (`src/sutradhara/jobs/reconcilers/spine.py`): `discover` (`:24-44`), `process`
  (`:47-75`) over `due_workable` (open/backoff only — `blocked` is never revisited),
  `reconcile` (`:78-90`), `gate_open` NOT-EXISTS live-job gate (`:117-145`).
- **hdcache fill** (`src/sutradhara/hdcache/fill.py`): `submit_hdcache_fill` (`:244-265`)
  submits with **no `required_resources`**, gated only by the internal `live_job_cap`
  (`DEFAULT_LIVE_JOB_CAP = 500`, `:67`; `HDCACHE_FILL_PRIORITY = 50`, `:65`; `JOB_KIND`, `:61`).
- **resource_control** (`src/sutradhara/resource_control.py`): `capability` (`:149-156`,
  cached once per process); `run_managed` (`:159-197`) degrades to nice/ionice at
  **WARNING** on every call (`:190-196`); `SUTRADHARA_RESOURCE_CONTROL` disables (`:500-502`).
- **Tool versions**: `handlers/transcode.py::_tool_version` (`:272-289`, ffmpeg) and
  `handlers/pfr_index.py::_tool_version` (`:166-183`, ffprobe) are **identical** helpers,
  and the source of `blocked_tool=("ffmpeg", …)` (`transcode.py:86`) /
  `("ffprobe", …)` (`pfr_index.py:67`).
- **CLI**: `cli/worker.py::worker_cmd` (`:19-44`) recovers orphans **before any lock** (loop
  path `:31`; `--once` recovers via `drain()`'s default at `:28`); `cli/reconcile.py::
  reconcile_cmd` (`:32-58`) runs one `reconcile()` pass.
- **Dedupe index**: partial-unique `uq_job_dedupe_key_live` (`models.py:192-198`) — a second
  live insert with the same `dedupe_key` raises `IntegrityError` at flush.

Transaction discipline is unchanged everywhere: **flush, never commit/rollback**; callers own
the transaction (same rule as `run_one` / `record_attempt` / `record_condition`).

---

## A. Worker singleton — OS flock, orphan-reset gated behind it (report finding 4)
A second `sutra worker` doubles the in-memory lease budget **and** its startup
`reset_orphaned_running_jobs` resets the *live* worker's RUNNING jobs → concurrent duplicate
execution. Fix with an OS advisory lock.

- New `src/sutradhara/jobs/worker_lock.py`: `worker_lock(engine_or_url) -> contextmanager`
  acquiring a **non-blocking `fcntl.flock(LOCK_EX | LOCK_NB)`** on a lockfile **derived from
  the DB path**. For a `sqlite:///…` URL, lockfile = `<sqlite-file>.worker.lock`; for any
  other URL, a deterministic path under a state dir keyed by a hash of the URL (document the
  fallback in the module docstring). Write the holder `pid` (and `hostname:pid`) into the
  lockfile on acquire.
- On contention, **fail fast**: raise `WorkerAlreadyRunning` carrying the holder pid read
  from the lockfile. `cli/worker.py` converts it to a `click.ClickException` with a clear
  message naming the pid, and exits **non-zero** — for **both** the loop path and `--once`.
- **`reset_orphaned_running_jobs` must run only after the lock is held.** Acquire the lock in
  `worker_cmd` as its *first* action, before constructing `JobWorker` / calling
  `recover_orphans` / `drain`. It must be impossible to reset a live worker's RUNNING jobs.
  (Do not put the lock inside `JobWorker.drain` — unit tests call `drain` directly on
  throwaway engines and must not need a lock. The lock is a property of the `sutra worker`
  process boundary; expose `worker_lock` so a scenario can exercise it directly.)

## B. hdcache_fill joins the one gate + bound claim-all (report finding 1)
- `submit_hdcache_fill` (`fill.py:257-265`) must declare
  `required_resources=[{"pool": "io", "count": 1}]` on the `submit(...)` call, so admission
  runs through the **one lease gate** (io capacity 2 ⇒ at most 2 concurrent fills), not a
  claim-all of up to `live_job_cap` no-resource jobs.
- **Demote `live_job_cap` to a documented backstop**: keep it functioning as a queue-depth
  bound, but reword its docstrings/comments (`HdcacheFillConfig.live_job_cap`,
  `DEFAULT_LIVE_JOB_CAP`) to say it is a **backstop on enqueue depth, not the admission
  gate** — the `io` lease is the gate.
- **Bound the worker claim** (defense-in-depth, independent of B's lease): `_claim_available`
  must never claim more jobs than the executor can actually start. Thread a `max_new` budget
  = `executor_workers − len(in-flight futures)` from `drain` into `_claim_available` and stop
  claiming at the budget. Claimed-but-unstarted RUNNING rows (the orphan-reset hazard that
  also inflates the RUNNING count) must not accumulate. Preserve the existing aging/blocked-
  scan break behavior (`:97-102`).

## C. Loud degradation + optional refuse (report finding 5)
- resource_control degraded mode must log at **ERROR, once per process** (guard a
  module-level flag under the existing `_CAPABILITY_LOCK`), replacing the per-call WARNING at
  `run_managed:190-196`. Centralize the decision so **every** degrade path routes through one
  helper: the capability-probe degrade (`run_managed` fall-through `:191-197`), the
  systemd-launch `OSError` (`:187-190`), and the setup-failure retry (`:318-324`).
- New env `SUTRADHARA_RESOURCE_CONTROL_REQUIRE=1` makes that helper **raise** a new
  `ResourceControlUnavailable` (with `cap.reason`) instead of degrading — for all three paths.
- Surface capability at worker startup: after acquiring the lock (A), `cli/worker.py` echoes
  one line from `capability()` — e.g. `resource-control: systemd (user)` or
  `resource-control: DEGRADED — <reason>` — on **both** loop and `--once`. This is the cheaper
  choice than a new `--status` verb; **do not** add `--status`.

## D. Blocked-condition liveness — operator verbs + version-gated auto-reopen (finding 3; design §4.1/§5.5)
`blocked` is currently terminal except via `present → satisfied`; neither `discover` nor
`process` ever revisits a `blocked` row (it has `next_eligible_at = NULL` and is not
open/backoff). Two exits, one shared reopen routine.

- **Shared reopen** in `conditions.py`: `reopen_condition(session, row, *, actor, note)` —
  `blocked → open`, clear `reason`/`message`/`blocked_tool_name`/`blocked_tool_version`, set
  `attempt_count = 0` and `next_eligible_at = now` (a reopened target must be immediately
  workable, §5.5), and record who/when in the row `message` (e.g.
  `reopened by <actor> at <iso> (was blocked: <old-reason>)`). Flush only.
- **Operator verbs** on `cli/reconcile.py` (same `reconcile <domain>` command group):
  - `--list-blocked` — print each blocked condition for the domain:
    `target_key`, `reason`, `blocked_tool_name`, `blocked_tool_version`, `since` (`updated_at`).
  - `--reopen-blocked [--reason <filter>]` — reopen all blocked conditions for the domain
    (optionally only those whose `reason` matches the filter) via `reopen_condition`
    (`actor = getpass.getuser()`), print the count. These verbs must be **mutually exclusive
    with running a reconcile pass** (they act and return; do not also discover/process).
- **Version-gated auto-reopen (design §4.1/§5.5 — implement it)**: a `blocked` condition whose
  `blocked_tool_name` is set and whose **current** tool version differs from the stored
  `blocked_tool_version` reopens automatically.
  - New `src/sutradhara/jobs/tool_versions.py`: a small **provider registry** mapping a tool
    name → a `() -> str` version provider. **Extract the duplicated `_tool_version` logic**
    from `transcode.py`/`pfr_index.py` into this module (a single
    `current_tool_version(tool: str) -> str`) and have both handlers import it (register
    `ffmpeg`, `ffprobe`). `current_tool_version` returns `"unknown"` on missing tool/timeout,
    exactly as today.
  - New `reopen_version_bumped(session, domain) -> int` (in `spine.py` or a small
    `reconcilers/reopen.py`): scan `blocked` conditions for the domain with non-null
    `blocked_tool_name`; for each, if `current_tool_version(name) != blocked_tool_version` and
    the current version is not `"unknown"`, call `reopen_condition(actor="version-bump", …)`.
  - Call it at the **start of `spine.reconcile()`** (before `discover`) so a tool upgrade
    self-heals the backlog on the next scheduled pass. Bounded (the blocked set is small).
- **Design status note**: in `docs/design-reconciler-spine.md` §4.1, update the
  `blocked_tool_*` column comment's status from "version-gated re-open (…)" to note it is
  **implemented** (one line — e.g. append `— implemented 2026-07 via reopen_version_bumped`).
  Do **not** touch `docs/INDEX.md` (the session lead owns indexes).

## E. Hygiene batch (report finding 6)
- **Jitter + clamp** on both backoff computations: `RetryPolicy.delay_seconds`
  (`config.py:33-37`) and `_default_backoff_due` (`conditions.py:211-213`). Apply **±20%
  random jitter** and a **clamp to a configurable maximum (default 1h)**. Put the max in
  `WorkerConfig` (e.g. `max_backoff_seconds`, default 3600) and thread it to the condition
  helper (or add a module default there mirroring the config). Jitter must keep the result
  within `[base*0.8, min(base*1.2, max)]`.
- **Dedupe-insert `IntegrityError`**: `submit` (`engine.py:63-91`) must handle a concurrent
  insert violating `uq_job_dedupe_key_live` gracefully — catch `IntegrityError` on the
  flush, re-query the live row for the `dedupe_key`, and **return the existing job** (no
  crash). Preserve the existing pre-check fast path.
- **Never-fit jobs record provenance** instead of a silent `FAILED`: replace
  `_mark_never_fits` (`worker.py:115-118`) so a `can_ever_fit == False` candidate records a
  **`job_attempt`** (stamp `started_at`/`finished_at = now` and set the terminal error before
  `record_attempt`, since the job was never claimed) **and**, when reconciler-backed
  (`recon_domain` set), a **condition** via the engine's condition path with
  `CONDITION_BACKOFF, reason="never-fit"`. The job still ends terminal for this attempt, but
  the transcript and the condition now explain why (and a reconciler target backs off / gives
  up rather than vanishing). Reuse `record_attempt` and the `_record_reconciler_condition`
  discipline — do not invent a parallel writer.

## F. Forward-migration note (report finding 3, migration trap)
Add a short **"Forward migration — copy write port"** section stating: every `copy` target the
current `NotImplementedError` stub drove to `blocked(not-implemented)` stays blocked and will
**not** self-heal when the real write handler lands (the stub never wrote a `blocked_tool_*`,
so version-gated reopen cannot rescue it). Therefore the future copy-write-handler prompt
**must include `sutra reconcile copy --reopen-blocked --reason not-implemented` as an explicit
post-deploy step** to drain the stranded backlog. This is a note for the copy-handler prompt
author; implement nothing else for it here.

---

## Non-goals (do NOT do these)
- **No** multi-worker / Postgres / `SKIP LOCKED` / claimable-worklist work — the claim is
  already abstracted for that swap; the flock is single-node only.
- **No** wakeup pipeline (`domain_event` / `reconciliation_wakeup` tables), no in-process
  scheduler loop or daemon, no lease **persistence**.
- **No** production cadence / systemd units / bringup wiring — that is the sibling
  `~/system/docs/prompt-jobs-cadence-harness.md`.
- **No** `flush_bundle` / intake lease routing (explicitly a separate decision, out of scope).
- **No** hdcache M5/M6 scope — including the second uncounted concurrency window in
  `hdcache/manager.py` (`_serve_restore_request_parallel`, `:565+`). Do not touch it.
- **No** schema/migration changes. Every change here is behavioral over the existing tables.

## Tests — add these (extend the nearest existing suite; keep every current test green)
- `tests/test_worker_lock.py`
  - **two-process flock denial**: spawn a real second `sutra worker --once` (and once for the
    loop form) via `subprocess` against the same DB while a lock is held → non-zero exit,
    message names the holder pid.
  - **orphan reset gated**: with the lock held by a stand-in holder, a second worker start
    does **not** reset RUNNING jobs; with the lock free, startup resets orphans exactly as
    today.
- `tests/test_jobs.py` / `tests/test_worker.py` (extend)
  - **hdcache_fill lease**: `submit_hdcache_fill` writes `required_resources` with `io:1`;
    with `io` capacity 1, two due fills **serialize** (one RUNNING at a time).
  - **claim bounding**: with `executor_workers = k` and `k+N` due no-lease-cost jobs, one
    `_claim_available` pass claims at most the free-slot budget (no `k+N` RUNNING pileup).
  - **never-fit**: a candidate requiring more than capacity records a `job_attempt` +
    (reconciler-backed) a `backoff/never-fit` condition; it is not a bare silent `FAILED`.
- `tests/test_resource_control.py` (extend)
  - **REQUIRE raises**: with `SUTRADHARA_RESOURCE_CONTROL_REQUIRE=1` and systemd unavailable,
    `run_managed` raises `ResourceControlUnavailable`; without it, degrades and logs ERROR
    **once** across repeated calls.
- `tests/test_reconciler_conditions.py` / `tests/test_reconcile_spine.py` (extend)
  - **reopen verbs**: `--list-blocked` lists a blocked row's fields; `--reopen-blocked`
    (and `--reason` filter) moves `blocked → open`, clears backoff/blocked_tool fields, sets
    `next_eligible_at = now`, records actor/when.
  - **version-bump reopen**: a `blocked` row with `blocked_tool_name`/`_version` set; a fake
    provider returning a **different** version → `reopen_version_bumped` reopens it; **same**
    version (and `"unknown"`) → left blocked.
  - **jitter/clamp bounds**: repeated backoff computations stay within
    `[base*0.8, min(base*1.2, max)]` and never exceed the 1h clamp.
- `tests/test_jobs.py` (extend)
  - **IntegrityError path**: a forced concurrent duplicate live insert returns the existing
    job rather than raising.

## Verification
- `cd ~/sutradhara/repo && uv run pytest -q` — **green**, including every pre-existing test
  (the six rails are additive/behavioral; nothing about the claim/lease/attempt/condition
  contract changes shape).
- Editable-dep trap (from `CLAUDE.md`/memory): `~/system`'s `make scenario-*` imports
  sutradhara from the **working-tree branch** via an editable install. **Land this complete on
  `main`** or the harness silently regresses (`ModuleNotFoundError` / stale behavior). Commit
  at green milestones; direct-to-main, no PRs; never ask the operator to do hygiene.

## Acceptance criteria
1. A second `sutra worker` (loop or `--once`) exits non-zero naming the holder pid; orphan
   reset provably cannot run without the lock.
2. `submit_hdcache_fill` declares an `io:1` lease; `live_job_cap` is documented as a backstop;
   `_claim_available` never exceeds the executor free-slot budget.
3. Degraded resource-control logs ERROR once per process and is printed at worker startup;
   `SUTRADHARA_RESOURCE_CONTROL_REQUIRE=1` raises instead of degrading.
4. `sutra reconcile <domain> --list-blocked` / `--reopen-blocked [--reason …]` work; a tool
   version bump auto-reopens matching blocked conditions on the next `reconcile()`;
   `design-reconciler-spine.md` §4.1 status note updated (one line).
5. Backoff (retry + condition) carries ±20% jitter and a ≤1h clamp; dedupe `IntegrityError`
   returns the existing job; never-fit jobs record an attempt + condition.
6. Forward-migration note present naming the `--reopen-blocked --reason not-implemented` step.
7. `uv run pytest -q` green; the diff gate (independent review of the actual diff, per
   `AGENTS.md`) has something detailed to review — leave a clear implementation summary.
