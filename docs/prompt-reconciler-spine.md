# Codex prompt — P0.3: the reconciler spine (condition table + reconcile loop + reference copy reconciler)

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo` (single repo —
> no Shared-contract section).** Read `CLAUDE.md` + `AGENTS.md` first.
>
> **Authoritative design: `docs/design-reconciler-spine.md` — read it in full before writing
> code.** This prompt is the build order, the contracts that must be exact, and the acceptance
> tests; the design doc is the *why* for every decision. Source plan item: P0.3 in
> `docs/implementation-plan-ingest-v2.md`. Contract: `docs/design-reconciliation-model.md` (the
> model — desired-state, jobs-as-attempts, R1–R4), as refined by the spine doc §5.5 (two-axis
> condition).
>
> **This is an additive change.** It introduces a controller alongside the existing imperative
> `replication.self_heal`, which it does **not** touch or reroute. Existing behaviour is
> unchanged: the lease/retry/crash tests and **scenario Q (self-heal) stay green**.
>
> **Scope discipline — read this twice.** P0.3 ships **ONE new table**
> (`reconciliation_condition`) and a sweep-only, single-node `reconcile()` loop whose worklist is
> the condition index. **Do NOT build `domain_event` or `reconciliation_wakeup`** — they are
> documented as deferred in design §10 (they only earn their keep at multi-worker / edge-nudges).
> Building them now is out of scope and wrong.

## Why (one paragraph, then build)
Copy placement already reconciles desired-vs-observed imperatively (`replication.py`:
`target_pools` = desired, `_healthy_copies` = observed, `self_heal` closes the gap) — but only
when something *calls* `self_heal`, with no durable record of *why* a gap is or isn't being
worked, and no level-triggered guarantee. P0.3 supplies the missing controller: a durable
per-`(domain, target_key)` **condition** (the compact projection of the P0.2 attempt log + observed
reality) and a `reconcile()` loop that finds gaps and enqueues jobs to close them. The reference
consumer is **copies**; the gap-closing `copy` job is a deliberate **stub** (the byte-write is a
Remanence seam), so P0.3 proves the *controller*, not the mover — exercised end-to-end in tests
via a working memory-backend copy handler.

## What already exists — BUILD ON IT, do not rebuild
- **`Job` / `JobResult` / `run_one`** (`src/sutradhara/jobs/`): `run_one` claims a job, runs the
  handler, sets terminal status on three paths (`HandlerNotRegistered`, handler raised, normal
  return), calls `record_attempt(...)` **after** the handler on each path, returns `JobResult`
  (`ok`, `detail`, `step_state`). Caller commits. **`run_one` creates the `job_attempt` after the
  handler — the handler never holds the attempt** (this is why the condition's attempt-axis is
  written by `run_one`, not the handler — design §5.5).
- **`submit(session, kind, params, *, …, dedupe_key=…)`** (`engine.py`): creates a PENDING job;
  already has the Phase-1 live-only `dedupe_key` guard. Caller commits.
- **Worker** (`jobs/worker.py::_execute_job`): runs `run_one`, then `apply_retry_policy(session,
  job, config)` which may flip a FAILED job back to PENDING per `config.retry_for_kind(kind)`.
- **`apply_retry_policy`** (`engine.py`): re-pends a FAILED job if `attempts < retry.max_attempts`.
- **`replication.py`**: `target_pools(session, artifactclass, backends, *, key_epoch)` →
  `[(backend, PoolTarget)]` (raises `ReplicationPolicyMissing` if a class has no active pool
  memberships); `_healthy_copies(session, asset_hash)` → `[Copy]` (filters `logical_asset_hash` +
  `CopyHealth.OK` — **backend-free**); `replication_status(...)` (needs backends).
- **Catalog**: `ArtifactClassPool` (active membership artifactclass→pool, with `sort_order`),
  `Pool` (→ `Backend`), `Copy` (scoped to `logical_asset_hash` XOR `bundle_id`; has `pool_id`,
  `backend_id`), `IngestItem` (`intake_id` FK, `artifactclass`, `logical_asset_hash`), `Intake`
  (`status` ∈ receiving/verifying/quarantined/**registered**, has `artifactclass`).
- **`copy` handler** (`jobs/handlers/copy.py`): a **stub** that raises `NotImplementedError`.
  **Leave it as-is** — do not implement the byte-write.
- **Migration head**: chain the new revision from **`af859d4ffb71`** (the P0.2 `job_attempt`
  revision). Tests build schema via `create_all`, so models must stand alone.

## Build order

### A. Schema + migration (one new revision, chained from `af859d4ffb71`)
1. **`reconciliation_condition`** (new model in `jobs/models.py`) — design §4.1, exactly:
   `id` PK; `domain` str not null; `target_key` str not null; `observed_state` str **not null**;
   `condition` str not null; `reason` str null; `message` text null; `attempt_count` int not null
   default 0; `next_eligible_at` tz datetime null; `blocked_tool_name`/`blocked_tool_version` str
   null; `last_attempt_id` FK `job_attempt.id` null **ON DELETE SET NULL**; `last_attempt_at` tz
   null; `last_success_at` tz null; `updated_at` tz not null. **`UNIQUE (domain, target_key)`** and
   **`INDEX ix_condition_work (domain, condition, next_eligible_at)`** (the worklist index).
2. **Two nullable `job` columns** — `recon_domain` str null, `recon_target_key` str null, set only
   by a reconciler enqueue. Partial index `(recon_domain, recon_target_key) WHERE status IN
   ('pending','running','queued')` (backs the gate's live-job lookup).

### B. `JobResult.condition` channel + `ConditionProjection`
- Add `condition: ConditionProjection | None = None` to `JobResult` (`registry.py`). `None` default
  → every existing handler unaffected.
- `ConditionProjection` = frozen dataclass carrying **only Axis-B** fields: `condition: str`,
  `reason: str | None`, `message: str | None`, `next_eligible_at: datetime | None`,
  `blocked_tool: tuple[str, str] | None`. **No `observed_state`** (that's Axis A only).

### C. `jobs/reconcilers/` package
- **`registry.py`** — `Reconciler` frozen dataclass `(enumerate_targets, observe,
  reconcile_target)` + `register_reconciler(domain)` decorator + `get_reconciler(domain)` (mirror
  `jobs/registry.py`). `TargetObservation = (target_key: str, desired: bool, observed_state: str)`.
- **`conditions.py`** — the two no-commit helpers (flush only, never commit/rollback):
  - `record_observation(session, *, domain, target_key, desired, observed_state)` — **Axis A**,
    **upsert** (it always has `observed_state`). Applies the §5.5 reality rules (below).
  - `record_condition(session, *, domain, target_key, condition=None, reason=None, message=None,
    attempt=None, next_eligible_at=None, blocked_tool=None)` — **Axis B**, **UPDATE only; raise an
    invariant error if the row is absent** (it has no `observed_state` to insert; the row is
    guaranteed to exist because `process` ran `record_observation` before enqueuing).
- **`spine.py`** — `discover(session, domain, *, batch, cursor)`, `process(session, domain, *,
  limit)`, `reconcile(session, domain)` orchestrator (discover then process), and `gate_open(...)`.
- **`copy.py`** — the copy reconciler, `@register_reconciler("copy")`.

### D. `run_one` writes the attempt axis (engine.py)
After `record_attempt(...)` on **every** terminal path, **if `job.recon_domain` is set**, write
Axis B keyed by `job.recon_domain`/`job.recon_target_key` via `record_condition(..., attempt=the
just-recorded attempt, …)` per the §5.5 defaults (below). Jobs with `recon_domain IS NULL` flow
through unchanged.

### E. Worker skips retry for reconciler jobs (worker.py)
In `_execute_job`, **only call `apply_retry_policy` when `job.recon_domain IS NULL`.** Reconciler
jobs are single attempts; the condition backoff is their sole retry cadence. One-line guard.

### F. `submit()` passthrough (engine.py)
Add `recon_domain: str | None = None`, `recon_target_key: str | None = None` kwargs, set on the new
`Job`.

### G. CLI `sutra reconcile <domain>`
A command that runs `reconcile(session, domain)` in a `session_scope`, mirroring how the existing
scrub/self-heal CLI is wired. No daemon/loop.

## Contracts that MUST be exact (do not paraphrase — design §5.4/§5.5/§6)

**Two-axis condition (§5.5).** The condition is written on two disjoint axes:
- **Axis A — `record_observation` (reality).** Given `(desired, observed_state)`:
  - `desired ∧ present` → `satisfied`. **On the transition into `satisfied`** (and the `¬desired`
    inert case): clear Axis-B diagnostics (`reason`, `message`, `blocked_tool_*`), **reset
    `attempt_count = 0`**, set `next_eligible_at = NULL`.
  - `desired ∧ ¬present` → if condition is `satisfied` or absent, set `open` with
    `next_eligible_at = now`. **If condition is already `backoff`, `blocked`, or `suppressed`,
    leave it ENTIRELY untouched** — including a `backoff` that is now *due* (the gate enqueues a due
    backoff directly; flipping it to `open` would reset the retry cycle — that was a real bug).
  - `¬desired` → `satisfied`/inert.
  - Axis A **never** sets `backoff`/`blocked`, and **never resets `attempt_count` except on
    `satisfied`** (a `satisfied→open` regression inherits 0 from the prior satisfied).
- **Axis B — `run_one` post-attempt via `record_condition`:**
  - handler returned `JobResult.condition` → apply it.
  - raised `NotImplementedError` → `blocked` (`reason="not-implemented"`). *(This is the stub's
    path — it lands a clean terminal condition without touching the stub.)*
  - raised any other exception, or kind not registered → `backoff`, `attempt_count += 1`,
    `next_eligible_at = now + backoff(attempt_count)`.
  - `ok=False` with no projection → `backoff` (`reason="unclassified"`), `attempt_count += 1`.
  - `ok=True` → `record_condition(condition=None, attempt=…)`: set `last_success_at` + attempt
    links **only** — **no state transition, and do NOT reset `attempt_count`** (`satisfied` is Axis
    A's call after the next observe confirms `present`; a *false* success must not wipe the failure
    streak).
  - Axis B **never** sets `satisfied` and **never** writes `observed_state`.
- **Due-time invariant:** whenever any writer sets `condition ∈ {open, backoff}` it sets
  `next_eligible_at` non-null (`now` for open, `now + backoff` for backoff) — so the gate/worklist
  is a uniform `next_eligible_at <= now` with no `IS NULL` branch.

**`gate_open` (§5.4):** `desired ∧ condition IN ('open','backoff') ∧ next_eligible_at <= now ∧ NOT
EXISTS (SELECT 1 FROM job WHERE recon_domain=:d AND recon_target_key=:tk AND status IN
('pending','running','queued'))`. The live-job term queries the **`recon_*` columns**, NOT a
`dedupe_key` prefix.

**`process` (§5.3):** for each `due_workable` condition (`ix_condition_work`: `condition ∈
{open,backoff} ∧ next_eligible_at <= now`), **re-observe** the single target, `record_observation`,
then `gate_open` → if open, `reconcile_target`. Hot path = the condition index + the keyed row +
the single-target observe (all indexed); **no full-table scan**.

**`discover` (§5.2):** enumerate the live population in bounded `cursor` batches; `observe` each;
`record_observation`. Catches new gaps, regressions, policy changes. Does **not** enqueue.

### Copy reconciler specifics (§6) — these are easy to get wrong
- **The reconciler runs on CATALOG ROWS, not the runtime `backends` map.** `target_pools()` /
  `replication_status()` require `backends` and resolve write backends — **do not use them in the
  observe/discover/process path.** Instead:
  - **desired pool_ids** for a class = the active `ArtifactClassPool` memberships (the pool_ids).
    Factor this out of `target_pools` (the membership query *before* backend resolution) into a
    reusable backend-free helper, or query `ArtifactClassPool` directly. A class with no active
    memberships → treat as `ReplicationPolicyMissing` (empty set).
  - **observed pool_ids** = `{c.pool_id for c in _healthy_copies(session, asset_hash)}` (already
    backend-free).
- **`target_key = f"asset:{sha256_hex}:{pool_id}"`** (scope-tagged, design §4.2/§6.1).
- **Live membership (schema-accurate):** `IngestItem JOIN Intake ON IngestItem.intake_id =
  Intake.intake_id WHERE Intake.status = 'registered'`, class = `IngestItem.artifactclass`.
- **Desired = the UNION over ALL the asset's live class memberships:** `desired(pool) = pool ∈
  ⋃_{c ∈ live_classes(asset)} active_pool_ids(c)`. A class raising `ReplicationPolicyMissing`
  contributes `∅`. **This is load-bearing for shared assets** — an asset live under classes A *and*
  B must not close a pool B still requires when A drops it.
- **`observe(target_key)`** (concrete key `asset:{sha}:{pool}`): `desired = pool ∈ union(…)`,
  `observed_state = present if pool ∈ observed_pool_ids else missing`. **Never raises** — a true
  policy-shrink makes `desired=False` and Axis A closes the row.
- **`enumerate_targets`** (discovery): forms per-pool target keys from the union; if a class raises
  `ReplicationPolicyMissing` it forms **no** new targets for it and emits a **class-level
  configuration diagnostic** (logged + counted: "class X registered but no active pools — N items
  cannot be reconciled"); it never aborts the batch.
- **`reconcile_target(target_key)`**: resolve the missing pool's backend NAME from the catalog
  (`Pool.backend.name` — a catalog lookup, not the runtime map) and
  `submit(session, "copy", {"asset_hash": sha_hex, "target_backend": backend_name, "pool_id":
  pool_id}, recon_domain="copy", recon_target_key=target_key, dedupe_key=f"copy:{target_key}")`.

## Tests — DoD (paste; `tests/test_reconciler_spine.py` + `tests/test_copy_reconciler.py`)
Use a **working memory-backend copy handler** registered in tests to exercise the full loop where a
success is needed; the production stub stays untouched.
- **enqueue-from-reconcile** — missing pool → `discover` writes `open/missing`, `process` enqueues
  exactly one pool-targeted `copy` job.
- **regression recovered (no event)** — `satisfied` target loses its copy → next `discover`/`process`
  **reopens `satisfied → open/missing`** and enqueues.
- **dedup** — a target with a live `copy` job is not re-enqueued (gate's `NOT EXISTS` on `recon_*`
  columns; assert it matches even though `dedupe_key` has the `copy:` prefix).
- **success ⇒ satisfied via observation, not `ok`** — job succeeds → engine sets `last_success_at`
  but **not** `satisfied`; the next observe sets `satisfied/present`. Assert both moments.
- **`ok=True` never auto-satisfied** — handler returns `ok=True` without a healthy copy → stays
  `open` (observation still sees missing).
- **failure ⇒ backoff** — failing run → `backoff`, `attempt_count` bumped, future `next_eligible_at`;
  `ok=False` no projection → `backoff(unclassified)`.
- **backoff ACCUMULATES across retries** — drive N due-and-fail cycles → `attempt_count` climbs
  `1→2→3…` (NOT reset by `process` re-observing the due backoff), `next_eligible_at` grows, and at
  the give-up ceiling the condition becomes `blocked`.
- **shared asset, multi-class union** — asset under class A `{P1}` and B `{P2}`: both targets
  desired; removing A closes **only** `P1`, `P2` stays open (still required by B).
- **reconciler jobs do not job-level retry** — FAILED job with `recon_domain` set is **not**
  re-pended by `apply_retry_policy`; an imperative job (`recon_domain IS NULL`) still retries.
- **blocked stops hammering** — production stub (`raise NotImplementedError`) → `blocked(not-
  implemented)`; subsequent passes do not enqueue (reopen rule doesn't override `blocked`).
- **policy shrink to zero closes an existing row** — `open`/`backoff` `asset:{sha}:{pool}` row,
  class's last pool removed → `process` re-observe → `¬desired` → row **closed**, not re-selected.
- **missing policy on discovery ⇒ diagnostic + skip** — class with no active pools → `enumerate_targets`
  skips with a diagnostic, no condition row written, batch survives (no exception escapes).
- **stale diagnostics cleared on satisfied** — a `backoff(drive-error)` row reaching `satisfied`
  no longer carries the stale `reason`/`next_eligible_at`; `attempt_count` reset to 0.
- **`record_condition` raises if row absent** — calling it for a `(domain,target_key)` with no
  condition row raises an invariant error (does not insert).
- **no-commit** — `record_observation`/`record_condition` inside a rolled-back transaction leave no
  row.
- **existing suites green** — `uv run pytest`; lease/retry/crash tests untouched.

## Acceptance
- New `reconciliation_condition` table + the two `job.recon_*` columns + a new Alembic revision
  chained from `af859d4ffb71`; `create_all` builds them too. `git diff --stat` shows migration +
  models.
- A missing copy is **enqueued from a reconcile pass**; a regression is recovered by the bounded
  `discover` sweep; **no full-table scans on the hot path** (`process` uses `ix_condition_work` +
  indexed single-target observe).
- The two-axis contracts hold exactly (above) — verified by the backoff-accumulation, multi-class,
  false-success, policy-shrink, and record_condition-update-or-raise tests.
- The worker skips `apply_retry_policy` for `recon_domain`-tagged jobs.
- `uv run pytest` + format + type-check green; **scenario Q green from a clean slate** (`~/system`:
  `make reset && make up && make scenario-q`) — the spine is additive, `replication.self_heal` is
  untouched.

## Out of scope (do NOT do these here — design §8 / §10)
- **No `domain_event` / `reconciliation_wakeup` tables** — single-table P0.3 only; the pipeline is
  deferred (design §10).
- **No reconciler other than `copy`.** No derivation / verify / cloud.
- **No copy byte-write** — the `copy` handler stays a stub (raises `NotImplementedError`).
- **No bundle-scoped copies** — asset scope only.
- **No scheduler loop / daemon / multi-worker.**
- **No change to `replication.self_heal` / `replicate.py` / the archive fan-out** — the spine is
  additive and must not reroute the existing placement path (keeps scenario Q green).
