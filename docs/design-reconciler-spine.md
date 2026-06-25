# Design — the reconciler spine (P0.3): the condition table + the reconcile loop

> Status: **design, for review** (the owner + Claude, 2026-06-25). Implementation item
> **P0.3** in `implementation-plan-ingest-v2.md`. This is the **concrete, buildable**
> form of `design-reconciliation-model.md` Phase 3 — that doc defines *the model*
> (intent as desired-state, jobs as attempts, level-triggered convergence); this doc
> defines *the table and the control loop* that make it real, and wires the **first
> reconciler (copies)** through them as the proven template.
>
> **Scope note (2026-06-25, after a step-back review):** P0.3 is deliberately scoped to a
> **single new table** (`reconciliation_condition`) + a unified `reconcile()` loop whose
> worklist *is* the condition index. The richer **three-table** pipeline
> (`domain_event → reconciliation_wakeup → reconciliation_condition`) the plan originally
> named is **real and probably needed later** — but only once there is a multi-worker
> reconciler (needs a claimable worklist) or an edge-nudge producer (needs an event log).
> Neither exists in P0.3, so building them now would be exactly the "generic framework
> ahead of its second consumer" the model doc (§6) warns against. The full pipeline is
> **documented in §10** so it can be picked up without re-derivation; everything in §3–§9
> is designed to extend into it with no rework.
>
> Companions: `design-reconciliation-model.md` (the model + the R1–R4 rules referenced
> throughout), `design-worker-lease-scheduler.md` (the execution substrate the spine
> enqueues onto), `prompt-job-attempt-log.md` (P0.2 — the append-only attempt log the
> condition projects from).

## 1. What this is — and what it is not

P0.3 builds the **reconciler spine**: a small, domain-agnostic control loop that turns
"desired-state ≠ observed-state" into enqueued jobs, and a durable per-target **condition**
that records *why* a gap is or is not being worked. It then registers **one** reconciler —
**copies** — on that spine, making the archive's already-existing copy-placement
reconciliation *explicit* (today it is implicit, synchronous, and harness-driven; see §2).

It **is**: one new table (`reconciliation_condition`), a unified `reconcile()` loop
(`discover` + `process`, §5) whose worklist is the condition index, a tiny reconciler
registry, two no-commit condition helpers on disjoint axes (`record_observation` for reality,
`record_condition` for attempt outcome, §5.5), a small additive change to existing job
machinery (two nullable `job.recon_*` columns + a `JobResult.condition` channel + the
post-attempt condition write in `run_one` + a one-line worker guard so reconciler jobs skip
`apply_retry_policy`, §4.3/§5.5), and the copy-domain wiring over the existing `replication.py`
primitives.

It **is not**: the byte-write that actually moves a copy onto tape (a Remanence write-port
seam, §2.2, orthogonal); the `domain_event` / `reconciliation_wakeup` tables (§10 — deferred
to multi-worker / edge-nudges); any reconciler other than copies (derivation / verify /
cloud land with their domains, Phase 4 / P1.2+); an in-process scheduler loop, daemon, or
multi-worker reconciler (P5). P0.3 proves the **controller**, not the mover.

## 2. Ground truth — what already exists (build on it, do not rebuild)

### 2.1 The copy domain *already* computes desired, observed, and the gap
`src/sutradhara/replication.py` is, structurally, a reconciler written imperatively:

- **desired** — `target_pools(session, artifactclass, backends)` resolves the active
  `artifactclass_pool` memberships into the set of `PoolTarget`s a class must occupy.
  This is `policy × asset`, computed **live** (R1 — desired is derived, never stored).
- **observed** — `_healthy_copies(session, asset_hash)` → the `Copy` rows that exist and
  are `CopyHealth.OK`.
- **the gap** — `replication_status(...)` returns `{have, want, missing}` with
  `missing = want − have`.
- **gap-closing action** — `repair()` writes the missing copies; `self_heal()` restores
  a healthy copy first, then repairs.

What it lacks is the *controller*: `repair`/`self_heal` run **synchronously, in-process,
with backends passed in** (the `replicate.py` wrappers are "harness-facing"). There is no
durable record of "this `(asset, pool)` gap is open / in backoff / blocked," and no
level-triggered guarantee — convergence happens only when something calls `self_heal`. P0.3
supplies exactly that missing controller, reusing `target_pools` / `_healthy_copies` /
`replication_status` unchanged for desired/observed/gap.

### 2.2 The `copy` job is a deliberate stub — and that is fine for P0.3
`jobs/handlers/copy.py` registers `copy` but raises `NotImplementedError`: moving bytes to a
backend needs a write-capable port (the `rem.tape.write_object` seam, Remanence Layer 5) that
does not exist yet. `jobs/dispatch.py::dispatch_write_to_tape` records copy **intent** (a
PENDING `copy` job) only.

**Consequence for P0.3:** the reference reconciler *enqueues* the gap-closing job; whether
that job then moves real bytes is orthogonal. P0.3's acceptance is *"a missing copy is
enqueued from a reconcile pass,"* not *"a missing copy is materialized."* The spine is
exercised end-to-end (gap → enqueue → terminal → condition) in tests via a working
memory-backend copy path; the production tape byte-write lands later as its own seam.

### 2.3 P0.2 gives us the attempt log to project from
`job_attempt` (P0.2) is the append-only transcript — one row per `run_one`, carrying
`outcome / error / attempt_number / finished_at / code_version / detail`. The
`reconciliation_condition` (§4.1) is the **compact projection** of that log (R-model §3.6):
the reconciler reads the condition on its hot path and never scans raw attempts.

## 3. The decision: a generic per-target condition table (resolves R-model §3.2 / §8.1)

The model doc **§3.2** put the condition *on the domain row* ("per-domain, not a generic
table"), and **§8.1** parked "a generic `reconciliation_target` table" as the alternative to
adopt "if a 3rd domain shows the columns are truly identical." **P0.3 adopts the generic
table.** Two reasons, the first decisive:

1. **A desired-but-absent copy has no domain row to hold a condition.** The desired unit is
   `(asset × required-pool)`. A *present* copy is a `Copy` row; a *missing-but-desired* copy is
   **no row at all**. "Condition columns on the `Copy` row" therefore structurally cannot
   describe the backoff / attempt-count / blocked state of the exact case the reconciler exists
   to close. The condition must live on a **target-keyed row that exists whether or not the copy
   does**. §3.2 did not reckon with the absent case.
2. **The second consumer is already designed and identically shaped.** R-model **§3.7**
   established that copies *and* derivatives are both reconciled `policy × asset` with the same
   condition columns — precisely the "columns are truly identical" trigger §8.1 named.

**What stays per-domain is the *vocabulary*, not the *shape*.** `observed_state`
(`present|missing|stale|invalid`) and `reason` (`drive-error`, `unsupported-codec`, …) are
domain-specific *string values* in shared columns; each reconciler interprets them. The table
is generic; the meaning is the domain's.

This is the only departure from the companion model doc; everything else here implements its
R1–R4 rules verbatim.

## 4. The schema

One new table, plus small additive changes to existing job machinery, in `jobs/models.py`
(alongside `Job`/`JobAttempt`) with a single new Alembic revision chained from the P0.2 head.
Tests build schema via `create_all`, so the models must also stand alone.

### 4.1 `reconciliation_condition` — durable per-target projection of the attempt log + reality
One row per `(domain, target_key)`: the compact, hot-path-cheap summary the reconciler
**reads** to gate, the reconciler's observe **writes** on the reality axis, and the engine
**writes** on the attempt axis after a terminal outcome (the two-axis split, §5.5). Columns are
exactly R-model §3.2. **No `in_flight`** — it is *derived* (R2 / §5.4), so a worker crash
strands nothing. **This index is also the worklist** (§5.3): the spine has no separate
worklist table in P0.3.

```text
reconciliation_condition
  id                   PK
  domain               str  not null         # 'copy' (later 'derivation','verify','cloud')
  target_key           str  not null         # domain-encoded target identity (§4.2)
  observed_state       str  not null         # 'present' | 'missing' | 'stale' | 'invalid'  (domain vocab)
  condition            str  not null         # 'open' | 'backoff' | 'blocked' | 'suppressed' | 'satisfied'  (NO in_flight)
  reason               str  nullable         # machine-readable: 'drive-error','not-implemented','operator-suppressed',...
  message              text nullable         # human text
  attempt_count        int  not null default 0
  next_eligible_at     tz datetime nullable  # backoff/due gate; also the poke handle for "re-run now"
  blocked_tool_name    str  nullable         # version-gated re-open (derivation-like domains; unused by copy)
  blocked_tool_version str  nullable
  last_attempt_id      FK job_attempt.id nullable ON DELETE SET NULL   # link into the P0.2 transcript
  last_attempt_at      tz datetime nullable
  last_success_at      tz datetime nullable
  updated_at           tz datetime not null
  UNIQUE (domain, target_key)
  INDEX ix_condition_work (domain, condition, next_eligible_at)   # the worklist: open/backoff & due
```

`last_attempt_id` is `ON DELETE SET NULL` **defensively** — `job_attempt` is the permanent,
append-only 30-year transcript (R-model §3.6) and is **not** pruned; only terminal *job* rows
are. The condition merely must not assume its linked attempt is still resolvable, so the FK
degrades to NULL rather than cascading. (This is a weaker statement than P0.2's `job` FK,
which exists precisely so terminal *jobs* can be pruned.)

### 4.2 `target_key` — a domain-defined opaque string
The spine never parses `target_key`; each reconciler owns its format. For **copies** it is
`"asset:{asset_sha256_hex}:{pool_id}"` — the desired unit `(asset, required pool)`, with a
leading **scope tag** (`asset:`; a future `bundle:` for §6.1's bundle scope) so the key is
unambiguous across scopes. This exact string is persisted in `reconciliation_condition` and
`job.recon_target_key`, so §6 uses it verbatim. Keeping it opaque is what lets the table stay
generic (no per-domain FK columns).

*Trade-off (accepted):* no referential integrity — deleting an asset/pool orphans its
condition rows. This is acceptable for a **swept projection**: an orphaned target simply stops
appearing in `enumerate_targets`, so it is never re-enqueued, and a prune job can GC conditions
with no live target later. We pay a little integrity for a fully generic spine; revisit only if
orphan volume becomes real.

### 4.3 Two `job` columns + a `JobResult.condition` channel
The spine needs two small, additive changes to existing job machinery (same migration). They
are what makes the §3.4 "handler classifies, condition written with the attempt" contract
actually *buildable* against the real `run_one`, which creates the attempt **after** the
handler (§5.5):

- **`job.recon_domain` / `job.recon_target_key`** — nullable columns, set **only** when a
  reconciler enqueues a job (NULL for every imperative/legacy job, so their behaviour is
  unchanged). They tell `run_one` *which* `(domain, target_key)` a job closes — needed to write
  the condition after the attempt — and back the gate's live-job lookup (§5.4). Partial index
  `(recon_domain, recon_target_key) WHERE status IN ('pending','running','queued')`. `submit()`
  gains matching passthrough kwargs.
- **`JobResult.condition: ConditionProjection | None`** — the channel a handler uses to
  classify its own outcome (§5.5). `None` (the default) for every existing handler, so they are
  unaffected.

`ConditionProjection` is a small frozen value carrying **only Axis-B** fields: `(condition,
reason?, message?, next_eligible_at?, blocked_tool?)`. It deliberately does **not** include
`observed_state` — reality is Axis A (`record_observation`, §5.5), never written through the
attempt path — so a handler cannot reach across the axis boundary. A job with `recon_domain IS
NULL` flows through `run_one` exactly as today.

## 5. The spine

### 5.1 The reconciler registry
A module-level registry mirroring `jobs/registry.py::register_handler`:

```python
# jobs/reconcilers/registry.py
@dataclass(frozen=True)
class Reconciler:
    enumerate_targets: Callable[[Session, Cursor, int], Iterable[TargetObservation]]  # bulk observe (discover)
    observe:           Callable[[Session, str], TargetObservation]  # single-target observe (process, at decision time)
    reconcile_target:  Callable[[Session, str], None]               # (session, target_key) -> enqueues job(s)

def register_reconciler(domain: str) -> ...      # decorator, like register_handler
def get_reconciler(domain: str) -> Reconciler
```

`TargetObservation = (target_key, desired: bool, observed_state: str)`. `enumerate_targets`
is the **bulk** observe over a population batch (`discover`, §5.2); `observe` is the **single
target** form recomputed at decision time (`process`, §5.3) — both return the same shape, and
the domain implements observe once and maps it over the population for enumerate. **Observation
is the authority on reality**; it is what makes the level guarantee hold even for a target that
already reached `satisfied` (§5.5). P0.3 registers exactly one domain: `copy` (§6).

### 5.2 `discover` — refresh conditions from reality (the level-triggered backbone, R4)
`discover` is what **guarantees** convergence and self-heals. It enumerates the domain's
population in **bounded, cursored** batches and folds reality into conditions — it does **not**
enqueue:

```
discover(session, domain, *, batch, cursor):
  for obs in get_reconciler(domain).enumerate_targets(session, cursor, batch):
      record_observation(session, domain, obs.target_key, obs)   # Axis A: open new gaps / reopen / close
```

`record_observation` (§5.5) writes the reality axis: a `desired ∧ ¬present` target becomes
`open/missing` (initialising an absent row, or **reopening** a `satisfied` row that has
regressed — the level guarantee), and a `present` one becomes `satisfied`. Because `discover`
observes reality afresh, it catches **brand-new gaps, regressions, and policy changes** — none
of which have a prior condition row. It is paced and bounded (resumable by `cursor`); large
populations refresh over many passes, never one giant query (consistent with the substrate's
no-million-row-insert rule). It is **not** the hot path.

### 5.3 `process` — act on workable conditions (the condition index *is* the worklist)
The durable conditions written by `discover` (and by the engine's attempt outcomes, §5.5) are
themselves the worklist: `ix_condition_work` makes "open/backoff & due" a cheap indexed query,
so P0.3 needs **no separate wakeup table**.

```
process(session, domain, *, limit):
  rec = get_reconciler(domain)
  for cond in due_workable(session, domain, limit):       # ix_condition_work: condition∈{open,backoff} ∧ next_eligible_at<=now
      obs = rec.observe(session, cond.target_key)          # re-observe reality NOW (§5.5) — confirm at decision time
      cond = record_observation(session, domain, cond.target_key, obs)  # reality-axis: close if now present, hold if regressed
      if gate_open(session, domain, cond.target_key, cond):   # §5.4
          rec.reconcile_target(session, cond.target_key)      # enqueues job(s)
  # caller commits
```

The critical step is the **re-observe before acting**: `process` recomputes `(desired,
observed_state)` for the single target and folds it into the condition before gating —
**self-correcting and source-agnostic**, so a stale condition can never cause it to skip real
work or chase phantom work, and a target whose copy reappeared out-of-band closes cleanly.
**The hot path touches only the condition index and the keyed row (plus the single-target
observe, itself indexed) — never a full-table scan** (acceptance). `process` writes only the
condition's *observation* axis and never **interprets a failure** (R3 / §3.4 — failure
interpretation is the engine's, §5.5); it then enqueues.

**Crash semantics are trivial here:** a `process`/`discover` pass that crashes mid-way commits
nothing (the caller owns the transaction); conditions are durable, so the next pass simply
re-reads the index and re-observes. There is no transient worklist row to strand or GC — that
machinery only returns with `reconciliation_wakeup` (§10).

A single `reconcile(domain)` orchestrator runs `discover` then `process` each cycle (§7).

### 5.4 `gate_open` — `in_flight` is derived, never stored (R2)
The gate is R-model §3.3, simplified by the §5.5 `next_eligible_at`-non-null-while-open
invariant (so there is **no `IS NULL` branch**):

```
desired = true
AND cond.condition IN ('open','backoff')          -- not blocked / suppressed / satisfied
AND cond.next_eligible_at <= now()
AND NOT EXISTS (live job for this target)          -- pending/running/queued
```

The **`NOT EXISTS (live job)` term is a direct, indexed lookup on the job's reconciliation
columns** (§4.3) — written in full as the gate clause (note the leading `NOT`):

```
AND NOT EXISTS (SELECT 1 FROM job
                WHERE recon_domain = :domain AND recon_target_key = :target_key
                  AND status IN ('pending','running','queued'))
```

This is *not* a `dedupe_key` string match — `dedupe_key` carries a `copy:` prefix
(`f"{domain}:{target_key}"`) while the gate keys off `recon_target_key` directly, so matching
on `dedupe_key` would split one target into two identities. The reconciler still sets
`dedupe_key = f"{domain}:{target_key}"` as the Phase-1 live-insert backstop, but the gate
queries the **structured `recon_*` columns**, so the two can never disagree. Crash recovery
resets `RUNNING → PENDING` on the *job* only; the condition is untouched because `in_flight`
was never stored on it.

### 5.5 The condition state machine — two axes, two authorities (refines R-model §3.4)
R-model §3.4 attributes *all* condition-writing to the handler ("the handler classifies and
writes the condition with the attempt"). That is both **unbuildable** (`run_one` creates the
`job_attempt` *after* the handler returns or raises, so the handler never holds the attempt;
and a handler that *raises* — today's `copy` stub — runs no write code at all) **and
incomplete** (a target can change reality — lose a copy — with *no attempt at all*, so the
condition cannot be a projection of the attempt log alone). We refine §3.4: the condition is a
projection of the **latest observation *and* the latest attempt**, written on **two disjoint
axes by two authorities** that never touch each other's fields.

**Axis A — reality (`observed_state` + the `open ↔ satisfied` transition). Authority: the
reconciler's observe (`discover` in bulk, `process` per-target, §5.2/§5.3), via
`record_observation`.** Given a fresh `(desired, observed_state)`:
- `desired ∧ observed = present` → `satisfied/present` (the gap is closed — by our copy job,
  by self-heal, or out-of-band; observation is what *confirms* it). Never overrides
  `suppressed`.
- `desired ∧ observed ≠ present` → if the condition is `satisfied` (or absent), set
  `open/missing` with `next_eligible_at = now` — the **reopen rule** that closes the regression
  hole (a `satisfied` target that lost its copy fires immediately). But if the condition is
  already `backoff`, `blocked`, or `suppressed`, **leave it entirely untouched** — those are Axis
  B's / the operator's, not reality's, and reality has nothing new to say (it's still missing).
  **This includes a `backoff` that is now *due*:** a due backoff is *not* flipped to `open`; the
  gate (§5.4) already accepts `backoff ∧ next_eligible_at ≤ now`, so `process` enqueues it
  directly, and the next failure bumps `attempt_count` from its real value. (Axis A sets only
  `open` or `satisfied`; never `backoff`/`blocked`. When a class has no active pools —
  `ReplicationPolicyMissing` — an *existing* concrete target closes via the `¬desired` path below;
  *new* targets are simply not formed, with a class-level diagnostic, §6.1/§6.2.)
- `¬desired` → the target is no longer wanted (policy shrank); mark `satisfied`/inert (or GC).

**`record_observation` clears the stale Axis-B diagnostics** (`reason`, `message`,
`blocked_tool_*`) and **resets `attempt_count` to 0` *only on the transition into `satisfied`***
(gap closed — including the `¬desired` inert case). It does **not** reset on reopen: a
`satisfied → open` regression already inherits `attempt_count = 0` from the satisfied state, and
a `backoff` row is left untouched, so **`attempt_count` accumulates across the whole retry
cycle** — which is exactly what makes exponential backoff grow and give-up→`blocked` eventually
fire. (This is the bug the review caught: resetting on every due-backoff reopen pinned the
counter at 1 forever.) `next_eligible_at` is `now` on a fresh open and NULL on satisfied; a row
can never read `satisfied` while still carrying a stale `drive-error`. Observation never sets
`backoff` and never interprets *why* an attempt failed.

**Axis B — attempt outcome (`backoff` / `blocked` + `attempt_count`, `reason`,
`last_attempt_id/at`, `last_success_at`). Authority: the engine, post-attempt.** After
`record_attempt(...)` on **every** terminal path, if the job is reconciler-backed
(`recon_domain` set, §4.3), `run_one` writes Axis B linked to the just-created attempt:
- handler returned a `JobResult.condition` projection → apply it. The handler is where domain
  failure semantics live (e.g. `ffmpeg exit 218 → blocked(unsupported-codec)`); the projection
  may set `backoff`/`blocked` + `reason`.
- handler raised `NotImplementedError` → `blocked(not-implemented)` — so **the copy stub stays
  untouched** (it still raises) yet lands a clean terminal condition.
- handler raised any other exception, or kind not registered → `backoff` with `attempt_count++`
  and `next_eligible_at = now + backoff(attempt_count)`.
- handler returned `ok=False` with **no** projection → `backoff(unclassified)` (the handler
  said "failed" without classifying — a safe transient default, surfaced via `reason`).
- handler returned `ok=True` → the engine calls `record_condition(condition=None, attempt=…)`
  — the **metadata-only** form, which records `last_attempt_id/at` + `last_success_at` but
  performs **no state transition** and **does not reset `attempt_count`** (`condition`/`reason`/
  `next_eligible_at`/`attempt_count` all untouched — reset is Axis A's, on `satisfied`, so a
  *false* success that didn't actually fix the gap can't silently wipe the failure streak). The
  engine does **NOT** set `satisfied`. `JobResult.ok` means
  "the job machinery ran cleanly," *not* "the domain state is now good" — a `verify`-style job
  can run cleanly and discover corruption. `satisfied` is therefore Axis A's call: the *next*
  observe (process re-observes the target right after, or the next discover) confirms `present`
  and sets it. `ok=True` with **no** projection is the normal success case; `ok=True` *with* a
  non-`satisfied` projection (e.g. a verify job reporting `invalid`) is honoured as an Axis-B
  transition, still never auto-satisfied.

**Why the engine must own the backoff default on the raise/`ok=False` paths:** a failed
reconciler job stays *terminal (FAILED)* — **no live job remains** (see the retry contract
below) — so without a condition update the condition stays `open/due` and the next `process`
pass re-enqueues immediately and *hammers* the failing target. Writing `backoff` (future
`next_eligible_at`) there is what stops it.

**The condition backoff is the *sole* retry authority for reconciler-backed jobs (R3, and the
"jobs are ephemeral attempts" model).** The worker's `apply_retry_policy` (`worker.py`) would
otherwise re-pend a FAILED job on its *own* per-kind cadence (`config.retry_for_kind`, with its
own `not_before`), giving the same target two competing retry clocks. We pin it: **the worker
skips `apply_retry_policy` when `job.recon_domain` is set** — a reconciler job is exactly *one*
lease-bound attempt and never self-retries; a retry is a *fresh* job the reconciler enqueues
when the condition's `next_eligible_at` comes due. One target, one retry cadence (the
condition), one attempt per job. Imperative jobs (`recon_domain IS NULL`) keep job-level retry
exactly as today. This is the change that makes "no live job remains after a failure" actually
true — a one-line guard in `_execute_job`, the only worker change P0.3 makes.

The two non-condition writers are unchanged: the **reconciler** reads the condition to gate;
the **lease worker** does job-status transitions only (now skipping retry for reconciler jobs).

Both axes go through small no-commit helpers (flush, never commit/rollback — so observation /
attempt / job-status all commit atomically in the caller's transaction), mirroring
`catalog/facts.py` / `jobs/attempts.py`:

```python
# jobs/reconcilers/conditions.py
def record_observation(session, *, domain, target_key, desired, observed_state
                       ) -> ReconciliationCondition: ...        # Axis A (reality)
def record_condition(session, *, domain, target_key, condition=None, reason=None, message=None,
                     attempt=None, next_eligible_at=None, blocked_tool=None
                     ) -> ReconciliationCondition: ...           # Axis B (attempt outcome)
```

Each touches only its own axis. **`record_observation` is the row's creator (it upserts** — it
always has `observed_state`, which is `NOT NULL`). **`record_condition` only ever *updates* an
existing row and raises an invariant error if the row is absent** — it has no `observed_state` to
insert, and in the normal flow the row is guaranteed to exist because `process` ran
`record_observation` *before* it enqueued the job (and Axis B runs only for reconciler-backed
jobs, which the reconciler is the only thing that enqueues). A missing row at Axis-B time is a
real bug (a reconciler job ran with no preceding observation), so it should fail loudly, not
silently insert a half-formed row. `record_condition` always links `last_attempt_id`/
`last_attempt_at` from `attempt`; **`condition=None` is the metadata-only mode** (the success
path) — it refreshes `last_success_at` only and performs **no state transition** and **no
`attempt_count` reset**, leaving `condition`/`reason`/`next_eligible_at`/`attempt_count` for Axis
A (which resets `attempt_count` when it sets `satisfied`). A non-null `condition` applies the
Axis-B transition (and, for `backoff`, bumps `attempt_count` + sets `next_eligible_at`).

**Due-time invariant (enforced by *both* writers, since both can produce a workable
condition):** whenever a writer sets `condition ∈ {open, backoff}`, it sets `next_eligible_at`
non-null — `record_observation` sets `= now` when it opens a gap (fire immediately),
`record_condition` sets `= now + backoff(attempt_count)` when it backs off. `satisfied`,
`blocked`, and `suppressed` may leave it NULL (no schedule). This makes every gate/worklist
query a uniform `next_eligible_at <= now` with no `IS NULL` branch — and, critically, a row
reopened by Axis A is immediately workable rather than stranded on a NULL due-time.

## 6. The reference copy reconciler (`domain='copy'`)

Wiring over `replication.py` (`target_pools`, `_healthy_copies`, `replication_status`) — no
new desired/observed/gap math. The one piece that is *not* already in `replication.py` is
**where the artifactclass comes from**, which the review correctly flagged:

### 6.1 The desired-state source — `artifactclass` is on the unit, not the asset
`LogicalAsset` (PK `content_sha256`) carries **no** `artifactclass`; it lives on the
**archivable-unit** rows — `IngestItem.artifactclass` (per-occurrence) and `Bundle.artifactclass`
(the sealed artifact) — and `Copy` is scoped to **exactly one** of `logical_asset_hash` *or*
`bundle_id` (the `Copy` check constraint). So the copy domain enumerates **units, not bare
assets**, and resolves the class from the unit. We pin this explicitly:

- **Scope (P0.3):** the **asset** unit, sourced from its **live `IngestItem` memberships** —
  this is exactly what the existing `replication.py` / scenario-Q self-heal path operates on
  (`replication_status(asset_hash, artifactclass, …)`), so the reference reconciler stays
  congruent with what actually gets archived. **Bundle-scoped copies** (`Bundle.artifactclass`,
  `Copy.bundle_id`) are the *identical* pattern at bundle granularity with a `bundle:`-scoped
  target_key — but their observed/gap query does **not** exist yet (no `bundle_replication_status`)
  and bundle copies currently have **no self-heal at all**, so they are out of P0.3 scope and
  flagged as the highest-value *next* reconciler (§10 carried decision).
- **"Live" membership (schema-accurate):** `IngestItem` has **no** status of its own; archival
  acceptance lives on its parent `Intake.status`. So live membership is
  `IngestItem JOIN Intake ON IngestItem.intake_id = Intake.intake_id WHERE Intake.status =
  'registered'`, taking the class from `IngestItem.artifactclass` (the per-occurrence class;
  `Intake.artifactclass` is only the batch default). A `receiving`/`verifying`/`quarantined`
  intake imposes no copies — and a quarantined intake registers no `ingest_item` rows at all,
  so a present `IngestItem` already implies a non-quarantined intake; the `registered` filter
  still excludes `verifying`.
- **Desired pools = the union of `target_pools(artifactclass)` over the unit's live
  memberships.** An asset reachable under more than one live class wants copies in the union of
  those classes' pools (conservative: never under-replicate a shared asset). A unit with **no**
  live membership is `¬desired` (Axis A inerts/GCs it).
- **Missing pool policy is a sweep-level configuration diagnostic, not a per-target condition.**
  `target_pools` *raises* `ReplicationPolicyMissing` when a live class has **no active pool
  memberships** (the schema treats "registered class, zero pools" as a misconfiguration, *not*
  "zero copies wanted"). The subtlety the review caught: with no pool there is **no
  `asset:{sha}:{pool_id}` target_key to write a condition on** — you cannot form a target at all.
  This is categorically different from `blocked`, which is a *per-target* hold for a target that
  **exists** but can't progress. The handling splits by direction (§6.2): **during discovery**
  (forming *new* targets by enumerating a class), `ReplicationPolicyMissing` means there are no
  pools to enumerate, so the copy reconciler **emits a class-level configuration diagnostic**
  (logged + counted: *"class X has registered items but no active pools — N items cannot be
  reconciled"*) and **forms no new targets** — never halting the bounded `discover` pass, and not
  silent (the diagnostic is the operator signal, at the *right* granularity: one alert per
  misconfigured class, not a flood of per-asset rows). **For an already-existing concrete target
  row** (`process` re-observing `asset:{sha}:{pool}` after the class's last pool was removed),
  `observe` treats the empty pool set as `¬desired` and **closes/inerts the row** (§6.2) — so a
  policy shrinking to zero never strands a due row. When a pool policy is (re)added, the next
  `discover` forms real `(asset, pool)` targets and opens normal conditions. *(If per-class
  queryable visibility is later wanted, the natural extension is a `class:{artifactclass}`
  target_key carrying `blocked(policy-missing)` — a second, class-scoped target shape — deferred
  until there's demand.)*
- **`target_key = "asset:{sha256_hex}:{pool_id}"`** — the leading scope tag (`asset:` vs a
  future `bundle:`) matches `Copy`'s XOR scoping and keeps the key unambiguous.

### 6.2 The three reconciler functions
**The spine runs on catalog rows, not the runtime `backends` map.** `target_pools()` /
`replication_status()` require a `backends` map and resolve *write* backends (they raise if one
is unavailable) — that belongs to the stubbed copy *handler*, not the controller. So
observe/discover/process compute **desired pool_ids** from the active `ArtifactClassPool`
memberships (a backend-free query — factor the membership half out of `target_pools`) and
**observed pool_ids** from `_healthy_copies` (already backend-free). `reconcile_target` resolves a
missing pool's backend *name* via `Pool.backend` (a catalog lookup) for the job params. The whole
reconciler is free of the runtime `backends` map.

- **`observe(session, target_key)`** — for a **concrete existing** key `asset:{sha}:{pool}`, the
  key carries only `(asset, pool)` — **not a class**. So `desired` must be evaluated against the
  asset's **full set of live class memberships**, exactly as §6.1's union defines it:
  `desired = pool ∈ ⋃_{c ∈ live_classes(asset)} active_pool_ids(c)`, comparing to `_healthy_copies`.
  A class that raises `ReplicationPolicyMissing` contributes `∅` to the union (it is skipped, not
  fatal); `desired = False` only when the pool is in **none** of the live classes' pool sets —
  e.g. the asset's *last* class with that pool was removed. This is **load-bearing for shared
  assets:** an asset live under class A *and* B must **not** close a `pool` that B still requires
  just because A dropped it. `observe` of a concrete key therefore **never raises or strands** —
  a true policy-shrink closes the row on the next `process` pass; a partial change (one of several
  classes drops the pool) keeps it open while any class still wants it.
- **`enumerate_targets(session, cursor, batch)`** — the **discovery** direction: `observe` mapped
  over a bounded, id-cursored batch of live `IngestItem`s, *forming* the per-pool target keys from
  `target_pools(class)`. Here `ReplicationPolicyMissing` means there are **no pools to enumerate**,
  so it forms **no new targets** and emits the class-level diagnostic (§6.1) — you cannot
  manufacture a keyless target. The asymmetry is intentional: enumeration *creates* keys (raise →
  nothing to create), while `observe` *checks* an existing key (raise → that key is no longer
  desired → close it).
- **`reconcile_target(session, target_key)`** — for a missing pool, enqueue a **pool-targeted**
  `copy` job tagged with its reconciliation target:
  `submit(session, "copy", {"asset_hash": sha_hex, "target_backend": <pool's backend>,
  "pool_id": pool_id}, recon_domain="copy", recon_target_key=target_key,
  dedupe_key=f"copy:{target_key}")`. Pool-specific because `dispatch_write_to_tape` resolves a
  single tape backend and is ambiguous across pools, so the reconciler targets the exact missing
  placement. (`recon_*` are the §4.3 passthrough; `copy` params gain `pool_id` — inert today,
  forward-shaped for the write port.)
- **terminal (Axis B, §5.5)** — `run_one` writes the attempt axis after the attempt, keyed by
  the job's `recon_*` columns. The production `copy` handler is the stub: it `raise`s
  `NotImplementedError`, mapped to `blocked(not-implemented)` — the gap stops being re-enqueued
  (blocked, no live job) **without touching the stub**. When the real write port lands, success
  records `last_success_at` and the **next observe sets `satisfied/present`** (Axis A);
  classified failures return `backoff`/`blocked` projections. Tests drive the full loop —
  enqueue → success → re-observe → `satisfied` — with a working memory-backend copy handler.

This is the template every later reconciler copies: *observe (reality → Axis A) → (condition
index = worklist) → gate → enqueue → handler runs → engine writes Axis B → next observe
confirms `satisfied`.*

## 7. Runtime

P0.3 ships the spine as **plain functions + a CLI verb**, not a daemon:

- `discover(domain, batch)` and `process(domain, limit)` are ordinary functions, with a
  `reconcile(domain)` orchestrator that runs `discover` then `process`. They are invoked
  synchronously in tests and from a `sutra reconcile <domain>` CLI command, which the existing
  scheduled scrub/self-heal hook calls. This is how `self_heal` is already driven today, so no
  new runtime is introduced.
- An in-process periodic loop, reconciler-as-recurring-job, and any multi-worker reconciler are
  **deferred to P5** (reconciler scaling). The functions are written loop-ready (bounded,
  cursored, idempotent) so P5 wraps them without redesign.

This resolves R-model §8.4 for now: *function + CLI, decide the loop at scaling time.*

## 8. Scope discipline (what P0.3 does **not** build)

Per R-model §6 — do not build a generic framework ahead of its consumers:

- **One table, not three.** `domain_event` and `reconciliation_wakeup` are deferred to §10 —
  P0.3's single sweep-only, single-node reconciler needs neither, and the condition index is its
  worklist.
- **No reconciler other than `copy`.** Derivation / verify-freshness / cloud land with their
  domains (Phase 4 / P1.2+) and reuse this template.
- **No copy byte-write port** — the `copy` handler stays a stub (§2.2); the spine proves
  enqueue, not materialization.
- **No scheduler loop / daemon / multi-worker** (P5).
- **Only asset-scoped copies.** Bundle-scoped copies are the identical pattern (§6.1) but need a
  new observed/gap query and are not wired in P0.3.
- **Nothing in the registry the copy domain does not use.** The three registry hooks
  (`enumerate_targets`, `observe`, `reconcile_target`) exist because `copy` needs them now; the
  generic *table* is justified because §3.7's second consumer is already designed (§3), but no
  speculative columns beyond §3.2's set.

## 9. Tests & acceptance

**Tests** (`tests/test_reconciler_spine.py` + `tests/test_copy_reconciler.py`):

- **enqueue-from-reconcile** — an asset with a missing pool: `discover` writes `open/missing`,
  `process` re-observes the gap and enqueues exactly one pool-targeted `copy` job for that
  `(asset, pool)`.
- **regression recovered (no event needed)** — a target driven to `satisfied/present`, then its
  only healthy copy is removed: the next `discover`/`process` **reopens `satisfied →
  open/missing`** (the reopen rule, §5.5) and enqueues. This is the level guarantee — recovery
  needs no edge event.
- **dedup** — a target with a live `copy` job is **not** re-enqueued (the gate's `NOT EXISTS`
  query on the job's `recon_domain`/`recon_target_key` columns, §5.4 — assert it matches even
  though `dedupe_key` carries the `copy:` prefix).
- **success ⇒ satisfied via observation, not via `ok`** — a working memory-backend copy handler:
  the job succeeds (engine writes `last_success_at` + the attempt link via
  `record_condition(condition=None)` but does **not** set `satisfied`); the **re-observe** then
  sets `satisfied/present` (Axis A). Assert `satisfied` is absent immediately after the attempt
  and present after the next observe.
- **`ok=True` is never auto-satisfied** — a stub handler that returns `ok=True` **without**
  producing a healthy copy: the condition does **not** become `satisfied` (observation still sees
  `missing` → stays `open`). Guards `JobResult.ok ≠ domain-good`.
- **failure ⇒ backoff (Axis B)** — a failing run → `backoff` with `attempt_count` bumped and a
  future `next_eligible_at`; the next pass does not re-enqueue until due. `ok=False` with no
  projection → `backoff(unclassified)`.
- **backoff *accumulates* across retries** — drive a target through N due-and-fail cycles:
  `attempt_count` climbs `1→2→3…` (it is **not** reset by `process` re-observing the due backoff),
  `next_eligible_at` grows, and at the give-up ceiling the condition becomes `blocked`. Guards the
  finding-9 bug where re-observing a due backoff pinned the counter at 1.
- **shared asset, multi-class union** — an asset live under class A (pools `{P1}`) *and* class B
  (pools `{P2}`): both `asset:{sha}:P1` and `asset:{sha}:P2` are desired. Removing class A closes
  **only** `P1` (no longer in any live class's union) while `P2` stays open (still required by B).
  Guards the finding-9 shared-asset hazard where a singular-class observe would wrongly close `P2`.
- **reconciler jobs do not job-level retry** — a FAILED job with `recon_domain` set is **not**
  re-pended by `apply_retry_policy` (the worker skips it); it stays terminal, and the *only*
  re-attempt is a fresh job enqueued when the condition's `next_eligible_at` comes due. Assert an
  imperative job (`recon_domain IS NULL`) still retries per `config.retry_for_kind`.
- **blocked stops hammering** — the production stub (`raise NotImplementedError`) →
  `blocked(not-implemented)`; subsequent passes do not enqueue (no live job, and the reopen rule
  does **not** override `blocked`).
- **missing pool policy ⇒ diagnostic + skip on discovery** — a registered item whose class has
  no active pool memberships: `enumerate_targets` catches `ReplicationPolicyMissing`, the asset is
  skipped with a class-level diagnostic and **no condition row is written** (no pool to key on),
  and the `discover` pass still processes the rest of the batch (no exception escapes). Adding a
  pool policy → the next `discover` opens normal `(asset, pool)` conditions.
- **policy shrink to zero closes an existing row, never strands it** — an `open`/`backoff`
  `asset:{sha}:{pool}` row exists, then the class's last active pool is removed: `process`
  re-observes the concrete key, `observe` treats `ReplicationPolicyMissing` as `¬desired` →
  `record_observation` **closes/inerts the row**, and it is **not** selected on subsequent passes
  (the regression of finding-7 review: a stranded, perpetually-due row).
- **stale diagnostics cleared on satisfied** — a target that backed off with `reason=drive-error`,
  then reaches `satisfied`: the condition no longer carries the stale `reason`/`next_eligible_at`
  and `attempt_count` is reset to 0. A subsequent `satisfied → open` regression inherits the clean
  state (it does **not** itself reset — reset is satisfied's job, §5.5).
- **no-commit** — `record_observation` / `record_condition` inside a transaction the test rolls
  back leave no condition row (caller owns the boundary, like `record_attempt`/facts).
- **no hot-path full scan** — `process` issues only indexed queries (assert via query count /
  plan on `ix_condition_work` + the single-target observe).
- **existing suites green** — `uv run pytest`; the lease/retry/crash tests are untouched.

**Acceptance** (plan P0.3): a missing copy is **enqueued from a reconcile pass**; a regression
(or any dropped signal) is **recovered by the bounded `discover` sweep**; **no full-table scans
on the hot path**; new Alembic revision chained from the P0.2 head (`create_all` builds it too);
`uv run pytest` + format + type-check green; **scenario Q (self-heal) green from a clean slate**
(`~/system`).

On scenario Q specifically — the spine is **purely additive**: the existing synchronous
`replication.self_heal` remains the actual copy-placement path and is **left outside the spine
and untouched** (it must stay so until the real copy write port lands — §2.2). Scenario Q stays
green *because* P0.3 does not reroute placement through the stub `copy` handler; the spine's own
convergence is proven by its new tests (memory-backend copy handler), not by scenario Q.

## 10. Deferred: the three-table pipeline (`domain_event` + `reconciliation_wakeup`)

P0.3 ships one table and a sweep-only loop because that is all its single, single-node consumer
needs. Two capabilities will pull the other two tables back; this section documents them so
they can be added without re-derivation. **The `reconciliation_condition` table and the entire
two-axis state machine (§5.5) carry over unchanged** — these tables sit *in front of* the
condition, changing only how the worklist is fed and claimed.

### 10.1 When you need them
- **`reconciliation_wakeup` — when there is more than one reconciler worker.** The §5.3 worklist
  is the condition index, read directly. With a single worker that is safe; with **concurrent
  workers**, two passes could read the same due condition and double-enqueue (the `NOT EXISTS
  live job` gate narrows but does not eliminate the race). A `reconciliation_wakeup` row is the
  **claimable** unit of work: `claim_due_wakeups` atomically stamps `claimed_at` so exactly one
  worker takes a target. Adopt it when the reconciler goes multi-worker (P5).
- **`domain_event` — when you need sub-sweep latency (edge-triggering).** The `discover` sweep
  bounds worst-case latency to one sweep cycle. When that is too slow for some signal (e.g. scrub
  marking a copy `MISSING` should re-replicate *now*, not at the next sweep), a producer appends a
  `domain_event` that materialises a wakeup immediately. Per R4 this is **optimisation only** —
  the sweep remains the guarantee, so dropping every event must change nothing but latency.

### 10.2 The two tables (for when they land)
```text
domain_event                         # append-only audit + edge-nudge source
  id, domain, target_key, event_type, payload JSON, created_at, consumed_at
  index (domain, target_key); index (created_at)

reconciliation_wakeup                # dedup'd, claimable worklist (replaces "read the condition index directly")
  id, domain, target_key, next_eligible_at, source ('event'|'sweep'|'retry'), created_at, claimed_at
  UNIQUE INDEX (domain, target_key) WHERE claimed_at IS NULL    # one live wakeup per target (collapses N events)
  INDEX (domain, next_eligible_at)   WHERE claimed_at IS NULL    # due & unclaimed
```

### 10.3 How they slot in (no rework to §3–§9)
- `discover` and edge-events **upsert wakeups** instead of (or in addition to) writing conditions
  directly; the unique-live index collapses duplicates.
- `process` **claims** due wakeups (`claimed_at` = "taken for processing", atomic) instead of
  querying the condition index, then re-observes / gates / enqueues exactly as in §5.3, and
  **deletes** the wakeup when done (it is a transient token; the condition is the durable truth).
- **Crash recovery** gains one bounded step: reset wakeups whose `claimed_at` is older than a
  threshold (the wakeup analogue of the job `RUNNING → PENDING` orphan reset); since the gap is
  still open, the sweep regenerates a fresh wakeup, and the `claimed_at IS NULL` predicate keeps
  it unique among unclaimed rows. (In P0.3 there is no such row, so no such recovery — §5.3.)
- The condition, gate, two-axis writes, and retry contract are **identical**; only the worklist
  feed changes.

This is a strict superset of the P0.3 design, so the migration that adds these two tables is
additive and the loop change is localized to "where does `process` get its work-list."

## 11. Open decisions carried (not blocking P0.3)

1. **`target_key` opacity vs structured FKs** — opaque chosen (§4.2); revisit if orphaned
   conditions become a real volume problem (add a GC prune job before adding FKs).
2. **Reconciler runtime at scale** (R-model §8.4) — function+CLI now; in-process loop vs
   recurring-job decided at P5, alongside the §10 wakeup table.
3. **Backoff / give-up policy per domain** — copy uses a simple exponential backoff with a
   give-up→`blocked` ceiling; each later domain tunes its own (R-model §8.3).
4. **Discovery cadence & cursor strategy** — id-range cursor for P0.3; bounded indexed sweep +
   elastic pacing hardened in P5.
5. **Bundle-scoped copies as the next reconciler.** P0.3 is asset-scope only, predicate pinned
   (`IngestItem JOIN Intake WHERE Intake.status = 'registered'`, §6.1). Bundle-scope
   (`Copy.bundle_id`, `archive_fanout`) is the identical spine pattern but needs its own
   observed/gap query (no `bundle_replication_status` exists today) and currently has **no
   self-heal at all** — making bundle self-heal the highest-value reconciler to build next, and
   the true test that the spine generalises beyond a domain that already worked.
