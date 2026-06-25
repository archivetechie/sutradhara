# Codex prompt — P0.2: the `job_attempt` append-only audit log (sutradhara engine)

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`
> (single repo — no Shared-contract section).** Read `CLAUDE.md` + `AGENTS.md` first.
> Source: `docs/implementation-plan-ingest-v2.md` **item P0.2**; contract is
> `docs/design-reconciliation-model.md §3.6` (the attempt transcript; "the condition is
> its projection, the reconciler reads the condition, never the raw log").
>
> **This is a focused additive change** — one new table + one append in `run_one`. It
> changes *no existing behaviour*: scenario R and the lease/retry/crash tests stay green.
> It is **provenance + the foundation for P0.3's condition model**, not a feature.

## Why (one paragraph, then build)
Today the live `job` row carries only *current* state (`status`, `attempts`,
`started_at`, `finished_at`, `last_error`, `step_state`). Each retry **overwrites**
those — so a job that failed 3× then succeeded keeps only the last error, and you cannot
prune terminal `job` rows without erasing all history. Decouple the two: the `job` row
stays "what is true now," and a new append-only **`job_attempt`** table keeps the full
transcript of every run (who/when/outcome/error/leases/version). This is the 30-year
provenance and — load-bearing — **the source P0.3's `reconciliation_condition` projects
from** (last outcome, attempt count, blocking tool version). Get the columns right here
so P0.3 reads from them with no migration.

## What already exists — BUILD ON IT, do not rebuild
- **`Job` model** (`src/sutradhara/jobs/models.py`): `id, kind, params,
  required_resources, prerequisites, status, step_state, attempts, not_before, priority,
  dedupe_key, last_error, created_at, started_at, finished_at`. `JobStatus` enum (string
  values `pending/running/succeeded/failed/cancelled/queued`); `TERMINAL_STATUSES`.
- **`run_one`** (`src/sutradhara/jobs/engine.py`): claims a PENDING job (sets
  `started_at`, `attempts += 1`), runs the handler on `ctx.session`, then sets the job's
  **terminal status for this run** (`SUCCEEDED`/`FAILED`), `finished_at`, `last_error`,
  merges `step_state`, and returns the `JobResult`. The caller commits.
- **Worker** (`src/sutradhara/jobs/worker.py::_execute_job`): runs `run_one(session,
  job_id, granted_leases=granted)` in its own session, then `apply_retry_policy` (which
  may flip the job back to PENDING for a retry). The synchronous path is
  `engine.run_pending`.
- **Transaction boundary** (same as P0.1): the handler's facts **and** the job's terminal
  status are committed atomically by the caller. **So the attempt append MUST NOT
  `commit()`/`rollback()` — `flush()` only.**
- **Migration convention:** Alembic; a new revision chained from the current head (run
  `alembic revision`, don't hand-pick `down_revision`). Tests build schema via
  `create_all`, so the model must also stand alone.

## The schema — `job_attempt` (new table in `catalog/models.py` or `jobs/models.py`,
matching where `Job` lives)
Append-only. **One row per `run_one` execution.** Shape it for the P0.3 condition
projection (§3.6):

```text
job_attempt
  id              PK
  job_id          FK job.id ON DELETE SET NULL, nullable, indexed   # survives job pruning
  job_kind        str, not null                                      # denormalized — meaningful after prune
  attempt_number  int, not null                                      # = job.attempts at this run
  outcome         str, not null     # this run's terminal result: 'succeeded' | 'failed' (JobStatus values)
  error           text, nullable    # this run's last_error (full text)
  started_at      tz datetime, not null    # the run/claim start (job.started_at)
  finished_at     tz datetime, not null    # when this run ended
  granted_leases  JSON, not null default {}   # {pool: count} this run held
  worker_id       str, nullable     # hostname:pid of the runner
  code_version    str, nullable     # sutradhara version / git sha at run time
  detail          JSON, not null default {}   # handler transcript: a snapshot of this run's step_state,
                                              #   plus room for tool versions/exit codes later
  created_at      tz datetime, not null
```
- **FK is `ON DELETE SET NULL`, and `job_kind` is denormalized**, *specifically* so a
  terminal `job` row can be pruned later without deleting its attempts (the acceptance
  criterion). Do not use CASCADE.
- Index `job_id`; index `job_kind`. (No unique constraint needed — it's an append log.)
- **Why these columns:** P0.3's condition summarizes `outcome` (last outcome), `error`
  (reason), `finished_at`/`attempt_number` (recency/count), and `code_version` + a tool
  version in `detail` (the version-gated "retry after tool upgrade"). Keep `detail` an
  open JSON so a richer transcript needs no migration.

## The append — `src/sutradhara/jobs/attempts.py` (new), called from `run_one`
A small module mirroring `catalog/facts.py`'s discipline (no-commit, flush-only):
```python
def record_attempt(
    session: Session,
    job: Job,
    *,
    granted_leases: Mapping[str, int] | None = None,
    worker_id: str | None = None,
    code_version: str | None = None,
    detail: dict | None = None,
) -> JobAttempt: ...
```
- Builds one `JobAttempt` from `job` (job_id=job.id, job_kind=job.kind,
  attempt_number=job.attempts, outcome=job.status, error=job.last_error,
  started_at=job.started_at, finished_at=job.finished_at or now), plus the passed
  leases/worker/version/detail. `session.add(...)`, `flush()`, return it. **No commit.**
- `worker_id` default = `f"{socket.gethostname()}:{os.getpid()}"` via a helper;
  `code_version` default = `importlib.metadata.version("sutradhara")` (fallback
  `"unknown"`). Keep these overridable params so the worker can pass a richer id later.
- `detail` default = `{"step_state": dict(job.step_state)}` — capture this run's handler
  notes for provenance (free; `step_state` is already set).

**Call site:** in `run_one`, append the attempt **after** the run's terminal status is
set and **before** `return` — for *every* terminal path of a run: handler success
(`SUCCEEDED`), handler `ok=False`/exception (`FAILED`), and the `HandlerNotRegistered`
(`FAILED`) path. Pass `granted_leases` through (`run_one` already receives it). One
`run_one` call ⇒ exactly one `job_attempt` row.

> **Crash semantics (note in the module docstring):** a run that crashes mid-handler
> commits nothing, so it leaves **no** attempt row — it's recovered by orphan-reset +
> re-run (consistent with the no-commit boundary). The log records *completed* runs.

## Tests — `tests/test_job_attempts.py` (or extend `tests/test_jobs.py`)
- **append-per-run:** run one job to success → exactly one `job_attempt` with
  `outcome='succeeded'`, correct `attempt_number`, `granted_leases`, `started/finished`,
  and `detail.step_state` populated.
- **multiple attempts keep history:** a job that fails then is retried to a terminal
  state → **N attempt rows**, `attempt_number` monotonic, each carrying *its own*
  `outcome`/`error` (the failing run's error survives even after a later success/give-up).
- **prune without losing history (the acceptance):** delete a terminal `job` row →
  its `job_attempt` rows **survive** with `job_id` now NULL and `job_kind` intact.
- **no-commit invariant:** `record_attempt` inside a transaction the test then
  `rollback()`s leaves no `job_attempt` row (the API didn't commit behind the caller).
- **existing suites green** — `uv run pytest`; the lease/retry/crash-recovery tests must
  still pass unchanged (the attempt log is purely additive).

## Acceptance
- New `job_attempt` table + a new Alembic revision (chained from the current head);
  `create_all` builds it too. `git diff --stat` shows the migration + the model.
- `run_one` appends **exactly one** attempt per run on all three terminal paths.
- `record_attempt` never commits/rolls back (flush only), proven by the no-commit test.
- Prune-without-losing-history demonstrated by a test (FK `ON DELETE SET NULL` +
  denormalized `job_kind`).
- The attempt carries the fields P0.3's condition projects (outcome, error, finished_at,
  attempt_number, code_version, open `detail`) — so P0.3 needs no `job_attempt` migration.
- `uv run pytest` green; **scenario R green from a clean slate** (`~/system`: `make
  reset && make up && make scenario-r`) — behaviour is unchanged.

## Out of scope (do NOT do these here)
- **No condition model, no `reconciliation_wakeup`/`reconciliation_condition`/
  `domain_event` tables, no reconciler** — that is **P0.3**. This prompt only adds the
  attempt *log* P0.3 will project from.
- **No actual pruning job/policy** — only make pruning *possible* (the FK + denormalized
  kind). When/how terminal jobs get pruned is later.
- **No change to claim/lease/retry/crash-recovery logic** — the append is additive in
  `run_one`; `attempts` still increments at claim exactly as today.
- **No `step_state` removal** — it stays on `job` as current-run scratch; the attempt
  merely snapshots it.
