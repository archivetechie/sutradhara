# Codex prompt — P5.2: elastic resource enforcement (cgroup CPU/IO) — sutradhara

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo` (single repo).**
> Read `CLAUDE.md` + `AGENTS.md` first.
>
> **Authoritative design: `docs/design-elastic-resource-control.md` — read it in full** (it implements the
> already-settled `design-worker-lease-scheduler.md §4a`). This prompt is the build order, the
> must-be-exact contracts, and the acceptance tests. Source: plan item **P5.2** (pulled forward to
> near-term — it's an ops guardrail, not scale-deferred).
>
> **What this is.** The worker's lease does **admission** ("don't run more than N jobs") but **not CPU
> enforcement** — codecs lie about `-threads`, so one `transcode` can monopolize the box and starve a
> restore or the rem tape daemon (→ tape write errors / shoe-shining). P5.2 wraps every CPU-heavy
> **subprocess** in its own **cgroup v2** (via a transient `systemd-run --scope`) with a kernel-honored
> **`CPUWeight`**, giving **no-starvation + elasticity**. The lease is untouched (admission); the cgroup
> decides *how running jobs share* the box.
>
> **Read this twice — `CPUWeight` is FAIRNESS, not a hard cap.** It guarantees *no-starvation* and
> *elasticity* (spare when idle, yield by weight under contention); it does **not** limit a job to "≤ N
> cores." A hard ceiling, when a profile needs guaranteed headroom, is opt-in **`CPUQuota`/`cpu.max`**.

## What already exists — BUILD ON IT
- **The CPU-heavy subprocess sites (all via bare `subprocess.run`):** `transcode._run_ffmpeg`
  (`transcode.py:204`, ffmpeg), `pfr_index` (`pfr_index.py:118` + `:164`, ffprobe), the `rem-debug` RAO
  codec (`sealing/rao.py:365`), and `rem archive build/extract` (`rem_archive_cli.py`). These parse child
  output: **ffprobe/rem JSON on stdout, ffmpeg diagnostics on stderr, the child returncode**.
- **The lease/worker** (`design-worker-lease-scheduler.md`) — admission stays as-is. **Job kinds** are the
  role source; `Job.priority` is a separate **dispatch-ordering** integer (do NOT use it as the role).
- **Host:** Ubuntu, cgroup v2, go *through systemd* (`systemd-run`), never raw `/sys/fs/cgroup`.

## Build order

### A. `src/sutradhara/resource_control.py` — the wrapper, profiles, probe
- `ResourceProfile{cpu_weight:int, io_weight:int, nice:int, ionice:(class,level)|None, cpu_quota_pct:int|None}`.
- `RESOURCE_PROFILES = {'high':…, 'medium':…, 'low':…}` and `ROLE_BY_JOB_KIND` (per-kind default):
  - **high** — `restore`, release-verify: high `cpu_weight`, **`nice 0`**, normal ionice. Never throttled.
  - **medium** — `transcode`, `pfr-index`, `cloud-blob`: medium `cpu_weight` (≤ default 100), `nice 0`.
  - **low** — verify-freshness/re-derive/bulk: low `cpu_weight` (≪ 100), `nice 19`, `ionice -c idle`.
  - **All `nice ≥ 0`** (an unprivileged worker can't raise priority — `nice -1` would fail before exec).
    Negative nice / real-time is opt-in + capability-probed only. Prioritization comes from `cpu_weight`.
- `capability()` — **cached** probe: run `systemd-run [--user|--system] --scope --quiet --no-ask-password
  -p CPUWeight=100 -p IOWeight=100 [-p CPUQuota=…] -- true` with a **short timeout**, over the **full
  property set any profile uses (incl. `CPUQuota` when a profile sets `cpu_quota_pct`)**. Cache the
  **working property subset** (drop + log any rejected property — a scope can be allowed while `io`/quota
  is undelegated); fall to CPU-weight-only, then `'degraded'`.
- `run_managed(cmd, *, role, cpu_lease=None, timeout=None, **popen_kw) -> subprocess.CompletedProcess`:
  - **systemd:** `systemd-run [--user|--system] --scope --collect --quiet --no-ask-password
    --unit=sutradhara-rc-<kind>-<uniq>.scope -p CPUWeight=<w> [-p IOWeight=<k>] [-p CPUQuota=<q>%]
    -- ionice -c<c> -n<l> nice -n<n> <cmd>` — only **probed-OK** properties; **`--quiet`** (no `Running as
    unit…` on stderr); **never `--pipe`** (incompatible with `--scope`).
  - **degraded:** plain `subprocess.run` + best-effort `nice`/`ionice` (skip any non-permitted adjustment)
    + the `-threads` hint; **log degraded enforcement**.
  - **Child output preserved EXACTLY:** return a `CompletedProcess` whose `stdout`/`stderr`/`returncode`
    are the **child's, untouched**; `capture_output`/`text`/`timeout`/`check` pass through to the child.
  - **Child-exit vs setup-failure:** a **nonzero child exit (incl. ffmpeg/rem error) is returned ONCE and
    NEVER retried**. Degrade **only** on an unambiguous **pre-child** systemd setup failure — systemd-run's
    own diagnostic (`Failed to start transient scope` / `Failed to set unit properties`) **with no child
    output**. (Because the probe pre-validates, runtime setup failure is anomalous; the rare cgroup-drift
    case surfaces as a child failure and is re-attempted by the **job engine's** retry, not a per-call
    rerun — a real failure must never be double-run.)
  - **Timeout leaves NO orphan:** name the scope deterministically and on `TimeoutExpired` tear down the
    **whole scope** (`systemctl [--user] stop <unit>`; on degraded, run the child in a **new process group**
    and `os.killpg`), then re-raise `TimeoutExpired`.

### B. Route the four subprocess sites through `run_managed`
Replace the bare `subprocess.run` in `transcode._run_ffmpeg`, `pfr_index` (×2), `sealing/rao.py`,
`rem_archive_cli` with `run_managed(cmd, role=<kind's role>, cpu_lease=<the cpu requirement>, …)`. The
handler knows its kind (→ role via `ROLE_BY_JOB_KIND`) and its `required_resources` cpu count (→ weight /
optional quota input). A per-job override is an explicit `params["resource_role"]` — **not** `Job.priority`.
Keep `-threads ≈ cpu_lease` as a **hint** only.

### C. `--user` vs `--system`
Configurable by how the worker is deployed (default `--user`); `--system` **always** passes
`--no-ask-password` + the short timeout (no polkit hang). Document the unit setup for `--system`.

## Must-be-exact contracts
- **`CPUWeight` = no-starvation + elasticity, NOT a hard cap.** `CPUQuota` is the only hard ceiling
  (opt-in per profile).
- **Child output preserved byte-for-byte** (`--quiet`, no `--pipe`); the ffprobe/ffmpeg/rem parsers see
  the child's `stdout`/`stderr`/`returncode` unchanged.
- **A nonzero child exit is returned once, never retried**; only a pre-child setup failure degrades.
- **Timeout tears down the whole scope** (no orphan `ffmpeg`/`ffprobe`); `TimeoutExpired` still surfaces.
- **Probe validates the actual property set** (incl. `CPUQuota` when used) and caches the working subset;
  `run_managed` emits only validated properties; **the command always runs** (only the guarantee degrades).
- **All `nice ≥ 0`**; role from **job kind** (not `Job.priority`); `-threads` is a hint only.
- **The lease is unchanged** (admission); no reconciler/scheduler change.

## Tests — DoD (`tests/test_resource_control.py` + light edits where the four handlers are tested)
- **argv construction** — under a mocked `systemd` capability, `run_managed(role='medium', cpu_lease=8)`
  builds the exact `systemd-run --scope --collect --quiet --no-ask-password --unit=… -p CPUWeight=… [-p
  IOWeight=…] -- ionice … nice … <cmd>` (no `--pipe`); high/medium/low map to the right weights/`nice`/
  `ionice`, all `nice ≥ 0`.
- **child output preserved exactly** — `run_managed(cmd, capture_output=True, text=True)` returns the
  child's `stdout`/`stderr`/`returncode` (no `Running as unit…` in stderr) on **both** the systemd and
  degraded paths.
- **graceful fallback** — `systemd-run` absent → degraded plain `subprocess.run` + best-effort `nice`/
  `ionice`, warning logged, child still runs (existing `transcode`/`pfr-index`/`rao` tests stay green).
- **property rejection ≠ child failure** — a probed property rejected (`IOWeight` undelegated, **or
  `CPUQuota` disallowed**) → probe drops to the working subset (quota marked unavailable) and the child
  still runs (not a pre-child failure).
- **child nonzero returned once; only setup-failure degrades** — a child exiting **nonzero with stderr**
  (ffmpeg/rem error) is returned once, **not retried**; a simulated pre-child `systemd-run` setup-failure
  (its diagnostic, **no child output**) is the only case that degrades. Never confused despite both being
  exit 1.
- **timeout leaves no orphan** — a timed-out managed command leaves **no** active scope/`ffmpeg`/`ffprobe`
  (systemd: `systemctl stop`; degraded: `killpg`), and `TimeoutExpired` surfaces to the handler.
- **role from kind, not priority** — each job kind resolves to its profile; the integer `Job.priority`
  doesn't change the role; an explicit `params["resource_role"]` override wins.
- **live (`~/system`, real cgroup box, gated to a systemd host)** — a **lone transcode uses spare cores**;
  under a concurrent **high**-weight job, **neither is starved** (the kernel shares CPU by weight; the high
  job keeps its share regardless of ffmpeg's thread count — assert via `cpu.stat` / observed shares); a
  `cpu_quota_pct` profile is also capped at its quota.
- **existing suites green** — `uv run pytest`; format + type-check clean (the degraded path must not change
  behavior where systemd is absent).

## Acceptance (plan P5.2 — weight-vs-quota reality)
A lone transcode uses spare cores and **yields by weight** under contention; **operator/tape work is never
starved** by background derivation regardless of codec threads (weighted fairness). A hard "≤ N cores"
ceiling, where a job needs guaranteed headroom, is delivered by an opt-in `CPUQuota` — **not** by
`CPUWeight`. `uv run pytest` + format + type-check green, plus the `~/system` live check.

## Out of scope (do NOT build here)
- **P5.1 (reconciler scaling) / P5.3 (multi-worker/Postgres)** — separate, demand-driven.
- **No `Delegate=yes` worker** — the per-subprocess `systemd-run --scope` path first; the delegated-subtree
  variant is a later throughput optimization (the `run_managed` seam makes the swap internal).
- **No GPU or memory cgroup limits** — CPU + I/O first (`MemoryMax`/gpu are follow-ups).
- **No change to the rem tape daemon** — it's a separate service; P5.2 just stops sutradhara's CPU hogs
  from starving it.
- **No lease/scheduler change** — admission is unchanged.
