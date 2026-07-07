# Design — P5.2: Elastic resource enforcement (cgroup CPU/IO, not `-threads`)

> Design by Claude + the maintainer (2026-06-27), for review then implementation. **Repo: sutradhara (+ a
> `~/system` live check).** Plan item **P5.2** — pulled forward to **near-term** (it's an ops guardrail,
> not scale-deferred). Authoritative intent: **`design-worker-lease-scheduler.md §4a`** (the decisions
> are made there); this designs the *implementation*. Depends: the worker (built).

## 0. What this is — the missing *enforcement* half
The worker's lease model already does **admission** — "don't run more than N jobs; bound the io/drive/gpu
pools." But it deliberately does **not** enforce **CPU**, because codecs lie about `-threads` (you ask
for 4, libx265/hw-encoders/lookahead spawn more). So today **one transcode can monopolize the whole
box** and starve a concurrent restore or the tape daemon — and a starved tape write means errors /
shoe-shining. P5.2 adds the enforcement half: every CPU-heavy subprocess runs in its **own cgroup v2**
with a kernel-honored **`CPUWeight`**, which gives **proportional fair-share under contention** — a
transcode **cannot starve** a restore or the tape daemon no matter how many threads its codec spawns —
and **elasticity for free** (use spare cores when the box is idle, yield by weight under contention).
**`CPUWeight` is fairness, not a hard cap** (codex): it does *not* limit a job to "≤ N cores"; a hard
ceiling, when a job needs guaranteed headroom, is **`CPUQuota`/`cpu.max`** (decision 3). The guarantee
P5.2 makes is **no-starvation**, not "never oversubscribes."

## 1. The gap, precisely
- **Lease = admission (built):** `required_resources` counted pools; the claimer reserves them.
- **cgroup = moment-to-moment CPU sharing (missing):** the kernel, not ffmpeg, bounds the process tree.
- **Hazard is live now:** P1.2 already enqueues `transcode` jobs; the CPU-heavy subprocesses today —
  `ffmpeg` (`transcode.py:204`), `rem-debug` RAO build (`rao.py:365`, `rem_archive_cli.py`), `ffprobe`
  (`pfr_index.py:118/164`) — run via bare `subprocess.run`, uncapped.

## 2. Decisions
1. **Mechanism: a transient `systemd-run --scope` per heavy subprocess** (not a `Delegate=yes` worker
   service, §7). The scope's cgroup bounds the **entire** subprocess tree no matter how many threads the
   codec spawns. It's the portable option — it works whether or not the worker itself is a systemd unit,
   and needs no delegated subtree. Cost (a `systemd-run` fork per heavy subprocess) is negligible against
   a multi-minute transcode. `Delegate=yes` is a later throughput optimization (§7).
2. **One `run_managed` wrapper that every heavy subprocess routes through.** A new
   `src/sutradhara/resource_control.py::run_managed(cmd, *, role, cpu_lease=None, io_class=…, timeout=…)
   -> subprocess.CompletedProcess`. Replaces the bare `subprocess.run` in `transcode`, `pfr_index`,
   `rao`, `rem_archive_cli`. The CPU hog is the **subprocess**, so wrapping it (not the in-process Python
   handler) is the right grain.
3. **`CPUWeight` = no-starvation + elasticity; `CPUQuota` = the *only* hard cap (codex High).** Be precise
   about what each delivers: `CPUWeight` gives proportional fair-share under contention (**no-starvation**)
   **and** elasticity (a job alone on an idle box uses all spare cores), but it is **not** a ceiling — it
   never limits a job to "≤ N cores." `CPUQuota`/`cpu.max` is the only thing that imposes a **hard cap**,
   at the cost of elasticity (it caps even when the box is free). So:
   - **Default = `CPUWeight` only** (elastic, no-starvation) for the general case.
   - **`CPUQuota` is opt-in per profile** — the right tool when a job needs to leave **guaranteed
     headroom**, e.g. capping derivation at `(cores − reserve)%` to hold fixed CPU for the rem **tape
     daemon** if weighted fairness proves insufficient on the real box (§7).
   Add `IOWeight` + `ionice` for I/O, and `nice` to complement CPU scheduling (decision 4).
4. **Role-based profiles, role keyed by job KIND (codex Low); `nice ≥ 0` only (codex Medium).** The
   execution role comes from the **job kind** (a per-kind registry). `Job.priority` is the integer
   **dispatch-ordering** field (who gets a lease first — §4a's *first* priority layer) and is **not** the
   execution role; do not conflate them. A per-job execution-role override, if ever needed, is an explicit
   `params["resource_role"]`, not the integer. `role → ResourceProfile{cpu_weight, io_weight, nice, ionice,
   cpu_quota_pct?}`:
   - **high** — operator/urgent (`restore`, release-verify): **high** `cpu_weight`, **`nice 0`**, normal
     ionice. **Never throttled** — its priority comes from `cpu_weight`, *not* from raising OS niceness.
   - **medium** — fresh-intake pipeline (`transcode`, `pfr-index`, `cloud-blob`): **medium** `cpu_weight`
     (≤ default 100), `nice 0`. Yields to high.
   - **low / best-effort** — background (verify-freshness, re-derive, bulk migration): **low** `cpu_weight`
     (**well below the default 100**), `nice 19`, `ionice -c idle`. Soaks idle capacity, yields to everything.
   **All profiles use `nice ≥ 0`** — an unprivileged worker can lower priority but **cannot raise** it, so
   `nice -1` would fail before the command runs (codex Medium). Negative nice / `SCHED_*` real-time is an
   **opt-in, capability-probed** extra (off by default); the protection comes from `cpu_weight`, not nice.
   To protect the **rem tape daemon** (a separate service at default weight), derivation runs at ≤-default
   weight so it can't out-prioritize the daemon under contention; if fair-share isn't enough on the real
   box, add a `cpu_quota_pct` (decision 3).
5. **Graceful fallback REQUIRED; probe the real property set; child-exit-vs-setup-failure rule explicit
   (codex r3+r4).**
   - **Probe (cached):** a cheap `systemd-run [--user|--system] --scope --quiet --no-ask-password
     -p CPUWeight=100 -p IOWeight=100 [-p CPUQuota=…] -- true`, with a **short timeout**, over the **full
     property set any configured profile uses — including `CPUQuota` when a profile sets `cpu_quota_pct`**
     (codex r4). A user manager can create a scope yet **reject a property** (undelegated `io`, disallowed
     quota), failing *before the child runs*. Cache the **working property subset** (drop + log any rejected
     property, e.g. quota-unavailable); `run_managed` emits only validated properties. `--no-ask-password` +
     the timeout keep `--system` mode from **hanging on polkit/auth** (codex r4).
   - **Child-exit vs setup-failure (codex r4 Medium):** a child's exit — *including nonzero-with-stderr*
     (a real ffmpeg/rem failure) — is **returned once and NEVER retried**. Because the probe pre-validates
     the properties, a runtime `systemd-run` *setup* failure is anomalous; `run_managed` treats a nonzero
     as the **child's** by default, degrading **only** on an unambiguous **pre-child** setup failure —
     systemd-run's own diagnostic (`Failed to start transient scope` / `Failed to set unit properties`)
     **with no child output** (the child never ran). The rare "cgroup state changed after the probe" case
     surfaces as a child failure and is re-attempted by the **job engine's retry** (which re-probes), *not*
     by a per-call rerun — so a real failure can never be double-run.
   - **Degraded** = plain `subprocess.run` + best-effort `nice`/`ionice` (skip any non-permitted
     adjustment) + the `-threads` hint, logging degraded enforcement. **The command always runs; only the
     guarantee degrades.**
6. **`-threads` stays a HINT, never the bound.** Pass `-threads ≈ cpu_lease` to avoid pathological
   intra-cgroup thread thrash, but the guarantee is the cgroup (§4a).

## 3. The wiring
```text
resource_control.py
  ResourceProfile{ cpu_weight:int, io_weight:int, nice:int, ionice:(class,level)|None, cpu_quota_pct:int|None }
  RESOURCE_PROFILES: { 'high': …, 'medium': …, 'low': … }   # the three roles
  ROLE_BY_JOB_KIND:  { 'restore':'high', 'transcode':'medium', 'pfr-index':'medium',
                       'cloud-blob':'medium', 'verify':'low', … }   # per-kind default
  capability(): cached probe `systemd-run [--user|--system] --scope --quiet --no-ask-password
                  -p CPUWeight=100 -p IOWeight=100 [-p CPUQuota=…] -- true`  (short timeout)
                → caches the WORKING property subset incl. CPUQuota-if-a-profile-uses-it   # codex r3+r4
  run_managed(cmd, *, role, cpu_lease=None, timeout=None, **popen_kw) -> CompletedProcess
      # systemd:  systemd-run [--user|--system] --scope --collect --quiet --no-ask-password
      #             --unit=sutradhara-rc-<kind>-<uniq>.scope    # deterministic ⇒ teardown on timeout
      #             -p CPUWeight=<w> [-p IOWeight=<k>] [-p CPUQuota=<q>%]   # only PROBED-OK properties
      #             -- ionice -c<c> -n<l> nice -n<n> <cmd>      # --quiet ⇒ no "Running as unit…" on stderr
      # degraded:  nice -n<n> ionice -c<c> <cmd>   (best-effort; skip any adjustment not permitted) + warn
      # nonzero CHILD exit (incl. ffmpeg/rem error) ⇒ returned ONCE, never retried (codex r4)
      # ONLY a pre-child setup failure (systemd-run diagnostic, no child output) ⇒ degrade
      # on TimeoutExpired: tear down the scope (no orphan), re-raise TimeoutExpired
```
- **Timeout leaves no orphan (codex r3 Low).** The handlers treat `TimeoutExpired` as "the child stopped,"
  so a timed-out managed command must tear down the **whole scope**, not just the `systemd-run` parent.
  `run_managed` names the scope deterministically (`--unit=sutradhara-rc-…`) and, on timeout/abort,
  `systemctl [--user] stop <unit>` (kills the scope's cgroup) on the systemd path; on the degraded path it
  runs the child in a **new process group** and `os.killpg`s it. A test asserts a timed-out managed command
  leaves **no** active scope or child `ffmpeg`/`ffprobe` process.
- **Child output preserved EXACTLY (codex Medium).** The sites parse the child's streams — `ffprobe`/rem
  JSON on **stdout**, `ffmpeg` diagnostics on **stderr**, the child's **returncode**. `run_managed` must
  return a `CompletedProcess` whose `stdout`/`stderr`/`returncode` are the **child's, untouched**.
  **`--quiet`** suppresses systemd-run's `Running as unit…` line (which would otherwise pollute stderr and
  break ffmpeg error-parsing); **`--scope`** runs the command in the foreground and propagates its exit
  code; **do NOT add `--pipe`** (incompatible with `--scope` here). `capture_output`/`text`/`timeout`/
  `check` pass through to the child unchanged.
- **Hook sites:** `transcode._run_ffmpeg`, `pfr_index` (×2), `sealing/rao.py`, `rem_archive_cli` →
  `run_managed(cmd, role=<kind's role>, cpu_lease=<the cpu requirement>, …)`. The handler knows its kind
  (→ role) and its `required_resources` cpu count (→ weight/quota inputs).
- **`--user` vs `--system`** chosen by how the worker runs (§7); default `--user` for an operator-run
  worker, `--system` when the worker is a system service. Detected/configurable. **`--system` always passes
  `--no-ask-password` and the probe/launch carry a short timeout** so a polkit/auth path can never hang or
  block the worker (codex r4).
- **Lease unchanged.** Admission still decides *how many* run; the cgroup decides *how they share*.

## 4. Reuse vs new
**Reused:** the bare `subprocess.run` call sites (now routed through `run_managed`); the worker lease
(admission). The job `priority` field stays **dispatch-only** (it does *not* set the execution role — codex
r3); the execution role is per-kind, overridable via `params["resource_role"]`. **New:** `resource_control.py` (the wrapper +
profile registry + capability probe); the routing edits in the four subprocess sites. **Unchanged:** the
claimer/lease model, the handlers' *logic* (only their subprocess launch changes), the rem tape daemon
(its own service/resources).

## 5. Tests & acceptance
**Tests** (`tests/test_resource_control.py` + light edits where handlers are tested):
- **argv construction** — `run_managed(role='medium', cpu_lease=8)` under a mocked `systemd` capability
  builds the exact `systemd-run --scope --collect --quiet -p CPUWeight=… -p IOWeight=… -- ionice … nice …
  <cmd>` (with `--quiet`, no `--pipe`); `high`/`medium`/`low` map to the right weights/`nice`/`ionice`,
  all `nice ≥ 0`.
- **child output preserved exactly (codex Medium)** — `run_managed(cmd, capture_output=True, text=True)`
  returns the **child's** `stdout`/`stderr`/`returncode` byte-for-byte (no `Running as unit…` in stderr),
  on **both** the systemd and degraded paths — so the ffprobe/ffmpeg/rem parsers are unaffected.
- **graceful fallback** — with `systemd-run` absent (capability='degraded'), the command **still runs**
  via plain `subprocess.run` + best-effort `nice`/`ionice` (any non-permitted adjustment skipped, not
  fatal — codex Medium), and a degraded-enforcement warning is logged. (The existing `transcode`/
  `pfr-index`/`rao` tests stay green on this path — they run without systemd.)
- **property rejection ≠ child failure (codex r3 Medium)** — when `systemd-run` is present but a probed
  property is **rejected** (`IOWeight` undelegated, **or `CPUQuota` disallowed** for a quota profile —
  codex r4), the probe drops to the working subset (quota marked unavailable) and `run_managed` **still
  runs the child** — not a spurious failure before the child starts.
- **child nonzero is returned once; only setup-failure degrades (codex r4 Medium)** — a child that exits
  **nonzero with stderr** (a real ffmpeg/rem error) is returned **once and not retried**; a simulated
  pre-child `systemd-run` setup failure (its `Failed to start transient scope…` diagnostic, **no child
  output**) is the **only** case that degrades. The two are never confused despite both being exit 1.
- **timeout leaves no orphan (codex r3 Low)** — a managed command that hits `timeout` tears down the whole
  scope: **no** active scope/`ffmpeg`/`ffprobe` process remains (systemd: `systemctl stop <unit>`;
  degraded: `killpg`), and `TimeoutExpired` still surfaces to the handler.
- **role from kind, not priority (codex Low)** — each job kind resolves to its profile; the integer
  `Job.priority` does **not** change the execution role; an explicit `params["resource_role"]` override wins.
- **`-threads` is a hint** — passed ≈ lease, but no test relies on it for any cap.
- **live (`~/system`, real cgroup box)** — the plan acceptance: a **lone transcode uses spare cores**;
  under a concurrent **high**-weight job, **neither is starved** — the kernel shares CPU by weight and the
  high job keeps its share regardless of ffmpeg's thread count (assert via cgroup `cpu.stat` / observed
  shares). If a profile sets `cpu_quota_pct`, that job is also capped at the quota. Gated to a systemd host.
- **existing suites green** — `uv run pytest` (the fallback must not change behavior where systemd is absent).

**Acceptance** (plan P5.2, **restated to match weight-vs-quota reality, codex High**): a lone transcode
uses spare cores and **yields by weight** under contention; **operator/tape work is never starved** by
background derivation regardless of codec threads (weighted fairness). A hard "≤ N cores" ceiling, where a
job needs guaranteed headroom, is delivered by an opt-in `CPUQuota` — **not** by `CPUWeight`.

## 6. Scope (not here)
- **No reconciler scaling (P5.1) / multi-worker (P5.3)** — separate, demand-driven.
- **No `Delegate=yes` worker** — the per-subprocess scope path first; the delegated-subtree variant is a
  throughput optimization for high job rates (§7).
- **No GPU or memory cgroup limits** — CPU + I/O first; `MemoryMax`/gpu enforcement are follow-ups (the
  gpu *lease* already exists for admission).
- **No change to the rem tape daemon** — it's a separate service; P5.2 just stops sutradhara's CPU hogs
  from starving it.

## 7. Open decisions
1. **`systemd-run --user` vs `--system`** — depends on how the worker is deployed (operator process vs
   system service). Default `--user`, configurable; document the unit setup for `--system`.
2. **`CPUQuota` policy** — weight-only (chosen, elastic) vs an opt-in hard ceiling ≈ the lease for jobs
   that must not exceed (e.g. to reserve fixed headroom). Lean weight-only; expose a per-profile
   `cpu_quota_pct` for the few cases that want a cap.
3. **The exact weight numbers** — e.g. `CPUWeight` high/medium/low = 1000 / 300 / 100 (defaults; tune on
   the real box). IOWeight + ionice classes likewise.
4. **`Delegate=yes` worker (later)** — at high job rates, owning a delegated cgroup subtree and writing
   `cpu.weight`/`cpu.max` directly avoids a `systemd-run` fork per subprocess. Revisit if fork overhead
   ever matters; the `run_managed` seam makes the swap internal.
