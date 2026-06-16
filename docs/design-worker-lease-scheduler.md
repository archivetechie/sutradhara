# Design — sutradhara job execution model: worker, resource leases, granularity, validation

> Status: **design, for review** (the owner + Claude, 2026-06-16). Synthesised from a
> three-generation job-framework comparison (d2 custom / d3 Prefect / sutradhara)
> — memory `job-framework-lineage-lessons`. Decisions taken: **single-node lease
> worker now**; **atomic jobs + DAG, no per-file sub-rows**; **archive-everything:
> always archive, flag condition, gate access not preservation**. Codex prompts:
> `prompt-job-worker-leases.md` (this engine core) + the updated R prompts (its
> first consumers). Per-repo: lives in sutradhara/docs (engine work).

## 1. Goal & non-goals
**Goal:** make `jobs/` run jobs **concurrently and bounded** on one box, turn the
dead `required_resources`/`prerequisites` columns into enforced behaviour, settle
**job granularity** (so multi-file work doesn't recreate d2's `completed_failures`
swamp), and make **validation/corruption** a first-class, archive-everything-safe
part of the model.

**Non-goals now:** multi-node worker fleet; Postgres; a separate control plane
(server/UI/broker); GPU scheduling; per-tenant fairness. The lease model, handler
contract, and granularity rules are designed so the multi-node move later
(Postgres + `SELECT … FOR UPDATE SKIP LOCKED`) changes only the *claim*.

## 2. Starting point
`jobs/` has a `Job` table (`kind`/`params`/`status`/`step_state`/`attempts`/
`required_resources`/`prerequisites`/timestamps), a `register_handler(kind)`
registry, and handlers `verify`/`copy`/`restore`. But `run_pending` is a sequential
loop, `claim_pending` is not atomic, and leases + prerequisites are **stored and
never read**. Spec `spec-v0.1.md §6.3–6.6` designs exactly the scheduler below.

## 3. Resource-lease model (the one gate)
Resources are **counted pools** with a capacity; a job declares what it needs; the
scheduler dispatches only when **every** requested lease fits the remaining budget,
reserves on dispatch, releases on terminal.
- Pools (config capacities): `cpu` (default = box logical cores), `io` (a **hard
  concurrent-count cap** on heavy sequential reads, e.g. 2–3), `tape_drive`
  (= physical drives — **defined now, wired to real tape jobs in a later phase**),
  `gpu` (0/omitted now).
- Declaration: `required_resources: [{pool, count}]` — e.g. `transcode → [{cpu,8}]`,
  `pfr-index → [{io,1},{cpu,1}]`, `cloud-blob → [{io,1}]`.
- Invariant `Σ leased[pool] ≤ capacity[pool]`. ffmpeg `-threads` is **pinned to the
  leased `cpu` count** (fixes d2's uncapped-ffmpeg fire). Lease accounting is
  in-memory in the single worker process (authoritative — one worker).

## 4. Worker + scheduler (single-node)
**One worker process = one claimer/scheduler thread + a bounded execution pool.**
- The **claimer thread** owns dispatch. Loop: scan PENDING jobs whose
  **prerequisites are all SUCCEEDED** *and* whose `not_before` has passed, in
  priority/FIFO order; **work-conserving** — if the head job's leases don't fit,
  try later eligible jobs that *do* fit, but apply **aging** so a large `cpu:8` job
  can't be starved indefinitely by a stream of small ones (once a job has waited N
  scans it blocks smaller jobs from jumping it). For a dispatchable job, **claim it
  atomically** (`UPDATE job SET status='running', started_at=…, attempts=attempts+1
  WHERE id=? AND status='pending'`), reserve its leases, submit its handler to the
  execution pool.
- The **execution pool** is a `ThreadPoolExecutor` — correct because heavy jobs are
  subprocesses (ffmpeg/rem CLI) and the GIL is released while they run. The real
  concurrency bound is the lease budget, not pool size.
- On completion (success / `ok=False` / exception) the worker **releases the
  leases** and applies the retry policy. One claimer thread ⇒ no in-process claim
  race; the atomic `UPDATE` is belt-and-suspenders + forward-compatible with
  multi-worker Postgres.
- **Prerequisites (DAG)** are enforced at claim time — a job with an unmet prereq is
  skipped, not run. This is the "wait for all of N" primitive (see §5).
- **Retry/backoff:** add a `not_before` (a.k.a. `available_at`) timestamp column.
  On failure, if `attempts < max_attempts` (per-kind default, global fallback)
  re-enqueue PENDING with `not_before = now + backoff(attempts)`; else terminal
  FAILED. The claimer honours `not_before`.
- **Crash recovery:** on worker startup any job left `RUNNING` is orphaned (single
  worker ⇒ nobody else owns it) → reset to PENDING; in-memory leases vanish with the
  process, so budget is reclaimed. Fixes d2's "job holding a tape indefinitely".

## 5. Job granularity & multi-file units
**Rule: a job is the smallest unit of atomic, independently-retryable work. Choose
the granularity so the job is atomic; compose multi-unit work with the prerequisite
DAG — never with per-file sub-rows.** Two patterns:
- **Pattern A — atomic over many files → ONE job.** `cloud-blob` (one `rem archive
  build` over the intake dir), the Phase-S seal (one RAO build over an artifact),
  a tape write (one positional pass). No per-file partial success exists, so there
  is nothing to roll up; status is unambiguous.
- **Pattern B — independent per file → N jobs.** `transcode` (one ffmpeg per video
  file), `pfr-index` (one extract per file). Each is its own atomic job row, keyed
  for **enqueue idempotency** on `(kind, ingest_item_id)` so a re-scan never
  double-enqueues. They schedule independently under the lease budget.

**Completion of a multi-file unit** is answered two ways — never by a rolled-up
status field that racing workers mutate (d2's bug):
1. **A DAG join node, only at genuine synchronisation points** (e.g. the Phase-U
   release gate waiting on all of an artifact's copy jobs): a job whose
   `prerequisites` are the N child ids becomes eligible exactly when all N are
   SUCCEEDED. Don't add joins for fan-outs nothing waits on (proxies are
   fire-and-forget).
2. **A read-time query** for progress/reporting (`count(... where intake_id=X and
   status != SUCCEEDED)`); jobs carry `intake_id`/`ingest_item_id` in params.

There is **no `completed_failures` state.** Best-effort vs must-succeed is expressed
by the handler's `ok`: a best-effort proxy returns `ok=True` even when it makes no
proxy (so it never blocks a join); a must-succeed copy returns `ok=False`→FAILED
(holding its join until retried). This dissolves d2's per-file-rollup races,
`completed_failures`, requeue-whole-vs-failed-files, and `processingfailure`
throttling — each file's outcome is its own atomic job.

## 6. Validation depth & corruption handling
**Fixity ≠ validity.** A hash answers "did the bytes change since we first saw
them" (needs a reference; catches post-first-hash drift). A decode answers "are the
bytes a valid media file" (no reference; catches corruption already present —
source-side, or transfer corruption in the baseline/no-prior-manifest case — which
hashing *cannot* catch). We want both. Validation **depth** layers, cheap→dear:
1. **hash** — fixity.
2. **container parse** (`pfr-index`/ffprobe) — well-formedness (truncation, bad
   `moov`/MXF index).
3. **full decode** — essence validity. A *completed* proxy `transcode` already
   decodes every frame, so for proxied video it doubles as validation for free; a
   dedicated `validate` job covers non-proxied / non-video types.
**Principle: validate before committing to tape** where practical — a corrupt master
discovered after sealing is a preserved-but-broken master.

**Archive-everything — gate access, not preservation.** A file that fails decode is
**still archived** (its bytes may be recoverable later with expert tooling; refusing
to preserve it is culling). It is **flagged** in the catalog and **gated out of
normal restore**. Concretely:
- `LogicalAsset` gains a `validity` field (`ok` | `suspect` | `unvalidated`) + a
  condition note. (Validity is a property of the *content*, so it lives on the
  content-addressed asset, not per-occurrence.)
- The `transcode`/`validate` handler classifies failure **two ways**:
  - **decode/corruption error** → set `validity=suspect` + record the **hash×decode
    diagnostic** (hash-matched-but-undecodable ⇒ source-corrupt; hash-mismatch ⇒
    transfer-corrupt), make no proxy, **archive normally**.
  - **benign/operational** (unsupported codec, OOM, timeout) → just `no-proxy`;
    `validity` unchanged; archive + restore normally.
- The **restore path gates on `validity`**: a normal restore of a `suspect` asset is
  **refused** (or returns with a loud condition warning requiring an explicit
  `--force`/expert flag); search/browse badge it "flagged — may be damaged".
- The seal is unaffected: its per-member fixity check (`file_sha256` == registered)
  passes for a corrupt file (consistent hash of corrupt bytes); the seal never
  decodes.
- **One edge:** a file unreadable at the *filesystem* level (I/O error — bytes
  physically unavailable) is the only genuine can't-archive case; the handler must
  distinguish a read error from a decode error.

Because the suspect file is **archived in its artifact's bundle like everything
else**, there is no hold, no disk-pressure stall, and **no artifact split** — the
worry that a held file lands in a separate bundle disappears. Re-offload (while a
card source is still held) remains the *preferred* fix for transfer corruption, but
it is an optimisation, never a gate; the fallback is archive-it-flagged, never
exclude. The Phase-S send-scan still *surfaces* suspect members for the artifact
owner, but the default is archive-flagged.

## 7. First consumers & sequencing
Phase R's jobs are the first consumers and the reason to build this now:
- `transcode` — `[{cpu,8}]`, CPU-bound, Pattern B (per video file); two-mode failure
  + `validity` flag (§6).
- `pfr-index` — `[{io,1},{cpu,1}]`, **I/O-bound, parse-only** (ffprobe, no decode),
  Pattern B (per high-bitrate video file); extracts header/footer/index → sidecar
  (memory `pfr-pre-ingest-high-bitrate`). Reads the **original**, sibling of
  `transcode`.
- `cloud-blob` — `[{io,1}]`, Pattern A (one rao-aead object per intake → S3).

**Sequencing:** this engine core lands **before** R's handlers (which declare
resources, set `validity`, and run under the worker). R's *intake-scan + register*
half is synchronous and independent of the worker, so it can proceed in parallel.

## 8. Config
One source (no drift — d3's lesson): pool capacities (`cpu`/`io`/`tape_drive`/`gpu`),
per-kind default `required_resources`, per-kind `max_attempts`/backoff, execution-
pool size. `cpu` defaults to `os.cpu_count()`; drive capacity from the tape layer.

## 9. Storage substrate
**SQLite now** (WAL) — one worker process = one writer, no cross-process claim
contention. Migration trigger to **Postgres + `SELECT … FOR UPDATE SKIP LOCKED`** is
"workers on more than one process/box"; only `claim` changes then.

## 10. Lessons encoded
- Resource leases as the **single** gate — fixes d2's split gating + uncapped-ffmpeg
  fire.
- Atomic claim + enforced prereqs — fixes d2's re-read-before-save races.
- Atomic jobs + DAG join, no sub-rows — fixes d2's `completed_failures` swamp +
  whole-artifact requeue.
- ThreadPool over subprocesses, one loop of control — avoids d3's
  `ConcurrentTaskRunner` "event loop is closed".
- DB is the queue and the **only** state authority; no control plane — avoids d3's
  server+PG+fleet+shim weight and its two disagreeing state machines.
- Archive-everything, gate access not preservation — fixes d2's "failed transcode
  blocks the flow / fills the disk" while keeping the corruption signal and the
  bytes.

## 11. Resolved decisions (formerly open)
1. **Backoff:** add a `not_before`/`available_at` column. **Resolved: yes.**
2. **Head-of-line:** **work-conserving + aging** (let a small job that fits run ahead
   of a big one that doesn't, with starvation protection). **Resolved.**
3. **`io` pool:** **hard concurrent-count cap.** **Resolved.**
4. **`tape_drive` pool:** **define the pool now**, wire real tape-write jobs in a
   later phase. **Resolved.**
5. **Staging retention of suspect files** (park): default "it's on tape, expert
   recovery restores it"; optionally keep suspects warm — **deferred policy knob.**
