# Design — P2.5: Archive an arrangement from its frozen source-map

> Design by Claude + the owner + codex (2026-06-26), for review then implementation. **Repo: sutradhara
> (+ a `~/system` scenario).** Plan item **P2.5** (`docs/implementation-plan-ingest-v2.md`), arc §3.9/§7.5.
> Depends: **P2.3a** (the frozen `submission`/source-map, shipped) + **P2.4** (`rem archive build --map`,
> shipped). Consumes the round-trip P2.4 built.

## 0. What this is — the payoff slice
Drive **`rem archive build --map`** (P2.4) from a frozen P2.3a **`submission`**: build the arranged RAO
straight from the originals (no copied 4K staging tree), write it to the artifactclass's policy pools
(working RAO + offsite RAO-AEAD + d2 shelf), record the **bundle `Copy` + per-member `AssetLocator`s**,
and flip `submission.status` `pending_archive → archived`. **Acceptance:** archive an arrangement with
**no staging copy**, then restore an arranged member → `sha256 == original master`. That closes the
whole receive→arrange→archive→restore loop.

## 1. The shape — a new input front-end to the *existing* archive core
P2.5 is **not new archive machinery.** sutradhara already has the full pipeline — `flush_bundle()`
(`archive_fanout.py:437`): `Bundle` of members → `RemArchiveBuilder.build()` (shells `rem archive
build`) → `backend.write_object_to_pool()` (gRPC to tape) → `add_bundle_copy` (Copy keyed on
`bundle_id`) + per-member `record_asset_locator` (`{member_path, first_chunk_lba, size_bytes}`) →
`close_bundle` (sealed) — and it already fans out to the **offsite RAO-AEAD** pool (the RAS scenario).
It is **imperative (CLI-driven), not a reconciler**; the per-asset copy reconciler stays out of it.

So P2.5 is to `flush_bundle` exactly what P2.4's `--map` was to rem's build core: a **front-end that
feeds the existing pipeline an explicitly-arranged member set** instead of an accumulator/tree-walk.
The new code is small and bounded (§4) and adds **zero schema**: a `--map` adapter, a
submission→open-bundle projection, and the reusable `archive_submission()` core + CLI.

## 2. The decisions (codex review folded in)
1. **Imperative CLI, reusable core — *not* a job (yet).** Ship `sutra archive submission flush
   <submission_id>` over a reusable
   `archive_submission(session, submission_id, *, backends, builder, key_epoch, …)`. **Not a job:** the
   engine's default `RetryPolicy.max_attempts` is **1** and retry/backoff is applied by `JobWorker`, not
   the `sutra jobs run` path — so a job adds scope (resources, retry config, operator flow) without
   automatically buying retry. **But** the core is written with **durable/idempotent boundaries as if a
   job will wrap it later**: a thin `archive-submission` handler with a `tape_drive` lease + configured
   retries is then a clean, additive wrapper around the same function.
2. **A pre-populated *open* bundle (not "already-complete").** `flush_bundle` refuses any bundle whose
   `status != "open"` (`archive_fanout.py:458`). So `archive_submission` creates a **deterministic,
   submission-owned `Bundle` in status `open`**, fully enumerated up front (one `BundleMember` per
   `submission_member`, populating exactly `_member_input`'s fields — `logical_asset_hash`,
   `member_path = archive_path`, `source_path`, `size_bytes`, `file_sha256`), **skipping the threshold
   accumulator**, then lets `flush_bundle` move it `open → flushing → sealed`.
3. **`--map` is real new sutradhara adapter code.** `run_rem_archive_build()` only emits `--inputs`
   today (`rem_archive_cli.py:110`). Add `run_rem_archive_build(…, map_path=, source_root=, map_sha256=)`
   and `RemArchiveBuilder.build(…, map_mode=…)`; `flush_bundle` takes the map path for a map-sourced
   bundle (instead of `--rules`/`--inputs`). The rules-oriented **conformance scan gate**
   (`builder.scan()`, `archive_fanout.py:471`) is **neutralized in map mode** — the arrangement *is* the
   explicit, operator-chosen member set; there's no filesystem tree to blob-suggest or exclude.
4. **`--source-root` is a sandbox guard; absolute paths, no map synthesis — derivation pinned.** P2.3a's
   `source-map.tsv` stores **absolute** `source_path`s; P2.4 accepts absolute sources and uses
   `--source-root` only as the containment anchor (canonicalize + `starts_with`). So P2.5 hands rem the
   **existing** `/replica/submissions/<id>/source-map.tsv` directly — **no rem-facing map is
   synthesized** — with `--source-root` derived from **current catalog fields**: `Intake` has no
   source-root column (it stores `manifest_path`, `models.py:108`), so the BagIt payload root is
   **`Path(intake.manifest_path).parent / "data"`**, canonicalized. sutradhara **proves every
   `source_path` canonicalizes inside that root before invoking rem** (the single-intake invariant, P2.3a
   §8) and **fails closed** if `manifest_path` is absent, the derived root doesn't exist, or any source
   escapes. It passes `--map-sha256 = submission.manifest_digest` as the transit-integrity check.
5. **The catalog mapping stays sutradhara-owned.** rem's report returns, per member, the `archive_path`
   + hash/size + locator geometry (`first_chunk_lba`). P2.5 matches report members → `submission_member`
   **by `archive_path`** (unique per submission), takes `logical_asset_hash` from the `submission_member`
   (its `sha256`, = `IngestItem.logical_asset_hash`), and records the `AssetLocator`. The
   `ingest_item_id` rem echoes is an **integrity cross-check only** (assert it matches), never the
   catalog key — **Remanence is never responsible for sutradhara identity.**
6. **Partial-failure contract: all-or-nothing per call (chosen); resume deferred.** This is the
   load-bearing call (codex). `flush_bundle` today mutates bundle/copy/locator state in one session and
   rolls the **DB** back on any exception — but physical pool writes already performed are **not** rolled
   back. P2.5 makes that explicit and keeps the small-adapter shape: **`archive_submission` runs in one
   `session_scope` with no intermediate commits**, so a failure rolls back **all** catalog state (the
   bundle returns to non-existent, no copies/locators), the submission stays `pending_archive`, and any
   RAO already written to tape becomes an **orphan with no catalog reference** — reclaimed by a tape-GC
   sweep (the same accepted-orphan pattern as P2.3a's submission dirs; the sweep is deferred). **Retry =
   full re-archive** (re-write every pool). Because nothing partial is ever committed, **the recorders do
   NOT need to be made idempotent** (the earlier draft's "idempotent recorders → rerun records nothing"
   was wrong — a rollback leaves nothing to collide with; an over-long retry just produces fresh copies +
   orphans). The *only* idempotency is the **no-op early return when the submission is already
   `archived`** (decision 8). **Per-target checkpoint/resume** (commit each pool's copy; skip done pools
   on rerun) is a real **follow-up and an enhancement to `flush_bundle` itself** (it would help the
   accumulator path too), explicitly **out of P2.5**. The named cost — re-writing all pools + orphan tape
   objects on retry — is the price of staying a small adapter; resume is the optimization for when retry
   cost bites.
7. **Submission↔bundle linkage by *derived id* — zero schema change.** Bundle id =
   `submission-<submission_id>` (1:1, submission-owned); the reverse lookup is `session.get(Bundle,
   f"submission-{submission_id}")`, so **no `submission.bundle_id` FK is added** (the earlier draft's FK
   is dropped). This keeps the **P2.3a model genuinely unchanged**: P2.5 adds **no schema columns** — it
   reuses `Bundle`/`BundleMember` as-is, and the map context (`source_map_path`, `source_root`) is passed
   to `flush_bundle` as **call parameters**, re-derived from the submission each call, **not persisted**.
8. **Lifecycle: flip last, one commit.** `submission.status` `pending_archive → archived` (+
   `archived_at`) is set **only after every target-pool copy is durably recorded** (the bundle reaches
   `sealed`), in the **same `session_scope` commit** as the bundle/copies/locators (decision 6) — archive
   and flip are atomic. A failure before commit leaves `pending_archive` (and orphan tape objects); retry
   re-archives. The `archived` flip is also what later lets the P3.2 deletion gate release the
   landing-data hold P2.3a flagged.

## 3. The flow — `archive_submission(session, submission_id, …)`
Runs inside **one `session_scope`** (decision 6 — all-or-nothing):
1. **Load + guard.** Load the `submission`; `archived` → **no-op early return** (decision 8); not
   `pending_archive` (e.g. abandoned) → error. Load its `submission_member`s (ordered).
2. **Prove the anchor.** Derive `source_root = Path(intake.manifest_path).parent / "data"` (canonical);
   verify **every** `submission_member.source_path` canonicalizes **inside** it — fail closed before
   invoking rem if `manifest_path` is absent, the root is missing, or any source escapes (decision 4).
3. **Project the open bundle.** Create `Bundle(id="submission-<id>", artifactclass=
   submission.artifactclass, status="open")` + one `BundleMember` per `submission_member`
   (`logical_asset_hash`, `member_path=archive_path`, `source_path`, `size_bytes`, `file_sha256`). No
   persisted map columns — `source_map_path`/`source_root` are passed to `flush_bundle` (decision 7).
4. **Flush via map mode.** `flush_bundle(bundle, backends, builder, key_epoch, map_path=
   submission.source_map_path, source_root=…)` → per target pool: `RemArchiveBuilder.build(map_mode,
   source_map_path, source_root, map_sha256, encrypt=<pool is aead>)` → `backend.write_object_to_pool`
   → `add_bundle_copy` + `record_asset_locator` per member (matched by `archive_path`; `ingest_item_id`
   cross-checked) → `close_bundle` (sealed).
5. **Flip.** `submission.status = archived`, `archived_at = now` (linkage is the derived bundle id; no FK).
6. **Caller commits** the single transaction (no-commit discipline), as P2.3a. Any failure before this
   rolls everything back; orphan tape objects are swept later (decision 6).

## 4. Reuse vs. new code
**Reused unchanged:** `flush_bundle` build/write/record/fan-out, `target_pools`, `add_bundle_copy`,
`record_asset_locator`, the AEAD pool path + `KeyRegistry`, `backend.write_object_to_pool` (rem gRPC /
d2 CLI), the build-report parse. **New (bounded, zero schema):** `run_rem_archive_build` map args +
`RemArchiveBuilder` `map_mode`; `flush_bundle` gains optional `map_path`/`source_root` **parameters** (a
map-mode branch: build via `--map`, neutralize the scan gate) — no new columns; the submission→open-bundle
projection; `archive_submission()` + `sutra archive submission flush`; the `submission.status` flip.
**Unchanged:** rem (P2.4 done), the **P2.3a model (no migration)**, the copy reconciler, the
`--rules`/`--inputs` accumulator path. **Explicitly not changed:** the recorders stay plain inserts —
all-or-nothing (decision 6) means they're never replayed against committed state.

## 5. Tests & acceptance
**Tests** (`tests/test_archive_submission.py`, memory/d2-stub backends):
- **archive a submission** — working-RAO + offsite-AEAD (+ d2 shelf) bundle copies recorded; per-member
  `AssetLocator`s with correct `member_path`(=archive_path)/`first_chunk_lba`/`size`; bundle `sealed`;
  `submission.status=archived`, `bundle_id` set.
- **the payoff** — restore an arranged member (P2.2 member-aware restore) → `sha256 == original master`,
  and the restored handle's name is the **arranged** `member_path`.
- **no staging copy** — assert no 4K tree is materialized (rem reads originals via `--map`).
- **no-op replay** — `archive_submission` on an already-`archived` submission returns early; writes
  nothing; no new `Copy`/`AssetLocator`.
- **partial-failure = full rollback (the contract, decision 6)** — a pool write fails mid-flush → the
  whole `session_scope` rolls back: **no** `Bundle`/`Copy`/`AssetLocator` rows, `submission` stays
  `pending_archive`; an already-written RAO is an orphan with **no** catalog row (assert none references
  it). A **re-run re-archives from scratch** and ends `archived` (one valid bundle; the prior physical
  write remains an unswept orphan — names the cost, not a bug).
- **source-root anchor** — a `submission_member.source_path` escaping the intake root → refused **before**
  invoking rem.
- **identity stays ours** — locators are keyed by `submission_member`→`logical_asset_hash` (matched by
  `archive_path`); a report whose echoed `ingest_item_id` disagrees with the `submission_member` → fail
  (cross-check, not key).
- **regression** — the existing `flush_bundle` `--rules`/`--inputs` path unchanged.
- **`~/system` end-to-end** (the loop P2.4 deferred here): arrange → submit → `archive submission flush`
  (rem `--map`) → `archive extract`/restore → `sha256 == original`. Hermetic where possible; live-VTL
  variant alongside RAS.
- **gates** — `uv run pytest` + format + type-check green.

## 6. Scope (not here)
- **No projection / gateway** — separate project (arrangement gateway).
- **No per-target checkpoint/resume** — all-or-nothing per call (decision 6); resume is a deferred
  enhancement to `flush_bundle` itself, not P2.5.
- **No schema change** — reuses `Bundle`/`BundleMember`/`submission` as-is; no migration, no new column
  (decision 7). A schema-parity test is therefore *not* needed for P2.5.
- **No job wrapper** — later, as a thin handler over `archive_submission` (decision 1); the core is
  job-ready.
- **No multi-intake submissions** — single-intake (P2.3a §8); `--source-root` is one root.
- **No landing-data deletion** — P2.5 only flips `archived` (which *unblocks* the P3.2 deletion gate); the
  gate itself is P3.2.
- **No rem change** — P2.4 is shipped; if a contract gap surfaces it's a P2.4 follow-up, not P2.5.

## 7. Open decisions
1. **Partial-failure model** — **RESOLVED (decision 6): all-or-nothing per call**, orphan tape objects
   swept later; recorders stay plain inserts. Per-target resume is a deferred `flush_bundle` enhancement
   (a *future* item, not an open P2.5 question).
2. **Submission↔bundle linkage** — **RESOLVED (decision 7): derived id `submission-<id>` only**, no
   `submission.bundle_id` FK, zero schema change.
3. **Scan-gate in map mode** — skip entirely vs. run with `expect="permissive"`. Lean: skip (no tree to
   scan); keep `Bundle.scan_summary` empty/`map`. (Genuinely open.)
4. **d2 shelf copy for arrangements** — included by the artifactclass policy (`target_pools`, like RAS)
   vs. arrangement-specific policy. Lean: policy-driven, no special-casing. (Genuinely open.)
