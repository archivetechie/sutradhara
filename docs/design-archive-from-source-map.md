# Design — P2.5: Archive an arrangement from its frozen source-map

> Design by Claude + the maintainer + codex (2026-06-26), for review then implementation. **Repo: sutradhara
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
The new code is small and bounded (§4) — **one width-only migration plus logic**: a `--map` adapter, a
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
5. **Catalog mapping stays sutradhara-owned — but the parser must be extended to carry it.** rem's
   report returns, per member, `archive_path` + hash/size + locator geometry (`first_chunk_lba`) + the
   echoed `ingest_item_id`. P2.5 matches report members → `submission_member` **by `archive_path`**
   (unique per submission), takes `logical_asset_hash` from the `submission_member` (its `sha256`, =
   `IngestItem.logical_asset_hash`), and records the `AssetLocator`. The echoed `ingest_item_id` is an
   **integrity cross-check only**, never the catalog key — Remanence is never responsible for sutradhara
   identity. **Caveat (codex):** sutradhara's current parser (`_normalized_rem_build_report` /
   `_members_from_manifest`, `archive_fanout.py:932/:957`) **drops `ingest_item_id`** and has nowhere to
   carry it — so P2.5 must make it **map-mode aware** to thread `ingest_item_id` through, then cross-check
   `ingest_item_id` + `size_bytes` + `file_sha256` against the matched `submission_member`. That is why
   "build-report parse reused unchanged" is **not** true (corrected in §4).
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
7. **Submission↔bundle linkage by *derived id*; the P2.3a tables stay untouched.** Bundle id =
   `submission-<submission_id>` (1:1, submission-owned); the reverse lookup is `session.get(Bundle,
   f"submission-{submission_id}")`, so **no `submission.bundle_id` FK is added** (the earlier draft's FK
   is dropped), and the map context (`source_map_path`, `source_root`) passes to `flush_bundle` as **call
   parameters**, not persisted columns. The P2.3a *arrangement/submission* tables are **not** modified;
   P2.5's only schema change is the bundle/locator `member_path` widen (decision 9).
8. **Lifecycle: flip last, one commit.** `submission.status` `pending_archive → archived` (+
   `archived_at`) is set **only after every target-pool copy is durably recorded** (the bundle reaches
   `sealed`), in the **same `session_scope` commit** as the bundle/copies/locators (decision 6) — archive
   and flip are atomic. A failure before commit leaves `pending_archive` (and orphan tape objects); retry
   re-archives. The `archived` flip is also what later lets the P3.2 deletion gate release the
   landing-data hold P2.3a flagged.
9. **Path widths: widen `member_path`, fall back for `source_path`.** P2.3a arranged paths are up to
   **2048** chars, but the bundle/locator tables are narrower — `BundleMember.member_path` /
   `AssetLocator.member_path` are **1024**, and `BundleMember.source_path` is **2048** (vs
   `submission_member.source_path`'s 4096). `member_path` is the indexed unique key with **no
   side-channel**, so P2.5 **widens both `member_path` columns 1024 → 2048** — a width-only Alembic
   migration on the bundle/locator tables (backward-compatible; it also lifts the accumulator path's
   ceiling) — keeping the pipeline consistent (arrange→submit→archive all 2048) instead of leaving
   "submit accepts what archive can't store." `source_path` keeps the existing
   **`source_metadata["source_path_bytes_hex"]` fallback** (no widen — the idiomatic long/non-UTF-8-path
   mechanism `_member_source_path` already reads).

## 3. The flow — `archive_submission(session, submission_id, …)`
Runs inside **one `session_scope`** (decision 6 — all-or-nothing):
1. **Load + guard.** Load the `submission`; `archived` → **no-op early return** (decision 8); not
   `pending_archive` (e.g. abandoned) → error. Load its `submission_member`s (ordered).
2. **Prove the anchor + re-verify sources (before any external write).** Derive `source_root =
   Path(intake.manifest_path).parent / "data"` (canonical); verify **every**
   `submission_member.source_path` canonicalizes **inside** it; **and re-hash + re-stat each source
   against its `submission_member` `sha256`/`size`**. Fail closed if `manifest_path` is absent, the root
   is missing, any source escapes, or any source has **drifted** since submit. This is the only thing
   that catches same-size byte drift **before** a tape write — rem map mode validates containment + size
   but not source *content* (codex: it trusts the map's `sha256` for metadata), so without this a drifted
   source is only caught by post-write fan-out verification, leaving an **avoidable orphan tape object**.
   The cost is one local source re-read — cheap relative to the tape write it guards, and consistent with
   P2.3a's re-verify discipline.
3. **Project the open bundle.** Create `Bundle(id="submission-<id>", artifactclass=
   submission.artifactclass, status="open")` + one `BundleMember` per `submission_member`
   (`logical_asset_hash`, `member_path=archive_path` into the **widened 2048 column**, `size_bytes`,
   `file_sha256` — decision 9). **The source path goes in `source_metadata["source_path_bytes_hex"]`**
   (the fallback `_member_source_path` already reads, `archive_fanout.py:854`), **not** the
   `BundleMember.source_path` column — `SubmissionMember.source_path` (4096) is wider than
   `BundleMember.source_path` (2048), so a long but valid path would fail on Postgres (codex). No
   persisted map columns — `source_map_path`/`source_root` pass to `flush_bundle` (decision 7).
4. **Flush — map mode is per-target.** `flush_bundle(bundle, backends, builder, key_epoch,
   map_path=submission.source_map_path, source_root=…)` → `_build_for_target` dispatches by
   representation:
   - **RAO / RAO-AEAD targets** → `RemArchiveBuilder.build(map_mode, source_map_path, source_root,
     map_sha256, encrypt=<aead>)` (the new `--map` path). The map-aware parser carries `ingest_item_id`;
     recording **cross-checks `ingest_item_id` + `size_bytes` + `file_sha256` vs the matched
     `submission_member`** (§4/decision 5); locators carry **`first_chunk_lba`/`size`**.
   - **d2 shelf (`D2TAR_RAW`) target** → the **existing `_build_d2_tar`** path (`archive_fanout.py:610`),
     **unchanged** — it builds from the bundle's `MemberInput`s (not `--map`/rem), and its locators carry
     **`block_range`/`size`**, not RAO geometry, with **no `ingest_item_id` round-trip**. (This is why the
     step-3 `BundleMember`s — source via the `source_metadata` fallback — are still needed: they feed the
     d2 `MemberInput`s and the d2 member matching.)
   Then per target: `backend.write_object_to_pool` → `add_bundle_copy` + `record_asset_locator` →
   `close_bundle` (sealed) once **all** targets are recorded.
5. **Flip.** `submission.status = archived`, `archived_at = now` (linkage is the derived bundle id; no FK).
6. **Caller commits** the single transaction (no-commit discipline), as P2.3a. Any failure before this
   rolls everything back; orphan tape objects are swept later (decision 6).

## 4. Reuse vs. new code
**Reused unchanged:** `flush_bundle` build/write/record/fan-out, `target_pools`, `add_bundle_copy`,
`record_asset_locator`, the AEAD pool path + `KeyRegistry`, `backend.write_object_to_pool` (rem gRPC /
d2 CLI), the `_member_source_path` `source_metadata` fallback. **New (bounded):** a **width-only Alembic
migration** widening `BundleMember.member_path` + `AssetLocator.member_path` 1024→2048 (decision 9);
`run_rem_archive_build` map args + `RemArchiveBuilder` `map_mode`; `flush_bundle` gains optional
`map_path`/`source_root` **parameters** (a map-mode branch: build via `--map`, neutralize the scan gate)
— no new columns; **the build-report parser made map-mode aware** (`_normalized_rem_build_report` /
`_members_from_manifest` carry `ingest_item_id` — they drop it today — so recording can cross-check
`ingest_item_id`+`size`+`sha256` against the matched `submission_member`); the **pre-rem source
re-verification** (step 2, re-hash/re-stat vs `submission_member` before any external write); the
submission→open-bundle projection (source path via the `source_metadata` fallback, §3);
`archive_submission()` + `sutra archive submission flush`; the `submission.status` flip. **Unchanged:**
rem (P2.4 done), the **P2.3a model (no migration)**, the copy reconciler, the `--rules`/`--inputs`
accumulator path. The locator/blob-root recorders stay plain inserts — all-or-nothing (decision 6) means
they're never replayed against committed state.

## 5. Tests & acceptance
**Tests** (`tests/test_archive_submission.py`, memory/d2-stub backends):
- **archive a submission** — working-RAO + offsite-AEAD (+ d2 shelf) bundle copies recorded; per-member
  `AssetLocator`s with correct `member_path`(=archive_path)/`size`, and the right **per-target geometry**:
  **`first_chunk_lba`** for the RAO/AEAD copies, **`block_range`** for the d2 copy; `submission.status=
  archived`; the **derived bundle `submission-<submission_id>` exists and is `sealed`** (no
  `submission.bundle_id` FK to assert).
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
- **source drift fails pre-write** — mutate a source's bytes (same size) after submit →
  `archive_submission` fails in the **pre-rem re-verify** (step 2): **no** rem invocation, **no** tape
  write, **no** orphan object, `submission` stays `pending_archive`.
- **long paths archive** — a `submission_member.source_path` > 2048 chars projects via
  `source_metadata["source_path_bytes_hex"]`, and an `archive_path` in the 1025–2048 range fits the
  **widened** `member_path` columns — both archive cleanly (no truncation / Postgres width failure).
- **migration + schema parity** — the width-only Alembic revision widens `BundleMember.member_path` +
  `AssetLocator.member_path` to 2048; `create_all` and `alembic upgrade head` agree (extend
  `test_schema.py`); chains from the current head.
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
- **Only one schema change** — a width-only widen of `member_path` 1024→2048 on
  `BundleMember`/`AssetLocator` (decision 9); the P2.3a arrangement/submission tables and every other
  column are untouched. (A schema-parity test covers the widened columns.)
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
   `submission.bundle_id` FK, no P2.3a arrangement/submission table change.
3. **Scan-gate in map mode** — **RESOLVED (decision 3): neutralized** (skip the rules conformance scan;
   `Bundle.scan_summary` records a `map` marker). Skip-the-call vs. `expect="permissive"` is a trivial
   impl detail, not an open design question.
4. **d2 shelf copy for arrangements** — included by the artifactclass policy (`target_pools`, like RAS)
   vs. arrangement-specific policy. Lean: policy-driven, no special-casing. (Genuinely open.)
