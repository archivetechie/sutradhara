# Codex prompt — copy-grain M2: bundle self-heal on the reconciler spine (+ D4) — sutradhara

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`
> (single repo — no Shared-contract section).** Read `CLAUDE.md` + `AGENTS.md` first.
> **Authoritative, FROZEN design: `docs/design-copy-grain-durability.md` — §2 M2, §2 D4,
> §6 stage CG-M2. The design is the authority; where this prompt and the design disagree,
> the design wins — stop and flag, do not adapt.**
> **Depends on CG-M1 landing first** (`prompt-copygrain-m1-predicate.md`): this prompt calls
> `sutradhara.durability` (`durable_placements`, `direct_copies`, `bundle_replication_status`).
> Land M1 on `main` before starting M2.
>
> **What this is.** Bundle copies get self-heal — the archival gap the panel named (§0). A
> build primitive is factored out of `flush_bundle`, a `bundle-repair` job handler rebuilds a
> missing pool's copy from stored member bytes extracted from a healthy copy, a new
> `bundle_copy` reconciler domain drives it level-triggered, scrub stops mis-adopting unknown
> objects, and source selection is formalized with a trust-first order and an explicit SUSPECT
> lifecycle. **NO schema change** (that is M3). Every pre-existing test stays green.

## What already exists — BUILD ON IT, do not rebuild
Verify each `file:line` yourself before touching it.

- **`src/sutradhara/archive_fanout.py`** — `flush_bundle` (`:465-605`). The per-target loop
  (`:534-601`) is the extraction target: `_build_for_target` (`:663-688`) →
  `backend.write_object_to_pool` (`:551`) → `add_bundle_copy` (`:557-567`) →
  `_record_build_outputs` (`:773-813`) → `_verify_members_from_copy` (`:816-846`) →
  `copy.last_verified_at` (`:585`). **`_record_build_outputs` records `AssetLocator` +
  `BlobRoot` AND `ExclusionRecord`** (`:802-813` via `record_exclusion`) — the exclusion
  recording is the split point (design M2 / verify r1). `flush_bundle` refuses non-open bundles
  (`:498`) and owns lifecycle (`close_bundle` `:603`, `enqueue_post_flush_hdcache_fills` `:604`).
  Member sources come from `_member_input` (`:898-910`) which reads `member.source_path`
  (staging) — **repair must not use this**.
- **`src/sutradhara/archive_bundle.py`** — `record_asset_locator` (`:232-264`),
  `record_blob_root` (`:267-294`), `record_exclusion` (`:352-383`), `close_bundle` (`:170-175`).
- **`src/sutradhara/archive_restore.py`** — `read_member_to_path` (`:148-218`) returns the
  **stored** member bytes (pre reverse-transform) for one `AssetLocator` from one `Copy`;
  handles d2-tar / offset / RAO-plain / RAO-AEAD. `RemArchiveExtractor` (`:118-145`) is the RAO
  path. This is the extraction primitive repair uses.
- **`src/sutradhara/replication.py`** — `self_heal` (`:266-335`) is asset-grain today;
  `select_restore_source` (`:376-395`) is the `chooser` seam (default = lowest `Copy.id`);
  `_copy_media_id` (`:455-466`). `restore_copy` (`restore.py:53-102`) verifies stored + plaintext
  digests. `restore_asset` (`archive_restore.py:247-340`) collects `integrity_errors` but
  **never marks copies suspect** (`:312-337`).
- **Reconciler spine** — `src/sutradhara/jobs/reconcilers/`:
  - `registry.py` — `Reconciler(enumerate_targets, observe, reconcile_target)` +
    `register_reconciler(domain)` (`:41-50`), `TargetObservation(target_key, desired,
    observed_state)`.
  - `spine.py` — `discover` (`:24-44`) / `process` (`:47-75`) / `reconcile` (`:78-90`);
    `gate_open` NOT-EXISTS live-job gate (`:117-145`).
  - `conditions.py` — `record_observation` (`:38-89`, the only row-creator; `OBSERVED_PRESENT`
    /`OBSERVED_MISSING`), `record_condition` (`:92-155`).
  - `copy.py` (`:1-215`) and `derivation.py` (`:1-205`) are the two **domain templates** to copy
    from — a `make_target_key`/`parse_target_key`, a batched `enumerate_targets`, an `observe`,
    a `reconcile_target` that `submit(...)`s with `recon_domain`/`recon_target_key`/`dedupe_key`.
  - `hdcache.py` (`:1-68`) shows a domain whose `target_key` is an opaque id (sha hex).
- **Jobs** — `jobs/engine.py::submit` (`:46-91`: `recon_domain`/`recon_target_key`/`dedupe_key`
  args); `jobs/registry.py` (`JobContext`/`JobResult`/`ConditionProjection`/`register_handler`
  `:24-83`); `jobs/handlers/__init__.py` side-effect imports (`:14-21`); `jobs/handlers/copy.py`
  is the `NotImplementedError` stub to model a real handler against (do **not** implement it).
- **`src/sutradhara/scrub.py`** — `_ingest_record` (`:103-159`). On an enumerated locator not
  already cataloged, when `session.get(LogicalAsset, record.logical_id) is None` it **inserts a
  new `LogicalAsset` + asset-grain `Copy`** (`:133-156`). `ScrubReport` (`:45-59`) has no
  `unknown_object` counter yet.

Transaction discipline: **flush, never commit/rollback**; callers own the transaction. Reuse
`add_bundle_copy`, `record_attempt`, `record_condition`, `record_observation` — do not invent a
parallel writer.

---

## A. Factor `build_bundle_copy_for_pool` out of `archive_fanout` (design M2, verify r1)
Extract a reusable primitive that builds, writes, verifies, and records **ONE pool's** bundle
copy — with **no bundle lifecycle mutation, no conformance/scan gate, no customer-manifest
emission, and NO `ExclusionRecord` writes**:

```
build_bundle_copy_for_pool(
    session, *, bundle, target: PoolTarget, member_sources: Sequence[MemberInput],
    builder: ArchiveBuilder, backend: WritableStorageBackend, key_epoch: str | None,
    work_dir: Path,
) -> Copy
```
It does exactly the loop body at `archive_fanout.py:534-585` **minus** manifest emission
(`:586-601`) and **minus** exclusion recording:
- `_build_for_target` (member_sources, not `bundle.members`) → `write_object_to_pool`
  → `add_bundle_copy` → record `AssetLocator` (per member) + `BlobRoot` (per blob root) →
  `_verify_members_from_copy` → set `copy.last_verified_at`.
- **Split `_record_build_outputs`**: move `AssetLocator` + `BlobRoot` recording into the
  primitive; leave `ExclusionRecord` recording in `flush_bundle` only (a repair must never
  replay exclusion rows — verify r1). Refactor `flush_bundle` to call the primitive and then
  record exclusions itself, so `flush_bundle` behavior is **byte-for-byte unchanged** (its whole
  suite stays green).

## B. `bundle-repair` job handler (`jobs/handlers/bundle_repair.py`, register `"bundle-repair"`)
Params: `bundle_id` (str). Reconciler-backed (`recon_domain="bundle_copy"`). Steps:
1. Load the sealed `Bundle` + members. Compute **missing pools** = the class's write-eligible
   pools (§C `discover` set) minus pools that already hold a healthy bundle copy
   (via `durability.bundle_replication_status`). If none missing and floor satisfied → success,
   no-op.
2. **Pick a source** healthy bundle copy via `select_source(session, BundleTarget(bundle_id),
   purpose="self_heal")` (§D), with fallback iteration on failure.
3. **Extract stored member bytes** from that healthy copy into scratch: for each member's
   `AssetLocator` on the source copy, `read_member_to_path(... dest=scratch/<member.member_path>)`
   (design: `scratch/<member.member_path>` so the builder's `_rem_input_paths` root derivation
   works unchanged). **NEVER `member.source_path`** (retention purges staging at ~30 days —
   `sweep_staging`) and **NEVER re-stage from logical bytes** (AppleDouble merge is irreversible;
   zstd output is compressor-version-dependent). All pools store the same staged member bytes, so
   extraction from any healthy copy reproduces them exactly.
4. Build `member_sources: list[MemberInput]` pointing `source_path` at the extracted
   `scratch/<member_path>` files (carry each member's `logical_asset_hash`, `member_path`,
   stored `size_bytes`, stored `file_sha256`). For **each** missing pool call
   `build_bundle_copy_for_pool`.
5. Return `JobResult(ok=True, ...)`. On a source that fails digest verification, mark it SUSPECT
   per §D and iterate to the next candidate; raise only on candidate exhaustion.

Crash safety: everything flushes in the caller's transaction; a crash mid-repair leaves physical
orphan objects (scrub-visible, §E) but no partial catalog rows and no duplicate — re-running the
handler converges (the missing-pool set shrinks; already-written pools are skipped via
`add_bundle_copy` idempotency + the re-checked `bundle_replication_status`).

## C. New reconciler domain `bundle_copy` (`jobs/reconcilers/bundle_copy.py`, design M2 / P0.3)
Model on `copy.py`/`derivation.py`. **`target_key` = the bundle id** (one condition per sealed
bundle, like `hdcache.py` keys by sha hex).
- `discover` / `enumerate_targets(session, cursor, batch)`: sealed bundles × the class's
  **write-eligible** pools. (M2 has no `accepts_writes` column yet — treat every active
  `ArtifactClassPool` pool as write-eligible now; leave a `# M3/D3 accepts_writes` marker so M3
  narrows it in one line.)
- `observe`: `OBSERVED_PRESENT` iff **placement complete AND durability floor satisfied** —
  realized copies meet the class's `min_copies` and `min_impl_families`. M2 has no persisted
  floor yet (that is M3/D2): use the **global archival default `min_copies=3, min_impl_families=2`
  from the design (B4)** as a module constant, and compute realized families with the same
  per-family identity logic M3/EP2 will formalize (tape/d2 by locator media id via
  `replication._copy_media_id`; other families by backend row). Leave a `# M3/D2 floor` marker
  where the persisted floor plugs in. This makes drain-below-floor and family-collapse visible as
  open conditions **without a third sweep mechanism**.
- **Batched, not N+1** (verify r1): per cursor batch, prefetch class floors + pool→family maps
  once and aggregate realized copies with a **single GROUP-BY-bundle query**; `observe` for the
  batch consumes that aggregate. No per-bundle copy scan at 100k bundles. (Per-target `observe`
  in `process` may recompute one bundle — that path is already bounded by `limit`.)
- `reconcile_target(session, bundle_id)`: `submit("bundle-repair", {"bundle_id": bundle_id},
  recon_domain="bundle_copy", recon_target_key=bundle_id, dedupe_key=f"bundle_copy:{bundle_id}")`.
- Register via `register_reconciler("bundle_copy")` and import the module wherever domains are
  imported so registration runs (mirror how `copy`/`derivation`/`hdcache` register).

## D. Source selection: `select_source` + SUSPECT lifecycle (design D4)
Formalize the existing `chooser` seam into
`select_source(session, target: AssetTarget | BundleTarget, *, purpose) -> Copy | None` with
**exactly two** purposes (drop `verify`/`dr` until a consumer exists — adding an enum member
later is a one-line diff):
- `user_restore` → the policy `restore_preference` walk (current `_restore_pool_order`
  semantics in `archive_restore`).
- `self_heal` → **trust-first, cost-tiebreak** (v1 had this backwards): order candidates by
  `health==ok` first, then `last_verified_at DESC`, THEN plain-over-AEAD, then
  local-over-offsite. **Deterministic total order** (break final ties by `Copy.id`).
- Fallback: healing iterates candidates; a failed source is handled per SUSPECT below; raise only
  on exhaustion.

**SUSPECT lifecycle (new, explicit — design D4):**
- Only a **proven digest mismatch** latches `CopyHealth.SUSPECT`. Transport / timeout /
  short-read errors are **transient**: no latch, condition `backoff`.
- **Scrub gains `SUSPECT → OK`**: in `scrub.py`, a copy whose enumerated `integrity_hash`
  re-proves its recorded digest clears `SUSPECT → OK` (today `_update_existing_copy` `:161-191`
  only escalates to SUSPECT — make it a two-way transition on a proven-good re-verify). Rationale:
  a one-way latch under a flapping drive mass-condemns good copies and stampedes repairs.
- **`restore_asset` marks proven-mismatch copies suspect in-transaction** (design D4): where
  `restore_asset` records an `integrity_error` for a digest mismatch (`archive_restore.py:312-317`),
  set that copy's `health = SUSPECT` in the same transaction (a proven mismatch discovered by ANY
  read is a durability event). Transport/`ArchiveRestoreError` failures (`:326-331`) do **not**
  latch. Keep the existing fallthrough to the next candidate.
- **hdcache contract untouched** (frozen): `resolve_read_source` keeps wrapping `restore_asset`;
  `select_restore_source`/`select_source` stays cache-blind; the deletion gate keeps its verified
  semantics via M1's `require_verified=True`. No re-pointing.

## E. Scrub adoption fix — NO bundle auto-adoption (design M2, verify r2)
Final rule (design): an enumerated object whose `logical_id` **matches an existing
`LogicalAsset`** → `add_copy` (asset grain, existing behavior at `scrub.py:144-158`); **anything
else** → an **`unknown_object` quarantine entry in the scrub report** (counted; the persistent
count feeds the M3/D6 alarm), **never an invented row**. Crash-orphan bundle objects are
therefore NOT adopted: the bundle's `bundle_copy` condition stays open, the next `bundle-repair`
writes a fresh copy, and the orphan remains a scrub-visible unknown object (tape-space waste
accepted — same stance as D6 duplicates; the alarm keeps it rare and visible).
- **RESOLVED at the fold (2026-07-03, design §M2 amended — this supersedes the earlier
  escalation draft): the spec §7 adopt-unknown behavior STAYS.** The rule is three-way:
  (a) `logical_id` matches a `LogicalAsset` → `add_copy` (unchanged);
  (b) the record is **recognizably a bundle container** — rem archive `body_format` /
  the flush path's container naming — → increment `ScrubReport.unknown_objects` (+ a
  bounded locator list) as a quarantine entry; NEVER insert a `LogicalAsset` for a
  container hash;
  (c) everything else → the existing adopt-unknown bootstrap (`scrub.py:133-142`),
  byte-for-byte unchanged — it is the spec §7 tier-1 rebuild-from-media property and
  the two tests encoding it (`tests/test_scrub.py:101-119`, `:211-227`) MUST stay green
  untouched.
  Known documented residual: a bundle container the enumeration metadata cannot
  distinguish is still adopted-as-asset — pre-existing hazard, not a regression; do not
  attempt to solve it here.

---

## Non-goals (do NOT do these)
- **No** schema/migration change (no `implementation_family`, no `accepts_writes`/`retired`, no
  `[durability]` columns, no FK RESTRICT — all M3). Use the design's global floor default as a
  constant with a `# M3/D2` marker.
- **No** implementing the `copy` job handler / `replicate_asset` grain conversion (§D1 — future
  copy-handler prompt). **No** EP1/EP2 write-time enforcement, drain guard, or duplicate alarm
  (M3). **No** `flush_bundle` behavior change beyond the exclusion-recording split.
- **No** conformance gate, customer manifest, or lifecycle mutation inside the repair path.
- **No** `verify`/`dr` selector purposes. **No** hdcache-contract re-pointing.
- Do **not** touch `docs/INDEX.md`.

## Tests — add these (extend the nearest existing suite; keep every current test green)
- `flush_bundle` regression: unchanged copies/locators/blob-roots/exclusions after the factor-out
  (the whole `test_archive_fanout` suite green).
- `build_bundle_copy_for_pool`: builds one pool's copy from `member_sources`, records
  `Copy`+`AssetLocator`+`BlobRoot`, writes **no** `ExclusionRecord`, verifies members, sets
  `last_verified_at`.
- `bundle-repair` handler:
  - happy: damage one pool's bundle copy → repair rebuilds it from a healthy copy; repaired copy
    has `BlobRoot` + `AssetLocator`; restore is byte-identical.
  - **staging purged**: after `sweep_staging`, repair still succeeds (extracts from a healthy
    copy, not `member.source_path`).
  - **corrupt source fallback**: source #1 proven-mismatch → SUSPECT → repair heals from #2.
  - **crash mid-repair**: re-run converges, no duplicate copy, no mis-adoption.
- `bundle_copy` reconciler: `observe` = PRESENT only when placement complete AND floor satisfied;
  drain-below-floor / family-collapse show as open; `reconcile_target` enqueues one
  `bundle-repair` (deduped); enumerate is a single GROUP-BY (assert no per-bundle N+1 via query
  count or a large-batch shape test).
- `select_source(self_heal)`: trust-first order (stale plain vs fresh AEAD → picks fresh AEAD);
  deterministic total order.
- SUSPECT lifecycle: proven mismatch latches SUSPECT; transient read error does not; a later
  verified-good scrub clears `SUSPECT → OK`; `restore_asset` marks a proven-mismatch copy suspect
  in-transaction and still falls through to a good candidate.
- scrub: unknown `logical_id` → `unknown_objects` incremented, **no** `LogicalAsset`/`Copy`
  invented (and the two OLD tests reconciled per the escalation note above).

## Verification
- `cd ~/sutradhara/repo && uv run pytest -q` — **green**, including every pre-existing test
  (except the two deliberately-reconciled scrub tests — call that out explicitly in your summary).
- **Covers**: the harness verification member is scenario **BSH** + the **Q** extension in
  `~/system/docs/prompt-copygrain-harness-scenarios.md` (SKIP-gated until this lands).
- **Editable-dep trap** (`CLAUDE.md`/memory): `~/system`'s `make scenario-*` imports sutradhara
  from the **working-tree branch** via an editable install. **Land this complete on `main`** or
  the harness silently regresses. Commit at green milestones; direct-to-main, no PRs; never ask
  the operator to do hygiene.

## Acceptance criteria
1. `build_bundle_copy_for_pool` exists (records `Copy`+`AssetLocator`+`BlobRoot`, never
   `ExclusionRecord`, no lifecycle/conformance/manifest); `flush_bundle` uses it with unchanged
   behavior.
2. `bundle-repair` handler heals a missing pool by extracting stored member bytes from a healthy
   copy into `scratch/<member_path>` and calling the primitive; converges on re-run; survives
   staging purge and corrupt-source fallback.
3. `bundle_copy` reconciler domain (target_key = bundle id) observes placement-complete-AND-floor,
   batched (no N+1), enqueues `bundle-repair`.
4. `select_source` has exactly `user_restore`/`self_heal` purposes with trust-first self-heal
   order; SUSPECT latches only on proven mismatch, scrub clears `SUSPECT → OK`, `restore_asset`
   marks proven-mismatch copies suspect in-transaction.
5. scrub quarantines unknown objects (counted) instead of inventing rows; the design-vs-code
   contradiction is surfaced in the summary, not silently adapted.
6. `uv run pytest -q` green; the diff gate has a clear implementation summary to review.
