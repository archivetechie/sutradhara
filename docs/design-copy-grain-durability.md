# Design — copy-grain unification + durability enforcement

**Status:** design, for panel review (2026-07-03). Fable-authored amendment to the
pool/multi-copy model, from `~/system/docs/report-fable-review-hard-threads-2026-07-03.md`
(thread 1). Business rule pinned by the owner (B4): **≥3 copies spanning ≥2
implementation families for archival classes; media-generation diversity is
explicitly NOT policy.** Companion docs: `design-ingest-v2-rao-archive.md`
(§B4–B6, still the placement model), `design-reconciler-spine.md` (P0.3).

## 1. Problem

Five verified defect classes, all archival-grade (cites in the system report):

1. **The Copy asset-XOR-bundle fork** (`ck_copy_asset_xor_bundle`,
   `catalog/models.py:1154`) has produced three divergent "healthy copy"
   predicates (replication asset-only; retention's asset+bundle UNION,
   `retention.py:418-454`; restore's AssetLocator walk) — and the production
   bundle path has **no self-heal and no replication_status at all**.
2. **Durability diversity is unenforced**: nothing at policy-apply or write time
   prevents all copies of a class landing on one backend/implementation;
   `_assert_distinct_media` runs only inside `replication_status`
   (`replication.py:366`), never on any write path.
3. **Pool lifecycle is dangerous**: no state machine, and
   `AssetLocator.pool_id`/`BlobRoot.pool_id` are `ondelete=CASCADE`
   (`models.py:1007,1063`) — a pool-row delete destroys restore coordinates.
4. **Source selection is split**: `restore_asset` walks policy preference;
   `self_heal` uses lowest-`Copy.id` (`replication.py:376-427`) with no
   fallback and no suspect-marking — it can heal from the AEAD offsite copy
   while a plain working copy exists, and gives up if source #1 fails.
5. **Durability honesty defects**: restore-discovered corruption never marks
   the copy suspect (`archive_restore.py:312,326`); cross-artifactclass bundle
   bleed in restore/retention (no `Bundle.artifactclass` join,
   `archive_restore.py:271`, `retention.py:432`); `cloud-blob` hardcodes
   `representation=rao-aead-v1` regardless of pool
   (`jobs/handlers/cloud_blob.py:129`); `restore_preference` never validated
   against real pools; `replicate.py:33,74` keys sealing off hardcoded class
   names.

## 2. Decisions

### D1 — Bundle is the universal storage-copy grain (staged)

Target model: **every durable placement is a bundle copy.** A single-asset write
becomes a **degenerate 1-member bundle** (`bundle_id = asset-<hash12>` or
similar deterministic id; member_path = the canonical single member). One
`Copy` grain ⇒ one `replication_status`, one self-heal, one reconciler domain,
one restore dispatch (always via `AssetLocator`).

Rationale for acting **now**: the `copy` job handler is still a
`NotImplementedError` stub — the real write machinery for reconciler-driven
copies has not been built. Choosing the grain before that handler exists is the
cheap moment; after it lands, every stage doubles in cost.

**Staging (each stage lands green on main; J/N/O/Q stay green throughout):**

- **M1 — one predicate.** Extract a single shared module
  `sutradhara/durability.py`: `qualifying_copies(session, target, pool)` and
  `placement_status(session, target)` where `target = AssetTarget(hash) |
  BundleTarget(bundle_id)`. Implementation lifts retention's asset+bundle UNION.
  `replication_status`, `retention._qualifying_copies_for_pool`, and the restore
  preflight all call it. Adds `bundle_replication_status(bundle_id)`
  (want = `target_pools(bundle.artifactclass)`, have = bundle copies by pool).
  No schema change.
- **M2 — bundle self-heal on the spine.** New reconciler domain
  `bundle_copy` (P0.3's named "highest-value next"): `discover` enumerates
  sealed bundles × active pools, `observe` = qualifying bundle copy present +
  healthy, `reconcile_target` enqueues a `bundle-repair` job. The handler
  restores the bundle's members from a healthy copy (staged, with
  `staging_transform` fidelity), re-runs the **same per-pool build path
  `flush_bundle` uses** for just the missing pool, verifies, records the copy.
  Scrub-marked MISSING/corrupt bundle copies finally have a consumer.
- **M3 — degenerate-bundle write path.** `replicate_asset` (and the future
  `copy` handler) write 1-member bundles: create bundle row + member +
  AssetLocator + bundle-scoped Copy per pool. The o/n-archive scenario shims
  keep observable behavior byte-identical (same pools, same representations,
  same locators modulo the bundle wrapper); scenario expectations updated only
  where they assert `Copy.logical_asset_hash` directly.
- **M4 — retire the asset grain.** Backfill migration: for each legacy
  asset-scoped Copy, synthesize its 1-member bundle + AssetLocator (pure
  catalog rewrite; no tape I/O — locators are unchanged). Then
  `select_restore_source`/`_healthy_copies` asset filters are deleted in favor
  of D4's selector, and the XOR constraint tightens to bundle-only
  (`logical_asset_hash` column retained, NULL, dropped in a later cleanup).

M1+M2 are independent of M3+M4 and ship first; M3/M4 are gated on M2 green plus
a clean-slate `make suite`. If the panel finds M4's backfill riskier than
modeled, M4 alone may defer — M1–M3 already end the fork for all *new* writes.

### D2 — Declared durability requirements, enforced twice

- `Backend.implementation_family` (string, required; registry-validated values:
  `remanence`, `d2tape`, `ssh_disk`, `s3`, `memory`, …). Family = independent
  implementation+format lineage (rem-rust/RAO vs d2-java/tar), NOT media type.
- Artifactclass policy TOML grows a `durability` table:
  `min_copies` (int) and `min_impl_families` (int). Defaults for archival
  (master) classes: `min_copies=3, min_impl_families=2` (the B4 rule). Proxy/
  derived classes may declare lower.
- **Enforcement point 1 — policy apply:** `apply_artifactclass_policy`
  validates that the declared placement pools *can* satisfy the durability
  table (enough pools, spanning enough families) — violation is a hard error.
- **Enforcement point 2 — write commit:** after `replicate_asset`/
  `flush_bundle` fan-out (and after M2 repairs), assert the **realized** copies
  satisfy: distinct media (`_assert_distinct_media` moves into this path — it
  currently never runs on writes) AND family count ≥ declared. Failure ⇒ the
  operation reports failure loudly (copies already written stay recorded; the
  gap is a named error, not a silent success).
- `Pool.media_generation` (nullable string, e.g. `LTO-7`, `LTO-9`):
  **descriptive only** — enables "what still lives on LTO-9" migration-campaign
  queries. Never consulted by enforcement (B4: generations are economics, not
  policy).

### D3 — Pool lifecycle

- `Pool.state ∈ {active, draining, retired}` (default `active`).
  `draining`: no new placements (`target_pools` excludes it for writes);
  restores still read it. `retired`: neither; rows retained forever.
- FK changes: `AssetLocator.pool_id` and `BlobRoot.pool_id` →
  `ondelete=RESTRICT`. Pool rows are never deleted in normal operation —
  retirement is a state, not a row delete. (Copy.pool_id stays SET NULL: a
  copy outliving catalog config is representable; locators — the restore map —
  are not allowed to be destroyed by config changes.)
- Media migration shape (documented, not built): stand up new pool → add
  membership → M2's reconciler backfills → flip old pool to `draining` →
  verify placement_status complete everywhere → `retired`.

### D4 — One source selector, purpose-parametrized

`select_source(session, target, *, purpose, exclude=()) -> ordered candidates`
with `purpose ∈ {user_restore, self_heal, verify, dr}`:

- `user_restore`: policy `restore_preference` order, then remaining active
  pools by `sort_order` (current `_restore_pool_order` semantics, now validated
  — see D5).
- `self_heal`: **cheapest-trusted** — prefer plain representations over AEAD
  (no key materialization), local/working over offsite, health=ok with recent
  verify first; deterministic total order.
- `verify`/`dr`: all candidates / offsite-preferring respectively (thin now;
  the enum is the extension point).

Consumers: `restore_asset`, `self_heal` (which additionally gains **fallback
iteration** — on source failure, mark that copy `suspect` and try the next
candidate; raise only when exhausted), M2's `bundle-repair`, hdcache's
`resolve_read_source` (unchanged behavior, now via the same seam).

**Corruption is a durability event:** any integrity failure observed on any
read path (restore, self-heal source, repair verify) sets `copy.health =
SUSPECT` in the same transaction and (post-M2) nudges the relevant reconciler
condition. `restore_asset`'s current append-to-errors-and-continue keeps the
fallthrough but stops being silent.

### D5 — Defect fixes riding along

1. Cross-class bleed: restore and retention locator queries join
   `Bundle.artifactclass == requested class` (locators without bundles — pre-M4
   legacy — filter via the asset's class memberships).
2. `cloud-blob` records `representation = pool.representation` and refuses a
   pool whose representation it cannot produce.
3. `restore_preference` validated at policy apply: unknown pool ⇒ error;
   inactive/draining pool ⇒ warning (kept — restores may still read draining).
4. `replicate.py` drops the `{"o-archive","n-archive"}` name set: sealer/key
   provisioning derives from "any target pool has an AEAD representation"
   (`target_pools` already computes `key_epoch` per pool).
5. Duplicate-copy write race: `repair`/`bundle-repair` re-check
   `qualifying_copies` inside the write transaction before recording; and
   `placement_status` counts distinct pools, tolerating (flagging) duplicates.
   (A hard partial-unique index on live (target, pool) is rejected: scrub
   imports and migration transitions legitimately hold transient duplicates.)

## 3. Schema delta summary

- `backend.implementation_family` (required; backfilled by kind in migration).
- `pool.state` (default active), `pool.media_generation` (nullable).
- FK: `asset_locator.pool_id`, `blob_root.pool_id` → RESTRICT.
- Policy TOML: `[durability] min_copies / min_impl_families` per class.
- M3/M4 only: degenerate bundles; no new columns (bundle/member/locator reused);
  M4 backfill migration; XOR tightening deferred to cleanup.

## 4. Verification members (scenario-or-cover rule)

- **New hermetic scenario BSH (bundle self-heal):** archive a bundle to 3
  pools → damage/delete one pool's copy → scrub marks MISSING → `sutra
  reconcile bundle_copy` + worker → repaired copy on distinct media →
  `bundle_replication_status` complete; restore from the repaired pool
  byte-identical.
- **New hermetic scenario DIV (durability enforcement):** policy declaring 3
  pools on one family ⇒ apply rejected; conforming policy ⇒ write-commit
  assertion green; simulated single-family realized fan-out ⇒ loud failure.
- **POOL-LC legs (can live in DIV):** draining pool excluded from new writes,
  still restorable; pool-row delete attempt with live locators ⇒ RESTRICT error.
- Extend Q: self-heal fallback (source #1 corrupt-but-OK ⇒ marked suspect,
  heal completes from source #2) and preference-aware source choice (heals from
  plain, not AEAD, in a 3-pool topology with non-insertion preference order).
- M3/M4 gate: full clean-slate `make suite` (J/N/O/Q byte-compat is the bar).

## 5. Non-goals

Significance-driven copy counts (deferred; needs its own placement mechanism —
not a `target_pools` swap); same-kind multi-backend routing scenario;
media-generation *policy* (B4: descriptive only); lease/queue changes
(prompt-jobs-safety-rails owns those); hdcache scope (frozen design owns it);
any change to rem/d2tape on-tape formats.

## 6. Open questions for the panel

1. M4 backfill: synthesize bundles for legacy asset copies in one migration, or
   lazily on first touch? (Author leans one migration — lazy leaves the fork
   alive indefinitely.)
2. Degenerate-bundle id scheme + whether `bundle.artifactclass` for o/n legacy
   classes needs a compat alias.
3. Does the write-commit durability assertion belong per-operation (loud
   failure return) or as a catalog invariant check the reconciler also sweeps
   (author: both — the sweep catches drift the write path never saw)?
4. `implementation_family` on Backend vs Pool (author: Backend — family is a
   property of the adapter/toolchain, and pools inherit it).
5. Is `draining` worth having from day one, or does `active/retired` suffice
   until the first real migration campaign?
