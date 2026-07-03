# Design — copy-grain unification + durability enforcement (v2, panel-folded)

**Status:** **FROZEN 2026-07-03** — panel (4 lenses, ~36 findings, 8 blockers folded) →
verify r1 (7 findings folded) → verify r2 (6/7 confirmed resolved; the 7th resolved by
REMOVING bundle auto-adoption — a scope-reducing fold at the 2-round cap, no new
machinery). Next: cut prompts CG-M1/M2/M3 (§6).
v1 was panel-reviewed same day per `~/system/docs/process-panel-review.md`:
4 blind lenses (migration/compat, failure/durability, simplicity/cost — Opus;
contract/code-reality — codex xhigh), ~36 findings, 8 blockers. All folded here;
the fold materially SHRANK the design (M4 dropped, M3 reduced to a pin).
Origin: `~/system/docs/report-fable-review-hard-threads-2026-07-03.md` thread 1.
Business rule pinned by the owner (B4): **≥3 copies spanning ≥2 implementation
families for archival classes; media-generation diversity is explicitly NOT
policy.**

## 0. What the panel changed (read this first)

- **Production masters are already bundles** (`flush_bundle` is the s-masters/
  s-proxy write path); `replicate_asset` is harness-facing (J/N/O/Q) and the
  `copy` job handler is still a stub. So grain-*unification* work was aimed at
  the non-production path. The archival gap is bundle status + self-heal — that
  is M1+M2, and that is where the budget goes.
- **M4 (legacy backfill + XOR tightening) is DROPPED**, permanently: it risked
  the persistent pilot catalog, had a shared-asset `bundle.artifactclass`
  ambiguity, would have crashed scrub's asset-grain insert path under the
  tightened CHECK, and bought zero durability. The Copy XOR stays loose
  forever; legacy asset-grain rows convert opportunistically only if/when a
  real media migration rewrites their bytes anyway.
- **M3 shrinks to a grain pin** (§2 D1): the byte-identity claim in v1 was
  false (single-object seal vs archive/tar build are different bytes; Q.8 pins
  byte-identity), and converting a non-production writer now bought nothing.

## 1. Problem

Unchanged from v1 (verified, cites in the system report): (a) bundle copies
have no `replication_status`/self-heal — three divergent healthy-copy
predicates exist; (b) durability diversity is unenforced anywhere
(`_assert_distinct_media` runs only inside `replication_status`); (c) pool
rows can be deleted, CASCADE-destroying `AssetLocator`/`BlobRoot` restore
coordinates; (d) source selection is split (policy walk vs lowest-`Copy.id`),
self-heal has no fallback and never marks bad sources; (e) five durability-
honesty defects (suspect-marking, cross-class bleed, cloud-blob
representation, unvalidated `restore_preference`, hardcoded class names).

## 2. Decisions

### D1 — Copy grain: bundle for all FUTURE durable writers (a pin, not a migration)

- **Pin:** every new durable write path — the real `copy` job handler foremost
  — records **bundle grain**: a degenerate 1-member bundle row + member +
  `AssetLocator` + bundle-scoped `Copy`. The degenerate id is deterministic
  (`asset-<hash16>`), collision-checked against real bundle ids, and its
  `bundle.artifactclass` is the asset's class *for that placement*.
- **Bytes:** a 1-member degenerate write KEEPS the existing single-object
  seal/write path (`RaoCliSealer` / `write_object_to_pool`) — it does NOT use
  the flush archive/tar container. Only the catalog rows change grain. (Panel:
  the two build paths are not byte-compatible; Q asserts byte-identity across
  write→repair, so the seal path is load-bearing.)
- `replicate_asset` is NOT converted now. If it is still a live writer when
  the copy handler lands, it converts then, in one prompt, against a real
  consumer — with the harness shims (J's `lookup_by_hash`, status shape, Q
  self-heal, scrub expectations) updated in the same change, which the panel
  enumerated as the true blast radius.
- The XOR constraint is **never tightened**. Both grains stay legal at the
  schema level; M1's predicate makes the difference invisible to durability.

### M1 — One durability predicate module (no schema change)

New `sutradhara/durability.py`:

- Two views with an explicit **grain axis** (verify r1: `require_verified`
  alone does not encode the current grain split — retention counts asset
  Copies PLUS bundle copies via `AssetLocator`, while
  `select_restore_source`/`restore_copy` operate on asset-grain Copy rows only
  and `restore_copy` rejects bundle copies):
  - `durable_placements(session, target, *, require_verified: bool,
    artifactclass: str | None)` — the durability-accounting view: for an
    `AssetTarget`, asset-scoped Copies UNION bundle copies reachable via
    `AssetLocator`; for a `BundleTarget`, bundle copies. Used by
    `replication_status` (require_verified=False — a fresh unscubbed copy
    still counts, no J/N/O/Q drift), retention (True), floor checks.
  - `direct_copies(session, asset_hash)` — asset-grain Copy rows only: what
    today's `restore_copy`/self-heal can physically operate on. Self-heal
    keeps this view (bundle healing is M2's `bundle-repair`, not `self_heal`).
  The `require_verified` axis is semantics-preserving per caller, per
  `design-deletion-gate.md`.
- `placement_status(session, target)` — want (via `target_pools`) vs have
  (via the predicate), returning `PoolTarget`-shaped entries (the harness seam
  getattr-maps `.pool_id/.backend_name/...`; shape is a compat invariant).
  Flags duplicate copies per (target, pool) — see D6.
- `bundle_replication_status(session, bundle_id)` — the new capability.
- All current readers route through the module **with their current
  semantics**: `replication_status`, `repair`, `_healthy_copies`,
  `_healthy_copies_by_pool`, `select_restore_source`, retention's
  `_qualifying_copies_for_pool`, restore preflight. Six sites enumerated by
  the panel; routing is refactor-only, zero behavior change, each covered by
  existing tests.

### M2 — Bundle self-heal on the reconciler spine

- **Factored build primitive** (the panel's key correction): extract from
  `archive_fanout` a `build_bundle_copy_for_pool(session, bundle, pool_target,
  member_sources) -> Copy` that builds, verifies, and records ONE pool's
  bundle copy — including the per-pool `AssetLocator` + `BlobRoot` rows flush
  creates — with **no bundle lifecycle mutation, no conformance gate, no
  customer-manifest emission, and NO `ExclusionRecord` writes** (verify r1:
  the natural extraction point `_record_build_outputs` also records
  exclusions — split that; exclusion recording stays in `flush_bundle` only,
  or every repair would replay exclusion rows). `flush_bundle` refuses
  non-open bundles and owns lifecycle; repair must not go through it.
- **Member sources = stored member bytes extracted from a healthy copy** into
  scratch. NEVER `member.source_path` (retention purges staging at ~30 days —
  v1's wording made self-heal silently work for only the first month) and
  NEVER re-staging from logical bytes (AppleDouble merge is irreversible;
  zstd output is compressor-version-dependent). All pools store the same
  staged member bytes in different containers/sealing, so extraction from any
  healthy copy reproduces them exactly. Scratch layout (verify r1): each
  member extracts to `scratch/<member.member_path>` so the builder's
  `MemberInput` root derivation works unchanged.
- New reconciler domain `bundle_copy` (P0.3's named next domain):
  - `discover`: sealed bundles × the class's write-eligible pools.
  - `observe`: **placement complete AND durability floor satisfied** —
    realized copies meet the class's `min_copies` and `min_impl_families`
    (D2). This makes drain-below-floor and family-collapse — states with no
    missing pool copy — visible as open conditions, without inventing a third
    sweep mechanism.
  - **Batched, not N+1** (verify r1): per cursor batch, prefetch class floors
    + pool→family maps once and aggregate realized copies with a single
    GROUP-BY-bundle query; observe consumes the batch aggregate. No per-bundle
    copy scan at 100k bundles.
  - `reconcile_target`: enqueue a `bundle-repair` job (source via D4
    `self_heal` purpose, with fallback + suspect-marking).
- **Scrub adoption fix (verify r2 correction — NO bundle auto-adoption):**
  `CopyRecord.logical_id` is a stored-content sha256, not an archive id, so
  matching enumerated objects to `Bundle.archive_id` is type-unsound and is
  dropped. Final rule: an enumerated object whose `logical_id` matches a
  `LogicalAsset` → `add_copy` (asset grain, existing behavior); anything else
  → an **`unknown_object` quarantine entry in the scrub report** (counted;
  persistent count feeds the D6 alarm), never an invented row. Crash-orphan
  bundle objects are therefore NOT adopted: the bundle's condition stays open,
  the next `bundle-repair` writes and records a fresh copy, and the orphan
  remains a scrub-visible unknown object (tape-space waste accepted — same
  stance as D6 duplicates; the alarm keeps it rare and visible).

### D2 — Declared durability floor, enforced where it can be

- `Backend.implementation_family` (required string; registry maps every
  `BackendKind` value — all nine — to a family; migration backfills by kind).
- **Policy:** one global archival default — `min_copies=3,
  min_impl_families=2` (B4) — inherited by every artifactclass unless the
  class explicitly declares `[durability]` overrides (e.g. proxies:
  `min_copies=2, min_impl_families=1`). Safe-by-default: a new class that
  declares nothing gets the archival floor. Parser: `[durability]` added to
  the strict allow-list; persisted on `ArtifactClassPolicyRecord` (new
  columns — the current parser rejects unknown keys, so this is a named
  schema+parser change, not a drive-by).
- **EP1 — config time:** policy apply validates the declared write-eligible
  pools can satisfy the floor. The same validation re-runs on any pool
  write-fence change (D3's drain guard): flipping `accepts_writes=False` is
  REFUSED (or `--force`d with a loud alarm) if it would drop any active class
  below its floor without a complete replacement pool.
- **EP2 — write time, crash-safe via the condition machinery:** the fan-out
  transaction itself writes/updates the target's `bundle_copy` condition row
  (open, reason `durability-unverified`) **in the same transaction as the
  copies** — a durable outbox (verify r1: a check scheduled only after commit
  is lost if the process dies between commit and check). The immediate
  post-commit check is the fast path that resolves the condition right away;
  if it never runs, M2's level-triggered observe performs the identical check
  on the next sweep. The check: distinct media via a **per-family identity
  extractor** — tape → locator `tape_uuid`, d2 → `volume_uuid`/barcode;
  disk/cloud families (ssh_disk, s3, …) identify by **backend row** (the
  backend IS the host/filesystem/bucket; locators carry no media fields today
  and need none — two copies on one ssh_disk backend = same media);
  memory → exempt. v1's plan to reuse `_assert_distinct_media` verbatim would
  have made every non-tape pool (including the LAN backup) unwritable. Plus
  realized family count vs the floor. Deficiency classifies through the
  condition vocabulary, not a bare raise: transient backend failure →
  `backoff` (bounded retry); structural floor violation → `blocked` +
  operator alarm, **no hot-retry** (a structurally failing target must not
  manufacture duplicate copies each attempt).

### D3 — Pool lifecycle (slim)

- FK: `asset_locator.pool_id`, `blob_root.pool_id` → `ondelete=RESTRICT`.
  SQLite requires batch table-recreate; the migration must explicitly
  re-declare ALL constraints/indexes (house pattern `2f4a8bb0c2d7`) or the
  copy XOR/uniques silently vanish. A post-migration schema-assert test
  verifies the constraints survived.
- `Pool.accepts_writes` bool (write fence; `target_pools` excludes fenced
  pools for writes; restores still read them) — guarded per D2 EP1.
  `Pool.retired` bool settable only when the pool holds no live locators.
  The 3-state enum is deferred to the first real migration campaign.
- `Pool.media_generation` (nullable string; descriptive only, for
  migration-campaign queries — never enforcement, per B4).

### D4 — Source selection: two purposes, trust-first healing

- Formalize the existing `chooser` seam into `select_source(session, target,
  *, purpose)` with exactly **two** purposes: `user_restore` (policy
  `restore_preference` walk — current `_restore_pool_order` semantics) and
  `self_heal`. `verify`/`dr` purposes are dropped until a consumer exists
  (adding an enum member later is a one-line diff).
- `self_heal` ordering is **trust-first, cost-tiebreak** (v1 had this
  backwards): `health=ok` first, then `last_verified_at` DESC, THEN plain-
  over-AEAD and local-over-offsite as tiebreakers among equally-trusted
  candidates. Deterministic total order.
- **Fallback + suspect discipline:** healing iterates candidates; a failed
  source is handled per the SUSPECT rules below; raise only on exhaustion.
- **SUSPECT lifecycle (new, explicit):** only a **proven digest mismatch**
  latches `health=SUSPECT`. Transport/timeout/short-read errors are
  transient: no latch, condition backoff. Scrub's verify path gains
  SUSPECT→OK: a copy that re-proves its digest clears to `ok` (today SUSPECT
  is a one-way latch, which under a flapping drive would mass-condemn good
  copies and stampede repairs). `restore_asset` keeps its fallthrough but a
  proven-mismatch copy is marked in the same transaction — corruption
  discovered by ANY read is a durability event.
- **hdcache contract untouched** (frozen design): `resolve_read_source`
  continues to wrap `restore_asset`; `select_restore_source`/`select_source`
  stays cache-blind. The deletion gate keeps its verified semantics via M1's
  `require_verified=True` — no re-pointing.

### D5 — Defect fixes (each independently landable; can precede M1)

1. Cross-class bleed: restore + retention locator queries join
   `Bundle.artifactclass` for rows with `bundle_id`; rows with NULL
   `bundle_id` (legacy/SET-NULL) fall back to the asset's class memberships.
2. `cloud-blob` records `representation = pool.representation`, refusing
   pools it cannot produce.
3. `restore_preference` validated at policy apply (unknown pool → error;
   write-fenced pool → warning, kept readable).
4. Drop `replicate.py`'s `{"o-archive","n-archive"}` name set — sealer/epoch
   provisioning derives from "any target pool has an AEAD representation".
   Ships together with whatever next touches that write path (it gates epoch
   minting; not a drive-by).
5. Duplicates (see D6).

### D6 — Duplicate-copy stance (explicit)

No hard uniqueness on (target, pool) — scrub imports and repair transitions
legitimately overlap. Instead: `placement_status` counts distinct pools (never
raw rows) and FLAGS duplicates; a persistent duplicate count >1 for one
(target, pool) raises an operator alarm (it is a stuck-retry signal); EP2's
transient/structural classification prevents retry-manufactured duplicates;
repair re-checks `qualifying_copies` inside its write transaction. Tape
duplicates are documented as permanent-and-unreclaimable (no GC job — the
alarm exists so they stay rare); disk-family duplicates may be reclaimed by a
future janitor, explicitly out of scope here.

## 3. Schema delta summary

`backend.implementation_family` (required, backfilled by kind);
`pool.accepts_writes` (bool, default true), `pool.retired` (bool, default
false, guarded), `pool.media_generation` (nullable); FK RESTRICT ×2 (batch
recreate, constraints re-declared); `artifactclass_policy` durability columns
+ `[durability]` in the strict parser. NO Copy/XOR changes. NO grain
backfill. (The v2.1 `archive_id` index idea was dropped in verify r2 — no
bundle auto-adoption, so it is not load-bearing.)

## 4. Verification members (scenario-or-cover; failure legs are the point)

- **BSH (bundle self-heal, hermetic):** happy leg (damage one pool's copy →
  scrub marks → reconcile → repaired on distinct media → restore
  byte-identical) PLUS: repair **after `sweep_staging` purged the landing
  bytes**; corrupt-source fallback (source #1 proven-mismatch → SUSPECT →
  heals from #2); crash-mid-repair leaves no duplicate/no mis-adoption
  (re-run converges); repaired copy has `BlobRoot` + `AssetLocator`; family
  count preserved (distinct tape ≠ distinct family).
- **DIV (durability enforcement, hermetic):** EP1 rejects a 3-pool/1-family
  policy; drain guard refuses fencing a floor-critical pool; EP2 transient
  failure → backoff condition (retry converges, no duplicate), structural
  single-family fan-out → blocked + alarm, bundle state not stuck-open;
  pool-delete with live locators → RESTRICT error; schema-assert leg
  (XOR + uniques survive the batch migrations).
- **Extend Q:** trust-first source choice (stale plain vs fresh AEAD → picks
  fresh AEAD); transient read error does NOT latch SUSPECT; proven mismatch
  does, heal falls back, and a later verified-good scrub CLEARS SUSPECT.
- M1 routing is covered by the existing green suite (J/N/O/Q unchanged
  semantics) + sutradhara pytest.

## 5. Non-goals

M4-style backfill (dropped, permanently); converting `replicate_asset` now;
XOR tightening; significance-driven counts; same-kind multi-backend routing;
media-generation policy; duplicate GC; lease/queue changes
(`prompt-jobs-safety-rails.md`); hdcache scope; on-tape format changes;
`verify`/`dr` selector purposes; 3-state pool enum.

## 6. Prompt staging (after freeze)

1. **CG-M1** — durability predicate module + routing + duplicate flags
   (+ D5.1/D5.2/D5.3 riding along; pytest-only verification).
2. **CG-M2** — build primitive factor-out + `bundle_copy` reconciler domain +
   `bundle-repair` handler + scrub adoption fix + D4 selector/SUSPECT
   lifecycle + scenario BSH + Q extension.
3. **CG-M3** — D2/D3 schema + parser + EP1/EP2 + drain guard + scenario DIV.
   (D5.4 rides with whichever of these first touches the replicate write
   path.) Grain pin (D1) binds the future copy-handler prompt, which must
   also include `--reopen-blocked --reason not-implemented` per
   `prompt-jobs-safety-rails.md`.
