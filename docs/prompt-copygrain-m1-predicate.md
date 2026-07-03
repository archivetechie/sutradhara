# Codex prompt — copy-grain M1: one durability predicate module (+ D5.1/D5.2/D5.3) — sutradhara

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`
> (single repo — no Shared-contract section).** Read `CLAUDE.md` + `AGENTS.md` first.
> **Authoritative, FROZEN design: `docs/design-copy-grain-durability.md` — §2 M1, §2 D5,
> §2 D6, §6 stage CG-M1. The design is the authority; where this prompt and the design
> disagree, the design wins — stop and flag, do not adapt.**
> First stage of the CG set (CG-M1 → CG-M2 → CG-M3). **Nothing here depends on M2/M3.**
>
> **What this is.** One durability-predicate module that the three divergent
> healthy-copy predicates collapse into, plus three independently-landable defect fixes
> (D5.1/D5.2/D5.3) that ride along. **Pure refactor + fixes. NO schema change. NO grain
> backfill. Every pre-existing test stays green**, and each new behavior gets a new test.

## What already exists — BUILD ON IT, do not rebuild
The three grain-divergent predicates the design names (§1(a), §M1) — verify each `file:line`
yourself before touching it:

- **`src/sutradhara/replication.py`**
  - `PoolTarget` frozen dataclass (`:65-79`) — the shape every reader speaks; harness seams
    getattr-map `.pool_id/.backend_name/.representation/...` (compat invariant, §M1).
  - `target_pools` (`:94-155`) — the want side (active `ArtifactClassPool` in catalog order).
  - `replication_status` (`:338-373`) — have vs want over `_healthy_copies` (`:416-427`:
    `health==OK AND deleted_at IS NULL`, **no `last_verified_at` gate** ⇒ this is the
    `require_verified=False` semantics). Calls `_assert_distinct_media` (`:539-562`).
  - `repair` (`:209-263`) and `self_heal` (`:266-335`) both call `replication_status`;
    `self_heal` also calls `select_restore_source` (`:376-395`, over `_healthy_copies`) and
    `restore_copy`.
  - `_healthy_copies_by_pool` (`:398-413`) — used by `replicate_asset` (`:170`).
  - `_copy_media_id` (`:455-466`) — tape `tape_uuid`, else d2 `volume_uuid`/`barcode`.
- **`src/sutradhara/retention.py`**
  - `_qualifying_copies_for_pool` (`:418-454`) — asset-grain Copies **UNION** bundle copies
    reachable via `AssetLocator.copy_id == Copy.id` (`:432-446`), both gated on
    `last_verified_at IS NOT NULL` ⇒ this is the `require_verified=True` **and** the
    asset∪bundle-grain semantics the design's `durable_placements` generalizes.
  - `_pool_gate_status` (`:368-415`) is the sole caller; `_policy_targets` (`:457-469`)
    builds `PoolTarget`s through `target_pools`.
- **`src/sutradhara/restore.py`** — `restore_copy` (`:53-102`) / `_expected_asset_hash`
  (`:150-160`) **reject bundle copies** (`:155`) and operate only on asset-grain `Copy`
  rows ⇒ this is the `direct_copies` view (what a physical whole-copy restore can touch).
- **`src/sutradhara/archive_restore.py`** — `restore_asset` (`:247-340`) walks
  `AssetLocator` rows filtered **only** by `logical_asset_hash` (`:271-277`), then by
  `_restore_pool_order` (`:455-480`). No `Bundle.artifactclass` filter ⇒ the D5.1 bleed site.
- **`src/sutradhara/jobs/handlers/cloud_blob.py`** — hardcodes
  `Representation.RAO_AEAD_V1.value` (`:131`, and the fake-writer path `:170`) instead of
  deriving from `pool.representation` ⇒ the D5.2 site.
- **`src/sutradhara/artifactclass_policy.py`** — `apply_artifactclass_policy` (`:260-322`)
  persists `restore_preference` (`:315`) with **no validation** ⇒ the D5.3 site;
  `_validate_hdcache_privacy_mapping` (`:397-405`) is the house pattern for an apply-time check.
- **`src/sutradhara/catalog/models.py`** — `AssetLocator` (`:985-1033`; `bundle_id`
  nullable `:1017-1022`, `SET NULL` on bundle delete), `Bundle.artifactclass` (`:833`),
  `Copy` (`:1138-1217`).

Transaction discipline is unchanged everywhere: **flush, never commit/rollback**; callers own
the transaction. `add_copy`/`add_bundle_copy` idempotency and the `PoolTarget` shape are
**invariants** — do not change them.

---

## A. New module `src/sutradhara/durability.py` — the single predicate
Two views on an explicit **grain axis** plus two status functions. Everything is read-only
(SELECT + flush-free); it never writes rows.

Define the target inputs (small frozen dataclasses in this module):
- `AssetTarget(asset_hash: bytes, artifactclass: str)`
- `BundleTarget(bundle_id: str)`
- `Target = AssetTarget | BundleTarget`

### A.1 `durable_placements(session, target, *, require_verified: bool, artifactclass: str | None) -> list[Copy]`
The **durability-accounting** view (asset∪bundle):
- **`AssetTarget`** → the UNION of (a) asset-grain `Copy` rows
  (`Copy.logical_asset_hash == target.asset_hash`) and (b) bundle copies reachable via
  `AssetLocator` (`AssetLocator.logical_asset_hash == target.asset_hash`
  JOIN `Copy` ON `AssetLocator.copy_id == Copy.id`), de-duplicated by `Copy.id` — exactly the
  union `retention._qualifying_copies_for_pool` builds, but **not** pool-scoped.
- **`BundleTarget`** → bundle copies (`Copy.bundle_id == target.bundle_id`).
- Health gate: always `health == CopyHealth.OK AND deleted_at IS NULL`. When
  `require_verified=True`, additionally `last_verified_at IS NOT NULL`.
- `artifactclass` argument drives the **D5.1** cross-class filter on the bundle-grain leg
  (see §C). Pass `None` to disable class filtering (asset-grain-only callers).

### A.2 `direct_copies(session, asset_hash) -> list[Copy]`
Asset-grain `Copy` rows only (`logical_asset_hash == asset_hash`, `health==OK`,
`deleted_at IS NULL`, ordered by `Copy.id`) — **byte-for-byte the current `_healthy_copies`
result**. This is what `restore_copy`/self-heal can physically operate on today. Bundle healing
is M2's `bundle-repair`, NOT `self_heal` — `self_heal` keeps this view.

### A.3 `placement_status(session, target) -> PlacementStatus`
Want (via `target_pools` for the target's class) vs have (via `durable_placements`,
`require_verified=False`). Returns **`PoolTarget`-shaped entries** — reuse `PoolTarget`
attributes (the harness seam getattr-maps `.pool_id/.backend_name/...`; **shape is a compat
invariant**). Each entry additionally carries `have: bool` and, per **D6**, a
`duplicate_count: int` (distinct healthy copies for that `(target, pool)`) and
`is_duplicate: bool` (`duplicate_count > 1`). `placement_status` counts **distinct pools**,
never raw rows. Do **not** add any uniqueness constraint; duplicates are legal (D6).

### A.4 `bundle_replication_status(session, bundle_id) -> ...` (the new capability)
`placement_status` for a `BundleTarget(bundle_id)`: want = `target_pools(bundle.artifactclass)`,
have = bundle copies. This is the predicate M2's `bundle_copy` reconciler `observe` builds on;
give it a stable, documented return shape (mirror `replication.ReplicationStatus`:
`complete/have/want/missing`, entries `PoolTarget`-shaped).

## B. Route the enumerated readers through the module — SEMANTICS PRESERVED
Point every reader the design §M1 enumerates at `durability.py`, **each keeping its exact
current semantics** (this is refactor-only; each is covered by the existing green suite):

| Reader (`file:line`) | View + axis to route to |
|---|---|
| `replication.replication_status` (`:338`) | `durable_placements(AssetTarget, require_verified=False)` |
| `replication.repair` (`:209`) | via `replication_status` (unchanged) |
| `replication._healthy_copies_by_pool` (`:398`, feeds `replicate_asset`) | asset-grain, `require_verified=False` |
| `replication.select_restore_source` (`:376`) / `self_heal` source pick | `direct_copies` |
| `retention._qualifying_copies_for_pool` (`:418`) | `durable_placements(AssetTarget, require_verified=True)`, pool-scoped |
| `restore.restore_copy` preflight (`:150`) | `direct_copies` (bundle copies stay rejected) |

`replication._healthy_copies` becomes a thin shim over `direct_copies` (or is replaced at each
call site) — either way its result set does not change. **`_assert_distinct_media` stays exactly
where it is** (inside `replication_status`); M1 does not touch durability enforcement (that is
M3/EP2). The `require_verified` axis is the semantics-preserving knob per
`docs/design-deletion-gate.md`; the deletion gate keeps `require_verified=True`.

## C. D5.1 — cross-class bleed fix (rides with M1)
In the bundle-grain leg of `durable_placements` **and** in `archive_restore.restore_asset`'s
locator query (`:271-277`):
- For locators with a non-NULL `bundle_id`: JOIN `Bundle` and require
  `Bundle.artifactclass == artifactclass`.
- For locators with NULL `bundle_id` (legacy / `SET NULL` after bundle delete): fall back to
  the asset's **class memberships** — admit the locator iff the asset carries a membership equal
  to `artifactclass`. Ground the membership source yourself (candidates: `IngestItem.artifactclass`,
  `VirtualArrangementMember.artifactclass`); document which you use and why.
This closes the shared-pool leak where asset X archived in a `masters` bundle and a `proxies`
bundle could be restored from the wrong class's copy.

## D. D5.2 — cloud-blob representation from the pool (rides with M1)
In `jobs/handlers/cloud_blob.py`, replace the hardcoded `Representation.RAO_AEAD_V1.value`
(`:131` and the fake-writer path `:170`) with `pool.representation` (the handler already loads
`pool` at `:61`). **Refuse** a pool whose representation the cloud-blob path cannot produce
(raise a clear `ValueError`, mirroring the existing pool/backend guards `:61-66`). Keep the
`storage_metadata` shape otherwise identical.

## E. D5.3 — validate `restore_preference` at policy apply (rides with M1)
In `artifactclass_policy.apply_artifactclass_policy` (`:260-322`), validate each
`restore_preference` pool id (design §D5.3):
- **unknown pool** (no `Pool` row) → raise `ArtifactClassPolicyError` (hard error, like
  `UnknownPolicyPool` at `:277`).
- **write-fenced pool** — M1 has no `accepts_writes` column yet (that is M3/D3), so **only the
  unknown-pool check is enforceable now**. Structure the validation so the write-fence warning
  (`ArtifactClassPolicyWarning`, kept readable) is a **one-line addition** when M3 lands; leave a
  `# M3/D3` marker, but do NOT add the column or the warning here.

---

## Non-goals (do NOT do these)
- **No** schema/migration change of any kind. No new columns, no FK changes, no XOR tightening
  (the Copy XOR stays loose forever, §0).
- **No** grain backfill, **no** converting `replicate_asset` to bundle grain (§D1 — deferred to
  the future copy-handler prompt), **no** `[durability]` parser/columns (that is M3).
- **No** bundle self-heal, **no** `bundle_copy` reconciler, **no** `bundle-repair` handler,
  **no** SUSPECT-lifecycle or `select_source` changes (all M2).
- **No** change to `_assert_distinct_media`, `add_copy`/`add_bundle_copy`, or the `PoolTarget`
  shape. **No** behavior change to any routed reader — if a pre-existing test's expectation
  would move, you have changed semantics: stop and re-check.
- Do **not** touch `docs/INDEX.md` (the session lead owns indexes).

## Tests — add these (extend the nearest existing suite; keep every current test green)
- `tests/test_durability.py` (new)
  - `durable_placements(AssetTarget, require_verified=False)` returns asset-grain ∪
    bundle-via-`AssetLocator`, de-duped; `require_verified=True` drops copies with
    `last_verified_at IS NULL`; `BundleTarget` returns bundle copies only.
  - `direct_copies` == the old `_healthy_copies` set (asset-grain only; bundle copies excluded).
  - `placement_status` flags a `(target, pool)` with two healthy copies as
    `is_duplicate`/`duplicate_count == 2` while `have` counts the pool once (D6).
  - `bundle_replication_status` reports `complete`/`missing` for a sealed bundle.
- **Routing regression**: the existing `replication`/`retention`/`restore` suites stay green
  unchanged (prove semantics preserved). Add one test asserting `replication_status` still counts
  a fresh unverified copy (`require_verified=False`) so J/N/O/Q cannot drift.
- **D5.1**: asset in two bundles of different classes on a shared pool → restore under class A
  never selects class B's locator; NULL-`bundle_id` locator admitted via class-membership fallback.
- **D5.2**: cloud-blob copy records `representation == pool.representation`; a pool it cannot
  produce is refused.
- **D5.3**: policy with a `restore_preference` naming an unknown pool is rejected at apply.

## Verification
- `cd ~/sutradhara/repo && uv run pytest -q` — **green**, including every pre-existing test.
- **Editable-dep trap** (`CLAUDE.md`/memory): `~/system`'s `make scenario-*` imports sutradhara
  from the **working-tree branch** via an editable install. **Land this complete on `main`** or
  the harness silently regresses (`ModuleNotFoundError` / stale behavior). Commit at green
  milestones; direct-to-main, no PRs; never ask the operator to do hygiene.

## Acceptance criteria
1. `durability.py` exists with `durable_placements` (grain × `require_verified` axes),
   `direct_copies`, `placement_status` (PoolTarget-shaped + D6 duplicate flags), and
   `bundle_replication_status`.
2. Every §B reader routes through the module with **identical** observable behavior; the full
   pre-existing suite is green.
3. D5.1 cross-class bleed closed (bundle join + NULL-`bundle_id` class-membership fallback);
   D5.2 cloud-blob representation comes from the pool; D5.3 unknown-pool `restore_preference`
   rejected at apply.
4. No schema change, no grain backfill, `_assert_distinct_media` untouched; the diff gate (per
   `AGENTS.md`) has a clear implementation summary to review.
