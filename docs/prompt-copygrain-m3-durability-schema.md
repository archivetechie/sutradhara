# Codex prompt — copy-grain M3: durability schema + enforcement (D2/D3/EP1/EP2) — sutradhara

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`
> (single repo — no Shared-contract section).** Read `CLAUDE.md` + `AGENTS.md` first.
> **Authoritative, FROZEN design: `docs/design-copy-grain-durability.md` — §2 D2, §2 D3, §2 D6,
> §3 schema delta, §6 stage CG-M3. The design is the authority; where this prompt and the design
> disagree, the design wins — stop and flag, do not adapt.**
> **Depends on CG-M1 AND CG-M2 landing first.** EP2's write-time check reuses M2's `bundle_copy`
> `observe`; the floor constant M2 hardcoded becomes the persisted column here. Land M1+M2 on
> `main` before starting M3.
>
> **What this is.** The declared durability floor, its schema, and the two enforcement points
> (EP1 config-time, EP2 write-time). This is the **only** CG stage with a migration. Every
> pre-existing test stays green; the pilot catalog (`/var/lib/replica/pilot.db`) must migrate
> cleanly.

## What already exists — BUILD ON IT, do not rebuild
Verify each `file:line` yourself.

- **`src/sutradhara/catalog/models.py`** — `Backend` (`:640-666`), `Pool` (`:716-762`),
  `ArtifactClassPolicyRecord` (`:801-824`), `AssetLocator.pool_id` FK `ondelete=CASCADE`
  (`:1005-1010`), `BlobRoot.pool_id` FK `ondelete=CASCADE` (`:1061-1066`), and on `Copy` the two
  constraints the design names as the schema-assert canaries: `uq_copy_backend_locator`
  (`:1149-1153`) and `ck_copy_asset_xor_bundle` (`:1154-1158`).
- **`src/sutradhara/catalog/types.py`** — `BackendKind` (`:27-42`): the **nine** values the
  family registry must cover — `rem_tape, d2_tape, rem_disk, plain_disk, ssh_disk, s3, gcs,
  azure_blob, memory`.
- **`src/sutradhara/replication.py`** — `_copy_media_id` (`:455-466`, tape `tape_uuid` / d2
  `volume_uuid`|`barcode`) is the seed for EP2's per-family identity extractor;
  `_assert_distinct_media` (`:539-562`) is the existing (tape-only) check EP2 must **replace**
  with a per-family version (v1's plan to reuse it verbatim would make every non-tape pool,
  including the LAN `ssh_disk` backup, unwritable — design D2 EP2).
- **`src/sutradhara/artifactclass_policy.py`** — strict parser `_require_keys`/`_reject_keys`
  (`:197-206`, the allow-list to extend), `ArtifactClassPolicy` dataclass (`:115-124`),
  `apply_artifactclass_policy` (`:260-322`, where columns persist and EP1 validates). D5.3 from M1
  already validates `restore_preference` unknown pools here — add the write-fence warning branch.
- **`src/sutradhara/archive_fanout.py`** — `flush_bundle` per-target loop (`:534-605`); EP2's
  outbox write + post-commit check hook here. `close_bundle`/`enqueue_post_flush_hdcache_fills`
  (`:603-604`) mark the transaction boundary.
- **`src/sutradhara/jobs/reconcilers/`** — M2's `bundle_copy` domain (`bundle_copy.py`) with
  `observe` (placement-complete-AND-floor) and `record_observation`/`record_condition`
  (`conditions.py`). EP2 reuses `observe`; the outbox uses `record_observation`.
- **`src/sutradhara/scrub.py`** — M2's `ScrubReport.unknown_objects` counter; the D6 alarm reads
  the persistent count.
- **Migration house pattern** — `alembic/versions/2f4a8bb0c2d7_archive_locator_invariants.py`
  (`batch_alter_table` re-declaring constraints on SQLite). **Current head:
  `4e6f8a1c2b3d`** (`uv run alembic heads`) — chain new revisions from it.

Transaction discipline: **flush, never commit/rollback**; callers own the transaction.

---

## A. `Backend.implementation_family` (design D2) — required string + kind registry
- Add `Backend.implementation_family` (required `String`, no server default in steady state).
- New registry mapping **every** `BackendKind` (all nine) → a family. Proposed operator-meaningful
  mapping (confirm names with the design's intent; `rem_tape` and `d2_tape` **must** be distinct
  families so N's rem+d2 fan-out counts as 2 families):
  `rem_tape → "tape"`, `d2_tape → "d2tape"`, `rem_disk|plain_disk|ssh_disk → "disk"`,
  `s3|gcs|azure_blob → "cloud"`, `memory → "memory"`.
- Migration: add the column nullable-with-server-default, **backfill by kind** from the registry,
  then drop the server default and set `NOT NULL` (SQLite batch, re-declaring constraints).
- Fail closed: registering a backend whose kind has no family entry is a hard error.

## B. `Pool` lifecycle columns (design D3, slim)
- `Pool.accepts_writes` bool, default **true** (write fence): `target_pools` **excludes** fenced
  pools for writes; restores still **read** them (do not filter fenced pools out of the restore
  locator walk). Wire the M2 `# M3/D3 accepts_writes` marker in `bundle_copy.discover` to narrow
  the write-eligible set to `accepts_writes == True`.
- `Pool.retired` bool, default **false**, settable **only when the pool holds no live
  `AssetLocator`** (guarded — raise if live locators exist). The 3-state enum is **deferred**
  (§D3) — do not add it.
- `Pool.media_generation` nullable string — **descriptive only** (migration-campaign queries),
  never enforcement (B4). No code path may branch on it for durability.
- Migration adds the three columns (batch, server defaults for the bools, re-declare constraints).

## C. FK RESTRICT ×2 + schema-assert (design D3, §3)
- `asset_locator.pool_id` and `blob_root.pool_id` → `ondelete=RESTRICT` (a pool holding restore
  coordinates must not be CASCADE-deleted out from under them). SQLite requires
  `batch_alter_table` full table-recreate: the migration must **explicitly re-declare ALL
  constraints/indexes** for each recreated table (house pattern `2f4a8bb0c2d7`) or uniques/FKs
  silently vanish.
- **Post-migration schema-assert test** (design §D3): assert on a migrated DB that
  `ck_copy_asset_xor_bundle` and `uq_copy_backend_locator` on `copy` **survive**, that
  `uq_asset_locator_copy_asset_member` and `uq_blob_root_copy_root` survive, and that the two
  `pool_id` FKs are now `RESTRICT`. (The `copy` constraints are the design's named canaries —
  they double as a guard that no batch migration accidentally recreated `copy` and dropped them.)

## D. `[durability]` policy table + persisted floor (design D2)
- Parser: add `"durability"` to the strict allow-list (`_reject_keys` at
  `artifactclass_policy.py:202-206`) and parse a `[durability]` table with `min_copies` (int ≥ 1)
  and `min_impl_families` (int ≥ 1), rejecting unknown keys (house strictness). Extend the
  `ArtifactClassPolicy` dataclass with a `DurabilityPolicy` field.
- Persist on `ArtifactClassPolicyRecord`: new columns `min_copies`, `min_impl_families`.
- **Default (B4)**: one global archival default `min_copies=3, min_impl_families=2` — a class that
  declares no `[durability]` **inherits the archival floor** (safe-by-default). A class overrides
  explicitly (e.g. proxies: `min_copies=2, min_impl_families=1`). Migration backfills existing
  policy rows to `3/2`.
- Replace M2's hardcoded floor constant in `bundle_copy.observe` with the persisted per-class
  columns (remove the `# M3/D2 floor` marker).

## E. EP1 — config-time enforcement (design D2)
- **Policy apply** (`apply_artifactclass_policy`): validate the class's **write-eligible** pools
  (`accepts_writes == True`) can satisfy its floor — enough distinct pools for `min_copies` and
  enough distinct `implementation_family` values for `min_impl_families`. Fail apply otherwise
  (hard error, like `UnknownPolicyPool`).
- **Drain guard** on any `accepts_writes` flip: a `set_pool_write_fence(session, pool_id, *,
  accepts_writes, force=False)` function (+ a CLI verb — extend the existing pool/backend CLI;
  ground the module yourself) re-runs the same floor validation. Flipping `accepts_writes=False`
  is **REFUSED** if it would drop any active class below its floor **without a complete
  replacement pool**; `force=True` overrides with a **loud operator alarm** (ERROR log +
  structured record). Add the write-fenced-pool **warning** branch to D5.3's `restore_preference`
  validation (kept readable, `ArtifactClassPolicyWarning`).

## F. EP2 — write-time enforcement, crash-safe via the condition outbox (design D2)
Inside `flush_bundle`'s fan-out transaction (**same transaction as the copies**, before commit):
- **Durable outbox**: write/update the target bundle's `bundle_copy` condition row to `open`,
  reason `durability-unverified` (via `record_observation`, domain `bundle_copy`,
  `target_key=bundle.id`, `OBSERVED_MISSING`). A check scheduled only *after* commit is lost if
  the process dies between commit and check — the outbox row survives so M2's level-triggered
  `observe` performs the identical check on the next sweep.
- **Immediate post-commit fast path**: after the transaction commits, re-run the `bundle_copy`
  check (M2's `observe` + gate) to resolve the condition right away. If it never runs, the sweep
  covers it.
- **The check** = distinct media via a **per-family identity extractor** (generalize
  `_copy_media_id`): `tape` family → locator `tape_uuid`; `d2tape` → `volume_uuid`/`barcode`;
  `disk`/`cloud` families → **backend row identity** (the backend IS the host/filesystem/bucket;
  locators carry no media fields and need none — two copies on one `ssh_disk` backend = same
  media); `memory` → exempt. Plus realized `implementation_family` count vs `min_impl_families`,
  and realized copy count vs `min_copies`.
- **Deficiency classification** through the condition vocabulary, not a bare raise:
  - transient backend failure → `CONDITION_BACKOFF` (bounded retry).
  - structural floor violation (e.g. single-family fan-out that can never meet
    `min_impl_families`) → `CONDITION_BLOCKED` + operator alarm, **no hot-retry** (a structurally
    failing target must not manufacture duplicate copies each attempt). The bundle state must not
    be stuck-open silently — the blocked condition is the visible signal.

## G. D6 — duplicate-persistence alarm (design D6)
A persistent `duplicate_count > 1` for one `(target, pool)` (from M1's `placement_status`) raises
an **operator alarm** (it is a stuck-retry signal). Surface it through the same
condition/log alarm channel EP2 uses — do **not** add a uniqueness constraint, do **not** add a
GC/janitor (out of scope). Tape duplicates are permanent-and-unreclaimable by design; the alarm
keeps them rare and visible. EP2's transient/structural classification is what prevents
retry-manufactured duplicates; repair re-checks `bundle_replication_status` inside its write
transaction (already in M2).

## H. Grain pin note (design D1) — for the FUTURE copy-handler prompt, implement nothing
Add a short **"Forward — copy-handler grain pin"** section to `docs/design-copy-grain-durability.md`
(or a sibling note; do NOT touch INDEX) stating that the future real `copy` job handler MUST
record **bundle grain** (degenerate 1-member bundle: bundle row + member + `AssetLocator` +
bundle-scoped `Copy`; deterministic id `asset-<hash16>`, collision-checked against real bundle
ids), while KEEPING the single-object seal/write path (`RaoCliSealer`/`write_object_to_pool`) —
it does **not** use the flush archive/tar container (the two build paths are not byte-compatible;
Q asserts byte-identity across write→repair). It must also carry
`sutra reconcile copy --reopen-blocked --reason not-implemented` per
`prompt-jobs-safety-rails.md`. This is a note for that prompt's author — implement nothing else.

---

## Non-goals (do NOT do these)
- **No** Copy/XOR grain change, **no** grain backfill, **no** M4-style legacy conversion
  (dropped permanently, §0). **No** 3-state pool enum. **No** `media_generation` enforcement.
- **No** duplicate GC/janitor. **No** same-kind multi-backend routing. **No** significance-driven
  counts. **No** implementing `copy`/`replicate_asset` grain (future prompt).
- **No** lease/queue changes (`prompt-jobs-safety-rails.md`), **no** hdcache scope, **no** on-tape
  format changes.
- **Migration must be reversible** and must not touch data beyond the declared backfills; the
  persistent pilot catalog must upgrade cleanly. Do **not** touch `docs/INDEX.md`.

## Tests — add these (extend the nearest existing suite; keep every current test green)
- **Migration**: upgrade→downgrade→upgrade round-trips; `implementation_family` backfilled by
  kind for all nine kinds; policy rows backfilled to `3/2`; **schema-assert leg** (§C) passes.
- **Parser**: `[durability]` parsed; unknown key rejected; a class with no `[durability]` inherits
  `3/2`; proxies override to `2/1`.
- **EP1**: apply rejects a policy whose write-eligible pools give 3 pools but 1 family (can't meet
  `min_impl_families=2`); drain guard **refuses** fencing a floor-critical pool, `force=True`
  overrides with an alarm.
- **EP2**: single-family fan-out → `blocked` + alarm, bundle not stuck-open, **no duplicate**;
  transient backend failure → `backoff` (retry converges, no duplicate); the outbox condition
  exists after commit and the fast-path resolves it; per-family identity treats two `ssh_disk`
  copies on one backend as same media, two on distinct backends as distinct.
- **RESTRICT**: deleting a `Pool` with live `AssetLocator`/`BlobRoot` raises a RESTRICT error
  (not a CASCADE wipe of restore coordinates).
- **D6**: a persistent `(target, pool)` duplicate raises the alarm once (not per-scan spam).

## Verification
- `cd ~/sutradhara/repo && uv run pytest -q` — **green**, including every pre-existing test.
- `uv run alembic upgrade head` on a copy of a pre-M3 DB succeeds; `alembic downgrade` reverses.
- **Covers**: the harness verification member is scenario **DIV** (and BSH's family-preserved
  leg) in `~/system/docs/prompt-copygrain-harness-scenarios.md` (SKIP-gated until this lands).
- **Editable-dep trap** (`CLAUDE.md`/memory): `~/system`'s `make scenario-*` imports sutradhara
  from the **working-tree branch** via an editable install. **Land this complete on `main`** or
  the harness silently regresses. Commit at green milestones; direct-to-main, no PRs; never ask
  the operator to do hygiene.

## Acceptance criteria
1. `Backend.implementation_family` (registry over all nine kinds, migration backfills by kind);
   `Pool.accepts_writes`/`retired`(guarded)/`media_generation`(descriptive) added.
2. FK RESTRICT ×2 via `batch_alter_table` re-declaring ALL constraints/indexes; schema-assert
   test proves `ck_copy_asset_xor_bundle` + `uq_copy_backend_locator` (and the asset_locator/
   blob_root uniques + RESTRICT FKs) survived.
3. `[durability]` in the strict parser + persisted `min_copies`/`min_impl_families`; global
   default 3/2 inherited, explicit per-class override; M2's floor constant replaced by the column.
4. EP1 apply-time floor validation + drain guard on `accepts_writes` flips (refuse/`--force`+alarm).
5. EP2 outbox in the fan-out transaction + post-commit fast-path with per-family media identity
   and transient→backoff / structural→blocked classification; D6 duplicate alarm wired.
6. Grain pin note added for the future copy-handler prompt; migration round-trips; pilot DB
   upgrades cleanly; the diff gate has a clear implementation summary to review.
