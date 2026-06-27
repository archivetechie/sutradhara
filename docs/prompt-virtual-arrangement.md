# Codex prompt — P3.1: virtual arrangement (post-archive organize-forever) — sutradhara

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo` (single repo).**
> Read `CLAUDE.md` + `AGENTS.md` first.
>
> **Authoritative design: `docs/design-virtual-arrangement.md` — read it in full.** This prompt is the
> build order, the must-be-exact contracts, and the acceptance tests; the design doc is the *why*.
> Source: plan item **P3.1** in `docs/implementation-plan-ingest-v2.md`.
>
> **What this is.** The post-archive **organize-forever** layer — the virtual mirror of pre-archive
> `arrangement`. After content is archived, operators keep organizing it into **virtual arrangements**
> (named, permanently-mutable views) entirely in the catalog: `mv` / `exclude` / `tag` / `reject`. None
> of it touches BagIt landing data or rewrites RAO/d2 objects. Restore resolves *a virtual path → the
> archived asset → its locators → P2.2 member-aware restore*.
>
> **Identity grain (read twice): CONTENT-level.** Members key on the **archived asset =
> `(logical_asset_hash, artifactclass)`**, *not* `IngestItem` — the catalog is content-addressed (bytes
> dedupe across occurrences), and `restore_asset` is hash-based. Provenance stays in `IngestItem`; it is
> never the organizational grain here.

## What already exists — BUILD ON IT, do not rebuild
- **`restore_asset`** (`src/sutradhara/archive_restore.py`, `sutra archive restore`): hash-based —
  `restore_asset(session, asset_hash, artifactclass, destination, backends, …)` selects the best healthy
  `AssetLocator` by policy, extracts (P2.2 member-aware), verifies `sha256`, atomically writes. It
  **already refuses *suspect* assets without `--force`** — preserve that exactly.
- **`LogicalAsset`** (`catalog/models.py`): `content_sha256` PK, the deduped content. `Copy`/`AssetLocator`
  key on `logical_asset_hash`; a hash can have locators via bundles of **several artifactclasses**.
- **`arrangement.py`** (P2.3a): the pattern to mirror — imperative catalog mutations, caller-owned
  transaction (no internal commit), `canonical_member_path` normalization, partial-unique path index,
  reversible-vs-terminal status. Reuse `canonical_member_path`.
- **Migration head** is P2.5's `b2d7f3a8c91e` (widen bundle member paths). Chain the new revision from it.

## Build order

### A. Model + migration (`catalog/models.py`, `catalog/types.py`, one Alembic revision)
Add **four tables** + **reject columns on `LogicalAsset`** (design §3 is the spec):
- `virtual_arrangement` — `id`, `name` UNIQUE, `description` null, `created_by`/`created_at`/`updated_at`.
- `virtual_arrangement_member` — `id`, `va_id` FK(CASCADE) indexed, `logical_asset_hash` FK indexed,
  **`artifactclass`**, `path` (String **2048**, NFC relative), `excluded` bool default false (reversible),
  `added_by`/`added_at`/`updated_at`. **`UNIQUE(va_id, logical_asset_hash, artifactclass)`** + a
  **partial unique index** `(va_id, path) WHERE excluded = false` (sqlite + pg, like
  `uq_arrangement_member_path_active`).
- `virtual_arrangement_history` — `id`, `va_id` FK, **`va_member_id` FK virtual_arrangement_member
  (ondelete SET NULL)**, **`logical_asset_hash`**, **`artifactclass`**, `old_path`, `new_path`, `actor`,
  `changed_at`. Append-only; the denormalized `(hash, artifactclass)` make each row self-identify even
  after a member is removed.
- `asset_tag` — `id`, `logical_asset_hash` FK indexed, `tag`, `added_by`/`added_at`, `removed_by`/
  `removed_at` null (**soft-delete tombstone**). **Partial unique** `(logical_asset_hash, tag) WHERE
  removed_at IS NULL`.
- `LogicalAsset` += `rejected_at` (tz datetime null), `rejected_by` (str null), `rejection_reason`
  (str null). Present `rejected_at` ⇒ rejected.

Use `batch_alter_table` for the `LogicalAsset` column adds (sqlite). `create_all` and `alembic upgrade
head` must agree (extend `tests/test_schema.py`).

### B. The core module (`src/sutradhara/virtual_arrangement.py`) — imperative, caller-owns-txn
Mirror `arrangement.py`'s discipline (validate → mutate → `session.flush()`, **never commit**; raise
typed errors). Functions:
- `create_view(session, name, *, description=None, created_by) -> VirtualArrangement`.
- `add_member(session, view, asset_hash, path, *, artifactclass=None, added_by)` — **resolve the
  archived artifactclass**: the asset must have ≥1 healthy `AssetLocator`; if archived under exactly one
  class use it, if several **require `artifactclass`** (else raise), reject if none (not archived).
  Normalize `path` via `canonical_member_path`. Enforce the uniqueness rules.
- `move_member(session, view, from_path, to_path, *, actor)` — move the live member; write a
  `virtual_arrangement_history` row carrying `va_member_id` + `logical_asset_hash` + `artifactclass` +
  old/new + actor. Touches **only** the member row.
- `exclude_member` / `include_member` — flip `excluded` (reversible).
- `reject_asset(session, asset_hash, *, actor, reason=None)` / `unreject_asset(...)` — set/clear the
  `LogicalAsset` reject columns (content-level, all views).
- `add_tag` / `remove_tag` (soft-delete) — `asset_tag`, content-level.
- `list_view` / `show_view` — `excluded`/rejected hidden unless `include_hidden=True`.
- `resolve(session, view, path) -> (logical_asset_hash, artifactclass)` — the restore resolver.

### C. The reject gate in `restore_asset` (`archive_restore.py`)
Add a **second, independent** gate next to the existing suspect gate:
- New param `force_rejected: bool = False` (the existing `force`/suspect param is **unchanged**).
- If the `LogicalAsset` is rejected (`rejected_at` not null) and not `force_rejected` → refuse
  (a clear `RestoreRejected`-style error). `--force` does **not** bypass reject; `--force-rejected` does
  **not** bypass suspect.
- This makes the gate apply to **both** `sutra archive restore` and `sutra virtual restore`.
- **Do NOT** gate `restore_copy` or any preservation path (self-heal/scrub keep maintaining a rejected
  asset's copies). Reject gates **extraction only**, and **never deletes**.

### D. CLI (`src/sutradhara/cli/`, mirror `cli/arrangement.py`; wire into `cli/main.py`)
- `sutra virtual create|add|mv|exclude|include|ls|show` (+ `--artifactclass` on `add`,
  `--force`/`--force-rejected` on the restore path).
- `sutra virtual restore <view> <path> --dest <p> [--force] [--force-rejected]` → `resolve` →
  `restore_asset(hash, artifactclass, …, force=…, force_rejected=…)`.
- `sutra reject <asset-hash> [--reason]` / `sutra unreject <asset-hash>`.
- `sutra tag add|rm <tag> <asset-hash>`.

## Must-be-exact contracts
- **Member identity = `(va_id, logical_asset_hash, artifactclass)`**; `UNIQUE` on it; one path per
  archived-asset per view (no in-view aliases). Partial-unique `path` over `excluded = false`.
- **`add` resolves the class** (single→auto, multi→require `--artifactclass`, none→refuse-not-archived).
- **Restore unit is `(hash, artifactclass)` straight off the member** — never "derive from the bundle."
- **Two restore gates, two flags:** `--force` (suspect, unchanged) vs `--force-rejected` (reject);
  independent; both apply to archive **and** virtual restore. Preservation paths ungated.
- **Reject never deletes**; bytes always recoverable with `--force-rejected`.
- **History rows self-identify** (`va_member_id` + `logical_asset_hash` + `artifactclass`); append-only.
- **Tags soft-delete** (tombstone, partial-unique on active), content-level.
- **Catalog-only**: never mutate `IngestItem`, landing data, or RAO/d2 objects. Caller owns the
  transaction (no internal commit), like `arrangement.py`.
- **Virtual path is decoupled** from the archived `member_path`: restore uses the frozen locator.
- **No reconciler, no submit/freeze, no rem change.** Migration chained from `b2d7f3a8c91e`.

## Tests — DoD (`tests/test_virtual_arrangement.py` + extend `tests/test_schema.py`)
All of design §6:
- **mv edits catalog only** — member `path` + a history row change; `LogicalAsset`/`IngestItem`/landing/
  RAO unchanged.
- **multi-view** — same asset in two views at different paths, both resolve; `exclude` frees the path,
  `include` re-shows (reversible).
- **content grain / dedup** — two `IngestItem` occurrences, one `logical_asset_hash` → one member;
  restore returns the shared bytes.
- **restore-by-vpath = original bytes** — `sha256 == master`, resolved through the frozen `AssetLocator`
  regardless of the renamed virtual path.
- **reject gates extraction globally, never deletes** — both `archive restore` and `virtual restore`
  refuse without `--force-rejected`; copies/locators/landing intact; `--force-rejected` recovers bytes;
  preservation (`restore_copy`) unaffected; `unreject` restores visibility.
- **suspect vs reject force are distinct** — `--force` bypasses suspect not reject; `--force-rejected`
  bypasses reject not suspect; both needed if both.
- **multi-class restore unit** — a hash under two classes: `add` requires `--artifactclass`; restore uses
  that class's locators.
- **one path per archived-asset per view** — re-adding the same `(hash, artifactclass)` in one view is
  refused.
- **tags soft-delete** — `rm` tombstones (`removed_at`), audit preserved, re-add allowed, no restore
  effect.
- **history carries full identity** — `va_member_id` + `logical_asset_hash` + `artifactclass`; never
  mutated.
- **add requires archived** — refuse an asset with no healthy `AssetLocator`.
- **migration + schema parity** — four tables + `LogicalAsset` reject columns; `create_all` == `alembic
  upgrade head`; chains from `b2d7f3a8c91e`.
- **existing suites green** — `uv run pytest` (the new reject gate must not break `archive restore` /
  self-heal); format + type-check clean.

## Acceptance
- `virtual mv`/`tag add` edit the catalog only; **restore by virtual path returns the original bytes**
  (`sha256 == master`); **reject hides without deleting** and gates extraction globally via
  `--force-rejected`. `uv run pytest` + format + type-check green. A `~/system` scenario (archive →
  organize one asset into two views → restore by virtual path → `sha256 == original`; reject →
  restore-gated; `--force-rejected` recovers) — extend the arrangement scenario or add a sibling.

## Out of scope (do NOT build here)
- **No access/approval enforcement** — tags + reject are governance subjects only; identity model later.
- **No projection / SMB / browsable tree** — that's the **arrangement gateway** project, not core.
- **No submit/freeze** — virtual arrangements are mutable forever.
- **No auto-seeded default view**, no cross-view set algebra / saved queries.
- **No in-view aliases** (one path per archived-asset per view).
- **No rem / archive-object change**; no occurrence-level (`IngestItem`) membership.
