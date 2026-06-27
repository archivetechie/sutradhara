# Codex prompt — P3.2: lifecycle / deletion gate (retention engine) — sutradhara

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo` (single repo).**
> Read `CLAUDE.md` + `AGENTS.md` first.
>
> **Authoritative design: `docs/design-deletion-gate.md` — read it in full.** This prompt is the build
> order, the must-be-exact contracts, and the acceptance tests; the design doc is the *why*. Source:
> plan item **P3.2** (`docs/implementation-plan-ingest-v2.md`), Phase U.
>
> **What this is — the ONLY thing in the system that deletes bytes.** A deliberately paranoid, logged,
> **imperative** gate that reclaims the temporary copies (per-intake **cloud-temp blob** + **landing/
> staging originals**) **only after** proving the durable copies are **verified + offsite-confirmed** and
> nothing still needs the landing. *Nothing deletes before the gate.*
>
> **Read this twice — the gate is content/bundle-aware, NOT `replication_status`.** P2.5 archives content
> as **bundle copies** (`Copy.bundle_id` set, `logical_asset_hash` NULL) with per-asset `AssetLocator`s.
> `replication_status`/`_healthy_copies` only see asset-scoped copies, so the gate must compute durability
> over `AssetLocator`s joined to healthy `Copy`/`Bundle` (design §2/§5).

## What already exists — BUILD ON IT
- **Recipe + offsite flag:** `target_pools()` → `PoolTarget{pool_id, offsite_gate, …}` (`replication.py:94`);
  `Pool.offsite_gate` (`models.py`). **Copy→media:** `_copy_media_id()` (`replication.py:454`).
- **Healthy-copy helper:** `_healthy_copies` (`replication.py:416`, `health==OK`). **Restore** picks
  locators at `archive_restore.py:281`. **VA** archived-class check `_healthy_archived_artifactclasses`
  (`virtual_arrangement.py:401`).
- **Cloud-temp blob:** `cloud-blob:<intake>` bundle `Copy` on pool `cloud-temp`
  (`jobs/handlers/cloud_blob.py`). **S3 backend:** `backend/s3.py` (enumerate/write/read/verify — **no
  delete**).
- **Signals:** `Submission.status`/`Arrangement.status`, `Intake.requested_profile`,
  `Copy.last_verified_at`, the P0.3 `reconciliation_condition` (`jobs/models.py:144`), the derivation
  profile registry `profiles.entries_for` + `derivation.make_target_key`.
- **Landing-dependent work creators (must be frozen):** `create_from_intake` (`arrangement.py:99`),
  `prepare_intake` (`intake.py:317`). **Migration head:** P3.1's `c4e9b7a2d6f8`.

## Build order

### A. Two dependencies first
1. **P2.5 verify stamp (required, design §7 / §2 codex-High).** In `flush_bundle`, after a successful
   `_verify_members_from_copy` for a target's bundle copy, **stamp `Copy.last_verified_at = now`** (the
   read-back already happens; just record it). Without this, fresh archives never gate-release.
2. **A `delete_object` backend port (design §3 / codex-Medium).** Add `delete_object(locator)` to the
   writable-backend Protocol and the **S3** impl (+ others as needed), **idempotent**: deleting an absent
   object is **success**, not an error.

### B. Model + migration (`catalog/models.py`, `catalog/types.py`, one Alembic revision from `c4e9b7a2d6f8`)
- `offsite_confirmation` — `media_id` PK str, `confirmed_at`, `confirmed_by`, `shipment_id` null.
- `Intake` += `retention_state` (`'held'|'released'|'purged'`, default `held`, CHECK), `released_at` null,
  `staging_deleted_at` null.
- `Copy` += `deleted_at` tz datetime null (**tombstone**; keep the row).
- `retention_event` — `id`, `intake_id` FK indexed, `action` (`'released'|'cloud_blob_deleted'|
  'staging_deleted'`), `actor`, `at`, `detail` json null. Append-only.
- `batch_alter_table` for the `Intake`/`Copy` column adds (sqlite). `create_all` == `alembic upgrade head`
  (extend `tests/test_schema.py`).

### C. `deleted_at` joins the GLOBAL usable-copy predicate (design §3 / codex-Low)
Add `Copy.deleted_at.is_(None)` to **every** healthy/usable-copy filter: `_healthy_copies`
(`replication.py:416`), the restore locator selection (`archive_restore.py:281`), the `verify` handler,
`virtual_arrangement._healthy_archived_artifactclasses`, and the gate's own query. A tombstoned-but-
`health==OK` row must never be picked by restore.

### D. The gate (`src/sutradhara/retention.py`) — `releasable(session, intake) -> bool`
Exactly design §5. Pure read, no mutation. In order:
1. **Landing-dependency holds (checked first, decision 8):**
   - **non-terminal arrangement** over the intake: status ∈ {`draft`,`pending_derivatives`,`ready`}, **or**
     `submitted` that is NOT (submission row **exists AND** `status=='archived'`) — **fail closed** on a
     `submitted` arrangement with NULL/missing `submission_id`.
   - **prepared-profile**: if `requested_profile` is set, compute the **desired** derivation/index targets
     from the profile registry (`profiles.entries_for` per item → `(item, job_kind)`), and require **each**
     to have a **terminal** `reconciliation_condition` (`satisfied` or `blocked`). **A missing condition
     row counts as held** (the reconciler may not have discovered it yet — fail closed).
2. **Per-asset durability, per-pool existential (decision 2):** for each `IngestItem` →
   `(logical_asset_hash X, artifactclass)`, for each recipe pool `P` in `target_pools(artifactclass)`:
   require **≥1 qualifying** backing copy on `P` — `deleted_at IS NULL AND health==OK AND last_verified_at
   is not None`, where "backing copy on P" = asset-scoped `Copy(X, pool_id=P)` **or** `AssetLocator(X,
   pool_id=P)` via its `Copy`. If `P.offsite_gate`, the qualifying copy's `_copy_media_id` must be in
   `offsite_confirmation`. (Proxies — no offsite_gate pools — need no media.)
Intake releasable ⟺ no hold AND every asset releasable. **Reject (`LogicalAsset.rejected_at`) is NOT
consulted.**

### E. Deletions + the freeze (`retention.py`)
- **`run_retention(session, intake, *, actor)`** — if `intake.retention_state=='held'` and
  `releasable(intake)`: `delete_object` the `cloud-blob:<intake>` object (idempotent), **tombstone** its
  `Copy` (`deleted_at=now`, keep row), set `retention_state='released'`, `released_at=now`, write
  `released` + `cloud_blob_deleted` events. Order: external delete → DB → caller commits (a "deleted but
  DB rolled back" retry re-runs cleanly). If not releasable: no-op (stays `held`). Never deletes landing.
- **`sweep_staging(session, intake, *, actor, grace_days)`** *(the destructive step)* — if
  `retention_state=='released'` and `released_at + grace_days < now`: delete the landing/BagIt originals
  (delete-first/idempotent), set `retention_state='purged'`, `staging_deleted_at=now`, write
  `staging_deleted`. No hold re-check needed (the freeze guarantees no new work).
- **Freeze (decision 9):** `create_from_intake` (`arrangement.py`) and `prepare_intake` (`intake.py`)
  **refuse** when `intake.retention_state` ∈ {`released`,`purged`} (clear error pointing to virtual
  arrangements for post-archive organizing).

### F. CLI (`src/sutradhara/cli/retention.py` + wire `cli/main.py`)
- `sutra offsite confirm --tape <id> [--shipment <id>] [--confirmed-by <who>]` → record
  `offsite_confirmation` for the tape's `media_id` (accept `--media-id` directly this slice if barcode→uuid
  needs the tape catalog; §8.6). Idempotent.
- `sutra retention run [--intake <id>]` → `run_retention` over held intakes.
- `sutra retention sweep-staging [--intake <id>]` → `sweep_staging` over released+grace intakes.
- `sutra retention status [--intake <id>]` *(read-only)* → per-asset gate truth + per-intake state + grace
  deadline.

## Must-be-exact contracts
- **Gate is AssetLocator/bundle-aware**, **per-pool existential** (one qualifying non-deleted/healthy/
  verified copy per recipe pool); media checked **only** for `offsite_gate` pools; proxies need no media.
- **Three landing holds, all fail-closed** (non-terminal arrangement incl. NULL-submission; prepared-
  profile incl. **missing condition row**); plus the **decision-9 freeze** on landing-work creators.
- **Verified = `health==OK` AND `last_verified_at is not None`**; P2.5 must stamp it (A.1).
- **Reject is irrelevant to the gate.** **`deleted_at IS NULL`** is global (C). Cloud copy is **tombstoned,
  never hard-dropped**. `delete_object` is **idempotent**; delete-external-then-DB ordering.
- **Imperative, caller-owns-txn** (no internal commit), logged via `retention_event`. **Nothing deletes
  before the gate.** No reconciler, no rem change. Migration chained from `c4e9b7a2d6f8`.

## Tests — DoD (`tests/test_retention.py` + extend `tests/test_schema.py`, memory backends + fake cloud/staging)
All of design §6:
- **gate truth table** — written-but-unverified → NOT; verified+offsite-gated+unconfirmed → NOT;
  verified+confirmed → releasable; **proxy-only (no offsite_gate) → releasable on verification with ZERO
  offsite events** and no `media_id`.
- **bundle/AssetLocator durability** — an asset archived **only** via a source-map bundle copy (no
  asset-scoped `Copy`) is correctly present + releasable.
- **verified stamped at archive** — after P2.5 build-verify the bundle `Copy` has `last_verified_at`, so
  archive→offsite-confirm→retention-run releases; a `health==OK` + NULL-`last_verified_at` copy does not.
- **offsite confirm inheritance** — confirming a tape marks every copy on it; an unconfirmed tape's copy not.
- **live-arrangement hold + dedup** — held by a draft/ready arrangement or submitted+pending submission,
  **even when** the hash is durable via another intake; **fail-closed** on a submitted arrangement with
  NULL `submission_id`.
- **prepared-profile hold** — held while a desired target is `open` **or has no condition row yet** (run
  before discovery — a freshly-prepared intake with zero conditions must hold); releases when all desired
  targets are `satisfied`/`blocked`; holds **with no arrangement**.
- **per-pool existential** — a pool with a verified copy + an older unverified/deleted duplicate still
  counts as satisfied.
- **tombstoned copy globally unusable** — a cloud `Copy` with `deleted_at` set (but `health==OK`) is not
  picked by restore, not counted by the gate.
- **per-intake release** — all assets releasable → cloud `delete_object`'d + `Copy` tombstoned + `released`
  + grace started + events; **one** asset short → nothing deleted, stays `held`.
- **released freezes new work** — after `released`, `create_from_intake`/`prepare_intake` **refuse**.
- **delete idempotent / crash-safe** — `delete_object` on a missing object is a no-op; a deleted-but-DB-
  rolled-back retry re-runs cleanly.
- **staging grace** — `sweep-staging` no-op before `released_at + grace`; deletes + `purged` + logs after;
  durable copies untouched.
- **reject doesn't block** — a rejected-but-archived asset is still releasable; staging deletable; copies
  stand.
- **no-delete-before-gate** — a `held` intake's cloud blob + landing both intact.
- **idempotency** — `retention run` / `offsite confirm` / `sweep-staging` twice each (no double-delete).
- **audit** — every cloud/staging deletion writes a `retention_event`.
- **existing suites green** — `uv run pytest` (the P2.5 `last_verified_at` stamp + `deleted_at` predicate
  must not break archive/restore/self-heal); format + type-check clean.

## Acceptance
- **Nothing deletes before the gate.** On offsite-confirm the cloud blob expires and staging becomes
  deletable; **proxy-only assets release with no offsite event**; the three landing holds + freeze prevent
  deleting a live source. `uv run pytest` + format + type-check green, plus a `~/system` scenario (archive →
  `offsite confirm` → `retention run` deletes the blob + marks staging → `sweep-staging` after grace deletes
  landing; a not-yet-confirmed sibling stays held; a freshly-prepared intake stays held).

## Out of scope (do NOT build here)
- **No full chain-of-custody lifecycle** (`ejected`/`in_transit`) — gate needs only verified + offsite-confirmed.
- **Not a reconciler / no autonomous deletion** — operator/cron triggers the verbs.
- **No un-delete / staging recovery** — the durable archive is the recovery.
- **No re-verification scheduling / freshness policy** — read existing `last_verified_at`.
- **No rem / archive-object / reconciler-spine change.**
