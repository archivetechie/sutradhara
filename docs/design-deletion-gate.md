# Design — P3.2: Lifecycle / deletion gate (retention engine)

> Design by Claude + the owner (2026-06-27), for review then implementation. **Repo: sutradhara (+ a
> `~/system` scenario).** Plan item **P3.2** (`docs/implementation-plan-ingest-v2.md`), Phase U of
> `docs/prompt-ingest-v2-sutradhara.md`. Depends: copies (**P2.5**) + cloud-temp (**P1.3**). The signals
> it reads come from P2.5 (`submission.archived`), P3.1 (`LogicalAsset.rejected_at`), and the copy model.

## 0. What this is — the one place bytes get deleted
Everything else in this system *adds* and *preserves*; **P3.2 is the only thing that deletes.** It
reclaims the **temporary** copies — the per-intake **cloud-temp blob** and the **landing/staging
originals** — but **only after proving the durable copies are verified and offsite-confirmed.** It is a
deliberately paranoid, logged, imperative gate: *nothing deletes before the gate*, and the gate only
opens when durability is proven for the content's recipe.

## 1. Reuse-heavy — most of the machinery already exists
| Need | Already there (reuse) | New in P3.2 |
|---|---|---|
| The recipe (required copy set) | `ArtifactClassPool` + `target_pools()→PoolTarget` (`replication.py`) | — |
| Offsite flag per placement | **`Pool.offsite_gate`** (carried on `PoolTarget`) | — |
| Per-asset durability | `_healthy_copies` ideas — but `replication_status` is **asset-scoped only** | a new **AssetLocator/bundle-aware** per-pool durability check (decision 2) |
| Copy → tape identity | `_copy_media_id()` (tape_uuid / volume_uuid) | — |
| Per-intake assets | `IngestItem.intake_id` enumeration | — |
| Live-arrangement hold | `Arrangement.status` / `Submission.status` over `intake_id` | the explicit hold (decision 8) |
| Prepared-profile hold | `Intake.requested_profile` + `reconciliation_condition` (`derivation` domain) | hold until derivation/index facts terminal (decision 8) |
| Rejected signal | `LogicalAsset.rejected_at` — **not consulted** by the gate (decision 7) | — |
| Cloud-temp blob | `cloud-blob:<intake>` bundle `Copy` (pool `cloud-temp`) | its deletion — needs a **delete-capable backend port** (decision 6/§3) |
| Verified signal | `Copy.last_verified_at` — but P2.5 doesn't stamp it | a **P2.5 amendment** to stamp it after build-verify |
| **Offsite-confirmed signal** | — | the confirmation table + `offsite confirm` |
| **The release gate + deletions** | — | the whole retention engine |

## 2. Decisions
1. **Imperative, NOT a reconciler.** Deletion is destructive and irreversible; the P0.3 spine's
   reconcilers only ever *add* (the copy reconciler would *re-create* anything deleted). For a 30-year
   archive you never want an autonomous loop deleting bytes. So P3.2 is two imperative verbs —
   `sutra offsite confirm` and `sutra retention run`/`sweep-staging` — that **read** replication/copy
   state, compute releasability, and perform deletions as **logged** operations (matching P2.5/P3.1).
2. **The gate is AssetLocator/bundle-aware — NOT raw `replication_status` (codex High).** P2.5 archives
   content as **bundle copies** (`Copy.bundle_id` set, `logical_asset_hash` NULL) with per-asset
   **`AssetLocator`** rows. `replication_status`/`_healthy_copies` only count **asset-scoped** copies, so
   they'd see every source-map-archived asset as missing → **never releasable**. The gate instead checks
   durability **per recipe pool, existentially (codex r2):** a recipe pool `P` is **satisfied** iff there
   exists **at least one qualifying backing copy** on `P` — a copy that is **non-deleted**
   (`deleted_at IS NULL`), **healthy** (`health==OK`), and **verified** (`last_verified_at is not None`),
   where "backing copy on `P`" is either an asset-scoped `Copy(logical_asset_hash=X, pool_id=P)` **or** a
   healthy `AssetLocator(logical_asset_hash=X, pool_id=P)` via its `Copy`. *Complete* = every recipe pool
   (`target_pools(artifactclass)`) is satisfied. The **existential per pool** is load-bearing: an old
   unverified/deleted **duplicate** locator on a pool must not block release when a verified copy also
   exists there.
   - ⚠️ **P2.5 does not stamp `last_verified_at`** after its build-verify
     (`archive_fanout._verify_members_from_copy` reads the object back but never records it) — so **P3.2
     requires a P2.5 amendment: stamp `last_verified_at` after a successful bundle build-verify** (the
     read-back already happens; just record it), else fresh archives never gate-release (codex High). A
     separate scrub pass also satisfies it; the gate just reads `last_verified_at`.
   - **Offsite-confirmed** is checked **only for `offsite_gate` pools** (codex nit — *not* every pool, so
     non-tape pools never need a `media_id`): for each offsite_gate recipe pool, the **qualifying** copy's
     `_copy_media_id` must be in `offsite_confirmation`.
   - Classes with **no** `offsite_gate` placements (proxies) release on *complete + verified* alone — they
     **never wait for an offsite event that can't come** and need **no** `media_id`.
3. **Offsite confirmation is per-tape (media), and copies inherit it.** A tape holds many copies;
   `sutra offsite confirm --tape <id> [--shipment <id>]` records **one** confirmation (media_id, who,
   when), and every copy whose `_copy_media_id` matches is confirmed. Idempotent.
4. **Trim the Phase-U lifecycle.** Phase U sketched `written→verified→ejected→in_transit→
   offsite_confirmed`; the gate only needs **verified** (already readable) + **offsite-confirmed**. So
   P3.2 builds **just the offsite-confirmation record + an audit log** — `ejected`/`in_transit` are
   physical chain-of-custody ops-tracking, deferred (they can become event types later).
5. **Per-intake aggregation.** The cloud blob and the landing are per-intake. An **intake** releases when
   **all** its assets are releasable; that triggers the deletions.
6. **Two-phase deletion, both logged.** On release: **delete the cloud-temp blob immediately** (a
   redundant bridge once tape is safe) and **mark staging deletable** (start the grace clock). A
   **separate, grace-gated** step deletes the landing originals after `STAGING_GRACE_DAYS`. This staging
   deletion is the **independently-reviewable destructive step** (§4/§8).
7. **Reject does NOT block staging deletion.** A rejected asset (P3.1) is still durably archived — reject
   gates *extraction*, never *preservation*, so the tape copies stand. Its staging is therefore redundant
   and deletable like any archived asset. (Reject only ever hides + gates restore; it is irrelevant to
   the retention gate.)
8. **Never delete before the gate — EXPLICIT landing-dependency holds (codex r1–r3).** A per-asset
   durability gate is **not enough**: under content dedup the same hash may be durable via **another**
   intake, while several things still read **this** intake's landing `source_path` after its masters are
   archived. So, **checked first**, the intake is **held** while the landing is a live source for **any**:
   - **A non-terminal arrangement.** `draft`/`pending_derivatives`/`ready` reads landing on **submit**
     (`submit_arrangement` builds the source-map from live `item_metadata["source_path"]` and fails if the
     file is gone, `arrangement.py:405`, *before* any `Submission` exists); `submitted`+`pending_archive`
     reads it again at archive (`archive_submission._verify_sources`, `:152`). An arrangement is
     **terminal-for-landing** only if `abandoned`, **or** `submitted` **and** its submission row **exists
     and is `archived`** — **fail closed**: a `submitted` arrangement with a missing/NULL `submission_id`
     (the FK is `SET NULL`, `models.py:307`) counts **non-terminal** (codex r3 Low).
   - **Unfulfilled prepared-profile work (codex r3+r4 Medium).** If `Intake.requested_profile` is set, the
     P1.2 **derivation/index** jobs (`transcode`, `pfr-index`) read the original `source_path`
     (`transcode.py:29`, `pfr_index.py:28`) — **even with no arrangement**. The hold must **compute the
     *desired* targets from the profile registry** — the same enumeration the derivation reconciler uses
     (`profiles.entries_for(artifactclass, requested_profile, media_kind)` per item → `(item, job_kind)`
     targets) — then require **each desired target** to have a **terminal `reconciliation_condition`**
     (`satisfied` = fact present, or `blocked` = gave up). **A missing condition row counts as held
     (fail closed, codex r4):** `prepare` only sets `requested_profile`; the condition rows appear *later*
     via the reconciler's discover/reconcile pass, so a `retention run` that fires **before** discovery
     must NOT read "no conditions" as "done." An `open`/backoff target also holds.
   If an intake isn't fully releasable, **nothing** is deleted (cloud blob and landing both stay).
9. **`released` FREEZES the intake against new landing-dependent work (codex r5 Medium).** The decision-8
   holds are checked once at `retention run` (held→released); but `create_from_intake` and `prepare_intake`
   only require `Intake.status == registered` (`arrangement.py:99`, `intake.py:317`), so **between release
   and grace expiry** an operator could create a new draft arrangement or set a prepare profile that reads
   the (about-to-be-deleted) landing — and `sweep-staging` deletes without re-checking. Fix: **`released`
   is a hard lifecycle boundary** — any operation that creates landing-dependent work
   (`create_from_intake`, `prepare_intake`, and any future one) **refuses** on a `released`/`purged`
   intake. This is correct, not limiting: post-release the content is durably archived, so you organize it
   via **virtual arrangements** (P3.1, content-level, no landing), not pre-archive arrangement/prepare. So
   `sweep-staging` stays a simple `released + grace` check with no re-evaluation race (the freeze
   guarantees no new hold can appear after release).

## 3. The model (new)
```text
offsite_confirmation               # one row per tape/media confirmed offsite
  media_id        PK str           # the value _copy_media_id() returns (tape_uuid / volume_uuid)
  confirmed_at    tz datetime
  confirmed_by    str
  shipment_id     str null         # optional grouping (a batch of tapes shipped together)
  # a copy is offsite-confirmed  ⟺  _copy_media_id(copy) ∈ offsite_confirmation

intake (existing) +=               # the per-intake retention state machine
  retention_state   str            # 'held' | 'released' | 'purged'  (default 'held')
  released_at       tz datetime null # gate passed: cloud blob deleted + staging grace clock started
  staging_deleted_at tz datetime null # landing originals deleted (after grace)

copy (existing) +=                 # tombstone, do NOT hard-drop the cloud-temp copy (codex nit)
  deleted_at        tz datetime null # set when retention deletes the backing object; the row + locator
                                     # survive for 30-yr provenance, but the OBJECT IS GONE → not usable.

retention_event                    # append-only audit of every destructive / gate action
  id              PK
  intake_id       FK intake, indexed
  action          str              # 'released' | 'cloud_blob_deleted' | 'staging_deleted'
  actor           str
  at              tz datetime
  detail          json null        # e.g. {bytes_freed, blob_locator, media_id, path}
```
- `retention_state` on `Intake` (1:1, avoids a join); a separate `intake_retention` table is the
  alternative (§9). Grace deadline = `released_at + STAGING_GRACE_DAYS`.
- **`deleted_at IS NULL` joins the GLOBAL "usable copy" predicate (codex r2 Low).** A tombstoned copy's
  backing object is gone, so it must look unusable to **every** path, not just retention. Add
  `Copy.deleted_at.is_(None)` to the canonical healthy-copy filter — `_healthy_copies` (`replication.py`),
  the **restore** locator selection (`archive_restore.py:281`), `verify`, the virtual-arrangement
  `_healthy_archived_artifactclasses`, `retention status`, and the gate's own durability query. Otherwise
  a `health==OK`-but-tombstoned row could still be picked by restore. (The tombstone is general, but in
  P3.2 only cloud-temp copies are ever tombstoned.)
- **A new delete-capable backend port (codex Medium).** The S3 backend (`backend/s3.py`) exposes only
  enumerate/write/read/verify — **no delete**. P3.2 adds `delete_object(locator)` to the writable-backend
  Protocol + the S3 impl, with **idempotent not-found semantics** (deleting an already-gone object is
  success, not an error). The cloud-blob deletion is **external-delete-first then DB** (like P2.5's
  durability ordering): call `delete_object` (idempotent), **then** tombstone the `Copy` (`deleted_at`) +
  write the `retention_event`, then the caller commits. A crash leaving "object deleted, DB not committed"
  is safe — the retry's `delete_object` is a no-op and the DB update re-applies. The landing (filesystem)
  delete uses the same delete-first/idempotent discipline.

## 4. Operations (imperative CLI)
- **`sutra offsite confirm --tape <id> [--shipment <id>] [--confirmed-by <who>]`** → resolve the tape to
  its `media_id` (the copy locators' `tape_uuid`/`volume_uuid`; accept `--media-id` directly first slice
  if barcode→uuid resolution needs the tape catalog — §9), record `offsite_confirmation`. Idempotent.
- **`sutra retention run [--intake <id>]`** → for each `held` intake: evaluate **every** asset's
  releasability (§5, incl. the pending-submission hold). **If all releasable:** `delete_object` the
  `cloud-blob:<intake>` object (idempotent), **tombstone** its `Copy` (`deleted_at`, keep the row), set
  `retention_state='released'`, `released_at=now`, log `released` + `cloud_blob_deleted`. **If any asset
  is not releasable: do nothing** (stays `held`). Never deletes landing here. Idempotent
  (already-`released`/`purged` skipped; re-run re-deletes a missing object as a no-op).
- **`sutra retention sweep-staging [--intake <id>]`** *(the destructive step — review separately)* → for
  `released` intakes where `released_at + STAGING_GRACE_DAYS < now`: delete the landing/BagIt originals,
  set `retention_state='purged'`, `staging_deleted_at=now`, log `staging_deleted`. Separate verb so the
  irreversible landing deletion is opt-in and independently auditable. Safe **without** re-checking holds
  because `released` froze the intake (decision 9) — no new landing-dependent work can have appeared.
- **`sutra retention status [--intake <id>]`** *(read-only)* → per-asset gate truth (verified?
  offsite-confirmed? releasable?) + per-intake state + grace deadline. The transparency surface.

All verbs are imperative (operator- or cron-triggered), idempotent, and log every byte-affecting action.

## 5. The gate logic — `releasable(intake)`
```
releasable(intake):
    # (1) explicit landing-dependency holds (decision 8) — checked FIRST
    if any Arrangement over this intake is non-terminal:
        # terminal-for-landing = abandoned, OR (submitted AND submission EXISTS AND submission==archived)
        # non-terminal (held)  = draft / pending_derivatives / ready,
        #                        OR submitted with NULL/missing/pending_archive submission  (fail closed)
        return NOT releasable                                   # landing is still a live source
    if Intake.requested_profile is set:
        desired = ⋃ over the intake's items of profiles.entries_for(item) → (item, job_kind) targets
        for each desired target T:
            cond = reconciliation_condition(domain='derivation', target=T)
            if cond is MISSING or cond.state ∉ {satisfied, blocked}:   # missing ⇒ held (fail closed)
                return NOT releasable                           # a prepare job will still read landing

    # (2) per-asset durability — per recipe pool, ONE qualifying copy (decision 2)
    for each IngestItem of the intake → asset = (logical_asset_hash X, artifactclass):
        for each recipe pool P in target_pools(artifactclass):
            qualifying = a backing copy on P that is deleted_at IS NULL AND health==OK
                         AND last_verified_at is not None,
                         where "backing copy on P" = asset-scoped Copy(X, pool_id=P)
                                                     OR AssetLocator(X, pool_id=P) via its Copy
            if no qualifying copy on P:               return NOT releasable   # missing/unverified on P
            if P.offsite_gate AND _copy_media_id(qualifying) ∉ offsite_confirmation:
                                                      return NOT releasable   # offsite not confirmed
        # proxies (no offsite_gate pools): a qualifying verified copy per pool is enough — no media

    return releasable                                            # every check passed for every asset
```
Reject is **not** consulted (decision 7). Re-evaluated each `retention run`; a copy going
`MISSING`/`SUSPECT` *after* release is a scrub/self-heal concern (re-make the copy), not un-deletion.

## 6. Tests & acceptance
**Tests** (`tests/test_retention.py`, memory backends + a fake cloud-temp/staging):
- **gate truth table** — (a) written-but-unverified (`last_verified_at` NULL) → NOT releasable;
  (b) verified, offsite-gated, **not** confirmed → NOT releasable; (c) verified + offsite-confirmed →
  releasable; (d) **proxy-only class (no `offsite_gate` placements) → releasable on verification with ZERO
  offsite events** (and **no** `media_id` required on its non-offsite pools — codex nit).
- **bundle/AssetLocator durability (codex High)** — an asset archived **only** via a source-map **bundle
  copy** (`Copy.bundle_id`, asset's presence via `AssetLocator`, no asset-scoped `Copy`) is correctly seen
  as **present + releasable** — the gate must NOT use raw `replication_status`.
- **verified is stamped at archive (codex High)** — after P2.5 bundle build-verify, the bundle `Copy` has
  `last_verified_at` set, so `archive → offsite confirm → retention run` releases (a copy with `health==OK`
  but NULL `last_verified_at` does **not** release).
- **offsite confirm inheritance** — confirming a tape marks **every** copy on it offsite-confirmed; a copy
  on an unconfirmed tape is not.
- **live-arrangement hold + dedup (codex Medium)** — an intake is NOT releasable while it has a **draft**
  (or `pending_derivatives`/`ready`) arrangement, **or** a `submitted` arrangement whose submission is
  `pending_archive` — **even when** the asset hash is already durable via **another** intake (a
  duplicate-content fixture); deleting first would break `submit_arrangement` / `_verify_sources`. It
  releases once every arrangement is `abandoned` or submitted-and-`archived`.
- **submitted fails closed (codex r3 Low)** — a `submitted` arrangement with a **NULL/missing**
  `submission_id` is treated as non-terminal → held (never released on an inconsistent row).
- **prepared-profile hold (codex r3+r4 Medium)** — an intake with `requested_profile` set is **held**
  while any desired derivation/index target (computed from the profile registry) is `open` **or has no
  condition row yet** (a `retention run` *before* the reconciler discovered the targets — fail closed: a
  freshly-prepared intake with **zero** condition rows must hold, not release); it releases only when
  **every** desired target is `satisfied` or `blocked`. Holds **even with no arrangement**.
- **per-pool existential verify (codex r2)** — a recipe pool that has **both** a verified copy **and** an
  older unverified/deleted duplicate locator still counts as satisfied (one qualifying copy is enough);
  release is not blocked by the duplicate.
- **tombstoned copy is globally unusable (codex r2)** — a cloud-temp `Copy` with `deleted_at` set but
  `health==OK` is **not** selected by restore and **not** counted toward the gate's durability.
- **per-intake release** — all assets releasable → cloud object `delete_object`'d + `Copy` **tombstoned**
  (`deleted_at`, row kept) + `retention_state='released'` + grace started + audit rows; **one** asset short
  → **nothing deleted**, stays `held`.
- **delete is idempotent / crash-safe (codex Medium)** — `delete_object` on a missing object is a no-op;
  a "deleted-but-DB-rolled-back" retry re-runs cleanly (delete no-op + DB re-applied); the cloud `Copy`
  row + locator survive as a tombstone for provenance.
- **released freezes new landing work (codex r5)** — after an intake is `released`, `create_from_intake`
  and `prepare_intake` **refuse** (so no new arrangement/prepare can race the `sweep-staging` delete);
  `sweep-staging` then deletes safely without re-checking holds.
- **no-delete-before-gate** — a `held` intake's cloud blob and landing are both intact.
- **staging grace** — `sweep-staging` is a no-op before `released_at + STAGING_GRACE_DAYS`; deletes +
  `purged` + logs after; landing gone, durable copies untouched.
- **reject doesn't block** — a rejected-but-archived asset is still releasable; its intake's staging is
  deletable; the durable copies stand.
- **idempotency** — `retention run` twice (no double cloud-delete); `offsite confirm` twice (no error);
  `sweep-staging` twice (no double landing-delete).
- **audit** — every cloud/staging deletion writes a `retention_event`.
- **existing suites green** — `uv run pytest` (incl. the P2.5 `last_verified_at` stamp not breaking
  archive/restore).

**Acceptance** (plan P3.2): **nothing deletes before the gate**; on offsite-confirm the cloud blob expires
and staging becomes deletable; **proxy-only assets release with no offsite event**; plus a `~/system`
scenario (archive → offsite confirm → retention run deletes the blob + marks staging → sweep-staging after
grace deletes landing; a not-yet-confirmed sibling stays held).

## 7. Scope & cross-slice dependencies
- **Hard dependency — a P2.5 amendment (codex High):** P2.5's `flush_bundle` must **stamp
  `Copy.last_verified_at`** after a successful `_verify_members_from_copy` (the read-back already happens;
  it just isn't recorded). Without it, fresh archives never gate-release. This is a small, self-contained
  change that ships **with** P3.2 (or just before it).
- **New backend port (codex Medium):** `delete_object` on the writable-backend Protocol + S3 (+ d2/rem as
  needed), idempotent not-found = success. Built here.

**Maintenance invariant (two halves):** (a) the decision-8 holds enumerate **every current path that reads
the landing `source_path` after archive** — arrangement submit, submission archive, prepared-profile
derivation/index; **any new landing-reader must add a corresponding hold**. (b) the decision-9 freeze
covers **every operation that *creates* landing-dependent work** — `create_from_intake`, `prepare_intake`;
**any new such creator must refuse on a `released`/`purged` intake**. Together they guarantee the gate
never deletes a source something still needs, and nothing new needs it after release. Restore/scrub/verify
read the *durable* copies, not landing — they are neither holds nor frozen.

**Not here:**
- **No full chain-of-custody lifecycle** (`ejected`/`in_transit`) — deferred; the gate needs only verified
  + offsite-confirmed.
- **Not a reconciler / no autonomous deletion** — operator/cron triggers `retention run` + `sweep-staging`.
- **No un-delete / staging recovery** — once purged, the **durable archive is the recovery**.
- **No copy re-verification scheduling** — "verified" reads existing `last_verified_at`; a freshness policy
  (max-age re-verify before release) is a separate concern.
- **No rem / archive-object / reconciler-spine change.**

## 8. Open decisions
1. **Retention state home** — columns on `Intake` (chosen, 1:1) vs a separate `intake_retention` table.
2. **`STAGING_GRACE_DAYS` default** — a config value; pick a conservative default (e.g. 14–30 days).
3. **"Verified" strictness** — "ever verified" (`last_verified_at is not None`, chosen) vs a max-age
   freshness requirement before release. Lean ever-verified now; freshness is a follow-up.
4. **Cloud-blob delete** — inline in `retention run` (chosen) vs a separate logged job. Lean inline (one
   `delete_object` + tombstone; the *staging* delete is the one that's separated for review).
5. **Cloud-temp tombstone mechanism** — a `Copy.deleted_at` column (chosen) vs a status value vs relying
   solely on `retention_event.detail`. Lean `deleted_at` (queryable; scrub/reconciler skip it).
6. **Barcode → `media_id` resolution** — `offsite confirm --tape <barcode>` needs the tape catalog to map
   a physical barcode to the copy's `tape_uuid`; first slice may accept `--media-id` directly and defer
   barcode resolution to the rem tape catalog.
