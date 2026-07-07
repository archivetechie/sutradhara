# Design — P3.1: Virtual arrangement (post-archive organize-forever)

> Design by Claude + the maintainer (2026-06-27), for review then implementation. **Repo: sutradhara (+ a
> `~/system` scenario).** Plan item **P3.1** (`docs/implementation-plan-ingest-v2.md`), arc §3.10/§4.8/§2.4.
> Depends: **P2.2** (member-aware restore) + archive done (**P2.5**). Renames the plan's "virtual
> segregation (VS)" — *segregation* was internal jargon; this is the **virtual mirror of `arrangement`**.

## 0. What this is
After content is archived (immutable on tape), operators keep organizing it — **forever** — into
**virtual arrangements**: named, permanently-mutable views over archived **assets**. `mv` / `tag` /
`reject` are **catalog-only** — they never touch BagIt landing data and never rewrite RAO/d2 objects.
Restore resolves *a virtual path → logical asset → AssetLocator → tape/cloud*, reusing **P2.2
member-aware restore** for the bytes. Accept (plan P3.1): `mv`/`tag add` edit the catalog only; restore
by virtual path returns the original bytes; **reject hides without deleting**.

## 1. The identity grain — content-level (`LogicalAsset`), decided
**A virtual arrangement organizes *content*, not occurrences.** The catalog is content-addressed: a
`LogicalAsset` (= `content_sha256`) is stored **once**, even if it arrived as several `IngestItem`
occurrences (the same master on two cards). Restore is already content-level — `restore_asset(asset_hash,
…)` picks locators **by hash** — and the bytes are deduped, so there's nothing to organize or restore
*differently* between two identical-byte occurrences. So **members key on the content** — specifically
the **archived asset = `(logical_asset_hash, artifactclass)`** (the unit `restore_asset` actually
restores; the same hash can be archived under more than one class, §4) — and this is what makes the
model coherent:
- **Restore aligns** — a content-keyed member resolves directly into `restore_asset`; no item context.
- **Reject is coherent** — rejecting the *content* gates restore-by-hash **globally** (the restore path
  has the hash); an occurrence-level reject couldn't gate bytes shared with another occurrence.
- **"Archived" is unambiguous** — an asset is organizable iff its `LogicalAsset` has a healthy
  `AssetLocator`; no "which occurrence archived it" question (it's one asset, archived once).

It still **mirrors `arrangement` structurally** (a named workspace of members at paths); the grain just
shifts occurrence→content because **archive dedups** — pre-archive you arrange distinct *source files*
(occurrences), post-archive you organize the single deduped *asset*. Provenance (which intake/card) is
**not lost** — it stays in `IngestItem`, queryable by `logical_asset_hash`; it's just not the
organizational grain.

## 2. The mirror of `arrangement` — and where it diverges
| | `arrangement` (P2.3a, pre-archive) | **virtual arrangement** (P3.1, post-archive) |
|---|---|---|
| Unit | workspace + `arrangement_member` (→ master `IngestItem`) | view + `virtual_arrangement_member` (→ `LogicalAsset`) |
| Grain | **occurrence** (a source file) | **content** (a deduped asset) |
| Lifecycle | mutable → **submit freezes** it → archived | **mutable forever** — never submits, never freezes |
| Scope | one intake's masters | **any archived assets, cross-intake** |
| Coexist | one arrangement per submission | **many views at once** (by-program, by-event, by-speaker) |
| Bytes | source-map drives the archive write | **never touches bytes** — restore reads the frozen locator |

## 3. The model (multi-view, content-keyed)
```text
virtual_arrangement                # a named, permanent organizational view (= a "namespace")
  id            PK
  name          str  UNIQUE        # "programs", "by-speaker", "mahashivratri-2024"
  description   str  null
  created_by / created_at / updated_at

virtual_arrangement_member         # one archived ASSET placed at a path within one view
  id                  PK
  va_id               FK virtual_arrangement (CASCADE), indexed
  logical_asset_hash  FK logical_asset, indexed     # the CONTENT (deduped), not an occurrence
  artifactclass       str               # WHICH archived form — the restore unit is (hash, artifactclass);
                                         #   the same hash can have locators via bundles of several classes,
                                         #   and restore_asset needs the class up front to order pools (§4)
  path                str               # virtual path within THIS view (relative, normalized NFC), width 2048
  excluded            bool default false # per-view hide — a REVERSIBLE toggle (un-exclude to re-show)
  added_by / added_at / updated_at
  UNIQUE (va_id, logical_asset_hash, artifactclass)   # one placement per archived-asset per view (exclude toggles)
  # PARTIAL unique (va_id, path) WHERE excluded = false    # no two live members share a path (as P2.3a)

virtual_arrangement_history        # append-only audit of path moves within a view
  id, va_id, va_member_id FK virtual_arrangement_member (SET NULL), logical_asset_hash, artifactclass,
  old_path, new_path, actor, changed_at
  # va_member_id = precise live linkage to the moved member; (logical_asset_hash, artifactclass) are
  # denormalized so the audit row **self-identifies** the full member identity even after the member is
  # later removed — and disambiguates a hash placed under two classes in one view (codex r3).

asset_tag                          # CROSS-CUTTING governance tags on CONTENT, soft-deleted (audit-preserving)
  id                  PK
  logical_asset_hash  FK logical_asset, indexed
  tag                 str
  added_by / added_at
  removed_by / removed_at  null     # soft-delete tombstone — keeps the governance trail (no hard delete)
  # PARTIAL unique (logical_asset_hash, tag) WHERE removed_at IS NULL   # one ACTIVE instance; re-add allowed
```
**Reject (content-level) on `LogicalAsset`** — nullable columns, present ⇒ rejected:
```text
logical_asset (existing) +=
  rejected_at      tz datetime null   # rejected = hidden from default listings + restore-gated, NEVER deleted
  rejected_by      str null
  rejection_reason str null
```
- **`IngestItem.virtual_path` (the as-received seed) is left untouched** — virtual arrangements are a new
  layer on top (§8 decides its long-term fate).
- Reject is **content-level** (the asset's bytes are unwanted) and distinct from per-view `excluded` (just
  not in *this* view). It gates **extraction**, not **preservation** (§4).

## 4. Operations & restore
**Operations** (imperative, catalog-only; CLI group `sutra virtual`, name open §8):
- **`virtual create <name> [--description]`** → a new view.
- **`virtual add <name> <asset-hash> <path> [--artifactclass <c>]`** → place an **archived** asset (must
  have a healthy `AssetLocator`) in the view, recording its `artifactclass`. The class is **resolved at
  add-time**: if the hash is archived under exactly one class, use it; if under several, **require
  `--artifactclass`** (no arbitrary pick). Reject the add if the asset isn't archived.
- **`virtual mv <name> <from-path> <to-path>`** → move within the view; writes a history row. Touches
  **only** the member row.
- **`virtual exclude <name> <path>` / `virtual include <name> <path>`** → reversible per-view hide.
- **`reject <asset-hash> [--reason]` / `unreject <asset-hash>`** → the content-level reject marker.
- **`tag add|rm <tag> <asset-hash>`** → cross-cutting tags (soft-delete; governance only, no access
  enforcement — that waits for the identity model, arc §4.8).
- **`virtual ls <name>` / `show`** — `excluded`/rejected hidden by default (`--all` shows them).
- **`virtual restore <name> <path> --dest <path> [--force] [--force-rejected]`** → below.

**Restore-by-virtual-path (reuses P2.2, content-level).** Resolve `(view, path)` →
`virtual_arrangement_member` → `(logical_asset_hash, artifactclass)` → **`restore_asset(hash,
artifactclass, …)`** (P2.2) writes the bytes. The artifactclass comes **straight off the member** (stored
at add-time, §3) — *not* derived from "the bundle," which isn't singular when a hash is archived under
several classes (codex). The virtual path is **decoupled** from the archived `member_path`: restore uses
the frozen locator's archived name, so an asset organized as `/programs/.../opening.MOV` still restores
from RAO member `satsang/day-1/A001.MOV` with `sha256 == master`.

**The reject gate is global and lives in `restore_asset`, separate from the existing suspect gate.**
`restore_asset` today already refuses **suspect** (validity-flagged) assets without `--force`; P3.1 adds a
**second, independent gate**: a **rejected `LogicalAsset` refuses restore** unless explicitly overridden.
To keep the two intents distinct (and not silently change `--force`'s existing meaning), they are
**separate flags**: `--force` bypasses the **suspect** gate (unchanged); **`--force-rejected`** bypasses
the **reject** gate; an asset that is both needs both. Both gates apply to `sutra archive restore` *and*
`sutra virtual restore`. The reject gate covers **extraction only** — preservation (the copy reconciler,
scrub, self-heal, which use `restore_copy`) keeps maintaining a rejected asset's copies. Reject **never
deletes**; the bytes are always recoverable with `--force-rejected`; tags never affect restore.

## 5. Reuse vs. new code
**Reused:** `restore_asset` (P2.2 — the bytes; + a new independent reject gate & `--force-rejected`, the
existing suspect `--force` untouched), arrangement's path
normalization (`canonical_member_path`), `AssetLocator`/`Copy`/`LogicalAsset` resolution, the catalog
session discipline. **New:** the four tables + `LogicalAsset` reject columns + one Alembic migration
(chained from P2.5's head); the `virtual_arrangement` module + `sutra virtual`/`tag`/`reject` CLI; the
`(view, path) → logical_asset_hash → artifactclass` resolver. **Unchanged / not touched:** archived
RAO/d2 objects, BagIt landing data, the copy reconciler, rem. **No reconciler** (imperative, like
arrangement) and **no submit/freeze** (mutable forever).

## 6. Tests & acceptance
**Tests** (`tests/test_virtual_arrangement.py`):
- **mv edits catalog only** — `virtual mv` updates the member `path` + writes a history row; the
  `LogicalAsset`, every `IngestItem`, landing data, and the RAO object are **unchanged**.
- **multi-view** — the same asset placed in **two** views at **different** paths; both resolve
  independently; per-view partial-unique frees a path on `exclude`; `include` re-shows it (reversible).
- **content grain: dedup** — two `IngestItem` occurrences sharing one `logical_asset_hash` map to **one**
  asset; `virtual add` of the hash yields a single member; restore returns the (shared) bytes.
- **restore-by-vpath = original bytes** — restore via `(view, path)` returns `sha256 == master`,
  resolving through the **frozen AssetLocator** regardless of how the virtual path was renamed.
- **reject gates extraction globally, never deletes** — `reject` drops the asset from default `ls`, and
  **both** `archive restore` and `virtual restore` **refuse without `--force-rejected`**;
  `Copy`/`AssetLocator`/landing are intact; `--force-rejected` still yields the original bytes;
  preservation (`restore_copy`/self-heal) is unaffected; `unreject` restores visibility.
- **suspect vs. reject force are distinct** — `--force` bypasses the suspect gate but **not** reject;
  `--force-rejected` bypasses reject but **not** suspect; a both-flagged asset needs both (existing
  suspect behavior unchanged).
- **multi-class restore unit** — a hash archived under two classes: `virtual add` requires
  `--artifactclass`, the member records it, and restore uses **that class's** locators (not an arbitrary
  one); single-class adds need no flag.
- **one path per archived-asset per view** — adding the same `(hash, artifactclass)` twice in one view is
  refused (`UNIQUE`); the asset can still live in another view at another path.
- **per-view exclude vs. global reject** — `exclude` hides only in that view (asset still active
  elsewhere); `reject` hides the asset across all views and gates restore.
- **tags soft-delete** — `tag add`/`rm`; `rm` tombstones (`removed_at`) rather than hard-deleting (audit
  preserved); partial-unique allows re-add; governance-only (no restore effect).
- **history carries full member identity** — every `mv` appends `old→new` + actor + time **+ the moved
  member's `va_member_id`, `logical_asset_hash`, and `artifactclass`**; a hash placed under two classes in
  one view yields unambiguous per-class move audit; rows are never mutated.
- **add requires archived** — `virtual add` of an asset with no healthy `AssetLocator` is refused.
- **migration + schema parity** — four tables + `LogicalAsset` reject columns; `create_all` and `alembic
  upgrade head` agree; chains from the P2.5 head.
- **existing suites green** — `uv run pytest` (incl. the new reject gate not breaking `archive restore`).

**Acceptance** (plan P3.1): `virtual mv`/`tag add` edit the catalog only; restore by virtual path returns
the original bytes; reject hides without deleting; plus a `~/system` scenario (archive → organize one
asset into two views → restore by virtual path → `sha256 == original`; reject → restore-gated globally).

## 7. Scope (not here)
- **No access/approval enforcement** — tags + reject are governance subjects only; the identity model is
  later (arc §4.8).
- **No projection / SMB** — the browsable tree is the **arrangement gateway** project, not core.
- **No submit/freeze** — virtual arrangements are mutable forever by design.
- **No auto-seeded default view** — operator-created views first; auto-seeding a view from archived paths
  is a convenience deferred to a follow-up (§8).
- **No cross-view set algebra / saved queries** — deferred.
- **No rem / archive-object change** — VS never rewrites objects.

## 8. Open decisions
1. **Reject storage — `LogicalAsset` columns (chosen) vs. a marker table.** Columns
   (`rejected_at`/`rejected_by`/`rejection_reason`) are simplest + queryable; a small
   `logical_asset_rejection` table would add reject/un-reject **history**. Lean: columns now; table if
   reject history becomes a requirement.
2. **CLI command name** — `sutra virtual …` (working name) vs. `sutra va …` vs.
   `sutra virtual-arrangement …`. Lean `sutra virtual`; `tag`/`reject` are top-level (content-level).
3. **Artifactclass for restore** — **RESOLVED (codex r2): stored on the member** at add-time. "Derive
   from the bundle" was ambiguous — a hash can be archived under several classes, and `restore_asset`
   needs the class up front to order pools. The restore unit is `(hash, artifactclass)` (§3); add-time
   requires `--artifactclass` only when the hash is multi-class. (Alternative was scoping the whole *view*
   to one class — rejected: it would fragment organization across classes; member-scoped keeps mixed-class
   views possible.)
4. **`--force` split — RESOLVED (codex r2): two flags.** `--force` keeps its existing meaning (bypass the
   **suspect** gate); the new reject gate is bypassed by **`--force-rejected`**, so a suspect-force never
   silently un-hides a rejected asset (§4).
5. **One-path-per-archived-asset-per-view — intentional.** `UNIQUE(va_id, logical_asset_hash,
   artifactclass)` forbids aliasing the *same* archived asset to two paths *within one view* — cross-cutting
   placement is what *multiple views* are for. If a real need for in-view aliases appears, loosen to allow
   multiple member rows per asset; deferred.
6. **`IngestItem.virtual_path` fate** — keep as the as-received seed (chosen) vs. deprecate. Lean keep.
7. **Auto-seed a "default" view** — auto-create one view from each asset's archived path on archive, vs.
   operator-created views only. Lean operator-created first.
