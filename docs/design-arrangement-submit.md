# Design — P2.3a: arrangement model + submit + the frozen source-map

> Status: **implemented design** (the maintainer + Claude, 2026-06-26). Implementation item **P2.3a** — the
> first slice of plan item **P2.3** (decomposed: P2.3a model+submit+source-map / P2.3b projection /
> P2.3c watcher). Sources: `design-arrangement-arc.md` §2.4, §3.5–3.9. Depends: P1.1 (registered
> masters), P1.2 (proxies exist — for the *later* projection slice, not this one).
>
> **What this is — the archive-enabling core of arrangement.** The data model for arranging
> registered masters into an **archive namespace**, a CLI/API to arrange them (create / move /
> exclude), and **`submit`** → a **frozen, validated, immutable, ordered source-map**
> (`archive_path ← source_path, sha256, size, ingest_item_id`). The source-map is the **contract
> P2.5 (archive-from-source-map) consumes** to stream original bytes into RAO/d2 under arranged
> names **without copying the 4K staging tree** — the whole architectural point.
>
> **Explicitly NOT in P2.3a** (the other P2.3 slices / later phases):
> - **No projection** (P2.3b) — no server-side proxy tree, no `.sutra/members.json`, no SMB export.
> - **No watcher** (P2.3c) — no live filesystem-op → arrangement reconciliation. Arrange via direct
>   CLI/API; the watcher will be a *client* of this same API later.
> - **No archive-from-source-map** (P2.5) — P2.3a *produces* the source-map; archiving from it needs
>   the Remanence `rem archive build --map` (P2.4) and is P2.5.
> - **No virtual segregation** (P3.1) — post-archive virtual paths/tags.
>
> **Scoping insight: P2.3a is *imperative*, not a reconciler.** Create/move/exclude/submit are direct
> catalog mutations (operator intent applied immediately), not desired-state gaps. The reconciler
> dependency in the plan ("Depends: P0.3 projection reconciler") belongs to **P2.3b** — materializing
> the projection tree is the desired-state domain. P2.3a touches the P0.3 spine **not at all**.

## 1. Ground truth — what exists, what arrangement references
- **The master** is an `IngestItem` (registered by P1.1): `id`, `intake_id`, `logical_asset_hash`
  (= the content sha256), `as_received_path`, `virtual_path`, `size_bytes`, `artifactclass`, and
  `item_metadata["source_path"]` — **the absolute path to the original bytes** in the BagIt landing
  data (a file, or a normalized-package tar if P1.x ran). Live **masters** = `IngestItem JOIN Intake
  WHERE Intake.status = 'registered' AND NOT EXISTS (asset_derivation WHERE derived_item_id =
  IngestItem.id)` — the registered-liveness predicate **and** the master predicate together. The
  `NOT EXISTS` is load-bearing: P1.2's `record_derivation` writes proxies/mezz/preview as `IngestItem`
  rows **in the same `intake_id`**, so registered-liveness alone would wrongly pull derivatives in
  (the full rationale is §3.1).
- **The source-map contract** (arc §3.8/§3.9): the canonical archive interface is
  `ArchiveEntry(source_path, archive_path, sha256, size, ingest_item_id)`, emitted as a TSV under
  `/replica/submissions/<id>/source-map.tsv` (+ `submission.json` + `manifest-sha256.txt`), consumed
  by P2.5 → `rem archive build --map`.
- **Before P2.3a, nothing arrangement-side existed** (this is a pre-implementation design snapshot) —
  no `arrangement`/`arrangement_member`/`submission` tables, no arrangement module/CLI, no scenario.
  (arc §4's `arrangement`/`wakeup` sketches predate the P0.3 single-table + P1.2 code-registry decisions
  and are illustrative only; this doc designed fresh. P2.3a, now committed, builds exactly what follows.)

## 2. The model (four new tables)

```text
arrangement                       # a mutable draft workspace
  id            PK
  label         str
  intake_id     FK intake          # the source intake (single-intake first slice; §8)
  artifactclass str                # the class this arrangement submits under
  status        str                # 'draft' | 'pending_derivatives' | 'ready' | 'submitted' | 'abandoned'
  submission_id FK submission null # set on submit
  created_at / updated_at / submitted_at(null)

arrangement_member                # one arranged entry, mutable until submit
  id              PK
  arrangement_id  FK arrangement (CASCADE), indexed
  ingest_item_id  FK ingest_item    # the MASTER occurrence (not a derivative)
  member_path     str               # the arranged ARCHIVE path (relative, normalized)
  excluded        bool default false # excluded members are not archived
  created_at / updated_at
  # PARTIAL unique index — uniqueness applies ONLY to members that will actually be archived, so
  # excluding foo.mov frees that archive path for another member (sqlite/pg both support this):
  #   CREATE UNIQUE INDEX uq_arrangement_member_path
  #     ON arrangement_member (arrangement_id, member_path) WHERE excluded = false

submission                        # the frozen, IMMUTABLE output of submit
  id              PK  str/uuid     # generated at submit time, BEFORE the file write (§4) — the dir
                                   # name /replica/submissions/<id>/ must be known before the row exists
  arrangement_id  FK arrangement
  artifactclass   str
  source_map_path str               # /replica/submissions/<id>/source-map.tsv
  manifest_digest str               # sha256 of source-map.tsv (the immutable anchor)
  member_count    int
  status          str               # LIFECYCLE: 'pending_archive' | 'archived' (P2.5 flips; P2.3a writes 'pending_archive')
  archived_at     tz datetime null  # LIFECYCLE: set by P2.5 alongside the flip; NULL at submit
  submitted_by    str               # payload (immutable)
  submitted_at    tz datetime       # payload (immutable)
  UNIQUE (arrangement_id)           # DB-enforced one submission per arrangement — the backstop for the §4 race

submission_member                 # the frozen, IMMUTABLE source-map rows (DB-queryable mirror of the TSV)
  id              PK
  submission_id   FK submission (CASCADE), indexed
  ingest_item_id  FK ingest_item null   # SET NULL if the item is later pruned; provenance survives
  archive_path    str
  source_path     str, indexed          # the landing-data path — indexed for the deletion-gate query (§5)
  sha256          bytes
  size_bytes      int
  ord             int                   # archive_path order, frozen
  UNIQUE (submission_id, archive_path)
```

- **`arrangement_member` points at the *master* `IngestItem`** — never a derivative (§3.1). The
  optional `derivative_item_id` the projection slice needs is **not** added here — keep P2.3a's
  schema to what it uses.
- **The submission *payload* is immutable; only its *lifecycle* moves.** Two field classes, made
  explicit so implementers neither freeze the wrong thing nor treat the whole row as mutable:
  - **Immutable payload** (written once at submit, never edited — the frozen contract): `arrangement_id`,
    `artifactclass`, `source_map_path`, `manifest_digest`, `member_count`, `submitted_by`,
    `submitted_at`, **and all `submission_member` rows**. The on-disk `source-map.tsv` mirrors these and
    is byte-frozen.
  - **Mutable lifecycle**: `status` (`pending_archive` → `archived`) **and** an `archived_at`(null) set
    alongside the flip — these are the *only* mutations P2.5 makes; they describe progress, not the
    contract. P2.3a writes `status='pending_archive'`, `archived_at=NULL`, and nothing later touches a
    payload field.
- **TSV and rows are both rendered from one validated in-memory entry list.** Submit validates the
  members once into an ordered list of frozen entries, then renders **both** the `source-map.tsv` (the
  §3.9 archive export P2.5 / `rem --map` reads) **and** the `submission_member` rows from that same
  list — they cannot diverge. The TSV is written **first** (file-first, §4); the rows are inserted
  after, so the doc must **not** say "the TSV is generated from the rows" (the rows don't exist yet).
  Once committed, the `submission_member` rows are the **authoritative DB-queryable mirror**. (Review
  point — this reverses an earlier "TSV-only" call: the deletion gate, §5, must answer *"which
  unarchived submissions reference this landing `source_path`?"* efficiently, which a TSV-only design
  can't. `submission_member.source_path` (indexed) + `submission.status` make it a plain query.)

## 3. The arrange operations (CLI/API — the surface the watcher will later call)
P2.3a ships the **imperative** API + CLI; P2.3c's watcher becomes a client of exactly these:
- **`sutra arrangement create --from-intake <id> --label <l>`** → a `draft` arrangement + one
  `arrangement_member` per live **master** `IngestItem` of the intake, `member_path` defaulting to its
  `as_received_path`. **§3.1 — "master" must be an explicit predicate.** P1.2's `record_derivation`
  creates the proxy/mezz/preview as `IngestItem` rows **in the same `intake_id`** (the derived items
  share the intake), so "one member per `IngestItem` of the intake" would wrongly pull derivatives
  into the arrangement. A **master is an `IngestItem` that is *not* the derived side of any derivation
  edge**:
  ```sql
  WHERE IngestItem.intake_id = :intake AND NOT EXISTS (
          SELECT 1 FROM asset_derivation d WHERE d.derived_item_id = IngestItem.id)
  ```
  (`arrangement_member.ingest_item_id` is validated against the same predicate on add.) If the intake's
  review derivatives aren't ready, the workspace may sit `pending_derivatives` — but P2.3a does not
  *require* them (they matter for projection, P2.3b); a master-only arrangement is valid.
- **`sutra arrangement create --from-arrangement <ws> --label <l>`** → clone every (non-excluded)
  member of an existing arrangement into a fresh `draft` (same `ingest_item_id`/`member_path`). This is
  the **revise path** for an already-`submitted` arrangement (which is frozen — §4): clone, edit the
  clone, submit it → a new, independent submission. A thin variant of `--from-intake`.
- **`sutra arrangement mv <ws> <from-path> <to-path>`** → update `arrangement_member.member_path`
  (rename/move within the archive namespace). **Touches the arrangement row only — never the
  `IngestItem`, never the BagIt landing data, never `as_received_path`.** This is the P2.3a analogue
  of the plan's "moving a projected proxy updates the member path, not BagIt." Rejected on a
  `submitted` (frozen) arrangement.
- **`sutra arrangement exclude <ws> <path>`** → mark a member `excluded` (kept for provenance, not
  archived).
- **`sutra arrangement submit <ws>`** → §4.
- **`sutra arrangement list` / `show <ws>`** — inspection.

## 4. `submit` — validate, freeze, emit the source-map
**Validate** (arc §3.8), refuse on any failure (the draft stays mutable):
- every non-excluded member resolves to a **live, registered master** `IngestItem`
  (`Intake.status='registered'` **and** not the derived side of any `asset_derivation` edge — the §1/§3.1
  master predicate);
- **no duplicate `archive_path`** across **non-excluded** members (the partial-unique index guards the
  live set + an explicit check over the same set — excluded members never collide, since they aren't
  archived);
- every `member_path` is **relative and normalized** (no `..`, no leading `/`, NFC, forward slashes) and
  contains **no control characters** — *reject* any `member_path` (or resolved `source_path`) holding a
  tab, newline, CR, or other C0/C1 control char, so the TSV is unambiguous (a normalized relative path
  alone does not rule out tabs/newlines). These rules **are** the path policy for P2.3a — there is no
  separate named path-policy object; the existing `member_name` escaping covers the deeper non-UTF-8
  case, and the TSV control-char *rejection* is the simple guarantee here;
- the arrangement's `artifactclass` is compatible with all members (they are masters, not
  derivatives);
- **source bytes still match** — for each member, the master's `item_metadata["source_path"]` exists
  and (if online) re-hashes to `logical_asset_hash` (the same "don't trust, re-verify" discipline as
  P1.1 register). A missing/changed source fails submit.

**Freeze → source-map.** For each non-excluded member, in **`archive_path` lexical order**
(deterministic ⇒ the "immutable, *ordered*" criterion), emit:

```
ArchiveEntry(
  archive_path   = member.member_path,
  source_path    = ingest_item.item_metadata["source_path"],
  sha256         = ingest_item.logical_asset_hash,
  size           = ingest_item.size_bytes,
  ingest_item_id = ingest_item.id,
)
```

**Submit is terminal for the arrangement (review point — resolve the re-submit ambiguity).** Submit
sets `arrangement.status='submitted'` and `submission_id` (singular — **one submission per
arrangement**); a `submitted` arrangement is **frozen** (no further `mv`/`exclude`/re-submit). To
*revise*, **create a new draft** (`create --from-arrangement <ws>` clones the members into a fresh
`draft`; clone is the reopen path, a thin add) and submit *that* → its own submission. The original
submission and its source-map stand untouched. This keeps `submission_id` singular and `submitted`
terminal, instead of one arrangement accreting many submissions.

**DB ↔ filesystem ordering (review point — there is no cross-DB-FS atomic commit).** A caller-owned
DB transaction cannot atomically include filesystem writes, so submit uses an explicit **file-first,
then DB** order with the P2.1 durable-write helper:
1. **Generate the submission `id`** (a submit-time UUID, *not* an autoincrement PK — the directory name
   depends on it and must exist before the DB insert), then build the `submission_member` rows + the
   `source-map.tsv` / `submission.json` content in memory.
2. **Durably write the files** into `/replica/submissions/<id>/`. **Contract: `submission_root`
   (`/replica/submissions/`) must already exist** — a deployment-provisioned durable directory; submit
   creates **only** the `<id>/` subdir, then **fsyncs `submission_root`** so the new directory entry is
   durably linked. (This keeps the fsync requirement to a single, well-defined parent. Auto-creating an
   arbitrary multi-level root chain — the helper's `mkdir(parents=True)` convenience for tests — does
   *not* durably fsync each new ancestor and is not the production contract.) The parent fsync is
   **required, not optional**: without it a committed `submission` row could reference a dir that didn't
   survive a power loss (the §4 invariant below would break). Then write each file via
   `atomic_write_verified_file` (temp → fsync file → `os.replace` → fsync the file's dir), so each file
   is complete-or-absent, never partial.
3. **Then the DB, in this order** (the order matters — see Concurrency): (a) the **guarded status flip**
   `UPDATE arrangement SET status='submitted' WHERE id=:aid AND status <> 'submitted'` —
   **rowcount 0 ⇒ a concurrent submit already won ⇒ abort**; (b) insert the immutable `submission` +
   `submission_member` rows (`manifest_digest` = the TSV digest, `status='pending_archive'`); (c)
   `UPDATE arrangement SET submission_id=:id`. The flip is **first** so the concurrent
   loser is rejected before inserting rows; `submission_id` is set **last** because its FK requires the
   `submission` row to exist. The **caller commits** (no-commit discipline).
The only failure residue is an **orphan submission dir** (files written in step 2, then the DB rolled
back) — harmless (no `submission` row references it) and reclaimed by a **bounded orphan sweep**
(`/replica/submissions/<id>/` with no `submission` row, older than a threshold). A `submission` row
can never reference a missing/partial file, because the files are durable before the row exists.

**Concurrency — two submits of the same draft must not both win (review point).** "One submission per
arrangement" needs DB enforcement, not just app logic: two callers could each validate the draft, each
write a dir, and each try to insert. Three layers, defence-in-depth:
1. **Lock the arrangement row first** — `SELECT … FOR UPDATE` on the `arrangement` at the *start* of
   submit, **before** validation and the step-2 file write, and re-check `status`. **On Postgres** the
   loser blocks until the winner commits, then sees `status='submitted'` and **rejects before writing
   any files** — so the common race produces *no orphan*. **On SQLite** `SELECT … FOR UPDATE` is a
   no-op: both submitters can validate and write their dirs before one loses at the step-3 insert/flip,
   so the SQLite loser **may leave an orphan dir**. That is **safe** — DB correctness still holds via
   layers 2–3 below, and the orphan (no `submission` row) is swept. So: row-lock is a Postgres
   optimization to avoid the orphan, not a correctness requirement; correctness comes from layers 2–3.
2. **Status-guarded flip, *before* the `submission` insert** — `UPDATE arrangement SET status='submitted'
   WHERE id=:aid AND status <> 'submitted'`; **0 rows affected ⇒ abort + roll back** (its dir, if any,
   becomes a swept orphan). This is the **live primary** concurrent-loser guard (and where the row lock,
   when available, makes the loser block then lose). **It must run before the `submission` insert** — if
   the insert came first, the loser would trip `UNIQUE(arrangement_id)` (layer 3) at the insert flush and
   *never reach* this rowcount-0 branch, leaving the branch dead. Order: guarded flip → insert → set
   `submission_id`.
3. **`UNIQUE(submission.arrangement_id)`** — the backstop: any loser that somehow passed layer 2 trips an
   integrity error at the `submission` insert → roll back. No path lets two `submission` rows share an
   arrangement. (With the layer-2-first order this is genuine defence-in-depth, not the only guard.)

## 5. The source-map is the P2.5 contract (and a lifecycle note)
P2.5 (archive-from-source-map, gated on P2.4 `rem archive build --map`) consumes the TSV: open each
`source_path`, stream into the RAO/d2 entry named `archive_path`, verify `sha256` while streaming,
record `Copy` + locator rows — **no second copy of the 4K masters** (§3.9). P2.3a's only job is to
produce that contract correctly and immutably.

**Lifecycle note (flag for the deletion gate, P3.2):** the source-map references the **BagIt landing
data** (`source_path`), which is *staging*, not the durable copy. So **landing data for a submitted-
but-not-yet-archived arrangement must not be deleted** — the deletion gate (U) must treat "referenced
by an unarchived submission" as a hold. P2.3a makes that dependency **DB-queryable**: the gate joins
`submission_member.source_path` against `submission.status='pending_archive'` (this is *why*
`submission_member` exists and its `source_path` is indexed — §2). Enforcing the hold is P3.2's
concern, noted here so it isn't lost.

## 6. Tests & acceptance

**Tests** (`tests/test_arrangement.py`):
- **create-from-intake selects masters only** — with an intake whose review **proxies are prepared**
  (P1.2 derived `IngestItem`s in the same `intake_id`), `create --from-intake` makes a `draft` with one
  member per live **master** and **no derivative members** (the `NOT EXISTS asset_derivation` predicate,
  §3.1); `member_path == as_received_path`; a non-registered intake is rejected.
- **mv touches only the arrangement** — `mv` updates `arrangement_member.member_path`; the
  `IngestItem` (`as_received_path`, `virtual_path`) and the BagIt landing data are **unchanged**.
- **exclude frees the archive path** — an excluded member is omitted from the source-map; **and** after
  excluding `foo.mov`, another member can be `mv`'d to `foo.mov` (the partial-unique index ignores
  excluded rows), and submit accepts it.
- **submit emits a correct source-map** — `archive_path`/`source_path`/`sha256`/`size`/`ingest_item_id`
  match the members (`source_path` from `item_metadata`, `sha256 == logical_asset_hash`); rows are in
  `archive_path` order; `manifest-sha256.txt` matches the TSV; the `submission_member` rows mirror the
  TSV (same order/digest).
- **submit validation fails closed** — duplicate `archive_path` → reject; a `..`/absolute/non-NFC
  `member_path` → reject; a `member_path` containing a **tab/newline/CR/control char** → reject; a
  member whose `source_path` is missing or whose bytes no longer hash to `logical_asset_hash` → reject;
  the draft stays mutable (no partial submission written, no `submission` row).
- **submit is terminal; revise via clone** — after submit, `arrangement.status='submitted'` and the
  arrangement is frozen (`mv`/`exclude`/`submit` all reject); `create --from-arrangement` clones a fresh
  `draft`, and submitting *that* yields a **second, independent** submission; the first `submission` +
  `source-map.tsv` are byte-identical to before (immutability).
- **durable-write ordering** — files (`source-map.tsv`/`submission.json`/`manifest-sha256.txt`) are
  written via `atomic_write_verified_file` **before** the `submission` row; a forced DB-rollback after
  the file write leaves an **orphan dir** with **no** `submission` row (harmless, sweep-reclaimable),
  and **never** a `submission` row pointing at a missing/partial file.
- **one submission per arrangement** — three *distinct* failure paths, each its own test (they reject at
  different stages — don't fold them into one bullet):
  - **terminal-status reject** (the common case): a second `submit` of an already-`submitted` arrangement
    is refused at the **up-front status check** (`ArrangementFrozen`), before any file is written — no
    orphan. This is *not* the rowcount-0 path.
  - **`UNIQUE(arrangement_id)` backstop**: a direct second `submission` insert sharing `arrangement_id`
    raises an integrity error. Exercises the DB constraint in isolation.
  - **status-guarded flip, rowcount 0** (`ArrangementSubmitRace`): the genuinely concurrent path — two
    interleaved sessions both pass the initial status check, then the *loser's* guarded
    `UPDATE … WHERE status <> 'submitted'` affects **0 rows** → abort. Reachable only because the flip
    runs **before** the `submission` insert (§4); exercised by an explicit two-session test that injects
    a winning submit mid-loser via the `_write_submission_files` seam (the single-process happy path
    never reaches it). The loser leaves at most a swept orphan dir, never a committed row.
- **migration** — new revision chained from the **current** alembic head (P1.1's
  `6a0f4c2e9d1b` — P1.2/P2.1/P2.2 added none); `create_all` builds the **four** tables (`arrangement`,
  `arrangement_member`, `submission`, `submission_member`).
- **existing suites green** — `uv run pytest`.

**Acceptance** (plan P2.3, the P2.3a half): an `arrangement mv` updates the member path and **never
the `IngestItem`/BagIt**; **`submit` produces an immutable, ordered, validated source-map**
(`archive_path ← source_path, sha256, size, ingest_item_id`) under `/replica/submissions/<id>/`;
`uv run pytest` + format + type-check green; a `~/system` arrangement scenario (register → arrange →
submit → inspect the source-map) green. (The projected-proxy-move and the live watcher are P2.3b/c.)

## 7. Scope (not built here)
- **No projection / SMB / proxy tree / `.sutra/members.json`** — P2.3b.
- **No watcher / live FS-op reconcile** — P2.3c (a client of §3's API).
- **No archive-from-source-map** — P2.5 (needs P2.4 `rem --map`, Remanence).
- **No VS / virtual paths** — P3.1; arrangement is the *pre-archive* namespace.
- **No `derivative_item_id` on members, no condition/reconciler rows** — P2.3a is imperative; the
  projection desired-state is P2.3b.

## 8. Open decisions
1. **Single- vs multi-intake arrangements** — P2.3a scopes to one source intake (`--from-intake`). A
   cross-intake arrangement (combine cards into one program) is a natural extension; add when a real
   case appears (the model already keys members by `ingest_item_id`, so multi-intake is mostly
   relaxing `create`).
2. **Orphan-dir sweep ownership** — submit's file-first ordering can leave an orphan
   `/replica/submissions/<id>/` if the DB rolls back (§4). P2.3a defines it as harmless and
   sweep-reclaimable but does **not** build the sweep; fold it into the existing GC/janitor pass (or
   P3.2's deletion gate) rather than adding a one-off here.
3. **`member_path` default** — `as_received_path` chosen (the as-received structure is the starting
   point). `virtual_path` is identical at register, so this only matters if VS edits land pre-submit
   (they don't in this slice).
4. **Submission storage root** — `/replica/submissions/<id>/` per arc §3.8; confirm against the
   deployment's path conventions (mirrors `/replica/landing`, `/replica/arrangements`).
