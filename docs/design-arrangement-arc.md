# Design — Ingest arrangement: Send (commit-to-tape) and Arrangement (browse & governance)

> Status: **design, for review** (2026-06-18).
> **Reviewer note:** this document is self-contained. §0 gives the background an
> outside reviewer needs; later sections assume only that. Internally this spans
> two build phases — **Send** (a.k.a. Phase S) and the **Arrangement** layer
> (formerly "virtual segregation") — designed together because they share one
> data spine, but shipped separately.

---

## 0. Context for the reader

### 0.1 What the system is
**sutradhara** is the orchestration layer of a **long-horizon digital preservation
archive** (design horizon: decades). It ingests media (mostly video) and arbitrary
files from cameras, drives, and handoffs, verifies them, and writes them to
**LTO tape** (several independent copies) plus a temporary **cloud** disaster-recovery
copy. It sits above a Rust tape stack (drive/library control + an on-tape object
format) and a legacy tape writer, and below the **operator** (the person doing
ingest) and their desktop **finder tool** (a client app for browsing/organizing —
the GUI is a separate, deferred piece).

Two properties drive every decision below:
- **Tape is effectively write-once.** Once bytes are sealed onto tape you do not
  rewrite the tape to fix a mistake. Sealed layout is **immutable**.
- **Archive-everything.** Nothing is ever culled. Damaged or mis-decoded files are
  still preserved; we *flag* them and *gate access*, never refuse preservation.

### 0.2 The catalog (content-addressed)
State lives in a SQLite catalog. Two tables matter here:
- **`LogicalAsset`** — one row per *unique byte sequence*; its primary key **is** the
  SHA-256 of the bytes. Identical bytes from two sources are one `LogicalAsset`
  (full deduplication). It carries no per-location facts.
- **`ingest_item`** — one row per *received occurrence* of a file (FK → `LogicalAsset`
  by hash, FK → the `intake` batch it arrived in). The same bytes can occur in two
  different intakes at two different paths → **two `ingest_item`s, one
  `LogicalAsset`**. The `ingest_item` carries the *mutable, per-occurrence* facts:
  `as_received_path`, **`virtual_path`** (default = `as_received_path`),
  `(st_dev, st_ino)` (filesystem device + inode), `size_bytes`, and `artifactclass`.

**This per-occurrence/per-content split is load-bearing for this design:** arrangement
is a property of an *occurrence*, so it lives on `ingest_item`, never on
`LogicalAsset`.

### 0.3 The ingest flow, and where this design sits
```
[client]  sutra receive ──►  [staging share]  ──►  sutra intake scan  ──►  (jobs)
  reads a card/drive/         a BagIt "bag":        validates the bag,        proxies +
  folder, hashes on read,     data/ + checksum      registers LogicalAsset    cloud blob
  writes a verified bag       manifest + sentinel    + ingest_item per file

         ════════ DONE (implemented & tested) ════════
                                  │
   ┌──────────────────────────────┴───────────────────────────────┐
   │                          THIS DESIGN                           │
   │  SEND  (commit to tape)              ARRANGEMENT (browse/gov)  │
   │  rough-seg → artifacts → bundles      virtual_path tree + tags │
   └────────────────────────────────────────────────────────────────┘
```
- **`sutra receive`** (client) copies a source tree to the staging share, hashing on
  read, and writes a **bag** (a checksum-manifested directory in the BagIt/RFC-8493
  format). *Implemented.*
- **`sutra intake scan`** (server, runs continuously) detects completed bags,
  re-verifies them, **registers** each file as a `LogicalAsset` + `ingest_item`, and
  enqueues background **jobs**: transcode (proxies), a partial-restore index, and an
  encrypted **cloud blob** (a DR copy of the whole intake). *Implemented.* Within
  hours the content is safe (2 verified copies) and browsable (proxies).
- **Background jobs** run under a single-node **lease worker** (a bounded concurrent
  job runner). The **bundling/fan-out machinery** — packing same-class files into a
  tar, sealing it as one tape object, and writing several copies per policy — also
  **exists and is tested**. What does **not** exist is the operator step that decides
  *which files form a tape unit and when to write them*, and the layer that lets a
  human *organize and govern* the registered content. **That is this design.**

### 0.4 Key concepts used below (glossary)
- **artifactclass** — the *archival class* of a file (e.g. `masters`, `mezz`,
  `preview`), set at intake. It drives **placement policy**: how many tape/cloud
  copies, which storage pools, and whether they're encrypted. It is a *storage*
  grouping only.
- **proxy** — a derived, lightweight rendition of a master video: a *mezzanine* (edit
  quality) and a *preview* (low-res). Browsable immediately; linked to the master by a
  derivation edge.
- **artifact** *(new in this design)* — the operator's grouping of files **at send**:
  one folder of one artifactclass, the unit they commit to the archive together.
- **bundle** — the *storage* packing unit: a tar of same-class files sealed as one
  tape object. The bundling machinery packs by size; an artifact maps to one bundle
  by default (see §4.4).
- **seal / fan-out** — writing a bundle to tape as one object and replicating it to
  the several copies its policy requires. After seal, the bundle's layout is frozen.
- **PFR locator** — "partial file restore" coordinates for a file within a bundle:
  `{bundle, offset, length, sha256}`. **Restore reads these** — it never needs the
  file's path or arrangement.
- **`virtual_path`** — the file's location in a *logical browse tree*, independent of
  where its bytes physically are. Editable forever.
- **tag** — a free-form label on an occurrence; the *subject* future access/approval
  policies attach to.
- **staging** — disk on the server where verified originals live until they're sealed
  to tape and the deletion gate passes.
- **finder tool** — the operator's client app (browse + organize). A separate,
  partly-deferred component; this design specifies the **API surface it calls**, not
  the tool itself.

---

## 1. Problem & goal

After intake, content is preserved and browsable, but organized exactly as the
camera dumped it (`A001/C001.mov`). Two operator activities remain, and **neither
exists yet**:

1. **Send (commit to tape).** Decide which files form a tape unit and write them. The
   bundling *mechanism* exists; the *trigger* (turn a chosen folder of files into
   artifacts and feed the bundler) does not.
2. **Arrangement (browse & governance).** Give the content human meaning — a navigable
   tree and governance tags — **without moving archived bytes**.

**Goal:** specify both, plus **how the operator's actions reach the system** (the
trigger surface the finder tool drives), as one coherent arc with a clean internal
boundary.

---

## 2. Principles & the shared spine

### 2.1 The bag is a receipt; afterwards the catalog tracks by content + inode
Intake consumed the bag: it validated the checksum manifest and registered every file
as `LogicalAsset` (bytes) + `ingest_item` (occurrence, carrying `(st_dev, st_ino)` and
`size`). **From that moment the catalog — not any path — is the authority for what a
file is.** This is what makes everything below safe: the operator can freely move
files on staging, because identity is **content hash + inode**, not location.

### 2.2 Three orthogonal axes
Every file carries three independent organizational facts. Keeping them separate is
the core of the design:

| axis | set when | mutability | what it controls | layer |
|---|---|---|---|---|
| **`artifactclass`** | intake | fixed | tape/cloud copies, encryption, bundling | storage |
| **artifact membership** | send | **frozen at seal** | which bundle/tape holds the bytes | storage |
| **`virtual_path` + tags** | arrangement | **mutable forever** | where it appears to humans; who may access it | access |

They are *allowed to disagree*, and that is the point (§5.5).

### 2.3 Per-occurrence model
`virtual_path`, tags, and artifact membership all attach to **`ingest_item`** (the
occurrence), never to `LogicalAsset` (the bytes) — because the same bytes can occur in
two places that deserve different arrangement and governance.

---

## 3. The trigger architecture (how operator actions reach the system)

This is the part that ties the arc together and was previously implicit.

### 3.1 The finder tool ↔ sutra boundary
The **operator never types these commands.** They work in the **finder tool**, which
calls sutra's **CLI/API surface** on their behalf. sutra provides:
- the **engine** (validation, artifacts, bundling, the metadata model),
- the **CLI/API surface** the finder tool invokes (§6),
- and **watchers** — continuous, level-triggered scans of watched directories.

The finder tool (the client/GUI) is a **separate component**, out of scope here; this
design fixes the surface it depends on.

### 3.2 Level-triggered scans (the same pattern, every stage)
Every server-side step is a **scan**: a periodic, idempotent pass over a watched
directory that finds *completed* work and processes it. This pattern already runs
ingest:

```
sutra intake scan   — watches  landing/      , processes completed bags
sutra send  scan    — watches  ingest-ready/ , processes completed artifacts   (NEW)
```

"Idempotent" = re-running skips already-done items. "Completed" = signalled
explicitly (next point), never inferred from a half-finished state.

### 3.3 Batch on completion, at folder granularity — never file-by-file
Physical send/bundling is triggered **only when the operator marks a whole folder
done**, not as individual files move. Reasoning:
- The **artifact is the *final* structure.** Reacting per-file would seal a file into a
  tape bundle *before* the operator finished deciding where it belongs —
  manufacturing the very "frozen mistake" the seal boundary exists to avoid.
- Sealing is a **tape write**: coarse and costly. You do not seal a half-arranged
  folder and re-seal as more arrives.
- It mirrors the ingest rule we already rely on: **never act on incomplete input.**
  `intake scan` ignores a bag until its sentinel says "done."

So staging has **two areas**, and the boundary between them *is* the completion signal:

```
work area/         operator arranges freely, file-by-file, in the finder tool.
   │               sutra does nothing here (identity survives via content+inode).
   │  operator finishes a folder → finder tool moves it in + writes a sentinel LAST
   ▼
ingest-ready/<folder>/   atomic "this bunch is complete — archive it" signal.
   │                     granularity = the folder (= one artifact). Folders are
   │                     committed independently as each is finished.
   ▼
sutra send scan    per ready folder: re-locate → validate → artifact → bundle → seal
```

**Crash-safety caveat (same lesson as receive):** the "done" signal must be robust
against being observed mid-move. Use a **sentinel written last** (a `.ready.json` the
finder tool writes only after the folder is fully in place), or an atomic
whole-folder rename, so the watcher never sends a partially-moved artifact.

*(File-by-file is fine for **read** feedback — the finder tool may show live "this clip
is tracked / its proxy is ready" as the operator works — but never for the
send/bundle **write**.)*

### 3.4 The three send verbs (naming, disambiguated)
"Scan" means *the watcher pass*, consistent with `intake scan`. It does **not** mean
"validate one folder." Those are separate:

| command | what it is | who runs it |
|---|---|---|
| `sutra send scan [<ingest-ready>]` | the **watcher pass** — find every ready folder and process each (idempotent) | cron / watcher |
| `sutra send <folder>` | process **one** folder now (what `send scan` calls per folder; also the finder tool's explicit "send this") | finder tool |
| `sutra send check <folder>` | **dry-run** — re-locate + validate, print the report, change nothing | finder tool (preview) |

---

## 4. Send — committing files to tape

### 4.1 Rough segregation
In the **work area**, the operator reorganizes registered files into a sensible folder
structure using the finder tool / ordinary filesystem moves. Safe because identity is
content+inode, not path. The folder structure **at completion** defines the
**artifact** — the unit that becomes a tape bundle.

### 4.2 Send-scan: re-locate + validate (the gate)
When a completed folder is processed, sutra **re-locates** each file to its
`ingest_item` — by `(st_dev, st_ino) + size` (an inode survives a same-filesystem
move), falling back to **content-hash** match for cross-filesystem moves — and applies
a Warning/Error taxonomy:

| condition | severity | meaning |
|---|---|---|
| file matches a registered `ingest_item` | ok | archivable member |
| **unknown file** (no match in the catalog) | **Error** | something unregistered crept in |
| **missing asset** (an expected registered file is gone) | **Error** | archive-everything won't silently drop it |
| **class mismatch** vs the intake's metadata | **Warning** | proceeds, recorded on the artifact |

**Any Error halts that folder** — no artifact, no bundle, no seal — and is surfaced
for the operator (the same "quarantine, don't proceed" stance intake takes). Warnings
proceed and are recorded.

### 4.3 The artifact
A clean folder produces one **`artifact`** (one artifactclass) and **`artifact_member`**
rows (one per file, with the member's path *within the artifact*). `artifact` is a
thin orchestration/provenance record — **not** a new storage format.

### 4.4 Feeding the existing bundler (reuse, don't rebuild)
Each member is enqueued into the **existing per-artifactclass bundling machinery**
(accumulate → seal → fan-out, all implemented). Packing is **artifact-aligned by
default**: one artifact → one bundle, *unless* it is small enough to coalesce with
siblings or too large (→ several bundles). This optimizes the common case — restoring
an artifact is ideally one tape read — while still packing small artifacts
efficiently. Verification rides the seal (no extra read pass): the tape build checks
each member's stored hash against its registered hash.

### 4.5 Frozen at seal
Once an artifact's bundle is **sealed**, its physical membership is **immutable** (you
don't rewrite tape to fix an arrangement call). **Before** seal it's fully mutable —
move the file in the work area and re-send. This is the *only* place physical
re-grouping ever happens (§5.5 handles "noticed too late").

---

## 5. Arrangement — the browse & governance layer (forever)

This layer **never moves bytes.** It is available from the moment of registration and
remains editable for the life of the archive — before send, after seal, years later.

### 5.1 The browse tree (`virtual_path`)
- `ingest_item.virtual_path` (default = `as_received_path`) places the file in a
  logical browse tree, independent of physical storage.
- At send it is **seeded from the as-sent structure** (so the operator's one physical
  arrangement also becomes the initial browse tree) — **but only for files still at the
  untouched default**; a file the operator has already arranged is left as-is (send
  **never clobbers** a deliberate arrangement).
- It is **editable forever** thereafter, and may freely diverge from both the
  as-received and the as-sent structure.

### 5.2 History (`arrangement_event`)
Every arrangement action (`move`, `tag-add`, `tag-remove`, `reject`) appends to an
**`arrangement_event`** log: `(ingest_item, action, from, to, actor, reason,
timestamp)`. The live tables hold current state; this is the **permanent provenance
trail** — who arranged what, when, from where, and why. Arrangement decisions are never
silently lost.

### 5.3 Tags (governance subjects)
**`ingest_item_tag`** is a many-to-many of free-form labels on occurrences. Tags are
the **subjects** that future governance policies (access groups, restore-approval)
will key on — **never** the artifactclass, which stays purely archival. **Enforcement
is deferred** (there is no identity/auth model yet); this layer builds the *subjects*
only, and the module says so explicitly. Tag-driven *storage* effects (e.g. "tag X ⇒
keep an extra copy") are a separate future reconciler, out of scope here.

### 5.4 Reject
`/.rejected/…` is a reserved virtual subtree. `arrange reject` moves an item/subtree
there with a recorded reason. **The bytes stay archived** (archive-everything) — reject
is a *marker*, not a deletion.

### 5.5 Physical (frozen) vs intellectual (forever): the cross-artifact answer
A common question: *"We grouped a file into artifact A, then realized it belongs with
B. Can we move it?"* Two senses, only one ever needed:

- **Intellectual** — make it appear and be governed *with* B: a plain `arrange mv` of
  its `virtual_path` into B's subtree (+ retag). **Always free, anytime, even years
  post-seal.** Because `virtual_path` was seeded from the send structure, each artifact
  *is* a virtual subtree, so this reads naturally.
- **Physical** — re-bundle the bytes onto B's tape: **frozen at seal, and
  unnecessary** — because **restore resolves the physical PFR locator, never the
  artifact or the virtual path.** A file *virtually* in B but *physically* in A's
  bundle restores fine; it just rides tape A's read. The only cost of a mismatch is
  marginally worse restore tape-locality — never a correctness, access, or governance
  problem.

`arrange mv` **never touches `artifact_member`**; it edits only `virtual_path`. The two
axes coexist on the `ingest_item` and may disagree. This mirrors physical archives:
you don't move the boxes when a folder is misfiled — you re-describe it; the finding
aid points to box+folder. **`virtual_path` is the finding aid; the bundle locator is
box+folder.**

### 5.6 Resolution
Browse/search resolve `virtual_path`; **restore always resolves physical locators** and
batches reads by tape. The storage layer never learns the arrangement layer exists.

---

## 6. CLI / API surface (full specification)

The finder tool and watcher call these; `<src>` resolves uniformly: an integer =
`ingest_item.id`; a string ending in `/` = a virtual subtree (operation applies to all
contained items); otherwise an exact `virtual_path`.

### Send (commit to tape)
```
sutra send scan [<ingest-ready-dir>]
        Watcher pass. Find every completed folder (ready sentinel present, not yet
        sent) and run `send` on each. Idempotent. (cron/daemon)

sutra send <folder> [--artifactclass <c>] [--label <text>] [--accept-warnings]
        Process ONE folder: re-locate each file (inode→hash), validate (Errors
        halt; Warnings proceed if --accept-warnings), create one artifact (the
        folder) + artifact_member rows, seed virtual_path from the structure for
        members still at the as-received default, and enqueue the members into the
        bundler. Default class = the members' intake class.

sutra send check <folder> [--artifactclass <c>]
        Dry-run: re-locate + validate, print the Warning/Error report, change
        nothing. Non-zero exit on any Error.

sutra send ls   [--status pending|sealing|sealed] [--class <c>]
        List artifacts and their seal status.

sutra send show <artifact-id>
        Artifact detail: members (+ member path), the bundle(s) sealed into,
        copies/placements + per-copy status, warnings recorded at send.
```

### Arrange (the browse tree — works pre and post archive)
```
sutra arrange ls <virtual-prefix> [--tag <t>] [--long]
        List the virtual tree under a prefix. --tag filters; --long adds asset
        hash, size, validity, physical artifact/bundle, and current tags.

sutra arrange mv <src> <dst>
        Edit virtual_path. <dst> = new path/prefix. Subtree-aware (preserves
        relative structure). Appends an arrangement_event per item. NEVER touches
        artifact membership or bytes.

sutra arrange show <src>
        One item across all three axes: artifactclass; physical artifact / bundle
        / tape copies; virtual_path; tags; validity; proxy derivation edges.

sutra arrange history <src>
        The arrangement_event trail (moves + tag changes interleaved, newest first;
        who/when/from/to/reason).

sutra arrange reject <src> [--reason <text>]
        Move <src> into /.rejected/… with a recorded reason. Bytes stay archived.
```

### Tag (governance subjects)
```
sutra tag add  <tag> <src> [--reason <text>]     attach (subtree ⇒ all contained)
sutra tag rm   <tag> <src> [--reason <text>]     remove
sutra tag ls   <src>                             tags on an item/subtree
sutra tag list [--prefix <p>]                    the tag vocabulary in use + counts
```

---

## 7. Data model (new)
- **`artifact`** — `(id, artifactclass, label, status[pending|sealing|sealed],
  sent_at, sealed_at)`. No single `intake_id` (a send folder may mix intakes;
  provenance is per member).
- **`artifact_member`** — `(id, artifact_id→artifact, ingest_item_id→ingest_item,
  member_path, added_at)`; unique `(artifact_id, ingest_item_id)` and
  `(artifact_id, member_path)`.
- **`arrangement_event`** — append-only `(id, ingest_item_id→ingest_item, action,
  from_value, to_value, actor, reason, at)`.
- **`ingest_item_tag`** — `(id, ingest_item_id→ingest_item, tag, actor, at)`; unique
  `(ingest_item_id, tag)`; index on `tag`.
- **`ingest_item.virtual_path`** — already exists; seeded at send, edited by `arrange`.
- No change to `LogicalAsset` / `Copy` / `bundle` schemas.

---

## 8. Reused vs new
| | |
|---|---|
| **Reused (built & tested)** | receive/intake, `ingest_item` (+ inode + `virtual_path`), the bundling/seal/fan-out machinery, restore-by-PFR-locator, the lease worker, the intake-scan watcher pattern |
| **New — Send** | the `ingest-ready/` watcher + completion sentinel, `sutra send scan/send/check`, send-scan re-location + Warning/Error, `artifact`/`artifact_member`, artifact-aligned packing, freeze-at-seal |
| **New — Arrangement** | `virtual_path` seeding (never-clobber), `arrangement_event`, `ingest_item_tag`, `sutra arrange`/`sutra tag`, reject subtree, browse-resolves-virtual / restore-resolves-physical |

---

## 9. Sequencing
1. **Send first** — it is on the **archive critical path** (without it, masters never
   reach tape under the new flow; today bundling is hand-driven). Send replaces the
   manual step with the watcher → scan → artifact → bundler pipeline.
2. **Arrangement second** — a **forever, non-blocking** layer; nothing waits on it.
They share the `ingest_item` spine, so the schema additions can land in one migration
even though the behavior ships as two phases.

---

## 10. Deferred (named, not built)
- **Access/approval enforcement** — needs an identity model; tags are subjects only now.
- **The finder-tool GUI** — this design fixes the API surface; the tool is separate.
- **Tag-driven storage effects** (extra copies for a tag) — a future desired-state
  reconciler.
- **Migration re-pack** — the *only* future occasion bytes physically re-group: a tape
  migration/refresh you are doing anyway may let the current arrangement *seed* the
  new packing. Never a standalone reason to rewrite tape.

---

## 11. Decisions (settled) & open items
**Settled:**
1. Arrangement attaches to `ingest_item` (occurrence), not `LogicalAsset` (content).
2. **D1** — `virtual_path` is **seeded from the as-sent structure**, never clobbering a
   prior `arrange`, and editable forever.
3. **D2** — bundling is **artifact-aligned by default** (soft; optimizes restore
   locality, and arrangement absorbs every "should've been elsewhere" with no physical
   penalty).
4. Send is **batch-on-completion at folder granularity**, never file-by-file; the
   completion signal is a sentinel-written-last in `ingest-ready/`.

**Open (need a decision / reviewer input):**
1. **Finder-tool ↔ sutra transport.** (A) shared filesystem + watcher only (loosest;
   matches intake today); (B) direct CLI/API calls (interactive); **(C) both** — the
   recommended lean: watcher for the automatic send trigger, direct calls for
   interactive arrange/tag. Whether the direct path is local-CLI / SSH / a small HTTP
   service depends on **where the finder tool runs** (operator workstation vs the
   staging server) — undecided.
2. **Send granularity convenience.** v1: one `send` = one folder = one artifact. A
   "send these N subfolders as N artifacts in one pass" batch is a later convenience.
3. **"Missing asset" scope at send-scan.** v1 flags unknown files (Error) + class
   mismatch (Warning) against the sent folder; whole-intake completeness ("every
   registered file of intake X is sent before X closes") is a later lifecycle-gate
   concern, noted there.
