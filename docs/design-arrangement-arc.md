# Design - Intake acceptance, arrangement, submission, and virtual segregation

> Status: **design, for review** (2026-06-18).
>
> This document replaces the older "rough segregation on staging" framing with a
> stricter model:
>
> ```text
> inspect   = read-only validation/discovery
> register  = explicit catalog acceptance
> prepare   = explicit derivative/index/enrichment profile request
> arrange   = pre-archive human layout over cataloged assets
> submit    = frozen archive namespace/source-map
> archive   = stream original bytes under submitted names
> vs        = post-archive virtual namespace and tags
> ```
>
> The load-bearing invariant is: **the BagIt landing tree is immutable
> first-contact evidence. Arrangement and virtual segregation are namespaces over
> cataloged assets, not ad hoc moves of the received bag.**

---

## 0. Context

### 0.1 What Sutradhara is

Sutradhara is the orchestration layer of a long-horizon digital preservation
archive. It receives source material from cards, drives, folders, and handoffs;
verifies checksums; records catalog identity; creates review derivatives; and
writes durable copies to tape/cloud according to artifactclass policy.

The important storage properties are:

- **Archive-everything.** Bad, rejected, or hard-to-decode files are preserved and
  flagged. They are not silently culled.
- **Tape layout is effectively immutable.** Once a tape object is sealed, later
  organization should update catalog metadata, not rewrite the tape.
- **Human organization takes time.** Preservation and review preparation should
  not wait for final curation.

### 0.2 Current first-contact flow

`sutra receive` writes a BagIt-style intake to landing:

```text
/replica/landing/<intake-id>/
  bagit.txt
  bag-info.txt
  manifest-sha256.txt
  tagmanifest-sha256.txt
  intake.json
  data/
    DCIM/A001.MOV
```

`intake.json` is the completion sentinel and is written last. The `data/` tree is
the as-received payload and must remain immutable evidence. Operator sorting
must not rename, move, or delete files inside this tree.

macOS **packages** (`.fcpbundle`, `.photoslibrary`, `.app`, ...) are normalized to a
single tar payload at receive (see §2.5), so `data/` holds one entry per package,
never its internal files.

### 0.3 Catalog split

Two concepts matter throughout:

- **LogicalAsset** - one row per unique byte sequence, keyed by SHA-256.
- **IngestItem** - one row per received occurrence of a file. It points to a
  `LogicalAsset` and carries occurrence facts such as intake id,
  `as_received_path`, source path, `virtual_path`, size, artifactclass, and
  optional inode information.

Arrangement is normally an occurrence-level decision, so it attaches to
`IngestItem`. The same bytes in two intakes can deserve different human paths,
tags, or archive submission membership.

---

## 1. Problem

After receive/BagIt, we need three things that are related but should not be
collapsed into one command:

1. **Accept the intake into catalog truth.** A valid received bag should become
   `Intake`, `IngestItem`, and `LogicalAsset` rows only after an explicit
   registration/acceptance step.
2. **Prepare review derivatives.** For video-heavy work, humans should sort HD
   proxy/mezz files, not stream 4K masters over SMB from cheap clients.
3. **Turn human layout into archive layout.** Before archive, the human's rough
   arrangement should become the entry names used in the archive. After archive,
   later virtual segregation should change only catalog paths/tags.

The previous shorthand made `scan` too powerful: it implied that a filesystem
scan could validate, register catalog truth, enqueue expensive ffmpeg work, and
begin archive preparation. Operators need names and side effects they can trust.

---

## 2. Core Decisions

### 2.1 Split read-only inspection from durable mutation

| Step | Side effects | Purpose |
|---|---:|---|
| **inspect** | none, except optional inspection records/logs | Validate BagIt and report readiness/quarantine. |
| **register** | catalog mutation | Accept a valid intake into authoritative catalog truth. |
| **prepare** | desired profile + reconciliation wake-ups | One generic verb over the profile's job set (derivatives/indexes/enrichments); new kinds = config, not a new verb. cloud-temp (temporary DR) is automatic at register + gate-expired, not a prepare step. |
| **arrange** | arrangement rows + projection tree | Let humans sort registered assets before archive. |
| **submit** | frozen submission rows/source-map | Freeze arrangement as archive input. |
| **archive** | durable copy creation | Stream original bytes into RAO/d2 under submitted names. |
| **vs** | virtual path/tag rows | Edit post-archive logical organization. |

All steps should be idempotent. Idempotent does not mean side-effect free:
`register` and `prepare` are explicit, durable transitions.

### 2.2 BagIt data is never the working segregation tree

Do not use `/replica/landing/<intake-id>/data` as the human sorting area.

The landing bag is provenance. Arrangement is recorded in Sutradhara as:

```text
ingest_item_id -> human member path
```

and later frozen as:

```text
archive_path -> source_path -> sha256
```

### 2.3 Preparation is explicit and config-driven (one verb, not a verb per job)

Post-register work (review derivatives, indexes, future enrichments) should not be a
surprise side effect of read-only inspection. But the *set* of that work must not be
hard-coded into the CLI - new kinds (audio-extract, transcription, thumbnail, OCR,
scene-detect, ...) appear over time. So there is **one generic verb** driven by a
configurable **prepare profile**, not a verb per job kind:

```bash
sutra prepare <intake-id> --profile hd-review
```

A prepare profile maps `(artifactclass | media_kind, profile) -> [ {job_kind, params,
output_class}, ... ]` (e.g. `s-masters + hd-review -> [transcode(mezz,preview) → class
s-proxy, pfr-index → sidecar]`).
`prepare` ensures only the missing work (idempotent); a new kind is **config + a
handler**, never a new CLI verb. It is the generic surface over the existing job
framework, and the profile is desired-state the way copies are (the reconciliation
model): "this class should have these derivations." Automation can still fire it; the
work is reconciled (§2.6) and audited in the job/attempt log, not announced as a
per-kind lifecycle event.

**Each derivative output gets its own artifactclass — `output_class` — never the
source's.** The produced item (a proxy, an extracted audio stem) is just another
`IngestItem` with its own class, so it gets *its* copies by the normal `artifactclass
-> pools` rule — and the model **recurses** (a derivative's class may itself declare a
prepare profile). The class comes from the profile entry, so a proxy is always
`s-proxy` (1 copy) — **never inherited from `s-masters` (3 expensive copies)**. Today
`transcode` falls back to the source class when no class is passed
(`handlers/transcode.py:85`: `params.get("proxy_artifactclass") or item.artifactclass`)
— a footgun that would cut a proxy master-tier copies; **remove that fallback once the
profile carries `output_class`.** A `sidecar = true` derivative (e.g. pfr-index)
attaches to the source object and takes no copies of its own. This — copies driven by
class, derivatives driven by the profile, both reconciled `policy × asset` — is the
two-dimension desired-state model that `design-reconciliation-model.md §3.7` states
generically; the concrete derivative model lives here.

**cloud-temp is not one of these.** The encrypted cloud DR blob is a *temporary,
lifecycle-bounded* `Copy` - created automatically at register/prepare, and **expired
by the deletion-gate (Phase U)** once tape copies are verified + offsite-confirmed. It
is neither a CLI verb nor a permanent placement (a placement reconciler would fight the
expiry); its desired end-state is deletion.

### 2.4 Arrangement and VS are one namespace family

Pre-archive arrangement and post-archive virtual segregation share concepts:
asset identity, human path, actor, history, tags, reject markers, and restore
resolution. They differ in storage effect.

| Phase | Human path controls | Storage effect |
|---|---|---|
| **Arrangement** | Archive entry names for a not-yet-archived submission. | Frozen into source-map; archive writes originals under these names. |
| **Virtual segregation** | Browse/search/restore namespace after archive. | Catalog metadata only; sealed storage objects do not move. |

### 2.5 Package normalization at receive (macOS bundles)

Some "files" are macOS **packages** - directories the OS and operator treat as one
opaque item but which hold thousands-to-millions of internal files (`.fcpbundle`,
`.photoslibrary`, `.imovielibrary`, `.app`). They must be **one object end to end** -
one payload entry, one `LogicalAsset`, one `IngestItem`, one arrangement node, one
archived blob - never millions of each. We already draw this boundary at the archive
layer (blob config); we draw the **same boundary, from the same config, at receive**.

**Decision (A-prime): receive-time package wrapping as first-contact normalization.**
When `sutra receive`'s walk meets a directory matching a configured **package
boundary**, it does not recurse. It **defines** the package's canonical byte object by
streaming the subtree through a **pinned deterministic tar** into one SHA-256, landed
as a single payload:

- physical `data/A001.fcpbundle.tar` -> one `manifest-sha256.txt` entry (the bag stays
  RFC-8493-valid: one file, not millions);
- logical name `A001.fcpbundle`, recorded as `logical_member_path` alongside
  `stored_member_path` (`....tar`) - exactly as compression records `.zst` members;
- an **inner index** (`member -> offset/len + sha256`) as a tag file, so deep fixity
  and single-internal-file restore remain possible (the same inner index the archive
  blob keeps - produced here, once, at first contact);
- `register` creates **one master `IngestItem`** per package; `archive` streams the
  `.tar` as-is (already blobbed - no re-tar at seal).

This **reuses the existing logical/stored member-name + staging-transform machinery**
(the compression design); no new table is required.

**Wrap once; the archive stores it dense.** The package tar is produced exactly once,
at receive (its hash is the package's identity); the archive stores it as an opaque
**dense** member and never re-tars it. So there is one tar implementation to pin
(receive), **rem needs no package awareness** (dense object + ranged read), and
**package-blobbing moves from archive-time to receive-time** - the archive layer stops
adding package globs to the blob ruleset (they arrive pre-wrapped; the blob config now
covers only archive-time operator blobs). The package's **inner index** is a set of PFR
locators one level down, so single-internal-file restore reuses the existing
partial-restore path. Receive-layer mechanism + the pinned `package-tar-v1` profile +
golden-vector test: `design-receive-front-door.md` §12.

**It is normalization, not a reversal-transform.** The archive-side staging transforms
(AppleDouble merge, `.zst`) preserve a pre-existing single-file `LogicalAsset` and
reverse on restore. A package has **no single byte stream until receive defines one** -
its archival identity *is* the pinned tar; there is no "original" to reverse to. It is
recorded as a first-contact package-normalization in the bag's tag files.

**It does not eliminate the first-contact walk.** Receive still opens/stats every
internal file once to stream it into the tar (reading the bytes is unavoidable). What
A-prime removes is everything *downstream and repeated*: a copied landing tree of
millions of entries, a giant manifest, the catalog explosion at register, an
arrangement projection of a million render files, and a re-walk at every later stage.
**Walk once, then one object forever.**

**The tar profile must be pinned and versioned.** "One hash" is durable evidence only
if the tar is byte-reproducible across versions and platforms. Pin and version the full
profile: tar format (ustar/pax), member **path ordering** (deterministic sort), uid/gid
normalization, mtime policy, mode bits, symlink / xattr / resource-fork / AppleDouble /
sparse-file handling, and error policy. Use the **same pinned dialect as the archive
`.remwrap.tar`**, so the hash is stable *and* matches the blob the archive would have
produced.

**The boundary config is shared and versioned.** Receive runs at the edge with no DB,
so the package-boundary list is a **synced receive policy/profile** whose hash is
written into **`bag-info.txt`** (next to `canonicalization_version`), so the server
knows which boundary set the edge applied and the two never disagree.

**Detection is glob-driven, not the macOS package bit.** On the Linux staging host a
`.fcpbundle` is just a directory with no Finder package flag, so the portable signal is
the shared **package-glob list** (`*.fcpbundle`, `*.photoslibrary`, ...); the macOS
package bit is a secondary hint only when receive runs on a Mac. A boundary **stops
everything inside it** (no recursion, no per-file transform within) and matches at the
**outermost** package edge.

**Scope.** A-prime is the default for opaque packages. The native-directory alternative
(per-file manifest, collapse only at register) is **reserved** for any class where
file-manager-browsable native package *contents* in the bag are a hard external
requirement - not the case for `.fcpbundle`/`.photoslibrary`/`.app`, which Apple itself
models as single-file library packages with application-managed internal media.

### 2.6 Reconcilers enqueue work; wake-ups are event-driven, not full-table polling

The lifecycle stages should be **decoupled** from the jobs they cause - a new job
kind must not mean editing a stage. The durable way to get that decoupling is
**desired-state reconciliation**, but the reconciler must be awakened by an
**event/worklist plane**, not by repeatedly scanning every asset. Concretely:

- **A stage mutates catalog state and emits a domain event.** It does **not** itself
  decide which jobs to run. `register` *creates the asset*; it does not enqueue
  `transcode`.
- **Reconcilers own "what should exist" and enqueue jobs to close the gap.** "Prepare
  runs transcode" is really *"the derivation reconciler sees an asset that, per its
  class profile, should have a proxy and doesn't -> enqueue transcode."* The stage
  never had to know `transcode` exists; a new kind is **profile + handler** (§2.3,
  `prepare_requirement`). This is the same decoupling an event bus would give, but it
  **self-heals** and survives a missed moment.
- **Events are first-class wake-ups, never authorization.** Keep a **domain-event
  log** (`IntakeRegistered`, `PrepareRequested`, `Archived`, `OffsiteConfirmed`,
  `ToolVersionChanged`) for provenance and observability. In the same durable path,
  project those events into a small **reconciliation worklist**: "these targets may now
  have an open gap." The reconciler claims worklist rows, reloads the current catalog
  state for that target, and only then decides whether a job is wanted.

This distinction matters at scale. "Level-triggered" means **the decision is made from
current catalog facts**, not from the event payload. It does **not** mean "wake every
loop and ask the database which of 50 million assets are missing proxies." Pure
full-table polling becomes the bottleneck long before the storage layer does.

The production shape is therefore:

1. A catalog mutation commits the authoritative fact and appends a domain-event row.
2. A transactional outbox/worker projects the event into a domain worklist, e.g.
   `(domain='derivation', target=ingest_item_id, condition_key='hd-review:preview',
   reason='registered', due_at=now)`.
3. The reconciler reads **due worklist rows**, batches by domain/profile/condition,
   and performs the desired-state check only for those targets.
4. If the gap is real and no live attempt already exists, it enqueues a job attempt.
   If the gap is already closed, it marks the worklist row consumed.
5. Job completion updates the produced fact (`asset_derivation`, `Copy`, index row,
   verification timestamp) and may create follow-on wake-ups for dependent targets.
6. A scheduled **sweep** periodically rebuilds or audits the worklist from indexed
   catalog facts. Sweeps are safety nets for missed events, code bugs, and new profile
   rules; they are not the hot path.

**Why (the d3 lesson, per `design-reconciliation-model.md`).** Event-pinned work is
unsafe: a missed event (dispatcher crash, a job added after the event fired, a
repair/replay) means the work **silently never happens**. Desired-state reconciliation
converges because the catalog remains the source of truth. But desired-state
reconciliation must be **worklist-driven** in normal operation, with coarse sweeps as
backstop, or it becomes a database polling engine at archive scale.

**Design line:** events and worklists wake reconcilers; catalog facts authorize jobs.
Do not bind "event X creates job Y." Do not poll all assets every cycle. Build
per-domain reconcilers that can be fed by events, manual requests, job completions,
profile changes, and scheduled sweep findings through the same rebuildable worklist.

### 2.7 First-class fact-types vs generic job kinds (where the built-in line sits)

To stay flexible without becoming a soup of opaque jobs, the system is **first-class
on the nouns, generic on the verbs.**

- **Fact-types** are the small, fixed vocabulary the rest of the system *reasons
  about*: a **derivation** (the `asset_derivation` edge: master -> derived, `kind`), a
  **copy**, an **index** (PFR sidecar), **validity**. They earn first-class status
  because real features depend on knowing what they are - the projection layer finds a
  master's review proxy, restore distinguishes master vs derived, the restore-gate
  reads validity, search badges proxies.
- **Job kinds** are open-ended and *produce* facts: `transcode`, `pfr-index`,
  `thumbnail`, `transcription`, `scene-detect`, ... Each handler yields facts of a
  fact-type (transcode -> two derivation edges; pfr-index -> an index; cloud-temp -> a
  copy).

So there is **no "create derivatives" stage** - *derivation* is a built-in **noun**,
and the jobs that produce it are generic. The built-in/custom line is drawn at the
**fact-type** level, not the job level:

| layer | open or fixed? | example |
|---|---|---|
| fact-types (catalog vocabulary) | small, fixed, deliberate to add | derivation, copy, index, validity |
| job kinds (produce facts) | open: config + handler | transcode, pfr-index, thumbnail, transcription |
| prepare profile (class -> wanted facts, via which kind) | pure config | `s-masters -> mezz+preview derivations (transcode) + index (pfr-index)` |

**Consequence:** a new job kind that produces an *existing* fact-type (audio-extract,
transcription, scene-detect -> all derivations) is **config + a handler, no schema
change**. A genuinely novel output (e.g. embeddings -> a vector store) needs a **new
fact-type** - the rare, deliberate, first-class decision (drawn maybe once a year).
`transcode`/`pfr-index`/`cloud-temp` are "built-in" only in that they ship in the
default profile + a default handler; the engine has no built-in/custom tier.

---

## 3. End-to-end Flow

### 3.1 Receive

The receive command copies from source to landing, hashes on read, writes BagIt
manifests, and writes `intake.json` last.

```bash
sutra receive /source/card \
  --landing /replica/landing \
  --source-kind card \
  --artifactclass s-masters \
  --label "morning shoot"
```

At this point the filesystem contains received evidence, not yet accepted catalog
truth.

### 3.2 Inspect

Inspection validates an intake candidate without registering assets:

```bash
sutra intake inspect /replica/landing/<intake-id>
```

Checks:

- `intake.json` exists and is complete.
- BagIt tag files and manifests are present.
- `manifest-sha256.txt` matches every payload file under `data/`.
- Intake metadata and artifactclass policy are valid.

Possible statuses:

```text
valid
invalid
incomplete
already_registered
```

`inspect` may write an inspection report. It must not create `IngestItem`,
`LogicalAsset`, `AssetDerivation`, `Copy`, archive submission, or job rows.

### 3.3 Register

Registration is the explicit acceptance step:

```bash
sutra intake register <intake-id> --artifactclass s-masters
```

Effects:

- Create/update the authoritative `Intake` row.
- Create one master `IngestItem` per BagIt payload **object** (a normalized
  package, §2.5, is one object; its internal files are not individual items).
- Create/link content-addressed `LogicalAsset` rows.
- Record provenance:
  - bag root;
  - manifest path;
  - as-received relative path;
  - source path under BagIt `data/`;
  - size and sha256;
  - optional `st_dev`/`st_ino`.
- Initialize `virtual_path` from `as_received_path`, marked as default, not as
  deliberate curation.

Registration idempotency:

- Same intake and same manifest digest: no-op.
- Same intake with changed manifest or payload: error/quarantine.
- Duplicate content across intakes: one `LogicalAsset`, separate `IngestItem`
  provenance.

### 3.4 Prepare (derivatives, indexes, enrichments)

Preparation explicitly requests the post-register work needed for review,
arrangement, and VS - driven by the artifactclass/media **profile**, not a verb per
job kind (§2.3):

```bash
sutra prepare <intake-id> --profile hd-review
```

This records the requested profile and wakes the relevant reconcilers. The observed
effect is still idempotent - only missing jobs in the profile are queued after the
reconciler re-checks catalog state. An operator convenience composes register +
prepare:

```bash
sutra intake accept <intake-id> --artifactclass s-masters --prepare hd-review
```

Internally, `register` emits the coarse lifecycle milestone; `prepare` records the
requested profile and emits a profile-level wake-up (`PrepareRequested`). The
derivation reconciler then evaluates the current catalog state and enqueues only the
jobs still needed (§2.6, §2.7). There is no per-kind `…Requested` lifecycle event:

```text
IntakeRegistered            # lifecycle milestone (audit)
PrepareRequested(hd-review)  # wake-up for the derivation/index reconcilers
# reconciler -> enqueue transcode/pfr-index only if the gap is still open;
#               each run is a job_attempt, not a lifecycle event
```

The `hd-review` profile for video produces:

- preview proxy for quick browse/scrub;
- HD mezz/proxy for human arrangement on cheaper clients;
- derivation edges back to the original master;
- locators readable by the projection layer.

For non-video assets the same profile may mean thumbnail, PDF preview, OCR/text,
audio waveform, or no derivative - it is just a different **profile entry**. The CLI
verb never changes; the profile does.

**cloud-temp (temporary DR) is not part of `prepare`.** The encrypted cloud blob is a
temporary `Copy` created automatically at register (the DR copy is wanted ASAP,
independent of any review profile) and **expired by the deletion-gate (Phase U)** once
durable tape copies are confirmed - not a CLI verb, not a permanent placement (§2.3).

### 3.5 Create arrangement workspace

Arrangement starts after registration. It may require derivatives to be ready, or
it may enter `pending_derivatives` until review files exist.

```bash
sutra arrangement create \
  --from-intake <intake-id> \
  --label "morning rough"
```

Arrangement members point to the original/master `IngestItem`. A derivative item
may be attached for projection and playback, but the derivative is not the
archival target.

Workspace states:

```text
draft
pending_derivatives
ready
submitted
abandoned
```

### 3.6 Project workspace for ordinary file managers

Sutradhara materializes a server-side projection:

```text
/replica/arrangements/<workspace-id>/
  unsorted/
    A001.mp4
    A002.mp4
  .sutra/
    members.json
```

The visible files should be cheap review files, usually HD H.264/AAC MP4 for
video. The projection can be exported over SMB so Mac Finder, Windows File
Explorer, Linux GNOME Files, or Dolphin remain the human UI.

The hidden mapping links visible files to catalog identity:

```json
{
  "unsorted/A001.mp4": {
    "ingest_item_id": "item-master-aaa",
    "logical_asset_id": "asset-abc123",
    "derivative_item_id": "item-proxy-111",
    "archive_filename": "A001.MOV"
  }
}
```

The watcher/reconciler runs on the server. Clients do not need FUSE or a custom
driver in the first slice.

### 3.7 Reconcile file operations into arrangement intent

Operators move, rename, and delete projected proxy files. Sutradhara translates
those file operations into arrangement rows:

| User operation | Sutradhara effect |
|---|---|
| Move/rename known projected file | Update `arrangement_member.member_path`. |
| Create folder | Allowed; no catalog row until a member moves into it. |
| Delete known projected file | Mark excluded or remove from workspace, by policy. |
| Copy unknown file into workspace | Reject, quarantine, or create import-pending; never silently archive. |
| Modify projected file content | Reject or mark dirty; never mutate master bytes. |

The member path normalizes from review filename to master/archive filename:

```text
Projection path: satsang/day-1/A001.mp4
Submission path: satsang/day-1/A001.MOV
```

### 3.8 Submit arrangement

Submitting freezes the draft arrangement:

```bash
sutra arrangement submit <workspace-id> --artifactclass s-masters
```

Validation:

- Every member resolves to a registered master item.
- No unknown or dirty projected files remain.
- No duplicate submitted archive paths.
- Paths are relative, normalized, and policy-compliant.
- Artifactclass is compatible with all members.
- BagIt source hashes still match catalog expectations if sources are online.

Submission output is a frozen source-map, not a copied 4K staging tree:

```text
/replica/submissions/<submission-id>/
  submission.json
  source-map.tsv
  manifest-sha256.txt
```

Example:

```text
archive_path	source_path	sha256	size	ingest_item_id
satsang/day-1/A001.MOV	/replica/landing/intake-123/data/DCIM/A001.MOV	abc123	987654321	item-master-aaa
satsang/day-1/A002.MOV	/replica/landing/intake-123/data/DCIM/A002.MOV	def456	876543210	item-master-bbb
```

### 3.9 Archive from source-map

The archive writer consumes the source-map directly:

```text
open source_path
stream bytes into RAO/d2 entry named archive_path
compute/check sha256 while streaming
record copy and locator rows
```

There is no default second copy of large masters just to produce an arranged
directory tree.

If compatibility code insists on walking a physical tree, use fallbacks in this
order:

1. Reflink tree where filesystem support exists.
2. Hardlink tree only on the same filesystem and with read-only controls.
3. Symlink tree only if the bundler intentionally resolves targets and stores
   target bytes.
4. Real copy only as an explicit fallback.

The canonical archive interface is:

```python
ArchiveEntry(source_path, archive_path, sha256, size, ingest_item_id)
```

### 3.10 Virtual segregation after archive

After archive, operators continue organizing through virtual paths and tags:

```bash
sutra vs mv /as-received/DCIM/A001.MOV /programs/satsang/day-1/A001.MOV
sutra tag add kailash <asset-or-path>
```

Effects:

- Update virtual namespace rows/history.
- Do not touch BagIt landing data.
- Do not rewrite RAO/d2 objects.
- Restore resolves virtual path -> logical asset/ingest item -> physical
  locators -> tape/cloud.

---

## 4. Data Model

Names are illustrative; implementation should follow existing Sutradhara model
style.

### 4.1 Optional inspection records

Inspection records are not authoritative asset catalog truth.

```text
intake_inspection
  id
  intake_id
  landing_path
  status
  manifest_digest
  inspected_at
  diagnostics_json
```

### 4.2 Registration

Existing/current concepts, with explicit lifecycle semantics:

```text
intake
  id
  source_kind
  source_ref
  operator
  artifactclass
  label
  bag_root
  manifest_path
  status
  registered_at

ingest_item
  id
  intake_id
  logical_asset_hash
  as_received_path
  source_path
  virtual_path
  size_bytes
  sha256
  artifactclass
  st_dev
  st_ino
  metadata_json

logical_asset
  hash
  size_bytes
  media_type
  created_at
```

### 4.3 Prepare profile and derivations

The **prepare profile** is the desired-state config that drives `sutra prepare`
(§2.3): one row per (selector, profile) -> job kind + params. `kind` is **any
registered job kind** (transcode, pfr-index, thumbnail, transcription, ...), so new
prep work is a row + a handler, never a new CLI verb.

```text
prepare_requirement            # the prepare profile
  id
  profile                      # e.g. hd-review
  selector                     # artifactclass or media_type this entry applies to
  kind                         # any registered job kind
  params_json

asset_derivation
  id
  source_ingest_item_id
  derived_ingest_item_id
  kind
  profile
  status
  metadata_json
```

### 4.4 Reconciliation wake-ups and worklists

Reconcilers are desired-state controllers, but their hot path is a **derived
worklist**, not an unbounded catalog scan. The worklist may be implemented per-domain
for performance, but the logical shape is:

```text
domain_event
  id
  kind                         # IntakeRegistered, PrepareRequested, JobSucceeded, ...
  aggregate_type
  aggregate_id
  payload_json
  created_at
  projected_at                 # set after wake-ups are materialized

reconciliation_wakeup
  id
  domain                       # derivation, copy, verification, arrangement_projection
  target_type                  # ingest_item, logical_asset, copy, workspace, ...
  target_id
  condition_key                # preview, mezz, pfr-index, tape-copy:d2, verify:180d, ...
  desired_generation
  reason                       # registered, profile_changed, job_finished, sweep, manual
  due_at
  priority
  status                       # pending, claimed, consumed, suppressed
  claimed_by
  claimed_at
  last_error
  created_at
  updated_at
```

Rules:

- `domain_event` is append-only audit/provenance.
- `reconciliation_wakeup` is a **derived acceleration structure**. It is durable enough
  for worker claims, but not authoritative intent. If it is lost, a sweep can rebuild
  it from catalog state.
- Use a live-row uniqueness guard such as
  `(domain, target_type, target_id, condition_key) WHERE status IN ('pending','claimed')`
  so duplicate events and double-clicked operator actions coalesce without merging
  unrelated requirements for the same target.
- The reconciler reads only due wake-ups, then reloads the authoritative catalog rows
  and evaluates desired vs observed state. If the desired state is already satisfied,
  the wake-up is consumed without a job.
- Backoff/blocking state belongs with the domain condition the reconciler reads
  (`asset_derivation.status`, copy verification freshness, arrangement dirty state,
  and the condition summary in §4.5), not in the event log. The worklist answers
  "what should I check now?", not "what is true?"
- Scheduled sweeps insert `reason='sweep'` wake-ups for targets whose indexed condition
  says they may be open or stale. Sweeps must be bounded and indexed (by profile hash,
  status, `next_eligible_at`, `last_verified_at`, updated-at watermarks), not full
  rescans on every controller loop.

This keeps the d3 safety property (replay/rebuild can converge after missed events)
without turning Sutradhara into a database poller at tens-of-millions scale.

### 4.5 Reconciliation condition model

The worklist tells a reconciler **what to check next**. It does not answer **whether
work should run**. That decision comes from a compact, queryable **condition model**
maintained per domain. The model has four layers:

1. **Desired state** - durable intent, usually scoped by policy or operator request.
2. **Observed state** - facts already in the catalog (`asset_derivation`, `Copy`,
   index sidecar rows, verification timestamps, arrangement projection state).
3. **Condition summary** - the current per-target/per-requirement decision state used
   by reconcilers and dashboards.
4. **Attempt log** - append-only history of every job try and its outcome.

Keep these separate. Desired state is authoritative. Observed state proves whether the
desired fact exists. Condition summary is a maintained projection for hot-path queries.
Attempt history is audit/provenance and must not be scanned on every reconcile cycle.

#### Desired state

Desired state should be domain-owned, not hidden in job rows. For derivations and
indexes, `prepare` creates a profile assignment:

```text
prepare_assignment
  id
  scope_type                   # intake, workspace, query, explicit_items
  scope_id
  profile                      # hd-review
  status                       # active, suppressed, retired
  profile_generation
  requested_by
  requested_at
  metadata_json
```

The profile expands into per-target requirements. Broad assignments must be expanded
asynchronously and in shards; do not synchronously insert 20 million rows in the
operator request. The authoritative desired state is the assignment + profile
generation. Materialized per-target rows are acceleration/provenance:

```text
asset_requirement
  id
  assignment_id
  ingest_item_id
  fact_type                    # derivation, index
  condition_key                # preview, mezz, pfr-index
  prepare_requirement_id
  desired_generation           # profile/params generation this row represents
  params_hash
  status                       # active, suppressed, retired
  created_at
  updated_at
```

Other domains use the same pattern with their own nouns: placement policy creates copy
requirements; verification policy creates freshness requirements; arrangement
projection creates workspace projection requirements. Do **not** build one generic
"desired job" table. Share the condition contract, not the domain model.

#### Condition summary

Each domain keeps a compact condition row keyed by the target and requirement:

```text
reconciliation_condition
  id
  domain                       # derivation, copy, verification, arrangement_projection
  target_type
  target_id
  condition_key
  desired_generation
  observed_generation
  status                       # see table below
  reason_code
  reason_detail
  attempt_count
  last_attempt_id
  live_job_id
  next_eligible_at
  blocked_until_kind           # none, tool_version, source_generation, policy_generation, manual
  blocked_until_value
  suppressed_by
  suppressed_at
  updated_at
```

The status vocabulary is shared:

| status | Meaning | Reconciler action |
|---|---|---|
| `satisfied` | Desired fact exists and matches the desired generation/params. | Consume wake-up. |
| `open` | Desired fact is missing and no live attempt is running. | Eligible if due. |
| `stale` | Fact exists but profile, params, tool, or freshness generation changed. | Eligible if due. |
| `in_flight` | A pending/running attempt already owns this requirement. | Do not enqueue. |
| `failed_transient` | Last attempt failed, but retry is allowed after backoff. | Retry only after `next_eligible_at`. |
| `blocked` | Repeating now is pointless until an external generation changes. | Do not retry until unblock condition is met. |
| `suppressed` | Operator/policy deliberately paused this requirement. | Do not retry until unsuppressed. |
| `not_applicable` | The selector no longer applies or desired state was retired. | Consume wake-up and keep for audit/diagnosis if useful. |

The canonical enqueue predicate is:

```text
desired state is active
AND condition.status IN ('open', 'stale', 'failed_transient')
AND now >= condition.next_eligible_at
AND no live attempt exists for (domain, target_type, target_id, condition_key)
AND condition is not suppressed
AND condition is not blocked
```

Blocked conditions reopen by generation changes, not by time alone. Examples:

- corrupt source fixed by re-ingest -> `source_generation` changes;
- handler bug fixed -> `tool_version` changes;
- profile params changed -> `policy_generation` changes;
- operator override -> manual unblock changes status to `open`.

When one of those facts changes, the event projector or sweep inserts a wake-up for
the blocked condition. The reconciler then recomputes the condition from current
catalog state and may move it back to `open` or `stale`.

#### Attempt log

Every actual run appends an attempt record. This is the provenance trail, not the
reconciler hot path:

```text
job_attempt
  id
  domain
  target_type
  target_id
  condition_key
  job_kind
  params_hash
  status                       # queued, running, succeeded, failed, canceled
  worker_id
  tool_name
  tool_version
  source_generation
  desired_generation
  started_at
  finished_at
  error_code
  error_message
  metadata_json
```

Attempt completion updates observed facts and the condition summary in the same
transaction as the attempt result where possible. If a worker crashes between job
output and condition update, the next wake-up/sweep recomputes the condition from
observed facts.

#### Backoff and sweeps

Backoff is stored on the condition row (`next_eligible_at`, `attempt_count`,
`reason_code`), not derived by scanning attempts. Sweeps read indexed condition rows:

```text
WHERE status IN ('open', 'stale', 'failed_transient')
  AND next_eligible_at <= now
```

For recurrence, verification uses the observed freshness fact:

```text
WHERE domain = 'verification'
  AND last_verified_at < now - policy.interval
```

For broad profile changes, do not wake every affected asset synchronously. Store the
new profile generation, then run a sharded expander/sweep that materializes or updates
conditions in bounded batches. Until expansion finishes, the assignment generation is
the source of truth; missing condition rows are a backlog, not absence of desired
state.

#### Domain examples

```text
derivation condition:
  target = ingest_item:123
  condition_key = hd-review:preview
  desired_generation = profile hd-review v7
  observed_generation = preview params_hash abc if present

copy condition:
  target = logical_asset:sha256...
  condition_key = placement:d2:tape-primary
  desired_generation = artifactclass policy v4
  observed_generation = latest verified copy policy generation

verification condition:
  target = copy:456
  condition_key = verify:180d
  desired_generation = verification policy v2
  observed_generation = last_verified_at bucket / copy digest generation
```

### 4.6 Arrangement

```text
arrangement_workspace
  id
  label
  status
  created_by
  created_at
  source_intake_ids_json
  metadata_json

arrangement_member
  id
  workspace_id
  ingest_item_id          # original/master item
  derivative_item_id      # optional review/proxy item
  member_path             # archive-relative intended path
  display_path            # projection-visible path, if different
  role                    # master, sidecar, note, proxy
  status                  # active, excluded, dirty, missing_derivative
  updated_at

arrangement_event
  id
  workspace_id
  ingest_item_id
  action
  from_value
  to_value
  actor
  reason
  at
```

### 4.7 Submission

```text
archive_submission
  id
  workspace_id
  artifactclass
  status                  # frozen, archiving, archived, failed
  submitted_by
  submitted_at
  source_map_path
  manifest_path

archive_submission_member
  id
  submission_id
  ingest_item_id
  archive_path
  source_path_snapshot
  sha256_snapshot
  size_snapshot
```

### 4.8 Virtual namespace and tags

```text
virtual_path
  id
  ingest_item_id
  namespace
  path
  status                  # active, rejected, hidden
  updated_by
  updated_at

virtual_path_history
  id
  ingest_item_id
  namespace
  old_path
  new_path
  actor
  changed_at

ingest_item_tag
  id
  ingest_item_id
  tag
  actor
  at
```

Tags are governance subjects only in this phase. Access/approval enforcement
waits for the identity model.

---

## 5. CLI/API Surface

### 5.1 Intake lifecycle

```bash
sutra intake inspect /replica/landing/<intake-id>
sutra intake register <intake-id> --artifactclass s-masters
sutra intake accept <intake-id> --artifactclass s-masters --prepare hd-review
```

REST shape:

```text
POST /api/intakes/{id}/inspect
POST /api/intakes/{id}/register
POST /api/intakes/{id}/accept
```

### 5.2 Prepare

```bash
sutra prepare status <intake-id> [--profile hd-review]
sutra prepare <intake-id> --profile hd-review
sutra prepare <intake-id> --profile hd-review --retry-failed
```

`prepare` records a desired profile and inserts reconciliation wake-ups; the
reconciler queues the missing jobs idempotently. The **profile** (not the CLI) defines
which kinds, so new kinds need no new verb. cloud-temp is created at register and
gate-expired (§3.4), not here.

### 5.3 Arrangement

```bash
sutra arrangement create --from-intake <intake-id> --label "morning rough"
sutra arrangement project <workspace-id> --root /replica/arrangements
sutra arrangement reconcile <workspace-id>
sutra arrangement watch <workspace-id>
sutra arrangement validate <workspace-id>
sutra arrangement submit <workspace-id> --artifactclass s-masters
```

REST shape:

```text
POST /api/arrangements
GET  /api/arrangements/{id}
POST /api/arrangements/{id}/project
POST /api/arrangements/{id}/reconcile
POST /api/arrangements/{id}/validate
POST /api/arrangements/{id}/submit
```

### 5.4 Archive

```bash
sutra archive submit <submission-id>
```

The archive service receives source-map entries. It should not require a copied
submission directory.

### 5.5 Virtual segregation and tags

```bash
sutra vs ls <path> [--tag <tag>] [--long]
sutra vs mv <src> <dst>
sutra vs reject <src> [--reason <text>]
sutra vs history <src>

sutra tag add <tag> <src> [--reason <text>]
sutra tag rm  <tag> <src> [--reason <text>]
sutra tag ls  <src>
```

`<src>` may resolve by ingest item id, exact virtual path, or subtree prefix.

### 5.6 Reconciliation operations

Normal reconciliation is event/worklist driven and runs continuously on the server.
Operator/admin commands are for diagnosis, manual wake-ups, and safety sweeps:

```bash
sutra reconcile status [--domain derivation|copy|verification|arrangement]
sutra reconcile wake --domain derivation --target ingest_item:<id> \
  --condition hd-review:preview --reason manual
sutra reconcile sweep --domain derivation --profile hd-review
sutra reconcile sweep --domain verification --older-than 180d
```

REST shape:

```text
GET  /api/reconciliation/status
POST /api/reconciliation/wake
POST /api/reconciliation/sweep
```

`wake` inserts worklist rows for a target/condition; it does not enqueue jobs
directly. `sweep` performs a bounded indexed audit and inserts wake-ups for targets
whose condition may be open or stale. Neither command bypasses the reconciler's
desired-state check.

---

## 6. State Machines

Intake:

```text
received
  -> inspected_valid
  -> registered
  -> prepared
  -> arranged
  -> submitted
  -> archived
  -> releasable
```

Negative states:

```text
incomplete
quarantined
registration_failed
derivatives_failed
arrangement_invalid
archive_failed
```

Derivative readiness:

```text
not_required
missing
queued
running
ready
failed
stale
```

Arrangement:

```text
draft
pending_derivatives
ready
invalid
submitted
abandoned
```

Submission:

```text
frozen
archiving
archived
failed
```

---

## 7. Verification Rules

### 7.1 Inspect

- Recompute payload sha256 and compare to BagIt manifest.
- Fail closed on missing payload, extra manifest entry, digest mismatch, or
  malformed metadata.
- Do not create catalog truth.

### 7.2 Register

- Refuse invalid/uninspected intake unless `--inspect-now` is explicit.
- Check idempotency against manifest digest.
- Snapshot source path, size, and hash.

### 7.3 Prepare

- Record the requested prepare profile and emit reconciliation wake-ups.
- Reconciler queues only missing derivative requirements after reloading current
  catalog state.
- Preserve retry/regenerate as explicit operator choices.

### 7.4 Arrangement

- Projection files must map to known catalog members.
- Unknown copied-in files are not silently accepted.
- Dirty proxy files do not change master bytes.
- Duplicate archive paths are errors.
- Path normalization is deterministic and recorded.

### 7.5 Submission/archive

- Source-map is immutable once frozen.
- Archive writer verifies each streamed source against the submission snapshot.
- Archive entry path comes from submission member path, not source path.
- Restore by archive path returns original bytes.

### 7.6 VS

- Virtual path changes are history-recorded.
- Reject/hidden states change discovery defaults only; durable copies remain.
- Restore by virtual path resolves through catalog identity, not staging paths.

### 7.7 Reconciliation

- Hot-path reconcilers consume due `reconciliation_wakeup` rows; they do not scan all
  assets every loop.
- A wake-up is only a hint. The reconciler must re-read catalog desired/observed state
  before enqueueing any job.
- Duplicate wake-ups for the same live target and `condition_key` coalesce; different
  requirements for the same target do not.
- A durable desired-state record exists for every operator/policy request that must
  survive queue loss.
- Condition summaries are updated from observed facts and attempt outcomes; reconcilers
  do not scan the attempt log in the hot path.
- The enqueue predicate is explicit: active desired state, eligible condition,
  `next_eligible_at <= now`, no live attempt, not suppressed, not blocked.
- Blocked conditions reopen only when their unblock generation changes
  (`tool_version`, `source_generation`, `policy_generation`, or manual unblock).
- Missed event recovery is proven by scheduled sweeps that can rebuild wake-ups from
  indexed catalog state.
- Sweeps are bounded by domain indexes and watermarks (`status`, `next_eligible_at`,
  `last_verified_at`, profile hash, updated-at), not unconstrained table scans.

---

## 8. Scenario Plan

### R2 - explicit intake lifecycle

```text
receive BagIt intake
inspect valid intake -> valid, no authoritative items
register intake -> items/assets exist
ensure hd-review -> transcode jobs queued
run jobs -> derivatives ready
negative: corrupt BagIt inspect -> quarantined, no registration
negative: repeated register -> idempotent
```

### R3 - event/worklist reconciliation at scale

```text
register intake -> domain_event + reconciliation_wakeup rows written
run derivation reconciler on due wake-ups -> only those targets are evaluated
duplicate register/prepare wake-up -> coalesced, no duplicate live attempt
preview and pfr-index for same item -> separate condition_key rows, not merged
transcode fails transiently -> condition.failed_transient + next_eligible_at backoff
corrupt source blocks transcode -> condition.blocked until source_generation changes
drop/projector-miss a domain_event -> scheduled sweep recreates wake-up from catalog state
profile change -> generation advances; sharded sweep/expander creates affected wake-ups
verification sweep -> wake copies with last_verified_at older than threshold
```

### S0 - arrangement workspace

```text
given registered intake with ready derivatives
create arrangement
project workspace
move proxy file in projected tree
reconcile
assert arrangement_member.member_path changed
assert BagIt data path unchanged
unknown copied-in file -> invalid/quarantined
```

### S1 - submission source-map archive

```text
submit arrangement
assert source-map maps arranged archive paths to original BagIt source paths
archive from source-map without copied staging tree
restore arranged entry
assert sha256 == original master
```

### T - virtual segregation

```text
after archive, update virtual path
assert physical archive locators unchanged
restore by virtual path
assert bytes == original master
reject marker hides from default listing but does not delete copies
```

---

## 9. Migration from Current Implementation

The current implementation may still expose `sutra intake scan` as a combined
verify/register/enqueue compatibility command. Do not break existing harness
coverage abruptly. Migrate in phases:

1. Introduce explicit service functions:
   - `inspect_intake`
   - `register_intake`
   - `ensure_derivatives`
   - `ensure_cloud_temp`
2. Add the reconciliation outbox/worklist spine:
   - append `domain_event` rows for lifecycle/profile/job-completion facts;
   - project events into per-domain `reconciliation_wakeup` rows;
   - make reconcilers consume due wake-ups and re-check catalog truth before enqueueing
     jobs;
   - add bounded scheduled sweeps as missed-event recovery.
3. Add the reconciliation condition model before broad automation:
   - persist desired-state assignments (`prepare_assignment`, placement/freshness
     policy assignments);
   - maintain per-target/per-`condition_key` condition summaries;
   - append job attempts separately from condition state;
   - implement backoff, suppressed, blocked, and generation-based unblock semantics.
4. Keep `scan` as a compatibility wrapper or policy-controlled automation path.
5. Change harness scenarios to prove explicit lifecycle semantics and missed-event
   sweep recovery.
6. Reclassify automation as policy:

```text
intake.registration_mode = manual | auto_on_valid_inspect
derivatives.mode = manual | auto_on_registered
cloud_temp.mode = manual | auto_on_registered
```

Even with automation enabled, the domain events remain explicit and auditable.

---

## 10. Reused vs New

| Area | Status |
|---|---|
| Receive/BagIt | Reused. First-contact evidence exists. |
| Catalog identity | Reused, with lifecycle boundary clarified. |
| Job worker | Reused for derivatives/cloud/archive attempts. |
| RAO/d2 archive machinery | Reused, but should accept source-map entries as canonical input. |
| Intake inspect/register/prepare split | New/refactor from current scan behavior. |
| Arrangement workspace/projection/watcher | New. |
| Archive submission/source-map | New. |
| Virtual namespace/tags/history | New or extended from current `virtual_path`. |

---

## 11. Open Decisions

1. Compatibility naming: keep `scan`, introduce `inspect`, or support both while
   scenarios migrate.
2. Operator default: should `intake accept --prepare hd-review` be the standard
   command, with lower-level commands for admin/debug? (cloud-temp is automatic at
   register, not a flag - §3.4.)
3. Projection v1 mechanics:
   - materialized proxy files over SMB;
   - server-side watcher/reconciler;
   - no client-side FUSE in the first slice.
4. Delete semantics in arrangement: safer default is `excluded` with explicit
   remove-from-workspace, not silent deletion.
5. Source-map archive adapter: land direct source-map support in archive code, or
   first present a virtual tree adapter to existing RAO code.
6. Proxy readiness policy: whether arrangement creation blocks until `hd-review`
   is ready or creates a workspace in `pending_derivatives`.
7. Package normalization (§2.5) — **resolved.** Wrap-once at receive; the archive
   stores the package tar **dense** (rem unchanged; package-blobbing moves from
   archive to receive). `package_globs` + the pinned **`package-tar-v1`** profile live
   in the shared `receive` core, **bundled with the `receive` build** (offline edge),
   with `Package-Profile-Version` + hash in `bag-info.txt`. There is **one** tar
   dialect (receive's) because the archive never re-tars — no shared-dialect problem.
   Receive-layer spec: `design-receive-front-door.md` §12.

---

## 12. Summary

The corrected model is:

```text
BagIt receive gives immutable evidence.
Inspect proves it is valid.
Register accepts it into catalog truth.
Prepare explicitly asks for proxies/cloud work.
Arrangement records pre-archive human layout over review projections.
Submission freezes that layout as archive entry names.
Archive streams original BagIt bytes through a source-map, with no staging copy.
VS edits post-archive logical paths and tags only.
```

This gives operators predictable commands, avoids surprise ffmpeg work from a
passive scan, lets cheap clients sort HD proxies, avoids copying huge masters
just to arrange them, and keeps pre-archive arrangement and post-archive virtual
segregation on one coherent catalog spine.
