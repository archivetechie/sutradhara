# Design - Intake acceptance, arrangement, submission, and virtual segregation

> Status: **design, for review** (2026-06-18).
>
> This document replaces the older "rough segregation on staging" framing with a
> stricter model:
>
> ```text
> inspect   = read-only validation/discovery
> register  = explicit catalog acceptance
> prepare   = explicit derivative/cloud job request
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
| **prepare** | job requests | Explicitly request derivatives and temporary durability work. |
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

### 2.3 Proxy generation is explicit

Proxy/mezz generation should not be a surprise side effect of read-only
inspection.

Use:

```bash
sutra derivatives ensure --intake <intake-id> --profile hd-review
```

This queues only missing work. Retry/regenerate modes are explicit.

Automation can still exist, but it should fire the same explicit domain event:

```text
DerivativesRequested(profile=hd-review, intake_id=...)
```

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

### 3.4 Prepare derivatives and cloud-temp

Preparation explicitly requests work needed for review, arrangement, VS, and
temporary disaster recovery.

```bash
sutra derivatives ensure --intake <intake-id> --profile hd-review
sutra cloud-temp ensure --intake <intake-id>
```

An operator convenience command can compose the steps:

```bash
sutra intake accept <intake-id> \
  --artifactclass s-masters \
  --prepare hd-review \
  --cloud-temp
```

Internally this still emits separate events:

```text
IntakeRegistered
DerivativesRequested(profile=hd-review)
CloudTempRequested
```

For video, `hd-review` should produce:

- preview proxy for quick browse/scrub;
- HD mezz/proxy for human segregation on cheaper clients;
- derivation edges back to the original master;
- locators readable by the projection layer.

For non-video assets, the same profile may mean thumbnail, PDF preview, OCR/text,
audio waveform, or no derivative. Arrangement is generic; video is only one
derivative policy.

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

### 4.3 Derivatives

```text
derivative_requirement
  id
  profile
  media_type
  kind
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

### 4.4 Arrangement

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

### 4.5 Submission

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

### 4.6 Virtual namespace and tags

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
sutra intake accept <intake-id> --artifactclass s-masters --prepare hd-review --cloud-temp
```

REST shape:

```text
POST /api/intakes/{id}/inspect
POST /api/intakes/{id}/register
POST /api/intakes/{id}/accept
```

### 5.2 Derivatives

```bash
sutra derivatives status --intake <intake-id> --profile hd-review
sutra derivatives ensure --intake <intake-id> --profile hd-review
sutra derivatives retry --intake <intake-id> --profile hd-review --failed-only
```

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

- Queue only missing derivative requirements.
- Record requested profile and parameters.
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
2. Keep `scan` as a compatibility wrapper or policy-controlled automation path.
3. Change harness scenarios to prove explicit lifecycle semantics.
4. Reclassify automation as policy:

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
2. Operator default: should `intake accept --prepare hd-review --cloud-temp` be
   the standard command, with lower-level commands for admin/debug?
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
7. Package-boundary profile distribution (§2.5): how the edge gets the synced
   package-glob list (bundled with the `receive` build vs a fetched/pinned profile),
   and the exact `bag-info.txt` key + hash recording it. Plus where the pinned tar
   profile is defined so receive and the archive `.remwrap.tar` share one dialect.

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
