# Glossary

The internal vocabulary of Sutradhara, as the code actually uses it. Each
entry names the defining module so you can check the source. Terms that
appear in older design docs but not in the code are flagged as such.

<!-- code-anchor: packages/sutradhara-receive/src/sutradhara_receive src/sutradhara/intake.py src/sutradhara/catalog/types.py @ df8165b -->
## Receive and intake

**bag / BagIt** — the on-disk form of a received intake: a BagIt 1.0 bag
(`bagit.txt`, `bag-info.txt`, `manifest-sha256.txt`,
`tagmanifest-sha256.txt`, payload under `data/`), with paths canonicalized
per `receive-bagit-path-v2` and RFC 8493 percent-encoding for awkward
names. Defined in `sutradhara_receive.core`.

**landing / landing root** — the staging filesystem where receives arrive
and wait for registration. The landing tree is treated as a durable
queue: `sutra intake watch` polls it, and retention eventually deletes
released landing bytes.

**sentinel (`intake.json`)** — the file written last by a receive. Its
presence means the intake directory is complete; everything before it is
in-progress and sweepable. A `.receiving.json` marker names an intake
still being written.

**intake** — one landing batch admitted into the catalog. Status walks
`receiving` → `verifying` → `registered`, or ends `quarantined`
(`IntakeStatus` in `catalog/types.py`). Registration is idempotent, keyed
on an acceptance fingerprint of manifest digest plus artifactclass. For
device-relayed receives, the row also carries `card_id`/`device_id`
(indexed, copied from the gRPC intake at registration) so a later receive
of the same physical card can be matched against this one — see
"receive intent" below.

**quarantine** — the terminal state of a batch that failed validation
before ever being accepted: the intake row records the failure and no
ingest items are created. Re-quarantining an already-registered intake is
deliberately impossible, so registered truth is never retracted.

**verify.json sidecar** — destination-verification evidence written next
to a completed bag: the receive core re-reads the landed payload against
the manifest and records the result. `sutra receive verify-pending` sweeps
bags whose sidecar is absent, mid-transfer, or failed.

**releaseSafe / release safe** — whether the source medium may be
ejected. True only for card sources whose intake has reached committed,
verifying, or verified state (`grpc/status.py:release_safe_for_status`);
surfaced as `releaseSafe` in the device API and as `CARD SAFE TO REMOVE`
on the CLI.

**road mode** — not a term in this repo's code. It is the Rust
`~/sutra-agent` name for offline card-to-external-disk offload; on this
side it is just a normal receive whose landing arrives later.

**disposition (content novelty)** — the per-`IngestItem` verdict on
whether this occurrence's bytes were already known durable:
`new`, `known_durable`, `known_under_durable`, `reverified`, or the
reserved-but-currently-unassigned `legacy_unknown`
(`IngestDisposition` in `catalog/types.py`, computed in
`intake.py::_classify_disposition`). It is recorded with the policy
generation and evidence it was computed against, plus a `prior_intake_id`
pointing at the most recent earlier registered intake for the same
asset. This is the durable half of the receive-time "nothing new"
check — see "duplicate warning" under
[Relay and enrollment](#relay-and-enrollment) for the live, pre-registration
half computed by `receive_novelty.py`.

<!-- code-anchor: src/sutradhara/catalog/models.py src/sutradhara/durability.py @ df8165b -->
## Catalog identities

**logical asset** — content identity. One row per distinct SHA-256; the
hash is the primary key (`LogicalAsset` in `catalog/models.py`). "Losing
the database is not a data-loss event" works because assets are keyed by
their own bytes.

**ingest item** — occurrence identity: one appearance of an asset within
one intake, carrying the as-received path, provenance, and a content-novelty
**disposition** (see [Receive and intake](#receive-and-intake))
(`IngestItem`). The same bytes on two cards yield one logical asset and
two ingest items. These are "the two identities" the design docs mention.

**copy** — one stored realization of a logical asset or a bundle on one
backend (`Copy`). Exactly one of `logical_asset_hash` / `bundle_id` is
set. Health is `ok`, `suspect`, `corrupt`, or `missing`; retention
tombstones copies via `deleted_at` instead of deleting rows.

**bundle / bundle member** — a synthetic archive object packing one or
more assets for tape efficiency (`Bundle`, `BundleMember`). Bundles are
built open, sealed at flush, and stored as bundle-scoped copies.

**asset locator** — the per-asset pointer into a (possibly bundle-scoped)
copy: pool, member path, representation (`AssetLocator`). It is what
lets one bundle copy count as durable coverage for each member asset.

**copy grain** — the distinction between whole-asset `Copy` rows and
bundle-scoped coverage reached through `AssetLocator`. `durability.py`
computes placement status across both grains explicitly so nothing
conflates them.

**derivation** — a provenance edge from a source item to a derived item
(`AssetDerivation`), e.g. a mezzanine or preview transcode. Derived items
are ordinary ingest items with their own artifactclass.

**validity vs fixity** — fixity is "are the stored bytes intact"
(copy health, integrity hashes); validity is "does the content decode"
(`AssetValidity` on the asset: `ok`, `suspect`, `unvalidated`). Archive
everything, flag validity, gate restore — never preservation.

**suspect** — the sticky attention flag. A scrub hash-conflict marks a
copy `suspect`; decode trouble marks an asset's validity `suspect`.
Restores refuse suspect assets unless forced (`--force`).

**reject** — a content-level governance marker on a logical asset
(`rejected_at/by`, `rejection_reason`). It gates restore (`--force-rejected`
to override), never deletion or preservation. `sutra unreject` clears it.

**tag** — a soft-deleted governance label on an asset (`AssetTag`);
removal tombstones the row for audit.

<!-- code-anchor: src/sutradhara/artifactclass_policy.py src/sutradhara/catalog/models.py @ df8165b -->
## Policy and placement

**artifactclass** — the policy class of a piece of content (e.g. masters
vs proxies). It decides which pools copies go to, bundling targets,
restore preference order, staging transforms, hdcache eligibility, and
privacy level. Policies are strict TOML documents (unknown keys are
errors) applied with `sutra archive artifactclass apply`.

**pool** — the storage-policy surface a backend exposes
(`Pool`): representation, location, offsite gate, write fence
(`accepts_writes`), retired flag. Copies record the pool that wrote them.

**placement** — one desired pool target for an asset or bundle under a
policy; "placements" in the policy TOML name the pools and roles.

**pool naming like `o-copy-1-pool` / `n-copy-3`** — convention only:
"copy 1/2/3" in a pool id means the first/second/third copy of a
multi-copy recipe. There is no numeric copy column; the mapping lives in
policy documents and the Scenario O/N shims (`sealing/policy.py`).

**durability floor** — the write-eligibility rule validated at
policy-apply and pool-fence changes: by default at least `min_copies = 3`
copies across at least `min_impl_families = 2` implementation families
(`DurabilityPolicy` in `artifactclass_policy.py`).

**implementation family** — the failure-correlation group of a backend
kind: `tape` (rem), `d2tape`, `disk`, `cloud`, `memory`
(`BACKEND_IMPLEMENTATION_FAMILIES` in `catalog/types.py`). Two copies in
one family don't satisfy a two-family floor.

**offsite gate** — a pool flag meaning copies there only count for
retention release after their tape's media id has an operator-recorded
`OffsiteConfirmation` (`sutra offsite confirm`).

**cloud-temp / cloud blob** — the per-intake disaster-recovery blob
uploaded at registration to the `cloud-temp` backend/pool (an encrypted
RAO of the whole intake). Temporary by design: the retention gate deletes
it once durable copies are proven.

<!-- code-anchor: src/sutradhara/sealing src/sutradhara/keys/registry.py @ df8165b -->
## Sealing

**representation** — the stored form of a copy: `raw-bytes`,
`rao-plain-v1`, `rao-aead-v1`, or `d2tar-raw` (`Representation` in
`sealing/port.py`).

**RAO** — Remanence Archive Object, the container format the `rem` CLI
builds. In this repo "the RAO codec" means `RaoCliSealer`/`RaoCliOpener`:
a stateless local wrapper around `rem archive build/extract`, never a
daemon.

**sealer / opener** — the ports that convert plaintext to stored form and
back (`sealing/port.py`). Every restore and self-heal goes through an
opener, so no path can skip verification.

**key epoch** — one named encryption key in the local `KeyRegistry`.
Encrypted copies record their epoch; epochs are domain-tagged (`archive`
vs `hdcache`); retiring an epoch stops new writes but never deletes key
material. Root keys are only ever materialized to disk in short-lived
`0600` files for the duration of one `rem` call, and are best-effort
zeroized before removal.

<!-- code-anchor: src/sutradhara/arrangement.py src/sutradhara/virtual_arrangement.py @ df8165b -->
## Arrangement

**arrangement** — the mutable pre-archive workspace: registered masters
placed at archive paths, movable and excludable until submit
(`Arrangement`, `ArrangementMember`). One arrangement can be cloned from
another (`sutra arrangement create --from-arrangement`); the clone
records its lineage (`cloned_from_arrangement_id`) — this is how you
revise a terminal submission's contents without touching the frozen one.

**submission / source-map** — the frozen output of `arrangement submit`:
an immutable, validated, ordered `source-map.tsv`
(`archive_path ← source_path, sha256, size, ingest_item_id`) written
file-first under `/replica/submissions/<id>/` and mirrored to
`submission_member` rows. Submissions are terminal; revise by cloning the
arrangement.

**virtual arrangement** — the post-archive, permanently mutable
organizational view (`VirtualArrangement`). Members key on
`(logical_asset_hash, artifactclass)` at a virtual path; every edit is
catalog-only. Older docs call this "virtual segregation" or "VS" — same
thing, renamed 2026-06-27.

<!-- code-anchor: src/sutradhara/jobs @ df8165b -->
## Jobs and reconciliation

**job / job attempt** — a `Job` row is one unit of work dispatched by
`kind`; every run appends an immutable `JobAttempt` transcript. Jobs are
ephemeral attempts — intent lives in the catalog.

**lease** — an in-memory counted reservation against a resource pool
(`cpu`, `io`, `tape_drive`, `gpu`) that admits a job to the single-node
worker (`jobs/leases.py`). Admission control, not enforcement;
enforcement is cgroups via resource control.

**dedupe key** — an idempotency key on submit, unique among live jobs
only, so retrying a submit while the job is pending or running returns
the existing job.

**reconciler / domain** — a registered observer-actor for one kind of
desired state (`copy`, `bundle_copy`, `derivation`, `hdcache`,
`log_pipeline`, `restore_open`). Level-triggered: each bounded cycle
re-observes reality and enqueues at most one live job per target — except
`log_pipeline` and `restore_open`, which are alarm/state-only and never
call `submit` (`restore_open` reopens an agent-delivery restore item
whose lease expired before the device finished; see "The restore path"
in `architecture-overview.md`).

**condition** — the durable `(domain, target_key)` row recording the gap
between desired and observed (`ReconciliationCondition`). Two axes:
observation (creates rows; `satisfied` vs `open`) and attempt outcome
(`backoff` with exponential due times, escalating to `blocked` after 3
failures). Blocked rows need `--reopen-blocked` or a recorded
tool-version bump.

**gap board** — the operator view of open conditions and hdcache alarms,
served by `/api/ui/reconciliation`. hdcache degradation (reserve
pressure, lost backlog, walker trouble) is published as alarm conditions
in the same shape (`hdcache/alarms.py`).

**seam** — an injectable code boundary, usually named so tests can
substitute it (e.g. "the `rem.tape.write_object` seam", "the hdcache read
seam" around `resolve_read_source`). An architecture term, not a data
model term.

<!-- code-anchor: src/sutradhara/scrub.py src/sutradhara/replication.py src/sutradhara/retention.py @ 5c44b85 -->
## Verification and lifecycle

**scrub** — re-enumerating a backend and reconciling it against the
catalog: verify matches, insert unknowns, mark absentees `missing`, flag
hash conflicts `suspect`. Never deletes. The working demonstration that
the index is rebuildable.

**self-heal** — recovering a missing or unhealthy placement by opening a
healthy copy and re-sealing it to the gap (`replication.self_heal`,
driven by the copy and bundle-copy reconcilers).

**retention states** — `held` → `released` → `purged` on the intake. The
gate (`sutra retention run`) releases only when every recipe copy is
verified and offsite-confirmed where required and nothing still depends
on the landing bytes; `sweep-staging` deletes landing bytes after the
grace period (default 30 days). Retention is the only code that deletes
bytes.

**PFR (partial file restore)** — restoring a clip or byte range without
reading the whole stored object: a `pfr-index-v1` container-index sidecar
plus Remanence byte-range reads, with a fallback ladder to whole-member
restore (`pfr.py`, `sutra pfr`).

<!-- code-anchor: src/sutradhara/hdcache @ df8165b -->
## HD cache

**hdcache** — the expendable disk cache tier: independent JBOD disks in
front of tape, never part of durability. Cache disks and entries live in
their own tables (`cache_disk`, `cache_entry`), never as backends or
copies.

**anti-affinity / spread** — the placement rule that files at or above
the spread threshold (default 1 GiB) from the same bundle or group land
on different disks, so restores stream in parallel.

**walker** — the cache-local scrub: reconcile one disk's files against
its entries. Unknown file → delete; missing entry → repopulate. Guarded
by HMAC disk-identity sentinels and a mass-delete tripwire.

**retire vs dead** — `retire` marks a disk retiring and drains it through
verified local reads while entries stay servable; `dead` marks it gone
now, flips entries to `lost`, and starts a tape-grouped repopulation
drill. `forget` tombstones a drained dead disk's id.

**privacy level (`p2`/`p3`)** — an artifactclass's hdcache privacy tier.
Restoring cached private material requires the mapped capability
(`can_restore_p2`/`can_restore_p3`), fail-closed for unmapped levels.

**verified cache producer** — the bounded, digest-and-size-reverifying
chunk producer (`hdcache/manager.py::open_verified_cache_plaintext`)
that both restore delivery modes read cache hits through, so a cache
read is never less verified than an archival restore — see "The restore
path" in `architecture-overview.md`.

**disk circuit breaker** — a process-local, per-disk failure tripwire on
the restore-serving path: after enough cache-read failures within a
window, the disk is treated as down (skip straight to fallback) until a
recovery probe succeeds, so a wedged disk isn't retried into the ground.

<!-- code-anchor: src/sutradhara/grpc src/sutradhara/api/routes_devices.py @ 3d8310c -->
## Relay and enrollment

**agent / helper** — the Rust `sutra-agent` workstation program (separate
repository) that discovers local cards/drives and connects outbound to
the server over mTLS.

**relay** — the server-brokered path browser → server → bidi gRPC stream
→ agent. The browser never talks to localhost; the server relays receive
commands and folder listings over the device's `Connect` stream.

**enrollment / `.sutra-enroll` bundle** — certificate-based device
onboarding: a minted token plus device CSR yields an mTLS cert with
`CN = device_id`. The `.sutra-enroll` file (`sutra-enroll-bundle-v1`)
packages token, enroll URL, CA PEM, and endpoints for one-click setup;
`POST /api/enroll/bundle` produces it when `SUTRA_AGENT_BUNDLE_CONFIG` is
set.

**receive intent** — the durable per-request row (`IdempotencyRecord` in
`api/store.py`, extended by migration `d4e5f6a7b8c9`) tracking one
device-relayed receive attempt from first request to terminal outcome:
`warned → authorized → started → terminal(committed|aborted|quarantined|
failed)`. It is the same table that already handled plain HTTP
idempotency replay; receive-dedup adds the card/device identity and
duplicate-warning columns to it rather than introducing a parallel one.

**duplicate warning (409 handshake)** — `POST /api/devices/{id}/receive`
checks the requested card's identity against its receive history
(`api/receive_history.py:latest_card_history`) before starting a
transfer. A match returns HTTP 409 with a `duplicateWarning` body (prior
intake id, label, device, timestamp, state); the client resubmits with
`acknowledge_duplicate: true` to proceed anyway. `sutra-agent`/the
console are expected to surface this as a confirmation prompt, never a
silent retry.

**source lease** — a `SourceClaim` row (`card-identity:<card_id>`)
admitted at `authorized`, distinct from the job-engine "lease" in
[Jobs and reconciliation](#jobs-and-reconciliation). It prevents two
concurrent receives of the same physical card; a second request while one
is in flight gets a `source_busy` 409 instead of racing the first.

**archive_state** — an additive read-model field on intake rows
(`api/routes_intake_archive.py`, `archiveSemantics: 2`) computed with
ALL-semantics: `none` | `partial` | `complete`, true only when every
ingest item's content hash is sealed into a bundle or an archived
submission. Coexists with the older `archived` boolean, which stays
any-semantics until a later phase retires it.
