# sutra CLI reference

Every command group and leaf command of the `sutra` CLI, with arguments,
flags, and defaults. Everything here was captured from the live `--help`
output of `.venv/bin/sutra` (version 0.0.1) and cross-checked against
`src/sutradhara/cli/` at the commit in each section's anchor. If this page
and the CLI ever disagree, trust `--help` and file a doc fix.

For environment variables (database URL, key registry, hdcache tuning, and
the rest) see [`reference-config.md`](reference-config.md). For what the
pieces mean, see [`reference-glossary.md`](reference-glossary.md) and
[`architecture-overview.md`](architecture-overview.md).

<!-- code-anchor: src/sutradhara/cli/main.py src/sutradhara/catalog/session.py @ 5688438 -->
## Conventions

- The CLI lives in the project virtualenv: `.venv/bin/sutra`, or
  `uv run sutra ...`.
- Every command talks to the catalog database named by `SUTRADHARA_DB_URL`
  (default `sqlite:///./sutradhara.db`, a SQLite file in the current
  directory).
- Listing and inspection commands take `--json` to emit machine-readable
  output instead of a table.
- Exit codes are 0 on success and 1 on error unless a command documents
  otherwise. The exceptions that exist today: `sutra receive` exits 3 when
  the source could not be confirmed safe to release and 4 on destination
  verification failure; `sutra receive verify-pending` exits 4 when any bag
  fails verification; `sutra reconcile` exits 2 for an unknown domain.

## Command map

`sutra --help` lists 25 top-level entries. In lifecycle order:

| Command | Purpose |
|---|---|
| `sutra receive` | Receive source trees into landing intakes. |
| `sutra intake` | Inspect and register landing intakes. |
| `sutra prepare` | Record a prepare profile for the derivation reconciler. |
| `sutra arrangement` | Create, edit, inspect, and submit arrangement workspaces. |
| `sutra archive` | Archive artifactclass policy, bundles, review, and restore. |
| `sutra review` | Show or record a held-bundle review decision (same as `sutra archive review`). |
| `sutra virtual` | Create, edit, inspect, and restore virtual arrangements. |
| `sutra tag` | Manage content-level governance tags. |
| `sutra reject` / `sutra unreject` | Set or clear the reject marker on one logical asset. |
| `sutra list` | Query the catalog. |
| `sutra reconcile` | Run one bounded reconcile cycle for a domain. |
| `sutra jobs` | Submit, run, and inspect jobs. |
| `sutra worker` | Run the single-node lease-aware job worker. |
| `sutra backends` | Register and inspect storage backends. |
| `sutra scrub` | Re-enumerate a backend and reconcile against the catalog. |
| `sutra hdcache` | Manage the expendable HD cache disk tier. |
| `sutra retention` | Run the evidence-gated release/sweep funnel and corrections. |
| `sutra offsite` | Record or revoke offsite media confirmations. |
| `sutra pfr` | Partial file restore sidecars and cuts. |
| `sutra serve` | Serve the operator HTTP API and mTLS gRPC relay in one process. |
| `sutra serve-api` | Serve the operator API alone (Unix socket by default). |
| `sutra serve-grpc` | Serve streaming intake over gRPC+mTLS, or run enrollment admin actions. |
| `sutra db` | Schema management (dev convenience; production uses alembic). |
| `sutra admin` | Dangerous local catalog maintenance. |

<!-- code-anchor: src/sutradhara/cli/receive.py packages/sutradhara-receive/src/sutradhara_receive @ 5688438 -->
## sutra receive

Receive source trees (cards, drives, folders) into landing intakes as BagIt
bags. The group has a quirk worth knowing: `sutra receive SOURCE` dispatches
to a hidden `run` subcommand, so the common invocation looks like a leaf
command even though `receive` is a group. The implementation lives in the
`sutradhara-receive` package (Rust core with a Python wheel); the CLI here
is a thin wrapper.

### sutra receive [SOURCE]

Receive SOURCE into a landing share using a sentinel-last filesystem
contract: payload first, `intake.json` sentinel last, so a directory with a
sentinel is always complete.

| Flag | Default | Meaning |
|---|---|---|
| `--landing DIRECTORY` | required | Landing share where the completed intake directory will appear. |
| `--source-kind [card\|drive\|upload\|handoff\|download\|other]` | required | Physical or transfer source category. |
| `--source-ref TEXT` | | Operator-visible source identifier. |
| `--artifactclass TEXT` | `default` | Artifactclass for items. |
| `--label TEXT` | | Human label for this intake. |
| `--operator TEXT` | `$USER` | Operator name included in the intake id and sentinel. |
| `--resume TEXT` | | Resume a named sentinel-less intake id. |
| `--fake-source DIRECTORY` | | CI/harness source directory used instead of a device adapter. |
| `--confirm-timeout FLOAT` | | Poll for server confirmation before reporting source release as safe. |
| `--confirm-interval FLOAT` | `1.0` | Seconds between server confirmation polls. |
| `--verify [staged\|blocking]` | `staged` | Destination verification mode: `staged` releases the source after the copy and verifies in the background; `blocking` verifies before release. |
| `--json` | off | Emit JSON summary. |

Prints `CARD SAFE TO REMOVE` once release is safe. Exits 3 if release
safety could not be confirmed, 4 on a verification error.

### sutra receive sweep / sweep-orphans

Remove stale sentinel-less receive directories (`sweep` is an alias for
`sweep-orphans`). Flags: `--landing DIRECTORY` (required),
`--older-than-hours FLOAT` (default `24.0`), `--json`.

### sutra receive verify-pending

Verify completed bags whose destination-verification sidecar is absent,
mid-transfer, or failed. Flags: `--landing DIRECTORY` (required,
repeatable), `--json`. Exits 4 when any bag fails.

<!-- code-anchor: src/sutradhara/cli/intake.py src/sutradhara/intake.py @ 5688438 -->
## sutra intake and sutra prepare

Landing intakes cross the acceptance boundary here: `inspect` validates
without writing, `register` admits an intake into the catalog, `accept`
combines register with recording a prepare profile, and `watch` runs the
registrar as a polling loop. `sutra prepare` records the derivation profile
for an already-registered intake.

### sutra intake inspect PATH

Validate an intake directory or landing root without catalog writes.
Flags: `--json`.

### sutra intake register INTAKE_ID

Explicitly accept one completed intake into the catalog.

| Flag | Default | Meaning |
|---|---|---|
| `--landing-root DIRECTORY` | | Landing root containing INTAKE_ID. If omitted, INTAKE_ID may be a path. |
| `--artifactclass TEXT` | | Required for legacy/non-bag intakes. |
| `--cache-root DIRECTORY` | | Directory for register-time cloud-temp work. |
| `--cloud-backend TEXT` | `cloud-temp` | Backend name for intake cloud blob jobs. |
| `--cloud-pool TEXT` | `cloud-temp` | Pool id for intake cloud blob jobs. |
| `--json` | off | Emit JSON summary. |

### sutra intake accept INTAKE_ID

Register one intake and optionally record a prepare profile. Same flags as
`register` plus `--prepare TEXT` (prepare profile to record).

### sutra intake watch

Poll a landing root and register completed intakes. This is the server-side
registrar that treats the landing filesystem as the durable queue.

| Flag | Default | Meaning |
|---|---|---|
| `--landing-root DIRECTORY` (alias `--landing`) | required | Landing root to scan for completed intakes. |
| `--once` | off | Scan/process once and exit. |
| `--interval FLOAT` | `5.0` | Poll interval in seconds. |
| `--settle-seconds FLOAT` | `2.0` | Snapshot settle time before an intake counts as stable. |
| `--stable-polls INTEGER` | `2` | Stable snapshots required before processing. |
| `--validation-attempts INTEGER` | `2` | Validation retries before quarantine. |
| `--artifactclass TEXT` | | Required only for legacy non-BagIt intakes. |
| `--prepare TEXT` | | Prepare profile to record. |
| `--cache-root DIRECTORY` | | Directory for lock and register-time cloud-temp work. |
| `--cloud-backend TEXT` | `cloud-temp` | Backend name for intake cloud blob jobs. |
| `--cloud-pool TEXT` | `cloud-temp` | Pool id for intake cloud blob jobs. |
| `--json-lines` | off | Emit one JSON object per event. |

### sutra prepare INTAKE_ID

Record a prepare profile for the derivation reconciler. Flags:
`--profile TEXT` (required), `--json`. The profile is validated against the
code registry in `src/sutradhara/jobs/reconcilers/profiles.py`; the
derivation reconciler picks the desired state up on its next cycle.

<!-- code-anchor: src/sutradhara/cli/arrangement.py src/sutradhara/arrangement.py @ 5688438 -->
## sutra arrangement

Arrange registered masters into an archive namespace, then freeze the
result into an immutable source-map submission. Arrangements are mutable
workspaces; submissions are terminal (revise by cloning, not resubmitting).

| Command | Arguments | Purpose |
|---|---|---|
| `create` | | Create a draft arrangement from a registered intake (`--from-intake TEXT`) or clone an existing one (`--from-arrangement INTEGER`). `--label TEXT` required. |
| `list` | | List arrangement workspaces. |
| `show` | `ARRANGEMENT_ID` | Show one arrangement and its members. |
| `mv` | `ARRANGEMENT_ID FROM_PATH TO_PATH` | Move one active member to a new archive path. |
| `exclude` | `ARRANGEMENT_ID MEMBER_PATH` | Exclude one active member from submit output. |
| `submit` | `ARRANGEMENT_ID` | Freeze an arrangement into a source-map submission. `--submission-root DIRECTORY` (default `/replica/submissions`), `--submitted-by TEXT`. |

`create`, `list`, `show`, and `submit` accept `--json`.

<!-- code-anchor: src/sutradhara/cli/archive.py src/sutradhara/archive_restore.py src/sutradhara/artifactclass_policy.py @ 5688438 -->
## sutra archive

Artifactclass policy, durable bundles, held-bundle review, archiving frozen
submissions, and asset restore.

### sutra archive predicate-audit

Write the read-only preservation-gap audit as a schema-versioned JSON
artifact. `--output FILE` is required; an existing file is refused unless
`--force` is supplied. It reports every retention-passed intake whose archive
state is only *partial* — some of its material is on sealed, sufficiently
replicated media and some is not — which is the shape a preservation gap
takes. `summary.clean: true` means no such intake exists.

### sutra archive artifactclass apply ARTIFACTCLASS POLICY_PATH

Strict-validate and apply an artifactclass TOML policy. Unknown keys are an
error. The policy document names the placements (pools), bundling targets,
restore preference order, and optional staging/hdcache/durability sections;
applying it also validates the durability floor (default: at least 3 copies
across at least 2 implementation families).

### sutra archive bundle enqueue ARTIFACTCLASS ASSET_HASH_HEX SOURCE_PATH

Stage and add an existing logical asset to an artifactclass open bundle. This
is a wrapper over a one-member enqueue batch, so the class ruleset scan runs
at batch grain and the reported bundle is the one the member actually landed
in (an oversized member routes include-alone to its own bundle).

| Flag | Default | Meaning |
|---|---|---|
| `--scan-root DIRECTORY` | required | Source tree root the class ruleset is written against. `SOURCE_PATH` must live under it. |
| `--member-path TEXT` | scan-root-relative path | Override the name stored inside the archive. |
| `--staging-dir DIRECTORY` | alongside the source | Directory for copy-on-write staging transforms. |
| `--rem-bin TEXT` | `rem` | rem CLI binary. |

`--scan-root` is required rather than derived. Conformance rules match paths
*relative to the scan root*, so deriving the root from the file itself would
hand rem a directory under which `proxies/x.mov` is merely `x.mov`: a rule
scoped `proxies/**` would never fire and the member would be archived under a
rule that says not to archive it. Only the operator knows which tree the
class's ruleset was written against, so the command asks.

### sutra archive bundle enqueue-intake INTAKE_ID

Enqueue a registered intake's items as batches, one ruleset scan per
(artifactclass, tree root) — the root being the intake manifest's
`…/data` directory. Flags: `--staging-dir DIRECTORY`, `--rem-bin TEXT`
(default `rem`).

### sutra archive bundle flush BUNDLE_ID

Force-flush one open bundle to its bundle group's basis pools.

Force-flush is **group grain**, not class grain. The bundle is the accumulator
for every artifactclass whose storage placement matches, so sealing it here
seals other classes' material too — at whatever fill it has reached. Before
sealing, the command prints the accumulator's current fill (bytes against the
group's effective target) and the member classes the flush will seal, because
an operator sealing a 12%-full accumulator to get one class's material onto
tape is spending the whole group's tape efficiency to do it.

| Flag | Default | Meaning |
|---|---|---|
| `--deliverables-dir DIRECTORY` | | Directory for customer manifest receipts. |
| `--rem-bin TEXT` | `rem` | rem CLI binary. |
| `--key-epoch TEXT` | | Key epoch for rao-aead-v1 pools. |
| `--manifest-signing-key-file FILE` | | Raw HMAC key file for customer manifest receipts. |

### sutra archive bundle sweep

Run one sweeper pass. In order: reap `flushing` bundles whose claimer is gone,
void-seal empty orphan accumulators whose bundle group no live policy derives
any more, drain the non-empty ones, then flush every open bundle `bundle_due`
says is due — open funnel bundles (include-alone, cloud-blob) included, since
they have no flusher of their own.

A bundle the reaper returned to `open` is flushed on the same pass without
re-asking whether it is due: it was already judged flush-worthy when it was
claimed, and it can no longer take members.

This is the only caller of the age arm: a quiet class's accumulator seals here
or not at all. The same pass runs as the `bundle-sweep` job. A flush failure is
reported per bundle and exits non-zero without stopping the rest of the pass;
each bundle's flush is transactionally its own, so a failed one leaves nothing
behind — it is back to `open` and un-claimed, and the next pass retries it. A
failure *after* bytes reach a pool is different: that pool's copy is recorded
`suspect`, the bundle still seals with the copies that succeeded, and the
bundle-copy condition goes to `blocked` for a human, because retrying a tape
write appends a second object rather than replacing the first.

| Flag | Default | Meaning |
|---|---|---|
| `--rem-bin TEXT` | `rem` | rem CLI binary. |
| `--key-epoch TEXT` | | Key epoch for rao-aead-v1 pools. |
| `--no-reap` | off | Skip the stuck-claim reaper (diagnosis only). |

### sutra archive submission accumulate SUBMISSION_ID

Append one arrangement submission's members to its bundle group's open
accumulator. The submission no longer builds an object of its own: this
command writes catalog rows and touches no backend, and the sweeper seals the
accumulator when it is full or overdue. It reports the bundles the submission's
members currently sit in, their statuses, and whether the submission is
`archived` — which is derived, and true only once every member sits in a sealed
bundle with enough verified copies.

### sutra archive restore [ASSET_HASH_HEX]

Restore one asset using artifactclass pool preference.

| Flag | Default | Meaning |
|---|---|---|
| `--artifactclass TEXT` | required | Artifactclass restore policy. |
| `--dest FILE` | required | Destination path (written atomically after verification). |
| `--rem-bin TEXT` | `rem` | rem CLI binary. |
| `--member-name TEXT` | | Escaped customer manifest member name (resolve the asset by name instead of hash). |
| `--force` | off | Restore even when the logical asset is flagged suspect. |
| `--force-rejected` | off | Restore even when the logical asset is rejected. |
| `--privacy-override TEXT` | | Trusted CLI reason for restoring private hdcache assets without API grants. |

### sutra archive review BUNDLE_ID (also: sutra review)

Show or record a held-bundle review decision. With no `--action` it prints
the held summary.

| Flag | Default | Meaning |
|---|---|---|
| `--action [wrap\|blob\|exclude\|fix-source-and-rescan\|abort]` | | Record a review action. |
| `--scope [just-this-ingest\|persist-rule]` | `just-this-ingest` | Whether the decision applies once or persists as a rule. |
| `--subtree TEXT` | | Subtree/prefix this action covers. |
| `--why TEXT` | | Reason for the review decision. |
| `--who TEXT` | | Reviewer/operator name. |

<!-- code-anchor: src/sutradhara/cli/virtual.py src/sutradhara/virtual_arrangement.py @ 5688438 -->
## sutra virtual, tag, reject, unreject

Post-archive organization and governance. Virtual arrangements are named,
permanently mutable views over archived assets; every edit is catalog-only
and never touches stored bytes. Tags and reject markers are content-level:
they gate restore, never preservation, and nothing here deletes anything.

### sutra virtual

| Command | Arguments | Purpose |
|---|---|---|
| `create` | `NAME` | Create a virtual arrangement view. `--description TEXT`, `--created-by TEXT`, `--json`. |
| `add` | `NAME ASSET_HASH_HEX PATH` | Place one archived asset in the view. `--artifactclass TEXT` (required when the hash is archived under several classes), `--added-by TEXT`. |
| `ls` | `NAME` | List members. `--all` includes excluded/rejected members. `--json`. |
| `show` | `NAME` | Show the view and its members. `--json`. |
| `mv` | `NAME FROM_PATH TO_PATH` | Move one active member. `--actor TEXT` recorded on the history row. |
| `exclude` | `NAME PATH` | Hide one active member (reversible). |
| `include` | `NAME PATH` | Re-show one excluded member. |
| `restore` | `NAME PATH` | Restore one member by virtual path. `--dest FILE` (required), `--rem-bin TEXT` (default `rem`), `--force` (suspect), `--force-rejected`, `--privacy-override TEXT`. |

### sutra tag / reject / unreject

| Command | Arguments | Purpose |
|---|---|---|
| `tag add` | `TAG ASSET_HASH_HEX` | Add one governance tag. `--actor TEXT`. |
| `tag rm` | `TAG ASSET_HASH_HEX` | Soft-delete one governance tag (the row is tombstoned, not removed). `--actor TEXT`. |
| `reject` | `ASSET_HASH_HEX` | Reject one logical asset without deleting it. `--reason TEXT`, `--actor TEXT`. |
| `unreject` | `ASSET_HASH_HEX` | Clear the reject marker. |

<!-- code-anchor: src/sutradhara/cli/assets.py @ 5688438 -->
## sutra list

Catalog queries. Currently one subcommand:

- `sutra list assets` — list logical assets. `--limit INTEGER` (default
  `50`, `0` = unlimited), `--json`.

<!-- code-anchor: src/sutradhara/cli/reconcile.py src/sutradhara/jobs/reconcilers @ 5688438 -->
## sutra reconcile DOMAIN / record-fix

Run one bounded reconcile cycle for DOMAIN: observe desired state, discover
gaps, and enqueue the jobs that close them. The registered domains are
`copy`, `bundle_copy`, `derivation`, `hdcache`, `log_pipeline`, and
`restore_open` (reopens an agent-delivery restore item whose lease expired
before the device finished — an alarm/state-only domain that never
enqueues a job). An unknown domain exits 2.

After an operator fixes one recorded component, `sutra reconcile record-fix
COMPONENT --note TEXT` reopens at most 100 matching blocked conditions by
default. Matching is exact against the component snapshot captured when each
condition parked; `--limit INTEGER` may select a batch from 1 through 1000.
The reopen audit message records the local actor and supplied note.

| Flag | Default | Meaning |
|---|---|---|
| `--batch INTEGER` | `1000` | Discover batch size. |
| `--cursor INTEGER` | | Ingest-item id cursor for discovery. |
| `--limit INTEGER` | `100` | Process work limit. |
| `--cache-root DIRECTORY` | | Override the derivation cache root for this run. |
| `--list-blocked` | off | List blocked conditions for DOMAIN instead of reconciling. |
| `--reopen-blocked` | off | Reopen blocked conditions for DOMAIN. |
| `--reason TEXT` | | Filter `--reopen-blocked` by reason. |
| `--note TEXT` | | Required with `record-fix`; audited operator note. |

<!-- code-anchor: src/sutradhara/cli/jobs.py src/sutradhara/cli/worker.py src/sutradhara/jobs @ 5688438 -->
## sutra jobs and sutra worker

Direct job control and the worker loop. Most jobs are created by
reconcilers; `jobs submit` exists for operators and tests.

### sutra jobs

| Command | Arguments | Purpose |
|---|---|---|
| `list` | | List jobs. `--status [pending\|queued\|running\|succeeded\|failed\|cancelled]`, `--limit INTEGER` (default `50`, `0` = unlimited), `--json`. |
| `show` | `JOB_ID` | Print full detail for one job. |
| `run` | | Run pending jobs synchronously. `--id INTEGER` runs one specific job; `--limit INTEGER` (default `1`, `0` = drain queue) otherwise. |
| `submit` | `KIND` | Submit a new job. `-p/--param key=value` (repeatable, values JSON-decoded when possible), `--resource pool=count` (repeatable), `--prereq INTEGER` (prerequisite job id), `--not-before TEXT` (ISO-8601 UTC), `--priority INTEGER` (default `0`, lower runs earlier), `--dedupe-key TEXT` (idempotency key for submit retries). `KIND` must be a registered kind; `restore` is explicitly rejected here (exit 2, "restore jobs must be created from gated restore requests") — server-local restores are submitted only through `POST /api/ui/restores`, never directly. |

### sutra worker

Run the single-node lease-aware job worker that drains pending jobs under
counted resource pools.

| Flag | Default | Meaning |
|---|---|---|
| `--once` | off | Drain currently eligible jobs and exit. |
| `--pools TEXT` | | Override counted pool capacity, e.g. `--pools cpu=8 --pools io=2`. Repeatable. |

<!-- code-anchor: src/sutradhara/cli/backends.py src/sutradhara/backend/factory.py @ 5688438 -->
## sutra backends

Register and inspect storage backends and manage pool write fences.

### sutra backends add NAME

| Flag | Default | Meaning |
|---|---|---|
| `--kind [rem_tape\|d2_tape\|rem_disk\|plain_disk\|ssh_disk\|s3\|gcs\|azure_blob\|memory]` | required | Backend kind. |
| `--tier [self_describing\|catalog_authoritative]` | `self_describing` | Self-describing (rebuildable by enumeration) vs catalog-authoritative. |
| `--fixture FILE` | | Dev fixture file path for rem_tape tests/scrubs. |
| `--config TEXT` | | `key=value` (repeatable). Values are JSON-decoded if possible, else strings. |
| `--library-uuid TEXT` | | Set `config.library_uuid` for a specific Remanence library. |

### The rest

| Command | Arguments | Purpose |
|---|---|---|
| `list` | | List registered backends. `--json`. |
| `set-pool-retired` | `POOL_ID` | Set a pool's descriptive retired flag. `--retired/--active` (required). |
| `set-pool-writes` | `POOL_ID` | Set a pool's write fence with durability-floor validation. `--accepts-writes/--no-accepts-writes` (required), `--force` overrides a durability-floor drain refusal and records an alarm. |

<!-- code-anchor: src/sutradhara/cli/scrub.py src/sutradhara/scrub.py @ 5688438 -->
## sutra scrub

Re-enumerate a backend and reconcile it against the catalog: matching
copies get `last_checked_at` bumped, objects on the backend but missing
from the catalog are inserted, and catalog copies missing from the backend
are marked `MISSING`. Scrub never deletes. This is the working proof of the
rebuildable-index principle. Flags: `--backend TEXT` (required, a
registered backend name).

<!-- code-anchor: src/sutradhara/cli/hdcache.py src/sutradhara/hdcache @ 5688438 -->
## sutra hdcache

Manage the expendable HD cache disk tier: enrollment and lifecycle of
independent JBOD disks, fill scheduling, the walker, rebuild, and
repopulation drills. Tape stays the only durability layer; every hdcache
operation is reversible from the archive.

### sutra hdcache disk

| Command | Arguments | Purpose |
|---|---|---|
| `add` | `[BLOCK_DEV]` | Enroll one block device, or `--scan` to scan/enroll all candidates (`--yes` confirms batch enrollment). `--json`. |
| `list` | | List enrolled disks. `--all` includes dead disks. `--json`. |
| `locate` | `DISK_ID` | Blink or identify a physical disk, best effort. |
| `retire` | `DISK_ID` | Mark a disk retiring; entries stay present and servable while it drains. `--json`. |
| `dead` | `DISK_ID` | Mark a disk gone now and flip entries to lost in bounded batches. `--yes` confirms; `--confirm-mounted` additionally confirms a disk that still appears mounted. `--json`. |
| `forget` | `DISK_ID` | Validate that a dead disk has no cache entries and keep its id tombstoned. |

### The rest

| Command | Arguments | Purpose |
|---|---|---|
| `status` | | Show hdcache disk summary. `--disks` includes disk rows; `--disk TEXT` shows one disk in detail. `--json`. |
| `fill` | `SELECTOR` | Schedule fills for one sha256 or a whole artifactclass. `--dry-run` prints count and bytes; `--confirm-threshold-bytes INTEGER` (default `107374182400`, 100 GiB) requires `--yes` above that planned size. `--json`. |
| `walk` | `[DISK_ID]` | Run the disk walker for one disk or all disks. `--read-only` reports without deleting or marking entries lost. `--json`. |
| `rebuild` | | Rebuild untrusted cache rows from self-describing disk filenames (rows stay untrusted until cross-checked against the catalog). `--json`. |
| `drill status` | `[DISK_ID]` | Show remaining/refilled counts and ETA for dead-disk repopulation drills. `--json`. |

<!-- code-anchor: src/sutradhara/cli/retention.py src/sutradhara/retention.py @ 5688438 -->
## sutra retention and sutra offsite

The only place in the system that deletes bytes. Both stages require current
read-back digest agreement, offsite confirmation where configured, and a
Remanence catalog witness for Remanence copies. The staging sweep re-runs the
full gate, checks the frozen policy fingerprint, and atomically renames the
landing tree to a recovery tombstone before garbage collection. Batch entry
points stop above 25 candidates unless an operator supplies a deliberate
limit.

| Command | Arguments | Purpose |
|---|---|---|
| `retention run` | | Release held intakes whose durable copies pass the gate. `--intake TEXT`, `--actor TEXT`, `--batch-limit INTEGER`, `--dry-run`, `--json`. |
| `retention status` | | Show per-pool release holds and purge disposition. `--intake TEXT`, `--held`, `--grace-days INTEGER` (default `30`), `--json`. |
| `retention sweep-staging` | | Re-gate and purge released landing bytes after the grace period. `--intake TEXT`, `--actor TEXT`, `--grace-days INTEGER` (default `30`), `--break-glass` for a shorter grace, `--batch-limit INTEGER`, `--dry-run`, `--json`. |
| `retention abandon` | | Terminally exclude one held or released intake from deletion while preserving its staging bytes. Requires `--intake TEXT` and `--reason TEXT`; accepts `--actor TEXT`, `--json`. |
| `retention journal export` | | Run the exclusive chained exporter, checkpoint the published footer, then append segments and head anchors to the configured `ssh_disk` DR target. `--json`. |
| `retention journal check` | | Walk sequence/checksum/hash continuity, compare DR copies/head, and compare the current copy projection with verification receipts. Breaks alarm, print the runbook, and exit nonzero. `--json`. |
| `retention journal correct` | | Append an attributed superseding correction targeting `--source verify_receipt\|retention_event --event-id INTEGER`; requires `--reason`, accepts `--actor`, `--json`. |
| `retention sitrep` | | Print standing purge holds plus pending journal count, age, and staleness alarm state. `--json`. |
| `offsite confirm` | | Confirm one known media id as offsite. `--tape TEXT` or `--media-id TEXT`, `--shipment TEXT`, `--actor TEXT`, `--json`. |
| `offsite revoke` | | Revoke an offsite confirmation without deleting its history. `--tape TEXT` or `--media-id TEXT`, `--reason TEXT`, `--actor TEXT`, `--json`. |

<!-- code-anchor: src/sutradhara/cli/pfr.py src/sutradhara/pfr.py @ 5688438 -->
## sutra pfr

Partial file restore: container-index sidecars and byte-range clip cuts.
`cut` falls back to whole-member restore when the optimized path is not
available.

| Command | Arguments | Purpose |
|---|---|---|
| `cut` | `[ASSET_HASH_HEX]` | Cut a clip. `--artifactclass TEXT` (required), `--member-name TEXT`, `--from FLOAT` / `--to FLOAT` (file-relative in/out times, required), `-o/--output FILE`, `--rem-bin TEXT` (default `rem`), `--json`. |
| `status` | `[ASSET_HASH_HEX]` | Show PFR readiness for one asset or member selector. `--artifactclass TEXT` (required), `--member-name TEXT`, `--json`. |
| `reindex` | `[ASSET_HASH_HEX]` | Enqueue forced pfr-index jobs, bypassing the presence-gated reconciler. `--artifactclass TEXT`, `--member-name TEXT`, `--grammar [fallback]`, `--all` reindexes every current PFR sidecar, `--json`. |

<!-- code-anchor: src/sutradhara/cli/serve.py src/sutradhara/cli/api.py src/sutradhara/cli/grpc.py @ 5688438 -->
## sutra serve, serve-api, serve-grpc

The operator-facing servers. `sutra serve` runs both halves in one process;
the split commands exist for running them separately.

### sutra serve

Serve the operator HTTP API and the mTLS gRPC relay in one process.

| Flag | Default | Meaning |
|---|---|---|
| `--grpc-bind TEXT` | `127.0.0.1` | gRPC bind address. |
| `--grpc-port INTEGER` | `50051` | gRPC port. |
| `--landing-root DIRECTORY` | `/replica/landing` | Landing root for streamed intakes. |
| `--pki-dir DIRECTORY` | `/etc/sutradhara/pki` | gRPC PKI directory. |
| `--api-socket PATH` | `$SUTRA_API_SOCKET` or `/run/sutradhara/api.sock` | Unix socket for the HTTP API. |
| `--api-tcp` | off | Serve loopback TCP for local dev. |
| `--api-host TEXT` | `127.0.0.1` | Loopback TCP host. |
| `--api-port INTEGER` | `8770` | Loopback TCP port. |
| `--socket-mode TEXT` | `660` | Octal mode for the API Unix socket. |
| `--skip-artifactclass-validation` | off | Development/testing only: allow unknown artifactclasses. |

### sutra serve-api

Serve the operator API alone, on a Unix domain socket by default and never
on a tailnet/public bind. Flags: `--socket PATH` (default
`$SUTRA_API_SOCKET` or `/run/sutradhara/api.sock`), `--tcp`, `--host TEXT`
(default `127.0.0.1`), `--port INTEGER` (default `8770`),
`--socket-mode TEXT` (default `660`).

### sutra serve-grpc

Serve streaming intake over gRPC+mTLS, or run enrollment admin actions
without starting the server.

| Flag | Default | Meaning |
|---|---|---|
| `--bind TEXT` | `127.0.0.1` | LAN/Tailscale bind address. |
| `--port INTEGER` | `50051` | Port. |
| `--landing-root DIRECTORY` | `/replica/landing` | Landing root where streamed intakes are assembled. |
| `--pki-dir DIRECTORY` | `/etc/sutradhara/pki` | Sutradhara gRPC PKI directory. |
| `--issue-enroll-token` | off | Mint a 24h enrollment token (with `--device-id`, `--operator`). |
| `--revoke-device TEXT` | | Revoke all certificates for DEVICE_ID. |
| `--sign-csr FILE` | | Sign a device CSR (with `--token`, `--cert-out FILE`). |
| `--skip-artifactclass-validation` | off | Development/testing only: allow unknown artifactclasses. |

<!-- code-anchor: src/sutradhara/cli/db.py src/sutradhara/cli/admin.py @ 46bb240 -->
## sutra db and sutra admin

Schema and local maintenance.

| Command | Arguments | Purpose |
|---|---|---|
| `db init` | | Create all tables on the configured DB. Development convenience; production uses `alembic upgrade head` so the change history is tracked. `--echo` echoes SQL. |
| `admin doctor` | | Report local operational readiness (rem binary, key registry, and related seams). `--strict` exits non-zero on any WARN. |
| `admin keys mint-recovery` | `--public-key FILE --private-key FILE` | Offline-only mint of a recovery X-Wing keypair to operator-selected REMR/REMP paths. The private output is escrow material and is never imported. Prints the serving-host import command. Refuses to overwrite either output. |
| `admin keys import-public` | `--public-key FILE` | Import a canonical recovery REMR public epoch into `SUTRADHARA_KEY_REGISTRY_DIR`; the previous recovery public epoch is retired for new seals. |
| `admin reset` | | Drop and recreate the configured catalog database schema. Requires `--i-mean-it`. `--echo` echoes SQL. |
