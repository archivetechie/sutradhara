# Sutradhara

Orchestrator above [Remanence](https://github.com/archivetechie/remanence) — a
content-addressed media archive catalog and job engine, built for a production
video archive.

<!-- code-anchor: none -->
## What this is

Sutradhara is the orchestration layer that sits above Remanence (tape) and
other storage backends (disk, cloud). It maintains:

1. A **catalog** of logical media assets and every copy of each, across
   heterogeneous backends.
2. A **job engine** that runs ingest, copy, migrate, verify, repair, restore,
   transcode, and other archive-lifecycle work.

The catalog uses **content-hash (SHA-256) as the logical asset identity** and
is **rebuildable** by re-enumerating backends — losing the database is not a
data-loss event.

![The archive lifecycle: receive, register, arrange and submit, archive with sealed copies fanning out to every active pool, organize forever](docs/assets/lifecycle.svg)

*Fig. 1 — The lifecycle: bytes move left to right once; after `archive` the record is immutable and all further organization is catalog-only.*

## What this is not

- Not a tape driver — Remanence owns SCSI, library control, on-tape format,
  and parity. Sutradhara talks to it over gRPC.
- Not a MAM (media asset manager) — no editorial UI; archive operations only.
- Not a vendor product like Miria — it is first-party software designed to
  outlive its dependencies.

<!-- code-anchor: src/sutradhara/cli docs/INDEX.md @ 5688438 -->
## Status

Beyond the v0.1 anchor spec (see [`docs/spec-v0.1.md`](docs/spec-v0.1.md) for
the original design). The catalog, job engine, and CLI are built and in
active use, and the ingest → arrange → archive →
restore lifecycle is implemented end to end, including:

- Multi-backend copy fan-out with per-placement policy and durability
  enforcement (`sutra archive`, `sutra backends`).
- Landing intake, arrangement, and frozen-source-map archival
  (`sutra intake`, `sutra arrangement`).
- Post-archive organization via permanently-mutable virtual arrangements
  (`sutra virtual`, `sutra tag`).
- A level-triggered reconciler spine driving copies, derivations, cache
  fills, bundle self-heal, and expired restore-lease reopening
  (`sutra reconcile`).
- Retention / deletion gating that only reclaims landing data once durable
  copies are verified, offsite-confirmed, and independently witnessed
  against the backend's own catalog — every release and purge decision is
  exported to a tamper-evident, chained evidence journal (`sutra retention`,
  `sutra offsite`, `sutra retention journal export|check|correct`,
  `sutra retention sitrep`).
- Scrub and self-heal against live backend state (`sutra scrub`).
- An expendable HD-cache disk tier in front of tape, including a
  cache-first, bounded, digest-verified byte producer shared by both
  restore delivery modes below (`sutra hdcache`).
- Partial file restore: container-index sidecars and byte-range clip cuts
  with a whole-member fallback (`sutra pfr`).
- A single-node lease-aware job worker with cgroup-based resource control
  (`sutra worker`).
- An operator HTTP API + mTLS gRPC relay for browser/agent-driven intake and
  restore (`sutra serve`), with:
  - a receive-time content-novelty check — a device receive is flagged for
    operator confirmation only when nothing new is detected on the card
    (not merely because the card has been seen before), backed by a
    durable receive-intent state machine and per-file dedup evidence
    recorded on every ingest item; and
  - an agent-delivered restore protocol (gRPC `OpenRestore` /
    `CommitRestore` / `WatchAssignments`, an exclusive lease per restore
    item, and idempotent resume) alongside the original server-local
    restore path, both converging on the same verified-chunk read/write
    primitives. The receiving-device agent itself lives outside this
    repository; sutradhara implements the shared protocol and the server
    side only. Encrypted-tape reads that back the server-local path now
    survive tape-mount latency (a long grace window before the first byte,
    a much shorter one once streaming starts) and, for bundle members,
    stream only the covering ciphertext range instead of the whole stored
    object.

`docs/INDEX.md` tracks every design doc's status (current / implemented /
superseded / historical) and is the authoritative map of what's built versus
still proposed. `docs/roadmap.md` and
`docs/implementation-plan-ingest-v2.md` track what's next.

<!-- code-anchor: pyproject.toml packages @ 5688438 -->
## Layout

```
sutradhara/
├── README.md
├── LICENSE
├── packages/
│   └── sutradhara-receive/    # dependency-light receive contract (maturin: Python + Rust core)
├── src/
│   └── sutradhara/            # server/orchestrator package (the `sutra` CLI)
├── proto/                     # gRPC contracts (device, intake, restore, Remanence layer5)
├── docs/
│   ├── spec-v0.1.md           # original design — start here for the "why"
│   └── INDEX.md               # status of every design/contract/prompt doc
├── alembic/                   # DB schema migrations
└── tests/
```

The Rust workstation helper (`sutra-agent`, tray + headless binaries) lives
in its own repository and links `packages/sutradhara-receive` as a crate;
an earlier in-tree `packages/sutra-agent` was removed when it moved.

<!-- code-anchor: pyproject.toml src/sutradhara/cli/db.py src/sutradhara/catalog/session.py alembic @ 5688438 -->
## Install & verify

Requires Python ≥3.11 and [`uv`](https://docs.astral.sh/uv/).

```sh
uv sync                 # install dependencies (workspace incl. packages/sutradhara-receive)
uv run pytest -q        # fast, hermetic test suite
```

The CLI is installed as `sutra` inside the project's virtualenv
(`.venv/bin/sutra`). It talks to a database configured via `SUTRADHARA_DB_URL`
(default: `sqlite:///./sutradhara.db`, a SQLite file in the current working
directory — always set the variable explicitly for anything long-lived).
For a scratch local database:

```sh
export SUTRADHARA_DB_URL=sqlite:////tmp/sutradhara-dev.db
.venv/bin/sutra db init      # dev convenience; production uses alembic
.venv/bin/sutra --help
```

Production schema changes go through `alembic` (`alembic/`), not `sutra db
init`. [`docs/guide-quickstart.md`](docs/guide-quickstart.md) walks a full
local tour, including a catalog rebuild from a fixture backend and one
receive → register pass, plus troubleshooting.

<!-- code-anchor: src/sutradhara/cli src/sutradhara/backend/factory.py @ 5688438 -->
## CLI overview

`sutra --help` lists every command group; each group has its own `--help`,
and [`docs/reference-cli.md`](docs/reference-cli.md) documents every leaf
command with flags and defaults. The main groups, roughly in lifecycle
order:

| Command | Purpose |
|---|---|
| `sutra receive` | Pull a source tree (card, drive, folder) into a landing intake as a BagIt bag; sweep and verify landing shares. |
| `sutra intake` | Inspect, register, and watch landing intakes into the catalog. |
| `sutra prepare` | Record a derivation profile (transcode/index) for an intake. |
| `sutra arrangement` | Arrange registered masters into an archive namespace and submit a frozen source-map. |
| `sutra archive` | Manage artifactclass policy, build/review durable bundles, archive submissions, restore assets. |
| `sutra virtual` | Post-archive, permanently-mutable views: place, move, exclude, restore members by virtual path. |
| `sutra tag` / `reject` / `unreject` | Content-level governance: tags and reject markers (gate restore, never delete). |
| `sutra list` | Query the catalog (`list assets`). |
| `sutra reconcile` | Run one bounded reconcile cycle for a domain (`copy`, `bundle_copy`, `derivation`, `hdcache`, `log_pipeline`). |
| `sutra jobs` | Submit, run, and inspect individual jobs. |
| `sutra worker` | Run the single-node lease-aware job worker that drains pending jobs. |
| `sutra backends` | Register and inspect storage backends and their pools. |
| `sutra scrub` | Re-enumerate a backend and reconcile it against the catalog. |
| `sutra hdcache` | Manage the HD-cache disk tier (enrollment, fills, walker, repopulation). |
| `sutra retention` / `offsite` | Retention release gate (verified + offsite-confirmed + backend-witnessed), staging sweep, offsite confirmation, and the append-only deletion-evidence journal (`journal export/check/correct`, `sitrep`). |
| `sutra pfr` | Partial file restore: clip cuts, sidecar status, forced reindex. |
| `sutra serve` / `serve-api` / `serve-grpc` | Operator HTTP API and mTLS gRPC relay (device intake, browser console). |
| `sutra db` / `sutra admin` | Schema init (dev), doctor, and dangerous catalog maintenance. |

Backends currently supported: `rem_tape` (gRPC to Remanence), `d2_tape` (Java
CLI adapter for the legacy d2 tape library), `s3` (cloud), `ssh_disk`
(rsync/SSH to a LAN file server), and `memory` (tests only). Other kinds
accepted by `backends add` (`rem_disk`, `plain_disk`, `gcs`, `azure_blob`)
are reserved names without adapters yet.

<!-- code-anchor: src/sutradhara/rem_archive_cli.py src/sutradhara/keys/registry.py src/sutradhara/cli/serve.py @ 5688438 -->
## Configuration

Beyond `SUTRADHARA_DB_URL`, the environment variables most operators need:

- `REM_BIN` — path to the Remanence `rem` CLI, used for RAO
  sealing/opening and archive builds. Resolution falls back to `rem` on
  `PATH`, then `~/remanence/target/release/rem`. Run `sutra admin doctor`
  to check availability.
- `SUTRADHARA_KEY_REGISTRY_DIR` — root of the local key registry for
  encrypted (RAO-AEAD) copies. Defaults to
  `/var/lib/replica/sutradhara-key-registry`; deployments should create it
  with service-user ownership and mode `0700`. Root-key files are written
  `0600`; retiring an epoch never deletes key material.
- `SUTRA_API_SOCKET` — Unix domain socket path for `sutra serve`'s operator
  API (default `/run/sutradhara/api.sock`).

Every other knob — hdcache tuning, resource control, PFR, the d2tape
backend, test fakes — is documented with exact defaults in
[`docs/reference-config.md`](docs/reference-config.md).

<!-- code-anchor: src/sutradhara/sealing @ 5688438 -->
## Scenario O — sealed RAO copies

Scenario O seals per-copy representations before storage instead of storing
raw bytes. The default copy representation remains `raw-bytes`; `o-archive`
uses `rao-plain-v1` for copy 1 and `rao-aead-v1` for copy 2 — see the
"Configuration" section above for the `REM_BIN` and key-registry
requirements this depends on.

For RAO copies, `copy.integrity_hash` is the stored RAO object digest; the
logical asset itself stays keyed by the source plaintext SHA-256. RAO copy
rows record non-authoritative `storage_metadata` with the representation and
chunk size; an encrypted copy's `key_id` is recovered from the stored RAO
header via keyless inspection (`sutradhara.sealing.inspect_rao`), not stored
redundantly on the row.

<!-- code-anchor: none -->
## Documentation

- [`docs/guide-quickstart.md`](docs/guide-quickstart.md) — install, a
  scratch catalog, the rebuildable-index demo, one receive → register
  pass, troubleshooting.
- [`docs/architecture-overview.md`](docs/architecture-overview.md) — how
  the code is actually organized: data model, lifecycle, job engine and
  reconciler spine, backends, sealing, hdcache, operator surface.
- [`docs/reference-cli.md`](docs/reference-cli.md) — every command and
  flag, verified against `--help`.
- [`docs/reference-config.md`](docs/reference-config.md) — every
  environment variable with its exact default.
- [`docs/reference-database-schema.md`](docs/reference-database-schema.md) —
  every application table, field, relationship, and the reason for the
  catalogue's key modelling boundaries.
- [`docs/reference-glossary.md`](docs/reference-glossary.md) — the
  internal vocabulary, as the code uses it.
- [`docs/spec-v0.1.md`](docs/spec-v0.1.md) — the original design (why this
  exists, first principles).
- [`docs/INDEX.md`](docs/INDEX.md) — status of every design, contract, and
  prompt doc in `docs/`; the map of what's built vs. proposed.
- [`docs/arrangement-arc-guide.md`](docs/arrangement-arc-guide.md) — a
  plain-language walkthrough of the intake → arrange → archive →
  organize-forever lifecycle, for archivists and operators rather than
  developers.

## Maintainer

Built and maintained by a small archive operations team.
Sysadmin-led software; design favors simplicity, robustness, minimal
moving parts, and a 30-year horizon.

## License

AGPL-3.0-or-later. Same as Remanence.
