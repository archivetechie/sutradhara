# Sutradhara

Orchestrator above [Remanence](https://github.com/archivetechie/remanence) — a
content-addressed media archive catalog and job engine, built for the archive
Foundation video archive.

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

## What this is not

- Not a tape driver — Remanence owns SCSI, library control, on-tape format,
  and parity. Sutradhara talks to it over gRPC.
- Not a MAM (media asset manager) — no editorial UI; archive operations only.
- Not a vendor product like Miria — it is first-party software designed to
  outlive its dependencies.

<!-- code-anchor: src/sutradhara/cli docs/INDEX.md @ 74952cc -->
## Status

Beyond the v0.1 anchor spec (see [`docs/spec-v0.1.md`](docs/spec-v0.1.md) for
the original design). The catalog, job engine, and CLI are built and in
active use; large parts of the ingest → arrange → archive → restore lifecycle
are implemented, including:

- Multi-backend copy fan-out with per-placement policy and durability
  enforcement (`sutra archive`, `sutra backends`).
- Landing intake, arrangement, and frozen-source-map archival
  (`sutra intake`, `sutra arrangement`).
- Post-archive organization via permanently-mutable virtual arrangements
  (`sutra virtual`, `sutra tag`).
- A level-triggered reconciler spine driving copies and derivations
  (`sutra reconcile`).
- Retention / deletion gating that only reclaims landing data once durable
  copies are verified and offsite-confirmed (`sutra retention`, `sutra offsite`).
- Scrub and self-heal against live backend state (`sutra scrub`).
- An expendable HD-cache disk tier in front of tape (`sutra hdcache`).
- A single-node lease-aware job worker with cgroup-based resource control
  (`sutra worker`).
- An operator HTTP API + mTLS gRPC relay for browser/agent-driven intake and
  restore (`sutra serve`).

`docs/INDEX.md` tracks every design doc's status (current / implemented /
superseded / historical) and is the authoritative map of what's built versus
still proposed. `docs/roadmap.md` and
`docs/implementation-plan-ingest-v2.md` track what's next.

<!-- code-anchor: pyproject.toml @ 74952cc -->
## Layout

```
sutradhara/repo/
├── README.md
├── LICENSE
├── packages/
│   ├── sutra-agent/           # operator-facing edge receive agent (Rust)
│   └── sutradhara-receive/    # dependency-light receive filesystem contract
├── src/
│   └── sutradhara/            # server/orchestrator package (the `sutra` CLI)
├── docs/
│   ├── spec-v0.1.md           # original design — start here for the "why"
│   └── INDEX.md               # status of every design/contract/prompt doc
├── alembic/                   # DB schema migrations
└── tests/
```

<!-- code-anchor: pyproject.toml src/sutradhara/cli/db.py alembic @ 74952cc -->
## Install & verify

Requires Python ≥3.11 and [`uv`](https://docs.astral.sh/uv/).

```sh
uv sync                 # install dependencies (workspace incl. packages/sutradhara-receive)
uv run pytest -q        # fast, hermetic test suite
```

The CLI is installed as `sutra` inside the project's virtualenv
(`.venv/bin/sutra`). It talks to a database configured via `SUTRADHARA_DB_URL`
(default: sqlite at `/var/lib/replica/sutradhara.db`). For a scratch local
database:

```sh
export SUTRADHARA_DB_URL=sqlite:////tmp/sutradhara-dev.db
.venv/bin/sutra db init      # dev convenience; production uses alembic
.venv/bin/sutra --help
```

Production schema changes go through `alembic` (`alembic/`), not `sutra db
init`.

<!-- code-anchor: src/sutradhara/cli src/sutradhara/backend/factory.py @ 74952cc -->
## CLI overview

`sutra --help` lists every command group; each group has its own `--help`.
The main ones, roughly in lifecycle order:

| Command | Purpose |
|---|---|
| `sutra receive` | Pull a source tree (card, drive, folder) into a landing intake as a BagIt bag. |
| `sutra intake` | Inspect, register, and watch landing intakes into the catalog. |
| `sutra prepare` | Record a derivation profile (transcode/index) for an intake. |
| `sutra arrangement` | Arrange registered masters into an archive namespace and submit a frozen source-map. |
| `sutra archive` | Manage artifactclass policy, build/review durable bundles, archive submissions, restore assets. |
| `sutra virtual` | Post-archive, permanently-mutable views: place, move, exclude, restore members by virtual path. |
| `sutra tag` / `reject` / `unreject` | Content-level governance: tags and reject markers (gate restore, never delete). |
| `sutra reconcile` | Run one bounded reconcile cycle for a domain (copies, derivations). |
| `sutra jobs` | Submit, run, and inspect individual jobs. |
| `sutra worker` | Run the single-node lease-aware job worker that drains pending jobs. |
| `sutra backends` | Register and inspect storage backends and their pools. |
| `sutra hdcache` | Manage the HD-cache disk tier (enrollment, fills, walker, repopulation). |
| `sutra scrub` | Re-enumerate a backend and reconcile it against the catalog. |
| `sutra retention` / `offsite` | Retention release gate, offsite confirmation, staging sweep. |
| `sutra serve` / `serve-api` / `serve-grpc` | Operator HTTP API and mTLS gRPC relay (device intake, browser console). |
| `sutra admin` | Dangerous local catalog maintenance (`doctor`, `reset`). |

Backends currently supported: `rem_tape` (gRPC to Remanence), `d2_tape` (Java
CLI adapter for the legacy d2 tape library), `s3` (cloud), `ssh_disk`
(rsync/SSH to a LAN file server), and `memory` (tests only).

<!-- code-anchor: src/sutradhara/rem_archive_cli.py src/sutradhara/keys/registry.py src/sutradhara/cli/serve.py @ 74952cc -->
## Configuration

Beyond `SUTRADHARA_DB_URL`, the environment variables most operators need:

- `REM_BIN` — path to the local Remanence `rem-debug` CLI, used for RAO
  sealing/opening. Falls back to `~/remanence/target/release/rem-debug`. Run
  `sutra admin doctor` to check availability.
- `SUTRADHARA_KEY_REGISTRY_DIR` — root of the local key registry for
  encrypted (RAO-AEAD) copies. Defaults to
  `/var/lib/replica/sutradhara-key-registry`; deployments should create it
  with service-user ownership and mode `0700`. Root-key files are written
  `0600`; retiring an epoch never deletes key material.
- `SUTRA_API_SOCKET` — Unix domain socket path for `sutra serve`'s operator
  API (default `/run/sutradhara/api.sock`).

The HD-cache tier, resource control, and a handful of other subsystems have
their own tuning knobs (all `SUTRADHARA_*` environment variables) — see the
relevant design doc in `docs/` (`design-hd-disk-tier.md`,
`design-elastic-resource-control.md`) for the full list and defaults.

<!-- code-anchor: src/sutradhara/sealing @ 74952cc -->
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

- [`docs/spec-v0.1.md`](docs/spec-v0.1.md) — the original design (why this
  exists, first principles, architecture).
- [`docs/INDEX.md`](docs/INDEX.md) — status of every design, contract, and
  prompt doc in `docs/`; the map of what's built vs. proposed.
- [`docs/roadmap.md`](docs/roadmap.md) and
  [`docs/implementation-plan-ingest-v2.md`](docs/implementation-plan-ingest-v2.md)
  — what's built and what's next.
- [`docs/arrangement-arc-guide.md`](docs/arrangement-arc-guide.md) — a
  plain-language walkthrough of the intake → arrange → archive →
  organize-forever lifecycle, for archivists and operators rather than
  developers.

## Maintainer

Built and maintained by Ada Operator and the archive archives team. Small-team
/ sysadmin-led software; design favors simplicity, robustness, minimal
moving parts, and a 30-year horizon.

## License

AGPL-3.0-or-later. Same as Remanence.
