# Design — legacy Python `sutra-agent`: receive-side operator helper

> Status: **superseded** (2026-07-03). The active helper is the Rust
> `~/sutra-agent` binary. The legacy Python package was removed during the
> Rust control-plane cutover.
> This document is retained as historical context for the road-mode MVP.

## Goal

`sutra-agent` is the operator-facing shell around first-contact receive. It keeps
local defaults and a local run ledger, starts or resumes receives, sweeps stale
partial receives, and reports whether a removable source may be released.

The safety rule is fail-closed: the agent reports source release as allowed only
when the server has written `intake.verified.json`. A missing marker, timeout, or
`intake.quarantined.json` means do not release the source.

## Boundary

- `sutradhara-receive` owns bytes, hashing, BagIt metadata, canonical paths,
  resume mechanics, destination verification, and server-marker polling.
- `sutra-agent` owns local operator state: config, run ledger, and the
  release-safe user-facing status.
- Server-side `sutradhara.intake` owns inspection, catalog registration,
  quarantine, and the verified/quarantined marker files.

This keeps the receive contract in one shared Python component while allowing a
thin client helper to grow into GUI/device integration later.

## Local state

The agent config schema is `sutra-agent-config-v1`:

- `landing`
- `operator`
- `source_kind`
- `artifactclass`
- `ledger_path`
- `confirm_interval_seconds`

The ledger schema is `sutra-agent-ledger-v1`. It records each completed receive:
intake id, source, landing, intake directory, operator defaults, payload counts,
resume origin, and the last known confirmation state:

- `pending` — no server marker yet; source release is blocked.
- `verified` — `intake.verified.json` exists; source release is allowed.
- `quarantined` — `intake.quarantined.json` exists; source release is blocked.

Both files are JSON and atomically replaced on write.

## CLI MVP

```sh
sutra-agent config init --landing /replica/landing --operator operator
sutra-agent receive /Volumes/CARD --confirm-timeout 300
sutra-agent receive --resume <intake-id>
sutra-agent receive sweep --older-than-hours 24
sutra-agent status <intake-id>
```

The agent can run without a config when all required values are passed as flags,
which keeps tests and temporary deployments simple.
