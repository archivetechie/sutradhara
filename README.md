# Sutradhara

Orchestrator above [Remanence](https://github.com/archivetechie/remanence) — a content-addressed media archive catalog and job engine, designed for the the operating institution video archive.

**Status: v0.1 anchor spec, not yet implemented.** See [`docs/spec-v0.1.md`](docs/spec-v0.1.md) for the design.

## What this is

Sutradhara is the orchestration layer that sits above Remanence (tape) and future disk and cloud backends. It maintains:

1. A **catalog** of logical media assets and every copy of each across heterogeneous backends.
2. A **job engine** that runs ingest, copy, migrate, verify, repair, restore, transcode, MXF extraction, audio extraction, and transcription.

The catalog uses **content-hash as the logical asset identity** and is **rebuildable** by re-enumerating backends. Losing the database is not a data-loss event.

## What this is not

- Not a tape driver — Remanence owns SCSI, library control, on-tape format, and parity.
- Not a MAM — no editorial UI; archive operations only.
- Not a vendor product like Miria — it is first-party software designed to outlive its dependencies.

## Layout

```
sutradhara/repo/
├── README.md
├── LICENSE
├── docs/
│   └── spec-v0.1.md          # design doc — start here
└── (code arrives when Remanence's Layer 5 gRPC is ready)
```

## Scenario O Operational Notes

Scenario O can seal per-copy RAO representations before storage. The default
copy representation remains `raw-bytes`; `o-archive` uses `rao-plain-v1` for
copy 1 and `rao-aead-v1` for copy 2.

RAO sealing is a local Remanence CLI dependency. Sutradhara resolves it from
`$REM_BIN`, then `~/remanence/target/release/rem-debug`. Run
`sutra admin doctor` on a host to check `rem-debug` availability and key
registry accessibility.

Encrypted RAO copies use Sutradhara's local key registry. The default registry
directory is `/var/lib/replica/sutradhara-key-registry`; deployments should
create it with service-user ownership and mode `0700`, or set
`SUTRADHARA_KEY_REGISTRY_DIR` for non-root/dev environments. Root-key files are
written `0600`, and retiring an epoch does not delete key material.

For RAO copies, `copy.integrity_hash` is the stored RAO object digest. The
logical asset remains keyed by the source plaintext SHA-256. RAO copy rows
record non-authoritative `storage_metadata` with the representation and
`chunk_size`; encrypted-copy `key_id` is recovered from the stored RAO header
via keyless inspection (`sutradhara.sealing.inspect_rao`).

## Maintainer

Built and maintained by Ada Operator and the archive archives team. Small-team / sysadmin-led software; design favors simplicity, robustness, minimal moving parts, and a 30-year horizon.

## License

AGPL-3.0-or-later. Same as Remanence.
