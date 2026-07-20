# Documentation index

- [Architecture overview](architecture-overview.md) — system boundaries, durability,
  deletion-evidence gates, and operational data flow.
- [Quickstart](guide-quickstart.md) — local setup and first catalog workflow.
- [Data-model walkthrough](guide-data-model-walkthrough.md) — catalog concepts and
  relationships.
- [CLI reference](reference-cli.md) — operator commands, including retention,
  offsite confirmation, revocation, and correction surfaces.
- [Configuration reference](reference-config.md) — environment and backend settings.
- [Database schema reference](reference-database-schema.md) — authoritative tables,
  constraints, evidence projections, retention receipts, and compatibility
  downgrade export policy.
- [Schema conventions](reference-schema-conventions.md) — executable P1
  vocabulary/FK/clock manifest and P5 persistent-field writer/reader ownership.
- [Retention evidence journal](reference-retention-journal.md) — locked chained
  export, append-only DR shipping, checking, corrections, and ops alarms.
- [Glossary](reference-glossary.md) — project terminology.
- [Arrangement ARC guide](arrangement-arc-guide.md) — arrangement review workflow.
- [Examples](examples/README.md) — example configuration and agent bundles.

The deletion-evidence prompt-1 gate and prompt-2 journal implementations are
documented in the architecture, CLI, database-schema, configuration, and journal
references above. Their frozen design and implementation prompts remain in the
external system journal rather than being copied here.
