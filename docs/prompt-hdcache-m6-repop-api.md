# Codex prompt — hdcache M6: repopulation + drain + alarms + UI API — sutradhara

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`.**
> Read `CLAUDE.md` + `AGENTS.md` first. **Authoritative design:
> `docs/design-hd-disk-tier.md` §8.3–8.4, §13. Shared contract:
> `docs/contract-hdcache-restore.md` — NORMATIVE for §3 endpoints.**
> Depends on: M1–M5. Final server milestone; pairs with the system-ui prompt
> (`~/system-ui/docs/prompt-hdcache-restore-console-system-ui.md`).
>
> **What this is.** Closing the lifecycle: the repopulation planner + drill tracking,
> retire-drain, alarm wiring to the gap-board surface, and the HTTP endpoints the console
> consumes.

## What already exists — BUILD ON IT
- M1 `on_entries_lost` seam + dead-flip; M3 fill jobs + live-job cap; M5 events + walker.
- `archive_restore.py` extractors — the **bundle-scoped batch-extract variant** lands here
  (design §8.3: one materialization → all lost members, same verification; INV-5's spirit).
- The reconciliation-condition surface the console reads (ui-direction.md "gap board
  signals" / `GET /api/ui/reconciliation`) — alarm conditions ride it, design §8.4.
- The existing `/api/*` handler conventions (`sutra serve`, identity from headers).

## Build order

### A. Repopulation planner (`hdcache/repopulate.py`, design §8.3)
Groups lost entries **by source tape** (primary); multi-member bundle losses (co-located
small-file groups; multi-disk loss) batch-extract from one materialization via the new
`restore_asset` bundle variant. Enqueues M3 fill jobs under the live-job cap, **priority:
below ingest + operator restores, above migration** (config integers, design §12.7);
batches bounded ~30–60min of tape work. Jobs tagged with origin drill (disk_id +
timestamp).

### B. Drain (design §8.3 `retire`)
`retiring` disks: no placements (M2 already filters non-active); migrate entries via
**verified local read** (M1 read_entry_verified → M2 re-place → M1 write_entry),
`restore_asset` per-entry only on verification failure; auto → `dead` when empty.

### C. Drill visibility
`sutra hdcache drill status [<disk_id>]`: remaining/refilled, bytes/hr, ETA at current
priority; completion event.

### D. Alarms (design §8.4 — conditions, not logs)
Wire M5's reason-coded events + these thresholds into gap-board conditions: reserve
breach, lost-backlog threshold/growth-for-T, fill queue stalled, disk
unreachable/breaker, SMART degradation, walker tripwire, unmapped privacy level,
fallback-reason spikes. Owner string: archive operator.

### E. HTTP endpoints (contract §3 — field-for-field)
`GET /api/ui/restore-destinations`, `POST /api/ui/restores`,
`GET /api/ui/restore-requests[/{id}]` on the existing serve stack, identity-gated per
contract §1–§2, backed by the M4 request model + M5 orchestrator. No raw paths in or out.

## Must-be-exact
- Tape-drive lease behavior: a repop batch releases the drive between batches (operator
  restores interleave) — verify against the lease/priority machinery actually present;
  document the config integers chosen.
- Drain writes bypass nothing: INV-2 stream-verify on both read and write sides.

## Definition of done
- `uv run pytest -q` green. Tests: planner tape-grouping + batch-extract (multi-member
  loss ⇒ one materialization); drill accounting/ETA math; drain end-to-end on fake disks
  (verified move, fallback-to-tape on corrupt source, auto-dead); priority ordering under
  contention (repop yields to restore, outranks migration-priority jobs); alarm-condition
  emission matrix; endpoint contract tests (shapes, gates, denied-vs-failed, mixed cart,
  unknown destination) — assert against `contract-hdcache-restore.md` §3/§4 literally.
- Covers: harness scenario repop/drill legs + API smoke —
  `~/system/docs/prompt-hdcache-harness-scenario.md`.
- `docs/INDEX.md` + journal per AGENTS.md.
