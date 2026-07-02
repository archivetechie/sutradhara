# Codex prompt — hdcache M4: restore seam, gates, request model — sutradhara

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`.**
> Read `CLAUDE.md` + `AGENTS.md` first. **Authoritative design:
> `docs/design-hd-disk-tier.md` §6.1–6.3, §7 (grants/gate), §3 (restore_request tables).
> Shared contract: `docs/contract-hdcache-restore.md` — NORMATIVE, read it; never copy it.**
> Depends on: M1–M3. Consumed by: M5 (parallel serve), M6 (API endpoints).
>
> **What this is.** The security-critical slice: `resolve_read_source`, all three gates,
> the persisted restore-request model, CLI wiring, the restore-job hard gate, and
> single-stream cache serve with tape fallback. NO parallel orchestration / wake-ahead /
> deadlines (M5). NO HTTP endpoints (M6) — but the request model IS the contract §3/§4
> shapes, so build states/fields to that contract now.

## What already exists — BUILD ON IT
- `archive_restore.py:247` `restore_asset` — the tape path; called from `cli/archive.py:310`
  and `cli/virtual.py:222`; refuses suspect/rejected without force flags
  (`:261-266,343-361`) — **preserve exactly**.
- `api/identity.py` — role-tier `parse_identity` (`:18-22,76-81`); extend with **additive
  grants** per contract §1 (p3 ⊇ p2; admin NOT implicit). `GET /api/session` capabilities[]
  extension.
- `jobs/handlers/restore.py:33` (copy_id+dest_path) + `cli/jobs.py` generic submit — the
  bypass the panel flagged; close it (below).
- M1 `read_entry_verified` + `restore_request[_item]` tables; M3 opener work-dir + key
  domain.

## Build order

### A. Gates + resolver (`hdcache/manager.py`, design §6.1/§6.2)
`resolve_read_source(asset, artifactclass, identity_or_override)` wrapping `restore_asset`'s
call sites: (1) privacy — effective level (strictest across classes) → capability via the
configured map (contract §2), typed `RestoreDenied(required_capability)` (contract §5);
(2) validity — suspect/rejected with existing force semantics, **applied to the cache
branch too**; (3) destination — opaque destination_id → configured export roots,
canonicalize + symlink-escape reject + refuse-overwrite default (design §6.2); CLI keeps
paths but runs the same canonicalization helper.

### B. Request model (design §6.4 storage only — orchestration is M5)
Admission populates `restore_request` + items (states per contract §4); single-stream
sequential serve for now, updating item states (`queued → streaming → done |
fell_back_to_tape | denied | failed`; `waking_disk` arrives with M5).

### C. Cache serve + fallback (design §6.3, single-stream)
Trusted rows: verified serve (AEAD via Opener w/ stored_digest then plaintext digest; raw
stream-verify) → `atomic_write_verified_file`. **Untrusted rows: verify-then-serve,
promote atomically on success, delete + fallback on failure.** Any cache failure ⇒
structured reason-coded event + entry per design (`lost` only after the M1 identity/liveness
primitive confirms the disk is present — full breaker logic is M5, but the
never-lost-from-absence rule applies NOW) + tape fallback in the same request.

### D. CLI wiring
`sutra archive restore` / `sutra virtual restore` route through the resolver; private
assets fail closed absent `--privacy-override <reason>` (logged/audited, design §6.1).

### E. Restore-job hard gate (design §6.1, contract §6)
Handler requires `restore_request_item_id`, validates asset+destination against the gated
row, rejects raw `copy_id`/`dest_path`; `sutra jobs submit` refuses kind=restore
(`cli/jobs.py`, `jobs/dispatch.py`). Verify self-heal/scrub/verify flows never used this
job kind (they call primitives directly) — if anything does, route it to an internal kind
with no operator destination.

## Must-be-exact
- **Both-branches property**: privacy AND validity gates hold identically on cache-hit and
  tape-fallback (the panel's named drift risk) — dedicated tests.
- `select_restore_source` / `self_heal` remain untouched and cache-blind (INV-6).
- Denied ≠ failed everywhere; detail strings per contract §2/§5 (no CLI-hint strings in
  API-visible fields).

## Definition of done
- `uv run pytest -q` green. Tests: gate matrix (p2/p3 × has/lacks capability × admin
  without grant = denied); unmapped level ⇒ denied + alarm-event; strictest-wins
  multi-class; validity force semantics both branches; destination confinement (traversal,
  symlink, overwrite, unknown id); CLI fail-closed + override audit; hard gate (raw params
  rejected, jobs-submit refusal, request-item validation); untrusted verify-then-serve
  promote/delete; fallback-with-audit on digest mismatch/missing file; request/item state
  persistence per contract §4; INV-6 untouched-modules check.
- Covers: harness scenario private-asset legs (both paths) —
  `~/system/docs/prompt-hdcache-harness-scenario.md`.
- `docs/INDEX.md` + journal per AGENTS.md.
