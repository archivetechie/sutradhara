# Codex prompt — hdcache M3: policy extension, key domain, fills + convergence sweep — sutradhara

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`.**
> Read `CLAUDE.md` + `AGENTS.md` first. **Authoritative design:
> `docs/design-hd-disk-tier.md` §5, §7 (policy/keys), §2 (reuse table) — read in full.**
> Depends on: M1, M2. Consumed by: M4 (serve), M6 (repop).
>
> **What this is.** Everything that puts bytes ONTO cache disks: the artifactclass policy
> extension, the hdcache key domain, sealer work-dir, the bounded fill job, its producers,
> and the convergence sweep. NO restore/serve (M4), NO walker (M5), NO repop planner (M6 —
> but the fill job must already accept repop-produced work).

## What already exists — BUILD ON IT
- `artifactclass_policy.py` — strict parser (`_reject_keys:156`); **extend via the
  `staging_config` JSON-column pattern (`:267`)**: add `hdcache_config` on
  `ArtifactClassPolicyRecord` (+1 migration) carrying `{enabled: bool, privacy_level:
  "none"|"p2"|"p3"|…}`; parse/apply plumbing mirrors staging_config exactly.
- `keys/registry.py` `KeyRegistry` — add the **domain/purpose parameter** on
  `create_epoch` (design §7 amendment, §12.10): namespaced key_ids (`hdcache-*`);
  **cross-domain assertion at BOTH seal call sites** (hdcache fill refuses non-hdcache
  epochs; pool sealing refuses hdcache epochs). Keep the dev-seed behavior for tests but
  honor the design's production-startup-failure requirement where it already applies.
- `sealing/rao.py` `RaoCliSealer`/`RaoCliOpener` — add optional `work_dir` parameter
  (default: current behavior); hdcache calls point it at configured scratch (design §6.3).
- Jobs engine — `dedupe_key` live-only uniqueness (`jobs/models.py:192`),
  `recon_domain`/`recon_target_key` + conditions (`:144-189`), reconciler spine
  (`jobs/reconcilers/spine.py`) with bounded cursor batches.
- `archive_fanout.flush_bundle` — the post-archive hook point.
- M1 store (`write_entry` streams + verifies), M2 placement.

## Build order

### A. Policy + keys (above) — land first, they gate everything.
Policy-apply precondition (design §5/§13): `privacy_level ≠ none` with no capability
mapping configured (see `docs/contract-hdcache-restore.md` §2) ⇒ **reject the policy
edit**.

### B. Fill job (`jobs/handlers/hdcache_fill.py`, design §5 handler steps 1–5)
UPSERT/adopt the one-row-per-asset entry; re-place if pinned disk not active; landing
source first with same-attempt fallback to `restore_asset`; seal when effective privacy ≠
none (**strictest across classes** — design §7); mkstemp tmp → fsync → rename → flip
`present` → filled_bytes; ENOSPC ⇒ fail write, flag disk over-reserve, re-place. Priority:
the hdcache band (below operator restores, above migration — state the integers in config,
design §3/§12.7). `dedupe_key = "hdcache:"+sha`; recon domain `hdcache`.

### C. Producers + bounded scheduling (design §5)
- Post-`flush_bundle` hook for cacheable classes — stamps `bundle_key`/`group_key` into
  the job payload (M2 context contract).
- **Live-job cap** (config, default 500): the sweep and `on_entries_lost` (M1 seam — wire
  it here) top up, never dump.
- Convergence sweep = spine reconciler, sparse cadence: desired = every cacheable archived
  asset has a `present`, **policy-conformant** entry — including the privacy-raise re-seal
  path (raw entry under now-private class ⇒ lost-mark + delete + sealed refill) and the
  **epoch-retirement re-seal** (entries of retired hdcache epochs).
- Skip blocked/backing-off condition targets.

### D. CLI
`sutra hdcache fill <sha|class>` — class form prints count+bytes, `--dry-run` /
`--yes`-above-threshold (design §8.4).

## Must-be-exact
- Every fill write stream-verifies `content_sha256` (INV-2) regardless of source.
- Only archived assets fill (≥1 archival copy) — assert in handler.
- File-first, DB-second ordering (design §5).

## Definition of done
- `uv run pytest -q` green. Tests: policy parse/apply round-trip + unmapped-level
  rejection; cross-domain seal refusal (both directions); fill from landing + fallback
  mid-attempt (simulate ENOENT race); AEAD fill records epoch/stored_digest; UPSERT/adopt
  + re-place off dead disk; ENOSPC re-place; live-job cap honored under a 10^4 backlog
  (sweep tops up, never exceeds N); privacy-raise convergence; retired-epoch re-seal;
  dedupe idempotency; blocked-condition parking (no eternal retries).
- Covers: harness scenario (fill-at-ingest leg) —
  `~/system/docs/prompt-hdcache-harness-scenario.md`.
- `docs/INDEX.md` + journal per AGENTS.md.
