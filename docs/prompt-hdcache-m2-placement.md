# Codex prompt — hdcache M2: placement engine — sutradhara

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`.**
> Read `CLAUDE.md` + `AGENTS.md` first. **Authoritative design:
> `docs/design-hd-disk-tier.md` §4 (+§3 columns) — read the doc in full.**
> Depends on: M1 (tables + store). Consumed by: M3 (fills), M6 (repop/drain).
>
> **What this is.** The `DiskPlacementPolicy` port and the default engine — the point of
> the whole tier. Pure catalog/cache-table logic; no disk I/O, no jobs, no CLI.

## What already exists — BUILD ON IT
- M1: `cache_disk`, `cache_entry` (indexed `bundle_key`, `group_key`, `disk_id`).
- The tape-selection-policy boundary pattern (keep disk selection behind the port exactly
  the same way — policy is swappable, engine is deterministic).

## Build order

### A. Port + context (`src/sutradhara/hdcache/placement.py`)
Design §4's `DiskPlacementPolicy` protocol and `PlacementContext` verbatim (content_sha256,
size_bytes, artifactclass, bundle_key, sibling_disks, group_key, group_disk_counts).
Context builder: **two indexed selects on `cache_entry`** (`bundle_key` → sibling_disks;
`group_key` → group_disk_counts) — no catalog fan-out per fill (design §4; callers stamp
bundle_key/group_key, M3).

### B. Default engine — the five steps of design §4, exactly
1. Candidates: `active` disks, `free ≥ size + reserve`, where free counts in-flight
   `filling` sizes as committed; reserve default `max(largest_expected_file + tmp_headroom,
   2% capacity)` (config).
2. **Size gate**: anti-affinity only for `size_bytes ≥ spread_min_bytes` (config, default
   1 GiB). Below: **invert to co-location** — prefer the disk already holding the group's
   small members (same bundle_key first, then group_key), fall back to fill-balance.
3. Hard anti-affinity (≥ gate): exclude `sibling_disks` while ≥1 candidate remains;
   degrade to minimal-overlap, never fail placement.
4. Soft anti-affinity: min by `(group_disk_counts[disk], filled_ratio, tiebreak)`;
   tiebreak = `sha256(disk_id ‖ content_sha256)`.
5. Enclosure spread: optional, config, off by default.

### C. Config
`spread_min_bytes`, reserve parameters, enclosure toggle — wired through the existing
config mechanism, all defaults per design §12.5/§12.8.

## Must-be-exact
- Deterministic: same inputs ⇒ same disk (property-tested).
- The engine never touches archival tables (`copy`/`pool`/…) — INV-1 test extended to
  assert placement imports stay inside hdcache + catalog-read-only surfaces it needs.

## Definition of done
- `uv run pytest -q` green. Property tests: bundle members ≥ gate land on distinct disks
  whenever candidates ≥ members; small same-group files co-locate; balance (fill spread
  bounded under uniform load); determinism; in-flight accounting prevents co-selection
  overshoot (simulate 24 concurrent placements); degrade-not-fail when candidates < members
  and when array is near reserve.
- Covers: unit tests here; end-to-end placement effects asserted by the harness scenario
  (`~/system/docs/prompt-hdcache-harness-scenario.md`).
- `docs/INDEX.md` + journal per AGENTS.md.
