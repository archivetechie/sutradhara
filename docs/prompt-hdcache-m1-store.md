# Codex prompt — hdcache M1: schema + disk store + disk lifecycle CLI — sutradhara

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`.**
> Read `CLAUDE.md` + `AGENTS.md` first.
>
> **Authoritative design: `docs/design-hd-disk-tier.md` — read it in full** (frozen
> 2026-07-02). This prompt is the build order + acceptance for milestone M1; the design is
> the why. Prompt set: M1→M6 dependency-ordered; M1 has no hdcache prerequisites.
>
> **What this is.** The foundation slice: the two inventory tables + restore-request tables
> (schema only — no serve logic yet), the per-disk store (layout, atomic I/O, identity),
> and the disk lifecycle CLI. NO placement policy (M2), NO fills (M3), NO restore seam (M4),
> NO walker/rebuild logic beyond store primitives (M5).

## What already exists — BUILD ON IT, do not rebuild
- `catalog/models.py` — model conventions (BLOB(32) hash FKs, `ondelete=CASCADE` siblings,
  partial-unique patterns). Migration head: chain from the current alembic head.
- `restore.py:105` `atomic_write_verified_file` / `:141` `sha256_file` — the *discipline*
  (temp + fsync + rename; digest checks). M1 adds the **stream-hashing write helper**
  (hash-while-writing) the design §2 names as new code.
- `backend/ssh_disk.py` — reference for plain-disk file mechanics. Do NOT register any
  backend (design INV-1).
- CLI group pattern — mirror an existing `sutra <noun>` command family.

## Build order

### A. Models + migration (design §3 is the spec — follow it column-for-column)
`src/sutradhara/hdcache/models.py`: `cache_disk`, `cache_entry` (incl. `artifactclass`,
`bundle_key`/`group_key` indexed, `trusted`, states `filling|present|lost`),
`restore_request`, `restore_request_item` (state enums from
`docs/contract-hdcache-restore.md` §4 — normative). One alembic revision;
`tests/test_schema.py` extended (create_all ≡ upgrade head).

### B. Store (`src/sutradhara/hdcache/store.py`)
- Layout v1 exactly as design §3: `hdcache/v1/<aa>/<sha256hex>[.rao-aead-v1.<key_epoch>]`,
  `hdcache/v1/tmp/` (mkstemp-unique names ONLY), `hdcache-disk.json` sentinel
  `{disk_id, serial, fs_uuid, layout, enrolled_at, hmac}` — HMAC keyed by a server-held
  secret (config; document the key location).
- Primitives: `write_entry` (tmp → fsync → rename; **streams sha256 during write** and
  refuses on mismatch — INV-2), `read_entry_verified` (stream-verify on read),
  `delete_entry`, `enumerate_entries` (filename → (sha, representation, key_epoch)),
  `verify_disk_identity(mount, expected)` → findmnt mount check + block serial/wwn +
  fs_uuid + sentinel HMAC (design §8.2) returning a typed result (never raises destructive
  paths into being).
- All destructive store ops confined to the `hdcache/v1` subtree by construction.

### C. Disk lifecycle CLI (`sutra hdcache disk …`, design §8.1/§8.3 semantics; NO drain/repop yet)
- `add <block-dev>` and `add --scan` (batch: list unenrolled devices w/ serial +
  enclosure/bay via lsscsi/SES where available — degrade gracefully on absence), auto
  sequential disk_ids, **LUKS2 create + keyfile + escrow-presence check**, mkfs.xfs,
  record fs_uuid/slot, write sentinel, mount, insert row. Wrap privileged/hardware steps
  behind a `DiskProvisioner` port so hermetic tests use a tmpdir fake (no LUKS/mkfs in CI).
- `list` (hides dead by default; `--all`), `locate <id>` (SES LED, best-effort),
  `forget <id>` (dead + no referencing entries only), `retire <id>` / `dead <id>` — M1
  implements **state flips + batched entry lost-marking for `dead` (~1k rows/txn, design
  §8.3) + LUKS key-slot drop + prints what-will-happen/done-when**; the drain migration
  (retiring) and repopulation enqueue land in M3/M6 (leave a clearly named seam:
  `on_entries_lost(disk_id, ...)` hook that M3 wires to fill enqueueing).
- `status` — summary by default (totals, per-state counts, worst-N disks), `--disks` /
  `--disk <id>` for detail (design §8.4).
- Enrollment manifest output (disk_id ↔ serial ↔ bay printable list).

## Must-be-exact
- No `backend`/`pool`/`copy`/`artifactclass_pool` rows anywhere; add the **INV-1 invariant
  test** (design §11) now — it guards every later milestone.
- SMART polling: store a `smart_status` string via a provisioner-port method (fake in
  tests); no alerting logic yet (M6).
- disk_ids never reused; serial UNIQUE.

## Definition of done
- `uv run pytest -q` green. New tests: schema round-trip; store atomicity (crash-sim: tmp
  orphan left behind, rename-then-no-DB is representable); stream-hash mismatch refusal;
  identity-check matrix (wrong serial / fs_uuid / missing sentinel / bad HMAC ⇒ non-OK);
  enumerate round-trip incl. AEAD filenames; CLI flows on fake provisioner (add/--scan/
  retire/dead/forget/status incl. batched lost flip); INV-1 test.
- Covers (verification member): unit tests here + scenario cut in
  `~/system/docs/prompt-hdcache-harness-scenario.md` (asserts M1 enrollment + dead-flip
  end-to-end once M3/M4 land).
- `docs/INDEX.md` row update (this prompt → implemented) + journal note per AGENTS.md.
