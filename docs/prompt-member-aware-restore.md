# Codex prompt — P2.2: member-aware restore, true partial (`RAO_PLAIN`) — sutradhara

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo` (single repo).**
> Read `CLAUDE.md` + `AGENTS.md` first.
>
> **Authoritative design: `docs/design-member-aware-restore.md` — read it in full.** This prompt is
> the build order, the must-be-exact contracts, and the acceptance tests; the design doc is the *why*.
> Source: plan item **P2.2** in `docs/implementation-plan-ingest-v2.md`.
>
> **What this is.** Make member-level restore read **only the member's bytes**, not the whole bundle.
> Member-aware restore already exists (`archive_restore.py::restore_asset`, the `sutra archive restore`
> CLI) and is **already partial** for `D2TAR_RAW`/local-offset locators, but it **materializes the
> whole object for RAO**. P2.2 fixes that for **`RAO_PLAIN_V1`** — a near-trivial lift of a pattern
> that already exists at *build* time (`archive_fanout._verified_member_bytes`).
>
> **Scope is `RAO_PLAIN` only (read this twice).** `RAO_AEAD` *partial* restore is **explicitly OUT** —
> it needs a Remanence partial-encrypted-range contract that does not exist (the current `rem archive
> extract` reads the whole `--object` before decrypting; stored offsets are
> `cipher_offset(metadata_frame_len, …)`, not `first_chunk_lba*(chunk+tag)`). **`RAO_AEAD` member
> restore must keep working unchanged via whole-object materialize — do NOT attempt partial AEAD.**

## What already exists — BUILD ON IT, do not rebuild
- **`restore_asset`** (`src/sutradhara/archive_restore.py`): selects the best healthy `AssetLocator`
  by the artifactclass `restore_preference`, calls an extractor to write stored member bytes,
  `reverse_transforms_to_path` (`.zst`), verifies `sha256 == logical_asset_hash`, then **finishes with
  `restored_path.replace(output_path)`** after `output_path.parent.mkdir(parents=True, exist_ok=True)`.
- **Extractors** (`archive_restore.py`): `LocalArchiveExtractor.extract_to_path` is **partial** for
  `D2TAR_RAW` (`block_range`) and local `offset` (`_copy_backend_range_to_path(... offset, offset+size
  ...)`); RAO locators (carrying `first_chunk_lba`, not `offset`) fall to `RemArchiveExtractor` →
  `_materialize_copy_to_path` (**whole object**) → `rem archive extract`.
- **The partial pattern at build time** (`archive_fanout._verified_member_bytes`): for `RAO_PLAIN_V1`
  it reads `backend.read_range(copy_locator, ByteRange(first_chunk_lba*RAO_CHUNK_SIZE,
  first_chunk_lba*RAO_CHUNK_SIZE + member.size_bytes))` — **the exact partial read P2.2 lifts into
  restore.** (`D2TAR_RAW` → `block_range`; `offset` → cached container; `RAO_AEAD` → builder verifier.)
- **P2.1 leaf helpers** (`src/sutradhara/restore.py`): `atomic_write_verified_file(source, dest)`
  (temp → fsync file → `os.replace` → fsync dir; **rejects a relative dest and a missing parent**),
  `sha256_file`.
- **`RAO_CHUNK_SIZE`** = `sutradhara.sealing.rao.RAO_CHUNK_SIZE`. `AssetLocator.native_locator` carries
  `first_chunk_lba`, `size_bytes`, `member_path` (helpers `_first_chunk_lba`/`_size_bytes`/`_member_path`).

## Build order

### A. The shared `read_member_to_path` primitive (path/stream-first) — the DRY core
Add **one** canonical primitive (in `archive_restore.py` or a small `member_restore.py`) that both
restore **and** build-verify route through, so the partial-read logic lives in one place and they can
never diverge:

```python
def read_member_to_path(backend, copy, asset_locator, dest: Path) -> int:  # returns size_bytes written
    """Write one member's STORED bytes from one copy to dest, reading only that member where possible.
       Streams to a file (a member can be large) — never whole-into-memory."""
```
Dispatch on `Representation(asset_locator.representation)`:
- **`size_bytes == 0` → FIRST, before anything:** write an **empty** `dest`, do **no** backend read,
  do **not** read `first_chunk_lba`. (Empty files have `size_bytes==0` and may carry null
  `first_chunk_lba`.)
- **`D2TAR_RAW`** (`block_range`) → `read_range(ByteRange(start, end))` streamed to `dest` (as today).
- **local `offset`** → existing partial path.
- **`RAO_PLAIN_V1`** → `read_range(ByteRange(first_chunk_lba*RAO_CHUNK_SIZE,
  first_chunk_lba*RAO_CHUNK_SIZE + size_bytes))` streamed to `dest` — **the new branch** (mirror
  `_verified_member_bytes`). **No whole-object materialize, no rem subprocess.**
- **`RAO_AEAD_V1`** → **unchanged: whole-object materialize + `rem archive extract`** (the current
  `RemArchiveExtractor` path). Do **not** attempt partial.

Provide a thin `read_member_bytes(...) -> bytes` wrapper (read_member_to_path to a temp, read it) for
build-verify/tests only. **Route `archive_fanout._verified_member_bytes` through it** (its
`cached_container` optimisation for local-archive members may stay a build-side wrapper around the
primitive; do not keep a *second* partial-read implementation).

### B. Wire restore through it
`restore_asset`'s extractor step writes the stored member via `read_member_to_path` (replacing the
`LocalArchiveExtractor`/`RemArchiveExtractor` branching for the partial cases; AEAD still routes to the
existing materialize+rem path). The downstream `reverse_transforms_to_path` + sha verify are unchanged.

### C. Durable atomic destination (preserve the CLI behaviour)
Replace `restore_asset`'s final `restored_path.replace(output_path)` with the P2.1 durable write, but
**preserve today's relative-path + parent-creation UX** — in this order:
1. `dest = Path(destination).resolve()` (absolute — `atomic_write_verified_file` rejects relative);
2. `dest.parent.mkdir(parents=True, exist_ok=True)` (the helper rejects a missing parent; today
   `restore_asset` auto-creates nested parents);
3. `atomic_write_verified_file(restored_path, dest)`.

Do **not** loosen `atomic_write_verified_file`; normalise + mkdir at the caller.

### D. Leave P2.1 `restore_copy` untouched
Different unit (whole asset-scoped copy vs bundle member). Share only `sha256_file` /
`atomic_write_verified_file`; no change to `restore.py`'s `restore_copy`/handler.

## Must-be-exact contracts
- **`RAO_PLAIN` partial read range is `[first_chunk_lba*RAO_CHUNK_SIZE, +size_bytes)`** — identical to
  `_verified_member_bytes`. It must hit `backend.read_range` with **that** `ByteRange`, **never**
  `ByteRange.whole`/`ByteRange(0,0)`.
- **`size_bytes == 0`** is handled before any read or `first_chunk_lba` access → empty file.
- **`RAO_AEAD` is unchanged** (whole-object materialize + rem extract) — no partial, no regression.
- **Durable write** = resolve→mkdir(parents)→`atomic_write_verified_file`; relative and nested `--dest`
  still work.
- **One partial-read implementation** (`read_member_to_path`), shared by restore and build-verify.
- **`restore_asset` policy/verify/transform/suspect behaviour is otherwise unchanged.**

## Tests — DoD (`tests/test_member_restore.py` + extend `tests/test_archive_fanout_restore.py`)
Use a **counting/asserting backend** (records every `read_range` call's `ByteRange`).
- **`RAO_PLAIN` member restored partially (the accept criterion)** — multi-member RAO-plain bundle:
  restore one member → `sha256` matches **and** the backend served **only** `[first_chunk_lba*CHUNK,
  +size)`, **not** `ByteRange.whole`.
- **zero-byte member** — an archived empty file restores to an empty file with **zero** backend ranges
  served; `sha256` verifies `e3b0c4…`.
- **`RAO_AEAD` member still restores (no regression)** — AEAD member restores correctly via the
  existing materialize path; `sha256` matches. (No partial-AEAD assertion.)
- **durable atomic write + relative/nested `--dest`** — `sutra archive restore --dest new/rel/dir/file`
  resolves to absolute, auto-creates nested parents, restores; a failed restore leaves no partial file;
  existing dest replaced only on success.
- **shared primitive parity** — `read_member_to_path` produces identical bytes for build-verify and
  restore on the same member, and streams to a path (assert it does not load whole-into-memory, e.g.
  via the chunked read pattern / a large-member fixture).
- **non-RAO partial path unchanged** — `D2TAR_RAW` / offset members still restore partially.
- **policy fallback + integrity** — corrupt first-pool locator falls through to the next healthy one;
  all-fail → `RestoreIntegrityError`; suspect asset refused without `--force`.
- **existing suites green** — `uv run pytest`; the `archive_fanout`/restore scenarios in `~/system`.

## Acceptance
- A `RAO_PLAIN` member restores **reading only that member's byte range** (asserted partial, not
  whole-object) with `sha256` match; zero-byte members handled; `RAO_AEAD` member restore **still
  works** (whole-object, no regression); the non-RAO partial path and `restore_asset`'s policy/verify
  behaviour unchanged; member restore uses the durable atomic write with relative+nested `--dest`
  preserved; one shared `read_member_to_path` used by restore and build-verify.
- `uv run pytest` + format + type-check green; the `~/system` archive/restore scenarios green from a
  clean slate.

## Out of scope (do NOT build here)
- **No `RAO_AEAD` *partial* restore** — Remanence partial-encrypted-range contract first (design §3.2,
  §5/§8); AEAD stays whole-object materialize.
- **No sub-clip PFR** (time-range within a video) — P4.2.
- **No real container `pfr-index` sidecar** — P4.1; use the existing `asset_locator` chunk index.
- **No member-restore job** — deferred to P3.1 (VS); P2.2 makes the existing sync path partial.
- **No change to P2.1 `restore_copy` / the `restore` job handler** (different unit).
