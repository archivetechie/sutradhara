# Prompt RM0.2 — selection-preserving `RestorePlan` + plaintext streaming pipeline (sutradhara)

**Status:** pending (gpt-5.6-sol). Second milestone of streaming restore (RM0). Builds on **RM0.1**
(landed b47c6b5: `StreamingStorageBackend.open_range_chunks` context manager + `StreamKind`).
**Normative (read FIRST, binding — do NOT inline):** `docs/design-streaming-restore.md` v0.2 — §3.2
(`RestorePlan`), §3.3 (representation producers), §3.4 (Opener seam), §3.5 (bounded memory), §5-E +
**§8-M3** (verified chunk sink), **§8-M2** (selection semantics), **§8-M4** (two-layer fixity), and
RM0.2 in §8.1. **Read the real code you are refactoring (do NOT trust prose — cite it):**
`src/sutradhara/archive_restore.py` (`restore_asset` ~:313, `restore_assets_from_bundle` ~:414,
`read_member_to_path` ~:211, `_copy_backend_range_to_path` ~:826, `_restore_pool_order` ~:698, the
D2/RAO-plain/RAO-AEAD branches ~:237/253/264, bundle group choice `_choose_bundle_restore_group`
~:623), `replication.py` (`select_source_candidates(purpose="user_restore")` ~:463, `_user_restore_candidates`
~:502), `restore.py` (`restore_copy` ~:59, `atomic_write_verified_file` ~:110 — note it does NOT hash),
`staging.py` (`reverse_transforms_to_path` ~:299, `zstd-file-v1` ~:41/259/619, `appledouble-merge-v1`
reversible=False ~:351), the RM0.1 capability (`backend/port.py` `StreamingStorageBackend`/`StreamKind`,
`backend/remanence.py`/`s3.py` `open_range_chunks`), and `hdcache/manager.py`
(`canonicalize_restore_destination` ~:957 — the live confinement funnel).

## Scope
1. **`RestorePlan` (§3.2, §8-M2).** A built (not executed) plan from a restore request item:
   `item → AssetLocator / bundle membership → ordered source candidates → per-member (representation +
   transforms) → chunk producer`. Two selection paths, **semantics preserved exactly**:
   - **Single-asset:** resolve `select_source_candidates(purpose="user_restore")` (artifactclass pool
     preference + pool order) into **typed `(copy, locator, transforms)` candidates** (the selector
     returns `Copy` rows only — you must resolve the `AssetLocator` + transform/member relationship per
     candidate). Preserve: skip non-OK/deleted/missing-backend; **first-successful-candidate wins**;
     only a final logical-digest mismatch marks the copy `SUSPECT` (extraction/transform errors merely
     fall through to the next candidate).
   - **Bundle:** a SEPARATE group-candidate planner choosing one copy that covers EVERY requested
     locator, ordered by pool rank then copy-id; **no** cross-group retry and **no** SUSPECT on member
     mismatch (match current behavior — any change is an EXPLICIT decision, not an accident).
   `RestorePlan` exposes `iter_members() -> Iterator[PlannedMember]` and
   `open_member_stream(member) -> ContextManager[Iterator[bytes]]` (verified plaintext chunks).
2. **Streaming plaintext producers (§3.3), bounded per §3.5 (one chunk + a small queue per stage):**
   - `RAO_PLAIN_V1`: member-selective ranged reads via the RM0.1 `open_range_chunks` (generalize
     `_copy_backend_range_to_path`) — no CLI, no whole-object.
   - `RAW_BYTES` / `D2TAR_RAW`: direct ranged `open_range_chunks` where a `block_range` locator exists;
     a D2 tar member WITHOUT `block_range` stays a scratch/materialization path (honest — §8-M1), do not
     claim bounded/lazy for it.
   - **`RAO_AEAD_V1`: KEEP the existing materialize-whole-RAO + `rem archive extract` CLI path unchanged
     for now** (route it through the plan as a `buffered` member). AEAD streaming (option A) is **RM0.3**
     — do NOT attempt it here; do NOT break the current encrypted path.
3. **Transform stream stages (§3.3, review §8 minor):** implement `identity` and `zstd-file-v1` as
   streaming stages (`ZstdDecompressor.copy_stream` already streams). `appledouble-merge-v1` stays
   skipped (reversible=False). Unknown reversible transform kinds **fail closed** (as `staging.py:333`).
4. **The verified chunk sink (§5-E, §8-M3 — `atomic_write_verified_file` does NOT hash):** a new sink
   that (1) hashes the stored-member stream while feeding the transform, checks the staged/member
   digest; (2) hashes + counts the transformed logical output while writing an **exclusively-created**
   dest-side temp; (3) requires expected logical size + SHA-256; (4) fsync; (5) rename + dir-fsync ONLY
   after clean EOF AND both hashes match; (6) delete the temp on ANY source/transform/size/digest
   failure. Two-layer fixity = stored-member/staged digest + logical-asset digest after reversal
   (§8-M4) — do NOT claim whole-object stored fixity from a member range.
5. **Refactor the callers onto the plan.** `restore_asset` / `restore_assets_from_bundle` become
   "build plan → for each member, `open_member_stream` → verified chunk sink → dest". The LOCAL writer
   is the parity oracle (byte-for-byte identical output vs today for the local path). `restore_copy`
   (self-heal primitive) is UNCHANGED.

## Binding invariants
- **Selection + failure semantics are byte-for-byte preserved** (pool order, first-success, SUSPECT
  marking, bundle grouping) — enforced by PARITY tests, not assertion.
- `canonicalize_restore_destination` (confinement funnel) runs before source resolution — preserved.
- No whole-object fixity from a partial/member read (§8-M4).
- Wrap the existing selection/extraction machinery; do NOT fork a parallel restore path. AEAD path
  unchanged (RM0.3 owns it). No runtime/compat flag (git revert is the backout). `read_range`/RM0.1
  capability unchanged.

## Tests (§8.1 RM0.2 row)
- **Parity (the guard against silent regression):** pool-order preference; first-successful-candidate
  wins; a failing candidate → next candidate tried; final logical-digest mismatch → copy SUSPECT
  (extraction/transform error → fall through, NOT suspect); bundle group covers all requested locators,
  no cross-group retry, no member-mismatch SUSPECT; path confinement (`..`/absolute/symlink rejected).
  Assert the SAME outcomes as the pre-refactor code.
- **Streaming round-trip + bounded RSS:** archive → streamed restore → sha match, for `RAO_PLAIN_V1`,
  `RAW_BYTES`, ranged `D2TAR_RAW`; peak RSS bounded (not whole-object) on a large fixture.
- **Verified sink:** stored-member digest mismatch → no dest file, temp deleted; logical size mismatch
  → fail; logical hash mismatch → fail; success → exactly the verified bytes, atomic.
- **Transforms:** `zstd-file-v1` round-trips as a stream stage; unknown reversible kind fails closed.
- **AEAD still works:** an `RAO_AEAD_V1` restore through the plan produces correct plaintext via the
  existing (unchanged) path.
- **hdcache untouched:** the hdcache cache-hit path (`_serve_from_cache`) is NOT routed through the plan
  (§7.5) — its suite stays green.

## Definition of done (this repo's AGENTS.md)
`uv run pytest -q` green (paste tallies), `uv run ruff format --check src tests`,
`uv run ruff check src tests`, and `uv run mypy` **on the files you touched** clean (the repo carries
pre-existing mypy debt elsewhere — do not regress your files, do not attempt the whole tree). Summary:
files touched, each §8.1 behavior → its test, the parity assertions, and an explicit statement that
AEAD is unchanged and `canonicalize_restore_destination` still gates before source resolution. Do NOT
`#[ignore]`/skip the parity or verified-sink tests.
