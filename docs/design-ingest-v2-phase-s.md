# Design — Ingest v2, Phase S: bundling + data-driven policies

> Design by Claude + the owner (brainstorm 2026-06-10); implementation by codex.
> **Repo: `~/sutradhara/repo`.** Work order: `docs/prompt-ingest-v2-sutradhara.md`
> (Phase S). Master design: `~/system/docs/design-ingest-flow.md`. Verified by
> harness **scenario-S**. This doc is the *how*, grounded in the current code.

## 0. What Phase S adds
Today `fan_out → replicate_asset` seals **one asset**; `Copy.logical_asset_hash`
points straight at it; representation policy is the hardcoded
`o_archive_policy()` / `n_archive_policy()`. Phase S adds, for the new `s-*`
artifactclasses, **bundling** (a copy is a tar of N assets), **per-asset PFR
locators** (restore one asset out of a bundle), and **data-driven policy
documents**. O/N/Q/J keep their exact single-asset path and stay byte-identical.

## 1. Locked decisions (from the 2026-06-10 brainstorm)

1. **A bundle is a content-addressed _container_, never a business asset.** It
   reuses the content-addressed storage mechanism (its tar has a sha256, it gets
   `Copy` rows like any object), but it carries **no** asset semantics — no
   `virtual_path`, no tags, no proxies, it is never itself a restore target.
   Members are reached **only** via PFR. Made explicit by a dedicated `bundle`
   discriminator table (§3) and by excluding containers from every asset query.
2. **A bundle is single-class.** Every member shares one copy recipe (it is
   sealed once and placed by one policy). Proxies bundle with proxies, masters
   with masters.
3. **The bundle container is GNU tar** (the proven binary), one build, used by
   **all three** copies. See §2 for the layering and why this is the precise
   form of "rem-tar-v1 for working copies, GNU tar for the shelf."
4. **Two integrity layers, one read.** Building the tar streams every byte, so
   we (a) check each member's streamed sha256 == its registered hash, and
   (b) after each copy is written, **re-read every member and re-verify** — a
   media/write-error guard on every copy, not just a build-time check.
5. **Asset identity is the per-file plaintext sha256** (format-independent),
   preserved across all copies via PFR + per-member hashing. There is **no**
   single "bundle identity" the copies must share — each copy keeps its own
   `stored_digest`, exactly as raw/aead AOFs differ today.
6. **Oversize-artifact splitting is deferred** — a single artifact larger than
   the bundle max raises a clear error with a TODO marker; no M-way split yet.

## 2. Format layering (the locked decision, made precise)

`rem-tar-v1` (remanence's Rust pax container) and **the bundle** are *different
layers*. The rem path nests three:

```
REM tape (copy-1 aof-raw-v1 / copy-2 aof-aead-stream-v1):
  rem-tar-v1 [            ← remanence's own on-tape object envelope (UNCHANGED)
    AOF1 [               ← amber seal: raw (copy-1) or aead (copy-2)
      bundle.tar  ←GNU tar of the member assets (the new thing)
    ]
  ]

D2 shelf tape (copy-3 d2tar-raw):
  [bundle-meta][FM][ bundle.tar ][FM]   ← d2-cli lays the SAME GNU tar down raw
```

- The **bundle is GNU tar everywhere** (one build feeds all three copies):
  proven canonical writer, and the shelf copy (copy-3) is that raw tar —
  commodity-`tar`-readable in 20 years with zero of our software, which is its
  entire reason to exist.
- **`rem-tar-v1` stays exactly as it is** — remanence's object envelope around
  whatever it stores (the AOF). Bundling needs **no remanence change**: it
  already stores AOF objects; a bundle is just a larger AOF payload.
- **Refinement to flag:** we do **not** build the bundle itself as `rem-tar-v1`.
  Its one differentiating feature (256 KiB chunk-aligned bodies for
  parity/random-access) is **nullified inside the AOF**, and tape-level ranged
  reads for per-asset restore come from the **outer** `rem-tar-v1` envelope
  anyway (remanence `read_range`). So `rem-tar-v1`-as-bundle would add a
  Rust↔Python bridge + new remanence CLI surface for **no** gain over GNU tar.
  *(This is the one place this draft sharpens the lock; veto here if you intended
  the bundle bytes themselves to be `rem-tar-v1`.)*

amber and remanence are **untouched**. The only cross-repo change is the
`d2tape-cli` bundle-level write (§7).

## 3. Catalog model additions

Reuse the content-addressed mechanism; add container-ness + membership as new
tables. **`Copy`, `replication`, `scrub`, `status` are unchanged** — bundles ride
them for free because a bundle's bytes are just a content-addressed object.

- **`logical_asset` (unchanged schema):** holds content-addressed bytes for
  **both** real assets and **bundle tars**. A bundle tar is registered here like
  any object (`content_sha256` = sha256 of the GNU tar, `size_bytes` = tar size).
- **`bundle` (new):** the container discriminator + metadata.
  ```
  bundle_sha256  PK, FK -> logical_asset.content_sha256
  artifactclass  str
  member_count   int
  target_gb      int            # the packing target it was built under
  created_at     datetime
  ```
  A `logical_asset` **with** a `bundle` row **is a container**; **without** one it
  is a business asset. Every asset-facing query (`virtual_path`, tags, proxies,
  asset listings, restore-as-file) filters out rows that have a `bundle`.
- **`pfr` (new):** per-asset locator within a bundle.
  ```
  id             PK
  asset_sha256   FK -> logical_asset.content_sha256   # the member
  bundle_sha256  FK -> bundle.bundle_sha256
  offset         int            # byte offset of member data in bundle.tar
  length         int            # member size
  UNIQUE(asset_sha256, bundle_sha256)
  ```
  Offsets are into the **bundle tar plaintext**, shared by all copies (rem copies
  apply them after AOF open / through AOF offset mapping; the shelf applies them
  to the raw tar).
- **`Copy` (unchanged):** a bundle's copies have `logical_asset_hash =
  bundle_sha256` (the container asset). So copy-1/2/3 of a bundle are ordinary
  `Copy` rows on the container — `add_copy`, `replication_status`,
  representation-aware integrity, scrub all apply unchanged. O/N/Q/J copies are
  asset copies with **no** `bundle` row — literally untouched.

## 4. Part S1 — Artifactclass policy documents (`sutradhara/policy.py`)

Replace the hardcoded `o_/n_archive_policy()` with per-class TOML documents and
**one** accessor. Source dir from env **`SUTRADHARA_CLASSES`** (harness bringup
writes it).

**Document schema (archival scope ONLY; strict — unknown key/section = error):**
```toml
schema_version = 1
[[placements]]
placement_id   = "s-copy-1"
backend        = "primary-tape"
copy_class     = "copy-1"
representation = "aof-raw-v1"
offsite_gate   = false           # default false
# ... copy-2 (aof-aead-stream-v1, offsite_gate=true), copy-3 (d2tar-raw)
[proxies]
preview = true
mezz    = true
[bundling]
target_gb = 64
```

- **`policy.for_class(name) -> ClassPolicy`** (frozen dataclass: `placements:
  list[PlacementSpec]`, `proxies: Proxies(preview,mezz)`, `bundling:
  Bundling(target_gb)`). **No consumer reads TOML directly.** Strict validation
  rejects unknown keys/sections and missing required fields (a typo must never
  silently degrade durability).
- **`policy.representation_policy(name) -> RepresentationPolicy`** derives the
  existing `Mapping[(content_class, copy_class) -> representation]` from
  `placements`, so `replicate_asset`/`target_placements` stay **unchanged**.
- **Compat shims:** ship `o-archive.toml` / `n-archive.toml` reproducing O/N
  placements **exactly**; rewrite `o_archive_policy()` / `n_archive_policy()` (and
  the `replicate.py` branching) to read the documents. O/N tests must stay green.
- Governance (access/approval) keys on **tags** (Phase T), never here.

## 5. Part S2 — Bundler (`sutradhara/bundle.py`)

Input: an ordered list of `(asset_sha256, source_path, expected_size)`. Output:
one bundle tar on scratch + the PFR index + the bundle `content_sha256`.

- **Writer = GNU tar binary**, pinned + deterministic flags:
  `--format=posix --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner`,
  **no compression**. Members are **regular files only, normalized relative
  names**. Resolve the tar binary like amber resolves its CLI (`$SUTRADHARA_TAR`
  / known path / PATH); record the exact version string with the bundle.
- **Per-member streamed verify + offsets:** after writing, open the produced tar
  with Python `tarfile` in **read mode only** to read each `TarInfo.offset_data`
  (+ size) → PFR rows; for each member, seek to its offset, read `length`, assert
  `sha256 == registered`. Reading is the safe half of tar and every offset is
  validated by the hash, so a parse slip can't ship silently.
- **Read-back gate (standing test, run per build):** the produced tar must also
  extract cleanly under the proven reader (`tar -tf` / `tar -x` to a temp,
  member hashes match) before any copy is committed. Failure ⇒ no copies, error.
- **`B = sha256(bundle.tar)`** computed once = the container's `content_sha256`;
  never recomputed by re-taring.
- **Packing policy:** 1 artifact → 1 bundle (default); **N small same-class
  artifacts → 1 bundle** up to `BUNDLE_TARGET_GB` (from the class policy, default
  64). A single artifact exceeding the max ⇒ **clear error + `# TODO: oversize
  split`** (deferred).
- Scratch: ≈ `target_gb` per in-flight bundle; delete `bundle.tar` after all
  copies are written **and** verified.

## 6. Part S3 — Fan-out artifact integration

New entry `fan_out_artifact(session, artifact, artifactclass, *, backends, ...)`
where `artifact` = the ordered member list. It:
1. **Builds the bundle** (§5) on scratch.
2. **Registers** the bundle tar as a `logical_asset` + a `bundle` row
   (artifactclass, member_count, target_gb), and writes **PFR** rows.
3. **Seals + fans out** by calling the existing `replicate_asset` on the
   **container asset** (`asset_hash = B`, `source_path = bundle.tar`,
   `content_type = artifactclass`, `policy = policy.representation_policy(...)`,
   `sealer = AmberCliSealer(...)`, `key_epoch = ...`). The representation-aware
   integrity check already asserts `plaintext_digest == B`. Copies land on the
   container asset — **no change to `replicate_asset`**.
4. **Verifies every member on every copy** (§8) before deleting scratch.

The existing single-asset `fan_out` (O/N/Q/J) is unchanged. Dispatch: classes
whose policy has a `[bundling]` section (the `s-*` classes) go through
`fan_out_artifact`; O/N/Q/J keep the single-asset path (no tar, no PFR).

## 7. Part S4 — D2 shelf copy + `d2tape-cli` companion (cross-repo)

copy-3 (`d2tar-raw`) is the **raw GNU-tar bundle** on the d2 shelf — commodity
readable. The `d2tape` backend's `write_object_to_pool` is extended to send the
**pre-built bundle tar** to `d2tape-cli`, which writes it as **one tape object**
(bundle-level: one metadata block + filemark + the tar + filemark) and returns
`artifactStartVolumeBlock`. This is the granularity change that fixes shoeshine
(one object per bundle, not per small artifact) and is the **only** cross-repo
change.

- **`d2tape-cli` (Java) companion** — its own small design/prompt:
  - new write mode: ingest a provided tar as one object (no internal re-tar),
    bundle-level metadata, return `start_block`;
  - read-back + **ranged** read within the object (whole-then-slice is acceptable
    v1) for per-asset restore;
  - **no** old-tape read-compat required (confirmed: old d2 software need not
    read new tapes).
- **Sequencing:** S2/S3/S5 + the rem copies + PFR are buildable and unit-testable
  in sutradhara **now** (memory + rem). scenario-S's `s-copy-3` goes green when
  the `d2tape-cli` companion lands; land it as a tight follow-cut if needed.

## 8. Part S5 — Per-asset restore + verify-after-write

- **Restore:** `restore_asset(session, asset_sha256)` → look up `pfr` → `bundle`
  → select a healthy `Copy` of the bundle (prefer the working rem copy) →
  `read_range(offset, length)` on that copy → assert `sha256 == asset_sha256`.
  Multi-asset restores order by `(copy/tape, bundle, offset)`.
  - memory backend (unit tests): direct `read_range` on the raw-bytes bundle.
  - rem aof-raw copy: map plaintext offset through AOF1 framing + remanence
    `read_range` (proven). aof-aead copy: amber-open whole, then offset.
  - d2 shelf: d2 ranged read (whole-then-slice v1).
- **Verify-after-write (every copy):** after writing copy-1/2/3, re-read each
  member via that copy's read path and assert `sha256 == registered`. Distinct
  from the build-time check — it catches media/transmission errors per copy.

## 9. Tests (unit; per the prompt + DoD)

- Deterministic offsets (same inputs → same PFR), packing boundaries (1→1; N
  small → 1 up to target; oversize → error).
- Bundler round-trip with the **memory backend**: build → register → PFR →
  fan-out (raw-bytes) → per-asset restore → member hashes match.
- GNU-tar read-back gate: `tar`/`bsdtar` extract members, hashes match.
- Policy registry: strict validation **rejects unknown keys/sections**; compat
  shims reproduce O/N placements **exactly**; `for_class` shape.
- representation-aware integrity unchanged; container excluded from asset queries.
- **Non-regression:** full existing suite green; `raw-bytes` default intact.

## 10. Constraints / DoD
- O/N/Q/J behavior + tests unchanged; policy documents reproduce their placements
  exactly (compat shims).
- Encrypted copies aren't byte-deterministic — assert plaintext round-trip +
  within-run digest stability, never a fixed ciphertext digest.
- Per `AGENTS.md`: run `uv run pytest -q` (paste output), commit per phase to
  `wip/<topic>` (never leave the tree dirty), update `docs/INDEX.md`.

## 11. Open items to confirm
- The §2 refinement (bundle = GNU tar everywhere; `rem-tar-v1` stays the rem
  envelope) — veto if you meant the bundle bytes themselves to be `rem-tar-v1`.
- Mezz copy count (1 vs 2) — policy-table entry; default 1, revisit.
- `d2tape-cli` companion is a separate prompt; confirm whether Phase S commits
  land before or alongside it (affects scenario-S green timing).

## 12. Acceptance
Phase S = **scenario-S green** (rough-seg send → inode-match → multi-asset bundle
→ 3-copy fan-out → per-asset PFR restore from a bundle; foreign-file-at-send ⇒
Error), unit suites green throughout.
