# Codex prompt — RAO sealing migration (sutradhara): replace amber AOF1 with remanence RAO

> Design by Claude; implementation by codex. **Repo: `~/sutradhara/repo`** (the engine).
> One of a coordinated pair — the companion harness prompt
> (`~/system/docs/prompt-rao-sealing-migration-harness.md`, a different codex instance)
> migrates scenario N/O/Q assertions and retires the legacy amber surfaces. The two are
> decoupled by the **Shared contract** below. **This side lands first** — the harness
> side depends on it being on `main` (the harness uses editable installs tracking main).

## Why
Amber was merged into remanence (2026-06-12): AOF1's successor is **RAO**
(`remanence-aead` + `remanence-format`; spec `~/remanence/docs/rao-1.0-specification.md`;
merge review `~/remanence/docs/code-review-rao-amber-merge-2026-06-12.md`). The
`~/amber` repo is frozen. Sutradhara is the last live consumer of the frozen amber CLI
via `src/sutradhara/sealing/amber.py`. This migration makes sutradhara seal/open RAO
objects through remanence's CLI, after which the amber binary has zero consumers.

**Clean cutover, no dual-format support:** there is no production AOF1 data — scenario
state only counts from a clean slate (`make reset` in ~/system). Remove the AOF
representations entirely; do not keep an AOF read path.

## What exists today (read these first)
- `src/sutradhara/sealing/port.py` — `Representation` StrEnum, `SealResult`,
  `Sealer`/`Opener` protocols (context managers yielding sealed/plaintext paths).
- `src/sutradhara/sealing/amber.py` — `AmberCliSealer`/`AmberCliOpener`/`inspect_aof`:
  the module this migration replaces and deletes.
- `src/sutradhara/sealing/policy.py` — (content_class, copy_class) → representation maps
  for `o-archive` and `n-archive`.
- `src/sutradhara/replicate.py` + `src/sutradhara/replication.py` — wire the sealer/opener
  defaults; AEAD-specific key-epoch logic (`_epoch_for`, the
  `representation == Representation.AOF_AEAD_STREAM_V1.value` checks, `_assert_copy_integrity`).
- `src/sutradhara/cli/admin.py` — admin surface touching amber/inspect.
- `src/sutradhara/keys/registry.py` — `materialized_root_key(key_id)`; unchanged.
- Reference RAO CLI driver: `~/system/harness/seams/rao.py` (the harness already drives
  `rem-debug archive build/inspect/extract` and parses its JSON reports) and
  `~/system/scenarios/scenario_rao.py` (proves the CLI behavior end to end, including
  report field names).

## The work
1. **New `src/sutradhara/sealing/rao.py`** implementing the existing port:
   - `RaoCliSealer.seal(source, representation, *, key_epoch)` →
     `rem-debug archive build --inputs <source> --out <tmp>.rao --chunk-size 262144
     [--encrypt --key-file F --key-id ID] --object-id … --caller-object-id …
     --manifest-file-id … --timestamp …` per the Shared contract determinism rules.
     Yield `SealResult(sealed_path, stored_digest, plaintext_digest, representation)`
     with the digest mapping from the Shared contract (and its two drift assertions).
     `RAW_BYTES`/`D2TAR_RAW` pass-through behavior is unchanged — copy it from the
     amber module.
   - `RaoCliOpener.open(source, representation, *, key_epoch)` →
     `rem-debug archive extract --object <source> --dest <tmpdir>` (+ `--chunk-size`
     for `rao-plain-v1`, + `--key-file` for `rao-aead-v1`), then yield the single
     restored member (`<dest>/<basename>`; fail if not exactly one regular file).
   - `inspect_rao(path)` — keyless traceability (replaces `inspect_aof`): map the rem
     report's `representation` (`plaintext`/`encrypted`) to the catalog representation
     strings; recover `key_id` for encrypted objects; return the raw report alongside.
   - `resolve_rem_bin()` per the Shared contract resolution order.
   - Reuse the JSON-report parsing/subprocess idioms from `~/system/harness/seams/rao.py`
     rather than inventing new ones.
2. **`port.py`**: replace `AOF_RAW_V1`/`AOF_AEAD_STREAM_V1` with `RAO_PLAIN_V1 =
   "rao-plain-v1"` and `RAO_AEAD_V1 = "rao-aead-v1"`. `RAW_BYTES`, `D2TAR_RAW` unchanged.
3. **`policy.py`**: flip `o_archive_policy` / `n_archive_policy` to the new values
   (copy-1 `rao-plain-v1`, copy-2 `rao-aead-v1`; n copy-3 stays `d2tar-raw`).
4. **`replicate.py` / `replication.py`**: default sealer/opener become the RAO ones; every
   `AOF_AEAD_STREAM_V1` check becomes `RAO_AEAD_V1`; record copy metadata
   `{"representation": <value>, "chunk_size": 262144}` for RAO copies wherever
   representation is recorded today. `_assert_copy_integrity` semantics unchanged
   (plaintext_digest == asset hash; integrity_hash == stored_digest).
5. **`cli/admin.py`** and `sealing/__init__.py` exports: migrate to the new module.
6. **Delete `src/sutradhara/sealing/amber.py`** and all amber imports/references.
7. **Tests**: port `tests/test_sealing_amber.py` to the RAO module (rename accordingly);
   keep the existing fake-CLI unit-test style AND add one integration test against the
   real `rem-debug` binary (skip cleanly if the binary is absent) covering: plaintext
   round trip, encrypted round trip via a temp key registry, keyless-open failure, and
   **deterministic re-seal** (seal the same source twice → byte-identical object files).
   Update `tests/test_cli.py`, `tests/test_self_heal.py`, etc. as the imports move.

## Acceptance (definition of done — run and paste outputs)
- Full pytest suite green.
- `grep -rni "amber\|aof" src tests` returns nothing (docs/changelog mentions are fine).
- The deterministic re-seal test passes against the real binary on this box.
- `docs/INDEX.md` updated (this prompt → implemented when done); commit to `main`
  per AGENTS.md (the harness side cannot start until this is on main).

## Shared contract (IDENTICAL in both prompts — sutradhara seals, harness verifies)
1. **rem binary resolution:** `$REM_BIN` env var, else
   `~/remanence/target/release/rem-debug` (the order `~/system/harness/seams/rao.py::_rem_bin`
   already uses). Both sides must resolve identically.
2. **Representation strings (exact, catalog-visible):** `raw-bytes` and `d2tar-raw`
   unchanged; **new** `rao-plain-v1` (plaintext `rao-v1` tar bytes) and `rao-aead-v1`
   (encrypted `RAO1` envelope). The old `aof-raw-v1`/`aof-aead-stream-v1` are removed —
   clean cutover, no dual-format support. The rem CLI reports `representation` as
   `plaintext`/`encrypted`; map at the boundary.
3. **CLI surface:** `rem-debug archive build / inspect / extract` with JSON reports.
   Plaintext objects need `--chunk-size` on inspect/extract; encrypted objects carry
   geometry in the header (keyed inspect/extract take no chunk size).
4. **Chunk size:** every sutradhara-sealed RAO object uses **262144** (256 KiB),
   constant `RAO_CHUNK_SIZE` on both sides; recorded in copy metadata as
   `"chunk_size": 262144`.
5. **Identity and digests:**
   - Logical asset identity stays **sha256(source plaintext file)** — in RAO terms the
     single member's `file_sha256` row in the build report. `SealResult.plaintext_digest`
     = asset hash; assert it equals the files-row `file_sha256` or fail.
   - `SealResult.stored_digest` = **sha256 of the exact stored object bytes** =
     `Copy.integrity_hash` = the cross-backend comparison key. Compute sha256 of the
     sealed file locally and assert it equals the build report's `stored_digest`
     (halt on contract drift).
   - Caution: the build report's top-level `plaintext_digest` digests the `rao-v1` tar
     **body**, not the member — it is NOT the asset hash.
6. **Member path:** one member per object, at the source file's **basename**; full
   extract restores `<dest>/<basename>`.
7. **Determinism (the self-heal property):** RAO sealing consumes no randomness.
   `--object-id`, `--caller-object-id`, `--manifest-file-id` are UUID-shaped values
   derived solely from (asset plaintext digest, basename, representation string,
   key_id-or-empty) via sha256 with three distinct domain-separation labels, formatted
   with the UUID version/variant bit trick used in
   `~/system/scenarios/scenario_rao.py::_object_id`. `--timestamp` is the fixed
   constant `2026-01-01T00:00:00Z`. Re-sealing the same asset under the same key MUST
   be byte-identical (scenario Q asserts a rebuilt copy's stored_digest equals the lost
   copy's cataloged integrity_hash).
8. **Keys:** same registry as today — `$SUTRADHARA_KEY_REGISTRY_DIR`, default
   `/var/lib/replica/sutradhara-key-registry`. Sutradhara seals encrypted copies under
   `epoch.key_id`; the harness recovers `key_id` from a **keyless**
   `archive inspect` of the stored object (`keyed=false`, `key_id` exposed, no
   plaintext structure leaked), then `keys.materialized_root_key(key_id)` → keyed
   extract. The dev-key derivation seed string in `~/system/harness/seams/keys.py`
   (`system-harness:sutradhara-key-seam:amber-aead-dev:v1`) is **frozen** — renaming it
   would change dev key material; leave it even though it says "amber".
9. **Negative semantics:** keyless extract of an encrypted object fails as a
   missing-key error; wrong key fails as an authentication error (the harness's
   existing `MISSING_KEY_PATTERNS`/`AUTH_FAILURE_PATTERNS` already match rem's errors —
   scenario RAO proves it).
10. **Verification idiom:** RAO has no `verify` subcommand. Verification = authenticated
    extract (keyed for encrypted, chunk-sized for plaintext) + sha256 compare of the
    restored member, plus inspect/build `stored_digest` equality.
