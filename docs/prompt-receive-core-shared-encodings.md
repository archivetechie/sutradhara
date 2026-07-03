# Codex prompt — receive-core shared encodings (M0 for the Rust agent)

**Repo:** `~/sutradhara/repo` · package `packages/sutradhara-receive` (Rust crate + PyO3 wheel).
**Status:** implemented 2026-07-03.
**Why:** the Rust agent control-plane design
(`~/sutra-agent/docs/design-rust-agent-control-plane.md`, §0 "governing principle") requires
the drift-sensitive encodings the server *recomputes and rejects* to live in **one shared
implementation** — a `pub fn` in this crate, called by both the server (via the PyO3 wheel)
and the Rust agent — rather than a second hand-written Rust copy that silently drifts. The
5-lens panel found (contract lens) that the server's `manifest_digest` uses Python's spaced
`json.dumps` separators, so a naive Rust digest would fail **every** `CommitIntake`. This
prompt closes that class of bug **before** the agent is built. It is a prerequisite (M0) for
the agent prompt.

## Scope — expose/port these as crate `pub fn`s + golden fixtures

Each item: (a) ensure it's a `pub fn` in the Rust crate with the exact byte-for-byte encoding
the server validates; (b) keep/extend the PyO3 wheel surface so the server keeps calling the
same code; (c) add a golden fixture to the conformance corpus (extend
`scripts/extract_fixtures.py` + `fixtures/`) generated from the **current Python**
implementation so both sides gate on identical bytes.

Ground truth for the exact encodings (read these; do not guess):
- `manifest_digest` — server recompute+reject at `src/sutradhara/grpc/servicer.py:215`;
  encoding in `src/sutradhara/grpc/assembly.py:109-124` (Python `json.dumps` with **default
  spaced separators** `", "`/`": "`, `sort_keys=True`; per-entry keys `bytes`(int),
  `client_sha256`(lowercased), `relpath`(canonicalized); list sorted by canonical relpath).
- `source_plan_digest` / payload plan — `packages/sutradhara-receive/src/.../core.py:600-610`
  (`json.dumps` with **compact** `separators=(",",":")`, per-entry keys `mtime_ns`(ns),
  `relpath`, `size`(=plan_size), sorted). **Note the deliberate compact-vs-spaced difference
  from manifest_digest** — both must be reproduced exactly and independently.
- package-index construction (`PackageIndex` for package dirs) — the tar-metadata glue that is
  Python-only today (`core.py`); pair with the existing `build_package_tar`/
  `plan_payload_units`.
- `card_id` derivation — legacy Python helper `mounts.py` behavior:
  a formatting fn `card_id(volume_uuid: Option<..>, source, mount_path, label) -> "volume:<id>"`
  where the id is the **real volume UUID/serial when present** (Windows serial `"{hi:04X}-{lo:04X}"`,
  macOS diskutil VolumeUUID, Linux blkid UUID) and only **falls back** to `sha256(source|mount_path|
  label)[:24]` when none. (The platform *enumeration* of the serial stays in the agent's
  `platform/`; only the id **formatting/derivation** moves here so both sides agree.)
- confinement canonicalizer — `canonical_device_rel_path` in
  legacy Python helper `confine.py` behavior (reject backslash, leading `/`,
  drive-letter prefix, `.`/`..`/empty, `normpath(v)≠v`, `MAX_DEVICE_REL_PATH=1024`, non-final
  casefolded `PACKAGE_GLOBS`; returns canonical forward-slash rel path). Port as a `&str`-level
  `pub fn`; the agent must NOT use `std::path::Component` for wire paths.
- Confirm/export the already-present constants + fns the agent needs:
  `CANONICALIZATION_VERSION` (`"receive-bagit-path-v2"`), `PACKAGE_PROFILE_VERSION`
  (`""`/`"package-tar-v1"`), `canonicalize_manifest_path`, `PACKAGE_GLOBS`.

## Definition of done
1. Each encoding is a crate `pub fn` (Rust) with a golden fixture in `fixtures/` extracted
   from the current Python (extend `scripts/extract_fixtures.py`); a Rust test asserts the
   Rust fn matches the fixture **byte-for-byte**, and the Python side (wheel or existing code)
   is shown to produce the same bytes (so server and agent are provably identical).
2. `cargo test` + `cargo clippy` green in the crate; `uv run pytest -q` green in
   `~/sutradhara/repo` (the wheel swap must not regress the server). Paste outputs.
3. No behavior change to the server's validated bytes (these are snapshots of *current*
   behavior). If any current Python encoding looks wrong, STOP and flag — do not "fix" it here.
4. Commit; mark this prompt implemented in `docs/INDEX.md`; note the new crate fns in the
   crate README's public-API section.
