# Design — receive core in Rust (one implementation: crate + wheel + edge binary)

**Status:** implemented
**Depends on:** `design-receive-front-door.md` (the receive contract this migrates),
`design-streaming-intake-grpc.md` (server-side bag assembly), `design-sutra-agent.md`
(the helper daemon that consumes the contract), `design-intake-watch.md` (the road-mode
reconciliation path, §8.5).
**Review:** codex r1 folded (6 findings) 2026-07-02. Nothing is in production, so no
backwards-compatibility/deprecation ceremony anywhere in this design (the maintainer, 2026-07-02).
**Implementation:** M1-M6 landed 2026-07-02. The M6 artifact workflow is
`.github/workflows/receive-release.yml`; macOS signing and notarization secrets remain
outside the repository.

## 1. Problem & scope

`sutradhara-receive` is the dependency-light receive filesystem contract: hash-on-read
copy, canonical member paths, BagIt tag files, resumable landing directories, package
tar normalization, destination verification, `intake.json`-last sentinel. Today it is
pure Python, consumed three ways: (a) the server imports it at 15+ sites for intake
validation, gRPC bag assembly, and member-name canonicalization; (b) the Python
`sutra-agent` helper imports it for payload planning and CLI wrapping; (c) the Rust
agent (`~/sutra-agent`) shells out to `sutra receive --json` — which silently requires
the **full Python stack on every client machine**.

Three requirements (the maintainer, 2026-07-02) break the Python-on-client model:

1. **Road mode** — cards offloaded to external hard disks from MacBooks while the
   server is unreachable. The *full write side* (hash-on-read, package tar, resume,
   sweep) must run offline on machines we do not manage.
2. **Absolutely minimal operator hassle** — operators' jobs are hectic; install must be
   one signed, notarized artifact. No Python, no venv, no brew.
3. **Open-source reuse** — the receive contract should stand alone as a project other
   people/orgs can adopt ("verified hash-on-read offload to BagIt, with resume"). OSS
   DIT/offload tools exist (SEDER Media Suite, DIT Pro, the `bagit` library), but none
   with this exact contract-first shape: BagIt + resumable + byte-pinned conformance
   corpus (codex r1).

**Decision (the maintainer, 2026-07-02): rewrite the receive core once, in Rust, as a single
crate producing three artifacts** — (i) a single-file edge CLI binary, (ii) the crate itself
for Rust embedders (the Rust agent first), (iii) a PyO3/maturin wheel that preserves the
`sutradhara_receive` import surface so the server and Python helper swap dependencies
without a refactor. The orchestrator itself **stays Python** (`CLAUDE.md` boundary rule:
Python for orchestration on hosts we control; Rust for anything shipped to machines we
don't, and for on-media formats).

**Out of scope:** rewriting the Python `sutra-agent` helper daemon (device relay,
enrollment, mounts) in Rust — it keeps working unchanged through the wheel swap; a
possible later port is a separate design. Tape/backends/catalog untouched.

## 2. Why one implementation behind FFI (rejected alternatives)

The shared surface decomposes into three strata with different call shapes (§3), and
the strata kill the simpler patterns:

- **Rejected — rem-debug-style subprocess binary only.** Every *coarse* server call
  site (one call per bag/intake) would fit, but stratum 1 (pure canonicalization) is
  called per-path deep inside the orchestrator (`staging.py`, `archive_bundle.py`,
  `archive_restore.py`, gRPC servicer). You cannot subprocess a per-path string
  function, so this pattern forces a permanent second Python implementation of exactly
  the most drift-sensitive code.
- **Rejected — contract-as-spec with two implementations pinned by fixtures.** The
  write side includes package tar normalization — deterministic byte-format code, the
  worst possible thing to keep bit-identical in two languages. A drift bug surfaces as
  a quarantined bag from the road weeks after the operator hash-verified it: the exact
  opposite of requirement 2. Fixtures catch drift only for inputs someone thought to
  fixture; macOS NFD filenames guarantee inputs nobody thought to fixture.
- **Rejected — bundled Python (PyInstaller/briefcase) for clients.** Large artifacts,
  AV false positives on Windows, notarization friction on macOS, and it does nothing
  for OSS adoption.

PyO3 keeps one implementation of canonicalization, tar normalization, and bag writing
serving every consumer. It is consistent with the standing "stateless local, never a
daemon" ethos (an in-process library is even less daemon-like than a subprocess).
This **supersedes the Rust agent README's "Current Decision"** (delegate to
`sutra receive`) — that design was fine for LAN ingest stations, wrong for road
MacBooks (M5 updates the README).

## 3. Contract inventory — the three strata and their consumers

*(This section is the ground truth for structuring the crate; verified against the
tree 2026-07-02.)*

### 3.1 Stratum 1 — pure canonical-form functions (fine-grained, called everywhere)
`canonicalize_filesystem_path`, `canonicalize_manifest_path`, `safe_payload_path`,
`slug_operator`, `sha256_file`, the manifest/tag text builders (`bagit_manifest_text`,
`bag_info_text`, `tagmanifest_text`, `bag_info_metadata`), and the whole of
`member_name.py` (`escape_member_name` / `unescape_member_name`,
`escape_path_name` / `escape_path_text`). **Consumers:** server `staging.py`,
`archive_bundle.py`, `archive_restore.py`, `grpc/servicer.py`, `grpc/assembly.py`,
`intake_watch.py` — i.e. this stratum is really the archive's member-naming spec, not
just a receive concern. Note `member_name` escaping is **also a Remanence interop
contract** (same escaped form rem writes in `.remwrap.idx`) — a third reason it must
be single-sourced and bit-identical.

### 3.2 Stratum 2 — coarse per-intake operations (one call per bag)
Write side: `receive_source` (scan → collision check → hash-on-read copy → package tar
→ destination verify → BagIt tags → `intake.json` last), `plan_payload_units` /
`payload_plan_digest`, `write_bagit_files`, `sweep_orphans`. Read side:
`validate_bag`, `hash_payload_tree`, `read_bag_info`, `read_manifest_sha256`,
`read_package_index`, `manifest_mismatch`. **Consumers:** server `intake.py`
(`validate_bag`, `hash_payload_tree`), `intake_watch.py`, `grpc/assembly.py`
(`write_bagit_files` — called once at `CommitIntake`, not per chunk),
`grpc/server.py` (`sweep_orphans`), `api/routes_receive.py` (`receive_source`);
Python `sutra-agent` `grpc_client.py` (`plan_payload_units`), `ledger.py`,
`confine.py` (`PACKAGE_GLOBS`).

### 3.3 Stratum 3 — glue
`cli.py` (flag parsing, `--json` summary), `wait_for_server_confirmation` (client-side
marker polling), `AtomicWriteObserver` (progress callbacks). Lives wherever
convenient; **the `--json` summary rendering must live in the Rust core** so the edge
binary and the Python `sutra receive` CLI emit byte-identical JSON (§4.3).

### 3.4 Result/exception types
`ReceiveResult`, `FileReceipt`, `PayloadPlan`, `PayloadUnit`, `BagWriteResult`,
`BagValidationResult`, `OrphanSweepResult`, `ConfirmationResult`, `RejectedEntry`;
exception hierarchy `ReceiveError` ← `CollisionError` / `SourceScanError` /
`SourceMutationError` / `DestinationVerificationError`. The wheel must expose these
under the same names with the same attribute names. The normative surface is **not
prose**: M1 generates a `public_api.json` snapshot — `__all__` (currently 50 symbols),
submodule exports (`member_name`, `cli`), function signatures, dataclass fields and
properties, exception bases, and the CLI `--json` output schema — and M4's wheel is
diffed against it (codex r1).

## 4. Artifact matrix

One crate, living **inside `packages/sutradhara-receive/`** as a maturin *mixed*
Rust/Python project — `Cargo.toml` lands at that package root in M2 so `cargo test`
exists as soon as Rust code exists; the Python build backend remains setuptools until it
flips setuptools→maturin in M4, but the distribution name and the path do not move.
This is deliberate (codex r1): both this repo's uv workspace
(`packages/sutradhara-receive`) and `~/system`'s lock (`editable =
"../sutradhara/repo/packages/sutradhara-receive"`) pin that exact path — relocating the
package would break the editable-dep chain the harness depends on. OSS split-out to its
own repo remains §10. Dependency budget honors the "dependency-light" ethos:
`sha2`, `unicode-normalization`, `serde`/`serde_json`, `clap` (binary only),
`pyo3` (wheel only). **No tokio, no tonic** — receive is synchronous streaming I/O;
gRPC stays in the agents.

### 4.1 Edge CLI binary (`sutra-receive`)
Single-file Rust binary, behavior-compatible with today's CLI — and "behavior" means the **full
matrix** (codex r1), not the headline flags: the explicit `run` subcommand *and*
bare-command normalization (no leading subcommand → `run` prepended), `sweep` with its
`sweep-orphans` alias, `--fake-source` (mutually exclusive with `SOURCE`), `--resume`,
server-confirmation polling with its stderr messaging, JSON output rendered
`indent=2, sort_keys=True`, and the exit-code contract (0 ok; 1 runtime error; 2 usage;
**3 = received but source release unsafe** — confirmation pending/quarantined/
discrepancy/timeout; 130 interrupt). `cli.py` + `tests/test_receive_front_door.py` are
the normative reference; §6.3 fixtures capture the matrix so both CLIs are held to it.
Targets: macOS universal2 (signed + notarized), Windows MSVC, Linux glibc + musl
(musl static where supported; platform libc/system frameworks are expected on macOS,
Windows MSVC, and glibc). This is the road-mode artifact.

### 4.2 Crate for embedders
The Rust agent replaces `shared/receive.rs` (today a subprocess bridge that builds
`sutra receive --json` invocations) with direct crate calls — after M5 a road MacBook
carries one binary and zero Python.

### 4.3 PyO3/maturin wheel (`sutradhara-receive` on PyPI, module `sutradhara_receive`)
Mixed Rust/Python maturin project: the native module carries strata 1–2; thin Python
shims keep `sutradhara_receive.cli` (flag parsing stays Python; JSON summary text comes
from the core) and `sutradhara_receive.member_name` import paths alive. Import surface
is name-for-name identical to today (§3.4), so the server dep swap is `uv.lock` churn,
not a refactor. **Hook contract pinned** (codex r1): `receive_source` keeps both test/
progress surfaces exactly as typed today — `atomic_observer: AtomicWriteObserver` (an
object whose `before_rename(temp_path, final_path)` is called between temp-file fsync
and atomic rename) and `after_copy_hook: Callable[[Path, tuple[FileReceipt, ...]],
None]`. The native module invokes these as Python callables at the same points in the
Rust write path; the harness and front-door tests use them, so they are contract, not
convenience. Exceptions map to Python classes with the same hierarchy.

## 5. Binding contract values (bit-for-bit; fixtures are normative)

- `RECEIVE_VERSION = "receive-v2"`; `CANONICALIZATION_VERSION = "receive-bagit-path-v2"`;
  `PACKAGE_PROFILE_VERSION = "package-tar-v1"`; `BAG_PROFILE = "bagit-1.0"`.
- `PACKAGE_PROFILE_HASH =
  fc87e5e8ad47962fa800b2d2e7fac6ae1da148f142319a4c32efca1ed392ef3c` (sha256 of the
  compact sorted-key JSON of the tar profile params; the Rust crate must reproduce this
  exact digest — fixture, don't re-derive by eye).
- `PACKAGE_GLOBS = ("*.fcpbundle", "*.photoslibrary", "*.imovielibrary", "*.app")`.
- Bag layout names: `bagit.txt` (exact bytes `BagIt-Version: 1.0\nTag-File-Character-
  Encoding: UTF-8\n`), `manifest-sha256.txt`, `bag-info.txt`, `tagmanifest-sha256.txt`,
  `package-index.json`, payload dir `data/`.
- **Canonicalization semantics** (the NFD trap): filesystem components go
  `os.fsencode` → UTF-8 decode → **NFC normalize** → re-encode → escape; undecodable
  bytes fall through to escaped form. Manifest paths strip `./` prefixes, leading `/`,
  and a `data/` prefix, then unescape and re-canonicalize. `..` rejected; empty
  rejected. `escape_member_name`: valid UTF-8 passes through, literal `\` doubled,
  invalid/control bytes as lowercase `\xhh`. Rust uses `unicode-normalization`; NFC is
  stability-guaranteed for assigned codepoints, but fixtures must still include NFD
  (macOS APFS), mixed/invalid UTF-8, control bytes, backslashes, and unassigned-plane
  cases.
- **Tar-pax determinism:** `mtime=0`, file mode `0o644`, dir `0o755`, symlink `0o777`,
  member ordering and offset recording as today (`package-index.json` offsets).
  Implementation note for codex: evaluate the `tar` crate with fully explicit pax
  headers vs a bespoke writer — whichever survives the byte-identical bag fixtures.
- **Atomic-write discipline:** temp file + fsync + rename + parent-dir fsync;
  `intake.json` written last as the completion sentinel. 1 MiB copy buffer (perf
  convention, not contract).
- **Version marker:** bags carry `Receive-Package: sutradhara-receive/<version>` in
  `bag-info.txt`; server accepts only `SUPPORTED_RECEIVE_PACKAGES`. The current Python
  implementation still has
  `LEGACY_RECEIVE_SOFTWARE_AGENTS = {"sutradhara-receive/receive-v2"}`, but §7 removes
  that legacy path at M4 because nothing is in production.
- **Version source of truth:** the Cargo package version, Python distribution version,
  `RECEIVE_PACKAGE_VERSION`, and `Receive-Package` bag marker must all derive from the
  same release value.

## 6. Conformance fixtures (public part of the contract)

Extracted from the **current Python implementation** before any Rust lands (M1), and
kept in `packages/sutradhara-receive/fixtures/`:

1. **String-level:** canonicalization + escaping corpora — (input bytes → canonical
   member name), covering §5's Unicode/byte edge cases exhaustively. Round-trip
   property: `unescape(escape(x)) == x`.
2. **Bag-level:** small source trees (including one `.photoslibrary` package with a
   symlink and a non-UTF-8 filename) → expected bag bytes. **Determinism is specified,
   not assumed** (codex r1): `receive_source` already accepts `now=` (inject a fixed
   timestamp), but intake ids embed `uuid4` hex (`_mint_intake_id`) and temp paths are
   internal — so bag-level fixtures are byte-exact **after a defined normalization
   pass**: the extractor temporarily pins the existing private id minting function so
   tagmanifest hashes are stable, then minted intake ids become `<INTAKE-ID>`, temp
   fixture roots become `<FIXTURE-ROOT>`, source/landing roots become
   `<SOURCE>`/`<LANDING>`, persisted absolute paths inside CLI JSON/text output are
   normalized, and `receive.log` is excluded from bag-byte fixtures because it
   intentionally contains wall-clock provenance. Temp-file names never persist and
   need no rule. Fixtures for the lower-level writers (`write_bagit_files`,
   manifest/tag text builders, package tar digest + offsets) take no substitution and
   are literally byte-exact. No test-only id hook is added to the Python impl — M1 is
   observation-only; the Rust core MAY expose an id-injection parameter for its own
   tests.
3. **Validate/mismatch:** corrupted-bag fixtures → expected `BagValidationResult` /
   `manifest_mismatch` outputs; unsupported `Receive-Package` → quarantine error.
4. **CLI matrix** (§4.1): argv → (exit code, stdout, stderr, JSON payload) cases
   covering bare-command normalization, `run`, `sweep`/`sweep-orphans`, `--fake-source`
   exclusivity, `--resume`, confirmation-polling outcomes (release-ok / pending /
   quarantined / discrepancy / timeout → exit 3), and the `indent=2, sort_keys=True`
   JSON rendering.
5. **`public_api.json`** (§3.4): the generated API snapshot lives with the corpus and
   gates the M4 wheel.

Both test suites run the same corpus: `cargo test` in the crate, `pytest` through the
wheel. The corpus ships with the OSS project as the normative contract definition.

## 7. Version marker during migration

The Rust core ships as `sutradhara-receive` 0.1.0; bags carry
`sutradhara-receive/0.1.0`. **Nothing is in production, so there is no legacy
acceptance to preserve** (the maintainer, 2026-07-02): at M4, `SUPPORTED_RECEIVE_PACKAGES`
becomes `{"sutradhara-receive/0.1.0"}` only, `LEGACY_RECEIVE_SOFTWARE_AGENTS` is
dropped, and any local test bags are simply regenerated. The marker mechanism itself is
kept — it is the future drift/rollout gate once real edges exist — but no
dual-version window is engineered now.

## 8. Migration order (each item → one codex prompt)

- **M1 — fixture extraction (Python-only, observation-only, no behavior change).**
  Generator script + corpus committed (§6.1–6.4), plus the `public_api.json` snapshot
  (§6.5); pytest runner asserting the current implementation passes its own fixtures.
  This is the contract snapshot everything else is built against.
- **M2 — crate core, stratum 1.** Canonicalization, member-name escaping, tag-file
  text builders; `Cargo.toml` lands here and `cargo test` passes all string-level
  fixtures.
- **M3 — crate write side + edge binary.** `receive_source` (scan, collision,
  hash-on-read, package tar, resume, destination verify, atomic writes, sentinel),
  `plan_payload_units`, `sweep_orphans`; `sutra-receive` binary with today's flags;
  passes normalized bag-level fixtures, while lower-level writer/tar fixtures remain
  byte-for-byte.
- **M4 — read/verify side + in-place wheel flip.** `validate_bag`,
  `hash_payload_tree`, readers; `packages/sutradhara-receive` flips build backend
  setuptools→maturin **in place** (same path, same distribution name — §4 intro), its
  `core.py` logic replaced by the native module behind the unchanged import surface,
  diffed against `public_api.json`; `uv.lock` regenerated **in both workspaces** (this
  repo and `~/system`); `SUPPORTED_RECEIVE_PACKAGES` → `{0.1.0}` only and legacy sets
  dropped (§7); local test bags regenerated. Shims in `sutradhara.receive` /
  `sutradhara.member_name` keep working — they already re-export.
- **M5 — Rust agent integration.** Replace `shared/receive.rs` subprocess bridge with
  crate calls; **road-mode profile**: configured landing root on an external disk,
  fully offline, local ledger of completed intake ids; update the agent README's
  superseded "Current Decision". Road reconciliation needs **no new protocol**: bags
  are indistinguishable by design, so plugging the road disk into a connected station
  later hands off through the existing `sutra intake watch` path (§`design-intake-watch.md`).
- **M6 — distribution.** CI: maturin manylinux wheel; macOS universal2 binary signed +
  notarized; Windows + Linux binaries; release checksums. (Store signing/notarization
  credentials outside the repo.)

## 9. Testing & the harness trap

`uv run pytest -q` must stay fast and hermetic: from M4 the wheel is a normal
locked dependency (built once by `uv`/maturin, cached), and every receive-core change
means a Rust rebuild before pytest — acceptable because the contract is *designed*
frozen; low churn is the point. **The `~/system` harness consumes this tree's main as
an editable dep** — land each M-item complete and green (`harness/seams/intake.py`,
`scenario_iw_fixtures.py`, and Scenario J/N/O/Q are the canaries), and keep the wheel
buildable from a clean checkout so the harness never sees a half-migrated main.

## 10. Open decisions

1. **OSS name + repo split timing.** Working name stays `sutradhara-receive` in-tree
   (crate and distribution share the package dir, §4); rename and split to its own repo
   at publication (the maintainer's call — the tool deserves a name that doesn't require knowing
   what a sutradhara is).
2. **Python `sutra-agent` helper daemon port to Rust** (tonic) — separate design if/when;
   nothing here depends on it.
3. **PyPI/crates.io publication timing** — M6 makes artifacts; publishing is a
   separate, deliberate step.
