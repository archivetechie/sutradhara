# Design — offload verify overlap (single-read card path, staged verification)

**Repo:** `~/sutradhara` (receive core + server) + `~/sutra-agent` (relay client).
**Status:** draft 2026-07-05 — panel pending. **Driver:** operational hard constraint
(memory `card-offload-verify-must-overlap`, the owner 2026-07-05): cameramen queue for
empty cards; SilverStack's serial verify doubled offload wall-clock and operators
disabled verification outright. A verify design that costs a second pass on the
card-holding path is a verify design that gets turned off.

**Audit basis (codex, 2026-07-05, file:line evidence on record):** BOTH paths
violate the constraint today:
- **Online relay:** agent runs a full sha256 pre-pass per file (`hash_file()`)
  then re-reads the same file to stream chunks — **two complete source reads**
  while holding the card lease. Server-side hashing already overlaps its writes;
  the watcher's later `validate_bag()` re-read does NOT hold the card lease (fine).
- **Road mode:** `finish_receive()` runs a mandatory serial
  `verify_destination_files()` full re-read after the copy, before BagIt/sentinel;
  no disable knob. Wall-clock = transfer + full destination re-read.

## 1. Target semantics (the ruling)

**The card-holding critical path contains exactly ONE read of each source byte.**
sha256(source) is computed incrementally on that single read, and the destination
side hashes bytes as they land (server already does; road mode gains it on the
write stream). **Card release gates on transfer-hash agreement** (source in-flight
hash == destination in-flight hash, all files) — never on a destination re-read.

Destination-media risk after release is owned by the pipeline, where it already
lives: the watcher's `validate_bag()` re-read (online) and a new post-release
verify stage (road mode), both against the frozen manifest, both off the card
path. Archive-time verification against the manifest remains the durability
backstop. This is strictly stronger than the field reality today (verification
disabled entirely) and equal in transfer-error coverage to the two-pass design.

## 2. Changes

**2.1 Agent (online), `~/sutra-agent`:** delete the pre-pass. `stream_file()`
computes sha256 over the chunks as it reads-and-sends (one reader, hash updated
per chunk); the per-file hash is final when the last chunk is sent and goes into
FileDone/receipt exactly as today. No proto change (receipts already carry the
hash after bytes). Resume semantics: on resume, the already-sent prefix must be
re-hashed to seed the hasher — a prefix re-read ONLY on the resume path (rare,
recorded as acceptable; still strictly better than today's every-file pre-pass).

**2.2 Receive core (road mode), `packages/sutradhara-receive` (conformance-corpus
governed — crate discipline applies):**
- Destination in-flight hash: the existing copy loop already hashes source-on-read;
  extend `copy_file_with_digest*` to ALSO hash the bytes written to the
  destination stream (same buffer, second hasher — CPU overlaps I/O; sha256 at
  card speeds is not the bottleneck) and compare per file at copy end. Mismatch =
  hard fail (transfer corruption caught immediately).
- `verify_destination_files()` (the re-read) becomes **stage-2 verify**: runs
  AFTER the receive is complete-and-releasable. Concretely: copy + in-flight
  agreement → write BagIt + `intake.json` with a new manifest-tag field
  `Verify-Stage: transfer` → **card releasable now** (CLI prints release status;
  agent drops the card lease) → stage-2 re-read then flips the tag file entry to
  `Verify-Stage: destination` (rewrites tagmanifest accordingly). A
  `--verify=transfer|full-blocking` flag preserves the old serial behavior for
  callers that want it (default: staged). Bag consumers (watch) treat both stages
  as valid received bags; watch's own `validate_bag()` remains the authoritative
  server-side check.
- Package (tar) path gets the same treatment (hash the tar stream as written).

**2.3 Timing instrumentation (both paths, the audit's list):** per-file and
aggregate stage durations (read/hash/send|write, verify stages, lease
hold time) into the existing receipt/receive.log structures — enough for
campaign E2-perf to assert overlap ratio ≈1x from evidence, permanently.

**2.4 Explicitly NOT changing:** wire contract (proto untouched), plan digest,
bag layout beyond the one tag field, watch's re-read, archive-time verification,
eject-confirmation marker semantics.

## 3. Risks / panel charters

- **Conformance corpus:** 2.2 changes crate behavior — which fixtures/golden
  outputs move (the new tag field!), and does the Python wheel surface change?
  (`public_api.json` gate.) Contract lens must enumerate.
- **Resume-path hash seeding** (2.1): correctness under kill/resume mid-file;
  interaction with the in-flight journal.
- **Two-hasher copy loop** (2.2): actual throughput on weak road laptops (CPU-
  bound risk), buffer lifetimes.
- **Stage-2 crash window:** machine dies after release, before stage-2 — bag says
  `Verify-Stage: transfer` forever; watch still validates fully server-side when
  it arrives; road bags that never reach a server need a documented `sutra
  receive --verify-pending` sweep (include in design).
- **Operator comms:** the CLI/tray must say clearly "card safe to remove — deep
  verify continuing" so the released card isn't confused with completed verify.

## 4. Verification

Hermetic: single-read property test (instrument reader call counts), in-flight
agreement mismatch injection, staged-tag lifecycle, resume re-seed, corpus
updates. Live: campaign **E2-perf ratio** (the standing gate: ≤ ~1.2x raw
transfer) + a road-mode timed run in the VM with a USB-attached source disk.
