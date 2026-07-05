# Design — offload verify overlap (single-read card path, staged verification)

**Repo:** `~/sutradhara` (receive core) + `~/sutra-agent` (relay client).
**Status:** folded 2026-07-06 — verify round pending.
**Panel 2026-07-05/06:** 3 blind lenses — failure-modes (codex), contract/conformance
(Opus), feasibility/cost/operator (Opus). ~30 findings; the fold REVERSED two of the
draft's own mechanisms (road-mode second hasher = tautological same-buffer hash,
cut; bag-tag stage carrier = frozen-bag violation + fixture cascade, replaced by an
out-of-bag sidecar) and adopted the honest wall-clock model (the road bottleneck is
the unpipelined copy loop + per-file fsync, not only the verify pass).
**Driver:** memory `card-offload-verify-must-overlap` — verification that costs a
second pass on the card-holding path gets disabled by operators (SilverStack).
**Audit + panel evidence:** file:line records in the session logs; audit verdicts:
both paths VIOLATE today (agent pre-pass = second full source read; road mode =
mandatory serial destination re-read).

## 1. Target semantics (the ruling)

**The card-holding critical path contains exactly ONE read of each source byte**
(exception: resume, §2.1). Card release gates on:
- **online:** per-file receipt agreement (agent stream-hash == server in-flight
  hash — two genuinely independent reads: card vs wire) **AND a successful
  CommitIntake ack** (panel blocker: receipts alone can release before any
  watch-visible durable bag exists);
- **road:** copy-loop completion + the full durable sentinel chain (BagIt,
  `intake.json`, `.receiving.json` removal, directory fsync — all card-free,
  milliseconds; panel blocker: releasing before the sentinel chain can strand a
  watch-invisible bag after a crash while the card is already gone).

Destination-media integrity is owned OFF the card path: watch's `validate_bag()`
re-read (online) and the road **stage-2 verify** (§2.2), both against the frozen
manifest; archive-time verification remains the durability backstop. Road mode has
NO destination hash in stage 1 — a second hasher over the same RAM buffer proves
nothing (panel; the draft's claim is retracted). Coverage in stage 1 = source-read
integrity + (online) transport integrity; that is exactly what a released card
needs, and strictly more than the field status quo (verification off).

## 2. Changes

### 2.1 Agent, online path (`~/sutra-agent`)

- **Delete the `hash_file` pre-pass.** `send_file_chunks` computes sha256 over
  chunks as it reads-and-sends (single reader, hash finalized at last chunk);
  the manifest entry's `client_sha256` and the receipt comparison move
  post-stream (the wire contract already carries hashes after bytes; no proto
  change).
- **Mutation guard (panel blocker):** one-pass hashing would bless a same-size
  mid-stream source mutation. Port the receive-core stat/identity guard: stat
  before + after each streamed file (size, mtime, file id where the platform
  gives one); mismatch ⇒ fail that file loudly. Applies to package members too.
- **Resume (whole-file-only, matching the code — the draft's prefix-seeding
  worry was about a mid-file resume that does not exist):** on resume,
  (a) recovery compares the CURRENT plan digest against the journal's saved
  digest and aborts on drift (pairs with the server-side reject in the
  relay-hardening followups — verify implemented, else re-flag); (b) the
  resume-skip fast path (skip files whose server receipt already matches) now
  requires a local rehash of exactly those candidate files — a resume-only,
  skip-candidates-only single pass, accepted and documented. The digest-drift
  compare guards **both restart paths** — `recover()` AND the
  `ClaimOutcome::Existing` re-claim — before any `start_intake`/stream spawn;
  on drift: abort the server-side intake if one exists, then remove the journal
  row deliberately (a drifted plan is a NEW receive, never a resume).
- Release = receipts agreement + CommitIntake ack (§1). `prepared.size_bytes`
  sourcing moves from the deleted pre-pass to plan/stat.

### 2.2 Receive core, road path (`packages/sutradhara-receive` — crate
discipline applies)

- **Stage 1 (card-holding):** copy loop keeps its existing source hash-on-read.
  **Pipeline it** (double-buffered read/write on two threads) so wall-clock
  approaches max(card-read, dest-write) instead of their sum; **defer per-file
  `sync_all` to one batched fsync pass over the payload data files** at stage-1
  end, before the sentinel chain (the sentinel chain's own atomic writes +
  final directory fsync are unchanged; durability-before-`intake.json` is
  preserved; fsync never touches the card). Package/tar path: already single-read via `TarHashingWriter`
  (unchanged), but **fsync tar temps before rename** (panel: durability parity).
- **Release point:** after the sentinel chain (§1). The CLI prints an unmissable
  `CARD SAFE TO REMOVE — deep verify continuing` line at that moment and stage 2
  proceeds in the SAME foreground process (no daemon lifecycle; the terminal
  keeps working while the cameraman takes the card; `--verify=blocking`
  preserves old behavior of releasing only after stage 2).
- **Stage 2 (post-release):** the relocated `verify_destination_files()`
  re-read, exposed as its own public crate/API step (below). Outcome recorded in
  an **atomic sidecar** `verify.json` beside `intake.json` (temp+rename; never
  inside tagmanifest coverage). Precisely stated (verify round): **BagIt
  payload/tag BYTES are unchanged; the LANDING layout gains one file**, and
  `verify.json` joins `receive.log` in the fixture exclusion list — bag-byte
  fixtures are untouched, landing-tree snapshots exclude it by rule. States:
  `{stage: "transfer"|"full"|"failed", checked_at, mismatches:[...]}`; absent
  sidecar = legacy, semantics "transfer"; old readers ignore it; watch's
  `validate_bag` stays authoritative and unchanged.
- **API split (verify round — no mid-receive callback exists and none is
  added):** staged mode splits the public surface instead: `receive_source(...,
  verify="staged")` performs stage 1 through the sentinel chain and RETURNS
  (this return IS the release point — the CLI prints the release line, the
  library never writes stdout); the new public `verify_destination(bag_path)`
  performs stage 2 + sidecar write and is what the CLI then calls in the same
  process (and what `verify-pending` reuses). `verify="blocking"` =
  `receive_source` runs both internally and returns only after stage 2 — the
  release line prints only after full verify in that mode (the two mode flows
  are distinct; staged never blocks release on stage 2, blocking always does).
- **`--verify=staged|blocking`** (default `staged`) on `ReceiveOptions`, the
  CLI, and `receive_source(...)` — a deliberate Python wheel-surface change
  (public_api.json regenerated; panel: the draft's "wire contract unchanged"
  claim wrongly ignored the wheel surface).
- **`sutra-receive verify-pending` is normative, not a risk note:** sweeps
  landing roots for bags whose sidecar is absent/`transfer`/`failed`, runs
  stage-2 idempotently, writes the sidecar, exits 0 clean / 4 mismatches-found
  (new exit code, documented); corpus + cli_matrix + public_api entries added.
  Stage-2 mismatch = sidecar `failed` + loud stderr + nonzero exit; the bag is
  NOT quarantined locally (watch/server-side validation owns quarantine).

### 2.3 Instrumentation (honest minimum, panel-trimmed)

Per file: `{bytes, copy_wall_ns}` + stage-2 `{stage2_wall_ns}`; per receive:
`release_offset_ns` (release timestamp − start). Written to `receive.log`
(fixture-excluded) and the online receipt LOG lines — **never** to the pinned
`FileReceipt` dataclass. This is exactly what the E2-perf gate consumes.

### 2.4 Not changing (corrected list)

Wire proto; plan digest; bag layout and BYTES (sidecar lives outside the tag
set); watch `validate_bag`; eject-confirmation markers; `SUPPORTED_RECEIVE_
PACKAGES` (no bump — compatibility stated in §2.2); archive-time verification.
CHANGING and owned: `receive_source` signature + `public_api.json`, CLI matrix
(+`verify-pending`, exit 4), corpus additions for staged/pending/failed sidecar
lifecycles (bag-byte fixtures untouched by construction).

## 3. Wall-clock model (honest)

Online: card read once, hash rides the stream, release on commit ack ⇒
lease-hold ≈ bytes/card-read + commit tail. Road: stage 1 ≈ bytes/max(card-read,
dest-write) + batched-fsync + sentinel tail (pipelined); the old model was
read+write serial + per-file fsync + full destination re-read. **E2-perf gate
(campaign): release_offset ≤ ~1.2 × bytes/max(card-read, dest-write-bench)** —
measured against the max, not raw card speed (panel: the 1.2×-of-card-read gate
is unreachable when the destination is the bottleneck).

## 4. Verification

Hermetic: reader-call-count property (one open/read pass per file, stage 1);
stat-guard mutation injection (online + package members); release-ordering tests
(kill between each sentinel-chain step ⇒ either no sentinel or complete bag,
never released-card + watch-invisible bag); pipelined-loop equivalence (bytes +
hash identical to serial reference); sidecar lifecycle (staged→full, failed,
absent=legacy) + `verify-pending` idempotence + exit codes; resume rehash-skip
and plan-digest drift abort; corpus/public_api regeneration per §2.4. Live: the
campaign E2-perf ratio + a road-mode timed run (VM, USB source disk).

## 5. Work split (prompt set)

1. **receive-core (road):** §2.2 + §2.3 + corpus/api regeneration (conformance
   discipline; single prompt — the changes interlock).
2. **agent (online):** §2.1 (pre-pass deletion, stream hash, stat guard, resume
   rehash + digest-drift abort, release-on-commit) + its §2.3 lines.
3. Campaign E2-perf runs against both once landed (existing task #9 lane).
