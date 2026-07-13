# Codex prompt — Setu detailed design (v0.2), code-grounded

**Role:** You are producing the **detailed, implementation-ready design** for **Setu**
— extending the existing receive workflow to work over the public internet as our own
Signiant replacement. This is a DESIGN task: you write ONE design document grounded in
the ACTUAL code. You do NOT write production code, and you do NOT commit.

**Model/effort:** gpt-5.6-sol, xhigh reasoning. Take the time to be right.

## The decision is already made — do not relitigate it
The brainstorm kickoff `~/sutradhara/docs/design-setu-wan-receive-v0.1.md` records
owner-approved decisions. Treat these as FIXED inputs:
- Scope = **Signiant parity**: a self-tuning transport that saturates whatever pipe
  exists; serve BOTH a managed US site (site-to-site, the volume anchor) AND ad-hoc
  contributors (person-to-portal); steady multi-TB/week with bandwidth caps + scheduling.
- Engine = **QUIC on `quinn` + BBR congestion control (Option B)** with
  **parallel-TCP-over-443 as automatic fallback (Option A)**.
- Keep the ENTIRE existing sutra-agent control plane + intake→verify→RAO→copy-3 funnel;
  **swap ONLY the bulk data channel.** A faster pipe INTO the one intake funnel — NEVER
  a second ingest path that bypasses verify/dedup/policy.
- Phasing: Spec 0 measurement spike → Spec 1 (engine + public landing + site-to-site) →
  Spec 2 (ad-hoc portal) → Spec 3 (cloud relay POP, only if measured needed).

Your job is the HOW at implementation altitude for **Spec 1** (with Spec 0 designed
concretely, and Spec 2/3 sketched only enough to prove the Spec 1 seams don't preclude
them). If, while reading the real code, you find a v0.1 assumption is WRONG (e.g. the
"swap only the data channel" premise is more entangled than stated, or the intake funnel
can't accept an alternate transport without deeper change), SAY SO LOUDLY and design to
the real code — do not paper over it. (Precedent: the restore-agent design's "thin delta"
premise was refuted against the real code; that refutation was the most valuable output.)

## Read first (ground everything; cite file:line for every seam you will touch)
1. `~/sutradhara/AGENTS.md` — house rules.
2. `~/sutradhara/docs/design-setu-wan-receive-v0.1.md` — the fixed decisions above.
3. **The client control plane + data channel you are extending** — `~/sutra-agent/`:
   - `docs/architecture.md` (the relay model, one bidi control stream + second upload
     channel, no inbound socket invariant).
   - `src/relay/transport.rs` (outbound mTLS channel construction — note the
     restore/keepalive tuning already there), `src/relay/control.rs` (ControlDaemon,
     `DeviceService.Connect`), `src/relay/intake.rs` (ReceiveSupervisor: plans source,
     journals, opens intake with `planned_bytes_total`, streams `upload_chunk_bytes`
     chunks, `max_concurrent_files`, `stall_timeout_seconds`, commit + one re-upload),
     `src/relay/inflight.rs` (the in-flight journal / resume), `src/relay/config.rs`,
     `src/proto.rs` + `build.rs` (proto compiled from `../sutradhara/proto`).
   - This is the crate that must grow the `DataTransport` seam. Find where the intake
     supervisor actually writes chunks to the wire — that call site IS the seam.
4. **The server receive funnel** — `~/sutradhara/`: find and read the IntakeService
   server implementation, the `proto/` intake + device protos, and the receive contract
   in `packages/sutradhara-receive` (hashing, BagIt, `intake.json`, resume, re-upload
   semantics). Establish exactly where server-side bytes enter the verify→RAO→copy-3
   funnel — the landing point the QUIC/TCP data plane must feed WITHOUT bypassing.
5. **The backpressure pattern to reuse** — `~/remanence/docs/design-tape-io-pipelined-
   submission-v0.1.md` (TIO-5/6): the anti-shoe-shine host-RAM reservoir, one-in-flight
   submitter, watermark stop-start. The Setu disk↔socket backpressure problem is the same
   shape; reuse the model (and cite it), don't reinvent.
6. House doc style templates: `~/sutradhara/docs/design-restore-agent-v0.1.md` and
   `design-streaming-restore.md`.

## Hard constraints (from repo memory — violating these is the cardinal sin)
- **Single funnel / additive-bias guard:** the new transport must WRAP the existing
  intake path, not fork a parallel one. Server-side verify/dedup/policy/RAO/copy-3 must be
  the exact same funnel LAN receive uses today. Name the one seam and prove nothing routes
  around it.
- **NOT in production:** the archive stack is pre-production. Do NOT design runtime backout
  flags, compat switches, canary machinery, or dual old/new code paths. `git revert` +
  previous binary is the backout. Old behavior lives in golden fixtures, not shipped paths.
- **Cross-repo:** for every work item, name which repo it lands in (`~/sutra-agent` client
  transport legs; `~/sutradhara` server ingress + landing; proto changes are shared).
- **mTLS identity ports over:** QUIC uses TLS 1.3 natively — design the QUIC handshake to
  present the SAME enrolled client cert + verify the SAME enrolled CA the current relay
  does. Do not invent a second identity system.

## The throughput physics to fold in (established with the owner; treat as design inputs)
- Single-TCP ceiling over ~200ms RTT is ~1–6 Mbps regardless of pipe width (Mathis:
  throughput ≤ MSS/(RTT·√loss)). US↔Mumbai RTT ≈ 170–260ms. This is the floor BBR beats.
- Parallelism gives `min(N × per-stream, tightest bottleneck)`, NOT linear N×. It only
  multiplies when the single stream leaves bandwidth on the table (loss-throttled) OR when
  the bottleneck is a **per-flow** shaper. Once BBR fills the pipe, extra streams just
  re-slice it, and past a knee they self-congest and throughput DROPS.
- Therefore: **BBR-first (hit the ceiling with few streams), keep a small number of
  parallel streams specifically to (a) defeat per-flow shapers and (b) multi-file
  concurrency** — not as an unbounded multiplier. Chunk→stream assignment must be a
  work-queue (fast streams steal work), NOT fixed equal chunks (straggler barrier).
- The real ceiling is often the last mile (site uplink / Coimbatore downlink) or the disk,
  not the middle. **Spec 0's job is to diagnose WHICH bottleneck** — aggregate capacity vs
  per-flow shaping vs disk — because each calls for a different amount of engineering. Design
  Spec 0 to run 1 vs 4 vs 16 streams on the real link + iperf3 + a real Signiant transfer
  log, and to read out that diagnosis.
- POP relay is a SEPARATE, SMALLER lever than the engine (AWS Global Accelerator publishes
  ~1.5–2.5×; the engine is the 10–100× lever). Keep it deferred to Spec 3, gated on Spec 0.

## Deliverable: write `~/sutradhara/docs/design-setu-wan-receive-v0.2.md`
Detailed design at implementation altitude, house style, with at minimum:
1. **Status header + one-paragraph decision** (carry the v0.1 decision forward; note this
   is the detailed design the prompt set will flow from).
2. **Corrections to v0.1** (if any) — where the real code contradicts the kickoff, with
   file:line evidence. If none, say so explicitly and why.
3. **The `DataTransport` seam** — exact trait/interface, the precise call site in
   `src/relay/intake.rs` it replaces, how `QuicBbrTransport` and `ParallelTcpTransport`
   implement it, the connect-time capability probe + UDP-blocked detection + fallback
   negotiation (and how the server advertises/accepts each transport). Show the server-side
   symmetry.
4. **QUIC/BBR data plane** — quinn config; mTLS cert reuse; stream model (file/chunk →
   QUIC stream mapping); framing; flow control vs BBR pacing; the bandwidth-cap knob as a
   pacing rate; connection migration for resume; how backpressure signals reach the reader.
5. **Parallel-TCP fallback** — N-stream design, work-queue chunk assignment, how it reuses
   (or deliberately doesn't) the existing gRPC IntakeService, and the fallback trigger.
6. **Disk↔socket backpressure** — concrete reuse of the TIO reservoir/watermark model on
   BOTH send (read-ahead reservoir) and receive (bounded write + RAO/verify) sides; memory
   bounds; how it ties into the existing intake/commit and the one-funnel invariant.
7. **Public ingress / landing zone** — the exactly-two hardened mTLS ports, how they fit
   the akash `public_guard` nft default-deny model, direct-to-server (relay deferred).
8. **Site-to-site managed path (Spec 1)** — headless serve daemon, long-lived enrollment,
   watched-dir/workflow feed, the scheduler + bandwidth-cap policy shape.
9. **Resume at intercontinental scale** — how the in-flight journal extends; QUIC migration;
   multi-hour transfer + IP-change survival; interaction with the one re-upload rule.
10. **Spec 0 measurement spike** — concrete runbook + the bottleneck-diagnosis readout.
11. **Verification member** — how parity is PROVEN (goodput dashboard + A/B vs Signiant log)
    AND a hermetic harness scenario/cover in `~/system` (name it; it must exercise the seam).
12. **Security posture** — public surface, ad-hoc scoping (Spec 2 preview: push-only,
    quarantine, short-lived enrollment), relay end-to-end-encryption note.
13. **Build order** — Spec 1 decomposed into dependency-ordered, individually-promptable
    work items (each → one future codex prompt), each naming its repo and its verification
    member. Sketch Spec 2/3 only enough to prove Spec 1's seams don't preclude them.
14. **Open questions** for panel/owner.

## Rules of engagement
- Ground EVERY structural claim in real code with file:line. Do not invent APIs — read them.
- Prefer wrapping/extending existing types over new parallel ones; call out each reuse.
- Deviating silently from the v0.1 decisions or the constraints above is the cardinal sin;
  deviating openly with rationale is welcome.
- Write ONLY `~/sutradhara/docs/design-setu-wan-receive-v0.2.md`. Do NOT edit other files.
  Do NOT commit or touch git — the maintainer reviews and commits.
- End with a plain-text summary to stdout: the doc path, the 5–8 load-bearing design
  decisions, any v0.1 premise the code contradicted (with file:line), and the open
  questions — so the maintainer can review without re-reading the whole doc.
