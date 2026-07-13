# Design — Setu: WAN receive / Signiant replacement (v0.1)

**Status:** brainstorm kickoff (owner the owner, 2026-07-13). Pre-panel. Scope decision
made with the owner: **go for Signiant parity** (self-tuning transport that saturates
whatever pipe exists), serving **both** a managed US site (the volume anchor) and
occasional ad-hoc contributors, tuned for **steady multi-TB/week** with bandwidth
caps and scheduling. Next step in the working pattern: panel review → per-repo codex
prompt sets (sutra-agent for the client legs, sutradhara for the server).

**Name:** *Setu* (सेतु, "the bridge" — a span to a distant shore). Working name;
owner may veto.

> This doc spans two repos on purpose. The **control plane and receive funnel are
> already built** and are reused unchanged; the project is a **new WAN data plane**
> plus a **public landing zone**. Codex prompts get cut per-repo later: client
> transport legs in `~/sutra-agent`, server ingress + landing in `~/sutradhara`.
> Pointer row in `~/system` INDEX per repo convention.

## Decision (one paragraph)
Build our own accelerated WAN receive path — **"Setu"** — by **keeping the entire
existing sutra-agent control plane and intake→verify→RAO→copy-3 funnel, and swapping
only the bulk data channel** for a self-tuning transport: **QUIC over `quinn` with a
BBR congestion controller (Option B), with parallel-TCP-over-443 built in as an
automatic fallback (Option A)** for networks that block or throttle UDP. BBR's job
description *is* "fill whatever bandwidth exists," which is exactly the owner's bar,
and it is the open, un-patented version of the Aspera/Signiant trick. The client keeps
its outbound-dial model (NAT-friendly — a US laptop or box dials us; nothing inbound
opens on their end), its mTLS identity (QUIC uses TLS 1.3 natively, so the cert model
ports over unchanged), and its resume/verify machinery. We are adding a faster pipe
*into* the one existing intake funnel — never a second ingest path that bypasses
verify. Ship it in phases: measure the real link first, build the site-to-site engine,
then layer the ad-hoc portal, and add a cloud relay **only if** measurement proves the
direct path needs it.

## 1. Problem
Today receive works from a LAN client (proven end-to-end 2026-06-30). The new
requirement is to receive over the public internet: a new Isha site in **Bangalore**
(domestic), and contributors in the **US** (intercontinental), which today go through
**Signiant** — capable but costly. The owner wants to stop paying Signiant and run this
on our own stack, plugged into our catalog, RAO format, and copy-3 durability.

The hard part is narrow. The generic "receive a file over the internet" product —
reachability through NAT/firewalls, resume across drops, integrity, catalog landing,
identity — **we already built** (see §2). The one thing Signiant actually sells is
**WAN transport acceleration**: on a high-latency, slightly-lossy intercontinental link
(Coimbatore↔US ≈ 200–280 ms RTT), a single TCP stream collapses — a 500 Mbps pipe can
yield tens of Mbps, because loss-based TCP congestion control retreats hard when the
round-trip is long. That gap is the entire Signiant value proposition, and closing it
is this project.

**Two links, not one.** Bangalore is domestic (≈ 15–30 ms RTT); plain TCP already runs
near line-rate there, so Bangalore needs *no transport work* — only the public landing
zone and enrollment. The US link is where the accelerated transport earns its keep.

## 2. What already exists (the ~80% we do not rebuild)
From `~/sutra-agent` (`docs/architecture.md`) and the sutradhara receive funnel:
- **Outbound-dial control plane** — the agent dials out over mTLS and opens one
  bidirectional `DeviceService.Connect` stream; it never opens an inbound socket
  (unit-test-enforced). NAT/firewall traversal on the client side is structurally
  solved.
- **Intake → verify → RAO → copy-3** — file-chunk streaming over `IntakeService`,
  per-file hashing, BagIt tags, `intake.json` completion, readback-verify, landing in
  RAO under the multi-copy durability floor.
- **Resume + crash safety** — the in-flight journal records each receive through
  `streaming`; disconnects retry with exponential backoff.
- **Identity + enrollment** — cert-based mTLS, `.sutra-enroll` bundle, tray/NSIS
  installer path already in flight (the ad-hoc fast-lane client — see §4.3).
- **Hardened public NIC** — the akash box already runs a default-deny `public_guard`
  nft table; opening exactly the Setu ports fits that model.

## 3. The transport engine — options and decision
The core choice, since everything hangs on it.

**Option A — parallel TCP streams.** Stripe chunks across N TCP connections; independent
loss-backoffs fill a fat pipe better than one stream. Gets ~60–85% of line rate on a
clean-but-fat link, less on a genuinely lossy one; firewall-friendly (TCP/443); low
effort. Still fundamentally loss-based; N streams is N× as aggressive and can get
rate-limited.

**Option B — QUIC data plane on `quinn` with BBR (RECOMMENDED).** Bulk data over QUIC
(UDP) with a **model-based** congestion controller (BBR) that continuously probes the
pipe's bandwidth and fills it, rather than retreating on every dropped packet — the
Aspera/Signiant trick, done with a modern open algorithm. QUIC also gives connection
migration (a transfer survives the sender's IP changing / NAT rebind), native TLS 1.3
(cert identity ports over), and no head-of-line blocking. "Fill whatever's there" is
literally BBR's job; "cap to 200 Mbps during office hours" is a pacing-rate knob.
`quinn` is mature, pure-Rust, production-grade. Cost: UDP is blocked/throttled on some
networks (→ needs a fallback, which is A); building a tuned data mover on quinn is real
work.

**Option C — wrap a black-box UDP accelerator (kcp/UDT/etc.).** Collapses into B: the
best-supported "existing accelerated transport" *is* QUIC. A black-box mover also makes
"plug it into our own system" harder, because we lose control of framing/resume/identity
at exactly the seam where catalog + verify integration lives.

**Decision: B, with A as an automatic fallback.** A QUIC+BBR engine that saturates the
pipe; when it detects UDP is blocked or strangled, it transparently drops to
parallel-TCP-over-443. That hedge means Setu is never *worse* than Signiant even on a
network hostile to UDP.

**Why we are confident about B:** the hardest part of B is not the protocol — it is
*keeping the pipe full from disk without blowing memory*: bounded read-ahead on the send
side, watermark backpressure on the receive side so a slow RAO commit never stalls the
socket and a fast socket never OOMs. **This is the exact problem solved in TIO-5/6 for
tape** (the anti-shoe-shine reservoir, the one-in-flight submitter, the watermark
stop-start). We are pointing proven thinking at a WAN socket instead of a tape drive.

## 4. Architecture

### 4.1 Keep the control plane; swap the data plane
The sutra-agent control model is unchanged: outbound-dial, mTLS `DeviceService.Connect`,
enrollment, in-flight journal, intake funnel. The **only** change is the bulk data
channel. Introduce a `DataTransport` seam with two implementations —
`QuicBbrTransport` and `ParallelTcpTransport` — selected by a **capability probe at
connect time**. The intake supervisor talks to the *trait*, never the wire. The control
stream stays on gRPC/TCP (tiny, latency-tolerant); only the fat data moves to QUIC.

**One funnel invariant.** Setu adds a faster pipe *into* the existing intake→verify
funnel; it must not create a second ingest path that bypasses verify/dedup/policy. (This
is the additive-bias failure mode we have been bitten by — flagged explicitly for the
codex prompts.)

### 4.2 Public ingress — the landing zone
The server must be reachable from the US. **Recommend direct-to-server to start:** the
US agent dials the Coimbatore server's public QUIC endpoint (UDP/443) plus the control
endpoint, both hardened behind the existing `public_guard` nft table (open exactly those
ports), both mTLS-gated. Control-plane web surfaces stay behind the Authentik/dvarapala
wall; the data plane is cert-gated QUIC with no browser involved. Cloud relay POP
(§5) stays on the shelf pending measurement.

### 4.3 Two front doors, one funnel (the "both")
- **Site-to-site — the managed US box (volume anchor):** a headless `sutra-agent serve`
  daemon, enrolled once with a long-lived cert, fed by a watched directory or the site's
  workflow, streaming continuously over the QUIC engine with scheduling + bandwidth caps.
  Essentially today's agent + the new transport + a small scheduler. This carries the
  multi-TB/week.
- **Person-to-portal — ad-hoc contributors:** *fast lane* = the installable client
  already being built (Windows tray / native receive app), enrolled via a **short-lived,
  single-purpose throwaway bundle**; *convenience fallback* = a plain browser upload for
  people who will not install anything (un-accelerated, slow — acceptable because ad-hoc
  is the low-volume tail; the managed site drives volume).
- **Security scoping for ad-hoc:** capability-scoped enrollment — **push-only into a
  quarantine intake, no browse, no pull, rate-limited**; everything ad-hoc lands in
  quarantine → verify → policy before it is trusted. Same funnel, no new trust path.

### 4.4 Steady-high-volume machinery (the owner's data profile)
- **Bandwidth caps + scheduling** → BBR pacing rate as a per-site policy knob (e.g.,
  capped 07:00–22:00, uncapped overnight). Small scheduler in the daemon.
- **Disk ↔ socket backpressure** → reuse the TIO-5/6 reservoir + watermark pattern.
- **Resume at intercontinental scale** → extend the in-flight journal to survive
  multi-hour transfers and IP changes; QUIC connection migration does much of this for
  free.
- **Integrity** → existing per-file hashing + BagIt + readback-verify + copy-3,
  unchanged.

### 4.5 Proving parity (the verification member)
A goodput dashboard (rem-top style), per-transfer stats, and an **A/B harness against a
real Signiant transfer log on the same link**. We *prove* parity, not assert it. Per the
working pattern, the prompt set ships with its own verification member (a scenario or
covered gate).

## 5. Cloud relay POP — deferred, not skipped
A **cloud relay POP** (Point of Presence) is a node rented in a well-connected data
center that *both* ends dial; the sender pushes to the relay and the relay forwards to
us. This is how Signiant's own service works by default. It helps for three reasons, in
order of relevance: (1) **path** — "US → cloud POP → India" can beat "US → India
direct," because cloud providers run private backbones between regions that outrun the
public internet, and each shorter leg is a road BBR can fill; (2) **reachability**
without exposing the home server; (3) **staging/buffering** across short outages.

Deferred because: it is another box to run and secure; **data transits a third party**,
so it must be end-to-end encrypted so the relay never sees plaintext; **cloud egress
costs money** per GB and could eat back part of the Signiant savings (a cost-efficiency
call to make with real numbers); and it is only worth it **if the direct path is
actually bad** — which the Spec 0 measurement spike tells us directly. Build it only if
measurement demands it.

## 6. Program decomposition and phasing
This is a program, not one spec.
- **Spec 0 — measurement spike (~1 day).** iperf3, RTT/loss, and one real Signiant
  transfer log on the actual US↔Coimbatore link. Output: the true gap and the target
  number. De-risks everything; could even show parallel-TCP suffices (unlikely on a
  lossy intercontinental link, but cheap to rule in/out).
- **Spec 1 — the core (this design).** QUIC+BBR engine + TCP fallback + public ingress +
  the site-to-site managed path, landing in RAO with resume/verify/caps. Everything
  depends on this.
- **Spec 2 — later.** The ad-hoc person-to-portal path (scoped enrollment, quarantine
  funnel, installable fast-lane + browser fallback).
- **Spec 3 — only if measurement demands it.** The cloud relay POP.

**Bangalore note:** because Bangalore is domestic, the moment Spec 1's public landing
zone exists, Bangalore can receive over the *existing* TCP path at near line-rate with no
transport work — a near-term visible win that falls out of Spec 1 for free.

## 7. Security posture
- Public attack surface grows: exactly two hardened, mTLS-gated ports on the akash public
  NIC, inside the existing `public_guard` default-deny table.
- Ad-hoc enrollment is short-lived, capability-scoped (push-only, quarantine, no
  browse/pull, rate-limited); ad-hoc content is untrusted until it clears verify/policy.
- If a relay is ever added, data must be end-to-end encrypted so the relay cannot read
  plaintext; no plaintext persisted at the POP.
- Enrollment bundles and certs never committed; standard secret hygiene.

## 8. Open questions (for panel / owner)
1. **Link numbers** — real US uplink / Coimbatore downlink bandwidth, RTT, loss, and
   Signiant's observed throughput (Spec 0 answers this).
2. **Name** — *Setu* ok, or another family name?
3. **Relay** — accept "direct-first, relay only if measured bad," or is there a known
   peering problem that argues for a relay from day one?
4. **Browser fallback scope** — is an un-accelerated browser upload worth building in
   Spec 2, or do all ad-hoc senders install the client?
5. **BBR variant / tuning** — BBRv2 vs v3-style; pacing-cap policy granularity
   (per-site, per-time-window) — technical, resolved during Spec 1 design.

## 9. Next steps
1. Owner reviews this doc.
2. Panel review (multi-lens: transport/perf, security/trust-boundary, cost-efficiency,
   failure-modes/ops, contract coherence) → fold → verify round.
3. Spec 0 measurement spike (can run in parallel with the panel).
4. Cut the Spec 1 codex prompt set per-repo (sutra-agent client transport; sutradhara
   ingress + landing), each with its verification member.
