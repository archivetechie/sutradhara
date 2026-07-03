# Codex prompt — relay hardening (3 follow-ups from the Rust-agent panel)

**Repo:** `~/sutradhara/repo` (server side). **Status:** implemented.
**Origin:** the 5-lens panel on `~/sutra-agent/docs/design-rust-agent-control-plane.md` §13
found three gaps in the **already-built server relay** — client-independent, so filed here.
Ordered by severity: #1 is a data-safety issue (a false "safe to eject"); #2–#3 are security
hardening.

## 1 [safety, highest] — reject a resume whose source plan changed
**Problem:** on a resumed receive, if the card was mutated between attempts (file added, mtime
touched), the agent re-streams only "the rest" against a **stale** plan; the assembled bag can
mix old+new card state and still commit → status `verified` → the operator is told "safe to
eject" for a bag that doesn't match the card. `StartIntake` carries `source_plan_digest`
(`grpc/servicer.py:349`) but the server currently **stores it opaquely and does not enforce it
on resume**.
**Fix:** when an intake is resumed (same `intake_id`/`idempotency_key`) with a
`source_plan_digest` **different** from the one recorded at first `StartIntake`, **reject**
(e.g. `FAILED_PRECONDITION "source changed; start a new intake"`) or mint a fresh intake —
never continue the old one. Confirm the exact current behavior first, then add the check +
a test that a changed digest is refused and an unchanged one resumes.

## 2 [security] — `reenroll` must not supersede an active cert without proof
**Problem:** `store.record_device_enrollment` revokes a prior active fingerprint on re-enroll
**without proof-of-possession of the old key** (`api/routes_devices.py` enroll path +
`grpc/ca.py`), so a fresh token alone can silently rotate/replace a device's cert.
**Fix:** require either proof of the old key (an mTLS call on the existing cert) **or** an
admin/MFA-confirmed rotation before superseding an active fingerprint; emit an event/notice on
rotation and **evict the old live stream** from the registry immediately. Ties to the
operator-owned-recovery / AD-distrust posture (`ad-password-distrust-needs-mfa`) — keep rotation
operator/admin-owned, not central-IT. Add tests for: self-rotation with old-key proof
(allowed), rotation without proof (refused), cross-operator (already 409).

## 3 [security] — propagate revocation/expiry to a live stream
**Problem:** `grpc/device_service.py` re-resolves device identity **only on command dispatch**,
so a cert revoked mid-session keeps its `Connect` stream alive and functional until its next
command; TLS `NotAfter` is not re-checked after the handshake.
**Fix:** re-resolve identity (reject revoked) on **heartbeat and every inbound message**, not
just command dispatch; enforce a **max stream lifetime**; evict on revocation. Add a test: a
device revoked mid-stream is evicted on its next heartbeat/message, not just its next command.

## Definition of done (AGENTS.md applies)
Per item: root-cause confirmed against current code before the fix; `uv run pytest -q` green
with the new tests (paste counts); no change to the client wire contract (these are server
behavior). Consider whether the scenario-catalog Group 2 (DRE/security) or a failure-mode
scenario in `~/system` should cover #1's false-verified path — note it. Commit; mark implemented
in `docs/INDEX.md`.
