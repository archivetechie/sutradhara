# Codex prompt — device_id uniqueness & re-enroll supersede (sutradhara)

**Repo:** `~/sutradhara/repo` · **Status:** pending
**Companion:** `~/system-ui/docs/prompt-device-id-uniqueness-system-ui.md` (UI affordance; shares the contract below verbatim).

## Why
Today two workstations can enroll under the same `device_id` and both hold valid
certs: `record_device_enrollment` **adds** a fingerprint without retiring prior
ones, and nothing at mint time checks whether a `device_id` is already taken. The
in-memory connected-device registry is last-writer-wins, so one silently shadows
the other. `operator_for_device` even *raises* on "conflicting active operator
enrollments" — the one-active-enrollment-per-device invariant is assumed but never
enforced. This makes the enrolled identity ambiguous and lets one operator's name
collide with another's.

We want **one active certificate per `device_id`, owned by one operator**, while
still allowing a machine to legitimately **re-enroll** (cert expiry, key loss/
compromise, reinstall). So the rule is not "reject duplicates" — it is **unique
identity, and re-enrollment supersedes** (rotates the cert; the old one is revoked).

---

## Shared contract (identical in the system-ui companion prompt)

**Invariant:** at most **one active (non-revoked) certificate enrollment per
`device_id`**, owned by **exactly one operator**. Re-enrolling rotates the cert:
the new certificate supersedes (revokes) the prior one for that device. A device
already owned by a *different* operator is **not self-serviceable** — only an admin
revoke (`POST /api/devices/{id}/revoke`, exists) can free the name.

**Mint — `POST /api/enroll/token`** gains an optional body field `reenroll: bool`
(default `false`). Behavior by current ownership of `device_id`:

| Current active enrollment | `reenroll` | Result |
|---|---|---|
| none | any | mint token (new device) |
| owned by requesting operator | `false` | **409 `device_already_enrolled`** |
| owned by requesting operator | `true` | mint token (rotation intended) |
| owned by a different operator | any | **409 `device_other_operator`** |

**Sign — `POST /api/enroll/csr`** is the enforcement boundary (defense in depth —
a token could in principle outlive an ownership change): recording the new cert
**revokes every other active fingerprint for that `device_id` owned by the same
operator** (supersede), and **refuses** (does not touch existing rows) if the device
has an active enrollment under a *different* operator. Re-signing the *same*
fingerprint stays idempotent.

**Error shape:** the existing `{ "error": <code>, "detail": <msg> }` JSON body.
Codes: `device_already_enrolled` (409), `device_other_operator` (409). The UI
branches on these codes (see companion).

**Unchanged:** token remains single-use / device-bound / operator-scoped / 24h /
tailnet-only. `IntakeService`, the streaming/relay path, and the cert format are
untouched.

---

## Files
- `src/sutradhara/grpc/store.py` — `record_device_enrollment` (the enforcement point).
- `src/sutradhara/grpc/ca.py` — `sign_device_csr` (release token + surface refusal).
- `src/sutradhara/api/routes_devices.py` — `EnrollTokenRequest` (+`reenroll`),
  `post_enroll_token` (mint guard), `post_enroll_csr` (map refusal → 409).
- Tests: `tests/grpc/test_store.py`, `tests/grpc/test_ca.py`,
  `tests/api/test_routes_devices.py` (match the actual existing test module paths).

## Milestone 1 — storage invariant in `record_device_enrollment`
Make recording a fingerprint enforce operator-exclusivity + supersede.

- [ ] **Test first** (`test_store.py`):
  - `record_device_enrollment(D, fp1, op="a")` then `record_device_enrollment(D, fp2, op="a")`
    → `operator_for_device(D) == "a"`; `resolve_device(D, fp1)` fails (revoked);
    `resolve_device(D, fp2)` resolves. Exactly one active row for D.
  - Re-record the same `(D, fp1, "a")` twice → still exactly one active row (idempotent).
  - `record_device_enrollment(D, fp1, op="a")` then `record_device_enrollment(D, fp3, op="b")`
    → raises `PermissionError`; op "a"/fp1 row **unchanged** (still active), no fp3 row.
- [ ] **Implement:** in `record_device_enrollment`, within the session:
  1. Load active (`revoked is False`) enrollments for `device_id`.
  2. If any active row has `operator != operator` (the arg) → `raise PermissionError("device belongs to a different operator")` and mutate nothing.
  3. Revoke (`revoked=True`, `revoked_at=now`) every active row whose `cert_fingerprint != normalized`.
  4. Reactivate-or-insert the `(device_id, normalized)` row for `operator` (keep the existing reactivate-if-present branch).
- [ ] Run the store tests → green. Commit.

## Milestone 2 — mint guard on `POST /api/enroll/token`
- [ ] **Test first** (`test_routes_devices.py`, using the app's test client with the
  operator identity headers the suite already uses):
  - fresh `device_id` → 200, returns a token.
  - same `device_id`, same operator, no `reenroll` → 409, body `error == "device_already_enrolled"`.
  - same `device_id`, same operator, `{"reenroll": true}` → 200, token minted.
  - `device_id` enrolled to operator A, mint as operator B → 409, `error == "device_other_operator"`.
- [ ] **Implement:**
  - Add `reenroll: bool = False` to `EnrollTokenRequest`.
  - In `post_enroll_token`, before `issue_enroll_token`, open a read session and call
    `operator_for_device(session, body.device_id)` (catch its `PermissionError` for the
    conflicting-operators anomaly → treat as `device_other_operator`):
    - `None` → proceed.
    - `== identity.operator_username` and not `body.reenroll` → `_raise(409, "device_already_enrolled", "device already enrolled — re-enroll to rotate its certificate")`.
    - `== identity.operator_username` and `body.reenroll` → proceed.
    - else (different operator) → `_raise(409, "device_other_operator", "device is enrolled to a different operator; an admin must revoke it first")`.
- [ ] Run the route tests → green. Commit.

## Milestone 3 — surface sign-time refusal cleanly
`record_device_enrollment` can now raise `PermissionError` at sign time (rare race:
ownership changed between mint and CSR redemption). Ensure the token is released and
the endpoint returns a clean 409, not a 500.

- [ ] **Test first** (`test_ca.py` and/or `test_routes_devices.py`): seed an active
  enrollment for `device_id` under operator A; consume a token minted for operator B
  (constructed directly via `issue_enroll_token`, bypassing the M2 guard) through
  `sign_device_csr` → raises `CertificateError`; the token is left **unused**
  (`release_enroll_token` restored it); no new fingerprint recorded.
- [ ] **Implement** in `sign_device_csr`: wrap the `record_device_enrollment` call;
  on `PermissionError`, `release_enroll_token(session, token)` in a fresh txn and
  `raise CertificateError("device belongs to a different operator")`. In
  `post_enroll_csr`, the existing `except (ValueError, CertificateError)` handler
  should map this case to `_raise(409, "device_other_operator", str(exc))` (split the
  handler so genuine bad-CSR stays `400 bad_enrollment`).
- [ ] Run tests → green. Commit.

## Milestone 4 — full-suite regression
- [ ] Run the whole test suite (`pytest`) → green. Commit.

## Out of scope
- No new tables/migrations (reuse `GrpcDeviceEnrollment` + its `revoked` flag).
- No change to the connected-device registry, the relay, or `IntakeService`.
- Admin reassignment UX beyond the existing `POST /api/devices/{id}/revoke`.
- Rate-limiting (the app-level `enroll_csr_limiter` stays; Caddy plugin is separate).

## Definition of done
One active cert per `device_id`; duplicate mint by the same operator returns
`409 device_already_enrolled`; `reenroll:true` rotates and revokes the prior cert;
a different operator is refused at both mint and sign with `409 device_other_operator`;
full suite green.
