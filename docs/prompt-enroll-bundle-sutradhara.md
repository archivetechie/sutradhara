# Prompt — enrollment bundle endpoint (sutradhara)

**Repo:** `~/sutradhara`. **Status:** pending. **Cut:** 2026-07-04.
**Design:** `~/sutra-agent/docs/design-agent-tray-installer.md` §7 (read it).
**Contract:** read `docs/contract-enroll-bundle.md` — **it is normative**; do not
restate or reinterpret its rules, implement them.

## Task

Server member of the tray/installer arc, in dependency order:

1. **Extract the mint guard.** `api/routes_devices.py::post_enroll_token` inlines ~65
   lines of policy (ownership checks, re-enroll gate, `_rotation_authorization`,
   self-rotation proof, `issue_enroll_token`, expiry computation). Extract
   `mint_enroll_token(...) -> (token, expires_at)` so the policy exists ONCE; rewire
   `post_enroll_token` through it. Behavior-preserving — existing enroll tests must
   pass unchanged (except where the charset rule below adds a new rejection).

2. **`device_id` charset enforcement** at the mint guard: `^[A-Za-z0-9._-]{1,128}$`
   (contract). Applies to `/api/enroll/token` and the new bundle endpoint. Clear 400
   (`invalid_device_id`) with the allowed pattern in the detail.

3. **`POST /api/enroll/bundle`** — body `{device_id, reenroll}`; operator-authenticated
   identically to `/api/enroll/token`; calls `mint_enroll_token`; packages the bundle
   JSON per the contract schema; returns an **explicit Response** with the contract's
   three headers (a dict return cannot set them). Response body never logged; add a
   regression test asserting no token substring appears in captured logs.

4. **`agent_bundle` server config section** (endpoints list with per-entry optional
   `server_name`, enroll CA path, console URL) — source for the non-minted bundle
   fields. Missing/incomplete section ⇒ the endpoint returns 503
   (`bundle_not_configured`) rather than a partial bundle.

## Definition of done

Per `AGENTS.md`. Hermetic tests: mint-guard extraction equivalence; charset matrix
(valid, 129-char, slash, `..`, CRLF, empty); bundle response — schema fields, header
triple (`Content-Type`, RFC 6266 `Content-Disposition` filename, `Cache-Control:
no-store, private`), token matches a redeemable token (round-trip against
`/api/enroll/csr` in the existing test harness), 503 on unconfigured, re-enroll path
mints with rotation authority, no-log assertion. `covers`: existing enroll/CSR test
suite extended in place — no new scenario needed server-side (the browser-QA gate lives
in the system-ui prompt).
