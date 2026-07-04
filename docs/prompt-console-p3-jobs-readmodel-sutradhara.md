# codex prompt — console P3 (server): jobs/resources read-models + reconciliation extension

**Repo:** `~/sutradhara/repo` (work here only). **Normative source:**
`~/system-ui/docs/design-console-excellence.md` §4.1 (shapes are normative — field lists
verbatim), §4 conventions (envelope/sanitization/caps), §2.4 (gates), §6-P3 — read before
writing. Status: implemented 2026-07-04.

## Tasks
1. **New contract doc `docs/contract-jobs-readmodel.md`** (single-sourced; copy the §4.1
   shapes + conventions; this doc is what the UI phase codes against). Registered in
   `docs/INDEX.md`.
2. **`GET /api/ui/jobs?state=&kind=&limit=`** (`can_view`): single `job`-table query,
   newest-first, cap 200, envelope `{"total","truncated","jobs":[...]}` with the §4.1 field
   list exactly. `status` is the six-state enum `queued|pending|running|succeeded|failed|
   cancelled`. **`target_summary`** derived server-side from `recon_target_key` else a params
   summary — NEVER a raw path (B7). `last_error` sanitized.
3. **`GET /api/ui/jobs/{id}`** (`can_view`): job + `job_attempt` rows per §4.1 (attempt fields
   verbatim incl. `granted_leases`, `detail`). `detail`/`error` sanitized with a **recursive
   JSON sanitizer** (B8): walk nested objects/lists/strings, redact absolute paths — extend
   `_sanitize_detail` (`routes_restore.py:354`) into a shared helper both routers use.
4. **`GET /api/ui/resources`** (`can_view`): pools from the worker's `jobs/config.py` config;
   `in_use` = Σ required over running jobs; `waiting` = pending jobs needing the pool.
   Complete enumeration — no total/truncated. Label the cross-process approximation in the
   contract (§7-G): capacity may diverge if the worker ran with pool overrides.
5. **Additive `/api/ui/reconciliation` extension (B4 + B6a):** payload gains
   `blocked_tool_name`, `blocked_tool_version`, `attempt_count` (columns exist on
   `ReconciliationCondition`) and a **plain-language `cause`** (safe for all viewers, e.g.
   "SMART degradation detected on disk d004"); the raw `message` becomes **admin-only —
   omitted server-side for non-admin sessions** (the endpoint reads `can_admin` from the
   session identity). Record the additive amendment in `contract-hdcache-restore.md` (it
   already notes it as planned).
6. **Tests**: shapes (field-for-field vs contract); six-state enum passthrough; cap-200 +
   total/truncated correctness; `target_summary` never contains a path for jobs with pathy
   params (adversarial fixture); recursive sanitizer on nested detail (dict-in-list-in-dict
   with absolute paths); `can_view` gate on all three; reconciliation: non-admin gets `cause`
   but no `message`, admin gets both; blocked condition carries blocked_tool_*/attempt_count.

## Constraints
No UI work. No schema migrations (all columns exist). No other endpoint changes. Keep the
error envelope identical to the restore contract.

## Done
`uv run pytest -q` green (full suite). Commit to main:
`console-p3(server): jobs/resources read-models + reconciliation cause/blocked_tool extension`.
Print a 10-line summary.
