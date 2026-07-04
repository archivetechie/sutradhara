# codex prompt — console P1 (server): groups/authz backbone

**Repo:** `~/sutradhara/repo` (work here only). **Single source (normative):**
`~/system-ui/docs/design-console-excellence.md` — read §2 (whole), §6-P1, §7-N/§7-B/§7-C/§7-F
before writing code. This prompt implements the *server* member of the atomic three-repo P1;
dvarapala and system-ui land separately. Status: implemented (2026-07-04; diff gate PASS; P1 browser-QA PASS).

## Tasks
1. **Refactor `src/sutradhara/api/identity.py` to a flat `GROUP_CAPABILITIES` map +
   union-over-all-groups** (design §2.2). Groups (§7-F rename): `sutradhara-ingest` (was
   `-operator`), `sutradhara-oversight` (was `-viewer`), new `sutradhara-restore`, existing
   `sutradhara-admin`, plus privacy caps `sutradhara-restore-p2`/`-p3`.
   - Capability sets exactly per design §2.2/§2.3: ingest = `can_view, can_receive`;
     restore = `can_view, can_restore` **only** (§7-N); oversight = `can_view`;
     admin = `can_view, can_admin` (NO implicit receive/restore — §7-B);
     `-p2`/`-p3` = the privacy cap **only** (no view/restore alone — §7-N); p3 ⊇ p2 preserved.
   - New capability `can_restore`. `role` becomes a **display-only** precedence label
     (admin > restore > ingest > oversight); nothing gates on `role`.
   - **Old group names must keep working during migration**: accept `sutradhara-operator` as
     alias for `-ingest` and `sutradhara-viewer` for `-oversight` (marked deprecated in the
     map), so a not-yet-migrated member is not stranded (§6-P1 lockout rule). Emit no alias in
     `/api/session` output — capabilities only, as today.
2. **Tighten `POST /api/ui/restores` from `can_view` to `can_restore`** (§7-C): new
   `_require_restore` in `routes_restore.py` (mirror `_require_view`/`_require_admin`
   pattern at `routes_restore.py:223`). Per-item p2/p3 admission gates unchanged.
3. **Amend `docs/contract-hdcache-restore.md`**: §3 POST gate `can_view` → `can_restore`
   (record as design decision §7-C), and note the **additive** future `/api/ui/reconciliation`
   extension (design B4) as a planned amendment — do not implement the extension in P1.
4. **Tests** (extend the existing identity/API test modules):
   - each single group → exact capability set (all six groups incl. aliases);
   - unions: Ingest+Restore, Admin+Ingest, Restore+p3, Restore+p2;
   - negatives: admin alone has no receive/restore; `-p2`/`-p3` alone → cap only (no
     view/restore); unknown group (`sutradhara-admin-extra`) → nothing; empty groups → no caps;
   - POST /api/ui/restores: 403 for `can_view`-only session, 2xx path for `can_restore`;
   - old-name alias resolves to the same capabilities as the new name.

## Constraints
- Do NOT rename or add gates on any other endpoint in P1 (the §2.4 table's other **fix** rows
  land in their own phases). Do NOT touch `~/dvarapala` or `~/system-ui`.
- No secrets in code/logs. Keep the error envelope + sanitization behavior unchanged.

## Done
`uv run pytest -q` green (full suite). Commit directly to main with message
`console-p1(server): flat GROUP_CAPABILITIES + can_restore gate + contract amendment`.
Print a 10-line summary: files touched, test counts, contract diff summary.
