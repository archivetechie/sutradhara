# Codex prompt — PFR wiring (sutradhara): real pfr-index handler + RaoObject + sutra pfr CLI

Status: pending — **blocked on format-anatomy M6 landing** (prompt-pfr-core-m6-seams.md)
Normative contract: **`docs/design-pfr-wiring-mxf.md` §3 + §4 in full — the authority.**
Context: `~/format-anatomy/analysis/2026-07-05-sutradhara-wiring-survey.md` (file:line map
of every surface), design-derivation-reconciler.md §3.4a, design-pfr-restore.md.

## Scope

1. **Packaging** (design §6.2): add format-anatomy as an editable uv path dep (follow the
   `sutradhara-receive` precedent exactly); `uv sync` here AND in `~/system`; distribution
   rename to `format-anatomy` happens in that repo — depend on the new name.
2. **Component A** — swap `jobs/handlers/pfr_index.py` internals per design §3: isolated
   scrape under the job lease (120 s wall-clock), blob dir under the intake's pfr/blobs/,
   payload with injected `ingest_item_id`/`logical_asset_hash`, atomic publication,
   upgraded `_has_pfr_sidecar` (parse + blob-ref verify), the EXACT failure matrix
   (closed over ReasonId; §3.4a blocked+SUSPECT only on reproducible determination),
   `recipe_version` persisted in fact metadata, `SUTRADHARA_FAKE_FFPROBE` retired.
3. **Component B** — `RaoObject` ByteRangeSource + cut path per design §4: shared
   `member_byte_base()` primitive (refactor archive_restore/archive_fanout to use it;
   re-inlining forbidden), session-scoped reader on `RemanenceBackend` (ONE
   OpenReadSession per cut, context-managed, gRPC status mapping per design), locator
   size cross-check vs GetFile, `cut_from_sidecar` happy path, blob LRU cache (20 GiB
   default, config key) + regeneration via `scrape_source`, the three-rung ladder as an
   explicit recorded state machine, rung-3 scratch preflight, io-lease + per-item lock +
   temp-rename on output.
4. **CLI**: `sutra pfr cut|status|reindex` per design §4/§3 (selector =
   `resolve_member_asset_hash` grammar; human summary default, `--json` envelope;
   reindex force semantics per design §3).
5. **Docs hygiene**: stale banner atop design-pfr-restore.md §5 pointing here; update this
   repo's docs INDEX (if present) and prompt statuses.

## Verification member

Unit tests per design §5: RaoObject (session reuse — assert exactly one OpenReadSession
per cut via fake server call-count; shared member-base; half-open boundaries; negative
offsets vs member; session-invalid → SourceChanged), handler matrix (EVERY ReasonId row,
determination logic incl. the two-consecutive rule, atomic-publication crash points via
injected kills), blob-cache eviction + regeneration, reindex (direct enqueue, dedupe key,
atomic replace). All against a fake in-process ReadObjectRange gRPC server — no live rem.
Full sutradhara suite stays green (uv run pytest). The harness scenario is a separate
prompt (~/system) — do NOT write scenarios here.

## Constraints

**Concurrent-thread awareness**: another implementation thread (unified logs, P-L1b) may be
working in this repo simultaneously (routes_logs.py, logs_store.py, reconcilers/log_pipeline.py,
their tests). Do NOT touch those files; stage ONLY the files you created/modified (never
`git add -A`/`git add .`); if `pyproject.toml` has uncommitted changes you didn't make, stop
and report rather than committing over them. Pre-existing red tests from other threads are
not yours to fix — success bar = no NEW failures.


Follow the design where this prompt is silent; design wins on conflict — note conflicts in
the report. No changes to pfr_core (M6 already provides every seam; if a seam is missing,
STOP and report rather than patching around it). Report → docs/report-pfr-wiring.md.
