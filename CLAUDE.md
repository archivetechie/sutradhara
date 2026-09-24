# sutradhara — working conventions

## What this repo is
The archive orchestrator (formerly "lodestar"): content-addressed catalog,
multi-backend copy fan-out with per-placement sealing (via Remanence REM-OBJECT),
scrub + self-heal, key registry. Code in `src/sutradhara/`; CLI `sutra`
(`.venv/bin/sutra`); DB via `SUTRADHARA_DB_URL`
(default sqlite at /var/lib/replica/sutradhara.db).
Backends: `rem_tape` (gRPC to remanence), `d2_tape` (java CLI adapter),
`memory` (tests), `s3` (ingest v2). Sealing: `sealing/` (Sealer/Opener ports,
REM-OBJECT = stateless local `rem-debug` codec — NEVER a daemon/gRPC service, by
decision).

## Verify
`uv run pytest -q` (fast, hermetic). End-to-end truth lives in `~/system`:
`make suite` (the scenario harness drives this repo as an editable dep).

## The trap that bites
**`~/system` consumes this repo as an editable path dep from THIS working
tree's main.** Breaking main breaks the harness silently — land complete, run
pytest before every commit. Policy compat (o/n archive shims) must keep
Scenario J/N/O/Q green.

## Pattern + hygiene
Designs, prompts, reviews and journals live in the supervising private repository;
this public repository carries distilled architecture, guides and specifications.
Implementation is independently reviewed and verified by the harness scenarios.
Gardener creates isolated recovery checkpoints and reviewed documentation candidates.
It does not automatically commit the active branch, push it, or delete remote branches.
Track working-document lifecycle in the private repository's documentation registry.
Keep the public navigation index `docs/INDEX.md` current when public guides change.
