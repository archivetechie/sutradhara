# sutradhara — working conventions

## What this repo is
The archive orchestrator (formerly "lodestar"): content-addressed catalog,
multi-backend copy fan-out with per-placement sealing (via Remanence RAO),
scrub + self-heal, key registry. Code in `src/sutradhara/`; CLI `sutra`
(`.venv/bin/sutra`); DB via `SUTRADHARA_DB_URL`
(default sqlite at /var/lib/replica/sutradhara.db).
Backends: `rem_tape` (gRPC to remanence), `d2_tape` (java CLI adapter),
`memory` (tests), `s3` (ingest v2). Sealing: `sealing/` (Sealer/Opener ports,
RAO = stateless local `rem-debug` codec — NEVER a daemon/gRPC service, by
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
The maintainer brainstorms with Claude → design/prompt docs in `docs/` → codex (or
Claude) implements → harness scenarios verify. Background `gardener` auto-
commits idle work, pushes, prunes merged branches — never ask the maintainer to do
repo hygiene. Docs lifecycle: `docs/INDEX.md` + `docs/archive/`.
