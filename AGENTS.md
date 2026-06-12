# Agent conventions (codex and others)

Read CLAUDE.md. Non-negotiables:
1. **Run what you changed**: `uv run pytest -q` — paste the output. For
   storage-path changes, note which `~/system` scenario verifies it.
2. **Commit; never leave the tree dirty** (WIP → `wip/<topic>` branch).
   ~/system consumes this working tree's main as an editable dep — a broken
   main silently breaks the steering harness.
3. **Update `docs/INDEX.md`** for prompts you implement / docs you add.
4. Facts that bite: RAO sealing uses the local Remanence `rem-debug` CLI (no
   service); the key registry default is
   `/var/lib/replica/sutradhara-key-registry`; two QuadStor VTLs exist
   (rem=mainlib, d2tape=d2lib) — never assume one library/backend; policy
   documents are strict-validated (unknown keys = error).
