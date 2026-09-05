# Contributing

Sutradhara welcomes focused bug reports, documentation corrections, tests, and
small implementation changes. For large behavior or schema changes, open an
issue first so the operational invariants and migration path can be agreed.

## Development setup

```sh
uv sync --locked --extra dev
uv run pytest -q
uv run ruff check src tests packages/sutradhara-receive/src
uv run ruff format --check src tests packages/sutradhara-receive/src
uv run python scripts/check-mypy-baseline.py
```

Partial-file restore is an optional integration. Tests that require the
separately distributed `format-anatomy` package skip when it is absent; install
a compatible copy into the environment when changing PFR behavior.

Strict mypy has a checked-in diagnostic-specific baseline. New diagnostics fail
the gate. Baseline changes must only remove fixed diagnostics or accompany an
explicitly reviewed explanation; do not regenerate it to hide new errors.

When changing protobuf sources, run `scripts/regenerate-proto.sh` and commit the
generated stubs. When changing `packages/sutradhara-receive`, also run its
`cargo test --locked` suite and regenerate the shared fixture corpus to prove it
did not drift.

## Change expectations

- Add regression coverage for behavior changes and migration coverage for
  schema changes.
- Preserve fail-closed deletion, restore verification, path confinement, and
  the distinction between provisional `WRITTEN` and durable `CHECKPOINTED`
  Remanence writes.
- Keep credentials, private infrastructure paths, customer data, and working
  design journals out of the public repository.
- Update the public references and `docs/INDEX.md` when a command,
  configuration variable, contract, or deployment step changes.
- Keep commits narrow and explain the operational reason for the change.

By participating, you agree to follow [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).
