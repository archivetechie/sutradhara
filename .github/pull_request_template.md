## Change

Describe the behavior and operational reason for this change.

## Verification

- [ ] `uv run pytest -q`
- [ ] `uv run ruff check src tests packages/sutradhara-receive/src`
- [ ] `uv run ruff format --check src tests packages/sutradhara-receive/src`
- [ ] `uv run python scripts/check-mypy-baseline.py`
- [ ] Generated protobuf and receive fixtures are unchanged or intentionally updated
- [ ] Public docs and `docs/INDEX.md` are updated for changed interfaces or contracts

## Safety

State the effect on deletion evidence, durability, restore verification, path
confinement, credentials, migrations, and rollback. Use “not applicable” only
after checking each boundary.
