# Changelog

All notable user-visible changes are recorded here. Sutradhara is pre-1.0, so
minor releases may still change commands, configuration, schemas, and wire
contracts; migration notes accompany incompatible changes.

## Unreleased

### Added

- Continuous integration for Python 3.11/3.12, formatting, lint, strict-mypy
  baseline enforcement, generated protobuf checks, and a pinned live
  Remanence-daemon wire proof.
- Production systemd templates, a Caddy identity-boundary example, deployment
  guide, public design-decision register, security policy, contributor guide,
  issue templates, and code of conduct.
- Exhaustive `create_all` versus Alembic schema-shape regression coverage.

### Changed

- Partial-file restore is an optional integration, so a standalone checkout can
  install and run without the separate `format-anatomy` repository.
- Remanence gRPC clients reject TCP until an mTLS client path exists.
- SQLite writer connections use `synchronous=FULL`.
- Device CSR common names are parsed structurally with `cryptography`.
- The vendored Remanence Layer 5 contract includes BOT-recovery inventory
  events.

## 0.0.1

Initial pre-alpha package version. The project was already used operationally,
but no stable public-interface or release-compatibility promise was made.
