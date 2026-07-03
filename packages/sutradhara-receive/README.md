# sutradhara-receive

Dependency-light receive filesystem contract for Sutradhara.

This package owns the first-contact receive core shared by edge clients and the
server intake verifier: canonical member paths, hash-on-read copy, BagIt tag
files, resumable landing directories, destination verification, package tar
normalization, and server confirmation markers.

New receive bags include a `Receive-Package: sutradhara-receive/<version>` label
in `bag-info.txt`. Server intake validates that marker with the same shared code,
so unsupported edge/server receive-contract drift is quarantined instead of being
accepted silently.

It deliberately has no catalog, database, backend, cloud, or Remanence runtime
dependencies. The main `sutradhara` package imports this package for server-side
intake validation and keeps compatibility shims for historical
`sutradhara.receive` imports.

## Public API

The Rust crate exposes the byte-sensitive receive encodings as `pub fn`s so the
server, PyO3 wheel, and Rust agent use one implementation:

- `manifest_digest` for gRPC `CommitIntake` manifest entries. It preserves the
  server's Python-compatible spaced JSON encoding.
- `source_plan_digest` and `payload_plan_digest` for compact source-plan
  metadata.
- `build_package_index` and `package_index_package` for package-directory
  `package-index.json` construction alongside `build_package_tar`.
- `derive_card_id` for `volume:<id>` card identifiers from a real volume
  UUID/serial or the stable fallback hash.
- `canonical_device_rel_path` for forward-slash device-relative wire paths.

The crate also exports the shared constants and primitives used by the agent:
`CANONICALIZATION_VERSION`, `PACKAGE_PROFILE_VERSION`,
`canonicalize_manifest_path`, and `PACKAGE_GLOBS`.

## CLI

The package installs a standalone edge command:

```sh
sutra-receive /path/to/source \
  --landing /replica/landing \
  --source-kind card \
  --operator operator \
  --artifactclass camera-original \
  --json
```

Resume is explicit and source-less:

```sh
sutra-receive --resume <intake-id> --landing /replica/landing --source-kind card
```

Stale partial receives can be swept with:

```sh
sutra-receive sweep --landing /replica/landing --older-than-hours 24 --json
```

## Distribution

Release CI lives in `.github/workflows/receive-release.yml`. A tag named
`sutradhara-receive-v<version>` builds the manylinux wheel, Linux glibc and musl
CLI archives, a Windows MSVC CLI zip, a macOS universal2 CLI zip, and
`SHA256SUMS`. The tag version must match the Cargo and Python package version.

macOS tag releases are signed and notarized with GitHub Actions secrets. The
workflow expects the Developer ID certificate and notarization credentials to be
configured outside the repository; tag releases fail closed if those secrets are
missing. Branch and pull-request builds still compile the macOS universal2 binary
but package it unsigned.
