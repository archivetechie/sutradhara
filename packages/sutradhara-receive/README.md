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
