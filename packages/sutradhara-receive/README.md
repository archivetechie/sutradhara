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
