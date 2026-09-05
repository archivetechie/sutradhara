# proto/

Vendored Layer 5 gRPC contract from [Remanence](https://github.com/archivetechie/remanence).

- **`layer5.proto`** — copied verbatim from `archivetechie/remanence:proto/layer5.proto`.
  Layer 5 is implemented, but its pre-1.0 wire contract may still evolve; synchronize
  this copy and regenerate the committed stubs whenever the Remanence contract changes.
- **`google/rpc/status.proto`**, **`google/rpc/error_details.proto`** — Remanence's vendored subset of googleapis' rich-error detail types, copied verbatim from `archivetechie/remanence:proto/google/rpc/`. They decode the `grpc-status-details-bin` trailer on `INVALID_ARGUMENT` responses (`PlanBatchRead` names offending targets by index through `google.rpc.BadRequest`).

## Why vendor?

Two reasons:

1. **Reproducible builds.** Sutradhara's generated Python stubs (`src/sutradhara/_proto/*_pb2.py`) are committed; pinning the source `.proto` to a specific copy in this repo means regeneration is deterministic and doesn't depend on the state of the Remanence checkout on whoever's machine.
2. **Loose-coupling discipline.** Sutradhara treats Remanence's Layer 5 as an external system, not an in-tree dependency. The vendor relationship is explicit: a commit in Sutradhara that updates `proto/layer5.proto` is a deliberate API-tracking event.

## Regeneration

When updating `layer5.proto` to a newer Remanence version:

```bash
cp ~/remanence/proto/layer5.proto proto/layer5.proto
cp ~/remanence/proto/google/rpc/*.proto proto/google/rpc/
./scripts/regenerate-proto.sh
git add proto/ src/sutradhara/_proto/
git commit -m "proto: bump layer5.proto to remanence <commit-sha>"
```

The regeneration script writes generated `*_pb2.py` and `*_pb2_grpc.py` into `src/sutradhara/_proto/`. Both are committed (per the gRPC ecosystem convention) so a fresh `pip install -e .[dev]` does not need to run `grpcio-tools` at install time.
