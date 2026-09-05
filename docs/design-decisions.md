# Design decision register

The private working journal contains exploratory drafts and operational detail
that cannot be published safely. This register records the decisions a public
contributor needs in order to understand the current code without reproducing
that journal. The linked architecture and reference pages remain the detailed
source for each current contract.

| Decision | Why | Consequence | Current reference |
| --- | --- | --- | --- |
| Sutradhara orchestrates; storage backends own bytes | Catalog policy and physical-media mechanics fail in different ways and evolve independently. | Backends expose a narrow port; Remanence remains a separate daemon and source of physical truth. | [Architecture: boundaries](architecture-overview.md) |
| Logical assets are content-addressed | The same bytes arriving through different intakes must converge without erasing provenance. | Intakes and occurrences remain distinct while durable policy attaches to one logical digest. | [Data-model walkthrough](guide-data-model-walkthrough.md) |
| Deletion is evidence-gated | A job success flag is not proof that recoverable copies exist. | Staging purge requires durable copy evidence, fresh backend witnesses, and an append-only retention journal. | [Retention journal](reference-retention-journal.md) |
| Jobs and reconciliation conditions are durable data | Archive work must survive process loss and explain why desired state is not satisfied. | Workers lease database rows; reconcilers observe level-triggered conditions and explicitly park blocked work. | [Architecture: job engine](architecture-overview.md) |
| `WRITTEN` is not `CHECKPOINTED` | Tape transports may acknowledge bytes before a durable filemark/checkpoint boundary. | Sutradhara requeues interrupted writes and does not promote them as durable until Remanence confirms the checkpoint. | [Architecture: Remanence boundary](architecture-overview.md) |
| Restore is verified before publication | A readable source or successful subprocess exit does not establish integrity. | Restore checks stored and plaintext digests, then atomically renames and fsyncs the result. | [Architecture: restore](architecture-overview.md) |
| Operator HTTP identity terminates at a local proxy boundary | Sutradhara consumes Authentik identity headers rather than implementing another login system. | Production HTTP uses a protected Unix socket; the proxy must scrub client-supplied identity headers before authentication. | [Deployment guide](guide-deployment.md) |
| Partial-file restore is optional | The core archive must remain installable without the separately distributed format-analysis implementation. | PFR imports are lazy; unavailable PFR work becomes a visible blocked condition rather than breaking the CLI or worker. | [Quickstart](guide-quickstart.md) |
| Historical `rao-*` identifiers remain stable | Remanence renamed the object format after persisted representation names already existed. | Public prose says REM-OBJECT; database and compatibility identifiers retain their `rao-*` spelling. | [Glossary](reference-glossary.md) |

Changes that reverse one of these decisions should update this register, the
linked reference, migration notes where applicable, and `docs/INDEX.md` in the
same commit.
