# sutra-agent

Operator-facing edge agent for Sutradhara receive workflows.

The agent wraps `sutradhara-receive` without reimplementing the filesystem
contract. It keeps a local JSON config and receive ledger, starts or resumes
receives, sweeps stale partial receives, and reports source release as safe only
after the server writes `intake.verified.json`.

```sh
sutra-agent config init \
  --landing /replica/landing \
  --operator operator \
  --source-kind card \
  --artifactclass camera-original

sutra-agent receive /path/to/source --confirm-timeout 300
sutra-agent status <intake-id>
```

