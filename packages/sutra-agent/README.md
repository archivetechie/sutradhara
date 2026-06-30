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

Operator-console relay mode keeps an outbound mTLS control stream open to
`sutra serve` and reports mounted cards without exposing local paths:

```sh
sutra-agent enroll \
  --server https://system-ui.dvarapala.internal \
  --device-id mac-1 \
  --token TOKEN \
  --ca-cert /path/to/pinned-ca.crt \
  --output-dir ~/.config/sutra-agent

sutra-agent config init \
  --server 100.81.52.26:50051 \
  --client-cert ~/.config/sutra-agent/client.crt \
  --client-key ~/.config/sutra-agent/client.key \
  --ca-cert ~/.config/sutra-agent/ca.crt \
  --device-id mac-1

sutra-agent serve --status
sutra-agent serve
```

The macOS launchd template lives at
`packaging/launchd/com.sutradhara.sutra-agent.plist`.
