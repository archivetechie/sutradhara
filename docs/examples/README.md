# Sutradhara deployment examples

## Agent bundle dev config

`agent-bundle.dev.json` matches the Windows laptop tunnel topology:

- `https://127.0.0.1:50051` is the SSH local forward to the Sutradhara gRPC port.
- `https://system-ui.dvarapala.internal/api/enroll/csr` is the dvarapala-routed
  enrollment endpoint.
- `/home/user/dvarapala/enroll-ca.crt` is the exported Caddy internal root CA for
  `system-ui.dvarapala.internal`.

Export the dvarapala CA before starting `sutra serve`:

```sh
(cd /home/user/dvarapala && docker compose exec -T caddy cat /data/caddy/pki/authorities/local/root.crt > enroll-ca.crt)
export SUTRA_AGENT_BUNDLE_CONFIG=/home/user/sutradhara/docs/examples/agent-bundle.dev.json
```
