# Deployment guide

This guide turns a checkout into the four long-running Sutradhara duties: the
combined HTTP/gRPC server, job worker, intake watcher, and scheduled reconciler
passes. The retention-journal timer is shipped beside them. Adapt paths, users,
network addresses, and storage policy to the site; do not copy example identity
or network values unchanged.

## Install and migrate

Install a pinned checkout in a path readable by the service account. The unit
templates use `/opt/sutradhara` and the `replica` user:

```sh
cd /opt/sutradhara
uv sync --locked
sudo install -d -o replica -g replica -m 0700 /var/lib/sutradhara
sudo install -d -o replica -g replica -m 0750 /var/cache/sutradhara
sudo -u replica env \
  SUTRADHARA_DB_URL=sqlite:////var/lib/sutradhara/catalog.db \
  /opt/sutradhara/.venv/bin/alembic upgrade head
```

Create `/etc/sutradhara/sutradhara.env` as a root-owned `0640` file. At minimum:

```ini
SUTRADHARA_DB_URL=sqlite:////var/lib/sutradhara/catalog.db
REM_BIN=/opt/remanence/bin/rem
SUTRA_RECEIVE_LANDING_ROOT=/replica/landing
SUTRADHARA_KEY_REGISTRY_DIR=/var/lib/sutradhara/key-registry
SUTRADHARA_CACHE_ROOT=/var/cache/sutradhara
SUTRADHARA_GRPC_BIND=127.0.0.1
```

Remanence daemon endpoints are persisted per backend, not read from an
environment variable. Register each library after the migration, using its
actual Remanence library UUID:

```sh
sudo -u replica env \
  SUTRADHARA_DB_URL=sqlite:////var/lib/sutradhara/catalog.db \
  /opt/sutradhara/.venv/bin/sutra backends add mainlib \
    --kind rem_tape \
    --config daemon_endpoint=unix:/var/lib/remanence/rem.sock \
    --library-uuid '<library-uuid>'
```

The adapter deliberately accepts only `unix:` endpoints. Remanence's TCP
listener requires mTLS, while Sutradhara does not yet have a client-key
configuration for that boundary. Put the two daemons on the same host or use an
explicitly secured local socket bridge; never substitute plaintext TCP.

Remanence owns tape-device permissions. On a hardware host, grant its documented
service user/group access to the changer and drives. If the chosen Remanence CLI
deployment uses Linux `CAP_SYS_RAWIO`, apply `setcap cap_sys_rawio+ep` to the
installed binary after every replacement and verify it with `getcap`; do not run
Sutradhara as root to compensate.

## Recovery and hot keys

Create a recovery pair on an offline operator machine and keep the private half
there:

```sh
sutra admin keys mint-recovery \
  --public-key /secure-transfer/recovery.remr \
  --private-key /offline-escrow/recovery.remp
```

Copy only the public `.remr` file to the serving host, then import it:

```sh
sudo -u replica /opt/sutradhara/.venv/bin/sutra admin keys import-public \
  --public-key /secure-transfer/recovery.remr
```

The serving host creates and retains its hot epochs under
`SUTRADHARA_KEY_REGISTRY_DIR`; the directory must be `0700` and owned by the
service user. Never put the recovery private key in that registry.

To enable downloadable workstation-enrollment bundles, copy
[`docs/examples/agent-bundle.dev.json`](examples/agent-bundle.dev.json) to a
root-owned site configuration, replace every development address and CA path,
make the referenced enrollment CA readable by the service user, and add
`SUTRA_AGENT_BUNDLE_CONFIG=/etc/sutradhara/agent-bundle.json` to the environment
file. Without it the server remains usable, but `/api/enroll/bundle` returns
`bundle_not_configured`.

## Services

Templates live in [`systemd/`](../systemd/). Review every `User`, `Group`, path,
and bind before installing them:

```sh
sudo cp systemd/sutradhara-*.service systemd/sutradhara-*.timer /etc/systemd/system/
sudo cp systemd/sutradhara-tmpfiles.conf /etc/tmpfiles.d/sutradhara.conf
sudo systemd-tmpfiles --create /etc/tmpfiles.d/sutradhara.conf
sudo systemctl daemon-reload
sudo systemctl enable --now \
  sutradhara-serve.service \
  sutradhara-worker.service \
  sutradhara-intake-watch.service \
  sutradhara-reconcile@bundle_copy.timer \
  sutradhara-reconcile@copy.timer \
  sutradhara-reconcile@derivation.timer \
  sutradhara-reconcile@hdcache.timer \
  sutradhara-reconcile@log_pipeline.timer \
  sutradhara-reconcile@restore_open.timer \
  sutradhara-retention-journal-export.timer
```

`sutra serve` combines the browser HTTP API and device gRPC relay because they
share process-local device state. The worker is a singleton per database. The
intake watcher crosses verified landing bags into the catalog. Reconcile timers
assert desired state periodically; they are safe to rerun. On first start, the
server creates its CA and server key below the configured PKI directory. Back up
that directory as sensitive state and restrict it to the service account.

## Reverse proxy and identity boundary

The HTTP API trusts `X-Authentik-*` identity headers. That trust is safe only
when clients cannot reach the Unix socket and the proxy deletes every
client-supplied header in that namespace before forward authentication. The
order is part of the security boundary:

1. Remove `X-Authentik-*` from the inbound request.
2. Run Authentik forward authentication.
3. Copy only the approved identity headers from Authentik.
4. Proxy `/api/*` over `/run/sutradhara/api.sock` and preserve the original
   `Host` header for the API's same-origin check.

[`deploy/Caddyfile.example`](../deploy/Caddyfile.example) is a concrete Caddy
fragment with that order and the enrollment-token exception. The Caddy process
must have execute permission on `/run/sutradhara` and group access to the `0660`
socket. Do not expose `sutra serve-api --tcp`: loopback TCP is a development mode
where any local process can forge an operator identity.

## Verify

After startup, check the migration, processes, socket ownership, and denial path:

```sh
sudo -u replica env \
  SUTRADHARA_DB_URL=sqlite:////var/lib/sutradhara/catalog.db \
  /opt/sutradhara/.venv/bin/alembic current
systemctl --no-pager --full status sutradhara-serve sutradhara-worker sutradhara-intake-watch
stat /run/sutradhara/api.sock
curl --unix-socket /run/sutradhara/api.sock http://localhost/api/session
```

The unauthenticated socket request must be denied. Complete the proxy check with
a real authenticated browser session and a forged-header negative request. Run
`sutra admin doctor`, inspect blocked jobs/reconciler conditions, and verify that
retention-journal exports are advancing before enabling automated retention.
