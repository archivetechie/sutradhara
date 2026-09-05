# Security policy

## Supported versions

Sutradhara is pre-1.0. Security fixes are made on the current `main` branch and
documented in the changelog; older snapshots are not maintained as separate
release lines.

## Reporting a vulnerability

Do not disclose a suspected vulnerability in a public issue. Use GitHub's
private vulnerability-reporting flow for this repository. If that flow is not
available, open a non-sensitive issue asking the maintainers to establish a
private channel, without including exploit details, credentials, customer data,
hostnames, or archive paths.

Include the affected commit, configuration, impact, reproduction steps, and any
known mitigation. Never send production secrets or personal data. The
maintainers will acknowledge the report, assess severity, coordinate a fix, and
credit the reporter if desired.

## Important deployment boundary

The operator HTTP API trusts identity headers only behind a proxy that strips
all client-supplied `X-Authentik-*` headers before forward authentication. See
[`docs/guide-deployment.md`](docs/guide-deployment.md). The loopback `--tcp`
mode is for local development and does not defend against another local process
forging those headers.
