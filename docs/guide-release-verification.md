# Release verification

Receive releases wait for the complete repository CI workflow and each platform
builder. Tagged publication additionally requires an annotated, GitHub-verified
signed tag on an ancestor of `main`, with successful main-push CI for that exact
commit. Preview builds carry an explicit ineligible publication record.

Each builder records its source commit, workflow attempt, actual Rust compiler,
Cargo version, runner identity, lockfile hashes, and artifact hashes. Assembly
requires all five builders and rejects missing or changed payloads, stale source
identities, different workflow attempts, and dependency-lock mismatches.

Release files include `release-manifest.json`, individual `build-*.json` records,
`eligibility.json`, and `SHA256SUMS`. Download the complete release set and run
`sha256sum -c SHA256SUMS` before using a binary. Inspect the manifest's source and
CI links to identify what was checked. GitHub build attestations bind the release
payloads and manifest to the publishing workflow; verify them against this
repository using GitHub's attestation tooling.

Actions use immutable commit references. Rust, uv, maturin, and the manylinux
container are pinned. Runner images and operating-system package repositories
remain externally maintained inputs; the records do not promise bit-identical
rebuilds. A new release workflow is unverified until its remote jobs run.

The helper's local regression suite is:

```sh
python -m unittest discover -s scripts -p test_release_evidence.py
```

Checks of this helper are separate from executing every platform build, signing,
notarization, attestation, or publication. Specification drafts and external
deposits have their own acceptance process and are not published by this workflow.
