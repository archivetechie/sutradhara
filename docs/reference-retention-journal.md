# Retention evidence journal

The retention journal is an emit-only flight recorder. Retention release and
staging purge never read it, so an exporter or DR outage raises an alarm without
changing a deletion-gate decision.

## Export and publication

`sutra retention journal export` takes the same non-blocking process-level
`flock` used by the worker singleton. It merges new `verify_receipt` rows before
new `retention_event` rows, with each source ordered by its monotonic
`event_id`. Each JSONL entry has a canonical payload checksum, a global sequence
number, and a SHA-256 previous-entry link. The first link uses the fixed v1
genesis; later files continue from the prior file's head.

Published files live below `SUTRADHARA_RETENTION_JOURNAL_DIR` in UTC-dated
directories. A file is written to a temporary neighbor, fsynced, renamed, and
the directory fsynced before the catalog checkpoint advances. Every file ends
with a checksummed v1 footer containing the envelope and hash-algorithm ids,
global sequence, chain head, and inclusive cursors for both source tables. On
restart the exporter reads published footers, not the checkpoint. A crash after
rename but before checkpoint therefore resumes without omissions or duplicates.

The checkpoint is a singleton optimization row. It is not evidence and is not a
gate input.

## Append-only DR shipping

Production shipping requires
`SUTRADHARA_RETENTION_JOURNAL_DR_BACKEND` to name exactly one catalog backend of
kind `ssh_disk`. Journal segments and per-segment head anchors are published to
dated relative keys below `SUTRADHARA_RETENTION_JOURNAL_DR_PREFIX`. Remote
publication uses a same-filesystem hard-link operation that fails if the final
name already exists. An existing identical object makes a retry idempotent; an
existing different object is an alarm. No mirror or delete operation is used.
Use a dedicated `ssh_disk` backend/root for this journal; an archive-object
backend whose scrub enumerates every file is not a journal destination.

Local append-only destinations are available through the library API for
hermetic tests and DR drills. They use the same names and collision rules as the
SSH destination; they are not a second exporter.

## Checking and corrections

`sutra retention journal check` walks every entry and footer, verifies sequence
continuity and cross-file links, compares local published bytes and the current
head with DR, and compares each copy's current measurement projection with its
latest verification receipt. Any break opens a non-gating journal alarm, prints
the recovery runbook, and exits nonzero.

Published files have no repair verb. Use `sutra retention journal correct
--source verify_receipt|retention_event --event-id ID --reason TEXT --actor WHO`
to append an attributed `correction_recorded` event. Its
`supersedes_source`/`supersedes_event_id` columns identify the immutable target.
Offsite revocation uses the same rule and targets the confirmation event it
supersedes.

## Operations

Every mutating retention/offsite CLI invocation attempts export after its
catalog transaction commits. Export or shipping failure is printed as a warning
and projected on the gap board; it never changes the completed command's gate
result. The supplied
`systemd/sutradhara-retention-journal-export.{service,timer}` runs the same sole
export command hourly.

`sutra retention sitrep` prints standing purge holds and the count/age of
unexported entries. Pending evidence older than
`SUTRADHARA_RETENTION_JOURNAL_STALE_SECONDS` (default two hours) opens the
`retention_journal_alarm/export-stale` condition.

When a check fails, compare the local segments with the dated DR copies and head
anchors, compare both with the source database rows, and use the database WAL
point-in-time state to find the first divergence. Do not rewrite a published
file; preserve it and re-export/cross-check the damaged evidence.
