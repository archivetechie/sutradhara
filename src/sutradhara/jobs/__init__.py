"""Job engine — submit, lease, run, and track work.

The SQLite catalog is the durable queue. The single-node worker adds an
in-memory counted lease scheduler for `cpu`, `io`, `tape_drive`, and `gpu`,
uses guarded `PENDING -> RUNNING` claims, enforces prerequisite DAGs, resets
orphaned `RUNNING` jobs on startup, and applies retry backoff through
`Job.not_before`.

Cancellation is still only represented as a status value; there is no cancel
API yet.
"""
