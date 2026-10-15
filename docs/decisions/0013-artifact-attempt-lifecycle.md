# 0013. Fence artifact publication to the current open attempt

Status: accepted, 2026-10-15

Extends ADR 0011 with an attempt-lifecycle boundary. Object sealing and
resumable chunk semantics remain unchanged.

## Attempt lifecycle

New uploads, appends and completion require the worker's current, open
attempt. The API checks this under the job lock both before object storage
and before publishing its result in PostgreSQL. Reclamation or a terminal
report closes the attempt, so a late worker receives a conflict and cannot
publish artifact metadata over a retry. An identical completion retry for
an already completed artifact still succeeds without changing state.

An expired lease that has not been reclaimed remains renewable; expiry
alone does not close its attempt. Object writes stay outside retried SQL
transactions. Closure during an in-flight write can leave attempt-scoped
bytes or a sealed object with unfinished metadata. These are retained for
operator cleanup, not promoted to a completed artifact. This does not add
automatic object garbage collection or training-checkpoint restoration.

## Verification

`fencing_chaos_test.go` covers reclaimed attempts after a retry, closure
during append and seal, same-digest completion retries after a terminal
report, and expired-but-unreclaimed accepted uploads. The tests use real
PostgreSQL and filesystem storage. No live S3 or physical GPU result is
implied.
