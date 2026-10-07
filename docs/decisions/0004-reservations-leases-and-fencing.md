# 0004. Capacity reservation by compare-and-swap, leases, and a fenced leader

Status: accepted, 2026-10-06

## Reservation

A reservation is `(job, attempt, worker, gpus)`. Creating one is a single
transaction that:

1. locks the job row and requires state QUEUED;
2. increments `workers.gpus_reserved` with the predicate
   `gpus_reserved + n <= gpus_total AND state = 'READY'` and requires exactly
   one updated row (compare-and-swap on capacity);
3. inserts the reservation; a partial unique index
   `reservations(job_id) WHERE released_at IS NULL` makes a second active
   reservation for the same job a constraint violation, not a convention;
4. transitions the job to RESERVED and writes the decision and outbox dispatch.

Release sets `released_at` with the predicate `released_at IS NULL` and
decrements capacity only if that update touched a row, so a double release
(reconciler and worker racing) returns capacity exactly once.

Two schedulers racing for the last GPU on a worker: both run step 2; Postgres
serialises the row update, the second sees `gpus_reserved + n > gpus_total`,
updates zero rows, and its transaction aborts with `ErrCapacityConflict`. This
holds with leader election switched off, which is what the concurrency test
does.

## Leases

Dispatching creates a lease `(attempt, worker, token, expires_at)`. The worker
renews all of its leases on each heartbeat. The reconciler expires leases with
`expires_at < now()`, marks the attempt LOST, releases its reservation, and
either requeues the job with a new attempt or fails/expires it when retries or
the deadline are exhausted. Lease expiry is the only way capacity is reclaimed
from a silent worker; it never depends on the worker saying goodbye.

## Leader election and fencing

The scheduler is a single logical authority. Each replica tries
`pg_try_advisory_lock` on a dedicated connection. The holder increments
`scheduler_leader.epoch` in the same session and becomes leader with that
epoch. If the process dies or its connection drops, Postgres releases the lock
and another replica acquires it with epoch+1.

An advisory lock alone does not prevent split brain: a paused leader can wake
after losing its connection and still hold an in-flight transaction on a
different pooled connection. So every reservation transaction reads
`scheduler_leader.epoch FOR SHARE` and aborts with `ErrFenced` if it is not the
caller's epoch. A new leader's epoch bump takes `FOR UPDATE` on that row, so an
old leader's transaction either commits before the bump (and was valid) or
fails after it.

Capacity CAS (above) is the safety net; fencing is what keeps two leaders from
producing contradictory *decisions* for the same job.
