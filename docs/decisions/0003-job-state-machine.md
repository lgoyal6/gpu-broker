# 0003. One job state machine, enforced in one domain function

Status: accepted, 2026-10-06

## States

```text
SUBMITTED -> QUEUED -> RESERVED -> DISPATCHED -> RUNNING -> SUCCEEDED
                                      |              |  \-> FAILED
                                      |              +-> CANCEL_REQUESTED -> CANCELLED
                                      +-> EXPIRED
```

The full table lives in `controlplane/internal/domain/job/transitions.go` and is
the only place a state may change (`job.Transition`). Every applied transition
returns a `TransitionRecord` carrying actor, reason, timestamp and correlation
id, and the store persists that record in the same transaction as the new state.
An edge not in the table returns `ErrIllegalTransition`; nothing coerces it.

Edges beyond the prompt's diagram, and why each exists:

| Edge | Why |
|---|---|
| SUBMITTED -> FAILED | admission rejected (quota, budget); the row is kept so "why was I refused" is answerable |
| QUEUED/RESERVED -> CANCELLED | nothing is running, so cancellation is immediate; the reservation is released in the same transaction |
| QUEUED/RESERVED -> EXPIRED | the deadline passed before the job could start |
| RESERVED -> QUEUED | a stale reservation was reclaimed or the job was preempted before dispatch |
| RESERVED -> FAILED | the reclaimed reservation was the job's last attempt |
| DISPATCHED -> QUEUED | the lease expired without an acknowledgement and retries remain; a **new attempt** is created |
| DISPATCHED -> CANCEL_REQUESTED | the worker may already hold the dispatch; only it can confirm the stop |
| RUNNING -> QUEUED | worker, node or spot capacity lost with retries remaining; new attempt |
| RUNNING -> EXPIRED | capacity lost after the deadline had passed, so a retry could not meet it |
| CANCEL_REQUESTED -> SUCCEEDED/FAILED | the job finished before the stop took effect; the worker's terminal report is the truth |

Terminal: SUCCEEDED, FAILED, CANCELLED, EXPIRED. Terminal states have no
outgoing edges; a test asserts the table is total.

## Attempts

A job owns 1..n attempts. An attempt's terminal outcome (SUCCEEDED, FAILED,
LOST, PREEMPTED, CANCELLED, EXPIRED, TIMED_OUT) is written once; the store has a
unique terminal row per attempt and an update predicate `WHERE outcome IS NULL`.
A retry creates attempt n+1. The old attempt is never mutated after it ends.

## Cancellation race

Cancel and dispatch both lock the job row (`SELECT ... FOR UPDATE`) and apply a
transition from the state they read. Whichever commits first wins; the second
sees the new state and acts on it:

- cancel first: job is CANCELLED, the scheduler's reservation transaction finds
  a non-QUEUED job and does nothing;
- reserve/dispatch first: cancel sees DISPATCHED and moves to CANCEL_REQUESTED;
  the worker learns it on its next heartbeat or ack (the ack of a
  CANCEL_REQUESTED attempt is answered with `cancel`).

Cancellation of an already-cancelled or cancel-requested job returns the
current job and records nothing (idempotent).
