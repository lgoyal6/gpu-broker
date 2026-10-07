# 0002. PostgreSQL is the source of truth and the durable queue

Status: accepted, 2026-10-06

## Decision

PostgreSQL 16 holds jobs, attempts, reservations, leases, quotas, budget ledger,
scheduling policies, carbon and cost snapshots, decisions, audit events,
idempotency keys and the outbox. Migrations are plain SQL files embedded in the
binary and applied by `gpubroker migrate` under an advisory lock, recorded in
`schema_migrations` with a checksum so an edited migration fails loudly.

The durable queue is the `outbox_events` table, consumed with
`FOR UPDATE SKIP LOCKED` and a visibility timeout (`locked_until`):

- A producer writes the event in the **same transaction** as the state change
  that caused it. There is no window where the job is RESERVED but no dispatch
  exists, or a dispatch exists for a job that rolled back.
- A consumer claims a batch, which sets `locked_until = now() + visibility`.
  If it dies, the claim lapses and the event is redelivered. Delivery is
  therefore at-least-once, and every consumer is idempotent by key
  (dispatch acknowledgement is unique per attempt; worker events are unique
  per `(attempt_id, seq)`).
- `delivered_at` is set by an explicit ack. Delivered rows are pruned after the
  retention window; undelivered rows are never pruned.

## Why not NATS, Kafka or Redis

A separate broker would need its own durability story, its own HA, and a
two-phase write between it and Postgres (or an outbox anyway). The dispatch
rate of a GPU broker is bounded by GPU count, tens per second at most, which
`SKIP LOCKED` serves with orders of magnitude to spare (see
`docs/evidence/benchmarks.md`, queue throughput). The adapter is behind the
`application.Queue` port, so moving the transport later changes one package.

## Simulator

The deterministic simulator (`gpubroker sim`) drives the pure policy
(`policy.Decide`) with a discrete-event model of workers, arrivals, durations,
failures, spot interruptions and carbon. It does not reimplement the store.
There is one store implementation, Postgres, and every application use case
is tested against a real Postgres (a container locally, a service container in
CI). A second in-memory store would be a second set of transaction semantics
to keep honest; the simulator's job is policy evaluation, not persistence.

## Revisit when

Sustained dispatch rate exceeds ~500/s, or a consumer outside the cluster needs
the event stream.
