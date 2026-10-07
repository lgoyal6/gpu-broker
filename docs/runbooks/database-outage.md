# Database outage

**Alerts:** GpubDatabaseOutage, GpubSubmitAvailabilityBurn, GpubStatusStale

## What breaks, what does not

PostgreSQL is the source of truth and the queue. While it is down:

- `POST /v1/jobs` and every API call answer `503 unavailable` (retryable).
- The scheduler cannot tick; the reconciler cannot reclaim.
- **Running attempts keep running.** Agents keep their pods; their heartbeats
  fail and they retry. Events are spooled on the agent and delivered later.
- Leases expire in the database's clock, but nothing reclaims them until the
  reconciler is back, and a renewal of an expired-but-unreclaimed lease is
  accepted (ADR 0004). A short outage therefore loses no work.
- The status page serves its last good snapshot with its `generated_at`.

## Diagnose

```bash
kubectl -n gpub get pods -l app.kubernetes.io/component=postgres   # bundled DB only
kubectl -n gpub logs deploy/gb-reconciler --tail=50 | grep -i error
kubectl -n gpub exec deploy/gb-api -c api -- gpubroker admin leader --metrics-addr=
```

Managed database: check the provider console for failover, storage full,
connection limit (`too many clients`), or credential rotation.

## Recover

1. Restore the database (failover, free storage, raise `max_connections`).
2. Nothing in the control plane needs a restart: every role reconnects.
3. Watch `gpub_store_up` return to 1 and `gpub_outbox_undelivered` drain.
4. Expect a burst of `GpubStuckJobs` if the outage exceeded the lease TTL and
   some agents also died: those attempts are LOST and retried, by design.

## Afterwards

If the outage was longer than an hour, check `gpub_reconciler_repairs_total`.
A non-zero ledger or capacity repair means a transaction was cut in a way
the code did not anticipate; file it with the reconciler log lines.
