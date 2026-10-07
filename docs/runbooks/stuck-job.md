# Stuck job or repaired state

**Alerts:** GpubStuckJobs, GpubStateRepaired

## Diagnose one job

```bash
gpuctl status <job-id>            # transitions with actor and reason
gpuctl policy explain <job-id>    # every decision and why
gpuctl logs <job-id>
```

| Symptom | Meaning | Action |
|---|---|---|
| QUEUED, last decision WAIT `insufficient_free_gpus` | no capacity | capacity-exhaustion runbook |
| QUEUED, WAIT `budget_insufficient` | project budget exhausted | raise budget or cancel |
| QUEUED, WAIT `tenant GPU quota` | tenant at its concurrency quota | expected backpressure |
| QUEUED, `not_before` set | EcoShift delayed it; it starts at that time | none, or resubmit with `--max-delay 0` |
| RESERVED > 2 min | worker never claimed it | reconciler reclaims it; check the agent |
| DISPATCHED/RUNNING with no events | agent lost; lease will expire | stale-worker runbook |
| CANCEL_REQUESTED for long | worker has not confirmed the stop | the lease expiry ends it as CANCELLED |

## Repairs

`gpub_reconciler_repairs_total{kind="ledger"}` or `{kind="capacity"}` > 0
means the reconciler found a finished job still holding budget, or a worker
whose `gpus_reserved` disagreed with its active reservations, and fixed it.
That should never happen; each one is a bug. Capture:

```sql
SELECT j.id, j.state, l.kind, l.amount FROM budget_ledger l JOIN jobs j ON j.id = l.job_id
WHERE j.updated_at > now() - interval '1 day' ORDER BY j.id, l.id;
SELECT id, gpus, gpus_reserved FROM workers;
SELECT worker_id, sum(gpus) FROM reservations WHERE released_at IS NULL GROUP BY 1;
```
