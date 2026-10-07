# Scheduler split brain or no leader

**Alerts:** GpubSchedulerSplitBrain, GpubNoSchedulerLeader

## How it is supposed to work (ADR 0004)

Each scheduler replica tries `pg_try_advisory_lock`. The holder bumps
`scheduler_leader.epoch` and stamps every reservation transaction with it.
A transaction from an older epoch aborts with `fenced`. Capacity CAS makes
overbooking impossible even if fencing were bypassed.

So "split brain" here means a deposed leader *tried* to act and was fenced
(`gpub_scheduler_fenced_total` increased), or two replicas briefly reported
`gpub_scheduler_is_leader=1` across a handover scrape. Neither can place a
job twice.

## Diagnose

```bash
gbadmin leader                                  # epoch and holder pod
kubectl -n gpub logs -l app.kubernetes.io/component=scheduler --tail=50 | grep -E 'leader|fenced|stepping'
kubectl -n gpub get pods -l app.kubernetes.io/component=scheduler -o wide
```

- Fenced once after a pod restart or network blip: expected; the old leader
  stepped down. No action.
- Fenced repeatedly: two replicas keep stealing leadership. Look for a
  replica whose database connection is being reset (proxy idle timeout below
  the tick interval, PgBouncer in transaction mode). The advisory lock needs
  a session; PgBouncer in transaction mode breaks it. Point schedulers at the
  database directly or at a session-mode pool.
- No leader: every replica failed to connect (see database-outage) or the
  scheduler Deployment is scaled to 0.

## Recover

```bash
kubectl -n gpub rollout restart deploy/gb-scheduler
```

Restarting is safe at any time: the new leader starts from durable state.
Measured takeover after a forced leader deletion is in
`docs/evidence/kind-e2e.md`.
