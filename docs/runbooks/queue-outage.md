# Queue outage (dispatch backlog)

**Alerts:** GpubQueueBacklog, GpubDispatchLatencySLO

The queue is the `outbox_events` table (ADR 0002). A backlog means
reservations are being made but workers are not claiming their dispatches.

## Diagnose

```bash
# Which workers have undelivered dispatches, and how old?
kubectl -n gpub exec statefulset/gb-postgres -- psql -U gpub -d gpub -c \
  "SELECT topic, count(*), min(created_at), max(attempts) FROM outbox_events
   WHERE delivered_at IS NULL GROUP BY topic ORDER BY 2 DESC"
kubectl -n gpub get pods -l app.kubernetes.io/component=agent -o wide
kubectl -n gpub logs ds/gb-agent --tail=50 | grep -E 'claim|refused|lease_invalid'
```

Common causes:

- **Agent down on that node** (topic `dispatch.<worker>`): the reservation is
  reclaimed after `ClaimTimeout` (2 min) by the reconciler and the job is
  placed elsewhere. Fix the agent; nothing is lost.
- **`lease_invalid` in agent logs**: the agent's dispatch key differs from the
  control plane's (a Secret rotated on one side). Restart the agents after
  the Secret is consistent.
- **High `attempts` on events**: consumers claim and crash before handling.
  The event is redelivered after the visibility timeout (30 s); look for a
  panic in the API log for `/v1/workers/claim`.

## Recover

Fix the cause; the backlog drains by itself. Never delete undelivered
events: the reconciler releases the capacity behind them, and a deleted event
with a live reservation is exactly the state it exists to repair.
