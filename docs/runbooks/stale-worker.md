# Stale worker

**Alert:** GpubStaleWorker

A worker that stops heartbeating is marked OFFLINE after 90 s and receives no
placements. Its attempts' leases expire after 45 s and the reconciler marks
them LOST; jobs with retries left are requeued as new attempts.

## Diagnose

```bash
kubectl -n gpub get pods -l app.kubernetes.io/component=agent -o wide
kubectl -n gpub logs <agent-pod> --tail=100
kubectl get node <node> -o wide; kubectl describe node <node> | sed -n '/Conditions/,/Addresses/p'
```

- Agent CrashLoopBackOff: read its last log line. `register failed` usually
  means the bootstrap token in the Secret does not match the pool's token
  (rerun the migrate Job after fixing the Secret).
- Node NotReady: the node is gone; the attempts are already being retried.
- Node cordoned (drain in progress): the agent reports DRAINING, which is not
  stale; this alert fires only if heartbeats actually stopped.

## Recover

Fix or replace the node. A restarted agent re-adopts its node's attempt Jobs
from the Kubernetes API (Jobs labelled with its worker id); attempts whose
leases were already reclaimed are abandoned (the heartbeat tells the agent),
and their pods are deleted.
