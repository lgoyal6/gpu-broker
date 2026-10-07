# Kubernetes recovery verification

Local Kind 0.33.0, Kubernetes 1.37.0, four nodes, PostgreSQL 16.6.
Real busybox containers ran against declared virtual GPU capacity. This
does not validate GPU hardware, a cloud provider, or production adoption.

Reproduce from the repository root:

```bash
OUT=/tmp/gpub-kind-e2e.json bash controlplane/tests/e2e/kind.sh
```

| Behavior | Observed result |
|---|---|
| Install and execute a real container job | Succeeded; captured its expected log line and checked the pod isolation settings |
| Upgrade and rollback under traffic | 64 in-cluster Service requests, zero failed requests; upgrade took 7.197 sec |
| API and agent pod restart during a job | The job succeeded on attempt 1, without a retry |
| Drain a virtual GPU node | First attempt recorded LOST; attempt 2 succeeded on remaining capacity |
| Forced scheduler leader deletion | Leader epoch advanced from 3 to 4; takeover observed in 0.808 sec; another job succeeded |
| Public status writes and shape | POST returned 405; response had only the allowlisted aggregate fields |

Drain-to-success was 79.070 seconds, including the retry's 40-second job.
This is not lease-expiration latency. The rollout test probes the Service
from inside the cluster, avoiding pod-pinned port-forward artifacts.

The chart keeps HTTP serving for five seconds before SIGTERM to allow
terminating endpoints to leave Service routing, then drains requests.
Earlier tests exposed both a stale local smoke-server collision and real
connection failures during immediate termination. Isolated CLI ports,
Service probes and the pre-stop handler addressed those distinct causes.

This was one local run. Hosted CI and real GPU deployment remain unverified.
The allocation-only scheduler optimization made afterward is covered by
database race tests and unchanged simulator outcomes; this deployment
measurement used the preceding scheduler build.
