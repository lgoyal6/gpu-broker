# 0010. Helm chart, Kind CI path, and the optional real GPU profile

Status: accepted, 2026-10-06

The chart lives at `infra/helm/gpu-broker`. One image, one Deployment per role.

- Namespaces: the control plane namespace and a separate `gpub-jobs` namespace
  where attempt Jobs run under default-deny NetworkPolicy.
- Service accounts per role. Only the worker agent has Kubernetes API rights:
  create/get/list/watch/delete Jobs and get/list/watch/delete Pods in the jobs
  namespace, and get on Nodes (to notice cordon/drain). The agent reads its
  own Node, but its shared service account can get any Node; this is not
  a per-node authorization boundary. Other roles automount no token.
- Scheduler: 2 replicas, leader elected through Postgres (ADR 0004). A
  PodDisruptionBudget keeps one available.
- Worker agent: a DaemonSet selected by `gpub.dev/pool` node labels, with GPU
  tolerations, node selectors and the `nvidia.com/gpu` extended resource set
  only in the `gpu` profile (`values-gpu.yaml`).
- PostgreSQL is a single StatefulSet enabled only by `postgresql.enabled`
  (local/Kind). Production points `database.existingSecret` at a managed
  database; the chart supports an ExternalSecret reference.
- Migrations run as a plain Job named for each release revision. Services
  wait for schema readiness, avoiding a hook/readiness dependency cycle.
  Rollback does not run down-migrations (see the runbook).
- ServiceMonitor and PrometheusRule are rendered when the Prometheus Operator
  CRDs exist (`monitoring.enabled`). Dashboards ship as a ConfigMap with the
  Grafana sidecar label.

The control-plane CI workflow runs `tests/e2e/kind.sh`: create a 4-node Kind cluster, load the image,
install, submit jobs through the API, and verify rollout, rollback, pod
restart, node drain and scheduler leader replacement. Kind has no GPUs, so the
Kind profile declares virtual GPU capacity on workers and does not request
`nvidia.com/gpu`; the attempt pods are real containers.

A real GPU cluster is behind `values-gpu.yaml`. It has not been run against
real GPU nodes by this repository; `docs/runbooks/deploy-gpu.md` lists the
exact steps and what to verify.
