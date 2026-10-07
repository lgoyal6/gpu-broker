# Upgrade and rollback

```bash
helm upgrade gb infra/helm/gpu-broker -n gpub -f <values> --set image.digest=sha256:... --wait
helm history gb -n gpub
helm rollback gb <revision> -n gpub --wait
```

- Each revision runs `gb-migrate-<revision>`. Roles wait for the schema they
  were built against before serving (`/readyz` stays 503 until then), so new
  pods never serve against an old schema and the RollingUpdate
  (`maxUnavailable: 0`) keeps old pods serving meanwhile.
- Migrations are additive (new tables, columns with defaults, new views).
  A rollback runs the older binary against the newer schema; it does not
  migrate down. A migration that is not additive must ship in two releases:
  add, migrate readers, then remove.
- Editing an applied migration file fails `gpubroker migrate` with a
  checksum error. Add a new migration instead.
- Secrets survive upgrades (`lookup` + `resource-policy: keep`); a rollback
  never rotates the dispatch key.
- API and status pods use Kubernetes' five-second pre-stop sleep handler
  (enabled by default in Kubernetes 1.30 or newer) before SIGTERM. This leaves HTTP serving while
  terminating endpoints leave Service routing. The process then drains
  requests for up to 20 seconds within the 30-second termination grace.

The Kind e2e (`controlplane/tests/e2e/kind.sh`) performs an upgrade and
rollback while an in-cluster client probes the API Service. It fails on
any non-200 response or connection error. A pod-pinned port-forward cannot
measure Service availability; separate isolated local ports serve the CLI.
