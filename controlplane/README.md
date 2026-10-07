# Multi-pool GPU control plane

Submit container jobs against tenant budgets, inspect why a pool was chosen,
and recover attempts after worker failures. EcoShift adds explicit cost and
carbon tradeoffs. PostgreSQL stores jobs, reservations, leases, budgets and a
durable outbox; two scheduler replicas share one fenced leader.

## Offline quickstart

Requires Go 1.27.1. From this directory:

```bash
go build -o /tmp/gpubroker ./cmd/gpubroker
/tmp/gpubroker sim --workload all --policy all --out /tmp/gpub-simulator.json
/tmp/gpubroker forecast-eval -h
```

The bundled carbon dataset has a manifest and checksum. Arrivals, prices and
capacity are modelled; simulator output is not measured savings or usage.

## Local Kubernetes quickstart

Requires Docker, Kind 0.33.0, kubectl, Helm 4.3.0 and Go. From the repository root:

```bash
KEEP=1 OUT=/tmp/gpub-kind-e2e.json bash controlplane/tests/e2e/kind.sh
kubectl -n gpub port-forward svc/gb-api 18080:8080
```

The script creates the dedicated `gpub` cluster, deploys the control plane,
runs real busybox containers, and exercises upgrades and failure recovery.
It deletes this cluster on exit unless `KEEP=1`. Do not run it against a
cluster with work you need to preserve.

In another terminal, build the client and create a local tenant:

```bash
cd controlplane
go build -o /tmp/gpuctl ./cmd/gpuctl
kubectl -n gpub exec deploy/gb-api -c api -- \
  gpubroker admin create-tenant --name local --handle local --budget-usd 10 --metrics-addr=
```

Set `GPUB_TOKEN` to the returned token and `GPUB_API_URL` to
`http://localhost:18080`. Keep the token private. Then:

```bash
/tmp/gpuctl doctor
/tmp/gpuctl pools
/tmp/gpuctl submit --image gpub-e2e-busybox:1 --gpus 1 --max-runtime 1m \
  --runtime kubernetes -- sh -c 'echo hello'
/tmp/gpuctl status JOB_ID
/tmp/gpuctl policy explain JOB_ID
/tmp/gpuctl logs JOB_ID
```

The operator interface is at `/ui`; its data requests require the tenant
token. The public status
service uses a database role allowed to read only two aggregate views.
Use [the real GPU deployment runbook](../docs/runbooks/deploy-gpu.md) for the
optional hardware profile.

## Verification

```bash
go vet ./...
go test -p 2 -race -count=1 ./...
GPUB_REQUIRE_DB=1 GPUB_TEST_DATABASE_URL='postgres://USER:PASSWORD@HOST/DATABASE' \
  go test -p 2 -race -count=1 -timeout=15m ./...
```

Without a database URL, database integration tests skip. CI requires one so
missing configuration cannot produce a passing integration result. The
account needs permission to create and drop isolated test databases. Use a
dedicated local database, never a production account.

To measure fixed synthetic workloads, set `GPUB_BENCH_OUT` and the database
URL, then run `go test -run TestBenchReport -timeout=30m ./internal/bench`.
Results depend on hardware and database placement. Production SLOs are
objectives, not observed service guarantees.

Use Go's built-in allocation and CPU profiling on the named scheduling
workloads before tuning their hot paths:

```bash
go test -run '^$' -bench BenchmarkDecide -benchmem \
  -cpuprofile=/tmp/gpub-cpu.prof -memprofile=/tmp/gpub-memory.prof ./internal/bench
go tool pprof /tmp/gpub-cpu.prof
```

## Current boundaries

- Kind declares virtual GPUs. Real GPU/device-plugin behavior needs hardware validation.
- Object storage uses a shared filesystem. Production replicas need an RWX
  volume; an S3 adapter is not implemented. The Kind RWO profile keeps API
  replicas on one infrastructure node.
- Carbon data is a historical fixture. Production needs a live provider;
  stale or missing data triggers an explicit fallback.
- Forecast evaluation includes a persistence baseline. A forecast must pass
  the trust gate before the scheduler may delay jobs on its predictions.
- Spot interruption is simulated. Real provider interruption integration
  remains to be validated.
- There is no measured production adoption or production SLO history for
  this new control plane.

See [architecture decisions](../docs/decisions/README.md),
[runbooks](../docs/runbooks/README.md), and the
[design](../docs/CONTROL_PLANE.md).
