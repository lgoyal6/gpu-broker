# Synthetic control-plane benchmarks

Reproduce from `controlplane/` with a dedicated PostgreSQL test database:

```bash
GPUB_TEST_DATABASE_URL='postgres://USER:PASSWORD@HOST/DATABASE' \
GPUB_BENCH_OUT=/tmp/gpub-bench.json \
  go test -run TestBenchReport -count=1 -timeout=30m ./internal/bench
```

Measured on macOS arm64, Go 1.27.1, GOMAXPROCS=2, Docker PostgreSQL 16.6.
The host was also running a Kind deployment test. These are local synthetic
measurements; they are not isolated hardware benchmarks or production SLOs.

| Workload, balanced policy | Tick p50 | Tick p99 | Job decisions/sec |
|---|---:|---:|---:|
| 50 jobs, 20 workers | 0.326 ms | 0.751 ms | 130,145 |
| 500 jobs, 100 workers | 13.233 ms | 14.522 ms | 37,586 |
| 2,000 jobs, 500 workers | 247.643 ms | 485.906 ms | 7,283 |

Decisions include placements and refusals, not only successful dispatches.
The p99 values come from finite runs, including only 20 iterations for the
largest workload; treat them as sample quantiles.

| Database workload | Result |
|---|---|
| 32 reservers, 4 workers with 2 GPUs each | 523 successful reservations/sec, 77.5% conflicts, transaction p99 58.68 ms |
| 32 reservers, 64 workers with 8 GPUs each | 2,084 successful reservations/sec, no observed conflicts, transaction p99 18.11 ms |
| Tick over 1,000 queued jobs, 100 workers | 322 placements in 2.647 sec |
| 20,000 events, 8 consumers, claim batch 16 | 4,401 enqueues/sec, 10,720 claim/ack events/sec |
| Reclaim 200 expired worker attempts | Reconcile pass 0.783 sec |

The reclaim measurement starts after leases have expired. It excludes the
45-second lease TTL and reconcile interval, so it is not end-to-end outage
detection latency. Real capacity planning requires repeated runs on the
target deployment and measured arrival patterns.

## Allocation profiling and optimization

Go's allocation profile identified per-job candidate scratch buffers and
repeated worker indexes as the main avoidable allocations. Reusing scratch
within one tick and indexing workers once reduced allocations on the same
2,000-job, 500-worker balanced workload from 688.9 MB/tick to 81.4 MB/tick
(88.2%). These are allocated bytes, not peak resident memory.

All 32 simulator workload/policy outcomes remained identical after the
change, and PostgreSQL integration tests passed with the race detector.
Retained candidate and rejection records remain owned by each decision;
only temporary scoring inputs are reused. The latency and database tables
above were measured before this optimization. Timing comparisons need an
isolated rerun with identical CPU limits.

```bash
go test -run '^$' -bench 'BenchmarkDecide/large' -benchtime=3x -benchmem \
  -cpuprofile=/tmp/gpub-cpu.prof -memprofile=/tmp/gpub-memory.prof ./internal/bench
go tool pprof -top -alloc_space /tmp/gpub-memory.prof
```
