# 0001. Build the multi-pool control plane in Go beside the Python club broker

Status: accepted, 2026-10-06

## Context

`gpu_broker/` is a working single-process Python broker for one club: SQLite,
one daemon (`gpu run`) that holds every credential, fair share plus bounded age,
two currencies, a read-only public status app, seeded demo data in its own
database, pilot mode, and a privacy-bounded scheduler trace (`gpu trace export`
/ `replay`). 808 tests, CI green, used by real club members.

The new product is a multi-pool, multi-tenant broker with separate `api`,
`scheduler`, `worker-agent`, `reconciler` and `status` processes, PostgreSQL as
the source of truth, leader election, Kubernetes deployment, and EcoShift
carbon/cost placement. Its correctness claims are concurrency claims (two
schedulers, racing cancels, redelivered dispatches) and its evidence has to
include scheduler benchmarks in a compiled language.

## Decision

1. The control plane is a Go module at `controlplane/`, with the layout from
   the build workflow: `cmd/`, `internal/domain`, `internal/application`,
   `internal/adapters`, `internal/transport`, `migrations/` (embedded), and
   tests beside the code plus `controlplane/tests/` for e2e and fault tests.
2. The Python broker is **not** rewritten or removed. It stays the club product
   and keeps its CLI (`gpu`), tests, and CI job unchanged.
3. The two are joined at a frozen contract, not at shared code: the Go
   `gpuctl trace replay` reads the Python `gpu-broker.schedule-trace/v1` export
   and replays it through the Go policies, so a real club trace can be used to
   compare policies without either side importing the other.
4. The Go CLI is `gpuctl`, not `gpu`. Two binaries named `gpu` on one `PATH`
   would make every README command ambiguous. `gpuctl` carries the command set
   from the product prompt (`submit`, `status`, `cancel`, `logs`, `pools`,
   `policy explain`, `trace export`, `trace replay`, `doctor`, `demo seed`,
   `worker register`).
5. Server roles ship as one binary, `gpubroker <role>`, and one container image.
   Roles are separate processes and Deployments; one image keeps the Helm chart
   and the Kind e2e from building five images that differ only in `main`.

## Consequences

- Fair share plus bounded age is ported to Go as a pure function and pinned to
  the Python behaviour by a golden test over the same inputs
  (`internal/domain/policy/fairshare_parity_test.go`).
- Two products in one repository means two quickstarts. The README gives the
  club broker first (it is what members use) and the control plane second.
- Nothing in the Go module imports SQLite or the Python schema.

## Revisit when

The club broker needs multi-pool placement itself. At that point the club
deployment should become one tenant of the control plane and `gpu run` should
be retired behind a documented migration, not before.
