# Architecture decisions

Each record states the decision, what it rules out, and what would make us
revisit it. Records are append-only: a reversed decision gets a new record that
supersedes the old one.

| # | Decision |
|---|---|
| [0001](0001-go-control-plane-beside-python-broker.md) | Build the multi-pool control plane in Go beside the Python club broker |
| [0002](0002-postgres-source-of-truth-and-outbox-queue.md) | PostgreSQL is the source of truth and the durable queue |
| [0003](0003-job-state-machine.md) | One job state machine, enforced in one domain function |
| [0004](0004-reservations-leases-and-fencing.md) | Capacity reservation by compare-and-swap, leases, and a fenced scheduler leader |
| [0005](0005-scheduling-decisions-and-policies.md) | Scheduling decisions are typed records produced by a pure, layered policy |
| [0006](0006-ecoshift-providers-and-fallback.md) | EcoShift providers, staleness, and explicit fallback |
| [0007](0007-worker-agent-isolation-boundary.md) | Worker agent, dispatch authentication, and the isolation boundary |
| [0008](0008-tenancy-auth-and-public-status.md) | Tenancy, authentication, and the read-only public status surface |
| [0009](0009-evidence-classes.md) | Simulator, seeded demo, pilot, and real-user evidence are never mixed |
| [0010](0010-kubernetes-deployment.md) | Helm chart, Kind CI path, and the optional real GPU profile |
