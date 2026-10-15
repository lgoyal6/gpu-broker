# 0015. Apply placement eligibility before holds and preemption

Status: accepted, 2026-10-15

Extends ADR 0005. A starvation hold reserves future opportunity only for a
job that is currently eligible apart from occupied capacity. Future
NotBefore, expired deadlines, tenant active quota, project budget, job cap,
and interruptible-last-attempt restrictions prevent the hold. Emergency
continues to ignore NotBefore. Eligible old jobs retain protection from
backfill while a compatible worker drains.

Preemption uses the same placement filter with the capacity that removing
the proposed victim would free. Budget, protected heads and interruptibility
must pass before another tenant's running job may be stopped.

These changes alter decisions on identical snapshots. The affected builtins
are versioned v2: fair-share, deadline-first, lowest-cost, lowest-carbon,
balanced and emergency. FIFO and priority remain v1. Stored v1 decisions and
the source-bound v1 studies remain historical; their numbers do not describe
v2 behavior. Policy registration retains immutable prior versions.

Deterministic controls cover eligible and ineligible holds, future delay and
Emergency behavior, affordable preemption, and budget/cap/interruptibility
refusals. Local simulations remain estimates and require fresh outcome
comparison after a policy change.
