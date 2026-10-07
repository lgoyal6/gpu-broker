# 0005. Scheduling decisions are typed records produced by a pure, layered policy

Status: accepted, 2026-10-06

## Decision record

Every tick produces one `Decision` per job it considered:

```text
job id, policy version, action (PLACE | DELAY | WAIT | PREEMPT | EXPIRE),
candidates considered, candidates rejected (worker, reason code, detail),
chosen pool and worker, score components (named, weighted, raw and normalised),
expected start, budget effect (micro-USD held), carbon effect (gCO2e, or null),
fallbacks applied (e.g. carbon_stale), input snapshot digest
```

Decisions for PLACE/DELAY/PREEMPT/EXPIRE are persisted with the job. A WAIT is
persisted only when its reason changes, so a job that waits for an hour does
not write 720 identical rows (the same rule the Python broker adopted in
`8a966bd`).

## Policy pipeline

`policy.Decide(snapshot, policy) []Decision` is pure and deterministic for a
fixed snapshot: no clock, no randomness, no map iteration in output order.
Stages, in the order the product prompt adds them:

1. **Ordering**: FIFO baseline; priority with bounded aging
   (`effective = priority + min(max_boost, wait / aging_step)`); fair share by
   tenant (the Python formula, `w_fair*(1-share) + w_age*min(1, wait/age_max)`,
   ported and parity-tested); deadline-aware (least slack first).
2. **Filtering** with typed rejection reasons: capability, capacity, pool not
   allowed, worker not ready, budget, deadline unreachable.
3. **Scoring** of survivors: capacity fit (best fit; penalise leftover GPUs that
   no queued job can use), cost, carbon, deadline slack. Weights come from the
   policy; components are stored raw and normalised so `gpuctl policy explain`
   can show the arithmetic.
4. **Preemption** only when the policy enables it and the victim is marked
   preemptible with strictly lower priority class.
5. **Delay** only when the job allows it, the deadline still holds after the
   delay, and the forecast improvement exceeds the forecast's own uncertainty.

Capacity consumed by earlier decisions in the same tick is subtracted from the
snapshot, so one tick never double-books.

## Starvation

Bounded aging alone is not enough when a large job needs a whole worker and
small jobs keep backfilling it. A job that has waited longer than
`starvation_guard` becomes a *protected head*: later jobs in the same tick may
not be placed on workers that could host it once drained. The fairness
simulation (`policy/starvation_test.go`) runs a large job against a continuous
small-job stream and asserts it starts within a bound.
