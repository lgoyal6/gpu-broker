# Capacity exhaustion and budget runway

**Alerts:** GpubCapacityExhausted, GpubBudgetLow

## Capacity

```bash
gpuctl pools
```

```promql
sum by (pool) (gpub_pool_gpus{state="reserved"}) / sum by (pool) (gpub_pool_gpus{state="total"})
gpub_pool_fragmentation_ratio
sum by (tenant) (gpub_queue_depth)
```

- High allocation and high fragmentation: free GPUs exist but are spread
  across workers so large jobs cannot fit. The starvation guard will hold a
  worker for a large job after 6 h; to act sooner, cordon a worker so it
  drains of small jobs.
- One tenant dominates the queue: quotas (`max_queued_jobs`,
  `max_active_gpus`) are the lever, not budget.
- Genuinely full: add nodes to the GPU pool (label them `gpub.dev/pool=<id>`
  and the DaemonSet schedules an agent).

## Budget

`gpub_project_available_usd` is budget less outstanding holds and settled
spend. Holds are released when attempts end, so a low value with many
running jobs recovers by itself. A persistently low value is a decision for
the project owner: raise the budget, or the scheduler keeps refusing to
place jobs with `budget_insufficient` (it never overspends).
