# Service level objectives

| SLO | Objective | Indicator (PromQL) | Alert |
|---|---|---|---|
| Submission availability | 99.5% of `POST /v1/jobs` over 30 days are not 5xx | `gpub:submit_availability:ratio_rate5m` | `GpubSubmitAvailabilityBurn` pages at 14.4x burn over 5m (2% of the monthly budget in an hour) |
| Dispatch latency | 99% of reservations claimed by their worker within 60 s | `histogram_quantile(0.99, ... gpub_job_stage_seconds_bucket{stage="reserve_to_dispatch"})` | `GpubDispatchLatencySLO` |
| Job recovery | An attempt on a dead worker is requeued within lease TTL (45 s) + reconcile interval (10 s) = 55 s | `gpub_stale_reclaims_total`, measured in `docs/evidence/benchmarks.md` and the Kind drain test | `GpubStuckJobs` |
| Status freshness | The public snapshot is under 60 s old | `gpub_status_snapshot_age_seconds` | `GpubStatusStale` |

Notes:

- 4xx responses (budget, quota, validation) are the system working and do not
  count against availability. `queue_full` is a 503 and does count: the queue
  cap is capacity planning, and running out of it is an outage for users.
- The recovery objective is a bound by construction, not a measured
  percentile: detection cannot be faster than the lease TTL, and the
  reconciler acts within one interval. Shortening either trades false
  positives (a slow heartbeat is reclaimed) for faster recovery.
- These objectives have not been measured against production traffic. The
  alert thresholds are starting points to tune once a pilot exists.
