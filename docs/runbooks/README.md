# Control-plane runbooks

Each alert in `infra/helm/gpu-broker/alerts.yaml` links to one of these.
Commands assume the release is `gb` in namespace `gpub`; `gpubroker admin`
runs inside the API pod, which has the database URL:

```bash
alias gbadmin='kubectl -n gpub exec deploy/gb-api -c api -- gpubroker admin --metrics-addr='
```

| Runbook | Alerts |
|---|---|
| [SLOs](slo.md) | definitions behind the burn-rate alerts |
| [Database outage](database-outage.md) | GpubDatabaseOutage, GpubSubmitAvailabilityBurn, GpubStatusStale |
| [Queue outage](queue-outage.md) | GpubQueueBacklog, GpubDispatchLatencySLO |
| [Scheduler split brain / no leader](scheduler-split-brain.md) | GpubSchedulerSplitBrain, GpubNoSchedulerLeader |
| [Stale worker](stale-worker.md) | GpubStaleWorker |
| [Stuck job](stuck-job.md) | GpubStuckJobs, GpubStateRepaired |
| [Provider outage](provider-outage.md) | GpubCarbonProviderOutage |
| [Capacity exhaustion](capacity-exhaustion.md) | GpubCapacityExhausted, GpubBudgetLow |
| [Deploy to real GPU nodes](deploy-gpu.md) | (procedure) |
| [Upgrade and rollback](upgrade-rollback.md) | (procedure) |
