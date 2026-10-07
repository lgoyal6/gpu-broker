# Simulator evaluation

Reproduce from `controlplane/`:

```bash
go run ./cmd/gpubroker sim --seed 42 --workload all --policy all --out /tmp/gpub-simulator.json
```

Evidence class: simulator. Synthetic arrivals and modelled fleet capacity,
cost and board power use observed GB carbon data. Estimated carbon is not a
power measurement. These results do not establish production savings.

| Workload | Lowest cost, USD | Lowest carbon, USD | Lowest cost, estimated kg | Lowest carbon, estimated kg |
|---|---:|---:|---:|---:|
| steady-mixed | 7,390.35 | 7,957.87 | 168.85 | 181.21 |
| bursty-tenant | 5,584.43 | 6,334.26 | 111.48 | 133.98 |
| deadline-crunch | 6,847.10 | 7,323.37 | 151.96 | 159.22 |
| carbon-flex | 6,981.00 | 7,497.72 | 159.71 | 172.13 |

All jobs finished within the simulation horizon. Both policies missed ten
deadlines in deadline-crunch. The carbon-scored policy does not improve
aggregate estimated emissions on these workloads. Instantaneous placement
scores and whole-workload outcomes differ; policy explanations expose the
inputs, and this evaluation prevents a misleading savings claim.
