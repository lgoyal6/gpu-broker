# Carbon provider outage

**Alert:** GpubCarbonProviderOutage

When a region's newest carbon reading is older than the policy's
`CarbonMaxAge` (2 h), carbon-weighted policies fall back explicitly
(ADR 0006): carbon weight 0, the declared fallback weight applied, decisions
record `carbon_stale:<region>` and `fallback:<policy>`, carbon effect is
null. Jobs keep being placed; they are simply not carbon-optimised, and no
decision claims they were.

## Diagnose

```promql
gpub_carbon_reading_age_seconds
sum by (reason) (increase(gpub_ecoshift_fallback_total[1h]))
```

This repository ships no live carbon provider (a commercial one needs an API
key; see `docs/evidence/gaps.md`). In the Kind profile, carbon is replayed
history and goes stale 2 h after the carbon-replay Job ran; rerun it:

```bash
kubectl -n gpub delete job -l app.kubernetes.io/component=carbon-replay
helm upgrade gb infra/helm/gpu-broker -n gpub -f infra/helm/gpu-broker/values-kind.yaml
```

## Recover

Restore the provider. Nothing else: the next tick after a fresh reading uses
carbon again. Do not raise `CarbonMaxAge` to silence the alert; that makes
decisions claim carbon effects from data that no longer describes the grid.
