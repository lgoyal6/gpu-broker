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

For live GB regions, enable `carbon.live=true` and leave `fixtureReplay=false`.
The reconciler polls the public ESO API every five minutes. Check its logs for
`carbon refresh failed`, outbound HTTPS/DNS, and these series:

```promql
increase(gpub_carbon_refresh_total{result="failure"}[30m])
time() - gpub_carbon_refresh_success_timestamp_seconds
```

The success timestamp remains zero until the first response is persisted, so
a provider that never starts is visible as well. The reading-age metric uses
the provider's interval timestamp, not the fetch time. Unsupported regions
need their own provider; never relabel GB data as another region.

In the Kind profile, carbon is replayed history and goes stale 2 h after the
carbon-replay Job ran; rerun it:

```bash
kubectl -n gpub delete job -l app.kubernetes.io/component=carbon-replay
helm upgrade gb infra/helm/gpu-broker -n gpub -f infra/helm/gpu-broker/values-kind.yaml
```

## Recover

Restore the provider. Nothing else: the next tick after a fresh reading uses
carbon again. Do not raise `CarbonMaxAge` to silence the alert; that makes
decisions claim carbon effects from data that no longer describes the grid.
