# 0006. EcoShift providers, staleness, and explicit fallback

Status: accepted, 2026-10-06

## Providers

`ecoshift.CarbonProvider` returns intensity (gCO2e/kWh) for a region at a time,
with the observation time, source name, and a forecast with an uncertainty
band. `ecoshift.CostProvider` returns USD per GPU-hour for a pool at a time.
Capacity comes from the store (registered workers), not from a provider.

Implemented providers:

- `fixture`: a versioned dataset file (`controlplane/data/carbon/*.json`) with a
  manifest carrying source URL, retrieval time, date range and SHA-256. The
  committed dataset comes from the GB National Grid ESO Carbon Intensity API
  (keyless public API), retrieved by `scripts/fetch_carbon_fixture.py`:
  - national half-hourly **actual** intensity (observed) and ESO's own
    forecast, used for forecast evaluation;
  - regional half-hourly intensity, which ESO publishes as a **modelled
    estimate**, not a metered observation. Placement across regions uses it
    and decisions label it `source: eso-regional-estimate`.
- `static-price`: per-pool prices from the pool definition (on-demand and spot).
- `eso-live`: the same public provider's current `/regional` response, polled
  by the reconciler independently of lease recovery. Regions 1, 8, and 13 map
  to the existing GB pool region names. The interval start is the reading
  timestamp; fetch time never makes an old reading fresh. Regional values
  remain labelled `eso-regional-estimate`, including legitimate zero values.
  The adapter rejects missing, duplicate, negative, stale, future, or malformed
  readings before storing any of the response. Fetches have a ten-second
  deadline and a bounded response size. Repeated polls upsert the same interval.
  Failures preserve prior readings, which age into the existing fallback.
  Enable with `carbon.live=true` in Helm or `reconciler --carbon-provider=eso`.

A live commercial provider (WattTime, Electricity Maps) needs an API key this
repository does not have. The interface and the staleness rules below are what
such an adapter plugs into. The public adapter covers GB only; mapping other
cloud regions to GB would misrepresent their emissions and is not allowed.

## Energy model

Energy = board power (TDP, from the vendor spec sheet per GPU model) x GPUs x
hours x PUE (pool attribute, default 1.0). This is an upper bound, not a
measurement; decisions label the carbon effect `estimate:tdp`.

## Staleness and fallback

A carbon reading older than `max_age` (policy field, default 2h) or missing is
**stale**. The policy then applies its declared fallback
(`fallback: cost` or `fallback: deadline`), sets carbon weight to zero, records
`fallbacks: ["carbon_stale:<region>"]` on the decision, sets the carbon effect
to null, and increments `gpub_ecoshift_fallback_total{reason="carbon_stale"}`.
The decision never says "lowest carbon" when it had no carbon data.

## Policies

`lowest-cost`, `lowest-carbon`, `deadline-first`, `balanced` (weighted),
`emergency` (ignores delay, earliest start wins). Each has a version string
stored on every decision it makes.

## Forecasting

Added after the policies work on observed data. `seasonal-7d@v1` predicts each
half-hour as the mean of the same half-hour over the previous 7 days. It is
evaluated on the national observed series, on a held-out final week, against
naive persistence (last observed value), same-time-yesterday, and ESO's
published forecast, with MAE and MAPE reported in
`docs/evidence/forecast.md`. The policy uses a forecast only if the evaluation
shows it beats persistence on that region; otherwise delay decisions are
disabled for the region and say so.
