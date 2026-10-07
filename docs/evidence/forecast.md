# Carbon forecast evaluation

Reproduce from `controlplane/`:

```bash
go run ./cmd/gpubroker forecast-eval --out /tmp/gpub-forecast.json
```

The bundled observed GB national dataset covers 2026-08-03 through
2026-09-28. Its SHA256 is
`b1dadabff81afe4968e54a655adb2b0b51835588fe9d0674f79ab0973af9a7b1`.
Evaluation uses the final week, hourly forecast anchors, and a 24-hour
horizon. Horizons overlap, so the 6,912 evaluated predictions are not
independent samples.

| Model | MAE (g/kWh) | P90 absolute error (g/kWh) |
|---|---:|---:|
| Persistence | 43.28 | 91.00 |
| Same time yesterday | 37.06 | 71.00 |
| Seasonal seven-day mean | 45.11 | 101.14 |
| ESO published forecast | 11.53 | 24.00 |

The seasonal model does not beat persistence on this dataset. Its presence
is not evidence of better scheduling. Region-level trust gates reject
prediction-based delay when their baseline comparison fails. A live provider
and production monitoring remain necessary before operational use.
