# Policy v2 replay and losing axes

The eligibility corrections in ADR 0015 change scheduling behavior. Historical
v1 measurements remain bound to their original source and do not describe v2.
An intermediate full-suite run exposed an invalid aggregate-winner assertion:
the carbon-focused policy did not reduce estimated emissions on this workload.
That failing test, its exact source, and its raw output are retained in release
verification evidence. No weights, seeds or workload parameters were tuned to
make this comparison favorable.

**Carbon-reduction performance acceptance: FAIL for this workload and seed.**
The controlled correctness test below does not change that losing result.

The unchanged `carbon-flex` workload uses seed 7, a seven-day horizon, five-minute
ticks, modeled arrivals and spot interruptions, and 698 jobs. The recorded carbon
fixture is `controlplane/data/carbon/gb-2026-08-03_2026-09-28.json`, SHA-256
`3692c2119f480b788ed806cfd43b0f9ddeee5bc468f18062476c329912c30f82`.

| Observed simulator outcome | lowest-cost v2 | lowest-carbon v2 |
|---|---:|---:|
| Completed jobs | 698 | 698 |
| Estimated cost, rounded USD | 7,269 | 7,668 |
| TDP-based estimated carbon, rounded kg | 174.1 | 174.8 |
| Modeled spot interruptions | 13 | 30 |
| Delay decisions | 0 | 48 |
| Queue wait p95, hours | 0.51 | 7.60 |

This replay has higher estimated carbon, cost and tail wait under lowest-carbon.
The policies consume the simulator's interruption stream differently, so the
realized failure schedules are not matched per job. One seed supplies no
statistical uncertainty estimate. These are modeled outcomes, without measured
GPU energy, real provider charges, or a carbon-savings claim.

The tests separate two questions. A controlled scenario with identical completed
work, fixed intensities and no interruptions verifies the intended cost/carbon
tradeoff end to end. The real-series replay verifies complete work accounting,
finite observations, evidence labels and delay behavior, and prints both full
results without requiring a winner:

```bash
cd controlplane
GOMAXPROCS=2 go test -p 2 -race -count=1 -v ./internal/sim \
  -run '^Test(PoliciesTradeCostAgainstCarbonOnControlledWork|RealDataPolicyReplayReportsAccounting)$'
```

The recorded local run uses macOS arm64, Go 1.27.2 and GOMAXPROCS=2. A wider
comparative study needs a frozen failure model, repeated seeds and matched
useful work. This replay does not replace that study.
