package sim

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/carbon"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/ecoshift"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
)

func dataset(t *testing.T) *carbon.Dataset {
	t.Helper()
	d, err := carbon.Load("../../data/carbon/gb-2026-08-03_2026-09-28.json")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestRunIsDeterministic(t *testing.T) {
	d := dataset(t)
	sc, err := Workload("steady-mixed", d.Regional, d.Start, 42)
	if err != nil {
		t.Fatal(err)
	}
	sc.Horizon /= 7 // one day is enough to prove determinism
	p, _ := policy.Lookup("balanced")
	a, b := Run(sc, p), Run(sc, p)
	a.DecideNanos, b.DecideNanos = nil, nil
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("same seed, different results:\n%+v\n%+v", a, b)
	}
	if a.EvidenceClass != "simulator" {
		t.Fatal("simulator output not labelled")
	}
	if a.Completed == 0 {
		t.Fatal("nothing completed")
	}
}

func TestDemoWorkloadIsBoundedAndDeterministic(t *testing.T) {
	d := dataset(t)
	first, err := DemoWorkload("carbon-flex", d.Regional, d.Start, 42, 32)
	if err != nil {
		t.Fatal(err)
	}
	second, err := DemoWorkload("carbon-flex", d.Regional, d.Start, 42, 32)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same demo seed returned different workloads")
	}
	if len(first.Jobs) == 0 || len(first.Jobs) > 32 || first.Horizon != 24*time.Hour {
		t.Fatalf("demo bounds not applied: jobs=%d horizon=%s", len(first.Jobs), first.Horizon)
	}
	for _, job := range first.Jobs {
		if job.Submit >= first.Horizon {
			t.Fatalf("job %q falls outside the simulated horizon", job.ID)
		}
	}
	if _, err := DemoWorkload("carbon-flex", d.Regional, d.Start, 42, 65); err == nil {
		t.Fatal("accepted a job count above the demo cap")
	}
	if _, err := DemoWorkload("missing", d.Regional, d.Start, 42, 32); err == nil {
		t.Fatal("accepted an unknown workload")
	}
}

// A controlled, matched workload isolates the objectives from forecasting,
// queueing and interruptions. The replay below reports their combined effect.
func TestPoliciesTradeCostAgainstCarbonOnControlledWork(t *testing.T) {
	start := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	sc := Scenario{Name: "controlled-cost-carbon", Start: start, Horizon: 2 * time.Hour, Tick: 5 * time.Minute, Seed: 7, BudgetUSD: 100,
		Pools: []PoolSpec{
			{ID: "cheap", Region: "high", PriceUSD: 1, PUE: 1, Workers: []WorkerSpec{{1, "a100", 80}}},
			{ID: "clean", Region: "low", PriceUSD: 2, PUE: 1, Workers: []WorkerSpec{{1, "a100", 80}}},
		},
		Jobs:   []JobSpec{{ID: "matched-job", Tenant: "tenant", GPUs: 1, Duration: time.Hour, MaxRuntime: time.Hour}},
		Carbon: map[string][]ecoshift.Reading{},
	}
	for region, intensity := range map[string]float64{"high": 200, "low": 20} {
		for at := start; !at.After(start.Add(sc.Horizon)); at = at.Add(sc.Tick) {
			sc.Carbon[region] = append(sc.Carbon[region], ecoshift.Reading{At: at, GramsPerKWh: intensity, Source: "controlled-fixture"})
		}
	}
	cost, _ := policy.Lookup("lowest-cost")
	carb, _ := policy.Lookup("lowest-carbon")
	rc, rk := Run(sc, cost), Run(sc, carb)
	if rc.Completed != 1 || rk.Completed != 1 || rc.Unfinished != 0 || rk.Unfinished != 0 {
		t.Fatalf("controlled policies did not complete identical work: %+v / %+v", rc, rk)
	}
	if !(rc.CostUSD < rk.CostUSD) || !(rk.CarbonKg < rc.CarbonKg) {
		t.Fatalf("controlled cost/carbon tradeoff missing: %+v / %+v", rc, rk)
	}
}

// The unchanged real-series workload is an empirical comparison, not an
// oracle that a forecast-driven policy must beat another policy in aggregate.
func TestRealDataPolicyReplayReportsAccounting(t *testing.T) {
	d := dataset(t)
	sc, err := Workload("carbon-flex", d.Regional, d.Start, 7)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"lowest-cost", "lowest-carbon"} {
		p, _ := policy.Lookup(name)
		r := Run(sc, p)
		if r.Jobs != len(sc.Jobs) || r.Completed != r.Jobs || r.Expired != 0 || r.Unfinished != 0 {
			t.Fatalf("replay lost or skipped work: %+v", r)
		}
		if r.EvidenceClass != "simulator" || r.PolicyVersion != p.Version {
			t.Fatalf("replay attribution: %+v", r)
		}
		for _, value := range []float64{r.CostUSD, r.CarbonKg, r.WaitHoursP95} {
			if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
				t.Fatalf("invalid replay accounting: %+v", r)
			}
		}
		if (name == "lowest-cost" && r.Delays != 0) || (name == "lowest-carbon" && r.Delays == 0) {
			t.Fatalf("fixture did not exercise the intended delay behavior: %+v", r)
		}
		body, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(string(body))
	}
}

// Without carbon data every carbon-weighted decision must record a fallback,
// and no carbon effect may be claimed.
func TestMissingCarbonFallsBackEverywhere(t *testing.T) {
	d := dataset(t)
	sc, _ := Workload("steady-mixed", d.Regional, d.Start, 3)
	sc.Horizon /= 7
	sc.Carbon = nil
	p, _ := policy.Lookup("lowest-carbon")
	r := Run(sc, p)
	if r.FallbackDecisions == 0 || r.Delays != 0 {
		t.Fatalf("fallbacks %d delays %d", r.FallbackDecisions, r.Delays)
	}
}

func TestReplayPythonClubTrace(t *testing.T) {
	// A synthetic file in the Python exporter's exact shape (field names
	// from gpu_broker/schedule_trace.py); real club traces are private.
	tr := map[string]any{"schema": "gpu-broker.schedule-trace/v1", "digest": "x", "jobs": []map[string]any{}}
	jobs := []map[string]any{}
	for i := 0; i < 30; i++ {
		s := float64(i * 600)
		st, fin := s+float64(i%5)*300, s+float64(i%5)*300+3600
		jobs = append(jobs, map[string]any{"job_key": "job-" + string(rune('a'+i%26)), "user_key": []string{"user-a", "user-b", "user-c"}[i%3],
			"gpu_type": "a10g", "requested_hours": 1.0, "state": "COMPLETED", "submitted_offset_seconds": s,
			"started_offset_seconds": st, "finished_offset_seconds": fin})
	}
	jobs = append(jobs, map[string]any{"job_key": "never", "user_key": "user-a", "gpu_type": "a10g", "state": "REFUSED",
		"submitted_offset_seconds": 10.0, "started_offset_seconds": nil, "finished_offset_seconds": nil})
	tr["jobs"] = jobs
	b, _ := json.Marshal(tr)
	path := filepath.Join(t.TempDir(), "trace.json")
	_ = os.WriteFile(path, b, 0o600)
	sc, info, err := LoadTrace(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	if info.Jobs != 30 || info.Skipped != 1 || len(sc.Pools) != 1 {
		t.Fatalf("info %+v pools %d", info, len(sc.Pools))
	}
	fifo, _ := policy.Lookup("fifo")
	fs, _ := policy.Lookup("fair-share")
	a, b2 := Run(sc, fifo), Run(sc, fs)
	if a.Completed != 30 || b2.Completed != 30 {
		t.Fatalf("replay completed %d / %d of 30", a.Completed, b2.Completed)
	}
}
