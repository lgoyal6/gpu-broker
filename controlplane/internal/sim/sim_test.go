package sim

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/carbon"
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

// The policies must disagree for the reasons they claim to: lowest-carbon
// emits less than lowest-cost on the carbon-flex workload, and lowest-cost
// spends less than lowest-carbon.
func TestPoliciesTradeCostAgainstCarbonOnRealData(t *testing.T) {
	d := dataset(t)
	sc, err := Workload("carbon-flex", d.Regional, d.Start, 7)
	if err != nil {
		t.Fatal(err)
	}
	cost, _ := policy.Lookup("lowest-cost")
	carb, _ := policy.Lookup("lowest-carbon")
	rc, rk := Run(sc, cost), Run(sc, carb)
	t.Logf("lowest-cost: $%.0f, %.1f kg; lowest-carbon: $%.0f, %.1f kg, %d delays", rc.CostUSD, rc.CarbonKg, rk.CostUSD, rk.CarbonKg, rk.Delays)
	if !(rk.CarbonKg < rc.CarbonKg) {
		t.Fatalf("lowest-carbon emitted %.1f kg >= lowest-cost %.1f kg", rk.CarbonKg, rc.CarbonKg)
	}
	if !(rc.CostUSD < rk.CostUSD) {
		t.Fatalf("lowest-cost spent $%.0f >= lowest-carbon $%.0f", rc.CostUSD, rk.CostUSD)
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
