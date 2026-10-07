package policy

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/ecoshift"
)

var t0 = time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)

func mkWorker(id, pool string, gpus, reserved int) domain.Worker {
	return domain.Worker{ID: domain.WorkerID(id), PoolID: domain.PoolID(pool), State: domain.WorkerReady,
		GPUsReserved: reserved,
		Capability:   domain.Capability{GPUModel: "a100", GPUs: gpus, GPUMemGB: 80, Runtimes: []string{"sim", "kubernetes"}}}
}

func mkJob(id, tenant string, gpus int, submitted time.Time) domain.Job {
	return domain.Job{ID: domain.JobID(id), TenantID: domain.TenantID(tenant), ProjectID: domain.ProjectID("p-" + tenant),
		Requirements: domain.Requirements{GPUs: gpus}, MaxRuntime: time.Hour, MaxAttempts: 3,
		State: domain.JobQueued, SubmittedAt: submitted}
}

func twoRegionSnapshot() Snapshot {
	return Snapshot{
		Now: t0,
		Pools: map[domain.PoolID]domain.Pool{
			"north": {ID: "north", Region: "north-scotland", PriceMicroUSDPerGPUHour: 3_000_000, PUE: 1.1},
			"south": {ID: "south", Region: "london", PriceMicroUSDPerGPUHour: 2_000_000, PUE: 1.1},
		},
		Workers: []domain.Worker{mkWorker("w-north", "north", 4, 0), mkWorker("w-south", "south", 4, 0)},
		Carbon: map[string]ecoshift.RegionCarbon{
			"north-scotland": {Current: &ecoshift.Reading{At: t0.Add(-10 * time.Minute), GramsPerKWh: 20, Source: "fixture"}},
			"london":         {Current: &ecoshift.Reading{At: t0.Add(-10 * time.Minute), GramsPerKWh: 250, Source: "fixture"}},
		},
	}
}

func mustPolicy(t testing.TB, name string) Policy {
	t.Helper()
	p, err := Lookup(name)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestBuiltinsValidateAndVersionsAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, n := range BuiltinNames() {
		p := mustPolicy(t, n)
		if seen[p.Version] {
			t.Fatalf("duplicate version %s", p.Version)
		}
		seen[p.Version] = true
	}
	bad := Policy{Name: "x", Version: "x@1", Ordering: OrderFairShare, FairShare: FairShare{WFair: 0.9, WAge: 0.1, AgeMax: time.Hour}}
	if bad.Validate() == nil {
		t.Fatal("a fair-share policy that can starve validated")
	}
	if (Policy{Name: "c", Version: "c@1", Ordering: OrderFIFO, Weights: Weights{Carbon: 1}, CarbonMaxAge: time.Hour}).Validate() == nil {
		t.Fatal("carbon weight without a fallback validated")
	}
}

func TestLowestCostAndLowestCarbonDisagreeForTheRightReason(t *testing.T) {
	s := twoRegionSnapshot()
	s.Jobs = []domain.Job{mkJob("j1", "a", 1, t0)}
	cost := Decide(s, mustPolicy(t, "lowest-cost"))[0]
	carbon := Decide(s, mustPolicy(t, "lowest-carbon"))[0]
	if cost.Action != ActionPlace || cost.ChosenPool != "south" {
		t.Fatalf("lowest-cost chose %s/%s", cost.Action, cost.ChosenPool)
	}
	if carbon.Action != ActionPlace || carbon.ChosenPool != "north" {
		t.Fatalf("lowest-carbon chose %s/%s", carbon.Action, carbon.ChosenPool)
	}
	if carbon.CarbonEffect == nil || *carbon.CarbonEffect <= 0 || carbon.CarbonBasis != ecoshift.EnergyBasis {
		t.Fatalf("carbon decision without a labelled carbon effect: %+v", carbon)
	}
	if carbon.PolicyVersion != "lowest-carbon@v1" || cost.BudgetEffect != 2_000_000 {
		t.Fatalf("decision fields: %+v / %+v", carbon.PolicyVersion, cost.BudgetEffect)
	}
}

func TestStaleCarbonFallsBackExplicitlyAndNeverClaimsCarbon(t *testing.T) {
	s := twoRegionSnapshot()
	rc := s.Carbon["north-scotland"]
	rc.Current = &ecoshift.Reading{At: t0.Add(-5 * time.Hour), GramsPerKWh: 20}
	s.Carbon["north-scotland"] = rc
	s.Jobs = []domain.Job{mkJob("j1", "a", 1, t0)}
	d := Decide(s, mustPolicy(t, "lowest-carbon"))[0]
	if d.ChosenPool != "south" {
		t.Fatalf("stale carbon should fall back to cost and pick the cheaper pool, got %s", d.ChosenPool)
	}
	want := []string{"carbon_stale:north-scotland", "fallback:cost"}
	if !reflect.DeepEqual(d.Fallbacks, want) {
		t.Fatalf("fallbacks %v want %v", d.Fallbacks, want)
	}
	for _, c := range d.Candidates {
		for _, comp := range c.Components {
			if comp.Name == "carbon" && comp.Contribution != 0 {
				t.Fatalf("carbon contributed %v while stale", comp.Contribution)
			}
		}
	}
	delete(s.Carbon, "london")
	d = Decide(s, mustPolicy(t, "lowest-carbon"))[0]
	if len(d.Fallbacks) == 0 {
		t.Fatal("missing carbon source did not record a fallback")
	}
}

func TestDelayOnlyWhenPessimisticForecastBeatsCurrent(t *testing.T) {
	s := twoRegionSnapshot()
	s.Workers = []domain.Worker{mkWorker("w-south", "south", 4, 0)}
	j := mkJob("j1", "a", 1, t0)
	j.MaxDelay = 6 * time.Hour
	s.Jobs = []domain.Job{j}
	rc := s.Carbon["london"]
	rc.ForecastModel = "seasonal-7d@v1"
	rc.Forecast = []ecoshift.ForecastPoint{
		{At: t0.Add(2 * time.Hour), GramsPerKWh: 100, Low: 80, High: 120},
		{At: t0.Add(4 * time.Hour), GramsPerKWh: 90, Low: 60, High: 140},
		{At: t0.Add(9 * time.Hour), GramsPerKWh: 10, Low: 5, High: 15}, // beyond MaxDelay
	}
	s.Carbon["london"] = rc
	p := mustPolicy(t, "lowest-carbon")
	d := Decide(s, p)[0]
	if d.Action != ActionDelay || !d.DelayUntil.Equal(t0.Add(2*time.Hour)) || d.CarbonSaved == nil {
		t.Fatalf("want DELAY to +2h, got %s %v", d.Action, d.DelayUntil)
	}

	j.Deadline = ptr(t0.Add(2*time.Hour + 30*time.Minute))
	s.Jobs = []domain.Job{j}
	if d := Decide(s, p)[0]; d.Action != ActionPlace {
		t.Fatalf("delay past deadline-runtime: %s", d.Action)
	}
	j.Deadline = nil

	rc.Forecast = []ecoshift.ForecastPoint{{At: t0.Add(time.Hour), GramsPerKWh: 150, Low: 50, High: 240}}
	s.Carbon["london"] = rc
	s.Jobs = []domain.Job{j}
	if d := Decide(s, p)[0]; d.Action != ActionPlace {
		t.Fatalf("delayed on a forecast whose band overlaps the current value: %s", d.Action)
	}

	rc.Forecast = []ecoshift.ForecastPoint{{At: t0.Add(time.Hour), GramsPerKWh: 10, Low: 5, High: 15}}
	s.Carbon["london"] = rc
	if d := Decide(s, mustPolicy(t, "emergency"))[0]; d.Action != ActionPlace {
		t.Fatalf("emergency delayed: %s", d.Action)
	}
	j.MaxDelay = 0
	s.Jobs = []domain.Job{j}
	if d := Decide(s, p)[0]; d.Action != ActionPlace {
		t.Fatalf("job without MaxDelay was delayed: %s", d.Action)
	}
}

func TestPreemptionOnlyWhenAllowedAndStrictlyLowerPriority(t *testing.T) {
	s := twoRegionSnapshot()
	s.Workers = []domain.Worker{mkWorker("w1", "south", 4, 4)}
	s.Running = []RunningJob{
		{JobID: "old-low", WorkerID: "w1", GPUs: 2, Priority: 1, Preemptible: true, StartedAt: t0.Add(-3 * time.Hour)},
		{JobID: "new-low", WorkerID: "w1", GPUs: 2, Priority: 1, Preemptible: true, StartedAt: t0.Add(-time.Hour)},
	}
	hi := mkJob("hi", "a", 2, t0)
	hi.Priority = 5
	s.Jobs = []domain.Job{hi}
	d := Decide(s, mustPolicy(t, "balanced"))[0]
	if d.Action != ActionPreempt || d.Victim != "new-low" {
		t.Fatalf("want preempt of the most recently started victim, got %s %s", d.Action, d.Victim)
	}
	if d := Decide(s, mustPolicy(t, "fair-share"))[0]; d.Action != ActionWait {
		t.Fatalf("fair-share does not allow preemption, got %s", d.Action)
	}
	hi.Priority = 1
	s.Jobs = []domain.Job{hi}
	if d := Decide(s, mustPolicy(t, "balanced"))[0]; d.Action != ActionWait {
		t.Fatalf("preempted an equal-priority job: %s", d.Action)
	}
	s.Running[0].Preemptible, s.Running[1].Preemptible = false, false
	hi.Priority = 9
	s.Jobs = []domain.Job{hi}
	if d := Decide(s, mustPolicy(t, "balanced"))[0]; d.Action != ActionWait {
		t.Fatalf("preempted a non-preemptible job: %s", d.Action)
	}
}

func TestExpireQuotaAndBudgetDecisions(t *testing.T) {
	s := twoRegionSnapshot()
	late := mkJob("late", "a", 1, t0.Add(-2*time.Hour))
	late.Deadline = ptr(t0.Add(-time.Minute))
	quota := mkJob("quota", "b", 2, t0)
	poor := mkJob("poor", "c", 1, t0)
	s.Jobs = []domain.Job{late, quota, poor}
	s.TenantActiveGPUs = map[domain.TenantID]int{"b": 3}
	s.TenantMaxGPUs = map[domain.TenantID]int{"b": 4}
	s.ProjectAvailable = map[domain.ProjectID]domain.MicroUSD{"p-c": 1_000_000}
	byID := map[domain.JobID]Decision{}
	for _, d := range Decide(s, mustPolicy(t, "fifo")) {
		byID[d.JobID] = d
	}
	if byID["late"].Action != ActionExpire {
		t.Fatalf("late: %s", byID["late"].Action)
	}
	if byID["quota"].Action != ActionWait {
		t.Fatalf("quota: %s", byID["quota"].Action)
	}
	if d := byID["poor"]; d.Action != ActionWait || len(d.Rejected) != 2 || d.Rejected[0].Reason != domain.RejectBudget {
		t.Fatalf("poor: %+v", d)
	}
}

func randomSnapshot(r *rand.Rand) Snapshot {
	s := Snapshot{Now: t0, Pools: map[domain.PoolID]domain.Pool{}, ProjectAvailable: map[domain.ProjectID]domain.MicroUSD{},
		TenantUsage: map[domain.TenantID]float64{}, Carbon: map[string]ecoshift.RegionCarbon{}}
	regions := []string{"r0", "r1", "r2"}
	for i, rg := range regions {
		id := domain.PoolID(fmt.Sprintf("pool%d", i))
		s.Pools[id] = domain.Pool{ID: id, Region: rg, PriceMicroUSDPerGPUHour: domain.MicroUSD(1+r.IntN(5)) * 1_000_000,
			Interruptible: r.IntN(3) == 0, PUE: 1.2}
		s.Carbon[rg] = ecoshift.RegionCarbon{Current: &ecoshift.Reading{At: t0, GramsPerKWh: float64(r.IntN(400))}}
	}
	models := []string{"a100", "h100", "l4"}
	for i := 0; i < 2+r.IntN(8); i++ {
		g := 1 + r.IntN(8)
		w := domain.Worker{ID: domain.WorkerID(fmt.Sprintf("w%02d", i)), PoolID: domain.PoolID(fmt.Sprintf("pool%d", r.IntN(3))),
			State: domain.WorkerReady, GPUsReserved: r.IntN(g + 1),
			Capability: domain.Capability{GPUModel: models[r.IntN(3)], GPUs: g, GPUMemGB: 24 + r.IntN(60)}}
		if r.IntN(6) == 0 {
			w.State = domain.WorkerDraining
		}
		s.Workers = append(s.Workers, w)
	}
	for i := 0; i < r.IntN(40); i++ {
		tn := fmt.Sprintf("t%d", r.IntN(5))
		j := mkJob(fmt.Sprintf("j%03d", i), tn, 1+r.IntN(4), t0.Add(-time.Duration(r.IntN(20000))*time.Second))
		if r.IntN(3) == 0 {
			j.Requirements.GPUModel = models[r.IntN(3)]
		}
		j.Requirements.MinGPUMemGB = r.IntN(60)
		j.Priority = r.IntN(10)
		j.MaxRuntime = time.Duration(1+r.IntN(10)) * time.Hour
		s.Jobs = append(s.Jobs, j)
		s.ProjectAvailable[j.ProjectID] = domain.MicroUSD(r.IntN(80)) * 1_000_000
		s.TenantUsage[j.TenantID] = float64(r.IntN(100))
	}
	return s
}

// For any snapshot and any policy: no worker booked past its free capacity in
// one tick, no project past its available budget, nothing placed outside a
// worker's capability.
func TestDecideInvariantsOverRandomSnapshots(t *testing.T) {
	r := rand.New(rand.NewPCG(42, 99))
	for run := 0; run < 1500; run++ {
		s := randomSnapshot(r)
		workers := map[domain.WorkerID]domain.Worker{}
		for _, w := range s.Workers {
			workers[w.ID] = w
		}
		jobs := map[domain.JobID]domain.Job{}
		for _, j := range s.Jobs {
			jobs[j.ID] = j
		}
		for _, name := range BuiltinNames() {
			used := map[domain.WorkerID]int{}
			spent := map[domain.ProjectID]domain.MicroUSD{}
			ds := Decide(s, mustPolicy(t, name))
			if len(ds) != len(s.Jobs) {
				t.Fatalf("%s: %d decisions for %d jobs", name, len(ds), len(s.Jobs))
			}
			for _, d := range ds {
				if d.Action != ActionPlace {
					continue
				}
				j, w := jobs[d.JobID], workers[d.ChosenWorker]
				if _, ok := w.Capability.Satisfies(j.Requirements); !ok || w.State != domain.WorkerReady {
					t.Fatalf("%s: placed %s on incapable/unready worker %s", name, j.ID, w.ID)
				}
				used[w.ID] += j.Requirements.GPUs
				spent[j.ProjectID] += d.BudgetEffect
			}
			for id, n := range used {
				if n > workers[id].FreeGPUs() {
					t.Fatalf("%s: worker %s booked %d of %d free", name, id, n, workers[id].FreeGPUs())
				}
			}
			for p, c := range spent {
				if c > s.ProjectAvailable[p] {
					t.Fatalf("%s: project %s spent %v of %v", name, p, c, s.ProjectAvailable[p])
				}
			}
		}
	}
}

func TestDecideIsDeterministicUnderInputPermutation(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for run := 0; run < 200; run++ {
		s := randomSnapshot(r)
		for _, name := range BuiltinNames() {
			p := mustPolicy(t, name)
			want, _ := json.Marshal(Decide(s, p))
			for k := 0; k < 3; k++ {
				s2 := s
				s2.Jobs = append([]domain.Job(nil), s.Jobs...)
				s2.Workers = append([]domain.Worker(nil), s.Workers...)
				r.Shuffle(len(s2.Jobs), func(i, j int) { s2.Jobs[i], s2.Jobs[j] = s2.Jobs[j], s2.Jobs[i] })
				r.Shuffle(len(s2.Workers), func(i, j int) { s2.Workers[i], s2.Workers[j] = s2.Workers[j], s2.Workers[i] })
				got, _ := json.Marshal(Decide(s2, p))
				if stripDigest(t, got) != stripDigest(t, want) {
					t.Fatalf("%s: decisions changed with input order", name)
				}
			}
		}
	}
}

// The digest covers slice order, so permutation comparisons drop it.
func stripDigest(t *testing.T, b []byte) string {
	var ds []map[string]any
	if err := json.Unmarshal(b, &ds); err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		delete(d, "input_digest")
	}
	out, _ := json.Marshal(ds)
	return string(out)
}

func TestFairShareMatchesPythonGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/fairshare_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var g struct {
		Cases []struct {
			Share       float64 `json:"share"`
			WaitMinutes int     `json:"wait_minutes"`
			Score       float64 `json:"score"`
			FairTerm    float64 `json:"fair_term"`
			AgeTerm     float64 `json:"age_term"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	if len(g.Cases) < 50 {
		t.Fatalf("golden has only %d cases", len(g.Cases))
	}
	fs := mustPolicy(t, "fair-share").FairShare
	for _, c := range g.Cases {
		sc, f, a := FairShareScore(fs, c.Share, time.Duration(c.WaitMinutes)*time.Minute)
		if math.Abs(sc-c.Score) > 1e-9 || math.Abs(f-c.FairTerm) > 1e-9 || math.Abs(a-c.AgeTerm) > 1e-9 {
			t.Errorf("share %.2f wait %dm: go (%v,%v,%v) python (%v,%v,%v)", c.Share, c.WaitMinutes, sc, f, a, c.Score, c.FairTerm, c.AgeTerm)
		}
	}
}

func TestFairShareOrdersLightTenantFirst(t *testing.T) {
	s := twoRegionSnapshot()
	s.Workers = []domain.Worker{mkWorker("w1", "south", 1, 0)}
	s.Jobs = []domain.Job{mkJob("heavy", "h", 1, t0.Add(-time.Minute)), mkJob("light", "l", 1, t0)}
	s.TenantUsage = map[domain.TenantID]float64{"h": 90, "l": 10}
	ds := Decide(s, mustPolicy(t, "fair-share"))
	if ds[0].JobID != "light" || ds[0].Action != ActionPlace || ds[1].Action != ActionWait {
		t.Fatalf("fair share: %s %s / %s %s", ds[0].JobID, ds[0].Action, ds[1].JobID, ds[1].Action)
	}
	if ds := Decide(s, mustPolicy(t, "fifo")); ds[0].JobID != "heavy" {
		t.Fatalf("fifo put %s first", ds[0].JobID)
	}
}

func TestBestFitAvoidsFragmentation(t *testing.T) {
	s := twoRegionSnapshot()
	s.Workers = []domain.Worker{mkWorker("big", "south", 8, 0), mkWorker("small", "south", 2, 0)}
	s.Jobs = []domain.Job{mkJob("j", "a", 2, t0)}
	if d := Decide(s, mustPolicy(t, "fifo"))[0]; d.ChosenWorker != "small" {
		t.Fatalf("2-GPU job went to %s, fragmenting the 8-GPU worker", d.ChosenWorker)
	}
}

func ptr[T any](v T) *T { return &v }
