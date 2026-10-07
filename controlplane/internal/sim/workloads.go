package sim

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/ecoshift"
)

// Fleet is the modelled fleet every named workload runs on. Regions map to
// the GB dataset's regions so carbon is real ESO data; prices are list-price
// shaped, not quotes.
func Fleet() []PoolSpec {
	return []PoolSpec{
		{ID: "scot-spot", Region: "gb-north-scotland", PriceUSD: 1.10, Interruptible: true, PUE: 1.1,
			Workers: []WorkerSpec{{8, "a100", 80}, {8, "a100", 80}}},
		{ID: "london-od", Region: "gb-london", PriceUSD: 2.40, PUE: 1.2,
			Workers: []WorkerSpec{{8, "a100", 80}, {8, "a100", 80}, {8, "a100", 80}}},
		{ID: "midlands-l4", Region: "gb-west-midlands", PriceUSD: 0.80, PUE: 1.2,
			Workers: []WorkerSpec{{4, "l4", 24}, {4, "l4", 24}, {4, "l4", 24}, {4, "l4", 24}}},
	}
}

// WorkloadNames lists the named workloads in report order.
var WorkloadNames = []string{"steady-mixed", "bursty-tenant", "deadline-crunch", "carbon-flex"}

// Workload builds a named workload over the carbon dataset. The simulation
// starts 8 days into the dataset so the forecaster has a week of history.
func Workload(name string, regional map[string][]ecoshift.Reading, datasetStart time.Time, seed uint64) (Scenario, error) {
	sc := Scenario{Name: name, Start: datasetStart.Add(8 * 24 * time.Hour), Horizon: 7 * 24 * time.Hour, Tick: 5 * time.Minute,
		Pools: Fleet(), Carbon: regional, InterruptPerHour: 0.03, Seed: seed, BudgetUSD: 50000}
	r := rand.New(rand.NewPCG(seed, 7))
	tenants := []string{"t0", "t1", "t2", "t3", "t4", "t5"}
	n := 0
	add := func(tenant string, at time.Duration, mut func(*JobSpec)) {
		n++
		gpus := []int{1, 1, 1, 1, 2, 2, 4, 8}[r.IntN(8)]
		// Log-uniform durations from 20 minutes to 10 hours.
		dur := time.Duration(math.Exp(math.Log(20)+r.Float64()*(math.Log(600)-math.Log(20)))) * time.Minute
		j := JobSpec{ID: fmt.Sprintf("job-%05d", n), Tenant: tenant, Submit: at, GPUs: gpus, Duration: dur,
			MaxRuntime: (dur*3/2 + time.Minute).Truncate(time.Minute), Priority: r.IntN(10), Preemptible: r.IntN(3) == 0}
		if r.IntN(4) == 0 {
			j.MinMemGB = 40 // excludes the 24 GB L4s
		}
		if mut != nil {
			mut(&j)
		}
		sc.Jobs = append(sc.Jobs, j)
	}
	// Poisson arrivals over the first 6 days, rate per tenant per hour.
	poisson := func(tenant string, perHour float64, mut func(*JobSpec)) {
		for t := 0.0; ; {
			t += r.ExpFloat64() / perHour
			if t > 6*24 {
				return
			}
			add(tenant, time.Duration(t*float64(time.Hour)), mut)
		}
	}
	switch name {
	case "steady-mixed":
		for _, t := range tenants {
			poisson(t, 0.9, nil)
		}
	case "bursty-tenant":
		for i := 0; i < 160; i++ { // t0 floods the queue in the first hour
			add("t0", time.Duration(r.IntN(3600))*time.Second, nil)
		}
		for _, t := range tenants[1:] {
			poisson(t, 0.6, nil)
		}
	case "deadline-crunch":
		for _, t := range tenants {
			poisson(t, 0.9, func(j *JobSpec) {
				if r.IntN(5) < 2 {
					d := j.Submit + j.MaxRuntime + time.Duration(30+r.IntN(150))*time.Minute
					j.Deadline = &d
				}
			})
		}
	case "carbon-flex":
		for _, t := range tenants {
			poisson(t, 0.8, func(j *JobSpec) {
				if r.IntN(10) < 7 {
					j.MaxDelay = 8 * time.Hour
				}
			})
		}
	default:
		return sc, fmt.Errorf("unknown workload %q (have %v)", name, WorkloadNames)
	}
	return sc, nil
}

// ---- trace replay ----

// pythonTrace is the club broker's gpu-broker.schedule-trace/v1 export.
type pythonTrace struct {
	Schema string `json:"schema"`
	Digest string `json:"digest"`
	Jobs   []struct {
		JobKey    string   `json:"job_key"`
		UserKey   string   `json:"user_key"`
		GPUType   string   `json:"gpu_type"`
		Hours     float64  `json:"requested_hours"`
		State     string   `json:"state"`
		Submitted float64  `json:"submitted_offset_seconds"`
		Started   *float64 `json:"started_offset_seconds"`
		Finished  *float64 `json:"finished_offset_seconds"`
	} `json:"jobs"`
}

// ReplayInfo says what a replay could and could not hold constant.
type ReplayInfo struct {
	Schema   string   `json:"schema"`
	Jobs     int      `json:"jobs_replayed"`
	Skipped  int      `json:"jobs_skipped_never_started"`
	Capacity int      `json:"declared_gpus_per_type"`
	Limits   []string `json:"limits"`
}

// LoadTrace turns either trace schema into a scenario over a single declared
// pool per GPU type. Only jobs that actually started are replayed, with their
// observed durations: a job that never held capacity has no duration to
// replay, and inventing one would compare policies over different work.
func LoadTrace(path string, capacityPerType int) (Scenario, ReplayInfo, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Scenario{}, ReplayInfo{}, err
	}
	var head struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return Scenario{}, ReplayInfo{}, err
	}
	start := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC) // arbitrary: traces carry offsets only
	sc := Scenario{Name: "trace", Start: start, Tick: time.Minute, Seed: 1, BudgetUSD: 1e9}
	info := ReplayInfo{Schema: head.Schema, Capacity: capacityPerType, Limits: []string{
		"capacity is declared (--capacity per GPU type), not observed",
		"durations are observed run times; outcomes and prices are not re-simulated",
		"no carbon data is attached to a replayed trace; carbon terms fall back",
	}}
	types := map[string]bool{}
	var horizon float64
	addJob := func(key, user, gpuType string, gpus int, submitted float64, started, finished *float64, deadline *float64, prio int) {
		if started == nil || finished == nil {
			info.Skipped++
			return
		}
		dur := time.Duration((*finished - *started) * float64(time.Second))
		if dur <= 0 {
			dur = time.Second
		}
		j := JobSpec{ID: key, Tenant: user, Submit: time.Duration(submitted * float64(time.Second)), GPUs: gpus, GPUModel: gpuType,
			Duration: dur, MaxRuntime: dur + time.Minute, Priority: prio}
		if deadline != nil {
			d := time.Duration(*deadline * float64(time.Second))
			j.Deadline = &d
		}
		types[gpuType] = true
		horizon = math.Max(horizon, *finished)
		sc.Jobs = append(sc.Jobs, j)
	}
	switch head.Schema {
	case "gpu-broker.schedule-trace/v1":
		var t pythonTrace
		if err := json.Unmarshal(raw, &t); err != nil {
			return sc, info, err
		}
		for _, j := range t.Jobs {
			addJob(j.JobKey, j.UserKey, j.GPUType, 1, j.Submitted, j.Started, j.Finished, nil, 0)
		}
		info.Limits = append(info.Limits, "the club trace has no GPU counts, deadlines or priorities: 1 GPU, no deadline, priority 0")
	case application.TraceSchema:
		var t application.Trace
		if err := json.Unmarshal(raw, &t); err != nil {
			return sc, info, err
		}
		if application.TraceDigest(t) != t.Digest {
			return sc, info, fmt.Errorf("trace digest mismatch: the file was modified after export")
		}
		for _, j := range t.Jobs {
			model := j.GPUModel
			if model == "" {
				model = "any"
			}
			addJob(j.Key, j.UserKey, model, j.GPUs, j.SubmittedS, j.StartedS, j.FinishedS, j.DeadlineS, j.Priority)
		}
	default:
		return sc, info, fmt.Errorf("unknown trace schema %q", head.Schema)
	}
	names := make([]string, 0, len(types))
	for t := range types {
		names = append(names, t)
	}
	sort.Strings(names)
	for _, t := range names {
		sc.Pools = append(sc.Pools, PoolSpec{ID: "replay-" + t, Region: "none", PriceUSD: 1, PUE: 1,
			Workers: []WorkerSpec{{GPUs: capacityPerType, GPUModel: t, MemGB: 1024}}})
	}
	// Jobs that asked for any model run on whichever declared type exists.
	for i := range sc.Jobs {
		if sc.Jobs[i].GPUModel == "any" {
			sc.Jobs[i].GPUModel = ""
		}
	}
	info.Jobs = len(sc.Jobs)
	// Replay well past the last observed finish so slower policies can drain.
	sc.Horizon = time.Duration(horizon*float64(time.Second))*3 + 24*time.Hour
	return sc, info, nil
}
