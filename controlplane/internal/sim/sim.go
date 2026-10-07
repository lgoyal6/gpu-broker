// Package sim is the deterministic simulator. It drives the production
// policy (policy.Decide) tick by tick over a modelled fleet, arrival process,
// spot interruptions and a real carbon series, and reports what happened.
// Everything it produces is labelled evidence_class=simulator (ADR 0009):
// it evaluates policies, it is not evidence of adoption, cost or capacity.
package sim

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/ecoshift"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
)

type WorkerSpec struct {
	GPUs     int
	GPUModel string
	MemGB    int
}

type PoolSpec struct {
	ID            string
	Region        string
	PriceUSD      float64
	Interruptible bool
	PUE           float64
	Workers       []WorkerSpec
}

// JobSpec is one arrival. Duration is the true run time, unknown to the
// scheduler, which only sees MaxRuntime.
type JobSpec struct {
	ID          string
	Tenant      string
	Submit      time.Duration // offset from the simulation start
	GPUs        int
	GPUModel    string
	MinMemGB    int
	Duration    time.Duration
	MaxRuntime  time.Duration
	Priority    int
	Preemptible bool
	Deadline    *time.Duration // offset
	MaxDelay    time.Duration
}

type Scenario struct {
	Name             string
	Start            time.Time
	Horizon          time.Duration
	Tick             time.Duration
	Pools            []PoolSpec
	Jobs             []JobSpec
	Carbon           map[string][]ecoshift.Reading // region -> history covering [Start-7d, Start+Horizon]
	InterruptPerHour float64                       // per running job on interruptible pools
	Seed             uint64
	BudgetUSD        float64 // per tenant
}

type running struct {
	job     domain.Job
	spec    JobSpec
	worker  domain.WorkerID
	pool    PoolSpec
	model   string
	started time.Time
	end     time.Time
}

// Result is one (scenario, policy) run.
type Result struct {
	EvidenceClass     string             `json:"evidence_class"`
	Workload          string             `json:"workload"`
	Policy            string             `json:"policy"`
	Jobs              int                `json:"jobs"`
	Completed         int                `json:"completed"`
	Expired           int                `json:"expired"`
	Unfinished        int                `json:"unfinished_at_horizon"`
	WaitHoursP50      float64            `json:"wait_hours_p50"`
	WaitHoursP95      float64            `json:"wait_hours_p95"`
	WaitHoursP99      float64            `json:"wait_hours_p99"`
	WaitHoursMax      float64            `json:"wait_hours_max"`
	DeadlineJobs      int                `json:"deadline_jobs"`
	DeadlineMisses    int                `json:"deadline_misses"`
	Utilization       float64            `json:"gpu_utilization"`
	JainFairness      float64            `json:"jain_fairness"`
	CostUSD           float64            `json:"cost_usd"`
	CarbonKg          float64            `json:"carbon_kg_estimate"`
	Interruptions     int                `json:"spot_interruptions"`
	Preemptions       int                `json:"preemptions"`
	Delays            int                `json:"delay_decisions"`
	FallbackDecisions int                `json:"fallback_decisions"`
	Decisions         int                `json:"decisions"`
	DecideCalls       int                `json:"decide_calls"`
	TenantMeanWaitH   map[string]float64 `json:"tenant_mean_wait_hours"`
	PolicyVersion     string             `json:"policy_version"`
	DecideNanos       []int64            `json:"-"`
}

// Run simulates one policy over the scenario. Same scenario, same policy,
// same seed: same Result, field for field (TestRunIsDeterministic).
func Run(sc Scenario, p policy.Policy) Result {
	rng := rand.New(rand.NewPCG(sc.Seed, 0x9e3779b97f4a7c15))
	res := Result{EvidenceClass: "simulator", Workload: sc.Name, Policy: p.Name, PolicyVersion: p.Version, Jobs: len(sc.Jobs),
		TenantMeanWaitH: map[string]float64{}}

	pools := map[domain.PoolID]domain.Pool{}
	poolSpec := map[domain.PoolID]PoolSpec{}
	var workers []domain.Worker
	totalGPUs := 0
	for _, ps := range sc.Pools {
		id := domain.PoolID(ps.ID)
		pools[id] = domain.Pool{ID: id, Name: ps.ID, Region: ps.Region, PriceMicroUSDPerGPUHour: domain.MicroUSD(ps.PriceUSD * 1e6),
			Interruptible: ps.Interruptible, PUE: ps.PUE}
		poolSpec[id] = ps
		for i, w := range ps.Workers {
			workers = append(workers, domain.Worker{ID: domain.WorkerID(fmt.Sprintf("%s-w%02d", ps.ID, i)), PoolID: id, State: domain.WorkerReady,
				Capability: domain.Capability{GPUModel: w.GPUModel, GPUs: w.GPUs, GPUMemGB: w.MemGB, Runtimes: []string{"sim"}}})
			totalGPUs += w.GPUs
		}
	}
	workerIdx := map[domain.WorkerID]int{}
	for i, w := range workers {
		workerIdx[w.ID] = i
	}
	series := map[string]ecoshift.Series{}
	history := map[string][]ecoshift.Reading{}
	for r, rs := range sc.Carbon {
		series[r] = ecoshift.NewSeries(rs)
		history[r] = rs
	}

	arrivals := append([]JobSpec(nil), sc.Jobs...)
	sort.SliceStable(arrivals, func(i, j int) bool { return arrivals[i].Submit < arrivals[j].Submit })
	specs := map[domain.JobID]JobSpec{}
	var queue []domain.Job
	var run []running
	usage := map[domain.TenantID]float64{}
	avail := map[domain.ProjectID]domain.MicroUSD{}
	waits := map[string][]float64{}
	var allWaits []float64
	busyGPUHours := 0.0
	next := 0
	lastWait := map[domain.JobID]string{}

	for now := sc.Start; now.Before(sc.Start.Add(sc.Horizon)); now = now.Add(sc.Tick) {
		// Fair-share usage decays with a 14-day half-life, as in production.
		decay := math.Pow(0.5, sc.Tick.Hours()/(14*24))
		for k := range usage {
			usage[k] *= decay
		}
		// Completions and interruptions.
		kept := run[:0]
		for _, r := range run {
			hours := sc.Tick.Hours()
			if r.end.Before(now) {
				hours = math.Max(0, r.end.Sub(now.Add(-sc.Tick)).Hours())
			}
			busyGPUHours += hours * float64(r.job.Requirements.GPUs)
			if !r.end.After(now) {
				res.Completed++
				res.CostUSD += r.pool.PriceUSD * float64(r.job.Requirements.GPUs) * r.end.Sub(r.started).Hours()
				res.CarbonKg += carbonKg(series[r.pool.Region], r, r.started, r.end)
				workers[workerIdx[r.worker]].GPUsReserved -= r.job.Requirements.GPUs
				usage[r.job.TenantID] += float64(r.job.Requirements.GPUs) * r.end.Sub(r.started).Hours()
				continue
			}
			if r.pool.Interruptible && rng.Float64() < sc.InterruptPerHour*sc.Tick.Hours() {
				res.Interruptions++
				res.CostUSD += r.pool.PriceUSD * float64(r.job.Requirements.GPUs) * now.Sub(r.started).Hours()
				res.CarbonKg += carbonKg(series[r.pool.Region], r, r.started, now)
				workers[workerIdx[r.worker]].GPUsReserved -= r.job.Requirements.GPUs
				j := r.job
				j.State, j.NotBefore = domain.JobQueued, nil
				if j.Attempt < j.MaxAttempts {
					queue = append(queue, j)
				} else {
					res.Unfinished++
				}
				continue
			}
			kept = append(kept, r)
		}
		run = kept

		for next < len(arrivals) && sc.Start.Add(arrivals[next].Submit).Compare(now) <= 0 {
			a := arrivals[next]
			next++
			j := domain.Job{ID: domain.JobID(a.ID), TenantID: domain.TenantID(a.Tenant), ProjectID: domain.ProjectID("p-" + a.Tenant),
				Requirements: domain.Requirements{GPUs: a.GPUs, GPUModel: a.GPUModel, MinGPUMemGB: a.MinMemGB},
				Priority:     a.Priority, Preemptible: a.Preemptible, MaxRuntime: a.MaxRuntime, MaxDelay: a.MaxDelay,
				MaxAttempts: 3, State: domain.JobQueued, SubmittedAt: sc.Start.Add(a.Submit), PolicyName: p.Name}
			if a.Deadline != nil {
				d := sc.Start.Add(*a.Deadline)
				j.Deadline = &d
				res.DeadlineJobs++
			}
			if _, ok := avail[j.ProjectID]; !ok {
				avail[j.ProjectID] = domain.MicroUSD(sc.BudgetUSD * 1e6)
			}
			specs[j.ID] = a
			queue = append(queue, j)
		}
		if len(queue) == 0 {
			continue
		}

		snap := policy.Snapshot{Now: now, Jobs: queue, Workers: workers, Pools: pools, TenantUsage: usage,
			ProjectAvailable: avail, Carbon: carbonAt(history, now)}
		for _, r := range run {
			snap.Running = append(snap.Running, policy.RunningJob{JobID: r.job.ID, TenantID: r.job.TenantID, WorkerID: r.worker,
				GPUs: r.job.Requirements.GPUs, Priority: r.job.Priority, Preemptible: r.job.Preemptible && r.job.Attempt < r.job.MaxAttempts,
				StartedAt: r.started})
		}
		t0 := time.Now()
		ds := policy.Decide(snap, p)
		res.DecideNanos = append(res.DecideNanos, time.Since(t0).Nanoseconds())
		res.DecideCalls++

		byID := map[domain.JobID]int{}
		for i, j := range queue {
			byID[j.ID] = i
		}
		remove := map[domain.JobID]bool{}
		for _, d := range ds {
			if len(d.Fallbacks) > 0 {
				res.FallbackDecisions++
			}
			if d.Action == policy.ActionWait && lastWait[d.JobID] == d.Reason {
				continue // same rule as production: an unchanged WAIT is not a new decision
			}
			res.Decisions++
			lastWait[d.JobID] = d.Reason
			i := byID[d.JobID]
			switch d.Action {
			case policy.ActionPlace:
				j := queue[i]
				j.Attempt++
				j.State = domain.JobRunning
				spec := specs[j.ID]
				w := &workers[workerIdx[d.ChosenWorker]]
				w.GPUsReserved += j.Requirements.GPUs
				end := now.Add(spec.Duration)
				if spec.Duration > j.MaxRuntime {
					end = now.Add(j.MaxRuntime)
				}
				wait := now.Sub(j.SubmittedAt).Hours()
				waits[string(j.TenantID)] = append(waits[string(j.TenantID)], wait)
				allWaits = append(allWaits, wait)
				if j.Deadline != nil && end.After(*j.Deadline) {
					res.DeadlineMisses++
				}
				// The simulator knows the true duration, so it charges the actual
				// cost at placement; production holds the maximum and settles
				// later. This only moves when a budget runs out, not the order.
				avail[j.ProjectID] -= domain.CostOf(pools[d.ChosenPool].PriceMicroUSDPerGPUHour, j.Requirements.GPUs, end.Sub(now))
				run = append(run, running{job: j, spec: spec, worker: d.ChosenWorker, pool: poolSpec[d.ChosenPool],
					model: w.Capability.GPUModel, started: now, end: end})
				remove[j.ID] = true
			case policy.ActionDelay:
				queue[i].NotBefore = d.DelayUntil
				res.Delays++
			case policy.ActionExpire:
				res.Expired++
				if queue[i].Deadline != nil {
					res.DeadlineMisses++
				}
				remove[queue[i].ID] = true
			case policy.ActionPreempt:
				for k, r := range run {
					if r.job.ID != d.Victim {
						continue
					}
					res.Preemptions++
					res.CostUSD += r.pool.PriceUSD * float64(r.job.Requirements.GPUs) * now.Sub(r.started).Hours()
					res.CarbonKg += carbonKg(series[r.pool.Region], r, r.started, now)
					workers[workerIdx[r.worker]].GPUsReserved -= r.job.Requirements.GPUs
					v := r.job
					v.State, v.NotBefore = domain.JobQueued, nil
					queue = append(queue, v)
					run = append(run[:k], run[k+1:]...)
					break
				}
			}
		}
		rest := queue[:0]
		for _, j := range queue {
			if !remove[j.ID] {
				rest = append(rest, j)
			}
		}
		queue = rest
	}
	res.Unfinished += len(queue) + len(run)
	capacity := float64(totalGPUs) * sc.Horizon.Hours()
	if capacity > 0 {
		res.Utilization = round(busyGPUHours / capacity)
	}
	sort.Float64s(allWaits)
	res.WaitHoursP50, res.WaitHoursP95, res.WaitHoursP99 = pct(allWaits, .5), pct(allWaits, .95), pct(allWaits, .99)
	if len(allWaits) > 0 {
		res.WaitHoursMax = round(allWaits[len(allWaits)-1])
	}
	var xs []float64
	tenants := make([]string, 0, len(waits))
	for t := range waits {
		tenants = append(tenants, t)
	}
	sort.Strings(tenants)
	for _, t := range tenants {
		m := mean(waits[t])
		res.TenantMeanWaitH[t] = round(m)
		xs = append(xs, 1/(1+m))
	}
	res.JainFairness = round(jain(xs))
	res.CostUSD, res.CarbonKg = round(res.CostUSD), round(res.CarbonKg)
	return res
}

// carbonAt gives each region its reading at now and the history the
// forecaster may use. History after now is never visible to the policy.
func carbonAt(history map[string][]ecoshift.Reading, now time.Time) map[string]ecoshift.RegionCarbon {
	out := map[string]ecoshift.RegionCarbon{}
	for region, rs := range history {
		k := sort.Search(len(rs), func(i int) bool { return rs[i].At.After(now) })
		if k == 0 {
			continue
		}
		past := rs[:k]
		rc := ecoshift.RegionCarbon{Current: &past[len(past)-1]}
		if fc, model, ok := ecoshift.TrustedForecast(past, now, 24*time.Hour); ok {
			rc.Forecast, rc.ForecastModel = fc, model
		}
		out[region] = rc
	}
	return out
}

// carbonKg integrates board-power energy against the series over [from, to).
func carbonKg(s ecoshift.Series, r running, from, to time.Time) float64 {
	total := 0.0
	model := r.model
	for t := from; t.Before(to); {
		slot := t.Truncate(ecoshift.Step)
		end := slot.Add(ecoshift.Step)
		if end.After(to) {
			end = to
		}
		kwh, ok := ecoshift.EnergyKWh(model, r.job.Requirements.GPUs, end.Sub(t), r.pool.PUE)
		if !ok {
			return total
		}
		total += kwh * s[slot] / 1000
		t = end
	}
	return total
}

func pct(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return round(sorted[int(math.Ceil(q*float64(len(sorted))))-1])
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := 0.0
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

// jain is Jain's fairness index: 1 when all xs are equal, 1/n at worst.
func jain(xs []float64) float64 {
	if len(xs) == 0 {
		return 1
	}
	s, s2 := 0.0, 0.0
	for _, x := range xs {
		s += x
		s2 += x * x
	}
	if s2 == 0 {
		return 1
	}
	return s * s / (float64(len(xs)) * s2)
}

func round(x float64) float64 { return math.Round(x*1e4) / 1e4 }
