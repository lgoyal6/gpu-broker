package policy

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/ecoshift"
)

type Action string

const (
	ActionPlace   Action = "PLACE"
	ActionDelay   Action = "DELAY"
	ActionWait    Action = "WAIT"
	ActionPreempt Action = "PREEMPT"
	ActionExpire  Action = "EXPIRE"
)

type Rejection struct {
	WorkerID domain.WorkerID     `json:"worker_id"`
	PoolID   domain.PoolID       `json:"pool_id"`
	Reason   domain.RejectReason `json:"reason"`
	Detail   string              `json:"detail,omitempty"`
}

type ScoreComponent struct {
	Name         string  `json:"name"`
	Raw          float64 `json:"raw"`
	Normalized   float64 `json:"normalized"`
	Weight       float64 `json:"weight"`
	Contribution float64 `json:"contribution"`
}

type Candidate struct {
	WorkerID    domain.WorkerID  `json:"worker_id"`
	PoolID      domain.PoolID    `json:"pool_id"`
	Region      string           `json:"region"`
	Score       float64          `json:"score"`
	Components  []ScoreComponent `json:"components"`
	Cost        domain.MicroUSD  `json:"cost_micro_usd"`
	CarbonGrams *float64         `json:"carbon_grams"`
}

// OrderKey is why a job sat where it did in the queue.
type OrderKey struct {
	Position int      `json:"position"`
	Score    float64  `json:"score"`
	Terms    []string `json:"terms"`
}

// Decision is the durable record of what the scheduler decided for one job in
// one tick, with enough of the inputs to explain it later.
type Decision struct {
	JobID         domain.JobID    `json:"job_id"`
	TenantID      domain.TenantID `json:"-"`
	PolicyVersion string          `json:"policy_version"`
	Action        Action          `json:"action"`
	Reason        string          `json:"reason"`
	Order         OrderKey        `json:"order"`
	Considered    int             `json:"candidates_considered"`
	Rejected      []Rejection     `json:"candidates_rejected"`
	Candidates    []Candidate     `json:"candidates_scored"`
	ChosenWorker  domain.WorkerID `json:"chosen_worker,omitempty"`
	ChosenPool    domain.PoolID   `json:"chosen_pool,omitempty"`
	ExpectedStart *time.Time      `json:"expected_start"`
	DelayUntil    *time.Time      `json:"delay_until,omitempty"`
	BudgetEffect  domain.MicroUSD `json:"budget_effect_micro_usd"`
	PriceRate     domain.MicroUSD `json:"price_micro_usd_per_gpu_hour"`
	CarbonEffect  *float64        `json:"carbon_effect_grams"`
	CarbonBasis   string          `json:"carbon_basis,omitempty"`
	// CarbonSource names where the intensity came from (a live provider, an
	// ESO estimate, or "replay:<dataset>"), so a replayed demo can never be
	// read as live carbon data.
	CarbonSource string   `json:"carbon_source,omitempty"`
	CarbonSaved  *float64 `json:"carbon_saved_grams,omitempty"`
	Fallbacks    []string `json:"fallbacks"`
	// Victim is another tenant's job; it is kept out of the decision body
	// (which the job's owner can read) and recorded in the audit log instead.
	Victim      domain.JobID `json:"-"`
	InputDigest string       `json:"input_digest"`
}

// Decide produces one decision per QUEUED job in the snapshot, in queue order.
func Decide(s Snapshot, p Policy) []Decision {
	digest := s.Digest()
	ordered := orderJobs(s, p)

	free := make(map[domain.WorkerID]int, len(s.Workers))
	workersByID := make(map[domain.WorkerID]domain.Worker, len(s.Workers))
	workers := append([]domain.Worker(nil), s.Workers...)
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	for _, w := range workers {
		free[w.ID] = w.FreeGPUs()
		workersByID[w.ID] = w
	}
	avail := make(map[domain.ProjectID]domain.MicroUSD, len(s.ProjectAvailable))
	for k, v := range s.ProjectAvailable {
		avail[k] = v
	}
	active := make(map[domain.TenantID]int, len(s.TenantActiveGPUs))
	for k, v := range s.TenantActiveGPUs {
		active[k] = v
	}
	// Protected heads are chosen before any placement. Choosing them in queue
	// order is not enough: under fair share a starving heavy tenant sorts
	// behind fresh light tenants, who would backfill its worker first.
	held := map[domain.WorkerID]domain.JobID{}
	byAge := append([]orderedJob(nil), ordered...)
	sort.SliceStable(byAge, func(a, b int) bool {
		x, y := byAge[a].job, byAge[b].job
		if !x.SubmittedAt.Equal(y.SubmittedAt) {
			return x.SubmittedAt.Before(y.SubmittedAt)
		}
		return x.ID < y.ID
	})
	for _, oj := range byAge {
		guardStarvation(oj.job, workers, s, p, held)
	}
	preempted := map[domain.JobID]bool{}

	// Scoring copies these inputs into each decision. Reuse the temporary
	// slice, while keeping the retained candidate/rejection records separate.
	survivors := make([]candidateInput, 0, len(workers))
	out := make([]Decision, 0, len(ordered))
	for _, oj := range ordered {
		j := oj.job
		d := Decision{
			JobID: j.ID, TenantID: j.TenantID, PolicyVersion: p.Version, Order: oj.key,
			InputDigest: digest, Fallbacks: []string{}, Rejected: []Rejection{}, Candidates: []Candidate{},
		}

		if j.Deadline != nil && !s.Now.Before(*j.Deadline) {
			d.Action, d.Reason = ActionExpire, "deadline passed before the job could start"
			out = append(out, d)
			continue
		}
		if j.NotBefore != nil && s.Now.Before(*j.NotBefore) && !p.Emergency {
			t := *j.NotBefore
			d.Action, d.Reason, d.ExpectedStart = ActionWait, "delayed by an earlier EcoShift decision", &t
			out = append(out, d)
			continue
		}
		if max, ok := s.TenantMaxGPUs[j.TenantID]; ok && max > 0 && active[j.TenantID]+j.Requirements.GPUs > max {
			d.Action = ActionWait
			d.Reason = fmt.Sprintf("tenant GPU quota: %d active + %d requested > %d", active[j.TenantID], j.Requirements.GPUs, max)
			out = append(out, d)
			continue
		}

		survivors = survivors[:0]
		d.Rejected = make([]Rejection, 0, len(workers))
		for _, w := range workers {
			d.Considered++
			pool := s.Pools[w.PoolID]
			if rej, ok := filter(j, w, pool, free[w.ID], held, avail, s); !ok {
				d.Rejected = append(d.Rejected, rej)
				continue
			}
			survivors = append(survivors, candidateInput{w: w, pool: pool, free: free[w.ID]})
		}

		if len(survivors) == 0 {
			if p.AllowPreemption {
				if victim, w, ok := pickVictim(j, workersByID, s, free, preempted); ok {
					preempted[victim.JobID] = true
					d.Action, d.Victim, d.ChosenWorker = ActionPreempt, victim.JobID, w.ID
					d.ChosenPool = w.PoolID
					d.Reason = fmt.Sprintf("no free capacity; preempting lower-priority job (priority %d < %d)", victim.Priority, j.Priority)
					out = append(out, d)
					continue
				}
			}
			d.Action, d.Reason = ActionWait, summarize(d.Rejected)
			out = append(out, d)
			continue
		}

		cands, fallbacks := score(j, survivors, s, p)
		d.Candidates, d.Fallbacks = cands, fallbacks
		best := cands[0]
		bestPool := s.Pools[best.PoolID]

		if until, saved, ok := delayFor(j, best, bestPool, s, p, fallbacks); ok {
			d.Action, d.DelayUntil, d.ExpectedStart = ActionDelay, &until, &until
			d.CarbonSaved = &saved
			d.CarbonBasis = ecoshift.EnergyBasis
			d.CarbonSource = s.Carbon[best.Region].Current.Source
			d.ChosenPool = best.PoolID
			d.Reason = fmt.Sprintf("forecast intensity in %s is lower by more than its uncertainty; delaying %s",
				best.Region, until.Sub(s.Now).Round(time.Minute))
			out = append(out, d)
			continue
		}

		now := s.Now
		d.Action, d.ChosenWorker, d.ChosenPool, d.ExpectedStart = ActionPlace, best.WorkerID, best.PoolID, &now
		d.BudgetEffect, d.PriceRate = best.Cost, bestPool.PriceMicroUSDPerGPUHour
		d.CarbonEffect = best.CarbonGrams
		if best.CarbonGrams != nil {
			d.CarbonBasis = ecoshift.EnergyBasis
			d.CarbonSource = s.Carbon[best.Region].Current.Source
		}
		d.Reason = fmt.Sprintf("best of %d candidates (score %.3f)", len(cands), best.Score)
		free[best.WorkerID] -= j.Requirements.GPUs
		avail[j.ProjectID] -= best.Cost
		active[j.TenantID] += j.Requirements.GPUs
		out = append(out, d)
	}
	return out
}

type candidateInput struct {
	w    domain.Worker
	pool domain.Pool
	free int
}

func filter(j domain.Job, w domain.Worker, pool domain.Pool, free int, held map[domain.WorkerID]domain.JobID,
	avail map[domain.ProjectID]domain.MicroUSD, s Snapshot) (Rejection, bool) {
	rej := func(r domain.RejectReason, detail string) (Rejection, bool) {
		return Rejection{WorkerID: w.ID, PoolID: w.PoolID, Reason: r, Detail: detail}, false
	}
	if len(j.AllowedPools) > 0 && !containsPool(j.AllowedPools, w.PoolID) {
		return rej(domain.RejectPoolNotAllowed, "")
	}
	if w.State != domain.WorkerReady {
		return rej(domain.RejectWorkerNotReady, string(w.State))
	}
	if reason, ok := w.Capability.Satisfies(j.Requirements); !ok {
		return rej(reason, "")
	}
	if free < j.Requirements.GPUs {
		return rej(domain.RejectCapacity, fmt.Sprintf("%d free, %d needed", free, j.Requirements.GPUs))
	}
	if holder, ok := held[w.ID]; ok && holder != j.ID {
		// No detail: the holder is usually another tenant's job, and decision
		// bodies are readable by the job's owner.
		return rej(domain.RejectProtectedHead, "")
	}
	if pool.Interruptible && !(j.Attempt+1 < j.MaxAttempts) {
		return rej(domain.RejectInterruptible, "this would be the last attempt")
	}
	cost := domain.CostOf(pool.PriceMicroUSDPerGPUHour, j.Requirements.GPUs, j.MaxRuntime)
	if a, ok := avail[j.ProjectID]; ok && cost > a {
		return rej(domain.RejectBudget, fmt.Sprintf("needs %s, project has %s", cost, a))
	}
	if j.BudgetCap > 0 && cost > j.BudgetCap {
		return rej(domain.RejectBudget, fmt.Sprintf("needs %s, job cap %s", cost, j.BudgetCap))
	}
	return Rejection{}, true
}

func containsPool(ps []domain.PoolID, p domain.PoolID) bool {
	for _, x := range ps {
		if x == p {
			return true
		}
	}
	return false
}

// score computes components for every surviving candidate and returns them
// best first. Carbon is all-or-nothing per decision: if any candidate's
// region has no fresh reading the carbon term is dropped for all of them,
// because comparing a known intensity against an unknown one is a guess.
func score(j domain.Job, cs []candidateInput, s Snapshot, p Policy) ([]Candidate, []string) {
	fallbacks := []string{}
	weights := p.Weights
	costs := make([]float64, len(cs))
	carbons := make([]*float64, len(cs))
	carbonUsable := weights.Carbon > 0

	for i, c := range cs {
		costs[i] = float64(domain.CostOf(c.pool.PriceMicroUSDPerGPUHour, j.Requirements.GPUs, j.MaxRuntime))
		region := c.pool.Region
		rc := s.Carbon[region]
		kwh, known := ecoshift.EnergyKWh(c.w.Capability.GPUModel, j.Requirements.GPUs, j.MaxRuntime, c.pool.PUE)
		if !known {
			if carbonUsable {
				fallbacks = appendOnce(fallbacks, "gpu_power_unknown:"+c.w.Capability.GPUModel)
			}
			continue
		}
		if !rc.Fresh(s.Now, maxAge(p)) {
			if carbonUsable {
				fallbacks = appendOnce(fallbacks, "carbon_stale:"+region)
			}
			continue
		}
		g := kwh * rc.Current.GramsPerKWh
		carbons[i] = &g
	}
	if carbonUsable && len(fallbacks) > 0 {
		weights.Carbon = 0
		switch p.Fallback {
		case FallbackCost:
			if weights.Cost == 0 {
				weights.Cost = 1
			}
		case FallbackDeadline:
			if weights.Deadline == 0 {
				weights.Deadline = 1
			}
		}
		fallbacks = append(fallbacks, "fallback:"+string(p.Fallback))
	}

	costNorm := normalizeLowerBetter(costs)
	carbonVals := make([]float64, len(cs))
	for i, c := range carbons {
		if c != nil {
			carbonVals[i] = *c
		}
	}
	carbonNorm := normalizeLowerBetter(carbonVals)

	out := make([]Candidate, len(cs))
	for i, c := range cs {
		leftover := c.free - j.Requirements.GPUs
		fit := 1.0
		if c.w.Capability.GPUs > 0 {
			fit = 1 - float64(leftover)/float64(c.w.Capability.GPUs)
		}
		deadlineRaw := 1.0
		if j.Deadline != nil && c.pool.Interruptible {
			deadlineRaw = 0 // a deadline job on capacity that can vanish
		}
		comps := []ScoreComponent{
			component("fit", float64(leftover), fit, weights.Fit),
			component("cost", costs[i], costNorm[i], weights.Cost),
			component("deadline", deadlineRaw, deadlineRaw, weights.Deadline),
		}
		if carbons[i] != nil {
			comps = append(comps, component("carbon", *carbons[i], carbonNorm[i], weights.Carbon))
		} else {
			comps = append(comps, ScoreComponent{Name: "carbon", Raw: math.NaN(), Weight: weights.Carbon})
		}
		total := 0.0
		for _, sc := range comps {
			total += sc.Contribution
		}
		// NaN is not valid JSON; an unknown carbon value is stored as raw 0
		// with the candidate's CarbonGrams nil, which is the field readers use.
		for k := range comps {
			if math.IsNaN(comps[k].Raw) {
				comps[k].Raw = 0
			}
		}
		out[i] = Candidate{
			WorkerID: c.w.ID, PoolID: c.w.PoolID, Region: c.pool.Region, Score: round6(total),
			Components: comps, Cost: domain.MicroUSD(costs[i]), CarbonGrams: carbons[i],
		}
	}
	sort.SliceStable(out, func(a, b int) bool {
		if out[a].Score != out[b].Score {
			return out[a].Score > out[b].Score
		}
		return out[a].WorkerID < out[b].WorkerID
	})
	return out, fallbacks
}

func component(name string, raw, norm, w float64) ScoreComponent {
	return ScoreComponent{Name: name, Raw: round6(raw), Normalized: round6(norm), Weight: w, Contribution: round6(norm * w)}
}

// round6 keeps scores stable across platforms whose float formatting differs
// in the last bits, so the decision digest test is portable.
func round6(x float64) float64 { return math.Round(x*1e6) / 1e6 }

func maxAge(p Policy) time.Duration {
	if p.CarbonMaxAge > 0 {
		return p.CarbonMaxAge
	}
	return 2 * time.Hour
}

func normalizeLowerBetter(v []float64) []float64 {
	out := make([]float64, len(v))
	if len(v) == 0 {
		return out
	}
	lo, hi := v[0], v[0]
	for _, x := range v {
		lo, hi = math.Min(lo, x), math.Max(hi, x)
	}
	for i, x := range v {
		if hi == lo {
			out[i] = 1
		} else {
			out[i] = (hi - x) / (hi - lo)
		}
	}
	return out
}

func appendOnce(xs []string, x string) []string {
	for _, y := range xs {
		if y == x {
			return xs
		}
	}
	return append(xs, x)
}

// delayFor decides whether waiting for cleaner power is worth it. All four
// must hold: the policy and the job allow delay, carbon was not a fallback,
// the region has a trusted forecast, and the pessimistic (High) forecast
// beats the current reading by DelayMinImprovement within the window that
// still meets the deadline.
func delayFor(j domain.Job, best Candidate, pool domain.Pool, s Snapshot, p Policy, fallbacks []string) (time.Time, float64, bool) {
	if !p.AllowDelay || p.Emergency || j.MaxDelay <= 0 || len(fallbacks) > 0 || best.CarbonGrams == nil {
		return time.Time{}, 0, false
	}
	rc := s.Carbon[pool.Region]
	if rc.ForecastModel == "" || rc.Current == nil {
		return time.Time{}, 0, false
	}
	limit := s.Now.Add(j.MaxDelay)
	if j.Deadline != nil {
		latest := j.Deadline.Add(-j.MaxRuntime)
		if latest.Before(limit) {
			limit = latest
		}
	}
	// A job already delayed once is not delayed again: the forecast that
	// justified the first delay is what we committed to.
	if j.NotBefore != nil {
		return time.Time{}, 0, false
	}
	cur := rc.Current.GramsPerKWh
	bestAt, bestHigh := time.Time{}, cur
	for _, fp := range rc.Forecast {
		if !fp.At.After(s.Now) || fp.At.After(limit) {
			continue
		}
		if fp.High < bestHigh {
			bestAt, bestHigh = fp.At, fp.High
		}
	}
	if bestAt.IsZero() || cur <= 0 || (cur-bestHigh)/cur < p.DelayMinImprovement {
		return time.Time{}, 0, false
	}
	saved := *best.CarbonGrams * (cur - bestHigh) / cur
	return bestAt, round6(saved), true
}

// guardStarvation protects one worker for a job that has waited past the
// guard, so later, smaller jobs in the same tick cannot keep backfilling the
// capacity it needs. Only workers that could host the job once drained are
// candidates; the one with the most free GPUs is held.
func guardStarvation(j domain.Job, workers []domain.Worker, s Snapshot, p Policy, held map[domain.WorkerID]domain.JobID) {
	if p.StarvationGuard <= 0 || s.Now.Sub(j.SubmittedAt) < p.StarvationGuard {
		return
	}
	if j.Deadline != nil && !s.Now.Before(*j.Deadline) {
		return
	}
	// The hold applies even when the job fits right now: under fair share it
	// may sort behind light tenants who would take the freed GPUs first.
	var pick *domain.Worker
	for i := range workers {
		w := &workers[i]
		if _, taken := held[w.ID]; taken || w.State != domain.WorkerReady {
			continue
		}
		if len(j.AllowedPools) > 0 && !containsPool(j.AllowedPools, w.PoolID) {
			continue
		}
		if _, ok := w.Capability.Satisfies(j.Requirements); !ok {
			continue
		}
		if pick == nil || w.FreeGPUs() > pick.FreeGPUs() {
			pick = w
		}
	}
	if pick != nil {
		held[pick.ID] = j.ID
	}
}

// pickVictim finds a running preemptible job of strictly lower priority whose
// removal lets j fit. Lowest priority first, then the most recently started
// (least work lost), then job id.
func pickVictim(j domain.Job, byID map[domain.WorkerID]domain.Worker, s Snapshot, free map[domain.WorkerID]int, taken map[domain.JobID]bool) (RunningJob, domain.Worker, bool) {
	var cands []RunningJob
	for _, r := range s.Running {
		if !r.Preemptible || r.Priority >= j.Priority || taken[r.JobID] {
			continue
		}
		w, ok := byID[r.WorkerID]
		if !ok || w.State != domain.WorkerReady {
			continue
		}
		if len(j.AllowedPools) > 0 && !containsPool(j.AllowedPools, w.PoolID) {
			continue
		}
		if _, ok := w.Capability.Satisfies(j.Requirements); !ok {
			continue
		}
		if free[w.ID]+r.GPUs < j.Requirements.GPUs {
			continue
		}
		cands = append(cands, r)
	}
	if len(cands) == 0 {
		return RunningJob{}, domain.Worker{}, false
	}
	sort.Slice(cands, func(a, b int) bool {
		x, y := cands[a], cands[b]
		if x.Priority != y.Priority {
			return x.Priority < y.Priority
		}
		if !x.StartedAt.Equal(y.StartedAt) {
			return x.StartedAt.After(y.StartedAt)
		}
		return x.JobID < y.JobID
	})
	return cands[0], byID[cands[0].WorkerID], true
}

// summarize turns rejections into one WAIT reason: the most common code, with
// ties broken by name so the reason is stable.
func summarize(rs []Rejection) string {
	if len(rs) == 0 {
		return "no workers registered"
	}
	counts := map[domain.RejectReason]int{}
	for _, r := range rs {
		counts[r.Reason]++
	}
	type rc struct {
		r domain.RejectReason
		n int
	}
	var xs []rc
	for r, n := range counts {
		xs = append(xs, rc{r, n})
	}
	sort.Slice(xs, func(a, b int) bool {
		if xs[a].n != xs[b].n {
			return xs[a].n > xs[b].n
		}
		return xs[a].r < xs[b].r
	})
	return fmt.Sprintf("no candidate: %s on %d of %d workers", xs[0].r, xs[0].n, len(rs))
}
