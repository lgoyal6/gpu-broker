package application

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"slices"
	"sort"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
)

const maxCPPlannerOutput = 4 << 20

type CPPlacement struct {
	JobID            string `json:"job_id"`
	WorkerID         string `json:"worker_id"`
	StartOffsetS     int64  `json:"start_offset_s"`
	FinishOffsetS    int64  `json:"finish_offset_s"`
	ReservedMicroUSD int64  `json:"reserved_micro_usd"`
}

type CPProposal struct {
	Schema           string        `json:"schema"`
	SnapshotVersion  string        `json:"snapshot_version"`
	SnapshotDigest   string        `json:"snapshot_digest"`
	SolverStatus     string        `json:"solver_status"`
	Placements       []CPPlacement `json:"placements"`
	FallbackRequired bool          `json:"fallback_required"`
}

type PlacementPlanner interface {
	Propose(context.Context, map[string]any) (CPProposal, error)
}

type ExecCPPlanner struct {
	Python        string
	Script        string
	Timeout       time.Duration // process startup plus bounded solve; default 2s
	SolverTimeout time.Duration // CP-SAT solve time; default 250ms
	Horizon       time.Duration
	Slot          time.Duration
}

func (p ExecCPPlanner) Propose(ctx context.Context, snapshot map[string]any) (CPProposal, error) {
	var out CPProposal
	if p.Python == "" || p.Script == "" {
		return out, fmt.Errorf("CP-SAT planner executable and script are required")
	}
	timeout := p.Timeout
	if timeout <= 0 || timeout > 5*time.Second {
		timeout = 2 * time.Second
	}
	solveTimeout := p.SolverTimeout
	if solveTimeout <= 0 || solveTimeout > time.Second {
		solveTimeout = 250 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	input, err := json.Marshal(snapshot)
	if err != nil {
		return out, fmt.Errorf("encode CP-SAT snapshot: %w", err)
	}
	file, err := os.CreateTemp("", "gpub-cpsat-*.json")
	if err != nil {
		return out, fmt.Errorf("create CP-SAT input: %w", err)
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err := file.Write(input); err != nil {
		file.Close()
		return out, fmt.Errorf("write CP-SAT input: %w", err)
	}
	if err := file.Close(); err != nil {
		return out, fmt.Errorf("close CP-SAT input: %w", err)
	}
	cmd := exec.CommandContext(ctx, p.Python, p.Script, name, "--timeout-s", fmt.Sprintf("%.3f", solveTimeout.Seconds()))
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return out, fmt.Errorf("CP-SAT planner timed out: %w", ctx.Err())
		}
		return out, fmt.Errorf("CP-SAT planner failed: %w (%s)", err, trimOutput(stderr.String(), 512))
	}
	if stdout.Len() > maxCPPlannerOutput {
		return out, fmt.Errorf("CP-SAT output exceeds %d bytes", maxCPPlannerOutput)
	}
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		return out, fmt.Errorf("decode CP-SAT proposal: %w", err)
	}
	if out.Schema != "gpu-broker.placement-proposal/v1" || out.FallbackRequired ||
		(out.SolverStatus != "OPTIMAL" && out.SolverStatus != "FEASIBLE") {
		return out, fmt.Errorf("CP-SAT did not produce an applicable proposal (status=%s)", out.SolverStatus)
	}
	return out, nil
}

func (p ExecCPPlanner) Snapshot(s policy.Snapshot, jobs []domain.Job) (map[string]any, string, string) {
	horizon := p.Horizon
	if horizon <= 0 || horizon > 45*time.Minute {
		horizon = 45 * time.Minute
	}
	slot := p.Slot
	if slot <= 0 || slot > time.Minute {
		slot = time.Second
	}
	workerRows := make([]map[string]any, 0, len(s.Workers))
	for _, w := range s.Workers {
		pool := s.Pools[w.PoolID]
		workerRows = append(workerRows, map[string]any{
			"id": string(w.ID), "gpu_model": w.Capability.GPUModel,
			"free_gpus": max(0, w.FreeGPUs()), "price_micro_usd_per_gpu_hour": int64(pool.PriceMicroUSDPerGPUHour),
			"max_runtime_s": int64(horizon.Seconds()), "memory_gb": w.Capability.GPUMemGB,
			"labels": []string{"pool:" + string(w.PoolID)},
		})
	}
	jobRows := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		model := j.Requirements.GPUModel
		if model == "" {
			model = "any"
		}
		budget := int64(1 << 52)
		if j.BudgetCap > 0 {
			budget = int64(j.BudgetCap)
		}
		release := int64(0)
		if j.NotBefore != nil && j.NotBefore.After(s.Now) {
			release = int64(math.Ceil(j.NotBefore.Sub(s.Now).Seconds()))
		}
		row := map[string]any{"id": string(j.ID), "tenant": string(j.TenantID), "project": string(j.ProjectID),
			"gpu_model": model, "gpus": j.Requirements.GPUs, "runtime_s": max(int64(1), int64(math.Ceil(j.MaxRuntime.Seconds()))),
			"setup_s": 0, "release_s": release, "budget_micro_usd": budget,
			"memory_gb": j.Requirements.MinGPUMemGB, "required_labels": []string{}}
		if j.Deadline != nil {
			row["deadline_s"] = max(int64(0), int64(math.Ceil(j.Deadline.Sub(s.Now).Seconds())))
		}
		labels := make([]string, 0, len(j.AllowedPools))
		for _, id := range j.AllowedPools {
			labels = append(labels, "pool:"+string(id))
		}
		sort.Strings(labels)
		row["required_labels"] = labels
		jobRows = append(jobRows, row)
	}
	projectBudget := map[string]int64{}
	for project, amount := range s.ProjectAvailable {
		projectBudget[string(project)] = int64(amount)
	}
	tenantMax, tenantActive := map[string]int{}, map[string]int{}
	for tenant, amount := range s.TenantMaxGPUs {
		tenantMax[string(tenant)] = amount
	}
	for tenant, amount := range s.TenantActiveGPUs {
		tenantActive[string(tenant)] = amount
	}
	version := s.Digest()
	payload := map[string]any{"version": version, "now_epoch_s": s.Now.Unix(),
		"horizon_s": int64(horizon.Seconds()), "slot_s": max(int64(1), int64(slot.Seconds())),
		"jobs": jobRows, "workers": workerRows, "project_budget_micro_usd": projectBudget,
		"tenant_max_gpus": tenantMax, "tenant_active_gpus": tenantActive}
	canonical, _ := json.Marshal(payload)
	sum := sha256.Sum256(canonical)
	return payload, version, hex.EncodeToString(sum[:])
}

func trimOutput(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func (s *Service) cpSatDecisions(ctx context.Context, snapshot policy.Snapshot) []policy.Decision {
	fallbackPolicy, _ := policy.Lookup("deadline-shortest")
	cpPolicy, _ := policy.Lookup("cp-sat")
	fallback := func(reason string) []policy.Decision {
		decisions := policy.Decide(snapshot, fallbackPolicy)
		for i := range decisions {
			decisions[i].PolicyVersion = cpPolicy.Version
			decisions[i].Fallbacks = append(decisions[i].Fallbacks, "CP-SAT fallback to deadline-shortest: "+reason)
		}
		return decisions
	}
	if s.CPPlanner == nil {
		return fallback("planner is not configured")
	}
	return cpSatDecisionsWithPlanner(ctx, snapshot, s.CPPlanner, fallbackPolicy, cpPolicy, fallback)
}

func sCPPlannerSnapshot(snapshot policy.Snapshot) (map[string]any, string, string) {
	return (ExecCPPlanner{}).Snapshot(snapshot, snapshot.Jobs)
}

func cpSatDecisionsWithPlanner(ctx context.Context, snapshot policy.Snapshot, planner PlacementPlanner,
	fallbackPolicy, cpPolicy policy.Policy, fallback func(string) []policy.Decision) []policy.Decision {
	payload, version, digest := sCPPlannerSnapshot(snapshot)
	proposal, err := planner.Propose(ctx, payload)
	if err != nil {
		return fallback(err.Error())
	}
	if proposal.SnapshotVersion != version || proposal.SnapshotDigest != digest {
		return fallback("proposal is stale or bound to a different snapshot")
	}
	if err := validateCPProposal(payload, proposal); err != nil {
		return fallback("proposal validation failed: " + err.Error())
	}
	placements := make(map[domain.JobID]CPPlacement, len(proposal.Placements))
	for _, placement := range proposal.Placements {
		placements[domain.JobID(placement.JobID)] = placement
	}
	jobs := make(map[domain.JobID]domain.Job, len(snapshot.Jobs))
	for _, job := range snapshot.Jobs {
		jobs[job.ID] = job
	}
	ordered := append([]CPPlacement(nil), proposal.Placements...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].StartOffsetS != ordered[j].StartOffsetS {
			return ordered[i].StartOffsetS < ordered[j].StartOffsetS
		}
		return ordered[i].JobID < ordered[j].JobID
	})
	current := snapshot
	decisions := make([]policy.Decision, 0, len(snapshot.Jobs))
	position := 0
	for _, placement := range ordered {
		job := jobs[domain.JobID(placement.JobID)]
		position++
		if placement.StartOffsetS > 0 {
			d := policy.Decide(policy.Snapshot{Now: current.Now, Jobs: []domain.Job{job}}, fallbackPolicy)[0]
			d.PolicyVersion, d.Action = cpPolicy.Version, policy.ActionWait
			d.Reason = "CP-SAT proposal schedules this job in the future"
			expected := current.Now.Add(time.Duration(placement.StartOffsetS) * time.Second)
			d.ExpectedStart, d.Order.Position = &expected, position
			decisions = append(decisions, d)
			continue
		}
		workerID := domain.WorkerID(placement.WorkerID)
		candidateSnapshot := current
		candidateSnapshot.Jobs = []domain.Job{job}
		candidateSnapshot.Workers = nil
		for _, worker := range current.Workers {
			if worker.ID == workerID {
				candidateSnapshot.Workers = append(candidateSnapshot.Workers, worker)
			}
		}
		ds := policy.Decide(candidateSnapshot, fallbackPolicy)
		if len(ds) != 1 || ds[0].Action != policy.ActionPlace || ds[0].ChosenWorker != workerID {
			return fallback("planned immediate placement failed the current policy eligibility recheck")
		}
		d := ds[0]
		d.PolicyVersion, d.Order.Position = cpPolicy.Version, position
		d.Reason = "CP-SAT feasible placement rechecked against current service snapshot"
		decisions = append(decisions, d)
		consume(&current, d, []domain.Job{job})
	}
	for _, job := range snapshot.Jobs {
		if _, exists := placements[job.ID]; exists {
			continue
		}
		d := policy.Decide(policy.Snapshot{Now: current.Now, Jobs: []domain.Job{job}}, fallbackPolicy)[0]
		d.PolicyVersion, d.Action = cpPolicy.Version, policy.ActionWait
		d.Reason = "CP-SAT proposal left job unplaced within the bounded horizon"
		position++
		d.Order.Position = position
		decisions = append(decisions, d)
	}
	return decisions
}

func validateCPProposal(payload map[string]any, proposal CPProposal) error {
	jobs, _ := payload["jobs"].([]map[string]any)
	workers, _ := payload["workers"].([]map[string]any)
	if jobs == nil || workers == nil {
		return fmt.Errorf("snapshot has invalid jobs/workers")
	}
	jobByID := map[string]map[string]any{}
	workerByID := map[string]map[string]any{}
	for _, job := range jobs {
		id, _ := job["id"].(string)
		jobByID[id] = job
	}
	for _, worker := range workers {
		id, _ := worker["id"].(string)
		workerByID[id] = worker
	}
	seen := map[string]bool{}
	intervals := map[string][][3]int64{}
	tenantIntervals := map[string][][3]int64{}
	projectCost := map[string]int64{}
	for _, placement := range proposal.Placements {
		job, jobOK := jobByID[placement.JobID]
		worker, workerOK := workerByID[placement.WorkerID]
		if !jobOK || !workerOK || seen[placement.JobID] {
			return fmt.Errorf("unknown or duplicate job/worker placement")
		}
		seen[placement.JobID] = true
		start, finish := placement.StartOffsetS, placement.FinishOffsetS
		horizon, _ := number(payload["horizon_s"])
		release, _ := number(job["release_s"])
		if start < release || finish <= start || finish > horizon {
			return fmt.Errorf("invalid placement interval")
		}
		setup, _ := number(job["setup_s"])
		runtime, _ := number(job["runtime_s"])
		duration := setup + runtime
		if finish-start < duration {
			return fmt.Errorf("placement interval is shorter than job duration")
		}
		model, _ := job["gpu_model"].(string)
		workerModel, _ := worker["gpu_model"].(string)
		if model != "any" && model != workerModel {
			return fmt.Errorf("GPU model mismatch")
		}
		gpus, _ := number(job["gpus"])
		workerGPUs, _ := number(worker["free_gpus"])
		if gpus > workerGPUs {
			return fmt.Errorf("worker GPU capacity exceeded")
		}
		memory, _ := number(job["memory_gb"])
		workerMemory, _ := number(worker["memory_gb"])
		if memory > workerMemory {
			return fmt.Errorf("worker memory exceeded")
		}
		labels, _ := job["required_labels"].([]string)
		workerLabels, _ := worker["labels"].([]string)
		for _, required := range labels {
			if !slices.Contains(workerLabels, required) {
				return fmt.Errorf("worker label constraint failed")
			}
		}
		price, _ := number(worker["price_micro_usd_per_gpu_hour"])
		cost := int64(math.Ceil(float64(gpus*duration*price) / 3600))
		if placement.ReservedMicroUSD != cost {
			return fmt.Errorf("reservation cost mismatch")
		}
		if budget, ok := number(job["budget_micro_usd"]); ok && cost > budget {
			return fmt.Errorf("job budget exceeded")
		}
		if deadline, ok := number(job["deadline_s"]); ok && finish > deadline {
			return fmt.Errorf("job deadline exceeded")
		}
		if gpus < 1 {
			return fmt.Errorf("invalid GPU count")
		}
		project, _ := job["project"].(string)
		projectCost[project] += cost
		intervals[placement.WorkerID] = append(intervals[placement.WorkerID], [3]int64{start, finish, gpus})
		tenant, _ := job["tenant"].(string)
		tenantIntervals[tenant] = append(tenantIntervals[tenant], [3]int64{start, finish, gpus})
	}
	for workerID, items := range intervals {
		worker := workerByID[workerID]
		capacity := int64(worker["free_gpus"].(int))
		points := [][2]int64{}
		for _, item := range items {
			points = append(points, [2]int64{item[0], item[2]}, [2]int64{item[1], -item[2]})
		}
		sort.Slice(points, func(i, j int) bool {
			if points[i][0] != points[j][0] {
				return points[i][0] < points[j][0]
			}
			return points[i][1] < points[j][1]
		})
		var used int64
		for _, point := range points {
			used += point[1]
			if used > capacity {
				return fmt.Errorf("worker %s concurrency exceeded", workerID)
			}
		}
	}
	budgets, _ := payload["project_budget_micro_usd"].(map[string]int64)
	for project, cost := range projectCost {
		if budget, ok := budgets[project]; ok && cost > budget {
			return fmt.Errorf("project budget exceeded")
		}
	}
	for tenant, items := range tenantIntervals {
		maxValues, _ := payload["tenant_max_gpus"].(map[string]int)
		activeValues, _ := payload["tenant_active_gpus"].(map[string]int)
		capacity := int64(1 << 30)
		if maxGPUs, ok := maxValues[tenant]; ok && maxGPUs > 0 {
			capacity = int64(maxGPUs - activeValues[tenant])
		}
		points := [][2]int64{}
		for _, item := range items {
			points = append(points, [2]int64{item[0], item[2]}, [2]int64{item[1], -item[2]})
		}
		sort.Slice(points, func(i, j int) bool {
			if points[i][0] != points[j][0] {
				return points[i][0] < points[j][0]
			}
			return points[i][1] < points[j][1]
		})
		var used int64
		for _, point := range points {
			used += point[1]
			if used > capacity {
				return fmt.Errorf("tenant %s concurrency exceeded", tenant)
			}
		}
	}
	return nil
}

func number(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		if v == math.Trunc(v) && v <= math.MaxInt64 && v >= math.MinInt64 {
			return int64(v), true
		}
	}
	return 0, false
}
