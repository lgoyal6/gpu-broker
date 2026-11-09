package application

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
)

type cpPlannerFunc func(context.Context, map[string]any) (CPProposal, error)

func (f cpPlannerFunc) Propose(ctx context.Context, snapshot map[string]any) (CPProposal, error) {
	return f(ctx, snapshot)
}

func cpFixture() policy.Snapshot {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	job := domain.Job{ID: "job-1", TenantID: "tenant-1", ProjectID: "project-1", UserID: "user-1",
		Requirements: domain.Requirements{GPUs: 1, GPUModel: "t4", Runtime: "process"},
		MaxRuntime:   time.Minute, BudgetCap: 100_000, PolicyName: "cp-sat", MaxAttempts: 1,
		State: domain.JobQueued, SubmittedAt: now, UpdatedAt: now}
	worker := domain.Worker{ID: "worker-1", PoolID: "pool-1", State: domain.WorkerReady,
		Capability: domain.Capability{GPUModel: "t4", GPUs: 1, GPUMemGB: 16, Runtimes: []string{"process"}}}
	pool := domain.Pool{ID: "pool-1", Kind: domain.PoolKind("process"), Region: "us-central1",
		PriceMicroUSDPerGPUHour: 539_999, PUE: 1}
	return policy.Snapshot{Now: now, Jobs: []domain.Job{job}, Workers: []domain.Worker{worker},
		Pools: map[domain.PoolID]domain.Pool{"pool-1": pool}, ProjectAvailable: map[domain.ProjectID]domain.MicroUSD{"project-1": 1_000_000},
		TenantMaxGPUs: map[domain.TenantID]int{"tenant-1": 1}, TenantActiveGPUs: map[domain.TenantID]int{}}
}

func cpValidProposal(s policy.Snapshot) (CPProposal, map[string]any) {
	payload, version, digest := (ExecCPPlanner{}).Snapshot(s, s.Jobs)
	return CPProposal{Schema: "gpu-broker.placement-proposal/v1", SnapshotVersion: version,
		SnapshotDigest: digest, SolverStatus: "OPTIMAL", Placements: []CPPlacement{{JobID: "job-1", WorkerID: "worker-1",
			StartOffsetS: 0, FinishOffsetS: 60, ReservedMicroUSD: 9000}}}, payload
}

func TestCPSATServicePathAcceptsFeasibleSnapshotBoundPlan(t *testing.T) {
	snapshot := cpFixture()
	proposal, _ := cpValidProposal(snapshot)
	svc := &Service{CPPlanner: cpPlannerFunc(func(_ context.Context, _ map[string]any) (CPProposal, error) { return proposal, nil })}
	decisions := svc.cpSatDecisions(context.Background(), snapshot)
	if len(decisions) != 1 || decisions[0].Action != policy.ActionPlace || decisions[0].ChosenWorker != "worker-1" {
		t.Fatalf("unexpected CP-SAT decision: %+v", decisions)
	}
	if decisions[0].PolicyVersion != "cp-sat@v1" || len(decisions[0].Fallbacks) != 0 {
		t.Fatalf("plan was mislabeled or fell back: %+v", decisions[0])
	}
}

func TestCPSATServicePathFallsBackForStaleOrInfeasiblePlan(t *testing.T) {
	for name, mutate := range map[string]func(*CPProposal){
		"stale":         func(p *CPProposal) { p.SnapshotDigest = "stale" },
		"over-capacity": func(p *CPProposal) { p.Placements[0].FinishOffsetS = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := cpFixture()
			proposal, _ := cpValidProposal(snapshot)
			mutate(&proposal)
			svc := &Service{CPPlanner: cpPlannerFunc(func(_ context.Context, _ map[string]any) (CPProposal, error) { return proposal, nil })}
			decisions := svc.cpSatDecisions(context.Background(), snapshot)
			if len(decisions) != 1 || decisions[0].PolicyVersion != "cp-sat@v1" || len(decisions[0].Fallbacks) != 1 {
				t.Fatalf("expected safe fallback, got %+v", decisions)
			}
		})
	}
}

func TestCPSATServicePathFallsBackWhenSolverTimesOut(t *testing.T) {
	svc := &Service{CPPlanner: cpPlannerFunc(func(context.Context, map[string]any) (CPProposal, error) {
		return CPProposal{}, errors.New("deadline exceeded")
	})}
	decisions := svc.cpSatDecisions(context.Background(), cpFixture())
	if len(decisions) != 1 || len(decisions[0].Fallbacks) != 1 || decisions[0].Action != policy.ActionPlace {
		t.Fatalf("expected deterministic deadline-shortest fallback, got %+v", decisions)
	}
}

func TestCPSATWaitsForProposalFutureStart(t *testing.T) {
	snapshot := cpFixture()
	proposal, _ := cpValidProposal(snapshot)
	proposal.Placements[0].StartOffsetS = 10
	proposal.Placements[0].FinishOffsetS = 70
	svc := &Service{CPPlanner: cpPlannerFunc(func(_ context.Context, _ map[string]any) (CPProposal, error) { return proposal, nil })}
	decisions := svc.cpSatDecisions(context.Background(), snapshot)
	if len(decisions) != 1 || decisions[0].Action != policy.ActionWait || decisions[0].ExpectedStart == nil ||
		!decisions[0].ExpectedStart.Equal(snapshot.Now.Add(10*time.Second)) {
		t.Fatalf("expected future wait decision, got %+v", decisions)
	}
}

func TestExecCPPlannerAgainstPythonFixtureWhenConfigured(t *testing.T) {
	python, script := os.Getenv("GPUB_CP_SAT_TEST_PYTHON"), os.Getenv("GPUB_CP_SAT_TEST_SCRIPT")
	if python == "" || script == "" {
		t.Skip("set GPUB_CP_SAT_TEST_PYTHON and GPUB_CP_SAT_TEST_SCRIPT for the Python planner contract")
	}
	snapshot := cpFixture()
	payload, version, digest := (ExecCPPlanner{}).Snapshot(snapshot, snapshot.Jobs)
	proposal, err := (ExecCPPlanner{Python: python, Script: script, Timeout: 3 * time.Second, SolverTimeout: 250 * time.Millisecond}).Propose(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if proposal.SnapshotVersion != version || proposal.SnapshotDigest != digest || len(proposal.Placements) != 1 {
		t.Fatalf("planner output does not bind to input snapshot: %+v", proposal)
	}
}
