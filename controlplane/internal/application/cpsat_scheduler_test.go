package application_test

import (
	"context"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/testkit"
)

type cpProposalFunc func(context.Context, map[string]any) (application.CPProposal, error)

func (f cpProposalFunc) Propose(ctx context.Context, input map[string]any) (application.CPProposal, error) {
	return f(ctx, input)
}

func TestCPSATPolicyAppliesFeasiblePlanThroughAuthoritativeService(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("cp-sat", 100)
	e.Worker(e.Pool("pool-cp", "us-central1", 1, false), "worker-cp", 1)
	job := e.Submit(user, 1, func(req *application.SubmitRequest) {
		req.Policy, req.MaxRuntime = "cp-sat", time.Minute
	})
	snapshot, err := e.Svc.Snapshot(e.Ctx)
	e.Must(err)
	_, version, digest := (application.ExecCPPlanner{}).Snapshot(snapshot, snapshot.Jobs)
	worker := snapshot.Workers[0]
	price := int64(snapshot.Pools[worker.PoolID].PriceMicroUSDPerGPUHour)
	reserved := int64((1*60*price + 3599) / 3600)
	e.Svc.CPPlanner = cpProposalFunc(func(_ context.Context, _ map[string]any) (application.CPProposal, error) {
		return application.CPProposal{Schema: "gpu-broker.placement-proposal/v1", SnapshotVersion: version,
			SnapshotDigest: digest, SolverStatus: "OPTIMAL", Placements: []application.CPPlacement{{
				JobID: string(job.ID), WorkerID: string(worker.ID), StartOffsetS: 0, FinishOffsetS: 60,
				ReservedMicroUSD: reserved,
			}}}, nil
	})
	result := e.Tick()
	if result.Applied["PLACE"] != 1 {
		t.Fatalf("CP-SAT service tick did not place job: %+v", result)
	}
	actual, err := e.Svc.GetJob(e.Ctx, user, job.ID)
	e.Must(err)
	if actual.Job.State != domain.JobReserved {
		t.Fatalf("job state %s, want RESERVED", actual.Job.State)
	}
}
