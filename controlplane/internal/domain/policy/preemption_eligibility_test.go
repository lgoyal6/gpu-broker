package policy

import (
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

func TestPreemptionRequiresAnEligiblePlacementAfterVictimRemoval(t *testing.T) {
	for _, mode := range []string{"affordable", "project_budget", "job_cap", "last_interruptible_attempt"} {
		t.Run(mode, func(t *testing.T) {
			s := twoRegionSnapshot()
			s.Workers = []domain.Worker{mkWorker("w", "south", 1, 1)}
			job := mkJob("high", "a", 1, t0.Add(-time.Minute))
			job.Priority = 9
			s.ProjectAvailable = map[domain.ProjectID]domain.MicroUSD{job.ProjectID: 10_000_000}
			s.Running = []RunningJob{{JobID: "victim", TenantID: "b", WorkerID: "w", GPUs: 1, Priority: 0, Preemptible: true, StartedAt: t0.Add(-time.Minute)}}
			switch mode {
			case "project_budget":
				s.ProjectAvailable[job.ProjectID] = 0
			case "job_cap":
				job.BudgetCap = 1
			case "last_interruptible_attempt":
				pool := s.Pools["south"]
				pool.Interruptible = true
				s.Pools["south"] = pool
				job.MaxAttempts = 1
			}
			s.Jobs = []domain.Job{job}
			d := Decide(s, mustPolicy(t, "balanced"))[0]
			want := ActionWait
			if mode == "affordable" {
				want = ActionPreempt
			}
			if d.Action != want {
				t.Fatalf("ineligible preemption: %+v, want %s", d, want)
			}
		})
	}
}
