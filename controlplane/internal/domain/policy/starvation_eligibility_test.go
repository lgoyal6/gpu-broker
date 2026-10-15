package policy

import (
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

func TestStarvationHoldRequiresNonCapacityEligibility(t *testing.T) {
	for _, mode := range []string{"eligible", "project_budget", "job_cap", "last_interruptible_attempt", "tenant_quota"} {
		t.Run(mode, func(t *testing.T) {
			s := twoRegionSnapshot()
			s.Workers = []domain.Worker{mkWorker("w", "south", 1, 0)}
			old := mkJob("old", "a", 1, t0.Add(-7*time.Hour))
			ready := mkJob("ready", "b", 1, t0.Add(-time.Minute))
			s.ProjectAvailable = map[domain.ProjectID]domain.MicroUSD{old.ProjectID: 100_000_000, ready.ProjectID: 100_000_000}
			s.TenantUsage = map[domain.TenantID]float64{"a": 100}
			switch mode {
			case "project_budget":
				s.ProjectAvailable[old.ProjectID] = 0
			case "job_cap":
				old.BudgetCap = 1
			case "last_interruptible_attempt":
				pool := s.Pools["south"]
				pool.Interruptible = true
				s.Pools["south"] = pool
				old.MaxAttempts = 1
			case "tenant_quota":
				s.TenantMaxGPUs = map[domain.TenantID]int{"a": 1}
				s.TenantActiveGPUs = map[domain.TenantID]int{"a": 1}
			}
			s.Jobs = []domain.Job{old, ready}
			decisions := map[domain.JobID]Action{}
			for _, d := range Decide(s, mustPolicy(t, "fair-share")) {
				decisions[d.JobID] = d.Action
			}
			if mode == "eligible" {
				if decisions[old.ID] != ActionPlace || decisions[ready.ID] != ActionWait {
					t.Fatalf("eligible head lost protection: %+v", decisions)
				}
			} else if decisions[old.ID] != ActionWait || decisions[ready.ID] != ActionPlace {
				t.Fatalf("ineligible head held ready capacity: %+v", decisions)
			}
		})
	}
}
