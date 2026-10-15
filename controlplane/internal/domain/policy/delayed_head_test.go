package policy

import (
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

func TestDelayedHeadProtectsCapacityOnlyWhenReady(t *testing.T) {
	for _, mode := range []string{"future", "ready", "emergency"} {
		t.Run(mode, func(t *testing.T) {
			s := twoRegionSnapshot()
			s.Workers = []domain.Worker{mkWorker("w", "south", 1, 0)}
			old := mkJob("old", "a", 1, t0.Add(-7*time.Hour))
			future := t0.Add(time.Hour)
			if mode != "ready" {
				old.NotBefore = &future
			}
			ready := mkJob("ready", "b", 1, t0.Add(-time.Minute))
			s.Jobs = []domain.Job{old, ready}
			p := mustPolicy(t, "balanced")
			p.Emergency = mode == "emergency"
			decisions := map[domain.JobID]Action{}
			for _, d := range Decide(s, p) {
				decisions[d.JobID] = d.Action
			}
			if mode == "future" {
				if decisions[old.ID] != ActionWait || decisions[ready.ID] != ActionPlace {
					t.Fatalf("delayed head blocked ready capacity: %+v", decisions)
				}
			} else if decisions[old.ID] != ActionPlace || decisions[ready.ID] != ActionWait {
				t.Fatalf("ready head lost starvation protection: %+v", decisions)
			}
		})
	}
}
