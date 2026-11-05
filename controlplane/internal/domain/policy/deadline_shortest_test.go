package policy

import (
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

func TestDeadlineShortestOrdersByPredictedRuntimeThenDeadline(t *testing.T) {
	short := mkJob("short", "a", 1, t0)
	short.MaxRuntime = 2 * time.Minute
	short.Deadline = ptrTime(t0.Add(40 * time.Minute))
	long := mkJob("long", "a", 1, t0)
	long.MaxRuntime = 20 * time.Minute
	long.Deadline = ptrTime(t0.Add(10 * time.Minute))
	s := Snapshot{Now: t0, Jobs: []domain.Job{long, short}}
	ordered := orderJobs(s, mustPolicy(t, "deadline-shortest"))
	if ordered[0].job.ID != "short" {
		t.Fatalf("shortest job should lead, got %s", ordered[0].job.ID)
	}
}

func ptrTime(t time.Time) *time.Time { return &t }
