package policy

import (
	"fmt"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

// simulateBigJob runs Decide tick by tick on one 8-GPU worker: a big job
// needing all 8 GPUs arrives first, then a 1-GPU, 1-hour job arrives every 10
// minutes from rotating light tenants forever. It returns when the big job
// started, or -1 if it never did within the horizon.
func simulateBigJob(t *testing.T, p Policy, horizon time.Duration) time.Duration {
	t.Helper()
	tick := 5 * time.Minute
	w := mkWorker("w", "south", 8, 0)
	pools := twoRegionSnapshot().Pools
	type run struct {
		id  domain.JobID
		end time.Time
		n   int
	}
	var running []run
	queue := []domain.Job{mkJob("big", "heavy", 8, t0)}
	usage := map[domain.TenantID]float64{"heavy": 500}
	next := 0
	for now := t0; now.Sub(t0) < horizon; now = now.Add(tick) {
		if now.Sub(t0)%(10*time.Minute) == 0 {
			tn := domain.TenantID(fmt.Sprintf("light%d", next%4))
			queue = append(queue, mkJob(fmt.Sprintf("s%04d", next), string(tn), 1, now))
			next++
		}
		kept := running[:0]
		for _, r := range running {
			if now.Before(r.end) {
				kept = append(kept, r)
			} else {
				w.GPUsReserved -= r.n
			}
		}
		running = kept
		ds := Decide(Snapshot{Now: now, Jobs: queue, Workers: []domain.Worker{w}, Pools: pools, TenantUsage: usage}, p)
		placed := map[domain.JobID]bool{}
		for _, d := range ds {
			if d.Action != ActionPlace {
				continue
			}
			placed[d.JobID] = true
			for _, j := range queue {
				if j.ID == d.JobID {
					if j.ID == "big" {
						return now.Sub(t0)
					}
					w.GPUsReserved += j.Requirements.GPUs
					running = append(running, run{j.ID, now.Add(time.Hour), j.Requirements.GPUs})
					usage[j.TenantID] += 1
				}
			}
		}
		rest := queue[:0]
		for _, j := range queue {
			if !placed[j.ID] {
				rest = append(rest, j)
			}
		}
		queue = rest
	}
	return -1
}

func TestStarvationGuardBoundsTheWaitOfALargeJob(t *testing.T) {
	p := mustPolicy(t, "fair-share")
	got := simulateBigJob(t, p, 72*time.Hour)
	// Guard (6h) + longest small job (1h) + one tick.
	bound := p.StarvationGuard + time.Hour + 5*time.Minute
	if got < 0 || got > bound {
		t.Fatalf("big job started after %v, bound %v", got, bound)
	}
	t.Logf("big job started after %v (bound %v)", got, bound)
}

// Negative control: the same workload with the guard off starves the big
// job for the whole horizon. Without this the test above could pass for a
// reason unrelated to the guard.
func TestWithoutGuardTheLargeJobStarves(t *testing.T) {
	p := mustPolicy(t, "fair-share")
	p.StarvationGuard = 0
	if got := simulateBigJob(t, p, 72*time.Hour); got >= 0 {
		t.Fatalf("big job started after %v with no guard; the workload no longer exercises starvation", got)
	}
}
