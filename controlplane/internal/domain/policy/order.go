package policy

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

type orderedJob struct {
	job domain.Job
	key OrderKey
}

// orderJobs sorts the queue by the policy's ordering. Every mode ends in the
// same total tiebreak (submit time, then id) so two ticks over the same
// snapshot cannot disagree about who is next.
func orderJobs(s Snapshot, p Policy) []orderedJob {
	jobs := make([]orderedJob, len(s.Jobs))
	shares := usageShares(s.TenantUsage)
	for i, j := range s.Jobs {
		jobs[i] = orderedJob{job: j, key: orderKey(j, s.Now, shares, p)}
	}
	sort.SliceStable(jobs, func(a, b int) bool {
		x, y := jobs[a], jobs[b]
		if x.key.Score != y.key.Score {
			return x.key.Score > y.key.Score
		}
		if !x.job.SubmittedAt.Equal(y.job.SubmittedAt) {
			return x.job.SubmittedAt.Before(y.job.SubmittedAt)
		}
		return x.job.ID < y.job.ID
	})
	for i := range jobs {
		jobs[i].key.Position = i + 1
	}
	return jobs
}

func usageShares(usage map[domain.TenantID]float64) map[domain.TenantID]float64 {
	total := 0.0
	for _, u := range usage {
		total += u
	}
	out := make(map[domain.TenantID]float64, len(usage))
	if total <= 0 {
		return out
	}
	for t, u := range usage {
		out[t] = u / total
	}
	return out
}

// FairShareScore is the Python broker's priority function, term for term.
func FairShareScore(fs FairShare, share float64, wait time.Duration) (score, fairTerm, ageTerm float64) {
	fairTerm = fs.WFair * (1 - share)
	ageTerm = fs.WAge * math.Min(1, wait.Hours()/fs.AgeMax.Hours())
	return fairTerm + ageTerm, fairTerm, ageTerm
}

func orderKey(j domain.Job, now time.Time, shares map[domain.TenantID]float64, p Policy) OrderKey {
	wait := now.Sub(j.SubmittedAt)
	if wait < 0 {
		wait = 0
	}
	switch p.Ordering {
	case OrderFIFO:
		// Higher score sorts first, so FIFO is "older is higher". The constant
		// score makes the tiebreak (submit time) the whole ordering.
		return OrderKey{Score: 0, Terms: []string{"fifo: submit time"}}
	case OrderPriorityAging:
		boost := math.Min(p.MaxAgingBoost, float64(wait)/float64(p.AgingStep))
		eff := float64(j.Priority) + boost
		return OrderKey{Score: round6(eff), Terms: []string{
			fmt.Sprintf("priority %d", j.Priority),
			fmt.Sprintf("aging +%.3f (cap %.0f)", boost, p.MaxAgingBoost),
		}}
	case OrderFairShare:
		share := shares[j.TenantID]
		sc, f, a := FairShareScore(p.FairShare, share, wait)
		terms := []string{
			fmt.Sprintf("fair %.3f (usage share %.3f)", f, share),
			fmt.Sprintf("age %.3f (waited %s)", a, wait.Round(time.Second)),
		}
		if p.PriorityWeight > 0 {
			sc += p.PriorityWeight * float64(j.Priority)
			terms = append(terms, fmt.Sprintf("priority %d x %.1f", j.Priority, p.PriorityWeight))
		}
		return OrderKey{Score: round6(sc), Terms: terms}
	case OrderDeadline:
		if j.Deadline == nil {
			// No deadline sorts after every deadline job, oldest first. The
			// large negative base keeps it below any real slack.
			return OrderKey{Score: -1e12 + wait.Hours(), Terms: []string{"no deadline"}}
		}
		slack := j.Deadline.Sub(now) - j.MaxRuntime
		return OrderKey{Score: round6(-slack.Hours()), Terms: []string{
			fmt.Sprintf("slack %s", slack.Round(time.Second)),
		}}
	}
	return OrderKey{}
}
