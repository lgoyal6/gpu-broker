// Package policy turns a snapshot of the queue, the workers and the carbon
// and price data into one typed Decision per job. Decide is pure and
// deterministic for a fixed snapshot: it reads no clock, draws no random
// numbers, and never lets map iteration order reach its output.
package policy

import (
	"fmt"
	"sort"
	"time"
)

// Ordering is how the queue is sorted before placement.
type Ordering string

const (
	OrderFIFO          Ordering = "fifo"
	OrderPriorityAging Ordering = "priority-aging"
	OrderFairShare     Ordering = "fair-share"
	OrderDeadline      Ordering = "deadline"
)

// Weights scale the normalised score components. They need not sum to 1;
// decisions store raw, normalised and weighted values so the arithmetic is
// visible.
type Weights struct {
	Fit, Cost, Carbon, Deadline float64
}

// FairShare is the Python broker's formula, kept identical so a club trace
// replays the same ordering (see fairshare_parity_test.go):
//
//	score = WFair*(1 - usage_share) + WAge*min(1, wait/AgeMax)
type FairShare struct {
	WFair, WAge float64
	AgeMax      time.Duration
}

// Fallback is what a policy does when the carbon data it depends on is stale.
type Fallback string

const (
	FallbackCost     Fallback = "cost"
	FallbackDeadline Fallback = "deadline"
)

type Policy struct {
	Name     string
	Version  string
	Ordering Ordering
	Weights  Weights

	AgingStep     time.Duration // priority-aging: +1 priority per step waited
	MaxAgingBoost float64       // ... up to this many levels
	FairShare     FairShare
	// PriorityWeight adds weight*priority to the fair-share score. At 1.0 a
	// priority level outweighs the whole fair-share range [0, 1], so fair
	// share orders jobs within a priority class. A policy that preempts by
	// priority must order by it too, or a preempted job (older, so higher
	// age term) is placed ahead of the job that preempted it and the two
	// thrash. 0 keeps the Python broker's ordering exactly.
	PriorityWeight float64

	// StarvationGuard: a job that has waited this long protects one worker
	// from backfill until it can start. Zero disables the guard.
	StarvationGuard time.Duration

	AllowPreemption bool
	AllowDelay      bool
	// DelayMinImprovement is the fraction by which the pessimistic forecast
	// must beat the current intensity before a job is delayed.
	DelayMinImprovement float64
	CarbonMaxAge        time.Duration
	Fallback            Fallback
	// Emergency ignores delay and picks the earliest start regardless of cost.
	Emergency bool
}

func (p Policy) Validate() error {
	if p.Name == "" || p.Version == "" {
		return fmt.Errorf("policy needs a name and a version")
	}
	switch p.Ordering {
	case OrderFIFO, OrderPriorityAging, OrderFairShare, OrderDeadline:
	default:
		return fmt.Errorf("policy %s: unknown ordering %q", p.Name, p.Ordering)
	}
	if p.Ordering == OrderFairShare && p.FairShare.WAge < p.FairShare.WFair {
		// Same rule the Python config enforces: if the age term cannot
		// outweigh the fairness term, a heavy tenant can starve forever.
		return fmt.Errorf("policy %s: fair-share WAge (%.2f) must be >= WFair (%.2f) or heavy tenants starve",
			p.Name, p.FairShare.WAge, p.FairShare.WFair)
	}
	if p.Ordering == OrderPriorityAging && (p.AgingStep <= 0 || p.MaxAgingBoost <= 0) {
		return fmt.Errorf("policy %s: priority aging needs a positive step and bound", p.Name)
	}
	if p.AllowPreemption && p.Ordering == OrderFairShare && p.PriorityWeight < 1 {
		return fmt.Errorf("policy %s: preemption by priority needs PriorityWeight >= 1 or preempted jobs are re-placed first", p.Name)
	}
	if p.Weights.Fit < 0 || p.Weights.Cost < 0 || p.Weights.Carbon < 0 || p.Weights.Deadline < 0 {
		return fmt.Errorf("policy %s: weights must be non-negative", p.Name)
	}
	if p.Weights.Carbon > 0 && p.CarbonMaxAge <= 0 {
		return fmt.Errorf("policy %s: a carbon weight needs CarbonMaxAge", p.Name)
	}
	if p.Weights.Carbon > 0 && p.Fallback == "" {
		return fmt.Errorf("policy %s: a carbon weight needs an explicit fallback", p.Name)
	}
	return nil
}

var defaultFairShare = FairShare{WFair: 0.5, WAge: 0.5, AgeMax: 12 * time.Hour}

// Builtins are the named policies, in the order the product adds them. The
// version string is stored on every decision; change it whenever behaviour
// changes.
func Builtins() map[string]Policy {
	ps := []Policy{
		{Name: "fifo", Version: "fifo@v1", Ordering: OrderFIFO, Weights: Weights{Fit: 1}},
		{Name: "priority", Version: "priority@v1", Ordering: OrderPriorityAging,
			AgingStep: time.Hour, MaxAgingBoost: 5, Weights: Weights{Fit: 1}},
		{Name: "fair-share", Version: "fair-share@v1", Ordering: OrderFairShare,
			FairShare: defaultFairShare, StarvationGuard: 6 * time.Hour, Weights: Weights{Fit: 1}},
		{Name: "deadline-first", Version: "deadline-first@v1", Ordering: OrderDeadline,
			StarvationGuard: 6 * time.Hour, Weights: Weights{Fit: 0.3, Deadline: 1}},
		{Name: "lowest-cost", Version: "lowest-cost@v1", Ordering: OrderFairShare,
			FairShare: defaultFairShare, StarvationGuard: 6 * time.Hour,
			Weights: Weights{Fit: 0.2, Cost: 1}},
		{Name: "lowest-carbon", Version: "lowest-carbon@v1", Ordering: OrderFairShare,
			FairShare: defaultFairShare, StarvationGuard: 6 * time.Hour,
			Weights: Weights{Fit: 0.2, Carbon: 1}, AllowDelay: true, DelayMinImprovement: 0.15,
			CarbonMaxAge: 2 * time.Hour, Fallback: FallbackCost},
		{Name: "balanced", Version: "balanced@v1", Ordering: OrderFairShare,
			FairShare: defaultFairShare, StarvationGuard: 6 * time.Hour,
			Weights: Weights{Fit: 0.3, Cost: 0.4, Carbon: 0.4, Deadline: 0.3}, AllowDelay: true,
			DelayMinImprovement: 0.2, CarbonMaxAge: 2 * time.Hour, Fallback: FallbackCost,
			AllowPreemption: true, PriorityWeight: 1},
		{Name: "emergency", Version: "emergency@v1", Ordering: OrderPriorityAging,
			AgingStep: time.Hour, MaxAgingBoost: 5, Weights: Weights{Fit: 1}, Emergency: true,
			AllowPreemption: true},
	}
	out := make(map[string]Policy, len(ps))
	for _, p := range ps {
		if err := p.Validate(); err != nil {
			panic(err) // a builtin that fails validation is a programming error
		}
		out[p.Name] = p
	}
	return out
}

// BuiltinNames returns the builtin policy names, sorted.
func BuiltinNames() []string {
	names := make([]string, 0)
	for n := range Builtins() {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Lookup returns a builtin by name.
func Lookup(name string) (Policy, error) {
	p, ok := Builtins()[name]
	if !ok {
		return Policy{}, fmt.Errorf("unknown policy %q (have %v)", name, BuiltinNames())
	}
	return p, nil
}
