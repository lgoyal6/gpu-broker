package domain

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"time"
)

// LedgerKind is one of three movements. Their sum per job is the invariant:
//
//	held = settled + released + outstanding, outstanding >= 0
//
// and once the job is terminal, outstanding == 0 and no further HOLD or
// SETTLE is accepted, so a finished job cannot consume future budget.
type LedgerKind string

const (
	LedgerHold    LedgerKind = "HOLD"
	LedgerSettle  LedgerKind = "SETTLE"
	LedgerRelease LedgerKind = "RELEASE"
)

type LedgerEntry struct {
	ProjectID ProjectID
	JobID     JobID
	AttemptID AttemptID
	Kind      LedgerKind
	Amount    MicroUSD // always positive; Kind gives the direction
	At        time.Time
}

// JobBudget is the per-job projection of the ledger.
type JobBudget struct {
	Held, Settled, Released MicroUSD
	Closed                  bool
}

func (b JobBudget) Outstanding() MicroUSD { return b.Held - b.Settled - b.Released }

var (
	ErrBudgetClosed   = errors.New("job budget is closed: the job is finished")
	ErrSettleTooLarge = errors.New("settle exceeds the outstanding hold")
	ErrNonPositive    = errors.New("ledger amount must be positive")
)

// ApplyLedger validates one entry against the job's projection and returns
// the new projection. The store calls it inside the transaction that writes
// the entry, with the job row locked.
func ApplyLedger(b JobBudget, e LedgerEntry) (JobBudget, error) {
	if e.Amount <= 0 {
		return b, ErrNonPositive
	}
	if b.Closed {
		return b, ErrBudgetClosed
	}
	switch e.Kind {
	case LedgerHold:
		b.Held += e.Amount
	case LedgerSettle:
		if e.Amount > b.Outstanding() {
			return b, fmt.Errorf("%w: settle %s, outstanding %s", ErrSettleTooLarge, e.Amount, b.Outstanding())
		}
		b.Settled += e.Amount
	case LedgerRelease:
		if e.Amount > b.Outstanding() {
			return b, fmt.Errorf("%w: release %s, outstanding %s", ErrSettleTooLarge, e.Amount, b.Outstanding())
		}
		b.Released += e.Amount
	default:
		return b, fmt.Errorf("unknown ledger kind %q", e.Kind)
	}
	return b, nil
}

// CloseBudget releases whatever is outstanding and closes the projection. It
// returns the release entry to write (zero Amount when nothing was left).
func CloseBudget(b JobBudget, e LedgerEntry) (JobBudget, LedgerEntry) {
	e.Kind = LedgerRelease
	e.Amount = b.Outstanding()
	b.Released += e.Amount
	b.Closed = true
	return b, e
}

// CostOf is the charge for gpus over d at rate per GPU-hour, rounded up to
// the micro-dollar so a settle is never under-charged by truncation.
func CostOf(rate MicroUSD, gpus int, d time.Duration) MicroUSD {
	if d <= 0 || gpus <= 0 || rate <= 0 {
		return 0
	}
	// rate*gpus*ns overflows int64 within a day at datacenter prices, so the
	// product is taken in 128 bits.
	hi, lo := bits.Mul64(uint64(rate)*uint64(gpus), uint64(d))
	lo, carry := bits.Add64(lo, uint64(time.Hour)-1, 0)
	hi += carry
	if hi >= uint64(time.Hour) {
		return MicroUSD(math.MaxInt64)
	}
	q, _ := bits.Div64(hi, lo, uint64(time.Hour))
	if q > math.MaxInt64 {
		return MicroUSD(math.MaxInt64)
	}
	return MicroUSD(q)
}

// ProjectAvailable is what a new hold may take: the budget less outstanding
// holds and settled spend.
func ProjectAvailable(budget, outstandingHolds, settled MicroUSD) MicroUSD {
	return budget - outstandingHolds - settled
}
