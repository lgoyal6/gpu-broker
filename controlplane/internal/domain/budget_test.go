package domain

import (
	"errors"
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

func TestCostOfRoundsUpAndDoesNotOverflow(t *testing.T) {
	if got := CostOf(1_000_000, 1, time.Hour); got != 1_000_000 {
		t.Fatalf("1 GPU-hour at $1: %v", got)
	}
	if got := CostOf(1_000_000, 1, time.Nanosecond); got != 1 {
		t.Fatalf("a nanosecond must round up to one micro-dollar, got %v", got)
	}
	// $98.32/GPU-h (p5.48xlarge-class) x 8 GPUs x 30 days overflows int64 if
	// computed in nanoseconds; the 128-bit path must get it exactly.
	got := CostOf(98_320_000, 8, 30*24*time.Hour)
	if want := MicroUSD(98_320_000 * 8 * 30 * 24); got != want {
		t.Fatalf("got %v want %v", got, want)
	}
	if got := CostOf(math.MaxInt64, 1000, 1000*time.Hour); got != math.MaxInt64 {
		t.Fatalf("overflow must saturate, got %v", got)
	}
}

// A random sequence of ledger operations, then close. Whatever was accepted,
// held == settled + released at close, nothing is ever negative, and a closed
// budget accepts nothing.
func TestLedgerInvariantsUnderRandomSequences(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 11))
	kinds := []LedgerKind{LedgerHold, LedgerSettle, LedgerRelease}
	for run := 0; run < 5000; run++ {
		var b JobBudget
		for i := 0; i < 20; i++ {
			e := LedgerEntry{Kind: kinds[r.IntN(3)], Amount: MicroUSD(r.IntN(200) - 10)}
			nb, err := ApplyLedger(b, e)
			if err != nil {
				if nb != b {
					t.Fatalf("refused entry changed the projection")
				}
				continue
			}
			b = nb
			if b.Outstanding() < 0 || b.Settled < 0 || b.Released < 0 {
				t.Fatalf("negative projection %+v", b)
			}
		}
		b, rel := CloseBudget(b, LedgerEntry{})
		if b.Outstanding() != 0 || b.Held != b.Settled+b.Released {
			t.Fatalf("close left %v outstanding: %+v", b.Outstanding(), b)
		}
		if rel.Kind != LedgerRelease || rel.Amount < 0 {
			t.Fatalf("bad release %+v", rel)
		}
		for _, k := range kinds {
			if _, err := ApplyLedger(b, LedgerEntry{Kind: k, Amount: 1}); !errors.Is(err, ErrBudgetClosed) {
				t.Fatalf("closed budget accepted %s: %v", k, err)
			}
		}
	}
}

func TestSettleCannotExceedHold(t *testing.T) {
	b, _ := ApplyLedger(JobBudget{}, LedgerEntry{Kind: LedgerHold, Amount: 100})
	if _, err := ApplyLedger(b, LedgerEntry{Kind: LedgerSettle, Amount: 101}); !errors.Is(err, ErrSettleTooLarge) {
		t.Fatalf("over-settle accepted: %v", err)
	}
}
