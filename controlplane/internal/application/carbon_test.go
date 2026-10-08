package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

type carbonSourceStub struct {
	rows  []domain.CarbonSnapshot
	err   error
	calls int
}

func (s *carbonSourceStub) Fetch(context.Context, time.Time) ([]domain.CarbonSnapshot, error) {
	s.calls++
	return s.rows, s.err
}

type carbonStoreStub struct {
	tx      carbonTxStub
	entered bool
	retry   bool
	err     error
}

func (s *carbonStoreStub) InTx(ctx context.Context, f func(Tx) error) error {
	s.entered = true
	if s.err != nil {
		return s.err
	}
	if s.retry {
		if err := f(&s.tx); err != nil {
			return err
		}
	}
	return f(&s.tx)
}

type carbonTxStub struct {
	Tx
	rows []domain.CarbonSnapshot
}

func (s *carbonTxStub) UpsertCarbon(_ context.Context, rows []domain.CarbonSnapshot) error {
	s.rows = rows
	return nil
}

func TestCarbonRefreshDoesNotFetchInsideTransactionRetry(t *testing.T) {
	row := domain.CarbonSnapshot{Region: "gb-london", At: time.Now(), GramsPerKWh: 0, Source: "eso-regional-estimate"}
	source := &carbonSourceStub{rows: []domain.CarbonSnapshot{row}}
	store := &carbonStoreStub{retry: true}
	if err := RefreshCarbon(context.Background(), store, source, time.Now()); err != nil {
		t.Fatal(err)
	}
	if source.calls != 1 || len(store.tx.rows) != 1 || store.tx.rows[0] != row {
		t.Fatal("transaction retry refetched or changed observations")
	}
}

func TestCarbonRefreshPreservesStateOnProviderFailure(t *testing.T) {
	want := errors.New("provider unavailable")
	store := &carbonStoreStub{}
	if err := RefreshCarbon(context.Background(), store, &carbonSourceStub{err: want}, time.Now()); !errors.Is(err, want) || store.entered {
		t.Fatalf("failed provider wrote to store: %v", err)
	}
	store.err = want
	if err := RefreshCarbon(context.Background(), store, &carbonSourceStub{}, time.Now()); !errors.Is(err, want) {
		t.Fatalf("database error lost: %v", err)
	}
}
