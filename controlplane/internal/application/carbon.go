package application

import (
	"context"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

type CarbonSource interface {
	Fetch(context.Context, time.Time) ([]domain.CarbonSnapshot, error)
}

// RefreshCarbon performs network I/O before entering a retryable transaction.
// A provider failure leaves the last observations untouched, allowing the
// scheduler's freshness rule to trigger its explicit fallback.
func RefreshCarbon(ctx context.Context, store Store, source CarbonSource, now time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	readings, err := source.Fetch(ctx, now)
	if err != nil {
		return err
	}
	return store.InTx(ctx, func(tx Tx) error { return tx.UpsertCarbon(ctx, readings) })
}
