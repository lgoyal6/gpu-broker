package status

import (
	"context"
	"errors"
	"testing"
	"time"
)

type outageSource struct {
	at   time.Time
	down bool
}

func (s *outageSource) Read(context.Context) ([]PoolRow, QueueRow, time.Time, error) {
	if s.down {
		return nil, QueueRow{}, time.Time{}, errors.New("injected database outage")
	}
	return nil, QueueRow{}, s.at, nil
}

func TestCachedStatusPublishesAdvancingAgeDuringDatabaseOutage(t *testing.T) {
	clock := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	source := &outageSource{at: clock}
	var ages []time.Duration
	server := New(source, time.Second, func() time.Time { return clock }, func(age time.Duration) { ages = append(ages, age) })
	first, err := server.snapshot(context.Background())
	if err != nil || len(ages) != 1 || ages[0] != 0 {
		t.Fatalf("fresh snapshot: err=%v ages=%v", err, ages)
	}
	source.down = true
	for _, elapsed := range []time.Duration{10 * time.Second, 3 * time.Minute} {
		clock = source.at.Add(elapsed)
		cached, err := server.snapshot(context.Background())
		if err != nil || cached != first || cached.GeneratedAt != source.at || len(ages) < 2 || ages[len(ages)-1] != elapsed {
			t.Fatalf("stale status age stopped advancing: elapsed=%v err=%v ages=%v", elapsed, err, ages)
		}
	}
	if _, err := New(source, time.Second, func() time.Time { return clock }, nil).snapshot(context.Background()); err == nil {
		t.Fatal("initial read outage returned a fabricated snapshot")
	}
}
