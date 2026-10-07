package postgres_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/testkit"
)

// Competing reservations against the same worker, each for a different job:
// the capacity compare-and-swap must let exactly as many through as there
// are GPUs, however the transactions interleave.
func TestCapacityCompareAndSwapUnderContention(t *testing.T) {
	e := testkit.New(t)
	e.Worker(e.Pool("pool-a", "london", 1, false), "node-1", 3)
	var wid string
	if err := e.Store.Pool.QueryRow(e.Ctx, `SELECT id FROM workers`).Scan(&wid); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 20; round++ {
		e.Exec(`UPDATE workers SET gpus_reserved = 0`)
		var ok, conflict atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				err := e.Store.InTx(e.Ctx, func(tx application.Tx) error {
					return tx.ReserveCapacity(e.Ctx, domainWorker(wid), 1)
				})
				switch {
				case err == nil:
					ok.Add(1)
				case errors.Is(err, application.ErrCapacityConflict):
					conflict.Add(1)
				default:
					t.Error(err)
				}
			}()
		}
		close(start)
		wg.Wait()
		if ok.Load() != 3 || conflict.Load() != 13 {
			t.Fatalf("round %d: %d reserved, %d conflicts on a 3-GPU worker", round, ok.Load(), conflict.Load())
		}
		if n := e.QueryInt(`SELECT gpus_reserved FROM workers`); n != 3 {
			t.Fatalf("gpus_reserved %d", n)
		}
	}
}

// The database rejects overbooking even for a statement that skips the
// predicate: the CHECK constraint is the last line.
func TestOverbookingIsAConstraintViolation(t *testing.T) {
	e := testkit.New(t)
	e.Worker(e.Pool("pool-a", "london", 1, false), "node-1", 2)
	if _, err := e.Store.Pool.Exec(e.Ctx, `UPDATE workers SET gpus_reserved = 3`); err == nil {
		t.Fatal("gpus_reserved > gpus accepted")
	}
}

// Serialization failures are injected from the database itself (SQLSTATE
// 40001) on the first two attempts; InTx must retry and succeed, and report
// each retry to the hook the metrics layer counts.
func TestInTxRetriesSerializationFailures(t *testing.T) {
	e := testkit.New(t)
	var calls, hooks int
	e.Store.RetryHook = func(code string) {
		if code == "40001" {
			hooks++
		}
	}
	err := e.Store.InTx(e.Ctx, func(tx application.Tx) error {
		calls++
		if calls <= 2 {
			_, err := e.Store.Pool.Exec(context.Background(), `DO $$ BEGIN RAISE EXCEPTION 'injected' USING ERRCODE = '40001'; END $$`)
			return err
		}
		return nil
	})
	if err != nil || calls != 3 || hooks != 2 {
		t.Fatalf("err=%v calls=%d hooks=%d", err, calls, hooks)
	}
	// A non-retryable error is returned at once.
	calls = 0
	err = e.Store.InTx(e.Ctx, func(tx application.Tx) error {
		calls++
		return application.ErrInvalid
	})
	if !errors.Is(err, application.ErrInvalid) || calls != 1 {
		t.Fatalf("non-retryable: %v after %d calls", err, calls)
	}
}

func TestMigrateIsIdempotentAndDetectsEditedMigrations(t *testing.T) {
	e := testkit.New(t)
	applied, err := e.Store.Migrate(e.Ctx)
	if err != nil || len(applied) != 0 {
		t.Fatalf("second migrate applied %v (%v)", applied, err)
	}
	e.Exec(`UPDATE schema_migrations SET checksum = 'edited' WHERE version = '0001_init'`)
	if _, err := e.Store.Migrate(e.Ctx); err == nil {
		t.Fatal("edited migration not detected")
	}
	if err := e.Store.SchemaReady(e.Ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDoubleReleaseReturnsCapacityOnce(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 100)
	e.Worker(e.Pool("pool-a", "london", 1, false), "node-1", 2)
	j := e.Submit(user, 2)
	e.Tick()
	var released atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := e.Store.InTx(e.Ctx, func(tx application.Tx) error {
				r, err := tx.ActiveReservation(e.Ctx, j.ID)
				if errors.Is(err, application.ErrNotFound) {
					return nil
				}
				if err != nil {
					return err
				}
				ok, err := tx.ReleaseReservation(e.Ctx, r.ID, testkit.T0, "test")
				if err != nil || !ok {
					return err
				}
				released.Add(1)
				return tx.ReleaseCapacity(e.Ctx, r.WorkerID, r.GPUs)
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if released.Load() != 1 || e.QueryInt(`SELECT gpus_reserved FROM workers`) != 0 {
		t.Fatalf("released %d times, gpus_reserved %d", released.Load(), e.QueryInt(`SELECT gpus_reserved FROM workers`))
	}
}

// Every role registers the builtin policies at startup at the same moment.
func TestConcurrentPolicyRegistrationAndVersionImmutability(t *testing.T) {
	e := testkit.New(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.Svc.RegisterPolicies(e.Ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	e.Exec(`UPDATE scheduling_policies SET spec = jsonb_set(spec, '{Weights,Cost}', '9') WHERE version = 'balanced@v1'`)
	if err := e.Svc.RegisterPolicies(e.Ctx); !errors.Is(err, application.ErrConflict) {
		t.Fatalf("changed spec under the same version accepted: %v", err)
	}
}
