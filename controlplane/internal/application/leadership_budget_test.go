package application_test

import (
	"errors"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/testkit"
	"sync"
	"testing"
	"time"
)

func TestLeaderReplacementPreservesInflightWorkerCompletion(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 100)
	w := e.Worker(e.Pool("pool-a", "london", 1, false), "node-1", 2)
	old, err := e.Store.TryLead(e.Ctx, "old")
	e.Must(err)
	t.Cleanup(func() { old.Release(e.Ctx) })
	j := e.Submit(user, 1)
	_, err = e.Svc.Tick(e.Ctx, old.Epoch)
	e.Must(err)
	ds, err := e.Svc.ClaimDispatches(e.Ctx, w, 1)
	e.Must(err)
	if len(ds) != 1 {
		t.Fatalf("dispatches: %+v", ds)
	}
	_, err = e.Svc.Ack(e.Ctx, w, ds[0])
	e.Must(err)
	old.Release(e.Ctx)
	next, err := e.Store.TryLead(e.Ctx, "replacement")
	e.Must(err)
	if next == nil || next.Epoch <= old.Epoch {
		t.Fatalf("replacement: %+v", next)
	}
	defer next.Release(e.Ctx)
	e.Clock.Advance(time.Minute)
	for i := range 2 {
		out, err := e.Svc.ReportEvents(e.Ctx, w, ds[0].AttemptID, []application.WorkerEvent{exitEvent(int64(i+1), domain.OutcomeSucceeded, 0)})
		e.Must(err)
		if out != domain.OutcomeSucceeded {
			t.Fatalf("completion under new leader: %s", out)
		}
	}
	if e.Job(user, j.ID).State != domain.JobSucceeded || e.QueryInt(`SELECT count(*) FROM budget_ledger WHERE kind = 'SETTLE'`) != 1 || e.QueryInt(`SELECT sum(gpus_reserved) FROM workers`) != 0 {
		t.Fatal("leader replacement changed completion, settlement or capacity")
	}
	e.Submit(user, 1)
	if _, err := e.Svc.Tick(e.Ctx, old.Epoch); !errors.Is(err, application.ErrFenced) {
		t.Fatalf("old leader placed replacement work: %v", err)
	}
	if n := e.QueryInt(`SELECT count(*) FROM reservations`); n != 1 {
		t.Fatalf("stale leader created another reservation: %d", n)
	}
	result, err := e.Svc.Tick(e.Ctx, next.Epoch)
	e.Must(err)
	if result.Applied["PLACE"] != 1 {
		t.Fatalf("replacement leader cannot place valid work: %+v", result)
	}
}

func TestConcurrentJobsCannotOverspendOneProject(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 1)
	boot := e.Pool("pool-a", "london", 1, false)
	e.Worker(boot, "node-1", 1)
	e.Worker(boot, "node-2", 1)
	for range 8 {
		e.Submit(user, 1)
	}
	epoch := e.Lead()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := e.Svc.Tick(e.Ctx, epoch); err != nil {
				t.Errorf("concurrent scheduling: %v", err)
			}
		})
	}
	wg.Wait()
	if held := e.QueryInt(`SELECT coalesce(sum(amount), 0) FROM budget_ledger WHERE kind = 'HOLD'`); held != 1_000_000 {
		t.Fatalf("project holds %d, budget 1000000", held)
	}
	if active := e.QueryInt(`SELECT count(*) FROM reservations WHERE released_at IS NULL`); active != 1 {
		t.Fatalf("%d active reservations with budget for one job", active)
	}
	if queued := e.QueryInt(`SELECT count(*) FROM jobs WHERE state = 'QUEUED'`); queued != 7 {
		t.Fatalf("budget refusals lost queued work: %d", queued)
	}
}
