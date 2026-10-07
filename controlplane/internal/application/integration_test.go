package application_test

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/testkit"
)

func exitEvent(seq int64, o domain.AttemptOutcome, code int) application.WorkerEvent {
	return application.WorkerEvent{Seq: seq, Kind: "exit", Outcome: o, ExitCode: &code, Message: "exit"}
}

// runToCompletion drives one dispatch through ack and a terminal event.
func runAttempt(t *testing.T, e *testkit.Env, w application.Principal, o domain.AttemptOutcome) application.Dispatch {
	t.Helper()
	ds, err := e.Svc.ClaimDispatches(e.Ctx, w, 4)
	e.Must(err)
	if len(ds) != 1 {
		t.Fatalf("want 1 dispatch, got %d", len(ds))
	}
	res, err := e.Svc.Ack(e.Ctx, w, ds[0])
	e.Must(err)
	if res != application.AckRun {
		t.Fatalf("ack result %s", res)
	}
	e.Clock.Advance(30 * time.Minute)
	_, err = e.Svc.ReportEvents(e.Ctx, w, ds[0].AttemptID, []application.WorkerEvent{exitEvent(1, o, 0)})
	e.Must(err)
	return ds[0]
}

func states(trs []domain.TransitionRecord) []domain.JobState {
	out := []domain.JobState{domain.JobSubmitted}
	for _, r := range trs {
		out = append(out, r.To)
	}
	return out
}

func TestHappyPathRecordsEveryTransitionAndClosesTheLedger(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 100)
	boot := e.Pool("pool-a", "london", 2.0, false)
	w := e.Worker(boot, "node-1", 4)
	j := e.Submit(user, 2)
	if j.State != domain.JobQueued {
		t.Fatalf("submitted job is %s", j.State)
	}
	r := e.Tick()
	if r.Applied["PLACE"] != 1 {
		t.Fatalf("tick: %+v", r)
	}
	runAttempt(t, e, w, domain.OutcomeSucceeded)

	v, err := e.Svc.GetJob(e.Ctx, user, j.ID)
	e.Must(err)
	want := []domain.JobState{domain.JobSubmitted, domain.JobQueued, domain.JobReserved, domain.JobDispatched, domain.JobRunning, domain.JobSucceeded}
	if got := states(v.Transitions); !slices.Equal(got, want) {
		t.Fatalf("transitions %v want %v", got, want)
	}
	actors := []domain.ActorKind{domain.ActorOperator, domain.ActorScheduler, domain.ActorWorker, domain.ActorWorker, domain.ActorWorker}
	for i, tr := range v.Transitions {
		if tr.Actor.Kind != actors[i] || tr.Reason == "" || tr.CorrelationID == "" {
			t.Fatalf("transition %d: %+v", i, tr)
		}
	}
	// 2 GPUs x 30 min x $2 = $2 settled; the rest of the $4 hold released.
	if v.Budget.Settled != 2_000_000 || v.Budget.Held != 4_000_000 || v.Budget.Outstanding() != 0 {
		t.Fatalf("budget %+v", v.Budget)
	}
	if n := e.QueryInt(`SELECT gpus_reserved FROM workers`); n != 0 {
		t.Fatalf("capacity not returned: %d", n)
	}
	if n := e.QueryInt(`SELECT count(*) FROM leases`); n != 0 {
		t.Fatalf("lease left behind")
	}
}

// Two schedulers with fencing deliberately disabled (same epoch) race for the
// only GPU. Exactly one reservation may win, on every repetition.
func TestConcurrentReservationsExactlyOneWins(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 1000)
	boot := e.Pool("pool-a", "london", 1.0, false)
	e.Worker(boot, "node-1", 1)
	for i := 0; i < 12; i++ {
		e.Submit(user, 1)
	}
	epoch := e.Lead()
	var wg sync.WaitGroup
	results := make([]application.TickResult, 8)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := e.Svc.Tick(e.Ctx, epoch)
			if err != nil {
				t.Errorf("tick %d: %v", i, err)
			}
			results[i] = r
		}(i)
	}
	wg.Wait()
	placed, conflicts := 0, 0
	for _, r := range results {
		placed += r.Applied["PLACE"]
		conflicts += r.Conflicts
	}
	if placed != 1 {
		t.Fatalf("%d placements succeeded on a 1-GPU worker", placed)
	}
	if n := e.QueryInt(`SELECT count(*) FROM reservations WHERE released_at IS NULL`); n != 1 {
		t.Fatalf("%d active reservations", n)
	}
	if n := e.QueryInt(`SELECT gpus_reserved FROM workers`); n != 1 {
		t.Fatalf("gpus_reserved %d", n)
	}
	t.Logf("8 concurrent ticks: 1 placement, %d capacity conflicts, rest stale", conflicts)
}

func TestFencedLeaderCannotReserve(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 100)
	e.Worker(e.Pool("pool-a", "london", 1.0, false), "node-1", 2)
	old, err := e.Store.TryLead(e.Ctx, "old")
	e.Must(err)
	oldEpoch := old.Epoch
	old.Release(e.Ctx) // the old leader loses its connection...
	nl, err := e.Store.TryLead(e.Ctx, "new")
	e.Must(err)
	defer nl.Release(e.Ctx)
	e.Submit(user, 1)
	// ...but its process keeps running a tick with its stale epoch.
	if _, err := e.Svc.Tick(e.Ctx, oldEpoch); !errors.Is(err, application.ErrFenced) {
		t.Fatalf("stale leader tick: %v", err)
	}
	if n := e.QueryInt(`SELECT count(*) FROM reservations`); n != 0 {
		t.Fatalf("fenced leader reserved %d", n)
	}
	if r, err := e.Svc.Tick(e.Ctx, nl.Epoch); err != nil || r.Applied["PLACE"] != 1 {
		t.Fatalf("new leader: %+v %v", r, err)
	}
}

func TestOnlyOneProcessHoldsLeadership(t *testing.T) {
	e := testkit.New(t)
	a, err := e.Store.TryLead(e.Ctx, "a")
	e.Must(err)
	b, err := e.Store.TryLead(e.Ctx, "b")
	e.Must(err)
	if a == nil || b != nil {
		t.Fatalf("a=%v b=%v", a != nil, b != nil)
	}
	a.Release(e.Ctx)
	c, err := e.Store.TryLead(e.Ctx, "c")
	e.Must(err)
	if c == nil || c.Epoch != a.Epoch+1 {
		t.Fatalf("takeover epoch: %+v", c)
	}
	c.Release(e.Ctx)
}

func TestDoubleAckAndTamperedDispatchAreRefused(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 100)
	boot := e.Pool("pool-a", "london", 1.0, false)
	w := e.Worker(boot, "node-1", 2)
	other := e.Worker(boot, "node-2", 2)
	e.Submit(user, 2, func(r *application.SubmitRequest) { r.AllowedPools = []domain.PoolID{"pool-a"} })
	e.Tick()
	ds, err := e.Svc.ClaimDispatches(e.Ctx, w, 4)
	e.Must(err)
	if len(ds) == 0 {
		ds, err = e.Svc.ClaimDispatches(e.Ctx, other, 4)
		e.Must(err)
		w, other = other, w
	}
	d := ds[0]
	bad := d
	bad.Command = []string{"rm", "-rf", "/"}
	bad.SpecDigest = application.SpecDigest(bad.Image, bad.Command, bad.GPUs, bad.MaxRuntimeS)
	if _, err := e.Svc.Ack(e.Ctx, w, bad); !errors.Is(err, application.ErrLeaseInvalid) {
		t.Fatalf("tampered dispatch: %v", err)
	}
	if _, err := e.Svc.Ack(e.Ctx, other, d); !errors.Is(err, application.ErrForbidden) {
		t.Fatalf("another worker acked: %v", err)
	}
	if _, err := e.Svc.Ack(e.Ctx, w, d); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Svc.Ack(e.Ctx, w, d); !errors.Is(err, application.ErrAlreadyAcked) {
		t.Fatalf("second ack: %v", err)
	}
}

func TestQueueRedeliveryAfterAbandonedClaim(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 100)
	w := e.Worker(e.Pool("pool-a", "london", 1.0, false), "node-1", 2)
	j := e.Submit(user, 1)
	e.Tick()
	// A consumer claims the event and dies before handling it.
	e.Must(e.Store.InTx(e.Ctx, func(tx application.Tx) error {
		evs, err := tx.Claim(e.Ctx, "dispatch."+string(w.WorkerID), 4, e.Clock.Now(), 30*time.Second, "crashed")
		if len(evs) != 1 {
			t.Fatalf("claimed %d", len(evs))
		}
		return err
	}))
	if ds, _ := e.Svc.ClaimDispatches(e.Ctx, w, 4); len(ds) != 0 {
		t.Fatal("event redelivered inside its visibility timeout")
	}
	e.Clock.Advance(31 * time.Second)
	ds, err := e.Svc.ClaimDispatches(e.Ctx, w, 4)
	e.Must(err)
	if len(ds) != 1 || ds[0].JobID != j.ID {
		t.Fatalf("redelivery: %+v", ds)
	}
	if n := e.QueryInt(`SELECT attempts FROM outbox_events`); n != 2 {
		t.Fatalf("delivery attempts %d", n)
	}
	e.Clock.Advance(time.Minute)
	if ds, _ := e.Svc.ClaimDispatches(e.Ctx, w, 4); len(ds) != 0 {
		t.Fatal("delivered event redelivered")
	}
}

func TestWorkerDeathReclaimsLeaseAndRetriesAsNewAttempt(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 100)
	boot := e.Pool("pool-a", "london", 1.0, false)
	dead := e.Worker(boot, "node-1", 1)
	j := e.Submit(user, 1)
	e.Tick()
	ds, _ := e.Svc.ClaimDispatches(e.Ctx, dead, 1)
	_, err := e.Svc.Ack(e.Ctx, dead, ds[0])
	e.Must(err)
	e.Clock.Advance(10 * time.Minute)
	alive := e.Worker(boot, "node-2", 1) // registered after the death: heartbeat is fresh
	r, err := e.Svc.Reconcile(e.Ctx)
	e.Must(err)
	if r.ExpiredLeases != 1 || r.WorkersOffline != 1 {
		t.Fatalf("reconcile: %+v", r)
	}
	if got := e.Job(user, j.ID); got.State != domain.JobQueued {
		t.Fatalf("after reclaim: %s", got.State)
	}
	e.Tick()
	d2 := runAttempt(t, e, alive, domain.OutcomeSucceeded)
	if d2.AttemptID == ds[0].AttemptID {
		t.Fatal("retry reused the attempt")
	}
	if n := e.QueryInt(`SELECT count(*) FROM job_attempts WHERE job_id = $1`, j.ID); n != 2 {
		t.Fatalf("attempts %d", n)
	}
	if n := e.QueryInt(`SELECT count(*) FROM job_attempts WHERE outcome = 'LOST'`); n != 1 {
		t.Fatalf("lost attempts %d", n)
	}
	// The closed attempt is immutable in the database too.
	if _, err := e.Store.Pool.Exec(e.Ctx, `UPDATE job_attempts SET outcome = 'SUCCEEDED' WHERE outcome = 'LOST'`); err == nil {
		t.Fatal("closed attempt was rewritten")
	}
	// A late report from the dead worker changes nothing.
	out, err := e.Svc.ReportEvents(e.Ctx, dead, ds[0].AttemptID, []application.WorkerEvent{exitEvent(1, domain.OutcomeFailed, 1)})
	e.Must(err)
	if out != domain.OutcomeLost || e.Job(user, j.ID).State != domain.JobSucceeded {
		t.Fatalf("late report changed state: %s %s", out, e.Job(user, j.ID).State)
	}
}

func TestRetryExhaustionFailsTheJob(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 100)
	boot := e.Pool("pool-a", "london", 1.0, false)
	j := e.Submit(user, 1, func(r *application.SubmitRequest) { r.MaxAttempts = 2 })
	for i := 0; i < 2; i++ {
		w := e.Worker(boot, "node-"+string(rune('a'+i)), 1)
		e.Tick()
		ds, _ := e.Svc.ClaimDispatches(e.Ctx, w, 1)
		if len(ds) != 1 {
			t.Fatalf("round %d: no dispatch", i)
		}
		e.Clock.Advance(5 * time.Minute)
		_, err := e.Svc.Reconcile(e.Ctx)
		e.Must(err)
	}
	if got := e.Job(user, j.ID); got.State != domain.JobFailed {
		t.Fatalf("after 2 lost attempts: %s", got.State)
	}
	if n := e.QueryInt(`SELECT sum(gpus_reserved) FROM workers`); n != 0 {
		t.Fatalf("capacity leaked: %d", n)
	}
}

func TestTerminalReportIsAppliedExactlyOnce(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 100)
	w := e.Worker(e.Pool("pool-a", "london", 1.0, false), "node-1", 1)
	j := e.Submit(user, 1)
	e.Tick()
	d := runAttempt(t, e, w, domain.OutcomeSucceeded)
	settled := e.QueryInt(`SELECT count(*) FROM budget_ledger WHERE kind = 'SETTLE'`)
	for i := 0; i < 3; i++ {
		out, err := e.Svc.ReportEvents(e.Ctx, w, d.AttemptID, []application.WorkerEvent{exitEvent(int64(2+i), domain.OutcomeFailed, 1)})
		e.Must(err)
		if out != domain.OutcomeSucceeded {
			t.Fatalf("duplicate terminal returned %s", out)
		}
	}
	if e.QueryInt(`SELECT count(*) FROM budget_ledger WHERE kind = 'SETTLE'`) != settled {
		t.Fatal("duplicate terminal settled again")
	}
	if e.Job(user, j.ID).State != domain.JobSucceeded {
		t.Fatal("state changed by duplicate")
	}
	// Same seq twice is deduplicated at the event table.
	_, err := e.Svc.ReportEvents(e.Ctx, w, d.AttemptID, []application.WorkerEvent{{Seq: 1, Kind: "log", Message: "again"}})
	e.Must(err)
	if n := e.QueryInt(`SELECT count(*) FROM attempt_events WHERE attempt_id = $1 AND seq = 1`, d.AttemptID); n != 1 {
		t.Fatalf("seq 1 stored %d times", n)
	}
}

// Cancel and claim race on the same RESERVED job, many times. Whichever
// commits first wins, and the outcome is always one of the two consistent
// pairs: (no dispatch, CANCELLED) or (dispatch, CANCEL_REQUESTED).
func TestCancelRacingDispatchHasOneDeterministicOutcome(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 10000)
	w := e.Worker(e.Pool("pool-a", "london", 0.01, false), "node-1", 1)
	outcomes := map[string]int{}
	for i := 0; i < 30; i++ {
		j := e.Submit(user, 1)
		e.Tick()
		var wg sync.WaitGroup
		var ds []application.Dispatch
		wg.Add(2)
		go func() { defer wg.Done(); ds, _ = e.Svc.ClaimDispatches(e.Ctx, w, 1) }()
		go func(i int) {
			defer wg.Done()
			// Jitter so both orderings actually occur across iterations.
			time.Sleep(time.Duration(i%8) * 500 * time.Microsecond)
			if _, err := e.Svc.Cancel(e.Ctx, user, j.ID, "race"); err != nil {
				t.Error(err)
			}
		}(i)
		wg.Wait()
		st := e.Job(user, j.ID).State
		switch {
		case len(ds) == 0 && st == domain.JobCancelled:
			outcomes["cancel-first"]++
		case len(ds) == 1 && st == domain.JobCancelRequested:
			outcomes["dispatch-first"]++
			res, err := e.Svc.Ack(e.Ctx, w, ds[0])
			e.Must(err)
			if res != application.AckCancel {
				t.Fatalf("ack of a cancel-requested job said %s", res)
			}
			_, err = e.Svc.ReportEvents(e.Ctx, w, ds[0].AttemptID, []application.WorkerEvent{exitEvent(1, domain.OutcomeCancelled, 137)})
			e.Must(err)
			if e.Job(user, j.ID).State != domain.JobCancelled {
				t.Fatal("cancel did not complete")
			}
		default:
			t.Fatalf("inconsistent race outcome: %d dispatches, job %s", len(ds), st)
		}
		if n := e.QueryInt(`SELECT gpus_reserved FROM workers`); n != 0 {
			t.Fatalf("capacity after race %d", n)
		}
		// Cancelling again is a no-op.
		again, err := e.Svc.Cancel(e.Ctx, user, j.ID, "again")
		e.Must(err)
		if again.State != domain.JobCancelled {
			t.Fatalf("idempotent cancel returned %s", again.State)
		}
	}
	t.Logf("race outcomes: %v", outcomes)
	if outcomes["cancel-first"] == 0 || outcomes["dispatch-first"] == 0 {
		t.Fatalf("only one ordering occurred (%v); the race is not being exercised", outcomes)
	}
}

func TestStaleReservationIsReclaimed(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 100)
	boot := e.Pool("pool-a", "london", 1.0, false)
	e.Worker(boot, "node-1", 1)
	j := e.Submit(user, 1)
	e.Tick()
	e.Clock.Advance(3 * time.Minute)
	e.Worker(boot, "node-1", 1) // keep the worker alive; it just never claims
	r, err := e.Svc.Reconcile(e.Ctx)
	e.Must(err)
	if r.StaleReservations != 1 || e.Job(user, j.ID).State != domain.JobQueued {
		t.Fatalf("reconcile %+v state %s", r, e.Job(user, j.ID).State)
	}
	if n := e.QueryInt(`SELECT gpus_reserved FROM workers`); n != 0 {
		t.Fatalf("capacity %d", n)
	}
}

func TestBudgetRefusalAndNoSpendAfterFinish(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 1) // $1
	e.Worker(e.Pool("pool-a", "london", 2.0, false), "node-1", 4)
	j, err := e.Svc.Submit(e.Ctx, user, application.SubmitRequest{Image: "x/y:1", Command: []string{"a"}, GPUs: 1,
		MaxRuntime: time.Hour}, "c")
	if !errors.Is(err, application.ErrBudget) || j.State != domain.JobFailed {
		t.Fatalf("over-budget submit: %v %s", err, j.State)
	}
	// The database refuses a hold on a finished job even if code tried.
	_, err = e.Store.Pool.Exec(e.Ctx, `INSERT INTO budget_ledger (project_id, job_id, kind, amount, at)
		SELECT project_id, id, 'HOLD', 1, now() FROM jobs WHERE id = $1`, j.ID)
	if err == nil {
		t.Fatal("hold on a FAILED job accepted")
	}
}

func TestTenantIsolation(t *testing.T) {
	e := testkit.New(t)
	a, _ := e.Tenant("alpha", 100)
	b, _ := e.Tenant("bravo", 100)
	w := e.Worker(e.Pool("pool-a", "london", 1.0, false), "node-1", 2)
	ja := e.Submit(a, 1)
	e.Tick()
	runAttempt(t, e, w, domain.OutcomeSucceeded)

	if _, err := e.Svc.GetJob(e.Ctx, b, ja.ID); !errors.Is(err, application.ErrNotFound) {
		t.Fatalf("get: %v", err)
	}
	if _, err := e.Svc.Cancel(e.Ctx, b, ja.ID, "x"); !errors.Is(err, application.ErrNotFound) {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := e.Svc.Decisions(e.Ctx, b, ja.ID); !errors.Is(err, application.ErrNotFound) {
		t.Fatalf("decisions: %v", err)
	}
	if _, err := e.Svc.Logs(e.Ctx, b, ja.ID, 10); !errors.Is(err, application.ErrNotFound) {
		t.Fatalf("logs: %v", err)
	}
	if js, _ := e.Svc.ListJobs(e.Ctx, b, 100); len(js) != 0 {
		t.Fatalf("bravo lists %d of alpha's jobs", len(js))
	}
	// A worker credential is not a user credential.
	if _, err := e.Svc.GetJob(e.Ctx, w, ja.ID); !errors.Is(err, application.ErrForbidden) {
		t.Fatalf("worker read a job: %v", err)
	}
}

func TestPreemptionStopsVictimAndRequeuesIt(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 1000)
	w := e.Worker(e.Pool("pool-a", "london", 1.0, false), "node-1", 2)
	low := e.Submit(user, 2, func(r *application.SubmitRequest) { r.Preemptible = true; r.Priority = 1; r.Policy = "balanced" })
	e.Tick()
	ds, _ := e.Svc.ClaimDispatches(e.Ctx, w, 1)
	_, err := e.Svc.Ack(e.Ctx, w, ds[0])
	e.Must(err)
	high := e.Submit(user, 2, func(r *application.SubmitRequest) { r.Priority = 8; r.Policy = "balanced" })
	if r := e.Tick(); r.Applied["PREEMPT"] != 1 {
		t.Fatalf("tick: %+v", r)
	}
	hb, err := e.Svc.Heartbeat(e.Ctx, w, domain.WorkerReady, []domain.AttemptID{ds[0].AttemptID})
	e.Must(err)
	if len(hb.Stop) != 1 || hb.Stop[0].Kind != "preempt" {
		t.Fatalf("heartbeat stop orders %+v", hb.Stop)
	}
	_, err = e.Svc.ReportEvents(e.Ctx, w, ds[0].AttemptID, []application.WorkerEvent{exitEvent(1, domain.OutcomePreempted, 143)})
	e.Must(err)
	if st := e.Job(user, low.ID).State; st != domain.JobQueued {
		t.Fatalf("victim %s", st)
	}
	e.Tick()
	if st := e.Job(user, high.ID).State; st != domain.JobReserved {
		t.Fatalf("high-priority job %s", st)
	}
}

func TestEvidenceClassIsImmutable(t *testing.T) {
	e := testkit.New(t)
	_, tc := e.Tenant("acme", 1)
	if _, err := e.Store.Pool.Exec(e.Ctx, `UPDATE tenants SET evidence_class = 'real' WHERE id = $1`, tc.TenantID); err != nil {
		t.Fatalf("no-op update refused: %v", err)
	}
	if _, err := e.Store.Pool.Exec(e.Ctx, `UPDATE tenants SET evidence_class = 'seeded' WHERE id = $1`, tc.TenantID); err == nil {
		t.Fatal("evidence class was changed")
	}
}

func TestQuotaAndQueueBackpressure(t *testing.T) {
	e := testkit.New(t)
	user, tc := e.Tenant("acme", 1000)
	e.Exec(`UPDATE quotas SET max_queued_jobs = 3 WHERE tenant_id = $1`, tc.TenantID)
	for i := 0; i < 3; i++ {
		e.Submit(user, 1)
	}
	_, err := e.Svc.Submit(e.Ctx, user, application.SubmitRequest{Image: "x/y:1", Command: []string{"a"}, GPUs: 1, MaxRuntime: time.Hour}, "c")
	if !errors.Is(err, application.ErrQuotaExceeded) {
		t.Fatalf("4th submit: %v", err)
	}
	if n := e.QueryInt(`SELECT count(*) FROM jobs`); n != 3 {
		t.Fatalf("refused submission wrote a row: %d jobs", n)
	}
	e.Svc.Cfg.MaxQueuedGlobal = 3
	other, _ := e.Tenant("other", 10)
	if _, err := e.Svc.Submit(e.Ctx, other, application.SubmitRequest{Image: "x/y:1", Command: []string{"a"}, GPUs: 1, MaxRuntime: time.Hour}, "c"); !errors.Is(err, application.ErrQueueFull) {
		t.Fatalf("global cap: %v", err)
	}
}

func TestTimeoutRequestsAStop(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 100)
	w := e.Worker(e.Pool("pool-a", "london", 1.0, false), "node-1", 1)
	e.Submit(user, 1, func(r *application.SubmitRequest) { r.MaxRuntime = 10 * time.Minute })
	e.Tick()
	ds, _ := e.Svc.ClaimDispatches(e.Ctx, w, 1)
	_, err := e.Svc.Ack(e.Ctx, w, ds[0])
	e.Must(err)
	for i := 0; i < 24; i++ { // keep the lease alive (TTL 45s) while 12 minutes pass
		e.Clock.Advance(30 * time.Second)
		_, err := e.Svc.Heartbeat(e.Ctx, w, domain.WorkerReady, []domain.AttemptID{ds[0].AttemptID})
		e.Must(err)
	}
	r, err := e.Svc.Reconcile(e.Ctx)
	e.Must(err)
	if r.TimeoutStops != 1 {
		t.Fatalf("reconcile %+v", r)
	}
	hb, _ := e.Svc.Heartbeat(e.Ctx, w, domain.WorkerReady, []domain.AttemptID{ds[0].AttemptID})
	if len(hb.Stop) != 1 || hb.Stop[0].Kind != "timeout" {
		t.Fatalf("stop orders %+v", hb.Stop)
	}
}

func TestResumableArtifactUpload(t *testing.T) {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 100)
	w := e.Worker(e.Pool("pool-a", "london", 1.0, false), "node-1", 1)
	j := e.Submit(user, 1)
	e.Tick()
	ds, _ := e.Svc.ClaimDispatches(e.Ctx, w, 1)
	a := ds[0].AttemptID
	n, err := e.Svc.UploadChunk(e.Ctx, w, a, "model.bin", 0, []byte("hello "))
	e.Must(err)
	// The agent retries a chunk it already sent (it lost the response).
	_, err = e.Svc.UploadChunk(e.Ctx, w, a, "model.bin", 0, []byte("hello "))
	var om *application.OffsetMismatchError
	if !errors.As(err, &om) || om.Committed != n {
		t.Fatalf("resend at old offset: %v", err)
	}
	// Object storage fails once; the retry at the same offset succeeds.
	e.Objects.FailNext = 1
	if _, err := e.Svc.UploadChunk(e.Ctx, w, a, "model.bin", n, []byte("world")); !errors.Is(err, application.ErrUnavailable) {
		t.Fatalf("injected failure: %v", err)
	}
	n, err = e.Svc.UploadChunk(e.Ctx, w, a, "model.bin", n, []byte("world"))
	e.Must(err)
	const sha = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9" // sha256("hello world")
	if err := e.Svc.CompleteArtifact(e.Ctx, w, a, "model.bin", "0000"); err == nil {
		t.Fatal("wrong digest accepted")
	}
	e.Must(e.Svc.CompleteArtifact(e.Ctx, w, a, "model.bin", sha))
	e.Must(e.Svc.CompleteArtifact(e.Ctx, w, a, "model.bin", sha)) // idempotent
	b, art, err := e.Svc.ReadArtifact(e.Ctx, user, j.ID, a, "model.bin", 0, 100)
	e.Must(err)
	if string(b) != "hello world" || art.State != domain.ArtifactComplete || n != 11 {
		t.Fatalf("read %q %+v", b, art)
	}
	other, _ := e.Tenant("other", 1)
	if _, _, err := e.Svc.ReadArtifact(e.Ctx, other, j.ID, a, "model.bin", 0, 100); !errors.Is(err, application.ErrNotFound) {
		t.Fatalf("cross-tenant artifact read: %v", err)
	}
}
