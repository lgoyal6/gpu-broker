package domain

import (
	"errors"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

// The literal table. Changing an edge must change this test in the same
// commit, so a new edge is a reviewed decision rather than a side effect.
var wantEdges = map[JobState][]JobState{
	JobSubmitted:       {JobQueued, JobFailed, JobCancelled},
	JobQueued:          {JobReserved, JobCancelled, JobExpired},
	JobReserved:        {JobDispatched, JobQueued, JobCancelled, JobExpired, JobFailed},
	JobDispatched:      {JobRunning, JobQueued, JobCancelRequested, JobExpired, JobFailed},
	JobRunning:         {JobSucceeded, JobFailed, JobCancelRequested, JobQueued, JobExpired},
	JobCancelRequested: {JobCancelled, JobSucceeded, JobFailed},
	JobSucceeded:       nil,
	JobFailed:          nil,
	JobCancelled:       nil,
	JobExpired:         nil,
}

func TestTransitionTableIsLiteral(t *testing.T) {
	if len(transitions) != len(AllJobStates) {
		t.Fatalf("table has %d states, AllJobStates has %d", len(transitions), len(AllJobStates))
	}
	for _, from := range AllJobStates {
		for _, to := range AllJobStates {
			want := slices.Contains(wantEdges[from], to)
			if got := CanTransition(from, to); got != want {
				t.Errorf("%s -> %s: CanTransition=%v, want %v", from, to, got, want)
			}
		}
	}
}

func TestTerminalStatesHaveNoEdgesAndEveryStateIsReachable(t *testing.T) {
	reached := map[JobState]bool{JobSubmitted: true}
	frontier := []JobState{JobSubmitted}
	for len(frontier) > 0 {
		s := frontier[0]
		frontier = frontier[1:]
		for _, n := range LegalTargets(s) {
			if !reached[n] {
				reached[n] = true
				frontier = append(frontier, n)
			}
		}
	}
	for _, s := range AllJobStates {
		if !reached[s] {
			t.Errorf("%s is unreachable from SUBMITTED", s)
		}
		if s.Terminal() != (len(wantEdges[s]) == 0) {
			t.Errorf("%s: Terminal()=%v disagrees with the table", s, s.Terminal())
		}
	}
}

func TestTransitionRejectsIllegalEdgeWithoutChangingTheJob(t *testing.T) {
	j := Job{ID: "job_1", State: JobQueued, Version: 3}
	got, _, err := Transition(j, JobRunning, Actor{Kind: ActorScheduler}, "skip", "c1", time.Now())
	var ite *IllegalTransitionError
	if !errors.As(err, &ite) || !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("want IllegalTransitionError, got %v", err)
	}
	if got.State != JobQueued || got.Version != 3 {
		t.Fatalf("job changed on a refused transition: %+v", got)
	}
}

func TestTransitionRecordsActorReasonTimeAndCorrelation(t *testing.T) {
	at := time.Date(2026, 10, 6, 12, 0, 0, 0, time.FixedZone("PDT", -7*3600))
	j := Job{ID: "job_1", State: JobRunning}
	got, rec, err := Transition(j, JobSucceeded, Actor{Kind: ActorWorker, ID: "w1"}, "exit 0", "corr-9", at)
	if err != nil {
		t.Fatal(err)
	}
	if rec.From != JobRunning || rec.To != JobSucceeded || rec.Actor.ID != "w1" ||
		rec.Reason != "exit 0" || rec.CorrelationID != "corr-9" || !rec.At.Equal(at) || rec.At.Location() != time.UTC {
		t.Fatalf("bad record %+v", rec)
	}
	if got.FinishedAt == nil || got.Version != 1 {
		t.Fatalf("terminal transition did not stamp FinishedAt/Version: %+v", got)
	}
	if _, _, err := Transition(j, JobSucceeded, Actor{}, "x", "", at); !errors.Is(err, ErrMissingActor) {
		t.Fatalf("missing actor accepted: %v", err)
	}
	if _, _, err := Transition(j, JobSucceeded, Actor{Kind: ActorWorker}, "", "", at); !errors.Is(err, ErrMissingReason) {
		t.Fatalf("missing reason accepted: %v", err)
	}
}

// Random walks over the table: whatever sequence of requested transitions is
// thrown at a job, its state is always valid, terminal states are absorbing,
// and Version counts exactly the accepted transitions.
func TestTransitionRandomWalkInvariants(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for walk := 0; walk < 2000; walk++ {
		j := Job{ID: "job_w", State: JobSubmitted}
		accepted := int64(0)
		for step := 0; step < 30; step++ {
			to := AllJobStates[r.IntN(len(AllJobStates))]
			wasTerminal := j.State.Terminal()
			next, _, err := Transition(j, to, Actor{Kind: ActorSystem}, "walk", "", time.Unix(int64(step), 0))
			if err == nil {
				if wasTerminal {
					t.Fatalf("left terminal state %s", j.State)
				}
				accepted++
			}
			j = next
			if !j.State.Valid() {
				t.Fatalf("invalid state %q", j.State)
			}
		}
		if j.Version != accepted {
			t.Fatalf("version %d != accepted %d", j.Version, accepted)
		}
	}
}

func TestOutcomeMapping(t *testing.T) {
	now := time.Unix(1000, 0)
	past := now.Add(-time.Minute)
	base := Job{State: JobRunning, Attempt: 1, MaxAttempts: 3}
	cases := []struct {
		name string
		job  Job
		o    AttemptOutcome
		want JobState
	}{
		{"success", base, OutcomeSucceeded, JobSucceeded},
		{"user code failure is not retried", base, OutcomeFailed, JobFailed},
		{"timeout is not retried", base, OutcomeTimedOut, JobFailed},
		{"lost with retries requeues", base, OutcomeLost, JobQueued},
		{"preempted with retries requeues", base, OutcomePreempted, JobQueued},
		{"lost on last attempt fails", Job{State: JobRunning, Attempt: 3, MaxAttempts: 3}, OutcomeLost, JobFailed},
		{"lost past deadline expires", Job{State: JobRunning, Attempt: 1, MaxAttempts: 3, Deadline: &past}, OutcomeLost, JobExpired},
		{"lost while cancel requested cancels", Job{State: JobCancelRequested, Attempt: 1, MaxAttempts: 3}, OutcomeLost, JobCancelled},
	}
	for _, c := range cases {
		if got := JobStateForOutcome(c.job, c.o, now); got != c.want {
			t.Errorf("%s: got %s want %s", c.name, got, c.want)
		}
		if !CanTransition(c.job.State, c.want) {
			t.Errorf("%s: mapping produced an illegal edge %s -> %s", c.name, c.job.State, c.want)
		}
	}
}

func TestClosedAttemptIsImmutable(t *testing.T) {
	a := Attempt{ID: "a1"}
	a, err := CloseAttempt(a, OutcomeLost, nil, "lease expired", time.Unix(5, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CloseAttempt(a, OutcomeSucceeded, nil, "late report", time.Unix(6, 0)); !errors.Is(err, ErrAttemptClosed) {
		t.Fatalf("closed attempt was reopened: %v", err)
	}
}

func TestCapabilitySatisfies(t *testing.T) {
	c := Capability{GPUModel: "a100", GPUs: 4, GPUMemGB: 80, Runtimes: []string{"kubernetes"}}
	cases := []struct {
		r    Requirements
		want RejectReason
	}{
		{Requirements{GPUs: 4, GPUModel: "a100", MinGPUMemGB: 80, Runtime: "kubernetes"}, ""},
		{Requirements{GPUs: 1, GPUModel: "h100"}, RejectGPUModel},
		{Requirements{GPUs: 1, MinGPUMemGB: 81}, RejectGPUMemory},
		{Requirements{GPUs: 1, Runtime: "sim"}, RejectRuntime},
		{Requirements{GPUs: 5}, RejectGPUCount},
	}
	for _, c2 := range cases {
		reason, ok := c.Satisfies(c2.r)
		if reason != c2.want || ok != (c2.want == "") {
			t.Errorf("%+v: got %q/%v want %q", c2.r, reason, ok, c2.want)
		}
	}
}
