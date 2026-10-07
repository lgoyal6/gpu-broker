// Package domain holds the broker's business rules: the job state machine,
// capacity and budget invariants, and the typed objects every other layer
// passes around. It imports nothing outside the standard library and knows
// nothing about HTTP, SQL, Kubernetes or carbon APIs.
package domain

import (
	"errors"
	"fmt"
	"time"
)

// JobState is the lifecycle of a job. The legal edges are in transitions and
// nowhere else; see docs/decisions/0003-job-state-machine.md for why each
// edge beyond the product diagram exists.
type JobState string

const (
	JobSubmitted       JobState = "SUBMITTED"
	JobQueued          JobState = "QUEUED"
	JobReserved        JobState = "RESERVED"
	JobDispatched      JobState = "DISPATCHED"
	JobRunning         JobState = "RUNNING"
	JobCancelRequested JobState = "CANCEL_REQUESTED"
	JobSucceeded       JobState = "SUCCEEDED"
	JobFailed          JobState = "FAILED"
	JobCancelled       JobState = "CANCELLED"
	JobExpired         JobState = "EXPIRED"
)

// AllJobStates is the closed set, in lifecycle order. Tests use it to prove
// the transition table is total.
var AllJobStates = []JobState{
	JobSubmitted, JobQueued, JobReserved, JobDispatched, JobRunning,
	JobCancelRequested, JobSucceeded, JobFailed, JobCancelled, JobExpired,
}

var transitions = map[JobState][]JobState{
	JobSubmitted: {JobQueued, JobFailed, JobCancelled},
	JobQueued:    {JobReserved, JobCancelled, JobExpired},
	// RESERVED -> QUEUED: a stale or preempted reservation is reclaimed before
	// any worker saw it. RESERVED -> FAILED: that happened on the last attempt.
	JobReserved: {JobDispatched, JobQueued, JobCancelled, JobExpired, JobFailed},
	// DISPATCHED -> QUEUED: the lease lapsed without an ack and retries remain.
	// The retry is a new attempt; the lapsed one is closed as LOST.
	JobDispatched: {JobRunning, JobQueued, JobCancelRequested, JobExpired, JobFailed},
	// RUNNING -> QUEUED: worker, node or spot capacity was lost and retries
	// remain. RUNNING -> SUCCEEDED/FAILED are the worker's terminal reports.
	// RUNNING -> EXPIRED: capacity was lost after the deadline had passed, so
	// a retry could not meet it.
	JobRunning: {JobSucceeded, JobFailed, JobCancelRequested, JobQueued, JobExpired},
	// The job may finish before the stop reaches it; the worker's terminal
	// report is the truth, so SUCCEEDED and FAILED are legal here too.
	JobCancelRequested: {JobCancelled, JobSucceeded, JobFailed},
	JobSucceeded:       nil,
	JobFailed:          nil,
	JobCancelled:       nil,
	JobExpired:         nil,
}

// Terminal reports whether no further transition is possible.
func (s JobState) Terminal() bool {
	next, ok := transitions[s]
	return ok && len(next) == 0
}

// Valid reports whether s is one of the declared states.
func (s JobState) Valid() bool {
	_, ok := transitions[s]
	return ok
}

// HoldsCapacity reports whether a job in this state has, or may have, a
// reservation that consumes worker capacity.
func (s JobState) HoldsCapacity() bool {
	switch s {
	case JobReserved, JobDispatched, JobRunning, JobCancelRequested:
		return true
	}
	return false
}

// CanTransition reports whether from -> to is an edge in the table.
func CanTransition(from, to JobState) bool {
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// LegalTargets returns a copy of the outgoing edges of s.
func LegalTargets(s JobState) []JobState {
	return append([]JobState(nil), transitions[s]...)
}

// ActorKind says who caused a transition. It is stored on every transition
// record so "who moved this job" is answerable after the fact.
type ActorKind string

const (
	ActorUser       ActorKind = "user"
	ActorOperator   ActorKind = "operator"
	ActorScheduler  ActorKind = "scheduler"
	ActorWorker     ActorKind = "worker"
	ActorReconciler ActorKind = "reconciler"
	ActorSystem     ActorKind = "system"
)

// Actor identifies the principal behind a transition.
type Actor struct {
	Kind ActorKind
	ID   string
}

// TransitionRecord is the durable account of one state change. The store
// writes it in the same transaction as the new state.
type TransitionRecord struct {
	JobID         JobID
	From          JobState
	To            JobState
	Actor         Actor
	Reason        string
	At            time.Time
	CorrelationID string
}

var (
	ErrIllegalTransition = errors.New("illegal job transition")
	ErrMissingReason     = errors.New("transition requires a reason")
	ErrMissingActor      = errors.New("transition requires an actor")
)

// IllegalTransitionError names the edge that was refused.
type IllegalTransitionError struct {
	JobID    JobID
	From, To JobState
}

func (e *IllegalTransitionError) Error() string {
	return fmt.Sprintf("job %s: %s -> %s is not a legal transition", e.JobID, e.From, e.To)
}

func (e *IllegalTransitionError) Unwrap() error { return ErrIllegalTransition }

// Transition is the only function that changes a job's state. It never
// coerces: an edge outside the table is an error and the job is returned
// unchanged.
func Transition(j Job, to JobState, actor Actor, reason, correlationID string, at time.Time) (Job, TransitionRecord, error) {
	if actor.Kind == "" {
		return j, TransitionRecord{}, ErrMissingActor
	}
	if reason == "" {
		return j, TransitionRecord{}, ErrMissingReason
	}
	if !CanTransition(j.State, to) {
		return j, TransitionRecord{}, &IllegalTransitionError{JobID: j.ID, From: j.State, To: to}
	}
	rec := TransitionRecord{
		JobID: j.ID, From: j.State, To: to, Actor: actor, Reason: reason,
		At: at.UTC(), CorrelationID: correlationID,
	}
	j.State = to
	j.UpdatedAt = rec.At
	j.Version++
	if to.Terminal() {
		t := rec.At
		j.FinishedAt = &t
	}
	return j, rec, nil
}
