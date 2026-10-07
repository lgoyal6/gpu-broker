// Package agent is the worker-agent process: register, heartbeat, claim
// signed dispatches, run them in a runtime, stream events, and recover after
// a restart. See ADR 0007 for the protocol and the isolation boundary.
package agent

import (
	"context"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

// Spec is what a runtime needs to start an attempt. It is built only from a
// dispatch whose MAC verified.
type Spec struct {
	AttemptID  domain.AttemptID
	JobID      domain.JobID
	Image      string
	Command    []string
	GPUs       int
	MaxRuntime time.Duration
}

// Status is a runtime's view of one attempt.
type Status struct {
	Done     bool
	Outcome  domain.AttemptOutcome
	ExitCode *int
	Reason   string
	Logs     []string // new lines since the previous Poll
	// Artifact, when non-nil on a finished sim attempt, is uploaded before
	// the terminal event is reported.
	Artifact []byte
}

// Runtime launches and observes attempts. Start must be idempotent: after an
// agent restart it is called again for attempts the runtime may already be
// running, and must adopt rather than duplicate them.
type Runtime interface {
	Name() string
	Start(ctx context.Context, s Spec) error
	Poll(ctx context.Context, id domain.AttemptID) (Status, error)
	Stop(ctx context.Context, id domain.AttemptID, outcome domain.AttemptOutcome) error
	// List returns attempts the runtime holds, for recovery after restart.
	List(ctx context.Context) ([]domain.AttemptID, error)
	// Draining reports whether the node is being drained (cordoned).
	Draining(ctx context.Context) bool
}
