package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

// closeAttempt ends the job's open attempt and everything it holds, in one
// transaction with the job row locked:
//
//	attempt outcome -> settle the run -> release the rest of the hold ->
//	release the reservation (and capacity, once) -> drop the lease ->
//	move the job by domain.JobStateForOutcome
//
// The ledger is written before the job state so the "no spend after finish"
// trigger sees the job still open.
func (s *Service) closeAttempt(ctx context.Context, tx Tx, j domain.Job, o domain.AttemptOutcome, exit *int,
	reason string, actor domain.Actor, corr string) (domain.Job, error) {
	now := s.Clock.Now()
	res, err := tx.ActiveReservation(ctx, j.ID)
	if err != nil {
		return j, fmt.Errorf("close attempt of %s: %w", j.ID, err)
	}
	a, err := tx.GetAttempt(ctx, res.AttemptID)
	if err != nil {
		return j, err
	}
	closed, err := domain.CloseAttempt(a, o, exit, reason, now)
	if err != nil {
		return j, err
	}
	if err := tx.CloseAttempt(ctx, closed); err != nil {
		return j, err
	}

	b, err := tx.JobBudget(ctx, j.ID)
	if err != nil {
		return j, err
	}
	b.Closed = false // the job is still open until the transition below
	var cost domain.MicroUSD
	if a.AckedAt != nil {
		ran := now.Sub(*a.AckedAt)
		if limit := j.MaxRuntime + s.Cfg.TimeoutGrace; ran > limit {
			ran = limit
		}
		cost = min(domain.CostOf(res.PriceRate, res.GPUs, ran), b.Outstanding())
	}
	entry := domain.LedgerEntry{ProjectID: j.ProjectID, JobID: j.ID, AttemptID: a.ID, At: now}
	if cost > 0 {
		e := entry
		e.Kind, e.Amount = domain.LedgerSettle, cost
		if b, err = domain.ApplyLedger(b, e); err != nil {
			return j, err
		}
		if err := tx.InsertLedger(ctx, e); err != nil {
			return j, err
		}
	}
	// Each attempt's hold is closed with the attempt; a retry takes a new hold.
	if _, rel := domain.CloseBudget(b, entry); rel.Amount > 0 {
		if err := tx.InsertLedger(ctx, rel); err != nil {
			return j, err
		}
	}
	released, err := tx.ReleaseReservation(ctx, res.ID, now, string(o))
	if err != nil {
		return j, err
	}
	if released {
		if err := tx.ReleaseCapacity(ctx, res.WorkerID, res.GPUs); err != nil {
			return j, err
		}
	}
	if err := tx.DeleteLease(ctx, a.ID); err != nil {
		return j, err
	}
	s.Obs.AttemptOutcome(o)

	to := domain.JobStateForOutcome(j, o, now)
	j.NotBefore = nil
	return s.apply(ctx, tx, j, to, actor, fmt.Sprintf("attempt %d %s: %s", a.Number, o, reason), corr)
}

// ---- registration and heartbeat ----

type RegisterRequest struct {
	Name       string
	Capability domain.Capability
}

var workerNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{0,62}$`)

// Register creates a worker in the bootstrap token's pool, or re-issues a
// credential for an existing worker of that name (an agent that lost its
// state directory). Capability is fixed at first registration; changing it
// under live reservations could leave gpus_reserved above gpus.
func (s *Service) Register(ctx context.Context, p Principal, in RegisterRequest) (domain.Worker, string, error) {
	if p.Kind != TokenBootstrap {
		return domain.Worker{}, "", ErrForbidden
	}
	c := in.Capability
	if !workerNameRE.MatchString(in.Name) || c.GPUs < 1 || c.GPUs > 64 || c.GPUMemGB < 0 || len(c.Runtimes) == 0 ||
		!modelRE.MatchString(c.GPUModel) {
		return domain.Worker{}, "", fmt.Errorf("%w: worker needs a dns-like name, 1..64 GPUs, a model and runtimes", ErrInvalid)
	}
	for _, r := range c.Runtimes {
		if r != "sim" && r != "kubernetes" {
			return domain.Worker{}, "", fmt.Errorf("%w: runtime %q (have sim, kubernetes)", ErrInvalid, r)
		}
	}
	var w domain.Worker
	var raw string
	err := s.Store.InTx(ctx, func(tx Tx) error {
		now := s.Clock.Now()
		pool, err := tx.GetPool(ctx, p.PoolID)
		if err != nil {
			return err
		}
		c.Region = pool.Region
		w, err = tx.FindWorker(ctx, p.PoolID, in.Name)
		switch {
		case errors.Is(err, ErrNotFound):
			w = domain.Worker{ID: domain.WorkerID(s.IDs.New("wrk")), PoolID: p.PoolID, Name: in.Name, Capability: c,
				State: domain.WorkerReady, LastHeartbeat: now}
			if err := tx.InsertWorker(ctx, w); err != nil {
				return err
			}
		case err != nil:
			return err
		default:
			if err := tx.TouchWorker(ctx, w.ID, now, domain.WorkerReady, 0); err != nil {
				return err
			}
		}
		r, tok := s.newToken(TokenWorker)
		tok.WorkerID, raw = w.ID, r
		if err := tx.InsertToken(ctx, tok); err != nil {
			return err
		}
		return tx.InsertAudit(ctx, domain.AuditEvent{Actor: p.Actor(), Action: "worker.register", Target: string(w.ID),
			At: now, CorrelationID: string(w.ID)})
	})
	return w, raw, err
}

type StopOrder struct {
	AttemptID domain.AttemptID `json:"attempt_id"`
	Kind      string           `json:"kind"` // cancel | preempt | timeout
}

type HeartbeatResult struct {
	Renewed []domain.AttemptID `json:"renewed"`
	Stop    []StopOrder        `json:"stop"`
	// Abandon lists attempts the worker reported holding that the control
	// plane no longer considers its own (lease reclaimed): kill them.
	Abandon  []domain.AttemptID `json:"abandon"`
	LeaseTTL time.Duration      `json:"-"`
}

func (s *Service) Heartbeat(ctx context.Context, p Principal, state domain.WorkerState, held []domain.AttemptID) (HeartbeatResult, error) {
	if p.Kind != TokenWorker {
		return HeartbeatResult{}, ErrForbidden
	}
	if state != domain.WorkerReady && state != domain.WorkerDraining {
		return HeartbeatResult{}, fmt.Errorf("%w: worker may report READY or DRAINING", ErrInvalid)
	}
	if len(held) > 256 {
		return HeartbeatResult{}, fmt.Errorf("%w: at most 256 held attempts", ErrInvalid)
	}
	out := HeartbeatResult{LeaseTTL: s.Cfg.LeaseTTL}
	err := s.Store.InTx(ctx, func(tx Tx) error {
		now := s.Clock.Now()
		out = HeartbeatResult{LeaseTTL: s.Cfg.LeaseTTL}
		if err := tx.TouchWorker(ctx, p.WorkerID, now, state, len(held)); err != nil {
			return err
		}
		renewed, err := tx.RenewLeases(ctx, p.WorkerID, held, now, now.Add(s.Cfg.LeaseTTL))
		if err != nil {
			return err
		}
		out.Renewed = renewed
		ok := map[domain.AttemptID]bool{}
		for _, id := range renewed {
			ok[id] = true
		}
		for _, id := range held {
			if !ok[id] {
				out.Abandon = append(out.Abandon, id)
			}
		}
		open, err := tx.OpenAttemptsForWorker(ctx, p.WorkerID)
		if err != nil {
			return err
		}
		for _, o := range open {
			switch {
			case o.JobState == domain.JobCancelRequested:
				out.Stop = append(out.Stop, StopOrder{AttemptID: o.Attempt.ID, Kind: "cancel"})
			case o.StopRequested != "":
				out.Stop = append(out.Stop, StopOrder{AttemptID: o.Attempt.ID, Kind: o.StopRequested})
			}
		}
		return nil
	})
	return out, err
}

// ---- dispatch ----

type Dispatch struct {
	AttemptID    domain.AttemptID `json:"attempt_id"`
	JobID        domain.JobID     `json:"job_id"`
	WorkerID     domain.WorkerID  `json:"worker_id"`
	Image        string           `json:"image"`
	Command      []string         `json:"command"`
	GPUs         int              `json:"gpus"`
	GPUModel     string           `json:"gpu_model"`
	Runtime      string           `json:"runtime"`
	MaxRuntimeS  int64            `json:"max_runtime_s"`
	LeaseExpires time.Time        `json:"lease_expires"`
	SpecDigest   string           `json:"spec_digest"`
	MAC          string           `json:"mac"`
}

// SpecDigest covers everything the worker will execute. The MAC covers the
// digest, so changing the command in the queue invalidates the dispatch.
func SpecDigest(image string, command []string, gpus int, maxRuntimeS int64) string {
	b, _ := json.Marshal(struct {
		I string
		C []string
		G int
		R int64
	}{image, command, gpus, maxRuntimeS})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

type dispatchEvent struct {
	JobID     domain.JobID     `json:"job_id"`
	AttemptID domain.AttemptID `json:"attempt_id"`
}

func dispatchTopic(w domain.WorkerID) string { return "dispatch." + string(w) }

// ClaimDispatches is the consumer side of the dispatch queue, in two phases:
// claim (sets a visibility timeout and commits), then handle each event in its
// own transaction that also marks it delivered. A crash between the phases
// leaves the event invisible until the timeout, then it is redelivered;
// handling is idempotent, so a redelivery yields the same dispatch.
func (s *Service) ClaimDispatches(ctx context.Context, p Principal, max int) ([]Dispatch, error) {
	if p.Kind != TokenWorker {
		return nil, ErrForbidden
	}
	if max <= 0 || max > 16 {
		max = 4
	}
	var evs []domain.OutboxEvent
	err := s.Store.InTx(ctx, func(tx Tx) (err error) {
		evs, err = tx.Claim(ctx, dispatchTopic(p.WorkerID), max, s.Clock.Now(), s.Cfg.DispatchVisibility, string(p.WorkerID))
		return err
	})
	if err != nil {
		return nil, err
	}
	var out []Dispatch
	for _, ev := range evs {
		d, ok, err := s.handleDispatch(ctx, p, ev)
		if err != nil {
			return out, err
		}
		if ok {
			out = append(out, d)
		}
	}
	return out, nil
}

func (s *Service) handleDispatch(ctx context.Context, p Principal, ev domain.OutboxEvent) (Dispatch, bool, error) {
	var de dispatchEvent
	if err := json.Unmarshal(ev.Payload, &de); err != nil {
		return Dispatch{}, false, err
	}
	var d Dispatch
	var ok bool
	err := s.Store.InTx(ctx, func(tx Tx) error {
		ok = false
		now := s.Clock.Now()
		j, err := tx.LockJob(ctx, de.JobID)
		if err != nil {
			return err
		}
		a, err := tx.GetAttempt(ctx, de.AttemptID)
		if err != nil {
			return err
		}
		if err := tx.MarkDelivered(ctx, ev.ID, now); err != nil {
			return err
		}
		if a.WorkerID != p.WorkerID || a.Outcome != "" || j.Attempt != a.Number {
			return nil // superseded: cancelled, reclaimed, or not ours
		}
		var lease domain.Lease
		switch j.State {
		case domain.JobReserved:
			lease = domain.Lease{AttemptID: a.ID, WorkerID: p.WorkerID, ExpiresAt: now.Add(s.Cfg.LeaseTTL), RenewedAt: now}
			if err := tx.PutLease(ctx, lease); err != nil {
				return err
			}
			if _, err := s.apply(ctx, tx, j, domain.JobDispatched, p.Actor(), "claimed by worker", string(a.ID)); err != nil {
				return err
			}
			s.Obs.StageLatency("reserve_to_dispatch", now.Sub(a.CreatedAt))
		case domain.JobDispatched:
			if lease, err = tx.GetLease(ctx, a.ID); err != nil {
				return err
			}
		default:
			return nil
		}
		maxS := int64(j.MaxRuntime / time.Second)
		digest := SpecDigest(j.Image, j.Command, j.Requirements.GPUs, maxS)
		exp := lease.ExpiresAt.Truncate(time.Second)
		d = Dispatch{AttemptID: a.ID, JobID: j.ID, WorkerID: p.WorkerID, Image: j.Image, Command: j.Command,
			GPUs: j.Requirements.GPUs, GPUModel: j.Requirements.GPUModel, Runtime: j.Requirements.Runtime, MaxRuntimeS: maxS,
			LeaseExpires: exp, SpecDigest: digest, MAC: s.Signer.Sign(a.ID, p.WorkerID, exp, digest)}
		ok = true
		return nil
	})
	return d, ok, err
}

type AckResult string

const (
	AckRun    AckResult = "run"
	AckCancel AckResult = "cancel"
)

// Ack is accepted once per attempt. It also re-checks the attempt against
// the worker's registered capability: the scheduler should never have placed
// it otherwise, and the ack is where a worker would start running it.
func (s *Service) Ack(ctx context.Context, p Principal, d Dispatch) (AckResult, error) {
	if p.Kind != TokenWorker || d.WorkerID != p.WorkerID {
		return "", ErrForbidden
	}
	if !s.Signer.Verify(d.AttemptID, d.WorkerID, d.LeaseExpires, d.SpecDigest, d.MAC) {
		return "", ErrLeaseInvalid
	}
	var res AckResult
	err := s.Store.InTx(ctx, func(tx Tx) error {
		now := s.Clock.Now()
		j, err := tx.LockJob(ctx, d.JobID)
		if err != nil {
			return err
		}
		a, err := tx.GetAttempt(ctx, d.AttemptID)
		if err != nil {
			return err
		}
		if a.WorkerID != p.WorkerID || a.JobID != j.ID {
			return ErrForbidden
		}
		if SpecDigest(j.Image, j.Command, j.Requirements.GPUs, int64(j.MaxRuntime/time.Second)) != d.SpecDigest {
			return ErrLeaseInvalid
		}
		w, err := tx.GetWorker(ctx, p.WorkerID)
		if err != nil {
			return err
		}
		if reason, ok := w.Capability.Satisfies(j.Requirements); !ok {
			return fmt.Errorf("%w: attempt exceeds worker capability (%s)", ErrForbidden, reason)
		}
		acked, err := tx.AckAttempt(ctx, a.ID, now)
		if err != nil {
			return err
		}
		if !acked {
			return ErrAlreadyAcked
		}
		switch j.State {
		case domain.JobCancelRequested:
			res = AckCancel
			return nil
		case domain.JobDispatched:
		default:
			return fmt.Errorf("%w: job is %s", ErrConflict, j.State)
		}
		if _, err := tx.RenewLeases(ctx, p.WorkerID, []domain.AttemptID{a.ID}, now, now.Add(s.Cfg.LeaseTTL)); err != nil {
			return err
		}
		if j.StartedAt == nil {
			j.StartedAt = &now
			s.Obs.StageLatency("submit_to_start", now.Sub(j.SubmittedAt))
		}
		if _, err := s.apply(ctx, tx, j, domain.JobRunning, p.Actor(), "worker acknowledged and started", string(a.ID)); err != nil {
			return err
		}
		res = AckRun
		return nil
	})
	return res, err
}

// WorkerEvent is one structured event from an attempt. Kind "exit" is
// terminal and carries Outcome.
type WorkerEvent struct {
	Seq      int64                 `json:"seq"`
	Kind     string                `json:"kind"`
	At       time.Time             `json:"at"`
	Message  string                `json:"message,omitempty"`
	Outcome  domain.AttemptOutcome `json:"outcome,omitempty"`
	ExitCode *int                  `json:"exit_code,omitempty"`
}

var eventKinds = map[string]bool{"started": true, "log": true, "progress": true, "exit": true}

// ReportEvents stores events deduplicated on (attempt, seq) and applies a
// terminal event exactly once: the attempt's outcome column is written once,
// and a repeated terminal event returns the stored outcome.
func (s *Service) ReportEvents(ctx context.Context, p Principal, attempt domain.AttemptID, evs []WorkerEvent) (domain.AttemptOutcome, error) {
	if p.Kind != TokenWorker {
		return "", ErrForbidden
	}
	if len(evs) == 0 || len(evs) > 500 {
		return "", fmt.Errorf("%w: 1..500 events per call", ErrInvalid)
	}
	for _, e := range evs {
		if e.Seq < 1 || !eventKinds[e.Kind] || len(e.Message) > 16384 {
			return "", fmt.Errorf("%w: event seq>=1, known kind, message <= 16KiB", ErrInvalid)
		}
		if e.Kind == "exit" && !e.Outcome.Valid() {
			return "", fmt.Errorf("%w: exit event needs an outcome", ErrInvalid)
		}
	}
	var final domain.AttemptOutcome
	err := s.Store.InTx(ctx, func(tx Tx) error {
		final = ""
		a, err := tx.GetAttempt(ctx, attempt)
		if err != nil {
			return err
		}
		if a.WorkerID != p.WorkerID {
			return ErrNotFound
		}
		j, err := tx.LockJob(ctx, a.JobID)
		if err != nil {
			return err
		}
		a, err = tx.GetAttempt(ctx, attempt) // re-read under the job lock
		if err != nil {
			return err
		}
		for _, e := range evs {
			payload, _ := json.Marshal(e)
			at := e.At
			if at.IsZero() {
				at = s.Clock.Now()
			}
			if _, err := tx.InsertAttemptEvent(ctx, AttemptEvent{AttemptID: attempt, Seq: e.Seq, Kind: e.Kind, Payload: payload, At: at}); err != nil {
				return err
			}
			if e.Kind != "exit" {
				continue
			}
			if a.Outcome != "" {
				final = a.Outcome // duplicate terminal report
				continue
			}
			if j.State == domain.JobDispatched && e.Outcome == domain.OutcomeSucceeded {
				return fmt.Errorf("%w: success reported before acknowledgement", ErrConflict)
			}
			if _, err := s.closeAttempt(ctx, tx, j, e.Outcome, e.ExitCode, e.Message, p.Actor(), string(attempt)); err != nil {
				return err
			}
			final = e.Outcome
			a.Outcome = e.Outcome
		}
		return nil
	})
	return final, err
}

// ---- artifacts ----

var artifactNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func artifactKey(tenant domain.TenantID, job domain.JobID, attempt domain.AttemptID, name string) string {
	return fmt.Sprintf("%s/%s/%s/%s", tenant, job, attempt, name)
}

// UploadChunk appends at an explicit offset. A mismatch returns the
// committed size so the agent resumes from there instead of guessing.
func (s *Service) UploadChunk(ctx context.Context, p Principal, attempt domain.AttemptID, name string, offset int64, data []byte) (int64, error) {
	if p.Kind != TokenWorker {
		return 0, ErrForbidden
	}
	if !artifactNameRE.MatchString(name) || offset < 0 || len(data) > 8<<20 {
		return 0, fmt.Errorf("%w: artifact name or chunk size (8 MiB max)", ErrInvalid)
	}
	var key string
	err := s.Store.InTx(ctx, func(tx Tx) error {
		a, err := tx.GetAttempt(ctx, attempt)
		if err != nil {
			return err
		}
		if a.WorkerID != p.WorkerID {
			return ErrNotFound
		}
		j, err := tx.LockJob(ctx, a.JobID)
		if err != nil {
			return err
		}
		key = artifactKey(j.TenantID, j.ID, attempt, name)
		if existing, _, err := tx.GetArtifact(ctx, attempt, name); err == nil {
			if existing.State == domain.ArtifactComplete {
				return fmt.Errorf("%w: artifact already complete", ErrConflict)
			}
			return nil
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		return tx.UpsertArtifact(ctx, domain.JobArtifact{AttemptID: attempt, Name: name, State: domain.ArtifactUploading}, key, s.Clock.Now())
	})
	if err != nil {
		return 0, err
	}
	size, appendErr := s.Objects.Append(ctx, key, offset, data)
	if appendErr != nil {
		var om *OffsetMismatchError
		if !errors.As(appendErr, &om) {
			s.Obs.ArtifactFailure("append")
			return size, appendErr
		}
		// Repair metadata after a lost object-store acknowledgment. Never
		// publish the caller's requested offset as a committed byte count.
		size = om.Committed
	}
	err = s.Store.InTx(ctx, func(tx Tx) error {
		return tx.UpsertArtifact(ctx, domain.JobArtifact{AttemptID: attempt, Name: name, Size: size, State: domain.ArtifactUploading}, key, s.Clock.Now())
	})
	if err != nil {
		return size, err
	}
	return size, appendErr
}

func (s *Service) CompleteArtifact(ctx context.Context, p Principal, attempt domain.AttemptID, name, sha string) error {
	if p.Kind != TokenWorker {
		return ErrForbidden
	}
	var art domain.JobArtifact
	var key string
	err := s.Store.InTx(ctx, func(tx Tx) error {
		a, err := tx.GetAttempt(ctx, attempt)
		if err != nil {
			return err
		}
		if a.WorkerID != p.WorkerID {
			return ErrNotFound
		}
		art, key, err = tx.GetArtifact(ctx, attempt, name)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	if art.State == domain.ArtifactComplete {
		if art.SHA256 == sha {
			return nil
		}
		return fmt.Errorf("%w: artifact completed with a different digest", ErrConflict)
	}
	// Sealing is an external write, so it is outside the retried database
	// callback. It atomically verifies bytes and fences all in-flight uploads.
	size, err := s.Objects.Seal(ctx, key, sha)
	if err != nil {
		s.Obs.ArtifactFailure("seal")
		return err
	}
	return s.Store.InTx(ctx, func(tx Tx) error {
		current, _, err := tx.GetArtifact(ctx, attempt, name)
		if err != nil {
			return err
		}
		if current.State == domain.ArtifactComplete {
			if current.SHA256 == sha {
				return nil
			}
			return ErrConflict
		}
		return tx.UpsertArtifact(ctx, domain.JobArtifact{AttemptID: attempt, Name: name, Size: size, SHA256: sha, State: domain.ArtifactComplete}, key, s.Clock.Now())
	})
}

// ReadArtifact is the tenant-scoped download path.
func (s *Service) ReadArtifact(ctx context.Context, p Principal, job domain.JobID, attempt domain.AttemptID, name string, offset int64, max int) ([]byte, domain.JobArtifact, error) {
	if p.Kind != TokenUser {
		return nil, domain.JobArtifact{}, ErrForbidden
	}
	var key string
	var art domain.JobArtifact
	err := s.Store.InTx(ctx, func(tx Tx) error {
		if _, err := tx.GetJob(ctx, p.TenantID, job); err != nil {
			return err
		}
		a, err := tx.GetAttempt(ctx, attempt)
		if err != nil || a.JobID != job {
			return ErrNotFound
		}
		art, key, err = tx.GetArtifact(ctx, attempt, name)
		return err
	})
	if err != nil {
		return nil, art, err
	}
	b, err := s.Objects.Read(ctx, key, offset, max)
	return b, art, err
}
