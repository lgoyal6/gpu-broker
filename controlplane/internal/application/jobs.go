package application

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
)

// SubmitRequest is the validated boundary type. The transport decodes JSON
// into it; Validate is the only gate between untrusted input and a Job.
type SubmitRequest struct {
	ProjectID    domain.ProjectID
	Image        string
	Command      []string
	GPUs         int
	GPUModel     string
	MinGPUMemGB  int
	Runtime      string
	Priority     int
	Preemptible  bool
	Deadline     *time.Time
	MaxRuntime   time.Duration
	MaxDelay     time.Duration
	BudgetCapUSD float64
	Policy       string
	AllowedPools []domain.PoolID
	MaxAttempts  int
}

var (
	imageRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]*(:[A-Za-z0-9._-]+)?(@sha256:[a-f0-9]{64})?$`)
	modelRE = regexp.MustCompile(`^[a-z0-9-]{0,32}$`)
)

const (
	maxCommandArgs  = 64
	maxCommandBytes = 8192
	maxRuntimeLimit = 7 * 24 * time.Hour
)

func (r *SubmitRequest) Validate(now time.Time, policies map[string]policy.Policy) error {
	bad := func(f string, a ...any) error { return fmt.Errorf("%w: "+f, append([]any{ErrInvalid}, a...)...) }
	if !imageRE.MatchString(r.Image) || len(r.Image) > 256 {
		return bad("image %q is not a valid image reference", r.Image)
	}
	if len(r.Command) == 0 || len(r.Command) > maxCommandArgs {
		return bad("command needs 1..%d arguments", maxCommandArgs)
	}
	total := 0
	for _, a := range r.Command {
		total += len(a)
		if strings.ContainsRune(a, 0) {
			return bad("command arguments may not contain NUL")
		}
	}
	if total > maxCommandBytes {
		return bad("command is %d bytes; the limit is %d", total, maxCommandBytes)
	}
	if r.GPUs < 1 || r.GPUs > 64 {
		return bad("gpus must be 1..64")
	}
	if !modelRE.MatchString(r.GPUModel) || !modelRE.MatchString(r.Runtime) {
		return bad("gpu_model and runtime must be lowercase identifiers")
	}
	if r.MinGPUMemGB < 0 || r.MinGPUMemGB > 1024 {
		return bad("min_gpu_mem_gb must be 0..1024")
	}
	if r.Priority < 0 || r.Priority > 9 {
		return bad("priority must be 0..9")
	}
	if r.MaxRuntime <= 0 || r.MaxRuntime > maxRuntimeLimit {
		return bad("max_runtime must be positive and at most %s", maxRuntimeLimit)
	}
	if r.MaxDelay < 0 || r.MaxDelay > 48*time.Hour {
		return bad("max_delay must be 0..48h")
	}
	if r.Deadline != nil && !r.Deadline.After(now.Add(r.MaxRuntime)) {
		return bad("deadline %s cannot be met: it is before now + max_runtime", r.Deadline.UTC().Format(time.RFC3339))
	}
	if r.BudgetCapUSD < 0 {
		return bad("budget_cap_usd must be >= 0")
	}
	if r.MaxAttempts == 0 {
		r.MaxAttempts = 3
	}
	if r.MaxAttempts < 1 || r.MaxAttempts > 10 {
		return bad("max_attempts must be 1..10")
	}
	if r.Policy != "" {
		if _, ok := policies[r.Policy]; !ok {
			return bad("unknown policy %q", r.Policy)
		}
	}
	if len(r.AllowedPools) > 16 {
		return bad("at most 16 allowed pools")
	}
	return nil
}

// Submit admits a job. Quota and global queue limits are refusals with no
// row written: they are backpressure, and recording every refused retry would
// let a client grow the table by being refused. A budget refusal is recorded
// as a FAILED job, because "why did my job fail" must be answerable later.
func (s *Service) Submit(ctx context.Context, p Principal, req SubmitRequest, corr string) (domain.Job, error) {
	if p.Kind != TokenUser {
		return domain.Job{}, ErrForbidden
	}
	now := s.Clock.Now()
	if err := req.Validate(now, s.Policies); err != nil {
		return domain.Job{}, err
	}
	if req.Policy == "" {
		req.Policy = s.Cfg.DefaultPolicy
	}
	var job domain.Job
	var refusal error
	err := s.Store.InTx(ctx, func(tx Tx) error {
		refusal = nil
		var proj domain.Project
		var err error
		if req.ProjectID == "" {
			proj, err = tx.DefaultProject(ctx, p.TenantID)
		} else {
			proj, err = tx.GetProject(ctx, p.TenantID, req.ProjectID)
		}
		if err != nil {
			return err
		}
		q, err := tx.GetQuota(ctx, p.TenantID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		queued, err := tx.CountQueued(ctx, p.TenantID)
		if err != nil {
			return err
		}
		if q.MaxQueuedJobs > 0 && queued >= q.MaxQueuedJobs {
			return fmt.Errorf("%w: tenant has %d queued jobs, limit %d", ErrQuotaExceeded, queued, q.MaxQueuedJobs)
		}
		if q.MaxActiveGPUs > 0 && req.GPUs > q.MaxActiveGPUs {
			return fmt.Errorf("%w: job asks for %d GPUs, tenant limit is %d", ErrQuotaExceeded, req.GPUs, q.MaxActiveGPUs)
		}
		all, err := tx.CountQueuedAll(ctx)
		if err != nil {
			return err
		}
		if all >= s.Cfg.MaxQueuedGlobal {
			return fmt.Errorf("%w: %d jobs queued", ErrQueueFull, all)
		}

		job = domain.Job{
			ID: domain.JobID(s.IDs.New("job")), TenantID: p.TenantID, ProjectID: proj.ID, UserID: p.UserID,
			Image: req.Image, Command: req.Command,
			Requirements: domain.Requirements{GPUs: req.GPUs, GPUModel: req.GPUModel, MinGPUMemGB: req.MinGPUMemGB, Runtime: req.Runtime},
			Priority:     req.Priority, Preemptible: req.Preemptible, Deadline: utcPtr(req.Deadline),
			MaxRuntime: req.MaxRuntime, MaxDelay: req.MaxDelay, BudgetCap: domain.MicroUSD(req.BudgetCapUSD * 1e6),
			PolicyName: req.Policy, AllowedPools: req.AllowedPools, MaxAttempts: req.MaxAttempts,
			State: domain.JobSubmitted, SubmittedAt: now, UpdatedAt: now, EvidenceClass: p.Evidence,
		}
		if err := tx.InsertJob(ctx, job); err != nil {
			return err
		}

		// Admission budget check: the cheapest pool the job could use must fit.
		// The authoritative check is the hold at reservation, under the
		// project lock; this one exists so an impossible job fails now.
		cheapest, ok, err := s.cheapestCost(ctx, tx, job)
		if err != nil {
			return err
		}
		outstanding, settled, err := tx.ProjectSpend(ctx, proj.ID)
		if err != nil {
			return err
		}
		avail := domain.ProjectAvailable(proj.BudgetMicroUSD, outstanding, settled)
		to, reason := domain.JobQueued, "admitted"
		switch {
		case !ok:
			to, reason = domain.JobFailed, "no registered pool can ever run this job"
			refusal = fmt.Errorf("%w: no pool matches the job's pool, model and runtime constraints", ErrInvalid)
		case cheapest > avail:
			to, reason = domain.JobFailed, fmt.Sprintf("budget: needs %s on the cheapest pool, project has %s", cheapest, avail)
			refusal = fmt.Errorf("%w: needs %s on the cheapest eligible pool, project has %s available", ErrBudget, cheapest, avail)
		case job.BudgetCap > 0 && cheapest > job.BudgetCap:
			to, reason = domain.JobFailed, fmt.Sprintf("budget: needs %s, job cap %s", cheapest, job.BudgetCap)
			refusal = fmt.Errorf("%w: needs %s on the cheapest eligible pool, job cap is %s", ErrBudget, cheapest, job.BudgetCap)
		}
		next, err := s.apply(ctx, tx, job, to, p.Actor(), reason, corr)
		if err != nil {
			return err
		}
		job = next
		return tx.InsertAudit(ctx, domain.AuditEvent{TenantID: p.TenantID, Actor: p.Actor(), Action: "job.submit",
			Target: string(job.ID), At: now, CorrelationID: corr})
	})
	if err != nil {
		return domain.Job{}, err
	}
	if refusal != nil {
		return job, refusal
	}
	return job, nil
}

// cheapestCost is the lowest hold over pools whose workers could ever run
// the job. ok is false when no registered worker matches; an empty pool
// table (fresh install) is treated as "unknown", not "impossible".
func (s *Service) cheapestCost(ctx context.Context, tx Tx, j domain.Job) (domain.MicroUSD, bool, error) {
	pools, err := tx.ListPools(ctx)
	if err != nil {
		return 0, false, err
	}
	workers, err := tx.ListWorkers(ctx)
	if err != nil {
		return 0, false, err
	}
	if len(workers) == 0 {
		return 0, true, nil
	}
	byID := map[domain.PoolID]domain.Pool{}
	for _, p := range pools {
		byID[p.ID] = p
	}
	best, found := domain.MicroUSD(0), false
	for _, w := range workers {
		if len(j.AllowedPools) > 0 && !containsPoolID(j.AllowedPools, w.PoolID) {
			continue
		}
		if _, ok := w.Capability.Satisfies(j.Requirements); !ok {
			continue
		}
		c := domain.CostOf(byID[w.PoolID].PriceMicroUSDPerGPUHour, j.Requirements.GPUs, j.MaxRuntime)
		if !found || c < best {
			best, found = c, true
		}
	}
	return best, found, nil
}

func containsPoolID(ps []domain.PoolID, p domain.PoolID) bool {
	for _, x := range ps {
		if x == p {
			return true
		}
	}
	return false
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

// apply runs the domain transition, persists it with its record, and
// reports it. Every state change in the application goes through here.
func (s *Service) apply(ctx context.Context, tx Tx, j domain.Job, to domain.JobState, actor domain.Actor, reason, corr string) (domain.Job, error) {
	prev := j.Version
	from := j.State
	next, rec, err := domain.Transition(j, to, actor, reason, corr, s.Clock.Now())
	if err != nil {
		return j, err
	}
	if err := tx.SaveJob(ctx, next, prev); err != nil {
		return j, err
	}
	if err := tx.InsertTransition(ctx, rec); err != nil {
		return j, err
	}
	s.Obs.Transition(from, to, j.EvidenceClass)
	return next, nil
}

// Cancel is idempotent: cancelling a job that is already cancelled, cancel
// requested, or finished returns it unchanged and records nothing.
func (s *Service) Cancel(ctx context.Context, p Principal, id domain.JobID, corr string) (domain.Job, error) {
	if p.Kind != TokenUser {
		return domain.Job{}, ErrForbidden
	}
	var job domain.Job
	err := s.Store.InTx(ctx, func(tx Tx) error {
		if _, err := tx.GetJob(ctx, p.TenantID, id); err != nil {
			return err // tenant check before taking any lock
		}
		j, err := tx.LockJob(ctx, id)
		if err != nil {
			return err
		}
		job = j
		switch j.State {
		case domain.JobSubmitted, domain.JobQueued:
			job, err = s.apply(ctx, tx, j, domain.JobCancelled, p.Actor(), "cancelled by user before placement", corr)
		case domain.JobReserved:
			// No worker has claimed it: stop here and give the capacity back.
			job, err = s.closeAttempt(ctx, tx, j, domain.OutcomeCancelled, nil, "cancelled before dispatch", p.Actor(), corr)
		case domain.JobDispatched, domain.JobRunning:
			job, err = s.apply(ctx, tx, j, domain.JobCancelRequested, p.Actor(), "cancel requested by user", corr)
		default:
			return nil
		}
		if err != nil {
			return err
		}
		return tx.InsertAudit(ctx, domain.AuditEvent{TenantID: p.TenantID, Actor: p.Actor(), Action: "job.cancel",
			Target: string(id), At: s.Clock.Now(), CorrelationID: corr})
	})
	return job, err
}

type JobView struct {
	Job         domain.Job
	Transitions []domain.TransitionRecord
	Budget      domain.JobBudget
	Artifacts   []domain.JobArtifact
}

func (s *Service) GetJob(ctx context.Context, p Principal, id domain.JobID) (JobView, error) {
	if p.Kind != TokenUser {
		return JobView{}, ErrForbidden
	}
	var v JobView
	err := s.Store.InTx(ctx, func(tx Tx) error {
		var err error
		if v.Job, err = tx.GetJob(ctx, p.TenantID, id); err != nil {
			return err
		}
		if v.Transitions, err = tx.ListTransitions(ctx, id); err != nil {
			return err
		}
		if v.Budget, err = tx.JobBudget(ctx, id); err != nil {
			return err
		}
		v.Artifacts, err = tx.ListArtifacts(ctx, p.TenantID, id)
		return err
	})
	return v, err
}

func (s *Service) ListJobs(ctx context.Context, p Principal, limit int) ([]domain.Job, error) {
	if p.Kind != TokenUser {
		return nil, ErrForbidden
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var out []domain.Job
	err := s.Store.InTx(ctx, func(tx Tx) (err error) {
		out, err = tx.ListJobs(ctx, p.TenantID, limit)
		return err
	})
	return out, err
}

func (s *Service) Decisions(ctx context.Context, p Principal, id domain.JobID) ([]StoredDecision, error) {
	if p.Kind != TokenUser {
		return nil, ErrForbidden
	}
	var out []StoredDecision
	err := s.Store.InTx(ctx, func(tx Tx) error {
		if _, err := tx.GetJob(ctx, p.TenantID, id); err != nil {
			return err
		}
		var err error
		out, err = tx.ListDecisions(ctx, p.TenantID, id)
		return err
	})
	return out, err
}

// Logs returns the job's log events (kind "log") across attempts.
func (s *Service) Logs(ctx context.Context, p Principal, id domain.JobID, limit int) ([]AttemptEvent, error) {
	if p.Kind != TokenUser {
		return nil, ErrForbidden
	}
	if limit <= 0 || limit > 5000 {
		limit = 1000
	}
	var out []AttemptEvent
	err := s.Store.InTx(ctx, func(tx Tx) error {
		if _, err := tx.GetJob(ctx, p.TenantID, id); err != nil {
			return err
		}
		var err error
		out, err = tx.ListAttemptEvents(ctx, id, []string{"log", "started", "exit"}, limit)
		return err
	})
	return out, err
}

func (s *Service) Pools(ctx context.Context) ([]domain.Pool, []domain.Worker, error) {
	var pools []domain.Pool
	var workers []domain.Worker
	err := s.Store.InTx(ctx, func(tx Tx) (err error) {
		if pools, err = tx.ListPools(ctx); err != nil {
			return err
		}
		workers, err = tx.ListWorkers(ctx)
		return err
	})
	return pools, workers, err
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
