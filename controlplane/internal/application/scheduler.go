package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/ecoshift"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
)

// TickResult summarises one scheduling pass.
type TickResult struct {
	TickID    string
	Decisions []policy.Decision
	Applied   map[policy.Action]int
	Conflicts int
	Skipped   int
}

// Snapshot reads everything a tick needs in one transaction, so the policy
// sees a consistent view. It is bounded by MaxQueueScan: a flood of
// submissions grows the table, not the scheduler's memory.
func (s *Service) Snapshot(ctx context.Context) (policy.Snapshot, error) {
	var snap policy.Snapshot
	err := s.Store.InTx(ctx, func(tx Tx) error {
		now := s.Clock.Now()
		snap = policy.Snapshot{Now: now, Pools: map[domain.PoolID]domain.Pool{}}
		var err error
		if snap.Jobs, err = tx.ListQueued(ctx, s.Cfg.MaxQueueScan); err != nil {
			return err
		}
		if snap.Workers, err = tx.ListWorkers(ctx); err != nil {
			return err
		}
		pools, err := tx.ListPools(ctx)
		if err != nil {
			return err
		}
		for _, p := range pools {
			snap.Pools[p.ID] = p
		}
		if snap.Running, err = tx.ListRunning(ctx); err != nil {
			return err
		}
		if snap.TenantUsage, err = tx.TenantUsage(ctx, now, s.Cfg.FairShareWindow, s.Cfg.FairShareHalfLife); err != nil {
			return err
		}
		if snap.TenantActiveGPUs, err = tx.TenantActiveGPUs(ctx); err != nil {
			return err
		}
		if snap.TenantMaxGPUs, err = tx.TenantMaxGPUs(ctx); err != nil {
			return err
		}
		if snap.ProjectAvailable, err = tx.ProjectAvailableAll(ctx); err != nil {
			return err
		}
		snap.Carbon, err = s.carbonView(ctx, tx, now)
		return err
	})
	return snap, err
}

// carbonView builds each pool region's current reading and, when the
// forecaster beats persistence on the recent backtest, a forecast.
func (s *Service) carbonView(ctx context.Context, tx Tx, now time.Time) (map[string]ecoshift.RegionCarbon, error) {
	regions, err := tx.CarbonRegions(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]ecoshift.RegionCarbon{}
	for _, r := range regions {
		hist, err := tx.CarbonHistory(ctx, r, now.Add(-s.Cfg.CarbonHistory), now)
		if err != nil {
			return nil, err
		}
		if len(hist) == 0 {
			continue
		}
		rc := ecoshift.RegionCarbon{Current: &hist[len(hist)-1]}
		if fc, model, ok := ecoshift.TrustedForecast(hist, now, 24*time.Hour); ok {
			rc.Forecast, rc.ForecastModel = fc, model
		}
		out[r] = rc
	}
	return out, nil
}

// Tick runs one scheduling pass as leader epoch `epoch`. Each decision is
// applied in its own transaction: one capacity conflict or stale job must not
// roll back the other placements of the tick.
func (s *Service) Tick(ctx context.Context, epoch int64) (TickResult, error) {
	start := time.Now()
	res := TickResult{TickID: s.IDs.New("tick"), Applied: map[policy.Action]int{}}
	snap, err := s.Snapshot(ctx)
	if err != nil {
		return res, err
	}
	// Each job is decided by its own policy. Jobs are grouped by policy and
	// each group decided against the capacity left by the groups before it,
	// in a fixed order, so the tick is deterministic.
	groups := map[string][]domain.Job{}
	for _, j := range snap.Jobs {
		groups[j.PolicyName] = append(groups[j.PolicyName], j)
	}
	names := make([]string, 0, len(groups))
	for n := range groups {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		p, ok := s.Policies[name]
		if !ok {
			p = s.Policies[s.Cfg.DefaultPolicy]
		}
		sub := snap
		sub.Jobs = groups[name]
		ds := policy.Decide(sub, p)
		for _, d := range ds {
			if d.Action == policy.ActionPlace {
				consume(&snap, d, groups[name])
			}
		}
		res.Decisions = append(res.Decisions, ds...)
	}

	for _, d := range res.Decisions {
		s.Obs.Decision(d)
		err := s.applyDecision(ctx, epoch, res.TickID, d)
		switch {
		case err == nil:
			res.Applied[d.Action]++
		case errors.Is(err, ErrFenced):
			s.Obs.Fenced()
			return res, err
		case errors.Is(err, ErrCapacityConflict):
			res.Conflicts++
			s.Obs.ReservationConflict("capacity")
		case errors.Is(err, errStale):
			res.Skipped++
		case errors.Is(err, ErrBudget):
			res.Skipped++
			s.Obs.ReservationConflict("budget")
		default:
			return res, fmt.Errorf("apply %s for %s: %w", d.Action, d.JobID, err)
		}
	}
	s.Obs.TickDuration(time.Since(start), len(res.Decisions))
	return res, nil
}

// consume subtracts a placement from the shared snapshot so the next policy
// group in the same tick sees the remaining capacity and budget.
func consume(snap *policy.Snapshot, d policy.Decision, jobs []domain.Job) {
	var gpus int
	var tenant domain.TenantID
	var project domain.ProjectID
	for _, j := range jobs {
		if j.ID == d.JobID {
			gpus, tenant, project = j.Requirements.GPUs, j.TenantID, j.ProjectID
		}
	}
	for i := range snap.Workers {
		if snap.Workers[i].ID == d.ChosenWorker {
			snap.Workers[i].GPUsReserved += gpus
		}
	}
	if snap.TenantActiveGPUs == nil {
		snap.TenantActiveGPUs = map[domain.TenantID]int{}
	}
	snap.TenantActiveGPUs[tenant] += gpus
	if v, ok := snap.ProjectAvailable[project]; ok {
		snap.ProjectAvailable[project] = v - d.BudgetEffect
	}
}

var errStale = errors.New("decision is stale: the job changed since the snapshot")

func (s *Service) applyDecision(ctx context.Context, epoch int64, tick string, d policy.Decision) error {
	actor := domain.Actor{Kind: domain.ActorScheduler, ID: fmt.Sprintf("epoch-%d", epoch)}
	return s.Store.InTx(ctx, func(tx Tx) error {
		if err := tx.CheckEpoch(ctx, epoch); err != nil {
			return err
		}
		now := s.Clock.Now()
		j, err := tx.LockJob(ctx, d.JobID)
		if err != nil {
			return err
		}
		if j.State != domain.JobQueued {
			return errStale
		}
		switch d.Action {
		case policy.ActionWait:
			// Record a WAIT only when its reason changes; a job waiting an
			// hour would otherwise write one identical row per tick.
			last, reason, ok, err := tx.LastDecision(ctx, j.ID)
			if err != nil {
				return err
			}
			if ok && last == policy.ActionWait && reason == d.Reason {
				return nil
			}
			return tx.InsertDecision(ctx, tick, d, now)
		case policy.ActionExpire:
			if _, err := s.apply(ctx, tx, j, domain.JobExpired, actor, d.Reason, tick); err != nil {
				return err
			}
		case policy.ActionDelay:
			prev := j.Version
			j.NotBefore = d.DelayUntil
			j.Version++
			j.UpdatedAt = now
			if err := tx.SaveJob(ctx, j, prev); err != nil {
				return err
			}
		case policy.ActionPreempt:
			victim, err := tx.ActiveReservation(ctx, d.Victim)
			if err != nil {
				return errStale
			}
			if err := tx.RequestStop(ctx, victim.AttemptID, "preempt"); err != nil {
				return err
			}
			if err := tx.InsertAudit(ctx, domain.AuditEvent{Actor: actor, Action: "job.preempt",
				Target: fmt.Sprintf("%s for %s", d.Victim, d.JobID), At: now, CorrelationID: tick}); err != nil {
				return err
			}
		case policy.ActionPlace:
			if err := s.place(ctx, tx, epoch, j, d, actor, tick, now); err != nil {
				return err
			}
		}
		return tx.InsertDecision(ctx, tick, d, now)
	})
}

// place is the reservation transaction of ADR 0004: budget hold under the
// project lock, capacity compare-and-swap, a new attempt, the reservation,
// RESERVED, and the dispatch event, all or nothing.
func (s *Service) place(ctx context.Context, tx Tx, epoch int64, j domain.Job, d policy.Decision, actor domain.Actor, tick string, now time.Time) error {
	if !j.RetriesLeft() {
		return errStale
	}
	proj, err := tx.LockProject(ctx, j.ProjectID)
	if err != nil {
		return err
	}
	outstanding, settled, err := tx.ProjectSpend(ctx, proj.ID)
	if err != nil {
		return err
	}
	pool, err := tx.GetPool(ctx, d.ChosenPool)
	if err != nil {
		return err
	}
	// Price is re-read here rather than trusted from the snapshot, and fixed
	// onto the reservation, so a settle can never exceed this hold.
	hold := domain.CostOf(pool.PriceMicroUSDPerGPUHour, j.Requirements.GPUs, j.MaxRuntime)
	if hold > domain.ProjectAvailable(proj.BudgetMicroUSD, outstanding, settled) {
		return ErrBudget
	}
	if err := tx.ReserveCapacity(ctx, d.ChosenWorker, j.Requirements.GPUs); err != nil {
		return err
	}
	j.Attempt++
	a := domain.Attempt{ID: domain.AttemptID(s.IDs.New("att")), JobID: j.ID, Number: j.Attempt, WorkerID: d.ChosenWorker, CreatedAt: now}
	if err := tx.InsertAttempt(ctx, a); err != nil {
		return err
	}
	r := domain.Reservation{ID: domain.ReservationID(s.IDs.New("res")), JobID: j.ID, AttemptID: a.ID, WorkerID: d.ChosenWorker,
		PoolID: pool.ID, GPUs: j.Requirements.GPUs, PriceRate: pool.PriceMicroUSDPerGPUHour, HoldAmount: hold, Epoch: epoch, CreatedAt: now}
	if err := tx.InsertReservation(ctx, r); err != nil {
		return err
	}
	if hold > 0 {
		if err := tx.InsertLedger(ctx, domain.LedgerEntry{ProjectID: j.ProjectID, JobID: j.ID, AttemptID: a.ID,
			Kind: domain.LedgerHold, Amount: hold, At: now}); err != nil {
			return err
		}
	}
	if _, err := s.apply(ctx, tx, j, domain.JobReserved, actor, d.Reason, tick); err != nil {
		return err
	}
	payload, _ := json.Marshal(dispatchEvent{JobID: j.ID, AttemptID: a.ID})
	s.Obs.StageLatency("queue_to_reserve", now.Sub(j.SubmittedAt))
	return tx.Enqueue(ctx, dispatchTopic(d.ChosenWorker), string(a.ID), payload, now)
}

// Explain runs the policy over the current snapshot for one hypothetical
// job without writing anything: `gpuctl policy explain`.
func (s *Service) Explain(ctx context.Context, p Principal, req SubmitRequest) (policy.Decision, error) {
	if p.Kind != TokenUser {
		return policy.Decision{}, ErrForbidden
	}
	now := s.Clock.Now()
	if err := req.Validate(now, s.Policies); err != nil {
		return policy.Decision{}, err
	}
	if req.Policy == "" {
		req.Policy = s.Cfg.DefaultPolicy
	}
	snap, err := s.Snapshot(ctx)
	if err != nil {
		return policy.Decision{}, err
	}
	j := domain.Job{ID: "explain", TenantID: p.TenantID, ProjectID: req.ProjectID, Requirements: domain.Requirements{
		GPUs: req.GPUs, GPUModel: req.GPUModel, MinGPUMemGB: req.MinGPUMemGB, Runtime: req.Runtime},
		Priority: req.Priority, Deadline: req.Deadline, MaxRuntime: req.MaxRuntime, MaxDelay: req.MaxDelay,
		BudgetCap: domain.MicroUSD(req.BudgetCapUSD * 1e6), PolicyName: req.Policy, AllowedPools: req.AllowedPools,
		MaxAttempts: req.MaxAttempts, State: domain.JobQueued, SubmittedAt: now}
	if j.ProjectID == "" {
		_ = s.Store.InTx(ctx, func(tx Tx) error {
			pr, err := tx.DefaultProject(ctx, p.TenantID)
			j.ProjectID = pr.ID
			return err
		})
	}
	// The hypothetical job joins the queue so ordering is real, but only its
	// own decision is returned; other tenants' decisions stay private.
	snap.Jobs = append(snap.Jobs, j)
	for _, d := range policy.Decide(snap, s.Policies[req.Policy]) {
		if d.JobID == "explain" {
			return d, nil
		}
	}
	return policy.Decision{}, ErrNotFound
}
