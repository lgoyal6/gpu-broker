package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

// ReconcileResult counts what one pass repaired. Each field is a metric and
// each non-zero value is something the runbook explains.
type ReconcileResult struct {
	ExpiredLeases     int
	StaleReservations int
	WorkersOffline    int
	TimeoutStops      int
	LedgerRepairs     int
	CapacityRepairs   int
	Pruned            int64
}

var reconcileActor = domain.Actor{Kind: domain.ActorReconciler, ID: "reconciler"}

// Reconcile repairs state after crashes, partitions and silent workers. It
// is safe to run concurrently with itself and with the scheduler: every
// repair locks the job row and re-checks the condition it acts on, and each
// repair is its own transaction so one failure does not block the rest.
func (s *Service) Reconcile(ctx context.Context) (ReconcileResult, error) {
	var r ReconcileResult
	now := s.Clock.Now()

	// 1. Workers that stopped heartbeating stop receiving placements.
	err := s.Store.InTx(ctx, func(tx Tx) error {
		ids, err := tx.MarkWorkersOffline(ctx, now.Add(-s.Cfg.WorkerOfflineAfter))
		r.WorkersOffline = len(ids)
		return err
	})
	if err != nil {
		return r, err
	}

	// 2. Expired leases: the worker stopped renewing. The attempt is LOST and
	// the job is retried (new attempt) or ended by domain.JobStateForOutcome.
	var leases []domain.Lease
	if err := s.Store.InTx(ctx, func(tx Tx) (err error) {
		leases, err = tx.ExpiredLeases(ctx, now, 200)
		return err
	}); err != nil {
		return r, err
	}
	for _, l := range leases {
		done, err := s.reclaim(ctx, l.AttemptID, now, func(tx Tx) (bool, error) {
			cur, err := tx.GetLease(ctx, l.AttemptID)
			if errors.Is(err, ErrNotFound) {
				return false, nil
			}
			// Renewed between our read and the lock: not ours to reclaim.
			return err == nil && cur.Expired(now), err
		}, "lease expired: worker stopped renewing")
		if err != nil {
			return r, err
		}
		if done {
			r.ExpiredLeases++
			s.Obs.StaleLease("lease")
		}
	}

	// 3. Reservations no worker ever claimed (RESERVED past ClaimTimeout).
	var stale []domain.Reservation
	if err := s.Store.InTx(ctx, func(tx Tx) (err error) {
		stale, err = tx.ListStaleReservations(ctx, now.Add(-s.Cfg.ClaimTimeout), 200)
		return err
	}); err != nil {
		return r, err
	}
	for _, res := range stale {
		done, err := s.reclaim(ctx, res.AttemptID, now, func(tx Tx) (bool, error) {
			j, err := tx.LockJob(ctx, res.JobID)
			return err == nil && j.State == domain.JobReserved, err
		}, "reservation never claimed by its worker")
		if err != nil {
			return r, err
		}
		if done {
			r.StaleReservations++
			s.Obs.StaleLease("reservation")
		}
	}

	// 4. Attempts past max runtime: ask the worker to stop. If the worker is
	// gone, its lease expires and step 2 reclaims it.
	var running []domain.JobID
	if err := s.Store.InTx(ctx, func(tx Tx) (err error) {
		running, err = tx.ListJobIDsInState(ctx, domain.JobRunning, now, 1000)
		return err
	}); err != nil {
		return r, err
	}
	for _, id := range running {
		err := s.Store.InTx(ctx, func(tx Tx) error {
			j, err := tx.LockJob(ctx, id)
			if err != nil || j.State != domain.JobRunning || j.StartedAt == nil {
				return err
			}
			if now.Sub(*j.StartedAt) <= j.MaxRuntime+s.Cfg.TimeoutGrace {
				return nil
			}
			res, err := tx.ActiveReservation(ctx, id)
			if err != nil {
				return err
			}
			r.TimeoutStops++
			return tx.RequestStop(ctx, res.AttemptID, "timeout")
		})
		if err != nil {
			return r, err
		}
	}

	// 5. Terminal jobs with an outstanding hold: release it. Non-zero means a
	// path closed a job without closing its budget; the metric says which.
	err = s.Store.InTx(ctx, func(tx Tx) error {
		ids, err := tx.TerminalJobsWithOutstanding(ctx, 200)
		if err != nil {
			return err
		}
		for _, id := range ids {
			j, err := tx.LockJob(ctx, id)
			if err != nil {
				return err
			}
			b, err := tx.JobBudget(ctx, id)
			if err != nil {
				return err
			}
			if b.Outstanding() <= 0 {
				continue
			}
			if err := tx.InsertLedger(ctx, domain.LedgerEntry{ProjectID: j.ProjectID, JobID: id, Kind: domain.LedgerRelease,
				Amount: b.Outstanding(), At: now}); err != nil {
				return err
			}
			r.LedgerRepairs++
		}
		return nil
	})
	if err != nil {
		return r, err
	}

	// 6. Capacity counters versus active reservations.
	if err := s.Store.InTx(ctx, func(tx Tx) (err error) {
		r.CapacityRepairs, err = tx.RecomputeCapacity(ctx)
		return err
	}); err != nil {
		return r, err
	}

	// 7. Retention: delivered outbox rows (7d), idempotency keys and
	// heartbeat history (24h). The newest heartbeat lives on the worker row.
	if err := s.Store.InTx(ctx, func(tx Tx) error {
		a, err := tx.PruneOutbox(ctx, now.Add(-7*24*time.Hour))
		if err != nil {
			return err
		}
		b, err := tx.PruneIdempotency(ctx, now.Add(-24*time.Hour))
		if err != nil {
			return err
		}
		c, err := tx.PruneHeartbeats(ctx, now.Add(-24*time.Hour))
		r.Pruned = a + b + c
		return err
	}); err != nil {
		return r, err
	}
	s.Obs.Repair("ledger", r.LedgerRepairs)
	s.Obs.Repair("capacity", r.CapacityRepairs)
	return r, nil
}

// reclaim closes an attempt as LOST if check (run under the job lock) still
// says it should be.
func (s *Service) reclaim(ctx context.Context, attempt domain.AttemptID, now time.Time, check func(Tx) (bool, error), reason string) (bool, error) {
	done := false
	err := s.Store.InTx(ctx, func(tx Tx) error {
		done = false
		a, err := tx.GetAttempt(ctx, attempt)
		if err != nil {
			return err
		}
		j, err := tx.LockJob(ctx, a.JobID)
		if err != nil {
			return err
		}
		a, err = tx.GetAttempt(ctx, attempt)
		if err != nil {
			return err
		}
		if a.Outcome != "" {
			return tx.DeleteLease(ctx, attempt) // already closed; drop a stray lease
		}
		ok, err := check(tx)
		if err != nil || !ok {
			return err
		}
		if _, err := s.closeAttempt(ctx, tx, j, domain.OutcomeLost, nil, reason, reconcileActor, string(attempt)); err != nil {
			return fmt.Errorf("reclaim %s: %w", attempt, err)
		}
		done = true
		return nil
	})
	return done, err
}
