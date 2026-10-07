package postgres

import (
	"context"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/transport/status"
)

// StatusSource reads the public views. It is meant to be opened with the
// gpub_status role's credentials, which can read these two views and nothing
// else; the queries here are the only SQL the status process ever sends.
type StatusSource struct{ S *Store }

func (s StatusSource) Read(ctx context.Context) ([]status.PoolRow, status.QueueRow, time.Time, error) {
	rows, err := s.S.Pool.Query(ctx, `SELECT pool, region, workers_ready, gpus_total, gpus_reserved, newest_heartbeat,
		tenants_7d, succeeded_7d, failed_7d, cancelled_7d, expired_7d, pilot_jobs_7d, real_jobs_7d, generated_at
		FROM public_pool_status_v1`)
	if err != nil {
		return nil, status.QueueRow{}, time.Time{}, err
	}
	var out []status.PoolRow
	var at time.Time
	for rows.Next() {
		var r status.PoolRow
		if err := rows.Scan(&r.Pool, &r.Region, &r.WorkersReady, &r.GPUsTotal, &r.GPUsReserved, &r.NewestHeartbeat,
			&r.Tenants7d, &r.Succeeded7d, &r.Failed7d, &r.Cancelled7d, &r.Expired7d, &r.PilotJobs7d, &r.RealJobs7d, &at); err != nil {
			rows.Close()
			return nil, status.QueueRow{}, time.Time{}, err
		}
		out = append(out, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, status.QueueRow{}, time.Time{}, err
	}
	var q status.QueueRow
	err = s.S.Pool.QueryRow(ctx, `SELECT queued_jobs, queued_pilot, queued_real, generated_at FROM public_queue_status_v1`).
		Scan(&q.QueuedJobs, &q.QueuedPilot, &q.QueuedReal, &at)
	return out, q, at.UTC(), err
}
