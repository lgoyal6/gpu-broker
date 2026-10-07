package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/ecoshift"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
)

type tx struct{ tx pgx.Tx }

var _ application.Tx = (*tx)(nil)

func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ErrNotFound
	}
	return err
}

// mapErr turns constraint violations into application errors so the
// transport can answer 409 instead of 500.
func mapErr(err error) error {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch pg.Code {
		case "23505", "23514", "23503":
			return fmt.Errorf("%w: %s", application.ErrConflict, pg.Message)
		}
	}
	return err
}

func (t *tx) exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := t.tx.Exec(ctx, sql, args...)
	if err != nil {
		return 0, mapErr(err)
	}
	return tag.RowsAffected(), nil
}

// ---- fencing ----

func (t *tx) CheckEpoch(ctx context.Context, epoch int64) error {
	var cur int64
	if err := t.tx.QueryRow(ctx, `SELECT epoch FROM scheduler_leader WHERE id = 1 FOR SHARE`).Scan(&cur); err != nil {
		return err
	}
	if cur != epoch {
		return fmt.Errorf("%w: epoch %d, current %d", application.ErrFenced, epoch, cur)
	}
	return nil
}

// ---- tenancy ----

func (t *tx) InsertTenant(ctx context.Context, x domain.Tenant) error {
	_, err := t.exec(ctx, `INSERT INTO tenants (id, name, evidence_class, created_at) VALUES ($1,$2,$3,$4)`,
		x.ID, x.Name, x.EvidenceClass, x.CreatedAt)
	return err
}

func (t *tx) GetTenant(ctx context.Context, id domain.TenantID) (domain.Tenant, error) {
	var x domain.Tenant
	err := t.tx.QueryRow(ctx, `SELECT id, name, evidence_class, created_at FROM tenants WHERE id = $1`, id).
		Scan(&x.ID, &x.Name, &x.EvidenceClass, &x.CreatedAt)
	return x, notFound(err)
}

func (t *tx) InsertUser(ctx context.Context, u domain.User) error {
	_, err := t.exec(ctx, `INSERT INTO users (id, tenant_id, handle, role) VALUES ($1,$2,$3,$4)`, u.ID, u.TenantID, u.Handle, u.Role)
	return err
}

func (t *tx) GetUser(ctx context.Context, id domain.UserID) (domain.User, error) {
	var u domain.User
	err := t.tx.QueryRow(ctx, `SELECT id, tenant_id, handle, role FROM users WHERE id = $1`, id).Scan(&u.ID, &u.TenantID, &u.Handle, &u.Role)
	return u, notFound(err)
}

func (t *tx) InsertProject(ctx context.Context, p domain.Project) error {
	_, err := t.exec(ctx, `INSERT INTO projects (id, tenant_id, name, budget_micro_usd) VALUES ($1,$2,$3,$4)`,
		p.ID, p.TenantID, p.Name, p.BudgetMicroUSD)
	return err
}

func (t *tx) GetProject(ctx context.Context, tenant domain.TenantID, id domain.ProjectID) (domain.Project, error) {
	var p domain.Project
	err := t.tx.QueryRow(ctx, `SELECT id, tenant_id, name, budget_micro_usd FROM projects WHERE id = $1 AND tenant_id = $2`, id, tenant).
		Scan(&p.ID, &p.TenantID, &p.Name, &p.BudgetMicroUSD)
	return p, notFound(err)
}

func (t *tx) DefaultProject(ctx context.Context, tenant domain.TenantID) (domain.Project, error) {
	var p domain.Project
	err := t.tx.QueryRow(ctx, `SELECT id, tenant_id, name, budget_micro_usd FROM projects WHERE tenant_id = $1 ORDER BY name = 'default' DESC, id LIMIT 1`, tenant).
		Scan(&p.ID, &p.TenantID, &p.Name, &p.BudgetMicroUSD)
	return p, notFound(err)
}

func (t *tx) LockProject(ctx context.Context, id domain.ProjectID) (domain.Project, error) {
	var p domain.Project
	err := t.tx.QueryRow(ctx, `SELECT id, tenant_id, name, budget_micro_usd FROM projects WHERE id = $1 FOR UPDATE`, id).
		Scan(&p.ID, &p.TenantID, &p.Name, &p.BudgetMicroUSD)
	return p, notFound(err)
}

func (t *tx) ProjectSpend(ctx context.Context, id domain.ProjectID) (domain.MicroUSD, domain.MicroUSD, error) {
	var held, settled, released int64
	err := t.tx.QueryRow(ctx, `SELECT
		coalesce(sum(amount) FILTER (WHERE kind = 'HOLD'), 0),
		coalesce(sum(amount) FILTER (WHERE kind = 'SETTLE'), 0),
		coalesce(sum(amount) FILTER (WHERE kind = 'RELEASE'), 0)
		FROM budget_ledger WHERE project_id = $1`, id).Scan(&held, &settled, &released)
	return domain.MicroUSD(held - settled - released), domain.MicroUSD(settled), err
}

func (t *tx) UpsertQuota(ctx context.Context, q domain.Quota) error {
	_, err := t.exec(ctx, `INSERT INTO quotas (tenant_id, max_queued_jobs, max_active_gpus) VALUES ($1,$2,$3)
		ON CONFLICT (tenant_id) DO UPDATE SET max_queued_jobs = EXCLUDED.max_queued_jobs, max_active_gpus = EXCLUDED.max_active_gpus`,
		q.TenantID, q.MaxQueuedJobs, q.MaxActiveGPUs)
	return err
}

func (t *tx) GetQuota(ctx context.Context, tenant domain.TenantID) (domain.Quota, error) {
	q := domain.Quota{TenantID: tenant}
	err := t.tx.QueryRow(ctx, `SELECT max_queued_jobs, max_active_gpus FROM quotas WHERE tenant_id = $1`, tenant).
		Scan(&q.MaxQueuedJobs, &q.MaxActiveGPUs)
	return q, notFound(err)
}

func (t *tx) InsertToken(ctx context.Context, k application.Token) error {
	_, err := t.exec(ctx, `INSERT INTO api_tokens (id, kind, secret_hash, user_id, worker_id, pool_id) VALUES ($1,$2,$3,$4,$5,$6)`,
		k.ID, k.Kind, k.SecretHash, nullStr(string(k.UserID)), nullStr(string(k.WorkerID)), nullStr(string(k.PoolID)))
	return err
}

func (t *tx) GetToken(ctx context.Context, id string) (application.Token, error) {
	var k application.Token
	var uid, wid, pid *string
	var revoked *time.Time
	err := t.tx.QueryRow(ctx, `SELECT id, kind, secret_hash, user_id, worker_id, pool_id, revoked_at FROM api_tokens WHERE id = $1`, id).
		Scan(&k.ID, &k.Kind, &k.SecretHash, &uid, &wid, &pid, &revoked)
	if err != nil {
		return k, notFound(err)
	}
	k.UserID, k.WorkerID, k.PoolID = domain.UserID(deref(uid)), domain.WorkerID(deref(wid)), domain.PoolID(deref(pid))
	k.Revoked = revoked != nil
	return k, nil
}

func (t *tx) ReplaceTokenSecret(ctx context.Context, id string, hash []byte) error {
	_, err := t.exec(ctx, `UPDATE api_tokens SET secret_hash = $2, revoked_at = NULL WHERE id = $1`, id, hash)
	return err
}

func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ---- pools and workers ----

func (t *tx) UpsertPool(ctx context.Context, p domain.Pool) error {
	_, err := t.exec(ctx, `INSERT INTO pools (id, name, kind, region, price_micro_usd_per_gpu_hour, interruptible, pue)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, kind = EXCLUDED.kind, region = EXCLUDED.region,
		  price_micro_usd_per_gpu_hour = EXCLUDED.price_micro_usd_per_gpu_hour,
		  interruptible = EXCLUDED.interruptible, pue = EXCLUDED.pue`,
		p.ID, p.Name, p.Kind, p.Region, p.PriceMicroUSDPerGPUHour, p.Interruptible, math.Max(1, p.PUE))
	return err
}

const poolCols = `id, name, kind, region, price_micro_usd_per_gpu_hour, interruptible, pue`

func scanPool(r pgx.Row) (domain.Pool, error) {
	var p domain.Pool
	err := r.Scan(&p.ID, &p.Name, &p.Kind, &p.Region, &p.PriceMicroUSDPerGPUHour, &p.Interruptible, &p.PUE)
	return p, err
}

func (t *tx) GetPool(ctx context.Context, id domain.PoolID) (domain.Pool, error) {
	p, err := scanPool(t.tx.QueryRow(ctx, `SELECT `+poolCols+` FROM pools WHERE id = $1`, id))
	return p, notFound(err)
}

func (t *tx) ListPools(ctx context.Context) ([]domain.Pool, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+poolCols+` FROM pools ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (domain.Pool, error) { return scanPool(r) })
}

const workerCols = `id, pool_id, name, gpu_model, gpus, gpu_mem_gb, runtimes, region, gpus_reserved, state, last_heartbeat, version`

func scanWorker(r pgx.Row) (domain.Worker, error) {
	var w domain.Worker
	err := r.Scan(&w.ID, &w.PoolID, &w.Name, &w.Capability.GPUModel, &w.Capability.GPUs, &w.Capability.GPUMemGB,
		&w.Capability.Runtimes, &w.Capability.Region, &w.GPUsReserved, &w.State, &w.LastHeartbeat, &w.Version)
	return w, err
}

func (t *tx) InsertWorker(ctx context.Context, w domain.Worker) error {
	_, err := t.exec(ctx, `INSERT INTO workers (id, pool_id, name, gpu_model, gpus, gpu_mem_gb, runtimes, region, state, last_heartbeat)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		w.ID, w.PoolID, w.Name, w.Capability.GPUModel, w.Capability.GPUs, w.Capability.GPUMemGB, w.Capability.Runtimes,
		w.Capability.Region, w.State, w.LastHeartbeat)
	return err
}

func (t *tx) GetWorker(ctx context.Context, id domain.WorkerID) (domain.Worker, error) {
	w, err := scanWorker(t.tx.QueryRow(ctx, `SELECT `+workerCols+` FROM workers WHERE id = $1`, id))
	return w, notFound(err)
}

func (t *tx) FindWorker(ctx context.Context, pool domain.PoolID, name string) (domain.Worker, error) {
	w, err := scanWorker(t.tx.QueryRow(ctx, `SELECT `+workerCols+` FROM workers WHERE pool_id = $1 AND name = $2`, pool, name))
	return w, notFound(err)
}

func (t *tx) ListWorkers(ctx context.Context) ([]domain.Worker, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+workerCols+` FROM workers ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (domain.Worker, error) { return scanWorker(r) })
}

func (t *tx) TouchWorker(ctx context.Context, id domain.WorkerID, at time.Time, state domain.WorkerState, held int) error {
	n, err := t.exec(ctx, `UPDATE workers SET last_heartbeat = $2, state = $3, version = version + 1 WHERE id = $1`, id, at, state)
	if err != nil {
		return err
	}
	if n == 0 {
		return application.ErrNotFound
	}
	_, err = t.exec(ctx, `INSERT INTO worker_heartbeats (worker_id, at, held_attempts) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, id, at, held)
	return err
}

func (t *tx) MarkWorkersOffline(ctx context.Context, before time.Time) ([]domain.WorkerID, error) {
	rows, err := t.tx.Query(ctx, `UPDATE workers SET state = 'OFFLINE', version = version + 1
		WHERE state <> 'OFFLINE' AND last_heartbeat < $1 RETURNING id`, before)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (domain.WorkerID, error) {
		var id domain.WorkerID
		return id, r.Scan(&id)
	})
}

// ReserveCapacity is the compare-and-swap from ADR 0004: the predicate is
// evaluated against the row's current value under its row lock, so two
// concurrent reservations for the last GPUs cannot both succeed.
func (t *tx) ReserveCapacity(ctx context.Context, w domain.WorkerID, gpus int) error {
	n, err := t.exec(ctx, `UPDATE workers SET gpus_reserved = gpus_reserved + $2, version = version + 1
		WHERE id = $1 AND state = 'READY' AND gpus_reserved + $2 <= gpus`, w, gpus)
	if err != nil {
		return err
	}
	if n != 1 {
		return application.ErrCapacityConflict
	}
	return nil
}

func (t *tx) ReleaseCapacity(ctx context.Context, w domain.WorkerID, gpus int) error {
	n, err := t.exec(ctx, `UPDATE workers SET gpus_reserved = gpus_reserved - $2, version = version + 1
		WHERE id = $1 AND gpus_reserved >= $2`, w, gpus)
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: release of %d GPUs on %s would go negative", application.ErrConflict, gpus, w)
	}
	return nil
}

// RecomputeCapacity repairs gpus_reserved from the active reservations. It
// is the reconciler's last resort; a non-zero result means some path leaked
// capacity and is surfaced as a metric.
func (t *tx) RecomputeCapacity(ctx context.Context) (int, error) {
	n, err := t.exec(ctx, `WITH truth AS (
			SELECT w.id, coalesce(sum(r.gpus), 0)::int AS reserved
			FROM workers w LEFT JOIN reservations r ON r.worker_id = w.id AND r.released_at IS NULL
			GROUP BY w.id)
		UPDATE workers w SET gpus_reserved = LEAST(truth.reserved, w.gpus), version = w.version + 1
		FROM truth WHERE truth.id = w.id AND w.gpus_reserved <> truth.reserved`)
	return int(n), err
}

// ---- jobs ----

const jobCols = `id, tenant_id, project_id, user_id, image, command, gpus, gpu_model, min_gpu_mem_gb, runtime, priority,
	preemptible, deadline, max_runtime_s, max_delay_s, budget_cap, policy_name, allowed_pools, max_attempts, attempt,
	state, not_before, submitted_at, started_at, finished_at, updated_at, version, evidence_class`

func scanJob(r pgx.Row) (domain.Job, error) {
	var j domain.Job
	var cmd []byte
	var maxRun, maxDelay int64
	var pools []string
	err := r.Scan(&j.ID, &j.TenantID, &j.ProjectID, &j.UserID, &j.Image, &cmd, &j.Requirements.GPUs,
		&j.Requirements.GPUModel, &j.Requirements.MinGPUMemGB, &j.Requirements.Runtime, &j.Priority, &j.Preemptible,
		&j.Deadline, &maxRun, &maxDelay, &j.BudgetCap, &j.PolicyName, &pools, &j.MaxAttempts, &j.Attempt,
		&j.State, &j.NotBefore, &j.SubmittedAt, &j.StartedAt, &j.FinishedAt, &j.UpdatedAt, &j.Version, &j.EvidenceClass)
	if err != nil {
		return j, err
	}
	if err := json.Unmarshal(cmd, &j.Command); err != nil {
		return j, err
	}
	j.MaxRuntime, j.MaxDelay = time.Duration(maxRun)*time.Second, time.Duration(maxDelay)*time.Second
	for _, p := range pools {
		j.AllowedPools = append(j.AllowedPools, domain.PoolID(p))
	}
	utc(&j.SubmittedAt)
	utc(&j.UpdatedAt)
	utcp(j.Deadline)
	utcp(j.NotBefore)
	utcp(j.StartedAt)
	utcp(j.FinishedAt)
	return j, nil
}

func utc(t *time.Time) { *t = t.UTC() }
func utcp(t *time.Time) {
	if t != nil {
		*t = t.UTC()
	}
}

func poolStrings(ps []domain.PoolID) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, string(p))
	}
	return out
}

func (t *tx) InsertJob(ctx context.Context, j domain.Job) error {
	cmd, err := json.Marshal(j.Command)
	if err != nil {
		return err
	}
	_, err = t.exec(ctx, `INSERT INTO jobs (`+jobCols+`) VALUES
		($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)`,
		j.ID, j.TenantID, j.ProjectID, j.UserID, j.Image, cmd, j.Requirements.GPUs, j.Requirements.GPUModel,
		j.Requirements.MinGPUMemGB, j.Requirements.Runtime, j.Priority, j.Preemptible, j.Deadline,
		int64(j.MaxRuntime/time.Second), int64(j.MaxDelay/time.Second), j.BudgetCap, j.PolicyName, poolStrings(j.AllowedPools),
		j.MaxAttempts, j.Attempt, j.State, j.NotBefore, j.SubmittedAt, j.StartedAt, j.FinishedAt, j.UpdatedAt, j.Version, j.EvidenceClass)
	return err
}

func (t *tx) GetJob(ctx context.Context, tenant domain.TenantID, id domain.JobID) (domain.Job, error) {
	j, err := scanJob(t.tx.QueryRow(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = $1 AND tenant_id = $2`, id, tenant))
	return j, notFound(err)
}

func (t *tx) LockJob(ctx context.Context, id domain.JobID) (domain.Job, error) {
	j, err := scanJob(t.tx.QueryRow(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = $1 FOR UPDATE`, id))
	return j, notFound(err)
}

// SaveJob writes the mutable fields with a version predicate. The job row is
// normally already locked; the predicate is what catches a caller that
// saved a copy read before someone else's change.
func (t *tx) SaveJob(ctx context.Context, j domain.Job, prev int64) error {
	n, err := t.exec(ctx, `UPDATE jobs SET state = $3, attempt = $4, not_before = $5, started_at = $6, finished_at = $7,
		updated_at = $8, version = $9 WHERE id = $1 AND version = $2`,
		j.ID, prev, j.State, j.Attempt, j.NotBefore, j.StartedAt, j.FinishedAt, j.UpdatedAt, j.Version)
	if err != nil {
		return err
	}
	if n != 1 {
		return application.ErrStaleVersion
	}
	return nil
}

func (t *tx) InsertTransition(ctx context.Context, r domain.TransitionRecord) error {
	_, err := t.exec(ctx, `INSERT INTO job_transitions (job_id, from_state, to_state, actor_kind, actor_id, reason, at, correlation_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, r.JobID, r.From, r.To, r.Actor.Kind, r.Actor.ID, r.Reason, r.At, r.CorrelationID)
	return err
}

func (t *tx) ListTransitions(ctx context.Context, id domain.JobID) ([]domain.TransitionRecord, error) {
	rows, err := t.tx.Query(ctx, `SELECT job_id, from_state, to_state, actor_kind, actor_id, reason, at, correlation_id
		FROM job_transitions WHERE job_id = $1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (domain.TransitionRecord, error) {
		var x domain.TransitionRecord
		err := r.Scan(&x.JobID, &x.From, &x.To, &x.Actor.Kind, &x.Actor.ID, &x.Reason, &x.At, &x.CorrelationID)
		x.At = x.At.UTC()
		return x, err
	})
}

func (t *tx) ListJobs(ctx context.Context, tenant domain.TenantID, limit int) ([]domain.Job, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+jobCols+` FROM jobs WHERE tenant_id = $1 ORDER BY submitted_at DESC, id LIMIT $2`, tenant, limit)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (domain.Job, error) { return scanJob(r) })
}

func (t *tx) CountQueued(ctx context.Context, tenant domain.TenantID) (int, error) {
	var n int
	err := t.tx.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE tenant_id = $1 AND state IN ('SUBMITTED', 'QUEUED')`, tenant).Scan(&n)
	return n, err
}

func (t *tx) CountQueuedAll(ctx context.Context) (int, error) {
	var n int
	err := t.tx.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE state IN ('SUBMITTED', 'QUEUED')`).Scan(&n)
	return n, err
}

func (t *tx) ListQueued(ctx context.Context, limit int) ([]domain.Job, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+jobCols+` FROM jobs WHERE state = 'QUEUED' ORDER BY submitted_at, id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (domain.Job, error) { return scanJob(r) })
}

func (t *tx) ListRunning(ctx context.Context) ([]policy.RunningJob, error) {
	// A job on its last attempt is reported non-preemptible: preempting it
	// would fail it outright rather than requeue it.
	rows, err := t.tx.Query(ctx, `SELECT j.id, j.tenant_id, r.worker_id, r.gpus, j.priority,
		j.preemptible AND j.attempt < j.max_attempts, coalesce(j.started_at, r.created_at)
		FROM jobs j JOIN reservations r ON r.job_id = j.id AND r.released_at IS NULL
		WHERE j.state IN ('RESERVED', 'DISPATCHED', 'RUNNING') ORDER BY j.id`)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (policy.RunningJob, error) {
		var x policy.RunningJob
		err := r.Scan(&x.JobID, &x.TenantID, &x.WorkerID, &x.GPUs, &x.Priority, &x.Preemptible, &x.StartedAt)
		x.StartedAt = x.StartedAt.UTC()
		return x, err
	})
}

func (t *tx) ListJobIDsInState(ctx context.Context, s domain.JobState, before time.Time, limit int) ([]domain.JobID, error) {
	rows, err := t.tx.Query(ctx, `SELECT id FROM jobs WHERE state = $1 AND updated_at < $2 ORDER BY updated_at LIMIT $3`, s, before, limit)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (domain.JobID, error) {
		var id domain.JobID
		return id, r.Scan(&id)
	})
}

func (t *tx) TenantActiveGPUs(ctx context.Context) (map[domain.TenantID]int, error) {
	rows, err := t.tx.Query(ctx, `SELECT j.tenant_id, sum(r.gpus)::int FROM reservations r JOIN jobs j ON j.id = r.job_id
		WHERE r.released_at IS NULL GROUP BY j.tenant_id`)
	if err != nil {
		return nil, err
	}
	out := map[domain.TenantID]int{}
	for rows.Next() {
		var id domain.TenantID
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}

func (t *tx) TenantMaxGPUs(ctx context.Context) (map[domain.TenantID]int, error) {
	rows, err := t.tx.Query(ctx, `SELECT tenant_id, max_active_gpus FROM quotas`)
	if err != nil {
		return nil, err
	}
	out := map[domain.TenantID]int{}
	for rows.Next() {
		var id domain.TenantID
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}

func (t *tx) ProjectAvailableAll(ctx context.Context) (map[domain.ProjectID]domain.MicroUSD, error) {
	rows, err := t.tx.Query(ctx, `SELECT p.id, p.budget_micro_usd
		- coalesce(sum(l.amount) FILTER (WHERE l.kind = 'HOLD'), 0)
		+ coalesce(sum(l.amount) FILTER (WHERE l.kind = 'RELEASE'), 0)
		FROM projects p LEFT JOIN budget_ledger l ON l.project_id = p.id GROUP BY p.id`)
	if err != nil {
		return nil, err
	}
	out := map[domain.ProjectID]domain.MicroUSD{}
	for rows.Next() {
		var id domain.ProjectID
		var n int64
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = domain.MicroUSD(n)
	}
	return out, rows.Err()
}

// TenantUsage is decayed GPU-hours per tenant: each closed reservation's
// GPU-hours weighted by 0.5^(age/halfLife), over the window. Same shape as
// the Python broker's decayed usage, in GPU-hours rather than currencies.
func (t *tx) TenantUsage(ctx context.Context, now time.Time, window, halfLife time.Duration) (map[domain.TenantID]float64, error) {
	rows, err := t.tx.Query(ctx, `SELECT j.tenant_id,
		sum(r.gpus * extract(epoch FROM (coalesce(r.released_at, $1) - r.created_at)) / 3600.0
		    * power(0.5, extract(epoch FROM ($1 - coalesce(r.released_at, $1))) / $3))
		FROM reservations r JOIN jobs j ON j.id = r.job_id
		WHERE coalesce(r.released_at, $1) >= $2 GROUP BY j.tenant_id`, now, now.Add(-window), halfLife.Seconds())
	if err != nil {
		return nil, err
	}
	out := map[domain.TenantID]float64{}
	for rows.Next() {
		var id domain.TenantID
		var v float64
		if err := rows.Scan(&id, &v); err != nil {
			return nil, err
		}
		out[id] = v
	}
	return out, rows.Err()
}

// ---- attempts ----

const attemptCols = `id, job_id, number, worker_id, created_at, acked_at, ended_at, coalesce(outcome, ''), exit_code, reason`

func scanAttempt(r pgx.Row) (domain.Attempt, error) {
	var a domain.Attempt
	err := r.Scan(&a.ID, &a.JobID, &a.Number, &a.WorkerID, &a.CreatedAt, &a.AckedAt, &a.EndedAt, &a.Outcome, &a.ExitCode, &a.Reason)
	utc(&a.CreatedAt)
	utcp(a.AckedAt)
	utcp(a.EndedAt)
	return a, err
}

func (t *tx) InsertAttempt(ctx context.Context, a domain.Attempt) error {
	_, err := t.exec(ctx, `INSERT INTO job_attempts (id, job_id, number, worker_id, created_at) VALUES ($1,$2,$3,$4,$5)`,
		a.ID, a.JobID, a.Number, a.WorkerID, a.CreatedAt)
	return err
}

func (t *tx) GetAttempt(ctx context.Context, id domain.AttemptID) (domain.Attempt, error) {
	a, err := scanAttempt(t.tx.QueryRow(ctx, `SELECT `+attemptCols+` FROM job_attempts WHERE id = $1`, id))
	return a, notFound(err)
}

// AckAttempt is the exactly-once acknowledgement: the predicate admits one
// ack per attempt, so a redelivered dispatch acks zero rows.
func (t *tx) AckAttempt(ctx context.Context, id domain.AttemptID, at time.Time) (bool, error) {
	n, err := t.exec(ctx, `UPDATE job_attempts SET acked_at = $2 WHERE id = $1 AND acked_at IS NULL AND outcome IS NULL`, id, at)
	return n == 1, err
}

func (t *tx) CloseAttempt(ctx context.Context, a domain.Attempt) error {
	n, err := t.exec(ctx, `UPDATE job_attempts SET outcome = $2, exit_code = $3, reason = $4, ended_at = $5
		WHERE id = $1 AND outcome IS NULL`, a.ID, a.Outcome, a.ExitCode, a.Reason, a.EndedAt)
	if err != nil {
		return err
	}
	if n != 1 {
		return domain.ErrAttemptClosed
	}
	return nil
}

func (t *tx) RequestStop(ctx context.Context, id domain.AttemptID, kind string) error {
	_, err := t.exec(ctx, `UPDATE job_attempts SET stop_requested = $2 WHERE id = $1 AND outcome IS NULL AND stop_requested IS NULL`, id, kind)
	return err
}

func (t *tx) OpenAttemptsForWorker(ctx context.Context, w domain.WorkerID) ([]application.OpenAttempt, error) {
	rows, err := t.tx.Query(ctx, `SELECT a.id, a.job_id, a.number, a.worker_id, a.created_at, a.acked_at, a.ended_at,
		coalesce(a.outcome, ''), a.exit_code, a.reason, j.state, coalesce(a.stop_requested, '')
		FROM job_attempts a JOIN jobs j ON j.id = a.job_id
		WHERE a.worker_id = $1 AND a.outcome IS NULL ORDER BY a.id`, w)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (application.OpenAttempt, error) {
		var o application.OpenAttempt
		a := &o.Attempt
		err := r.Scan(&a.ID, &a.JobID, &a.Number, &a.WorkerID, &a.CreatedAt, &a.AckedAt, &a.EndedAt, &a.Outcome,
			&a.ExitCode, &a.Reason, &o.JobState, &o.StopRequested)
		return o, err
	})
}

func (t *tx) InsertAttemptEvent(ctx context.Context, e application.AttemptEvent) (bool, error) {
	n, err := t.exec(ctx, `INSERT INTO attempt_events (attempt_id, seq, kind, payload, at) VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (attempt_id, seq) DO NOTHING`, e.AttemptID, e.Seq, e.Kind, e.Payload, e.At)
	return n == 1, err
}

func (t *tx) ListAttemptEvents(ctx context.Context, job domain.JobID, kinds []string, limit int) ([]application.AttemptEvent, error) {
	rows, err := t.tx.Query(ctx, `SELECT e.attempt_id, e.seq, e.kind, e.payload, e.at FROM attempt_events e
		JOIN job_attempts a ON a.id = e.attempt_id WHERE a.job_id = $1 AND (cardinality($2::text[]) = 0 OR e.kind = ANY($2))
		ORDER BY a.number, e.seq LIMIT $3`, job, kinds, limit)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (application.AttemptEvent, error) {
		var e application.AttemptEvent
		err := r.Scan(&e.AttemptID, &e.Seq, &e.Kind, &e.Payload, &e.At)
		return e, err
	})
}

// ---- reservations and leases ----

const resCols = `id, job_id, attempt_id, worker_id, pool_id, gpus, price_rate, hold_amount, epoch, created_at, released_at`

func scanRes(r pgx.Row) (domain.Reservation, error) {
	var x domain.Reservation
	err := r.Scan(&x.ID, &x.JobID, &x.AttemptID, &x.WorkerID, &x.PoolID, &x.GPUs, &x.PriceRate, &x.HoldAmount, &x.Epoch,
		&x.CreatedAt, &x.ReleasedAt)
	utc(&x.CreatedAt)
	return x, err
}

func (t *tx) InsertReservation(ctx context.Context, r domain.Reservation) error {
	_, err := t.exec(ctx, `INSERT INTO reservations (`+resCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULL)`,
		r.ID, r.JobID, r.AttemptID, r.WorkerID, r.PoolID, r.GPUs, r.PriceRate, r.HoldAmount, r.Epoch, r.CreatedAt)
	return err
}

func (t *tx) ActiveReservation(ctx context.Context, job domain.JobID) (domain.Reservation, error) {
	r, err := scanRes(t.tx.QueryRow(ctx, `SELECT `+resCols+` FROM reservations WHERE job_id = $1 AND released_at IS NULL`, job))
	return r, notFound(err)
}

// ReleaseReservation reports whether this call released it. Only the caller
// that gets true returns capacity, so a double release returns it once.
func (t *tx) ReleaseReservation(ctx context.Context, id domain.ReservationID, at time.Time, reason string) (bool, error) {
	n, err := t.exec(ctx, `UPDATE reservations SET released_at = $2, release_reason = $3 WHERE id = $1 AND released_at IS NULL`, id, at, reason)
	return n == 1, err
}

func (t *tx) ListStaleReservations(ctx context.Context, before time.Time, limit int) ([]domain.Reservation, error) {
	rows, err := t.tx.Query(ctx, `SELECT r.id, r.job_id, r.attempt_id, r.worker_id, r.pool_id, r.gpus, r.price_rate, r.hold_amount,
		r.epoch, r.created_at, r.released_at
		FROM reservations r JOIN jobs j ON j.id = r.job_id
		WHERE r.released_at IS NULL AND j.state = 'RESERVED' AND r.created_at < $1 ORDER BY r.created_at LIMIT $2`, before, limit)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (domain.Reservation, error) { return scanRes(r) })
}

func (t *tx) PutLease(ctx context.Context, l domain.Lease) error {
	_, err := t.exec(ctx, `INSERT INTO leases (attempt_id, worker_id, expires_at, renewed_at) VALUES ($1,$2,$3,$4)
		ON CONFLICT (attempt_id) DO UPDATE SET expires_at = EXCLUDED.expires_at, renewed_at = EXCLUDED.renewed_at
		WHERE leases.worker_id = EXCLUDED.worker_id`, l.AttemptID, l.WorkerID, l.ExpiresAt, l.RenewedAt)
	return err
}

func (t *tx) GetLease(ctx context.Context, a domain.AttemptID) (domain.Lease, error) {
	var l domain.Lease
	err := t.tx.QueryRow(ctx, `SELECT attempt_id, worker_id, expires_at, renewed_at FROM leases WHERE attempt_id = $1 FOR UPDATE`, a).
		Scan(&l.AttemptID, &l.WorkerID, &l.ExpiresAt, &l.RenewedAt)
	return l, notFound(err)
}

// RenewLeases extends this worker's leases, including ones that have
// expired but not yet been reclaimed: an attempt is lost when the reconciler
// reclaims it, not the instant its lease lapses, so an API outage longer than
// the TTL does not kill every running job. A reclaim deletes the lease row
// while holding it FOR UPDATE (GetLease), so a renewal that races it either
// commits first (and the reclaim sees a live lease and stops) or updates zero
// rows (and the worker is told to abandon).
func (t *tx) RenewLeases(ctx context.Context, w domain.WorkerID, ids []domain.AttemptID, now, expires time.Time) ([]domain.AttemptID, error) {
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = string(id)
	}
	rows, err := t.tx.Query(ctx, `UPDATE leases SET expires_at = $3, renewed_at = $4
		WHERE worker_id = $1 AND attempt_id = ANY($2) RETURNING attempt_id`, w, strs, expires, now)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (domain.AttemptID, error) {
		var id domain.AttemptID
		return id, r.Scan(&id)
	})
}

func (t *tx) DeleteLease(ctx context.Context, a domain.AttemptID) error {
	_, err := t.exec(ctx, `DELETE FROM leases WHERE attempt_id = $1`, a)
	return err
}

func (t *tx) ExpiredLeases(ctx context.Context, now time.Time, limit int) ([]domain.Lease, error) {
	rows, err := t.tx.Query(ctx, `SELECT attempt_id, worker_id, expires_at, renewed_at FROM leases
		WHERE expires_at <= $1 ORDER BY expires_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (domain.Lease, error) {
		var l domain.Lease
		return l, r.Scan(&l.AttemptID, &l.WorkerID, &l.ExpiresAt, &l.RenewedAt)
	})
}

// ---- ledger ----

func (t *tx) InsertLedger(ctx context.Context, e domain.LedgerEntry) error {
	_, err := t.exec(ctx, `INSERT INTO budget_ledger (project_id, job_id, attempt_id, kind, amount, at) VALUES ($1,$2,$3,$4,$5,$6)`,
		e.ProjectID, e.JobID, nullStr(string(e.AttemptID)), e.Kind, e.Amount, e.At)
	return err
}

func (t *tx) JobBudget(ctx context.Context, job domain.JobID) (domain.JobBudget, error) {
	var b domain.JobBudget
	var state domain.JobState
	err := t.tx.QueryRow(ctx, `SELECT
		coalesce(sum(l.amount) FILTER (WHERE l.kind = 'HOLD'), 0),
		coalesce(sum(l.amount) FILTER (WHERE l.kind = 'SETTLE'), 0),
		coalesce(sum(l.amount) FILTER (WHERE l.kind = 'RELEASE'), 0),
		(SELECT state FROM jobs WHERE id = $1)
		FROM budget_ledger l WHERE l.job_id = $1`, job).Scan(&b.Held, &b.Settled, &b.Released, &state)
	b.Closed = state.Terminal()
	return b, err
}

func (t *tx) TerminalJobsWithOutstanding(ctx context.Context, limit int) ([]domain.JobID, error) {
	rows, err := t.tx.Query(ctx, `SELECT l.job_id FROM budget_ledger l JOIN jobs j ON j.id = l.job_id
		WHERE j.state IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'EXPIRED')
		GROUP BY l.job_id
		HAVING sum(CASE l.kind WHEN 'HOLD' THEN l.amount ELSE -l.amount END) <> 0 LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (domain.JobID, error) {
		var id domain.JobID
		return id, r.Scan(&id)
	})
}

// ---- policies and decisions ----

func (t *tx) UpsertPolicy(ctx context.Context, p policy.Policy) error {
	spec, err := json.Marshal(p)
	if err != nil {
		return err
	}
	// Every role registers the builtins at startup, concurrently, so the
	// insert must tolerate a racing twin. A version is immutable once stored:
	// if the spec changed without a version bump, decisions recorded under it
	// would no longer describe the policy that made them.
	if _, err := t.exec(ctx, `INSERT INTO scheduling_policies (version, name, spec) VALUES ($1,$2,$3)
		ON CONFLICT (version) DO NOTHING`, p.Version, p.Name, spec); err != nil {
		return err
	}
	var have []byte
	if err := t.tx.QueryRow(ctx, `SELECT spec FROM scheduling_policies WHERE version = $1`, p.Version).Scan(&have); err != nil {
		return err
	}
	var a, b any
	_ = json.Unmarshal(have, &a)
	_ = json.Unmarshal(spec, &b)
	if !reflect.DeepEqual(a, b) {
		return fmt.Errorf("%w: policy %s changed without a version bump", application.ErrConflict, p.Version)
	}
	return nil
}

func (t *tx) InsertDecision(ctx context.Context, tick string, d policy.Decision, at time.Time) error {
	body, err := json.Marshal(d)
	if err != nil {
		return err
	}
	_, err = t.exec(ctx, `INSERT INTO decisions (job_id, tenant_id, tick_id, policy_version, action, reason, fallback, body, input_digest, at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		d.JobID, d.TenantID, tick, d.PolicyVersion, d.Action, d.Reason, len(d.Fallbacks) > 0, body, d.InputDigest, at)
	return err
}

func (t *tx) LastDecision(ctx context.Context, job domain.JobID) (policy.Action, string, bool, error) {
	var a policy.Action
	var r string
	err := t.tx.QueryRow(ctx, `SELECT action, reason FROM decisions WHERE job_id = $1 ORDER BY id DESC LIMIT 1`, job).Scan(&a, &r)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, nil
	}
	return a, r, err == nil, err
}

func (t *tx) ListDecisions(ctx context.Context, tenant domain.TenantID, job domain.JobID) ([]application.StoredDecision, error) {
	rows, err := t.tx.Query(ctx, `SELECT id, tick_id, at, body FROM decisions WHERE job_id = $1 AND tenant_id = $2 ORDER BY id`, job, tenant)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (application.StoredDecision, error) {
		var d application.StoredDecision
		err := r.Scan(&d.ID, &d.TickID, &d.At, &d.Body)
		return d, err
	})
}

// ---- carbon ----

func (t *tx) UpsertCarbon(ctx context.Context, rs []domain.CarbonSnapshot) error {
	batch := &pgx.Batch{}
	for _, r := range rs {
		batch.Queue(`INSERT INTO carbon_snapshots (region, interval_start, grams_per_kwh, source, observed_at)
			VALUES ($1,$2,$3,$4,$5) ON CONFLICT (region, interval_start, source)
			DO UPDATE SET grams_per_kwh = EXCLUDED.grams_per_kwh, observed_at = EXCLUDED.observed_at`,
			r.Region, r.At, r.GramsPerKWh, r.Source, r.ObservedAt)
	}
	return t.tx.SendBatch(ctx, batch).Close()
}

func (t *tx) CarbonHistory(ctx context.Context, region string, since, until time.Time) ([]ecoshift.Reading, error) {
	rows, err := t.tx.Query(ctx, `SELECT interval_start, grams_per_kwh, source FROM carbon_snapshots
		WHERE region = $1 AND interval_start >= $2 AND interval_start <= $3 ORDER BY interval_start, source`, region, since, until)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (ecoshift.Reading, error) {
		var x ecoshift.Reading
		err := r.Scan(&x.At, &x.GramsPerKWh, &x.Source)
		x.At = x.At.UTC()
		return x, err
	})
}

func (t *tx) CarbonRegions(ctx context.Context) ([]string, error) {
	rows, err := t.tx.Query(ctx, `SELECT DISTINCT region FROM pools ORDER BY region`)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (string, error) {
		var s string
		return s, r.Scan(&s)
	})
}

// ---- outbox ----

func (t *tx) Enqueue(ctx context.Context, topic, key string, payload []byte, availableAt time.Time) error {
	_, err := t.exec(ctx, `INSERT INTO outbox_events (topic, key, payload, available_at) VALUES ($1,$2,$3,$4)`, topic, key, payload, availableAt)
	return err
}

// Claim takes up to n undelivered events whose visibility has lapsed. SKIP
// LOCKED lets concurrent consumers take disjoint batches without waiting on
// each other; locked_until is what makes an abandoned claim redeliverable.
func (t *tx) Claim(ctx context.Context, topic string, n int, now time.Time, vis time.Duration, by string) ([]domain.OutboxEvent, error) {
	rows, err := t.tx.Query(ctx, `UPDATE outbox_events SET locked_until = $3, locked_by = $4, attempts = attempts + 1
		WHERE id IN (SELECT id FROM outbox_events
		             WHERE topic = $1 AND delivered_at IS NULL AND available_at <= $5
		               AND (locked_until IS NULL OR locked_until <= $5)
		             ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED)
		RETURNING id, topic, key, payload, available_at, attempts`, topic, n, now.Add(vis), by, now)
	if err != nil {
		return nil, err
	}
	evs, err := collect(rows, func(r pgx.Rows) (domain.OutboxEvent, error) {
		var e domain.OutboxEvent
		return e, r.Scan(&e.ID, &e.Topic, &e.Key, &e.Payload, &e.AvailableAt, &e.Attempts)
	})
	// RETURNING order is not guaranteed; consumers rely on id order.
	sortEvents(evs)
	return evs, err
}

func sortEvents(evs []domain.OutboxEvent) {
	for i := 1; i < len(evs); i++ {
		for j := i; j > 0 && evs[j].ID < evs[j-1].ID; j-- {
			evs[j], evs[j-1] = evs[j-1], evs[j]
		}
	}
}

func (t *tx) MarkDelivered(ctx context.Context, id int64, at time.Time) error {
	_, err := t.exec(ctx, `UPDATE outbox_events SET delivered_at = $2 WHERE id = $1 AND delivered_at IS NULL`, id, at)
	return err
}

func (t *tx) PruneOutbox(ctx context.Context, before time.Time) (int64, error) {
	return t.exec(ctx, `DELETE FROM outbox_events WHERE delivered_at IS NOT NULL AND delivered_at < $1`, before)
}

func (t *tx) OutboxDepth(ctx context.Context) (map[string]int, error) {
	rows, err := t.tx.Query(ctx, `SELECT split_part(topic, '.', 1), count(*) FROM outbox_events WHERE delivered_at IS NULL GROUP BY 1`)
	if err != nil {
		return nil, err
	}
	out := map[string]int{}
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			return nil, err
		}
		out[k] = n
	}
	return out, rows.Err()
}

// ---- idempotency, audit, artifacts ----

func (t *tx) GetIdempotency(ctx context.Context, principal, key string) (application.IdempotencyRecord, error) {
	var r application.IdempotencyRecord
	err := t.tx.QueryRow(ctx, `SELECT principal, key, method, path, body_hash, status, response, created_at
		FROM idempotency_keys WHERE principal = $1 AND key = $2`, principal, key).
		Scan(&r.Principal, &r.Key, &r.Method, &r.Path, &r.BodyHash, &r.Status, &r.Response, &r.CreatedAt)
	return r, notFound(err)
}

func (t *tx) PutIdempotency(ctx context.Context, r application.IdempotencyRecord) error {
	_, err := t.exec(ctx, `INSERT INTO idempotency_keys (principal, key, method, path, body_hash, status, response, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, r.Principal, r.Key, r.Method, r.Path, r.BodyHash, r.Status, r.Response, r.CreatedAt)
	return err
}

func (t *tx) PruneIdempotency(ctx context.Context, before time.Time) (int64, error) {
	return t.exec(ctx, `DELETE FROM idempotency_keys WHERE created_at < $1`, before)
}

func (t *tx) PruneHeartbeats(ctx context.Context, before time.Time) (int64, error) {
	return t.exec(ctx, `DELETE FROM worker_heartbeats WHERE at < $1`, before)
}

func (t *tx) InsertAudit(ctx context.Context, e domain.AuditEvent) error {
	_, err := t.exec(ctx, `INSERT INTO audit_events (tenant_id, actor_kind, actor_id, action, target, at, correlation_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, nullStr(string(e.TenantID)), e.Actor.Kind, e.Actor.ID, e.Action, e.Target, e.At, e.CorrelationID)
	return err
}

func (t *tx) UpsertArtifact(ctx context.Context, a domain.JobArtifact, key string, at time.Time) error {
	_, err := t.exec(ctx, `INSERT INTO job_artifacts (attempt_id, name, size, sha256, state, object_key, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (attempt_id, name) DO UPDATE
		SET size = EXCLUDED.size, sha256 = EXCLUDED.sha256, state = EXCLUDED.state, updated_at = EXCLUDED.updated_at
		WHERE job_artifacts.state = 'UPLOADING'`, a.AttemptID, a.Name, a.Size, a.SHA256, a.State, key, at)
	return err
}

func (t *tx) GetArtifact(ctx context.Context, a domain.AttemptID, name string) (domain.JobArtifact, string, error) {
	var x domain.JobArtifact
	var key string
	err := t.tx.QueryRow(ctx, `SELECT attempt_id, name, size, sha256, state, object_key FROM job_artifacts WHERE attempt_id = $1 AND name = $2`, a, name).
		Scan(&x.AttemptID, &x.Name, &x.Size, &x.SHA256, &x.State, &key)
	return x, key, notFound(err)
}

func (t *tx) ListArtifacts(ctx context.Context, tenant domain.TenantID, job domain.JobID) ([]domain.JobArtifact, error) {
	rows, err := t.tx.Query(ctx, `SELECT f.attempt_id, f.name, f.size, f.sha256, f.state FROM job_artifacts f
		JOIN job_attempts a ON a.id = f.attempt_id JOIN jobs j ON j.id = a.job_id
		WHERE j.id = $1 AND j.tenant_id = $2 ORDER BY a.number, f.name`, job, tenant)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(r pgx.Rows) (domain.JobArtifact, error) {
		var x domain.JobArtifact
		return x, r.Scan(&x.AttemptID, &x.Name, &x.Size, &x.SHA256, &x.State)
	})
}

func collect[T any](rows pgx.Rows, f func(pgx.Rows) (T, error)) ([]T, error) {
	defer rows.Close()
	var out []T
	for rows.Next() {
		v, err := f(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
