// Package application holds the use cases: submit, cancel, schedule, the
// worker protocol and reconciliation. It orchestrates domain rules inside
// store transactions and knows nothing about HTTP or SQL; the store, clock,
// object store and carbon source are ports injected by the caller.
package application

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/ecoshift"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
)

var (
	ErrNotFound          = errors.New("not found")
	ErrConflict          = errors.New("conflict")
	ErrCapacityConflict  = errors.New("capacity conflict: worker no longer has the free GPUs")
	ErrFenced            = errors.New("fenced: this scheduler is no longer the leader")
	ErrUnauthorized      = errors.New("unauthorized")
	ErrForbidden         = errors.New("forbidden")
	ErrInvalid           = errors.New("invalid request")
	ErrQuotaExceeded     = errors.New("quota exceeded")
	ErrQueueFull         = errors.New("queue full")
	ErrBudget            = errors.New("budget insufficient")
	ErrAlreadyAcked      = errors.New("dispatch already acknowledged")
	ErrLeaseInvalid      = errors.New("lease token invalid")
	ErrStaleVersion      = errors.New("stale version")
	ErrIdempotencyReused = errors.New("idempotency key reused with a different request")
	ErrUnavailable       = errors.New("dependency unavailable")
)

type Clock interface{ Now() time.Time }

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// IDGen returns unique ids with a type prefix ("job", "att", ...).
type IDGen interface{ New(prefix string) string }

// Store runs fn in one transaction. Implementations retry fn on
// serialization failures and deadlocks, so fn must be safe to re-run: it may
// read and write only through tx and must not have external side effects.
type Store interface {
	InTx(ctx context.Context, fn func(tx Tx) error) error
}

type TokenKind string

const (
	TokenUser      TokenKind = "user"
	TokenWorker    TokenKind = "worker"
	TokenBootstrap TokenKind = "bootstrap"
)

type Token struct {
	ID         string
	Kind       TokenKind
	SecretHash []byte
	UserID     domain.UserID
	WorkerID   domain.WorkerID
	PoolID     domain.PoolID
	Revoked    bool
}

// OpenAttempt is what the heartbeat path needs about an attempt a worker holds.
type OpenAttempt struct {
	Attempt       domain.Attempt
	JobState      domain.JobState
	StopRequested string
}

type StoredDecision struct {
	ID     int64
	TickID string
	At     time.Time
	Body   []byte // policy.Decision as JSON
}

type AttemptEvent struct {
	AttemptID domain.AttemptID
	Seq       int64
	Kind      string
	Payload   []byte
	At        time.Time
}

type IdempotencyRecord struct {
	Principal, Key, Method, Path, BodyHash string
	Status                                 int
	Response                               []byte
	CreatedAt                              time.Time
}

// Tx is everything a use case can do inside one transaction. Tenant-scoped
// reads take the tenant id explicitly; there is no unscoped read of a tenant
// object reachable from a user request (Lock* methods are for internal
// actors that already hold a non-user authority).
type Tx interface {
	CheckEpoch(ctx context.Context, epoch int64) error

	InsertTenant(ctx context.Context, t domain.Tenant) error
	GetTenant(ctx context.Context, id domain.TenantID) (domain.Tenant, error)
	InsertUser(ctx context.Context, u domain.User) error
	GetUser(ctx context.Context, id domain.UserID) (domain.User, error)
	InsertProject(ctx context.Context, p domain.Project) error
	GetProject(ctx context.Context, tenant domain.TenantID, id domain.ProjectID) (domain.Project, error)
	DefaultProject(ctx context.Context, tenant domain.TenantID) (domain.Project, error)
	LockProject(ctx context.Context, id domain.ProjectID) (domain.Project, error)
	ProjectSpend(ctx context.Context, id domain.ProjectID) (outstanding, settled domain.MicroUSD, err error)
	UpsertQuota(ctx context.Context, q domain.Quota) error
	GetQuota(ctx context.Context, tenant domain.TenantID) (domain.Quota, error)
	InsertToken(ctx context.Context, t Token) error
	GetToken(ctx context.Context, id string) (Token, error)
	ReplaceTokenSecret(ctx context.Context, id string, hash []byte) error

	UpsertPool(ctx context.Context, p domain.Pool) error
	GetPool(ctx context.Context, id domain.PoolID) (domain.Pool, error)
	ListPools(ctx context.Context) ([]domain.Pool, error)
	InsertWorker(ctx context.Context, w domain.Worker) error
	GetWorker(ctx context.Context, id domain.WorkerID) (domain.Worker, error)
	FindWorker(ctx context.Context, pool domain.PoolID, name string) (domain.Worker, error)
	ListWorkers(ctx context.Context) ([]domain.Worker, error)
	TouchWorker(ctx context.Context, id domain.WorkerID, at time.Time, state domain.WorkerState, held int) error
	MarkWorkersOffline(ctx context.Context, heartbeatBefore time.Time) ([]domain.WorkerID, error)
	ReserveCapacity(ctx context.Context, w domain.WorkerID, gpus int) error
	ReleaseCapacity(ctx context.Context, w domain.WorkerID, gpus int) error
	RecomputeCapacity(ctx context.Context) (repaired int, err error)

	InsertJob(ctx context.Context, j domain.Job) error
	GetJob(ctx context.Context, tenant domain.TenantID, id domain.JobID) (domain.Job, error)
	LockJob(ctx context.Context, id domain.JobID) (domain.Job, error)
	SaveJob(ctx context.Context, j domain.Job, prevVersion int64) error
	InsertTransition(ctx context.Context, r domain.TransitionRecord) error
	ListTransitions(ctx context.Context, id domain.JobID) ([]domain.TransitionRecord, error)
	ListJobs(ctx context.Context, tenant domain.TenantID, limit int) ([]domain.Job, error)
	CountQueued(ctx context.Context, tenant domain.TenantID) (int, error)
	CountQueuedAll(ctx context.Context) (int, error)
	ListQueued(ctx context.Context, limit int) ([]domain.Job, error)
	ListRunning(ctx context.Context) ([]policy.RunningJob, error)
	ListJobIDsInState(ctx context.Context, s domain.JobState, updatedBefore time.Time, limit int) ([]domain.JobID, error)
	TenantActiveGPUs(ctx context.Context) (map[domain.TenantID]int, error)
	TenantMaxGPUs(ctx context.Context) (map[domain.TenantID]int, error)
	ProjectAvailableAll(ctx context.Context) (map[domain.ProjectID]domain.MicroUSD, error)
	TenantUsage(ctx context.Context, now time.Time, window, halfLife time.Duration) (map[domain.TenantID]float64, error)

	InsertAttempt(ctx context.Context, a domain.Attempt) error
	GetAttempt(ctx context.Context, id domain.AttemptID) (domain.Attempt, error)
	AckAttempt(ctx context.Context, id domain.AttemptID, at time.Time) (bool, error)
	CloseAttempt(ctx context.Context, a domain.Attempt) error
	RequestStop(ctx context.Context, id domain.AttemptID, kind string) error
	OpenAttemptsForWorker(ctx context.Context, w domain.WorkerID) ([]OpenAttempt, error)
	InsertAttemptEvent(ctx context.Context, e AttemptEvent) (bool, error)
	ListAttemptEvents(ctx context.Context, job domain.JobID, kinds []string, limit int) ([]AttemptEvent, error)

	InsertReservation(ctx context.Context, r domain.Reservation) error
	ActiveReservation(ctx context.Context, job domain.JobID) (domain.Reservation, error)
	ReleaseReservation(ctx context.Context, id domain.ReservationID, at time.Time, reason string) (bool, error)
	ListStaleReservations(ctx context.Context, createdBefore time.Time, limit int) ([]domain.Reservation, error)
	PutLease(ctx context.Context, l domain.Lease) error
	GetLease(ctx context.Context, a domain.AttemptID) (domain.Lease, error)
	RenewLeases(ctx context.Context, w domain.WorkerID, ids []domain.AttemptID, now, expires time.Time) ([]domain.AttemptID, error)
	DeleteLease(ctx context.Context, a domain.AttemptID) error
	ExpiredLeases(ctx context.Context, now time.Time, limit int) ([]domain.Lease, error)

	InsertLedger(ctx context.Context, e domain.LedgerEntry) error
	JobBudget(ctx context.Context, job domain.JobID) (domain.JobBudget, error)
	TerminalJobsWithOutstanding(ctx context.Context, limit int) ([]domain.JobID, error)

	UpsertPolicy(ctx context.Context, p policy.Policy) error
	InsertDecision(ctx context.Context, tick string, d policy.Decision, at time.Time) error
	LastDecision(ctx context.Context, job domain.JobID) (policy.Action, string, bool, error)
	ListDecisions(ctx context.Context, tenant domain.TenantID, job domain.JobID) ([]StoredDecision, error)

	UpsertCarbon(ctx context.Context, rs []domain.CarbonSnapshot) error
	CarbonHistory(ctx context.Context, region string, since, until time.Time) ([]ecoshift.Reading, error)
	CarbonRegions(ctx context.Context) ([]string, error)

	Enqueue(ctx context.Context, topic, key string, payload []byte, availableAt time.Time) error
	Claim(ctx context.Context, topic string, n int, now time.Time, visibility time.Duration, by string) ([]domain.OutboxEvent, error)
	MarkDelivered(ctx context.Context, id int64, at time.Time) error
	PruneOutbox(ctx context.Context, deliveredBefore time.Time) (int64, error)
	OutboxDepth(ctx context.Context) (map[string]int, error)

	GetIdempotency(ctx context.Context, principal, key string) (IdempotencyRecord, error)
	PutIdempotency(ctx context.Context, r IdempotencyRecord) error
	PruneIdempotency(ctx context.Context, before time.Time) (int64, error)
	PruneHeartbeats(ctx context.Context, before time.Time) (int64, error)
	InsertAudit(ctx context.Context, e domain.AuditEvent) error

	UpsertArtifact(ctx context.Context, a domain.JobArtifact, key string, at time.Time) error
	GetArtifact(ctx context.Context, a domain.AttemptID, name string) (domain.JobArtifact, string, error)
	ListArtifacts(ctx context.Context, tenant domain.TenantID, job domain.JobID) ([]domain.JobArtifact, error)
}

// ObjectStore holds logs and artifacts. Writes are resumable: Append at an
// explicit offset fails with ErrOffsetMismatch carrying the committed size.
type ObjectStore interface {
	Append(ctx context.Context, key string, offset int64, data []byte) (int64, error)
	Size(ctx context.Context, key string) (int64, error)
	Read(ctx context.Context, key string, offset int64, max int) ([]byte, error)
	Digest(ctx context.Context, key string) (string, error)
}

type OffsetMismatchError struct{ Committed int64 }

func (e *OffsetMismatchError) Error() string {
	return "offset mismatch; committed size is " + strconv.FormatInt(e.Committed, 10)
}
