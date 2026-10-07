package domain

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

type (
	TenantID      string
	UserID        string
	ProjectID     string
	PoolID        string
	WorkerID      string
	JobID         string
	AttemptID     string
	ReservationID string
)

// EvidenceClass is fixed at tenant creation and never updated (a database
// trigger rejects the update). It is what keeps simulator, seeded demo and
// pilot rows out of anything presented as real use. See ADR 0009.
type EvidenceClass string

const (
	EvidenceSimulator EvidenceClass = "simulator"
	EvidenceSeeded    EvidenceClass = "seeded"
	EvidencePilot     EvidenceClass = "pilot"
	EvidenceReal      EvidenceClass = "real"
)

func (e EvidenceClass) Valid() bool {
	switch e {
	case EvidenceSimulator, EvidenceSeeded, EvidencePilot, EvidenceReal:
		return true
	}
	return false
}

type Tenant struct {
	ID            TenantID
	Name          string
	EvidenceClass EvidenceClass
	CreatedAt     time.Time
}

type Role string

const (
	RoleMember   Role = "member"
	RoleOperator Role = "operator"
)

type User struct {
	ID       UserID
	TenantID TenantID
	Handle   string
	Role     Role
}

type Project struct {
	ID       ProjectID
	TenantID TenantID
	Name     string
	// BudgetMicroUSD is the project's spend ceiling. Holds plus settled spend
	// may never exceed it.
	BudgetMicroUSD MicroUSD
}

// Quota bounds what one tenant can occupy. It is backpressure that does not
// depend on budget: a tenant with a large budget still cannot queue without
// bound, because every queued job is rescored on every tick.
type Quota struct {
	TenantID      TenantID
	MaxQueuedJobs int
	MaxActiveGPUs int
}

// MicroUSD is money in millionths of a dollar. Integers, so a ledger sum is
// exact and a hold can be compared against a settle without rounding.
type MicroUSD int64

func (m MicroUSD) String() string {
	sign := ""
	if m < 0 {
		sign, m = "-", -m
	}
	return fmt.Sprintf("%s$%d.%06d", sign, int64(m)/1_000_000, int64(m)%1_000_000)
}

type PoolKind string

const (
	PoolKubernetes PoolKind = "kubernetes"
	PoolSimulated  PoolKind = "simulated"
)

type Pool struct {
	ID     PoolID
	Name   string
	Kind   PoolKind
	Region string
	// PriceMicroUSDPerGPUHour is the price the scheduler charges against the
	// budget. It is fixed onto the reservation when the job is placed, so a
	// price change mid-run cannot make a settle exceed its hold.
	PriceMicroUSDPerGPUHour MicroUSD
	// Interruptible pools (spot) can lose capacity at any time; jobs placed
	// there must have retries left for the placement to be useful.
	Interruptible bool
	// PUE multiplies board energy to facility energy. 1.0 means unknown and is
	// the conservative default for a placement comparison.
	PUE float64
}

type WorkerState string

const (
	WorkerReady    WorkerState = "READY"
	WorkerDraining WorkerState = "DRAINING"
	WorkerOffline  WorkerState = "OFFLINE"
)

// Capability is what a worker declares at registration. The ack path checks
// an attempt against it again, independent of the scheduler, so a scheduler
// bug cannot run a job outside what the worker said it can do.
type Capability struct {
	GPUModel string
	GPUs     int
	GPUMemGB int
	Runtimes []string
	Region   string
}

type Worker struct {
	ID            WorkerID
	PoolID        PoolID
	Name          string
	Capability    Capability
	GPUsReserved  int
	State         WorkerState
	LastHeartbeat time.Time
	Version       int64
}

func (w Worker) FreeGPUs() int { return w.Capability.GPUs - w.GPUsReserved }

// Requirements is what a job needs from one worker. Jobs are single-node.
type Requirements struct {
	GPUs        int
	GPUModel    string // empty means any model
	MinGPUMemGB int
	Runtime     string
}

// RejectReason is a stable code; it appears in decisions, the API and metrics.
type RejectReason string

const (
	RejectGPUModel       RejectReason = "gpu_model_mismatch"
	RejectGPUMemory      RejectReason = "gpu_memory_insufficient"
	RejectRuntime        RejectReason = "runtime_unsupported"
	RejectGPUCount       RejectReason = "gpu_count_exceeds_worker"
	RejectCapacity       RejectReason = "insufficient_free_gpus"
	RejectWorkerNotReady RejectReason = "worker_not_ready"
	RejectPoolNotAllowed RejectReason = "pool_not_allowed"
	RejectBudget         RejectReason = "budget_insufficient"
	RejectDeadline       RejectReason = "deadline_unreachable"
	RejectProtectedHead  RejectReason = "held_for_starving_job"
	RejectInterruptible  RejectReason = "interruptible_without_retries"
)

// Satisfies checks static capability only (not free capacity). It is the
// single definition used by the scheduler filter and by the ack-time check.
func (c Capability) Satisfies(r Requirements) (RejectReason, bool) {
	if r.GPUModel != "" && r.GPUModel != c.GPUModel {
		return RejectGPUModel, false
	}
	if r.MinGPUMemGB > c.GPUMemGB {
		return RejectGPUMemory, false
	}
	if r.Runtime != "" && !slices.Contains(c.Runtimes, r.Runtime) {
		return RejectRuntime, false
	}
	if r.GPUs > c.GPUs {
		return RejectGPUCount, false
	}
	return "", true
}

// Job is the aggregate root. State changes only through Transition.
type Job struct {
	ID            JobID
	TenantID      TenantID
	ProjectID     ProjectID
	UserID        UserID
	Image         string
	Command       []string
	Requirements  Requirements
	Priority      int // 0 (lowest) .. 9
	Preemptible   bool
	Deadline      *time.Time
	MaxRuntime    time.Duration
	MaxDelay      time.Duration // 0: may not be delayed for carbon or cost
	BudgetCap     MicroUSD      // per-job ceiling, 0 = project budget only
	PolicyName    string
	AllowedPools  []PoolID // empty = any
	MaxAttempts   int
	Attempt       int // number of the current (latest) attempt, 0 before the first
	State         JobState
	NotBefore     *time.Time // set by a DELAY decision
	SubmittedAt   time.Time
	StartedAt     *time.Time
	FinishedAt    *time.Time
	UpdatedAt     time.Time
	Version       int64
	EvidenceClass EvidenceClass
}

func (j Job) RetriesLeft() bool { return j.Attempt < j.MaxAttempts }

// AttemptOutcome is written once per attempt and never changed.
type AttemptOutcome string

const (
	OutcomeSucceeded AttemptOutcome = "SUCCEEDED"
	OutcomeFailed    AttemptOutcome = "FAILED"
	OutcomeLost      AttemptOutcome = "LOST"
	OutcomePreempted AttemptOutcome = "PREEMPTED"
	OutcomeCancelled AttemptOutcome = "CANCELLED"
	OutcomeExpired   AttemptOutcome = "EXPIRED"
	OutcomeTimedOut  AttemptOutcome = "TIMED_OUT"
)

func (o AttemptOutcome) Valid() bool {
	switch o {
	case OutcomeSucceeded, OutcomeFailed, OutcomeLost, OutcomePreempted,
		OutcomeCancelled, OutcomeExpired, OutcomeTimedOut:
		return true
	}
	return false
}

type Attempt struct {
	ID        AttemptID
	JobID     JobID
	Number    int
	WorkerID  WorkerID
	CreatedAt time.Time
	AckedAt   *time.Time
	EndedAt   *time.Time
	Outcome   AttemptOutcome // empty while open
	ExitCode  *int
	Reason    string
}

var ErrAttemptClosed = errors.New("attempt already has an outcome")

// CloseAttempt records an attempt's outcome. A closed attempt is immutable;
// a retry is a new Attempt, never a reopened one.
func CloseAttempt(a Attempt, outcome AttemptOutcome, exit *int, reason string, at time.Time) (Attempt, error) {
	if a.Outcome != "" {
		return a, ErrAttemptClosed
	}
	if !outcome.Valid() {
		return a, fmt.Errorf("unknown attempt outcome %q", outcome)
	}
	t := at.UTC()
	a.Outcome, a.ExitCode, a.Reason, a.EndedAt = outcome, exit, reason, &t
	return a, nil
}

// JobStateForOutcome maps a closed attempt to the job's next state, given
// whether a retry is allowed. Lost and preempted attempts are retried while
// attempts remain; a job the user's code failed is not, because rerunning
// the same failing command spends budget to learn nothing new.
func JobStateForOutcome(j Job, o AttemptOutcome, now time.Time) JobState {
	switch o {
	case OutcomeSucceeded:
		return JobSucceeded
	case OutcomeCancelled:
		return JobCancelled
	case OutcomeExpired:
		return JobExpired
	case OutcomeLost, OutcomePreempted:
		if j.State == JobCancelRequested {
			return JobCancelled
		}
		if !j.RetriesLeft() {
			return JobFailed
		}
		if j.Deadline != nil && !now.Before(*j.Deadline) {
			return JobExpired
		}
		return JobQueued
	default: // FAILED, TIMED_OUT
		return JobFailed
	}
}

type Reservation struct {
	ID         ReservationID
	JobID      JobID
	AttemptID  AttemptID
	WorkerID   WorkerID
	PoolID     PoolID
	GPUs       int
	PriceRate  MicroUSD // per GPU-hour, fixed at placement
	HoldAmount MicroUSD
	Epoch      int64 // scheduler leader epoch that created it (fencing)
	CreatedAt  time.Time
	ReleasedAt *time.Time
}

type Lease struct {
	AttemptID AttemptID
	WorkerID  WorkerID
	ExpiresAt time.Time
	RenewedAt time.Time
}

func (l Lease) Expired(now time.Time) bool { return !now.Before(l.ExpiresAt) }

type ArtifactState string

const (
	ArtifactUploading ArtifactState = "UPLOADING"
	ArtifactComplete  ArtifactState = "COMPLETE"
)

type JobArtifact struct {
	AttemptID AttemptID
	Name      string
	Size      int64
	SHA256    string
	State     ArtifactState
}

// AuditEvent records a security-relevant action by a principal.
type AuditEvent struct {
	TenantID      TenantID
	Actor         Actor
	Action        string
	Target        string
	At            time.Time
	CorrelationID string
}

// OutboxEvent is a durable message written in the same transaction as the
// state change that caused it. Delivery is at-least-once.
type OutboxEvent struct {
	ID          int64
	Topic       string
	Key         string
	Payload     []byte
	AvailableAt time.Time
	Attempts    int
}

// CarbonSnapshot is one intensity reading used for a decision.
type CarbonSnapshot struct {
	Region      string
	At          time.Time // the interval the reading describes
	ObservedAt  time.Time // when we obtained it
	GramsPerKWh float64
	Source      string
}

// CostSnapshot is the price a decision used.
type CostSnapshot struct {
	PoolID                  PoolID
	At                      time.Time
	PriceMicroUSDPerGPUHour MicroUSD
	Source                  string
}
