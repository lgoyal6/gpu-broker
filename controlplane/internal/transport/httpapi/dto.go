package httpapi

import (
	"encoding/json"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

// Wire types. They are the contract in contracts/openapi/gpubroker-v1.yaml;
// the contract test validates live responses against that file, so a field
// added here without the schema (or the reverse) fails CI.

type SubmitJobRequest struct {
	ProjectID    string     `json:"project_id,omitempty"`
	Image        string     `json:"image"`
	Command      []string   `json:"command"`
	GPUs         int        `json:"gpus"`
	GPUModel     string     `json:"gpu_model,omitempty"`
	MinGPUMemGB  int        `json:"min_gpu_mem_gb,omitempty"`
	Runtime      string     `json:"runtime,omitempty"`
	Priority     int        `json:"priority,omitempty"`
	Preemptible  bool       `json:"preemptible,omitempty"`
	Deadline     *time.Time `json:"deadline,omitempty"`
	MaxRuntimeS  int64      `json:"max_runtime_s"`
	MaxDelayS    int64      `json:"max_delay_s,omitempty"`
	BudgetCapUSD float64    `json:"budget_cap_usd,omitempty"`
	Policy       string     `json:"policy,omitempty"`
	AllowedPools []string   `json:"allowed_pools,omitempty"`
	MaxAttempts  int        `json:"max_attempts,omitempty"`
}

func (r SubmitJobRequest) toApp() application.SubmitRequest {
	pools := make([]domain.PoolID, 0, len(r.AllowedPools))
	for _, p := range r.AllowedPools {
		pools = append(pools, domain.PoolID(p))
	}
	return application.SubmitRequest{
		ProjectID: domain.ProjectID(r.ProjectID), Image: r.Image, Command: r.Command, GPUs: r.GPUs, GPUModel: r.GPUModel,
		MinGPUMemGB: r.MinGPUMemGB, Runtime: r.Runtime, Priority: r.Priority, Preemptible: r.Preemptible,
		Deadline: r.Deadline, MaxRuntime: time.Duration(r.MaxRuntimeS) * time.Second,
		MaxDelay: time.Duration(r.MaxDelayS) * time.Second, BudgetCapUSD: r.BudgetCapUSD, Policy: r.Policy,
		AllowedPools: pools, MaxAttempts: r.MaxAttempts,
	}
}

type Job struct {
	ID            string     `json:"id"`
	ProjectID     string     `json:"project_id"`
	State         string     `json:"state"`
	Image         string     `json:"image"`
	Command       []string   `json:"command"`
	GPUs          int        `json:"gpus"`
	GPUModel      string     `json:"gpu_model"`
	Priority      int        `json:"priority"`
	Preemptible   bool       `json:"preemptible"`
	Policy        string     `json:"policy"`
	Attempt       int        `json:"attempt"`
	MaxAttempts   int        `json:"max_attempts"`
	MaxRuntimeS   int64      `json:"max_runtime_s"`
	MaxDelayS     int64      `json:"max_delay_s"`
	Deadline      *time.Time `json:"deadline"`
	NotBefore     *time.Time `json:"not_before"`
	SubmittedAt   time.Time  `json:"submitted_at"`
	StartedAt     *time.Time `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at"`
	EvidenceClass string     `json:"evidence_class"`
}

func jobDTO(j domain.Job) Job {
	return Job{ID: string(j.ID), ProjectID: string(j.ProjectID), State: string(j.State), Image: j.Image, Command: j.Command,
		GPUs: j.Requirements.GPUs, GPUModel: j.Requirements.GPUModel, Priority: j.Priority, Preemptible: j.Preemptible,
		Policy: j.PolicyName, Attempt: j.Attempt, MaxAttempts: j.MaxAttempts, MaxRuntimeS: int64(j.MaxRuntime / time.Second),
		MaxDelayS: int64(j.MaxDelay / time.Second), Deadline: j.Deadline, NotBefore: j.NotBefore, SubmittedAt: j.SubmittedAt,
		StartedAt: j.StartedAt, FinishedAt: j.FinishedAt, EvidenceClass: string(j.EvidenceClass)}
}

type Transition struct {
	From          string    `json:"from"`
	To            string    `json:"to"`
	ActorKind     string    `json:"actor_kind"`
	Reason        string    `json:"reason"`
	At            time.Time `json:"at"`
	CorrelationID string    `json:"correlation_id"`
}

type Budget struct {
	HeldUSD        float64 `json:"held_usd"`
	SettledUSD     float64 `json:"settled_usd"`
	ReleasedUSD    float64 `json:"released_usd"`
	OutstandingUSD float64 `json:"outstanding_usd"`
}

type Artifact struct {
	AttemptID string `json:"attempt_id"`
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	SHA256    string `json:"sha256"`
	State     string `json:"state"`
}

type JobDetail struct {
	Job         Job          `json:"job"`
	Transitions []Transition `json:"transitions"`
	Budget      Budget       `json:"budget"`
	Artifacts   []Artifact   `json:"artifacts"`
}

func usd(m domain.MicroUSD) float64 { return float64(m) / 1e6 }

func jobDetailDTO(v application.JobView) JobDetail {
	d := JobDetail{Job: jobDTO(v.Job), Transitions: []Transition{}, Artifacts: []Artifact{},
		Budget: Budget{HeldUSD: usd(v.Budget.Held), SettledUSD: usd(v.Budget.Settled), ReleasedUSD: usd(v.Budget.Released),
			OutstandingUSD: usd(v.Budget.Outstanding())}}
	for _, t := range v.Transitions {
		// actor_id is deliberately omitted: it can name a worker or scheduler
		// replica, which is infrastructure detail, not the tenant's.
		d.Transitions = append(d.Transitions, Transition{From: string(t.From), To: string(t.To), ActorKind: string(t.Actor.Kind),
			Reason: t.Reason, At: t.At, CorrelationID: t.CorrelationID})
	}
	for _, a := range v.Artifacts {
		d.Artifacts = append(d.Artifacts, Artifact{AttemptID: string(a.AttemptID), Name: a.Name, Size: a.Size, SHA256: a.SHA256, State: string(a.State)})
	}
	return d
}

type JobList struct {
	Jobs []Job `json:"jobs"`
}

type DecisionList struct {
	Decisions []json.RawMessage `json:"decisions"`
}

type LogLine struct {
	AttemptID string    `json:"attempt_id"`
	Seq       int64     `json:"seq"`
	Kind      string    `json:"kind"`
	At        time.Time `json:"at"`
	Message   string    `json:"message"`
}

type LogList struct {
	Lines []LogLine `json:"lines"`
}

type Pool struct {
	ID                 string   `json:"id"`
	Name               string   `json:"name"`
	Kind               string   `json:"kind"`
	Region             string   `json:"region"`
	PriceUSDPerGPUHour float64  `json:"price_usd_per_gpu_hour"`
	Interruptible      bool     `json:"interruptible"`
	WorkersReady       int      `json:"workers_ready"`
	GPUsTotal          int      `json:"gpus_total"`
	GPUsFree           int      `json:"gpus_free"`
	GPUModels          []string `json:"gpu_models"`
}

type PoolList struct {
	Pools []Pool `json:"pools"`
}

type CreateUserRequest struct {
	Handle string `json:"handle"`
	Role   string `json:"role"`
}

type CreateUserResponse struct {
	UserID string `json:"user_id"`
	Token  string `json:"token"`
}

// ---- worker protocol ----

type RegisterWorkerRequest struct {
	Name     string   `json:"name"`
	GPUModel string   `json:"gpu_model"`
	GPUs     int      `json:"gpus"`
	GPUMemGB int      `json:"gpu_mem_gb"`
	Runtimes []string `json:"runtimes"`
}

type RegisterWorkerResponse struct {
	WorkerID string `json:"worker_id"`
	PoolID   string `json:"pool_id"`
	Token    string `json:"token"`
}

type HeartbeatRequest struct {
	State string   `json:"state"`
	Held  []string `json:"held"`
}

type HeartbeatResponse struct {
	Renewed   []string                `json:"renewed"`
	Stop      []application.StopOrder `json:"stop"`
	Abandon   []string                `json:"abandon"`
	LeaseTTLS int64                   `json:"lease_ttl_s"`
}

type ClaimRequest struct {
	Max int `json:"max"`
}

type ClaimResponse struct {
	Dispatches []application.Dispatch `json:"dispatches"`
}

type AckResponse struct {
	Action string `json:"action"`
}

type EventsRequest struct {
	Events []application.WorkerEvent `json:"events"`
}

type EventsResponse struct {
	Outcome string `json:"outcome"`
}

type UploadResponse struct {
	Offset int64 `json:"offset"`
}

type CompleteArtifactRequest struct {
	SHA256 string `json:"sha256"`
}

// ErrorBody is the stable error envelope. Code is the machine-readable part
// and never changes meaning; Message may be reworded.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Hint      string `json:"hint,omitempty"`
	RequestID string `json:"request_id"`
	JobID     string `json:"job_id,omitempty"`
	Offset    *int64 `json:"offset,omitempty"`
}
