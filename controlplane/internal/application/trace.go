package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

const TraceSchema = "gpu-broker.controlplane-trace/v1"

// Minimums mirror the Python exporter: aliases do not anonymise a queue of
// one person, so small histories are refused rather than exported.
const (
	MinTraceJobs  = 20
	MinTraceUsers = 5
)

// TraceJob carries only what policy evaluation needs. No command, image,
// id, name, or wall-clock time leaves the database.
type TraceJob struct {
	Key         string   `json:"job_key"`
	UserKey     string   `json:"user_key"`
	GPUs        int      `json:"gpus"`
	GPUModel    string   `json:"gpu_model"`
	Priority    int      `json:"priority"`
	Preemptible bool     `json:"preemptible"`
	MaxRuntimeS int64    `json:"max_runtime_s"`
	MaxDelayS   int64    `json:"max_delay_s"`
	DeadlineS   *float64 `json:"deadline_offset_seconds"`
	State       string   `json:"state"`
	Attempts    int      `json:"attempts"`
	SubmittedS  float64  `json:"submitted_offset_seconds"`
	StartedS    *float64 `json:"started_offset_seconds"`
	FinishedS   *float64 `json:"finished_offset_seconds"`
}

type Trace struct {
	Schema   string     `json:"schema"`
	Source   string     `json:"source"`
	Evidence string     `json:"evidence_class"`
	Privacy  []string   `json:"privacy_excluded"`
	Jobs     []TraceJob `json:"jobs"`
	Digest   string     `json:"digest"`
}

// ExportTrace exports the operator's own tenant. Users inside the tenant are
// aliased by first submission; the tenant itself is not named.
func (s *Service) ExportTrace(ctx context.Context, p Principal) (Trace, error) {
	if p.Kind != TokenUser || p.Role != domain.RoleOperator {
		return Trace{}, ErrForbidden
	}
	var jobs []domain.Job
	if err := s.Store.InTx(ctx, func(tx Tx) (err error) {
		jobs, err = tx.ListJobs(ctx, p.TenantID, 100000)
		return err
	}); err != nil {
		return Trace{}, err
	}
	sort.Slice(jobs, func(i, j int) bool {
		if !jobs[i].SubmittedAt.Equal(jobs[j].SubmittedAt) {
			return jobs[i].SubmittedAt.Before(jobs[j].SubmittedAt)
		}
		return jobs[i].ID < jobs[j].ID
	})
	users := map[domain.UserID]string{}
	for _, j := range jobs {
		if _, ok := users[j.UserID]; !ok {
			users[j.UserID] = fmt.Sprintf("user-%03d", len(users)+1)
		}
	}
	if len(jobs) < MinTraceJobs || len(users) < MinTraceUsers {
		return Trace{}, fmt.Errorf("%w: %d jobs from %d users; a trace needs at least %d jobs from %d users",
			ErrInvalid, len(jobs), len(users), MinTraceJobs, MinTraceUsers)
	}
	epoch := jobs[0].SubmittedAt
	off := func(t *time.Time) *float64 {
		if t == nil {
			return nil
		}
		v := t.Sub(epoch).Seconds()
		return &v
	}
	tr := Trace{Schema: TraceSchema, Source: "gpubroker control plane", Evidence: string(p.Evidence),
		Privacy: []string{"commands", "images", "job ids", "user names", "tenant", "workers", "wall-clock timestamps"}}
	for i, j := range jobs {
		tr.Jobs = append(tr.Jobs, TraceJob{Key: fmt.Sprintf("job-%05d", i+1), UserKey: users[j.UserID], GPUs: j.Requirements.GPUs,
			GPUModel: j.Requirements.GPUModel, Priority: j.Priority, Preemptible: j.Preemptible,
			MaxRuntimeS: int64(j.MaxRuntime / time.Second), MaxDelayS: int64(j.MaxDelay / time.Second), DeadlineS: off(j.Deadline),
			State: string(j.State), Attempts: j.Attempt, SubmittedS: j.SubmittedAt.Sub(epoch).Seconds(),
			StartedS: off(j.StartedAt), FinishedS: off(j.FinishedAt)})
	}
	tr.Digest = TraceDigest(tr)
	return tr, nil
}

// TraceDigest hashes the trace with its digest field empty.
func TraceDigest(t Trace) string {
	t.Digest = ""
	b, _ := json.Marshal(t)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
