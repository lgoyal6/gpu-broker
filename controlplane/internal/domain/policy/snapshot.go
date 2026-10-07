package policy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/ecoshift"
)

// RunningJob is what preemption needs to know about a job holding capacity.
type RunningJob struct {
	JobID       domain.JobID
	TenantID    domain.TenantID
	WorkerID    domain.WorkerID
	GPUs        int
	Priority    int
	Preemptible bool
	StartedAt   time.Time
}

// Snapshot is the complete input to one scheduling tick. Everything Decide
// needs is here as data, so a decision can be replayed from its snapshot.
type Snapshot struct {
	Now     time.Time
	Jobs    []domain.Job // QUEUED jobs only
	Workers []domain.Worker
	Pools   map[domain.PoolID]domain.Pool
	Running []RunningJob

	// TenantUsage is decayed GPU-hours per tenant over the fair-share window.
	TenantUsage map[domain.TenantID]float64
	// TenantActiveGPUs and TenantMaxGPUs enforce the concurrency quota.
	TenantActiveGPUs map[domain.TenantID]int
	TenantMaxGPUs    map[domain.TenantID]int
	// ProjectAvailable is budget less outstanding holds and settled spend.
	ProjectAvailable map[domain.ProjectID]domain.MicroUSD

	Carbon map[string]ecoshift.RegionCarbon // by region
}

// Digest is a stable hash of the snapshot, stored on every decision: two
// decisions with the same digest and policy version must be identical.
// encoding/json writes map keys sorted, which is what makes this stable.
func (s Snapshot) Digest() string {
	s.Now = s.Now.UTC()
	b, err := json.Marshal(s)
	if err != nil {
		panic(err) // every field is plain data; failure is a programming error
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16])
}
