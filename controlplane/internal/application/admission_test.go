package application_test

import (
	"errors"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/testkit"
)

func TestConcurrentSubmissionsRespectAdmissionLimitsAndReopenedSlot(t *testing.T) {
	for _, limit := range []string{"tenant", "global"} {
		t.Run(limit, func(t *testing.T) {
			e := testkit.New(t)
			a, _ := e.Tenant("a", 100)
			b, _ := e.Tenant("b", 100)
			want := application.ErrQueueFull
			if limit == "tenant" {
				e.Exec(`UPDATE quotas SET max_queued_jobs = 1 WHERE tenant_id = $1`, a.TenantID)
				b, want = a, application.ErrQuotaExceeded
			} else {
				e.Svc.Cfg.MaxQueuedGlobal = 1
			}
			req := application.SubmitRequest{Image: "fixture:v1", Command: []string{"sim"}, GPUs: 1, MaxRuntime: time.Hour}
			type result struct {
				job domain.Job
				who application.Principal
				err error
			}
			start, done := make(chan struct{}), make(chan result, 8)
			for i := range 8 {
				who := a
				if i%2 == 1 {
					who = b
				}
				go func() {
					<-start
					job, err := e.Svc.Submit(e.Ctx, who, req, "concurrent")
					done <- result{job, who, err}
				}()
			}
			close(start)
			var accepted []result
			for range 8 {
				r := <-done
				if r.err == nil {
					accepted = append(accepted, r)
				} else if !errors.Is(r.err, want) || r.job.ID != "" {
					t.Fatalf("admission refusal: %+v", r)
				}
			}
			if len(accepted) != 1 || e.QueryInt(`SELECT count(*) FROM jobs`) != 1 {
				t.Fatalf("admission limit exceeded: accepted=%d rows=%d", len(accepted), e.QueryInt(`SELECT count(*) FROM jobs`))
			}
			_, err := e.Svc.Cancel(e.Ctx, accepted[0].who, accepted[0].job.ID, "reopen")
			e.Must(err)
			job, err := e.Svc.Submit(e.Ctx, a, req, "reopened")
			e.Must(err)
			if job.State != domain.JobQueued || e.QueryInt(`SELECT count(*) FROM jobs WHERE state = 'QUEUED'`) != 1 {
				t.Fatal("cancellation did not reopen one admission slot")
			}
			if _, err := e.Svc.Submit(e.Ctx, a, req, "serial-refusal"); !errors.Is(err, want) || e.QueryInt(`SELECT count(*) FROM jobs`) != 2 {
				t.Fatalf("serial refusal after reopened slot: %v", err)
			}
		})
	}
}
