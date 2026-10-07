package metrics

import (
	"context"
	"errors"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
)

func TestEventMetricsExposeOperationalContract(t *testing.T) {
	m := New()
	m.ReservationConflict("capacity")
	m.StaleLease("worker")
	m.AttemptOutcome(domain.OutcomeLost)
	m.ArtifactFailure("upload")
	m.Fenced()
	m.SetLeader(true)
	m.StageLatency("reserve_to_dispatch", 2*time.Second)
	m.HTTP("POST /v1/jobs", 503, time.Second)
	m.StatusAge(61 * time.Second)
	m.Decision(policy.Decision{PolicyVersion: "balanced-v1", Fallbacks: []string{"carbon_stale:gb-london", "fallback:deadline"}})

	families, err := m.Reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]float64{}
	for _, family := range families {
		for _, metric := range family.Metric {
			if metric.Counter != nil {
				values[family.GetName()] += metric.Counter.GetValue()
			}
			if metric.Gauge != nil {
				values[family.GetName()] += metric.Gauge.GetValue()
			}
		}
	}
	for _, name := range []string{"gpub_scheduler_fenced_total", "gpub_scheduler_is_leader", "gpub_ecoshift_fallback_total", "gpub_artifact_failures_total", "gpub_http_requests_total"} {
		if values[name] != 1 {
			t.Fatalf("%s = %v, want 1", name, values[name])
		}
	}
	if values["gpub_status_snapshot_age_seconds"] != 61 {
		t.Fatal("status freshness was not exported")
	}
}

type gaugeStore struct{ fail bool }

func (s gaugeStore) InTx(ctx context.Context, fn func(application.Tx) error) error {
	if s.fail {
		return errors.New("database unavailable")
	}
	return fn(emptyGaugeTx{})
}

type emptyGaugeTx struct{ application.Tx }

func (emptyGaugeTx) ListQueued(context.Context, int) ([]domain.Job, error) { return nil, nil }
func (emptyGaugeTx) ListWorkers(context.Context) ([]domain.Worker, error)  { return nil, nil }
func (emptyGaugeTx) OutboxDepth(context.Context) (map[string]int, error)   { return nil, nil }
func (emptyGaugeTx) ProjectAvailableAll(context.Context) (map[domain.ProjectID]domain.MicroUSD, error) {
	return nil, nil
}
func (emptyGaugeTx) CarbonRegions(context.Context) ([]string, error) { return nil, nil }

func TestFailedRefreshDoesNotAdvanceFreshness(t *testing.T) {
	m := New()
	now := time.Unix(1000, 0)
	if err := m.Collect(context.Background(), gaugeStore{}, now); err != nil {
		t.Fatal(err)
	}
	if err := m.Collect(context.Background(), gaugeStore{fail: true}, now.Add(time.Minute)); err == nil {
		t.Fatal("outage hidden")
	}
	families, err := m.Reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == "gpub_state_refresh_timestamp_seconds" && f.Metric[0].Gauge.GetValue() != 1000 {
			t.Fatal("failed refresh advanced freshness")
		}
		if f.GetName() == "gpub_store_up" && f.Metric[0].Gauge.GetValue() != 0 {
			t.Fatal("failed refresh reported healthy")
		}
	}
}
