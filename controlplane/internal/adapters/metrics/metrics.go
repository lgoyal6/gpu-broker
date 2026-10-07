// Package metrics turns application events and store state into Prometheus
// series. Names are part of the operator contract (dashboards and alert rules
// reference them); renaming one is a breaking change to infra/.
package metrics

import (
	"context"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
)

var latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600}

type Metrics struct {
	Reg *prometheus.Registry

	transitions   *prometheus.CounterVec
	decisions     *prometheus.CounterVec
	fallbacks     *prometheus.CounterVec
	tickSeconds   prometheus.Histogram
	decisionSecs  prometheus.Histogram
	conflicts     *prometheus.CounterVec
	staleLeases   *prometheus.CounterVec
	outcomes      *prometheus.CounterVec
	stage         *prometheus.HistogramVec
	artifactFails *prometheus.CounterVec
	repairs       *prometheus.CounterVec
	fenced        prometheus.Counter
	txRetries     *prometheus.CounterVec
	httpRequests  *prometheus.CounterVec
	httpSeconds   *prometheus.HistogramVec
	carbonEffect  prometheus.Counter
	isLeader      prometheus.Gauge

	queueDepth    *prometheus.GaugeVec
	poolGPUs      *prometheus.GaugeVec
	fragmentation *prometheus.GaugeVec
	heartbeatAge  *prometheus.GaugeVec
	outboxDepth   *prometheus.GaugeVec
	projectAvail  *prometheus.GaugeVec
	carbonAge     *prometheus.GaugeVec
	statusAge     prometheus.Gauge
	storeUp       prometheus.Gauge
	lastRefresh   prometheus.Gauge
}

func New() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { r.MustRegister(c) }
	m := &Metrics{Reg: r}
	m.transitions = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gpub_job_transitions_total",
		Help: "Job state transitions."}, []string{"from", "to", "evidence_class"})
	m.decisions = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gpub_scheduler_decisions_total",
		Help: "Scheduling decisions by policy version and action."}, []string{"policy", "action"})
	m.fallbacks = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gpub_ecoshift_fallback_total",
		Help: "Decisions that fell back because carbon data was stale, missing, or unusable."}, []string{"policy", "reason"})
	m.tickSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "gpub_scheduler_tick_seconds",
		Help: "Wall time of one scheduling pass, snapshot to last applied decision.", Buckets: latencyBuckets})
	m.decisionSecs = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "gpub_scheduler_decision_seconds",
		Help: "Tick time divided by decisions in the tick.", Buckets: []float64{1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, .01, .05, .1}})
	m.conflicts = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gpub_reservation_conflicts_total",
		Help: "Reservations refused at commit (capacity CAS lost, budget changed)."}, []string{"reason"})
	m.staleLeases = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gpub_stale_reclaims_total",
		Help: "Attempts reclaimed by the reconciler."}, []string{"kind"})
	m.outcomes = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gpub_attempt_outcomes_total",
		Help: "Attempt terminal outcomes. LOST and PREEMPTED are retries."}, []string{"outcome"})
	m.stage = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gpub_job_stage_seconds",
		Help: "Latency of job lifecycle stages.", Buckets: latencyBuckets}, []string{"stage"})
	m.artifactFails = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gpub_artifact_failures_total",
		Help: "Artifact upload or verification failures."}, []string{"reason"})
	m.repairs = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gpub_reconciler_repairs_total",
		Help: "State the reconciler had to repair. Non-zero means some path leaked."}, []string{"kind"})
	m.fenced = prometheus.NewCounter(prometheus.CounterOpts{Name: "gpub_scheduler_fenced_total",
		Help: "Ticks aborted because this scheduler's epoch was superseded (split-brain prevented)."})
	m.txRetries = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gpub_db_tx_retries_total",
		Help: "Transactions retried after serialization failure or deadlock."}, []string{"sqlstate"})
	m.httpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gpub_http_requests_total",
		Help: "API requests by route and status class."}, []string{"route", "code"})
	m.httpSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gpub_http_request_seconds",
		Help: "API latency by route.", Buckets: latencyBuckets}, []string{"route"})
	m.carbonEffect = prometheus.NewCounter(prometheus.CounterOpts{Name: "gpub_ecoshift_carbon_saved_grams_total",
		Help: "Estimated (board-power TDP) carbon saved by DELAY decisions, against the pessimistic forecast."})
	m.isLeader = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gpub_scheduler_is_leader",
		Help: "1 while this scheduler replica holds leadership."})

	m.queueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gpub_queue_depth",
		Help: "Queued jobs by tenant and requested pool ('any' when unconstrained)."}, []string{"tenant", "pool"})
	m.poolGPUs = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gpub_pool_gpus",
		Help: "GPUs per pool by state (total, reserved) on non-offline workers."}, []string{"pool", "state"})
	m.fragmentation = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gpub_pool_fragmentation_ratio",
		Help: "1 - (largest free block on one worker / total free GPUs). 0 = all free GPUs on one worker."}, []string{"pool"})
	m.heartbeatAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gpub_worker_heartbeat_age_seconds",
		Help: "Seconds since each worker's last heartbeat."}, []string{"pool", "worker", "state"})
	m.outboxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gpub_outbox_undelivered",
		Help: "Undelivered queue events by topic family."}, []string{"topic"})
	m.projectAvail = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gpub_project_available_usd",
		Help: "Project budget less holds and settled spend."}, []string{"project"})
	m.carbonAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gpub_carbon_reading_age_seconds",
		Help: "Age of the newest carbon reading per pool region."}, []string{"region"})
	m.statusAge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gpub_status_snapshot_age_seconds",
		Help: "Age of the public status snapshot being served."})
	m.storeUp = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gpub_store_up",
		Help: "1 when the reconciler's last database read succeeded."})
	m.lastRefresh = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gpub_state_refresh_timestamp_seconds", Help: "Unix timestamp of the last successful reconciler gauge refresh."})
	for _, c := range []prometheus.Collector{m.lastRefresh, m.storeUp, m.transitions, m.decisions, m.fallbacks, m.tickSeconds, m.decisionSecs,
		m.conflicts, m.staleLeases, m.outcomes, m.stage, m.artifactFails, m.repairs, m.fenced, m.txRetries,
		m.httpRequests, m.httpSeconds, m.carbonEffect, m.isLeader, m.queueDepth, m.poolGPUs, m.fragmentation,
		m.heartbeatAge, m.outboxDepth, m.projectAvail, m.carbonAge, m.statusAge} {
		f(c)
	}
	return m
}

var _ application.Observer = (*Metrics)(nil)

func (m *Metrics) Transition(from, to domain.JobState, ec domain.EvidenceClass) {
	m.transitions.WithLabelValues(string(from), string(to), string(ec)).Inc()
}

func (m *Metrics) Decision(d policy.Decision) {
	m.decisions.WithLabelValues(d.PolicyVersion, string(d.Action)).Inc()
	for _, f := range d.Fallbacks {
		reason := f
		if i := strings.IndexByte(f, ':'); i > 0 {
			reason = f[:i]
		}
		if reason == "fallback" {
			continue
		}
		m.fallbacks.WithLabelValues(d.PolicyVersion, reason).Inc()
	}
	if d.Action == policy.ActionDelay && d.CarbonSaved != nil {
		m.carbonEffect.Add(*d.CarbonSaved)
	}
}

func (m *Metrics) TickDuration(d time.Duration, n int) {
	m.tickSeconds.Observe(d.Seconds())
	if n > 0 {
		m.decisionSecs.Observe(d.Seconds() / float64(n))
	}
}

func (m *Metrics) ReservationConflict(r string) { m.conflicts.WithLabelValues(r).Inc() }
func (m *Metrics) StaleLease(kind string)       { m.staleLeases.WithLabelValues(kind).Inc() }
func (m *Metrics) AttemptOutcome(o domain.AttemptOutcome) {
	m.outcomes.WithLabelValues(string(o)).Inc()
}
func (m *Metrics) StageLatency(stage string, d time.Duration) {
	m.stage.WithLabelValues(stage).Observe(d.Seconds())
}
func (m *Metrics) ArtifactFailure(r string) { m.artifactFails.WithLabelValues(r).Inc() }
func (m *Metrics) Fenced()                  { m.fenced.Inc() }
func (m *Metrics) TxRetry(code string)      { m.txRetries.WithLabelValues(code).Inc() }
func (m *Metrics) SetLeader(on bool) {
	if on {
		m.isLeader.Set(1)
	} else {
		m.isLeader.Set(0)
	}
}
func (m *Metrics) Repair(kind string, n int) {
	if n > 0 {
		m.repairs.WithLabelValues(kind).Add(float64(n))
	}
}
func (m *Metrics) HTTP(route string, code int, d time.Duration) {
	m.httpRequests.WithLabelValues(route, codeClass(code)).Inc()
	m.httpSeconds.WithLabelValues(route).Observe(d.Seconds())
}
func (m *Metrics) StatusAge(d time.Duration) { m.statusAge.Set(d.Seconds()) }

func codeClass(c int) string {
	return string(rune('0'+c/100)) + "xx"
}

// Collect refreshes the state gauges from the store. It runs on the
// reconciler's loop, so gauges come from one process and do not double-count
// across API replicas.
func (m *Metrics) Collect(ctx context.Context, store application.Store, now time.Time) error {
	err := m.collect(ctx, store, now)
	if err != nil {
		m.storeUp.Set(0)
	} else {
		m.storeUp.Set(1)
		m.lastRefresh.Set(float64(now.Unix()))
	}
	return err
}

func (m *Metrics) collect(ctx context.Context, store application.Store, now time.Time) error {
	return store.InTx(ctx, func(tx application.Tx) error {
		jobs, err := tx.ListQueued(ctx, 100000)
		if err != nil {
			return err
		}
		m.queueDepth.Reset()
		for _, j := range jobs {
			pool := "any"
			if len(j.AllowedPools) == 1 {
				pool = string(j.AllowedPools[0])
			}
			m.queueDepth.WithLabelValues(string(j.TenantID), pool).Inc()
		}
		workers, err := tx.ListWorkers(ctx)
		if err != nil {
			return err
		}
		m.poolGPUs.Reset()
		m.heartbeatAge.Reset()
		m.fragmentation.Reset()
		type agg struct{ total, reserved, free, maxFree int }
		pools := map[domain.PoolID]*agg{}
		for _, w := range workers {
			m.heartbeatAge.WithLabelValues(string(w.PoolID), w.Name, string(w.State)).Set(now.Sub(w.LastHeartbeat).Seconds())
			if w.State == domain.WorkerOffline {
				continue
			}
			a := pools[w.PoolID]
			if a == nil {
				a = &agg{}
				pools[w.PoolID] = a
			}
			a.total += w.Capability.GPUs
			a.reserved += w.GPUsReserved
			if w.State == domain.WorkerReady {
				a.free += w.FreeGPUs()
				a.maxFree = max(a.maxFree, w.FreeGPUs())
			}
		}
		for id, a := range pools {
			m.poolGPUs.WithLabelValues(string(id), "total").Set(float64(a.total))
			m.poolGPUs.WithLabelValues(string(id), "reserved").Set(float64(a.reserved))
			frag := 0.0
			if a.free > 0 {
				frag = 1 - float64(a.maxFree)/float64(a.free)
			}
			m.fragmentation.WithLabelValues(string(id)).Set(frag)
		}
		depth, err := tx.OutboxDepth(ctx)
		if err != nil {
			return err
		}
		m.outboxDepth.Reset()
		for k, v := range depth {
			m.outboxDepth.WithLabelValues(k).Set(float64(v))
		}
		avail, err := tx.ProjectAvailableAll(ctx)
		if err != nil {
			return err
		}
		m.projectAvail.Reset()
		for k, v := range avail {
			m.projectAvail.WithLabelValues(string(k)).Set(float64(v) / 1e6)
		}
		regions, err := tx.CarbonRegions(ctx)
		if err != nil {
			return err
		}
		m.carbonAge.Reset()
		for _, r := range regions {
			hist, err := tx.CarbonHistory(ctx, r, now.Add(-7*24*time.Hour), now)
			if err != nil {
				return err
			}
			age := 7 * 24 * time.Hour
			if len(hist) > 0 {
				age = now.Sub(hist[len(hist)-1].At)
			}
			m.carbonAge.WithLabelValues(r).Set(age.Seconds())
		}
		return nil
	})
}
