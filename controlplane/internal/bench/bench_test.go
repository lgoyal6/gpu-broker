// Package bench holds the named, reproducible benchmarks reported in
// docs/evidence/benchmarks.md. Run:
//
//	GPUB_BENCH_OUT=../.agent-work/bench.json GPUB_TEST_DATABASE_URL=... \
//	  go test -run TestBenchReport -timeout 30m ./internal/bench/
//
// Each workload is generated from a fixed seed, so two runs measure the same
// work. Numbers depend on hardware; the report records what it ran on.
package bench

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/ecoshift"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/testkit"
)

var t0 = time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

// Snapshot sizes are named so the report and the code agree on what "large" means.
var sizes = []struct {
	Name          string
	Jobs, Workers int
}{
	{"small-50j-20w", 50, 20},
	{"medium-500j-100w", 500, 100},
	{"large-2000j-500w", 2000, 500},
}

func snapshot(jobs, workers int, seed uint64) policy.Snapshot {
	r := rand.New(rand.NewPCG(seed, 1))
	s := policy.Snapshot{Now: t0, Pools: map[domain.PoolID]domain.Pool{}, TenantUsage: map[domain.TenantID]float64{},
		ProjectAvailable: map[domain.ProjectID]domain.MicroUSD{}, Carbon: map[string]ecoshift.RegionCarbon{}}
	regions := []string{"gb-london", "gb-north-scotland", "gb-west-midlands"}
	for i, rg := range regions {
		id := domain.PoolID(fmt.Sprintf("pool-%d", i))
		s.Pools[id] = domain.Pool{ID: id, Region: rg, PriceMicroUSDPerGPUHour: domain.MicroUSD(1+i) * 900_000, PUE: 1.2, Interruptible: i == 1}
		s.Carbon[rg] = ecoshift.RegionCarbon{Current: &ecoshift.Reading{At: t0.Add(-10 * time.Minute), GramsPerKWh: float64(20 + 100*i)}}
	}
	models := []string{"a100", "h100", "l4"}
	for i := 0; i < workers; i++ {
		g := []int{4, 8, 8}[r.IntN(3)]
		s.Workers = append(s.Workers, domain.Worker{ID: domain.WorkerID(fmt.Sprintf("w%04d", i)), PoolID: domain.PoolID(fmt.Sprintf("pool-%d", r.IntN(3))),
			State: domain.WorkerReady, GPUsReserved: r.IntN(g), Capability: domain.Capability{GPUModel: models[r.IntN(3)], GPUs: g, GPUMemGB: 80}})
	}
	for i := 0; i < jobs; i++ {
		tn := domain.TenantID(fmt.Sprintf("t%02d", r.IntN(20)))
		s.Jobs = append(s.Jobs, domain.Job{ID: domain.JobID(fmt.Sprintf("j%05d", i)), TenantID: tn, ProjectID: domain.ProjectID("p" + string(tn)),
			Requirements: domain.Requirements{GPUs: []int{1, 1, 2, 4, 8}[r.IntN(5)]}, Priority: r.IntN(10), MaxRuntime: time.Duration(1+r.IntN(8)) * time.Hour,
			MaxAttempts: 3, State: domain.JobQueued, SubmittedAt: t0.Add(-time.Duration(r.IntN(36000)) * time.Second)})
		s.TenantUsage[tn] = float64(r.IntN(1000))
		s.ProjectAvailable[domain.ProjectID("p"+string(tn))] = 1_000_000_000
	}
	return s
}

// BenchmarkDecide is the `go test -bench` view of the same snapshots.
func BenchmarkDecide(b *testing.B) {
	p, _ := policy.Lookup("balanced")
	for _, sz := range sizes {
		s := snapshot(sz.Jobs, sz.Workers, 7)
		b.Run(sz.Name, func(b *testing.B) {
			for b.Loop() {
				policy.Decide(s, p)
			}
			b.ReportMetric(float64(sz.Jobs)*float64(b.N)/b.Elapsed().Seconds(), "decisions/s")
		})
	}
}

type latencyResult struct {
	Workload        string  `json:"workload"`
	Policy          string  `json:"policy"`
	Iterations      int     `json:"iterations"`
	P50ms           float64 `json:"tick_p50_ms"`
	P99ms           float64 `json:"tick_p99_ms"`
	DecisionsPerSec float64 `json:"decisions_per_second"`
}

func pct(xs []time.Duration, q float64) float64 {
	s := append([]time.Duration(nil), xs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(q*float64(len(s))+0.5) - 1
	return float64(s[max(0, min(i, len(s)-1))].Microseconds()) / 1000
}

func decideLatency() []latencyResult {
	var out []latencyResult
	for _, pn := range []string{"fifo", "fair-share", "balanced"} {
		p, _ := policy.Lookup(pn)
		for _, sz := range sizes {
			s := snapshot(sz.Jobs, sz.Workers, 7)
			iters := max(20, 20000/sz.Jobs)
			var ds []time.Duration
			var total time.Duration
			policy.Decide(s, p) // warm
			for i := 0; i < iters; i++ {
				start := time.Now()
				policy.Decide(s, p)
				d := time.Since(start)
				ds = append(ds, d)
				total += d
			}
			out = append(out, latencyResult{Workload: sz.Name, Policy: p.Version, Iterations: iters, P50ms: pct(ds, .5), P99ms: pct(ds, .99),
				DecisionsPerSec: float64(sz.Jobs*iters) / total.Seconds()})
		}
	}
	return out
}

type dbResult struct {
	Name    string             `json:"name"`
	Params  map[string]any     `json:"params"`
	Results map[string]float64 `json:"results"`
}

// reservationContention: C goroutines reserve and release 1 GPU at a time
// on W workers through the capacity CAS. Measures committed reservations per
// second and the conflict rate when demand exceeds capacity.
func reservationContention(t *testing.T, workers, gpus, goroutines int, dur time.Duration) dbResult {
	e := testkit.New(t)
	boot := e.Pool("pool-a", "gb-london", 1, false)
	var ids []domain.WorkerID
	for i := 0; i < workers; i++ {
		w := e.Worker(boot, fmt.Sprintf("n%03d", i), gpus)
		ids = append(ids, w.WorkerID)
	}
	var ok, conflict atomic.Int64
	var lats []time.Duration
	var mu sync.Mutex
	stop := time.Now().Add(dur)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(g), 3))
			var mine []time.Duration
			for time.Now().Before(stop) {
				w := ids[r.IntN(len(ids))]
				start := time.Now()
				err := e.Store.InTx(e.Ctx, func(tx application.Tx) error { return tx.ReserveCapacity(e.Ctx, w, 1) })
				mine = append(mine, time.Since(start))
				if err == nil {
					ok.Add(1)
					_ = e.Store.InTx(e.Ctx, func(tx application.Tx) error { return tx.ReleaseCapacity(e.Ctx, w, 1) })
				} else {
					conflict.Add(1)
				}
			}
			mu.Lock()
			lats = append(lats, mine...)
			mu.Unlock()
		}(g)
	}
	wg.Wait()
	if n := e.QueryInt(`SELECT sum(gpus_reserved) FROM workers`); n != 0 {
		t.Fatalf("contention left %d GPUs reserved", n)
	}
	total := float64(ok.Load() + conflict.Load())
	return dbResult{Name: "reservation-contention", Params: map[string]any{"workers": workers, "gpus_per_worker": gpus, "goroutines": goroutines, "seconds": dur.Seconds()},
		Results: map[string]float64{"reservations_per_second": float64(ok.Load()) / dur.Seconds(), "conflict_rate": float64(conflict.Load()) / total,
			"cas_tx_p50_ms": pct(lats, .5), "cas_tx_p99_ms": pct(lats, .99)}}
}

// tickThroughput: N queued jobs, W workers; how long the production Tick
// takes to place everything that fits, end to end through Postgres
// (snapshot, Decide, one transaction per placement with fencing, hold,
// capacity CAS, attempt, reservation, outbox).
func tickThroughput(t *testing.T, jobs, workers int) dbResult {
	e := testkit.New(t)
	user, _ := e.Tenant("bench", 1e7)
	e.Exec(`UPDATE quotas SET max_queued_jobs = 100000, max_active_gpus = 100000`)
	boot := e.Pool("pool-a", "gb-london", 1, false)
	for i := 0; i < workers; i++ {
		e.Worker(boot, fmt.Sprintf("n%03d", i), 8)
	}
	for i := 0; i < jobs; i++ {
		e.Submit(user, 1+i%4, func(r *application.SubmitRequest) { r.Policy = "balanced" })
	}
	start := time.Now()
	r := e.Tick()
	d := time.Since(start)
	placed := r.Applied[policy.ActionPlace]
	return dbResult{Name: "tick-throughput", Params: map[string]any{"queued_jobs": jobs, "workers": workers, "gpus_per_worker": 8},
		Results: map[string]float64{"tick_seconds": d.Seconds(), "placements": float64(placed), "placements_per_second": float64(placed) / d.Seconds(),
			"decisions": float64(len(r.Decisions))}}
}

// queueThroughput: enqueue N dispatch events across T topics, then C
// consumers claim and ack them with SKIP LOCKED until the queue is empty.
func queueThroughput(t *testing.T, events, topics, consumers int) dbResult {
	e := testkit.New(t)
	start := time.Now()
	for i := 0; i < events; i += 500 {
		e.Must(e.Store.InTx(e.Ctx, func(tx application.Tx) error {
			for k := i; k < min(i+500, events); k++ {
				if err := tx.Enqueue(e.Ctx, fmt.Sprintf("dispatch.w%02d", k%topics), "k", []byte(`{}`), e.Clock.Now()); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	enq := time.Since(start)
	var delivered atomic.Int64
	start = time.Now()
	var wg sync.WaitGroup
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			topic := fmt.Sprintf("dispatch.w%02d", c%topics)
			for {
				var evs []domain.OutboxEvent
				_ = e.Store.InTx(e.Ctx, func(tx application.Tx) (err error) {
					evs, err = tx.Claim(e.Ctx, topic, 16, e.Clock.Now(), 30*time.Second, "bench")
					return err
				})
				if len(evs) == 0 {
					return
				}
				_ = e.Store.InTx(e.Ctx, func(tx application.Tx) error {
					for _, ev := range evs {
						if err := tx.MarkDelivered(e.Ctx, ev.ID, e.Clock.Now()); err != nil {
							return err
						}
					}
					return nil
				})
				delivered.Add(int64(len(evs)))
			}
		}(c)
	}
	wg.Wait()
	cons := time.Since(start)
	if delivered.Load() != int64(events) {
		t.Fatalf("delivered %d of %d", delivered.Load(), events)
	}
	return dbResult{Name: "queue-throughput", Params: map[string]any{"events": events, "topics": topics, "consumers": consumers, "claim_batch": 16},
		Results: map[string]float64{"enqueue_per_second": float64(events) / enq.Seconds(), "claim_ack_per_second": float64(events) / cons.Seconds()}}
}

// recovery: W workers die holding one running attempt each; one reconcile
// pass must reclaim every lease and requeue every job.
func recovery(t *testing.T, n int) dbResult {
	e := testkit.New(t)
	user, _ := e.Tenant("bench", 1e6)
	e.Exec(`UPDATE quotas SET max_queued_jobs = 100000, max_active_gpus = 100000`)
	boot := e.Pool("pool-a", "gb-london", 1, false)
	var ws []application.Principal
	for i := 0; i < n; i++ {
		ws = append(ws, e.Worker(boot, fmt.Sprintf("n%03d", i), 1))
		e.Submit(user, 1)
	}
	e.Tick()
	for _, w := range ws {
		ds, _ := e.Svc.ClaimDispatches(e.Ctx, w, 1)
		for _, d := range ds {
			_, err := e.Svc.Ack(e.Ctx, w, d)
			e.Must(err)
		}
	}
	e.Clock.Advance(2 * time.Minute) // every lease is now expired
	start := time.Now()
	r, err := e.Svc.Reconcile(e.Ctx)
	e.Must(err)
	d := time.Since(start)
	if r.ExpiredLeases != n {
		t.Fatalf("reclaimed %d of %d", r.ExpiredLeases, n)
	}
	return dbResult{Name: "worker-failure-recovery", Params: map[string]any{"dead_workers_with_running_attempts": n, "lease_ttl_s": 45},
		Results: map[string]float64{"reconcile_pass_seconds": d.Seconds(), "reclaims_per_second": float64(n) / d.Seconds(),
			"worst_case_detection_seconds": 45 + 10, "note_reconcile_interval_s": 10}}
}

func TestBenchReport(t *testing.T) {
	out := os.Getenv("GPUB_BENCH_OUT")
	if out == "" {
		t.Skip("set GPUB_BENCH_OUT to run the benchmark report")
	}
	rep := map[string]any{
		"evidence_class": "synthetic benchmark",
		"go":             runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "cpus": runtime.NumCPU(),
		"host":           os.Getenv("GPUB_BENCH_HOST"),
		"started":        time.Now().UTC().Format(time.RFC3339),
		"decide_latency": decideLatency(),
	}
	if os.Getenv("GPUB_TEST_DATABASE_URL") != "" {
		rep["postgres"] = []dbResult{
			reservationContention(t, 4, 2, 32, 10*time.Second),
			reservationContention(t, 64, 8, 32, 10*time.Second),
			tickThroughput(t, 1000, 100),
			queueThroughput(t, 20000, 8, 8),
			recovery(t, 200),
		}
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	if err := os.WriteFile(out, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Log(string(b))
}
