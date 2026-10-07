// Package status serves the unauthenticated public status page. It is a
// separate process that reads two aggregate views through a database role
// with SELECT on those views only (ADR 0008), so nothing here can mutate a
// job even if this code were wrong.
package status

import (
	"context"
	_ "embed"
	"encoding/json"
	"html/template"
	"net/http"
	"sort"
	"sync"
	"time"
)

// PoolRow and QueueRow are what the views return. They are the input; the
// public DTO below is built from them field by field.
type PoolRow struct {
	Pool, Region    string
	WorkersReady    int64
	GPUsTotal       int64
	GPUsReserved    int64
	NewestHeartbeat *time.Time
	Tenants7d       int64
	Succeeded7d     *int64
	Failed7d        *int64
	Cancelled7d     *int64
	Expired7d       *int64
	PilotJobs7d     *int64
	RealJobs7d      *int64
}

type QueueRow struct {
	QueuedJobs  int64
	QueuedPilot *int64
	QueuedReal  *int64
}

type Source interface {
	Read(ctx context.Context) ([]PoolRow, QueueRow, time.Time, error)
}

// PublicStatusV1 is the allowlist DTO. Every field is listed in
// allowlist_test.go; adding one here without adding it there fails CI.
type PublicStatusV1 struct {
	Schema      string       `json:"schema"`
	GeneratedAt time.Time    `json:"generated_at"`
	Evidence    string       `json:"evidence"`
	Pools       []PublicPool `json:"pools"`
	Queue       PublicQueue  `json:"queue"`
}

type PublicPool struct {
	Pool           string          `json:"pool"`
	Region         string          `json:"region"`
	WorkersReady   int64           `json:"workers_ready"`
	GPUsTotal      int64           `json:"gpus_total"`
	GPUsReserved   int64           `json:"gpus_reserved"`
	Utilization    *float64        `json:"utilization"`
	HeartbeatFresh bool            `json:"heartbeat_fresh"`
	Suppressed     bool            `json:"suppressed"`
	Outcomes7d     *PublicOutcomes `json:"outcomes_7d"`
	PilotJobs7d    *int64          `json:"pilot_jobs_7d"`
	RealJobs7d     *int64          `json:"real_jobs_7d"`
}

type PublicOutcomes struct {
	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
	Cancelled int64 `json:"cancelled"`
	Expired   int64 `json:"expired"`
}

type PublicQueue struct {
	QueuedJobs  int64  `json:"queued_jobs"`
	QueuedPilot *int64 `json:"queued_pilot"`
	QueuedReal  *int64 `json:"queued_real"`
}

// Build constructs the DTO by addition. Nothing is copied wholesale, so a
// new column in a view cannot reach the page without an explicit line here.
func Build(pools []PoolRow, q QueueRow, at time.Time) PublicStatusV1 {
	out := PublicStatusV1{Schema: "gpu-broker.public-status/v1", GeneratedAt: at.UTC(),
		Evidence: "pilot and real tenants only; seeded demo and simulator data are excluded. Outcome counts are withheld for pools with fewer than three tenants in the window.",
		Pools:    []PublicPool{}}
	for _, r := range pools {
		p := PublicPool{Pool: r.Pool, Region: r.Region, WorkersReady: r.WorkersReady, GPUsTotal: r.GPUsTotal, GPUsReserved: r.GPUsReserved}
		if r.GPUsTotal > 0 {
			u := float64(r.GPUsReserved) / float64(r.GPUsTotal)
			p.Utilization = &u
		}
		p.HeartbeatFresh = r.NewestHeartbeat != nil && at.Sub(*r.NewestHeartbeat) < 2*time.Minute
		if r.Succeeded7d == nil {
			p.Suppressed = true
		} else {
			p.Outcomes7d = &PublicOutcomes{Succeeded: *r.Succeeded7d, Failed: deref(r.Failed7d), Cancelled: deref(r.Cancelled7d), Expired: deref(r.Expired7d)}
			p.PilotJobs7d, p.RealJobs7d = r.PilotJobs7d, r.RealJobs7d
		}
		out.Pools = append(out.Pools, p)
	}
	sort.Slice(out.Pools, func(i, j int) bool { return out.Pools[i].Pool < out.Pools[j].Pool })
	out.Queue = PublicQueue{QueuedJobs: q.QueuedJobs, QueuedPilot: q.QueuedPilot, QueuedReal: q.QueuedReal}
	return out
}

func deref(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

// Server caches the snapshot briefly: a link that gets attention arrives as
// a burst, and rebuilding per reader turns one link into a query storm
// against the database the scheduler is writing to.
type Server struct {
	src    Source
	ttl    time.Duration
	now    func() time.Time
	onAge  func(time.Duration)
	mu     sync.Mutex
	cached *PublicStatusV1
	at     time.Time
	mux    *http.ServeMux
	Routes []string
}

func New(src Source, ttl time.Duration, now func() time.Time, onAge func(time.Duration)) *Server {
	s := &Server{src: src, ttl: ttl, now: now, onAge: onAge, mux: http.NewServeMux()}
	// GET only, by construction. routes_test.go walks Routes and fails on
	// any other method.
	for path, h := range map[string]http.HandlerFunc{
		"/":            s.page,
		"/status.json": s.json,
		"/healthz":     func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) },
	} {
		route := "GET " + path
		if path == "/" {
			route = "GET /{$}"
		}
		s.Routes = append(s.Routes, route)
		s.mux.HandleFunc(route, h)
	}
	sort.Strings(s.Routes)
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) snapshot(ctx context.Context) (*PublicStatusV1, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cached != nil && s.now().Sub(s.at) < s.ttl {
		if s.onAge != nil {
			s.onAge(s.now().Sub(s.cached.GeneratedAt))
		}
		return s.cached, nil
	}
	pools, q, at, err := s.src.Read(ctx)
	if err != nil {
		if s.cached != nil {
			// Serve the last good snapshot, which carries its own
			// generated_at, rather than an error page during a DB blip.
			return s.cached, nil
		}
		return nil, err
	}
	v := Build(pools, q, at)
	s.cached, s.at = &v, s.now()
	if s.onAge != nil {
		s.onAge(s.now().Sub(v.GeneratedAt))
	}
	return s.cached, nil
}

func (s *Server) json(w http.ResponseWriter, r *http.Request) {
	v, err := s.snapshot(r.Context())
	if err != nil {
		http.Error(w, "status unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=5")
	_ = json.NewEncoder(w).Encode(v)
}

//go:embed page.html
var pageSrc string

var pageTmpl = template.Must(template.New("page").Funcs(template.FuncMap{
	"pct": func(f *float64) string {
		if f == nil {
			return "n/a"
		}
		return template.HTMLEscapeString(fmtPct(*f))
	},
	"num": func(p *int64) string {
		if p == nil {
			return "withheld"
		}
		return template.HTMLEscapeString(fmtInt(*p))
	},
}).Parse(pageSrc))

func (s *Server) page(w http.ResponseWriter, r *http.Request) {
	v, err := s.snapshot(r.Context())
	if err != nil {
		http.Error(w, "status unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'")
	_ = pageTmpl.Execute(w, v)
}
