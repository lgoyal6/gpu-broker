// Package testkit builds a real control plane against a fresh PostgreSQL
// database for integration tests. It is imported only from _test files.
package testkit

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/objectstore"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/postgres"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

// DatabaseURL returns the admin URL for test databases. Without it the test
// is skipped locally, but fails when GPUB_REQUIRE_DB=1 (as CI sets), so a
// misconfigured CI cannot pass by skipping everything.
func DatabaseURL(t testing.TB) string {
	u := os.Getenv("GPUB_TEST_DATABASE_URL")
	if u == "" {
		if os.Getenv("GPUB_REQUIRE_DB") == "1" {
			t.Fatal("GPUB_TEST_DATABASE_URL is unset but GPUB_REQUIRE_DB=1")
		}
		t.Skip("GPUB_TEST_DATABASE_URL not set; skipping PostgreSQL test")
	}
	return u
}

var dbCounter atomic.Int64

// FreshDB creates an empty database, migrates it, and drops it at cleanup.
func FreshDB(t testing.TB) (*postgres.Store, string) {
	t.Helper()
	admin := DatabaseURL(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	name := fmt.Sprintf("gpubt_%d_%d", os.Getpid(), dbCounter.Add(1))
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create db: %v", err)
	}
	conn.Close(ctx)
	u, _ := url.Parse(admin)
	u.Path = "/" + name
	store, err := postgres.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		store.Close()
		c, err := pgx.Connect(context.Background(), admin)
		if err == nil {
			_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
			c.Close(context.Background())
		}
	})
	return store, u.String()
}

// Clock is a settable clock safe for concurrent use.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

func NewClock(t time.Time) *Clock { return &Clock{now: t.UTC()} }

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *Clock) Set(t time.Time) {
	c.mu.Lock()
	c.now = t.UTC()
	c.mu.Unlock()
}

// SeqIDs makes readable deterministic ids ("job_000003").
type SeqIDs struct{ n atomic.Int64 }

func (s *SeqIDs) New(prefix string) string { return fmt.Sprintf("%s_%06d", prefix, s.n.Add(1)) }

var DispatchKey = []byte(strings.Repeat("k", 32))

// Env is a wired control plane over a fresh database.
type Env struct {
	T       testing.TB
	Store   *postgres.Store
	URL     string
	Clock   *Clock
	Svc     *application.Service
	Objects *objectstore.FS
	Ctx     context.Context
}

var T0 = time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

func New(t testing.TB) *Env {
	t.Helper()
	store, u := FreshDB(t)
	clock := NewClock(T0)
	signer, err := application.NewLeaseSigner(DispatchKey)
	if err != nil {
		t.Fatal(err)
	}
	objs, err := objectstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	svc := application.NewService(store, clock, &SeqIDs{}, signer, objs, nil, application.DefaultConfig())
	e := &Env{T: t, Store: store, URL: u, Clock: clock, Svc: svc, Objects: objs, Ctx: context.Background()}
	if err := svc.RegisterPolicies(e.Ctx); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *Env) Must(err error) {
	e.T.Helper()
	if err != nil {
		e.T.Fatal(err)
	}
}

// Tenant creates a real-evidence tenant with an operator user.
func (e *Env) Tenant(name string, budgetUSD float64) (application.Principal, application.TenantCreated) {
	e.T.Helper()
	tc, err := e.Svc.CreateTenant(e.Ctx, application.TenantSetup{Name: name, EvidenceClass: domain.EvidenceReal,
		Handle: name + "-op", BudgetUSD: budgetUSD})
	e.Must(err)
	p, err := e.Svc.Authenticate(e.Ctx, tc.Token)
	e.Must(err)
	return p, tc
}

// Pool creates a pool and returns its bootstrap principal.
func (e *Env) Pool(id, region string, priceUSD float64, interruptible bool) application.Principal {
	e.T.Helper()
	tok, err := e.Svc.UpsertPool(e.Ctx, domain.Pool{ID: domain.PoolID(id), Name: id, Kind: domain.PoolSimulated, Region: region,
		PriceMicroUSDPerGPUHour: domain.MicroUSD(priceUSD * 1e6), Interruptible: interruptible, PUE: 1.1})
	e.Must(err)
	p, err := e.Svc.Authenticate(e.Ctx, tok)
	e.Must(err)
	return p
}

// Worker registers a sim worker and returns its principal.
func (e *Env) Worker(boot application.Principal, name string, gpus int) application.Principal {
	e.T.Helper()
	_, tok, err := e.Svc.Register(e.Ctx, boot, application.RegisterRequest{Name: name,
		Capability: domain.Capability{GPUModel: "a100", GPUs: gpus, GPUMemGB: 80, Runtimes: []string{"sim", "kubernetes"}}})
	e.Must(err)
	p, err := e.Svc.Authenticate(e.Ctx, tok)
	e.Must(err)
	return p
}

func (e *Env) Submit(p application.Principal, gpus int, mut ...func(*application.SubmitRequest)) domain.Job {
	e.T.Helper()
	req := application.SubmitRequest{Image: "ghcr.io/example/train:1", Command: []string{"sim", "duration=60s"}, GPUs: gpus,
		MaxRuntime: time.Hour, Policy: "fair-share"}
	for _, m := range mut {
		m(&req)
	}
	j, err := e.Svc.Submit(e.Ctx, p, req, "test")
	e.Must(err)
	return j
}

func (e *Env) Job(p application.Principal, id domain.JobID) domain.Job {
	e.T.Helper()
	v, err := e.Svc.GetJob(e.Ctx, p, id)
	e.Must(err)
	return v.Job
}

func (e *Env) Tick() application.TickResult {
	e.T.Helper()
	r, err := e.Svc.Tick(e.Ctx, e.Lead())
	e.Must(err)
	return r
}

// Lead takes scheduler leadership once per env and returns the epoch.
func (e *Env) Lead() int64 {
	e.T.Helper()
	if v, ok := leaders.Load(e); ok {
		return v.(int64)
	}
	l, err := e.Store.TryLead(e.Ctx, "test")
	e.Must(err)
	if l == nil {
		e.T.Fatal("could not take leadership")
	}
	e.T.Cleanup(func() { l.Release(context.Background()) })
	leaders.Store(e, l.Epoch)
	return l.Epoch
}

var leaders sync.Map

// Exec runs raw SQL against the env's database (fault injection, assertions).
func (e *Env) Exec(sql string, args ...any) {
	e.T.Helper()
	if _, err := e.Store.Pool.Exec(e.Ctx, sql, args...); err != nil {
		e.T.Fatalf("exec %q: %v", sql, err)
	}
}

func (e *Env) QueryInt(sql string, args ...any) int64 {
	e.T.Helper()
	var n int64
	if err := e.Store.Pool.QueryRow(e.Ctx, sql, args...).Scan(&n); err != nil {
		e.T.Fatalf("query %q: %v", sql, err)
	}
	return n
}
