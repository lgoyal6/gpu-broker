// Command gpubroker runs one control-plane role per process:
//
//	gpubroker migrate | api | scheduler | reconciler | status | worker-agent
//	gpubroker sim | replay | forecast-eval | admin
//
// Every dependency is built here, explicitly, and passed down; nothing below
// main reads the environment or holds global state.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/metrics"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/postgres"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	role, args := os.Args[1], os.Args[2:]
	var err error
	switch role {
	case "migrate":
		err = runMigrate(ctx, args, log)
	case "api":
		err = runAPI(ctx, args, log)
	case "scheduler":
		err = runScheduler(ctx, args, log)
	case "reconciler":
		err = runReconciler(ctx, args, log)
	case "status":
		err = runStatus(ctx, args, log)
	case "worker-agent":
		err = runAgent(ctx, args, log)
	case "sim":
		err = runSim(args)
	case "replay":
		err = runReplay(args)
	case "forecast-eval":
		err = runForecastEval(args)
	case "admin":
		err = runAdmin(ctx, args)
	case "-h", "--help", "help":
		usage()
		return
	default:
		usage()
		err = fmt.Errorf("unknown role %q", role)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("exit", "role", role, "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `gpubroker <role> [flags]

Server roles (one per process; see infra/helm):
  migrate        apply embedded SQL migrations (and the status login role)
  api            authenticated job, pool and worker API (:8080), metrics (:9090)
  scheduler      single logical scheduler; leader-elected through Postgres
  reconciler     reclaims expired leases and stale reservations; refreshes gauges
  status         read-only public status page (:8081) on the gpub_status role
  worker-agent   runs dispatched attempts in the sim or kubernetes runtime

Offline:
  sim            deterministic policy simulator over named workloads
  replay         replay a club or control-plane trace through the policies
  forecast-eval  evaluate the carbon forecaster against baselines
  admin          create-tenant | create-pool | load-carbon

Run "gpubroker <role> -h" for flags.
`)
}

// common flags shared by database-backed roles.
type common struct {
	dbURL       string
	metricsAddr string
}

func (c *common) bind(fs *flag.FlagSet) {
	fs.StringVar(&c.dbURL, "database-url", os.Getenv("GPUB_DATABASE_URL"), "PostgreSQL URL (env GPUB_DATABASE_URL)")
	fs.StringVar(&c.metricsAddr, "metrics-addr", ":9090", "Prometheus metrics listen address; empty disables")
}

func readSecret(path, env string) ([]byte, error) {
	if path != "" {
		b, err := os.ReadFile(path)
		return []byte(strings.TrimSpace(string(b))), err
	}
	if v := os.Getenv(env); v != "" {
		return []byte(v), nil
	}
	return nil, fmt.Errorf("secret missing: pass a file or set %s", env)
}

// service builds the application with its adapters. One constructor, every
// dependency visible.
func service(ctx context.Context, c common, m *metrics.Metrics, objs application.ObjectStore) (*application.Service, *postgres.Store, error) {
	if c.dbURL == "" {
		return nil, nil, errors.New("--database-url or GPUB_DATABASE_URL is required")
	}
	store, err := postgres.Open(ctx, c.dbURL)
	if err != nil {
		return nil, nil, err
	}
	store.RetryHook = m.TxRetry
	// Roles start alongside the migration Job (helm --wait cannot wait on a
	// hook that runs after readiness), so each waits for the schema it was
	// built against. Older binaries accept a newer, additive schema, which is
	// what makes `helm rollback` safe without down-migrations.
	for {
		err := store.SchemaReady(ctx)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	key, err := readSecret(os.Getenv("GPUB_DISPATCH_KEY_FILE"), "GPUB_DISPATCH_KEY")
	if err != nil {
		return nil, nil, err
	}
	signer, err := application.NewLeaseSigner(key)
	if err != nil {
		return nil, nil, err
	}
	svc := application.NewService(store, application.SystemClock{}, application.RandomIDs{}, signer, objs, m, application.DefaultConfig())
	return svc, store, nil
}

func serveMetrics(ctx context.Context, addr string, m *metrics.Metrics, log *slog.Logger) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.Reg, promhttp.HandlerOpts{}))
	go serve(ctx, &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}, log)
}

// serve runs an HTTP server until ctx ends, then drains it for up to 20s.
func serve(ctx context.Context, srv *http.Server, log *slog.Logger) error {
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("listening", "addr", srv.Addr)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return srv.Shutdown(sctx)
	}
}
