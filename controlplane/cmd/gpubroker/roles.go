package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/metrics"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/postgres"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/agent"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/transport/httpapi"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/transport/status"
)

var roleName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

func runMigrate(ctx context.Context, args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	var c common
	c.bind(fs)
	statusLogin := fs.String("status-login", "", "also create/update this LOGIN role as a member of gpub_status")
	statusPwFile := fs.String("status-password-file", "", "password file for --status-login")
	poolConfig := fs.String("pool-config", "", "JSON list of pools with bootstrap_token_file to upsert after migrating")
	_ = fs.Parse(args)
	// The migration Job may start before the database (same Helm release):
	// retry the connection rather than crash-looping through backoff.
	var store *postgres.Store
	var err error
	for {
		if store, err = postgres.Open(ctx, c.dbURL); err == nil {
			break
		}
		log.Warn("database not reachable yet", "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	defer store.Close()
	applied, err := store.Migrate(ctx)
	if err != nil {
		return err
	}
	log.Info("migrations applied", "new", applied)
	if *statusLogin != "" {
		if !roleName.MatchString(*statusLogin) {
			return fmt.Errorf("invalid role name %q", *statusLogin)
		}
		pw, err := readSecret(*statusPwFile, "GPUB_STATUS_PASSWORD")
		if err != nil {
			return err
		}
		// Role DDL cannot take bind parameters; the name is validated above
		// and the password is passed through quote_literal on the server.
		_, err = store.Pool.Exec(ctx, fmt.Sprintf(`DO $$ BEGIN
			IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '%[1]s') THEN CREATE ROLE %[1]s LOGIN; END IF;
			END $$;`, *statusLogin))
		if err != nil {
			return err
		}
		var lit string
		if err := store.Pool.QueryRow(ctx, `SELECT quote_literal($1)`, string(pw)).Scan(&lit); err != nil {
			return err
		}
		if _, err := store.Pool.Exec(ctx, fmt.Sprintf(`ALTER ROLE %s LOGIN PASSWORD %s; GRANT gpub_status TO %s;`, *statusLogin, lit, *statusLogin)); err != nil {
			return err
		}
		log.Info("status login role ready", "role", *statusLogin)
	}
	if *poolConfig != "" {
		if err := applyPoolConfig(ctx, store, *poolConfig, log); err != nil {
			return err
		}
	}
	return nil
}

type poolEntry struct {
	ID                 string  `json:"id"`
	Name               string  `json:"name"`
	Kind               string  `json:"kind"`
	Region             string  `json:"region"`
	PriceUSD           float64 `json:"price_usd"`
	Interruptible      bool    `json:"interruptible"`
	PUE                float64 `json:"pue"`
	BootstrapTokenFile string  `json:"bootstrap_token_file"`
}

func applyPoolConfig(ctx context.Context, store *postgres.Store, path string, log *slog.Logger) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var pools []poolEntry
	if err := json.Unmarshal(raw, &pools); err != nil {
		return fmt.Errorf("pool config: %w", err)
	}
	signer, _ := application.NewLeaseSigner([]byte(strings.Repeat("x", 32))) // unused by pool upserts
	svc := application.NewService(store, application.SystemClock{}, application.RandomIDs{}, signer, nil, nil, application.DefaultConfig())
	for _, p := range pools {
		tok, err := readSecret(p.BootstrapTokenFile, "")
		if err != nil {
			return fmt.Errorf("pool %s: %w", p.ID, err)
		}
		if err := svc.UpsertPoolWithToken(ctx, domain.Pool{ID: domain.PoolID(p.ID), Name: p.Name, Kind: domain.PoolKind(p.Kind),
			Region: p.Region, PriceMicroUSDPerGPUHour: domain.MicroUSD(p.PriceUSD * 1e6), Interruptible: p.Interruptible, PUE: p.PUE}, string(tok)); err != nil {
			return fmt.Errorf("pool %s: %w", p.ID, err)
		}
		log.Info("pool ready", "pool", p.ID, "region", p.Region)
	}
	return nil
}

func runAPI(ctx context.Context, args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("api", flag.ExitOnError)
	var c common
	c.bind(fs)
	addr := fs.String("addr", ":8080", "listen address")
	objDir := fs.String("object-dir", "/var/lib/gpubroker/objects", "artifact store root (a PersistentVolume in Kubernetes)")
	demo := fs.Bool("demo", false, "enable POST /v1/demo/seed (seeded, labelled data; never in production)")
	_ = fs.Parse(args)
	m := metrics.New()
	svc, store, err := service(ctx, c, m, *objDir)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := svc.RegisterPolicies(ctx); err != nil {
		return err
	}
	serveMetrics(ctx, c.metricsAddr, m, log)
	api := httpapi.New(svc, log, m, store.SchemaReady, *demo)
	return serve(ctx, &http.Server{Addr: *addr, Handler: api.Handler(), ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout: 60 * time.Second, WriteTimeout: 60 * time.Second}, log)
}

// runScheduler is the leader loop. Only the leader ticks; every replica
// keeps trying to acquire, so killing the leader hands over within one
// interval of Postgres noticing the dropped connection.
func runScheduler(ctx context.Context, args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("scheduler", flag.ExitOnError)
	var c common
	c.bind(fs)
	interval := fs.Duration("interval", 2*time.Second, "tick interval")
	holder := fs.String("holder", os.Getenv("POD_NAME"), "identity recorded as leader (defaults to $POD_NAME)")
	_ = fs.Parse(args)
	if *holder == "" {
		*holder, _ = os.Hostname()
	}
	m := metrics.New()
	svc, store, err := service(ctx, c, m, "")
	if err != nil {
		return err
	}
	defer store.Close()
	if err := svc.RegisterPolicies(ctx); err != nil {
		return err
	}
	serveMetrics(ctx, c.metricsAddr, m, log)
	var lead *postgres.Leadership
	defer func() {
		if lead != nil {
			lead.Release(context.Background())
		}
	}()
	t := time.NewTicker(*interval)
	defer t.Stop()
	for {
		if lead == nil {
			l, err := store.TryLead(ctx, *holder)
			if err != nil {
				log.Warn("leader election failed", "err", err)
			} else if l != nil {
				lead = l
				m.SetLeader(true)
				log.Info("became leader", "epoch", l.Epoch, "holder", *holder)
			}
		}
		if lead != nil {
			if err := lead.Alive(ctx); err != nil {
				log.Warn("lost leadership connection; stepping down", "err", err)
				lead.Release(context.Background())
				lead = nil
				m.SetLeader(false)
			} else if res, err := svc.Tick(ctx, lead.Epoch); err != nil {
				if errors.Is(err, application.ErrFenced) {
					log.Warn("fenced by a newer leader; stepping down", "epoch", lead.Epoch)
					lead.Release(context.Background())
					lead = nil
					m.SetLeader(false)
				} else if ctx.Err() == nil {
					log.Error("tick failed", "err", err)
				}
			} else if len(res.Decisions) > 0 {
				log.Info("tick", "tick", res.TickID, "decisions", len(res.Decisions), "applied", res.Applied, "conflicts", res.Conflicts)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func runReconciler(ctx context.Context, args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("reconciler", flag.ExitOnError)
	var c common
	c.bind(fs)
	interval := fs.Duration("interval", 10*time.Second, "reconcile interval")
	_ = fs.Parse(args)
	m := metrics.New()
	svc, store, err := service(ctx, c, m, "")
	if err != nil {
		return err
	}
	defer store.Close()
	serveMetrics(ctx, c.metricsAddr, m, log)
	t := time.NewTicker(*interval)
	defer t.Stop()
	for {
		res, err := svc.Reconcile(ctx)
		if err != nil && ctx.Err() == nil {
			log.Error("reconcile failed", "err", err)
		} else if res != (application.ReconcileResult{}) {
			log.Info("reconciled", "result", fmt.Sprintf("%+v", res))
		}
		if err := m.Collect(ctx, store, time.Now().UTC()); err != nil && ctx.Err() == nil {
			log.Warn("gauge refresh failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func runStatus(ctx context.Context, args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	dbURL := fs.String("database-url", os.Getenv("GPUB_STATUS_DATABASE_URL"), "URL for the gpub_status login role (env GPUB_STATUS_DATABASE_URL)")
	addr := fs.String("addr", ":8081", "listen address")
	metricsAddr := fs.String("metrics-addr", ":9090", "metrics listen address")
	ttl := fs.Duration("cache-ttl", 5*time.Second, "snapshot cache TTL")
	_ = fs.Parse(args)
	store, err := postgres.Open(ctx, *dbURL)
	if err != nil {
		return err
	}
	defer store.Close()
	m := metrics.New()
	serveMetrics(ctx, *metricsAddr, m, log)
	srv := status.New(postgres.StatusSource{S: store}, *ttl, func() time.Time { return time.Now().UTC() }, m.StatusAge)
	return serve(ctx, &http.Server{Addr: *addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}, log)
}

func runAgent(ctx context.Context, args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("worker-agent", flag.ExitOnError)
	apiURL := fs.String("api-url", os.Getenv("GPUB_API_URL"), "control-plane API URL")
	bootFile := fs.String("bootstrap-token-file", "", "pool bootstrap token file (or env GPUB_BOOTSTRAP_TOKEN)")
	keyFile := fs.String("dispatch-key-file", os.Getenv("GPUB_DISPATCH_KEY_FILE"), "dispatch MAC key file (or env GPUB_DISPATCH_KEY)")
	name := fs.String("name", os.Getenv("NODE_NAME"), "worker name (defaults to $NODE_NAME)")
	model := fs.String("gpu-model", "a100", "declared GPU model")
	gpus := fs.Int("gpus", 1, "declared GPU count")
	mem := fs.Int("gpu-mem-gb", 80, "declared GPU memory per GPU")
	runtime := fs.String("runtime", "sim", "sim | kubernetes")
	stateDir := fs.String("state-dir", "/var/lib/gpubroker-agent", "credential, spool and sim state")
	ns := fs.String("namespace", "gpub-jobs", "kubernetes namespace for attempt Jobs")
	gpuRes := fs.String("gpu-resource", "", `extended resource to request, e.g. "nvidia.com/gpu"; empty in Kind`)
	cpu := fs.String("cpu", "1", "per-attempt CPU limit")
	memLimit := fs.String("memory", "1Gi", "per-attempt memory limit")
	heartbeat := fs.Duration("heartbeat", 10*time.Second, "heartbeat interval (must be well under the 45s lease)")
	_ = fs.Parse(args)
	boot, err := readSecret(*bootFile, "GPUB_BOOTSTRAP_TOKEN")
	if err != nil && !fileExists(*stateDir+"/worker-token") {
		return err
	}
	key, err := readSecret(*keyFile, "GPUB_DISPATCH_KEY")
	if err != nil {
		return err
	}
	clock := application.SystemClock{}
	var rt agent.Runtime
	switch *runtime {
	case "sim":
		rt, err = agent.NewSimRuntime(*stateDir+"/sim", clock)
	case "kubernetes":
		cfg, cerr := rest.InClusterConfig()
		if cerr != nil {
			return fmt.Errorf("kubernetes runtime needs in-cluster config: %w", cerr)
		}
		cs, cerr := kubernetes.NewForConfig(cfg)
		if cerr != nil {
			return cerr
		}
		k := &agent.KubeRuntime{Client: cs, Namespace: *ns, NodeName: *name, GPUResource: *gpuRes, CPU: *cpu, Memory: *memLimit}
		if *gpuRes != "" {
			k.Tolerations = []corev1.Toleration{{Key: *gpuRes, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}
		}
		rt = k
	default:
		return fmt.Errorf("unknown runtime %q", *runtime)
	}
	if err != nil {
		return err
	}
	a, err := agent.New(agent.Config{APIURL: *apiURL, BootstrapToken: string(boot), Name: *name, GPUModel: *model, GPUs: *gpus,
		GPUMemGB: *mem, StateDir: *stateDir, DispatchKey: key, Heartbeat: *heartbeat}, rt, clock, log)
	if err != nil {
		return err
	}
	// Registration retries: the API may not be up yet when the DaemonSet starts.
	for {
		if err = a.Register(ctx); err == nil {
			break
		}
		log.Warn("register failed; retrying", "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	if k, ok := rt.(*agent.KubeRuntime); ok {
		k.WorkerID = a.WorkerID()
	}
	if err := a.Recover(ctx); err != nil {
		log.Warn("recovery incomplete", "err", err)
	}
	return a.Run(ctx)
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}
