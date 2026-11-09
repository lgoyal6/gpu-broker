package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/carbon"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/gcscheckpoint"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/metrics"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/postgres"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/spot"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/agent"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/transport/grpcgateway"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/transport/httpapi"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/transport/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	grpcAddr := fs.String("grpc-addr", os.Getenv("GPUB_GRPC_ADDR"), "worker lifecycle gRPC listen address; empty disables")
	grpcCert := fs.String("grpc-tls-cert-file", "", "worker gateway server certificate")
	grpcKey := fs.String("grpc-tls-key-file", "", "worker gateway server private key")
	grpcCA := fs.String("grpc-client-ca-file", "", "CA allowed to issue worker client certificates")
	var objects artifactStoreOptions
	objects.bind(fs)
	demo := fs.Bool("demo", false, "enable POST /v1/demo/seed (seeded, labelled data; never in production)")
	_ = fs.Parse(args)
	grpcTLS, err := grpcgateway.ServerTLSConfig(*grpcAddr, grpcgateway.TLSFiles{CAFile: *grpcCA, CertFile: *grpcCert, KeyFile: *grpcKey})
	if err != nil {
		return err
	}
	m := metrics.New()
	objectStore, err := objects.build(ctx)
	if err != nil {
		return err
	}
	svc, store, err := service(ctx, c, m, objectStore)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := svc.RegisterPolicies(ctx); err != nil {
		return err
	}
	serveMetrics(ctx, c.metricsAddr, m, log)
	api := httpapi.New(svc, log, m, store.SchemaReady, *demo)
	var grpcServer *grpc.Server
	if *grpcAddr != "" {
		listener, err := net.Listen("tcp", *grpcAddr)
		if err != nil {
			return fmt.Errorf("listen worker gRPC: %w", err)
		}
		gateway := &grpcgateway.Server{Handler: api.Handler()}
		options := []grpc.ServerOption{grpc.MaxRecvMsgSize(10 << 20), grpc.MaxSendMsgSize(10 << 20)}
		if grpcTLS != nil {
			options = append(options, grpc.Creds(credentials.NewTLS(grpcTLS)))
		}
		grpcServer = grpc.NewServer(options...)
		grpcgateway.RegisterWorkerGatewayServer(grpcServer, gateway)
		go func() {
			if err := grpcServer.Serve(listener); err != nil && ctx.Err() == nil {
				log.Error("worker gRPC server stopped", "err", err)
			}
		}()
		log.Info("worker gRPC listening", "addr", *grpcAddr)
		defer grpcServer.GracefulStop()
	}
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
	svc, store, err := service(ctx, c, m, nil)
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
	carbonProvider := fs.String("carbon-provider", "none", "none | eso (live GB regional estimates)")
	carbonInterval := fs.Duration("carbon-interval", 5*time.Minute, "live carbon polling interval (at least one minute)")
	_ = fs.Parse(args)
	if *carbonProvider != "none" && *carbonProvider != "eso" {
		return fmt.Errorf("unknown carbon provider %q", *carbonProvider)
	}
	if *interval <= 0 || *carbonInterval < time.Minute {
		return fmt.Errorf("reconcile interval must be positive and carbon interval at least one minute")
	}
	m := metrics.New()
	svc, store, err := service(ctx, c, m, nil)
	if err != nil {
		return err
	}
	defer store.Close()
	serveMetrics(ctx, c.metricsAddr, m, log)
	if *carbonProvider == "eso" {
		// Provider latency must not block lease reclamation. Join the poller
		// before closing its database connection during process shutdown.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			pollCarbon(ctx, store, carbon.ESO{Endpoint: carbon.ESOEndpoint, Client: &http.Client{}}, *carbonInterval, m, log)
		}()
		defer wg.Wait()
	}
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

func pollCarbon(ctx context.Context, store application.Store, source application.CarbonSource, interval time.Duration, m *metrics.Metrics, log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		err := application.RefreshCarbon(ctx, store, source, time.Now().UTC())
		m.CarbonRefresh(err == nil)
		if err != nil && ctx.Err() == nil {
			log.Warn("carbon refresh failed; existing readings will age into fallback", "err", err)
		}
		select {
		case <-ctx.Done():
			return
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
	grpcTarget := fs.String("grpc-target", os.Getenv("GPUB_GRPC_TARGET"), "worker lifecycle gRPC target; when set, agent RPCs use gRPC")
	grpcCA := fs.String("grpc-ca-file", "", "CA allowed to issue the gateway server certificate")
	grpcCert := fs.String("grpc-client-cert-file", "", "worker client certificate")
	grpcKey := fs.String("grpc-client-key-file", "", "worker client private key")
	grpcName := fs.String("grpc-server-name", "", "expected gateway certificate name when using a tunnel")
	bootFile := fs.String("bootstrap-token-file", "", "pool bootstrap token file (or env GPUB_BOOTSTRAP_TOKEN)")
	keyFile := fs.String("dispatch-key-file", os.Getenv("GPUB_DISPATCH_KEY_FILE"), "dispatch MAC key file (or env GPUB_DISPATCH_KEY)")
	name := fs.String("name", os.Getenv("NODE_NAME"), "worker name (defaults to $NODE_NAME)")
	model := fs.String("gpu-model", "a100", "declared GPU model")
	gpus := fs.Int("gpus", 1, "declared GPU count")
	mem := fs.Int("gpu-mem-gb", 80, "declared GPU memory per GPU")
	runtime := fs.String("runtime", "sim", "sim | kubernetes | process (trusted host execution)")
	stateDir := fs.String("state-dir", "/var/lib/gpubroker-agent", "credential, spool and runtime state")
	ns := fs.String("namespace", "gpub-jobs", "kubernetes namespace for attempt Jobs")
	gpuRes := fs.String("gpu-resource", "", `extended resource to request, e.g. "nvidia.com/gpu"; empty in Kind`)
	cpu := fs.String("cpu", "1", "per-attempt CPU limit")
	memLimit := fs.String("memory", "1Gi", "per-attempt memory limit")
	heartbeat := fs.Duration("heartbeat", 2*time.Second, "heartbeat interval (must be well under the 45s lease)")
	spotProvider := fs.String("spot-provider", "none", "none | aws | gcp (provider metadata interruption notices)")
	checkpointBucket := fs.String("checkpoint-bucket", os.Getenv("GPUB_CHECKPOINT_BUCKET"), "GCS bucket for replacement checkpoint restore")
	checkpointPrefix := fs.String("checkpoint-prefix", os.Getenv("GPUB_CHECKPOINT_PREFIX"), "GCS object prefix for checkpoints")
	recoveryJobID := fs.String("checkpoint-job-id", os.Getenv("GPUB_CHECKPOINT_JOB_ID"), "job to restore before registration on a replacement")
	recoverySource := fs.String("checkpoint-source-revision", os.Getenv("GPUB_CHECKPOINT_SOURCE_REVISION"), "pinned source revision expected in the checkpoint")
	recoveryWorkloadSHA := fs.String("checkpoint-workload-sha256", os.Getenv("GPUB_CHECKPOINT_WORKLOAD_SHA256"), "pinned workload SHA-256 expected in the checkpoint")
	requireCheckpointRestore := fs.Bool("require-checkpoint-restore", false, "fail closed before worker registration unless a compatible checkpoint restores")
	_ = fs.Parse(args)
	grpcTLS, err := grpcgateway.ClientTLSConfig(*grpcTarget, grpcgateway.TLSFiles{CAFile: *grpcCA, CertFile: *grpcCert, KeyFile: *grpcKey, ServerName: *grpcName})
	if err != nil {
		return err
	}
	var interruption agent.InterruptionSource
	switch *spotProvider {
	case "none":
	case "aws":
		interruption = spot.AWS{Client: imds.New(imds.Options{EnableFallback: aws.FalseTernary})}
	case "gcp":
		interruption = spot.GCE{}
	default:
		return fmt.Errorf("unknown spot provider %q", *spotProvider)
	}
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
	case "process":
		rt, err = agent.NewProcessRuntime(*stateDir + "/process")
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
	if *grpcTarget == "" && *apiURL == "" {
		return errors.New("--api-url or --grpc-target is required")
	}
	preRegistration, err := checkpointRecovery(rt, *requireCheckpointRestore, *checkpointBucket, *checkpointPrefix, *recoveryJobID, *recoverySource, *recoveryWorkloadSHA)
	if err != nil {
		return err
	}
	a, err := agent.New(agent.Config{APIURL: *apiURL, GRPCTarget: *grpcTarget, GRPCTLS: grpcTLS, BootstrapToken: string(boot), Name: *name, GPUModel: *model, GPUs: *gpus,
		GPUMemGB: *mem, StateDir: *stateDir, DispatchKey: key, Heartbeat: *heartbeat, Interruption: interruption, PreRegistration: preRegistration}, rt, clock, log)
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
	defer a.Close()
	return a.Run(ctx)
}

func checkpointRecovery(rt agent.Runtime, required bool, bucket, prefix, jobID, sourceRevision, workloadSHA256 string) (agent.PreRegistrationHook, error) {
	if !required {
		return nil, nil
	}
	if bucket == "" || jobID == "" || sourceRevision == "" || !isSHA256(workloadSHA256) {
		return nil, errors.New("required checkpoint restore needs bucket, job ID, source revision, and workload SHA-256")
	}
	resumePath, ok := rt.(interface{ ResumeDirectory(domain.JobID) string })
	if !ok {
		return nil, errors.New("required checkpoint restore needs the process runtime")
	}
	return gcscheckpoint.Recovery{
		Client: gcscheckpoint.Client{Tokens: gcscheckpoint.MetadataTokenSource{}},
		Bucket: bucket, Prefix: prefix, JobID: jobID,
		Destination: resumePath.ResumeDirectory(domain.JobID(jobID)), Required: true,
		ExpectedContext: map[string]string{"job_id": jobID, "source_revision": sourceRevision, "workload_sha256": workloadSHA256},
	}, nil
}

func isSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}
