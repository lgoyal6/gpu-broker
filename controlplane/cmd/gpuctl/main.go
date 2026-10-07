// Command gpuctl is the CLI for the control plane. It talks only to the
// authenticated API through the client generated from the OpenAPI contract.
//
//	export GPUB_API_URL=http://localhost:8080 GPUB_TOKEN=gpub_user_...
//	gpuctl submit --gpus 2 --max-runtime 2h -- python train.py
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain/policy"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/sim"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/transport/client"
)

const usage = `gpuctl <command> [flags]

  submit [flags] -- <command...>   queue a job (prints its id)
  status [job-id]                  list your jobs, or one job with its transitions
  cancel <job-id>                  cancel (idempotent)
  logs <job-id>                    log, start and exit events
  pools                            pools, capacity and prices
  policy explain <job-id>          every recorded decision, with its arithmetic
  policy explain --gpus N ...      what the scheduler would decide right now
  policy list                      builtin policies and versions
  trace export <file>              privacy-bounded trace of your tenant (operator)
  trace replay <file>              replay a trace locally through the policies
  doctor                           check the API, your token, and pool health
  demo seed                        create labelled demo tenants (API must run --demo)
  worker register                  register a worker with a pool bootstrap token

Environment: GPUB_API_URL (default http://localhost:8080), GPUB_TOKEN.
`

func main() {
	if len(os.Args) < 2 || os.Args[1] == "-h" || os.Args[1] == "help" || os.Args[1] == "--help" {
		fmt.Print(usage)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := run(ctx, os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func apiURL() string {
	if u := os.Getenv("GPUB_API_URL"); u != "" {
		return u
	}
	return "http://localhost:8080"
}

func newClient(token string) (*client.ClientWithResponses, error) {
	return client.NewClientWithResponses(apiURL(), client.WithHTTPClient(&http.Client{Timeout: 60 * time.Second}),
		client.WithRequestEditorFn(func(_ context.Context, r *http.Request) error {
			r.Header.Set("Authorization", "Bearer "+token)
			r.Header.Set("User-Agent", "gpuctl")
			return nil
		}))
}

func userClient() (*client.ClientWithResponses, error) {
	t := os.Getenv("GPUB_TOKEN")
	if t == "" {
		return nil, errors.New("GPUB_TOKEN is not set (an operator creates users: gpubroker admin create-tenant, or POST /v1/users)")
	}
	return newClient(t)
}

// apiError renders the stable error envelope, hint included.
func apiError(code int, body []byte) error {
	var eb client.ErrorBody
	if json.Unmarshal(body, &eb) == nil && eb.Error.Code != "" {
		msg := fmt.Sprintf("%s (%d): %s", eb.Error.Code, code, eb.Error.Message)
		if eb.Error.Hint != nil {
			msg += "\n  hint: " + *eb.Error.Hint
		}
		if eb.Error.JobId != nil {
			msg += "\n  job: " + *eb.Error.JobId
		}
		return errors.New(msg + "\n  request: " + eb.Error.RequestId)
	}
	return fmt.Errorf("HTTP %d: %s", code, strings.TrimSpace(string(body)))
}

func idemKey() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "gpuctl-" + hex.EncodeToString(b[:])
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

type submitFlags struct {
	fs                         *flag.FlagSet
	image, model, pol, project string
	runtime, deadline, pools   string
	gpus, mem, prio, attempts  int
	maxRuntime, maxDelay       time.Duration
	budget                     float64
	preemptible                bool
}

func newSubmitFlags(name string) *submitFlags {
	s := &submitFlags{fs: flag.NewFlagSet(name, flag.ExitOnError)}
	f := s.fs
	f.StringVar(&s.image, "image", "busybox:1.36", "container image")
	f.IntVar(&s.gpus, "gpus", 1, "GPUs on one worker")
	f.StringVar(&s.model, "gpu-model", "", "require this GPU model (a100, h100, l4, ...)")
	f.IntVar(&s.mem, "min-gpu-mem-gb", 0, "minimum GPU memory")
	f.StringVar(&s.runtime, "runtime", "", "require a runtime: sim | kubernetes")
	f.IntVar(&s.prio, "priority", 0, "0..9")
	f.BoolVar(&s.preemptible, "preemptible", false, "may be preempted by higher-priority work")
	f.DurationVar(&s.maxRuntime, "max-runtime", time.Hour, "hard runtime limit; also sizes the budget hold")
	f.DurationVar(&s.maxDelay, "max-delay", 0, "allow EcoShift to delay the start by up to this much")
	f.StringVar(&s.deadline, "deadline", "", "RFC3339 time the job must finish by")
	f.Float64Var(&s.budget, "budget-cap-usd", 0, "per-job spend ceiling")
	f.StringVar(&s.pol, "policy", "", "policy name (gpuctl policy list)")
	f.StringVar(&s.pools, "pools", "", "comma-separated pool ids the job may use")
	f.StringVar(&s.project, "project", "", "project id (default project if empty)")
	f.IntVar(&s.attempts, "max-attempts", 0, "attempts before giving up on lost capacity (default 3)")
	return s
}

func (s *submitFlags) request(cmd []string) (client.SubmitJobRequest, error) {
	r := client.SubmitJobRequest{Image: s.image, Command: cmd, Gpus: s.gpus, MaxRuntimeS: int64(s.maxRuntime.Seconds())}
	opt := func(v string) *string {
		if v == "" {
			return nil
		}
		return &v
	}
	r.GpuModel, r.Policy, r.ProjectId = opt(s.model), opt(s.pol), opt(s.project)
	if s.runtime != "" {
		rt := client.SubmitJobRequestRuntime(s.runtime)
		r.Runtime = &rt
	}
	if s.mem > 0 {
		r.MinGpuMemGb = &s.mem
	}
	if s.prio > 0 {
		r.Priority = &s.prio
	}
	if s.preemptible {
		r.Preemptible = &s.preemptible
	}
	if s.maxDelay > 0 {
		d := int64(s.maxDelay.Seconds())
		r.MaxDelayS = &d
	}
	if s.budget > 0 {
		f := float32(s.budget)
		r.BudgetCapUsd = &f
	}
	if s.attempts > 0 {
		r.MaxAttempts = &s.attempts
	}
	if s.pools != "" {
		ps := strings.Split(s.pools, ",")
		r.AllowedPools = &ps
	}
	if s.deadline != "" {
		t, err := time.Parse(time.RFC3339, s.deadline)
		if err != nil {
			return r, fmt.Errorf("--deadline: %w", err)
		}
		r.Deadline = &t
	}
	return r, nil
}

func run(ctx context.Context, cmd string, args []string) error {
	switch cmd {
	case "submit":
		sf := newSubmitFlags("submit")
		_ = sf.fs.Parse(args)
		command := sf.fs.Args()
		if len(command) == 0 {
			return errors.New("give the command after --, e.g. gpuctl submit --gpus 1 -- python train.py")
		}
		req, err := sf.request(command)
		if err != nil {
			return err
		}
		c, err := userClient()
		if err != nil {
			return err
		}
		r, err := c.SubmitJobWithResponse(ctx, &client.SubmitJobParams{IdempotencyKey: idemKey()}, req)
		if err != nil {
			return err
		}
		if r.JSON201 == nil {
			return apiError(r.StatusCode(), r.Body)
		}
		fmt.Printf("%s %s (policy %s)\n", r.JSON201.Id, r.JSON201.State, r.JSON201.Policy)
		return nil
	case "status":
		c, err := userClient()
		if err != nil {
			return err
		}
		if len(args) == 0 {
			r, err := c.ListJobsWithResponse(ctx, nil)
			if err != nil {
				return err
			}
			if r.JSON200 == nil {
				return apiError(r.StatusCode(), r.Body)
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
			fmt.Fprintln(tw, "JOB\tSTATE\tGPUS\tPOLICY\tATTEMPT\tSUBMITTED")
			for _, j := range r.JSON200.Jobs {
				fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%d/%d\t%s\n", j.Id, j.State, j.Gpus, j.Policy, j.Attempt, j.MaxAttempts, j.SubmittedAt.Format(time.RFC3339))
			}
			return tw.Flush()
		}
		r, err := c.GetJobWithResponse(ctx, args[0])
		if err != nil {
			return err
		}
		if r.JSON200 == nil {
			return apiError(r.StatusCode(), r.Body)
		}
		d := r.JSON200
		fmt.Printf("%s  %s  gpus=%d policy=%s attempt=%d/%d  evidence=%s\n", d.Job.Id, d.Job.State, d.Job.Gpus, d.Job.Policy,
			d.Job.Attempt, d.Job.MaxAttempts, d.Job.EvidenceClass)
		fmt.Printf("budget: held $%.4f  settled $%.4f  released $%.4f  outstanding $%.4f\n",
			d.Budget.HeldUsd, d.Budget.SettledUsd, d.Budget.ReleasedUsd, d.Budget.OutstandingUsd)
		for _, t := range d.Transitions {
			fmt.Printf("  %s  %-16s -> %-16s %-10s %s\n", t.At.Format("15:04:05"), t.From, t.To, t.ActorKind, t.Reason)
		}
		for _, a := range d.Artifacts {
			fmt.Printf("  artifact %s/%s %d bytes %s\n", a.AttemptId, a.Name, a.Size, a.State)
		}
		return nil
	case "cancel":
		if len(args) != 1 {
			return errors.New("usage: gpuctl cancel <job-id>")
		}
		c, err := userClient()
		if err != nil {
			return err
		}
		r, err := c.CancelJobWithResponse(ctx, args[0], &client.CancelJobParams{IdempotencyKey: idemKey()})
		if err != nil {
			return err
		}
		if r.JSON200 == nil {
			return apiError(r.StatusCode(), r.Body)
		}
		fmt.Println(r.JSON200.Id, r.JSON200.State)
		return nil
	case "logs":
		if len(args) != 1 {
			return errors.New("usage: gpuctl logs <job-id>")
		}
		c, err := userClient()
		if err != nil {
			return err
		}
		r, err := c.JobLogsWithResponse(ctx, args[0], nil)
		if err != nil {
			return err
		}
		if r.JSON200 == nil {
			return apiError(r.StatusCode(), r.Body)
		}
		for _, l := range r.JSON200.Lines {
			fmt.Printf("%s %s #%d %-7s %s\n", l.At.Format(time.RFC3339), l.AttemptId, l.Seq, l.Kind, l.Message)
		}
		return nil
	case "pools":
		c, err := userClient()
		if err != nil {
			return err
		}
		r, err := c.ListPoolsWithResponse(ctx)
		if err != nil {
			return err
		}
		if r.JSON200 == nil {
			return apiError(r.StatusCode(), r.Body)
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "POOL\tREGION\tKIND\tWORKERS\tGPUS FREE/TOTAL\tMODELS\t$/GPU-H\tSPOT")
		for _, p := range r.JSON200.Pools {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d/%d\t%s\t%.2f\t%v\n", p.Id, p.Region, p.Kind, p.WorkersReady, p.GpusFree, p.GpusTotal,
				strings.Join(p.GpuModels, ","), p.PriceUsdPerGpuHour, p.Interruptible)
		}
		return tw.Flush()
	case "policy":
		return policyCmd(ctx, args)
	case "trace":
		return traceCmd(ctx, args)
	case "doctor":
		return doctor(ctx)
	case "demo":
		if len(args) != 1 || args[0] != "seed" {
			return errors.New("usage: gpuctl demo seed")
		}
		c, err := userClient()
		if err != nil {
			return err
		}
		r, err := c.DemoSeedWithResponse(ctx)
		if err != nil {
			return err
		}
		if r.JSON201 == nil {
			return apiError(r.StatusCode(), r.Body)
		}
		fmt.Println("Seeded demo tenants (evidence_class=seeded; excluded from the public status page):")
		printJSON(r.JSON201)
		return nil
	case "worker":
		return workerCmd(ctx, args)
	}
	return fmt.Errorf("unknown command %q\n\n%s", cmd, usage)
}

func policyCmd(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: gpuctl policy explain <job-id> | gpuctl policy explain --gpus N ... | gpuctl policy list")
	}
	c, err := userClient()
	if err != nil {
		return err
	}
	switch args[0] {
	case "list":
		r, err := c.ListPoliciesWithResponse(ctx)
		if err != nil {
			return err
		}
		if r.JSON200 == nil {
			return apiError(r.StatusCode(), r.Body)
		}
		for _, p := range r.JSON200.Policies {
			mark := " "
			if p.Name == r.JSON200.Default {
				mark = "*"
			}
			fmt.Printf("%s %-16s %s\n", mark, p.Name, p.Version)
		}
		return nil
	case "explain":
	default:
		return fmt.Errorf("unknown policy subcommand %q", args[0])
	}
	rest := args[1:]
	if len(rest) == 1 && !strings.HasPrefix(rest[0], "-") {
		r, err := c.JobDecisionsWithResponse(ctx, rest[0])
		if err != nil {
			return err
		}
		if r.JSON200 == nil {
			return apiError(r.StatusCode(), r.Body)
		}
		if len(r.JSON200.Decisions) == 0 {
			fmt.Println("no decisions recorded yet (the scheduler has not considered this job)")
		}
		for _, d := range r.JSON200.Decisions {
			explainDecision(d)
		}
		return nil
	}
	sf := newSubmitFlags("policy explain")
	_ = sf.fs.Parse(rest)
	req, err := sf.request([]string{"explain"})
	if err != nil {
		return err
	}
	r, err := c.ExplainPolicyWithResponse(ctx, req)
	if err != nil {
		return err
	}
	if r.JSON200 == nil {
		return apiError(r.StatusCode(), r.Body)
	}
	fmt.Println("hypothetical job, current snapshot (nothing was written):")
	explainDecision(*r.JSON200)
	return nil
}

func explainDecision(d client.Decision) {
	fmt.Printf("%s  %s  %s\n  reason: %s\n", d.PolicyVersion, d.Action, d.InputDigest[:12], d.Reason)
	if d.Order.Terms != nil {
		fmt.Printf("  queue position %d, score %.4f (%s)\n", d.Order.Position, d.Order.Score, strings.Join(*d.Order.Terms, "; "))
	}
	if d.ChosenPool != nil {
		w := ""
		if d.ChosenWorker != nil {
			w = *d.ChosenWorker
		}
		fmt.Printf("  chosen: pool %s worker %s  hold $%.4f\n", *d.ChosenPool, w, float64(d.BudgetEffectMicroUsd)/1e6)
	}
	if d.CarbonEffectGrams != nil {
		src := ""
		if d.CarbonSource != nil {
			src = *d.CarbonSource
		}
		fmt.Printf("  carbon: %.1f g (%s, source %s)\n", *d.CarbonEffectGrams, deref(d.CarbonBasis), src)
	} else {
		fmt.Println("  carbon: unknown (no fresh carbon data for the chosen region; no carbon claim made)")
	}
	if d.DelayUntil != nil {
		fmt.Printf("  delayed until %s, est. saving %.1f g\n", d.DelayUntil.Format(time.RFC3339), derefF(d.CarbonSavedGrams))
	}
	if len(d.Fallbacks) > 0 {
		fmt.Printf("  fallbacks: %s\n", strings.Join(d.Fallbacks, ", "))
	}
	fmt.Printf("  considered %d workers:\n", d.CandidatesConsidered)
	for _, c := range d.CandidatesScored {
		parts := []string{}
		for _, comp := range c.Components {
			parts = append(parts, fmt.Sprintf("%s %.3fx%.2f", comp.Name, comp.Normalized, comp.Weight))
		}
		fmt.Printf("    %-24s %-14s score %.4f  [%s]\n", c.WorkerId, c.PoolId, c.Score, strings.Join(parts, " + "))
	}
	for _, r := range d.CandidatesRejected {
		fmt.Printf("    %-24s %-14s rejected: %s %s\n", r.WorkerId, r.PoolId, r.Reason, deref(r.Detail))
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefF(f *float32) float32 {
	if f == nil {
		return 0
	}
	return *f
}

func traceCmd(ctx context.Context, args []string) error {
	if len(args) < 2 {
		return errors.New("usage: gpuctl trace export <file> | gpuctl trace replay <file> [--capacity N] [--policy a,b]")
	}
	switch args[0] {
	case "export":
		c, err := userClient()
		if err != nil {
			return err
		}
		r, err := c.ExportTraceWithResponse(ctx)
		if err != nil {
			return err
		}
		if r.JSON200 == nil {
			return apiError(r.StatusCode(), r.Body)
		}
		if err := os.WriteFile(args[1], r.Body, 0o600); err != nil {
			return err
		}
		fmt.Printf("wrote %s: %d jobs, digest %s\n", args[1], len(r.JSON200.Jobs), r.JSON200.Digest[:16])
		return nil
	case "replay":
		fs := flag.NewFlagSet("trace replay", flag.ExitOnError)
		capacity := fs.Int("capacity", 1, "declared GPUs per GPU type (traces do not record capacity)")
		pol := fs.String("policy", "fifo,fair-share,priority,deadline-first", "policies to compare")
		_ = fs.Parse(args[2:])
		sc, info, err := sim.LoadTrace(args[1], *capacity)
		if err != nil {
			return err
		}
		printJSON(info)
		fmt.Println("SIMULATOR OUTPUT (evidence_class=simulator): replayed ordering only; see limits above.")
		tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', tabwriter.AlignRight)
		fmt.Fprintln(tw, "policy\tdone\tp50 wait h\tp95 wait h\tmax wait h\tjain\t")
		for _, name := range strings.Split(*pol, ",") {
			p, err := policy.Lookup(name)
			if err != nil {
				return err
			}
			res := sim.Run(sc, p)
			fmt.Fprintf(tw, "%s\t%d\t%.2f\t%.2f\t%.2f\t%.3f\t\n", name, res.Completed, res.WaitHoursP50, res.WaitHoursP95, res.WaitHoursMax, res.JainFairness)
		}
		return tw.Flush()
	}
	return fmt.Errorf("unknown trace subcommand %q", args[0])
}

func doctor(ctx context.Context) error {
	ok := true
	check := func(name string, err error, fix string) {
		if err != nil {
			ok = false
			fmt.Printf("FAIL  %s: %v\n      fix: %s\n", name, err, fix)
			return
		}
		fmt.Printf("ok    %s\n", name)
	}
	resp, err := http.Get(apiURL() + "/readyz")
	if err == nil {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			err = fmt.Errorf("%d %s", resp.StatusCode, strings.TrimSpace(string(b)))
		}
	}
	check("API ready at "+apiURL(), err, "start the api role (gpubroker api) and run gpubroker migrate; check GPUB_API_URL")
	c, err := userClient()
	check("token present", err, "export GPUB_TOKEN=gpub_user_...")
	if err != nil {
		return errors.New("doctor found problems")
	}
	r, err := c.ListPoolsWithResponse(ctx)
	if err == nil && r.JSON200 == nil {
		err = apiError(r.StatusCode(), r.Body)
	}
	check("token accepted", err, "the token was revoked or mistyped; ask an operator for a new one")
	if err == nil {
		ready := 0
		for _, p := range r.JSON200.Pools {
			ready += p.WorkersReady
		}
		var perr error
		if len(r.JSON200.Pools) == 0 {
			perr = errors.New("no pools defined")
		} else if ready == 0 {
			perr = errors.New("pools exist but no worker is READY (heartbeats stale or none registered)")
		}
		check(fmt.Sprintf("%d pools, %d ready workers", len(r.JSON200.Pools), ready), perr,
			"gpubroker admin create-pool, then run a worker-agent with that pool's bootstrap token")
	}
	if !ok {
		return errors.New("doctor found problems")
	}
	return nil
}

func workerCmd(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "register" {
		return errors.New("usage: gpuctl worker register --bootstrap-token T --name N --gpus G --gpu-model M")
	}
	fs := flag.NewFlagSet("worker register", flag.ExitOnError)
	boot := fs.String("bootstrap-token", os.Getenv("GPUB_BOOTSTRAP_TOKEN"), "pool bootstrap token")
	name := fs.String("name", "", "worker name")
	gpus := fs.Int("gpus", 1, "GPUs")
	model := fs.String("gpu-model", "a100", "GPU model")
	mem := fs.Int("gpu-mem-gb", 80, "GPU memory")
	rt := fs.String("runtime", "sim", "sim | kubernetes")
	_ = fs.Parse(args[1:])
	c, err := newClient(*boot)
	if err != nil {
		return err
	}
	r, err := c.RegisterWorkerWithResponse(ctx, client.RegisterWorkerRequest{Name: *name, Gpus: *gpus, GpuModel: *model, GpuMemGb: *mem,
		Runtimes: []client.RegisterWorkerRequestRuntimes{client.RegisterWorkerRequestRuntimes(*rt)}})
	if err != nil {
		return err
	}
	if r.JSON201 == nil {
		return apiError(r.StatusCode(), r.Body)
	}
	printJSON(r.JSON201)
	return nil
}
