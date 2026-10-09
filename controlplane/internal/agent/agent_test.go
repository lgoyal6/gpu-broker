package agent_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/agent"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/testkit"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/transport/httpapi"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// flaky fails every request while down is set: an API outage.
type flaky struct {
	h    http.Handler
	down atomic.Bool
}

type interruptionSource struct{ interrupted atomic.Bool }

func (s *interruptionSource) Interrupted(context.Context, time.Time) (bool, error) {
	return s.interrupted.Load(), nil
}

func (f *flaky) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if f.down.Load() {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	f.h.ServeHTTP(w, r)
}

type rig struct {
	e    *testkit.Env
	api  *flaky
	url  string
	boot string
	user application.Principal
}

func newRig(t *testing.T) *rig {
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 100)
	boot, err := e.Svc.UpsertPool(e.Ctx, domain.Pool{ID: "pool-a", Name: "pool-a", Kind: domain.PoolSimulated, Region: "london",
		PriceMicroUSDPerGPUHour: 1_000_000, PUE: 1.1})
	e.Must(err)
	f := &flaky{h: httpapi.New(e.Svc, quiet, nil, nil, false).Handler()}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return &rig{e: e, api: f, url: srv.URL, boot: boot, user: user}
}

func (r *rig) agent(t *testing.T, dir, name string, key []byte) *agent.Agent {
	return r.agentWithSource(t, dir, name, key, nil)
}

func (r *rig) agentWithSource(t *testing.T, dir, name string, key []byte, source agent.InterruptionSource) *agent.Agent {
	rt, err := agent.NewSimRuntime(dir+"/sim", r.e.Clock)
	r.e.Must(err)
	a, err := agent.New(agent.Config{APIURL: r.url, BootstrapToken: r.boot, Name: name, GPUModel: "a100", GPUs: 2, GPUMemGB: 80,
		StateDir: dir, DispatchKey: key, Interruption: source}, rt, r.e.Clock, quiet)
	r.e.Must(err)
	r.e.Must(a.Register(r.e.Ctx))
	r.e.Must(a.Recover(r.e.Ctx))
	return a
}

func TestAgentPersistsSpotInterruptionAndDrains(t *testing.T) {
	r := newRig(t)
	source := &interruptionSource{}
	dir := t.TempDir()
	a := r.agentWithSource(t, dir, "node-1", testkit.DispatchKey, source)
	id := r.submit(t, "sim duration=1h")
	r.e.Tick()
	r.e.Must(a.Step(r.e.Ctx)) // claim and start before the notice arrives
	if st := r.state(t, id); st != domain.JobRunning {
		t.Fatalf("before interruption: %s", st)
	}
	source.interrupted.Store(true)
	r.e.Must(a.Step(r.e.Ctx)) // persist notice, stop work, and report PREEMPTED
	if st := r.state(t, id); st != domain.JobQueued {
		t.Fatalf("interrupted job was not requeued: %s", st)
	}
	if _, err := os.Stat(filepath.Join(dir, "interrupted")); err != nil {
		t.Fatalf("interruption marker: %v", err)
	}

	// The marker survives a process restart and prevents a new claim while the
	// node is draining. This is the safety boundary for an EC2 termination
	// notice: no replacement work may start on the node.
	b := r.agent(t, dir, "node-1", testkit.DispatchKey)
	r.e.Must(b.Step(r.e.Ctx))
	if n := r.e.QueryInt(`SELECT count(*) FROM job_attempts WHERE acked_at IS NOT NULL`); n != 1 {
		t.Fatalf("draining agent claimed replacement work: %d acknowledged attempts", n)
	}
}

func (r *rig) submit(t *testing.T, cmd string) domain.JobID {
	j := r.e.Submit(r.user, 1, func(s *application.SubmitRequest) { s.Command = strings.Fields(cmd); s.Runtime = "sim" })
	return j.ID
}

func (r *rig) state(t *testing.T, id domain.JobID) domain.JobState { return r.e.Job(r.user, id).State }

func TestAgentRunsAJobEndToEnd(t *testing.T) {
	r := newRig(t)
	a := r.agent(t, t.TempDir(), "node-1", testkit.DispatchKey)
	id := r.submit(t, "sim duration=5m logs=3 artifact_bytes=3000000")
	r.e.Tick()
	r.e.Must(a.Step(r.e.Ctx)) // claim, ack, start
	if st := r.state(t, id); st != domain.JobRunning {
		t.Fatalf("after claim: %s", st)
	}
	for i := 0; i < 12; i++ { // heartbeat every 30s while it runs (lease TTL 45s)
		r.e.Clock.Advance(30 * time.Second)
		r.e.Must(a.Step(r.e.Ctx))
	}
	if st := r.state(t, id); st != domain.JobSucceeded {
		t.Fatalf("after run: %s", st)
	}
	v, _ := r.e.Svc.GetJob(r.e.Ctx, r.user, id)
	if len(v.Artifacts) != 1 || v.Artifacts[0].Size != 3000000 || v.Artifacts[0].State != domain.ArtifactComplete {
		t.Fatalf("artifact %+v", v.Artifacts)
	}
	logs, _ := r.e.Svc.Logs(r.e.Ctx, r.user, id, 100)
	if len(logs) != 5 { // started, 3 log lines, exit
		t.Fatalf("logs %d", len(logs))
	}
}

// The agent process dies mid-run and a new one starts on the same state
// directory: it re-adopts the attempt from the runtime and finishes it as
// the same attempt. No second attempt, no duplicate terminal.
func TestAgentRestartRecoversTheInterruptedAttempt(t *testing.T) {
	r := newRig(t)
	dir := t.TempDir()
	a := r.agent(t, dir, "node-1", testkit.DispatchKey)
	id := r.submit(t, "sim duration=10m")
	r.e.Tick()
	r.e.Must(a.Step(r.e.Ctx))
	r.e.Clock.Advance(20 * time.Second)
	// a is gone (no shutdown, no goodbye). A new agent on the same state.
	b := r.agent(t, dir, "node-1", testkit.DispatchKey)
	for i := 0; i < 25; i++ { // keep heartbeating while the job runs
		r.e.Clock.Advance(30 * time.Second)
		r.e.Must(b.Step(r.e.Ctx))
	}
	if st := r.state(t, id); st != domain.JobSucceeded {
		t.Fatalf("after restart: %s", st)
	}
	if n := r.e.QueryInt(`SELECT count(*) FROM job_attempts`); n != 1 {
		t.Fatalf("%d attempts; restart should not retry", n)
	}
	if n := r.e.QueryInt(`SELECT count(*) FROM workers`); n != 1 {
		t.Fatalf("restart re-registered as a new worker: %d", n)
	}
}

// The agent is away longer than the lease. The reconciler reclaims the
// attempt; when the agent comes back the heartbeat tells it to abandon, and
// its late result is never reported over the retry.
func TestAgentAbandonsAnAttemptTheControlPlaneReclaimed(t *testing.T) {
	r := newRig(t)
	dir := t.TempDir()
	a := r.agent(t, dir, "node-1", testkit.DispatchKey)
	id := r.submit(t, "sim duration=10m")
	r.e.Tick()
	r.e.Must(a.Step(r.e.Ctx))
	r.e.Clock.Advance(2 * time.Minute) // past the 45s lease
	rec, err := r.e.Svc.Reconcile(r.e.Ctx)
	r.e.Must(err)
	if rec.ExpiredLeases != 1 || r.state(t, id) != domain.JobQueued {
		t.Fatalf("reconcile %+v, job %s", rec, r.state(t, id))
	}
	b := r.agent(t, dir, "node-1", testkit.DispatchKey)
	r.e.Must(b.Step(r.e.Ctx)) // heartbeat -> abandon; then it may claim the retry
	if n := r.e.QueryInt(`SELECT count(*) FROM attempt_events e JOIN job_attempts a ON a.id = e.attempt_id
		WHERE a.outcome = 'LOST' AND e.kind = 'exit'`); n != 0 {
		t.Fatal("abandoned attempt reported a terminal event")
	}
}

func TestAgentCancelsOnStopOrder(t *testing.T) {
	r := newRig(t)
	a := r.agent(t, t.TempDir(), "node-1", testkit.DispatchKey)
	id := r.submit(t, "sim duration=1h")
	r.e.Tick()
	r.e.Must(a.Step(r.e.Ctx))
	_, err := r.e.Svc.Cancel(r.e.Ctx, r.user, id, "user")
	r.e.Must(err)
	if st := r.state(t, id); st != domain.JobCancelRequested {
		t.Fatalf("after cancel: %s", st)
	}
	r.e.Must(a.Step(r.e.Ctx)) // heartbeat delivers stop, poll sees it, exit CANCELLED
	if st := r.state(t, id); st != domain.JobCancelled {
		t.Fatalf("after stop: %s", st)
	}
}

func TestAgentRefusesADispatchItCannotVerify(t *testing.T) {
	r := newRig(t)
	a := r.agent(t, t.TempDir(), "node-1", []byte(strings.Repeat("w", 32))) // wrong key
	id := r.submit(t, "sim duration=1m")
	r.e.Tick()
	r.e.Must(a.Step(r.e.Ctx))
	if n := r.e.QueryInt(`SELECT count(*) FROM job_attempts WHERE acked_at IS NOT NULL`); n != 0 {
		t.Fatal("agent acked a dispatch whose MAC did not verify")
	}
	if st := r.state(t, id); st != domain.JobDispatched {
		t.Fatalf("job %s", st)
	}
}

// The API is down when the job finishes. The exit event waits in the spool
// and is delivered exactly once when the API returns.
func TestSpoolDeliversTheTerminalEventAfterAnOutage(t *testing.T) {
	r := newRig(t)
	a := r.agent(t, t.TempDir(), "node-1", testkit.DispatchKey)
	id := r.submit(t, "sim duration=1m")
	r.e.Tick()
	r.e.Must(a.Step(r.e.Ctx))
	r.e.Clock.Advance(2 * time.Minute)
	r.api.down.Store(true)
	if err := a.Step(r.e.Ctx); err == nil {
		t.Fatal("step succeeded during the outage")
	}
	r.api.down.Store(false)
	// The lease would have expired while the API was down; renew by stepping
	// immediately after recovery (heartbeat precedes the flush).
	r.e.Must(a.Step(r.e.Ctx))
	if st := r.state(t, id); st != domain.JobSucceeded {
		t.Fatalf("after recovery: %s", st)
	}
	r.e.Must(a.Step(r.e.Ctx))
	if n := r.e.QueryInt(`SELECT count(*) FROM attempt_events WHERE kind = 'exit'`); n != 1 {
		t.Fatalf("%d exit events", n)
	}
}

func TestKubernetesManifestEnforcesTheIsolationBoundary(t *testing.T) {
	k := &agent.KubeRuntime{Namespace: "gpub-jobs", NodeName: "gpu-node-7", WorkerID: "wrk_ABC", GPUResource: "nvidia.com/gpu",
		CPU: "4", Memory: "16Gi", Tolerations: []corev1.Toleration{{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}}
	j := k.Manifest(agent.Spec{AttemptID: "att_000123", JobID: "job_1", Image: "ghcr.io/x/train:1", Command: []string{"python", "train.py"},
		GPUs: 2, MaxRuntime: 90 * time.Minute})
	pod := j.Spec.Template.Spec
	c := pod.Containers[0]
	switch {
	case j.Name != "gpub-att-000123":
		t.Fatalf("name %s", j.Name)
	case j.Spec.PodFailurePolicy == nil || j.Spec.PodFailurePolicy.Rules[0].OnPodConditions[0].Type != corev1.DisruptionTarget:
		t.Fatal("no pod failure policy for disruptions; an evicted attempt would be reported as the user's failure")
	case *j.Spec.BackoffLimit != 0:
		t.Fatal("job controller may retry; retries belong to the control plane")
	case *j.Spec.ActiveDeadlineSeconds != 5400:
		t.Fatal("deadline")
	case *pod.AutomountServiceAccountToken || *pod.EnableServiceLinks:
		t.Fatal("service account token or service links exposed")
	case !*pod.SecurityContext.RunAsNonRoot || pod.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault:
		t.Fatal("pod security context")
	case *c.SecurityContext.AllowPrivilegeEscalation || !*c.SecurityContext.ReadOnlyRootFilesystem || *c.SecurityContext.Privileged:
		t.Fatal("container security context")
	case len(c.SecurityContext.Capabilities.Drop) != 1 || c.SecurityContext.Capabilities.Drop[0] != "ALL":
		t.Fatal("capabilities not dropped")
	case pod.NodeName != "gpu-node-7" || pod.RestartPolicy != corev1.RestartPolicyNever:
		t.Fatal("placement")
	}
	if q := c.Resources.Limits["nvidia.com/gpu"]; q.Value() != 2 {
		t.Fatalf("gpu limit %v", q.String())
	}
	if c.Command[0] != "python" || c.Args[0] != "train.py" {
		t.Fatal("command split")
	}
	kind := &agent.KubeRuntime{Namespace: "gpub-jobs"}
	if _, ok := kind.Manifest(agent.Spec{AttemptID: "a", MaxRuntime: time.Minute, GPUs: 1}).Spec.Template.Spec.Containers[0].Resources.Limits["nvidia.com/gpu"]; ok {
		t.Fatal("Kind profile requested a GPU resource")
	}
}
