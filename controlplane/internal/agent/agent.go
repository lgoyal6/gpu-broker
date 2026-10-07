package agent

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/transport/client"
)

type Config struct {
	APIURL         string
	BootstrapToken string
	Name           string
	GPUModel       string
	GPUs           int
	GPUMemGB       int
	StateDir       string
	DispatchKey    []byte
	Heartbeat      time.Duration
	MaxConcurrent  int
}

// Agent holds only what it can rebuild from its state directory and the
// runtime: the worker credential, the spool of unsent events, and the set
// of attempts the runtime is running.
type Agent struct {
	cfg     Config
	rt      Runtime
	clock   application.Clock
	log     *slog.Logger
	signer  *application.LeaseSigner
	api     *client.ClientWithResponses
	token   string
	worker  domain.WorkerID
	mu      sync.Mutex
	running map[domain.AttemptID]Spec
	stopped map[domain.AttemptID]domain.AttemptOutcome
	start   map[domain.AttemptID]time.Time
	drain   bool
}

func New(cfg Config, rt Runtime, clock application.Clock, log *slog.Logger) (*Agent, error) {
	if err := os.MkdirAll(filepath.Join(cfg.StateDir, "spool"), 0o750); err != nil {
		return nil, err
	}
	signer, err := application.NewLeaseSigner(cfg.DispatchKey)
	if err != nil {
		return nil, err
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = 10 * time.Second
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = cfg.GPUs
	}
	return &Agent{cfg: cfg, rt: rt, clock: clock, log: log, signer: signer,
		running: map[domain.AttemptID]Spec{}, stopped: map[domain.AttemptID]domain.AttemptOutcome{}, start: map[domain.AttemptID]time.Time{}}, nil
}

func (a *Agent) client(token string) (*client.ClientWithResponses, error) {
	return client.NewClientWithResponses(a.cfg.APIURL, client.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}),
		client.WithRequestEditorFn(func(_ context.Context, r *http.Request) error {
			r.Header.Set("Authorization", "Bearer "+token)
			return nil
		}))
}

// Register loads the worker credential from the state directory, or
// registers with the bootstrap token and stores it (0600) before using it.
func (a *Agent) Register(ctx context.Context) error {
	path := filepath.Join(a.cfg.StateDir, "worker-token")
	idPath := filepath.Join(a.cfg.StateDir, "worker-id")
	if b, err := os.ReadFile(path); err == nil && len(b) > 0 {
		a.token = string(bytes.TrimSpace(b))
		id, _ := os.ReadFile(idPath)
		a.worker = domain.WorkerID(bytes.TrimSpace(id))
	} else {
		boot, err := a.client(a.cfg.BootstrapToken)
		if err != nil {
			return err
		}
		r, err := boot.RegisterWorkerWithResponse(ctx, client.RegisterWorkerRequest{Name: a.cfg.Name, GpuModel: a.cfg.GPUModel,
			Gpus: a.cfg.GPUs, GpuMemGb: a.cfg.GPUMemGB, Runtimes: []client.RegisterWorkerRequestRuntimes{client.RegisterWorkerRequestRuntimes(a.rt.Name())}})
		if err != nil {
			return err
		}
		if r.JSON201 == nil {
			return fmt.Errorf("register: %d %s", r.StatusCode(), r.Body)
		}
		a.token, a.worker = r.JSON201.Token, domain.WorkerID(r.JSON201.WorkerId)
		// The id is written first: a token without its id would leave a
		// restarted Kubernetes agent unable to find its own attempt Jobs.
		if err := os.WriteFile(idPath, []byte(a.worker), 0o600); err != nil {
			return err
		}
		if err := os.WriteFile(path, []byte(a.token), 0o600); err != nil {
			return err
		}
		a.log.Info("registered", "worker", r.JSON201.WorkerId, "pool", r.JSON201.PoolId)
	}
	c, err := a.client(a.token)
	a.api = c
	return err
}

// WorkerID is the registered worker's id.
func (a *Agent) WorkerID() domain.WorkerID { return a.worker }

// Recover re-adopts attempts the runtime still holds after a restart. The
// next heartbeat tells us which of them the control plane still considers
// ours; the rest are stopped as abandoned.
func (a *Agent) Recover(ctx context.Context) error {
	ids, err := a.rt.List(ctx)
	if err != nil {
		return err
	}
	a.mu.Lock()
	for _, id := range ids {
		a.running[id] = Spec{AttemptID: id}
		a.start[id] = a.clock.Now()
	}
	a.mu.Unlock()
	if len(ids) > 0 {
		a.log.Info("recovered attempts from runtime", "count", len(ids))
	}
	return a.flushSpool(ctx)
}

// Run loops until ctx is cancelled. A SIGTERM cancels ctx: the agent stops
// claiming, but leaves running attempts in the runtime so a restarted agent
// can adopt them (pods outlive the agent pod).
func (a *Agent) Run(ctx context.Context) error {
	t := time.NewTicker(a.cfg.Heartbeat)
	defer t.Stop()
	for {
		if err := a.Step(ctx); err != nil && ctx.Err() == nil {
			a.log.Warn("agent step failed; will retry", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Step is one iteration: heartbeat, act on stop orders, poll the runtime,
// report, claim new work. Exposed so tests drive it deterministically.
func (a *Agent) Step(ctx context.Context) error {
	a.drain = a.rt.Draining(ctx)
	if err := a.heartbeat(ctx); err != nil {
		return err
	}
	if err := a.poll(ctx); err != nil {
		return err
	}
	if err := a.flushSpool(ctx); err != nil {
		return err
	}
	if a.drain {
		return nil
	}
	return a.claim(ctx)
}

func (a *Agent) held() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, 0, len(a.running))
	for id := range a.running {
		out = append(out, string(id))
	}
	return out
}

func (a *Agent) heartbeat(ctx context.Context) error {
	state := client.READY
	if a.drain {
		state = client.DRAINING
	}
	r, err := a.api.HeartbeatWithResponse(ctx, client.HeartbeatRequest{State: state, Held: a.held()})
	if err != nil {
		return err
	}
	if r.JSON200 == nil {
		return fmt.Errorf("heartbeat: %d %s", r.StatusCode(), r.Body)
	}
	for _, s := range r.JSON200.Stop {
		o := map[client.StopOrderKind]domain.AttemptOutcome{"cancel": domain.OutcomeCancelled, "preempt": domain.OutcomePreempted,
			"timeout": domain.OutcomeTimedOut}[s.Kind]
		a.stop(ctx, domain.AttemptID(s.AttemptId), o)
	}
	for _, id := range r.JSON200.Abandon {
		// The control plane already reclaimed this attempt (lease expired
		// while we were away): kill it so it stops consuming the GPU, and
		// do not report it; its outcome is already recorded.
		_ = a.rt.Stop(ctx, domain.AttemptID(id), domain.OutcomeLost)
		a.forget(domain.AttemptID(id))
		a.log.Warn("abandoned attempt the control plane reclaimed", "attempt", id)
	}
	return nil
}

func (a *Agent) stop(ctx context.Context, id domain.AttemptID, o domain.AttemptOutcome) {
	a.mu.Lock()
	_, ok := a.running[id]
	if ok {
		a.stopped[id] = o
	}
	a.mu.Unlock()
	if ok {
		if err := a.rt.Stop(ctx, id, o); err != nil {
			a.log.Warn("stop failed", "attempt", id, "err", err)
		}
	}
}

func (a *Agent) forget(id domain.AttemptID) {
	a.mu.Lock()
	delete(a.running, id)
	delete(a.stopped, id)
	delete(a.start, id)
	a.mu.Unlock()
	if f, ok := a.rt.(interface{ Forget(domain.AttemptID) }); ok {
		f.Forget(id)
	}
}

func (a *Agent) poll(ctx context.Context) error {
	a.mu.Lock()
	ids := make([]domain.AttemptID, 0, len(a.running))
	for id := range a.running {
		ids = append(ids, id)
	}
	a.mu.Unlock()
	for _, id := range ids {
		a.mu.Lock()
		spec, started, stopReq := a.running[id], a.start[id], a.stopped[id]
		a.mu.Unlock()
		// Local timeout enforcement: the control plane also enforces it,
		// but the agent is closest to the work and stops it first.
		if spec.MaxRuntime > 0 && stopReq == "" && a.clock.Now().Sub(started) > spec.MaxRuntime {
			a.stop(ctx, id, domain.OutcomeTimedOut)
		}
		st, err := a.rt.Poll(ctx, id)
		if err != nil {
			return err
		}
		for _, line := range st.Logs {
			if err := a.spool(id, application.WorkerEvent{Kind: "log", Message: line}); err != nil {
				return err
			}
		}
		if !st.Done {
			continue
		}
		if stopReq != "" && st.Outcome != domain.OutcomeSucceeded {
			st.Outcome = stopReq
		}
		if len(st.Artifact) > 0 {
			if err := a.upload(ctx, id, "output.bin", st.Artifact); err != nil {
				return err
			}
		}
		if err := a.spool(id, application.WorkerEvent{Kind: "exit", Outcome: st.Outcome, ExitCode: st.ExitCode, Message: st.Reason}); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) claim(ctx context.Context) error {
	a.mu.Lock()
	free := a.cfg.MaxConcurrent - len(a.running)
	a.mu.Unlock()
	if free <= 0 {
		return nil
	}
	r, err := a.api.ClaimDispatchesWithResponse(ctx, client.ClaimRequest{Max: min(free, 16)})
	if err != nil {
		return err
	}
	if r.JSON200 == nil {
		return fmt.Errorf("claim: %d %s", r.StatusCode(), r.Body)
	}
	for _, d := range r.JSON200.Dispatches {
		if err := a.accept(ctx, d); err != nil {
			a.log.Warn("dispatch refused", "attempt", d.AttemptId, "err", err)
		}
	}
	return nil
}

// accept verifies a dispatch locally before anything runs: the MAC must
// verify under the shared key, it must be addressed to us, and the spec it
// carries must hash to the digest the MAC covers.
func (a *Agent) accept(ctx context.Context, d client.Dispatch) error {
	digest := application.SpecDigest(d.Image, d.Command, d.Gpus, d.MaxRuntimeS)
	if digest != d.SpecDigest || !a.signer.Verify(domain.AttemptID(d.AttemptId), domain.WorkerID(d.WorkerId), d.LeaseExpires, d.SpecDigest, d.Mac) {
		return application.ErrLeaseInvalid
	}
	r, err := a.api.AckDispatchWithResponse(ctx, d.AttemptId, d)
	if err != nil {
		return err
	}
	switch {
	case r.JSON409 != nil && r.JSON409.Error.Code == "already_acknowledged":
		return nil // redelivered; we are already running it
	case r.JSON200 == nil:
		return fmt.Errorf("ack: %d %s", r.StatusCode(), r.Body)
	}
	id := domain.AttemptID(d.AttemptId)
	if r.JSON200.Action == client.AckResponseActionCancel {
		return a.spool(id, application.WorkerEvent{Kind: "exit", Outcome: domain.OutcomeCancelled, Message: "cancelled before start"})
	}
	spec := Spec{AttemptID: id, JobID: domain.JobID(d.JobId), Image: d.Image, Command: d.Command, GPUs: d.Gpus,
		MaxRuntime: time.Duration(d.MaxRuntimeS) * time.Second}
	a.mu.Lock()
	a.running[id] = spec
	a.start[id] = a.clock.Now()
	a.mu.Unlock()
	if err := a.rt.Start(ctx, spec); err != nil {
		code := 125
		return a.spool(id, application.WorkerEvent{Kind: "exit", Outcome: domain.OutcomeFailed, ExitCode: &code, Message: "runtime start: " + err.Error()})
	}
	return a.spool(id, application.WorkerEvent{Kind: "started", Message: "started in " + a.rt.Name()})
}

// ---- spool ----
//
// Every event is appended (and fsynced) to <state>/spool/<attempt>.jsonl
// with its sequence number before any attempt to send it. flushSpool sends
// whatever is unsent; the server deduplicates by (attempt, seq), so a crash
// after sending but before recording "sent" just resends. A spool file is
// deleted only after its exit event was accepted.

func (a *Agent) spoolPath(id domain.AttemptID) string {
	return filepath.Join(a.cfg.StateDir, "spool", string(id)+".jsonl")
}

func readSpool(path string) ([]application.WorkerEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []application.WorkerEvent
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		var e application.WorkerEvent
		if json.Unmarshal(sc.Bytes(), &e) == nil {
			out = append(out, e)
		}
		// A torn last line from a crash mid-append is skipped; its event is
		// re-derived on the next poll (logs) or was never acknowledged.
	}
	return out, sc.Err()
}

func (a *Agent) spool(id domain.AttemptID, e application.WorkerEvent) error {
	path := a.spoolPath(id)
	prior, err := readSpool(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, p := range prior {
		if p.Kind == "exit" && e.Kind == "exit" {
			return nil // one terminal event per attempt, ever
		}
	}
	e.Seq = int64(len(prior) + 1)
	if e.At.IsZero() {
		e.At = a.clock.Now()
	}
	b, _ := json.Marshal(e)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (a *Agent) flushSpool(ctx context.Context) error {
	files, err := filepath.Glob(filepath.Join(a.cfg.StateDir, "spool", "*.jsonl"))
	if err != nil {
		return err
	}
	for _, path := range files {
		id := domain.AttemptID(trimExt(filepath.Base(path)))
		evs, err := readSpool(path)
		if err != nil || len(evs) == 0 {
			continue
		}
		body := client.EventsRequest{}
		terminal := false
		for _, e := range evs {
			ce := client.WorkerEvent{Seq: e.Seq, Kind: client.WorkerEventKind(e.Kind), At: e.At}
			if e.Message != "" {
				m := e.Message
				if len(m) > 16384 {
					m = m[:16384]
				}
				ce.Message = &m
			}
			if e.Outcome != "" {
				o := client.WorkerEventOutcome(e.Outcome)
				ce.Outcome = &o
				terminal = true
			}
			ce.ExitCode = e.ExitCode
			body.Events = append(body.Events, ce)
		}
		if len(body.Events) > 500 {
			body.Events = body.Events[len(body.Events)-500:]
		}
		r, err := a.api.ReportEventsWithResponse(ctx, string(id), body)
		if err != nil {
			return err
		}
		switch {
		case r.JSON200 != nil:
		case r.StatusCode() == http.StatusNotFound:
			// The control plane does not know the attempt for this worker:
			// nothing to deliver to. Drop the spool rather than retry forever.
			a.log.Warn("dropping spool for unknown attempt", "attempt", id)
			terminal = true
		default:
			return fmt.Errorf("events for %s: %d %s", id, r.StatusCode(), r.Body)
		}
		if terminal {
			_ = os.Remove(path)
			a.forget(id)
		} else {
			// Non-terminal events were accepted; keep only the count so the
			// next seq continues, by truncating to a marker-free rewrite.
			if err := compactSpool(path, evs); err != nil {
				return err
			}
		}
	}
	return nil
}

// compactSpool keeps the spool's length (the next sequence number) while
// dropping already-accepted payloads, so a long-running job's spool does not
// grow without bound.
func compactSpool(path string, evs []application.WorkerEvent) error {
	var buf bytes.Buffer
	for _, e := range evs {
		e.Message = ""
		b, _ := json.Marshal(e)
		buf.Write(append(b, '\n'))
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func trimExt(s string) string { return s[:len(s)-len(filepath.Ext(s))] }

// upload sends an artifact in chunks, resuming from the server's committed
// offset after any mismatch (a lost response, a restart, a retry).
func (a *Agent) upload(ctx context.Context, id domain.AttemptID, name string, data []byte) error {
	const chunk = 1 << 20
	off := int64(0)
	for tries := 0; off < int64(len(data)) && tries < 50; tries++ {
		end := min(off+chunk, int64(len(data)))
		r, err := a.api.UploadArtifactChunkWithBodyWithResponse(ctx, string(id), name, &client.UploadArtifactChunkParams{Offset: off},
			"application/octet-stream", bytes.NewReader(data[off:end]))
		if err != nil {
			return err
		}
		switch {
		case r.JSON200 != nil:
			off = r.JSON200.Offset
		case r.JSON409 != nil && r.JSON409.Error.Offset != nil:
			off = *r.JSON409.Error.Offset
		case r.StatusCode() == http.StatusServiceUnavailable:
			time.Sleep(200 * time.Millisecond)
		default:
			return fmt.Errorf("upload %s: %d %s", name, r.StatusCode(), r.Body)
		}
	}
	sum := sha256.Sum256(data)
	r, err := a.api.CompleteArtifactWithResponse(ctx, string(id), name, client.CompleteArtifactRequest{Sha256: hex.EncodeToString(sum[:])})
	if err != nil {
		return err
	}
	if r.StatusCode() != http.StatusNoContent {
		return fmt.Errorf("complete %s: %d %s", name, r.StatusCode(), r.Body)
	}
	return nil
}
