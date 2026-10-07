package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

// SimRuntime is the credential-free boundary: it never executes the job
// command. It reads a declarative spec from the command arguments,
//
//	sim duration=90s exit=0 logs=3 artifact_bytes=1024
//
// and plays it against the clock. Anything that is not "sim key=value" is
// treated as an opaque command that "runs" for its max runtime / 100 and
// succeeds, so a real command string is stored and shown but never run.
// State is persisted under Dir so a restarted agent re-adopts attempts.
type SimRuntime struct {
	Dir   string
	Clock application.Clock
	mu    sync.Mutex
}

type simState struct {
	Spec      Spec          `json:"spec"`
	StartedAt time.Time     `json:"started_at"`
	Duration  time.Duration `json:"duration"`
	Exit      int           `json:"exit"`
	Logs      int           `json:"logs"`
	LogsSent  int           `json:"logs_sent"`
	Artifact  int           `json:"artifact_bytes"`
	Stopped   string        `json:"stopped,omitempty"`
}

func NewSimRuntime(dir string, clock application.Clock) (*SimRuntime, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	return &SimRuntime{Dir: dir, Clock: clock}, nil
}

func (r *SimRuntime) Name() string { return "sim" }

func parseSim(s Spec) (simState, error) {
	st := simState{Spec: s, Duration: s.MaxRuntime / 100, Logs: 2}
	if len(s.Command) == 0 || s.Command[0] != "sim" {
		return st, nil
	}
	for _, kv := range s.Command[1:] {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return st, fmt.Errorf("sim spec: %q is not key=value", kv)
		}
		var err error
		switch k {
		case "duration":
			st.Duration, err = time.ParseDuration(v)
		case "exit":
			st.Exit, err = strconv.Atoi(v)
		case "logs":
			st.Logs, err = strconv.Atoi(v)
		case "artifact_bytes":
			st.Artifact, err = strconv.Atoi(v)
		default:
			return st, fmt.Errorf("sim spec: unknown key %q", k)
		}
		if err != nil {
			return st, fmt.Errorf("sim spec %s: %w", k, err)
		}
	}
	if st.Logs > 100 || st.Artifact > 4<<20 {
		return st, fmt.Errorf("sim spec: at most 100 log lines and 4 MiB of artifact")
	}
	return st, nil
}

func (r *SimRuntime) path(id domain.AttemptID) string {
	return filepath.Join(r.Dir, string(id)+".json")
}

func (r *SimRuntime) load(id domain.AttemptID) (simState, error) {
	var st simState
	b, err := os.ReadFile(r.path(id))
	if err != nil {
		return st, err
	}
	return st, json.Unmarshal(b, &st)
}

// save writes via rename so a crash never leaves a half-written state file.
func (r *SimRuntime) save(st simState) error {
	b, _ := json.Marshal(st)
	tmp := r.path(st.Spec.AttemptID) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, r.path(st.Spec.AttemptID))
}

func (r *SimRuntime) Start(_ context.Context, s Spec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.load(s.AttemptID); err == nil {
		return nil // adopted after restart
	}
	st, err := parseSim(s)
	if err != nil {
		// An invalid sim spec is the job's fault: it runs and fails, rather
		// than wedging the agent.
		st = simState{Spec: s, Exit: 2, Logs: 0}
		st.Stopped = "invalid:" + err.Error()
	}
	st.StartedAt = r.Clock.Now()
	return r.save(st)
}

func (r *SimRuntime) Poll(_ context.Context, id domain.AttemptID) (Status, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, err := r.load(id)
	if err != nil {
		if os.IsNotExist(err) {
			return Status{Done: true, Outcome: domain.OutcomeLost, Reason: "runtime has no record of the attempt"}, nil
		}
		return Status{}, err
	}
	now := r.Clock.Now()
	elapsed := now.Sub(st.StartedAt)
	var out Status
	due := st.Logs
	if st.Duration > 0 && elapsed < st.Duration {
		due = int(float64(st.Logs) * float64(elapsed) / float64(st.Duration))
	}
	for st.LogsSent < due {
		st.LogsSent++
		out.Logs = append(out.Logs, fmt.Sprintf("sim step %d/%d", st.LogsSent, st.Logs))
	}
	switch {
	case strings.HasPrefix(st.Stopped, "invalid:"):
		code := 2
		out.Done, out.Outcome, out.ExitCode, out.Reason = true, domain.OutcomeFailed, &code, strings.TrimPrefix(st.Stopped, "invalid:")
	case st.Stopped != "":
		code := 143
		out.Done, out.Outcome, out.ExitCode, out.Reason = true, domain.AttemptOutcome(st.Stopped), &code, "stopped"
	case elapsed >= st.Duration:
		code := st.Exit
		out.Done, out.ExitCode = true, &code
		out.Outcome, out.Reason = domain.OutcomeSucceeded, "exit 0"
		if code != 0 {
			out.Outcome, out.Reason = domain.OutcomeFailed, fmt.Sprintf("exit %d", code)
		}
		if st.Artifact > 0 {
			out.Artifact = []byte(strings.Repeat("x", st.Artifact))
		}
	}
	return out, r.save(st)
}

func (r *SimRuntime) Stop(_ context.Context, id domain.AttemptID, o domain.AttemptOutcome) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, err := r.load(id)
	if err != nil {
		return nil // nothing to stop
	}
	if st.Stopped == "" {
		st.Stopped = string(o)
	}
	return r.save(st)
}

// Forget removes a finished attempt's state once its terminal event is acknowledged.
func (r *SimRuntime) Forget(id domain.AttemptID) { _ = os.Remove(r.path(id)) }

func (r *SimRuntime) List(context.Context) ([]domain.AttemptID, error) {
	ents, err := os.ReadDir(r.Dir)
	if err != nil {
		return nil, err
	}
	var out []domain.AttemptID
	for _, e := range ents {
		if n := e.Name(); strings.HasSuffix(n, ".json") {
			out = append(out, domain.AttemptID(strings.TrimSuffix(n, ".json")))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (r *SimRuntime) Draining(context.Context) bool { return false }
