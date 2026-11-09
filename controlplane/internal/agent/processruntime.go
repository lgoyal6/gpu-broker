package agent

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

const (
	processMaxRuntime      = 7 * 24 * time.Hour
	processMaxArgs         = 128
	processMaxArgBytes     = 64 << 10
	processMaxLogBytes     = 4 << 20
	processMaxArtifact     = 4 << 20
	processPollLogBytes    = 64 << 10
	processMaxLogLine      = 16 << 10
	processStopGrace       = 2 * time.Second
	processCheckpointGrace = 20 * time.Second
)

// ProcessRuntime runs argv directly as a child process under the agent's host
// UID. This is trusted-host execution, not tenant isolation. Image is descriptive
// metadata here; it does not unpack or run a container image. GPU jobs must run
// through the configured authoritative service, which provides their isolation.
type ProcessRuntime struct {
	Dir             string
	mu              sync.Mutex
	procs           map[domain.AttemptID]*processHandle
	checkpointGrace time.Duration
}

type processMetadata struct {
	Version     int                   `json:"version"`
	Spec        Spec                  `json:"spec"`
	SpecHash    string                `json:"spec_hash"`
	PID         int                   `json:"pid,omitempty"`
	PGID        int                   `json:"pgid,omitempty"`
	StartToken  string                `json:"start_token,omitempty"`
	ArgvHash    string                `json:"argv_hash,omitempty"`
	GroupToken  string                `json:"group_token,omitempty"`
	StartedAt   time.Time             `json:"started_at"`
	Deadline    time.Time             `json:"deadline"`
	LogOffset   int64                 `json:"log_offset"`
	LogPartial  string                `json:"log_partial,omitempty"`
	StopOutcome domain.AttemptOutcome `json:"stop_outcome,omitempty"`
	Done        bool                  `json:"done"`
	Outcome     domain.AttemptOutcome `json:"outcome,omitempty"`
	ExitCode    *int                  `json:"exit_code,omitempty"`
	Reason      string                `json:"reason,omitempty"`
}

type processHandle struct{ done chan struct{} }

var _ Runtime = (*ProcessRuntime)(nil)

// NewProcessRuntime creates a host-process runtime with durable per-attempt state.
func NewProcessRuntime(dir string) (*ProcessRuntime, error) {
	if dir == "" {
		return nil, errors.New("process runtime state directory is required")
	}
	if err := os.MkdirAll(filepath.Join(dir, "attempts"), 0o700); err != nil {
		return nil, err
	}
	return &ProcessRuntime{Dir: dir, procs: make(map[domain.AttemptID]*processHandle), checkpointGrace: processCheckpointGrace}, nil
}

func (r *ProcessRuntime) Name() string { return "process" }

func (r *ProcessRuntime) attemptKey(id domain.AttemptID) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

func (r *ProcessRuntime) statePath(id domain.AttemptID) string {
	return filepath.Join(r.Dir, "attempts", r.attemptKey(id)+".json")
}

func (r *ProcessRuntime) workDir(id domain.AttemptID) string {
	return filepath.Join(r.Dir, "attempts", r.attemptKey(id))
}

func (r *ProcessRuntime) jobDir(id domain.JobID) string {
	sum := sha256.Sum256([]byte(id))
	return filepath.Join(r.Dir, "jobs", hex.EncodeToString(sum[:]))
}

func (r *ProcessRuntime) checkpointDir(job domain.JobID, attempt domain.AttemptID) string {
	return filepath.Join(r.jobDir(job), "checkpoints", r.attemptKey(attempt))
}

func (r *ProcessRuntime) resumeDir(job domain.JobID) string {
	return filepath.Join(r.jobDir(job), "resume")
}

// ResumeDirectory returns the stable job-scoped restore target. A controller
// must populate it before registration or dispatch for replacement attempts.
func (r *ProcessRuntime) ResumeDirectory(job domain.JobID) string { return r.resumeDir(job) }

func (r *ProcessRuntime) uploadMarker(job domain.JobID, attempt domain.AttemptID) string {
	return filepath.Join(r.jobDir(job), "barriers", r.attemptKey(attempt), "GCS_UPLOAD_COMPLETE")
}

func (r *ProcessRuntime) logPath(id domain.AttemptID) string {
	return filepath.Join(r.workDir(id), "stdout.log")
}

func (r *ProcessRuntime) load(id domain.AttemptID) (processMetadata, error) {
	var m processMetadata
	b, err := os.ReadFile(r.statePath(id))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("decode process state for %s: %w", id, err)
	}
	if m.Version != 1 || m.Spec.AttemptID != id {
		return m, fmt.Errorf("invalid process state for attempt %s", id)
	}
	return m, nil
}

// save uses a synced temporary file and directory sync so a persisted PID never
// points at a partial metadata file after an agent or host restart.
func (r *ProcessRuntime) save(m processMetadata) error {
	path := r.statePath(m.Spec.AttemptID)
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func specHash(s Spec) string {
	b, _ := json.Marshal(s)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func validateProcessSpec(s Spec) error {
	if s.AttemptID == "" || s.JobID == "" || len(s.Command) == 0 || s.Command[0] == "" {
		return errors.New("process runtime requires job/attempt IDs and non-empty argv")
	}
	if len(s.Command) > processMaxArgs {
		return fmt.Errorf("process argv exceeds %d arguments", processMaxArgs)
	}
	bytes := 0
	for _, arg := range s.Command {
		bytes += len(arg)
	}
	if bytes > processMaxArgBytes {
		return fmt.Errorf("process argv exceeds %d bytes", processMaxArgBytes)
	}
	if s.MaxRuntime <= 0 || s.MaxRuntime > processMaxRuntime {
		return fmt.Errorf("process max runtime must be between 1ns and %s", processMaxRuntime)
	}
	if _, err := exec.LookPath(s.Command[0]); err != nil {
		return fmt.Errorf("resolve executable %q: %w", s.Command[0], err)
	}
	return nil
}

func (r *ProcessRuntime) Start(ctx context.Context, s Spec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateProcessSpec(s); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if m, err := r.load(s.AttemptID); err == nil {
		if m.SpecHash != specHash(s) {
			return fmt.Errorf("attempt %s already has different process metadata", s.AttemptID)
		}
		if m.Done {
			return nil
		}
		if m.PID <= 0 {
			deadline := time.Now().Add(500 * time.Millisecond)
			for time.Now().Before(deadline) {
				pid, identity, found := findProcessByToken(m.GroupToken, m.Spec.Command)
				if found {
					m.PID, m.PGID = pid, identity.PGID
					m.StartToken, m.ArgvHash = identity.StartToken, identity.ArgvHash
					if err := r.save(m); err != nil {
						return err
					}
					return nil
				}
				time.Sleep(10 * time.Millisecond)
			}
			return r.markLost(m, "no process matched the durable start intent")
		}
		if m.StartToken == "" || m.ArgvHash == "" {
			deadline := time.Now().Add(500 * time.Millisecond)
			for time.Now().Before(deadline) {
				id, inspectErr := inspectProcess(m.PID)
				if inspectErr == nil && id.PGID == m.PGID && id.ArgvHash == argvHash(m.Spec.Command) {
					m.StartToken, m.ArgvHash = id.StartToken, id.ArgvHash
					return r.save(m)
				}
				if processExited(m.PID) {
					return r.markLost(m, "process exited before its identity was persisted")
				}
				time.Sleep(10 * time.Millisecond)
			}
			return r.markLost(m, "could not validate the process from its durable start intent")
		}
		ok, err := processMatches(m.PID, m.PGID, m.StartToken, m.ArgvHash)
		if (err == nil && ok) || processGroupMatches(m.PGID, m.GroupToken) {
			return nil // adopted after a worker-agent restart; never duplicate it.
		}
		return r.markLost(m, "process identity no longer matches persisted state")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	if err := os.MkdirAll(r.workDir(s.AttemptID), 0o700); err != nil {
		return err
	}
	checkpointDir := r.checkpointDir(s.JobID, s.AttemptID)
	resumeDir := r.resumeDir(s.JobID)
	uploadMarker := r.uploadMarker(s.JobID, s.AttemptID)
	for _, dir := range []string{checkpointDir, resumeDir, filepath.Dir(uploadMarker)} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	for _, marker := range []string{filepath.Join(checkpointDir, "CHECKPOINT_COMPLETE"), uploadMarker} {
		if err := os.Remove(marker); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("clear stale checkpoint marker: %w", err)
		}
	}
	now := time.Now().UTC()
	var tokenBytes [16]byte
	if _, err := rand.Read(tokenBytes[:]); err != nil {
		return fmt.Errorf("create process group token: %w", err)
	}
	m := processMetadata{Version: 1, Spec: s, SpecHash: specHash(s), GroupToken: hex.EncodeToString(tokenBytes[:]),
		StartedAt: now, Deadline: now.Add(s.MaxRuntime)}
	cmd, err := newProcessCommand(s.Command)
	if err != nil {
		return err
	}
	logFile, err := os.OpenFile(r.logPath(s.AttemptID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if err := r.save(m); err != nil {
		_ = logFile.Close()
		return err
	}
	cmd.Dir = r.workDir(s.AttemptID)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.Env = processEnvironment(os.Environ(), map[string]string{
		"GPUBROKER_ATTEMPT_ID":           string(s.AttemptID),
		"GPUBROKER_ATTEMPT_TOKEN":        m.GroupToken,
		"GPU_BROKER_CHECKPOINT_DIR":      checkpointDir,
		"GPU_BROKER_RESUME_DIR":          resumeDir,
		"GPU_BROKER_JOB_ID":              string(s.JobID),
		"GPU_BROKER_ATTEMPT":             string(s.AttemptID),
		"GPU_BROKER_CHECKPOINT_DEADLINE": "20",
		"GPU_BROKER_GCS_UPLOAD_COMPLETE": uploadMarker,
	})
	if err := setProcessGroup(cmd); err != nil {
		_ = logFile.Close()
		return err
	}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		_ = os.Remove(r.statePath(s.AttemptID))
		return fmt.Errorf("start process: %w", err)
	}
	_ = logFile.Close()
	m.PID, m.PGID = cmd.Process.Pid, cmd.Process.Pid
	if err := r.save(m); err != nil {
		_ = signalProcessGroup(m.PGID, syscall.SIGKILL)
		_ = cmd.Wait()
		return fmt.Errorf("persist process start: %w", err)
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		id, inspectErr := inspectProcess(m.PID)
		if inspectErr == nil && id.PGID == m.PGID && id.ArgvHash == argvHash(s.Command) {
			m.StartToken, m.ArgvHash = id.StartToken, id.ArgvHash
			break
		}
		if time.Now().After(deadline) {
			if processExited(m.PID) {
				err := cmd.Wait()
				if processGroupAlive(m.PGID) {
					cleanupOwnedProcessGroup(m.PGID, m.GroupToken)
				}
				finishProcessMetadata(&m, err)
				if saveErr := r.save(m); saveErr != nil {
					return saveErr
				}
				return nil
			}
			_ = signalProcessGroup(m.PGID, syscall.SIGKILL)
			err := cmd.Wait()
			finishProcessMetadata(&m, err)
			_ = r.save(m)
			return fmt.Errorf("started process could not be identified: %v", inspectErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := r.save(m); err != nil {
		_ = signalProcessGroup(m.PGID, syscall.SIGKILL)
		_ = cmd.Wait()
		return fmt.Errorf("persist process identity: %w", err)
	}
	h := &processHandle{done: make(chan struct{})}
	r.procs[s.AttemptID] = h
	go r.wait(s.AttemptID, cmd, h)
	return nil
}

func argvHash(argv []string) string {
	b, _ := json.Marshal(argv)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func processEnvironment(base []string, additions map[string]string) []string {
	allowed := map[string]struct{}{
		"PATH": {}, "HOME": {}, "TMPDIR": {}, "TMP": {}, "TEMP": {}, "LANG": {}, "LC_ALL": {},
		"LC_CTYPE": {}, "TZ": {}, "LD_LIBRARY_PATH": {}, "DYLD_LIBRARY_PATH": {}, "CUDA_HOME": {},
		"CUDA_VISIBLE_DEVICES": {}, "NVIDIA_VISIBLE_DEVICES": {}, "CUBLAS_WORKSPACE_CONFIG": {},
	}
	out := make([]string, 0, len(allowed)+len(additions))
	for _, entry := range base {
		key, _, ok := strings.Cut(entry, "=")
		if ok {
			_, keep := allowed[key]
			if _, replace := additions[key]; replace || !keep {
				continue
			}
		}
		out = append(out, entry)
	}
	keys := make([]string, 0, len(additions))
	for key := range additions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out = append(out, key+"="+additions[key])
	}
	return out
}

func (r *ProcessRuntime) wait(id domain.AttemptID, cmd *exec.Cmd, h *processHandle) {
	err := cmd.Wait()
	if m, loadErr := r.load(id); loadErr == nil && processGroupAlive(m.PGID) && processGroupMatches(m.PGID, m.GroupToken) {
		cleanupOwnedProcessGroup(m.PGID, m.GroupToken)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	defer close(h.done)
	m, loadErr := r.load(id)
	if loadErr == nil && !m.Done {
		finishProcessMetadata(&m, err)
		_ = r.save(m)
	}
	delete(r.procs, id)
}

func finishProcessMetadata(m *processMetadata, err error) {
	m.Done = true
	code := processExitCode(err)
	m.ExitCode = &code
	m.Outcome, m.Reason = domain.OutcomeSucceeded, "exit 0"
	if code != 0 {
		m.Outcome, m.Reason = domain.OutcomeFailed, fmt.Sprintf("exit %d", code)
	}
	if m.StopOutcome != "" {
		m.Outcome, m.Reason = m.StopOutcome, "stopped by runtime request"
	}
}

func processExitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok {
			if ws.Signaled() {
				return 128 + int(ws.Signal())
			}
			return ws.ExitStatus()
		}
	}
	return 125
}

func cleanupOwnedProcessGroup(pgid int, token string) {
	if !processGroupAlive(pgid) || !processGroupMatches(pgid, token) {
		return
	}
	_ = signalProcessGroup(pgid, syscall.SIGTERM)
	termDeadline := time.Now().Add(processStopGrace)
	for processGroupAlive(pgid) && processGroupMatches(pgid, token) && time.Now().Before(termDeadline) {
		time.Sleep(25 * time.Millisecond)
	}
	if processGroupAlive(pgid) && processGroupMatches(pgid, token) {
		_ = signalProcessGroup(pgid, syscall.SIGKILL)
	}
	reapDeadline := time.Now().Add(2 * time.Second)
	for processGroupAlive(pgid) && processGroupMatches(pgid, token) && time.Now().Before(reapDeadline) {
		time.Sleep(25 * time.Millisecond)
	}
}

func (r *ProcessRuntime) Poll(ctx context.Context, id domain.AttemptID) (Status, error) {
	if err := ctx.Err(); err != nil {
		return Status{}, err
	}
	r.mu.Lock()
	m, err := r.load(id)
	if err != nil {
		r.mu.Unlock()
		if errors.Is(err, os.ErrNotExist) {
			return Status{Done: true, Outcome: domain.OutcomeLost, Reason: "runtime has no record of the attempt"}, nil
		}
		return Status{}, err
	}
	if !m.Done && !time.Now().Before(m.Deadline) {
		r.mu.Unlock()
		if err := r.Stop(ctx, id, domain.OutcomeTimedOut); err != nil {
			return Status{}, err
		}
		r.mu.Lock()
		m, err = r.load(id)
	}
	if err == nil && !m.Done {
		ok, identityErr := processMatches(m.PID, m.PGID, m.StartToken, m.ArgvHash)
		if (identityErr != nil || !ok) && !processGroupMatches(m.PGID, m.GroupToken) {
			if h := r.procs[id]; h != nil {
				r.mu.Unlock()
				select {
				case <-h.done:
					return r.Poll(ctx, id)
				case <-time.After(50 * time.Millisecond):
				}
				r.mu.Lock()
				m, err = r.load(id)
			}
			if err == nil && !m.Done {
				_ = r.markLost(m, "process exited before its exit status was recorded")
			}
			m, err = r.load(id)
		}
	}
	if err != nil {
		r.mu.Unlock()
		return Status{}, err
	}
	logs, logErr := r.readLogs(&m)
	if logErr != nil {
		r.mu.Unlock()
		return Status{}, logErr
	}
	if err := r.save(m); err != nil {
		r.mu.Unlock()
		return Status{}, err
	}
	out := Status{Done: m.Done, Outcome: m.Outcome, ExitCode: m.ExitCode, Reason: m.Reason, Logs: logs}
	if m.Done {
		artifact, err := readProcessArtifact(r.workDir(id))
		if err != nil {
			r.mu.Unlock()
			return Status{}, err
		}
		out.Artifact = artifact
	}
	r.mu.Unlock()
	return out, nil
}

func (r *ProcessRuntime) readLogs(m *processMetadata) ([]string, error) {
	f, err := os.Open(r.logPath(m.Spec.AttemptID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	end := min(info.Size(), processMaxLogBytes)
	if m.LogOffset > end {
		m.LogOffset = end
	}
	n := min(int64(processPollLogBytes), end-m.LogOffset)
	buf := make([]byte, n)
	if n > 0 {
		read, err := f.ReadAt(buf, m.LogOffset)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		buf = buf[:read]
		m.LogOffset += int64(read)
	}
	text := m.LogPartial + string(buf)
	parts := strings.Split(text, "\n")
	if !m.Done {
		m.LogPartial = parts[len(parts)-1]
		if len(m.LogPartial) > processMaxLogLine {
			m.LogPartial = m.LogPartial[:processMaxLogLine]
		}
		parts = parts[:len(parts)-1]
	} else {
		m.LogPartial = ""
	}
	logs := make([]string, 0, len(parts))
	for _, line := range parts {
		line = strings.TrimSuffix(line, "\r")
		if len(line) > processMaxLogLine {
			line = line[:processMaxLogLine]
		}
		if line != "" {
			logs = append(logs, line)
		}
	}
	return logs, nil
}

func readProcessArtifact(dir string) ([]byte, error) {
	path := filepath.Join(dir, "output.bin")
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("process artifact must be a regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, processMaxArtifact))
}

func (r *ProcessRuntime) Stop(ctx context.Context, id domain.AttemptID, outcome domain.AttemptOutcome) error {
	if outcome == "" {
		outcome = domain.OutcomeCancelled
	}
	r.mu.Lock()
	m, err := r.load(id)
	if errors.Is(err, os.ErrNotExist) {
		r.mu.Unlock()
		return nil
	}
	if err != nil {
		r.mu.Unlock()
		return err
	}
	if m.Done {
		r.mu.Unlock()
		return nil
	}
	ok, identityErr := processMatches(m.PID, m.PGID, m.StartToken, m.ArgvHash)
	if (identityErr != nil || !ok) && !processGroupMatches(m.PGID, m.GroupToken) {
		err := r.markLost(m, "refused to signal a process with a stale or mismatched identity")
		r.mu.Unlock()
		return err
	}
	m.StopOutcome = outcome
	if err := r.save(m); err != nil {
		r.mu.Unlock()
		return err
	}
	h := r.procs[id]
	r.mu.Unlock()

	if outcome == domain.OutcomePreempted && identityErr == nil && ok {
		// The checkpoint handler belongs to the workload process. Never signal
		// the process group here: a shell supervisor must remain alive to flush
		// the checkpoint and exit cleanly.
		stillSame, _ := processMatches(m.PID, m.PGID, m.StartToken, m.ArgvHash)
		if stillSame {
			proc, err := os.FindProcess(m.PID)
			if err == nil {
				_ = proc.Signal(syscall.SIGUSR1)
			}
		}
		checkpointMarker := filepath.Join(r.checkpointDir(m.Spec.JobID, m.Spec.AttemptID), "CHECKPOINT_COMPLETE")
		uploadMarker := r.uploadMarker(m.Spec.JobID, m.Spec.AttemptID)
		checkpointDeadline := time.Now().Add(r.checkpointGrace)
		for time.Now().Before(checkpointDeadline) {
			if _, err := os.Stat(uploadMarker); err == nil {
				break // uploader publishes this only after the manifest commit.
			}
			if _, err := os.Stat(checkpointMarker); err != nil {
				alive, _ := processMatches(m.PID, m.PGID, m.StartToken, m.ArgvHash)
				if !alive {
					break // no complete checkpoint can be uploaded.
				}
			} else if h != nil {
				select {
				case <-h.done:
					// Work has exited after a complete checkpoint. Keep waiting
					// for the external GCS publisher, within the same deadline.
				default:
				}
			}
			select {
			case <-ctx.Done():
				checkpointDeadline = time.Now()
			case <-time.After(50 * time.Millisecond):
			}
		}
	}

	if processGroupAlive(m.PGID) && processGroupOwned(m) {
		if err := signalProcessGroup(m.PGID, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
	} else if processGroupAlive(m.PGID) {
		return errors.New("refused to signal process group after its identity could no longer be validated")
	}
	until := time.Now().Add(processStopGrace)
	for time.Now().Before(until) && processGroupAlive(m.PGID) {
		select {
		case <-ctx.Done():
			until = time.Now()
		case <-time.After(25 * time.Millisecond):
		}
	}
	if processGroupAlive(m.PGID) && processGroupOwned(m) {
		if err := signalProcessGroup(m.PGID, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
	} else if processGroupAlive(m.PGID) {
		return errors.New("refused SIGKILL after process group identity became stale")
	}
	reapDeadline := time.Now().Add(2 * time.Second)
	for processGroupAlive(m.PGID) && time.Now().Before(reapDeadline) {
		time.Sleep(25 * time.Millisecond)
	}
	if processGroupAlive(m.PGID) {
		return errors.New("process group survived SIGKILL")
	}
	if h != nil {
		select {
		case <-h.done: // cmd.Wait reaped the direct child.
		case <-time.After(2 * time.Second):
			return errors.New("timed out waiting for process reaping after SIGKILL")
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	m, err = r.load(id)
	if err != nil {
		return err
	}
	if !m.Done {
		m.Done, m.Outcome, m.Reason = true, outcome, "process group stopped"
		code := 137
		m.ExitCode = &code
		if err := r.save(m); err != nil {
			return err
		}
	}
	return nil
}

func (r *ProcessRuntime) markLost(m processMetadata, reason string) error {
	m.Done, m.Outcome, m.Reason = true, domain.OutcomeLost, reason
	code := 125
	m.ExitCode = &code
	return r.save(m)
}

func processGroupOwned(m processMetadata) bool {
	ok, err := processMatches(m.PID, m.PGID, m.StartToken, m.ArgvHash)
	return (err == nil && ok) || processGroupMatches(m.PGID, m.GroupToken)
}

func (r *ProcessRuntime) List(ctx context.Context) ([]domain.AttemptID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(r.Dir, "attempts"))
	if err != nil {
		return nil, err
	}
	ids := make([]domain.AttemptID, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(r.Dir, "attempts", entry.Name()))
		if err != nil {
			return nil, err
		}
		var m processMetadata
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, fmt.Errorf("decode process state %s: %w", entry.Name(), err)
		}
		if m.Version != 1 || m.Spec.AttemptID == "" || r.statePath(m.Spec.AttemptID) != filepath.Join(r.Dir, "attempts", entry.Name()) {
			return nil, fmt.Errorf("invalid process state %s", entry.Name())
		}
		ids = append(ids, m.Spec.AttemptID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func (r *ProcessRuntime) Draining(context.Context) bool { return false }
