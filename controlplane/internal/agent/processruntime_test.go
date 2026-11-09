package agent

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

func processSpec(id string, command ...string) Spec {
	return Spec{AttemptID: domain.AttemptID(id), JobID: "job-process-test", Command: command, MaxRuntime: 5 * time.Second}
}

func pollProcessUntilDone(t *testing.T, r *ProcessRuntime, id domain.AttemptID) Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st, err := r.Poll(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if st.Done {
			return st
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("process did not finish before test deadline")
	return Status{}
}

func TestProcessRuntimeExecutesArgvWithoutShellInterpolation(t *testing.T) {
	r, err := NewProcessRuntime(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	exe, err := exec.LookPath("printf")
	if err != nil {
		t.Skip("printf is unavailable")
	}
	sentinel := filepath.Join(t.TempDir(), "must-not-exist")
	arg := "literal;touch " + sentinel
	s := processSpec("argv-no-shell", exe, "%s\\n", arg)
	if err := r.Start(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	st := pollProcessUntilDone(t, r, s.AttemptID)
	if st.Outcome != domain.OutcomeSucceeded || strings.Join(st.Logs, "\n") != arg {
		t.Fatalf("status=%+v, wanted literal argv output %q", st, arg)
	}
	if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
		t.Fatalf("shell metacharacters were interpreted; sentinel stat error=%v", err)
	}
}

func TestProcessRuntimeDoesNotPassAgentCredentialsToWorkload(t *testing.T) {
	r, err := NewProcessRuntime(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GPUBROKER_BOOTSTRAP_TOKEN", "agent-secret")
	t.Setenv("DATABASE_URL", "postgres://agent-secret")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "cloud-secret")
	exe, err := exec.LookPath("env")
	if err != nil {
		t.Skip("env is unavailable")
	}
	s := processSpec("environment-filter", exe)
	if err := r.Start(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	st := pollProcessUntilDone(t, r, s.AttemptID)
	output := strings.Join(st.Logs, "\n")
	for _, secret := range []string{"agent-secret", "cloud-secret", "DATABASE_URL=", "AWS_SECRET_ACCESS_KEY=", "GPUBROKER_BOOTSTRAP_TOKEN="} {
		if strings.Contains(output, secret) {
			t.Fatalf("workload inherited sensitive parent environment entry %q", secret)
		}
	}
	if !strings.Contains(output, "PATH=") {
		t.Fatal("workload did not receive the minimal executable search path")
	}
}

func TestProcessRuntimeAdoptsAndStopsProcessGroupAfterRestart(t *testing.T) {
	dir := t.TempDir()
	r1, err := NewProcessRuntime(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := processSpec("adopt-stop", "/bin/sh", "-c", "sleep 30 & wait")
	if err := r1.Start(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	m1, err := r1.load(s.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := NewProcessRuntime(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := r2.Start(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	m2, err := r2.load(s.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if m1.PID != m2.PID || m1.StartToken != m2.StartToken {
		t.Fatalf("restart duplicated or replaced process: first=%+v second=%+v", m1, m2)
	}
	ids, err := r2.List(context.Background())
	if err != nil || len(ids) != 1 || ids[0] != s.AttemptID {
		t.Fatalf("List() = %v, %v", ids, err)
	}
	if err := r2.Stop(context.Background(), s.AttemptID, domain.OutcomeCancelled); err != nil {
		t.Fatal(err)
	}
	if processGroupMatches(m2.PGID, m2.GroupToken) {
		t.Fatalf("process group %d survived Stop", m2.PGID)
	}
	st, err := r2.Poll(context.Background(), s.AttemptID)
	if err != nil || !st.Done || st.Outcome != domain.OutcomeCancelled {
		t.Fatalf("stopped status = %+v, %v", st, err)
	}
}

func TestProcessRuntimePreemptionSignalsCheckpointThenStops(t *testing.T) {
	dir := t.TempDir()
	r, err := NewProcessRuntime(dir)
	if err != nil {
		t.Fatal(err)
	}
	r.checkpointGrace = time.Second
	t.Setenv("GPU_BROKER_CHECKPOINT_DIR", "")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	s := processSpec("checkpoint-signal", exe, "-test.run=^TestProcessRuntimeCheckpointHelper$")
	if err := r.Start(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	checkpointMarker := filepath.Join(r.checkpointDir(s.JobID, s.AttemptID), "CHECKPOINT_COMPLETE")
	uploadMarker := r.uploadMarker(s.JobID, s.AttemptID)
	readyMarker := filepath.Join(r.checkpointDir(s.JobID, s.AttemptID), "READY")
	readyDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(readyDeadline) {
		if _, err := os.Stat(readyMarker); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(readyMarker); err != nil {
		t.Fatalf("checkpoint helper did not install its signal handler: %v", err)
	}
	uploaderDone := make(chan struct{})
	go func() {
		defer close(uploaderDone)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(checkpointMarker); err == nil {
				time.Sleep(150 * time.Millisecond) // stand in for object + manifest commit
				_ = os.WriteFile(uploadMarker, []byte("manifest committed"), 0o600)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	started := time.Now()
	if err := r.Stop(context.Background(), s.AttemptID, domain.OutcomePreempted); err != nil {
		t.Fatal(err)
	}
	<-uploaderDone
	if time.Since(started) < 150*time.Millisecond {
		t.Fatal("runtime stopped before the fake GCS manifest commit")
	}
	marker := filepath.Join(r.checkpointDir(s.JobID, s.AttemptID), "CHECKPOINT_COMPLETE")
	if b, err := os.ReadFile(marker); err != nil || string(b) != "saved" {
		t.Fatalf("checkpoint marker=%q, err=%v", b, err)
	}
	st, err := r.Poll(context.Background(), s.AttemptID)
	if err != nil || !st.Done || st.Outcome != domain.OutcomePreempted {
		t.Fatalf("preempted status = %+v, %v", st, err)
	}
}

func TestProcessRuntimePreemptionWithoutHandlerLeavesNoCheckpoint(t *testing.T) {
	r, err := NewProcessRuntime(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r.checkpointGrace = 100 * time.Millisecond
	s := processSpec("no-checkpoint", "/bin/sleep", "30")
	if err := r.Start(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(context.Background(), s.AttemptID, domain.OutcomePreempted); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(r.checkpointDir(s.JobID, s.AttemptID), "CHECKPOINT_COMPLETE")
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("workload without a checkpoint handler created a marker: %v", err)
	}
}

func TestProcessRuntimeBoundsCheckpointWaitWhenUploaderDoesNotCommit(t *testing.T) {
	r, err := NewProcessRuntime(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r.checkpointGrace = 150 * time.Millisecond
	t.Setenv("GPU_BROKER_CHECKPOINT_DIR", "")
	t.Setenv("GPU_BROKER_CHECKPOINT_DIR", "")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	s := processSpec("checkpoint-bounded", exe, "-test.run=^TestProcessRuntimeCheckpointHelper$")
	if err := r.Start(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(r.checkpointDir(s.JobID, s.AttemptID), "READY")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(ready); err != nil {
		t.Fatalf("checkpoint helper did not become ready: %v", err)
	}
	started := time.Now()
	if err := r.Stop(context.Background(), s.AttemptID, domain.OutcomePreempted); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Stop exceeded its checkpoint barrier bound: %s", elapsed)
	}
	if _, err := os.Stat(filepath.Join(r.checkpointDir(s.JobID, s.AttemptID), "CHECKPOINT_COMPLETE")); err != nil {
		t.Fatalf("workload did not write its local checkpoint marker: %v", err)
	}
	if _, err := os.Stat(r.uploadMarker(s.JobID, s.AttemptID)); !os.IsNotExist(err) {
		t.Fatalf("missing GCS commit marker was treated as committed: %v", err)
	}
}

// This test function becomes a child-process workload only when the marker
// environment variable is present; in the parent test process it is a no-op.
func TestProcessRuntimeCheckpointHelper(t *testing.T) {
	if os.Getenv("GPU_BROKER_CHECKPOINT_DIR") == "" {
		return
	}
	if os.Getenv("GPU_BROKER_JOB_ID") != "job-process-test" ||
		os.Getenv("GPU_BROKER_ATTEMPT") == "" ||
		os.Getenv("GPU_BROKER_CHECKPOINT_DEADLINE") != "20" ||
		os.Getenv("GPU_BROKER_RESUME_DIR") == "" ||
		os.Getenv("GPU_BROKER_GCS_UPLOAD_COMPLETE") == "" {
		os.Exit(6)
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	if err := os.WriteFile(filepath.Join(os.Getenv("GPU_BROKER_CHECKPOINT_DIR"), "READY"), []byte("ready"), 0o600); err != nil {
		os.Exit(5)
	}
	<-ch
	dir := os.Getenv("GPU_BROKER_CHECKPOINT_DIR")
	if dir == "" {
		os.Exit(3)
	}
	if err := os.WriteFile(filepath.Join(dir, "CHECKPOINT_COMPLETE"), []byte("saved"), 0o600); err != nil {
		os.Exit(4)
	}
	if os.Getenv("GPU_BROKER_ATTEMPT") == "checkpoint-bounded" {
		select {}
	}
	os.Exit(0)
}

func TestProcessRuntimeDeadlineTerminatesProcess(t *testing.T) {
	r, err := NewProcessRuntime(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := processSpec("deadline", "/bin/sleep", "30")
	s.MaxRuntime = 50 * time.Millisecond
	if err := r.Start(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	st, err := r.Poll(context.Background(), s.AttemptID)
	if err != nil || !st.Done || st.Outcome != domain.OutcomeTimedOut {
		t.Fatalf("deadline status = %+v, %v", st, err)
	}
}

func TestProcessRuntimeRejectsStalePIDWithoutSignalingIt(t *testing.T) {
	r, err := NewProcessRuntime(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := processSpec("stale-pid", "/bin/sleep", "1")
	m := processMetadata{Version: 1, Spec: s, SpecHash: specHash(s), PID: os.Getpid(), PGID: syscall.Getpgrp(),
		StartToken: "not-this-process", ArgvHash: argvHash(os.Args), StartedAt: time.Now(), Deadline: time.Now().Add(time.Second)}
	if err := r.save(m); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(context.Background(), s.AttemptID, domain.OutcomeCancelled); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(os.Getpid(), 0); err != nil {
		t.Fatalf("stale PID validation signaled the test process: %v", err)
	}
	st, err := r.Poll(context.Background(), s.AttemptID)
	if err != nil || !st.Done || st.Outcome != domain.OutcomeLost {
		t.Fatalf("stale PID status = %+v, %v", st, err)
	}
}

func TestProcessRuntimeBoundsArtifactReturnedToAgent(t *testing.T) {
	r, err := NewProcessRuntime(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := processSpec("artifact-cap", "/bin/sh", "-c", "head -c 5000000 /dev/zero > output.bin")
	if err := r.Start(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	st := pollProcessUntilDone(t, r, s.AttemptID)
	if len(st.Artifact) > processMaxArtifact {
		t.Fatalf("artifact size %d exceeds cap %d", len(st.Artifact), processMaxArtifact)
	}
}
