package main

import (
	"strings"
	"testing"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/gcscheckpoint"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/agent"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

func TestCheckpointRecoveryRequiresPinnedProcessContext(t *testing.T) {
	rt, err := agent.NewProcessRuntime(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	workload := strings.Repeat("a", 64)
	hook, err := checkpointRecovery(rt, true, "bucket", "track03", "job-42", "revision-7", workload)
	if err != nil {
		t.Fatal(err)
	}
	recovery, ok := hook.(gcscheckpoint.Recovery)
	if !ok {
		t.Fatalf("hook type = %T", hook)
	}
	if recovery.Destination != rt.ResumeDirectory(domain.JobID("job-42")) || !recovery.Required || recovery.ExpectedContext["workload_sha256"] != workload {
		t.Fatalf("recovery config = %+v", recovery)
	}
	if _, err = checkpointRecovery(rt, true, "", "track03", "job-42", "revision-7", workload); err == nil {
		t.Fatal("accepted missing bucket")
	}
	if _, err = checkpointRecovery(rt, true, "bucket", "track03", "job-42", "revision-7", strings.Repeat("z", 64)); err == nil {
		t.Fatal("accepted non-hex workload digest")
	}
	if _, err = checkpointRecovery(rt, false, "", "", "", "", ""); err != nil {
		t.Fatalf("initial attempt should not require restore: %v", err)
	}
	if strings.TrimSpace(rt.ResumeDirectory(domain.JobID("job-42"))) == "" {
		t.Fatal("empty resume path")
	}
}
