// gpubroker-checkpoint-upload commits a workload checkpoint to GCS. It uses
// only the attached VM service account metadata token and runtime-provided
// paths; flags contain no credentials.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/gcscheckpoint"
)

func main() {
	err := run(os.Args[1:], os.Getenv, gcscheckpoint.Client{Tokens: gcscheckpoint.MetadataTokenSource{}})
	if err != nil {
		fatalf("%v", err)
	}
}

func run(args []string, getenv func(string) string, client gcscheckpoint.Client) error {
	var bucket, prefix, sourceRevision, workloadSHA256 string
	var attempt int
	flags := flag.NewFlagSet("gpubroker-checkpoint-upload", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.StringVar(&bucket, "bucket", "", "GCS checkpoint bucket")
	flags.StringVar(&prefix, "prefix", "checkpoints", "GCS object prefix")
	flags.StringVar(&sourceRevision, "source-revision", "", "pinned source revision")
	flags.StringVar(&workloadSHA256, "workload-sha256", "", "SHA-256 of the running workload file")
	flags.IntVar(&attempt, "attempt-number", 0, "optional scheduler-provided attempt ordinal; omitted if unknown")
	if err := flags.Parse(args); err != nil {
		return err
	}
	for _, key := range []string{"GPU_BROKER_JOB_ID", "GPUBROKER_ATTEMPT_ID", "GPU_BROKER_CHECKPOINT_DIR", "GPU_BROKER_GCS_UPLOAD_COMPLETE"} {
		if getenv(key) == "" {
			return fmt.Errorf("runtime did not provide %s", key)
		}
	}
	if bucket == "" || sourceRevision == "" || !validSHA256(workloadSHA256) || attempt < 0 {
		return fmt.Errorf("bucket, pinned source revision, workload SHA-256, and nonnegative optional attempt number are required")
	}
	workloadContext := map[string]string{
		"job_id": getenv("GPU_BROKER_JOB_ID"), "attempt_id": getenv("GPUBROKER_ATTEMPT_ID"),
		"source_revision": sourceRevision, "workload_sha256": workloadSHA256,
	}
	marker := filepath.Join(getenv("GPU_BROKER_CHECKPOINT_DIR"), "CHECKPOINT_COMPLETE")
	stepBytes, err := os.ReadFile(marker)
	if err != nil {
		return fmt.Errorf("checkpoint is not complete: %w", err)
	}
	step, err := strconv.Atoi(strings.TrimSpace(string(stepBytes)))
	if err != nil || step < 0 {
		return fmt.Errorf("checkpoint marker has invalid step")
	}
	var attemptNumber *int
	if attempt > 0 {
		attemptNumber = &attempt
	}
	_, err = client.PutAndSignal(context.Background(), bucket, prefix, getenv("GPU_BROKER_JOB_ID"),
		getenv("GPUBROKER_ATTEMPT_ID"), attemptNumber, step, getenv("GPU_BROKER_CHECKPOINT_DIR"),
		getenv("GPU_BROKER_GCS_UPLOAD_COMPLETE"), workloadContext)
	if err != nil {
		return fmt.Errorf("checkpoint upload was not committed: %w", err)
	}
	return nil
}

func validSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
func sha256Text(s string) string        { sum := sha256.Sum256([]byte(s)); return hex.EncodeToString(sum[:]) }
func fatalf(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...); os.Exit(1) }
