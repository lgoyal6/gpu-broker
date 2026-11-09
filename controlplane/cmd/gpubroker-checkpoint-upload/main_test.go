package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/gcscheckpoint"
)

type fixedToken string

func (t fixedToken) Token(context.Context) (string, error) { return string(t), nil }

func TestCallerCommitsAndSignalsAgainstFakeGCS(t *testing.T) {
	objects := map[string][]byte{}
	var uploaded []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fake" {
			http.Error(w, "unauthorized", 401)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/upload/storage/v1/") {
			if r.URL.Query().Get("ifGenerationMatch") != "0" {
				http.Error(w, "not create-only", 400)
				return
			}
			name := r.URL.Query().Get("name")
			b, _ := io.ReadAll(r.Body)
			objects[name] = b
			uploaded = append(uploaded, name)
			w.WriteHeader(200)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	checkpoint := t.TempDir()
	if err := os.WriteFile(filepath.Join(checkpoint, "model.bin"), []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkpoint, "CHECKPOINT_COMPLETE"), []byte("12\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	job, attemptID := "job-42", "attempt-abc"
	jobHash, attemptHash := sha256Text(job), sha256Text(attemptID)
	marker := filepath.Join(t.TempDir(), "jobs", jobHash, "barriers", attemptHash, "GCS_UPLOAD_COMPLETE")
	env := map[string]string{"GPU_BROKER_JOB_ID": job, "GPUBROKER_ATTEMPT_ID": attemptID, "GPU_BROKER_CHECKPOINT_DIR": checkpoint, "GPU_BROKER_GCS_UPLOAD_COMPLETE": marker}
	getenv := func(k string) string { return env[k] }
	client := gcscheckpoint.Client{HTTP: server.Client(), Tokens: fixedToken("fake"), BaseURL: server.URL}
	args := []string{"--bucket=test-bucket", "--prefix=run", "--attempt-number=2", "--source-revision=base-revision", "--workload-sha256=" + strings.Repeat("a", 64)}
	if err := run(args, getenv, client); err != nil {
		t.Fatal(err)
	}
	if len(uploaded) != 2 || !strings.HasSuffix(uploaded[0], "model.bin") || !strings.HasSuffix(uploaded[1], "MANIFEST.json") {
		t.Fatalf("uploads not data-then-manifest: %v", uploaded)
	}
	var manifest gcscheckpoint.Manifest
	if err := json.Unmarshal(objects[uploaded[1]], &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.JobID != job || manifest.Attempt == nil || *manifest.Attempt != 2 || manifest.AttemptID != attemptID || manifest.Step != 12 || manifest.Context["attempt_id"] != attemptID || manifest.Context["source_revision"] != "base-revision" {
		t.Fatalf("manifest did not bind caller context: %+v", manifest)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("caller did not publish barrier after manifest: %v", err)
	}
}
