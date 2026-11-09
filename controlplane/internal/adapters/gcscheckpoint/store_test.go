package gcscheckpoint

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type testToken string

func (t testToken) Token(context.Context) (string, error) { return string(t), nil }

type fakeGCS struct {
	objects    map[string][]byte
	uploaded   []string
	failUpload bool
}

func (g *fakeGCS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer token" {
		http.Error(w, "unauthorized", 401)
		return
	}
	switch {
	case strings.HasPrefix(r.URL.Path, "/upload/storage/v1/b/"):
		if r.URL.Query().Get("ifGenerationMatch") != "0" {
			http.Error(w, "missing create-only condition", 400)
			return
		}
		name := r.URL.Query().Get("name")
		if g.failUpload {
			http.Error(w, "interrupted", 500)
			return
		}
		if _, exists := g.objects[name]; exists {
			http.Error(w, "exists", 412)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		g.objects[name] = body
		g.uploaded = append(g.uploaded, name)
		w.WriteHeader(200)
	case strings.HasPrefix(r.URL.Path, "/storage/v1/b/"):
		var names []string
		for name := range g.objects {
			if strings.HasPrefix(name, r.URL.Query().Get("prefix")) {
				names = append(names, name)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": func() []map[string]string {
			a := []map[string]string{}
			for _, n := range names {
				a = append(a, map[string]string{"name": n})
			}
			return a
		}()})
	case strings.HasPrefix(r.URL.Path, "/download/storage/v1/b/"):
		escaped := r.URL.EscapedPath()
		objectPart := strings.SplitN(escaped, "/o/", 2)
		if len(objectPart) != 2 {
			http.NotFound(w, r)
			return
		}
		name, _ := url.PathUnescape(objectPart[1])
		body, ok := g.objects[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(body)
	default:
		http.NotFound(w, r)
	}
}
func newTestClient(t *testing.T, g *fakeGCS) Client {
	t.Helper()
	s := httptest.NewServer(g)
	t.Cleanup(s.Close)
	return Client{HTTP: s.Client(), Tokens: testToken("token"), BaseURL: s.URL}
}
func fixture(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, "state.bin"), []byte("checkpoint-state"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "CHECKPOINT_COMPLETE"), []byte("7"), 0600); err != nil {
		t.Fatal(err)
	}
	return d
}
func TestPutLatestRestoreAndSignal(t *testing.T) {
	g := &fakeGCS{objects: map[string][]byte{}}
	c := newTestClient(t, g)
	src := fixture(t)
	state := t.TempDir()
	ctx := testContext("job-1", "attempt-id")
	marker := filepath.Join(state, "jobs", fmt.Sprintf("%x", sha256sum("job-1")), "barriers", fmt.Sprintf("%x", sha256sum("attempt-id")), "GCS_UPLOAD_COMPLETE")
	attempt := 1
	_, err := c.PutAndSignal(context.Background(), "bucket", "root", "job-1", "attempt-id", &attempt, 7, src, marker, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.uploaded) != 2 || !strings.HasSuffix(g.uploaded[0], "state.bin") || !strings.HasSuffix(g.uploaded[1], "MANIFEST.json") {
		t.Fatalf("expected data before manifest, got %v", g.uploaded)
	}
	if _, err = os.Stat(marker); err != nil {
		t.Fatalf("manifest committed but marker missing: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "restore")
	// Replacement attempts have a new AttemptID; source/workload/job are the
	// stable compatibility context, while the manifest retains the old ID.
	wantContext := map[string]string{"job_id": "job-1", "source_revision": ctx["source_revision"], "workload_sha256": ctx["workload_sha256"]}
	recovery := Recovery{Client: c, Bucket: "bucket", Prefix: "root", JobID: "job-1", Destination: dest, ExpectedContext: wantContext, Required: true}
	if err = recovery.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = recovery.Prepare(context.Background()); err != nil {
		t.Fatalf("idempotent recovery: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "state.bin"))
	if err != nil || string(got) != "checkpoint-state" {
		t.Fatalf("restore = %q, %v", got, err)
	}
	if err = os.WriteFile(filepath.Join(dest, "state.bin"), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = recovery.Prepare(context.Background()); err == nil {
		t.Fatal("accepted tampered local recovery on restart")
	}
}

func TestRecoveryFailsClosedWhenRequiredManifestIsMissing(t *testing.T) {
	c := newTestClient(t, &fakeGCS{objects: map[string][]byte{}})
	recovery := Recovery{Client: c, Bucket: "bucket", Prefix: "root", JobID: "job-1", Destination: filepath.Join(t.TempDir(), "resume"), ExpectedContext: map[string]string{"job_id": "job-1"}, Required: true}
	if err := recovery.Prepare(context.Background()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("required recovery = %v, want missing-manifest error", err)
	}
}
func sha256sum(s string) []byte { h := sha256.Sum256([]byte(s)); return h[:] }
func TestIncompleteUploadHasNoRestorableManifest(t *testing.T) {
	g := &fakeGCS{objects: map[string][]byte{}, failUpload: true}
	c := newTestClient(t, g)
	if _, err := c.Put(context.Background(), "bucket", "root", "job-1", "attempt-id", nil, 7, fixture(t), testContext("job-1", "attempt-id")); err == nil {
		t.Fatal("expected upload failure")
	}
	for _, name := range g.uploaded {
		if strings.HasSuffix(name, "MANIFEST.json") {
			t.Fatal("failed upload committed manifest")
		}
	}
	if _, err := c.Latest(context.Background(), "bucket", "root", "job-1"); err == nil {
		t.Fatal("incomplete checkpoint must not be restorable")
	}
}

func TestUnknownAttemptOrdinalIsOmittedAndAttemptIDIsAuthoritative(t *testing.T) {
	g := &fakeGCS{objects: map[string][]byte{}}
	c := newTestClient(t, g)
	ref, err := c.Put(context.Background(), "bucket", "root", "job-1", "attempt-unique", nil, 7, fixture(t), testContext("job-1", "attempt-unique"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(ref.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"attempt":`) {
		t.Fatalf("unknown ordinal must be omitted: %s", raw)
	}
	if ref.Manifest.AttemptID != "attempt-unique" {
		t.Fatalf("authoritative attempt ID missing: %+v", ref.Manifest)
	}
}
func TestRestoreRejectsContextLengthHashAndNonemptyDestination(t *testing.T) {
	g := &fakeGCS{objects: map[string][]byte{}}
	c := newTestClient(t, g)
	ref, err := c.Put(context.Background(), "bucket", "root", "job-1", "attempt-id", nil, 7, fixture(t), testContext("job-1", "attempt-id"))
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Restore(context.Background(), "bucket", ref, t.TempDir(), testContext("job-1", "other")); err == nil {
		t.Fatal("accepted mismatched context")
	}
	bad := t.TempDir()
	if err = os.WriteFile(filepath.Join(bad, "occupied"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = c.Restore(context.Background(), "bucket", ref, bad, testContext("job-1", "attempt-id")); err == nil {
		t.Fatal("accepted nonempty destination")
	}
	ref.Manifest.Files[0].Bytes++
	if err = c.Restore(context.Background(), "bucket", ref, t.TempDir(), testContext("job-1", "attempt-id")); err == nil {
		t.Fatal("accepted altered length")
	}
	ref.Manifest.Files[0].Bytes--
	ref.Manifest.Files[0].SHA256 = strings.Repeat("0", 64)
	if err = c.Restore(context.Background(), "bucket", ref, t.TempDir(), testContext("job-1", "attempt-id")); err == nil {
		t.Fatal("accepted altered hash")
	}
}
func TestLatestMissingManifest(t *testing.T) {
	c := newTestClient(t, &fakeGCS{objects: map[string][]byte{"root/job-1/attempt-1/state.bin": []byte("partial")}})
	if _, err := c.Latest(context.Background(), "bucket", "root", "job-1"); err == nil {
		t.Fatal("expected missing manifest")
	}
}

func TestRestoreRejectsSymlinkDestination(t *testing.T) {
	g := &fakeGCS{objects: map[string][]byte{}}
	c := newTestClient(t, g)
	ref, err := c.Put(context.Background(), "bucket", "root", "job-1", "attempt-id", nil, 7, fixture(t), testContext("job-1", "attempt-id"))
	if err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	link := filepath.Join(t.TempDir(), "restore-link")
	if err = os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err = c.Restore(context.Background(), "bucket", ref, link, testContext("job-1", "attempt-id")); err == nil {
		t.Fatal("accepted symlink restore destination")
	}
}

func testContext(job, attempt string) map[string]string {
	return map[string]string{"job_id": job, "attempt_id": attempt, "source_revision": "test-revision", "workload_sha256": strings.Repeat("a", 64)}
}
