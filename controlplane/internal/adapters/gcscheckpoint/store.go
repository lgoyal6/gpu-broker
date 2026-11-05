package gcscheckpoint

// Package gcscheckpoint exports create-only checkpoint objects and restores
// only a manifest-committed, hash-verified checkpoint. Credentials are read
// from the attached GCE service account metadata server.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	metadataTokenURL   = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
	maxManifestBytes   = 1 << 20
	maxCheckpointBytes = 512 << 20
)

type TokenSource interface {
	Token(context.Context) (string, error)
}

type MetadataTokenSource struct{ Client *http.Client }

func (s MetadataTokenSource) Token(ctx context.Context) (string, error) {
	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataTokenURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("read GCE service-account token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata token endpoint returned HTTP %d", resp.StatusCode)
	}
	var payload struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<10)).Decode(&payload); err != nil {
		return "", err
	}
	if payload.AccessToken == "" {
		return "", fmt.Errorf("metadata token response did not contain an access token")
	}
	return payload.AccessToken, nil
}

type Client struct {
	HTTP    *http.Client
	Tokens  TokenSource
	BaseURL string
}

type File struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}
type Manifest struct {
	Schema    string            `json:"schema"`
	JobID     string            `json:"job_id"`
	AttemptID string            `json:"attempt_id"`
	Attempt   *int              `json:"attempt,omitempty"`
	Step      int               `json:"step"`
	WrittenAt string            `json:"written_at"`
	Files     []File            `json:"files"`
	Bytes     int64             `json:"bytes"`
	Context   map[string]string `json:"context"`
}
type Reference struct {
	Object   string
	Manifest Manifest
}

// Recovery restores a required committed checkpoint before the worker can
// register. When Required is false, no lookup is done, which is correct for a
// first attempt and avoids silently adopting stale job state.
type Recovery struct {
	Client          Client
	Bucket          string
	Prefix          string
	JobID           string
	Destination     string
	ExpectedContext map[string]string
	Required        bool
}

func (r Recovery) Prepare(ctx context.Context) error {
	if !r.Required {
		return nil
	}
	if r.Bucket == "" || r.JobID == "" || r.Destination == "" || r.ExpectedContext["job_id"] != r.JobID {
		return fmt.Errorf("required checkpoint recovery is incompletely configured")
	}
	ref, err := r.Client.Latest(ctx, r.Bucket, r.Prefix, r.JobID)
	if err != nil {
		return err
	}
	manifestID, err := manifestIdentity(ref.Manifest)
	if err != nil {
		return err
	}
	marker := r.Destination + ".GCS_RESTORE_COMPLETE"
	if st, statErr := os.Lstat(r.Destination); statErr == nil {
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("restore destination is not a real directory")
		}
		if raw, readErr := os.ReadFile(marker); readErr == nil && strings.TrimSuffix(string(raw), "\n") == ref.Object+"\n"+manifestID {
			return verifyRestored(ref.Manifest, r.Destination)
		} else if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
			return readErr
		}
		entries, readErr := os.ReadDir(r.Destination)
		if readErr != nil {
			return readErr
		}
		if len(entries) != 0 {
			return fmt.Errorf("unverified data already exists in restore destination")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	if err := r.Client.Restore(ctx, r.Bucket, ref, r.Destination, r.ExpectedContext); err != nil {
		return err
	}
	if err := writeRestoreMarker(marker, ref.Object+"\n"+manifestID+"\n"); err != nil {
		_ = os.RemoveAll(r.Destination)
		return err
	}
	return nil
}

func manifestIdentity(m Manifest) (string, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
func writeRestoreMarker(path, contents string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".gcs-restore-marker-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := io.WriteString(tmp, contents); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
func verifyRestored(m Manifest, directory string) error {
	root, err := filepath.Abs(directory)
	if err != nil {
		return err
	}
	var total int64
	seen := make(map[string]struct{}, len(m.Files))
	for _, file := range m.Files {
		if !safeObjectSuffix(file.Name) || !validSHA(file.SHA256) || file.Bytes < 0 {
			return fmt.Errorf("invalid manifest file entry")
		}
		if _, ok := seen[file.Name]; ok {
			return fmt.Errorf("duplicate manifest file entry")
		}
		seen[file.Name] = struct{}{}
		path := filepath.Join(root, filepath.FromSlash(file.Name))
		if !withinRoot(root, path) {
			return fmt.Errorf("unsafe restored path")
		}
		st, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !st.Mode().IsRegular() || st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("restored artifact is not a regular file")
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		h := sha256.New()
		n, readErr := io.Copy(h, f)
		closeErr := f.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if n != file.Bytes || hex.EncodeToString(h.Sum(nil)) != file.SHA256 {
			return fmt.Errorf("restored artifact integrity mismatch: %s", file.Name)
		}
		total += n
	}
	if total != m.Bytes {
		return fmt.Errorf("restored manifest byte count mismatch")
	}
	return nil
}

func (c Client) endpoint() string {
	if c.BaseURL != "" {
		return strings.TrimRight(c.BaseURL, "/")
	}
	return "https://storage.googleapis.com"
}
func (c Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 30 * time.Second}
}
func (c Client) token(ctx context.Context) (string, error) {
	if c.Tokens == nil {
		return "", fmt.Errorf("GCE metadata token source is required")
	}
	return c.Tokens.Token(ctx)
}

func (c Client) Put(ctx context.Context, bucket, prefix, jobID, attemptID string, attempt *int, step int, directory string, contextData map[string]string) (Reference, error) {
	var empty Reference
	if (attempt != nil && *attempt < 1) || step < 0 || !safeSegment(jobID) || attemptID == "" || !safeBucket(bucket) {
		return empty, fmt.Errorf("invalid checkpoint identity")
	}
	if contextData["job_id"] != jobID || contextData["attempt_id"] != attemptID || contextData["source_revision"] == "" || !validSHA(contextData["workload_sha256"]) {
		return empty, fmt.Errorf("checkpoint context must bind job, attempt, source, and workload")
	}
	root, err := filepath.Abs(directory)
	if err != nil {
		return empty, err
	}
	marker, err := os.ReadFile(filepath.Join(root, "CHECKPOINT_COMPLETE"))
	if err != nil {
		return empty, fmt.Errorf("checkpoint completion marker missing: %w", err)
	}
	if strings.TrimSpace(string(marker)) != strconv.Itoa(step) {
		return empty, fmt.Errorf("checkpoint marker step does not match upload step")
	}
	var paths []string
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, e := filepath.Rel(root, path)
		if e != nil {
			return e
		}
		if filepath.ToSlash(rel) == "CHECKPOINT_COMPLETE" {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("checkpoint contains non-regular file %s", rel)
		}
		paths = append(paths, rel)
		return nil
	})
	if err != nil {
		return empty, err
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return empty, fmt.Errorf("checkpoint contains no files")
	}
	jobRoot, err := checkpointJobPrefix(prefix, jobID)
	if err != nil {
		return empty, err
	}
	attemptHash := sha256.Sum256([]byte(attemptID))
	base := jobRoot + "attempt-" + hex.EncodeToString(attemptHash[:]) + fmt.Sprintf("/step-%012d", step)
	manifest := Manifest{Schema: "gpu-broker.gcs-checkpoint/v1", JobID: jobID, AttemptID: attemptID, Attempt: attempt, Step: step, WrittenAt: time.Now().UTC().Format(time.RFC3339Nano), Context: contextData}
	for _, rel := range paths {
		clean := filepath.ToSlash(rel)
		if !safeObjectSuffix(clean) {
			return empty, fmt.Errorf("unsafe checkpoint path %q", clean)
		}
		path := filepath.Join(root, rel)
		info, e := os.Lstat(path)
		if e != nil {
			return empty, e
		}
		if !info.Mode().IsRegular() || info.Size()+manifest.Bytes > maxCheckpointBytes {
			return empty, fmt.Errorf("checkpoint exceeds %d-byte limit", maxCheckpointBytes)
		}
		f, e := os.Open(path)
		if e != nil {
			return empty, e
		}
		h := sha256.New()
		n, e := io.Copy(h, io.LimitReader(f, maxCheckpointBytes+1))
		closeErr := f.Close()
		if e != nil {
			return empty, e
		}
		if closeErr != nil {
			return empty, closeErr
		}
		if n != info.Size() || n+manifest.Bytes > maxCheckpointBytes {
			return empty, fmt.Errorf("checkpoint file changed or exceeds size limit: %s", clean)
		}
		name := base + "/" + clean
		f, e = os.Open(path)
		if e != nil {
			return empty, e
		}
		e = c.uploadCreateOnlyReader(ctx, bucket, name, f, n)
		_ = f.Close() // net/http may close the request body after transmission.
		if e != nil {
			return empty, fmt.Errorf("upload checkpoint object %s: %w", clean, e)
		}
		manifest.Files = append(manifest.Files, File{Name: clean, Bytes: n, SHA256: hex.EncodeToString(h.Sum(nil))})
		manifest.Bytes += n
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return empty, err
	}
	if len(raw) > maxManifestBytes {
		return empty, fmt.Errorf("manifest exceeds size limit")
	}
	manifestKey := base + "/MANIFEST.json"
	if err := c.uploadCreateOnly(ctx, bucket, manifestKey, raw); err != nil {
		return empty, fmt.Errorf("commit checkpoint manifest last: %w", err)
	}
	return Reference{Object: manifestKey, Manifest: manifest}, nil
}

func (c Client) Latest(ctx context.Context, bucket, prefix, jobID string) (Reference, error) {
	var best Reference
	if !safeBucket(bucket) || !safeSegment(jobID) {
		return best, fmt.Errorf("invalid checkpoint lookup identity")
	}
	token, err := c.token(ctx)
	if err != nil {
		return best, err
	}
	jobRoot, err := checkpointJobPrefix(prefix, jobID)
	if err != nil {
		return best, err
	}
	query := url.Values{"prefix": {jobRoot}, "maxResults": {"1000"}, "fields": {"items(name),nextPageToken"}}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint()+"/storage/v1/b/"+url.PathEscape(bucket)+"/o?"+query.Encode(), nil)
		if err != nil {
			return best, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := c.httpClient().Do(req)
		if err != nil {
			return best, err
		}
		var page struct {
			Items []struct {
				Name string `json:"name"`
			} `json:"items"`
			Next string `json:"nextPageToken"`
		}
		decErr := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&page)
		closeErr := resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return best, fmt.Errorf("list checkpoint objects returned HTTP %d", resp.StatusCode)
		}
		if decErr != nil {
			return best, decErr
		}
		if closeErr != nil {
			return best, closeErr
		}
		for _, item := range page.Items {
			if !strings.HasSuffix(item.Name, "/MANIFEST.json") {
				continue
			}
			raw, e := c.download(ctx, bucket, item.Name, token, maxManifestBytes)
			if e != nil {
				return best, e
			}
			var m Manifest
			if e = json.Unmarshal(raw, &m); e != nil {
				return best, e
			}
			if m.Schema != "gpu-broker.gcs-checkpoint/v1" || m.JobID != jobID || m.AttemptID == "" || m.Context["job_id"] != jobID || m.Context["attempt_id"] != m.AttemptID || m.Step < 0 {
				continue
			}
			attemptHash := sha256.Sum256([]byte(m.AttemptID))
			if !strings.HasPrefix(item.Name, jobRoot+"attempt-"+hex.EncodeToString(attemptHash[:])+"/") {
				continue
			}
			if best.Object == "" || m.Step > best.Manifest.Step || (m.Step == best.Manifest.Step && m.WrittenAt > best.Manifest.WrittenAt) {
				best = Reference{Object: item.Name, Manifest: m}
			}
		}
		if page.Next == "" {
			break
		}
		query.Set("pageToken", page.Next)
	}
	if best.Object == "" {
		return best, os.ErrNotExist
	}
	return best, nil
}

// RestoreLatest fetches and verifies the newest complete manifest matching a
// job and expected workload context, then restores its contents atomically
// into an empty destination tree.
func (c Client) RestoreLatest(ctx context.Context, bucket, prefix, jobID, destination string, expectedContext map[string]string) (Reference, error) {
	ref, err := c.Latest(ctx, bucket, prefix, jobID)
	if err != nil {
		return Reference{}, err
	}
	if err := c.Restore(ctx, bucket, ref, destination, expectedContext); err != nil {
		return Reference{}, err
	}
	return ref, nil
}

func (c Client) Restore(ctx context.Context, bucket string, ref Reference, destination string, expectedContext map[string]string) error {
	if !safeBucket(bucket) || !strings.HasSuffix(ref.Object, "/MANIFEST.json") || ref.Manifest.Schema != "gpu-broker.gcs-checkpoint/v1" {
		return fmt.Errorf("invalid checkpoint manifest reference")
	}
	if ref.Manifest.AttemptID == "" || ref.Manifest.Context["job_id"] != ref.Manifest.JobID || ref.Manifest.Context["attempt_id"] != ref.Manifest.AttemptID || !equalContext(ref.Manifest.Context, expectedContext) {
		return fmt.Errorf("checkpoint workload context mismatch")
	}
	attemptHash := sha256.Sum256([]byte(ref.Manifest.AttemptID))
	if !strings.Contains(ref.Object, "/"+ref.Manifest.JobID+"/attempt-"+hex.EncodeToString(attemptHash[:])+"/") {
		return fmt.Errorf("checkpoint object path does not match manifest attempt")
	}
	token, err := c.token(ctx)
	if err != nil {
		return err
	}
	root, err := filepath.Abs(destination)
	if err != nil {
		return err
	}
	if st, statErr := os.Lstat(root); statErr == nil {
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("restore destination is not a real directory")
		}
		entries, readErr := os.ReadDir(root)
		if readErr != nil {
			return readErr
		}
		if len(entries) != 0 {
			return fmt.Errorf("restore destination must be empty")
		}
	} else if errors.Is(statErr, os.ErrNotExist) {
	} else {
		return statErr
	}
	if ref.Manifest.Bytes < 0 || len(ref.Manifest.Files) == 0 || len(ref.Manifest.Files) > 10000 {
		return fmt.Errorf("invalid checkpoint manifest limits")
	}
	parent := filepath.Dir(root)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(parent, ".gcs-checkpoint-restore-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	var total int64
	seen := make(map[string]struct{}, len(ref.Manifest.Files))
	for _, file := range ref.Manifest.Files {
		if !safeObjectSuffix(file.Name) || file.Bytes < 0 || !validSHA(file.SHA256) {
			return fmt.Errorf("unsafe manifest file entry")
		}
		if _, ok := seen[file.Name]; ok {
			return fmt.Errorf("duplicate manifest file entry")
		}
		seen[file.Name] = struct{}{}
		total += file.Bytes
		if total > maxCheckpointBytes {
			return fmt.Errorf("checkpoint exceeds restore size limit")
		}
		name := strings.TrimSuffix(ref.Object, "MANIFEST.json") + file.Name
		body, e := c.download(ctx, bucket, name, token, file.Bytes+1)
		if e != nil {
			return e
		}
		if int64(len(body)) != file.Bytes {
			return fmt.Errorf("checkpoint file length mismatch: %s", file.Name)
		}
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != file.SHA256 {
			return fmt.Errorf("checkpoint file hash mismatch: %s", file.Name)
		}
		path := filepath.Join(stage, filepath.FromSlash(file.Name))
		if !withinRoot(stage, path) {
			return fmt.Errorf("checkpoint path escapes destination")
		}
		if err = rejectSymlinkParents(stage, filepath.Dir(path)); err != nil {
			return err
		}
		if e = os.MkdirAll(filepath.Dir(path), 0o700); e != nil {
			return e
		}
		if e = os.WriteFile(path, body, 0o600); e != nil {
			return e
		}
	}
	if total != ref.Manifest.Bytes {
		return fmt.Errorf("manifest byte count mismatch")
	}
	if err := os.Rename(stage, root); err != nil {
		return fmt.Errorf("publish verified checkpoint restore: %w", err)
	}
	return nil
}

func (c Client) uploadCreateOnly(ctx context.Context, bucket, name string, body []byte) error {
	return c.uploadCreateOnlyReader(ctx, bucket, name, strings.NewReader(string(body)), int64(len(body)))
}
func (c Client) uploadCreateOnlyReader(ctx context.Context, bucket, name string, body io.Reader, size int64) error {
	token, err := c.token(ctx)
	if err != nil {
		return err
	}
	query := url.Values{"uploadType": {"media"}, "name": {name}, "ifGenerationMatch": {"0"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint()+"/upload/storage/v1/b/"+url.PathEscape(bucket)+"/o?"+query.Encode(), body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = size
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("create-only upload returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// PutAndSignal publishes the attempt-bound completion marker only after all
// checkpoint objects and the final manifest have been committed.
func (c Client) PutAndSignal(ctx context.Context, bucket, prefix, jobID string, attemptID string, attempt *int, step int, directory, marker string, contextData map[string]string) (Reference, error) {
	if attemptID == "" {
		return Reference{}, fmt.Errorf("attempt ID is required")
	}
	jobHash := sha256.Sum256([]byte(jobID))
	attemptHash := sha256.Sum256([]byte(attemptID))
	expectedSuffix := filepath.ToSlash(filepath.Join("jobs", hex.EncodeToString(jobHash[:]), "barriers", hex.EncodeToString(attemptHash[:]), "GCS_UPLOAD_COMPLETE"))
	if !strings.HasSuffix(filepath.ToSlash(marker), expectedSuffix) {
		return Reference{}, fmt.Errorf("upload barrier does not match job and attempt")
	}
	if err := os.MkdirAll(filepath.Dir(marker), 0o700); err != nil {
		return Reference{}, err
	}
	if _, err := os.Lstat(marker); err == nil {
		return Reference{}, fmt.Errorf("upload barrier already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return Reference{}, err
	}
	ref, err := c.Put(ctx, bucket, prefix, jobID, attemptID, attempt, step, directory, contextData)
	if err != nil {
		return Reference{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(marker), ".GCS_UPLOAD_COMPLETE-*")
	if err != nil {
		return Reference{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return Reference{}, err
	}
	if _, err := io.WriteString(tmp, ref.Object+"\n"); err != nil {
		_ = tmp.Close()
		return Reference{}, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return Reference{}, err
	}
	if err := tmp.Close(); err != nil {
		return Reference{}, err
	}
	if err := os.Link(tmpName, marker); err != nil {
		return Reference{}, err
	}
	return ref, nil
}
func (c Client) download(ctx context.Context, bucket, name, token string, limit int64) ([]byte, error) {
	if limit < 0 || limit > maxCheckpointBytes+1 {
		return nil, fmt.Errorf("invalid download limit")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint()+"/download/storage/v1/b/"+url.PathEscape(bucket)+"/o/"+url.PathEscape(name)+"?alt=media", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download object returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("download exceeds declared limit")
	}
	return body, nil
}
func safeSegment(s string) bool {
	return s != "" && s != "." && s != ".." && !strings.ContainsAny(s, "/\\")
}
func safeBucket(s string) bool { return safeSegment(s) && len(s) <= 222 }
func safeObjectSuffix(s string) bool {
	if s == "" || strings.HasPrefix(s, "/") || strings.Contains(s, "\\") {
		return false
	}
	for _, part := range strings.Split(s, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}
func equalContext(a, b map[string]string) bool {
	for k, v := range b {
		if av, ok := a[k]; !ok || av != v {
			return false
		}
	}
	return true
}
func validSHA(s string) bool {
	if len(s) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
func checkpointJobPrefix(prefix, jobID string) (string, error) {
	p := strings.Trim(prefix, "/")
	if p != "" && !safeObjectSuffix(p) {
		return "", fmt.Errorf("unsafe checkpoint prefix")
	}
	if p == "" {
		return jobID + "/", nil
	}
	return p + "/" + jobID + "/", nil
}
func withinRoot(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}
func rejectSymlinkParents(root, parent string) error {
	rel, err := filepath.Rel(root, parent)
	if err != nil || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("checkpoint path escapes destination")
	}
	cur := root
	if rel == "." {
		return nil
	}
	for _, part := range strings.Split(rel, string(os.PathSeparator)) {
		cur = filepath.Join(cur, part)
		st, e := os.Lstat(cur)
		if e == nil && st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in restore destination")
		}
		if e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
	}
	return nil
}
