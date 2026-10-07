package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
)

// The model implements the documented conditional-write boundary. The real
// SDK and MinIO tests separately verify wire compatibility and authentication.
type modelS3 struct {
	mu                   sync.Mutex
	objects              map[string][]byte
	failManifest         bool
	loseManifestResponse bool
}

func newModelS3() *modelS3 { return &modelS3{objects: map[string][]byte{}} }

func (m *modelS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, ok := m.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey"}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(append([]byte(nil), raw...))),
		ETag: aws.String(hashBytes(raw))}, nil
}

func (m *modelS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	raw, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := aws.ToString(in.Key)
	old, exists := m.objects[key]
	if (in.IfNoneMatch != nil && exists) || (in.IfMatch != nil && (!exists || aws.ToString(in.IfMatch) != hashBytes(old))) {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed"}
	}
	if strings.HasSuffix(key, "/manifest.json") && m.failManifest {
		m.failManifest = false
		return nil, errors.New("manifest storage unavailable")
	}
	m.objects[key] = raw
	if strings.HasSuffix(key, "/manifest.json") && m.loseManifestResponse {
		m.loseManifestResponse = false
		return nil, errors.New("committed response lost")
	}
	return &s3.PutObjectOutput{ETag: aws.String(hashBytes(raw))}, nil
}

func makeS3(t *testing.T, client S3Client) *S3 {
	t.Helper()
	store, err := NewS3(client, "private-artifacts", "gpub")
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestS3ResumeRestartReadAndSeal(t *testing.T) {
	client := newModelS3()
	s3Contract(t, func() *S3 { return makeS3(t, client) })
}

// Reused against the actual S3-compatible server, with fresh SDK clients.
func s3Contract(t *testing.T, factory func() *S3) {
	t.Helper()
	ctx := context.Background()
	key := "tenant/job/attempt/" + t.Name()
	store := factory()
	if size, err := store.Size(ctx, key); err != nil || size != 0 {
		t.Fatalf("missing size: %d %v", size, err)
	}
	if _, err := store.Digest(ctx, key); !errors.Is(err, application.ErrNotFound) {
		t.Fatal(err)
	}
	if size, err := store.Append(ctx, key, 0, []byte("first")); err != nil || size != 5 {
		t.Fatalf("first: %d %v", size, err)
	}
	restarted := factory()
	if size, err := restarted.Append(ctx, key, 5, []byte("-second")); err != nil || size != 12 {
		t.Fatalf("resume: %d %v", size, err)
	}
	var mismatch *application.OffsetMismatchError
	if size, err := store.Append(ctx, key, 5, []byte("stale")); size != 12 || !errors.As(err, &mismatch) || mismatch.Committed != 12 {
		t.Fatalf("stale: %d %v", size, err)
	}
	if raw, err := store.Read(ctx, key, 3, 6); err != nil || string(raw) != "st-sec" {
		t.Fatalf("range: %q %v", raw, err)
	}
	if raw, err := store.Read(ctx, key, 100, 4); err != nil || len(raw) != 0 {
		t.Fatalf("past end: %q %v", raw, err)
	}
	digest := hashBytes([]byte("first-second"))
	if got, err := store.Digest(ctx, key); err != nil || got != digest {
		t.Fatalf("digest: %s %v", got, err)
	}
	if _, err := store.Seal(ctx, key, hashBytes([]byte("wrong"))); !errors.Is(err, application.ErrConflict) {
		t.Fatal(err)
	}
	if size, err := store.Seal(ctx, key, digest); err != nil || size != 12 {
		t.Fatalf("seal: %d %v", size, err)
	}
	if size, err := factory().Seal(ctx, key, digest); err != nil || size != 12 {
		t.Fatalf("repeat seal: %d %v", size, err)
	}
	if _, err := factory().Append(ctx, key, 12, []byte("changed")); !errors.Is(err, application.ErrConflict) {
		t.Fatal(err)
	}
}

func TestS3CompetingReplicasCommitOnlyOneManifest(t *testing.T) {
	client := newModelS3()
	s3Race(t, func() *S3 { return makeS3(t, client) })
}

func s3Race(t *testing.T, factory func() *S3) {
	t.Helper()
	start := make(chan struct{})
	results := make(chan error, 2)
	ctx := context.Background()
	key := "tenant/job/attempt/" + t.Name()
	for _, store := range []*S3{factory(), factory()} {
		go func() { <-start; _, err := store.Append(ctx, key, 0, []byte("payload")); results <- err }()
	}
	close(start)
	wins, conflicts := 0, 0
	for i := 0; i < 2; i++ {
		err := <-results
		var mismatch *application.OffsetMismatchError
		switch {
		case err == nil:
			wins++
		case errors.As(err, &mismatch) && mismatch.Committed == 7:
			conflicts++
		default:
			t.Fatal(err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("wins=%d conflicts=%d", wins, conflicts)
	}
	if got, err := factory().Digest(ctx, key); err != nil || got != hashBytes([]byte("payload")) {
		t.Fatalf("digest: %s %v", got, err)
	}
}

func TestS3UnpublishedChunkAndLostResponseRecovery(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-publish", true: "after-publish"}[lost], func(t *testing.T) {
			client := newModelS3()
			client.failManifest, client.loseManifestResponse = !lost, lost
			store := makeS3(t, client)
			ctx := context.Background()
			if _, err := store.Append(ctx, "artifact", 0, []byte("data")); !errors.Is(err, application.ErrUnavailable) {
				t.Fatal(err)
			}
			want := int64(0)
			if lost {
				want = 4
			}
			if got, err := makeS3(t, client).Size(ctx, "artifact"); err != nil || got != want {
				t.Fatalf("committed size: %d %v", got, err)
			}
			if lost {
				var mismatch *application.OffsetMismatchError
				if _, err := store.Append(ctx, "artifact", 0, []byte("data")); !errors.As(err, &mismatch) || mismatch.Committed != 4 {
					t.Fatal(err)
				}
			} else if _, err := store.Append(ctx, "artifact", 0, []byte("data")); err != nil {
				t.Fatal(err)
			}
			digest := hashBytes([]byte("data"))
			if got, err := store.Digest(ctx, "artifact"); err != nil || got != digest {
				t.Fatalf("digest: %s %v", got, err)
			}
			client.loseManifestResponse = true
			if _, err := store.Seal(ctx, "artifact", digest); !errors.Is(err, application.ErrUnavailable) {
				t.Fatal(err)
			}
			if size, err := makeS3(t, client).Seal(ctx, "artifact", digest); err != nil || size != 4 {
				t.Fatalf("resume seal: %d %v", size, err)
			}
		})
	}
}

func TestS3CorruptionFailsClosed(t *testing.T) {
	client := newModelS3()
	store := makeS3(t, client)
	ctx := context.Background()
	if _, err := store.Append(ctx, "artifact", 0, []byte("data")); err != nil {
		t.Fatal(err)
	}
	chunkKey := "gpub/artifact/chunks/" + hashBytes([]byte("data"))
	client.objects[chunkKey] = []byte("evil")
	if _, err := store.Read(ctx, "artifact", 0, 4); !errors.Is(err, application.ErrUnavailable) {
		t.Fatal(err)
	}
	if _, err := store.Seal(ctx, "artifact", hashBytes([]byte("data"))); !errors.Is(err, application.ErrUnavailable) {
		t.Fatal(err)
	}
	client.objects["gpub/artifact/manifest.json"] = []byte(`{"version":1,"size":10,"chunks":[],"sealed":false}`)
	if _, err := store.Size(ctx, "artifact"); !errors.Is(err, application.ErrUnavailable) {
		t.Fatal(err)
	}
	client.objects["gpub/artifact/manifest.json"] = bytes.Repeat([]byte("x"), maxManifestBytes+1)
	if _, err := store.Size(ctx, "artifact"); !errors.Is(err, application.ErrUnavailable) {
		t.Fatal(err)
	}
}

func TestS3EmptyArtifactAndInputBounds(t *testing.T) {
	store := makeS3(t, newModelS3())
	ctx := context.Background()
	if _, err := store.Append(ctx, "artifact", 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Seal(ctx, "artifact", hashBytes(nil)); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"", "../escape", "/absolute", "duplicate//segment"} {
		if _, err := store.Append(ctx, key, 0, nil); !errors.Is(err, application.ErrInvalid) {
			t.Fatal(key, err)
		}
	}
	if _, err := store.Append(ctx, "other", -1, nil); !errors.Is(err, application.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := store.Read(ctx, "other", -1, 1); !errors.Is(err, application.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := store.Read(ctx, "other", 0, maxChunkBytes+1); !errors.Is(err, application.ErrInvalid) {
		t.Fatal(err)
	}
}
