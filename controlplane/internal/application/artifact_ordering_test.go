package application_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

type delayedChunkResponse struct {
	application.ObjectStore
	offset          int64
	started, resume chan struct{}
}

func (p *delayedChunkResponse) Append(ctx context.Context, key string, offset int64, data []byte) (int64, error) {
	n, err := p.ObjectStore.Append(ctx, key, offset, data)
	if offset == p.offset {
		close(p.started)
		select {
		case <-p.resume:
		case <-ctx.Done():
			return n, ctx.Err()
		}
	}
	return n, err
}

func TestDelayedChunkResponsePreservesNewerArtifactMetadata(t *testing.T) {
	for _, action := range []string{"append", "offset_mismatch", "complete"} {
		t.Run(action, func(t *testing.T) {
			e, user, worker, dispatch := openArtifactAttempt(t)
			offset := int64(3)
			if action == "offset_mismatch" {
				offset = 0
			}
			store := &delayedChunkResponse{ObjectStore: e.Objects, offset: offset, started: make(chan struct{}), resume: make(chan struct{})}
			e.Svc.Objects = store
			ctx, cancel := context.WithTimeout(e.Ctx, 10*time.Second)
			defer cancel()
			type response struct {
				size int64
				err  error
			}
			done := make(chan response, 1)
			go func() {
				n, err := e.Svc.UploadChunk(ctx, worker, dispatch.AttemptID, "checkpoint.bin", offset, []byte("aaa"))
				done <- response{n, err}
			}()
			select {
			case <-store.started:
			case <-ctx.Done():
				t.Fatal("chunk response did not reach barrier")
			}
			want := "oldaaa"
			state := domain.ArtifactUploading
			if action == "complete" {
				sum := sha256.Sum256([]byte(want))
				e.Must(e.Svc.CompleteArtifact(ctx, worker, dispatch.AttemptID, "checkpoint.bin", hex.EncodeToString(sum[:])))
				state = domain.ArtifactComplete
			} else {
				if action == "offset_mismatch" {
					want = "old"
				}
				_, err := e.Svc.UploadChunk(ctx, worker, dispatch.AttemptID, "checkpoint.bin", int64(len(want)), []byte("bbb"))
				e.Must(err)
				want += "bbb"
			}
			close(store.resume)
			result := <-done
			if action == "complete" {
				if !errors.Is(result.err, application.ErrConflict) {
					t.Fatalf("late append after completion: %v", result.err)
				}
			} else if action == "offset_mismatch" {
				var mismatch *application.OffsetMismatchError
				if !errors.As(result.err, &mismatch) || mismatch.Committed != int64(len(want)) {
					t.Fatalf("stale resume offset: %+v", result)
				}
			} else {
				e.Must(result.err)
			}
			if result.size != int64(len(want)) {
				t.Fatalf("returned size=%d, actual=%d", result.size, len(want))
			}
			data, art, err := e.Svc.ReadArtifact(ctx, user, dispatch.JobID, dispatch.AttemptID, "checkpoint.bin", 0, 100)
			e.Must(err)
			if string(data) != want || art.Size != int64(len(want)) || art.State != state {
				t.Fatalf("bytes=%q metadata=%+v", data, art)
			}
		})
	}
}
