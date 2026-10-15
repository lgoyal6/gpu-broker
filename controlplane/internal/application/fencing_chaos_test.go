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
	"github.com/lgoyal6/gpu-broker/controlplane/internal/testkit"
)

func openArtifactAttempt(t *testing.T) (*testkit.Env, application.Principal, application.Principal, application.Dispatch) {
	t.Helper()
	e := testkit.New(t)
	user, _ := e.Tenant("acme", 100)
	w := e.Worker(e.Pool("pool-a", "london", 1, false), "node-1", 1)
	e.Submit(user, 1)
	e.Tick()
	ds, err := e.Svc.ClaimDispatches(e.Ctx, w, 1)
	e.Must(err)
	if len(ds) != 1 {
		t.Fatalf("dispatches: %+v", ds)
	}
	_, err = e.Svc.Ack(e.Ctx, w, ds[0])
	e.Must(err)
	_, err = e.Svc.UploadChunk(e.Ctx, w, ds[0].AttemptID, "checkpoint.bin", 0, []byte("old"))
	e.Must(err)
	return e, user, w, ds[0]
}

func TestCompletedArtifactRetrySurvivesTerminalReport(t *testing.T) {
	e, _, worker, dispatch := openArtifactAttempt(t)
	sum := sha256.Sum256([]byte("old"))
	digest := hex.EncodeToString(sum[:])
	e.Must(e.Svc.CompleteArtifact(e.Ctx, worker, dispatch.AttemptID, "checkpoint.bin", digest))
	_, err := e.Svc.ReportEvents(e.Ctx, worker, dispatch.AttemptID, []application.WorkerEvent{exitEvent(1, domain.OutcomeSucceeded, 0)})
	e.Must(err)
	e.Must(e.Svc.CompleteArtifact(e.Ctx, worker, dispatch.AttemptID, "checkpoint.bin", digest))
	if err := e.Svc.CompleteArtifact(e.Ctx, worker, dispatch.AttemptID, "checkpoint.bin", "different"); !errors.Is(err, application.ErrConflict) {
		t.Fatalf("different digest retry: %v", err)
	}
}

func TestExpiredUnreclaimedArtifactAttemptRemainsWritable(t *testing.T) {
	e, _, worker, dispatch := openArtifactAttempt(t)
	e.Clock.Advance(2 * time.Minute)
	_, err := e.Svc.UploadChunk(e.Ctx, worker, dispatch.AttemptID, "checkpoint.bin", 3, []byte("more"))
	e.Must(err)
	sum := sha256.Sum256([]byte("oldmore"))
	e.Must(e.Svc.CompleteArtifact(e.Ctx, worker, dispatch.AttemptID, "checkpoint.bin", hex.EncodeToString(sum[:])))
}

func TestReclaimedAttemptCannotPublishArtifactsOverRetry(t *testing.T) {
	for _, action := range []string{"new", "append", "complete"} {
		t.Run(action, func(t *testing.T) {
			e, user, old, dispatch := openArtifactAttempt(t)
			e.Clock.Advance(2 * time.Minute)
			_, err := e.Svc.Reconcile(e.Ctx)
			e.Must(err)
			boot, err := e.Svc.UpsertPool(e.Ctx, domain.Pool{ID: "pool-b", Name: "pool-b", Kind: domain.PoolSimulated, Region: "london", PriceMicroUSDPerGPUHour: 1_000_000})
			e.Must(err)
			principal, err := e.Svc.Authenticate(e.Ctx, boot)
			e.Must(err)
			fresh := e.Worker(principal, "node-2", 1)
			e.Tick()
			ds, err := e.Svc.ClaimDispatches(e.Ctx, fresh, 1)
			e.Must(err)
			if len(ds) != 1 || ds[0].AttemptID == dispatch.AttemptID {
				t.Fatalf("retry dispatch: %+v", ds)
			}
			_, err = e.Svc.Ack(e.Ctx, fresh, ds[0])
			e.Must(err)
			ledger := e.QueryInt(`SELECT count(*) FROM budget_ledger`)
			switch action {
			case "new":
				_, err = e.Svc.UploadChunk(e.Ctx, old, dispatch.AttemptID, "late.bin", 0, []byte("late"))
			case "append":
				_, err = e.Svc.UploadChunk(e.Ctx, old, dispatch.AttemptID, "checkpoint.bin", 3, []byte("late"))
			case "complete":
				sum := sha256.Sum256([]byte("old"))
				err = e.Svc.CompleteArtifact(e.Ctx, old, dispatch.AttemptID, "checkpoint.bin", hex.EncodeToString(sum[:]))
			}
			if !errors.Is(err, application.ErrConflict) {
				t.Fatalf("reclaimed %s accepted: %v", action, err)
			}
			out, err := e.Svc.ReportEvents(e.Ctx, old, dispatch.AttemptID, []application.WorkerEvent{exitEvent(1, domain.OutcomeSucceeded, 0)})
			e.Must(err)
			view, err := e.Svc.GetJob(e.Ctx, user, dispatch.JobID)
			e.Must(err)
			if out != domain.OutcomeLost || view.Job.State != domain.JobRunning || view.Job.Attempt != 2 || len(view.Artifacts) != 1 || view.Artifacts[0].Size != 3 || view.Artifacts[0].State != domain.ArtifactUploading || e.QueryInt(`SELECT count(*) FROM budget_ledger`) != ledger || e.QueryInt(`SELECT sum(gpus_reserved) FROM workers`) != 1 {
				t.Fatalf("late worker changed retry, ledger, artifact or capacity: %+v", view)
			}
		})
	}
}

type pausedArtifactStore struct {
	application.ObjectStore
	started, resume chan struct{}
	seal            bool
}

func (p *pausedArtifactStore) pause(ctx context.Context) error {
	close(p.started)
	select {
	case <-p.resume:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *pausedArtifactStore) Append(ctx context.Context, key string, offset int64, data []byte) (int64, error) {
	n, err := p.ObjectStore.Append(ctx, key, offset, data)
	if err == nil && !p.seal {
		err = p.pause(ctx)
	}
	return n, err
}

func (p *pausedArtifactStore) Seal(ctx context.Context, key, sha string) (int64, error) {
	n, err := p.ObjectStore.Seal(ctx, key, sha)
	if err == nil && p.seal {
		err = p.pause(ctx)
	}
	return n, err
}

func TestArtifactPublicationRechecksAttemptAfterObjectWrite(t *testing.T) {
	for _, seal := range []bool{false, true} {
		t.Run(map[bool]string{false: "append", true: "complete"}[seal], func(t *testing.T) {
			e, user, worker, dispatch := openArtifactAttempt(t)
			store := &pausedArtifactStore{ObjectStore: e.Objects, started: make(chan struct{}), resume: make(chan struct{}), seal: seal}
			e.Svc.Objects = store
			ctx, cancel := context.WithTimeout(e.Ctx, 10*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if seal {
					sum := sha256.Sum256([]byte("old"))
					done <- e.Svc.CompleteArtifact(ctx, worker, dispatch.AttemptID, "checkpoint.bin", hex.EncodeToString(sum[:]))
				} else {
					_, err := e.Svc.UploadChunk(ctx, worker, dispatch.AttemptID, "checkpoint.bin", 3, []byte("late"))
					done <- err
				}
			}()
			select {
			case <-store.started:
			case <-ctx.Done():
				t.Fatal("object write did not reach publication barrier")
			}
			_, err := e.Svc.ReportEvents(e.Ctx, worker, dispatch.AttemptID, []application.WorkerEvent{exitEvent(1, domain.OutcomeLost, 1)})
			e.Must(err)
			close(store.resume)
			if err := <-done; !errors.Is(err, application.ErrConflict) {
				t.Fatalf("closed attempt publication succeeded: %v", err)
			}
			view, err := e.Svc.GetJob(e.Ctx, user, dispatch.JobID)
			e.Must(err)
			if len(view.Artifacts) != 1 || view.Artifacts[0].State != domain.ArtifactUploading || view.Artifacts[0].Size != 3 || view.Artifacts[0].SHA256 != "" {
				t.Fatalf("stale object write published metadata: %+v", view.Artifacts)
			}
		})
	}
}
