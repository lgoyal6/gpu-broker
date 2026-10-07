package objectstore

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
)

func TestFSSealSurvivesRestartAndRejectsAppends(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	first, err := NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Append(ctx, "artifact", 0, []byte("baseline")); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Seal(ctx, "artifact", hashBytes([]byte("wrong"))); !errors.Is(err, application.ErrConflict) {
		t.Fatal(err)
	}
	digest := hashBytes([]byte("baseline"))
	if size, err := first.Seal(ctx, "artifact", digest); err != nil || size != 8 {
		t.Fatalf("seal: %d %v", size, err)
	}
	restarted, err := NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Append(ctx, "artifact", 8, []byte("changed")); !errors.Is(err, application.ErrConflict) {
		t.Fatal(err)
	}
	if size, err := restarted.Seal(ctx, "artifact", digest); err != nil || size != 8 {
		t.Fatalf("repeat seal: %d %v", size, err)
	}
	if _, err := restarted.Seal(ctx, "artifact", hashBytes([]byte("other"))); !errors.Is(err, application.ErrConflict) {
		t.Fatal(err)
	}
	if got, err := restarted.Digest(ctx, "artifact"); err != nil || got != digest {
		t.Fatalf("digest: %s %v", got, err)
	}
}

func TestFSCompetingInstancesCommitOnlyOneAppend(t *testing.T) {
	dir := t.TempDir()
	stores := make([]*FS, 2)
	for i := range stores {
		var err error
		stores[i], err = NewFS(dir)
		if err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, store := range stores {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := store.Append(context.Background(), "artifact", 0, []byte("payload"))
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	wins, conflicts := 0, 0
	for err := range results {
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
}

func TestFSLockHonorsAnotherProcessAndCancellation(t *testing.T) {
	if dir := os.Getenv("GPUB_FS_LOCK_CHILD"); dir != "" {
		store, err := NewFS(dir)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := store.Append(ctx, "artifact", 0, []byte("cannot-write")); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("append bypassed another process's lock: %v", err)
		}
		return
	}
	dir := t.TempDir()
	held, err := lockedFile(context.Background(), filepath.Join(dir, "artifact"), os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestFSLockHonorsAnotherProcessAndCancellation$")
	cmd.Env = append(os.Environ(), "GPUB_FS_LOCK_CHILD="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v %s", err, output)
	}
	if st, err := held.Stat(); err != nil || st.Size() != 0 {
		t.Fatalf("locked artifact changed: %v %v", st, err)
	}
}

func TestFSInvalidReadAndKeys(t *testing.T) {
	store, err := NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"../escape", "", ".gpub-seals/metadata"} {
		if _, err := store.Append(context.Background(), key, 0, nil); !errors.Is(err, application.ErrInvalid) {
			t.Fatal(key, err)
		}
	}
	if _, err := store.Read(context.Background(), "artifact", -1, 4); !errors.Is(err, application.ErrInvalid) {
		t.Fatal(err)
	}
}
