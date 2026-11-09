package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
)

type testPrerequisite struct {
	calls int
	err   error
}

func (p *testPrerequisite) Prepare(context.Context) error { p.calls++; return p.err }

func TestRegisterFailsBeforeAPIWhenRequiredRestoreFails(t *testing.T) {
	want := errors.New("no complete checkpoint")
	pre := &testPrerequisite{err: want}
	rt, err := NewSimRuntime(filepath.Join(t.TempDir(), "sim"), application.SystemClock{})
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{StateDir: t.TempDir(), DispatchKey: []byte(strings.Repeat("k", 32)), PreRegistration: pre}, rt, application.SystemClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Register(context.Background()); !errors.Is(err, want) || !strings.Contains(err.Error(), "pre-registration recovery") {
		t.Fatalf("Register error = %v", err)
	}
	if pre.calls != 1 {
		t.Fatalf("restore hook calls = %d", pre.calls)
	}
}

func TestRegisterPreparesRecoveryOnlyOnceAcrossAPIReentries(t *testing.T) {
	pre := &testPrerequisite{}
	rt, err := NewSimRuntime(filepath.Join(t.TempDir(), "sim"), application.SystemClock{})
	if err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	if err := os.WriteFile(filepath.Join(state, "worker-token"), []byte("cached-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "worker-id"), []byte("worker-1"), 0o600); err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{APIURL: "http://localhost", StateDir: state, DispatchKey: []byte(strings.Repeat("k", 32)), PreRegistration: pre}, rt, application.SystemClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	_ = a.Register(context.Background())
	_ = a.Register(context.Background())
	if pre.calls != 1 {
		t.Fatalf("restore hook calls = %d, want exactly once", pre.calls)
	}
}
