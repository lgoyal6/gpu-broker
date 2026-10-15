package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/transport/client"
)

type retryAbandonRuntime struct {
	Runtime
	stops, forgotten int
}

func (r *retryAbandonRuntime) Stop(context.Context, domain.AttemptID, domain.AttemptOutcome) error {
	r.stops++
	if r.stops <= 2 {
		return errors.New("injected runtime stop failure")
	}
	return nil
}

func (r *retryAbandonRuntime) Forget(domain.AttemptID) { r.forgotten++ }

func TestFailedAbandonStopRetainsOwnershipUntilRetrySucceeds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"renewed":[],"stop":[],"abandon":["att-old"],"lease_ttl_s":30}`)
	}))
	defer server.Close()
	runtime := &retryAbandonRuntime{}
	a, err := New(Config{StateDir: t.TempDir(), GPUs: 1, DispatchKey: make([]byte, 32)}, runtime, application.SystemClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	a.api, err = client.NewClientWithResponses(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	a.running["att-old"] = Spec{AttemptID: "att-old"}
	for range 2 {
		if err := a.heartbeat(context.Background()); err == nil || len(a.held()) != 1 || runtime.forgotten != 0 {
			t.Fatalf("failed stop lost ownership: err=%v held=%v forgotten=%d", err, a.held(), runtime.forgotten)
		}
	}
	if err := a.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runtime.stops != 3 || runtime.forgotten != 1 || len(a.held()) != 0 {
		t.Fatalf("successful retry: stops=%d forgotten=%d held=%v", runtime.stops, runtime.forgotten, a.held())
	}
}
