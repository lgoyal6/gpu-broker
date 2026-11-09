package httpapi

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOperatorUIExposesAuthenticatedRunWatchCancelAndRecoveryTrace(t *testing.T) {
	server := &Server{}
	req := httptest.NewRequest("GET", "/ui", nil)
	rec := httptest.NewRecorder()
	server.ui(rec, req)
	if rec.Code != 200 {
		t.Fatalf("GET /ui = %d", rec.Code)
	}
	for _, text := range []string{
		"Run a job", "Submit run", "Watch selected job", "Cancel selected job",
		"/v1/jobs", "/decisions", "/logs?limit=1000", "recovery_trace",
		"Download and verify", "SHA-256", "budget_cap_usd", "deadline",
		"does not claim a checkpoint was consumed",
	} {
		if !strings.Contains(rec.Body.String(), text) {
			t.Errorf("operator UI is missing %q", text)
		}
	}
	policy := rec.Header().Get("Content-Security-Policy")
	if !strings.Contains(policy, "default-src 'self'") || !strings.Contains(policy, "connect-src 'self'") {
		t.Fatalf("UI has unexpected content security policy %q", policy)
	}
}
