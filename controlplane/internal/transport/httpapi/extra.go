package httpapi

import (
	_ "embed"
	"net/http"
	"strings"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
)

func (s *Server) traceExport(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	tr, err := s.Svc.ExportTrace(r.Context(), p)
	if err != nil {
		return err
	}
	return writeJSON(w, 200, tr)
}

type DemoSeedResponse struct {
	EvidenceClass string                    `json:"evidence_class"`
	Personas      []application.DemoPersona `json:"personas"`
}

func (s *Server) demoSeed(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	if !s.Demo {
		return application.ErrNotFound
	}
	ps, err := s.Svc.DemoSeed(r.Context(), p, strings.TrimPrefix(s.Svc.IDs.New("run"), "run_"))
	if err != nil {
		return err
	}
	return writeJSON(w, 201, DemoSeedResponse{EvidenceClass: "seeded", Personas: ps})
}

//go:embed ui.html
var uiHTML []byte

// ui is the operator interface: a static page with no data in it. It holds
// the operator's token in sessionStorage and calls the same authenticated
// API as gpuctl, so it adds no server-side capability of its own.
func (s *Server) ui(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'")
	w.Header().Set("X-Frame-Options", "DENY")
	_, _ = w.Write(uiHTML)
}
