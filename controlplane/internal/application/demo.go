package application

import (
	"context"
	"fmt"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

// DemoPersona is a fictional tenant. Its rows are evidence_class=seeded and
// are excluded from the public status page by the view itself.
type DemoPersona struct {
	Name  string `json:"name"`
	Token string `json:"token"`
	Jobs  int    `json:"jobs"`
}

var demoPersonas = []struct {
	name     string
	jobs     int
	gpus     int
	priority int
	delay    time.Duration
}{
	{"demo-vision-lab", 4, 1, 3, 4 * time.Hour},
	{"demo-nlp-group", 3, 2, 5, 0},
	{"demo-robotics", 2, 1, 1, 8 * time.Hour},
}

// DemoSeed creates the fictional personas and submits their jobs through the
// real Submit path. It is only reachable when the API runs with --demo.
func (s *Service) DemoSeed(ctx context.Context, p Principal, run string) ([]DemoPersona, error) {
	if p.Kind != TokenUser || p.Role != domain.RoleOperator {
		return nil, ErrForbidden
	}
	var out []DemoPersona
	for _, d := range demoPersonas {
		tc, err := s.CreateTenant(ctx, TenantSetup{Name: fmt.Sprintf("%s-%s", d.name, run), EvidenceClass: domain.EvidenceSeeded,
			Handle: "demo", BudgetUSD: 500})
		if err != nil {
			return out, err
		}
		dp, err := s.Authenticate(ctx, tc.Token)
		if err != nil {
			return out, err
		}
		for i := 0; i < d.jobs; i++ {
			_, err := s.Submit(ctx, dp, SubmitRequest{Image: "busybox:1.36", GPUs: d.gpus, Priority: d.priority,
				Command: []string{"sh", "-c", fmt.Sprintf("echo demo %d; sleep 5", i)}, MaxRuntime: 10 * time.Minute,
				MaxDelay: d.delay, Policy: "balanced"}, "demo-seed")
			if err != nil {
				return out, err
			}
		}
		out = append(out, DemoPersona{Name: d.name, Token: tc.Token, Jobs: d.jobs})
	}
	return out, nil
}
