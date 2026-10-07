package postgres_test

import (
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/adapters/postgres"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/testkit"
)

// The status process's database role can read the two public views and
// nothing else. This is the structural half of "status cannot mutate jobs".
func TestStatusRoleIsReadOnlyAndSeesOnlyAggregates(t *testing.T) {
	e := testkit.New(t)
	role := fmt.Sprintf("gpub_status_t%d", os.Getpid())
	e.Exec(`DROP ROLE IF EXISTS ` + role)
	e.Exec(`CREATE ROLE ` + role + ` LOGIN PASSWORD 'x' IN ROLE gpub_status`)
	t.Cleanup(func() {
		_, _ = e.Store.Pool.Exec(e.Ctx, `DROP OWNED BY `+role)
		_, _ = e.Store.Pool.Exec(e.Ctx, `DROP ROLE IF EXISTS `+role)
	})
	u, _ := url.Parse(e.URL)
	u.User = url.UserPassword(role, "x")
	ro, err := postgres.Open(e.Ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()

	// Three real tenants and one seeded tenant run one job each.
	w := e.Worker(e.Pool("pool-a", "london", 1, false), "node-1", 8)
	for i, ec := range []domain.EvidenceClass{domain.EvidenceReal, domain.EvidenceReal, domain.EvidencePilot, domain.EvidenceSeeded} {
		tc, err := e.Svc.CreateTenant(e.Ctx, application.TenantSetup{Name: fmt.Sprintf("t%d", i), EvidenceClass: ec, Handle: "op", BudgetUSD: 100})
		e.Must(err)
		p, _ := e.Svc.Authenticate(e.Ctx, tc.Token)
		e.Submit(p, 1)
	}
	e.Tick()
	ds, _ := e.Svc.ClaimDispatches(e.Ctx, w, 16)
	for _, d := range ds {
		_, err := e.Svc.Ack(e.Ctx, w, d)
		e.Must(err)
		code := 0
		_, err = e.Svc.ReportEvents(e.Ctx, w, d.AttemptID, []application.WorkerEvent{{Seq: 1, Kind: "exit", Outcome: domain.OutcomeSucceeded, ExitCode: &code}})
		e.Must(err)
	}
	// The views use now(), so finished_at must be recent in database time.
	e.Exec(`UPDATE jobs SET finished_at = now() - interval '1 hour'`)

	pools, q, _, err := postgres.StatusSource{S: ro}.Read(e.Ctx)
	if err != nil {
		t.Fatalf("status role cannot read the views: %v", err)
	}
	if len(pools) != 1 || pools[0].Tenants7d != 3 || pools[0].Succeeded7d == nil || *pools[0].Succeeded7d != 3 {
		t.Fatalf("seeded tenant leaked into, or real ones missing from, the aggregate: %+v", pools)
	}
	if *pools[0].PilotJobs7d != 1 || *pools[0].RealJobs7d != 2 || q.QueuedJobs != 0 {
		t.Fatalf("evidence split %+v %+v", pools[0], q)
	}
	for _, stmt := range []string{
		`SELECT count(*) FROM jobs`,
		`UPDATE jobs SET state = 'CANCELLED'`,
		`INSERT INTO outbox_events (topic, key, payload) VALUES ('x','y','{}')`,
		`DELETE FROM reservations`,
		`SELECT count(*) FROM api_tokens`,
	} {
		if _, err := ro.Pool.Exec(e.Ctx, stmt); err == nil {
			t.Fatalf("status role was allowed: %s", stmt)
		}
	}

	// Below three tenants the outcome counts are withheld.
	e.Exec(`UPDATE jobs SET finished_at = now() - interval '30 days' WHERE tenant_id IN (SELECT id FROM tenants WHERE name = 't0')`)
	pools, _, _, err = postgres.StatusSource{S: ro}.Read(e.Ctx)
	e.Must(err)
	if pools[0].Succeeded7d != nil {
		t.Fatalf("two-tenant window published outcomes: %+v", pools[0])
	}
}
