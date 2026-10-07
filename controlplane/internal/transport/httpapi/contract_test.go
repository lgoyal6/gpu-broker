package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/testkit"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/transport/client"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/transport/httpapi"
)

const specPath = "../../../../contracts/openapi/gpubroker-v1.yaml"

func loadSpec(t *testing.T) (*openapi3.T, routers.Router) {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile(specPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatalf("spec is invalid: %v", err)
	}
	doc.Servers = nil // match any host
	r, err := gorillamux.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	return doc, r
}

func TestEveryRouteIsInTheContractAndViceVersa(t *testing.T) {
	doc, _ := loadSpec(t)
	var spec []string
	for path, item := range doc.Paths.Map() {
		for method := range item.Operations() {
			spec = append(spec, method+" "+path)
		}
	}
	sort.Strings(spec)
	srv := httpapi.New(nil, nil, nil, nil, false)
	got := append([]string(nil), srv.Routes...)
	sort.Strings(got)
	if strings.Join(spec, "\n") != strings.Join(got, "\n") {
		t.Fatalf("routes and contract differ\nserver:\n  %s\ncontract:\n  %s", strings.Join(got, "\n  "), strings.Join(spec, "\n  "))
	}
}

// validating checks every request and response that passes through it
// against the contract. Any drift between server and spec fails the test
// that produced the traffic.
type validating struct {
	t       *testing.T
	router  routers.Router
	next    http.RoundTripper
	checked atomic.Int64
}

func (v *validating) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	route, params, err := v.router.FindRoute(req)
	if err != nil {
		v.t.Errorf("%s %s is not in the contract: %v", req.Method, req.URL.Path, err)
		return v.next.RoundTrip(req)
	}
	opts := &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc, IncludeResponseStatus: true}
	in := &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route, Options: opts}
	reqCopy := req.Clone(req.Context())
	reqCopy.Body = io.NopCloser(bytes.NewReader(body))
	in.Request = reqCopy
	if err := openapi3filter.ValidateRequest(req.Context(), in); err != nil {
		v.t.Errorf("request %s %s violates the contract: %v", req.Method, req.URL.Path, err)
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	resp, err := v.next.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	rb, _ := io.ReadAll(resp.Body)
	resp.Body = io.NopCloser(bytes.NewReader(rb))
	out := &openapi3filter.ResponseValidationInput{RequestValidationInput: in, Status: resp.StatusCode, Header: resp.Header,
		Body: io.NopCloser(bytes.NewReader(rb)), Options: opts}
	if err := openapi3filter.ValidateResponse(req.Context(), out); err != nil {
		v.t.Errorf("response %d to %s %s violates the contract: %v\nbody: %s", resp.StatusCode, req.Method, req.URL.Path, err, rb)
	}
	v.checked.Add(1)
	return resp, nil
}

type harness struct {
	e   *testkit.Env
	srv *httptest.Server
	val *validating
}

func newHarness(t *testing.T) *harness {
	e := testkit.New(t)
	_, router := loadSpec(t)
	api := httpapi.New(e.Svc, nil, nil, nil, true)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	return &harness{e: e, srv: srv, val: &validating{t: t, router: router, next: http.DefaultTransport}}
}

func (h *harness) client(t *testing.T, token string) *client.ClientWithResponses {
	c, err := client.NewClientWithResponses(h.srv.URL, client.WithHTTPClient(&http.Client{Transport: h.val}),
		client.WithRequestEditorFn(func(_ context.Context, r *http.Request) error {
			r.Header.Set("Authorization", "Bearer "+token)
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func ptr[T any](v T) *T { return &v }

// The whole worker and user protocol through the generated client, with
// every exchange validated against the contract.
func TestProtocolThroughGeneratedClientMatchesContract(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, tc := h.e.Tenant("acme", 100)
	bootTok, err := h.e.Svc.UpsertPool(ctx, domain.Pool{ID: "pool-a", Name: "pool-a", Kind: domain.PoolSimulated, Region: "london",
		PriceMicroUSDPerGPUHour: 2_000_000, PUE: 1.1})
	h.e.Must(err)

	boot := h.client(t, bootTok)
	reg, err := boot.RegisterWorkerWithResponse(ctx, client.RegisterWorkerRequest{Name: "node-1", GpuModel: "a100", Gpus: 4,
		GpuMemGb: 80, Runtimes: []client.RegisterWorkerRequestRuntimes{"sim"}})
	h.e.Must(err)
	if reg.JSON201 == nil {
		t.Fatalf("register: %d %s", reg.StatusCode(), reg.Body)
	}
	worker := h.client(t, reg.JSON201.Token)
	user := h.client(t, tc.Token)

	sub, err := user.SubmitJobWithResponse(ctx, &client.SubmitJobParams{IdempotencyKey: "k1"}, client.SubmitJobRequest{
		Image: "ghcr.io/example/train:1", Command: []string{"sim", "duration=1s"}, Gpus: 2, MaxRuntimeS: 3600, Policy: ptr("fair-share")})
	h.e.Must(err)
	if sub.JSON201 == nil || sub.JSON201.State != client.JobStateQUEUED {
		t.Fatalf("submit: %d %s", sub.StatusCode(), sub.Body)
	}
	jobID := sub.JSON201.Id

	// Idempotent replay returns the same job; a different body is refused.
	again, err := user.SubmitJobWithResponse(ctx, &client.SubmitJobParams{IdempotencyKey: "k1"}, client.SubmitJobRequest{
		Image: "ghcr.io/example/train:1", Command: []string{"sim", "duration=1s"}, Gpus: 2, MaxRuntimeS: 3600, Policy: ptr("fair-share")})
	h.e.Must(err)
	if again.JSON201 == nil || again.JSON201.Id != jobID || again.HTTPResponse.Header.Get("Idempotent-Replay") != "true" {
		t.Fatalf("replay: %d %s", again.StatusCode(), again.Body)
	}
	diff, err := user.SubmitJobWithResponse(ctx, &client.SubmitJobParams{IdempotencyKey: "k1"}, client.SubmitJobRequest{
		Image: "ghcr.io/example/train:1", Command: []string{"other"}, Gpus: 1, MaxRuntimeS: 3600})
	h.e.Must(err)
	if diff.JSON422 == nil || diff.JSON422.Error.Code != "idempotency_key_reused" {
		t.Fatalf("reused key: %d %s", diff.StatusCode(), diff.Body)
	}
	if n := h.e.QueryInt(`SELECT count(*) FROM jobs`); n != 1 {
		t.Fatalf("replays created %d jobs", n)
	}

	exp, err := user.ExplainPolicyWithResponse(ctx, client.SubmitJobRequest{Image: "x/y:1", Command: []string{"a"}, Gpus: 1, MaxRuntimeS: 600})
	h.e.Must(err)
	if exp.JSON200 == nil || exp.JSON200.Action != client.PLACE {
		t.Fatalf("explain: %d %s", exp.StatusCode(), exp.Body)
	}
	if n := h.e.QueryInt(`SELECT count(*) FROM decisions`); n != 0 {
		t.Fatal("explain wrote a decision")
	}

	h.e.Tick()
	hb, err := worker.HeartbeatWithResponse(ctx, client.HeartbeatRequest{State: client.READY, Held: []string{}})
	h.e.Must(err)
	if hb.JSON200 == nil || hb.JSON200.LeaseTtlS != 45 {
		t.Fatalf("heartbeat: %s", hb.Body)
	}
	cl, err := worker.ClaimDispatchesWithResponse(ctx, client.ClaimRequest{Max: 4})
	h.e.Must(err)
	if cl.JSON200 == nil || len(cl.JSON200.Dispatches) != 1 {
		t.Fatalf("claim: %s", cl.Body)
	}
	d := cl.JSON200.Dispatches[0]
	ack, err := worker.AckDispatchWithResponse(ctx, d.AttemptId, d)
	h.e.Must(err)
	if ack.JSON200 == nil || ack.JSON200.Action != client.AckResponseActionRun {
		t.Fatalf("ack: %s", ack.Body)
	}
	ack2, err := worker.AckDispatchWithResponse(ctx, d.AttemptId, d)
	h.e.Must(err)
	if ack2.JSON409 == nil || ack2.JSON409.Error.Code != "already_acknowledged" {
		t.Fatalf("second ack: %d %s", ack2.StatusCode(), ack2.Body)
	}

	up, err := worker.UploadArtifactChunkWithBodyWithResponse(ctx, d.AttemptId, "out.txt", &client.UploadArtifactChunkParams{Offset: 0},
		"application/octet-stream", strings.NewReader("hello world"))
	h.e.Must(err)
	if up.JSON200 == nil || up.JSON200.Offset != 11 {
		t.Fatalf("upload: %s", up.Body)
	}
	stale, err := worker.UploadArtifactChunkWithBodyWithResponse(ctx, d.AttemptId, "out.txt", &client.UploadArtifactChunkParams{Offset: 0},
		"application/octet-stream", strings.NewReader("hello world"))
	h.e.Must(err)
	if stale.JSON409 == nil || stale.JSON409.Error.Offset == nil || *stale.JSON409.Error.Offset != 11 {
		t.Fatalf("stale offset: %s", stale.Body)
	}
	done, err := worker.CompleteArtifactWithResponse(ctx, d.AttemptId, "out.txt", client.CompleteArtifactRequest{
		Sha256: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"})
	h.e.Must(err)
	if done.StatusCode() != 204 {
		t.Fatalf("complete: %d %s", done.StatusCode(), done.Body)
	}

	code := 0
	ev, err := worker.ReportEventsWithResponse(ctx, d.AttemptId, client.EventsRequest{Events: []client.WorkerEvent{
		{Seq: 1, Kind: client.Log, At: time.Now(), Message: ptr("epoch 1 loss 0.31")},
		{Seq: 2, Kind: client.Exit, At: time.Now(), Outcome: ptr(client.WorkerEventOutcomeSUCCEEDED), ExitCode: &code},
	}})
	h.e.Must(err)
	if ev.JSON200 == nil || ev.JSON200.Outcome != "SUCCEEDED" {
		t.Fatalf("events: %s", ev.Body)
	}

	job, err := user.GetJobWithResponse(ctx, jobID)
	h.e.Must(err)
	if job.JSON200 == nil || job.JSON200.Job.State != client.JobStateSUCCEEDED || len(job.JSON200.Artifacts) != 1 {
		t.Fatalf("job: %s", job.Body)
	}
	for _, f := range []func() (int, []byte){
		func() (int, []byte) { r, _ := user.JobDecisionsWithResponse(ctx, jobID); return r.StatusCode(), r.Body },
		func() (int, []byte) { r, _ := user.JobLogsWithResponse(ctx, jobID, nil); return r.StatusCode(), r.Body },
		func() (int, []byte) { r, _ := user.ListJobsWithResponse(ctx, nil); return r.StatusCode(), r.Body },
		func() (int, []byte) { r, _ := user.ListPoolsWithResponse(ctx); return r.StatusCode(), r.Body },
		func() (int, []byte) { r, _ := user.ListPoliciesWithResponse(ctx); return r.StatusCode(), r.Body },
		func() (int, []byte) {
			r, _ := user.DownloadArtifactWithResponse(ctx, jobID, d.AttemptId, "out.txt", nil)
			return r.StatusCode(), r.Body
		},
		func() (int, []byte) {
			r, _ := user.CancelJobWithResponse(ctx, jobID, &client.CancelJobParams{IdempotencyKey: "c1"})
			return r.StatusCode(), r.Body
		},
		func() (int, []byte) {
			r, _ := user.CreateUserWithResponse(ctx, &client.CreateUserParams{IdempotencyKey: "u1"}, client.CreateUserRequest{Handle: "bob", Role: client.CreateUserRequestRoleMember})
			return r.StatusCode(), r.Body
		},
		func() (int, []byte) { r, _ := user.ExportTraceWithResponse(ctx); return r.StatusCode(), r.Body },
		func() (int, []byte) { r, _ := user.DemoSeedWithResponse(ctx); return r.StatusCode(), r.Body },
	} {
		if code, body := f(); code >= 500 {
			t.Fatalf("server error %d: %s", code, body)
		}
	}
	logs, _ := user.JobLogsWithResponse(ctx, jobID, nil)
	if logs.JSON200 == nil || len(logs.JSON200.Lines) != 2 || logs.JSON200.Lines[0].Message != "epoch 1 loss 0.31" {
		t.Fatalf("logs: %s", logs.Body)
	}
	if h.val.checked.Load() < 20 {
		t.Fatalf("only %d exchanges validated", h.val.checked.Load())
	}
}

func TestErrorsAreStableAndAuthIsUniform(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, tc := h.e.Tenant("acme", 1)
	_, other := h.e.Tenant("other", 1)
	for _, tok := range []string{"", "garbage", "gpub_user_nope_nope", tc.Token + "x"} {
		r, err := h.client(t, tok).ListJobsWithResponse(ctx, nil)
		h.e.Must(err)
		if r.JSON401 == nil && r.StatusCode() != 401 {
			t.Fatalf("token %q: %d", tok, r.StatusCode())
		}
		var eb httpapi.ErrorBody
		_ = json.Unmarshal(r.Body, &eb)
		if eb.Error.Code != "unauthorized" || eb.Error.RequestID == "" {
			t.Fatalf("401 body %s", r.Body)
		}
	}
	user := h.client(t, tc.Token)
	// Missing Idempotency-Key, validated by the server (the spec marks it required,
	// so this call goes around the validating client).
	req, _ := http.NewRequest("POST", h.srv.URL+"/v1/jobs", strings.NewReader(`{"image":"x/y:1","command":["a"],"gpus":1,"max_runtime_s":60}`))
	req.Header.Set("Authorization", "Bearer "+tc.Token)
	resp, err := http.DefaultClient.Do(req)
	h.e.Must(err)
	var eb httpapi.ErrorBody
	_ = json.NewDecoder(resp.Body).Decode(&eb)
	resp.Body.Close()
	if resp.StatusCode != 400 || eb.Error.Code != "invalid_request" || !strings.Contains(eb.Error.Message, "Idempotency-Key") {
		t.Fatalf("no idempotency key: %d %+v", resp.StatusCode, eb)
	}
	// Over budget: 422 with the job id of the FAILED row that explains it.
	h.e.Worker(h.e.Pool("pool-a", "london", 5, false), "node-1", 1)
	r, err := user.SubmitJobWithResponse(ctx, &client.SubmitJobParams{IdempotencyKey: "b1"}, client.SubmitJobRequest{
		Image: "x/y:1", Command: []string{"a"}, Gpus: 1, MaxRuntimeS: 3600})
	h.e.Must(err)
	if r.JSON422 == nil || r.JSON422.Error.Code != "budget_insufficient" || r.JSON422.Error.JobId == nil || r.JSON422.Error.Hint == nil {
		t.Fatalf("budget: %d %s", r.StatusCode(), r.Body)
	}
	// Another tenant's job is indistinguishable from a missing one.
	g, err := h.client(t, other.Token).GetJobWithResponse(ctx, *r.JSON422.Error.JobId)
	h.e.Must(err)
	m, err := h.client(t, other.Token).GetJobWithResponse(ctx, "job_does_not_exist")
	h.e.Must(err)
	if g.StatusCode() != 404 || m.StatusCode() != 404 || g.JSON404.Error.Code != m.JSON404.Error.Code {
		t.Fatalf("cross-tenant %d vs missing %d", g.StatusCode(), m.StatusCode())
	}
	// Unknown fields are refused rather than ignored.
	req, _ = http.NewRequest("POST", h.srv.URL+"/v1/policy/explain", strings.NewReader(`{"image":"x/y:1","command":["a"],"gpus":1,"max_runtime_s":60,"gpu":"h100"}`))
	req.Header.Set("Authorization", "Bearer "+tc.Token)
	resp, err = http.DefaultClient.Do(req)
	h.e.Must(err)
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("unknown field accepted: %d", resp.StatusCode)
	}
	// Demo seeding is not reachable unless the API runs with --demo.
	plain := httptest.NewServer(httpapi.New(h.e.Svc, nil, nil, nil, false).Handler())
	defer plain.Close()
	req, _ = http.NewRequest("POST", plain.URL+"/v1/demo/seed", nil)
	req.Header.Set("Authorization", "Bearer "+tc.Token)
	resp, err = http.DefaultClient.Do(req)
	h.e.Must(err)
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("demo seed without --demo: %d", resp.StatusCode)
	}
	_ = application.ErrInvalid
}
