// Package httpapi is the authenticated control-plane API. It translates
// HTTP to application calls and back; no business rule lives here.
package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/domain"
)

// HTTPObserver records per-route request metrics.
type HTTPObserver interface {
	HTTP(route string, code int, d time.Duration)
}

type Server struct {
	Svc   *application.Service
	Log   *slog.Logger
	Obs   HTTPObserver
	Ready func(ctx context.Context) error
	// Demo enables POST /v1/demo/seed. Off unless the process is started
	// with --demo, which the Kind and local profiles do and production does not.
	Demo bool
	mux  *http.ServeMux
	// Routes lists "METHOD /path" for every registered route; the contract
	// test compares it with the OpenAPI document.
	Routes []string
}

func New(svc *application.Service, log *slog.Logger, obs HTTPObserver, ready func(context.Context) error, demo bool) *Server {
	s := &Server{Svc: svc, Log: log, Obs: obs, Ready: ready, Demo: demo, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

type handler func(w http.ResponseWriter, r *http.Request, p application.Principal) error

const maxBody = 1 << 20

func (s *Server) handle(method, path string, auth bool, h handler) {
	route := method + " " + path
	s.Routes = append(s.Routes, route)
	s.mux.HandleFunc(route, func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rid := r.Header.Get("X-Request-ID")
		if rid == "" || len(rid) > 64 {
			rid = newRequestID()
		}
		w.Header().Set("X-Request-ID", rid)
		rec := &statusRecorder{ResponseWriter: w, code: 200}
		ctx := context.WithValue(r.Context(), ridKey{}, rid)
		r = r.WithContext(ctx)
		if !strings.HasPrefix(path, "/v1/attempts/{id}/artifacts/{name}") || method != http.MethodPut {
			r.Body = http.MaxBytesReader(rec, r.Body, maxBody)
		} else {
			r.Body = http.MaxBytesReader(rec, r.Body, 8<<20+1)
		}
		var p application.Principal
		var err error
		if auth {
			p, err = s.Svc.Authenticate(ctx, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		}
		if err == nil {
			err = h(rec, r, p)
		}
		if err != nil {
			s.writeError(rec, r, err)
		}
		if s.Obs != nil {
			s.Obs.HTTP(route, rec.code, time.Since(start))
		}
	})
}

type ridKey struct{}

func requestID(r *http.Request) string {
	v, _ := r.Context().Value(ridKey{}).(string)
	return v
}

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }

func writeJSON(w http.ResponseWriter, code int, v any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	return json.NewEncoder(w).Encode(v)
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: body: %v", application.ErrInvalid, err)
	}
	return nil
}

// errorMap is the stable mapping from application errors to status and code.
var errorMap = []struct {
	err  error
	code int
	name string
	hint string
}{
	{application.ErrUnauthorized, 401, "unauthorized", "send Authorization: Bearer <token>"},
	{application.ErrForbidden, 403, "forbidden", ""},
	{application.ErrNotFound, 404, "not_found", ""},
	{application.ErrInvalid, 400, "invalid_request", ""},
	{application.ErrIdempotencyReused, 422, "idempotency_key_reused", "use a new Idempotency-Key for a different request"},
	{application.ErrBudget, 422, "budget_insufficient", "raise the project budget, lower max_runtime_s, or restrict to a cheaper pool"},
	{application.ErrQuotaExceeded, 429, "quota_exceeded", "wait for queued jobs to start or ask an operator to raise the quota"},
	{application.ErrQueueFull, 503, "queue_full", "retry after the Retry-After interval"},
	{application.ErrAlreadyAcked, 409, "already_acknowledged", "drop this dispatch; it is already running"},
	{application.ErrLeaseInvalid, 409, "lease_invalid", "the dispatch was altered or is not addressed to this worker"},
	{application.ErrConflict, 409, "conflict", ""},
	{application.ErrStaleVersion, 409, "conflict", "retry the request"},
	{application.ErrUnavailable, 503, "unavailable", "retry with backoff"},
	{domain.ErrIllegalTransition, 409, "illegal_transition", ""},
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	body := ErrorBody{Error: ErrorDetail{Code: "internal", Message: "internal error", RequestID: requestID(r)}}
	code := 500
	var om *application.OffsetMismatchError
	switch {
	case errors.As(err, &om):
		code = 409
		body.Error.Code, body.Error.Message = "offset_mismatch", err.Error()
		body.Error.Offset = &om.Committed
		body.Error.Hint = "resume the upload from the returned offset"
	default:
		for _, m := range errorMap {
			if errors.Is(err, m.err) {
				code, body.Error.Code, body.Error.Hint = m.code, m.name, m.hint
				body.Error.Message = err.Error()
				break
			}
		}
	}
	if je, ok := err.(interface{ JobID() string }); ok {
		body.Error.JobID = je.JobID()
	}
	if code == 503 {
		w.Header().Set("Retry-After", "5")
	}
	if code == 500 && s.Log != nil {
		// The cause is logged with the request id and never sent: internal
		// errors can carry SQL or paths.
		s.Log.Error("request failed", "request_id", requestID(r), "route", r.Pattern, "err", err)
	}
	_ = writeJSON(w, code, body)
}

// refusedJob carries the id of a job recorded as FAILED at admission, so the
// error response can point at the row that explains it.
type refusedJob struct {
	error
	id domain.JobID
}

func (e refusedJob) JobID() string { return string(e.id) }
func (e refusedJob) Unwrap() error { return e.error }

// ---- idempotency ----

// idempotent wraps a user mutation. Same key and same body replays the
// stored response; same key with a different body is refused. The record is
// written after the mutation commits; two concurrent requests with one key
// both execute only if they race inside the gap, and the unique key then
// rejects the second record, so the second caller gets a 409 rather than a
// silently divergent replay.
func (s *Server) idempotent(h handler) handler {
	return func(w http.ResponseWriter, r *http.Request, p application.Principal) error {
		key := r.Header.Get("Idempotency-Key")
		if key == "" || len(key) > 128 {
			return fmt.Errorf("%w: Idempotency-Key header (1..128 chars) is required on this route", application.ErrInvalid)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return fmt.Errorf("%w: %v", application.ErrInvalid, err)
		}
		sum := sha256.Sum256(body)
		bodyHash := hex.EncodeToString(sum[:])
		var prior application.IdempotencyRecord
		err = s.Svc.Store.InTx(r.Context(), func(tx application.Tx) (e error) {
			prior, e = tx.GetIdempotency(r.Context(), p.Key(), key)
			return e
		})
		switch {
		case err == nil:
			if prior.BodyHash != bodyHash || prior.Method != r.Method || prior.Path != r.URL.Path {
				return application.ErrIdempotencyReused
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Idempotent-Replay", "true")
			w.WriteHeader(prior.Status)
			_, _ = w.Write(prior.Response)
			return nil
		case !errors.Is(err, application.ErrNotFound):
			return err
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		buf := &bufferWriter{header: http.Header{}, code: 200}
		if err := h(buf, r, p); err != nil {
			// Errors are not cached: a refused request may succeed later
			// (quota frees up) and should be retryable with the same key.
			return err
		}
		rec := application.IdempotencyRecord{Principal: p.Key(), Key: key, Method: r.Method, Path: r.URL.Path,
			BodyHash: bodyHash, Status: buf.code, Response: buf.buf.Bytes(), CreatedAt: s.Svc.Clock.Now()}
		if err := s.Svc.Store.InTx(r.Context(), func(tx application.Tx) error { return tx.PutIdempotency(r.Context(), rec) }); err != nil {
			return err
		}
		for k, v := range buf.header {
			w.Header()[k] = v
		}
		w.WriteHeader(buf.code)
		_, _ = w.Write(buf.buf.Bytes())
		return nil
	}
}

type bufferWriter struct {
	header http.Header
	code   int
	buf    bytes.Buffer
}

func (b *bufferWriter) Header() http.Header         { return b.header }
func (b *bufferWriter) WriteHeader(c int)           { b.code = c }
func (b *bufferWriter) Write(p []byte) (int, error) { return b.buf.Write(p) }

// ---- routes ----

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	s.mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if s.Ready != nil {
			if err := s.Ready(r.Context()); err != nil {
				http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = io.WriteString(w, "ready\n")
	})

	s.handle("POST", "/v1/jobs", true, s.idempotent(s.submit))
	s.handle("GET", "/v1/jobs", true, s.listJobs)
	s.handle("GET", "/v1/jobs/{id}", true, s.getJob)
	s.handle("POST", "/v1/jobs/{id}/cancel", true, s.idempotent(s.cancel))
	s.handle("GET", "/v1/jobs/{id}/decisions", true, s.decisions)
	s.handle("GET", "/v1/jobs/{id}/logs", true, s.logs)
	s.handle("GET", "/v1/jobs/{id}/artifacts/{attempt}/{name}", true, s.downloadArtifact)
	s.handle("GET", "/v1/pools", true, s.pools)
	s.handle("GET", "/v1/policies", true, s.policies)
	s.handle("POST", "/v1/policy/explain", true, s.explain)
	s.handle("POST", "/v1/users", true, s.idempotent(s.createUser))
	s.handle("GET", "/v1/trace/export", true, s.traceExport)
	s.handle("POST", "/v1/demo/seed", true, s.demoSeed)

	s.handle("POST", "/v1/workers/register", true, s.register)
	s.handle("POST", "/v1/workers/heartbeat", true, s.heartbeat)
	s.handle("POST", "/v1/workers/claim", true, s.claim)
	s.handle("POST", "/v1/attempts/{id}/ack", true, s.ack)
	s.handle("POST", "/v1/attempts/{id}/events", true, s.events)
	s.handle("PUT", "/v1/attempts/{id}/artifacts/{name}", true, s.upload)
	s.handle("POST", "/v1/attempts/{id}/artifacts/{name}/complete", true, s.completeArtifact)

	s.mux.HandleFunc("GET /ui", s.ui)
	sort.Strings(s.Routes)
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	var req SubmitJobRequest
	if err := decode(r, &req); err != nil {
		return err
	}
	j, err := s.Svc.Submit(r.Context(), p, req.toApp(), requestID(r))
	if err != nil {
		if j.ID != "" {
			return refusedJob{err, j.ID}
		}
		return err
	}
	return writeJSON(w, http.StatusCreated, jobDTO(j))
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	js, err := s.Svc.ListJobs(r.Context(), p, limit)
	if err != nil {
		return err
	}
	out := JobList{Jobs: []Job{}}
	for _, j := range js {
		out.Jobs = append(out.Jobs, jobDTO(j))
	}
	return writeJSON(w, 200, out)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	v, err := s.Svc.GetJob(r.Context(), p, domain.JobID(r.PathValue("id")))
	if err != nil {
		return err
	}
	return writeJSON(w, 200, jobDetailDTO(v))
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	j, err := s.Svc.Cancel(r.Context(), p, domain.JobID(r.PathValue("id")), requestID(r))
	if err != nil {
		return err
	}
	return writeJSON(w, 200, jobDTO(j))
}

func (s *Server) decisions(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	ds, err := s.Svc.Decisions(r.Context(), p, domain.JobID(r.PathValue("id")))
	if err != nil {
		return err
	}
	out := DecisionList{Decisions: []json.RawMessage{}}
	for _, d := range ds {
		out.Decisions = append(out.Decisions, d.Body)
	}
	return writeJSON(w, 200, out)
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	evs, err := s.Svc.Logs(r.Context(), p, domain.JobID(r.PathValue("id")), limit)
	if err != nil {
		return err
	}
	out := LogList{Lines: []LogLine{}}
	for _, e := range evs {
		var we application.WorkerEvent
		_ = json.Unmarshal(e.Payload, &we)
		out.Lines = append(out.Lines, LogLine{AttemptID: string(e.AttemptID), Seq: e.Seq, Kind: e.Kind, At: e.At, Message: we.Message})
	}
	return writeJSON(w, 200, out)
}

func (s *Server) downloadArtifact(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	off, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	b, art, err := s.Svc.ReadArtifact(r.Context(), p, domain.JobID(r.PathValue("id")), domain.AttemptID(r.PathValue("attempt")),
		r.PathValue("name"), off, 8<<20)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Artifact-Size", strconv.FormatInt(art.Size, 10))
	w.Header().Set("X-Artifact-State", string(art.State))
	w.Header().Set("X-Artifact-SHA256", art.SHA256)
	_, err = w.Write(b)
	return err
}

func (s *Server) pools(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	if p.Kind != application.TokenUser {
		return application.ErrForbidden
	}
	pools, workers, err := s.Svc.Pools(r.Context())
	if err != nil {
		return err
	}
	out := PoolList{Pools: []Pool{}}
	for _, pl := range pools {
		dto := Pool{ID: string(pl.ID), Name: pl.Name, Kind: string(pl.Kind), Region: pl.Region,
			PriceUSDPerGPUHour: usd(pl.PriceMicroUSDPerGPUHour), Interruptible: pl.Interruptible, GPUModels: []string{}}
		models := map[string]bool{}
		for _, wk := range workers {
			if wk.PoolID != pl.ID || wk.State == domain.WorkerOffline {
				continue
			}
			dto.GPUsTotal += wk.Capability.GPUs
			if wk.State == domain.WorkerReady {
				dto.WorkersReady++
				dto.GPUsFree += wk.FreeGPUs()
			}
			if !models[wk.Capability.GPUModel] {
				models[wk.Capability.GPUModel] = true
				dto.GPUModels = append(dto.GPUModels, wk.Capability.GPUModel)
			}
		}
		sort.Strings(dto.GPUModels)
		out.Pools = append(out.Pools, dto)
	}
	return writeJSON(w, 200, out)
}

func (s *Server) policies(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	if p.Kind != application.TokenUser {
		return application.ErrForbidden
	}
	type pol struct {
		Name    string `json:"name"`
		Version string `json:"version"`
		Spec    any    `json:"spec"`
	}
	out := struct {
		Default  string `json:"default"`
		Policies []pol  `json:"policies"`
	}{Default: s.Svc.Cfg.DefaultPolicy}
	names := make([]string, 0, len(s.Svc.Policies))
	for n := range s.Svc.Policies {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		out.Policies = append(out.Policies, pol{Name: n, Version: s.Svc.Policies[n].Version, Spec: s.Svc.Policies[n]})
	}
	return writeJSON(w, 200, out)
}

func (s *Server) explain(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	var req SubmitJobRequest
	if err := decode(r, &req); err != nil {
		return err
	}
	d, err := s.Svc.Explain(r.Context(), p, req.toApp())
	if err != nil {
		return err
	}
	return writeJSON(w, 200, d)
}

func (s *Server) createUser(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	var req CreateUserRequest
	if err := decode(r, &req); err != nil {
		return err
	}
	id, tok, err := s.Svc.AddUser(r.Context(), p, req.Handle, domain.Role(req.Role))
	if err != nil {
		return err
	}
	return writeJSON(w, 201, CreateUserResponse{UserID: string(id), Token: tok})
}

func (s *Server) register(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	var req RegisterWorkerRequest
	if err := decode(r, &req); err != nil {
		return err
	}
	wk, tok, err := s.Svc.Register(r.Context(), p, application.RegisterRequest{Name: req.Name, Capability: domain.Capability{
		GPUModel: req.GPUModel, GPUs: req.GPUs, GPUMemGB: req.GPUMemGB, Runtimes: req.Runtimes}})
	if err != nil {
		return err
	}
	return writeJSON(w, 201, RegisterWorkerResponse{WorkerID: string(wk.ID), PoolID: string(wk.PoolID), Token: tok})
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	var req HeartbeatRequest
	if err := decode(r, &req); err != nil {
		return err
	}
	held := make([]domain.AttemptID, 0, len(req.Held))
	for _, h := range req.Held {
		held = append(held, domain.AttemptID(h))
	}
	res, err := s.Svc.Heartbeat(r.Context(), p, domain.WorkerState(req.State), held)
	if err != nil {
		return err
	}
	out := HeartbeatResponse{Renewed: []string{}, Stop: res.Stop, Abandon: []string{}, LeaseTTLS: int64(res.LeaseTTL / time.Second)}
	if out.Stop == nil {
		out.Stop = []application.StopOrder{}
	}
	for _, a := range res.Renewed {
		out.Renewed = append(out.Renewed, string(a))
	}
	for _, a := range res.Abandon {
		out.Abandon = append(out.Abandon, string(a))
	}
	return writeJSON(w, 200, out)
}

func (s *Server) claim(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	var req ClaimRequest
	if err := decode(r, &req); err != nil {
		return err
	}
	ds, err := s.Svc.ClaimDispatches(r.Context(), p, req.Max)
	if err != nil {
		return err
	}
	if ds == nil {
		ds = []application.Dispatch{}
	}
	return writeJSON(w, 200, ClaimResponse{Dispatches: ds})
}

func (s *Server) ack(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	var d application.Dispatch
	if err := decode(r, &d); err != nil {
		return err
	}
	if string(d.AttemptID) != r.PathValue("id") {
		return fmt.Errorf("%w: path and body attempt ids differ", application.ErrInvalid)
	}
	res, err := s.Svc.Ack(r.Context(), p, d)
	if err != nil {
		return err
	}
	return writeJSON(w, 200, AckResponse{Action: string(res)})
}

func (s *Server) events(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	var req EventsRequest
	if err := decode(r, &req); err != nil {
		return err
	}
	o, err := s.Svc.ReportEvents(r.Context(), p, domain.AttemptID(r.PathValue("id")), req.Events)
	if err != nil {
		return err
	}
	return writeJSON(w, 200, EventsResponse{Outcome: string(o)})
}

func (s *Server) upload(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	off, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil || off < 0 {
		return fmt.Errorf("%w: offset query parameter is required", application.ErrInvalid)
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("%w: %v", application.ErrInvalid, err)
	}
	n, err := s.Svc.UploadChunk(r.Context(), p, domain.AttemptID(r.PathValue("id")), r.PathValue("name"), off, data)
	if err != nil {
		return err
	}
	return writeJSON(w, 200, UploadResponse{Offset: n})
}

func (s *Server) completeArtifact(w http.ResponseWriter, r *http.Request, p application.Principal) error {
	var req CompleteArtifactRequest
	if err := decode(r, &req); err != nil {
		return err
	}
	if err := s.Svc.CompleteArtifact(r.Context(), p, domain.AttemptID(r.PathValue("id")), r.PathValue("name"), req.SHA256); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
