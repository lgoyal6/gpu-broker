// Package grpcgateway exposes the authenticated worker lifecycle API over
// gRPC. It delegates to the HTTP contract handler so authentication, request
// validation and lifecycle transitions continue to have one implementation.
package grpcgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const maxRequestBytes = 9 << 20
const maxEventChunks = 16

type Server struct {
	UnimplementedWorkerGatewayServer
	Handler http.Handler
}

func (s *Server) Call(ctx context.Context, in *HttpRequest) (*HttpResponse, error) {
	if s.Handler == nil || in == nil {
		return nil, status.Error(codes.Unavailable, "worker gateway unavailable")
	}
	if in.Method != http.MethodGet && in.Method != http.MethodPost && in.Method != http.MethodPut && in.Method != http.MethodDelete {
		return nil, status.Error(codes.InvalidArgument, "unsupported method")
	}
	if len(in.Body) > maxRequestBytes {
		return nil, status.Error(codes.ResourceExhausted, "request body exceeds 9 MiB")
	}
	path := in.PathAndQuery
	if !strings.HasPrefix(path, "/v1/") || strings.HasPrefix(path, "//") || strings.Contains(path, "\\") {
		return nil, status.Error(codes.InvalidArgument, "path must be a control-plane v1 route")
	}
	// Only worker lifecycle routes cross this adapter. In particular, jobs,
	// operator APIs and artifact downloads remain on the authenticated HTTP API.
	if !strings.HasPrefix(path, "/v1/workers/") && !strings.HasPrefix(path, "/v1/attempts/") {
		return nil, status.Error(codes.PermissionDenied, "route is not available on the worker gateway")
	}
	resp, err := s.invoke(ctx, in.Method, path, in.Headers, in.Body)
	if err != nil {
		return nil, err
	}
	return &HttpResponse{Status: resp.Status, Headers: resp.Headers, Body: resp.Body}, nil
}

func (s *Server) invoke(ctx context.Context, method, path string, headers map[string]string, body []byte) (*LifecycleResponse, error) {
	if s.Handler == nil {
		return nil, status.Error(codes.Unavailable, "worker gateway unavailable")
	}
	if len(body) > maxRequestBytes {
		return nil, status.Error(codes.ResourceExhausted, "request body exceeds 9 MiB")
	}
	deadline := time.Now().Add(30 * time.Second)
	if d, ok := ctx.Deadline(); ok {
		if d.Before(deadline) {
			deadline = d
		}
	}
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	req, err := http.NewRequestWithContext(bounded, method, "http://gpubroker"+path, bytes.NewReader(body))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid worker request")
	}
	for name, value := range headers {
		switch http.CanonicalHeaderKey(name) {
		case "Authorization", "Content-Type", "Accept", "Idempotency-Key", "X-Request-Id":
			req.Header.Set(name, value)
		default:
			return nil, status.Errorf(codes.InvalidArgument, "header %q is not permitted", name)
		}
	}
	if req.Header.Get("Authorization") == "" {
		if md, ok := metadata.FromIncomingContext(bounded); ok {
			if values := md.Get("authorization"); len(values) == 1 {
				req.Header.Set("Authorization", values[0])
			}
		}
	}
	rec := httptest.NewRecorder()
	s.Handler.ServeHTTP(rec, req)
	if err := bounded.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	res := rec.Result()
	defer res.Body.Close()
	responseBody := rec.Body.Bytes()
	if len(responseBody) > maxRequestBytes {
		return nil, status.Error(codes.ResourceExhausted, "response body exceeds 9 MiB")
	}
	responseHeaders := map[string]string{}
	for k, values := range res.Header {
		if len(values) > 0 && (k == "Content-Type" || k == "Retry-After" || k == "X-Request-Id") {
			responseHeaders[k] = values[0]
		}
	}
	return &LifecycleResponse{Status: int32(res.StatusCode), Headers: responseHeaders, Body: append([]byte(nil), responseBody...)}, nil
}

func jsonBody(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid lifecycle payload")
	}
	return b, nil
}

func (s *Server) RegisterWorker(ctx context.Context, in *WorkerRegistrationRequest) (*LifecycleResponse, error) {
	if in == nil || len(in.Runtimes) > 8 {
		return nil, status.Error(codes.InvalidArgument, "invalid worker registration")
	}
	b, _ := jsonBody(map[string]any{"name": in.Name, "gpu_model": in.GpuModel, "gpus": in.Gpus, "gpu_mem_gb": in.GpuMemGb, "runtimes": in.Runtimes})
	return s.invoke(ctx, http.MethodPost, "/v1/workers/register", map[string]string{"Content-Type": "application/json"}, b)
}

func (s *Server) Heartbeat(ctx context.Context, in *HeartbeatRequest) (*LifecycleResponse, error) {
	if in == nil || len(in.HeldAttempts) > 256 {
		return nil, status.Error(codes.InvalidArgument, "heartbeat may hold at most 256 attempts")
	}
	b, _ := jsonBody(map[string]any{"state": in.State, "held": in.HeldAttempts})
	return s.invoke(ctx, http.MethodPost, "/v1/workers/heartbeat", map[string]string{"Content-Type": "application/json"}, b)
}

func (s *Server) ClaimDispatches(ctx context.Context, in *ClaimDispatchesRequest) (*LifecycleResponse, error) {
	if in == nil || in.Max < 0 || in.Max > 16 {
		return nil, status.Error(codes.InvalidArgument, "claim max must be 0..16")
	}
	b, _ := jsonBody(map[string]any{"max": in.Max})
	return s.invoke(ctx, http.MethodPost, "/v1/workers/claim", map[string]string{"Content-Type": "application/json"}, b)
}

func (s *Server) AckDispatch(ctx context.Context, in *AckDispatchRequest) (*LifecycleResponse, error) {
	if in == nil || in.AttemptId == "" || len(in.DispatchJson) > 64<<10 {
		return nil, status.Error(codes.InvalidArgument, "invalid dispatch acknowledgement")
	}
	return s.invoke(ctx, http.MethodPost, "/v1/attempts/"+url.PathEscape(in.AttemptId)+"/ack", map[string]string{"Content-Type": "application/json"}, in.DispatchJson)
}

func (s *Server) ReportEvents(ctx context.Context, in *ReportEventsRequest) (*LifecycleResponse, error) {
	if in == nil || in.AttemptId == "" || len(in.EventsJson) > 1<<20 {
		return nil, status.Error(codes.InvalidArgument, "invalid worker event batch")
	}
	return s.invoke(ctx, http.MethodPost, "/v1/attempts/"+url.PathEscape(in.AttemptId)+"/events", map[string]string{"Content-Type": "application/json"}, in.EventsJson)
}

func (s *Server) StreamEvents(stream WorkerGateway_StreamEventsServer) error {
	if err := requireBoundedStream(stream.Context()); err != nil {
		return err
	}
	var attempt string
	var body []byte
	final := false
	chunks := 0
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		chunks++
		if final || chunk.AttemptId == "" || (attempt != "" && attempt != chunk.AttemptId) ||
			len(chunk.EventsJson) == 0 || chunks > maxEventChunks || len(body)+len(chunk.EventsJson) > 1<<20 {
			return status.Error(codes.InvalidArgument, "event stream must have one attempt, 1..16 nonempty chunks, and fit within 1 MiB")
		}
		attempt = chunk.AttemptId
		body = append(body, chunk.EventsJson...)
		final = chunk.Final
	}
	if !final || attempt == "" || len(body) == 0 {
		return status.Error(codes.InvalidArgument, "event stream ended before its final chunk")
	}
	resp, err := s.invoke(stream.Context(), http.MethodPost, "/v1/attempts/"+url.PathEscape(attempt)+"/events", map[string]string{"Content-Type": "application/json"}, body)
	if err != nil {
		return err
	}
	return stream.SendAndClose(resp)
}

func (s *Server) UploadArtifactChunk(ctx context.Context, in *UploadArtifactChunkRequest) (*LifecycleResponse, error) {
	if in == nil || in.AttemptId == "" || in.Name == "" || in.Offset < 0 || len(in.Chunk) > 1<<20 {
		return nil, status.Error(codes.InvalidArgument, "artifact chunk must be at most 1 MiB with a nonnegative offset")
	}
	path := "/v1/attempts/" + url.PathEscape(in.AttemptId) + "/artifacts/" + url.PathEscape(in.Name) + "?offset=" + strconv.FormatInt(in.Offset, 10)
	return s.invoke(ctx, http.MethodPut, path, map[string]string{"Content-Type": "application/octet-stream"}, in.Chunk)
}

func (s *Server) CompleteArtifact(ctx context.Context, in *CompleteArtifactRequest) (*LifecycleResponse, error) {
	if in == nil || in.AttemptId == "" || in.Name == "" || len(in.Sha256) != 64 {
		return nil, status.Error(codes.InvalidArgument, "invalid artifact completion")
	}
	b, _ := jsonBody(map[string]string{"sha256": in.Sha256})
	path := "/v1/attempts/" + url.PathEscape(in.AttemptId) + "/artifacts/" + url.PathEscape(in.Name) + "/complete"
	return s.invoke(ctx, http.MethodPost, path, map[string]string{"Content-Type": "application/json"}, b)
}

func (s *Server) WatchJob(in *WatchJobRequest, stream WorkerGateway_WatchJobServer) error {
	if in == nil || in.JobId == "" {
		return status.Error(codes.InvalidArgument, "job id is required")
	}
	if err := requireBoundedStream(stream.Context()); err != nil {
		return err
	}
	interval := time.Duration(in.IntervalMs) * time.Millisecond
	if interval < 250*time.Millisecond {
		interval = time.Second
	}
	if interval > 5*time.Second {
		interval = 5 * time.Second
	}
	ctx := stream.Context()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	path := "/v1/jobs/" + url.PathEscape(in.JobId)
	var previous []byte
	totalSent := 0
	for n := 0; n < 60; n++ {
		resp, err := s.invoke(ctx, http.MethodGet, path, nil, nil)
		if err != nil {
			return err
		}
		if !bytes.Equal(previous, resp.Body) {
			totalSent += len(resp.Body)
			if totalSent > 16<<20 {
				return status.Error(codes.ResourceExhausted, "watch stream exceeded 16 MiB")
			}
			if err := stream.Send(resp); err != nil {
				return err // synchronous Send applies network backpressure
			}
			previous = append(previous[:0], resp.Body...)
		}
		var current struct {
			Job struct {
				State string `json:"state"`
			} `json:"job"`
		}
		_ = json.Unmarshal(resp.Body, &current)
		switch current.Job.State {
		case "SUCCEEDED", "FAILED", "CANCELLED", "EXPIRED":
			return nil
		}
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-ticker.C:
		}
	}
	return nil
}

func requireBoundedStream(ctx context.Context) error {
	d, ok := ctx.Deadline()
	if !ok {
		return status.Error(codes.FailedPrecondition, "stream requires a client deadline of 30 seconds or less")
	}
	if time.Until(d) > 30*time.Second {
		return status.Error(codes.InvalidArgument, "stream deadline may not exceed 30 seconds")
	}
	return nil
}

func (s *Server) Validate() error {
	if s.Handler == nil {
		return fmt.Errorf("worker gateway requires an HTTP contract handler")
	}
	return nil
}
