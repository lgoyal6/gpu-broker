package grpcgateway

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func gatewayClient(t *testing.T, h http.Handler) WorkerGatewayClient {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	RegisterWorkerGatewayServer(s, &Server{Handler: h})
	go func() { _ = s.Serve(l) }()
	t.Cleanup(func() { s.Stop(); _ = l.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := grpc.DialContext(ctx, l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return NewWorkerGatewayClient(c)
}

func TestCallForwardsWorkerLifecycleAndAuth(t *testing.T) {
	client := gatewayClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/workers/heartbeat" || r.Header.Get("Authorization") != "Bearer worker-secret" {
			http.Error(w, "missing worker auth or route", http.StatusUnauthorized)
			return
		}
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"echo":"` + strings.TrimSpace(string(body)) + `"}`))
	}))
	resp, err := client.Call(context.Background(), &HttpRequest{Method: http.MethodPost, PathAndQuery: "/v1/workers/heartbeat", Headers: map[string]string{"Authorization": "Bearer worker-secret", "Content-Type": "application/json"}, Body: []byte(`{"state":"READY"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != http.StatusAccepted || !strings.Contains(string(resp.Body), `"state":"READY"`) || resp.Headers["Content-Type"] != "application/json" {
		t.Fatalf("unexpected gRPC lifecycle response: %+v", resp)
	}
}

func TestCallBoundsRouteHeadersBodyAndDeadline(t *testing.T) {
	client := gatewayClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	_, err := client.Call(context.Background(), &HttpRequest{Method: http.MethodGet, PathAndQuery: "/v1/jobs"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("jobs route status = %v, want PermissionDenied", err)
	}
	_, err = client.Call(context.Background(), &HttpRequest{Method: http.MethodGet, PathAndQuery: "/v1/workers/heartbeat", Headers: map[string]string{"Cookie": "secret"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unapproved header status = %v, want InvalidArgument", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = client.Call(ctx, &HttpRequest{Method: http.MethodGet, PathAndQuery: "/v1/workers/heartbeat"})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("deadline status = %v, want DeadlineExceeded", err)
	}
}

func TestWatchJobStreamsChangesAndStopsAtTerminalState(t *testing.T) {
	var calls atomic.Int32
	client := gatewayClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/jobs/job_1" || r.Header.Get("Authorization") != "Bearer user-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		state := "RUNNING"
		if calls.Add(1) > 1 {
			state = "SUCCEEDED"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"job":{"state":"`+state+`"}}`)
	}))
	ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer user-token")), 3*time.Second)
	defer cancel()
	stream, err := client.WatchJob(ctx, &WatchJobRequest{JobId: "job_1", IntervalMs: 250})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil || !strings.Contains(string(first.Body), "RUNNING") {
		t.Fatalf("first update = %+v, err=%v", first, err)
	}
	second, err := stream.Recv()
	if err != nil || !strings.Contains(string(second.Body), "SUCCEEDED") {
		t.Fatalf("terminal update = %+v, err=%v", second, err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("terminal stream error = %v, want EOF", err)
	}
}

func TestStreamEventsBoundsChunksAndBlockedReceiveByClientDeadline(t *testing.T) {
	client := gatewayClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("incomplete or oversized event stream reached the HTTP handler")
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	stream, err := client.StreamEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&EventChunk{AttemptId: "attempt_1", EventsJson: []byte("["), Final: false}); err != nil {
		t.Fatal(err)
	}
	// The server is waiting for the next chunk. The client's bounded stream
	// context must release its blocking Recv when the deadline expires.
	<-ctx.Done()
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("stalled event stream error = %v, want DeadlineExceeded", err)
	}

	bounded, boundedCancel := context.WithTimeout(context.Background(), time.Second)
	defer boundedCancel()
	tooMany, err := client.StreamEvents(bounded)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= maxEventChunks; i++ {
		chunk := &EventChunk{AttemptId: "attempt_1", EventsJson: []byte("x"), Final: i == maxEventChunks}
		if err := tooMany.Send(chunk); err != nil {
			break // The server may reject the stream before the next Send returns.
		}
	}
	if _, err := tooMany.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("over-limit event chunk count error = %v, want InvalidArgument", err)
	}
}

func TestStreamsRequireDeadlinesAndSlowWatchConsumerUnblocksOnCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ended := make(chan struct{}, 4)
	var watchCalls atomic.Int32
	srv := grpc.NewServer(grpc.ChainStreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, stream)
		ended <- struct{}{}
		return err
	}))
	RegisterWorkerGatewayServer(srv, &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seq := watchCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"job":{"state":"RUNNING"},"seq":`+strconv.FormatInt(int64(seq), 10)+`,"padding":"`+strings.Repeat("x", 3<<20)+`"}`)
	})})
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { srv.Stop(); _ = ln.Close() })
	ctx, dialCancel := context.WithTimeout(context.Background(), time.Second)
	defer dialCancel()
	conn, err := grpc.DialContext(ctx, ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := NewWorkerGatewayClient(conn)
	watch, err := client.WatchJob(context.Background(), &WatchJobRequest{JobId: "job_1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unbounded watch error = %v, want FailedPrecondition", err)
	}
	noDeadline, err := client.StreamEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := noDeadline.Send(&EventChunk{AttemptId: "attempt_1", EventsJson: []byte("[]"), Final: true}); err != nil {
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("unbounded event stream send error = %v", err)
		}
	} else if _, err := noDeadline.CloseAndRecv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unbounded event stream error = %v, want FailedPrecondition", err)
	}
	for range 2 {
		select {
		case <-ended:
		case <-time.After(time.Second):
			t.Fatal("unbounded stream did not return promptly")
		}
	}

	watchCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = client.WatchJob(watchCtx, &WatchJobRequest{JobId: "job_1", IntervalMs: 250})
	if err != nil {
		t.Fatal(err)
	}
	// Leave the response unread so the server cannot rely on the client
	// draining the stream. Its deadline must release any blocked Send.
	<-watchCtx.Done()
	if watchCalls.Load() < 2 {
		t.Fatalf("watch requests = %d, want repeated updates during unread stream", watchCalls.Load())
	}
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("server stream remained blocked after client deadline")
	}
}
