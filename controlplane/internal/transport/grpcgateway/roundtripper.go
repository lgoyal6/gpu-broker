package grpcgateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// RoundTripper lets the existing generated worker client use the gRPC
// lifecycle transport without changing its OpenAPI request/response types.
type RoundTripper struct {
	Client WorkerGatewayClient
}

func (t RoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.Client == nil {
		return nil, ErrUnavailable
	}
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(io.LimitReader(req.Body, maxRequestBytes+1))
		if err != nil {
			return nil, err
		}
	}
	if len(body) > maxRequestBytes {
		return nil, ErrTooLarge
	}
	ctx := req.Context()
	if auth := req.Header.Get("Authorization"); auth != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", auth)
	}
	path := req.URL.Path
	parts := strings.Split(strings.Trim(path, "/"), "/")
	var out *LifecycleResponse
	var err error
	switch {
	case req.Method == http.MethodPost && path == "/v1/workers/register":
		var in struct {
			Name     string   `json:"name"`
			GPUModel string   `json:"gpu_model"`
			GPUs     int32    `json:"gpus"`
			GPUMemGB int32    `json:"gpu_mem_gb"`
			Runtimes []string `json:"runtimes"`
		}
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
		out, err = t.Client.RegisterWorker(ctx, &WorkerRegistrationRequest{Name: in.Name, GpuModel: in.GPUModel, Gpus: in.GPUs, GpuMemGb: in.GPUMemGB, Runtimes: in.Runtimes})
	case req.Method == http.MethodPost && path == "/v1/workers/heartbeat":
		var in struct {
			State string   `json:"state"`
			Held  []string `json:"held"`
		}
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
		out, err = t.Client.Heartbeat(ctx, &HeartbeatRequest{State: in.State, HeldAttempts: in.Held})
	case req.Method == http.MethodPost && path == "/v1/workers/claim":
		var in struct {
			Max int32 `json:"max"`
		}
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
		out, err = t.Client.ClaimDispatches(ctx, &ClaimDispatchesRequest{Max: in.Max})
	case req.Method == http.MethodPost && len(parts) == 4 && parts[0] == "v1" && parts[1] == "attempts" && parts[3] == "ack":
		out, err = t.Client.AckDispatch(ctx, &AckDispatchRequest{AttemptId: parts[2], DispatchJson: body})
	case req.Method == http.MethodPost && len(parts) == 4 && parts[0] == "v1" && parts[1] == "attempts" && parts[3] == "events":
		stream, streamErr := t.Client.StreamEvents(ctx)
		if streamErr != nil {
			return nil, streamErr
		}
		const eventChunk = 64 << 10
		if len(body) == 0 {
			streamErr = stream.Send(&EventChunk{AttemptId: parts[2], Final: true})
		} else {
			for off := 0; off < len(body); off += eventChunk {
				end := min(off+eventChunk, len(body))
				streamErr = stream.Send(&EventChunk{AttemptId: parts[2], EventsJson: body[off:end], Final: end == len(body)})
				if streamErr != nil {
					break
				}
			}
		}
		if streamErr != nil {
			return nil, streamErr
		}
		out, err = stream.CloseAndRecv()
	case req.Method == http.MethodPut && len(parts) == 5 && parts[0] == "v1" && parts[1] == "attempts" && parts[3] == "artifacts":
		offset, parseErr := strconv.ParseInt(req.URL.Query().Get("offset"), 10, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid artifact offset: %w", parseErr)
		}
		out, err = t.Client.UploadArtifactChunk(ctx, &UploadArtifactChunkRequest{AttemptId: parts[2], Name: parts[4], Offset: offset, Chunk: body})
	case req.Method == http.MethodPost && len(parts) == 6 && parts[0] == "v1" && parts[1] == "attempts" && parts[3] == "artifacts" && parts[5] == "complete":
		var in struct {
			SHA256 string `json:"sha256"`
		}
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
		out, err = t.Client.CompleteArtifact(ctx, &CompleteArtifactRequest{AttemptId: parts[2], Name: parts[4], Sha256: in.SHA256})
	default:
		return nil, fmt.Errorf("no typed worker gRPC method for %s %s", req.Method, path)
	}
	if err != nil {
		return nil, err
	}
	resp := &http.Response{StatusCode: int(out.Status), Status: strconv.Itoa(int(out.Status)) + " " + http.StatusText(int(out.Status)), Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(out.Body)), Request: req, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, ContentLength: int64(len(out.Body))}
	for k, v := range out.Headers {
		resp.Header.Set(k, v)
	}
	return resp, nil
}

var (
	ErrUnavailable = errors.New("gRPC worker gateway unavailable")
	ErrTooLarge    = &http.MaxBytesError{Limit: maxRequestBytes}
)

func Dial(ctx context.Context, target string) (*grpc.ClientConn, error) {
	return DialTLS(ctx, target, nil)
}

func DialTLS(ctx context.Context, target string, config *tls.Config) (*grpc.ClientConn, error) {
	if config == nil && !loopbackAddress(target) {
		return nil, fmt.Errorf("plaintext gRPC requires a loopback target")
	}
	transport := insecure.NewCredentials()
	if config != nil {
		transport = credentials.NewTLS(config)
	}
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return grpc.DialContext(bounded, target, grpc.WithTransportCredentials(transport), grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRequestBytes+1<<20), grpc.MaxCallSendMsgSize(maxRequestBytes+1<<20)))
}
