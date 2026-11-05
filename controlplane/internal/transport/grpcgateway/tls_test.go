package grpcgateway

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/testkit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

func TestTLSRequiresCompleteIdentityAndLoopbackPlaintext(t *testing.T) {
	for _, address := range []string{"127.0.0.1:9000", "[::1]:9000", "localhost:9000", ""} {
		if cfg, err := ClientTLSConfig(address, TLSFiles{}); cfg != nil || err != nil {
			t.Fatalf("loopback/disabled %q: cfg=%v err=%v", address, cfg, err)
		}
	}
	for _, address := range []string{":9000", "0.0.0.0:9000", "remote.example:9000", "dns:///127.0.0.1:9000"} {
		for _, load := range []func(string, TLSFiles) (*tls.Config, error){ServerTLSConfig, ClientTLSConfig} {
			if _, err := load(address, TLSFiles{}); err == nil {
				t.Fatalf("plaintext non-loopback accepted: %q", address)
			}
		}
	}
	for _, files := range []TLSFiles{{CAFile: "ca"}, {CertFile: "cert", KeyFile: "key"}, {ServerName: "name"}} {
		if _, err := ClientTLSConfig("127.0.0.1:9000", files); err == nil {
			t.Fatalf("incomplete TLS identity accepted: %+v", files)
		}
	}
	if _, err := Dial(context.Background(), "remote.example:9000"); err == nil {
		t.Fatal("plaintext remote dial accepted")
	}
}

func TestMutualTLSVerifiesBothPeersAndPreservesWorkerAuth(t *testing.T) {
	serverPaths, clientPaths := testkit.MutualTLSFiles(t)
	serverFiles, clientFiles := TLSFiles(serverPaths), TLSFiles(clientPaths)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, err := ServerTLSConfig(l.Addr().String(), serverFiles)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	RegisterWorkerGatewayServer(s, &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer test-worker" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})})
	go func() { _ = s.Serve(l) }()
	t.Cleanup(func() { s.Stop(); _ = l.Close() })
	clientTLS, err := ClientTLSConfig(l.Addr().String(), clientFiles)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := DialTLS(ctx, l.Addr().String(), clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := NewWorkerGatewayClient(conn)
	for _, auth := range []bool{false, true} {
		c := ctx
		expected := http.StatusUnauthorized
		if auth {
			c = metadata.AppendToOutgoingContext(c, "authorization", "Bearer test-worker")
			expected = http.StatusAccepted
		}
		response, err := client.Heartbeat(c, &HeartbeatRequest{State: "READY"})
		if err != nil || response.GetStatus() != int32(expected) {
			t.Fatalf("auth=%v response=%v err=%v", auth, response, err)
		}
	}
	_, otherPaths := testkit.MutualTLSFiles(t)
	otherClient := TLSFiles(otherPaths)
	untrusted, err := ClientTLSConfig(l.Addr().String(), otherClient)
	if err != nil {
		t.Fatal(err)
	}
	// Trust the real server, but present a worker certificate from another CA.
	untrusted.RootCAs = clientTLS.RootCAs
	missing := clientTLS.Clone()
	missing.Certificates = nil
	wrongName := clientTLS.Clone()
	wrongName.ServerName = "other-server.local"
	before := calls.Load()
	for name, config := range map[string]*tls.Config{"untrusted worker": untrusted, "missing worker": missing, "wrong server": wrongName} {
		t.Run(name, func(t *testing.T) {
			bounded, stop := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer stop()
			c, err := DialTLS(bounded, l.Addr().String(), config)
			if err == nil {
				defer c.Close()
				t.Fatal("invalid peer reached ready gRPC connection")
			}
		})
	}
	if calls.Load() != before {
		t.Fatal("unauthenticated TLS peer reached lifecycle handler")
	}
}
