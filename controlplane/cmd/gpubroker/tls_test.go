package main

import (
	"context"
	"strings"
	"testing"
)

func TestGRPCFlagsFailClosedBeforeOpeningDatabaseOrCredentials(t *testing.T) {
	for _, c := range []struct {
		role string
		args []string
		want string
	}{
		{"api", []string{"--grpc-addr=0.0.0.0:9443"}, "plaintext gRPC"},
		{"api", []string{"--grpc-addr=127.0.0.1:9443", "--grpc-tls-cert-file=missing.pem"}, "mutual TLS requires"},
		{"agent", []string{"--grpc-target=cloud.example:9443"}, "plaintext gRPC"},
		{"agent", []string{"--grpc-target=127.0.0.1:9443", "--grpc-ca-file=missing.pem"}, "mutual TLS requires"},
	} {
		var err error
		if c.role == "api" {
			err = runAPI(context.Background(), c.args, nil)
		} else {
			err = runAgent(context.Background(), c.args, nil)
		}
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s %v: %v", c.role, c.args, err)
		}
	}
}
