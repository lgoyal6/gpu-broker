package grpcgateway

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
)

// TLSFiles pins a run's trust root and local certificate. ServerName is the
// expected server identity when a client connects through a loopback tunnel.
type TLSFiles struct {
	CAFile, CertFile, KeyFile, ServerName string
}

func loopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	return host == "localhost" || net.ParseIP(host).IsLoopback()
}

func loadTLS(address string, files TLSFiles, server bool) (*tls.Config, error) {
	if files == (TLSFiles{}) {
		if address == "" || loopbackAddress(address) {
			return nil, nil
		}
		return nil, fmt.Errorf("plaintext gRPC requires a loopback address; configure mutual TLS for %q", address)
	}
	if address == "" || files.CAFile == "" || files.CertFile == "" || files.KeyFile == "" {
		return nil, fmt.Errorf("mutual TLS requires a gRPC address, CA, certificate and key")
	}
	cert, err := tls.LoadX509KeyPair(files.CertFile, files.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load gRPC certificate/key: %w", err)
	}
	root, err := os.ReadFile(files.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read gRPC CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(root) {
		return nil, fmt.Errorf("gRPC CA contains no valid certificates")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}}
	if server {
		config.ClientCAs = pool
		config.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		config.RootCAs = pool
		config.ServerName = files.ServerName
	}
	return config, nil
}

// ServerTLSConfig returns nil only for a disabled or loopback plaintext listener.
func ServerTLSConfig(address string, files TLSFiles) (*tls.Config, error) {
	return loadTLS(address, files, true)
}

// ClientTLSConfig verifies both peers against the explicit run CA.
func ClientTLSConfig(target string, files TLSFiles) (*tls.Config, error) {
	return loadTLS(target, files, false)
}
