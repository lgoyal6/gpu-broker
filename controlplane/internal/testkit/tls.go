package testkit

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TLSIdentityFiles is an ephemeral identity used only by local TLS tests.
type TLSIdentityFiles struct{ CAFile, CertFile, KeyFile, ServerName string }

// MutualTLSFiles creates a fresh CA and server/worker identities under t.TempDir.
func MutualTLSFiles(t testing.TB) (TLSIdentityFiles, TLSIdentityFiles) {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test run CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	root, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: root}), 0600); err != nil {
		t.Fatal(err)
	}
	leaf := func(name string, serial int64, usage x509.ExtKeyUsage) TLSIdentityFiles {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		c := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
			DNSNames: []string{name}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
		der, err := x509.CreateCertificate(rand.Reader, c, ca, &k.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		encodedKey, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			t.Fatal(err)
		}
		f := TLSIdentityFiles{CAFile: caPath, CertFile: filepath.Join(dir, name+".pem"), KeyFile: filepath.Join(dir, name+".key")}
		for path, b := range map[string]*pem.Block{f.CertFile: {Type: "CERTIFICATE", Bytes: der}, f.KeyFile: {Type: "PRIVATE KEY", Bytes: encodedKey}} {
			if err := os.WriteFile(path, pem.EncodeToMemory(b), 0600); err != nil {
				t.Fatal(err)
			}
		}
		return f
	}
	server := leaf("controlplane.local", 2, x509.ExtKeyUsageServerAuth)
	client := leaf("worker.local", 3, x509.ExtKeyUsageClientAuth)
	client.ServerName = "controlplane.local"
	return server, client
}
