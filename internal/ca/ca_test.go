package ca

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"testing"
	"time"
)

func TestDeriveCADeterministic(t *testing.T) {
	psk := []byte("runtime-psk-abcdefghijklmnop")

	// Deriving twice from the same PSK must produce a byte-identical certificate, since hosts pin this cert out-of-band
	a, err := DeriveCA(psk)
	if err != nil {
		t.Fatalf("DeriveCA: %v", err)
	}
	b, err := DeriveCA(psk)
	if err != nil {
		t.Fatalf("DeriveCA: %v", err)
	}
	if !bytes.Equal(a.certDER, b.certDER) {
		t.Fatal("expected identical CA certificates from the same PSK")
	}

	// A different PSK must produce a different CA
	c, err := DeriveCA([]byte("a-totally-different-psk-value"))
	if err != nil {
		t.Fatalf("DeriveCA: %v", err)
	}
	if bytes.Equal(a.certDER, c.certDER) {
		t.Fatal("expected different CA certificates from different PSKs")
	}
}

func TestIssueWorkloadCertVerifies(t *testing.T) {
	root, err := DeriveCA([]byte("runtime-psk-abcdefghijklmnop"))
	if err != nil {
		t.Fatalf("DeriveCA: %v", err)
	}

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	der, err := root.IssueWorkloadCert(HostURI("host-1"), pub, time.Hour)
	if err != nil {
		t.Fatalf("IssueWorkloadCert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}

	// The leaf must chain to the CA
	pool := NewCertPool([]*CA{root})
	_, err = leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	if err != nil {
		t.Fatalf("leaf does not verify against CA: %v", err)
	}

	// The leaf must carry the exact SPIFFE identity
	id, err := SPIFFEIDFromCert(leaf)
	if err != nil {
		t.Fatalf("SPIFFEIDFromCert: %v", err)
	}
	if id.String() != "spiffe://francis/host/host-1" {
		t.Fatalf("unexpected SPIFFE id %q", id.String())
	}
}

func TestHostIDFromCert(t *testing.T) {
	root, err := DeriveCA([]byte("runtime-psk-abcdefghijklmnop"))
	if err != nil {
		t.Fatalf("DeriveCA: %v", err)
	}
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	// A host certificate yields its host ID
	hostDER, err := root.IssueWorkloadCert(HostURI("host-1"), pub, time.Hour)
	if err != nil {
		t.Fatalf("IssueWorkloadCert: %v", err)
	}
	hostLeaf, err := x509.ParseCertificate(hostDER)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	got, err := HostIDFromCert(hostLeaf)
	if err != nil {
		t.Fatalf("HostIDFromCert: %v", err)
	}
	if got != "host-1" {
		t.Fatalf("unexpected host ID %q", got)
	}

	// A runtime certificate is not a host identity and must be rejected
	runtimeDER, err := root.IssueWorkloadCert(RuntimeURI("runtime-1"), pub, time.Hour)
	if err != nil {
		t.Fatalf("IssueWorkloadCert: %v", err)
	}
	runtimeLeaf, err := x509.ParseCertificate(runtimeDER)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	_, err = HostIDFromCert(runtimeLeaf)
	if err == nil {
		t.Fatal("expected a runtime certificate to be rejected as a host identity")
	}
}

func TestVerifyPeerSPIFFE(t *testing.T) {
	root, err := DeriveCA([]byte("runtime-psk-abcdefghijklmnop"))
	if err != nil {
		t.Fatalf("DeriveCA: %v", err)
	}
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	der, err := root.IssueWorkloadCert(HostURI("host-1"), pub, time.Hour)
	if err != nil {
		t.Fatalf("IssueWorkloadCert: %v", err)
	}

	pool := NewCertPool([]*CA{root})
	roots := func() *x509.CertPool { return pool }

	// Accept when the namespace matches and no specific peer is pinned
	verify := VerifyPeerSPIFFE(roots, HostPrefix, nil)
	err = verify([][]byte{der}, nil)
	if err != nil {
		t.Fatalf("expected accept, got %v", err)
	}

	// Accept when pinned to the matching host ID
	verifyPinned := VerifyPeerSPIFFE(roots, HostPrefix, func() string { return "host-1" })
	err = verifyPinned([][]byte{der}, nil)
	if err != nil {
		t.Fatalf("expected accept for pinned host, got %v", err)
	}

	// Reject when pinned to a different host ID
	verifyWrong := VerifyPeerSPIFFE(roots, HostPrefix, func() string { return "host-2" })
	err = verifyWrong([][]byte{der}, nil)
	if err == nil {
		t.Fatal("expected rejection for mismatched host ID")
	}

	// Reject a runtime-namespaced identity when a host is expected
	verifyNs := VerifyPeerSPIFFE(roots, RuntimePrefix, nil)
	err = verifyNs([][]byte{der}, nil)
	if err == nil {
		t.Fatal("expected rejection for wrong namespace")
	}

	// Reject a cert signed by an untrusted CA
	other, err := DeriveCA([]byte("a-totally-different-psk-value"))
	if err != nil {
		t.Fatalf("DeriveCA: %v", err)
	}
	otherDER, err := other.IssueWorkloadCert(HostURI("host-1"), pub, time.Hour)
	if err != nil {
		t.Fatalf("IssueWorkloadCert: %v", err)
	}
	err = verify([][]byte{otherDER}, nil)
	if err == nil {
		t.Fatal("expected rejection for untrusted CA")
	}
}
