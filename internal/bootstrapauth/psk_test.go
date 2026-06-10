package bootstrapauth

import (
	"testing"
)

func TestPSKChallengeResponse(t *testing.T) {
	hostPSK := []byte("host-psk-abcdefghijklmnop")
	p, err := NewPSK(hostPSK)
	if err != nil {
		t.Fatalf("NewPSK: %v", err)
	}

	cb := []byte("channel-binding-value-0123456789")
	cn, err := Nonce()
	if err != nil {
		t.Fatalf("Nonce: %v", err)
	}
	sn, err := Nonce()
	if err != nil {
		t.Fatalf("Nonce: %v", err)
	}

	clientProof := p.ClientProof(cb, cn, sn)
	serverProof := p.ServerProof(cb, cn, sn)

	// Both proofs verify against the matching channel binding and nonces
	if !p.VerifyClientProof(cb, cn, sn, clientProof) {
		t.Fatal("client proof should verify")
	}
	if !p.VerifyServerProof(cb, cn, sn, serverProof) {
		t.Fatal("server proof should verify")
	}

	// A tampered channel binding must fail, which is what defeats a MITM that terminates TLS
	badCB := []byte("a-different-channel-binding-val1")
	if p.VerifyClientProof(badCB, cn, sn, clientProof) {
		t.Fatal("client proof must fail with a different channel binding")
	}

	// The client and server proofs must not be interchangeable, which is what defeats a reflection attack
	if p.VerifyServerProof(cb, cn, sn, clientProof) {
		t.Fatal("client proof must not verify as a server proof")
	}
	if p.VerifyClientProof(cb, cn, sn, serverProof) {
		t.Fatal("server proof must not verify as a client proof")
	}

	// A different host PSK must not verify
	other, err := NewPSK([]byte("other-psk-abcdefghijklmnop"))
	if err != nil {
		t.Fatalf("NewPSK: %v", err)
	}
	if other.VerifyClientProof(cb, cn, sn, clientProof) {
		t.Fatal("proof must not verify under a different PSK")
	}
}

func TestNewPSKTooShort(t *testing.T) {
	_, err := NewPSK([]byte("short"))
	if err == nil {
		t.Fatal("expected error for a too-short PSK")
	}
}
