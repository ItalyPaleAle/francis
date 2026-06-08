package bootstrapauth

import (
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
)

// minPSKLen is the minimum acceptable host PSK length in bytes
const minPSKLen = 16

// pskClientInfo and pskServerInfo domain-separate the two MAC subkeys so the host and runtime proofs use different keys
// Distinct keys per direction are what defeat a reflection attack, where an attacker bounces one side's proof back as the other side's
const (
	pskClientInfo = "francis-bootstrap-client"
	pskServerInfo = "francis-bootstrap-server"
)

// PSK holds the two direction-separated MAC subkeys derived from a host pre-shared key
type PSK struct {
	kClient []byte
	kServer []byte
}

// NewPSK derives the client and server subkeys from a host PSK
func NewPSK(hostPSK []byte) (*PSK, error) {
	if len(hostPSK) < minPSKLen {
		return nil, fmt.Errorf("host PSK must be at least %d bytes", minPSKLen)
	}

	kClient, err := hkdf.Key(sha256.New, hostPSK, nil, pskClientInfo, sha256.Size)
	if err != nil {
		return nil, fmt.Errorf("failed to derive client subkey: %w", err)
	}
	kServer, err := hkdf.Key(sha256.New, hostPSK, nil, pskServerInfo, sha256.Size)
	if err != nil {
		return nil, fmt.Errorf("failed to derive server subkey: %w", err)
	}
	return &PSK{kClient: kClient, kServer: kServer}, nil
}

// ClientProof computes the host's proof, binding both nonces and the channel-binding value
func (p *PSK) ClientProof(cb, clientNonce, serverNonce []byte) []byte {
	return mac(p.kClient, transcript(cb, clientNonce, serverNonce))
}

// ServerProof computes the runtime's proof, which proves the runtime also holds the host PSK and observed the same TLS session
func (p *PSK) ServerProof(cb, clientNonce, serverNonce []byte) []byte {
	return mac(p.kServer, transcript(cb, clientNonce, serverNonce))
}

// VerifyClientProof checks a host proof in constant time
func (p *PSK) VerifyClientProof(cb, clientNonce, serverNonce, proof []byte) bool {
	want := p.ClientProof(cb, clientNonce, serverNonce)
	return subtle.ConstantTimeCompare(want, proof) == 1
}

// VerifyServerProof checks a runtime proof in constant time
func (p *PSK) VerifyServerProof(cb, clientNonce, serverNonce, proof []byte) bool {
	want := p.ServerProof(cb, clientNonce, serverNonce)
	return subtle.ConstantTimeCompare(want, proof) == 1
}

// transcript builds the MAC input by length-prefixing each field, so two different field combinations can never encode to the same bytes
func transcript(cb, clientNonce, serverNonce []byte) []byte {
	out := make([]byte, 0, 12+len(cb)+len(clientNonce)+len(serverNonce))
	out = appendField(out, clientNonce)
	out = appendField(out, serverNonce)
	out = appendField(out, cb)
	return out
}

// appendField appends a 4-byte big-endian length followed by the field bytes
func appendField(dst, field []byte) []byte {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(field)))
	dst = append(dst, l[:]...)
	dst = append(dst, field...)
	return dst
}

// mac computes HMAC-SHA256 over msg with the given key
func mac(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(msg)
	return m.Sum(nil)
}
