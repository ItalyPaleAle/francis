package certholder

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"sync/atomic"
)

// errNoCertificate is returned by GetCertificate when the holder has no certificate yet
var errNoCertificate = errors.New("no workload certificate available")

// Holder stores the current workload certificate and trust-anchor pool, both swappable atomically
// New TLS handshakes read the live values through the Get* callbacks and a roots accessor, so a rotation never rebuilds a tls.Config and never drops a live connection
type Holder struct {
	cert  atomic.Pointer[tls.Certificate]
	roots atomic.Pointer[x509.CertPool]
}

// New returns a Holder seeded with an optional certificate and trust pool
func New(cert *tls.Certificate, roots *x509.CertPool) *Holder {
	h := &Holder{}
	if cert != nil {
		h.cert.Store(cert)
	}
	if roots != nil {
		h.roots.Store(roots)
	}
	return h
}

// SetCertificate atomically swaps in a freshly issued workload certificate
func (h *Holder) SetCertificate(cert *tls.Certificate) {
	h.cert.Store(cert)
}

// SetRoots atomically swaps in an updated trust bundle, which happens when the runtime publishes a new set of anchors during a root rotation
func (h *Holder) SetRoots(roots *x509.CertPool) {
	h.roots.Store(roots)
}

// Certificate returns the current workload certificate, or nil if none has been installed yet
func (h *Holder) Certificate() *tls.Certificate {
	return h.cert.Load()
}

// Roots returns the current trust bundle, read by the SPIFFE peer verifier on every handshake
func (h *Holder) Roots() *x509.CertPool {
	return h.roots.Load()
}

// GetClientCertificate supplies the current workload certificate during a client handshake
func (h *Holder) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	cert := h.cert.Load()
	if cert == nil {
		// An empty certificate tells the TLS stack we have nothing to present, which is the case during the very first bootstrap before a cert has been issued
		return &tls.Certificate{}, nil
	}
	return cert, nil
}

// GetCertificate supplies the current workload certificate during a server handshake
func (h *Holder) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	cert := h.cert.Load()
	if cert == nil {
		return nil, errNoCertificate
	}
	return cert, nil
}
