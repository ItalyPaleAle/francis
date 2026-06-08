package channelbind

import (
	"fmt"

	"github.com/quic-go/webtransport-go"
)

// exporterLabel is the RFC 5705 exporter label for Francis channel binding
// Mixing this value into the bootstrap proofs binds a proof to one specific TLS session
const exporterLabel = "EXPORTER-francis-bootstrap-v1"

// exporterLength is the number of bytes of keying material to export
const exporterLength = 32

// Export returns the channel-binding value for a WebTransport session
// Both endpoints of the same session derive identical bytes, while a man-in-the-middle that terminates TLS sees a different value, so a proof bound to this value cannot be relayed onto another connection
func Export(session *webtransport.Session) ([]byte, error) {
	// The exported keying material is only available once the TLS handshake has completed, which is always the case by the time application streams are open
	state := session.SessionState().ConnectionState.TLS
	cb, err := state.ExportKeyingMaterial(exporterLabel, nil, exporterLength)
	if err != nil {
		return nil, fmt.Errorf("failed to export TLS keying material: %w", err)
	}
	return cb, nil
}
