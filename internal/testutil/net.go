//go:build unit

package testutil

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// FreeUDPAddr returns a localhost address with a currently-free UDP port
func FreeUDPAddr(t *testing.T) string {
	t.Helper()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := pc.LocalAddr().String()
	err = pc.Close()
	require.NoError(t, err)

	return addr
}
