//go:build integration

// Package ports provides free UDP port allocation for integration tests
//
// Hosts and runtimes serve over WebTransport (HTTP/3 over QUIC), which is UDP, so ports are reserved on the UDP stack
// Reserved ports are chosen from below the OS ephemeral range so that an outbound dial's ephemeral source port can never collide with a server port a sibling process is about to bind, which otherwise crashes quic-go during accept
package ports

import (
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// reserveWindowLow is the lowest port the allocator will hand out, staying clear of well-known ports
const reserveWindowLow = 10000

// Reserve returns n free UDP ports on 127.0.0.1
// Each port is probed to confirm it is bindable and is distinct from the others, and all are below the OS ephemeral range
func Reserve(t *testing.T, n int) []int {
	t.Helper()

	high := ephemeralMin() - 1
	require.Greater(t, high, reserveWindowLow, "no usable port window below the ephemeral range")

	ports := make([]int, 0, n)
	seen := make(map[int]struct{}, n)

	// Probe random ports in the window until n distinct, currently-free ones are found
	for len(ports) < n {
		// #nosec G404 -- Random generator not used for security-related reasons
		port := reserveWindowLow + rand.IntN(high-reserveWindowLow+1)
		_, dup := seen[port]
		if dup {
			continue
		}

		free := probe(port)
		if !free {
			continue
		}

		seen[port] = struct{}{}
		ports = append(ports, port)
	}

	return ports
}

// probe reports whether a UDP port on 127.0.0.1 is currently bindable
// The socket is closed immediately, but because the port sits below the ephemeral range no outbound dial in this process will claim it before a server binds it
func probe(port int) bool {
	pc, err := net.ListenPacket("udp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		return false
	}
	_ = pc.Close()
	return true
}

// ephemeralMin returns the lowest port the OS uses for ephemeral (outbound) connections
// It reads the Linux setting and falls back to the common default when unavailable
func ephemeralMin() int {
	const fallback = 32768

	data, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		return fallback
	}

	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return fallback
	}

	low, err := strconv.Atoi(fields[0])
	if err != nil || low <= reserveWindowLow {
		return fallback
	}

	return low
}
