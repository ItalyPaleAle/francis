package netutils

import (
	"net"
	"strings"
)

// IsLoopbackBind reports whether a bind address (host:port) only accepts connections from the same machine
// A malformed address, or one with no host or an unspecified host, listens beyond loopback or not at all, so it reports false
func IsLoopbackBind(bind string) bool {
	host, _, err := net.SplitHostPort(bind)
	if err != nil {
		return false
	}

	if strings.ToLower(host) == "localhost" {
		return true
	}

	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
