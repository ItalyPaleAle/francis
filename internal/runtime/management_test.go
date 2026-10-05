package runtime

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdvertiseFromBind(t *testing.T) {
	hostIP := func(ip string) func() (string, error) {
		return func() (string, error) {
			return ip, nil
		}
	}
	noHostIP := func() (string, error) {
		return "", errors.New("no host IP")
	}

	tests := []struct {
		name      string
		bind      string
		getHostIP func() (string, error)
		want      string
		wantErr   bool
	}{
		{name: "no host is replaced", bind: ":8443", getHostIP: hostIP("10.0.0.5"), want: "10.0.0.5:8443"},
		{name: "unspecified IPv4 is replaced", bind: "0.0.0.0:8443", getHostIP: hostIP("10.0.0.5"), want: "10.0.0.5:8443"},
		{name: "unspecified IPv6 is replaced", bind: "[::]:8443", getHostIP: hostIP("10.0.0.5"), want: "10.0.0.5:8443"},
		{name: "detected IPv6 is bracketed", bind: ":8443", getHostIP: hostIP("2600::1"), want: "[2600::1]:8443"},
		{name: "specific IP is kept", bind: "10.0.0.7:8443", getHostIP: noHostIP, want: "10.0.0.7:8443"},
		{name: "loopback is kept", bind: "127.0.0.1:8443", getHostIP: noHostIP, want: "127.0.0.1:8443"},
		{name: "hostname is kept", bind: "runtime.example.com:8443", getHostIP: noHostIP, want: "runtime.example.com:8443"},
		{name: "detection failure is returned", bind: ":8443", getHostIP: noHostIP, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := advertiseFromBind(tt.bind, tt.getHostIP)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
