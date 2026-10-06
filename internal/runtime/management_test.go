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

func TestAdvertiseWithBindPort(t *testing.T) {
	tests := []struct {
		name      string
		advertise string
		bind      string
		want      string
		wantErr   bool
	}{
		{name: "IPv4 with port is kept", advertise: "10.0.0.5:9000", bind: ":8443", want: "10.0.0.5:9000"},
		{name: "hostname with port is kept", advertise: "francis-0.example.com:9000", bind: ":8443", want: "francis-0.example.com:9000"},
		{name: "bracketed IPv6 with port is kept", advertise: "[fd00::5]:9000", bind: ":8443", want: "[fd00::5]:9000"},
		{name: "IPv4 gets the bind port", advertise: "10.0.0.5", bind: ":8443", want: "10.0.0.5:8443"},
		{name: "hostname gets the bind port", advertise: "francis-0.example.com", bind: "0.0.0.0:7400", want: "francis-0.example.com:7400"},
		{name: "bare IPv6 gets the bind port", advertise: "fd00::5", bind: "[::]:8443", want: "[fd00::5]:8443"},
		{name: "bracketed IPv6 gets the bind port", advertise: "[fd00::5]", bind: ":8443", want: "[fd00::5]:8443"},
		{name: "trailing colon gets the bind port", advertise: "francis-0.example.com:", bind: ":8443", want: "francis-0.example.com:8443"},
		{name: "empty host is rejected", advertise: ":", bind: ":8443", wantErr: true},
		{name: "too many colons is rejected", advertise: "a:b:c", bind: ":8443", wantErr: true},
		{name: "unbalanced bracket is rejected", advertise: "[fd00::5", bind: ":8443", wantErr: true},
		{name: "invalid bind is rejected", advertise: "10.0.0.5", bind: "8443", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := advertiseWithBindPort(tt.advertise, tt.bind)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
