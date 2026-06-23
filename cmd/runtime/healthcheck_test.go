package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/runtime"
)

var (
	healthcheckRuntimePSK = []byte("healthcheck-runtime-psk-0123456789")
	healthcheckHostPSK    = []byte("healthcheck-host-psk-0123456789012345")
)

// startHealthcheckRuntime starts a runtime on a free loopback port and returns its dial address and the runtime PSK the healthcheck must verify against
func startHealthcheckRuntime(t *testing.T) (addr string, psk []byte) {
	t.Helper()

	addr = freeLoopbackUDPAddr(t)
	prov, err := standalone.NewStandaloneMemory(slog.New(slog.DiscardHandler), standalone.StandaloneMemoryOptions{}, components.ProviderConfig{
		HostHealthCheckDeadline:   20 * time.Second,
		AlarmsLeaseDuration:       20 * time.Second,
		AlarmsFetchAheadInterval:  2500 * time.Millisecond,
		AlarmsFetchAheadBatchSize: 25,
	})
	require.NoError(t, err)

	rt, err := runtime.NewRuntime(prov,
		runtime.WithBind(addr),
		runtime.WithRuntimePSKs(healthcheckRuntimePSK),
		runtime.WithHostBootstrapPSK(healthcheckHostPSK),
		runtime.WithAlarmsPollInterval(time.Hour),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		_ = rt.Run(ctx)
	}()
	t.Cleanup(cancel)

	return addr, healthcheckRuntimePSK
}

func writeHealthcheckConfig(t *testing.T, addr string, psk []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	err := os.WriteFile(path, []byte("bind: "+addr+"\nruntimePSKs:\n  - "+string(psk)+"\n"), 0o600)
	require.NoError(t, err)
	return path
}

// freeLoopbackUDPAddr returns a 127.0.0.1 address with a currently-free UDP port
func freeLoopbackUDPAddr(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	a := pc.LocalAddr().String()
	require.NoError(t, pc.Close())
	return a
}

// quietStdoutStderr swaps os.Stdout and os.Stderr for the duration of fn so a failing probe does not spam test output
// It is not safe for parallel tests, which these probes are not
func quietStdoutStderr(t *testing.T, fn func()) {
	t.Helper()
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = devnull.Close()
	})

	origOut := os.Stdout
	origErr := os.Stderr
	os.Stdout = devnull
	os.Stderr = devnull
	t.Cleanup(func() {
		os.Stdout = origOut
		os.Stderr = origErr
	})
	fn()
}

func TestRunHealthcheckSucceedsVerifyingAgainstDerivedCA(t *testing.T) {
	addr, psk := startHealthcheckRuntime(t)
	configPath := writeHealthcheckConfig(t, addr, psk)

	// The runtime starts listening asynchronously, so retry until the probe observes it
	// By default the probe verifies the runtime's SPIFFE certificate against the CA derived from the configured PSKs
	require.Eventually(t, func() bool {
		return runHealthcheck([]string{
			"-config", configPath,
			"-addr", addr,
			"-timeout", "2s",
		}) == 0
	}, 15*time.Second, 100*time.Millisecond, "healthcheck with CA verification should succeed against the running runtime")
}

func TestRunHealthcheckSucceedsInsecureSkipVerify(t *testing.T) {
	addr, _ := startHealthcheckRuntime(t)

	// With -insecure-skip-verify and -addr set, no config is loaded at all
	require.Eventually(t, func() bool {
		return runHealthcheck([]string{
			"-addr", addr,
			"-insecure-skip-verify",
			"-timeout", "2s",
		}) == 0
	}, 15*time.Second, 100*time.Millisecond, "insecure healthcheck should succeed against the running runtime")
}

func TestRunHealthcheckFailsWhenServerUnreachable(t *testing.T) {
	// A loopback port that nothing listens on makes the QUIC dial fail within the timeout
	addr := freeLoopbackUDPAddr(t)

	quietStdoutStderr(t, func() {
		// runHealthcheck returns non-zero when it cannot dial
		// It must not os.Exit, which would kill the test binary
		code := runHealthcheck([]string{
			"-addr", addr,
			"-insecure-skip-verify",
			"-timeout", "500ms",
		})
		assert.NotZero(t, code, "healthcheck should fail when the runtime is unreachable")
	})
}

func TestLoopbackBindAddr(t *testing.T) {
	tests := []struct {
		bind string
		want string
	}{
		{":8443", "127.0.0.1:8443"},
		{"0.0.0.0:8443", "127.0.0.1:8443"},
		{"127.0.0.1:8443", "127.0.0.1:8443"},
		{"example.com:8443", "127.0.0.1:8443"},
	}
	for _, tt := range tests {
		got, err := loopbackBindAddr(tt.bind)
		require.NoError(t, err, "bind %q", tt.bind)
		assert.Equal(t, tt.want, got, "bind %q", tt.bind)
	}

	// Missing port and unparseable addresses are rejected
	_, err := loopbackBindAddr("bad")
	assert.Error(t, err)
	_, err = loopbackBindAddr(":")
	assert.Error(t, err)
}
