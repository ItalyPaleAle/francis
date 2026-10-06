package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServeDashboard(t *testing.T) {
	files := fstest.MapFS{
		"index.html": {Data: []byte(`<html><head><meta name="francis-dashboard-mode" content="embedded" /></head></html>`)},
	}

	// Reserve a free port, then release it for the server
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	err = ln.Close()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- serveDashboard(ctx, files, addr)
	}()

	// Wait for the server to accept connections
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		conn, rErr := net.Dial("tcp", addr)
		if assert.NoError(c, rErr) {
			_ = conn.Close()
		}
	}, 5*time.Second, 50*time.Millisecond)

	// A navigation to one of the dashboard's routes gets the page, in standalone mode
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/hosts", nil)
	require.NoError(t, err)
	req.Header.Set("Accept", "text/html")
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Contains(t, string(body), `content="standalone"`)

	// Canceling the context stops the server
	cancel()
	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the server did not stop")
	}
}

func TestDisplayAddress(t *testing.T) {
	tests := []struct {
		name     string
		address  string
		expected string
	}{
		{name: "IPv4 address", address: "127.0.0.1:7402", expected: "127.0.0.1:7402"},
		{name: "unspecified IPv4 address", address: "0.0.0.0:7402", expected: "localhost:7402"},
		{name: "unspecified IPv6 address", address: "[::]:7402", expected: "localhost:7402"},
		{name: "IPv6 loopback address", address: "[::1]:7402", expected: "[::1]:7402"},
		{name: "invalid address", address: "garbage", expected: "garbage"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, displayAddress(tt.address))
		})
	}
}
