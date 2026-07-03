package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_config_loopbackBindAddr(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for receiver constructor.
		path    string
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := loadConfig(tt.path)
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}
			got, gotErr := cfg.loopbackBindAddr()
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("loopbackBindAddr() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("loopbackBindAddr() succeeded unexpectedly")
			}
			// TODO: update the condition below to compare got with tt.want.
			if true {
				t.Errorf("loopbackBindAddr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoopbackBindAddr(t *testing.T) {
	var cfg config

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
		cfg = config{Bind: tt.bind}
		got, err := cfg.loopbackBindAddr()
		require.NoError(t, err, "bind %q", tt.bind)
		assert.Equal(t, tt.want, got, "bind %q", tt.bind)
	}

	// Missing port and unparseable addresses are rejected
	cfg = config{Bind: "bad"}
	_, err := cfg.loopbackBindAddr()
	require.Error(t, err)

	cfg = config{Bind: ":"}
	_, err = cfg.loopbackBindAddr()
	require.Error(t, err)
}
