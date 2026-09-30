package netutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsLoopbackBind(t *testing.T) {
	tests := []struct {
		bind string
		want bool
	}{
		{bind: "127.0.0.1:7401", want: true},
		{bind: "127.1.2.3:80", want: true},
		{bind: "[::1]:7401", want: true},
		{bind: "localhost:7401", want: true},
		{bind: "LocalHost:7401", want: true},
		{bind: "0.0.0.0:7401", want: false},
		{bind: "[::]:7401", want: false},
		{bind: ":7401", want: false},
		{bind: "10.0.0.1:7401", want: false},
		{bind: "example.com:7401", want: false},
		{bind: "not-an-address", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.bind, func(t *testing.T) {
			assert.Equal(t, tc.want, IsLoopbackBind(tc.bind))
		})
	}
}
