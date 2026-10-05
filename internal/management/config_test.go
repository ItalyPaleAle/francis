package management

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	tokA := strings.Repeat("a", MinTokenLength)
	tokB := strings.Repeat("b", MinTokenLength)
	short := strings.Repeat("c", MinTokenLength-1)

	t.Run("applies the default bind", func(t *testing.T) {
		cfg := Config{ReadOnlyTokens: []string{tokA}}
		err := cfg.Validate()
		require.NoError(t, err)
		assert.Equal(t, DefaultBind, cfg.Bind)
	})

	t.Run("keeps a custom bind", func(t *testing.T) {
		cfg := Config{Bind: "0.0.0.0:9000", ManagementTokens: []string{tokA}}
		err := cfg.Validate()
		require.NoError(t, err)
		assert.Equal(t, "0.0.0.0:9000", cfg.Bind)
	})

	t.Run("accepts tokens in both lists", func(t *testing.T) {
		cfg := Config{ReadOnlyTokens: []string{tokA}, ManagementTokens: []string{tokB}}
		err := cfg.Validate()
		require.NoError(t, err)
	})

	tests := []struct {
		name   string
		cfg    Config
		errMsg string
	}{
		{name: "invalid bind without port", cfg: Config{Bind: "localhost", ReadOnlyTokens: []string{tokA}}, errMsg: "invalid management bind address"},
		{name: "invalid bind garbage", cfg: Config{Bind: "::::", ReadOnlyTokens: []string{tokA}}, errMsg: "invalid management bind address"},
		{name: "no tokens", cfg: Config{}, errMsg: "at least one read-only or management token"},
		{name: "empty token lists", cfg: Config{ReadOnlyTokens: []string{}, ManagementTokens: []string{}}, errMsg: "at least one read-only or management token"},
		{name: "short read-only token", cfg: Config{ReadOnlyTokens: []string{tokA, short}}, errMsg: "management read-only token at index 1 is shorter than 32 characters"},
		{name: "short management token", cfg: Config{ManagementTokens: []string{short}}, errMsg: "management management token at index 0 is shorter than 32 characters"},
		{name: "duplicate within read-only list", cfg: Config{ReadOnlyTokens: []string{tokA, tokB, tokA}}, errMsg: "management read-only token at index 2 is a duplicate"},
		{name: "duplicate within management list", cfg: Config{ManagementTokens: []string{tokB, tokB}}, errMsg: "management management token at index 1 is a duplicate"},
		{name: "duplicate across lists", cfg: Config{ReadOnlyTokens: []string{tokA}, ManagementTokens: []string{tokB, tokA}}, errMsg: "management management token at index 1 is a duplicate"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			require.Error(t, err)
			assert.ErrorContains(t, err, tc.errMsg)
		})
	}
}
