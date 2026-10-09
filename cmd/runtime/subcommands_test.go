package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubcommands(t *testing.T) {
	t.Run("every subcommand is registered", func(t *testing.T) {
		// The demo subcommand is only in builds with the demo build tag, so it isn't required here
		for _, name := range []string{"healthcheck", "print-ca", "dashboard", "backup", "restore", "version"} {
			assert.NotNil(t, subcommands[name], "subcommand %s", name)
		}
	})

	t.Run("a name can't be registered twice", func(t *testing.T) {
		assert.Panics(t, func() {
			registerSubcommand("version", func(context.Context, []string) int { return 0 })
		})
	})
}
