package main

import (
	"context"
)

// subcommandFunc runs a subcommand with the arguments that follow its name, and returns the process's exit code
type subcommandFunc func(ctx context.Context, args []string) int

// subcommands holds every subcommand by name, which the file defining each one adds from an init function
var subcommands = map[string]subcommandFunc{}

// registerSubcommand adds a subcommand, and panics if another one has the same name, so a clash fails as soon as the binary starts
func registerSubcommand(name string, run subcommandFunc) {
	_, exists := subcommands[name]
	if exists {
		panic("subcommand '" + name + "' is registered twice")
	}

	subcommands[name] = run
}
