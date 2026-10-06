package main

import (
	"context"
	"fmt"

	"github.com/italypaleale/francis/internal/buildinfo"
)

func init() {
	// The version subcommand prints the application's version
	registerSubcommand("version", runVersion)
}

// runVersion shows the app version
func runVersion(_ context.Context, _ []string) int {
	fmt.Printf("%s %s\n", buildinfo.AppName, buildinfo.BuildDescription)
	return 0
}
