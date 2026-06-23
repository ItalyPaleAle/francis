package main

import (
	"fmt"

	"github.com/italypaleale/francis/internal/buildinfo"
)

// runVersion shows the app version
func runVersion() {
	fmt.Printf("%s %s\n", buildinfo.AppName, buildinfo.BuildDescription)
}
