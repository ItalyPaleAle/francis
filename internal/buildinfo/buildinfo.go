package buildinfo

import (
	"fmt"
)

// These variables will be set at build time
var (
	AppName    string = "francis"
	AppVersion string = "canary"
	BuildId    string
	CommitHash string
	BuildDate  string
	Production string
)

// BuildDescription is the build description
// This is set during initialization
var BuildDescription string

func init() {
	if BuildId != "" && BuildDate != "" && CommitHash != "" {
		BuildDescription = fmt.Sprintf("%s, %s (%s)", BuildId, BuildDate, CommitHash)
	} else {
		BuildDescription = "null"
	}

	if Production != "1" {
		BuildDescription += " (non-production)"
	}
}
