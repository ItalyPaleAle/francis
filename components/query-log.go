package components

import (
	"time"
)

// QueryLogConfig controls optional SQL statement logging
// SQL statement traces and their text are emitted independently from this configuration
type QueryLogConfig struct {
	// Enabled logs every SQL statement at Debug level with its duration
	Enabled bool

	// IncludeParameters includes query parameter values in traces and in SQL logs that include statement text
	// Parameter values may contain sensitive information and are excluded by default
	IncludeParameters bool

	// SlowThreshold logs a Warn record for every SQL statement whose duration reaches this value
	// A value of 0 (the default) disables slow-query warnings
	SlowThreshold time.Duration
}

// OperationLogConfig controls optional provider-operation logging
// Provider-operation traces are emitted independently from this configuration
type OperationLogConfig struct {
	// Enabled logs every provider operation at Debug level with its duration
	Enabled bool

	// SlowThreshold logs a Warn record for every successful provider operation whose duration reaches this value
	// Expected domain outcomes are treated like successful operations while warnings and failures use their policy level
	// A value of 0 (the default) disables slow-operation warnings
	SlowThreshold time.Duration
}
