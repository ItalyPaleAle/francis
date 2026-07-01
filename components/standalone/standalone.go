// Package standalone provides in-memory actor providers with optional persistence.
//
// The standalone providers keep all data in memory for fast access, but can optionally persist changes to a backing store (SQLite or PostgreSQL) for durability across restarts.
//
// Available providers:
//   - StandaloneMemory: Pure in-memory, no persistence
//   - StandaloneSQLiteBacked: In-memory with SQLite persistence
//   - StandalonePostgresBacked: In-memory with PostgreSQL persistence
//
// These providers are designed for single-instance deployments. For multi-instance deployments with coordination, use the sqlite or postgres packages instead.
package standalone

import (
	"fmt"
	"strings"
)

// DefaultTablePrefix is the prefix added to the name of every table (and other schema object) when none is configured.
const DefaultTablePrefix = "francis"

// resolveTablePrefix resolves the configured table prefix into the value stored on a provider.
// An unset (empty) prefix falls back to the default.
// A non-empty prefix is returned with a trailing separator so tables are named e.g. "francis_hosts".
func resolveTablePrefix(tablePrefix string) string {
	if tablePrefix == "" {
		tablePrefix = DefaultTablePrefix
	}
	if tablePrefix == "" {
		return ""
	}
	return tablePrefix + "_"
}

// applyTablePrefix applies the given (already-resolved) table prefix to a query loaded from an embedded migration script.
// In those files, every table (and other schema object) name is written with a "%s" placeholder immediately before it (e.g. "%shosts"), which this replaces with the prefix.
func applyTablePrefix(tablePrefix string, query string) string {
	n := strings.Count(query, "%s")
	if n == 0 {
		return query
	}

	args := make([]any, n)
	for i := range args {
		args[i] = tablePrefix
	}

	// The only value interpolated here is the statically-derived table prefix, so there's no risk of SQL injection
	// #nosec G201
	return fmt.Sprintf(query, args...)
}
