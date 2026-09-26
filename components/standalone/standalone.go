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
	"database/sql"
	"strings"

	"github.com/italypaleale/francis/components"
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

// applyTablePrefix applies the given (already-resolved) prefixes to a query loaded from an embedded migration script.
// In those files, every reference to a table (or other schema object) is written with a "%s" placeholder immediately before the name (e.g. "%shosts"), which this replaces with tablePrefix, including the schema if any.
// Names that can't be schema-qualified, such as those of new indexes, use a "%p" placeholder instead, which this replaces with namePrefix.
func applyTablePrefix(query string, tablePrefix string, namePrefix string) string {
	// The only values interpolated here are the statically-derived prefixes (with the schema quoted), so there's no risk of SQL injection
	return strings.
		NewReplacer(
			"%s", tablePrefix,
			"%p", namePrefix,
		).
		Replace(query)
}

// encodeWorkflowLabels serializes an actor state's workflow labels for the backing store, returning nil when there are none so the column stays NULL
func encodeWorkflowLabels(labels *components.WorkflowLabels) any {
	if labels == nil {
		return nil
	}

	// The struct is three plain fields and the encoding never fails for them, so an error here would be a programming error rather than a runtime condition
	enc, err := labels.JSON()
	if err != nil || enc == "" {
		return nil
	}
	return enc
}

// decodeWorkflowLabels reads an actor state's workflow labels back from the backing store, treating a NULL or unparseable column as none
func decodeWorkflowLabels(raw sql.NullString) *components.WorkflowLabels {
	if !raw.Valid || raw.String == "" {
		return nil
	}

	labels, err := components.DecodeWorkflowLabels([]byte(raw.String))
	if err != nil {
		return nil
	}
	return labels
}
