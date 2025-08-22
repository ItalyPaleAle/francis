// This code was adapted from https://github.com/dapr/components-contrib/blob/v1.14.6/
// Copyright (C) 2023 The Dapr Authors
// License: Apache2

package sqlite

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultBusyTimeout      = 2500 * time.Millisecond
	DefaultConnectionString = "data.db"
)

// sqliteConnectionString is the connection string for a SQLite database
type sqliteConnectionString string

// IsInMemoryDB returns true if the connection string is for an in-memory database.
func (m sqliteConnectionString) IsInMemoryDB() bool {
	lc := strings.ToLower(string(m))

	// First way to define an in-memory database is to use ":memory:" or "file::memory:" as connection string
	if strings.HasPrefix(lc, ":memory:") || strings.HasPrefix(lc, "file::memory:") {
		return true
	}

	// Another way is to pass "mode=memory" in the "query string"
	idx := strings.IndexRune(lc, '?')
	if idx < 0 {
		return false
	}
	qs, _ := url.ParseQuery(lc[(idx + 1):])

	return len(qs["mode"]) > 0 && qs["mode"][0] == "memory"
}

// Parse and validate the connection string.
func (m *sqliteConnectionString) Parse(log *slog.Logger) error {
	if m == nil {
		return errors.New("connection string pointer cannot be nil")
	}
	if *m == "" {
		*m = sqliteConnectionString(DefaultConnectionString)
	}

	connString := string(*m)

	// Check if we're using the in-memory database
	isMemoryDB := m.IsInMemoryDB()

	// Get the "query string" from the connection string if present
	idx := strings.IndexRune(connString, '?')
	var qs url.Values
	if idx > 0 {
		qs, _ = url.ParseQuery(connString[(idx + 1):])
	}
	if len(qs) == 0 {
		qs = make(url.Values, 2)
	}

	// If the database is in-memory, we must ensure that cache=shared is set
	if isMemoryDB {
		qs["cache"] = []string{"shared"}
	}

	// Check if the database is read-only or immutable
	isReadOnly := false
	if len(qs["mode"]) > 0 {
		// Keep the first value only
		qs["mode"] = []string{
			strings.ToLower(qs["mode"][0]),
		}
		if qs["mode"][0] == "ro" {
			isReadOnly = true
		}
	}
	if len(qs["immutable"]) > 0 {
		// Keep the first value only
		qs["immutable"] = []string{
			strings.ToLower(qs["immutable"][0]),
		}
		if qs["immutable"][0] == "1" {
			isReadOnly = true
		}
	}

	// We do not want to override a _txlock if set, but we'll show a warning if it's not "immediate"
	if len(qs["_txlock"]) > 0 {
		// Keep the first value only
		qs["_txlock"] = []string{
			strings.ToLower(qs["_txlock"][0]),
		}
		if qs["_txlock"][0] != "immediate" {
			log.Warn("Database connection is being created with a _txlock different from the recommended value 'immediate'")
		}
	} else {
		qs["_txlock"] = []string{"immediate"}
	}

	// Add pragma values
	var hasBusyTimeout, hasJournalMode bool
	if len(qs["_pragma"]) == 0 {
		qs["_pragma"] = make([]string, 0, 3)
	} else {
		for _, p := range qs["_pragma"] {
			p = strings.ToLower(p)
			switch {
			case strings.HasPrefix(p, "busy_timeout"):
				hasBusyTimeout = true
			case strings.HasPrefix(p, "journal_mode"):
				hasJournalMode = true
			case strings.HasPrefix(p, "foreign_keys"):
				return errors.New("found forbidden option '_pragma=foreign_keys' in the connection string")
			}
		}
	}
	if !hasBusyTimeout {
		qs["_pragma"] = append(qs["_pragma"], fmt.Sprintf("busy_timeout(%d)", DefaultBusyTimeout.Milliseconds()))
	}
	if !hasJournalMode {
		switch {
		case isMemoryDB:
			// For in-memory databases, set the journal to MEMORY, the only allowed option besides OFF (which would make transactions ineffective)
			qs["_pragma"] = append(qs["_pragma"], "journal_mode(MEMORY)")
		case isReadOnly:
			// Set the journaling mode to "DELETE" (the default) if the database is read-only
			qs["_pragma"] = append(qs["_pragma"], "journal_mode(DELETE)")
		default:
			// Enable WAL
			qs["_pragma"] = append(qs["_pragma"], "journal_mode(WAL)")
		}
	}
	qs["_pragma"] = append(qs["_pragma"], "foreign_keys(1)")

	// Build the final connection string
	if idx > 0 {
		connString = connString[:idx]
	}
	connString += "?" + qs.Encode()

	// If the connection string doesn't begin with "file:", add the prefix
	if !strings.HasPrefix(strings.ToLower(connString), "file:") {
		connString = "file:" + connString
	}

	*m = sqliteConnectionString(connString)

	return nil
}
