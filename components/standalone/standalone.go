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
