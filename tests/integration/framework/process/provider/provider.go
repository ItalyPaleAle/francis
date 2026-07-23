//go:build integration

// Package provider abstracts the actor-provider backend used by an integration test topology, so a scenario can pick any supported provider and have the harness manage its lifecycle (temp SQLite file, Postgres schema, shared DB handles, ...).
//
// The same backend serves both runtime topologies:
//
// - In the local topology each host embeds the provider, so the backend yields a local.HostOption per host
// - In the remote topology a single runtime owns the provider, so the backend builds one components.ActorProvider for it
package provider

import (
	"log/slog"
	"testing"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/host/local"
	"github.com/italypaleale/francis/tests/integration/framework/process"
)

// Variant identifies a supported provider backend
type Variant string

const (
	// SQLite is the multi-instance SQLite provider, backed by a shared file
	SQLite Variant = "sqlite"
	// Postgres is the multi-instance Postgres provider, backed by a per-run schema
	Postgres Variant = "postgres"
	// StandaloneMemory is the single-instance, pure in-memory provider
	StandaloneMemory Variant = "standalone-memory"
	// StandaloneSQLite is the single-instance in-memory provider with SQLite persistence
	StandaloneSQLite Variant = "standalone-sqlite"
	// StandalonePostgres is the single-instance in-memory provider with Postgres persistence
	StandalonePostgres Variant = "standalone-postgres"
)

// LocalMultiHost reports whether more than one local host may share this backend
// The standalone providers coordinate nothing across processes, so they are single-instance in the local topology
// In the remote topology coordination lives in the runtime, so any variant supports multiple hosts there
func (v Variant) LocalMultiHost() bool {
	return v == SQLite || v == Postgres
}

// SharedStore reports whether the backing store can be opened by more than one process at once
// It gates running multiple runtime replicas against one store: the on-disk SQLite file and the Postgres schema qualify, while the in-memory standalone connection cannot be shared
func (v Variant) SharedStore() bool {
	return v == SQLite || v == Postgres
}

// Backend owns the provider-side store of a test topology
// It is a process so its store is prepared before, and torn down after, the hosts and runtime that use it
type Backend interface {
	process.Interface

	// Variant returns the backend's variant
	Variant() Variant

	// LocalHostOption returns the option a local host passes to local.NewHost to embed this store
	// It is valid only after Run and may be called once per host
	LocalHostOption(t *testing.T) local.HostOption

	// NewProvider builds a provider instance against the shared store for a runtime to own
	// It is valid only after Run
	NewProvider(t *testing.T, log *slog.Logger) components.ActorProvider

	// ProviderOptions returns the raw provider options for the shared store, so a scenario can build a second provider against the same backend, such as a clusteradmin.Admin
	// It is valid only after Run
	ProviderOptions(t *testing.T) components.ProviderOptions
}

// New builds the Backend for the given variant
func New(v Variant) Backend {
	switch v {
	case SQLite:
		return &sqliteBackend{}
	case Postgres:
		return &postgresBackend{standalone: false}
	case StandaloneMemory:
		return &standaloneMemoryBackend{}
	case StandaloneSQLite:
		return &standaloneSQLiteBackend{}
	case StandalonePostgres:
		return &postgresBackend{standalone: true}
	default:
		panic("integration: unknown provider variant: " + string(v))
	}
}

// All returns every supported variant, for table-driven scenarios
func All() []Variant {
	return []Variant{SQLite, Postgres, StandaloneMemory, StandaloneSQLite, StandalonePostgres}
}

// providerConfig returns the provider configuration used across the harness
// It mirrors the component defaults so behavior matches a real deployment
func providerConfig() components.ProviderConfig {
	return components.ProviderConfig{
		HostHealthCheckDeadline:   components.DefaultHostHealthCheckDeadline,
		AlarmsLeaseDuration:       components.DefaultAlarmsLeaseDuration,
		AlarmsFetchAheadInterval:  components.DefaultAlarmsFetchAheadInterval,
		AlarmsFetchAheadBatchSize: components.DefaultAlarmsFetchAheadBatchSize,
	}
}
