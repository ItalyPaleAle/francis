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
	"time"

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

// Options tunes the provider a backend builds
// The zero value keeps the component defaults, which is what every scenario that is not about failure handling wants
type Options struct {
	// HostHealthCheckDeadline overrides how long a host registration survives without a health check
	// Failure-detection scenarios shorten it so a host that stops reporting is expired in seconds rather than the default twenty
	HostHealthCheckDeadline time.Duration

	// AlarmsLeaseDuration overrides how long an alarm lease is held
	// Scenarios that kill the host executing an alarm shorten it so another host can take the lease over quickly
	AlarmsLeaseDuration time.Duration

	// QueryTimeout overrides the timeout the provider applies to a single database query, where the provider supports one
	// Shortening it makes a database that has stopped answering surface as an error quickly instead of hanging on the default timeout
	QueryTimeout time.Duration

	// HealthCheck overrides how hosts retry their health checks, which also sets the shortest deadline the provider accepts
	// Failure scenarios shrink it so they can run against a deadline of a few seconds rather than the twelve the default policy needs
	HealthCheck components.HealthCheckPolicy

	// Stallable makes the backend hand every consumer its own database handle whose pool a scenario can exhaust on demand, simulating a database that has become unavailable for that consumer alone
	// Only the SQLite backend supports it
	Stallable bool
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

// Stallable is implemented by a backend that can simulate a database outage affecting a single consumer
//
// Consumers are numbered in the order the backend hands out database handles, which is the order the framework starts the processes that own them: one per local host on the local topology, one per runtime replica on the remote one
type Stallable interface {
	// Stall makes every provider call on the given consumer's database handle block until Unstall
	Stall(t *testing.T, consumer int)

	// Unstall lets the consumer's provider calls through again
	// It is idempotent, so a scenario can call it unconditionally during cleanup
	Unstall(t *testing.T, consumer int)
}

// New builds the Backend for the given variant, with the given options applied to the provider it constructs
func New(v Variant, opts Options) Backend {
	switch v {
	case SQLite:
		return &sqliteBackend{opts: opts}
	case Postgres:
		return &postgresBackend{standalone: false, opts: opts}
	case StandaloneMemory:
		return &standaloneMemoryBackend{opts: opts}
	case StandaloneSQLite:
		return &standaloneSQLiteBackend{opts: opts}
	case StandalonePostgres:
		return &postgresBackend{standalone: true, opts: opts}
	default:
		panic("integration: unknown provider variant: " + string(v))
	}
}

// All returns every supported variant, for table-driven scenarios
func All() []Variant {
	return []Variant{SQLite, Postgres, StandaloneMemory, StandaloneSQLite, StandalonePostgres}
}

// providerConfig returns the provider configuration used across the harness
// It mirrors the component defaults so behavior matches a real deployment, with any overrides the scenario asked for applied on top
func providerConfig(opts Options) components.ProviderConfig {
	cfg := components.ProviderConfig{
		HostHealthCheckDeadline:   components.DefaultHostHealthCheckDeadline,
		AlarmsLeaseDuration:       components.DefaultAlarmsLeaseDuration,
		AlarmsFetchAheadInterval:  components.DefaultAlarmsFetchAheadInterval,
		AlarmsFetchAheadBatchSize: components.DefaultAlarmsFetchAheadBatchSize,
	}
	if opts.HostHealthCheckDeadline > 0 {
		cfg.HostHealthCheckDeadline = opts.HostHealthCheckDeadline
	}
	if opts.AlarmsLeaseDuration > 0 {
		cfg.AlarmsLeaseDuration = opts.AlarmsLeaseDuration
	}
	cfg.HealthCheck = &opts.HealthCheck
	return cfg
}
