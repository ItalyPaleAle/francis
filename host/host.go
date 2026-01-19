// Package host provides backward compatibility for the local host package.
// New code should import github.com/italypaleale/francis/host/local directly.
package host

import (
	"github.com/italypaleale/francis/host/local"
)

// Host is the local actor host.
type Host = local.Host

// HostOption configures a host.
type HostOption = local.HostOption

// RegisterActorOptions contains options for registering an actor.
type RegisterActorOptions = local.RegisterActorOptions

// SQLiteProviderOptions configures the SQLite provider.
type SQLiteProviderOptions = local.SQLiteProviderOptions

// Re-export functions
var (
	NewHost                               = local.NewHost
	WithAddress                           = local.WithAddress
	WithBindPort                          = local.WithBindPort
	WithBindAddress                       = local.WithBindAddress
	WithServerTLSCertificate              = local.WithServerTLSCertificate
	WithServerTLSCA                       = local.WithServerTLSCA
	WithServerTLSInsecureSkipTLSValidation = local.WithServerTLSInsecureSkipTLSValidation
	WithLogger                            = local.WithLogger
	WithSQLiteProvider                    = local.WithSQLiteProvider
	WithPostgresProvider                  = local.WithPostgresProvider
	WithPeerAuthenticationSharedKey       = local.WithPeerAuthenticationSharedKey
	WithPeerAuthenticationMTLS            = local.WithPeerAuthenticationMTLS
	WithHostHealthCheckDeadline           = local.WithHostHealthCheckDeadline
	WithAlarmsPollInterval                = local.WithAlarmsPollInterval
	WithAlarmsLeaseDuration               = local.WithAlarmsLeaseDuration
	WithAlarmsFetchAheadInterval          = local.WithAlarmsFetchAheadInterval
	WithAlarmsFetchAheadBatchSize         = local.WithAlarmsFetchAheadBatchSize
	WithShutdownGracePeriod               = local.WithShutdownGracePeriod
	WithProviderRequestTimeout            = local.WithProviderRequestTimeout
)
