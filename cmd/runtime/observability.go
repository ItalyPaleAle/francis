package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kitconfig "github.com/italypaleale/go-kit/config"
	"github.com/italypaleale/go-kit/observability"
	"github.com/italypaleale/go-kit/servicerunner"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/italypaleale/francis/internal/buildinfo"
)

// observabilityShutdownTimeout bounds how long the telemetry exporters get to flush during shutdown
const observabilityShutdownTimeout = 10 * time.Second

// shutdownServices returns the telemetry shutdown functions as servicerunner Services
func (o *observabilityHandles) shutdownServices() []servicerunner.Service {
	return []servicerunner.Service{
		o.traceShutdown,
		o.metricShutdown,
		o.logShutdown,
	}
}

// GetLoadedConfigPath returns the path the config was loaded from
func (cfg *config) GetLoadedConfigPath() string {
	return cfg.loadedConfigPath
}

// SetLoadedConfigPath records the path the config was loaded from
func (cfg *config) SetLoadedConfigPath(path string) {
	cfg.loadedConfigPath = path
}

// GetInstanceID returns the resolved OpenTelemetry instance ID
func (cfg *config) GetInstanceID() string {
	return cfg.instanceID
}

// GetOtelResource builds the OpenTelemetry resource describing this runtime
// It is schemaless so it never returns an error, which the go-kit initializers treat as fatal
func (cfg *config) GetOtelResource(name string) (*resource.Resource, error) {
	return resource.NewSchemaless(
		attribute.String("service.name", name),
		attribute.String("service.version", buildinfo.AppVersion),
		attribute.String("service.instance.id", cfg.GetInstanceID()),
	), nil
}

// observabilityHandles bundles the initialized telemetry handles and their shutdown functions
type observabilityHandles struct {
	log            *slog.Logger
	meter          metric.Meter
	traceShutdown  func(ctx context.Context) error
	metricShutdown func(ctx context.Context) error
	logShutdown    func(ctx context.Context) error
}

// initObservability initializes logs, traces, and metrics using go-kit
// Traces and metrics are configured entirely through the standard OTEL_* env vars, so export stays off until an exporter is configured
func initObservability(ctx context.Context, cfg *config) (*observabilityHandles, error) {
	// Resolve a stable instance ID once so every signal's resource shares it
	instanceID, err := kitconfig.GetInstanceID()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve instance ID: %w", err)
	}
	cfg.instanceID = instanceID

	// Logs: level and format come from config, while OTLP export is controlled by OTEL_LOGS_EXPORTER
	log, logShutdown, err := observability.InitLogs(ctx, cfg.getObservabilityInitLogOpts())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logging: %w", err)
	}

	// Traces: the sampler and exporter are driven entirely by the OTEL_* env vars, so Sampler is left nil
	_, traceShutdown, err := observability.InitTraces(ctx, observability.InitTracesOpts{
		Config:  cfg,
		AppName: buildinfo.AppName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize tracing: %w", err)
	}

	// Metrics: the meter is always created, while OTLP export is controlled by OTEL_METRICS_EXPORTER
	meter, metricShutdown, err := observability.InitMetrics(ctx, observability.InitMetricsOpts{
		Config:  cfg,
		AppName: buildinfo.AppName,
		Prefix:  buildinfo.AppName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics: %w", err)
	}

	return &observabilityHandles{
		log:            log,
		meter:          meter,
		traceShutdown:  traceShutdown,
		metricShutdown: metricShutdown,
		logShutdown:    logShutdown,
	}, nil
}
