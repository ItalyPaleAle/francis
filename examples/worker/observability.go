package main

import (
	"context"

	kitconfig "github.com/italypaleale/go-kit/config"
	"github.com/italypaleale/go-kit/observability"
	"github.com/italypaleale/go-kit/servicerunner"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
)

// appName identifies this worker as the source of its telemetry
const appName = "francis-worker"

// obsConfig is a minimal implementation of kitconfig.Base so the go-kit observability initializers can build an OpenTelemetry resource
// A real application would implement these methods on its own configuration object
type obsConfig struct {
	loadedConfigPath string
	instanceID       string
}

func (c *obsConfig) GetLoadedConfigPath() string {
	return c.loadedConfigPath
}

func (c *obsConfig) SetLoadedConfigPath(path string) {
	c.loadedConfigPath = path
}

func (c *obsConfig) GetInstanceID() string {
	return c.instanceID
}

func (c *obsConfig) GetOtelResource(name string) (*resource.Resource, error) {
	return resource.NewSchemaless(
		attribute.String("service.name", name),
		attribute.String("service.instance.id", c.GetInstanceID()),
	), nil
}

// initObservability wires up OpenTelemetry tracing and logging for the worker
// In the local topology the application owns the OpenTelemetry setup: InitTraces installs the global tracer and the W3C trace-context propagator that Francis uses to carry a trace between hosts
// Everything is driven by the standard OTEL_* environment variables, so tracing and log export stay off until an exporter is configured
// It returns the telemetry shutdown functions, which are themselves servicerunner Services, to be flushed once the worker has drained
func initObservability(ctx context.Context) ([]servicerunner.Service, error) {
	// Resolve a stable instance ID once so every signal's resource shares it
	cfg := &obsConfig{}
	instanceID, err := kitconfig.GetInstanceID()
	if err != nil {
		return nil, err
	}
	cfg.instanceID = instanceID

	// Logs: written to stdout, and also exported via OpenTelemetry when OTEL_LOGS_EXPORTER is set, carrying the active trace and span IDs so logs correlate with traces
	logger, logShutdown, err := observability.InitLogs(ctx, observability.InitLogsOpts{
		Level:      "debug",
		Config:     cfg,
		AppName:    appName,
		AppVersion: "example",
	})
	if err != nil {
		return nil, err
	}

	// Publish the logger as the package-wide logger used across this example
	log = logger

	// Traces: installs the global tracer provider and the W3C trace-context propagator, with the exporter selected by OTEL_TRACES_EXPORTER
	_, traceShutdown, err := observability.InitTraces(ctx, observability.InitTracesOpts{
		Config:  cfg,
		AppName: appName,
	})
	if err != nil {
		return nil, err
	}

	return []servicerunner.Service{traceShutdown, logShutdown}, nil
}
