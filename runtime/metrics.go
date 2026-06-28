package runtime

import (
	"go.opentelemetry.io/otel/metric"
)

// runtimeMetrics holds the OpenTelemetry instruments recorded by the runtime
type runtimeMetrics struct {
	// rpcRequests counts host requests handled, tagged by message kind
	rpcRequests metric.Int64Counter
	// rpcDuration records how long each request took to handle, in seconds
	rpcDuration metric.Float64Histogram
	// alarmsExecuted counts alarms the runtime dispatched and completed
	alarmsExecuted metric.Int64Counter
	// hostsConnected tracks how many hosts are currently connected to the runtime
	hostsConnected metric.Int64UpDownCounter
}

// newRuntimeMetrics creates the runtime instruments from the meter
// A no-op meter yields no-op instruments, so callers can record without nil checks
func newRuntimeMetrics(meter metric.Meter) (*runtimeMetrics, error) {
	rpcRequests, err := meter.Int64Counter(
		"francis.runtime.rpc.requests",
		metric.WithDescription("Number of host requests handled by the runtime"),
	)
	if err != nil {
		return nil, err
	}

	rpcDuration, err := meter.Float64Histogram(
		"francis.runtime.rpc.duration",
		metric.WithDescription("Duration of runtime request handling"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	alarmsExecuted, err := meter.Int64Counter(
		"francis.runtime.alarms.executed",
		metric.WithDescription("Number of alarms dispatched and completed by the runtime"),
	)
	if err != nil {
		return nil, err
	}

	hostsConnected, err := meter.Int64UpDownCounter(
		"francis.runtime.hosts.connected",
		metric.WithDescription("Number of hosts currently connected to the runtime"),
	)
	if err != nil {
		return nil, err
	}

	return &runtimeMetrics{
		rpcRequests:    rpcRequests,
		rpcDuration:    rpcDuration,
		alarmsExecuted: alarmsExecuted,
		hostsConnected: hostsConnected,
	}, nil
}
