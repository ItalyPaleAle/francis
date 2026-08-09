//go:build integration

// Package faults holds the scenarios that break a running cluster and assert it recovers
//
// The existing failover scenarios stop a host or a runtime cleanly, which is the case where the cluster is told what happened
// These cover the cases where it is not: a network cut between a host and the control plane, a peer that stops answering mid-invocation, a host that dies without deregistering, and a host whose database stops answering so its health checks fail
// Every scenario runs on SQLite, since what is being exercised is the host, runtime, and placement behavior rather than anything provider-specific, and each one shortens the health, lease, and request timeouts so a failure is detected in seconds instead of the production defaults
package faults

import (
	"strconv"
	"time"

	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	"github.com/italypaleale/francis/tests/integration/suite"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

const (
	// healthCheckDeadline is how long a host registration survives without a health check
	// The retry policy scales itself to this short deadline so failure scenarios finish quickly
	healthCheckDeadline = 4 * time.Second
	// queryTimeout gives ordinary SQLite work enough headroom on slower Windows runners while still bounding stalled queries
	// Health checks remain limited by the policy's shorter per-attempt timeout
	queryTimeout = 2 * time.Second
	// requestTimeout leaves enough time for provider initialization while still bounding provider requests, runtime requests, and peer dials
	// Health checks do not inherit this timeout because their retry policy supplies its own attempt context
	requestTimeout = 3 * time.Second
	// alarmsLeaseDuration is how long a host holds the lease on an alarm it is executing, and therefore how long a surviving host waits before it can take over one whose owner died
	alarmsLeaseDuration = 5 * time.Second
	// alarmsPollInterval keeps alarms firing quickly enough to observe a handover within a test
	alarmsPollInterval = 250 * time.Millisecond

	// recoveryTimeout bounds how long a scenario waits for the cluster to route around a failure
	// It is generous because recovery is gated on a registration or a lease expiring and then on placement settling
	recoveryTimeout = 90 * time.Second
	// recoveryInterval is how often a scenario retries while waiting for recovery
	recoveryInterval = 500 * time.Millisecond
)

// Register the fault scenarios
func init() {
	suite.Register(&runtimePartition{})
	suite.Register(&peerPartition{})
	suite.Register(&silentHostDeath{})
	suite.Register(&healthCheckFailure{})
	suite.Register(&alarmAfterSilentDeath{})
	suite.Register(&hostDeathMidInvocation{})
	suite.Register(&providerOutage{})
	suite.Register(&jobSurvivesHostDeath{})
	suite.Register(&addressConflict{})
	suite.Register(&stateWriteFailure{})
	suite.Register(&linkFlapping{})
}

// labelHosts assigns each host a stable label and returns them in host order, so the probe can report which host ran an invocation or an alarm
func labelHosts(c *cluster.Cluster) []string {
	labels := make([]string, c.Len())
	for i := range c.Len() {
		labels[i] = "h" + strconv.Itoa(i)
		shared.SetHostLabel(c.Service(i), labels[i])
	}
	return labels
}

// hostIndex returns the index of the host carrying the given label, or -1
func hostIndex(labels []string, label string) int {
	for i, l := range labels {
		if l == label {
			return i
		}
	}
	return -1
}
